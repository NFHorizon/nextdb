import assert from "node:assert/strict"

const baseUrl = process.env.NEXTDB_BASE_URL ?? "http://127.0.0.1:3188"
const testId = `rt-${Date.now()}-${Math.random().toString(36).slice(2)}`
const channelId = `channel-${testId}`
const aliceUserId = `alice-${testId}`
const bobUserId = `bob-${testId}`
const alicePhoneSessionId = `${aliceUserId}-phone`
const aliceDesktopSessionId = `${aliceUserId}-desktop`
const aliceTabletSessionId = `${aliceUserId}-tablet`
const bobSessionId = `${bobUserId}-laptop`
const bobTabletSessionId = `${bobUserId}-tablet`
const adminUserId = `admin-${testId}`
const adminSessionId = `${adminUserId}-console`

const sockets = []

try {
  const health = await getJson("/v1/health")
  assert.equal(health.ok, true, `server at ${baseUrl} is not healthy`)

  const admin = await openRealtimeSocket(adminUserId, adminSessionId)
  sockets.push(admin)
  admin.send({ type: "subscribeConnectionEvents" })
  await admin.waitForFrame((frame) => frame.type === "connectionEventsSubscribed", "connection events subscribed")

  const alicePhone = await openRealtimeSocket(aliceUserId, alicePhoneSessionId, {
    device: "phone",
    capabilities: ["audio", "video"],
  })
  const aliceDesktop = await openRealtimeSocket(aliceUserId, aliceDesktopSessionId)
  const aliceTablet = await openRealtimeSocket(aliceUserId, aliceTabletSessionId)
  const bob = await openRealtimeSocket(bobUserId, bobSessionId)
  const bobTablet = await openRealtimeSocket(bobUserId, bobTabletSessionId)
  sockets.push(alicePhone, aliceDesktop, aliceTablet, bob, bobTablet)
  await admin.waitForConnectionEvent("connected", alicePhoneSessionId)

  await waitForConnections(aliceUserId, [alicePhoneSessionId, aliceDesktopSessionId, aliceTabletSessionId])
  await waitForConnections(bobUserId, [bobSessionId, bobTabletSessionId])

  const alicePhoneConnection = await waitForConnectionSession(aliceUserId, alicePhoneSessionId)
  assert.equal(alicePhoneConnection.metadata.device, "phone")
  assert.deepEqual(alicePhoneConnection.metadata.capabilities, ["audio", "video"])
  alicePhone.send({
    type: "updateConnectionMetadata",
    metadata: { device: "phone", capabilities: ["audio", "video", "game"], rttBucket: "low" },
  })
  const metadataAck = await alicePhone.waitForFrame(
    (frame) => frame.type === "connectionMetadataUpdated" && frame.session?.sessionId === alicePhoneSessionId,
    "connection metadata updated",
  )
  assert.deepEqual(metadataAck.session.metadata.capabilities, ["audio", "video", "game"])
  await admin.waitForConnectionEvent(
    "metadataUpdated",
    alicePhoneSessionId,
    (event) => event.session?.metadata?.rttBucket === "low",
  )

  const missingSessionJoin = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(channelId)}/join`, {
    userId: aliceUserId,
    sessionId: `${aliceUserId}-missing`,
    metadata: { device: "missing" },
  })
  assert.equal(missingSessionJoin.status, 400)
  assert.match(missingSessionJoin.body.error, /active connection/)

  const stolenSessionJoin = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(channelId)}/join`, {
    userId: bobUserId,
    sessionId: alicePhoneSessionId,
    metadata: { device: "wrong-user" },
  })
  assert.equal(stolenSessionJoin.status, 400)
  assert.match(stolenSessionJoin.body.error, /active connection/)

  alicePhone.send({ type: "subscribeObjects" })
  await alicePhone.waitForFrame((frame) => frame.type === "objectsSubscribed", "alice phone objects subscribed")
  await admin.waitForConnectionEvent(
    "subscriptionsUpdated",
    alicePhoneSessionId,
    (event) => event.session?.subscribedObjects === true,
  )
  aliceDesktop.send({ type: "subscribeUserEvents" })
  await waitForConnectionSubscriptions(aliceUserId, {
    objectSessionIds: [alicePhoneSessionId],
    userEventSessionIds: [aliceDesktopSessionId],
  })
  aliceDesktop.send({ type: "unsubscribeUserEvents" })
  await aliceDesktop.waitForFrame((frame) => frame.type === "userEventsUnsubscribed", "alice desktop user events unsubscribed")
  await waitForConnectionSubscriptions(aliceUserId, {
    objectSessionIds: [alicePhoneSessionId],
    userEventSessionIds: [],
  })
  aliceDesktop.send({ type: "subscribeUserEvents" })
  await waitForConnectionSubscriptions(aliceUserId, {
    objectSessionIds: [alicePhoneSessionId],
    userEventSessionIds: [aliceDesktopSessionId],
  })
  const aliceWebSocketConnections = await getJson(`/v1/admin/connections?userId=${encodeURIComponent(aliceUserId)}&transport=webSocket`)
  assert.equal(aliceWebSocketConnections.total, 3)
  assert.equal(aliceWebSocketConnections.transports.webSocket, 3)
  assert.equal(aliceWebSocketConnections.transports.webTransport, 0)
  assert.equal(aliceWebSocketConnections.userSummaries.length, 1)
  assert.equal(aliceWebSocketConnections.userSummaries[0].userId, aliceUserId)
  assert.equal(aliceWebSocketConnections.userSummaries[0].sessionCount, 3)
  assert.deepEqual(
    aliceWebSocketConnections.userSummaries[0].sessionIds.sort(),
    [aliceDesktopSessionId, alicePhoneSessionId, aliceTabletSessionId].sort(),
  )
  assert.equal(aliceWebSocketConnections.userSummaries[0].objectSessions, 1)
  assert.equal(aliceWebSocketConnections.userSummaries[0].userEventSessions, 1)
  assert.equal(aliceWebSocketConnections.userSummaries[0].transports.webSocket, 3)
  assert.deepEqual(
    aliceWebSocketConnections.sessions.map((session) => session.transport),
    ["webSocket", "webSocket", "webSocket"],
  )
  const aliceWebTransportConnections = await getJson(`/v1/admin/connections?userId=${encodeURIComponent(aliceUserId)}&transport=webTransport`)
  assert.equal(aliceWebTransportConnections.total, 0)
  assert.deepEqual(aliceWebTransportConnections.userSummaries, [])
  const disconnectTablet = await postJsonStatus("/v1/admin/connections/disconnect", {
    userId: aliceUserId,
    sessionId: aliceTabletSessionId,
    reason: "smoke disconnect tablet",
  })
  assert.equal(disconnectTablet.status, 200)
  assert.equal(disconnectTablet.body.targeted, 1)
  assert.deepEqual(disconnectTablet.body.targetedSessionIds, [aliceTabletSessionId])
  await admin.waitForConnectionEvent(
    "disconnectRequested",
    aliceTabletSessionId,
    (event) => event.reason === "smoke disconnect tablet",
  )
  const closingFrame = await aliceTablet.waitForFrame((frame) => frame.type === "connectionClosing", "alice tablet connection closing")
  assert.equal(closingFrame.reason, "smoke disconnect tablet")
  await admin.waitForConnectionEvent("disconnected", aliceTabletSessionId)
  await waitForConnectionMissing(aliceUserId, aliceTabletSessionId)
  const aliceAfterDisconnect = await getJson(`/v1/admin/connections?userId=${encodeURIComponent(aliceUserId)}`)
  assert.equal(aliceAfterDisconnect.total, 2)
  assert.equal(aliceAfterDisconnect.userSummaries[0].sessionCount, 2)

  const signalBeforeJoin = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(channelId)}/signal`, {
    fromUserId: aliceUserId,
    toUserId: bobUserId,
    kind: "offer",
    payload: { nonce: `${testId}-before-join` },
  })
  assert.equal(signalBeforeJoin.status, 400)
  assert.match(signalBeforeJoin.body.error, /fromUserId must join/)

  await join(channelId, aliceUserId, alicePhoneSessionId, { device: "phone" })
  await join(channelId, aliceUserId, aliceDesktopSessionId, { device: "desktop" })
  await alicePhone.waitForMemberEvent("realtime.channel.memberJoined", aliceDesktopSessionId)
  assert.equal(aliceTablet.findMemberEvent("realtime.channel.memberJoined", aliceDesktopSessionId), undefined)

  const signalToNonMember = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(channelId)}/signal`, {
    fromUserId: aliceUserId,
    toUserId: bobUserId,
    kind: "offer",
    payload: { nonce: `${testId}-to-non-member` },
  })
  assert.equal(signalToNonMember.status, 400)
  assert.match(signalToNonMember.body.error, /toUserId must join/)

  await join(channelId, bobUserId, bobSessionId, { device: "laptop" })
  await alicePhone.waitForMemberEvent("realtime.channel.memberJoined", bobSessionId)
  await aliceDesktop.waitForMemberEvent("realtime.channel.memberJoined", bobSessionId)
  assert.equal(aliceTablet.findMemberEvent("realtime.channel.memberJoined", bobSessionId), undefined)
  assert.equal(bobTablet.findMemberEvent("realtime.channel.memberJoined", bobSessionId), undefined)

  const presence = await updatePresence(channelId, bobUserId, bobSessionId, { device: "laptop", muted: true, ready: true })
  assert.equal(presence.channelId, channelId)
  assert.equal(presence.member.userId, bobUserId)
  assert.equal(presence.member.sessionId, bobSessionId)
  assert.equal(presence.member.metadata.muted, true)
  assert.equal(presence.member.metadata.ready, true)
  assert(presence.member.updatedAtMs >= presence.member.joinedAtMs)
  assert.equal(presence.members.length, 3)
  assert.equal(presence.delivered, 3)
  await alicePhone.waitForMemberEvent("realtime.channel.memberUpdated", bobSessionId)
  await aliceDesktop.waitForMemberEvent("realtime.channel.memberUpdated", bobSessionId)
  await bob.waitForMemberEvent("realtime.channel.memberUpdated", bobSessionId)
  assert.equal(aliceTablet.findMemberEvent("realtime.channel.memberUpdated", bobSessionId), undefined)
  assert.equal(bobTablet.findMemberEvent("realtime.channel.memberUpdated", bobSessionId), undefined)

  const joined = await getJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/members`)
  assert.equal(joined.members.length, 3)
  assert.deepEqual(
    joined.members.map((member) => member.sessionId).sort(),
    [aliceDesktopSessionId, alicePhoneSessionId, bobSessionId].sort(),
  )
  const bobMember = joined.members.find((member) => member.sessionId === bobSessionId)
  assert.equal(bobMember.metadata.ready, true)
  assert.equal(bobMember.updatedAtMs, presence.member.updatedAtMs)

  const initialState = await getJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`)
  assert.equal(initialState.channelId, channelId)
  assert.equal(initialState.state.channelId, channelId)
  assert.equal(initialState.state.version, 0)
  assert.equal(initialState.state.state, null)
  assert.equal(initialState.state.updatedAtMs, 0)

  const stateFromNonMember = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`, {
    fromUserId: `${testId}-not-joined`,
    expectedVersion: 0,
    state: { nonce: `${testId}-state-non-member` },
  })
  assert.equal(stateFromNonMember.status, 400)
  assert.match(stateFromNonMember.body.error, /fromUserId must join/)

  const stateNonce = `${testId}-state`
  const stateUpdate = await updateChannelState(channelId, aliceUserId, { nonce: stateNonce, phase: "lobby", tick: 1 }, 0)
  assert.equal(stateUpdate.channelId, channelId)
  assert.equal(stateUpdate.state.channelId, channelId)
  assert.equal(stateUpdate.state.version, 1)
  assert.equal(stateUpdate.state.state.nonce, stateNonce)
  assert.equal(stateUpdate.sequence, 2)
  assert.equal(stateUpdate.delivered, 3)
  assert(Number.isInteger(stateUpdate.state.updatedAtMs) && stateUpdate.state.updatedAtMs > 0)
  const bobState = await bob.waitForChannelState(stateNonce)
  assert.equal(bobState.event.payload.sequence, stateUpdate.sequence)
  assert.equal(bobState.event.payload.timestampMs, stateUpdate.state.updatedAtMs)
  assert.equal(bobState.event.payload.state.version, 1)
  await alicePhone.waitForChannelState(stateNonce)
  await aliceDesktop.waitForChannelState(stateNonce)
  assert.equal(aliceTablet.findChannelState(stateNonce), undefined)
  assert.equal(bobTablet.findChannelState(stateNonce), undefined)

  const staleStateUpdate = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`, {
    fromUserId: aliceUserId,
    expectedVersion: 0,
    state: { nonce: `${testId}-state-stale` },
  })
  assert.equal(staleStateUpdate.status, 409)
  assert.equal(staleStateUpdate.body.stateVersionConflict, true)
  assert.equal(staleStateUpdate.body.currentVersion, 1)

  const currentState = await getJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`)
  assert.equal(currentState.state.version, 1)
  assert.equal(currentState.state.state.nonce, stateNonce)

  const channelsAfterState = await getJson("/v1/realtime/channels")
  const channelSummary = channelsAfterState.channels.find((channel) => channel.channelId === channelId)
  assert(channelSummary)
  assert.equal(channelSummary.stateVersion, 1)
  assert.equal(channelSummary.stateUpdatedAtMs, stateUpdate.state.updatedAtMs)

  const emptySignalKind = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(channelId)}/signal`, {
    fromUserId: aliceUserId,
    toUserId: bobUserId,
    kind: " ",
    payload: { nonce: `${testId}-empty-kind` },
  })
  assert.equal(emptySignalKind.status, 400)
  assert.match(emptySignalKind.body.error, /kind is required/)

  const signalNonce = `${testId}-signal`
  const signal = await sendSignal(channelId, aliceUserId, bobUserId, "offer", { nonce: signalNonce })
  assert.equal(signal.delivered, true)
  assert.equal(signal.deliveredSessions, 1)
  assert.equal(signal.channelId, channelId)
  assert.equal(signal.sequence, 3)
  assert(Number.isInteger(signal.timestampMs) && signal.timestampMs > 0)
  const bobSignal = await bob.waitForChannelSignal(signalNonce)
  assert.equal(bobSignal.event.payload.sequence, signal.sequence)
  assert.equal(bobSignal.event.payload.timestampMs, signal.timestampMs)
  assert.equal(alicePhone.findChannelSignal(signalNonce), undefined)
  assert.equal(aliceDesktop.findChannelSignal(signalNonce), undefined)
  assert.equal(aliceTablet.findChannelSignal(signalNonce), undefined)
  assert.equal(bobTablet.findChannelSignal(signalNonce), undefined)

  const fromBobNonce = `${testId}-from-bob`
  const fromBob = await broadcast(channelId, bobUserId, "gameInput", { nonce: fromBobNonce }, false)
  assert.equal(fromBob.delivered, 2)
  assert.equal(fromBob.sequence, 4)

  await alicePhone.waitForChannelEvent(fromBobNonce)
  await aliceDesktop.waitForChannelEvent(fromBobNonce)
  assert.equal(aliceTablet.findChannelEvent(fromBobNonce), undefined)
  assert.equal(bob.findChannelEvent(fromBobNonce), undefined)
  assert.equal(bobTablet.findChannelEvent(fromBobNonce), undefined)

  alicePhone.close()
  await bob.waitForMemberEvent("realtime.channel.memberLeft", alicePhoneSessionId)
  assert.equal(aliceTablet.findMemberEvent("realtime.channel.memberLeft", alicePhoneSessionId), undefined)
  assert.equal(bobTablet.findMemberEvent("realtime.channel.memberLeft", alicePhoneSessionId), undefined)
  await waitForMembers(channelId, [aliceDesktopSessionId, bobSessionId])

  const afterDisconnectNonce = `${testId}-after-disconnect`
  const afterDisconnect = await broadcast(channelId, bobUserId, "gameInput", { nonce: afterDisconnectNonce }, false)
  assert.equal(afterDisconnect.delivered, 1)
  await aliceDesktop.waitForChannelEvent(afterDisconnectNonce)

  const fromAliceNonce = `${testId}-from-alice`
  const fromAlice = await broadcast(channelId, aliceUserId, "statePatch", { nonce: fromAliceNonce }, false)
  assert.equal(fromAlice.delivered, 1)

  await bob.waitForChannelEvent(fromAliceNonce)
  assert.equal(bobTablet.findChannelEvent(fromAliceNonce), undefined)
  assert.equal(alicePhone.findChannelEvent(fromAliceNonce), undefined)
  assert.equal(aliceDesktop.findChannelEvent(fromAliceNonce), undefined)
  assert.equal(aliceTablet.findChannelEvent(fromAliceNonce), undefined)

  await waitForMembers(channelId, [aliceDesktopSessionId, bobSessionId])

  console.log("realtime channel smoke ok")
} finally {
  await Promise.allSettled([
    leave(channelId, aliceUserId, aliceDesktopSessionId),
    leave(channelId, bobUserId, bobSessionId),
  ])
  for (const socket of sockets) {
    socket.close()
  }
}

async function join(channelId, userId, sessionId, metadata) {
  return postJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/join`, {
    userId,
    sessionId,
    metadata,
  })
}

async function leave(channelId, userId, sessionId) {
  return postJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/leave`, {
    userId,
    sessionId,
  })
}

async function updatePresence(channelId, userId, sessionId, metadata) {
  return postJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/presence`, {
    userId,
    sessionId,
    metadata,
  })
}

async function broadcast(channelId, fromUserId, kind, payload, includeSelf) {
  return postJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/broadcast`, {
    fromUserId,
    kind,
    payload,
    includeSelf,
  })
}

async function sendSignal(channelId, fromUserId, toUserId, kind, payload) {
  return postJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/signal`, {
    fromUserId,
    toUserId,
    kind,
    payload,
  })
}

async function updateChannelState(channelId, fromUserId, state, expectedVersion) {
  return postJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`, {
    fromUserId,
    state,
    expectedVersion,
  })
}

async function getJson(path) {
  const response = await fetch(new URL(path, baseUrl))
  if (!response.ok) {
    assert.fail(`${path} failed with ${response.status}: ${await response.text()}`)
  }
  return response.json()
}

async function postJson(path, body) {
  const response = await fetch(new URL(path, baseUrl), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    assert.fail(`${path} failed with ${response.status}: ${await response.text()}`)
  }
  return response.json()
}

async function postJsonStatus(path, body) {
  const response = await fetch(new URL(path, baseUrl), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  const text = await response.text()
  return {
    status: response.status,
    body: text ? JSON.parse(text) : undefined,
  }
}

async function waitForConnections(userId, sessionIds) {
  const wanted = new Set(sessionIds)
  await waitFor(async () => {
    const response = await getJson(`/v1/admin/connections?userId=${encodeURIComponent(userId)}`)
    const current = new Set(response.sessions.map((session) => session.sessionId))
    return [...wanted].every((sessionId) => current.has(sessionId))
  }, `connections for ${userId}`)
}

async function waitForConnectionSession(userId, sessionId) {
  let found
  await waitFor(async () => {
    const response = await getJson(`/v1/admin/connections?userId=${encodeURIComponent(userId)}`)
    found = response.sessions.find((session) => session.sessionId === sessionId)
    return found !== undefined
  }, `connection session ${sessionId}`)
  return found
}

async function waitForConnectionMissing(userId, sessionId) {
  await waitFor(async () => {
    const response = await getJson(`/v1/admin/connections?userId=${encodeURIComponent(userId)}`)
    return !response.sessions.some((session) => session.sessionId === sessionId)
  }, `missing connection ${sessionId}`)
}

async function waitForConnectionSubscriptions(userId, expected) {
  const objectSessions = new Set(expected.objectSessionIds)
  const userEventSessions = new Set(expected.userEventSessionIds)
  await waitFor(async () => {
    const response = await getJson(`/v1/admin/connections?userId=${encodeURIComponent(userId)}`)
    const objectCurrent = new Set(
      response.sessions
        .filter((session) => session.subscribedObjects)
        .map((session) => session.sessionId),
    )
    const userEventCurrent = new Set(
      response.sessions
        .filter((session) => session.subscribedUserEvents)
        .map((session) => session.sessionId),
    )
    return (
      objectCurrent.size === objectSessions.size &&
      userEventCurrent.size === userEventSessions.size &&
      [...objectSessions].every((sessionId) => objectCurrent.has(sessionId)) &&
      [...userEventSessions].every((sessionId) => userEventCurrent.has(sessionId))
    )
  }, `connection subscriptions for ${userId}`)
}

async function waitForMembers(channelId, sessionIds) {
  const expected = [...sessionIds].sort()
  await waitFor(async () => {
    const response = await getJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/members`)
    const actual = response.members.map((member) => member.sessionId).sort()
    return JSON.stringify(actual) === JSON.stringify(expected)
  }, `members for ${channelId}`)
}

function openRealtimeSocket(userId, sessionId, metadata) {
  if (typeof WebSocket === "undefined") {
    throw new Error("global WebSocket is required; run this smoke with Node.js 22 or newer")
  }

  const url = new URL("/v1/connect", baseUrl)
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:"
  url.searchParams.set("userId", userId)
  url.searchParams.set("sessionId", sessionId)
  if (metadata !== undefined) {
    url.searchParams.set("metadata", JSON.stringify(metadata))
  }

  const ws = new WebSocket(url)
  const frames = []
  const waiters = []

  ws.addEventListener("message", async (event) => {
    const frame = JSON.parse(await frameText(event.data))
    frames.push(frame)
    for (const waiter of [...waiters]) {
      if (waiter.predicate(frame)) {
        waiter.resolve(frame)
        waiters.splice(waiters.indexOf(waiter), 1)
      }
    }
  })

  const api = {
    frames,
    send: (frame) => ws.send(JSON.stringify(frame)),
    close: () => ws.close(),
    waitForFrame: (predicate, label) =>
      waitForFrame(frames, waiters, predicate, label),
    waitForChannelEvent: (nonce) =>
      waitForFrame(
        frames,
        waiters,
        (frame) => isChannelEvent(frame, nonce),
        `channel event ${nonce}`,
      ),
    findChannelEvent: (nonce) => frames.find((frame) => isChannelEvent(frame, nonce)),
    waitForChannelSignal: (nonce) =>
      waitForFrame(
        frames,
        waiters,
        (frame) => isChannelSignal(frame, nonce),
        `channel signal ${nonce}`,
      ),
    findChannelSignal: (nonce) => frames.find((frame) => isChannelSignal(frame, nonce)),
    waitForChannelState: (nonce) =>
      waitForFrame(
        frames,
        waiters,
        (frame) => isChannelState(frame, nonce),
        `channel state ${nonce}`,
      ),
    findChannelState: (nonce) => frames.find((frame) => isChannelState(frame, nonce)),
    waitForMemberEvent: (name, sessionId) =>
      waitForFrame(
        frames,
        waiters,
        (frame) => isMemberEvent(frame, name, sessionId),
        `${name} for ${sessionId}`,
      ),
    findMemberEvent: (name, sessionId) => frames.find((frame) => isMemberEvent(frame, name, sessionId)),
    waitForConnectionEvent: (eventType, sessionId, predicate = () => true) =>
      waitForFrame(
        frames,
        waiters,
        (frame) => isConnectionEvent(frame, eventType, sessionId, predicate),
        `connection ${eventType} for ${sessionId}`,
      ),
  }

  return new Promise((resolve, reject) => {
    ws.addEventListener("open", async () => {
      try {
        const hello = await api.waitForFrame((frame) => frame.type === "hello", `hello for ${sessionId}`)
        assert.equal(hello.sessionId, sessionId)
        assert.equal(hello.userId, userId)
        resolve(api)
      } catch (error) {
        reject(error)
      }
    })
    ws.addEventListener("error", () => {
      reject(new Error(`websocket failed for ${sessionId}`))
    })
  })
}

function waitForFrame(frames, waiters, predicate, label) {
  const existing = frames.find(predicate)
  if (existing) {
    return Promise.resolve(existing)
  }
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      const index = waiters.indexOf(waiter)
      if (index >= 0) {
        waiters.splice(index, 1)
      }
      reject(new Error(`timed out waiting for ${label}`))
    }, 5_000)
    const waiter = {
      predicate,
      resolve: (frame) => {
        clearTimeout(timeout)
        resolve(frame)
      },
    }
    waiters.push(waiter)
  })
}

function isChannelEvent(frame, nonce) {
  return (
    frame.type === "event" &&
    frame.event?.type === "volatileUserEvent" &&
    frame.event.name === "realtime.channel.event" &&
    frame.event.payload?.payload?.nonce === nonce
  )
}

function isChannelSignal(frame, nonce) {
  return (
    frame.type === "event" &&
    frame.event?.type === "volatileUserEvent" &&
    frame.event.name === "realtime.channel.signal" &&
    frame.event.payload?.payload?.nonce === nonce
  )
}

function isChannelState(frame, nonce) {
  return (
    frame.type === "event" &&
    frame.event?.type === "volatileUserEvent" &&
    frame.event.name === "realtime.channel.state" &&
    frame.event.payload?.state?.state?.nonce === nonce
  )
}

function isMemberEvent(frame, name, sessionId) {
  if (
    frame.type !== "event" ||
    frame.event?.type !== "volatileUserEvent" ||
    frame.event.name !== name
  ) {
    return false
  }
  if (name === "realtime.channel.memberJoined") {
    return frame.event.payload?.member?.sessionId === sessionId
  }
  if (name === "realtime.channel.memberUpdated") {
    return frame.event.payload?.member?.sessionId === sessionId
  }
  return frame.event.payload?.members?.some((member) => member.sessionId === sessionId)
}

function isConnectionEvent(frame, eventType, sessionId, predicate) {
  if (frame.type !== "connectionEvent" || frame.event?.eventType !== eventType) {
    return false
  }
  const eventSessionId = frame.event.session?.sessionId ?? frame.event.sessionId
  return eventSessionId === sessionId && predicate(frame.event)
}

async function frameText(data) {
  if (typeof data === "string") {
    return data
  }
  if (data instanceof ArrayBuffer) {
    return new TextDecoder().decode(data)
  }
  if (data instanceof Blob) {
    return data.text()
  }
  return Buffer.from(data).toString("utf8")
}

async function waitFor(check, label) {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    if (await check()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}`)
}
