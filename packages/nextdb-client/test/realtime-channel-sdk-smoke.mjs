import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient, decodeRealtimeBinaryFrame } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-realtime-channel-sdk-"))
const dataDir = join(tempRoot, "data")
const node = {
  url: "http://127.0.0.1:3395",
  addr: "127.0.0.1:3395",
  dataDir,
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const channelId = `sdk-channel-${suffix}`
  const alice = new NextDbClient({
    endpoint: node.url,
    userId: `alice-${suffix}`,
    sessionId: `alice-session-${suffix}`,
    connectionMetadata: {
      device: "desktop",
      capabilities: ["audio"],
    },
  })
  const bob = new NextDbClient({
    endpoint: node.url,
    userId: `bob-${suffix}`,
    sessionId: `bob-session-${suffix}`,
  })
  const charlie = new NextDbClient({
    endpoint: node.url,
    userId: `charlie-${suffix}`,
    sessionId: `charlie-session-${suffix}`,
  })
  const admin = new NextDbClient({
    endpoint: node.url,
    userId: `admin-${suffix}`,
    sessionId: `admin-session-${suffix}`,
  })
  const aliceChannel = alice.realtimeChannel(channelId)
  const bobChannel = bob.realtimeChannel(channelId)
  const charlieChannel = charlie.realtimeChannel(`sdk-channel-join-opens-${suffix}`)
  const connectionEvents = []
  const aliceSignals = []
  const bobSignals = []
  const aliceAnswers = []
  const aliceIceCandidates = []
  const bobOffers = []
  const aliceEvents = []
  const bobEvents = []
  const aliceGameInputs = []
  const bobStatePatches = []
  const bobLobbyEvents = []
  const bobRenegotiationSignals = []
  const aliceVideoEvents = []
  const bobVoiceEvents = []
  const bobRecentEventViews = []
  const bobRecentSignalViews = []
  const bobStates = []
  const bobStateViews = []
  const connectionViews = []
  const aliceMemberViews = []
  const bobMemberViews = []
  const aliceMemberEvents = []
  const bobMemberEvents = []
  const aliceMemberUpdates = []
  const bobMemberUpdates = []
  const aggregatePresenceEvents = []

  const stops = [
    admin.onConnectionEvent((event) => connectionEvents.push(event)),
    admin.watchConnections((snapshot) => connectionViews.push(snapshot), { immediate: false }),
    aliceChannel.onSignal((signal) => aliceSignals.push(signal)),
    bobChannel.onSignal((signal) => bobSignals.push(signal)),
    aliceChannel.onAnswer((signal) => aliceAnswers.push(signal)),
    aliceChannel.onIce((signal) => aliceIceCandidates.push(signal)),
    bobChannel.onOffer((signal) => bobOffers.push(signal)),
    aliceChannel.onEvent((event) => aliceEvents.push(event)),
    bobChannel.onEvent((event) => bobEvents.push(event)),
    aliceChannel.onGameInput((event) => aliceGameInputs.push(event)),
    bobChannel.onStatePatch((event) => bobStatePatches.push(event)),
    bobChannel.onEventKind("lobbyReady", (event) => bobLobbyEvents.push(event)),
    bobChannel.onSignalKind("renegotiate", (signal) => bobRenegotiationSignals.push(signal)),
    aliceChannel.onVideo((event) => aliceVideoEvents.push(event)),
    bobChannel.onVoice((event) => bobVoiceEvents.push(event)),
    bobChannel.watchRecentEvents((snapshot) => bobRecentEventViews.push(snapshot), { limit: 8 }),
    bobChannel.watchRecentSignals((snapshot) => bobRecentSignalViews.push(snapshot), { limit: 8 }),
    bobChannel.onState((event) => bobStates.push(event)),
    bobChannel.watchState((snapshot) => bobStateViews.push(snapshot)),
    aliceChannel.watchMembers((snapshot) => aliceMemberViews.push(snapshot)),
    bobChannel.watchMembers((snapshot) => bobMemberViews.push(snapshot)),
    aliceChannel.onMemberJoined((event) => aliceMemberEvents.push({ type: "joined", event })),
    aliceChannel.onMemberLeft((event) => aliceMemberEvents.push({ type: "left", event })),
    aliceChannel.onMemberUpdated((event) => aliceMemberUpdates.push(event)),
    bobChannel.onMemberJoined((event) => bobMemberEvents.push({ type: "joined", event })),
    bobChannel.onMemberLeft((event) => bobMemberEvents.push({ type: "left", event })),
    bobChannel.onMemberUpdated((event) => bobMemberUpdates.push(event)),
    alice.subscribeAggregatePresence(channelId, (event) => aggregatePresenceEvents.push(event)),
  ]

  try {
    await waitFor(
      () => connectionEvents.some((event) =>
        event.eventType === "connected" &&
        event.session?.sessionId === `alice-session-${suffix}` &&
        event.session?.metadata?.device === "desktop"
      ),
      "admin sees alice connection",
    )
    await waitFor(
      () => connectionViews.some((view) =>
        view.connections?.sessions.some((session) =>
          session.sessionId === `alice-session-${suffix}` &&
          session.metadata?.device === "desktop"
        )
      ),
      "admin connection watcher sees alice connection",
    )
    assert.equal(admin.cachedConnections()?.sessions.some((session) => session.sessionId === `alice-session-${suffix}`), true)
    alice.updateConnectionMetadata({
      device: "desktop",
      capabilities: ["audio", "video", "game"],
      transportPreference: "webtransport",
    })
    await waitFor(
      () => connectionEvents.some((event) =>
        event.eventType === "metadataUpdated" &&
        event.session?.sessionId === `alice-session-${suffix}` &&
        event.session?.metadata?.transportPreference === "webtransport"
      ),
      "admin sees alice connection metadata update",
    )
    await waitFor(
      () => admin.cachedConnections()?.sessions.some((session) =>
        session.sessionId === `alice-session-${suffix}` &&
        session.metadata?.transportPreference === "webtransport"
      ),
      "admin cached connections update metadata",
    )
    await waitFor(
      () => connectionEvents.some((event) =>
        event.eventType === "subscriptionsUpdated" &&
        event.session?.sessionId === `alice-session-${suffix}` &&
        event.session?.subscribedUserEvents === true
      ),
      "admin sees alice user-event subscription",
    )
    await waitFor(
      () => admin.cachedConnections()?.sessions.some((session) =>
        session.sessionId === `alice-session-${suffix}` &&
        session.subscribedUserEvents === true
      ),
      "admin cached connections update subscriptions",
    )
    const adminLocalStatus = await admin.localDataStatus()
    assert(adminLocalStatus.connectionSessions.sessionCount >= 1)

    const charlieJoin = await charlieChannel.join({ role: "observer" })
    assert.equal(charlieJoin.member.sessionId, `charlie-session-${suffix}`)
    await waitFor(
      () => aggregatePresenceEvents.some((event) =>
        event.source === "snapshot" &&
        event.channelId === channelId &&
        event.memberCount === 0 &&
        event.userCount === 0
      ),
      "aggregate presence initial snapshot",
    )

    const aliceJoin = await aliceChannel.join({ media: ["audio", "video"], role: "host" })
    assert.equal(aliceJoin.member.userId, `alice-${suffix}`)
    assert.equal(aliceChannel.cachedMembers()?.members.length, 1)
    assert((await alice.localDataStatus()).activeSubscriptions.realtimeChannels.includes(channelId))
    await waitFor(
      () => aggregatePresenceEvents.some((event) =>
        event.source === "update" &&
        event.channelId === channelId &&
        event.memberCount === 1 &&
        event.userCount === 1
      ),
      "aggregate presence sees alice join",
    )

    await assert.rejects(
      aliceChannel.signal(`bob-${suffix}`, "offer", { nonce: `early-offer-${suffix}` }),
      /toUserId must join/,
    )

    const bobJoin = await bobChannel.join({ media: ["game"], role: "player" })
    assert.equal(bobJoin.members.length, 2)
    assert.equal(bobChannel.cachedMembers()?.members.length, 2)
    await waitFor(
      () => aggregatePresenceEvents.some((event) =>
        event.source === "update" &&
        event.channelId === channelId &&
        event.memberCount === 2 &&
        event.userCount === 2
      ),
      "aggregate presence sees bob join",
    )
    await waitFor(
      () => aliceMemberViews.some((view) =>
        view.snapshot?.members.some((member) => member.userId === `bob-${suffix}`)
      ),
      "alice member watcher sees bob join",
    )

    const bobPresence = await bobChannel.updatePresence({ media: ["game"], role: "player", muted: true, ready: true })
    assert.equal(bobPresence.channelId, channelId)
    assert.equal(bobPresence.member.userId, `bob-${suffix}`)
    assert.equal(bobPresence.member.sessionId, `bob-session-${suffix}`)
    assert.equal(bobPresence.member.metadata.ready, true)
    assert.equal(bobPresence.member.metadata.muted, true)
    assert.equal(bobPresence.delivered, 2)
    await waitFor(
      () => aliceMemberUpdates.some((event) =>
        event.member.userId === `bob-${suffix}` &&
        event.member.metadata?.ready === true
      ),
      "alice sees bob presence update",
    )
    await waitFor(
      () => aliceMemberViews.some((view) =>
        view.snapshot?.members.some((member) =>
          member.userId === `bob-${suffix}` &&
          member.metadata?.ready === true
        )
      ),
      "alice member watcher sees bob presence",
    )
    await waitFor(
      () => bobMemberUpdates.some((event) =>
        event.member.userId === `bob-${suffix}` &&
        event.member.metadata?.muted === true
      ),
      "bob sees own presence update",
    )

    await assert.rejects(
      aliceChannel.signal(`bob-${suffix}`, " ", { nonce: `empty-kind-${suffix}` }),
      /kind is required/,
    )

    await waitFor(async () => {
      const members = await aliceChannel.members()
      return members.members.length === 2
    }, "channel membership")
    const membersAfterPresence = await aliceChannel.members()
    const bobMember = membersAfterPresence.members.find((member) => member.userId === `bob-${suffix}`)
    assert.equal(bobMember?.metadata?.ready, true)
    assert.equal(bobMember?.updatedAtMs, bobPresence.member.updatedAtMs)
    assert.equal(aliceChannel.cachedMembers()?.members.find((member) => member.userId === `bob-${suffix}`)?.metadata?.ready, true)
    assert.equal((await alice.localDataStatus()).realtimeChannelMembers[channelId]?.memberCount, 2)

    await waitFor(() => aliceMemberEvents.some((entry) => entry.type === "joined" && entry.event.member.userId === `bob-${suffix}`), "alice sees bob join")

    const initialState = await aliceChannel.state()
    assert.equal(initialState.channelId, channelId)
    assert.equal(initialState.state.channelId, channelId)
    assert.equal(initialState.state.version, 0)
    assert.equal(initialState.state.state, null)
    assert.equal(aliceChannel.cachedState()?.version, 0)
    await waitFor(() => bobStateViews.some((snapshot) => snapshot.snapshot?.version === 0), "bob state watcher sees initial snapshot")

    await assert.rejects(
      charlie.realtimeChannel(channelId).updateState({ nonce: `state-non-member-${suffix}` }, { expectedVersion: 0 }),
      /fromUserId must join/,
    )

    const snapshotNonce = `snapshot-${suffix}`
    const snapshot = await aliceChannel.updateState({ nonce: snapshotNonce, phase: "lobby", tick: 1 }, { expectedVersion: 0 })
    assert.equal(snapshot.channelId, channelId)
    assert.equal(snapshot.state.version, 1)
    assert.equal(snapshot.state.state.nonce, snapshotNonce)
    assert.equal(snapshot.sequence, 2)
    assert.equal(snapshot.delivered, 2)
    assert.equal(aliceChannel.cachedState()?.state?.nonce, snapshotNonce)
    await waitFor(() => bobStates.some((event) => event.state.state?.nonce === snapshotNonce), "bob receives state snapshot")
    await waitFor(() => bobStateViews.some((view) => view.snapshot?.state?.nonce === snapshotNonce), "bob state watcher sees state snapshot")
    const bobSnapshot = bobStates.find((event) => event.state.state?.nonce === snapshotNonce)
    assert.equal(bobSnapshot.sequence, snapshot.sequence)
    assert.equal(bobSnapshot.timestampMs, snapshot.state.updatedAtMs)
    assert.equal(bobChannel.cachedState()?.state?.nonce, snapshotNonce)
    const stateStatus = await bob.localDataStatus()
    assert.equal(stateStatus.realtimeChannelStates[channelId]?.version, 1)
    await assert.rejects(
      aliceChannel.updateState({ nonce: `state-stale-${suffix}` }, { expectedVersion: 0 }),
      /version conflict/,
    )

    const offerNonce = `offer-${suffix}`
    const offer = await aliceChannel.sendOffer(`bob-${suffix}`, { nonce: offerNonce, sdp: "fake-sdp" })
    assert.equal(offer.delivered, true)
    assert.equal(offer.deliveredSessions, 1)
    assert.equal(offer.channelId, channelId)
    assert.equal(offer.sequence, 3)
    assert(Number.isInteger(offer.timestampMs) && offer.timestampMs > 0)
    await waitFor(() => bobSignals.some((signal) => signal.payload?.nonce === offerNonce), "bob receives offer")
    const bobOffer = bobSignals.find((signal) => signal.payload?.nonce === offerNonce)
    assert.equal(bobOffer.sequence, offer.sequence)
    assert.equal(bobOffer.timestampMs, offer.timestampMs)
    assert.equal(bobOffers.some((signal) => signal.payload?.nonce === offerNonce), true)
    assert.equal(aliceSignals.some((signal) => signal.payload?.nonce === offerNonce), false)

    const answerNonce = `answer-${suffix}`
    const answer = await bobChannel.sendAnswer(`alice-${suffix}`, { nonce: answerNonce, sdp: "fake-answer" })
    assert.equal(answer.delivered, true)
    assert.equal(answer.deliveredSessions, 1)
    assert.equal(answer.sequence, 4)
    await waitFor(() => aliceAnswers.some((signal) => signal.payload?.nonce === answerNonce), "alice receives answer")
    assert.equal(bobSignals.some((signal) => signal.payload?.nonce === answerNonce), false)

    const iceNonce = `ice-${suffix}`
    const ice = await bobChannel.sendIce(`alice-${suffix}`, { nonce: iceNonce, candidate: "fake-candidate" })
    assert.equal(ice.delivered, true)
    assert.equal(ice.deliveredSessions, 1)
    assert.equal(ice.sequence, 5)
    await waitFor(() => aliceIceCandidates.some((signal) => signal.payload?.nonce === iceNonce), "alice receives ice")

    const gameNonce = `game-${suffix}`
    const gameInput = await bobChannel.sendGameInput({ nonce: gameNonce, frame: 7 }, { includeSelf: false })
    assert.equal(gameInput.delivered, 1)
    assert.equal(gameInput.sequence, 6)
    await waitFor(() => aliceGameInputs.some((event) => event.payload?.nonce === gameNonce), "alice receives game input")
    assert.equal(bobEvents.some((event) => event.payload?.nonce === gameNonce), false)

    const gameFrameNonce = `game-frame-${suffix}`
    const gameFrame = await bobChannel.sendGameInputFrame(new Uint8Array([7, 8, 9]), {
      contentType: "application/x.nextdb.game-input",
      metadata: { nonce: gameFrameNonce, frame: 8 },
      includeSelf: false,
    })
    assert.equal(gameFrame.delivered, 1)
    assert.equal(gameFrame.sequence, 7)
    await waitFor(() => aliceGameInputs.some((event) => event.payload?.metadata?.nonce === gameFrameNonce), "alice receives binary game input")
    const aliceGameFrame = aliceGameInputs.find((event) => event.payload?.metadata?.nonce === gameFrameNonce)
    assert.deepEqual([...decodeRealtimeBinaryFrame(aliceGameFrame.payload)], [7, 8, 9])
    assert.equal(aliceGameFrame.payload.contentType, "application/x.nextdb.game-input")

    const stateNonce = `state-${suffix}`
    const statePatch = await aliceChannel.sendStatePatch({ nonce: stateNonce, tick: 8 }, { includeSelf: false })
    assert.equal(statePatch.delivered, 1)
    assert.equal(statePatch.sequence, 8)
    await waitFor(() => bobStatePatches.some((event) => event.payload?.nonce === stateNonce), "bob receives state patch")
    assert.equal(aliceEvents.some((event) => event.payload?.nonce === stateNonce), false)

    const voiceNonce = `voice-${suffix}`
    const voice = await aliceChannel.sendVoice({ nonce: voiceNonce, codec: "opus", frame: "metadata-only" }, { includeSelf: true })
    assert.equal(voice.delivered, 2)
    assert.equal(voice.sequence, 9)
    await waitFor(() => aliceEvents.some((event) => event.kind === "voice" && event.payload?.nonce === voiceNonce), "alice receives voice metadata")
    await waitFor(() => bobEvents.some((event) => event.kind === "voice" && event.payload?.nonce === voiceNonce), "bob receives voice metadata")
    await waitFor(() => bobVoiceEvents.some((event) => event.payload?.nonce === voiceNonce), "bob typed voice listener receives metadata")

    const voiceFrameNonce = `voice-frame-${suffix}`
    const voiceFrame = await aliceChannel.sendVoiceFrame(new Uint8Array([1, 2, 3, 4]), {
      contentType: "audio/opus",
      codec: "opus",
      metadata: { nonce: voiceFrameNonce },
      includeSelf: false,
    })
    assert.equal(voiceFrame.delivered, 1)
    assert.equal(voiceFrame.sequence, 10)
    await waitFor(() => bobEvents.some((event) => event.kind === "voice" && event.payload?.metadata?.nonce === voiceFrameNonce), "bob receives voice binary frame")
    const bobVoiceFrame = bobEvents.find((event) => event.kind === "voice" && event.payload?.metadata?.nonce === voiceFrameNonce)
    assert.deepEqual([...decodeRealtimeBinaryFrame(bobVoiceFrame.payload)], [1, 2, 3, 4])
    assert.equal(bobVoiceFrame.payload.codec, "opus")
    assert.equal(bobVoiceFrame.payload.contentType, "audio/opus")
    await waitFor(() => bobVoiceEvents.some((event) => event.payload?.metadata?.nonce === voiceFrameNonce), "bob typed voice listener receives binary frame")
    const bobTypedVoiceFrame = bobVoiceEvents.find((event) => event.payload?.metadata?.nonce === voiceFrameNonce)
    assert.deepEqual([...decodeRealtimeBinaryFrame(bobTypedVoiceFrame.payload)], [1, 2, 3, 4])

    const videoNonce = `video-${suffix}`
    const video = await bobChannel.sendVideo({ nonce: videoNonce, codec: "h264", keyframe: true }, { includeSelf: false })
    assert.equal(video.delivered, 1)
    assert.equal(video.sequence, 11)
    await waitFor(() => aliceEvents.some((event) => event.kind === "video" && event.payload?.nonce === videoNonce), "alice receives video metadata")
    await waitFor(() => aliceVideoEvents.some((event) => event.payload?.nonce === videoNonce), "alice typed video listener receives metadata")
    assert.equal(bobEvents.some((event) => event.kind === "video" && event.payload?.nonce === videoNonce), false)

    const videoFrameNonce = `video-frame-${suffix}`
    const videoFrame = await bobChannel.sendVideoFrame("idr-frame", {
      contentType: "video/h264",
      codec: "h264",
      metadata: { nonce: videoFrameNonce, keyframe: true },
      includeSelf: false,
    })
    assert.equal(videoFrame.delivered, 1)
    assert.equal(videoFrame.sequence, 12)
    await waitFor(() => aliceEvents.some((event) => event.kind === "video" && event.payload?.metadata?.nonce === videoFrameNonce), "alice receives video binary frame")
    const aliceVideoFrame = aliceEvents.find((event) => event.kind === "video" && event.payload?.metadata?.nonce === videoFrameNonce)
    assert.equal(new TextDecoder().decode(decodeRealtimeBinaryFrame(aliceVideoFrame.payload)), "idr-frame")
    assert.equal(aliceVideoFrame.payload.contentType, "video/h264")
    await waitFor(() => aliceVideoEvents.some((event) => event.payload?.metadata?.nonce === videoFrameNonce), "alice typed video listener receives binary frame")
    const aliceTypedVideoFrame = aliceVideoEvents.find((event) => event.payload?.metadata?.nonce === videoFrameNonce)
    assert.equal(new TextDecoder().decode(decodeRealtimeBinaryFrame(aliceTypedVideoFrame.payload)), "idr-frame")

    const renegotiateNonce = `renegotiate-${suffix}`
    const renegotiate = await aliceChannel.signal(`bob-${suffix}`, "renegotiate", { nonce: renegotiateNonce, reason: "codec-change" })
    assert.equal(renegotiate.delivered, true)
    assert.equal(renegotiate.deliveredSessions, 1)
    assert.equal(renegotiate.sequence, 13)
    await waitFor(() => bobRenegotiationSignals.some((signal) => signal.payload?.nonce === renegotiateNonce), "bob typed custom signal listener receives renegotiate")
    assert.equal(bobSignals.some((signal) => signal.payload?.nonce === renegotiateNonce), true)
    await waitFor(
      () => bobRecentSignalViews.some((view) => view.signals.some((signal) => signal.payload?.nonce === renegotiateNonce)),
      "bob recent signal watcher receives renegotiate",
    )
    const bobRecentRenegotiateSignals = bobChannel.cachedRecentSignals({ kind: "renegotiate", limit: 1 })
    assert.equal(bobRecentRenegotiateSignals.length, 1)
    assert.equal(bobRecentRenegotiateSignals[0].payload?.nonce, renegotiateNonce)
    const bobStatusWithRecentSignals = await bob.localDataStatus()
    assert.equal(bobStatusWithRecentSignals.realtimeChannelSignals[channelId]?.latestSequence, 13)
    assert.equal(bobStatusWithRecentSignals.realtimeChannelSignals[channelId]?.signalCount >= 1, true)

    const lobbyNonce = `lobby-${suffix}`
    const lobbyReady = await aliceChannel.broadcast("lobbyReady", { nonce: lobbyNonce, map: "arena" }, { includeSelf: false })
    assert.equal(lobbyReady.delivered, 1)
    assert.equal(lobbyReady.sequence, 14)
    await waitFor(() => bobLobbyEvents.some((event) => event.payload?.nonce === lobbyNonce), "bob typed custom event listener receives lobby ready")
    assert.equal(bobEvents.some((event) => event.payload?.nonce === lobbyNonce), true)
    assert.equal(aliceEvents.some((event) => event.payload?.nonce === lobbyNonce), false)
    await waitFor(
      () => bobRecentEventViews.some((view) => view.events.some((event) => event.payload?.nonce === lobbyNonce)),
      "bob recent event watcher receives lobby ready",
    )
    const bobRecentLobbyEvents = bobChannel.cachedRecentEvents({ kind: "lobbyReady", limit: 1 })
    assert.equal(bobRecentLobbyEvents.length, 1)
    assert.equal(bobRecentLobbyEvents[0].payload?.nonce, lobbyNonce)
    const bobStatusWithRecentEvents = await bob.localDataStatus()
    assert.equal(bobStatusWithRecentEvents.realtimeChannelEvents[channelId]?.latestSequence, 14)
    assert.equal(bobStatusWithRecentEvents.realtimeChannelEvents[channelId]?.eventCount >= 1, true)
    const bobRealtimeCoverage = bobStatusWithRecentEvents.coverage.realtimeChannels[channelId]
    assert(bobRealtimeCoverage)
    assert.equal(bobRealtimeCoverage.activeSubscription, true)
    assert.equal(bobRealtimeCoverage.stateVersion, 1)
    assert.equal(bobRealtimeCoverage.members, 2)
    assert.equal(bobRealtimeCoverage.latestSignalSequence, 13)
    assert.equal(bobRealtimeCoverage.recentSignals >= 1, true)
    assert.equal(bobRealtimeCoverage.latestEventSequence, 14)
    assert.equal(bobRealtimeCoverage.recentEvents >= 1, true)

    await stopNode(child)
    child = startNode(node)
    await waitForHealth(node.url)

    await waitFor(async () => {
      const members = await aliceChannel.members()
      return (
        members.members.length === 2 &&
        members.members.some((member) => member.sessionId === `alice-session-${suffix}`) &&
        members.members.some((member) => member.sessionId === `bob-session-${suffix}`)
      )
    }, "channel membership after server restart", 10_000)
    await waitFor(
      () => aliceChannel.cachedMembers()?.members.some((member) => member.sessionId === `bob-session-${suffix}`),
      "cached channel membership after server restart",
      10_000,
    )

    const stateAfterRestart = await aliceChannel.state()
    assert.equal(stateAfterRestart.state.version, 0)
    assert.equal(stateAfterRestart.state.state, null)
    assert.equal(aliceChannel.cachedState()?.version, 0)

    const afterRestartNonce = `after-restart-${suffix}`
    const afterRestart = await aliceChannel.sendStatePatch({ nonce: afterRestartNonce, tick: 9 }, { includeSelf: false })
    assert.equal(afterRestart.delivered, 1)
    await waitFor(() => bobStatePatches.some((event) => event.payload?.nonce === afterRestartNonce), "bob receives state patch after restart", 10_000)

    const channels = await alice.listRealtimeChannels()
    const summary = channels.channels.find((channel) => channel.channelId === channelId)
    assert(summary)
    assert.equal(summary.memberCount, 2)
    assert.equal(summary.sequence, 1)
    assert.equal(summary.stateVersion, 0)

    await bobChannel.leave()
    await waitFor(() => aliceMemberEvents.some((entry) => entry.type === "left" && entry.event.members.length === 1), "alice sees bob leave")
    await waitFor(
      () => aggregatePresenceEvents.some((event) =>
        event.source === "update" &&
        event.channelId === channelId &&
        event.memberCount === 1 &&
        event.userCount === 1
      ),
      "aggregate presence sees bob leave",
    )
    const bobStatusAfterLeave = await bob.localDataStatus()
    assert.equal(bobStatusAfterLeave.activeSubscriptions.realtimeChannels.includes(channelId), false)
    assert.equal(bobStatusAfterLeave.coverage.realtimeChannels[channelId], undefined)
    assert.equal(bobChannel.cachedState(), undefined)
    assert.equal(bobChannel.cachedMembers(), undefined)
    assert.deepEqual(bobChannel.cachedRecentEvents(), [])
    assert.deepEqual(bobChannel.cachedRecentSignals(), [])
    await waitFor(
      () => aliceChannel.cachedMembers()?.members.length === 1,
      "alice cached members remove bob",
    )
    const afterLeave = await aliceChannel.members()
    assert.deepEqual(afterLeave.members.map((member) => member.userId), [`alice-${suffix}`])

    const cleanupState = await aliceChannel.updateState({ nonce: `cleanup-state-${suffix}` }, { expectedVersion: 0 })
    assert.equal(cleanupState.state.version, 1)
    await aliceChannel.leave()
    await waitFor(
      () => aggregatePresenceEvents.some((event) =>
        event.source === "update" &&
        event.channelId === channelId &&
        event.memberCount === 0 &&
        event.userCount === 0
      ),
      "aggregate presence sees alice leave",
    )
    await waitFor(async () => {
      const health = await admin.health()
      const channels = await admin.listRealtimeChannels()
      if (channels.channels.some((channel) => channel.channelId === channelId)) {
        throw new Error(JSON.stringify({
          target: channels.channels.find((channel) => channel.channelId === channelId),
          health: {
            realtimeChannels: health.realtimeChannels,
            realtimeChannelStates: health.realtimeChannelStates,
            realtimeChannelSequences: health.realtimeChannelSequences,
            realtimeMaintenance: health.realtimeMaintenance,
          },
        }))
      }
      return (
        !channels.channels.some((channel) => channel.channelId === channelId) &&
        health.realtimeMaintenance.totalOrphanStatesRemoved >= 1 &&
        health.realtimeMaintenance.totalOrphanSequencesRemoved >= 1
      )
    }, "empty realtime channel runtime state cleanup")

    console.log("realtime channel sdk smoke ok")
  } finally {
    for (const stop of stops) {
      stop()
    }
    await Promise.allSettled([
      aliceChannel.leave(),
      bobChannel.leave(),
      charlieChannel.leave(),
    ])
    admin.close()
    alice.close()
    bob.close()
    charlie.close()
  }
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode(node) {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_DATA_DIR: node.dataDir,
      NEXTDB_ADDR: node.addr,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_REALTIME_CHANNEL_SDK_SMOKE_LOGS === "1") {
      process.stdout.write(`[realtime-sdk] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_REALTIME_CHANNEL_SDK_SMOKE_LOGS === "1") {
      process.stderr.write(`[realtime-sdk] ${chunk}`)
    }
  })
  return child
}

async function stopNode(child) {
  if (!child || child.exitCode !== null) {
    return
  }
  child.kill("SIGTERM")
  await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL")
      resolve()
    }, 5_000)
    child.once("exit", () => {
      clearTimeout(timeout)
      resolve()
    })
  })
}

async function waitForHealth(url) {
  await waitFor(async () => {
    const response = await fetch(`${url}/v1/health`).catch(() => undefined)
    if (!response?.ok) {
      return false
    }
    const health = await response.json()
    return health.ok === true
  }, `health at ${url}`)
}

async function waitFor(check, label, timeoutMs = 5_000) {
  const started = Date.now()
  let lastError
  while (Date.now() - started < timeoutMs) {
    try {
      if (await check()) {
        return
      }
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`)
}
