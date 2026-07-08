import assert from "node:assert/strict"
import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-actor-window-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth(endpoint)

  const client = new NextDbClient({ endpoint, userId: "actor-window-user" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const firstRoomId = `actor-window-a-${suffix}`
  const secondRoomId = `actor-window-b-${suffix}`

  const firstMessages = await writeRoom(client, firstRoomId, 5)
  const firstLatestHotAndCold = await client.room(firstRoomId).messages.latest({ limit: 5 })
  assert.equal(firstLatestHotAndCold.source, "live")
  assert.deepEqual(
    firstLatestHotAndCold.messages.map((message) => message.body),
    ["message-4", "message-3", "message-2", "message-1", "message-0"],
  )
  assert.deepEqual(
    firstLatestHotAndCold.messages.slice(0, 2).map((message) => message.id),
    firstMessages.slice(-2).reverse().map((message) => message.id),
  )

  const firstBefore = await client.room(firstRoomId).messages.before(firstMessages[3].lsn, { limit: 3 })
  assert.deepEqual(firstBefore.messages.map((message) => message.body), ["message-2", "message-1", "message-0"])

  let health = await client.health()
  assert.equal(health.hotWindow, 2)
  assert.equal(health.maxHotRooms, 1)
  assert.equal(health.hotRoomCount, 1)
  assert.equal(health.roomCount, 1)
  let activationStatus = await client.runtimeActivationStatus()
  assert.equal(activationStatus.hotWindow, 2)
  assert.equal(activationStatus.maxHotRooms, 1)
  assert.equal(activationStatus.roomCount, 1)
  assert.equal(activationStatus.rooms[0]?.roomId, firstRoomId)
  assert.equal(activationStatus.rooms[0]?.messages, 2)
  assert.equal(activationStatus.rooms[0]?.newestLsn, firstMessages.at(-1).lsn)
  const activatedRoomRecordScope = await client.activateRuntimeRecords({ table: "rooms", key: firstRoomId })
  assert.equal(activatedRoomRecordScope.found, 1)
  assert.equal(activatedRoomRecordScope.actorScopes.length, 1)
  assert.equal(activatedRoomRecordScope.actorScope?.actorId.kind, "scope")
  assert.match(activatedRoomRecordScope.actorScope?.actorId.key ?? "", /^table:rooms\/bucket:[0-9a-f]{2}$/)
  assert.equal(activatedRoomRecordScope.actorScopes[0]?.actorId.key, activatedRoomRecordScope.actorScope?.actorId.key)
  assert.equal(activatedRoomRecordScope.actorScope?.tableActorId.kind, "table")
  assert.equal(activatedRoomRecordScope.actorScope?.tableActorId.key, "table:rooms")
  assert.equal(activatedRoomRecordScope.actorScope?.rows, 1)
  assert.equal(activatedRoomRecordScope.actorScope?.tableScopes, 1)
  const activatedScope = await client.activateRuntimeActor({ kind: "scope", key: `rooms/${firstRoomId}` })
  assert.equal(activatedScope.activated, true)
  assert.equal(activatedScope.turnCount, 1)
  assert.equal(activatedScope.after.kernelActors, activatedScope.before.kernelActors + 1)
  activationStatus = await client.runtimeActivationStatus()
  assert.equal(activationStatus.actorKernel.roomActors, 1)
  assert.equal(activationStatus.actorKernel.kernelActors, 3)
  assert.equal(activationStatus.actorKernel.kindCounts.scope, 2)
  assert.equal(activationStatus.actorKernel.kindCounts.table, 1)
  assert.equal(activationStatus.actorKernel.scopeRows, 1)
  assert.equal(activationStatus.actorKernel.tableScopes, 1)
  health = await client.health()
  assert.equal(health.actorKernel.kernelActors, 3)
  assert.equal(health.actorKernel.scopeRows, 1)
  assert.equal(health.actorKernel.tableScopes, 1)
  assert.equal(health.hotRoomCount, 1)

  const evictedFirst = await client.evictRuntimeRoom({ roomId: firstRoomId })
  assert.equal(evictedFirst.evicted, true)
  assert.equal(evictedFirst.afterRoomCount, 0)
  health = await client.health()
  assert.equal(health.hotRoomCount, 0)
  activationStatus = await client.runtimeActivationStatus()
  assert.equal(activationStatus.roomCount, 0)

  const stopActivationSubscription = client.subscribeRoom(firstRoomId, () => undefined, { catchUp: false })
  await waitFor(async () => {
    const status = await client.runtimeActivationStatus()
    return status.rooms.some((room) => room.roomId === firstRoomId && room.messages === 2)
  }, "subscription-driven room activation")
  stopActivationSubscription()
  const subscriptionActivatedFirstHot = await client.room(firstRoomId).messages.latest({ limit: 2 })
  assert.equal(subscriptionActivatedFirstHot.source, "live")
  assert.deepEqual(
    subscriptionActivatedFirstHot.messages.map((message) => message.id),
    firstMessages.slice(-2).toReversed().map((message) => message.id),
  )
  const evictedSubscriptionActivatedFirst = await client.evictRuntimeRoom({ roomId: firstRoomId })
  assert.equal(evictedSubscriptionActivatedFirst.evicted, true)

  const activatedFirst = await client.activateRuntimeRoom({ roomId: firstRoomId, limit: 2 })
  assert.equal(activatedFirst.source, "chatLog")
  assert.equal(activatedFirst.found, 2)
  assert.equal(activatedFirst.activated, true)
  assert.equal(activatedFirst.afterRoomCount, 1)
  activationStatus = await client.runtimeActivationStatus()
  assert.equal(activationStatus.rooms[0]?.roomId, firstRoomId)
  assert.equal(activationStatus.rooms[0]?.messages, 2)
  const explicitlyActivatedFirstHot = await client.room(firstRoomId).messages.latest({ limit: 2 })
  assert.equal(explicitlyActivatedFirstHot.source, "live")
  assert.deepEqual(
    explicitlyActivatedFirstHot.messages.map((message) => message.id),
    firstMessages.slice(-2).toReversed().map((message) => message.id),
  )

  await sleep(20)
  const secondMessages = await writeRoom(client, secondRoomId, 3)
  health = await client.health()
  assert.equal(health.hotRoomCount, 1)
  assert.equal(health.roomCount, 1)

  const evictedFirstLatest = await client.room(firstRoomId).messages.latest({ limit: 5 })
  assert.deepEqual(
    evictedFirstLatest.messages.map((message) => message.id),
    firstMessages.toReversed().map((message) => message.id),
  )
  const reactivatedFirstHot = await client.room(firstRoomId).messages.latest({ limit: 2 })
  assert.equal(reactivatedFirstHot.source, "live")
  assert.deepEqual(
    reactivatedFirstHot.messages.map((message) => message.id),
    firstMessages.slice(-2).toReversed().map((message) => message.id),
  )

  health = await client.health()
  assert.equal(health.hotRoomCount, 1)
  assert.equal(health.roomCount, 1)

  const secondStillRecoverable = await client.room(secondRoomId).messages.latest({ limit: 3 })
  assert.deepEqual(
    secondStillRecoverable.messages.map((message) => message.id),
    secondMessages.toReversed().map((message) => message.id),
  )
  const reactivatedSecondHot = await client.room(secondRoomId).messages.latest({ limit: 2 })
  assert.equal(reactivatedSecondHot.source, "live")
  assert.deepEqual(
    reactivatedSecondHot.messages.map((message) => message.id),
    secondMessages.slice(-2).toReversed().map((message) => message.id),
  )

  const evictedSecondForNestedSubscription = await client.evictRuntimeRoom({ roomId: secondRoomId })
  assert.equal(evictedSecondForNestedSubscription.evicted, true)
  const stopNestedActivationSubscription = client
    .nestedTable("rooms", secondRoomId, "messages")
    .subscribe(() => undefined, { catchUp: false })
  await waitFor(async () => {
    const status = await client.runtimeActivationStatus()
    return status.rooms.some((room) => room.roomId === secondRoomId && room.messages === 2)
  }, "nested subscription-driven room activation")
  stopNestedActivationSubscription()
  const nestedSubscriptionActivatedSecondHot = await client.room(secondRoomId).messages.latest({ limit: 2 })
  assert.equal(nestedSubscriptionActivatedSecondHot.source, "live")
  assert.deepEqual(
    nestedSubscriptionActivatedSecondHot.messages.map((message) => message.id),
    secondMessages.slice(-2).toReversed().map((message) => message.id),
  )

  const snapshot = await client.createSnapshot()
  assert.equal(snapshot.roomCount, 1)
  const tailRoomIds = [
    `actor-window-tail-a-${suffix}`,
    `actor-window-tail-b-${suffix}`,
    `actor-window-tail-c-${suffix}`,
  ]
  for (const tailRoomId of tailRoomIds) {
    await client.table("rooms").upsert(tailRoomId, {
      id: tailRoomId,
      title: `Actor Window Tail ${tailRoomId}`,
    }, {
      clientMutationId: `${tailRoomId}-upsert`,
    })
  }
  client.close()

  await stopNode(child, "SIGKILL")
  child = undefined

  child = startNode()
  await waitForHealth(endpoint)
  const recovered = new NextDbClient({ endpoint, userId: "actor-window-user" })
  const recoveredHealth = await recovered.health()
  assert.equal(recoveredHealth.startupRecovery.snapshotLoaded, true)
  assert.equal(recoveredHealth.hotWindow, 2)
  assert.equal(recoveredHealth.maxHotRooms, 1)
  assert.equal(recoveredHealth.hotRoomCount, 1)
  assert.equal(recoveredHealth.roomCount, 1)
  assert.ok(recoveredHealth.actorKernel.kernelActors >= 3)
  assert.ok(recoveredHealth.actorKernel.kindCounts.scope >= 2)
  assert.equal(recoveredHealth.actorKernel.kindCounts.table, 1)
  assert.equal(recoveredHealth.actorKernel.scopeRows, 1 + tailRoomIds.length)
  assert.ok(recoveredHealth.actorKernel.tableScopes >= 1)

  const recoveredFirst = await recovered.room(firstRoomId).messages.latest({ limit: 5 })
  assert.deepEqual(
    recoveredFirst.messages.map((message) => message.id),
    firstMessages.toReversed().map((message) => message.id),
  )
  const recoveredSecond = await recovered.room(secondRoomId).messages.latest({ limit: 3 })
  assert.deepEqual(
    recoveredSecond.messages.map((message) => message.id),
    secondMessages.toReversed().map((message) => message.id),
  )

  const tableSubscriptionBaseline = await recovered.health()
  const stopTableSubscription = recovered.table("rooms").subscribe(() => undefined, { catchUp: false })
  await waitFor(async () => {
    const status = await recovered.health()
    return status.actorKernel.scopeSubscriptionRefCount >=
      tableSubscriptionBaseline.actorKernel.scopeSubscriptionRefCount + 256
  }, "table subscription scope refcount")
  stopTableSubscription()
  await waitFor(async () => {
    const status = await recovered.health()
    return status.actorKernel.scopeSubscriptionRefCount ===
      tableSubscriptionBaseline.actorKernel.scopeSubscriptionRefCount
  }, "table subscription scope release")
  recovered.close()

  console.log("actor window smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function writeRoom(client, roomId, count) {
  await client.table("rooms").upsert(roomId, {
    id: roomId,
    title: `Actor Window ${roomId}`,
  }, {
    clientMutationId: `${roomId}-upsert`,
  })
  const messages = []
  for (let index = 0; index < count; index += 1) {
    messages.push(await client.room(roomId).messages.send(`message-${index}`, {
      clientMutationId: `${roomId}-message-${index}`,
    }))
  }
  return messages
}

function startNode() {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_HOT_WINDOW: "2",
      NEXTDB_MAX_HOT_ROOMS: "1",
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_ACTOR_WINDOW_SMOKE_LOGS === "1") {
      process.stdout.write(`[actor-window] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_ACTOR_WINDOW_SMOKE_LOGS === "1") {
      process.stderr.write(`[actor-window] ${chunk}`)
    }
  })
  return child
}

async function stopNode(child, signal = "SIGTERM") {
  if (!child || child.exitCode !== null) {
    return
  }
  child.kill(signal)
  await Promise.race([
    once(child, "exit"),
    new Promise((resolve) => setTimeout(resolve, 5_000)).then(() => {
      child.kill("SIGKILL")
      return once(child, "exit").catch(() => {})
    }),
  ])
}

async function waitForHealth(url) {
  await waitFor(async () => {
    const health = await getJson(`${url}/v1/health`).catch(() => undefined)
    return health?.ok === true
  }, `health at ${url}`)
}

async function waitFor(check, label, timeoutMs = 15_000) {
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
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`GET ${url} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
}

async function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      const port = typeof address === "object" && address ? address.port : undefined
      server.close((error) => {
        if (error) {
          reject(error)
          return
        }
        if (port === undefined) {
          reject(new Error("failed to allocate local port"))
          return
        }
        resolve(port)
      })
    })
  })
}
