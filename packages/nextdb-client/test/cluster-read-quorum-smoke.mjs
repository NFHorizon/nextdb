import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"
import { createServer } from "node:net"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-cluster-read-quorum-"))
const [nodeAPort, nodeBPort, nodeCPort] = await freePorts(3)
const nodes = [
  node("node-a", nodeAPort),
  node("node-b", nodeBPort),
  node("node-c", nodeCPort),
]
const [nodeA, nodeB, nodeC] = nodes
const clusterNodes = nodes.map((item) => `${item.id}=${item.url}`).join(",")
const children = []

try {
  await Promise.all(nodes.map((item) => mkdir(item.dataDir, { recursive: true })))
  for (const item of nodes) {
    children.push(startNode(item))
  }
  await Promise.all(nodes.map((item) => waitForHealth(item.url)))
  await Promise.all([
    waitForPeer(nodeB.url, nodeA.id),
    waitForPeer(nodeB.url, nodeC.id),
    waitForPeer(nodeC.url, nodeA.id),
    waitForPeer(nodeC.url, nodeB.id),
  ])

  const key = await keyForShard(0)
  const otherKey = await keyForShard(1)
  const write = await postJson(`${nodeA.url}/v1/records/rooms/${encodeURIComponent(key)}`, {
    value: { id: key, title: "read quorum" },
    durability: "strict",
    clientMutationId: `${key}-upsert`,
  })
  const otherWrite = await postJson(`${nodeA.url}/v1/records/rooms/${encodeURIComponent(otherKey)}`, {
    value: { id: otherKey, title: "read quorum" },
    durability: "strict",
    clientMutationId: `${otherKey}-upsert`,
  })
  const nonMatchingKey = await keyForShard(1, "nonmatch")
  const nonMatchingWrite = await postJson(`${nodeA.url}/v1/records/rooms/${encodeURIComponent(nonMatchingKey)}`, {
    value: { id: nonMatchingKey, title: "read quorum nonmatch", category: "other" },
    durability: "strict",
    clientMutationId: `${nonMatchingKey}-upsert`,
  })
  const targetLsn = Math.max(write.record.lsn, otherWrite.record.lsn, nonMatchingWrite.record.lsn)
  await waitForLsn(nodeB.url, targetLsn)
  await waitForLsn(nodeC.url, targetLsn)

  const userId = await userIdForShard(0)
  const userProfile = await postJson(`${nodeA.url}/v1/users/${encodeURIComponent(userId)}`, {
    displayName: "Read Quorum User",
    metadata: { tier: "cluster" },
  })
  await waitForLsn(nodeB.url, userProfile.user.lsn)
  await waitForLsn(nodeC.url, userProfile.user.lsn)
  const firstUserEvent = await postJson(`${nodeA.url}/v1/mutate`, {
    type: "publishUserEvent",
    userId,
    name: "notification.created",
    payload: { text: "first read quorum user event" },
    durability: "strict",
  })
  const secondUserEvent = await postJson(`${nodeA.url}/v1/mutate`, {
    type: "publishUserEvent",
    userId,
    name: "notification.created",
    payload: { text: "second read quorum user event" },
    durability: "strict",
  })
  const userEventTargetLsn = secondUserEvent.event.lsn
  await waitForLsn(nodeB.url, userEventTargetLsn)
  await waitForLsn(nodeC.url, userEventTargetLsn)

  const roomId = await roomIdForShard(0)
  const firstMessage = await postJson(`${nodeA.url}/v1/mutate`, {
    type: "sendMessage",
    roomId,
    userId: "alice",
    body: "first read quorum message",
    durability: "strict",
    clientMutationId: `${roomId}-message-1`,
  })
  const secondMessage = await postJson(`${nodeA.url}/v1/mutate`, {
    type: "sendMessage",
    roomId,
    userId: "alice",
    body: "second read quorum message",
    durability: "strict",
    clientMutationId: `${roomId}-message-2`,
  })
  const messageTargetLsn = secondMessage.message.lsn
  await waitForLsn(nodeB.url, messageTargetLsn)
  await waitForLsn(nodeC.url, messageTargetLsn)

  const objectId = await objectIdForShard(0)
  const otherObjectId = await objectIdForShard(1)
  await putObject(nodeA.url, objectId, "object read quorum", `${objectId}-put`)
  await putObject(nodeA.url, otherObjectId, "other object read quorum", `${otherObjectId}-put`)
  const objectTargetLsn = (await getJson(`${nodeA.url}/v1/health`)).currentLsn
  await waitForLsn(nodeB.url, objectTargetLsn)
  await waitForLsn(nodeC.url, objectTargetLsn)

  const replicaClient = new NextDbClient({ endpoint: nodeB.url })
  const allRead = await replicaClient.table("rooms").get(key, {
    minLsn: write.record.lsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.equal(allRead.key, key)
  assert.equal(allRead.value.title, "read quorum")
  assert.equal(allRead.lsn, write.record.lsn)
  const allList = await replicaClient.table("rooms").list({
    limit: 10,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert(allList.records.some((record) => record.key === key && record.value.title === "read quorum"))
  assert(allList.records.some((record) => record.key === otherKey && record.value.title === "read quorum"))
  const allIndex = await replicaClient.table("rooms").index("byTitle", {
    value: "read quorum",
    limit: 10,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert(allIndex.records.some((record) => record.key === key))
  assert(allIndex.records.some((record) => record.key === otherKey))
  assert(!allIndex.records.some((record) => record.key === nonMatchingKey))
  const allRangeIndex = await replicaClient.table("rooms").index("byTitle", {
    lower: "read quorum",
    upper: "read quorum",
    limit: 10,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert(allRangeIndex.records.some((record) => record.key === key))
  assert(allRangeIndex.records.some((record) => record.key === otherKey))
  assert(!allRangeIndex.records.some((record) => record.key === nonMatchingKey))
  const firstRangePage = await replicaClient.table("rooms").index("byTitle", {
    lower: "read quorum",
    upper: "read quorum",
    limit: 1,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.equal(firstRangePage.records.length, 1)
  assert.equal(firstRangePage.hasMore, true)
  assert(firstRangePage.nextCursor)
  const secondRangePage = await replicaClient.table("rooms").index("byTitle", {
    lower: "read quorum",
    upper: "read quorum",
    limit: 10,
    afterCursor: firstRangePage.nextCursor,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert(secondRangePage.records.some((record) => record.key !== firstRangePage.records[0].key))
  assert(!secondRangePage.records.some((record) => record.key === firstRangePage.records[0].key))
  assert(!secondRangePage.records.some((record) => record.key === nonMatchingKey))
  const predicateList = await replicaClient.table("rooms").list({
    limit: 10,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "all",
    predicate: { all: [{ field: "category", op: "exists", value: false }] },
  })
  assert(predicateList.records.some((record) => record.key === key))
  assert(predicateList.records.some((record) => record.key === otherKey))
  assert(!predicateList.records.some((record) => record.key === nonMatchingKey))
  const allLatestMessages = await replicaClient.room(roomId).messages.latest({
    limit: 2,
    minLsn: messageTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.deepEqual(
    allLatestMessages.messages.map((message) => message.body),
    ["second read quorum message", "first read quorum message"],
  )
  const allPreviousMessages = await replicaClient.room(roomId).messages.before(secondMessage.message.lsn, {
    limit: 1,
    minLsn: messageTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.deepEqual(
    allPreviousMessages.messages.map((message) => message.body),
    ["first read quorum message"],
  )
  const allUser = await replicaClient.getUser(userId, {
    minLsn: userProfile.user.lsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.equal(allUser.userId, userId)
  assert.equal(allUser.displayName, "Read Quorum User")
  const allUsers = await replicaClient.listUsers({
    limit: 10,
    minLsn: userProfile.user.lsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert(allUsers.users.some((user) => user.userId === userId && user.displayName === "Read Quorum User"))
  const allUserEvents = await replicaClient.listUserEvents(userId, {
    limit: 2,
    minLsn: userEventTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.deepEqual(
    allUserEvents.map((event) => event.payload.text),
    ["second read quorum user event", "first read quorum user event"],
  )
  const allPreviousUserEvents = await replicaClient.listUserEvents(userId, {
    limit: 1,
    beforeLsn: secondUserEvent.event.lsn,
    minLsn: userEventTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.deepEqual(
    allPreviousUserEvents.map((event) => event.payload.text),
    ["first read quorum user event"],
  )
  const allObjects = await replicaClient.listObjects({
    limit: 10,
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert(allObjects.objects.some((object) => object.id === objectId))
  assert(allObjects.objects.some((object) => object.id === otherObjectId))
  const allObjectMetadata = await replicaClient.getObjectMetadata(objectId, {
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.equal(allObjectMetadata.id, objectId)
  assert.equal(allObjectMetadata.contentType, "text/plain")
  const allObjectBody = await replicaClient.getObjectBody(objectId, {
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.equal(await allObjectBody.text(), "object read quorum")
  const allObjectRange = await replicaClient.getObjectBodyRange(objectId, {
    start: 0,
    end: 5,
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.equal(allObjectRange.contentRange, "bytes 0-5/18")
  assert.equal(await allObjectRange.body.text(), "object")
  const firstObjectPage = await replicaClient.listObjects({
    limit: 1,
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.equal(firstObjectPage.objects.length, 1)
  assert.equal(firstObjectPage.hasMore, true)
  assert(firstObjectPage.nextAfterId)
  const secondObjectPage = await replicaClient.listObjects({
    limit: 10,
    afterId: firstObjectPage.nextAfterId,
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert(secondObjectPage.objects.some((object) => object.id !== firstObjectPage.objects[0].id))
  assert(!secondObjectPage.objects.some((object) => object.id === firstObjectPage.objects[0].id))

  await stopNode(children[2])
  const quorumRead = await replicaClient.table("rooms").get(key, {
    minLsn: write.record.lsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.equal(quorumRead.key, key)
  assert.equal(quorumRead.value.title, "read quorum")

  const quorumList = await replicaClient.table("rooms").list({
    limit: 10,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert(quorumList.records.some((record) => record.key === key && record.value.title === "read quorum"))
  assert(quorumList.records.some((record) => record.key === otherKey && record.value.title === "read quorum"))
  const quorumIndex = await replicaClient.table("rooms").index("byTitle", {
    value: "read quorum",
    limit: 10,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert(quorumIndex.records.some((record) => record.key === key))
  assert(quorumIndex.records.some((record) => record.key === otherKey))
  assert(!quorumIndex.records.some((record) => record.key === nonMatchingKey))
  const quorumRangeIndex = await replicaClient.table("rooms").index("byTitle", {
    lower: "read quorum",
    upper: "read quorum",
    limit: 10,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert(quorumRangeIndex.records.some((record) => record.key === key))
  assert(quorumRangeIndex.records.some((record) => record.key === otherKey))
  assert(!quorumRangeIndex.records.some((record) => record.key === nonMatchingKey))
  const quorumPredicateList = await replicaClient.table("rooms").list({
    limit: 10,
    minLsn: targetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
    predicate: { all: [{ field: "category", op: "exists", value: false }] },
  })
  assert(quorumPredicateList.records.some((record) => record.key === key))
  assert(quorumPredicateList.records.some((record) => record.key === otherKey))
  assert(!quorumPredicateList.records.some((record) => record.key === nonMatchingKey))
  const quorumLatestMessages = await replicaClient.room(roomId).messages.latest({
    limit: 2,
    minLsn: messageTargetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.deepEqual(
    quorumLatestMessages.messages.map((message) => message.body),
    ["second read quorum message", "first read quorum message"],
  )
  const quorumPreviousMessages = await replicaClient.room(roomId).messages.before(secondMessage.message.lsn, {
    limit: 1,
    minLsn: messageTargetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.deepEqual(
    quorumPreviousMessages.messages.map((message) => message.body),
    ["first read quorum message"],
  )
  const quorumUser = await replicaClient.getUser(userId, {
    minLsn: userProfile.user.lsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.equal(quorumUser.userId, userId)
  assert.equal(quorumUser.displayName, "Read Quorum User")
  const quorumUsers = await replicaClient.listUsers({
    limit: 10,
    minLsn: userProfile.user.lsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert(quorumUsers.users.some((user) => user.userId === userId && user.displayName === "Read Quorum User"))
  const quorumUserEvents = await replicaClient.listUserEvents(userId, {
    limit: 2,
    minLsn: userEventTargetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.deepEqual(
    quorumUserEvents.map((event) => event.payload.text),
    ["second read quorum user event", "first read quorum user event"],
  )
  const quorumPreviousUserEvents = await replicaClient.listUserEvents(userId, {
    limit: 1,
    beforeLsn: secondUserEvent.event.lsn,
    minLsn: userEventTargetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.deepEqual(
    quorumPreviousUserEvents.map((event) => event.payload.text),
    ["first read quorum user event"],
  )
  const quorumObjects = await replicaClient.listObjects({
    limit: 10,
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert(quorumObjects.objects.some((object) => object.id === objectId))
  assert(quorumObjects.objects.some((object) => object.id === otherObjectId))
  const quorumObjectMetadata = await replicaClient.getObjectMetadata(objectId, {
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.equal(quorumObjectMetadata.id, objectId)
  const quorumObjectBody = await replicaClient.getObjectBody(objectId, {
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.equal(await quorumObjectBody.text(), "object read quorum")
  const quorumObjectRange = await replicaClient.getObjectBodyRange(objectId, {
    start: 0,
    end: 5,
    minLsn: objectTargetLsn,
    timeoutMs: 1_000,
    consistency: "quorum",
  })
  assert.equal(quorumObjectRange.contentRange, "bytes 0-5/18")
  assert.equal(await quorumObjectRange.body.text(), "object")

  await assert.rejects(
    () => replicaClient.table("rooms").get(key, {
      minLsn: write.record.lsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.table("rooms").list({
      limit: 10,
      minLsn: targetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /list read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.table("rooms").index("byTitle", {
      value: "read quorum",
      limit: 10,
      minLsn: targetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /list read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.table("rooms").index("byTitle", {
      lower: "read quorum",
      upper: "read quorum",
      limit: 10,
      minLsn: targetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /list read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.room(roomId).messages.latest({
      limit: 2,
      minLsn: messageTargetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /room message read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.room(roomId).messages.before(secondMessage.message.lsn, {
      limit: 1,
      minLsn: messageTargetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /room message read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.getUser(userId, {
      minLsn: userProfile.user.lsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /user read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.listUsers({
      limit: 10,
      minLsn: userProfile.user.lsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /user list read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.listUserEvents(userId, {
      limit: 2,
      minLsn: userEventTargetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /user event read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.listObjects({
      limit: 10,
      minLsn: objectTargetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /object list read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.getObjectMetadata(objectId, {
      minLsn: objectTargetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /object metadata read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.getObjectBody(objectId, {
      minLsn: objectTargetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /object metadata read quorum failed/,
  )
  await assert.rejects(
    () => replicaClient.getObjectBodyRange(objectId, {
      start: 0,
      end: 5,
      minLsn: objectTargetLsn,
      timeoutMs: 150,
      consistency: "all",
    }),
    /object metadata read quorum failed/,
  )
  replicaClient.close()

  console.log("cluster read quorum smoke ok")
} finally {
  await Promise.all(children.map((child) => stopNode(child)))
  await rm(tempRoot, { recursive: true, force: true })
}

function node(id, port) {
  return {
    id,
    url: `http://127.0.0.1:${port}`,
    addr: `127.0.0.1:${port}`,
    dataDir: join(tempRoot, id),
  }
}

function startNode(item) {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_DATA_DIR: item.dataDir,
      NEXTDB_ADDR: item.addr,
      NEXTDB_NODE_ID: item.id,
      NEXTDB_NODE_URL: item.url,
      NEXTDB_CLUSTER_NODES: clusterNodes,
      NEXTDB_WAL_SHARDS: "2",
      NEXTDB_SHARD_OWNERS: "0=node-a,1=node-a",
      NEXTDB_SHARD_EPOCHS: "0=1,1=1",
      NEXTDB_SHARD_REPLICAS: "0=node-b|node-c;1=node-b|node-c",
      NEXTDB_ENFORCE_SHARD_OWNERSHIP: "true",
      NEXTDB_WAL_REMOTE_ACKS: "all",
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_PEER_MONITOR_INTERVAL_MS: "100",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_CLUSTER_SMOKE_LOGS === "1") {
      process.stdout.write(`[${item.id}] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_CLUSTER_SMOKE_LOGS === "1") {
      process.stderr.write(`[${item.id}] ${chunk}`)
    }
  })
  return child
}

async function keyForShard(shard, label = "read-quorum") {
  for (let index = 0; index < 500; index += 1) {
    const key = `${label}-s${shard}-${Date.now()}-${index}`
    const route = await getJson(`${nodeA.url}/v1/cluster/route?table=rooms&recordKey=${encodeURIComponent(key)}`)
    if (route.shard === shard) {
      return key
    }
  }
  throw new Error(`failed to find key for shard ${shard}`)
}

async function roomIdForShard(shard, label = "read-quorum-room") {
  for (let index = 0; index < 500; index += 1) {
    const roomId = `${label}-s${shard}-${Date.now()}-${index}`
    const route = await getJson(`${nodeA.url}/v1/cluster/route?roomId=${encodeURIComponent(roomId)}`)
    if (route.shard === shard) {
      return roomId
    }
  }
  throw new Error(`failed to find room id for shard ${shard}`)
}

async function userIdForShard(shard, label = "read-quorum-user") {
  for (let index = 0; index < 500; index += 1) {
    const userId = `${label}-s${shard}-${Date.now()}-${index}`
    const route = await getJson(`${nodeA.url}/v1/cluster/route?key=${encodeURIComponent(userId)}`)
    if (route.shard === shard) {
      return userId
    }
  }
  throw new Error(`failed to find user id for shard ${shard}`)
}

async function objectIdForShard(shard, label = "read-quorum-object") {
  for (let index = 0; index < 500; index += 1) {
    const objectId = `${label}-s${shard}-${Date.now()}-${index}`
    const route = await getJson(`${nodeA.url}/v1/cluster/route?objectId=${encodeURIComponent(objectId)}`)
    if (route.shard === shard) {
      return objectId
    }
  }
  throw new Error(`failed to find object id for shard ${shard}`)
}

async function stopNode(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return
  }
  child.kill("SIGTERM")
  await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL")
      resolve()
    }, 2_000)
    child.once("exit", () => {
      clearTimeout(timeout)
      resolve()
    })
  })
}

async function waitForHealth(url) {
  await waitFor(async () => {
    try {
      const health = await getJson(`${url}/v1/health`)
      return health.ok === true
    } catch {
      return false
    }
  }, `health ${url}`)
}

async function waitForPeer(url, peerId) {
  await waitFor(async () => {
    const health = await getJson(`${url}/v1/health`)
    const peer = health.peerHealth?.peers?.[peerId]
    return peer?.ok === true && peer.acceptingWrites === true && typeof peer.currentLsn === "number"
  }, `peer ${peerId} from ${url}`)
}

async function waitForLsn(url, lsn) {
  const response = await getJson(`${url}/v1/sync/wait?minLsn=${encodeURIComponent(String(lsn))}&timeoutMs=5000`)
  assert.equal(response.caughtUp, true, `${url} did not catch up to LSN ${lsn}: ${JSON.stringify(response)}`)
  assert(response.currentLsn >= lsn)
}

async function waitFor(predicate, label) {
  const deadline = Date.now() + 10_000
  let lastError
  while (Date.now() < deadline) {
    try {
      if (await predicate()) {
        return
      }
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`)
}

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  assert(response.ok, `${url} returned ${response.status}: ${text}`)
  return JSON.parse(text)
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  const text = await response.text()
  assert(response.ok, `${url} returned ${response.status}: ${text}`)
  return JSON.parse(text)
}

async function putObject(endpoint, objectId, body, clientMutationId) {
  const url = new URL("/v1/objects", endpoint)
  url.searchParams.set("contentType", "text/plain")
  url.searchParams.set("objectId", objectId)
  url.searchParams.set("clientMutationId", clientMutationId)
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body,
  })
  const text = await response.text()
  assert(response.ok, `${url} returned ${response.status}: ${text}`)
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
          reject(new Error("failed to allocate cluster read quorum smoke port"))
          return
        }
        resolve(port)
      })
    })
  })
}

async function freePorts(count) {
  const ports = []
  while (ports.length < count) {
    const port = await freePort()
    if (!ports.includes(port)) {
      ports.push(port)
    }
  }
  return ports
}
