import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"
import { createServer } from "node:net"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-cluster-object-repair-"))
const [nodeAPort, nodeBPort, nodeCPort] = await freePorts(3)
const nodes = [
  node("node-a", nodeAPort),
  node("node-b", nodeBPort),
  node("node-c", nodeCPort),
]
const [nodeA, nodeB, nodeC] = nodes
const repairMode = process.env.NEXTDB_CLUSTER_OBJECT_REPAIR_MODE ?? "manual"
const clusterNodes = nodes.map((item) => `${item.id}=${item.url}`).join(",")
const children = new Map()

try {
  await Promise.all(nodes.map((item) => mkdir(item.dataDir, { recursive: true })))
  for (const item of nodes) {
    children.set(item.id, startNode(item))
  }
  await Promise.all(nodes.map((item) => waitForHealth(item.url)))
  await Promise.all([
    waitForPeer(nodeB.url, nodeA.id),
    waitForPeer(nodeB.url, nodeC.id),
    waitForPeer(nodeC.url, nodeA.id),
    waitForPeer(nodeC.url, nodeB.id),
  ])

  await stopNode(children.get(nodeC.id))
  children.delete(nodeC.id)

  const objectId = `object-repair-${Date.now()}`
  const objectBody = "object body repaired after replica restart"
  const object = await putObject(nodeA.url, objectId, objectBody, `${objectId}-put`)
  assert.equal(object.id, objectId)
  assert.equal(object.byteSize, objectBody.length)
  assert.equal(await getText(`${nodeB.url}/v1/objects/${encodeURIComponent(objectId)}/body`), objectBody)

  children.set(nodeC.id, startNode(nodeC))
  await waitForHealth(nodeC.url)

  if (repairMode === "controller") {
    await waitFor(async () => {
      try {
        return await getText(`${nodeC.url}/v1/objects/${encodeURIComponent(objectId)}/body`) === objectBody
      } catch {
        return false
      }
    }, "node-c object body repaired by controller")
  } else {
    const walRepair = await postJson(`${nodeA.url}/v1/admin/wal/replicate/repair?shard=0`, {})
    assert.equal(walRepair.repaired[0].satisfied, true)
    await waitForLsn(nodeC.url, object.lsn ?? (await getJson(`${nodeA.url}/v1/health`)).currentLsn)
    await assert.rejects(
      () => getText(`${nodeC.url}/v1/objects/${encodeURIComponent(objectId)}/body`),
      /returned 404/,
    )

    const objectRepair = await postJson(`${nodeA.url}/v1/admin/objects/repair?shard=0&objectId=${encodeURIComponent(objectId)}`, {})
    assert.equal(objectRepair.repaired.length, 1)
    assert.equal(objectRepair.repaired[0].shard, 0)
    assert.equal(objectRepair.repaired[0].satisfied, true)
    assert(objectRepair.repaired[0].objectsSent >= 1)
    assert(objectRepair.repaired[0].replicas.some((replica) =>
      replica.url.includes(`${nodeCPort}`) && replica.ok === true && replica.sent >= 1
    ))
    assert.equal(await getText(`${nodeC.url}/v1/objects/${encodeURIComponent(objectId)}/body`), objectBody)
  }

  console.log("cluster object repair smoke ok")
} finally {
  await Promise.all([...children.values()].map((child) => stopNode(child)))
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
      NEXTDB_WAL_SHARDS: "1",
      NEXTDB_SHARD_OWNERS: "0=node-a",
      NEXTDB_SHARD_EPOCHS: "0=1",
      NEXTDB_SHARD_REPLICAS: "0=node-b|node-c",
      NEXTDB_ENFORCE_SHARD_OWNERSHIP: "true",
      NEXTDB_WAL_REMOTE_ACKS: "quorum",
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_PEER_MONITOR_INTERVAL_MS: "100",
      NEXTDB_WAL_REPAIR_CONTROLLER_INTERVAL_MS: repairMode === "controller" && item.id === "node-a" ? "100" : "0",
      NEXTDB_OBJECT_REPAIR_CONTROLLER_INTERVAL_MS: repairMode === "controller" && item.id === "node-a" ? "100" : "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
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

async function stopNode(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) {
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

async function getText(url) {
  const response = await fetch(url)
  const text = await response.text()
  assert(response.ok, `${url} returned ${response.status}: ${text}`)
  return text
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
          reject(new Error("failed to allocate cluster object repair smoke port"))
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
