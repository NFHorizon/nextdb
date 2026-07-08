import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"
import { createServer } from "node:net"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-cluster-election-"))
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

  const firstKey = `election-before-${Date.now()}`
  const firstWrite = await postJson(`${nodeA.url}/v1/records/rooms/${encodeURIComponent(firstKey)}`, {
    value: { id: firstKey, title: "before election" },
    durability: "strict",
    clientMutationId: `${firstKey}-upsert`,
  })
  assert(firstWrite.record.lsn >= 1)
  await waitForLsn(nodeB.url, firstWrite.record.lsn)
  await waitForLsn(nodeC.url, firstWrite.record.lsn)
  const ownerHealth = await getJson(`${nodeA.url}/v1/health`)
  await waitForPeerLsn(nodeB.url, nodeA.id, ownerHealth.currentLsn)

  await stopNode(children[0])
  await waitFor(async () => {
    const health = await getJson(`${nodeB.url}/v1/health`)
    const peer = health.peerHealth?.peers?.[nodeA.id]
    return peer?.ok === false && peer.lastSeenOkLsn >= ownerHealth.currentLsn
  }, "node-b detects node-a failure")

  let committedProposalId
  await waitFor(async () => {
    const health = await getJson(`${nodeB.url}/v1/health`)
    committedProposalId = health.failoverController?.lastCommittedProposalId
    return typeof committedProposalId === "string" && committedProposalId.length > 0
  }, "node-b automatic failover commit")

  const proposals = await getJson(`${nodeB.url}/v1/admin/cluster/topology/proposals`)
  const proposal = proposals.proposals.find((candidate) => candidate.id === committedProposalId)
  assert(proposal, `missing committed proposal ${committedProposalId}`)
  assert.equal(proposal.phase, "committed")
  assert.equal(proposal.request.owner, nodeB.id)
  assert.equal(proposal.request.epoch, 2)
  assert.equal(proposal.requiredAcks, 2)
  assert(proposal.prepareAcks.some((ack) => ack.nodeId === nodeC.id && ack.applied))
  assert(proposal.commitResults.some((ack) => ack.nodeId === nodeC.id && ack.applied))

  await waitFor(async () => {
    const topology = await getJson(`${nodeC.url}/v1/cluster/topology`)
    return topology.shards[0]?.owner === nodeB.id && topology.shards[0]?.epoch === 2
  }, "node-c committed elected topology")

  const afterKey = `election-after-${Date.now()}`
  const afterWrite = await postJson(`${nodeB.url}/v1/records/rooms/${encodeURIComponent(afterKey)}`, {
    value: { id: afterKey, title: "after election" },
    durability: "strict",
    clientMutationId: `${afterKey}-upsert`,
  })
  assert(afterWrite.record.lsn > firstWrite.record.lsn)
  await waitForLsn(nodeC.url, afterWrite.record.lsn)
  const replicatedAfter = await getJson(`${nodeC.url}/v1/records/rooms/${encodeURIComponent(afterKey)}`)
  assert.equal(replicatedAfter.record.key, afterKey)
  assert.equal(replicatedAfter.record.value.title, "after election")

  const nodeBIntegrity = await getJson(`${nodeB.url}/v1/admin/wal/integrity`)
  const nodeCIntegrity = await getJson(`${nodeC.url}/v1/admin/wal/integrity`)
  assert.equal(nodeBIntegrity.ok, true)
  assert.equal(nodeCIntegrity.ok, true)

  console.log("cluster failover election smoke ok")
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
      NEXTDB_WAL_SHARDS: "1",
      NEXTDB_SHARD_OWNERS: "0=node-a",
      NEXTDB_SHARD_EPOCHS: "0=1",
      NEXTDB_SHARD_REPLICAS: "0=node-b|node-c",
      NEXTDB_ENFORCE_SHARD_OWNERSHIP: "true",
      NEXTDB_WAL_REMOTE_ACKS: "quorum",
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_FAILOVER_CONTROLLER_INTERVAL_MS: item.id === "node-b" ? "100" : "0",
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

async function waitForPeerLsn(url, peerId, lsn) {
  await waitFor(async () => {
    const health = await getJson(`${url}/v1/health`)
    const peer = health.peerHealth?.peers?.[peerId]
    return peer?.ok === true && peer.lastSeenOkLsn >= lsn
  }, `peer ${peerId} lsn ${lsn} from ${url}`)
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
          reject(new Error("failed to allocate cluster election smoke port"))
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
