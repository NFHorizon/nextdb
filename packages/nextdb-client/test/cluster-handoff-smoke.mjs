import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"
import { createServer } from "node:net"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-cluster-handoff-"))
const [nodeAPort, nodeBPort] = await freePorts(2)
const nodeA = {
  id: "node-a",
  url: `http://127.0.0.1:${nodeAPort}`,
  addr: `127.0.0.1:${nodeAPort}`,
  dataDir: join(tempRoot, "node-a"),
}
const nodeB = {
  id: "node-b",
  url: `http://127.0.0.1:${nodeBPort}`,
  addr: `127.0.0.1:${nodeBPort}`,
  dataDir: join(tempRoot, "node-b"),
}
const clusterNodes = `${nodeA.id}=${nodeA.url},${nodeB.id}=${nodeB.url}`
const children = []
const handoffMode = process.env.NEXTDB_CLUSTER_HANDOFF_MODE ?? "manual"

try {
  await Promise.all([
    mkdir(nodeA.dataDir, { recursive: true }),
    mkdir(nodeB.dataDir, { recursive: true }),
  ])
  children.push(startNode(nodeA))
  children.push(startNode(nodeB))
  await Promise.all([
    waitForHealth(nodeA.url),
    waitForHealth(nodeB.url),
  ])
  await waitForPeer(nodeA.url, nodeB.id)
  await waitForPeer(nodeB.url, nodeA.id)

  const nonOwnerSchema = await getJson(`${nodeB.url}/v1/schema`)
  nonOwnerSchema.version += 1
  nonOwnerSchema.tables.rooms.fields.shouldRejectOnReplica = { type: { kind: "string" }, optional: true }
  const nonOwnerApply = await fetch(`${nodeB.url}/v1/admin/schema/apply`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ schema: nonOwnerSchema, dryRun: false }),
  })
  assert.equal(nonOwnerApply.status, 409)
  const nonOwnerApplyBody = await nonOwnerApply.json()
  assert.equal(nonOwnerApplyBody.owner, nodeA.id)
  assert.equal(nonOwnerApplyBody.ownerUrl, nodeA.url)
  const nodeBAfterRejectedSchema = await getJson(`${nodeB.url}/v1/schema`)
  assert.equal(nodeBAfterRejectedSchema.version, 1)
  assert.equal(nodeBAfterRejectedSchema.tables.rooms.fields.shouldRejectOnReplica, undefined)

  const schemaClientThroughReplica = new NextDbClient({ endpoint: nodeB.url })
  const schema = await schemaClientThroughReplica.getSchema()
  schema.version += 1
  schema.tables.rooms.fields.replicatedLabel = { type: { kind: "string" }, optional: true }
  const schemaProposal = await schemaClientThroughReplica.startSchemaProposal(schema, {
    expectedVersion: 1,
    reason: "cluster schema proposal smoke",
  })
  assert.equal(schemaProposal.proposal.phase, "prepared")
  assert.equal(schemaProposal.proposal.requiredAcks, 2)
  assert.equal(schemaProposal.proposal.prepareAcks.filter((ack) => ack.applied).length, 2)
  assert(schemaProposal.proposal.prepareAcks.some((ack) => ack.nodeId === nodeB.id && ack.applied))
  await waitFor(async () => {
    const peerProposals = await getJson(`${nodeB.url}/v1/admin/schema/proposals`)
    return peerProposals.proposals.some((proposal) =>
      proposal.id === schemaProposal.proposal.id
        && proposal.phase === "prepared"
        && proposal.schema.version === schema.version
    )
  }, "node-b schema proposal prepare")
  const committedSchemaProposal = await schemaClientThroughReplica.commitSchemaProposal(schemaProposal.proposal.id)
  assert.equal(committedSchemaProposal.proposal.phase, "committed")
  assert.equal(committedSchemaProposal.proposal.commitAcks.filter((ack) => ack.applied).length, 2)
  assert(committedSchemaProposal.proposal.commitAcks.some((ack) => ack.nodeId === nodeB.id && ack.applied))
  await waitFor(async () => {
    const peerProposals = await getJson(`${nodeB.url}/v1/admin/schema/proposals`)
    return peerProposals.proposals.some((proposal) =>
      proposal.id === schemaProposal.proposal.id
        && proposal.phase === "committed"
        && proposal.schemaAuditLsn === committedSchemaProposal.proposal.schemaAuditLsn
    )
  }, "node-b schema proposal commit")
  const schemaApply = {
    ...committedSchemaProposal.proposal,
    applied: true,
    persisted: true,
    version: committedSchemaProposal.proposal.schema.version,
    schemaAuditLsn: committedSchemaProposal.proposal.schemaAuditLsn,
    peerPreflight: committedSchemaProposal.proposal.peerPreflight,
  }
  schemaClientThroughReplica.close()
  assert.equal(schemaApply.applied, true)
  assert.equal(schemaApply.persisted, true)
  assert.equal(schemaApply.version, schema.version)
  assert(Number.isInteger(schemaApply.schemaAuditLsn))
  assert.equal(schemaApply.peerPreflight?.requiredAcks, 1)
  assert.equal(schemaApply.peerPreflight?.acked, 1)
  assert(schemaApply.peerPreflight?.replicas.some((replica) => replica.nodeId === nodeB.id && replica.ok))
  await waitFor(async () => {
    const health = await getJson(`${nodeA.url}/v1/health`)
    const remote = health.walReplicas?.[0]?.remoteStatus?.remoteReplicas?.[0]
    return remote?.ok === true && remote.highestAckedLsn >= schemaApply.schemaAuditLsn
  }, "node-b remote schema WAL ack")
  await waitForLsn(nodeB.url, schemaApply.schemaAuditLsn)
  await waitFor(async () => {
    const remoteSchema = await getJson(`${nodeB.url}/v1/schema`)
    return remoteSchema.version === schema.version
      && remoteSchema.tables?.rooms?.fields?.replicatedLabel?.type?.kind === "string"
  }, "node-b replicated schema")
  const nodeBHistory = await getJson(`${nodeB.url}/v1/schema/history`)
  assert.deepEqual(nodeBHistory.entries.map((entry) => entry.version), [1, schema.version])
  const nodeBSchemaAudit = await getJson(`${nodeB.url}/v1/audit/wal?payloadType=schemaApplied&limit=10`)
  assert.equal(nodeBSchemaAudit.records.length, 1)
  assert.equal(nodeBSchemaAudit.records[0].payload.schema.version, schema.version)

  const firstKey = `handoff-before-${Date.now()}`
  const firstWrite = await postJson(`${nodeA.url}/v1/records/rooms/${encodeURIComponent(firstKey)}`, {
    value: { id: firstKey, title: "before handoff", replicatedLabel: "schema replicated" },
    durability: "strict",
    clientMutationId: `${firstKey}-upsert`,
  })
  assert(firstWrite.record.lsn >= 1)

  await waitFor(async () => {
    const health = await getJson(`${nodeA.url}/v1/health`)
    const remote = health.walReplicas?.[0]?.remoteStatus?.remoteReplicas?.[0]
    return remote?.ok === true && remote.highestAckedLsn >= firstWrite.record.lsn
  }, "node-b remote WAL ack")
  await waitForLsn(nodeB.url, firstWrite.record.lsn)

  const ownerConsistencyClient = new NextDbClient({ endpoint: nodeA.url })
  const replicatedWait = await ownerConsistencyClient.waitForLsn(firstWrite.record.lsn, {
    timeoutMs: 1_000,
    consistency: "all",
    shardKey: `rooms:${firstKey}`,
  })
  assert.equal(replicatedWait.caughtUp, true)
  assert.equal(replicatedWait.consistency, "all")
  assert.equal(replicatedWait.remoteRequiredAcks, 1)
  assert.equal(replicatedWait.remoteAcked, 1)
  assert.equal(replicatedWait.remoteCaughtUp, true)
  const replicatedFresh = await ownerConsistencyClient.table("rooms").get(firstKey, {
    minLsn: firstWrite.record.lsn,
    timeoutMs: 1_000,
    consistency: "all",
  })
  assert.equal(replicatedFresh.key, firstKey)
  assert.equal(replicatedFresh.value.title, "before handoff")
  ownerConsistencyClient.close()

  const replicatedBefore = await getJson(`${nodeB.url}/v1/records/rooms/${encodeURIComponent(firstKey)}`)
  assert.equal(replicatedBefore.record.key, firstKey)
  assert.equal(replicatedBefore.record.value.title, "before handoff")
  assert.equal(replicatedBefore.record.value.replicatedLabel, "schema replicated")

  const beforeObjectId = `handoff-object-before-${Date.now()}`
  const beforeObjectBody = "object body before handoff"
  const beforeObject = await putObject(nodeA.url, beforeObjectId, beforeObjectBody, `${beforeObjectId}-put`)
  assert.equal(beforeObject.id, beforeObjectId)
  assert.equal(beforeObject.byteSize, beforeObjectBody.length)
  await waitFor(async () => {
    const metadata = await getJson(`${nodeB.url}/v1/objects/${encodeURIComponent(beforeObjectId)}/metadata`)
    return metadata.id === beforeObjectId && await getText(`${nodeB.url}/v1/objects/${encodeURIComponent(beforeObjectId)}/body`) === beforeObjectBody
  }, "node-b replicated object before handoff")

  if (handoffMode === "failover-plan" || handoffMode === "failover-controller") {
    const ownerHealth = await getJson(`${nodeA.url}/v1/health`)
    await waitForPeerLsn(nodeB.url, nodeA.id, ownerHealth.currentLsn)
    await stopNode(children[0])
    await waitFor(async () => {
      const health = await getJson(`${nodeB.url}/v1/health`)
      const peer = health.peerHealth?.peers?.[nodeA.id]
      return peer?.ok === false && peer.lastSeenOkLsn >= ownerHealth.currentLsn
    }, "node-b detects node-a failure")

    const plan = await postJson(`${nodeB.url}/v1/admin/cluster/failover/plan`, { shard: 0 })
    assert.equal(plan.ready, true)
    assert.equal(plan.currentOwner, nodeA.id)
    assert.equal(plan.targetOwner, nodeB.id)
    assert.equal(plan.targetIsLocal, true)
    assert.equal(plan.targetIsReplica, true)
    assert.equal(plan.ownerHealthy, false)
    assert.equal(plan.targetCaughtUp, true)
    assert.equal(plan.requiredOverride.owner, nodeB.id)
    assert.equal(plan.requiredOverride.epoch, 2)

    const proposalResponse = handoffMode === "failover-controller"
      ? await waitForFailoverControllerProposal(nodeB.url)
      : await postJson(`${nodeB.url}/v1/admin/cluster/failover/proposals`, { shard: 0 })
    assert.equal(proposalResponse.plan.ready, true)
    assert.equal(proposalResponse.proposal.request.owner, nodeB.id)
    assert.equal(proposalResponse.proposal.request.epoch, 2)
    assert.equal(proposalResponse.proposal.phase, "failed")
    assert.equal(proposalResponse.proposal.requiredAcks, 2)
    assert.equal(proposalResponse.proposal.prepareAcks.filter((ack) => ack.applied).length, 1)
    assert.match(proposalResponse.proposal.lastError, /requires 2 acks/)

    const proposals = await getJson(`${nodeB.url}/v1/admin/cluster/topology/proposals`)
    assert(proposals.proposals.some((proposal) => proposal.id === proposalResponse.proposal.id))
    if (handoffMode === "failover-controller") {
      const health = await getJson(`${nodeB.url}/v1/health`)
      assert.equal(health.failoverController?.lastProposalId, proposalResponse.proposal.id)
      assert.equal(health.failoverController?.lastShard, 0)
    }

    const topology = await getJson(`${nodeB.url}/v1/cluster/topology`)
    assert.equal(topology.shards[0]?.owner, nodeA.id)
    assert.equal(topology.shards[0]?.epoch, 1)

    const nodeBIntegrity = await getJson(`${nodeB.url}/v1/admin/wal/integrity`)
    assert.equal(nodeBIntegrity.ok, true)

    console.log(handoffMode === "failover-controller" ? "cluster failover controller smoke ok" : "cluster failover plan smoke ok")
  } else {
    const workflowResponse = await postJson(`${nodeA.url}/v1/admin/cluster/handoff/workflows`, {
      shard: 0,
      targetOwner: nodeB.id,
    })
    assert.equal(workflowResponse.workflow.targetOwner, nodeB.id)

    if (handoffMode === "controller") {
      await waitFor(async () => {
        const health = await getJson(`${nodeA.url}/v1/health`)
        return health.handoffController?.lastAppliedWorkflowId === workflowResponse.workflow.id
      }, "node-a handoff controller apply")
    } else {
      const auto = await postJson(`${nodeA.url}/v1/admin/cluster/handoff/workflows/${workflowResponse.workflow.id}/auto`, {})
      assert.equal(auto.applied, true)
      assert.equal(auto.workflow.phase, "applied")
      assert.equal(auto.apply.topology.shards[0].owner, nodeB.id)
      assert.equal(auto.apply.topology.shards[0].epoch, 2)
      assert(auto.apply.propagation.some((result) => result.nodeId === nodeB.id && result.applied))
    }

    await waitFor(async () => {
      const topology = await getJson(`${nodeB.url}/v1/cluster/topology`)
      return topology.shards[0]?.owner === nodeB.id && topology.shards[0]?.epoch === 2
    }, "node-b committed topology")

    const afterKey = `handoff-after-${Date.now()}`
    const afterWrite = await postJson(`${nodeB.url}/v1/records/rooms/${encodeURIComponent(afterKey)}`, {
      value: { id: afterKey, title: "after handoff" },
      durability: "strict",
      clientMutationId: `${afterKey}-upsert`,
    })
    assert(afterWrite.record.lsn > firstWrite.record.lsn)

    await waitFor(async () => {
      const health = await getJson(`${nodeB.url}/v1/health`)
      const remote = health.walReplicas?.[0]?.remoteStatus?.remoteReplicas?.[0]
      return remote?.ok === true && remote.highestAckedLsn >= afterWrite.record.lsn
    }, "node-a remote WAL ack after handoff")

    const replicatedAfter = await getJson(`${nodeA.url}/v1/records/rooms/${encodeURIComponent(afterKey)}`)
    assert.equal(replicatedAfter.record.key, afterKey)
    assert.equal(replicatedAfter.record.value.title, "after handoff")

    const afterObjectId = `handoff-object-after-${Date.now()}`
    const afterObjectBody = "object body after handoff"
    const afterObject = await putObject(nodeB.url, afterObjectId, afterObjectBody, `${afterObjectId}-put`)
    assert.equal(afterObject.id, afterObjectId)
    assert.equal(afterObject.byteSize, afterObjectBody.length)
    await waitFor(async () => {
      const metadata = await getJson(`${nodeA.url}/v1/objects/${encodeURIComponent(afterObjectId)}/metadata`)
      return metadata.id === afterObjectId && await getText(`${nodeA.url}/v1/objects/${encodeURIComponent(afterObjectId)}/body`) === afterObjectBody
    }, "node-a replicated object after handoff")

    const nodeAIntegrity = await getJson(`${nodeA.url}/v1/admin/wal/integrity`)
    const nodeBIntegrity = await getJson(`${nodeB.url}/v1/admin/wal/integrity`)
    assert.equal(nodeAIntegrity.ok, true)
    assert.equal(nodeBIntegrity.ok, true)

    console.log("cluster handoff smoke ok")
  }
} finally {
  await Promise.all(children.map((child) => stopNode(child)))
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode(node) {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_DATA_DIR: node.dataDir,
      NEXTDB_ADDR: node.addr,
      NEXTDB_NODE_ID: node.id,
      NEXTDB_NODE_URL: node.url,
      NEXTDB_CLUSTER_NODES: clusterNodes,
      NEXTDB_WAL_SHARDS: "1",
      NEXTDB_SHARD_OWNERS: "0=node-a",
      NEXTDB_SHARD_EPOCHS: "0=1",
      NEXTDB_SHARD_REPLICAS: "0=node-b",
      NEXTDB_ENFORCE_SHARD_OWNERSHIP: "true",
      NEXTDB_WAL_REMOTE_ACKS: "all",
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_HANDOFF_CONTROLLER_INTERVAL_MS: handoffMode === "controller" && node.id === "node-a" ? "100" : "0",
      NEXTDB_FAILOVER_CONTROLLER_INTERVAL_MS: handoffMode === "failover-controller" && node.id === "node-b" ? "100" : "0",
      NEXTDB_PEER_MONITOR_INTERVAL_MS: "100",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_CLUSTER_SMOKE_LOGS === "1") {
      process.stdout.write(`[${node.id}] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_CLUSTER_SMOKE_LOGS === "1") {
      process.stderr.write(`[${node.id}] ${chunk}`)
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

async function waitForFailoverControllerProposal(url) {
  let proposalId
  await waitFor(async () => {
    const health = await getJson(`${url}/v1/health`)
    proposalId = health.failoverController?.lastProposalId
    return typeof proposalId === "string" && proposalId.length > 0
  }, `failover controller proposal from ${url}`)
  const proposals = await getJson(`${url}/v1/admin/cluster/topology/proposals`)
  const proposal = proposals.proposals.find((candidate) => candidate.id === proposalId)
  assert(proposal, `missing failover proposal ${proposalId}`)
  const plan = await postJson(`${url}/v1/admin/cluster/failover/plan`, { shard: proposal.request.shard })
  return { plan, proposal, topology: await getJson(`${url}/v1/cluster/topology`), overrides: {} }
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

async function getText(url) {
  const response = await fetch(url)
  const text = await response.text()
  assert(response.ok, `${url} returned ${response.status}: ${text}`)
  return text
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
          reject(new Error("failed to allocate cluster smoke port"))
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
