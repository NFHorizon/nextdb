import assert from "node:assert/strict"
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-export-import-"))
const source = {
  url: "http://127.0.0.1:3388",
  addr: "127.0.0.1:3388",
  dataDir: join(tempRoot, "source"),
}
const target = {
  url: "http://127.0.0.1:3389",
  addr: "127.0.0.1:3389",
  dataDir: join(tempRoot, "target"),
}
const encryptedTarget = {
  url: "http://127.0.0.1:3390",
  addr: "127.0.0.1:3390",
  dataDir: join(tempRoot, "encrypted-target"),
}
const archiveTarget = {
  url: "http://127.0.0.1:3391",
  addr: "127.0.0.1:3391",
  dataDir: join(tempRoot, "archive-target"),
}
const chainTarget = {
  url: "http://127.0.0.1:3392",
  addr: "127.0.0.1:3392",
  dataDir: join(tempRoot, "chain-target"),
}
const children = []
const encryptionKey = "export-import-encryption-key"

try {
  await Promise.all([
    mkdir(source.dataDir, { recursive: true }),
    mkdir(target.dataDir, { recursive: true }),
    mkdir(encryptedTarget.dataDir, { recursive: true }),
    mkdir(archiveTarget.dataDir, { recursive: true }),
    mkdir(chainTarget.dataDir, { recursive: true }),
  ])

  children.push(startNode(source, "source"))
  await waitForHealth(source.url)
  const sourceClient = new NextDbClient({ endpoint: source.url })
  const initialMetrics = await sourceClient.metrics()
  assert.match(initialMetrics, /^# HELP nextdb_up /m)
  assert.match(initialMetrics, /^nextdb_up 1$/m)
  assert.match(initialMetrics, /^nextdb_current_lsn 0$/m)
  const metricsResponse = await fetch(`${source.url}/v1/metrics`)
  assert.equal(metricsResponse.status, 200)
  assert(metricsResponse.headers.get("content-type")?.startsWith("text/plain; version=0.0.4"))

  const seedRoomId = `seed-room-${Date.now()}`
  const seedRoom = await postJson(`${source.url}/v1/records/rooms/${encodeURIComponent(seedRoomId)}`, {
    value: { id: seedRoomId, title: "Seed Room Before Schema v2" },
    durability: "strict",
    clientMutationId: `${seedRoomId}-upsert`,
  })
  assert(seedRoom.record.lsn >= 1)

  const schema = await getJson(`${source.url}/v1/schema`)
  schema.version = 2
  schema.tables.projects = {
    storage: { kind: "lru", maxItems: 1_000 },
    fields: {
      id: { type: { kind: "id", entity: "Project" }, optional: false },
      title: { type: { kind: "string" }, optional: false },
      ownerId: { type: { kind: "id", entity: "User" }, optional: false },
    },
    nested: {},
    indexes: {
      byOwner: { fields: ["ownerId"], unique: false },
    },
  }
  const apply = await postJson(`${source.url}/v1/admin/schema/apply`, { schema, dryRun: false })
  assert.equal(apply.applied, true)
  assert.equal(apply.version, 2)

  const proposalSchema = JSON.parse(JSON.stringify(schema))
  proposalSchema.version = 3
  proposalSchema.tables.projects.fields.description = {
    type: { kind: "string" },
    optional: true,
  }
  const preparedProposal = await sourceClient.startSchemaProposal(proposalSchema, {
    expectedVersion: 2,
    reason: "export import portable schema proposal",
  })
  assert.equal(preparedProposal.proposal.phase, "prepared")
  assert.equal(preparedProposal.proposal.schema.version, 3)
  const abortedProposal = await sourceClient.abortSchemaProposal(preparedProposal.proposal.id)
  assert.equal(abortedProposal.proposal.phase, "aborted")

  const topologyOverride = await sourceClient.applyTopologyOverride(0, {
    owner: "local",
    epoch: 2,
    replicas: ["local"],
  })
  assert.equal(topologyOverride.topology.shards[0].epoch, 2)
  const topologyProposal = await sourceClient.startTopologyProposal(0, {
    owner: "local",
    epoch: 3,
    replicas: ["local"],
  }, "export import portable topology proposal")
  assert.equal(topologyProposal.proposal.phase, "prepared")
  const abortedTopologyProposal = await sourceClient.abortTopologyProposal(topologyProposal.proposal.id)
  assert.equal(abortedTopologyProposal.proposal.phase, "aborted")

  const projectId = `project-${Date.now()}`
  const objectId = `export-import-object-${Date.now()}`
  const objectBody = "export import object body"
  const roomId = "export-import-room"
  const messageBody = "export import message"

  const project = await postJson(`${source.url}/v1/records/projects/${encodeURIComponent(projectId)}`, {
    value: { id: projectId, title: "Schema Carried Project", ownerId: "alice" },
    durability: "strict",
    clientMutationId: `${projectId}-upsert`,
  })
  assert(project.record.lsn >= 1)

  const object = await putObject(source.url, objectId, objectBody, `${objectId}-put`)
  assert.equal(object.id, objectId)
  assert.equal(object.byteSize, objectBody.length)

  const message = await postJson(`${source.url}/v1/mutate`, {
    type: "sendMessage",
    roomId,
    userId: "alice",
    body: messageBody,
    attachments: [objectId],
    durability: "strict",
    clientMutationId: `${roomId}-message`,
  })
  assert.equal(message.type, "messageCreated")
  assert(message.message.lsn > project.record.lsn)

  const bundle = await sourceClient.createExportBundle()
  assert.equal(bundle.manifest.schemaVersion, 2)
  assert.deepEqual(bundle.manifest.schemaHistoryVersions, [1, 2])
  assert.equal(bundle.manifest.schemaProposals, 1)
  assert.deepEqual(bundle.manifest.clusterControl, {
    topologyOverrides: 1,
    topologyLogEntries: 1,
    topologyProposals: 1,
    handoffWorkflows: 0,
    topologyLeaseTerm: abortedTopologyProposal.proposal.term,
  })
  assert.deepEqual(bundle.schemaHistoryVersions, [1, 2])
  assert.equal(bundle.schemaProposals, 1)
  assert.deepEqual(bundle.clusterControl, bundle.manifest.clusterControl)
  assert(bundle.schemaHistoryDir.endsWith("/schema/history"))
  assert(bundle.schemaProposalsPath.endsWith("/schema/proposals.json"))
  assert(bundle.clusterControlDir.endsWith("/cluster"))
  assert.equal(bundle.walRecords, 5)
  assert.equal(bundle.objects, 1)
  assert(bundle.schemaPath.endsWith("/schema.json"))

  const sourceBundles = await sourceClient.listExportBundles()
  assert(sourceBundles.bundles.some((entry) => entry.id === bundle.id && entry.schemaVersion === 2 && entry.ok))
  assert.deepEqual(sourceBundles.bundles.find((entry) => entry.id === bundle.id)?.schemaHistoryVersions, [1, 2])
  assert.equal(sourceBundles.bundles.find((entry) => entry.id === bundle.id)?.schemaProposals, 1)
  assert.deepEqual(sourceBundles.bundles.find((entry) => entry.id === bundle.id)?.clusterControl, bundle.manifest.clusterControl)

  const sourceVerify = await sourceClient.verifyExportBundle(bundle.id)
  assert.equal(sourceVerify.ok, true)
  assert.equal(sourceVerify.schemaVersion, 2)
  assert.deepEqual(sourceVerify.schemaHistoryVersions, [1, 2])
  assert.equal(sourceVerify.schemaProposals, 1)
  assert.deepEqual(sourceVerify.clusterControl, bundle.manifest.clusterControl)
  assert.equal(sourceVerify.encrypted, false)
  assert.deepEqual(sourceVerify.problems, [])

  const encryptedBundle = await sourceClient.createExportBundle({ encryptionKey })
  assert.equal(encryptedBundle.encrypted, true)
  assert.equal(encryptedBundle.manifest.encryption.encrypted, true)
  assert.equal(encryptedBundle.manifest.encryption.algorithm, "AES-256-GCM")
  assert(encryptedBundle.manifest.encryption.encryptedFiles > 0)
  assert.deepEqual(encryptedBundle.manifest.clusterControl, bundle.manifest.clusterControl)
  const encryptedSchemaBytes = await readFile(join(encryptedBundle.path, "schema.json"))
  assert.equal(encryptedSchemaBytes.includes(Buffer.from("\"projects\"")), false)
  const encryptedVerifyWithoutKey = await sourceClient.verifyExportBundle(encryptedBundle.id)
  assert.equal(encryptedVerifyWithoutKey.ok, false)
  assert.equal(encryptedVerifyWithoutKey.encrypted, true)
  assert(encryptedVerifyWithoutKey.problems.some((problem) => problem.includes("encryptionKey is required")))
  const encryptedVerify = await sourceClient.verifyExportBundle(encryptedBundle.id, { encryptionKey })
  assert.equal(encryptedVerify.ok, true)
  assert.equal(encryptedVerify.encrypted, true)
  assert.equal(encryptedVerify.schemaVersion, 2)
  assert.equal(encryptedVerify.walRecords, 5)
  assert.deepEqual(encryptedVerify.clusterControl, bundle.manifest.clusterControl)

  const deltaObjectId = `export-import-delta-object-${Date.now()}`
  const deltaObjectBody = "export import delta object body"
  const deltaMessageBody = "export import delta message"
  const deltaObject = await putObject(source.url, deltaObjectId, deltaObjectBody, `${deltaObjectId}-put`)
  assert.equal(deltaObject.id, deltaObjectId)
  const deltaMessage = await postJson(`${source.url}/v1/mutate`, {
    type: "sendMessage",
    roomId,
    userId: "alice",
    body: deltaMessageBody,
    attachments: [deltaObjectId],
    durability: "strict",
    clientMutationId: `${roomId}-delta-message`,
  })
  assert.equal(deltaMessage.type, "messageCreated")

  const deltaManifest = await sourceClient.exportManifest({ baseLsn: encryptedBundle.manifest.currentLsn })
  assert.equal(deltaManifest.incremental, true)
  assert.equal(deltaManifest.baseLsn, encryptedBundle.manifest.currentLsn)
  assert.equal(deltaManifest.wal.records, 2)
  assert.equal(deltaManifest.objects.live, 1)
  assert.equal(deltaManifest.objects.liveBytes, deltaObjectBody.length)

  const deltaBundle = await sourceClient.createExportBundle({ baseLsn: encryptedBundle.manifest.currentLsn })
  assert.equal(deltaBundle.manifest.incremental, true)
  assert.equal(deltaBundle.manifest.baseLsn, encryptedBundle.manifest.currentLsn)
  assert.equal(deltaBundle.walRecords, 2)
  assert.equal(deltaBundle.objects, 1)
  assert.equal(deltaBundle.objectBytes, deltaObjectBody.length)
  assert.equal(deltaBundle.manifest.wal.lowestLsn, encryptedBundle.manifest.currentLsn + 1)
  assert.equal(deltaBundle.manifest.wal.highestLsn, deltaMessage.message.lsn)
  const deltaVerify = await sourceClient.verifyExportBundle(deltaBundle.id)
  assert.equal(deltaVerify.ok, true, JSON.stringify(deltaVerify.problems))
  assert.equal(deltaVerify.manifest.incremental, true)
  assert.equal(deltaVerify.manifest.baseLsn, encryptedBundle.manifest.currentLsn)
  const chainVerify = await sourceClient.verifyExportBundleChain([bundle.id, deltaBundle.id])
  assert.equal(chainVerify.ok, true, JSON.stringify(chainVerify.problems))
  assert.equal(chainVerify.baseLsn, 0)
  assert.equal(chainVerify.highestLsn, deltaBundle.manifest.currentLsn)
  assert.deepEqual(chainVerify.bundles.map((entry) => entry.id), [bundle.id, deltaBundle.id])
  const brokenChainVerify = await sourceClient.verifyExportBundleChain([deltaBundle.id, bundle.id])
  assert.equal(brokenChainVerify.ok, false)
  assert(brokenChainVerify.problems.some((problem) => problem.includes("must be a full base bundle")))
  const deltaPreflight = await sourceClient.importBundlePreflight(deltaBundle.id)
  assert.equal(deltaPreflight.ok, false)
  assert(deltaPreflight.problems.some((problem) => problem.includes("bundle is incremental from base LSN")))
  const deltaMetadata = JSON.parse(await readFile(join(deltaBundle.objectMetadataDir, `${deltaObjectId}.json`), "utf8"))
  assert.equal(deltaMetadata.id, deltaObjectId)
  assert.equal(await readFile(join(deltaBundle.objectBlobDir, `${deltaObjectId}.bin`), "utf8"), deltaObjectBody)

  const backupObjectId = `export-import-backup-object-${Date.now()}`
  const backupPayloadObjectId = `export-import-backup-payload-${Date.now()}`
  const backupPayloadBody = "export import backup payload"
  await putObject(source.url, backupPayloadObjectId, backupPayloadBody, `${backupPayloadObjectId}-put`)
  const backupMessage = await postJson(`${source.url}/v1/mutate`, {
    type: "sendMessage",
    roomId,
    userId: "alice",
    body: "export import backup message",
    attachments: [backupPayloadObjectId],
    durability: "strict",
    clientMutationId: `${roomId}-backup-message`,
  })
  assert.equal(backupMessage.type, "messageCreated")
  const backupRun = await sourceClient.runExportBackup({
    encryptionKey,
    objectId: backupObjectId,
    clientMutationId: `${backupObjectId}-archive`,
  })
  assert.equal(backupRun.noOp, false)
  assert.equal(backupRun.mode, "incremental")
  assert.equal(backupRun.baseLsn, deltaBundle.manifest.currentLsn)
  assert.equal(backupRun.bundle.walRecords, 2)
  assert.equal(backupRun.bundle.manifest.currentLsn, backupMessage.message.lsn)
  assert.equal(backupRun.archived.object.id, backupObjectId)
  assert.equal(backupRun.archived.object.contentType, "application/vnd.nextdb.export-bundle-archive+json")
  assert.equal(backupRun.chain.ok, true, JSON.stringify(backupRun.chain.problems))
  assert.equal(backupRun.chain.highestLsn, backupRun.bundle.manifest.currentLsn)
  assert.equal(backupRun.run.noOp, false)
  assert.equal(backupRun.run.bundleId, backupRun.bundle.id)
  assert.equal(backupRun.run.objectId, backupObjectId)
  assert.equal(backupRun.run.chainOk, true)
  assert.deepEqual(backupRun.run.chainBundleIds, backupRun.chain.bundles.map((entry) => entry.id))
  const backupRuns = await sourceClient.listExportBackupRuns()
  assert.equal(backupRuns.runs[0].id, backupRun.run.id)
  const persistedBackupRuns = JSON.parse(await readFile(join(source.dataDir, "exports", "backup-runs.json"), "utf8"))
  assert(persistedBackupRuns.some((run) => run.id === backupRun.run.id && run.bundleId === backupRun.bundle.id))
  const backupRetentionCutoff = Date.now() + 60_000
  const backupRetentionPlan = await sourceClient.retainExportBackups({
    dryRun: true,
    keepLast: 0,
    beforeTimestampMs: backupRetentionCutoff,
    deleteBundles: true,
    deleteArchiveObjects: true,
  })
  assert.equal(backupRetentionPlan.dryRun, true)
  assert.equal(backupRetentionPlan.candidates, 1)
  assert(backupRetentionPlan.deletedRuns.includes(backupRun.run.id))
  assert(backupRetentionPlan.deletedBundles.includes(backupRun.bundle.id))
  assert(backupRetentionPlan.deletedArchiveObjects.includes(backupObjectId))
  const backupRetentionApply = await sourceClient.retainExportBackups({
    dryRun: false,
    keepLast: 0,
    beforeTimestampMs: backupRetentionCutoff,
    deleteBundles: true,
    deleteArchiveObjects: true,
  })
  assert.equal(backupRetentionApply.dryRun, false)
  assert.equal(backupRetentionApply.candidates, 1)
  assert(backupRetentionApply.deletedRuns.includes(backupRun.run.id))
  assert(backupRetentionApply.deletedBundles.includes(backupRun.bundle.id))
  assert(backupRetentionApply.deletedArchiveObjects.includes(backupObjectId))
  const backupRunsAfterRetention = await sourceClient.listExportBackupRuns()
  assert(!backupRunsAfterRetention.runs.some((run) => run.id === backupRun.run.id))
  await assert.rejects(readFile(join(backupRun.bundle.path, "manifest.json"), "utf8"))
  await assert.rejects(readFile(join(source.dataDir, "objects", "metadata", `${backupObjectId}.json`), "utf8"))
  const backupPolicy = await sourceClient.setExportBackupPolicy({
    enabled: false,
    intervalMs: 0,
    archiveObject: false,
    retentionKeepLast: 1,
    retentionDeleteBundles: true,
    retentionDeleteArchiveObjects: false,
  })
  assert.equal(backupPolicy.policy.archiveObject, false)
  assert.equal(backupPolicy.policy.retentionKeepLast, 1)
  const persistedBackupPolicy = JSON.parse(await readFile(join(source.dataDir, "exports", "backup-policy.json"), "utf8"))
  assert.equal(persistedBackupPolicy.archiveObject, false)
  const readBackupPolicy = await sourceClient.getExportBackupPolicy()
  assert.equal(readBackupPolicy.policy.retentionKeepLast, 1)
  const policyRun = await sourceClient.runExportBackupPolicy()
  assert.equal(policyRun.policy.archiveObject, false)
  assert.equal(policyRun.backup.noOp, false)
  assert.equal(policyRun.backup.archived, null)
  assert.equal(policyRun.retention.retained, 1)
  const policyRuns = await sourceClient.listExportBackupRuns()
  assert(policyRuns.runs.some((run) => run.id === policyRun.backup.run.id))

  const archiveObjectId = `export-import-archive-${Date.now()}`
  const archivedBundle = await sourceClient.archiveExportBundleToObject(encryptedBundle.id, {
    objectId: archiveObjectId,
    clientMutationId: `${archiveObjectId}-archive`,
  })
  assert.equal(archivedBundle.bundleId, encryptedBundle.id)
  assert.equal(archivedBundle.object.id, archiveObjectId)
  assert.equal(archivedBundle.object.contentType, "application/vnd.nextdb.export-bundle-archive+json")
  assert(archivedBundle.files > 0)
  assert(archivedBundle.bytes > 0)

  await stopNode(children.pop())

  await mkdir(join(target.dataDir, "exports"), { recursive: true })
  const targetBundlePath = join(target.dataDir, "exports", bundle.id)
  await cp(bundle.path, targetBundlePath, { recursive: true })
  await mkdir(join(encryptedTarget.dataDir, "exports"), { recursive: true })
  const encryptedTargetBundlePath = join(encryptedTarget.dataDir, "exports", encryptedBundle.id)
  await cp(encryptedBundle.path, encryptedTargetBundlePath, { recursive: true })
  await mkdir(join(archiveTarget.dataDir, "objects", "metadata"), { recursive: true })
  await mkdir(join(archiveTarget.dataDir, "objects", "blobs"), { recursive: true })
  await cp(
    join(source.dataDir, "objects", "metadata", `${archiveObjectId}.json`),
    join(archiveTarget.dataDir, "objects", "metadata", `${archiveObjectId}.json`),
  )
  await cp(
    join(source.dataDir, "objects", "blobs", `${archiveObjectId}.bin`),
    join(archiveTarget.dataDir, "objects", "blobs", `${archiveObjectId}.bin`),
  )
  await mkdir(join(chainTarget.dataDir, "exports"), { recursive: true })
  await cp(bundle.path, join(chainTarget.dataDir, "exports", bundle.id), { recursive: true })
  await cp(deltaBundle.path, join(chainTarget.dataDir, "exports", deltaBundle.id), { recursive: true })

  children.push(startNode(target, "target"))
  await waitForHealth(target.url)
  const targetClient = new NextDbClient({ endpoint: target.url })

  const targetBundles = await targetClient.listExportBundles()
  assert.equal(targetBundles.bundles.length, 1)
  assert.equal(targetBundles.bundles[0].id, bundle.id)
  assert.equal(targetBundles.bundles[0].schemaVersion, 2)
  assert.deepEqual(targetBundles.bundles[0].schemaHistoryVersions, [1, 2])
  assert.equal(targetBundles.bundles[0].schemaProposals, 1)
  assert.deepEqual(targetBundles.bundles[0].clusterControl, bundle.manifest.clusterControl)
  assert.equal(targetBundles.bundles[0].walRecords, 5)
  assert.equal(targetBundles.bundles[0].objects, 1)
  assert.equal(targetBundles.bundles[0].ok, true)

  const targetSchemaPath = join(targetBundlePath, "schema.json")
  const originalTargetSchema = await readFile(targetSchemaPath, "utf8")
  const brokenSchema = JSON.parse(originalTargetSchema)
  brokenSchema.tables.projects.fields.title.type = { kind: "int64" }
  await writeFile(targetSchemaPath, JSON.stringify(brokenSchema, null, 2))
  const brokenPreflight = await targetClient.importBundlePreflight(bundle.id)
  assert.equal(brokenPreflight.ok, false)
  assert(brokenPreflight.problems.some((problem) => problem.includes("failed schema validation")))
  await writeFile(targetSchemaPath, originalTargetSchema)

  const preflight = await targetClient.importBundlePreflight(bundle.id)
  assert.equal(preflight.ok, true)
  assert.equal(preflight.currentLsn, 0)
  assert.equal(preflight.bundleSchemaVersion, 2)
  assert.deepEqual(preflight.bundleSchemaHistoryVersions, [1, 2])
  assert.equal(preflight.bundleSchemaProposals, 1)
  assert.deepEqual(preflight.bundleClusterControl, bundle.manifest.clusterControl)
  assert.equal(preflight.bundleWalRecords, 5)

  const restore = await targetClient.restoreImportBundle(bundle.id)
  assert.equal(restore.restored, true)
  assert.equal(restore.schemaVersion, 2)
  assert.deepEqual(restore.schemaHistoryVersions, [1, 2])
  assert.equal(restore.schemaProposals, 1)
  assert.deepEqual(restore.clusterControl, bundle.manifest.clusterControl)
  assert.equal(restore.walRecords, 5)
  assert.equal(restore.objects, 1)
  assert.equal(restore.currentLsn, 5)

  const restoredSchema = await getJson(`${target.url}/v1/schema`)
  assert.equal(restoredSchema.version, 2)
  assert(restoredSchema.tables.projects)
  const restoredSchemaHistory = await targetClient.schemaHistory()
  assert.deepEqual(restoredSchemaHistory.entries.map((entry) => entry.version), [1, 2])
  const restoredV1Schema = await targetClient.getSchemaVersion(1)
  assert.equal(restoredV1Schema.version, 1)
  assert.equal(restoredV1Schema.tables.projects, undefined)
  const restoredProposals = await targetClient.schemaProposals()
  const restoredProposal = restoredProposals.proposals.find((proposal) => proposal.id === abortedProposal.proposal.id)
  assert(restoredProposal)
  assert.equal(restoredProposal.phase, "aborted")
  assert.equal(restoredProposal.schema.version, 3)
  const restoredTopology = await targetClient.topologyOverrides()
  assert.equal(restoredTopology.topology.shards[0].epoch, 2)
  assert.equal(restoredTopology.overrides["0"].owner, "local")
  const restoredTopologyLog = await targetClient.topologyLog()
  assert.equal(restoredTopologyLog.entries.length, 1)
  assert.equal(restoredTopologyLog.entries[0].reason, "operator topology override")
  const restoredTopologyProposals = await targetClient.topologyProposals()
  const restoredTopologyProposal = restoredTopologyProposals.proposals.find((proposal) => proposal.id === abortedTopologyProposal.proposal.id)
  assert(restoredTopologyProposal)
  assert.equal(restoredTopologyProposal.phase, "aborted")
  assert.equal(restoredTopologyProposal.term, abortedTopologyProposal.proposal.term)
  const restoredHealth = await targetClient.health()
  assert.equal(restoredHealth.topologyLease.currentTerm, abortedTopologyProposal.proposal.term)

  const restoredProject = await getJson(`${target.url}/v1/records/projects/${encodeURIComponent(projectId)}`)
  assert.equal(restoredProject.record.value.title, "Schema Carried Project")

  const projectIndex = await getJson(`${target.url}/v1/records/projects/indexes/byOwner?value=alice&limit=10`)
  assert.equal(projectIndex.records.length, 1)
  assert.equal((projectIndex.records[0].record ?? projectIndex.records[0]).key, projectId)

  const restoredObjectBody = await getText(`${target.url}/v1/objects/${encodeURIComponent(objectId)}/body`)
  assert.equal(restoredObjectBody, objectBody)

  const latestMessages = await getJson(`${target.url}/v1/rooms/${encodeURIComponent(roomId)}/messages/latest?limit=10`)
  assert.equal(latestMessages.messages.length, 1)
  assert.equal(latestMessages.messages[0].body, messageBody)
  assert.deepEqual(latestMessages.messages[0].attachments.map((attachment) => attachment.id ?? attachment), [objectId])

  const integrity = await getJson(`${target.url}/v1/admin/wal/integrity`)
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, 5)

  const duplicateRestore = await fetch(`${target.url}/v1/admin/import/bundles/${encodeURIComponent(bundle.id)}/restore`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}",
  })
  assert.equal(duplicateRestore.status, 409)

  const targetDeltaBundlePath = join(target.dataDir, "exports", deltaBundle.id)
  await cp(deltaBundle.path, targetDeltaBundlePath, { recursive: true })
  const targetDeltaPreflight = await targetClient.importBundleDeltaPreflight(deltaBundle.id)
  assert.equal(targetDeltaPreflight.ok, true, JSON.stringify(targetDeltaPreflight.problems))
  assert.equal(targetDeltaPreflight.baseLsn, bundle.manifest.currentLsn)
  assert.equal(targetDeltaPreflight.currentLsn, bundle.manifest.currentLsn)
  assert.equal(targetDeltaPreflight.bundleWalRecords, 2)
  assert.equal(targetDeltaPreflight.bundleHighestLsn, deltaMessage.message.lsn)
  const targetDeltaApply = await targetClient.applyImportBundleDelta(deltaBundle.id)
  assert.equal(targetDeltaApply.applied, true)
  assert.equal(targetDeltaApply.baseLsn, bundle.manifest.currentLsn)
  assert.equal(targetDeltaApply.walRecords, 2)
  assert.equal(targetDeltaApply.objects, 1)
  assert.equal(targetDeltaApply.currentLsn, deltaMessage.message.lsn)
  const targetDeltaBody = await getText(`${target.url}/v1/objects/${encodeURIComponent(deltaObjectId)}/body`)
  assert.equal(targetDeltaBody, deltaObjectBody)
  const targetMessagesAfterDelta = await getJson(`${target.url}/v1/rooms/${encodeURIComponent(roomId)}/messages/latest?limit=10`)
  assert.deepEqual(
    targetMessagesAfterDelta.messages.map((entry) => entry.body),
    [deltaMessageBody, messageBody],
  )

  children.push(startNode(chainTarget, "chain-target"))
  await waitForHealth(chainTarget.url)
  const chainTargetClient = new NextDbClient({ endpoint: chainTarget.url })
  const chainRestore = await chainTargetClient.restoreImportBundleChain([bundle.id, deltaBundle.id])
  assert.equal(chainRestore.restored, true)
  assert.equal(chainRestore.chain.ok, true, JSON.stringify(chainRestore.chain.problems))
  assert.equal(chainRestore.base.restored, true)
  assert.equal(chainRestore.base.currentLsn, bundle.manifest.currentLsn)
  assert.equal(chainRestore.deltas.length, 1)
  assert.equal(chainRestore.deltas[0].applied, true)
  assert.equal(chainRestore.deltas[0].baseLsn, bundle.manifest.currentLsn)
  assert.equal(chainRestore.currentLsn, deltaMessage.message.lsn)
  assert.equal(chainRestore.walRecords, 7)
  assert.equal(chainRestore.objects, 2)
  assert.equal(chainRestore.objectBytes, objectBody.length + deltaObjectBody.length)
  const chainDeltaBody = await getText(`${chainTarget.url}/v1/objects/${encodeURIComponent(deltaObjectId)}/body`)
  assert.equal(chainDeltaBody, deltaObjectBody)
  const chainMessages = await getJson(`${chainTarget.url}/v1/rooms/${encodeURIComponent(roomId)}/messages/latest?limit=10`)
  assert.deepEqual(
    chainMessages.messages.map((entry) => entry.body),
    [deltaMessageBody, messageBody],
  )

  children.push(startNode(encryptedTarget, "encrypted-target"))
  await waitForHealth(encryptedTarget.url)
  const encryptedTargetClient = new NextDbClient({ endpoint: encryptedTarget.url })
  const encryptedTargetBundles = await encryptedTargetClient.listExportBundles()
  assert.equal(encryptedTargetBundles.bundles.length, 1)
  assert.equal(encryptedTargetBundles.bundles[0].id, encryptedBundle.id)
  assert.equal(encryptedTargetBundles.bundles[0].encrypted, true)
  assert.equal(encryptedTargetBundles.bundles[0].schemaVersion, 2)

  const encryptedPreflightWithoutKey = await encryptedTargetClient.importBundlePreflight(encryptedBundle.id)
  assert.equal(encryptedPreflightWithoutKey.ok, false)
  assert.equal(encryptedPreflightWithoutKey.bundleEncrypted, true)
  assert(encryptedPreflightWithoutKey.problems.some((problem) => problem.includes("encryptionKey is required")))
  const encryptedPreflightWrongKey = await encryptedTargetClient.importBundlePreflight(encryptedBundle.id, {
    encryptionKey: "wrong-key",
  })
  assert.equal(encryptedPreflightWrongKey.ok, false)
  assert(encryptedPreflightWrongKey.problems.some((problem) => problem.includes("could not be decrypted")))

  const encryptedPreflight = await encryptedTargetClient.importBundlePreflight(encryptedBundle.id, { encryptionKey })
  assert.equal(encryptedPreflight.ok, true, JSON.stringify(encryptedPreflight.problems))
  assert.equal(encryptedPreflight.bundleEncrypted, true)
  assert.equal(encryptedPreflight.bundleWalRecords, 5)
  const encryptedRestore = await encryptedTargetClient.restoreImportBundle(encryptedBundle.id, { encryptionKey })
  assert.equal(encryptedRestore.restored, true)
  assert.equal(encryptedRestore.encrypted, true)
  assert.equal(encryptedRestore.schemaVersion, 2)
  assert.equal(encryptedRestore.currentLsn, 5)
  const encryptedRestoredObjectBody = await getText(`${encryptedTarget.url}/v1/objects/${encodeURIComponent(objectId)}/body`)
  assert.equal(encryptedRestoredObjectBody, objectBody)
  const encryptedRestoredProposals = await encryptedTargetClient.schemaProposals()
  assert(encryptedRestoredProposals.proposals.some((proposal) => proposal.id === abortedProposal.proposal.id))
  const encryptedRestoredTopologyLog = await encryptedTargetClient.topologyLog()
  assert.equal(encryptedRestoredTopologyLog.entries.length, 1)

  children.push(startNode(archiveTarget, "archive-target"))
  await waitForHealth(archiveTarget.url)
  const archiveTargetClient = new NextDbClient({ endpoint: archiveTarget.url })
  const materializedBundle = await archiveTargetClient.importBundleFromObject(archiveObjectId)
  assert.equal(materializedBundle.object.id, archiveObjectId)
  assert.equal(materializedBundle.bundle.id, encryptedBundle.id)
  assert.equal(materializedBundle.bundle.encrypted, true)
  assert.equal(materializedBundle.overwritten, false)
  assert.equal(materializedBundle.files, archivedBundle.files)
  assert.equal(materializedBundle.bytes, archivedBundle.bytes)

  const archivedObjectDuplicateImport = await fetch(
    `${archiveTarget.url}/v1/admin/import/bundles/from-object/${encodeURIComponent(archiveObjectId)}`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    },
  )
  assert.equal(archivedObjectDuplicateImport.status, 409)

  const archivePreflight = await archiveTargetClient.importBundlePreflight(encryptedBundle.id, { encryptionKey })
  assert.equal(archivePreflight.ok, true, JSON.stringify(archivePreflight.problems))
  assert.equal(archivePreflight.bundleEncrypted, true)
  assert.equal(archivePreflight.bundleWalRecords, 5)
  const archiveRestore = await archiveTargetClient.restoreImportBundle(encryptedBundle.id, { encryptionKey })
  assert.equal(archiveRestore.restored, true)
  assert.equal(archiveRestore.encrypted, true)
  assert.equal(archiveRestore.currentLsn, 5)
  const archiveRestoredObjectBody = await getText(`${archiveTarget.url}/v1/objects/${encodeURIComponent(objectId)}/body`)
  assert.equal(archiveRestoredObjectBody, objectBody)

  console.log("export import smoke ok")
} finally {
  await Promise.all(children.map((child) => stopNode(child)))
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode(node, name) {
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
    if (process.env.NEXTDB_EXPORT_IMPORT_SMOKE_LOGS === "1") {
      process.stdout.write(`[${name}] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_EXPORT_IMPORT_SMOKE_LOGS === "1") {
      process.stderr.write(`[${name}] ${chunk}`)
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

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`GET ${url} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
}

async function getText(url) {
  const response = await fetch(url)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`GET ${url} ${response.status}: ${text}`)
  }
  return text
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST ${url} ${response.status}: ${text}`)
  }
  return text ? JSON.parse(text) : undefined
}

async function putObject(baseUrl, objectId, body, clientMutationId) {
  const response = await fetch(
    `${baseUrl}/v1/objects?contentType=text/plain&objectId=${encodeURIComponent(objectId)}&clientMutationId=${encodeURIComponent(clientMutationId)}`,
    {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body,
    },
  )
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`PUT object ${objectId} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
}
