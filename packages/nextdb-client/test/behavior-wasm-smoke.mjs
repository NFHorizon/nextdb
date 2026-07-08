import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const behaviorCli = resolve(root, "packages/nextdb-behavior-sdk/dist/cli.js")
const behaviorExample = resolve(root, "examples/behaviors/echo-ts")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-behavior-wasm-"))
const dataDir = join(tempRoot, "data")
const behaviorOut = join(dataDir, "behaviors", "echo-ts")
const node = {
  url: "http://127.0.0.1:3398",
  addr: "127.0.0.1:3398",
  dataDir,
}
const children = []
let db

try {
  await mkdir(behaviorOut, { recursive: true })
  await run(process.execPath, [
    behaviorCli,
    "compile",
    "--manifest",
    join(behaviorExample, "nextdb.behavior.json"),
    "--entry",
    join(behaviorExample, "src/index.ts"),
    "--out",
    behaviorOut,
  ])

  children.push(startNode(node))
  await waitForHealth(node.url)
  db = new NextDbClient({ endpoint: node.url, userId: "alice" })

  const behaviors = await db.listBehaviors()
  assert(behaviors.some((behavior) => behavior.name === "echo-ts" && behavior.mutations.includes("echo.send")))
  const echoTsBehavior = behaviors.find((behavior) => behavior.name === "echo-ts")
  assert.equal(echoTsBehavior?.inputs?.["echo.send"]?.type?.kind, "object")
  assert.deepEqual(echoTsBehavior?.reads, [])
  assert.deepEqual(echoTsBehavior?.recordScopes, {
    read: ["rooms"],
    write: ["rooms"],
    nestedRead: ["rooms.messages"],
    nestedWrite: ["rooms.messages"],
  })
  assert.deepEqual(echoTsBehavior?.objectScopes, {
    write: ["behavior-object-*"],
  })
  assert.deepEqual(echoTsBehavior?.realtimeScopes, {
    write: ["behavior-state-*", "behavior-broadcast-*", "behavior-presence-*"],
  })
  assert.deepEqual(echoTsBehavior?.connectionScopes, {
    read: ["alice"],
    write: ["behavior-disconnect-*"],
  })
  assert.deepEqual(echoTsBehavior?.userScopes, {
    publish: ["behavior-inbox-*"],
  })
  assert.deepEqual(echoTsBehavior?.eventScopes, {
    publish: ["notification.created", "presence.ping"],
    realtimeBroadcast: ["behavior.channel.*"],
  })
	  assert.deepEqual(echoTsBehavior?.commands, [
	    "upsertRecord",
	    "putObject",
    "publishUserEvent",
    "publishUserVolatile",
    "broadcastRealtimeChannel",
    "updateRealtimeChannelState",
    "updateRealtimePresence",
    "disconnectConnections",
    "activateRuntimeRecords",
    "activateRuntimeRoom",
    "scheduleActorReminder",
    "sendMessage",
  ])

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(
      {
        name: "echo-ts",
        version: "0.1.0",
        modulePath: "echo-ts.wasm",
        mutations: ["echo.send"],
        inputs: {
          "echo.send": {
            type: {
              kind: "object",
              fields: {
                roomId: { type: { kind: "id", entity: "Room" } },
                body: { type: { kind: "int64" } },
              },
            },
          },
        },
        maxFuel: 1_000_000,
      },
      null,
      2,
    )}\n`,
  )
  await assert.rejects(
    () => db.reloadBehaviors(),
    (error) => error?.status === 500 && error.message.includes("nextdb.behavior.json"),
  )
  const stillLoaded = await db.listBehaviors()
  assert(stillLoaded.some((behavior) => behavior.name === "echo-ts" && behavior.mutations.includes("echo.send")))

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest({ commands: ["sendMessage"] }), null, 2)}\n`,
  )
  const restrictedReload = await db.reloadBehaviors()
  assert.equal(restrictedReload.loaded, 1)
  const restrictedRoomId = `behavior-restricted-${Date.now()}`
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: restrictedRoomId,
        body: "blocked by command policy",
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to return host command"),
  )
  const restrictedHealth = await db.health()
  const restrictedRuntime = restrictedHealth.behaviorRuntime
  const restrictedCounters = restrictedRuntime.behaviors.find((behavior) => behavior.name === "echo-ts")?.counters
  assert(restrictedCounters)
  assert.equal(restrictedRuntime.counters.invocations, 1)
  assert.equal(restrictedCounters.invocations, 1)
  assert.equal(restrictedCounters.commandRejections, 1)
  assert.equal(restrictedCounters.instancesCreated, 1)
  assert.equal(restrictedCounters.instancesDiscarded, 1)
  await assert.rejects(
    () => db.table("rooms").get(restrictedRoomId),
    (error) => error?.status === 404,
  )

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest({
      recordScopes: {
        write: ["users"],
        nestedWrite: ["rooms.messages"],
      },
    }), null, 2)}\n`,
  )
  const scopedReload = await db.reloadBehaviors()
  assert.equal(scopedReload.loaded, 1)
  const scopedRoomId = `behavior-scoped-${Date.now()}`
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: scopedRoomId,
        body: "blocked by record scope",
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to write table 'rooms'"),
  )
  await assert.rejects(
    () => db.table("rooms").get(scopedRoomId),
    (error) => error?.status === 404,
  )

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest({
      objectScopes: {
        write: ["other-object-*"],
      },
    }), null, 2)}\n`,
  )
  const objectScopedReload = await db.reloadBehaviors()
  assert.equal(objectScopedReload.loaded, 1)
  const objectScopedRoomId = `behavior-object-scoped-${Date.now()}`
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: objectScopedRoomId,
        body: "blocked by object scope",
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to write object"),
  )
  await assert.rejects(
    () => db.table("rooms").get(objectScopedRoomId),
    (error) => error?.status === 404,
  )

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest({
      realtimeScopes: {
        write: ["other-channel-*"],
      },
    }), null, 2)}\n`,
  )
  const realtimeScopedReload = await db.reloadBehaviors()
  assert.equal(realtimeScopedReload.loaded, 1)
  const blockedRealtimeRoomId = `behavior-state-blocked-${Date.now()}`
  await db.realtimeChannel(blockedRealtimeRoomId).join({ role: "blocked-behavior" })
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: blockedRealtimeRoomId,
        body: "blocked by realtime scope",
        channelState: "blocked",
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to write realtime channel"),
  )
  await assert.rejects(
    () => db.table("rooms").get(blockedRealtimeRoomId),
    (error) => error?.status === 404,
  )

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest({
      eventScopes: {
        realtimeBroadcast: ["other.channel.*"],
      },
    }), null, 2)}\n`,
  )
  const eventScopedReload = await db.reloadBehaviors()
  assert.equal(eventScopedReload.loaded, 1)
  const blockedEventRoomId = `behavior-broadcast-${Date.now()}`
  await db.realtimeChannel(blockedEventRoomId).join({ role: "blocked-event-behavior" })
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: blockedEventRoomId,
        body: "blocked by event scope",
        channelEvent: "blocked",
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to broadcast realtime event"),
  )
  await assert.rejects(
    () => db.table("rooms").get(blockedEventRoomId),
    (error) => error?.status === 404,
  )

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest({
      connectionScopes: {
        read: ["alice"],
        write: ["other-user-*"],
      },
    }), null, 2)}\n`,
  )
  const connectionScopedReload = await db.reloadBehaviors()
  assert.equal(connectionScopedReload.loaded, 1)
  const blockedConnectionRoomId = `behavior-disconnect-blocked-${Date.now()}`
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: blockedConnectionRoomId,
        body: "blocked by connection scope",
        disconnectUser: `behavior-disconnect-${Date.now()}`,
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to write connection sessions for user"),
  )
  await assert.rejects(
    () => db.table("rooms").get(blockedConnectionRoomId),
    (error) => error?.status === 404,
  )

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest({
      userScopes: {
        publish: ["other-inbox-*"],
      },
    }), null, 2)}\n`,
  )
  const userScopedReload = await db.reloadBehaviors()
  assert.equal(userScopedReload.loaded, 1)
  const blockedUserEventRoomId = `behavior-user-event-blocked-${Date.now()}`
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: blockedUserEventRoomId,
        body: "blocked by user scope",
        userEventUser: `behavior-inbox-${Date.now()}`,
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to publish user event to user"),
  )
  await assert.rejects(
    () => db.table("rooms").get(blockedUserEventRoomId),
    (error) => error?.status === 404,
  )

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest(), null, 2)}\n`,
  )
	  const restoredReload = await db.reloadBehaviors()
	  assert.equal(restoredReload.loaded, 1)

	  const scheduleRoomId = `behavior-reminder-${Date.now()}`
	  const scheduleMutationId = `${scheduleRoomId}-invoke`
	  const scheduleInput = {
	    roomId: scheduleRoomId,
	    body: "scheduled reminder",
	    scheduleReminder: "1",
	    scheduleReminderDueAtMs: "1",
	  }
	  const scheduled = await db.invokeBehavior({
	    behavior: "echo-ts",
	    mutation: "echo.send",
	    clientMutationId: scheduleMutationId,
	    input: scheduleInput,
	  })
	  assert.deepEqual(scheduled.committed.map((entry) => entry.type), [
	    "recordUpserted",
	    "objectCommitted",
	    "actorReminderScheduled",
	    "messageCreated",
	  ])
	  const scheduledReminder = scheduled.committed.find((entry) => entry.type === "actorReminderScheduled")
	  assert.equal(scheduledReminder?.response?.reminder?.reminderId, `echo-ts-reminder-${scheduleRoomId}`)
	  assert.equal(scheduledReminder?.response?.reminder?.dueAtMs, 4102444800000)
	  const scheduledRetry = await db.invokeBehavior({
	    behavior: "echo-ts",
	    mutation: "echo.send",
	    clientMutationId: scheduleMutationId,
	    input: scheduleInput,
	  })
	  const scheduledRetryReminder = scheduledRetry.committed.find((entry) => entry.type === "actorReminderScheduled")
	  assert.equal(scheduledRetryReminder?.response?.lsn, scheduledReminder?.response?.lsn)
	  const reminderAudit = await getJson(`${node.url}/v1/audit/wal?afterLsn=0&limit=500`)
	  const scheduleFacts = reminderAudit.records.filter((record) =>
	    record.payload?.type === "actorReminderScheduled" &&
	    record.payload?.reminder?.reminderId === `echo-ts-reminder-${scheduleRoomId}`
	  )
	  assert.equal(scheduleFacts.length, 1)
	  await assert.rejects(
	    () => db.invokeBehavior({
	      behavior: "echo-ts",
	      mutation: "echo.send",
	      clientMutationId: `${scheduleRoomId}-delay-invoke`,
	      input: {
	        roomId: `${scheduleRoomId}-delay`,
	        body: "relative reminder",
	        scheduleReminder: "1",
	      },
	    }),
	    (error) => error?.status === 400 && error.message.includes("requires dueAtMs"),
	  )

	  const activationRoomId = `behavior-runtime-activate-${Date.now()}`
	  await db.table("rooms").upsert(activationRoomId, {
    id: activationRoomId,
    title: "Behavior Runtime Activate Room",
  })
  await db.room(activationRoomId).messages.send("seed activation window", {
    clientMutationId: `${activationRoomId}-seed-message`,
  })
  const evictedActivationRoom = await db.evictRuntimeRoom({ roomId: activationRoomId })
  assert.equal(evictedActivationRoom.evicted, true)
  const activationBefore = await db.runtimeActivationStatus()
  assert(!activationBefore.rooms.some((room) => room.roomId === activationRoomId))
  const activationResponse = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    input: {
      roomId: activationRoomId,
      body: "runtime activation",
      runtimeActivate: "1",
    },
  })
  assert.deepEqual(activationResponse.committed.map((entry) => entry.type), [
    "runtimeRecordsActivated",
    "runtimeRoomActivated",
  ])
  assert.equal(activationResponse.committed[0]?.response?.table, "rooms")
  assert.equal(activationResponse.committed[0]?.response?.found, 1)
  assert.equal(activationResponse.committed[1]?.response?.roomId, activationRoomId)
  assert.equal(activationResponse.committed[1]?.response?.found, 1)
  const activationAfter = await db.runtimeActivationStatus()
  assert(activationAfter.rooms.some((room) => room.roomId === activationRoomId))
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      clientMutationId: `${activationRoomId}-activation-retry`,
      input: {
        roomId: activationRoomId,
        body: "runtime activation retry",
        runtimeActivate: "1",
      },
    }),
    (error) =>
      error?.status === 400 &&
      error.message.includes("clientMutationId requires replay-safe durable host commands"),
  )

  const auditReadRoomId = `behavior-audit-read-${Date.now()}`
  const auditReadRecord = await db.table("rooms").upsert(auditReadRoomId, {
    id: auditReadRoomId,
    title: "Behavior Audit Read Room",
  })
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: auditReadRoomId,
        body: "audit read blocked",
        echoAudit: "1",
      },
      read: {
        auditTraces: [{ kind: "record", table: "rooms", id: auditReadRoomId, limit: 5 }],
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to use read plan"),
  )

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest({
      reads: ["auditTraces", "auditReplays"],
      recordScopes: {
        read: ["rooms"],
        write: ["rooms"],
        nestedWrite: ["rooms.messages"],
      },
    }), null, 2)}\n`,
  )
  const auditReadReload = await db.reloadBehaviors()
  assert.equal(auditReadReload.loaded, 1)
  const auditReadResponse = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    input: {
      roomId: auditReadRoomId,
      body: "audit read allowed",
      echoAudit: "1",
    },
    read: {
      auditTraces: [{ kind: "record", table: "rooms", id: auditReadRoomId, limit: 5 }],
      auditReplays: [{ kind: "record", table: "rooms", id: auditReadRoomId, atLsn: auditReadRecord.lsn }],
    },
  })
  assert.equal(auditReadResponse.output.result.auditSeen, "trace+replay")
  assert.deepEqual(auditReadResponse.committed, [])

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoTsManifest(), null, 2)}\n`,
  )
  const restoredAfterAuditReadReload = await db.reloadBehaviors()
  assert.equal(restoredAfterAuditReadReload.loaded, 1)

  const inboxUserId = `behavior-inbox-${Date.now()}`
  const inboxRoomId = `behavior-user-event-${Date.now()}`
  const inboxResponse = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    clientMutationId: `${inboxRoomId}-invoke`,
    input: {
      roomId: inboxRoomId,
      body: "user event command",
      title: "Behavior User Event Room",
      userEventUser: inboxUserId,
    },
  })
  assert.deepEqual(inboxResponse.committed.map((entry) => entry.type), [
    "recordUpserted",
    "objectCommitted",
    "userEventPublished",
    "messageCreated",
  ])
  const inboxCommit = inboxResponse.committed.find((entry) => entry.type === "userEventPublished")
  assert.equal(inboxCommit?.event?.userId, inboxUserId)
  assert.equal(inboxCommit?.event?.name, "notification.created")
  assert.equal(inboxCommit?.event?.payload?.text, `behavior user event for ${inboxRoomId}`)
  const inboxEvents = await db.listUserEvents(inboxUserId, { limit: 5 })
  assert.equal(inboxEvents[0]?.id, inboxCommit.event.id)

  const volatileUserId = `behavior-inbox-volatile-${Date.now()}`
  const volatileClient = new NextDbClient({
    endpoint: node.url,
    userId: volatileUserId,
    sessionId: `${volatileUserId}-session`,
  })
  const volatileEvents = []
  const stopVolatileUserEvents = volatileClient.onUserEvent((event) => volatileEvents.push(event), { catchUp: false })
  await waitFor(
    async () => (await db.listConnections({ userId: volatileUserId })).sessions.some((session) => session.subscribedUserEvents),
    "behavior volatile user event subscription",
  )
  const volatileRoomId = `behavior-user-volatile-${Date.now()}`
  const volatileResponse = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    input: {
      roomId: volatileRoomId,
      body: "volatile user command",
      title: "Behavior User Volatile Room",
      userVolatileUser: volatileUserId,
    },
  })
  assert.deepEqual(volatileResponse.committed.map((entry) => entry.type), [
    "recordUpserted",
    "objectCommitted",
    "volatileUserPublished",
    "messageCreated",
  ])
  const volatileCommit = volatileResponse.committed.find((entry) => entry.type === "volatileUserPublished")
  assert.equal(volatileCommit?.userId, volatileUserId)
  assert.equal(volatileCommit?.name, "presence.ping")
  assert.equal(volatileCommit?.delivered, 1)
  await waitFor(
    () => volatileEvents.some((event) => event.type === "volatileUserEvent" && event.name === "presence.ping" && event.payload?.at === 1),
    "behavior volatile user event delivery",
  )
  stopVolatileUserEvents()
  volatileClient.close()

  const disconnectTargetUserId = `behavior-disconnect-${Date.now()}`
  const disconnectTarget = new NextDbClient({
    endpoint: node.url,
    userId: disconnectTargetUserId,
    sessionId: `${disconnectTargetUserId}-session`,
    connectionMetadata: { role: "behavior-disconnect-target" },
  })
  await disconnectTarget.realtimeChannel(`behavior-disconnect-channel-${Date.now()}`).join({ role: "target" })
  await waitFor(
    async () => (await db.listConnections({ userId: disconnectTargetUserId })).total === 1,
    "behavior disconnect target connection",
  )
  const disconnectRoomId = `behavior-disconnect-${Date.now()}`
  const disconnectResponse = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    input: {
      roomId: disconnectRoomId,
      body: "disconnect command",
      title: "Behavior Disconnect Room",
      disconnectUser: disconnectTargetUserId,
    },
  })
  const disconnectCommit = disconnectResponse.committed.find((entry) => entry.type === "connectionsDisconnectRequested")
  assert.equal(disconnectCommit?.targeted, 1)
  assert.deepEqual(disconnectCommit?.targetedSessionIds, [`${disconnectTargetUserId}-session`])
  await waitFor(
    async () => (await db.listConnections({ userId: disconnectTargetUserId })).total === 0,
    "behavior disconnect target removed",
  )
  disconnectTarget.close()

  const stateRoomId = `behavior-state-${Date.now()}`
  await db.realtimeChannel(stateRoomId).join({ role: "behavior" })
  const stateResponse = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    input: {
      roomId: stateRoomId,
      body: "state command",
      title: "Behavior State Room",
      channelState: "ready",
    },
  })
  assert.deepEqual(stateResponse.committed.map((entry) => entry.type), [
    "recordUpserted",
    "objectCommitted",
    "realtimeChannelStateUpdated",
    "messageCreated",
  ])
  const behaviorState = await db.realtimeChannel(stateRoomId).state()
  assert.equal(behaviorState.state.version, 1)
  assert.equal(behaviorState.state.state.label, "ready")
  assert.equal(behaviorState.state.state.roomId, stateRoomId)

  const broadcastRoomId = `behavior-broadcast-${Date.now()}`
  const broadcastEvents = []
  const broadcastChannel = db.realtimeChannel(broadcastRoomId)
  const stopBroadcastEvents = broadcastChannel.onEvent((event) => broadcastEvents.push(event))
  await broadcastChannel.join({ role: "behavior-listener" })
  const broadcastResponse = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    input: {
      roomId: broadcastRoomId,
      body: "broadcast command",
      title: "Behavior Broadcast Room",
      channelEvent: "tick",
    },
  })
  assert.deepEqual(broadcastResponse.committed.map((entry) => entry.type), [
    "recordUpserted",
    "objectCommitted",
    "realtimeChannelBroadcasted",
    "messageCreated",
  ])
  await waitFor(
    () => broadcastEvents.some((event) => event.kind === "behavior.channel.event" && event.payload?.label === "tick"),
    "behavior realtime broadcast event",
  )
  stopBroadcastEvents()

  const presenceRoomId = `behavior-presence-${Date.now()}`
  const presenceEvents = []
  const presenceChannel = db.realtimeChannel(presenceRoomId)
  const stopPresenceEvents = presenceChannel.onMemberUpdated((event) => presenceEvents.push(event))
  await presenceChannel.join({ role: "behavior-presence-listener", label: "before" })
  const presenceResponse = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    input: {
      roomId: presenceRoomId,
      body: "presence command",
      title: "Behavior Presence Room",
      channelPresence: "ready",
    },
  })
  assert.deepEqual(presenceResponse.committed.map((entry) => entry.type), [
    "recordUpserted",
    "objectCommitted",
    "realtimePresenceUpdated",
    "messageCreated",
  ])
  const presenceMembers = await presenceChannel.members()
  assert.equal(presenceMembers.members.length, 1)
  assert.equal(presenceMembers.members[0].metadata.label, "ready")
  assert.equal(presenceMembers.members[0].metadata.roomId, presenceRoomId)
  await waitFor(
    () => presenceEvents.some((event) => event.member.metadata?.label === "ready"),
    "behavior realtime presence update",
  )
  stopPresenceEvents()

  const incompatibleSchema = await db.getSchema()
  incompatibleSchema.version += 1
  incompatibleSchema.behaviors["echo-ts"].mutations["echo.send"].type.fields.title = {
    type: { kind: "string" },
  }
  await assert.rejects(
    () => db.applySchema(incompatibleSchema, { expectedVersion: incompatibleSchema.version - 1 }),
    (error) =>
      error?.status === 400 &&
      error.message.includes("incompatible schema migration") &&
      error.message.includes("behaviors.echo-ts.mutations.echo.send.fields.title required field cannot be added"),
  )
  const afterRejectedSchema = await db.getSchema()
  assert.equal(afterRejectedSchema.version, incompatibleSchema.version - 1)

  const schema = await db.getSchema()
  schema.version += 1
  schema.behaviors["echo-ts"].mutations["echo.send"].type.fields.attachment = {
    type: { kind: "objectRef", object: "Object" },
    optional: true,
  }
  const applied = await db.applySchema(schema, { expectedVersion: schema.version - 1 })
  assert.equal(applied.applied, true)

  const inputObjectId = `behavior-input-object-${Date.now()}`
  const inputObject = await db.putObject("behavior input object", {
    contentType: "text/plain",
    objectId: inputObjectId,
    clientMutationId: `${inputObjectId}-put`,
  })
  const missingInputObject = {
    ...inputObject,
    id: `${inputObjectId}-missing`,
    path: `objects/${inputObjectId}-missing`,
  }

  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      clientMutationId: `${inputObjectId}-missing-invoke`,
      input: {
        roomId: `${inputObjectId}-missing-room`,
        body: "missing object",
        attachment: missingInputObject,
      },
    }),
    (error) => error?.status === 404 && error.message.includes("object ref not found"),
  )
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      clientMutationId: `${inputObjectId}-mismatch-invoke`,
      input: {
        roomId: `${inputObjectId}-mismatch-room`,
        body: "mismatched object",
        attachment: { ...inputObject, sha256: "not-the-object-sha" },
      },
    }),
    (error) => error?.status === 400 && error.message.includes("object ref metadata does not match"),
  )

  const roomId = `behavior-room-${Date.now()}`
  const response = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    clientMutationId: `${roomId}-invoke`,
    input: {
      roomId,
      body: "hello wasm",
      title: "Behavior Wasm Room",
      attachment: inputObject,
    },
  })

  assert.equal(response.output.result.handledBy, "nextdb-echo-ts-behavior")
  assert.equal(response.output.result.userId, "alice")
  assert.deepEqual(response.committed.map((entry) => entry.type), [
    "recordUpserted",
    "objectCommitted",
    "messageCreated",
  ])

  const room = await db.table("rooms").get(roomId)
  assert.equal(room?.value.title, "Behavior Wasm Room")

  const objectCommit = response.committed.find((entry) => entry.type === "objectCommitted")
  assert(objectCommit?.object?.id)
  const objectBody = await db.getObjectBody(objectCommit.object.id)
  assert.equal(await objectBody.text(), "echo-ts")

  const messages = await db.room(roomId).messages.latest({ limit: 5 })
  assert.equal(messages.messages.length, 1)
  assert.equal(messages.messages[0].body, "[echo-ts:echo.send] hello wasm")
  assert.equal(messages.messages[0].senderId, "alice")

  const retry = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    clientMutationId: `${roomId}-invoke`,
    input: {
      roomId,
      body: "hello wasm changed",
      title: "Behavior Wasm Room Changed",
    },
  })
  assert.deepEqual(retry.committed.map((entry) => entry.type), [
    "recordUpserted",
    "objectCommitted",
    "messageCreated",
  ])

  const afterRetryMessages = await db.room(roomId).messages.latest({ limit: 5 })
  assert.equal(afterRetryMessages.messages.length, 1)
  assert.equal(afterRetryMessages.messages[0].body, "[echo-ts:echo.send] hello wasm")

  const audit = await getJson(`${node.url}/v1/audit/wal?clientMutationId=${encodeURIComponent(`${roomId}-invoke:002:sendMessage`)}`)
  assert.equal(audit.records.length, 1)

  const finalHealth = await db.health()
  const finalRuntime = finalHealth.behaviorRuntime
  const finalCounters = finalRuntime.behaviors.find((behavior) => behavior.name === "echo-ts")?.counters
  assert(finalCounters)
  assert(finalRuntime.counters.invocations >= 7)
  assert(finalCounters.successes >= 7)
  assert(finalCounters.instancesCreated >= 1)
  assert(finalCounters.instancesReused >= 1)
  assert(finalCounters.instancesReturned >= finalCounters.successes)
  const metrics = await db.metrics()
  assert.match(metrics, /nextdb_behavior_invocations_total \d+/)
  assert.match(metrics, /nextdb_behavior_invocations_total\{behavior="echo-ts"\} \d+/)
  assert.match(metrics, /nextdb_behavior_successes_total\{behavior="echo-ts"\} \d+/)
  assert.match(metrics, /nextdb_behavior_command_rejections_total \d+/)

  console.log("behavior wasm smoke ok")
} finally {
  db?.close()
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
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_BEHAVIOR_WASM_SMOKE_LOGS === "1") {
      process.stdout.write(`[behavior] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_BEHAVIOR_WASM_SMOKE_LOGS === "1") {
      process.stderr.write(`[behavior] ${chunk}`)
    }
  })
  return child
}

function echoTsManifest({
  reads = [],
	  commands = ["upsertRecord", "putObject", "publishUserEvent", "publishUserVolatile", "broadcastRealtimeChannel", "updateRealtimeChannelState", "updateRealtimePresence", "disconnectConnections", "activateRuntimeRecords", "activateRuntimeRoom", "scheduleActorReminder", "sendMessage"],
  recordScopes = {
    read: ["rooms"],
    write: ["rooms"],
    nestedRead: ["rooms.messages"],
    nestedWrite: ["rooms.messages"],
  },
  objectScopes = {
    write: ["behavior-object-*"],
  },
  realtimeScopes = {
    write: ["behavior-state-*", "behavior-broadcast-*", "behavior-presence-*"],
  },
  connectionScopes = {
    read: ["alice"],
    write: ["behavior-disconnect-*"],
  },
  userScopes = {
    publish: ["behavior-inbox-*"],
  },
  eventScopes = {
    publish: ["notification.created", "presence.ping"],
    realtimeBroadcast: ["behavior.channel.*"],
  },
} = {}) {
  return {
    name: "echo-ts",
    version: "0.1.0",
    modulePath: "echo-ts.wasm",
    abiEncoding: "postcard",
    mutations: ["echo.send"],
    inputs: {
      "echo.send": {
        type: {
          kind: "object",
          fields: {
	            roomId: { type: { kind: "id", entity: "Room" } },
	            body: { type: { kind: "string" } },
	            scheduleReminder: { type: { kind: "string" }, optional: true },
	            scheduleReminderDueAtMs: { type: { kind: "string" }, optional: true },
	          },
	        },
	      },
    },
    reads,
    recordScopes,
    objectScopes,
    realtimeScopes,
    connectionScopes,
    userScopes,
    eventScopes,
    commands,
    maxFuel: 10_000_000,
  }
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

async function run(command, args) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: root,
      stdio: process.env.NEXTDB_BEHAVIOR_WASM_SMOKE_LOGS === "1" ? "inherit" : "pipe",
    })
    let stderr = ""
    child.stderr?.on("data", (chunk) => {
      stderr += chunk
    })
    child.on("error", reject)
    child.on("exit", (code) => {
      if (code === 0) {
        resolve()
      } else {
        reject(new Error(`${command} ${args.join(" ")} failed with ${code}: ${stderr}`))
      }
    })
  })
}
