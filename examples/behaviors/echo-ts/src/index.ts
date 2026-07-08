import {
  activateRuntimeRecords,
  activateRuntimeRoom,
  broadcastRealtimeChannel,
  commandArray2,
  commandArray3,
  commandArray4,
  disconnectConnections,
  inputString,
  jsonString,
  object1,
  object2,
  output,
  publishUserEvent,
  publishUserVolatile,
  putObjectBase64,
  requestString,
  scheduleBehaviorReminder,
  sendMessage,
  updateRealtimePresence,
  updateRealtimeChannelState,
  upsertRecord,
} from "@nextdb/behavior-sdk/assembly"

export function handle(requestJson: string): string {
  const behavior = requestString(requestJson, "behavior")
  const mutation = requestString(requestJson, "mutation")
  const userId = requestString(requestJson, "userId")
  const roomId = inputString(requestJson, "roomId")
  const body = inputString(requestJson, "body")
  const channelState = inputString(requestJson, "channelState")
  const channelEvent = inputString(requestJson, "channelEvent")
  const channelPresence = inputString(requestJson, "channelPresence")
  const disconnectUser = inputString(requestJson, "disconnectUser")
  const disconnectSession = inputString(requestJson, "disconnectSession")
  const userEventUser = inputString(requestJson, "userEventUser")
  const userVolatileUser = inputString(requestJson, "userVolatileUser")
  const echoAudit = inputString(requestJson, "echoAudit")
  const runtimeActivate = inputString(requestJson, "runtimeActivate")
  const scheduleReminder = inputString(requestJson, "scheduleReminder")
  const scheduleReminderDueAtMs = inputString(requestJson, "scheduleReminderDueAtMs")
  let title = requestString(requestJson, "title")
  if (title.length == 0) {
    title = "Echo TS Room"
  }
  const message = "[" + behavior + ":" + mutation + "] " + body
  const objectId = "behavior-object-" + roomId
  const upsert = upsertRecord(
    "rooms",
    roomId,
    object2("id", jsonString(roomId), "title", jsonString(title)),
  )
  const objectPut = putObjectBase64("ZWNoby10cw==", "text/plain", objectId)
  const send = sendMessage(roomId, message)

  if (echoAudit.length > 0) {
    const sawTrace = requestJson.indexOf("\"auditTraces\"") >= 0 && requestJson.indexOf("\"recordUpserted\"") >= 0
    const sawReplay = requestJson.indexOf("\"auditReplays\"") >= 0 && requestJson.indexOf("\"exists\"") >= 0
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "auditSeen",
        jsonString(sawTrace && sawReplay ? "trace+replay" : "missing"),
      ),
    )
  }

  if (runtimeActivate.length > 0) {
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "runtimeActivate",
        jsonString(runtimeActivate),
      ),
      commandArray2(
        activateRuntimeRecords("rooms", roomId),
        activateRuntimeRoom(roomId, 2),
      ),
    )
  }

  if (scheduleReminder.length > 0) {
    let reminderDueAtMs: i64 = -1
    let reminderDelayMs: i64 = 10
    if (scheduleReminderDueAtMs.length > 0) {
      reminderDueAtMs = 4102444800000
      reminderDelayMs = -1
    }
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "scheduled",
        jsonString(scheduleReminder),
      ),
      commandArray4(
        upsert,
        objectPut,
        scheduleBehaviorReminder(
          "room",
          roomId,
          behavior,
          mutation,
          "echo-ts-reminder-" + roomId,
          reminderDueAtMs,
          reminderDelayMs,
          object2(
            "roomId",
            jsonString(roomId),
            "body",
            jsonString("scheduled " + body),
          ),
          userId,
        ),
        send,
      ),
    )
  }

  if (disconnectUser.length > 0 || disconnectSession.length > 0) {
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "userId",
        jsonString(userId),
      ),
      commandArray4(
        upsert,
        objectPut,
        disconnectConnections(
          disconnectUser,
          disconnectSession,
          "behavior requested disconnect",
        ),
        send,
      ),
    )
  }

  if (userEventUser.length > 0) {
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "userId",
        jsonString(userId),
      ),
      commandArray4(
        upsert,
        objectPut,
        publishUserEvent(
          userEventUser,
          "notification.created",
          object1("text", jsonString("behavior user event for " + roomId)),
        ),
        send,
      ),
    )
  }

  if (userVolatileUser.length > 0) {
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "userId",
        jsonString(userId),
      ),
      commandArray4(
        upsert,
        objectPut,
        publishUserVolatile(
          userVolatileUser,
          "presence.ping",
          object1("at", "1"),
        ),
        send,
      ),
    )
  }

  if (channelState.length > 0) {
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "userId",
        jsonString(userId),
      ),
      commandArray4(
        upsert,
        objectPut,
        updateRealtimeChannelState(
          roomId,
          object2("label", jsonString(channelState), "roomId", jsonString(roomId)),
          0,
        ),
        send,
      ),
    )
  }

  if (channelEvent.length > 0) {
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "userId",
        jsonString(userId),
      ),
      commandArray4(
        upsert,
        objectPut,
        broadcastRealtimeChannel(
          roomId,
          "behavior.channel.event",
          object2("label", jsonString(channelEvent), "roomId", jsonString(roomId)),
        ),
        send,
      ),
    )
  }

  if (channelPresence.length > 0) {
    return output(
      object2(
        "handledBy",
        jsonString("nextdb-echo-ts-behavior"),
        "userId",
        jsonString(userId),
      ),
      commandArray4(
        upsert,
        objectPut,
        updateRealtimePresence(
          roomId,
          object2("label", jsonString(channelPresence), "roomId", jsonString(roomId)),
        ),
        send,
      ),
    )
  }

  return output(
    object2(
      "handledBy",
      jsonString("nextdb-echo-ts-behavior"),
      "userId",
      jsonString(userId),
    ),
    commandArray3(
      upsert,
      objectPut,
      send,
    ),
  )
}
