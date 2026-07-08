export function output(resultJson: string, commandsJson: string = "[]"): string {
  return "{\"commands\":" + commandsJson + ",\"result\":" + resultJson + "}";
}

export function commandArray(commandJson: string): string {
  return "[" + commandJson + "]";
}

export function commandArray2(firstCommandJson: string, secondCommandJson: string): string {
  return "[" + firstCommandJson + "," + secondCommandJson + "]";
}

export function commandArray3(
  firstCommandJson: string,
  secondCommandJson: string,
  thirdCommandJson: string,
): string {
  return "[" + firstCommandJson + "," + secondCommandJson + "," + thirdCommandJson + "]";
}

export function commandArray4(
  firstCommandJson: string,
  secondCommandJson: string,
  thirdCommandJson: string,
  fourthCommandJson: string,
): string {
  return "[" + firstCommandJson + "," + secondCommandJson + "," + thirdCommandJson + "," + fourthCommandJson + "]";
}

export function commandArray5(
  firstCommandJson: string,
  secondCommandJson: string,
  thirdCommandJson: string,
  fourthCommandJson: string,
  fifthCommandJson: string,
): string {
  return "[" + firstCommandJson + "," + secondCommandJson + "," + thirdCommandJson + "," + fourthCommandJson + "," + fifthCommandJson + "]";
}

export function sendMessage(
  roomId: string,
  body: string,
  durability: string = "strict",
  attachmentsJson: string = "[]",
): string {
  return (
    "{\"type\":\"sendMessage\",\"roomId\":" +
    jsonString(roomId) +
    ",\"body\":" +
    jsonString(body) +
    ",\"attachments\":" +
    attachmentsJson +
    ",\"durability\":" +
    jsonString(durability) +
    "}"
  );
}

export function publishVolatile(roomId: string, name: string, payloadJson: string = "{}"): string {
  return (
    "{\"type\":\"publishVolatile\",\"roomId\":" +
    jsonString(roomId) +
    ",\"name\":" +
    jsonString(name) +
    ",\"payload\":" +
    payloadJson +
    "}"
  );
}

export function publishUserVolatile(
  userId: string,
  name: string,
  payloadJson: string = "{}",
): string {
  return (
    "{\"type\":\"publishUserVolatile\",\"userId\":" +
    jsonString(userId) +
    ",\"name\":" +
    jsonString(name) +
    ",\"payload\":" +
    payloadJson +
    "}"
  );
}

export function publishUserEvent(
  userId: string,
  name: string,
  payloadJson: string = "{}",
  durability: string = "strict",
  clientMutationId: string = "",
): string {
  let out =
    "{\"type\":\"publishUserEvent\",\"userId\":" +
    jsonString(userId) +
    ",\"name\":" +
    jsonString(name) +
    ",\"payload\":" +
    payloadJson +
    ",\"durability\":" +
    jsonString(durability);
  if (clientMutationId.length > 0) {
    out += ",\"clientMutationId\":" + jsonString(clientMutationId);
  }
  return out + "}";
}

export function putObjectBase64(
  bodyBase64: string,
  contentType: string = "application/octet-stream",
  objectId: string = "",
  clientMutationId: string = "",
): string {
  let out =
    "{\"type\":\"putObject\",\"bodyBase64\":" +
    jsonString(bodyBase64) +
    ",\"contentType\":" +
    jsonString(contentType);
  if (objectId.length > 0) {
    out += ",\"objectId\":" + jsonString(objectId);
  }
  if (clientMutationId.length > 0) {
    out += ",\"clientMutationId\":" + jsonString(clientMutationId);
  }
  return out + "}";
}

export function deleteObject(
  objectId: string,
  force: bool = false,
  clientMutationId: string = "",
): string {
  let out = "{\"type\":\"deleteObject\",\"objectId\":" + jsonString(objectId);
  if (force) {
    out += ",\"force\":true";
  }
  if (clientMutationId.length > 0) {
    out += ",\"clientMutationId\":" + jsonString(clientMutationId);
  }
  return out + "}";
}

export function upsertRecord(
  table: string,
  key: string,
  valueJson: string,
  durability: string = "strict",
  expectedLsn: i64 = -1,
): string {
  let out =
    "{\"type\":\"upsertRecord\",\"table\":" +
    jsonString(table) +
    ",\"key\":" +
    jsonString(key) +
    ",\"value\":" +
    valueJson +
    ",\"durability\":" +
    jsonString(durability);
  if (expectedLsn >= 0) {
    out += ",\"expectedLsn\":" + expectedLsn.toString();
  }
  return out + "}";
}

export function deleteRecord(
  table: string,
  key: string,
  durability: string = "strict",
  expectedLsn: i64 = -1,
): string {
  let out =
    "{\"type\":\"deleteRecord\",\"table\":" +
    jsonString(table) +
    ",\"key\":" +
    jsonString(key) +
    ",\"durability\":" +
    jsonString(durability);
  if (expectedLsn >= 0) {
    out += ",\"expectedLsn\":" + expectedLsn.toString();
  }
  return out + "}";
}

export function recordTransaction(operationsJson: string, durability: string = "strict"): string {
  return (
    "{\"type\":\"recordTransaction\",\"operations\":" +
    operationsJson +
    ",\"durability\":" +
    jsonString(durability) +
    "}"
  );
}

export function updateRealtimeChannelState(
  channelId: string,
  stateJson: string,
  expectedVersion: i64 = -1,
): string {
  let out =
    "{\"type\":\"updateRealtimeChannelState\",\"channelId\":" +
    jsonString(channelId) +
    ",\"state\":" +
    stateJson;
  if (expectedVersion >= 0) {
    out += ",\"expectedVersion\":" + expectedVersion.toString();
  }
  return out + "}";
}

export function updateRealtimePresence(
  channelId: string,
  metadataJson: string,
  sessionId: string = "",
): string {
  let out =
    "{\"type\":\"updateRealtimePresence\",\"channelId\":" +
    jsonString(channelId) +
    ",\"metadata\":" +
    metadataJson;
  if (sessionId.length > 0) {
    out += ",\"sessionId\":" + jsonString(sessionId);
  }
  return out + "}";
}

export function broadcastRealtimeChannel(
  channelId: string,
  kind: string,
  payloadJson: string = "{}",
  includeSelf: bool = true,
): string {
  let out =
    "{\"type\":\"broadcastRealtimeChannel\",\"channelId\":" +
    jsonString(channelId) +
    ",\"kind\":" +
    jsonString(kind) +
    ",\"payload\":" +
    payloadJson;
  if (!includeSelf) {
    out += ",\"includeSelf\":false";
  }
  return out + "}";
}

export function disconnectConnections(
  userId: string = "",
  sessionId: string = "",
  reason: string = "",
): string {
  let out = "{\"type\":\"disconnectConnections\"";
  if (userId.length > 0) {
    out += ",\"userId\":" + jsonString(userId);
  }
  if (sessionId.length > 0) {
    out += ",\"sessionId\":" + jsonString(sessionId);
  }
  if (reason.length > 0) {
    out += ",\"reason\":" + jsonString(reason);
  }
  return out + "}";
}

export function activateRuntimeRecords(
  table: string,
  key: string = "",
  afterKey: string = "",
  limit: i64 = -1,
  indexName: string = "",
  valueJson: string = "",
  valuesJson: string = "",
  lowerJson: string = "",
  upperJson: string = "",
  lowerValuesJson: string = "",
  upperValuesJson: string = "",
  afterCursor: string = "",
  predicateJson: string = "",
  parentKey: string = "",
  nested: string = "",
  order: string = "",
): string {
  let out = "{\"type\":\"activateRuntimeRecords\",\"table\":" + jsonString(table);
  if (parentKey.length > 0) {
    out += ",\"parentKey\":" + jsonString(parentKey);
  }
  if (nested.length > 0) {
    out += ",\"nested\":" + jsonString(nested);
  }
  if (key.length > 0) {
    out += ",\"key\":" + jsonString(key);
  }
  if (indexName.length > 0) {
    out += ",\"indexName\":" + jsonString(indexName);
  }
  if (valueJson.length > 0) {
    out += ",\"value\":" + valueJson;
  }
  if (valuesJson.length > 0) {
    out += ",\"values\":" + valuesJson;
  }
  if (lowerJson.length > 0) {
    out += ",\"lower\":" + lowerJson;
  }
  if (upperJson.length > 0) {
    out += ",\"upper\":" + upperJson;
  }
  if (lowerValuesJson.length > 0) {
    out += ",\"lowerValues\":" + lowerValuesJson;
  }
  if (upperValuesJson.length > 0) {
    out += ",\"upperValues\":" + upperValuesJson;
  }
  if (afterKey.length > 0) {
    out += ",\"afterKey\":" + jsonString(afterKey);
  }
  if (afterCursor.length > 0) {
    out += ",\"afterCursor\":" + jsonString(afterCursor);
  }
  if (order.length > 0) {
    out += ",\"order\":" + jsonString(order);
  }
  if (limit >= 0) {
    out += ",\"limit\":" + limit.toString();
  }
  if (predicateJson.length > 0) {
    out += ",\"predicate\":" + predicateJson;
  }
  return out + "}";
}

export function evictRuntimeRecords(
  table: string,
  key: string = "",
  afterKey: string = "",
  limit: i64 = -1,
  parentKey: string = "",
  nested: string = "",
): string {
  let out = "{\"type\":\"evictRuntimeRecords\",\"table\":" + jsonString(table);
  if (parentKey.length > 0) {
    out += ",\"parentKey\":" + jsonString(parentKey);
  }
  if (nested.length > 0) {
    out += ",\"nested\":" + jsonString(nested);
  }
  if (key.length > 0) {
    out += ",\"key\":" + jsonString(key);
  }
  if (afterKey.length > 0) {
    out += ",\"afterKey\":" + jsonString(afterKey);
  }
  if (limit >= 0) {
    out += ",\"limit\":" + limit.toString();
  }
  return out + "}";
}

export function activateRuntimeRoom(roomId: string, limit: i64 = -1): string {
  let out = "{\"type\":\"activateRuntimeRoom\",\"roomId\":" + jsonString(roomId);
  if (limit >= 0) {
    out += ",\"limit\":" + limit.toString();
  }
  return out + "}";
}

export function evictRuntimeRoom(roomId: string): string {
  return "{\"type\":\"evictRuntimeRoom\",\"roomId\":" + jsonString(roomId) + "}";
}

export function scheduleActorReminder(
  kind: string,
  key: string,
  reminderId: string = "",
  dueAtMs: i64 = -1,
  delayMs: i64 = -1,
  payloadJson: string = "",
): string {
  let out = "{\"type\":\"scheduleActorReminder\",\"kind\":" + jsonString(kind) + ",\"key\":" + jsonString(key);
  if (reminderId.length > 0) {
    out += ",\"reminderId\":" + jsonString(reminderId);
  }
  if (dueAtMs >= 0) {
    out += ",\"dueAtMs\":" + dueAtMs.toString();
  }
  if (delayMs >= 0) {
    out += ",\"delayMs\":" + delayMs.toString();
  }
  if (payloadJson.length > 0) {
    out += ",\"payload\":" + payloadJson;
  }
  return out + "}";
}

export function scheduleBehaviorReminder(
  kind: string,
  key: string,
  behavior: string,
  mutation: string,
  reminderId: string = "",
  dueAtMs: i64 = -1,
  delayMs: i64 = -1,
  inputJson: string = "",
  userId: string = "",
  callChainId: string = "",
  callDepth: i64 = -1,
  maxDepth: i64 = -1,
  deadlineMs: i64 = -1,
  pathJson: string = "",
  replyToJson: string = "",
): string {
  return scheduleActorReminder(
    kind,
    key,
    reminderId,
    dueAtMs,
    delayMs,
    behaviorContinuation(
      behavior,
      mutation,
      inputJson,
      userId,
      callChainId,
      callDepth,
      maxDepth,
      deadlineMs,
      pathJson,
      replyToJson,
    ),
  );
}

export function requestHostHttp(
  method: string,
  url: string,
  actorKind: string,
  actorKey: string,
  continuationJson: string,
  requestId: string = "",
  headersJson: string = "",
  bodyJson: string = "",
  bodyBase64: string = "",
  timeoutMs: i64 = -1,
  reminderId: string = "",
): string {
  let out = "{\"type\":\"requestHostHttp\",\"method\":" + jsonString(method) + ",\"url\":" + jsonString(url) + ",\"actorKind\":" + jsonString(actorKind) + ",\"actorKey\":" + jsonString(actorKey) + ",\"continuation\":" + continuationJson;
  if (requestId.length > 0) {
    out += ",\"requestId\":" + jsonString(requestId);
  }
  if (headersJson.length > 0) {
    out += ",\"headers\":" + headersJson;
  }
  if (bodyJson.length > 0) {
    out += ",\"body\":" + bodyJson;
  }
  if (bodyBase64.length > 0) {
    out += ",\"bodyBase64\":" + jsonString(bodyBase64);
  }
  if (timeoutMs >= 0) {
    out += ",\"timeoutMs\":" + timeoutMs.toString();
  }
  if (reminderId.length > 0) {
    out += ",\"reminderId\":" + jsonString(reminderId);
  }
  return out + "}";
}

export function behaviorContinuation(
  behavior: string,
  mutation: string,
  inputJson: string = "",
  userId: string = "",
  callChainId: string = "",
  callDepth: i64 = -1,
  maxDepth: i64 = -1,
  deadlineMs: i64 = -1,
  pathJson: string = "",
  replyToJson: string = "",
): string {
  let out = "{\"type\":\"behaviorContinuation\",\"behavior\":" + jsonString(behavior) + ",\"mutation\":" + jsonString(mutation);
  if (inputJson.length > 0) {
    out += ",\"input\":" + inputJson;
  }
  if (userId.length > 0) {
    out += ",\"userId\":" + jsonString(userId);
  }
  if (callChainId.length > 0) {
    out += ",\"callChainId\":" + jsonString(callChainId);
  }
  if (callDepth >= 0) {
    out += ",\"callDepth\":" + callDepth.toString();
  }
  if (maxDepth >= 0) {
    out += ",\"maxDepth\":" + maxDepth.toString();
  }
  if (deadlineMs >= 0) {
    out += ",\"deadlineMs\":" + deadlineMs.toString();
  }
  if (pathJson.length > 0) {
    out += ",\"path\":" + pathJson;
  }
  if (replyToJson.length > 0) {
    out += ",\"replyTo\":" + replyToJson;
  }
  return out + "}";
}

export function behaviorReplyTo(
  actorKind: string,
  actorKey: string,
  continuationJson: string,
  reminderId: string = "",
): string {
  let out = "{\"actorKind\":" + jsonString(actorKind) + ",\"actorKey\":" + jsonString(actorKey);
  if (reminderId.length > 0) {
    out += ",\"reminderId\":" + jsonString(reminderId);
  }
  return out + ",\"continuation\":" + continuationJson + "}";
}

export function object1(key: string, valueJson: string): string {
  return "{" + jsonString(key) + ":" + valueJson + "}";
}

export function object2(keyA: string, valueAJson: string, keyB: string, valueBJson: string): string {
  return "{" + jsonString(keyA) + ":" + valueAJson + "," + jsonString(keyB) + ":" + valueBJson + "}";
}

export function jsonString(value: string): string {
  let out = "\"";
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code == 34) {
      out += "\\\"";
    } else if (code == 92) {
      out += "\\\\";
    } else if (code == 10) {
      out += "\\n";
    } else if (code == 13) {
      out += "\\r";
    } else if (code == 9) {
      out += "\\t";
    } else {
      out += String.fromCharCode(code);
    }
  }
  return out + "\"";
}

export function inputString(requestJson: string, field: string): string {
  const inputIndex = requestJson.indexOf("\"input\"");
  if (inputIndex < 0) {
    return "";
  }
  return jsonFieldString(requestJson, field, inputIndex);
}

export function requestString(requestJson: string, field: string): string {
  return jsonFieldString(requestJson, field, 0);
}

function jsonFieldString(json: string, field: string, fromIndex: i32): string {
  const needle = "\"" + field + "\"";
  let index = json.indexOf(needle, fromIndex);
  if (index < 0) {
    return "";
  }
  index = json.indexOf(":", index + needle.length);
  if (index < 0) {
    return "";
  }
  index += 1;
  while (index < json.length && isWhitespace(json.charCodeAt(index))) {
    index += 1;
  }
  if (index >= json.length || json.charCodeAt(index) != 34) {
    return "";
  }
  index += 1;

  let out = "";
  while (index < json.length) {
    const code = json.charCodeAt(index);
    if (code == 34) {
      return out;
    }
    if (code == 92 && index + 1 < json.length) {
      index += 1;
      const escaped = json.charCodeAt(index);
      if (escaped == 110) {
        out += "\n";
      } else if (escaped == 114) {
        out += "\r";
      } else if (escaped == 116) {
        out += "\t";
      } else {
        out += String.fromCharCode(escaped);
      }
    } else {
      out += String.fromCharCode(code);
    }
    index += 1;
  }
  return out;
}

function isWhitespace(code: i32): bool {
  return code == 32 || code == 10 || code == 13 || code == 9;
}
