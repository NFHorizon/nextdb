export declare function output(resultJson: string, commandsJson?: string): string;
export declare function commandArray(commandJson: string): string;
export declare function commandArray2(firstCommandJson: string, secondCommandJson: string): string;
export declare function commandArray3(
  firstCommandJson: string,
  secondCommandJson: string,
  thirdCommandJson: string,
): string;
export declare function commandArray4(
  firstCommandJson: string,
  secondCommandJson: string,
  thirdCommandJson: string,
  fourthCommandJson: string,
): string;
export declare function commandArray5(
  firstCommandJson: string,
  secondCommandJson: string,
  thirdCommandJson: string,
  fourthCommandJson: string,
  fifthCommandJson: string,
): string;
export declare function sendMessage(
  roomId: string,
  body: string,
  durability?: string,
  attachmentsJson?: string,
): string;
export declare function publishVolatile(roomId: string, name: string, payloadJson?: string): string;
export declare function publishUserVolatile(
  userId: string,
  name: string,
  payloadJson?: string,
): string;
export declare function publishUserEvent(
  userId: string,
  name: string,
  payloadJson?: string,
  durability?: string,
  clientMutationId?: string,
): string;
export declare function putObjectBase64(
  bodyBase64: string,
  contentType?: string,
  objectId?: string,
  clientMutationId?: string,
): string;
export declare function deleteObject(
  objectId: string,
  force?: boolean,
  clientMutationId?: string,
): string;
export declare function upsertRecord(
  table: string,
  key: string,
  valueJson: string,
  durability?: string,
  expectedLsn?: number,
): string;
export declare function deleteRecord(
  table: string,
  key: string,
  durability?: string,
  expectedLsn?: number,
): string;
export declare function recordTransaction(operationsJson: string, durability?: string): string;
export declare function updateRealtimeChannelState(
  channelId: string,
  stateJson: string,
  expectedVersion?: number,
): string;
export declare function updateRealtimePresence(
  channelId: string,
  metadataJson: string,
  sessionId?: string,
): string;
export declare function broadcastRealtimeChannel(
  channelId: string,
  kind: string,
  payloadJson?: string,
  includeSelf?: boolean,
): string;
export declare function disconnectConnections(
  userId?: string,
  sessionId?: string,
  reason?: string,
): string;
export declare function activateRuntimeRecords(
  table: string,
  key?: string,
  afterKey?: string,
  limit?: number,
  indexName?: string,
  valueJson?: string,
  valuesJson?: string,
  lowerJson?: string,
  upperJson?: string,
  lowerValuesJson?: string,
  upperValuesJson?: string,
  afterCursor?: string,
  predicateJson?: string,
  parentKey?: string,
  nested?: string,
  order?: string,
): string;
export declare function evictRuntimeRecords(
  table: string,
  key?: string,
  afterKey?: string,
  limit?: number,
  parentKey?: string,
  nested?: string,
): string;
export declare function activateRuntimeRoom(roomId: string, limit?: number): string;
export declare function evictRuntimeRoom(roomId: string): string;
export declare function scheduleActorReminder(
  kind: string,
  key: string,
  reminderId?: string,
  dueAtMs?: number,
  delayMs?: number,
  payloadJson?: string,
): string;
export declare function scheduleBehaviorReminder(
  kind: string,
  key: string,
  behavior: string,
  mutation: string,
  reminderId?: string,
  dueAtMs?: number,
  delayMs?: number,
  inputJson?: string,
  userId?: string,
  callChainId?: string,
  callDepth?: number,
  maxDepth?: number,
  deadlineMs?: number,
  pathJson?: string,
  replyToJson?: string,
): string;
export declare function requestHostHttp(
  method: string,
  url: string,
  actorKind: string,
  actorKey: string,
  continuationJson: string,
  requestId?: string,
  headersJson?: string,
  bodyJson?: string,
  bodyBase64?: string,
  timeoutMs?: number,
  reminderId?: string,
): string;
export declare function behaviorContinuation(
  behavior: string,
  mutation: string,
  inputJson?: string,
  userId?: string,
  callChainId?: string,
  callDepth?: number,
  maxDepth?: number,
  deadlineMs?: number,
  pathJson?: string,
  replyToJson?: string,
): string;
export declare function behaviorReplyTo(
  actorKind: string,
  actorKey: string,
  continuationJson: string,
  reminderId?: string,
): string;
export declare function object1(key: string, valueJson: string): string;
export declare function object2(keyA: string, valueAJson: string, keyB: string, valueBJson: string): string;
export declare function jsonString(value: string): string;
export declare function inputString(requestJson: string, field: string): string;
export declare function requestString(requestJson: string, field: string): string;
