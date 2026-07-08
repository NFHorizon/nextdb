use nextdb_behavior_sdk::{
    BehaviorCommand, BehaviorInvokeOutput, BehaviorInvokeRequest,
    BehaviorRecordTransactionOperation, runtime_context,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EchoInput {
    room_id: String,
    body: String,
    #[serde(default)]
    runtime_activate: bool,
}

fn handle(request: BehaviorInvokeRequest<EchoInput>) -> BehaviorInvokeOutput {
    let context = runtime_context(&request);
    let room_id = request.input.room_id.clone();
    let existing_title = request
        .context
        .get("records")
        .and_then(serde_json::Value::as_array)
        .and_then(|records| records.first())
        .and_then(|entry| entry.get("record"))
        .and_then(|record| record.get("value"))
        .and_then(|value| value.get("title"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Echo Room");
    let nested_body = request
        .context
        .get("nestedRecords")
        .and_then(serde_json::Value::as_array)
        .and_then(|records| records.first())
        .and_then(|entry| entry.get("record"))
        .and_then(|record| record.get("value"))
        .and_then(|value| value.get("body"))
        .and_then(serde_json::Value::as_str);
    let object_body_reads = request
        .context
        .get("objectBodies")
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let realtime_state_version = request
        .context
        .get("realtimeChannelStates")
        .and_then(serde_json::Value::as_array)
        .and_then(|states| states.first())
        .and_then(|entry| entry.get("state"))
        .and_then(|snapshot| snapshot.get("version"))
        .and_then(serde_json::Value::as_u64);
    let realtime_state_label = request
        .context
        .get("realtimeChannelStates")
        .and_then(serde_json::Value::as_array)
        .and_then(|states| states.first())
        .and_then(|entry| entry.get("state"))
        .and_then(|snapshot| snapshot.get("state"))
        .and_then(|state| state.get("label"))
        .and_then(serde_json::Value::as_str);
    let realtime_member_count = request
        .context
        .get("realtimeChannelMembers")
        .and_then(serde_json::Value::as_array)
        .and_then(|states| states.first())
        .and_then(|entry| entry.get("members"))
        .and_then(serde_json::Value::as_array)
        .map(Vec::len);
    let connection_session_count = request
        .context
        .get("connectionSessions")
        .and_then(serde_json::Value::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|entry| entry.get("sessions"))
        .and_then(serde_json::Value::as_array)
        .map(Vec::len);
    let connection_metadata_device = request
        .context
        .get("connectionSessions")
        .and_then(serde_json::Value::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|entry| entry.get("sessions"))
        .and_then(serde_json::Value::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|session| session.get("metadata"))
        .and_then(|metadata| metadata.get("device"))
        .and_then(serde_json::Value::as_str);
    let body = format!(
        "[{}:{}] {}",
        request.behavior, request.mutation, request.input.body
    );
    let output_object_id = if request.input.runtime_activate {
        format!(
            "rust-behavior-output-{}-runtime-activation",
            request.input.room_id
        )
    } else {
        format!("rust-behavior-output-{}", request.input.room_id)
    };
    let mut output = BehaviorInvokeOutput::new(json!({
        "handledBy": "nextdb-echo-behavior",
        "userId": request.user_id,
        "clientMutationId": request.client_mutation_id,
        "existingTitle": existing_title,
        "nestedBody": nested_body,
        "objectBodyReads": object_body_reads,
        "realtimeStateVersion": realtime_state_version,
        "realtimeStateLabel": realtime_state_label,
        "realtimeMemberCount": realtime_member_count,
        "connectionSessionCount": connection_session_count,
        "connectionMetadataDevice": connection_metadata_device,
        "runtimeTimestampMs": context.as_ref().map(|context| context.timestamp_ms),
        "runtimeSenderKind": context.as_ref().map(|context| context.sender.kind.as_str()),
        "runtimeRngSeed": context.as_ref().map(|context| context.rng_seed.as_str()),
    }))
    .with_command(BehaviorCommand::upsert_record(
        "rooms",
        room_id.clone(),
        json!({
            "id": room_id.clone(),
            "title": existing_title,
        }),
    ))
    .with_command(BehaviorCommand::record_transaction(vec![
        BehaviorRecordTransactionOperation::nested_upsert(
            "rooms",
            room_id.clone(),
            "messages",
            format!("behavior-{}", request.mutation),
            json!({
                "id": format!("behavior-{}", request.mutation),
                "roomId": room_id,
                "senderId": request.user_id.as_deref().unwrap_or("behavior"),
                "body": body.clone(),
                "attachments": [],
                "createdAtMs": 0,
                "path": format!("tables/rooms/{}/messages/behavior-{}", request.input.room_id, request.mutation),
            }),
        ),
    ]))
    .with_command(BehaviorCommand::put_object(
        body.as_bytes(),
        "text/plain",
        Some(output_object_id),
    ));
    if request.input.runtime_activate {
        output = output
            .with_command(BehaviorCommand::activate_runtime_records(
                "rooms",
                Some(request.input.room_id.clone()),
            ))
            .with_command(BehaviorCommand::activate_runtime_room(
                request.input.room_id.clone(),
                Some(2),
            ));
    }
    output.with_command(BehaviorCommand::send_message(request.input.room_id, body))
}

nextdb_behavior_sdk::nextdb_behavior_postcard!(EchoInput, handle);
