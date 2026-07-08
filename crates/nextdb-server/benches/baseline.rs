use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BaselineWalRecord {
    lsn: u64,
    shard: u32,
    scope: String,
    table: String,
    key: String,
    event_type: String,
    payload: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BaselinePostcardWalFrame {
    lsn: u64,
    shard: u64,
    shard_epoch: u64,
    owner_node_id: String,
    timestamp_ms: u64,
    schema_version: u32,
    durability: String,
    payload_json: Vec<u8>,
    checksum: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BaselineFrame {
    topic: String,
    lsn: u64,
    version: u64,
    payload: Value,
}

fn bench_wal_json(c: &mut Criterion) {
    let record = sample_wal_record();
    let encoded = serde_json::to_vec(&record).expect("sample record encodes");

    let mut group = c.benchmark_group("wal_json");
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function("encode", |b| {
        b.iter(|| serde_json::to_vec(black_box(&record)).expect("sample record encodes"))
    });
    group.bench_function("decode", |b| {
        b.iter(|| {
            serde_json::from_slice::<BaselineWalRecord>(black_box(&encoded))
                .expect("sample record decodes")
        })
    });
    group.finish();
}

fn bench_wal_postcard_envelope(c: &mut Criterion) {
    let frame = sample_postcard_wal_frame();
    let encoded = postcard::to_allocvec(&frame).expect("sample postcard WAL frame encodes");

    let mut group = c.benchmark_group("wal_postcard_envelope");
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function("encode", |b| {
        b.iter(|| {
            postcard::to_allocvec(black_box(&frame)).expect("sample postcard WAL frame encodes")
        })
    });
    group.bench_function("decode", |b| {
        b.iter(|| {
            postcard::from_bytes::<BaselinePostcardWalFrame>(black_box(&encoded))
                .expect("sample postcard WAL frame decodes")
        })
    });
    group.finish();
}

fn bench_payload_hash(c: &mut Criterion) {
    let payload = serde_json::to_vec(&sample_wal_record()).expect("sample record encodes");

    let mut group = c.benchmark_group("payload_hash");
    group.throughput(Throughput::Bytes(payload.len() as u64));
    group.bench_function("sha256", |b| {
        b.iter(|| {
            let mut hasher = Sha256::new();
            hasher.update(black_box(&payload));
            hasher.finalize()
        })
    });
    group.finish();
}

fn bench_message_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_batch");
    for size in [1usize, 16, 80, 256] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(format!("prepare_{size}"), &size, |b, &size| {
            b.iter(|| prepare_message_batch(black_box(size)))
        });
    }
    group.finish();
}

fn bench_fanout_frame(c: &mut Criterion) {
    let frame = BaselineFrame {
        topic: "table.rooms.key.bench-room-1".to_owned(),
        lsn: 42,
        version: 7,
        payload: json!({
            "type": "record.updated",
            "table": "rooms",
            "key": "bench-room-1",
            "value": {
                "id": "bench-room-1",
                "title": "Benchmark Room",
                "memberCount": 64,
                "updatedAt": 1_750_000_000_000_u64
            }
        }),
    };
    let encoded = serde_json::to_vec(&frame).expect("sample frame encodes");

    let mut group = c.benchmark_group("fanout_frame");
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function("encode", |b| {
        b.iter(|| serde_json::to_string(black_box(&frame)).expect("sample frame encodes"))
    });
    group.finish();
}

fn sample_wal_record() -> BaselineWalRecord {
    BaselineWalRecord {
        lsn: 42,
        shard: 3,
        scope: "room:bench-room-1".to_owned(),
        table: "messages".to_owned(),
        key: "msg-0000000042".to_owned(),
        event_type: "message.created".to_owned(),
        payload: json!({
            "id": "msg-0000000042",
            "roomId": "bench-room-1",
            "senderId": "benchmark-writer",
            "body": "benchmark strict message 42",
            "createdAt": 1_750_000_000_000_u64,
            "metadata": {
                "durability": "strict",
                "clientMutationId": "bench-room-1-message-42"
            }
        }),
    }
}

fn sample_postcard_wal_frame() -> BaselinePostcardWalFrame {
    let record = sample_wal_record();
    let payload_json = serde_json::to_vec(&record.payload).expect("sample payload encodes");
    BaselinePostcardWalFrame {
        lsn: record.lsn,
        shard: record.shard as u64,
        shard_epoch: 1,
        owner_node_id: "node-bench".to_owned(),
        timestamp_ms: 1_750_000_000_000_u64,
        schema_version: 1,
        durability: "strict".to_owned(),
        payload_json,
        checksum: Some(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        ),
    }
}

fn prepare_message_batch(size: usize) -> Vec<BaselineWalRecord> {
    (0..size)
        .map(|index| {
            let mut record = sample_wal_record();
            record.lsn = index as u64 + 1;
            record.key = format!("msg-{index:010}");
            record.payload["id"] = json!(record.key);
            record.payload["body"] = json!(format!("benchmark strict batch message {index}"));
            record
        })
        .collect()
}

criterion_group!(
    benches,
    bench_wal_json,
    bench_wal_postcard_envelope,
    bench_payload_hash,
    bench_message_batch,
    bench_fanout_frame
);
criterion_main!(benches);
