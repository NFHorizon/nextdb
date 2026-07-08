use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::{
    model::Message,
    util::{hex_lower, time_bucket_day},
};

#[derive(Clone)]
pub struct ChatLog {
    root: PathBuf,
    writers: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<ChatLogBucketWriter>>>>>,
    projection: Arc<Mutex<Option<Arc<ChatLogProjection>>>>,
}

struct ChatLogBucketWriter {
    data: File,
    index: File,
    next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatLogIndexEntry {
    lsn: u64,
    created_at_ms: u64,
    id: String,
    offset: u64,
    len: u64,
}

struct ChatLogProjection {
    db: Database,
    messages: Keyspace,
    refs: Keyspace,
}

const CHAT_LOG_MESSAGES_KEYSPACE: &str = "chat_log_messages";
const CHAT_LOG_REFS_KEYSPACE: &str = "chat_log_refs";
const CHAT_LOG_MESSAGE_MAGIC: &[u8; 8] = b"NDBMSG01";

#[derive(Debug, Serialize)]
struct ChatLogMessageEnvelope<'a> {
    message: &'a Message,
}

#[derive(Debug, Deserialize)]
struct OwnedChatLogMessageEnvelope {
    message: Message,
}

impl ChatLogBucketWriter {
    async fn append_encoded(&mut self, message: &Message, encoded: &[u8]) -> Result<()> {
        self.append_many_encoded([(message, encoded.to_vec())])
            .await
    }

    async fn append_many_encoded<'a, I>(&mut self, messages: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a Message, Vec<u8>)>,
    {
        let mut offset = self.next_offset;
        let mut data = Vec::new();
        let mut index = Vec::new();
        for (message, encoded) in messages {
            let entry = ChatLogIndexEntry {
                lsn: message.lsn,
                created_at_ms: message.created_at_ms,
                id: message.id.clone(),
                offset,
                len: encoded.len() as u64,
            };
            offset = offset.saturating_add(encoded.len() as u64);
            data.extend_from_slice(&encoded);
            serde_json::to_writer(&mut index, &entry)?;
            index.push(b'\n');
        }
        self.data.write_all(&data).await?;
        self.next_offset = offset;
        self.index.write_all(&index).await?;
        Ok(())
    }

    async fn rewrite_index(&mut self, path: &Path, entries: &[ChatLogIndexEntry]) -> Result<()> {
        let mut encoded = Vec::new();
        for entry in entries {
            serde_json::to_writer(&mut encoded, entry)?;
            encoded.push(b'\n');
        }
        let index_path = index_path_for_bucket(path);
        fs::write(&index_path, encoded).await?;
        self.index = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_path)
            .await?;
        self.next_offset = self.data.metadata().await?.len();
        Ok(())
    }
}

impl ChatLog {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            writers: Arc::new(Mutex::new(HashMap::new())),
            projection: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn append(&self, message: &Message) -> Result<()> {
        let path = self.bucket_path(&message.room_id, time_bucket_day(message.created_at_ms));
        let encoded = encode_message_line(message)?;
        let writer = self.writer_for_path(path).await?;
        writer
            .lock()
            .await
            .append_encoded(message, &encoded)
            .await?;
        self.projection().await?.put_messages([message.clone()])?;
        Ok(())
    }

    pub async fn append_many(&self, messages: &[Message]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let first_path = self.bucket_path(
            &messages[0].room_id,
            time_bucket_day(messages[0].created_at_ms),
        );
        let mut same_bucket_messages = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let path = self.bucket_path(&message.room_id, time_bucket_day(message.created_at_ms));
            let encoded = encode_message_line(message)?;
            if path == first_path {
                same_bucket_messages.push((message, encoded));
                continue;
            }

            let mut batches = HashMap::<PathBuf, Vec<(&Message, Vec<u8>)>>::new();
            batches.insert(first_path.clone(), same_bucket_messages);
            batches.entry(path).or_default().push((message, encoded));
            for message in &messages[index + 1..] {
                let path =
                    self.bucket_path(&message.room_id, time_bucket_day(message.created_at_ms));
                let encoded = encode_message_line(message)?;
                batches.entry(path).or_default().push((message, encoded));
            }
            return self.append_batches(batches).await;
        }

        self.append_bucket(first_path, same_bucket_messages).await
    }

    async fn append_batches(
        &self,
        batches: HashMap<PathBuf, Vec<(&Message, Vec<u8>)>>,
    ) -> Result<()> {
        for (path, messages) in batches {
            self.append_bucket(path, messages).await?;
        }
        Ok(())
    }

    async fn append_bucket(&self, path: PathBuf, messages: Vec<(&Message, Vec<u8>)>) -> Result<()> {
        let projection_messages = messages
            .iter()
            .map(|(message, _encoded)| (*message).clone())
            .collect::<Vec<_>>();
        let writer = self.writer_for_path(path).await?;
        writer.lock().await.append_many_encoded(messages).await?;
        self.projection().await?.put_messages(projection_messages)?;
        Ok(())
    }

    pub async fn rebuild_from_messages(&self, messages: &[Message]) -> Result<()> {
        let marker = self.root.join(".rebuilt-from-wal");
        if marker.exists() {
            return Ok(());
        }
        self.close_writers().await;
        self.close_projection().await;
        if self.root.exists() {
            fs::remove_dir_all(&self.root).await?;
        }
        self.append_many(messages).await?;
        fs::create_dir_all(&self.root).await?;
        fs::write(marker, b"ok\n").await?;
        Ok(())
    }

    pub async fn force_rebuild_from_messages(&self, messages: &[Message]) -> Result<usize> {
        self.close_writers().await;
        self.close_projection().await;
        if self.root.exists() {
            fs::remove_dir_all(&self.root).await?;
        }
        fs::create_dir_all(&self.root).await?;

        self.append_many(messages).await?;
        fs::write(self.root.join(".rebuilt-from-wal"), b"ok\n").await?;
        Ok(messages.len())
    }

    async fn close_writers(&self) {
        self.writers.lock().await.clear();
    }

    async fn close_projection(&self) {
        *self.projection.lock().await = None;
    }

    async fn projection(&self) -> Result<Arc<ChatLogProjection>> {
        let mut projection = self.projection.lock().await;
        if let Some(projection) = projection.as_ref() {
            return Ok(projection.clone());
        }
        let opened = Arc::new(ChatLogProjection::open(self.root.join("_fjall"))?);
        *projection = Some(opened.clone());
        Ok(opened)
    }

    async fn writer_for_path(&self, path: PathBuf) -> Result<Arc<Mutex<ChatLogBucketWriter>>> {
        if let Some(writer) = self.writers.lock().await.get(&path).cloned() {
            return Ok(writer);
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        let next_offset = file.metadata().await?.len();
        let index = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_path_for_bucket(&path))
            .await?;
        let writer = Arc::new(Mutex::new(ChatLogBucketWriter {
            data: file,
            index,
            next_offset,
        }));

        let mut writers = self.writers.lock().await;
        Ok(writers.entry(path).or_insert(writer).clone())
    }

    pub async fn latest(
        &self,
        room_id: &str,
        before_lsn: Option<u64>,
        limit: usize,
    ) -> Result<Vec<Message>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let projected = self
            .projection()
            .await?
            .latest(room_id, before_lsn, limit)?;
        let room_dir = self.room_dir(room_id);
        if !projected.is_empty() || !room_dir.exists() {
            return Ok(projected);
        }

        if !room_dir.exists() {
            return Ok(Vec::new());
        }

        let mut buckets = Vec::new();
        let mut entries = fs::read_dir(room_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if let Some(bucket) = file_name
                .strip_prefix("bucket-")
                .and_then(|name| name.strip_suffix(".jsonl"))
                .and_then(|name| name.parse::<u64>().ok())
            {
                buckets.push(bucket);
            }
        }
        buckets.sort_by(|left, right| right.cmp(left));

        let mut messages = Vec::new();
        let mut seen = HashSet::new();
        for bucket in buckets {
            let bucket_messages = self
                .read_bucket_latest(
                    room_id,
                    bucket,
                    before_lsn,
                    limit.saturating_sub(messages.len()),
                )
                .await?;
            for message in bucket_messages {
                if seen.insert(message.id.clone()) {
                    messages.push(message);
                }
                if messages.len() >= limit {
                    return Ok(messages);
                }
            }
        }

        Ok(messages)
    }

    fn bucket_path(&self, room_id: &str, bucket: u64) -> PathBuf {
        self.room_dir(room_id)
            .join(format!("bucket-{bucket}.jsonl"))
    }

    fn room_dir(&self, room_id: &str) -> PathBuf {
        self.root.join("rooms").join(hex_lower(room_id.as_bytes()))
    }

    async fn read_bucket_latest(
        &self,
        room_id: &str,
        bucket: u64,
        before_lsn: Option<u64>,
        limit: usize,
    ) -> Result<Vec<Message>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let path = self.bucket_path(room_id, bucket);
        if let Some(messages) = self
            .read_bucket_latest_from_index(&path, before_lsn, limit)
            .await?
        {
            return Ok(messages);
        }

        let writer = self.writer_for_path(path.clone()).await?;
        let mut writer = writer.lock().await;
        let contents = fs::read(&path).await?;
        let (mut messages, index_entries) = decode_bucket_data(&contents, before_lsn)?;
        writer.rewrite_index(&path, &index_entries).await?;
        messages.sort_by(|left, right| compare_message_order(right, left));
        messages.truncate(limit);

        Ok(messages)
    }

    async fn read_bucket_latest_from_index(
        &self,
        path: &PathBuf,
        before_lsn: Option<u64>,
        limit: usize,
    ) -> Result<Option<Vec<Message>>> {
        let index_path = index_path_for_bucket(path);
        if !index_path.exists() {
            return Ok(None);
        }
        let index_bytes = match fs::read(&index_path).await {
            Ok(bytes) => bytes,
            Err(_) => return Ok(None),
        };
        let data_len = match fs::metadata(path).await {
            Ok(metadata) => metadata.len(),
            Err(_) => return Ok(None),
        };
        let mut all_entries = Vec::new();
        for line in index_bytes.split(|byte| *byte == b'\n') {
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            let Ok(entry) = serde_json::from_slice::<ChatLogIndexEntry>(line) else {
                return Ok(None);
            };
            all_entries.push(entry);
        }

        if !index_covers_data_file(&all_entries, data_len) {
            return Ok(None);
        }

        let mut entries = all_entries
            .into_iter()
            .filter(|entry| before_lsn.is_none_or(|before| entry.lsn < before))
            .collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(Some(Vec::new()));
        }
        entries.sort_by(|left, right| compare_index_order(right, left));
        entries.truncate(limit);

        let mut file = fs::File::open(path).await?;
        let mut messages = Vec::with_capacity(entries.len());
        for entry in entries {
            let mut encoded = vec![0_u8; entry.len as usize];
            if file.seek(SeekFrom::Start(entry.offset)).await.is_err()
                || file.read_exact(&mut encoded).await.is_err()
            {
                return Ok(None);
            }
            let line = trim_trailing_newline(&encoded);
            let Ok(message) = serde_json::from_slice::<Message>(line) else {
                return Ok(None);
            };
            messages.push(message);
        }
        Ok(Some(messages))
    }
}

impl ChatLogProjection {
    fn open(path: PathBuf) -> Result<Self> {
        let db = Database::builder(path).open()?;
        let messages = db.keyspace(CHAT_LOG_MESSAGES_KEYSPACE, KeyspaceCreateOptions::default)?;
        let refs = db.keyspace(CHAT_LOG_REFS_KEYSPACE, KeyspaceCreateOptions::default)?;
        Ok(Self { db, messages, refs })
    }

    fn put_messages(&self, messages: impl IntoIterator<Item = Message>) -> Result<()> {
        let messages = messages.into_iter().collect::<Vec<_>>();
        if messages.is_empty() {
            return Ok(());
        }

        let mut replacements = Vec::with_capacity(messages.len());
        for message in &messages {
            let ref_key = chat_log_ref_key(&message.room_id, &message.id);
            let previous_key = self.refs.get(&ref_key)?;
            replacements.push((ref_key, previous_key));
        }

        let mut batch = self.db.batch();
        for (message, (ref_key, previous_key)) in messages.iter().zip(replacements) {
            if let Some(previous_key) = previous_key {
                batch.remove(&self.messages, previous_key.as_ref().to_vec());
            }
            let key = chat_log_message_key(message);
            batch.insert(
                &self.messages,
                key.clone(),
                encode_projected_message(message)?,
            );
            batch.insert(&self.refs, ref_key, key);
        }
        batch.commit()?;
        Ok(())
    }

    fn latest(&self, room_id: &str, before_lsn: Option<u64>, limit: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::new();
        let mut seen = HashSet::new();
        for guard in self.messages.prefix(chat_log_room_prefix(room_id)) {
            let bytes = guard.value()?;
            let message = decode_projected_message(bytes.as_ref())?;
            if before_lsn.is_some_and(|before| message.lsn >= before) {
                continue;
            }
            if seen.insert(message.id.clone()) {
                messages.push(message);
            }
            if messages.len() >= limit {
                break;
            }
        }
        Ok(messages)
    }
}

fn decode_bucket_data(
    contents: &[u8],
    before_lsn: Option<u64>,
) -> Result<(Vec<Message>, Vec<ChatLogIndexEntry>)> {
    let mut messages = Vec::new();
    let mut index_entries = Vec::new();
    let mut offset = 0_u64;
    for raw_line in contents.split_inclusive(|byte| *byte == b'\n') {
        if raw_line.iter().all(|byte| byte.is_ascii_whitespace()) {
            offset = offset.saturating_add(raw_line.len() as u64);
            continue;
        }
        let line = trim_trailing_newline(raw_line);
        let message: Message = serde_json::from_slice(line)?;
        index_entries.push(ChatLogIndexEntry {
            lsn: message.lsn,
            created_at_ms: message.created_at_ms,
            id: message.id.clone(),
            offset,
            len: raw_line.len() as u64,
        });
        offset = offset.saturating_add(raw_line.len() as u64);
        if before_lsn.is_some_and(|before| message.lsn >= before) {
            continue;
        }
        messages.push(message);
    }
    Ok((messages, index_entries))
}

fn encode_message_line(message: &Message) -> Result<Vec<u8>> {
    let mut encoded = serde_json::to_vec(message)?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn encode_projected_message(message: &Message) -> Result<Vec<u8>> {
    let envelope = ChatLogMessageEnvelope { message };
    let postcard = postcard::to_allocvec(&envelope)?;
    let mut encoded = Vec::with_capacity(CHAT_LOG_MESSAGE_MAGIC.len() + postcard.len());
    encoded.extend_from_slice(CHAT_LOG_MESSAGE_MAGIC);
    encoded.extend_from_slice(&postcard);
    Ok(encoded)
}

fn decode_projected_message(bytes: &[u8]) -> Result<Message> {
    if !bytes.starts_with(CHAT_LOG_MESSAGE_MAGIC) {
        return Ok(serde_json::from_slice(bytes)?);
    }
    let envelope: OwnedChatLogMessageEnvelope =
        postcard::from_bytes(&bytes[CHAT_LOG_MESSAGE_MAGIC.len()..])?;
    Ok(envelope.message)
}

fn compare_message_order(left: &Message, right: &Message) -> Ordering {
    match (left.lsn, right.lsn) {
        (left_lsn, right_lsn) if left_lsn > 0 && right_lsn > 0 => left_lsn.cmp(&right_lsn),
        _ => left
            .created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.id.cmp(&right.id)),
    }
}

fn compare_index_order(left: &ChatLogIndexEntry, right: &ChatLogIndexEntry) -> Ordering {
    match (left.lsn, right.lsn) {
        (left_lsn, right_lsn) if left_lsn > 0 && right_lsn > 0 => left_lsn.cmp(&right_lsn),
        _ => left
            .created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.id.cmp(&right.id)),
    }
}

fn chat_log_message_key(message: &Message) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(message.room_id.len() + message.id.len() + 44);
    bytes.extend_from_slice(message.room_id.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(format!("{:020}", u64::MAX.saturating_sub(message.lsn)).as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(
        format!("{:020}", u64::MAX.saturating_sub(message.created_at_ms)).as_bytes(),
    );
    bytes.push(0);
    bytes.extend_from_slice(message.id.as_bytes());
    bytes
}

fn chat_log_room_prefix(room_id: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(room_id.len() + 1);
    bytes.extend_from_slice(room_id.as_bytes());
    bytes.push(0);
    bytes
}

fn chat_log_ref_key(room_id: &str, message_id: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(room_id.len() + message_id.len() + 1);
    bytes.extend_from_slice(room_id.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(message_id.as_bytes());
    bytes
}

fn index_covers_data_file(entries: &[ChatLogIndexEntry], data_len: u64) -> bool {
    if entries.is_empty() {
        return data_len == 0;
    }
    let mut by_offset = entries.iter().collect::<Vec<_>>();
    by_offset.sort_by_key(|entry| entry.offset);
    let mut expected_offset = 0_u64;
    for entry in by_offset {
        if entry.offset != expected_offset {
            return false;
        }
        let Some(end) = entry.offset.checked_add(entry.len) else {
            return false;
        };
        if end > data_len {
            return false;
        }
        expected_offset = end;
    }
    expected_offset == data_len
}

fn trim_trailing_newline(line: &[u8]) -> &[u8] {
    match line.last() {
        Some(b'\n') => &line[..line.len() - 1],
        _ => line,
    }
}

fn index_path_for_bucket(path: &Path) -> PathBuf {
    path.with_extension("idx")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static TEST_ROOT_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "nextdb-chat-log-{name}-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos(),
            TEST_ROOT_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
        ))
    }

    fn message(room_id: &str, id: &str, lsn: u64, created_at_ms: u64) -> Message {
        Message {
            id: id.to_string(),
            room_id: room_id.to_string(),
            sender_id: "user-a".to_string(),
            body: id.to_string(),
            attachments: Vec::new(),
            created_at_ms,
            lsn,
            path: format!("rooms/{room_id}/messages/{id}"),
        }
    }

    fn large_message(room_id: &str, index: u64) -> Message {
        let mut message = message(
            room_id,
            &format!("m{index:03}"),
            index,
            1_700_000_000_000 + index,
        );
        message.body = "x".repeat(2048);
        message
    }

    async fn assert_bucket_index_covers_data(path: &PathBuf) {
        let data_len = fs::metadata(path)
            .await
            .expect("read bucket metadata")
            .len();
        let index_bytes = fs::read(index_path_for_bucket(path))
            .await
            .expect("read bucket index");
        let entries = index_bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.iter().all(|byte| byte.is_ascii_whitespace()))
            .map(|line| serde_json::from_slice::<ChatLogIndexEntry>(line).expect("parse index"))
            .collect::<Vec<_>>();
        assert!(index_covers_data_file(&entries, data_len));
    }

    #[tokio::test]
    async fn rebuild_from_messages_batches_and_keeps_latest_semantics() {
        let root = test_root("rebuild");
        let log = ChatLog::new(root.clone());
        let messages = vec![
            message("room-a", "m1", 1, 1_700_000_000_000),
            message("room-a", "m2", 2, 1_700_000_000_001),
            message("room-a", "m3", 3, 1_700_000_000_002),
        ];

        log.rebuild_from_messages(&messages)
            .await
            .expect("rebuild messages");

        assert!(root.join(".rebuilt-from-wal").exists());
        let latest = log.latest("room-a", None, 10).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m3", "m2", "m1"]
        );
        let bucket = time_bucket_day(1_700_000_000_000);
        assert_bucket_index_covers_data(&log.bucket_path("room-a", bucket)).await;

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn force_rebuild_from_messages_replaces_existing_log() {
        let root = test_root("force-rebuild");
        let log = ChatLog::new(root.clone());
        log.append(&message("room-a", "old", 1, 1_700_000_000_000))
            .await
            .expect("append old");

        let count = log
            .force_rebuild_from_messages(&[
                message("room-a", "new-a", 2, 1_700_000_000_001),
                message("room-a", "new-b", 3, 1_700_000_000_002),
            ])
            .await
            .expect("force rebuild");

        assert_eq!(count, 2);
        assert!(root.join(".rebuilt-from-wal").exists());
        let latest = log.latest("room-a", None, 10).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["new-b", "new-a"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn latest_reads_large_bucket_from_tail_with_before_lsn() {
        let root = test_root("latest-tail");
        let log = ChatLog::new(root.clone());
        let messages = (1..=80)
            .map(|index| large_message("room-a", index))
            .collect::<Vec<_>>();
        log.append_many(&messages)
            .await
            .expect("append large bucket");

        let latest = log.latest("room-a", None, 5).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m080", "m079", "m078", "m077", "m076"]
        );

        let before = log
            .latest("room-a", Some(78), 3)
            .await
            .expect("latest before");
        assert_eq!(
            before
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m077", "m076", "m075"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn latest_uses_fjall_after_legacy_buckets_are_removed() {
        let root = test_root("latest-fjall-no-legacy");
        let log = ChatLog::new(root.clone());
        log.append_many(&[
            message("room-a", "m1", 1, 1_700_000_000_000),
            message("room-a", "m3", 3, 1_700_000_000_002),
            message("room-a", "m2", 2, 1_700_000_000_001),
        ])
        .await
        .expect("append messages");

        fs::remove_dir_all(root.join("rooms"))
            .await
            .expect("remove legacy buckets");

        let latest = log.latest("room-a", None, 3).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m3", "m2", "m1"]
        );
        let before = log
            .latest("room-a", Some(3), 2)
            .await
            .expect("latest before");
        assert_eq!(
            before
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m2", "m1"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn fjall_projection_uses_postcard_message_envelope_and_reads_legacy_json() {
        let root = test_root("message-projection-postcard");
        let projection = ChatLogProjection::open(root.join("_fjall")).expect("open projection");
        let first = message("room-a", "m1", 1, 1_700_000_000_000);

        projection
            .put_messages([first.clone()])
            .expect("put projected message");

        let raw = projection
            .messages
            .get(chat_log_message_key(&first))
            .expect("get projected message")
            .expect("projected message exists");
        assert!(raw.as_ref().starts_with(CHAT_LOG_MESSAGE_MAGIC));
        assert!(serde_json::from_slice::<Message>(raw.as_ref()).is_err());
        assert_eq!(
            decode_projected_message(raw.as_ref())
                .expect("decode projected message")
                .id,
            "m1"
        );

        let legacy = message("room-a", "m2", 2, 1_700_000_000_001);
        projection
            .messages
            .insert(
                chat_log_message_key(&legacy),
                serde_json::to_vec(&legacy).expect("encode legacy message"),
            )
            .expect("insert legacy message");

        let latest = projection
            .latest("room-a", None, 2)
            .expect("latest projected messages");

        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m2", "m1"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn append_many_spanning_buckets_preserves_latest_and_indexes() {
        let root = test_root("append-many-spanning-buckets");
        let log = ChatLog::new(root.clone());
        let day_one = 1_700_000_000_000;
        let day_two = day_one + 86_400_000;
        log.append_many(&[
            message("room-a", "a1", 1, day_one),
            message("room-b", "b1", 2, day_one),
            message("room-a", "a2", 3, day_two),
            message("room-b", "b2", 4, day_two),
        ])
        .await
        .expect("append spanning buckets");

        let latest_a = log.latest("room-a", None, 10).await.expect("latest room a");
        assert_eq!(
            latest_a
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a2", "a1"]
        );
        let latest_b = log.latest("room-b", None, 10).await.expect("latest room b");
        assert_eq!(
            latest_b
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["b2", "b1"]
        );

        for room_id in ["room-a", "room-b"] {
            assert_bucket_index_covers_data(&log.bucket_path(room_id, time_bucket_day(day_one)))
                .await;
            assert_bucket_index_covers_data(&log.bucket_path(room_id, time_bucket_day(day_two)))
                .await;
        }

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn latest_sorts_out_of_order_bucket_by_lsn() {
        let root = test_root("latest-out-of-order");
        let log = ChatLog::new(root.clone());
        log.append_many(&[
            message("room-a", "m1", 1, 1_700_000_000_000),
            message("room-a", "m4", 4, 1_700_000_000_003),
            message("room-a", "m2", 2, 1_700_000_000_001),
            message("room-a", "m5", 5, 1_700_000_000_004),
            message("room-a", "m3", 3, 1_700_000_000_002),
        ])
        .await
        .expect("append out-of-order bucket");

        let latest = log.latest("room-a", None, 3).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m5", "m4", "m3"]
        );
        let bucket = time_bucket_day(1_700_000_000_000);
        assert!(index_path_for_bucket(&log.bucket_path("room-a", bucket)).exists());

        let before = log
            .latest("room-a", Some(4), 3)
            .await
            .expect("latest before");
        assert_eq!(
            before
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m3", "m2", "m1"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn latest_uses_fjall_when_bucket_index_is_missing() {
        let root = test_root("latest-index-missing");
        let log = ChatLog::new(root.clone());
        log.append_many(&[
            message("room-a", "m1", 1, 1_700_000_000_000),
            message("room-a", "m3", 3, 1_700_000_000_002),
            message("room-a", "m2", 2, 1_700_000_000_001),
        ])
        .await
        .expect("append messages");
        let bucket = time_bucket_day(1_700_000_000_000);
        fs::remove_file(index_path_for_bucket(&log.bucket_path("room-a", bucket)))
            .await
            .expect("remove bucket index");

        let latest = log.latest("room-a", None, 3).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m3", "m2", "m1"]
        );
        assert!(!index_path_for_bucket(&log.bucket_path("room-a", bucket)).exists());

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn latest_uses_fjall_when_bucket_index_does_not_cover_data() {
        let root = test_root("latest-index-stale");
        let log = ChatLog::new(root.clone());
        log.append_many(&[
            message("room-a", "m1", 1, 1_700_000_000_000),
            message("room-a", "m2", 2, 1_700_000_000_001),
            message("room-a", "m3", 3, 1_700_000_000_002),
        ])
        .await
        .expect("append messages");
        let bucket = time_bucket_day(1_700_000_000_000);
        let index_path = index_path_for_bucket(&log.bucket_path("room-a", bucket));
        let index_bytes = fs::read(&index_path).await.expect("read index");
        let last_line_start = index_bytes[..index_bytes.len().saturating_sub(1)]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map(|index| index + 1)
            .unwrap_or(0);
        fs::write(&index_path, &index_bytes[..last_line_start])
            .await
            .expect("truncate index");

        let latest = log.latest("room-a", None, 3).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m3", "m2", "m1"]
        );
        assert!(!index_covers_data_file(
            &fs::read(&index_path)
                .await
                .expect("read stale index")
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.iter().all(|byte| byte.is_ascii_whitespace()))
                .map(|line| serde_json::from_slice::<ChatLogIndexEntry>(line).expect("parse index"))
                .collect::<Vec<_>>(),
            fs::metadata(log.bucket_path("room-a", bucket))
                .await
                .expect("bucket metadata")
                .len()
        ));

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn latest_uses_fjall_when_bucket_index_has_middle_gap() {
        let root = test_root("latest-index-gap");
        let log = ChatLog::new(root.clone());
        log.append_many(&[
            message("room-a", "m1", 1, 1_700_000_000_000),
            message("room-a", "m2", 2, 1_700_000_000_001),
            message("room-a", "m3", 3, 1_700_000_000_002),
        ])
        .await
        .expect("append messages");
        let bucket = time_bucket_day(1_700_000_000_000);
        let index_path = index_path_for_bucket(&log.bucket_path("room-a", bucket));
        let index_text = fs::read_to_string(&index_path).await.expect("read index");
        let lines = index_text.lines().collect::<Vec<_>>();
        fs::write(&index_path, format!("{}\n{}\n", lines[0], lines[2]))
            .await
            .expect("write gapped index");

        let latest = log.latest("room-a", None, 3).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m3", "m2", "m1"]
        );
        let data_len = fs::metadata(log.bucket_path("room-a", bucket))
            .await
            .expect("bucket metadata")
            .len();
        let gapped_entries = fs::read(&index_path)
            .await
            .expect("read gapped index")
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.iter().all(|byte| byte.is_ascii_whitespace()))
            .map(|line| serde_json::from_slice::<ChatLogIndexEntry>(line).expect("parse index"))
            .collect::<Vec<_>>();
        assert!(!index_covers_data_file(&gapped_entries, data_len));

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn corrupted_bucket_index_does_not_affect_fjall_latest_or_appends() {
        let root = test_root("latest-index-refresh-writer");
        let log = ChatLog::new(root.clone());
        log.append(&message("room-a", "m1", 1, 1_700_000_000_000))
            .await
            .expect("append first");
        log.append(&message("room-a", "m2", 2, 1_700_000_000_001))
            .await
            .expect("append second");

        let bucket = time_bucket_day(1_700_000_000_000);
        let bucket_path = log.bucket_path("room-a", bucket);
        let index_path = index_path_for_bucket(&bucket_path);
        fs::write(&index_path, b"")
            .await
            .expect("corrupt bucket index");

        let latest = log.latest("room-a", None, 2).await.expect("latest");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m2", "m1"]
        );
        assert!(!index_covers_data_file(
            &Vec::<ChatLogIndexEntry>::new(),
            fs::metadata(&bucket_path)
                .await
                .expect("bucket metadata")
                .len()
        ));

        log.append(&message("room-a", "m3", 3, 1_700_000_000_002))
            .await
            .expect("append after index rebuild");
        let latest = log
            .latest("room-a", None, 3)
            .await
            .expect("latest after append");
        assert_eq!(
            latest
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["m3", "m2", "m1"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn concurrent_appends_to_multiple_rooms_preserve_latest_order() {
        let root = test_root("concurrent-append");
        let log = ChatLog::new(root.clone());
        let mut tasks = Vec::new();
        for room_index in 0..8_u64 {
            for message_index in 0..16_u64 {
                let log = log.clone();
                tasks.push(tokio::spawn(async move {
                    let room_id = format!("room-{room_index}");
                    log.append(&message(
                        &room_id,
                        &format!("m{message_index:02}"),
                        message_index + 1,
                        1_700_000_000_000 + message_index,
                    ))
                    .await
                }));
            }
        }

        for task in tasks {
            task.await.expect("append task").expect("append message");
        }

        for room_index in 0..8_u64 {
            let room_id = format!("room-{room_index}");
            let latest = log.latest(&room_id, None, 16).await.expect("latest");
            assert_eq!(latest.len(), 16);
            assert_eq!(
                latest
                    .iter()
                    .map(|message| message.id.clone())
                    .collect::<Vec<_>>(),
                (0..16_u64)
                    .rev()
                    .map(|index| format!("m{index:02}"))
                    .collect::<Vec<_>>()
            );
        }

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }
}
