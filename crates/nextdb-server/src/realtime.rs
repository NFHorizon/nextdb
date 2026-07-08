use std::{
    collections::{BTreeSet, HashMap},
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock as StdRwLock},
};

use anyhow::{Context, Result};
use axum::{
    body::{BodyDataStream, Bytes},
    extract::ws::{Message as WsMessage, Utf8Bytes, WebSocket},
};
use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt},
    sync::{RwLock, mpsc},
};

use crate::{
    api::frames::{ClientFrame, ServerFrame},
    model::DeliveryEvent,
    util::now_ms,
};

#[derive(Clone, Default)]
pub struct RealtimeChannels {
    channels: Arc<RwLock<HashMap<String, HashMap<String, RealtimeMember>>>>,
    sequences: Arc<RwLock<HashMap<String, u64>>>,
    states: Arc<RwLock<HashMap<String, RealtimeChannelStateSnapshot>>>,
    maintenance: Arc<StdRwLock<RealtimeMaintenanceStatus>>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeMaintenanceStatus {
    pub last_sweep_at_ms: Option<u64>,
    pub last_stale_members_removed: usize,
    pub last_orphan_states_removed: usize,
    pub last_orphan_sequences_removed: usize,
    pub total_stale_members_removed: usize,
    pub total_orphan_states_removed: usize,
    pub total_orphan_sequences_removed: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeChannelsStatus {
    pub channel_count: usize,
    pub state_count: usize,
    pub sequence_count: usize,
    pub maintenance: RealtimeMaintenanceStatus,
}

impl RealtimeChannels {
    pub async fn join(
        &self,
        channel_id: String,
        user_id: String,
        session_id: Option<String>,
        metadata: serde_json::Value,
    ) -> RealtimeJoin {
        let mut channels = self.channels.write().await;
        let members = channels.entry(channel_id.clone()).or_default();
        let previous_members = members.values().cloned().collect();
        let now = now_ms();
        let member = RealtimeMember {
            user_id: user_id.clone(),
            session_id: session_id.clone(),
            metadata,
            joined_at_ms: now,
            updated_at_ms: now,
        };
        members.insert(member_key(&user_id, session_id.as_deref()), member.clone());
        RealtimeJoin {
            channel_id,
            member,
            previous_members,
            members: members.values().cloned().collect(),
        }
    }

    pub async fn leave(
        &self,
        channel_id: &str,
        user_id: &str,
        session_id: Option<&str>,
    ) -> RealtimeLeave {
        let mut remaining = Vec::new();
        let mut removed = Vec::new();
        let mut became_empty = false;
        {
            let mut channels = self.channels.write().await;
            if let Some(members) = channels.get_mut(channel_id) {
                if let Some(session_id) = session_id {
                    if let Some(member) = members.remove(&member_key(user_id, Some(session_id))) {
                        removed.push(member);
                    }
                } else {
                    let keys: Vec<String> = members
                        .iter()
                        .filter(|&(_key, member)| member.user_id == user_id)
                        .map(|(key, _member)| key.clone())
                        .collect();
                    for key in keys {
                        if let Some(member) = members.remove(&key) {
                            removed.push(member);
                        }
                    }
                }
                remaining = members.values().cloned().collect();
                if members.is_empty() {
                    channels.remove(channel_id);
                    became_empty = true;
                }
            }
        }
        if became_empty {
            self.remove_channel_runtime_state(channel_id).await;
        }
        RealtimeLeave {
            channel_id: channel_id.to_string(),
            removed,
            remaining,
        }
    }

    pub async fn leave_session(&self, user_id: &str, session_id: &str) -> Vec<RealtimeLeave> {
        let mut channels = self.channels.write().await;
        let member_key = member_key(user_id, Some(session_id));
        let mut leaves = Vec::new();
        let mut empty_channels = Vec::new();

        for (channel_id, members) in channels.iter_mut() {
            if let Some(member) = members.remove(&member_key) {
                let remaining = members.values().cloned().collect();
                if members.is_empty() {
                    empty_channels.push(channel_id.clone());
                }
                leaves.push(RealtimeLeave {
                    channel_id: channel_id.clone(),
                    removed: vec![member],
                    remaining,
                });
            }
        }

        for channel_id in empty_channels {
            channels.remove(&channel_id);
        }
        drop(channels);
        for channel_id in leaves
            .iter()
            .filter(|leave| leave.remaining.is_empty())
            .map(|leave| leave.channel_id.as_str())
        {
            self.remove_channel_runtime_state(channel_id).await;
        }

        leaves
    }

    pub async fn update_member(
        &self,
        channel_id: &str,
        user_id: &str,
        session_id: Option<&str>,
        metadata: serde_json::Value,
    ) -> Option<RealtimeMemberUpdate> {
        let mut channels = self.channels.write().await;
        let members = channels.get_mut(channel_id)?;
        let member = members.get_mut(&member_key(user_id, session_id))?;
        member.metadata = metadata;
        member.updated_at_ms = now_ms();
        let member = member.clone();
        Some(RealtimeMemberUpdate {
            channel_id: channel_id.to_string(),
            member,
            members: members.values().cloned().collect(),
        })
    }

    pub async fn update_user_members(
        &self,
        channel_id: &str,
        user_id: &str,
        metadata: serde_json::Value,
    ) -> Option<RealtimeMemberBatchUpdate> {
        let mut channels = self.channels.write().await;
        let members = channels.get_mut(channel_id)?;
        let now = now_ms();
        let mut updated = Vec::new();
        for member in members.values_mut() {
            if member.user_id == user_id {
                member.metadata = metadata.clone();
                member.updated_at_ms = now;
                updated.push(member.clone());
            }
        }
        if updated.is_empty() {
            return None;
        }
        Some(RealtimeMemberBatchUpdate {
            channel_id: channel_id.to_string(),
            updated,
            members: members.values().cloned().collect(),
        })
    }

    pub async fn members(&self, channel_id: &str) -> Vec<RealtimeMember> {
        self.channels
            .read()
            .await
            .get(channel_id)
            .map(|members| members.values().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn state(&self, channel_id: &str) -> RealtimeChannelStateSnapshot {
        self.states
            .read()
            .await
            .get(channel_id)
            .cloned()
            .unwrap_or_else(|| RealtimeChannelStateSnapshot::empty(channel_id))
    }

    pub async fn update_state(
        &self,
        channel_id: &str,
        state: serde_json::Value,
        expected_version: Option<u64>,
    ) -> Result<RealtimeChannelStateSnapshot, RealtimeStateConflict> {
        let mut states = self.states.write().await;
        let current_version = states
            .get(channel_id)
            .map(|snapshot| snapshot.version)
            .unwrap_or(0);
        if expected_version.is_some_and(|expected| expected != current_version) {
            return Err(RealtimeStateConflict {
                expected_version,
                current: states
                    .get(channel_id)
                    .cloned()
                    .unwrap_or_else(|| RealtimeChannelStateSnapshot::empty(channel_id)),
            });
        }
        let version = current_version.saturating_add(1);
        let snapshot = RealtimeChannelStateSnapshot {
            channel_id: channel_id.to_string(),
            version,
            state,
            updated_at_ms: now_ms(),
        };
        states.insert(channel_id.to_string(), snapshot.clone());
        Ok(snapshot)
    }

    pub async fn list_channels(&self) -> Vec<RealtimeChannelSummary> {
        let channels = self.channels.read().await;
        let sequences = self.sequences.read().await;
        let states = self.states.read().await;
        let mut summaries: Vec<RealtimeChannelSummary> = channels
            .iter()
            .map(|(channel_id, members)| {
                let members: Vec<RealtimeMember> = members.values().cloned().collect();
                let state = states.get(channel_id);
                RealtimeChannelSummary {
                    channel_id: channel_id.clone(),
                    member_count: members.len(),
                    sequence: *sequences.get(channel_id).unwrap_or(&0),
                    state_version: state.map(|snapshot| snapshot.version).unwrap_or(0),
                    state_updated_at_ms: state.map(|snapshot| snapshot.updated_at_ms),
                    members,
                }
            })
            .collect();
        summaries.sort_by(|left, right| left.channel_id.cmp(&right.channel_id));
        summaries
    }

    pub async fn has_member(&self, channel_id: &str, user_id: &str) -> bool {
        self.channels
            .read()
            .await
            .get(channel_id)
            .is_some_and(|members| members.values().any(|member| member.user_id == user_id))
    }

    pub async fn next_sequence(&self, channel_id: &str) -> u64 {
        let mut sequences = self.sequences.write().await;
        let sequence = sequences.entry(channel_id.to_string()).or_insert(0);
        *sequence += 1;
        *sequence
    }

    pub async fn status(&self) -> RealtimeChannelsStatus {
        let channel_count = self.channels.read().await.len();
        let state_count = self.states.read().await.len();
        let sequence_count = self.sequences.read().await.len();
        RealtimeChannelsStatus {
            channel_count,
            state_count,
            sequence_count,
            maintenance: self.maintenance_status(),
        }
    }

    pub fn maintenance_status(&self) -> RealtimeMaintenanceStatus {
        *self
            .maintenance
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub async fn cleanup_orphan_runtime_state(&self) -> (usize, usize) {
        let live_channels = self
            .channels
            .read()
            .await
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed_states = {
            let mut states = self.states.write().await;
            let before = states.len();
            states.retain(|channel_id, _| live_channels.contains(channel_id));
            before.saturating_sub(states.len())
        };
        let removed_sequences = {
            let mut sequences = self.sequences.write().await;
            let before = sequences.len();
            sequences.retain(|channel_id, _| live_channels.contains(channel_id));
            before.saturating_sub(sequences.len())
        };
        self.record_maintenance_sweep(now_ms(), 0, removed_states, removed_sequences);
        (removed_states, removed_sequences)
    }

    pub async fn cleanup_inactive_session_members(
        &self,
        active_user_sessions: &BTreeSet<(String, String)>,
    ) -> Vec<RealtimeLeave> {
        let mut channels = self.channels.write().await;
        let mut leaves = Vec::new();
        let mut empty_channels = Vec::new();
        let mut stale_members_removed = 0usize;

        for (channel_id, members) in channels.iter_mut() {
            let stale_keys = members
                .iter()
                .filter_map(|(key, member)| {
                    let session_id = member.session_id.as_ref()?;
                    (!active_user_sessions.contains(&(member.user_id.clone(), session_id.clone())))
                        .then(|| key.clone())
                })
                .collect::<Vec<_>>();
            if stale_keys.is_empty() {
                continue;
            }
            let mut removed = Vec::new();
            for key in stale_keys {
                if let Some(member) = members.remove(&key) {
                    stale_members_removed += 1;
                    removed.push(member);
                }
            }
            let remaining = members.values().cloned().collect::<Vec<_>>();
            if members.is_empty() {
                empty_channels.push(channel_id.clone());
            }
            leaves.push(RealtimeLeave {
                channel_id: channel_id.clone(),
                removed,
                remaining,
            });
        }

        for channel_id in &empty_channels {
            channels.remove(channel_id);
        }
        drop(channels);

        let mut removed_states = 0usize;
        let mut removed_sequences = 0usize;
        for channel_id in empty_channels {
            removed_states += usize::from(self.states.write().await.remove(&channel_id).is_some());
            removed_sequences +=
                usize::from(self.sequences.write().await.remove(&channel_id).is_some());
        }
        if stale_members_removed > 0 || removed_states > 0 || removed_sequences > 0 {
            self.record_maintenance_sweep(
                now_ms(),
                stale_members_removed,
                removed_states,
                removed_sequences,
            );
        }
        leaves
    }

    async fn remove_channel_runtime_state(&self, channel_id: &str) {
        let removed_states = usize::from(self.states.write().await.remove(channel_id).is_some());
        let removed_sequences =
            usize::from(self.sequences.write().await.remove(channel_id).is_some());
        if removed_states > 0 || removed_sequences > 0 {
            self.record_maintenance_sweep(now_ms(), 0, removed_states, removed_sequences);
        }
    }

    fn record_maintenance_sweep(
        &self,
        swept_at_ms: u64,
        stale_members: usize,
        removed_states: usize,
        removed_sequences: usize,
    ) {
        let mut status = self
            .maintenance
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        status.last_sweep_at_ms = Some(swept_at_ms);
        status.last_stale_members_removed = stale_members;
        status.last_orphan_states_removed = removed_states;
        status.last_orphan_sequences_removed = removed_sequences;
        status.total_stale_members_removed = status
            .total_stale_members_removed
            .saturating_add(stale_members);
        status.total_orphan_states_removed = status
            .total_orphan_states_removed
            .saturating_add(removed_states);
        status.total_orphan_sequences_removed = status
            .total_orphan_sequences_removed
            .saturating_add(removed_sequences);
    }
}

fn member_key(user_id: &str, session_id: Option<&str>) -> String {
    match session_id {
        Some(session_id) => format!("{user_id}\u{1f}{session_id}"),
        None => user_id.to_string(),
    }
}

pub fn unique_member_user_ids<'a>(
    members: impl IntoIterator<Item = &'a RealtimeMember>,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    members
        .into_iter()
        .filter_map(|member| {
            if seen.insert(member.user_id.clone()) {
                Some(member.user_id.clone())
            } else {
                None
            }
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeMember {
    pub user_id: String,
    pub session_id: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub joined_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeChannelSummary {
    pub channel_id: String,
    pub member_count: usize,
    pub sequence: u64,
    pub state_version: u64,
    pub state_updated_at_ms: Option<u64>,
    pub members: Vec<RealtimeMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeChannelStateSnapshot {
    pub channel_id: String,
    pub version: u64,
    pub state: serde_json::Value,
    pub updated_at_ms: u64,
}

impl RealtimeChannelStateSnapshot {
    fn empty(channel_id: &str) -> Self {
        Self {
            channel_id: channel_id.to_string(),
            version: 0,
            state: serde_json::Value::Null,
            updated_at_ms: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RealtimeStateConflict {
    pub expected_version: Option<u64>,
    pub current: RealtimeChannelStateSnapshot,
}

#[derive(Debug, Clone)]
pub struct RealtimeJoin {
    pub channel_id: String,
    pub member: RealtimeMember,
    pub previous_members: Vec<RealtimeMember>,
    pub members: Vec<RealtimeMember>,
}

#[derive(Debug, Clone)]
pub struct RealtimeLeave {
    pub channel_id: String,
    pub removed: Vec<RealtimeMember>,
    pub remaining: Vec<RealtimeMember>,
}

#[derive(Debug, Clone)]
pub struct RealtimeMemberUpdate {
    pub channel_id: String,
    pub member: RealtimeMember,
    pub members: Vec<RealtimeMember>,
}

#[derive(Debug, Clone)]
pub struct RealtimeMemberBatchUpdate {
    pub channel_id: String,
    pub updated: Vec<RealtimeMember>,
    pub members: Vec<RealtimeMember>,
}

pub(crate) enum RealtimeFrameRead {
    Frame(Box<ClientFrame>),
    Invalid { message: String },
    Ignored,
    Closed,
}

pub(crate) trait RealtimeFrameSource {
    fn next_client_frame<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<RealtimeFrameRead>> + Send + 'a>>;
}

impl RealtimeFrameSource for futures_util::stream::SplitStream<WebSocket> {
    fn next_client_frame<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<RealtimeFrameRead>> + Send + 'a>> {
        Box::pin(async move {
            match self.next().await {
                Some(Ok(WsMessage::Text(text))) => match decode_client_frame(text.as_str()) {
                    Ok(frame) => Ok(RealtimeFrameRead::Frame(Box::new(frame))),
                    Err(err) => Ok(RealtimeFrameRead::Invalid {
                        message: format!("invalid frame: {err}"),
                    }),
                },
                Some(Ok(WsMessage::Close(_))) | None => Ok(RealtimeFrameRead::Closed),
                Some(Ok(_)) => Ok(RealtimeFrameRead::Ignored),
                Some(Err(err)) => Err(anyhow::anyhow!(err).context("websocket receive failed")),
            }
        })
    }
}

#[allow(dead_code)]
pub(crate) struct JsonLineFrameSource<R> {
    reader: R,
}

#[allow(dead_code)]
impl<R> JsonLineFrameSource<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R> RealtimeFrameSource for JsonLineFrameSource<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    fn next_client_frame<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<RealtimeFrameRead>> + Send + 'a>> {
        Box::pin(async move {
            let mut line = String::new();
            let bytes = self.reader.read_line(&mut line).await?;
            if bytes == 0 {
                return Ok(RealtimeFrameRead::Closed);
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                return Ok(RealtimeFrameRead::Ignored);
            }
            match decode_client_frame(line) {
                Ok(frame) => Ok(RealtimeFrameRead::Frame(Box::new(frame))),
                Err(err) => Ok(RealtimeFrameRead::Invalid {
                    message: format!("invalid frame: {err}"),
                }),
            }
        })
    }
}

pub(crate) struct BodyJsonLineFrameSource {
    stream: BodyDataStream,
    buffer: Vec<u8>,
    closed: bool,
}

impl BodyJsonLineFrameSource {
    pub(crate) fn new(stream: BodyDataStream) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
            closed: false,
        }
    }

    fn next_buffered_frame(&mut self) -> Option<RealtimeFrameRead> {
        let newline = self.buffer.iter().position(|byte| *byte == b'\n')?;
        let mut line = self.buffer.drain(..=newline).collect::<Vec<_>>();
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        Some(decode_client_frame_line(&line))
    }
}

impl RealtimeFrameSource for BodyJsonLineFrameSource {
    fn next_client_frame<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<RealtimeFrameRead>> + Send + 'a>> {
        Box::pin(async move {
            loop {
                if let Some(frame) = self.next_buffered_frame() {
                    return Ok(frame);
                }
                if self.closed {
                    if self.buffer.is_empty() {
                        return Ok(RealtimeFrameRead::Closed);
                    }
                    let line = std::mem::take(&mut self.buffer);
                    return Ok(decode_client_frame_line(&line));
                }
                match self.stream.next().await {
                    Some(Ok(chunk)) => {
                        self.buffer.extend_from_slice(&chunk);
                    }
                    Some(Err(err)) => {
                        return Err(anyhow::anyhow!(err).context("jsonl body receive failed"));
                    }
                    None => {
                        self.closed = true;
                    }
                }
            }
        })
    }
}

pub(crate) async fn send_server_frame(
    sender: &mut (impl RealtimeFrameSink + ?Sized),
    frame: &ServerFrame,
) -> Result<()> {
    sender.send_frame(frame).await
}

#[derive(Clone, Debug)]
pub(crate) struct EncodedServerFrame {
    json: Arc<Bytes>,
}

impl EncodedServerFrame {
    fn new(json: Bytes) -> Self {
        Self {
            json: Arc::new(json),
        }
    }

    pub(crate) fn json(&self) -> &Bytes {
        self.json.as_ref()
    }

    fn websocket_text(&self) -> Result<Utf8Bytes> {
        Utf8Bytes::try_from((*self.json).clone()).context("encoded server frame is not utf8")
    }
}

pub(crate) trait RealtimeFrameSink: Send {
    fn send_frame<'a>(
        &'a mut self,
        frame: &'a ServerFrame,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    fn send_encoded_frames<'a>(
        &'a mut self,
        frames: &'a [EncodedServerFrame],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

impl RealtimeFrameSink for futures_util::stream::SplitSink<WebSocket, WsMessage> {
    fn send_frame<'a>(
        &'a mut self,
        frame: &'a ServerFrame,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let text = encode_server_frame(frame)?;
            self.send(WsMessage::Text(text.into())).await?;
            Ok(())
        })
    }

    fn send_encoded_frames<'a>(
        &'a mut self,
        frames: &'a [EncodedServerFrame],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            for frame in frames {
                self.feed(WsMessage::Text(frame.websocket_text()?)).await?;
            }
            self.flush().await?;
            Ok(())
        })
    }
}

#[allow(dead_code)]
pub(crate) struct JsonLineFrameSink<W> {
    writer: W,
}

#[allow(dead_code)]
impl<W> JsonLineFrameSink<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W> RealtimeFrameSink for JsonLineFrameSink<W>
where
    W: AsyncWrite + Unpin + Send,
{
    fn send_frame<'a>(
        &'a mut self,
        frame: &'a ServerFrame,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let text = encode_server_frame(frame)?;
            self.writer.write_all(text.as_bytes()).await?;
            self.writer.write_all(b"\n").await?;
            self.writer.flush().await?;
            Ok(())
        })
    }

    fn send_encoded_frames<'a>(
        &'a mut self,
        frames: &'a [EncodedServerFrame],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            for frame in frames {
                self.writer.write_all(frame.json()).await?;
                self.writer.write_all(b"\n").await?;
            }
            self.writer.flush().await?;
            Ok(())
        })
    }
}

pub(crate) struct ChannelJsonLineFrameSink {
    sender: mpsc::Sender<Result<Bytes, Infallible>>,
}

impl ChannelJsonLineFrameSink {
    pub(crate) fn new(sender: mpsc::Sender<Result<Bytes, Infallible>>) -> Self {
        Self { sender }
    }
}

impl RealtimeFrameSink for ChannelJsonLineFrameSink {
    fn send_frame<'a>(
        &'a mut self,
        frame: &'a ServerFrame,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let text = encode_server_frame_json_line(frame)?;
            self.sender
                .send(Ok(Bytes::from(text)))
                .await
                .context("jsonl response stream closed")?;
            Ok(())
        })
    }

    fn send_encoded_frames<'a>(
        &'a mut self,
        frames: &'a [EncodedServerFrame],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let total_len = frames.iter().map(|frame| frame.json().len() + 1).sum();
            let mut text = BytesMut::with_capacity(total_len);
            for frame in frames {
                text.extend_from_slice(frame.json());
                text.extend_from_slice(b"\n");
            }
            self.sender
                .send(Ok(text.freeze()))
                .await
                .context("jsonl response stream closed")?;
            Ok(())
        })
    }
}

fn decode_client_frame_line(line: &[u8]) -> RealtimeFrameRead {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line.is_empty() {
        return RealtimeFrameRead::Ignored;
    }
    match decode_client_frame_bytes(line) {
        Ok(frame) => RealtimeFrameRead::Frame(Box::new(frame)),
        Err(err) => RealtimeFrameRead::Invalid {
            message: format!("invalid frame: {err}"),
        },
    }
}

pub(crate) fn decode_client_frame(text: &str) -> serde_json::Result<ClientFrame> {
    serde_json::from_str(text)
}

fn decode_client_frame_bytes(bytes: &[u8]) -> serde_json::Result<ClientFrame> {
    serde_json::from_slice(bytes)
}

fn encode_server_frame(frame: &ServerFrame) -> serde_json::Result<String> {
    serde_json::to_string(frame)
}

pub(crate) fn encode_server_frame_bytes(frame: &ServerFrame) -> serde_json::Result<Bytes> {
    serde_json::to_vec(frame).map(Bytes::from)
}

pub(crate) fn encode_server_frame_to_encoded(
    frame: &ServerFrame,
) -> serde_json::Result<EncodedServerFrame> {
    encode_server_frame_bytes(frame).map(EncodedServerFrame::new)
}

pub(crate) fn encode_delivery_events_frame(
    events: &[&DeliveryEvent],
) -> serde_json::Result<Option<EncodedServerFrame>> {
    let frame = match events {
        [] => return Ok(None),
        [event] => BorrowedDeliveryEventsFrame::Event { event },
        events => BorrowedDeliveryEventsFrame::Events {
            events: events.to_vec(),
        },
    };
    serde_json::to_vec(&frame)
        .map(Bytes::from)
        .map(EncodedServerFrame::new)
        .map(Some)
}

pub(crate) fn encode_server_frame_json_line(frame: &ServerFrame) -> serde_json::Result<String> {
    encode_server_frame(frame).map(|frame| format!("{frame}\n"))
}

#[derive(Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum BorrowedDeliveryEventsFrame<'a> {
    Event { event: &'a DeliveryEvent },
    Events { events: Vec<&'a DeliveryEvent> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
        },
        task::{Context, Poll},
    };

    struct FlushCountingWriter {
        bytes: Vec<u8>,
        flushes: Arc<AtomicUsize>,
    }

    impl AsyncWrite for FlushCountingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.bytes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.flushes.fetch_add(1, AtomicOrdering::Relaxed);
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn json_line_sink_batches_frames_with_one_flush() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let writer = FlushCountingWriter {
            bytes: Vec::new(),
            flushes: Arc::clone(&flushes),
        };
        let mut sink = JsonLineFrameSink::new(writer);
        let frames = [
            ServerFrame::Hello {
                user_id: Some("alice".to_string()),
                session_id: "session-a".to_string(),
            },
            ServerFrame::ObjectsSubscribed,
        ];
        let encoded = frames
            .iter()
            .map(encode_server_frame_to_encoded)
            .collect::<serde_json::Result<Vec<_>>>()
            .expect("encode frames");
        sink.send_encoded_frames(&encoded)
            .await
            .expect("send batched frames");

        assert_eq!(flushes.load(AtomicOrdering::Relaxed), 1);
        let output = String::from_utf8(sink.writer.bytes).expect("utf8 jsonl");
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0],
            r#"{"type":"hello","userId":"alice","sessionId":"session-a"}"#
        );
        assert_eq!(lines[1], r#"{"type":"objectsSubscribed"}"#);
    }

    #[tokio::test]
    async fn encoded_server_frames_share_bytes_across_sinks() {
        let frames = [
            ServerFrame::Hello {
                user_id: Some("alice".to_string()),
                session_id: "session-a".to_string(),
            },
            ServerFrame::ObjectsSubscribed,
        ];
        let encoded = frames
            .iter()
            .map(encode_server_frame_to_encoded)
            .collect::<serde_json::Result<Vec<_>>>()
            .expect("encode frames");
        let encoded_clone = encoded.clone();
        assert_eq!(encoded[0].json().as_ptr(), encoded_clone[0].json().as_ptr());

        let left_flushes = Arc::new(AtomicUsize::new(0));
        let right_flushes = Arc::new(AtomicUsize::new(0));
        let mut left = JsonLineFrameSink::new(FlushCountingWriter {
            bytes: Vec::new(),
            flushes: Arc::clone(&left_flushes),
        });
        let mut right = JsonLineFrameSink::new(FlushCountingWriter {
            bytes: Vec::new(),
            flushes: Arc::clone(&right_flushes),
        });

        left.send_encoded_frames(&encoded).await.expect("send left");
        right
            .send_encoded_frames(&encoded_clone)
            .await
            .expect("send right");

        assert_eq!(left_flushes.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(right_flushes.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(left.writer.bytes, right.writer.bytes);
    }

    #[test]
    fn borrowed_delivery_events_encode_like_owned_server_frame() {
        let first = DeliveryEvent::VolatileRoomEvent {
            room_id: "room-a".to_string(),
            name: "tick".to_string(),
            payload: serde_json::json!({"n": 1}),
        };
        let second = DeliveryEvent::VolatileRoomEvent {
            room_id: "room-a".to_string(),
            name: "tick".to_string(),
            payload: serde_json::json!({"n": 2}),
        };
        let borrowed = encode_delivery_events_frame(&[&first, &second])
            .expect("encode borrowed events")
            .expect("non-empty frame");
        let owned = encode_server_frame_bytes(&ServerFrame::Events {
            events: vec![first, second],
        })
        .expect("encode owned events");

        assert_eq!(borrowed.json(), &owned);
    }

    #[tokio::test]
    async fn channel_members_are_session_scoped_for_same_user() {
        let channels = RealtimeChannels::default();
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("phone".to_string()),
                serde_json::json!({"device": "phone"}),
            )
            .await;
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("desktop".to_string()),
                serde_json::json!({"device": "desktop"}),
            )
            .await;

        let members = channels.members("call").await;
        assert_eq!(members.len(), 2);
        assert_eq!(unique_member_user_ids(&members), vec!["alice".to_string()]);

        let leave = channels.leave("call", "alice", Some("phone")).await;
        assert_eq!(leave.removed.len(), 1);
        assert_eq!(leave.remaining.len(), 1);
        assert_eq!(leave.remaining[0].session_id.as_deref(), Some("desktop"));
        assert!(channels.has_member("call", "alice").await);

        let leave = channels.leave("call", "alice", None).await;
        assert_eq!(leave.removed.len(), 1);
        assert!(leave.remaining.is_empty());
        assert!(!channels.has_member("call", "alice").await);
    }

    #[tokio::test]
    async fn channel_members_are_removed_when_session_leaves() {
        let channels = RealtimeChannels::default();
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("phone".to_string()),
                serde_json::json!({"device": "phone"}),
            )
            .await;
        channels
            .join(
                "game".to_string(),
                "alice".to_string(),
                Some("phone".to_string()),
                serde_json::json!({"device": "phone"}),
            )
            .await;
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("desktop".to_string()),
                serde_json::json!({"device": "desktop"}),
            )
            .await;

        let mut leaves = channels.leave_session("alice", "phone").await;
        leaves.sort_by(|left, right| left.channel_id.cmp(&right.channel_id));

        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].channel_id, "call");
        assert_eq!(leaves[0].removed[0].session_id.as_deref(), Some("phone"));
        assert_eq!(leaves[0].remaining.len(), 1);
        assert_eq!(
            leaves[0].remaining[0].session_id.as_deref(),
            Some("desktop")
        );
        assert_eq!(leaves[1].channel_id, "game");
        assert!(leaves[1].remaining.is_empty());
        assert_eq!(channels.status().await.channel_count, 1);
    }

    #[tokio::test]
    async fn channel_member_metadata_updates_are_session_scoped() {
        let channels = RealtimeChannels::default();
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("phone".to_string()),
                serde_json::json!({"device": "phone", "muted": false}),
            )
            .await;
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("desktop".to_string()),
                serde_json::json!({"device": "desktop", "muted": false}),
            )
            .await;

        let update = channels
            .update_member(
                "call",
                "alice",
                Some("phone"),
                serde_json::json!({"device": "phone", "muted": true}),
            )
            .await
            .expect("phone member should exist");
        assert_eq!(update.member.session_id.as_deref(), Some("phone"));
        assert_eq!(update.member.metadata["muted"], true);
        assert_eq!(update.members.len(), 2);

        let members = channels.members("call").await;
        let desktop = members
            .iter()
            .find(|member| member.session_id.as_deref() == Some("desktop"))
            .expect("desktop member should remain");
        assert_eq!(desktop.metadata["muted"], false);

        assert!(
            channels
                .update_member("call", "alice", Some("missing"), serde_json::json!({}))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn channel_member_metadata_can_update_all_sessions_for_user() {
        let channels = RealtimeChannels::default();
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("phone".to_string()),
                serde_json::json!({"ready": false}),
            )
            .await;
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("desktop".to_string()),
                serde_json::json!({"ready": false}),
            )
            .await;
        channels
            .join(
                "call".to_string(),
                "bob".to_string(),
                Some("laptop".to_string()),
                serde_json::json!({"ready": false}),
            )
            .await;

        let update = channels
            .update_user_members("call", "alice", serde_json::json!({"ready": true}))
            .await
            .expect("alice members should exist");
        assert_eq!(update.updated.len(), 2);
        assert_eq!(update.members.len(), 3);
        assert!(
            update
                .updated
                .iter()
                .all(|member| member.metadata["ready"] == true)
        );

        let members = channels.members("call").await;
        let bob = members
            .iter()
            .find(|member| member.user_id == "bob")
            .expect("bob member should remain");
        assert_eq!(bob.metadata["ready"], false);

        assert!(
            channels
                .update_user_members("call", "missing", serde_json::json!({}))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn channel_state_is_versioned_and_cas_checked() {
        let channels = RealtimeChannels::default();

        let initial = channels.state("call").await;
        assert_eq!(initial.version, 0);
        assert_eq!(initial.state, serde_json::Value::Null);

        let updated = channels
            .update_state("call", serde_json::json!({"phase": "lobby"}), Some(0))
            .await
            .expect("initial state update should pass");
        assert_eq!(updated.version, 1);
        assert_eq!(updated.state["phase"], "lobby");

        let conflict = channels
            .update_state("call", serde_json::json!({"phase": "started"}), Some(0))
            .await
            .expect_err("stale state update should conflict");
        assert_eq!(conflict.expected_version, Some(0));
        assert_eq!(conflict.current.version, 1);

        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("phone".to_string()),
                serde_json::json!({}),
            )
            .await;
        let summaries = channels.list_channels().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].state_version, 1);
        assert_eq!(
            summaries[0].state_updated_at_ms,
            Some(updated.updated_at_ms)
        );
    }

    #[tokio::test]
    async fn empty_channel_removes_runtime_state_and_sequence() {
        let channels = RealtimeChannels::default();
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("phone".to_string()),
                serde_json::json!({}),
            )
            .await;
        channels.next_sequence("call").await;
        channels
            .update_state("call", serde_json::json!({"phase": "lobby"}), Some(0))
            .await
            .expect("state update should pass");
        let before = channels.status().await;
        assert_eq!(before.channel_count, 1);
        assert_eq!(before.state_count, 1);
        assert_eq!(before.sequence_count, 1);

        let leave = channels.leave("call", "alice", Some("phone")).await;

        assert_eq!(leave.removed.len(), 1);
        assert!(leave.remaining.is_empty());
        let after = channels.status().await;
        assert_eq!(after.channel_count, 0);
        assert_eq!(after.state_count, 0);
        assert_eq!(after.sequence_count, 0);
        assert_eq!(after.maintenance.total_orphan_states_removed, 1);
        assert_eq!(after.maintenance.total_orphan_sequences_removed, 1);
        assert_eq!(channels.state("call").await.version, 0);
        assert_eq!(channels.next_sequence("call").await, 1);
    }

    #[tokio::test]
    async fn orphan_runtime_state_sweep_removes_state_without_members() {
        let channels = RealtimeChannels::default();
        channels.next_sequence("orphan").await;
        channels
            .update_state("orphan", serde_json::json!({"phase": "old"}), Some(0))
            .await
            .expect("state update should pass");
        assert_eq!(channels.status().await.state_count, 1);
        assert_eq!(channels.status().await.sequence_count, 1);

        let removed = channels.cleanup_orphan_runtime_state().await;

        assert_eq!(removed, (1, 1));
        let after = channels.status().await;
        assert_eq!(after.channel_count, 0);
        assert_eq!(after.state_count, 0);
        assert_eq!(after.sequence_count, 0);
        assert_eq!(after.maintenance.last_orphan_states_removed, 1);
        assert_eq!(after.maintenance.last_orphan_sequences_removed, 1);
    }

    #[tokio::test]
    async fn inactive_session_member_cleanup_removes_stale_members() {
        let channels = RealtimeChannels::default();
        channels
            .join(
                "call".to_string(),
                "alice".to_string(),
                Some("live".to_string()),
                serde_json::json!({}),
            )
            .await;
        channels
            .join(
                "call".to_string(),
                "bob".to_string(),
                Some("stale".to_string()),
                serde_json::json!({}),
            )
            .await;
        channels
            .join(
                "solo".to_string(),
                "carol".to_string(),
                Some("stale".to_string()),
                serde_json::json!({}),
            )
            .await;
        channels.next_sequence("solo").await;
        channels
            .update_state("solo", serde_json::json!({"phase": "old"}), Some(0))
            .await
            .expect("state update should pass");

        let active = BTreeSet::from([("alice".to_string(), "live".to_string())]);
        let mut leaves = channels.cleanup_inactive_session_members(&active).await;
        leaves.sort_by(|left, right| left.channel_id.cmp(&right.channel_id));

        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].channel_id, "call");
        assert_eq!(leaves[0].removed[0].user_id, "bob");
        assert_eq!(leaves[0].remaining.len(), 1);
        assert_eq!(leaves[1].channel_id, "solo");
        assert!(leaves[1].remaining.is_empty());
        let status = channels.status().await;
        assert_eq!(status.channel_count, 1);
        assert_eq!(status.state_count, 0);
        assert_eq!(status.sequence_count, 0);
        assert_eq!(status.maintenance.total_stale_members_removed, 2);
        assert_eq!(status.maintenance.total_orphan_states_removed, 1);
        assert_eq!(status.maintenance.total_orphan_sequences_removed, 1);
    }
}
