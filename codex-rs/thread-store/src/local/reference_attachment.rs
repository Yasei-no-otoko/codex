use std::collections::VecDeque;
use std::path::PathBuf;

use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::de::DeserializeSeed;
use serde::de::IgnoredAny;
use serde::de::MapAccess;
use serde::de::Visitor;
use tokio::io::AsyncWriteExt;
use tracing::warn;

use super::LocalThreadStore;
use super::helpers::managed_rollout_path;
use super::helpers::rollout_path_is_archived;
use super::rollout_lineage::RolloutLineageSegment;
use super::thread_rollout_resolver;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::WriteReferenceLogicalAttachmentOutcome;
use crate::WriteReferenceLogicalAttachmentParams;

/// Write a cold reference-backed history without materializing compressed ancestors.
pub(super) async fn write_reference_logical_attachment(
    store: &LocalThreadStore,
    params: WriteReferenceLogicalAttachmentParams,
) -> ThreadStoreResult<WriteReferenceLogicalAttachmentOutcome> {
    if params.max_bytes == 0 {
        return Err(ThreadStoreError::InvalidRequest {
            message: "reference logical attachment max_bytes must be positive".to_string(),
        });
    }

    // Reserve the source before resolving its path or metadata. The lifecycle lease blocks
    // archive/delete and the stable filesystem guard closes the cross-process rename window.
    let source_guards = store.acquire_fork_source_guards(params.thread_id).await?;
    let super::ForkSourceGuards {
        lifecycle: _source_lifecycle_guard,
        filesystem: source_filesystem_guard,
    } = source_guards;
    let source_filesystem_guard_for_resolver = source_filesystem_guard.clone();
    let resolved =
        thread_rollout_resolver::resolve_current_including_archived(store, params.thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound {
                thread_id: params.thread_id,
            })?;
    let path = codex_rollout::existing_rollout_path(resolved.path.as_path())
        .await
        .unwrap_or(resolved.path);
    let path = managed_rollout_path(
        store.config.codex_home.as_path(),
        path.as_path(),
        resolved.rollout_id,
    )?;
    if !params.include_archived
        && rollout_path_is_archived(store.config.codex_home.as_path(), path.as_path())
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {} is archived", params.thread_id),
        });
    }

    let requested_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read session metadata {}: {err}", path.display()),
        })?;
    if requested_meta.meta.id != params.thread_id {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout {} belongs to thread {}, not {}",
                path.display(),
                requested_meta.meta.id,
                params.thread_id
            ),
        });
    }
    if requested_meta.meta.history_base.is_none() {
        return Ok(WriteReferenceLogicalAttachmentOutcome::NotReference);
    }

    let (lineage, _ancestor_filesystem_guards) = store
        .resolve_rollout_lineage_for_reference_attachment(
            params.thread_id,
            source_filesystem_guard_for_resolver,
        )
        .await?;

    let mut writer = AsyncAttachmentWriter::new(params.output_path, params.max_bytes).await?;
    let mut child_meta = requested_meta;
    child_meta.meta.history_base = None;
    let header = RolloutLine {
        timestamp: child_meta.meta.timestamp.clone(),
        ordinal: None,
        item: RolloutItem::SessionMeta(child_meta),
    };
    let header = serde_json::to_vec(&header).map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to serialize reference attachment header: {err}"),
    })?;
    writer.push_header_line(header.as_slice()).await?;

    for segment in lineage.segments() {
        stream_segment(segment, &mut writer).await?;
    }
    let truncated = writer.finish().await?;
    Ok(WriteReferenceLogicalAttachmentOutcome::Written { truncated })
}

pub(super) async fn stream_segment(
    segment: &RolloutLineageSegment,
    writer: &mut AsyncAttachmentWriter,
) -> ThreadStoreResult<()> {
    let mut reader = codex_rollout::open_rollout_raw_line_reader(segment.rollout_path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to open rollout {} for reference attachment: {err}",
                segment.rollout_path.display()
            ),
        })?;
    let mut offset = 0_u64;
    while let Some(record) = reader
        .next_raw_line_limited(writer.max_bytes())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read rollout {} for reference attachment: {err}",
                segment.rollout_path.display()
            ),
        })?
    {
        let (line, byte_count, terminated, oversized) = match record {
            codex_rollout::RawRolloutLine::Complete(line) => {
                let byte_count = line.len();
                let terminated = line.ends_with(b"\n");
                (Some(line), byte_count, terminated, false)
            }
            codex_rollout::RawRolloutLine::Oversized {
                byte_count,
                terminated,
            } => (None, byte_count, terminated, true),
        };
        let next_offset = offset.saturating_add(byte_count as u64);
        if segment
            .end
            .is_some_and(|end| next_offset > end.end_byte_offset)
        {
            break;
        }
        offset = next_offset;
        if oversized {
            writer.mark_truncated();
        }
        if !terminated {
            break;
        }
        let Some(line) = line else {
            if segment.end.is_some_and(|end| offset == end.end_byte_offset) {
                break;
            }
            continue;
        };
        let Some(envelope) = envelope_kind(&line) else {
            continue;
        };
        if envelope.kind == "session_meta" || envelope.ghost_snapshot {
            continue;
        }
        writer.push_line(line.as_slice()).await?;
    }
    Ok(())
}

/// Validate only the rollout envelope. Payload values are visited as `IgnoredAny`, so a large
/// response object is parsed and discarded rather than copied into a `serde_json::Value`.
pub(super) fn envelope_kind(line: &[u8]) -> Option<EnvelopeInfo> {
    serde_json::from_slice::<EnvelopeKind>(line)
        .ok()
        .and_then(|kind| kind.0)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct EnvelopeInfo {
    pub(super) kind: String,
    pub(super) ghost_snapshot: bool,
}

struct EnvelopeKind(Option<EnvelopeInfo>);
struct EnvelopeVisitor;
struct ObjectSeed;
struct ObjectVisitor;

impl<'de> serde::Deserialize<'de> for EnvelopeKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EnvelopeVisitor)
    }
}

impl<'de> Visitor<'de> for EnvelopeVisitor {
    type Value = EnvelopeKind;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON rollout envelope object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut timestamp = false;
        let mut kind = None;
        let mut payload_seen = false;
        let mut payload_kind = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "timestamp" => {
                    map.next_value::<String>()?;
                    timestamp = true;
                }
                "type" => {
                    kind = Some(map.next_value::<String>()?);
                }
                "payload" => {
                    payload_kind = map.next_value_seed(ObjectSeed)?;
                    payload_seen = true;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(EnvelopeKind(
            (timestamp && payload_seen)
                .then_some(kind.map(|kind| EnvelopeInfo {
                    ghost_snapshot: kind == "response_item"
                        && payload_kind.as_deref() == Some("ghost_snapshot"),
                    kind,
                }))
                .flatten(),
        ))
    }
}

impl<'de> DeserializeSeed<'de> for ObjectSeed {
    type Value = Option<String>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ObjectVisitor)
    }
}

impl<'de> Visitor<'de> for ObjectVisitor {
    type Value = Option<String>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut kind = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "type" {
                kind = Some(map.next_value::<String>()?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(kind)
    }
}

pub(super) struct AsyncAttachmentWriter {
    file: tokio::fs::File,
    max_bytes: usize,
    head_budget: usize,
    head_bytes: usize,
    head_full: bool,
    tail_budget: usize,
    tail_bytes: usize,
    tail_lines: VecDeque<Vec<u8>>,
    truncated: bool,
}

impl AsyncAttachmentWriter {
    pub(super) async fn new(path: PathBuf, max_bytes: usize) -> ThreadStoreResult<Self> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to create reference attachment: {err}"),
            })?;
        Ok(Self {
            file,
            max_bytes,
            head_budget: max_bytes / 2,
            head_bytes: 0,
            head_full: false,
            tail_budget: max_bytes - max_bytes / 2,
            tail_bytes: 0,
            tail_lines: VecDeque::new(),
            truncated: false,
        })
    }

    pub(super) async fn push_line(&mut self, line: &[u8]) -> ThreadStoreResult<()> {
        let mut line = line.to_vec();
        if !line.ends_with(b"\n") {
            line.push(b'\n');
        }
        if line.len() > self.max_bytes {
            self.truncated = true;
            return Ok(());
        }
        if !self.head_full && self.head_bytes.saturating_add(line.len()) <= self.head_budget {
            self.head_bytes = self.head_bytes.saturating_add(line.len());
            self.file.write_all(line.as_slice()).await.map_err(|err| {
                ThreadStoreError::Internal {
                    message: format!("failed to write reference attachment: {err}"),
                }
            })?;
            return Ok(());
        }
        self.head_full = true;
        self.tail_bytes = self.tail_bytes.saturating_add(line.len());
        self.tail_lines.push_back(line);
        while self.tail_bytes > self.tail_budget {
            let Some(removed) = self.tail_lines.pop_front() else {
                break;
            };
            self.tail_bytes = self.tail_bytes.saturating_sub(removed.len());
            self.truncated = true;
        }
        Ok(())
    }

    pub(super) fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub(super) fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    pub(super) async fn push_header_line(&mut self, line: &[u8]) -> ThreadStoreResult<()> {
        let line_len = line.len() + usize::from(!line.ends_with(b"\n"));
        if line_len > self.head_budget || line_len > self.max_bytes {
            return Err(ThreadStoreError::InvalidRequest {
                message: "logical reference SessionMeta exceeds attachment head budget".to_string(),
            });
        }
        self.push_line(line).await
    }

    pub(super) async fn finish(mut self) -> ThreadStoreResult<bool> {
        for line in self.tail_lines {
            self.file.write_all(line.as_slice()).await.map_err(|err| {
                ThreadStoreError::Internal {
                    message: format!("failed to finalize reference attachment: {err}"),
                }
            })?;
        }
        self.file
            .flush()
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to flush reference attachment: {err}"),
            })?;
        if self.truncated {
            warn!("logical reference attachment was truncated to its head and tail");
        }
        Ok(self.truncated)
    }
}
