use std::collections::VecDeque;
use std::io::Write;
use std::path::PathBuf;

use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tracing::warn;

use super::LocalThreadStore;
use super::helpers::rollout_path_is_archived;
use super::helpers::scoped_rollout_path;
use super::read_thread::resolve_rollout_path;
use super::rollout_lineage::RolloutLineageSegment;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::WriteReferenceLogicalAttachmentOutcome;
use crate::WriteReferenceLogicalAttachmentParams;

/// Write a logical history attachment from an already loaded history.
pub fn write_reference_logical_attachment_from_items(
    output_path: PathBuf,
    items: Vec<RolloutItem>,
    max_bytes: usize,
) -> std::io::Result<WriteReferenceLogicalAttachmentOutcome> {
    let mut writer = BlockingAttachmentWriter::new(output_path, max_bytes)?;
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut items = items.into_iter();
    let Some(RolloutItem::SessionMeta(mut child_meta)) = items.next() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "logical reference history must begin with SessionMeta",
        ));
    };
    child_meta.meta.history_base = None;
    let header = RolloutLine {
        timestamp: timestamp.clone(),
        ordinal: None,
        item: RolloutItem::SessionMeta(child_meta),
    };
    let bytes = serde_json::to_vec(&header).map_err(std::io::Error::other)?;
    writer.push_header_line(bytes.as_slice())?;
    for item in items {
        if matches!(item, RolloutItem::SessionMeta(_)) {
            continue;
        }
        let line = RolloutLine {
            timestamp: timestamp.clone(),
            ordinal: None,
            item,
        };
        let bytes = serde_json::to_vec(&line).map_err(std::io::Error::other)?;
        writer.push_line(bytes.as_slice())?;
    }
    let truncated = writer.finish()?;
    Ok(WriteReferenceLogicalAttachmentOutcome::Written { truncated })
}

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

    let path = resolve_rollout_path(store, params.thread_id, params.include_archived)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: params.thread_id,
        })?;
    let path = codex_rollout::existing_rollout_path(path.as_path())
        .await
        .unwrap_or(path);
    let path = scoped_rollout_path(
        store.config.codex_home.clone(),
        path.as_path(),
        "Codex home",
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
    if !super::read_thread::reference_rollout_path_is_managed(
        store,
        params.thread_id,
        path.as_path(),
    )
    .await?
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "reference rollout for thread {} must resolve to its managed Codex home path",
                params.thread_id
            ),
        });
    }

    let (lineage, _writer_guards) = store
        .resolve_rollout_lineage_for_reference_attachment(params.thread_id)
        .await?;

    // Keep the process-local writer mutexes for every segment through the raw stream. The
    // resolver validates the snapshot under the same locks; retaining them here prevents a live
    // append from changing the source while the bounded attachment is being assembled.
    let mut _local_writer_guards = Vec::with_capacity(lineage.segments().len());
    for segment in lineage.segments().iter().rev() {
        _local_writer_guards.push(store.live_writer_locks.lock(segment.thread_id()).await);
    }

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

    let last_segment_index = lineage.segments().len().saturating_sub(1);
    for (segment_index, segment) in lineage.segments().iter().enumerate() {
        stream_segment(segment, segment_index, last_segment_index, &mut writer).await?;
    }
    let truncated = writer.finish().await?;
    Ok(WriteReferenceLogicalAttachmentOutcome::Written { truncated })
}

async fn stream_segment(
    segment: &RolloutLineageSegment,
    _segment_index: usize,
    _last_segment_index: usize,
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
    let mut uncompressed_offset = 0u64;
    while let Some(line) =
        reader
            .next_raw_line()
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!(
                    "failed to read rollout {} for reference attachment: {err}",
                    segment.rollout_path.display()
                ),
            })?
    {
        if !line.ends_with(b"\n") {
            break;
        }
        let next_offset = uncompressed_offset.saturating_add(line.len() as u64);
        if segment
            .end
            .is_some_and(|end| next_offset > end.end_byte_offset)
        {
            break;
        }
        uncompressed_offset = next_offset;
        let mut value = match serde_json::from_slice::<Value>(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if codex_rollout::strip_legacy_ghost_snapshot_rollout_line(&mut value) {
            continue;
        }
        if value.get("type").and_then(Value::as_str) == Some("session_meta") {
            continue;
        }
        writer.push_line(line.as_slice()).await?;
    }
    Ok(())
}

struct BlockingAttachmentWriter {
    file: std::fs::File,
    max_bytes: usize,
    head_budget: usize,
    head_bytes: usize,
    head_full: bool,
    tail_budget: usize,
    tail_bytes: usize,
    tail_lines: VecDeque<Vec<u8>>,
    truncated: bool,
}

impl BlockingAttachmentWriter {
    fn new(path: PathBuf, max_bytes: usize) -> std::io::Result<Self> {
        if max_bytes == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_bytes must be positive",
            ));
        }
        Ok(Self {
            file: std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path)?,
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

    fn push_line(&mut self, line: &[u8]) -> std::io::Result<()> {
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
            self.file.write_all(line.as_slice())?;
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

    fn push_header_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        let line_len = line.len() + usize::from(!line.ends_with(b"\n"));
        if line_len > self.head_budget || line_len > self.max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "logical reference SessionMeta exceeds attachment head budget",
            ));
        }
        self.push_line(line)
    }

    fn finish(mut self) -> std::io::Result<bool> {
        for line in self.tail_lines {
            self.file.write_all(line.as_slice())?;
        }
        self.file.flush()?;
        if self.truncated {
            warn!("logical reference attachment was truncated to its head and tail");
        }
        Ok(self.truncated)
    }
}

struct AsyncAttachmentWriter {
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
    async fn new(path: PathBuf, max_bytes: usize) -> ThreadStoreResult<Self> {
        if max_bytes == 0 {
            return Err(ThreadStoreError::InvalidRequest {
                message: "reference logical attachment max_bytes must be positive".to_string(),
            });
        }
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

    async fn push_line(&mut self, line: &[u8]) -> ThreadStoreResult<()> {
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

    async fn push_header_line(&mut self, line: &[u8]) -> ThreadStoreResult<()> {
        let line_len = line.len() + usize::from(!line.ends_with(b"\n"));
        if line_len > self.head_budget || line_len > self.max_bytes {
            return Err(ThreadStoreError::InvalidRequest {
                message: "logical reference SessionMeta exceeds attachment head budget".to_string(),
            });
        }
        self.push_line(line).await
    }

    async fn finish(mut self) -> ThreadStoreResult<bool> {
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
