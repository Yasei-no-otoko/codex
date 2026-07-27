use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;

use super::LocalThreadStore;
use super::read_thread;
use super::writer_lock::WriterLockGuard;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

/// One physical rollout range contributing to a logical paginated history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RolloutLineageSegment {
    pub(super) thread_id: ThreadId,
    pub(super) rollout_path: PathBuf,
    pub(super) start_ordinal: u64,
    pub(super) end: Option<HistoryPosition>,
}

/// Ordered physical rollout ranges contributing to one logical forked history.
///
/// This is the only local abstraction that follows SessionMeta.history_base pointers. Readers
/// consume its bounded physical segments without resolving or mutating fork pointers themselves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RolloutLineage {
    pub(super) segments: Vec<RolloutLineageSegment>,
    pub(super) history_mode: ThreadHistoryMode,
}

impl LocalThreadStore {
    pub(super) async fn resolve_rollout_lineage(
        &self,
        requested_thread_id: ThreadId,
    ) -> ThreadStoreResult<RolloutLineage> {
        self.resolve_rollout_lineage_with_representation(
            requested_thread_id,
            LineageRepresentation::Existing,
        )
        .await
    }

    /// Resolve a reference lineage while holding every cross-process writer lock until the
    /// caller finishes consuming the physical segments. Compression and delete/archive use the
    /// same locks, so a compressed ancestor cannot disappear after it is materialized but before
    /// a reverse/history scan opens it.
    pub(super) async fn resolve_rollout_lineage_for_reference_locked(
        &self,
        requested_thread_id: ThreadId,
    ) -> ThreadStoreResult<(RolloutLineage, Vec<WriterLockGuard>)> {
        self.resolve_rollout_lineage_with_representation_and_locks(
            requested_thread_id,
            LineageRepresentation::PlainForReference,
            None,
            false,
        )
        .await
    }

    /// Variant for callers that already hold the immediate source's filesystem lock. The guard
    /// is cloned only for the resolver; the original remains owned by the caller and can be kept
    /// through child durability (legacy latest forks do this).
    pub(super) async fn resolve_rollout_lineage_for_reference_locked_with_source_guard(
        &self,
        requested_thread_id: ThreadId,
        source_guard: WriterLockGuard,
    ) -> ThreadStoreResult<(RolloutLineage, Vec<WriterLockGuard>)> {
        self.resolve_rollout_lineage_with_representation_and_locks(
            requested_thread_id,
            LineageRepresentation::PlainForReference,
            Some((requested_thread_id, source_guard)),
            false,
        )
        .await
    }

    /// Resolve a reference lineage while the caller owns the immediate source's local writer
    /// mutex. The source filesystem guard is explicitly cloned for the resolver; the caller keeps
    /// both tokens through recorder initialization. Ancestors still acquire their own local and
    /// filesystem guards normally.
    pub(super) async fn resolve_rollout_lineage_for_reference_locked_with_source_tokens(
        &self,
        requested_thread_id: ThreadId,
        source_guard: WriterLockGuard,
    ) -> ThreadStoreResult<(RolloutLineage, Vec<WriterLockGuard>)> {
        self.resolve_rollout_lineage_with_representation_and_locks(
            requested_thread_id,
            LineageRepresentation::PlainForReference,
            Some((requested_thread_id, source_guard)),
            true,
        )
        .await
    }

    async fn resolve_rollout_lineage_with_representation(
        &self,
        requested_thread_id: ThreadId,
        representation: LineageRepresentation,
    ) -> ThreadStoreResult<RolloutLineage> {
        let (lineage, _writer_guards) = self
            .resolve_rollout_lineage_with_representation_and_locks(
                requested_thread_id,
                representation,
                None,
                false,
            )
            .await?;
        Ok(lineage)
    }

    async fn resolve_rollout_lineage_with_representation_and_locks(
        &self,
        requested_thread_id: ThreadId,
        representation: LineageRepresentation,
        prelocked_source: Option<(ThreadId, WriterLockGuard)>,
        prelocked_source_local: bool,
    ) -> ThreadStoreResult<(RolloutLineage, Vec<WriterLockGuard>)> {
        let mut segments = Vec::new();
        let mut seen = HashSet::new();
        let mut thread_id = requested_thread_id;
        let mut end = None;
        let mut history_mode = None;
        let mut writer_guards = Vec::new();

        loop {
            if !seen.insert(thread_id) {
                return Err(malformed_lineage(requested_thread_id, "cycle detected"));
            }
            let source_local_is_prelocked =
                prelocked_source_local && thread_id == requested_thread_id;
            let _local_writer_guard = match representation {
                LineageRepresentation::Existing => None,
                // The process-local lock serializes this store's live recorder while its path and
                // metadata are resolved. The filesystem guard below closes the cross-process
                // compression/delete/archive window.
                LineageRepresentation::PlainForReference if source_local_is_prelocked => None,
                LineageRepresentation::PlainForReference => {
                    Some(self.live_writer_locks.lock(thread_id).await)
                }
            };
            let _writer_guard = match representation {
                LineageRepresentation::Existing => None,
                LineageRepresentation::PlainForReference => {
                    let prelocked = prelocked_source
                        .as_ref()
                        .filter(|(prelocked_thread_id, _)| *prelocked_thread_id == thread_id)
                        .map(|(_, guard)| guard.clone());
                    if let Some(prelocked) = prelocked {
                        Some(prelocked)
                    } else if let Some(existing) = self.existing_writer_lock(thread_id).await {
                        // Live recorders explicitly share their guard with readers. Maintenance
                        // still acquires a fresh lock and conflicts while this scan owns the path.
                        writer_guards.push(existing.clone());
                        Some(existing)
                    } else {
                        let guard = self.writer_lock_coordinator.acquire(thread_id)?;
                        writer_guards.push(guard.clone());
                        Some(guard)
                    }
                }
            };
            let rollout_path =
                read_thread::resolve_rollout_path(self, thread_id, /*include_archived*/ true)
                    .await?
                    .ok_or_else(|| malformed_lineage(thread_id, "missing source rollout"))?;
            let rollout_path = match representation {
                LineageRepresentation::Existing => rollout_path,
                LineageRepresentation::PlainForReference => {
                    let rollout_path = super::helpers::scoped_rollout_path(
                        self.config.codex_home.clone(),
                        rollout_path.as_path(),
                        "Codex home",
                    )?;
                    codex_rollout::materialize_rollout_for_reference(rollout_path.as_path())
                        .await
                        .map_err(|err| ThreadStoreError::Internal {
                            message: format!(
                                "failed to materialize referenced rollout {}: {err}",
                                rollout_path.display()
                            ),
                        })?
                }
            };
            let meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!(
                        "failed to read lineage metadata {}: {err}",
                        rollout_path.display()
                    ),
                })?;
            if meta.meta.id != thread_id {
                return Err(malformed_lineage(
                    requested_thread_id,
                    "source rollout belongs to another thread",
                ));
            }
            if let Some(expected_mode) = history_mode {
                if meta.meta.history_mode != expected_mode {
                    return Err(malformed_lineage(
                        requested_thread_id,
                        "source rollout mixes legacy and paginated history",
                    ));
                }
            } else {
                history_mode = Some(meta.meta.history_mode);
            }
            if let Some(end) = end {
                validate_cutoff_bounds(
                    requested_thread_id,
                    rollout_path.as_path(),
                    &end,
                    meta.meta.history_mode,
                )
                .await?;
            }
            let start_ordinal = match meta.meta.history_mode {
                ThreadHistoryMode::Legacy => 0,
                ThreadHistoryMode::Paginated => match meta.meta.history_base {
                    Some(base) => base.end_ordinal_exclusive.checked_add(1).ok_or_else(|| {
                        malformed_lineage(requested_thread_id, "source ordinal overflow")
                    })?,
                    None => 1,
                },
            };
            segments.push(RolloutLineageSegment {
                thread_id,
                rollout_path,
                start_ordinal,
                end,
            });

            let Some(base) = meta.meta.history_base else {
                break;
            };
            thread_id = base.thread_id;
            end = Some(base);
        }

        segments.reverse();
        Ok((
            RolloutLineage {
                segments,
                history_mode: history_mode.unwrap_or(ThreadHistoryMode::Legacy),
            },
            writer_guards,
        ))
    }
}

#[derive(Clone, Copy)]
enum LineageRepresentation {
    Existing,
    PlainForReference,
}

impl RolloutLineage {
    pub(super) fn segments(&self) -> &[RolloutLineageSegment] {
        self.segments.as_slice()
    }

    pub(super) fn history_mode(&self) -> ThreadHistoryMode {
        self.history_mode
    }

    pub(super) fn segment_index_for_ordinal(&self, ordinal: u64) -> Option<usize> {
        self.segments.iter().position(|segment| {
            ordinal >= segment.start_ordinal()
                && segment
                    .end_ordinal()
                    .is_none_or(|end_ordinal| ordinal < end_ordinal)
        })
    }

    pub(super) async fn truncate_at(
        mut self,
        end: HistoryPosition,
    ) -> ThreadStoreResult<RolloutLineage> {
        let segment_index = self
            .segments
            .iter()
            .position(|segment| segment.thread_id == end.thread_id)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "fork position is outside the source lineage".to_string(),
            })?;
        self.segments.truncate(segment_index + 1);
        let segment = self
            .segments
            .last_mut()
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "rollout lineage has no segments".to_string(),
            })?;
        validate_cutoff_bounds(
            end.thread_id,
            segment.rollout_path.as_path(),
            &end,
            self.history_mode,
        )
        .await?;
        segment.end = Some(end);
        Ok(self)
    }
}

impl RolloutLineageSegment {
    pub(super) fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    pub(super) fn start_ordinal(&self) -> u64 {
        self.start_ordinal
    }

    pub(super) fn end_ordinal(&self) -> Option<u64> {
        self.end.map(|end| end.end_ordinal_exclusive)
    }
}

async fn validate_cutoff_bounds(
    requested_thread_id: ThreadId,
    rollout_path: &Path,
    end: &HistoryPosition,
    history_mode: ThreadHistoryMode,
) -> ThreadStoreResult<()> {
    if matches!(history_mode, ThreadHistoryMode::Paginated) && end.end_ordinal_exclusive == 0 {
        return Err(malformed_lineage(
            requested_thread_id,
            "cutoff cannot include source session metadata",
        ));
    }
    if matches!(history_mode, ThreadHistoryMode::Legacy) && end.end_ordinal_exclusive != 0 {
        return Err(malformed_lineage(
            requested_thread_id,
            "legacy cutoff must use the zero ordinal sentinel",
        ));
    }
    let file_len = tokio::fs::metadata(rollout_path)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read lineage metadata {}: {err}",
                rollout_path.display()
            ),
        })?
        .len();
    if end.end_byte_offset > file_len {
        return Err(malformed_lineage(
            requested_thread_id,
            "cutoff byte offset is past the source rollout",
        ));
    }
    match history_mode {
        ThreadHistoryMode::Legacy => {
            let last_complete_offset = super::legacy_fork::last_complete_jsonl_offset_at_or_before(
                rollout_path,
                end.end_byte_offset,
            )
            .await?;
            if last_complete_offset != end.end_byte_offset {
                return Err(malformed_lineage(
                    requested_thread_id,
                    "cutoff byte offset is not at a complete JSONL record",
                ));
            }
        }
        ThreadHistoryMode::Paginated => {
            validate_newline_boundary(requested_thread_id, rollout_path, end.end_byte_offset)
                .await?;
        }
    }
    Ok(())
}

async fn validate_newline_boundary(
    requested_thread_id: ThreadId,
    rollout_path: &Path,
    end_byte_offset: u64,
) -> ThreadStoreResult<()> {
    if end_byte_offset == 0 {
        return Err(malformed_lineage(
            requested_thread_id,
            "paginated cutoff must end after a JSONL newline",
        ));
    }
    let mut file =
        tokio::fs::File::open(rollout_path)
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!(
                    "failed to open lineage rollout {}: {err}",
                    rollout_path.display()
                ),
            })?;
    file.seek(SeekFrom::Start(end_byte_offset - 1))
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to seek lineage rollout {}: {err}",
                rollout_path.display()
            ),
        })?;
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read lineage cutoff {}: {err}",
                rollout_path.display()
            ),
        })?;
    if byte[0] != b'\n' {
        return Err(malformed_lineage(
            requested_thread_id,
            "paginated cutoff must end after a JSONL newline",
        ));
    }
    Ok(())
}

fn malformed_lineage(thread_id: ThreadId, detail: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid rollout history lineage for {thread_id}: {detail}"),
    }
}

#[cfg(test)]
#[path = "rollout_lineage_tests.rs"]
mod tests;
