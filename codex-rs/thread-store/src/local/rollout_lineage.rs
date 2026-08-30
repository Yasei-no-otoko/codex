use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;

use super::LocalThreadStore;
use super::legacy_envelope;
use super::thread_rollout_resolver;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// One immutable rollout range contributing to a paginated thread's history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RolloutLineageSegment {
    pub(super) rollout_id: ThreadId,
    pub(super) rollout_path: PathBuf,
    pub(super) start_ordinal: u64,
    pub(super) end: Option<HistoryPosition>,
}

/// Ordered rollout ranges contributing to one forked history.
///
/// This is the only local abstraction that follows SessionMeta.history_base pointers. Readers
/// consume its bounded rollout segments without resolving or mutating fork pointers themselves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RolloutLineage {
    pub(super) segments: Vec<RolloutLineageSegment>,
    history_mode: ThreadHistoryMode,
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

    pub(super) async fn resolve_rollout_lineage_for_reference(
        &self,
        requested_thread_id: ThreadId,
    ) -> ThreadStoreResult<RolloutLineage> {
        self.resolve_rollout_lineage_with_representation(
            requested_thread_id,
            LineageRepresentation::PlainForReference,
        )
        .await
    }

    async fn resolve_rollout_lineage_with_representation(
        &self,
        requested_thread_id: ThreadId,
        representation: LineageRepresentation,
    ) -> ThreadStoreResult<RolloutLineage> {
        let mut segments = Vec::new();
        let mut seen = HashSet::new();
        let mut next_rollout_id = None;
        let mut end = None;
        let mut history_mode = None;

        loop {
            let coordination_id = next_rollout_id.unwrap_or(requested_thread_id);
            let _writer_guard = match representation {
                LineageRepresentation::Existing => None,
                LineageRepresentation::PlainForReference => {
                    Some(self.live_writer_locks.lock(coordination_id).await)
                }
            };
            let (rollout_id, rollout_path) = match next_rollout_id {
                Some(rollout_id) => {
                    let rollout_path = resolve_rollout_path_by_id(self, rollout_id)
                        .await?
                        .ok_or_else(|| malformed_lineage(rollout_id, "missing source rollout"))?;
                    (rollout_id, rollout_path)
                }
                None => {
                    let resolved = thread_rollout_resolver::resolve_current_including_archived(
                        self,
                        requested_thread_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        malformed_lineage(requested_thread_id, "missing source rollout")
                    })?;
                    (resolved.rollout_id, resolved.path)
                }
            };
            if !seen.insert(rollout_id) {
                return Err(malformed_lineage(requested_thread_id, "cycle detected"));
            }
            let rollout_path = match representation {
                LineageRepresentation::Existing => rollout_path,
                // Every reference segment, including its leaf, must retain a canonical managed
                // pathname. Otherwise a later copy fallback could preserve only the child's
                // physical suffix and silently discard an inherited ancestor.
                LineageRepresentation::PlainForReference => super::helpers::managed_rollout_path(
                    self.config.codex_home.as_path(),
                    rollout_path.as_path(),
                    rollout_id,
                )?,
            };
            let meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!(
                        "failed to read lineage metadata {}: {err}",
                        rollout_path.display()
                    ),
                })?;
            if meta.meta.id != rollout_id
                || codex_rollout::rollout_id_from_path(rollout_path.as_path()) != Some(rollout_id)
            {
                return Err(malformed_lineage(
                    requested_thread_id,
                    "source rollout path or metadata belongs to another thread",
                ));
            }
            if history_mode
                .is_some_and(|expected| expected != meta.meta.history_mode)
            {
                return Err(malformed_lineage(
                    requested_thread_id,
                    "source rollout mixes history modes",
                ));
            }
            history_mode = Some(meta.meta.history_mode);
            let rollout_path = match representation {
                LineageRepresentation::Existing => rollout_path,
                LineageRepresentation::PlainForReference
                    if next_rollout_id.is_none() && meta.meta.history_base.is_none() =>
                {
                    // A newly shared standalone source must remain readable by older binaries.
                    codex_rollout::materialize_rollout_for_reference(rollout_path.as_path())
                        .await
                        .map_err(|err| ThreadStoreError::Internal {
                            message: format!(
                                "failed to materialize referenced rollout {}: {err}",
                                rollout_path.display()
                            ),
                        })?
                }
                // Already-shared compressed history requires a compatible reader regardless of
                // new forks. Read it without publishing decoded copies into ancestors' folders;
                // their owners may concurrently archive or unarchive those immutable files.
                LineageRepresentation::PlainForReference => rollout_path,
            };
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
                rollout_id,
                rollout_path,
                start_ordinal,
                end,
            });

            let Some(base) = meta.meta.history_base else {
                break;
            };
            next_rollout_id = Some(base.thread_id);
            end = Some(base);
        }

        segments.reverse();
        Ok(RolloutLineage {
            segments,
            history_mode: history_mode.ok_or_else(|| {
                malformed_lineage(requested_thread_id, "source lineage is empty")
            })?,
        })
    }
}

async fn resolve_rollout_path_by_id(
    store: &LocalThreadStore,
    rollout_id: ThreadId,
) -> ThreadStoreResult<Option<PathBuf>> {
    codex_rollout::find_rollout_path_by_rollout_id(store.config.codex_home.as_path(), rollout_id)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to locate rollout {rollout_id}: {err}"),
        })
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
            .position(|segment| segment.rollout_id == end.thread_id)
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
    pub(super) fn rollout_id(&self) -> ThreadId {
        self.rollout_id
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
    let end_byte_offset = end.end_byte_offset;
    let path = rollout_path.to_path_buf();
    if history_mode == ThreadHistoryMode::Legacy {
        let validation_path = path.clone();
        let complete_envelope = tokio::task::spawn_blocking(move || {
            legacy_envelope::validate_rollout_envelope_cutoff(
                validation_path.as_path(),
                end_byte_offset,
            )
        })
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to join legacy cutoff validation: {err}"),
        })?
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to validate legacy cutoff {}: {err}", rollout_path.display()),
        })?;
        if !complete_envelope {
            return Err(malformed_lineage(
                requested_thread_id,
                "cutoff byte offset is not at a complete JSONL record",
            ));
        }
        return Ok(());
    }
    let contains_prefix = tokio::task::spawn_blocking(move || {
        codex_rollout::rollout_contains_prefix(&path, end_byte_offset)
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join rollout prefix validation: {err}"),
    })?
    .map_err(|err| ThreadStoreError::Internal {
        message: format!(
            "failed to read lineage metadata {}: {err}",
            rollout_path.display()
        ),
    })?;
    if !contains_prefix {
        return Err(malformed_lineage(
            requested_thread_id,
            "cutoff byte offset is past the source rollout",
        ));
    }
    Ok(())
}

fn malformed_lineage(thread_id: ThreadId, detail: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid paginated history lineage for {thread_id}: {detail}"),
    }
}

#[cfg(test)]
#[path = "rollout_lineage_tests.rs"]
mod tests;
