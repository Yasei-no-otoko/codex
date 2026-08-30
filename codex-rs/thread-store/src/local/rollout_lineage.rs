use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use serde::de::DeserializeSeed;
use serde::de::IgnoredAny;
use serde::de::MapAccess;
use serde::de::Visitor;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;

use super::LocalThreadStore;
use super::legacy_envelope;
use super::thread_rollout_resolver;
use super::writer_lock::WriterLockGuard;
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
        let (lineage, _guards) = self
            .resolve_rollout_lineage_with_representation_and_guards(
                requested_thread_id,
                LineageRepresentation::PlainForReference,
                None,
            )
            .await?;
        Ok(lineage)
    }

    /// Resolve a reference lineage while retaining guards for every ancestor before its path or
    /// metadata is read. The caller already owns the source guard and keeps it through child
    /// durability; this method only returns guards acquired for inherited segments.
    pub(super) async fn resolve_rollout_lineage_for_reference_locked_with_source_guard(
        &self,
        requested_thread_id: ThreadId,
        source_guard: WriterLockGuard,
    ) -> ThreadStoreResult<(RolloutLineage, Vec<WriterLockGuard>)> {
        self.resolve_rollout_lineage_with_representation_and_guards(
            requested_thread_id,
            LineageRepresentation::PlainForReference,
            Some(source_guard),
        )
        .await
    }

    /// Resolve an attachment lineage while retaining stable filesystem guards for every
    /// immutable source and ancestor. The caller owns the source lifecycle lease and passes the
    /// source filesystem guard in; both remain held by the caller through streaming.
    pub(super) async fn resolve_rollout_lineage_for_reference_attachment(
        &self,
        requested_thread_id: ThreadId,
        source_guard: WriterLockGuard,
    ) -> ThreadStoreResult<(RolloutLineage, Vec<WriterLockGuard>)> {
        self.resolve_rollout_lineage_with_representation_and_guards(
            requested_thread_id,
            LineageRepresentation::ReadOnlyForAttachment,
            Some(source_guard),
        )
        .await
    }

    async fn resolve_rollout_lineage_with_representation(
        &self,
        requested_thread_id: ThreadId,
        representation: LineageRepresentation,
    ) -> ThreadStoreResult<RolloutLineage> {
        let (lineage, _guards) = self
            .resolve_rollout_lineage_with_representation_and_guards(
                requested_thread_id,
                representation,
                None,
            )
            .await?;
        Ok(lineage)
    }

    async fn resolve_rollout_lineage_with_representation_and_guards(
        &self,
        requested_thread_id: ThreadId,
        representation: LineageRepresentation,
        preheld_source_guard: Option<WriterLockGuard>,
    ) -> ThreadStoreResult<(RolloutLineage, Vec<WriterLockGuard>)> {
        let mut segments = Vec::new();
        let mut seen = HashSet::new();
        let mut next_rollout_id = None;
        let mut end = None;
        let mut history_mode = None;
        let mut ancestor_guards = Vec::new();

        loop {
            // A history base names an immutable rollout ID, while every mutator/compressor uses
            // the stable logical ID encoded before `_` in the canonical filename. Discover only
            // that filename first, take the stable-ID guard, then resolve the immutable ID again
            // under the guard before reading any rollout content.
            let ancestor_logical_thread_id = match (representation, next_rollout_id) {
                (LineageRepresentation::PlainForReference | LineageRepresentation::ReadOnlyForAttachment, Some(rollout_id)) => {
                    let path = resolve_rollout_path_by_id(self, rollout_id)
                        .await?
                        .ok_or_else(|| malformed_lineage(rollout_id, "missing source rollout"))?;
                    codex_rollout::rollout_thread_id_from_path(path.as_path()).ok_or_else(|| {
                        malformed_lineage(rollout_id, "source rollout has invalid filename")
                    })?
                }
                _ => requested_thread_id,
            };
            let _writer_guard = match representation {
                LineageRepresentation::Existing => None,
                LineageRepresentation::PlainForReference | LineageRepresentation::ReadOnlyForAttachment => Some(
                    self.live_writer_locks
                        .lock(ancestor_logical_thread_id)
                        .await,
                ),
            };
            let _filesystem_guard = match representation {
                LineageRepresentation::Existing => None,
                (LineageRepresentation::PlainForReference | LineageRepresentation::ReadOnlyForAttachment) if next_rollout_id.is_none() => {
                    let guard = match preheld_source_guard.as_ref() {
                        Some(guard) => guard.clone(),
                        // This branch has no external caller retaining a source guard; acquire
                        // one for the resolver itself and retain it through its path reads.
                        None => self
                            .writer_lock_coordinator
                            .acquire(ancestor_logical_thread_id)?,
                    };
                    Some(guard)
                }
                LineageRepresentation::PlainForReference | LineageRepresentation::ReadOnlyForAttachment => {
                    let guard = match self.existing_writer_lock(ancestor_logical_thread_id).await {
                        Some(guard) => guard,
                        None => self
                            .writer_lock_coordinator
                            .acquire(ancestor_logical_thread_id)?,
                    };
                    ancestor_guards.push(guard.clone());
                    Some(guard)
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
            // Both logical readers and reference preparation traverse immutable ancestry. Do not
            // let either path follow an external, traversal, or symlinked file: an Existing
            // lineage is just as security-sensitive as one we are about to share.
            let rollout_path = match representation {
                LineageRepresentation::ReadOnlyForAttachment => {
                    let existing_path = codex_rollout::existing_rollout_path(rollout_path.as_path())
                        .await
                        .unwrap_or(rollout_path);
                    super::helpers::managed_rollout_path(
                        self.config.codex_home.as_path(),
                        existing_path.as_path(),
                        rollout_id,
                    )?
                }
                LineageRepresentation::Existing | LineageRepresentation::PlainForReference => {
                    super::helpers::managed_rollout_path(
                        self.config.codex_home.as_path(),
                        rollout_path.as_path(),
                        rollout_id,
                    )?
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
            let path_thread_id = codex_rollout::rollout_thread_id_from_path(rollout_path.as_path());
            if path_thread_id != Some(meta.meta.id)
                || codex_rollout::rollout_id_from_path(rollout_path.as_path()) != Some(rollout_id)
                || (next_rollout_id.is_none() && path_thread_id != Some(requested_thread_id))
            {
                return Err(malformed_lineage(
                    requested_thread_id,
                    "source rollout filename or metadata belongs to another thread",
                ));
            }
            if history_mode.is_some_and(|expected| expected != meta.meta.history_mode) {
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
                LineageRepresentation::PlainForReference | LineageRepresentation::ReadOnlyForAttachment => rollout_path,
            };
            if let Some(end) = end {
                if matches!(representation, LineageRepresentation::ReadOnlyForAttachment) {
                    validate_raw_rollout_cutoff(
                        requested_thread_id,
                        rollout_path.as_path(),
                        end.end_byte_offset,
                        meta.meta.history_mode,
                    )
                    .await?;
                } else {
                    validate_cutoff_bounds(
                        requested_thread_id,
                        rollout_path.as_path(),
                        &end,
                        meta.meta.history_mode,
                    )
                    .await?;
                }
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
        Ok((
            RolloutLineage {
                segments,
                history_mode: history_mode.ok_or_else(|| {
                    malformed_lineage(requested_thread_id, "source lineage is empty")
                })?,
            },
            ancestor_guards,
        ))
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
    ReadOnlyForAttachment,
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
        // A shared legacy ancestor may already be compressed. Validate its decoded byte cutoff
        // through the raw reader; the plain-file reverse scanner cannot interpret zstd bytes.
        if rollout_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".jsonl.zst"))
        {
            return validate_raw_rollout_cutoff(
                requested_thread_id,
                rollout_path,
                end_byte_offset,
                history_mode,
            )
            .await;
        }
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
            message: format!(
                "failed to validate legacy cutoff {}: {err}",
                rollout_path.display()
            ),
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

/// Validate a raw JSONL cutoff without materializing a compressed rollout into a plain file.
async fn validate_raw_rollout_cutoff(
    requested_thread_id: ThreadId,
    rollout_path: &Path,
    end_byte_offset: u64,
    history_mode: ThreadHistoryMode,
) -> ThreadStoreResult<()> {
    // Cutoff validation only needs physical byte counts. Keep this bounded even when a legacy
    // record contains an arbitrarily large payload; attachment streaming will independently
    // discard that record when it exceeds its output budget.
    const CUTOFF_LINE_LIMIT: usize = 4 * 1024 * 1024;
    let mut reader = codex_rollout::open_rollout_raw_line_reader(rollout_path)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read lineage {}: {err}", rollout_path.display()),
        })?;
    let mut offset = 0_u64;
    let mut last_complete = 0_u64;
    let mut last_valid = 0_u64;
    while let Some(record) = reader
        .next_raw_line_limited(CUTOFF_LINE_LIMIT)
        .await
        .map_err(|err| {
            ThreadStoreError::Internal {
                message: format!("failed to scan lineage {}: {err}", rollout_path.display()),
            }
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
        let next = offset.saturating_add(byte_count as u64);
        if next > end_byte_offset {
            break;
        }
        offset = next;
        // An EOF-partial record is never a valid cutoff, even when its JSON happens to parse.
        if !terminated {
            break;
        }
        last_complete = offset;
        if history_mode == ThreadHistoryMode::Paginated || oversized {
            last_valid = offset;
        } else if line.as_deref().is_some_and(valid_legacy_envelope) {
            last_valid = offset;
        }
    }
    let boundary = if history_mode == ThreadHistoryMode::Paginated {
        last_complete
    } else {
        last_valid
    };
    if boundary != end_byte_offset {
        return Err(malformed_lineage(
            requested_thread_id,
            "cutoff byte offset is not at a complete JSONL record",
        ));
    }
    Ok(())
}

fn valid_legacy_envelope(line: &[u8]) -> bool {
    serde_json::from_slice::<EnvelopeValidity>(line)
        .map(|valid| valid.0)
        .unwrap_or(false)
}

struct EnvelopeValidity(bool);
struct EnvelopeVisitor;
struct ObjectSeed;
struct ObjectVisitor;

impl<'de> serde::Deserialize<'de> for EnvelopeValidity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EnvelopeVisitor)
    }
}

impl<'de> Visitor<'de> for EnvelopeVisitor {
    type Value = EnvelopeValidity;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object rollout envelope")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut timestamp = false;
        let mut kind = false;
        let mut payload = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "timestamp" => {
                    map.next_value::<String>()?;
                    timestamp = true;
                }
                "type" => {
                    map.next_value::<String>()?;
                    kind = true;
                }
                "payload" => {
                    map.next_value_seed(ObjectSeed)?;
                    payload = true;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(EnvelopeValidity(timestamp && kind && payload))
    }
}

impl<'de> DeserializeSeed<'de> for ObjectSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ObjectVisitor)
    }
}

impl<'de> Visitor<'de> for ObjectVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(())
    }
}

fn malformed_lineage(thread_id: ThreadId, detail: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid paginated history lineage for {thread_id}: {detail}"),
    }
}

#[cfg(test)]
#[path = "rollout_lineage_tests.rs"]
mod tests;
