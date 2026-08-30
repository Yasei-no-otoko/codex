use std::fs::File;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::ScanOutcome;

use super::LocalThreadStore;
use super::read_thread;
use super::rollout_lineage::RolloutLineage;
use super::thread_rollout_resolver;
use crate::LoadThreadHistoryParams;
use crate::StoredModelContext;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
#[path = "model_context_tests.rs"]
mod tests;

/// Loads rollout items needed to reconstruct the latest model-visible context.
///
/// Plain Legacy JSONL rollouts use a reverse scan. When it finds both a usable replacement-history
/// checkpoint and the completed user-turn context needed for resume metadata, the returned replay
/// starts with the canonical head `SessionMeta` followed by that newest suffix. When no bounded
/// cutoff is available, the scan continues to the beginning and returns the complete replay it
/// already accumulated.
///
/// Compressed Legacy rollouts keep the full-history path. Paginated rollouts retain their
/// lineage-aware reverse scan, including decoded compressed segments.
pub(super) async fn load_latest_model_context(
    store: &LocalThreadStore,
    params: LoadThreadHistoryParams,
) -> ThreadStoreResult<StoredModelContext> {
    let resolved = if params.include_archived {
        thread_rollout_resolver::resolve_current_including_archived(store, params.thread_id).await?
    } else {
        thread_rollout_resolver::resolve_current(store, params.thread_id).await?
    };
    let path =
        resolved
            .map(|resolved| resolved.path)
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: format!("no rollout found for thread id {}", params.thread_id),
            })?;

    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read session metadata {}: {err}", path.display()),
        })?;
    if session_meta.meta.id != params.thread_id {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout at {} belongs to thread {}, not {}",
                path.display(),
                session_meta.meta.id,
                params.thread_id
            ),
        });
    }

    let is_compressed = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .is_some_and(|file_name| file_name.ends_with(".jsonl.zst"));
    let items = match session_meta.meta.history_mode {
        ThreadHistoryMode::Legacy if is_compressed => {
            read_thread::load_history_items(path.as_path()).await?
        }
        ThreadHistoryMode::Legacy => scan_model_context_from_rollout(path, session_meta, None).await?,
        ThreadHistoryMode::Paginated => {
            let lineage = store.resolve_rollout_lineage(params.thread_id).await?;
            scan_model_context_from_lineage(lineage, session_meta).await?
        }
    };

    Ok(StoredModelContext {
        thread_id: params.thread_id,
        items,
    })
}

async fn scan_model_context_from_rollout(
    rollout_path: PathBuf,
    session_meta: SessionMetaLine,
    end_byte_offset: Option<u64>,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let scan = tokio::task::spawn_blocking(move || {
        scan_model_context_from_rollout_blocking(rollout_path.as_path(), session_meta, end_byte_offset)
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join model context scan: {err}"),
    })?;
    match scan {
        Ok(items) => Ok(items),
        Err(err) => Err(ThreadStoreError::Internal {
            message: format!("failed to scan legacy model context rollout: {err}"),
        }),
    }
}

/// Loads the frozen Legacy prefix selected by a complete JSONL-record cutoff.
pub(super) async fn load_legacy_fork_context(
    rollout_path: PathBuf,
    end_byte_offset: u64,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let session_meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read session metadata {}: {err}", rollout_path.display()),
        })?;
    scan_model_context_from_rollout(rollout_path, session_meta, Some(end_byte_offset)).await
}

/// Loads startup context from a fork's frozen inherited prefix.
pub(super) async fn load_for_fork(
    lineage: RolloutLineage,
    history_base: Option<HistoryPosition>,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let source_path = lineage
        .segments()
        .last()
        .map(|segment| segment.rollout_path.as_path())
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    let session_meta = codex_rollout::read_session_meta_line(source_path)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read session metadata {}: {err}",
                source_path.display()
            ),
        })?;
    match history_base {
        Some(history_base) => {
            let lineage = lineage.truncate_at(history_base).await?;
            scan_model_context_from_lineage(lineage, session_meta).await
        }
        None => Ok(vec![RolloutItem::SessionMeta(session_meta)]),
    }
}

async fn scan_model_context_from_lineage(
    lineage: RolloutLineage,
    session_meta: SessionMetaLine,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let scan = tokio::task::spawn_blocking(move || {
        scan_model_context_from_lineage_blocking(&lineage, session_meta)
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join model context scan: {err}"),
    })?;
    match scan {
        Ok(items) => Ok(items),
        Err(err) => Err(ThreadStoreError::Internal {
            message: format!("failed to scan paginated model context lineage: {err}"),
        }),
    }
}

fn scan_model_context_from_lineage_blocking(
    lineage: &RolloutLineage,
    session_meta: SessionMetaLine,
) -> io::Result<Vec<RolloutItem>> {
    let mut scan = ModelContextScan::new(lineage.history_mode());
    'segments: for segment in lineage.segments().iter().rev() {
        let file = codex_rollout::open_rollout_seekable_reader(segment.rollout_path.as_path())?;
        let mut scanner = match segment.end.map(|end| end.end_byte_offset) {
            Some(end_byte_offset) => ReverseJsonlScanner::new_at(file, end_byte_offset)?,
            None => ReverseJsonlScanner::new(file)?,
        };
        while let Some(outcome) = scanner.scan_next::<RolloutLine>()? {
            let ScanOutcome::Parsed(line) = outcome else {
                continue;
            };
            // Each rollout segment contributes only its local delta. Its session metadata is
            // replaced with the requested thread's canonical SessionMeta after replay.
            if matches!(&line.item, RolloutItem::SessionMeta(_)) {
                break;
            }
            match scan.push(line.item) {
                ModelContextScanProgress::Continue => {}
                ModelContextScanProgress::Complete => break 'segments,
            }
        }
    }

    Ok(finish_model_context_scan(scan, session_meta))
}

fn scan_model_context_from_rollout_blocking(
    rollout_path: &Path,
    session_meta: SessionMetaLine,
    end_byte_offset: Option<u64>,
) -> io::Result<Vec<RolloutItem>> {
    let mut scan = ModelContextScan::new(ThreadHistoryMode::Legacy);
    scan_model_context_segment(&mut scan, rollout_path, end_byte_offset)?;

    Ok(finish_model_context_scan(scan, session_meta))
}

fn scan_model_context_segment(
    scan: &mut ModelContextScan,
    rollout_path: &Path,
    end_byte_offset: Option<u64>,
) -> io::Result<bool> {
    let file = File::open(rollout_path)?;
    let mut scanner = match end_byte_offset {
        Some(end_byte_offset) => ReverseJsonlScanner::new_at(file, end_byte_offset)?,
        None => ReverseJsonlScanner::new(file)?,
    };
    while let Some(outcome) = scanner.scan_next::<serde_json::Value>()? {
        let ScanOutcome::Parsed(mut value) = outcome else {
            continue;
        };
        if codex_rollout::strip_legacy_ghost_snapshot_rollout_line(&mut value) {
            continue;
        }
        let Ok(line) = serde_json::from_value::<RolloutLine>(value) else {
            continue;
        };
        // Metadata updates can append SessionMeta records at the physical tail. Ignore every
        // segment marker; the requested thread's canonical head metadata is injected after replay.
        if matches!(&line.item, RolloutItem::SessionMeta(_)) {
            continue;
        }
        match scan.push(line.item) {
            ModelContextScanProgress::Continue => {}
            ModelContextScanProgress::Complete => return Ok(true),
        }
    }
    Ok(false)
}

fn finish_model_context_scan(
    scan: ModelContextScan,
    session_meta: SessionMetaLine,
) -> Vec<RolloutItem> {
    let canonical_meta = session_meta.clone();
    let mut items = scan.finish(session_meta);
    if !matches!(items.first(), Some(RolloutItem::SessionMeta(_))) {
        items.insert(0, RolloutItem::SessionMeta(canonical_meta));
    }
    items
}
