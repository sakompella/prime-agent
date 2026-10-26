//! Resuming the children a restart interrupted. A child run whose display
//! still reads `running` when its parent comes back never got a verdict
//! (`record_settled_display`): the daemon died under it, or the parent's
//! shutdown closed it. The restarted parent resumes such a run the way
//! the update restart resumes a mid-turn session (a continuation prompt
//! over the saved transcript), watches it again, and tells its own
//! session once what it resumed and what it could not.
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::SystemTime;

use pa_core::session_engine::rlm_notices::{
    create_rlm_children_resumed_notice, ChildResume, ChildResumeOutcome,
};

use super::{
    bail, now_ms, Arc, ChildRecord, Duration, Mutex, Result, SupervisorChildSessionsInner,
};

/// How long after its last write an interrupted child run is still
/// resumed. An older interruption (a session reopened days later) stays
/// a settled `error` row; resuming it would spend tokens on a stale task.
pub(super) const RESUME_WINDOW: Duration = Duration::from_secs(6 * 60 * 60);

pub(super) const RESUME_PROMPT: &str = "Your run was interrupted: Prime Agent or your parent session restarted. Continue the interrupted task from the saved transcript. Processes and kernel state from before the restart are gone, so inspect the current state (files, branches, running commands) before redoing or retrying work.";

/// Whether a child file last written at `modified` is recent enough to resume.
pub(super) fn within_resume_window(modified: SystemTime) -> bool {
    SystemTime::now()
        .duration_since(modified)
        .map_or(true, |age| age <= RESUME_WINDOW)
}

impl SupervisorChildSessionsInner {
    /// Resume each interrupted child, then deliver one notice listing the
    /// outcome for every child, with its session dir.
    pub(super) async fn resume_interrupted_children(
        this: &Arc<Self>,
        children: Vec<Arc<Mutex<ChildRecord>>>,
    ) {
        if children.is_empty() {
            return;
        }
        let mut outcomes = Vec::with_capacity(children.len());
        for record in &children {
            let outcome = match Self::resume_child(this, record).await {
                Ok(outcome) => outcome,
                Err(error) => ChildResumeOutcome::Failed(format!("{error:#}")),
            };
            let record = record.lock().await;
            outcomes.push(ChildResume {
                child_id: record.rlm_child_id.clone(),
                session_name: record.session_name.clone(),
                session_dir: record.session_dir.clone(),
                outcome,
            });
        }
        this.deliver_terminal_notice(create_rlm_children_resumed_notice(&outcomes, now_ms()))
            .await;
    }

    /// A child whose worker still runs its turn (an update restart already
    /// continued it), or whose transcript already ends in a finished turn,
    /// is only watched again; any other child gets the continuation prompt,
    /// which wakes its saved session. Either way the row turns running and
    /// a settle watcher owes the parent its result. The record's rename lock
    /// is held throughout, as the close walk holds it, so a parent close or
    /// replacement either runs first (and the child is left alone) or waits.
    async fn resume_child(
        this: &Arc<Self>,
        record: &Arc<Mutex<ChildRecord>>,
    ) -> Result<ChildResumeOutcome> {
        let rename_lock = record.lock().await.rename_lock.clone();
        let _guard = rename_lock.lock().await;
        let held = this
            .children
            .lock()
            .await
            .iter()
            .any(|held| Arc::ptr_eq(held, record));
        let (active_session_id, session_file, closed) = {
            let record = record.lock().await;
            (
                record.active_session_id.clone(),
                record.session_file.clone(),
                record.closed_by_parent,
            )
        };
        if !held || closed {
            bail!("the parent closed before the child could resume");
        }
        // Only rows written after this point belong to the resumed run.
        Self::prime_usage_cursor(record).await;
        let outcome = if matches!(this.child_busy(&active_session_id).await, Ok(true)) {
            ChildResumeOutcome::StillRunning
        } else if session_file
            .as_deref()
            .is_some_and(|file| ends_in_finished_turn(Path::new(file)))
        {
            ChildResumeOutcome::Finished
        } else {
            this.prompt_child(&active_session_id, RESUME_PROMPT, None)
                .await?;
            ChildResumeOutcome::Resumed
        };
        {
            let mut row = record.lock().await;
            row.settled_status = None;
            row.settled = false;
            row.notice_delivered = false;
            row.error = None;
            row.prompt_admitted = true;
        }
        this.refresh_running().await;
        this.emit_child_update(record).await;
        let watcher = Arc::clone(this);
        let record = Arc::clone(record);
        tokio::spawn(async move { watcher.watch_child_settle(&record).await });
        Ok(outcome)
    }
}

/// Whether the session's last message is an assistant turn that ended
/// normally (`stopReason: "stop"`): the run finished, for example after
/// the update restart continued it, and only its verdict is owed.
fn ends_in_finished_turn(session_file: &Path) -> bool {
    const TAIL_BYTES: u64 = 256 * 1024;
    let Ok(mut file) = std::fs::File::open(session_file) else {
        return false;
    };
    let length = file.metadata().map_or(0, |metadata| metadata.len());
    let mut tail = Vec::new();
    if file
        .seek(SeekFrom::Start(length.saturating_sub(TAIL_BYTES)))
        .and_then(|_| file.read_to_end(&mut tail))
        .is_err()
    {
        return false;
    }
    String::from_utf8_lossy(&tail)
        .lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|row| row["type"] == "message")
        .is_some_and(|row| {
            row["message"]["role"] == "assistant" && row["message"]["stopReason"] == "stop"
        })
}
