//! The boot wake for interrupted RLM children. A daemon restart stops every
//! child mid-run, and only the child's parent resumes it (its reseed, see
//! `rlm_children::resume`). A parent that was idle at the restart has no
//! turn to continue and no client to reopen it, so the boot wakes it here,
//! the way a message to a saved session wakes it. The parent's create
//! resumes the children and sends its one notice; a later client open of
//! the same file reuses that worker, so nothing resumes twice.
use std::collections::{BTreeMap, HashSet};
use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;

use crate::messaging::WakeOutcome;
use crate::rlm_ledger::RlmLedgerEdge;

use super::Supervisor;

/// One session the boot wakes; `child_id` is its own RLM child id when it
/// is itself a ledger child.
#[derive(Debug, PartialEq, Eq)]
struct ParentWake {
    session_file: String,
    child_id: Option<String>,
}

/// The parents of the `interrupted` edges, once each. A parent that is
/// itself an interrupted child is left out: its own parent's resume prompt
/// wakes it, and a wake here first would leave that resume looking at a
/// turn it never started. Sessions match by file stem (the session id), as
/// the ledger wake matches them.
fn parents_to_wake(
    edges: &[RlmLedgerEdge],
    interrupted: impl Fn(&RlmLedgerEdge) -> bool,
) -> Vec<ParentWake> {
    let session_id = |file: &str| Path::new(file).file_stem().map(OsString::from);
    let interrupted: Vec<&RlmLedgerEdge> = edges.iter().filter(|edge| interrupted(edge)).collect();
    let resuming: HashSet<OsString> = interrupted
        .iter()
        .filter_map(|edge| session_id(&edge.child))
        .collect();
    let mut parents = BTreeMap::new();
    for edge in interrupted {
        let Some(parent_id) = session_id(&edge.parent) else {
            continue;
        };
        if resuming.contains(&parent_id) {
            continue;
        }
        parents
            .entry(parent_id)
            .or_insert_with_key(|parent_id| ParentWake {
                session_file: edge.parent.clone(),
                child_id: edges
                    .iter()
                    .find(|own| session_id(&own.child).as_ref() == Some(parent_id))
                    .map(|own| own.child_id.clone()),
            });
    }
    parents.into_values().collect()
}

impl Supervisor {
    /// Wake every saved parent whose children a restart interrupted inside
    /// the resume window. Runs after the boot restore pass: a parent that
    /// pass (or adoption) already brought up is reused, not woken again.
    pub(super) async fn wake_parents_of_interrupted_children(self: &Arc<Self>) {
        let ledger = match self.rlm_spawn_ledger_for(None).await {
            Ok(ledger) => ledger,
            Err(error) => {
                self.log_line(&format!(
                    "interrupted RLM children: the spawn ledger is unavailable: {error:#}"
                ));
                return;
            }
        };
        let scan = tokio::task::spawn_blocking(move || {
            ledger.live_edges().map(|edges| {
                parents_to_wake(&edges, crate::rlm_children::recently_interrupted)
                    .into_iter()
                    .map(|parent| {
                        let info = crate::session_store::read_session_info(Path::new(
                            &parent.session_file,
                        ));
                        (parent, info)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .await;
        let parents = match scan {
            Ok(Ok(parents)) => parents,
            Ok(Err(error)) => {
                self.log_line(&format!(
                    "interrupted RLM children: the ledger scan failed: {error:#}"
                ));
                return;
            }
            Err(error) => {
                self.log_line(&format!(
                    "interrupted RLM children: the ledger scan task failed: {error}"
                ));
                return;
            }
        };
        for (parent, info) in parents {
            // A parent file deleted or archived since names nothing to wake.
            let Some(info) = info else {
                self.log_line(&format!(
                    "interrupted RLM children: parent {} is unreadable; not woken",
                    parent.session_file
                ));
                continue;
            };
            let outcome = self
                .launch_session_wake(
                    &parent.session_file,
                    info.cwd,
                    info.rlm_depth,
                    parent.child_id.as_deref(),
                )
                .await;
            self.log_line(&match outcome {
                WakeOutcome::Failed(error) => format!(
                    "interrupted RLM children: could not wake parent {}: {error}",
                    parent.session_file
                ),
                WakeOutcome::Woken(_) | WakeOutcome::Unknown => format!(
                    "interrupted RLM children: woke parent {} to resume them",
                    parent.session_file
                ),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

    use super::*;

    /// One ledger edge whose child file and display exist under `root`.
    fn edge(
        root: &Path,
        parent: &Path,
        child_id: &str,
        status: &str,
        age: Duration,
    ) -> RlmLedgerEdge {
        let child = root.join(child_id).join(format!("{child_id}.jsonl"));
        std::fs::create_dir_all(child.parent().unwrap()).unwrap();
        std::fs::write(&child, "{}\n").unwrap();
        crate::rlm_ledger::write_rlm_subagent_display(
            &crate::rlm_ledger::RlmSubagentDisplayEntry {
                type_tag: "rlm_subagent".to_string(),
                child_id: child_id.to_string(),
                session_name: child_id.to_string(),
                session_dir: child.parent().unwrap().display().to_string(),
                session_file: child.display().to_string(),
                rlm_parent_node_id: None,
                prompt: None,
                spawn_code: None,
                model: None,
                status: status.to_string(),
                created_at: 0,
            },
        )
        .unwrap();
        std::fs::File::options()
            .write(true)
            .open(&child)
            .unwrap()
            .set_modified(SystemTime::now() - age)
            .unwrap();
        RlmLedgerEdge {
            child_id: child_id.to_string(),
            parent: parent.display().to_string(),
            child: child.display().to_string(),
            depth: 1,
            name: child_id.to_string(),
            deleted: None,
            deleted_usage: None,
        }
    }

    fn file_of(edge: &RlmLedgerEdge) -> PathBuf {
        PathBuf::from(&edge.child)
    }

    /// The boot wakes the top of each interrupted family once: a parent of
    /// a recently interrupted child, never one whose children are all old
    /// or settled, and never an interrupted child (its own parent's resume
    /// wakes it). A settled child with interrupted children of its own is
    /// woken with its child identity.
    #[test]
    fn the_boot_wakes_each_parent_of_a_recently_interrupted_child_once() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let recent = Duration::from_secs(60);
        let old = Duration::from_hours(7);
        let parent = root.join("P.jsonl");
        let idle = root.join("Q.jsonl");
        let mut edges = vec![
            edge(root, &parent, "sub-run-1", "running", recent),
            edge(root, &parent, "sub-run-2", "running", recent),
            edge(root, &idle, "sub-stale", "running", old),
            edge(root, &idle, "sub-done", "completed", recent),
            edge(root, &idle, "sub-cancelled", "cancelled", recent),
        ];
        // P -> sub-mid (interrupted) -> sub-leaf (interrupted): only P wakes.
        let mid = edge(root, &parent, "sub-mid", "running", recent);
        let leaf = edge(root, &file_of(&mid), "sub-leaf", "running", recent);
        // Q -> sub-waiting (settled) -> sub-grandkid (interrupted): sub-waiting wakes.
        let waiting = edge(root, &idle, "sub-waiting", "completed", recent);
        let grandkid = edge(root, &file_of(&waiting), "sub-grandkid", "running", recent);
        edges.extend([mid, leaf, waiting.clone(), grandkid]);

        assert_eq!(
            parents_to_wake(&edges, crate::rlm_children::recently_interrupted),
            vec![
                ParentWake {
                    session_file: parent.display().to_string(),
                    child_id: None,
                },
                ParentWake {
                    session_file: waiting.child.clone(),
                    child_id: Some("sub-waiting".to_string()),
                },
            ]
        );
    }
}
