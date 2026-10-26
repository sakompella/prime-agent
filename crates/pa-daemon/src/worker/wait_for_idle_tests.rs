//! `wait_for_idle` with a `timeoutMs` budget: the worker answers at the
//! budget, so the route that carries the wait (and the in-flight slot it
//! holds on this worker) ends even while the session stays busy.
use super::*;

async fn created_worker(reply: Value) -> Arc<Worker> {
    let dir = std::env::temp_dir().join(format!("pa-wait-idle-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = WorkerConfig {
        socket_path: dir.join("worker.sock"),
        supervisor_socket_path: PathBuf::new(),
        token: "token".to_string(),
        worker_instance_id: String::new(),
        active_session_id: "wait-session".to_string(),
        agent_dir: dir.join("agent"),
        recovery_journal_path: dir.join("recovery.jsonl"),
        telemetry_disabled: None,
        script: Some(json!({ "responses": [reply] })),
    };
    let worker = Arc::new(Worker::new(config, None));
    let created = worker
        .dispatch(
            "create",
            &json!({ "noSession": true, "cwd": "/tmp", "name": "waiting" }),
        )
        .await;
    assert!(created.success, "create failed: {created:?}");
    worker
}

#[tokio::test]
async fn a_bounded_wait_answers_a_busy_session_at_its_budget() {
    // A turn that takes far longer than the wait's budget.
    let worker = created_worker(json!({ "text": "slow", "delayMs": 3_000 })).await;
    let prompted = worker
        .dispatch("follow_up", &json!({ "message": "long work" }))
        .await;
    assert!(prompted.success, "follow_up failed: {prompted:?}");
    let answer = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        worker.dispatch("wait_for_idle", &json!({ "timeoutMs": 50 })),
    )
    .await
    .expect("a bounded wait answers at its budget while the session stays busy");
    assert!(answer.success, "wait_for_idle failed: {answer:?}");
    assert_eq!(answer.data, Some(json!({ "idle": false })));
}

#[tokio::test]
async fn a_bounded_wait_on_an_idle_session_answers_idle() {
    let worker = created_worker(json!("ack")).await;
    let answer = worker
        .dispatch("wait_for_idle", &json!({ "timeoutMs": 60_000 }))
        .await;
    assert!(answer.success, "wait_for_idle failed: {answer:?}");
    assert_eq!(answer.data, Some(json!({ "idle": true })));
}
