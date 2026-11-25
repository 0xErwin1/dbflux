use super::*;
use std::time::Duration;

// ---------------------------------------------------------------------------
// wait_for_approval and terminal-state reporting of get_pending_execution
// ---------------------------------------------------------------------------

/// Queues a delete for `actor_id` straight in the shared store, as another
/// process would, and returns its id.
async fn queue_for(agent: &Agent, actor_id: &str) -> String {
    agent
        .runtime
        .write()
        .await
        .approval_service_mut()
        .request_execution(&dbflux_approval::ExecutionPlan {
            connection_id: "conn".to_string(),
            actor_id: actor_id.to_string(),
            tool_id: "delete_records".to_string(),
            classification: dbflux_policy::ExecutionClassification::Destructive,
            payload: json!({ "table": "items", "where": { "id": 1 } }),
        })
        .expect("queue an execution")
        .id
        .to_string()
}

async fn reject_as_person(agent: &Agent, pending_id: &str, reason: &str) {
    agent
        .runtime
        .write()
        .await
        .reject_pending_execution_with_origin_mut(
            pending_id,
            "local",
            Some(reason),
            EventOrigin::local(),
        )
        .expect("a person should be able to reject the pending execution");
}

async fn wait(agent: &Agent, pending_id: &str, timeout_secs: u64) -> Value {
    agent
        .call_json(
            "wait_for_approval",
            json!({ "pending_id": pending_id, "timeout_secs": timeout_secs }),
        )
        .await
}

#[tokio::test]
async fn wait_for_approval_is_listed_and_allowed_wherever_get_pending_execution_is() {
    let agent = start_agent(ADMIN_ROLE, None).await;

    assert!(
        agent
            .tools()
            .await
            .iter()
            .any(|tool| tool.name == "wait_for_approval")
    );

    let pending_id = queue_for(&agent, AGENT).await;
    let status = wait(&agent, &pending_id, 1).await;
    assert_eq!(status["status"], json!("still_pending"), "{status}");
}

#[tokio::test]
async fn wait_returns_approved_with_the_next_action() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    let pending_id = queue_for(&agent, AGENT).await;
    agent.approve_as_person(&pending_id).await;

    let status = wait(&agent, &pending_id, 5).await;

    assert_eq!(status["status"], json!("approved"), "{status}");
    assert_eq!(status["pending_id"], json!(pending_id));
    assert_eq!(status["next_action"], json!("repeat_identical_call"));
    assert!(status.get("reason").is_none(), "{status}");
}

#[tokio::test]
async fn wait_wakes_up_when_a_person_approves_during_the_wait() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    let pending_id = queue_for(&agent, AGENT).await;

    let (status, ()) = tokio::join!(wait(&agent, &pending_id, 30), async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        agent.approve_as_person(&pending_id).await;
    });

    assert_eq!(status["status"], json!("approved"), "{status}");
}

#[tokio::test]
async fn wait_returns_rejected_with_the_reason() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    let pending_id = queue_for(&agent, AGENT).await;
    reject_as_person(&agent, &pending_id, "wrong table").await;

    let status = wait(&agent, &pending_id, 5).await;

    assert_eq!(status["status"], json!("rejected"), "{status}");
    assert_eq!(status["reason"], json!("wrong table"));
    assert_eq!(status["next_action"], json!("do_not_retry"));
}

#[tokio::test]
async fn wait_and_get_report_an_expired_request_as_expired() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    agent
        .runtime
        .write()
        .await
        .approval_service_mut()
        .set_ttl_ms(-1_000);
    let pending_id = queue_for(&agent, AGENT).await;

    let status = wait(&agent, &pending_id, 5).await;
    assert_eq!(status["status"], json!("expired"), "{status}");
    assert_eq!(status["next_action"], json!("do_not_retry"));

    let fetched = agent
        .call_json("get_pending_execution", json!({ "pending_id": pending_id }))
        .await;
    assert_eq!(fetched["status"], json!("expired"), "{fetched}");
}

#[tokio::test]
async fn wait_times_out_as_still_pending() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    let pending_id = queue_for(&agent, AGENT).await;

    let started = std::time::Instant::now();
    let status = wait(&agent, &pending_id, 1).await;

    assert_eq!(status["status"], json!("still_pending"), "{status}");
    assert_eq!(status["next_action"], json!("call_wait_for_approval_again"));
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn wait_treats_another_clients_request_like_an_unknown_one() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    let other_id = queue_for(&agent, "another-client").await;
    let unknown_id = uuid::Uuid::new_v4().to_string();

    let other = wait(&agent, &other_id, 1).await;
    let unknown = wait(&agent, &unknown_id, 1).await;

    assert_eq!(other["status"], json!("not_found"), "{other}");
    assert_eq!(unknown["status"], json!("not_found"), "{unknown}");
    assert_eq!(other["next_action"], unknown["next_action"]);
    assert!(other.get("reason").is_none(), "{other}");
}

#[tokio::test]
async fn a_waiting_call_does_not_block_other_calls_on_the_session() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    let pending_id = queue_for(&agent, AGENT).await;

    let (waited, listed) = tokio::join!(wait(&agent, &pending_id, 2), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = std::time::Instant::now();
        let listed = agent.call_json("list_pending_executions", json!({})).await;
        (listed, started.elapsed())
    });

    assert_eq!(waited["status"], json!("still_pending"));
    assert_eq!(listed.0["count"], json!(1));
    assert!(
        listed.1 < Duration::from_millis(1_500),
        "list_pending_executions was blocked by the waiting call: {:?}",
        listed.1
    );
}

#[tokio::test]
async fn get_pending_execution_reports_an_approved_request_as_approved() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    let pending_id = queue_for(&agent, AGENT).await;
    agent.approve_as_person(&pending_id).await;

    let fetched = agent
        .call_json("get_pending_execution", json!({ "pending_id": pending_id }))
        .await;

    assert_eq!(fetched["status"], json!("approved"), "{fetched}");
    assert_eq!(fetched["pending_id"], json!(pending_id));
}

#[tokio::test]
async fn get_pending_execution_still_returns_the_entry_while_pending() {
    let agent = start_agent(ADMIN_ROLE, None).await;
    let pending_id = queue_for(&agent, AGENT).await;

    let fetched = agent
        .call_json("get_pending_execution", json!({ "pending_id": pending_id }))
        .await;

    assert_eq!(fetched["status"], json!("pending"), "{fetched}");
    assert_eq!(fetched["id"], json!(pending_id));
    assert_eq!(fetched["plan"]["tool_id"], json!("delete_records"));
}
