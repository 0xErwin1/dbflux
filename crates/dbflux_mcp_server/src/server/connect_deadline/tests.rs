use super::*;

use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, DbKind, PipelineState};
use dbflux_test_support::FakeDriver;

fn short_deadlines() -> ConnectDeadlines {
    ConnectDeadlines {
        stage: Duration::from_millis(100),
        login: Duration::from_millis(600),
        driver_connect: Duration::from_millis(100),
    }
}

fn opening_access() -> PipelineState {
    PipelineState::OpeningAccess {
        method_label: "SSM".to_string(),
    }
}

#[tokio::test]
async fn a_stalled_stage_fails_naming_the_stage_and_cancels_the_token() {
    let (state_sender, state_watcher) = dbflux_core::pipeline_state_channel();
    let cancel = CancelToken::new();
    let future = async move {
        state_sender.send_replace(opening_access());
        tokio::time::sleep(Duration::from_secs(5)).await;
        "finished"
    };

    let error = run_with_stage_deadline(future, state_watcher, &short_deadlines(), &cancel)
        .await
        .expect_err("a stalled stage must time out");

    assert_eq!(
        error,
        "Connection timed out after 100ms while Opening access (SSM)"
    );
    assert!(cancel.is_cancelled());
}

#[tokio::test]
async fn a_pipeline_that_keeps_advancing_is_not_cut_off() {
    let (state_sender, state_watcher) = dbflux_core::pipeline_state_channel();
    let cancel = CancelToken::new();
    let future = async move {
        for resolved in 0..5 {
            state_sender.send_replace(PipelineState::ResolvingValues { total: 5, resolved });
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        "finished"
    };

    let output = run_with_stage_deadline(future, state_watcher, &short_deadlines(), &cancel)
        .await
        .expect("each stage stays within its budget");

    assert_eq!(output, "finished");
    assert!(!cancel.is_cancelled());
}

#[tokio::test]
async fn waiting_for_login_gets_the_longer_budget() {
    let (state_sender, state_watcher) = dbflux_core::pipeline_state_channel();
    let cancel = CancelToken::new();
    let future = async move {
        state_sender.send_replace(PipelineState::WaitingForLogin {
            provider_name: "AWS SSO".to_string(),
            verification_url: None,
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        state_sender.send_replace(opening_access());
        "finished"
    };

    let output = run_with_stage_deadline(future, state_watcher, &short_deadlines(), &cancel)
        .await
        .expect("a login inside the login budget must not time out");
    assert_eq!(output, "finished");

    let (state_sender, state_watcher) = dbflux_core::pipeline_state_channel();
    let future = async move {
        state_sender.send_replace(PipelineState::WaitingForLogin {
            provider_name: "AWS SSO".to_string(),
            verification_url: None,
        });
        tokio::time::sleep(Duration::from_secs(5)).await;
        "finished"
    };

    let error = run_with_stage_deadline(future, state_watcher, &short_deadlines(), &cancel)
        .await
        .expect_err("a login past the login budget must time out");
    assert_eq!(
        error,
        "Connection timed out after 600ms while Waiting for login (AWS SSO)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_driver_connect_fails_naming_connecting_and_closes_the_late_connection() {
    let driver = FakeDriver::new(DbKind::Postgres);
    let connect_driver = driver.clone();

    let error = connect_with_deadline(
        move || {
            std::thread::sleep(Duration::from_millis(300));
            let profile = ConnectionProfile::new("slow", DbConfig::default_postgres());
            connect_driver
                .connect(&profile)
                .map_err(|error| error.to_string())
        },
        Duration::from_millis(100),
        "postgres",
    )
    .await
    .err()
    .expect("a slow connect must time out");

    assert_eq!(
        error,
        "Connection timed out after 100ms while Connecting (postgres)"
    );

    let closed_in_time = tokio::time::timeout(Duration::from_secs(5), async {
        while driver.stats().close_calls == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(closed_in_time.is_ok(), "the late connection must be closed");
    assert_eq!(driver.stats().close_calls, 1);
}

#[tokio::test]
async fn a_driver_connect_within_its_budget_returns_the_connection() {
    let driver = FakeDriver::new(DbKind::Postgres);
    let connect_driver = driver.clone();

    let connection = connect_with_deadline(
        move || {
            let profile = ConnectionProfile::new("fast", DbConfig::default_postgres());
            connect_driver
                .connect(&profile)
                .map_err(|error| error.to_string())
        },
        Duration::from_millis(500),
        "postgres",
    )
    .await;

    assert!(connection.is_ok());
    assert_eq!(driver.stats().close_calls, 0);
}

#[test]
fn default_deadlines_give_login_more_time_than_ordinary_stages() {
    let deadlines = ConnectDeadlines::default();
    assert_eq!(deadlines.stage, Duration::from_secs(30));
    assert_eq!(deadlines.login, Duration::from_secs(300));
    assert_eq!(deadlines.driver_connect, Duration::from_secs(60));
}
