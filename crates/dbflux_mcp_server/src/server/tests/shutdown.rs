//! Shutdown-boundary regressions for the MCP connection drain.
//!
//! These cover the two cancellation boundaries of `disconnect_all_with_timeout`:
//! session-factory ownership must survive a cancelled hook phase, and the
//! overall shutdown bound must hold while a synchronous factory shutdown
//! blocks — without pinning the Tokio runtime on exit.

use std::sync::atomic::AtomicUsize;

use dbflux_core::ExecutionSessionFactory;

use super::*;

struct ObservedFactory {
    calls: Arc<AtomicUsize>,
    gate: Option<Arc<(Mutex<bool>, std::sync::Condvar)>>,
    entered: Option<mpsc::Sender<()>>,
}
impl ExecutionSessionFactory for ObservedFactory {
    fn open(&self) -> Result<Arc<dyn dbflux_core::ExecutionSession>, DbError> {
        Err(DbError::NotSupported("probe has no sessions".into()))
    }
    fn shutdown(&self) -> Result<(), DbError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = &self.entered {
            entered.send(()).expect("observer alive");
        }
        if let Some(gate) = &self.gate {
            let (lock, changed) = &**gate;
            let mut released = lock.lock().expect("gate lock");
            while !*released {
                released = changed.wait(released).expect("gate wait");
            }
        }
        Ok(())
    }
}
struct ObservedConnection {
    inner: Arc<dyn Connection>,
    factory: ObservedFactory,
}
impl Connection for ObservedConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        self.inner.metadata()
    }
    fn ping(&self) -> Result<(), DbError> {
        self.inner.ping()
    }
    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }
    fn execute(&self, request: &QueryRequest) -> Result<QueryResult, DbError> {
        self.inner.execute(request)
    }
    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        self.inner.cancel(handle)
    }
    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        self.inner.schema()
    }
    fn kind(&self) -> DbKind {
        self.inner.kind()
    }
    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        self.inner.schema_loading_strategy()
    }
    fn dialect(&self) -> &dyn SqlDialect {
        self.inner.dialect()
    }
    fn execution_session_factory(&self) -> Option<&dyn ExecutionSessionFactory> {
        Some(&self.factory)
    }
}
fn observed(profile: &ConnectionProfile, calls: Arc<AtomicUsize>) -> ObservedConnection {
    ObservedConnection {
        inner: FakeDriver::new(DbKind::Postgres)
            .connect_arc(profile)
            .expect("fake connects"),
        factory: ObservedFactory {
            calls,
            gate: None,
            entered: None,
        },
    }
}
async fn cache_observed(state: &ServerState, key: String, connection: ObservedConnection) {
    state.connection_cache.write().await.insert(
        key,
        Arc::new(CachedConnection::new(Arc::new(connection), None)),
    );
}
fn blocking_profile(trace: &std::path::Path, id: u128) -> ConnectionProfile {
    let mut profile = ConnectionProfile::new("blocking", DbConfig::default_postgres());
    profile.id = uuid::Uuid::from_u128(id);
    let mut hook = process_hook(trace, "pre_disconnect", false);
    hook.timeout_ms = None;
    hook.env
        .insert("DBFLUX_MCP_HOOK_TEST_SLEEP_MS".into(), "60000".into());
    let mut hooks = ConnectionHooks::default();
    hooks.pre_disconnect.push(hook);
    profile.hooks = Some(hooks);
    profile
}

/// PreDisconnect hook child that blocks until the test creates the gate file,
/// so the deadline expires while the first profile's hook phase is running.
fn gate_blocking_hook(
    trace: &std::path::Path,
    gate: &std::path::Path,
) -> dbflux_core::ConnectionHook {
    let mut hook = process_hook(trace, "pre_disconnect", false);
    hook.timeout_ms = None;
    hook.env.insert(
        "DBFLUX_MCP_HOOK_TEST_WAIT_FOR_FILE".to_string(),
        gate.to_string_lossy().into_owned(),
    );
    hook
}

/// Connects through the real factory and caches the result under `profile_id`.
async fn connect_and_cache(state: &ServerState, profile_id: &str) {
    let connection = McpConnectionFactory::new(state.clone())
        .connect(profile_id, None)
        .await
        .expect("connection should succeed");
    state
        .connection_cache
        .write()
        .await
        .insert(profile_id.to_string(), connection);
}

/// Registers a 60s detached hook child under `profile_id`, the way a detached
/// PreConnect hook leaves a long-running process behind.
fn register_detached_sleeper(
    state: &ServerState,
    profile: &ConnectionProfile,
    trace: &std::path::Path,
) -> Arc<SharedDetachedProcess> {
    let mut detached_hook = process_hook(trace, "detached", false);
    detached_hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
    detached_hook.timeout_ms = None;
    detached_hook.env.insert(
        "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
        "60000".to_string(),
    );
    let (detached_sender, detached_receiver) = dbflux_core::detached_process_channel();
    ProcessExecutor
        .execute_hook(
            &detached_hook,
            &HookContext::from_profile(profile),
            &CancelToken::new(),
            None,
            None,
            Some(&detached_sender),
        )
        .expect("detached hook should spawn");
    let handle = detached_receiver
        .try_iter()
        .next()
        .expect("detached hook process handle");
    let detached_shared = Arc::new(SharedDetachedProcess::new(handle));
    state
        .detached_processes
        .register(&profile.id.to_string(), vec![Arc::clone(&detached_shared)]);
    detached_shared
}

// S2: several profiles connected, the first blocks its disconnect hook; the
// shutdown stays bounded and the others are still cleaned up without hooks.
struct ShutdownDeadlineFixture {
    state: ServerState,
    blocking_trace: std::path::PathBuf,
    fast_trace: std::path::PathBuf,
    gate: std::path::PathBuf,
    probe_profile: ConnectionProfile,
    session_entered_receiver: mpsc::Receiver<()>,
    shutdown_calls: Arc<AtomicUsize>,
}

/// Three cached profiles: the lexicographically first gets a PreDisconnect
/// hook that blocks on a gate file, the second a fast one, the third an
/// observed session factory.
async fn shutdown_deadline_fixture(directory: &std::path::Path) -> ShutdownDeadlineFixture {
    let blocking_trace = directory.join("blocking-hook.log");
    let fast_trace = directory.join("fast-hook.log");
    let gate = directory.join("release-blocking-hook");

    // `disconnect_all_with_timeout` drains connection ids in lexicographic
    // order, so the blocking hook goes on the profile that sorts first and
    // every later profile must still be cleaned up after the deadline.
    let mut profiles: Vec<ConnectionProfile> = (0..3)
        .map(|index| {
            ConnectionProfile::new(&format!("shutdown-{index}"), DbConfig::default_postgres())
        })
        .collect();
    profiles.sort_by(|left, right| left.id.to_string().cmp(&right.id.to_string()));
    let blocking_id = profiles[0].id.to_string();
    let fast_id = profiles[1].id.to_string();
    let probe_profile = profiles[2].clone();

    let mut blocking_hooks = ConnectionHooks::default();
    blocking_hooks
        .pre_disconnect
        .push(gate_blocking_hook(&blocking_trace, &gate));
    profiles[0].hooks = Some(blocking_hooks);

    let mut fast_hooks = ConnectionHooks::default();
    fast_hooks
        .pre_disconnect
        .push(process_hook(&fast_trace, "pre_disconnect", false));
    profiles[1].hooks = Some(fast_hooks);

    let fake_driver = FakeDriver::new(DbKind::Postgres);
    let state = test_state_with_profiles_and_errors(
        "postgres",
        Arc::new(fake_driver.clone()) as Arc<dyn DbDriver>,
        profiles,
        HashMap::new(),
        Box::new(NoopSecretStore),
    );

    for profile_id in [&blocking_id, &fast_id] {
        connect_and_cache(&state, profile_id).await;
    }

    let (session_entered, session_entered_receiver) = mpsc::channel();
    let shutdown_calls = Arc::new(AtomicUsize::new(0));
    cache_observed(
        &state,
        probe_profile.id.to_string(),
        ObservedConnection {
            inner: fake_driver
                .connect_arc(&probe_profile)
                .expect("probe driver connects"),
            factory: ObservedFactory {
                calls: Arc::clone(&shutdown_calls),
                gate: None,
                entered: Some(session_entered),
            },
        },
    )
    .await;

    ShutdownDeadlineFixture {
        state,
        blocking_trace,
        fast_trace,
        gate,
        probe_profile,
        session_entered_receiver,
        shutdown_calls,
    }
}

async fn assert_deadline_cleanup(
    fixture: &ShutdownDeadlineFixture,
    detached_shared: &Arc<SharedDetachedProcess>,
    result: Result<(), String>,
    elapsed: Duration,
    shutdown_timeout: Duration,
) {
    let error = result.expect_err("shutdown should report its deadline");
    assert!(error.contains("shutdown drain exceeded"));
    assert!(
        elapsed < shutdown_timeout + Duration::from_millis(500),
        "shutdown exceeded its deadline: {elapsed:?}"
    );
    assert!(
        fixture
            .state
            .connection_cache
            .read()
            .await
            .connection_ids()
            .is_empty(),
        "all cached connections must be drained once the deadline expires"
    );
    fixture
        .session_entered_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("session factory shutdown must be attempted for the drained probe connection");
    assert_eq!(fixture.shutdown_calls.load(Ordering::SeqCst), 1);
    assert!(
        detached_shared.terminated.load(Ordering::SeqCst),
        "detached process registered under a later profile must be terminated"
    );
    assert!(fixture.state.detached_processes.connection_ids().is_empty());
    let fast_events = std::fs::read_to_string(&fixture.fast_trace).unwrap_or_default();
    assert!(
        fast_events.is_empty(),
        "cleanup after the deadline must not run connection hooks: {fast_events}"
    );
    let blocking_events = std::fs::read_to_string(&fixture.blocking_trace).expect("hook events");
    assert!(
        blocking_events
            .lines()
            .any(|event| event == "pre_disconnect")
    );
}

#[tokio::test]
async fn shutdown_deadline_cleans_remaining_connections_without_hooks() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let fixture = shutdown_deadline_fixture(temp_dir.path()).await;
    let detached_shared = register_detached_sleeper(
        &fixture.state,
        &fixture.probe_profile,
        &temp_dir.path().join("detached-hook.log"),
    );

    // The blocking hook child writes its phase event as its first action and
    // then blocks on the gate file, so the first profile's PreDisconnect phase
    // cannot finish before the deadline. A child that never starts would fail
    // the fast-trace assertion below, because later profiles would then be
    // disconnected normally.
    let shutdown_timeout = Duration::from_secs(2);
    let started = Instant::now();
    let result =
        DbFluxServer::disconnect_all_with_timeout(fixture.state.clone(), shutdown_timeout).await;
    let elapsed = started.elapsed();

    assert_deadline_cleanup(
        &fixture,
        &detached_shared,
        result,
        elapsed,
        shutdown_timeout,
    )
    .await;

    // Release the gate in case the blocking child survived hook cancellation,
    // so it can exit before the test runtime drops.
    std::fs::write(&fixture.gate, b"release").expect("release blocking hook gate");
}

// S1/S2: draining a cache is not equivalent to attempting session teardown.
#[tokio::test]
async fn first_profile_factory_survives_hook_timeout() {
    let directory = tempfile::tempdir().expect("tempdir");
    let trace = directory.path().join("hook.log");
    let first = blocking_profile(&trace, 1);
    let mut later = ConnectionProfile::new("later", DbConfig::default_postgres());
    later.id = uuid::Uuid::from_u128(2);
    let state = test_state_with_profiles_and_errors(
        "postgres",
        Arc::new(FakeDriver::new(DbKind::Postgres)),
        vec![first.clone(), later.clone()],
        HashMap::new(),
        Box::new(NoopSecretStore),
    );
    let first_calls = Arc::new(AtomicUsize::new(0));
    let later_calls = Arc::new(AtomicUsize::new(0));
    cache_observed(
        &state,
        first.id.to_string(),
        observed(&first, first_calls.clone()),
    )
    .await;
    cache_observed(
        &state,
        later.id.to_string(),
        observed(&later, later_calls.clone()),
    )
    .await;
    let result =
        DbFluxServer::disconnect_all_with_timeout(state.clone(), Duration::from_secs(1)).await;
    // Wait for the cancelled hook's blocking task as well, so a delayed
    // callback cannot hide a call.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        std::fs::read_to_string(trace).expect("hook started"),
        "pre_disconnect\n"
    );
    eprintln!(
        "FIRST={} LATER={} CACHE={:?} RESULT={result:?}",
        first_calls.load(Ordering::SeqCst),
        later_calls.load(Ordering::SeqCst),
        state.connection_cache.read().await.connection_ids()
    );
    assert_eq!(
        first_calls.load(Ordering::SeqCst),
        1,
        "timed-out first profile must still attempt session shutdown"
    );
    assert_eq!(later_calls.load(Ordering::SeqCst), 1);
}

// S1/S2: finite gate tests both the API deadline and runtime exit after
// cancellation.
#[test]
fn blocked_fallback_is_bounded_and_does_not_pin_runtime_exit() {
    let directory = tempfile::tempdir().expect("tempdir");
    let trace = directory.path().join("hook.log");
    let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let worker_gate = gate.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let worker_calls = calls.clone();
    let (entered, enter_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let (exit_tx, exit_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = runtime.block_on(async {
            let first = blocking_profile(&trace, 1);
            let mut later = ConnectionProfile::new("gated", DbConfig::default_postgres());
            later.id = uuid::Uuid::from_u128(2);
            let state = test_state_with_profiles_and_errors(
                "postgres",
                Arc::new(FakeDriver::new(DbKind::Postgres)),
                vec![first.clone(), later.clone()],
                HashMap::new(),
                Box::new(NoopSecretStore),
            );
            let plain = McpConnectionFactory::new(state.clone())
                .connect(&first.id.to_string(), None)
                .await
                .expect("connect");
            state
                .connection_cache
                .write()
                .await
                .insert(first.id.to_string(), plain);
            let mut connection = observed(&later, worker_calls);
            connection.factory.gate = Some(worker_gate);
            connection.factory.entered = Some(entered);
            cache_observed(&state, later.id.to_string(), connection).await;
            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_millis(1500),
                DbFluxServer::disconnect_all_with_timeout(state, Duration::from_secs(1)),
            )
            .await;
            (
                result,
                started.elapsed(),
                std::fs::read_to_string(trace).expect("hook trace"),
            )
        });
        result_tx.send(result).expect("result observer");
        drop(runtime);
        exit_tx.send(()).expect("runtime observer");
    });
    let entered = enter_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    let result = result_rx.recv_timeout(Duration::from_secs(3));
    let exited = exit_rx.recv_timeout(Duration::from_millis(200)).is_ok();
    // Release BEFORE any assertions, including on the baseline where fallback
    // never starts.
    *gate.0.lock().expect("release lock") = true;
    gate.1.notify_all();
    worker.join().expect("runtime worker");
    let (result, elapsed, trace) = result.expect("bounded outer observer");
    eprintln!(
        "FACTORY_ENTERED={entered} CALLS={} RESULT={result:?} ELAPSED={elapsed:?} RUNTIME_EXIT_BEFORE_RELEASE={exited}",
        calls.load(Ordering::SeqCst)
    );
    assert_eq!(trace, "pre_disconnect\n");
    assert!(entered, "later factory shutdown must be attempted");
    assert!(
        result.is_ok(),
        "fallback exceeded the overall bound; outer timeout cancelled it"
    );
    assert!(
        exited,
        "cancelled blocking shutdown must not pin runtime exit"
    );
}

// S1 direct regression: no-hooks, two database variants, repeated disconnect,
// unrelated profile.
#[tokio::test]
async fn normal_no_hooks_variants_remain_intact() {
    let profile = ConnectionProfile::new("variants", DbConfig::default_postgres());
    let unrelated = ConnectionProfile::new("unrelated", DbConfig::default_postgres());
    let state = test_state_with_profiles_and_errors(
        "postgres",
        Arc::new(FakeDriver::new(DbKind::Postgres)),
        vec![profile.clone(), unrelated.clone()],
        HashMap::new(),
        Box::new(NoopSecretStore),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    for key in [
        profile.id.to_string(),
        format!("{}:analytics", profile.id),
        format!("{}:reporting", profile.id),
    ] {
        cache_observed(&state, key, observed(&profile, calls.clone())).await;
    }
    let unrelated_calls = Arc::new(AtomicUsize::new(0));
    cache_observed(
        &state,
        unrelated.id.to_string(),
        observed(&unrelated, unrelated_calls.clone()),
    )
    .await;
    DbFluxServer::disconnect_impl(state.clone(), &profile.id.to_string())
        .await
        .expect("normal disconnect");
    DbFluxServer::disconnect_impl(state.clone(), &profile.id.to_string())
        .await
        .expect("idempotent disconnect");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(unrelated_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.connection_cache.read().await.connection_ids(),
        vec![unrelated.id.to_string()]
    );
    DbFluxServer::disconnect_all_with_timeout(state.clone(), Duration::from_secs(1))
        .await
        .expect("normal shutdown");
    assert_eq!(unrelated_calls.load(Ordering::SeqCst), 1);
    assert!(
        state
            .connection_cache
            .read()
            .await
            .connection_ids()
            .is_empty()
    );
    eprintln!("NORMAL_VARIANTS=3 UNRELATED=1 REPEATED_DISCONNECT=OK");
}
