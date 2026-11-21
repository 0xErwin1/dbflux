use super::{
    AdoptionHooks, COUNTER_CAPACITY, COUNTER_LEASE, COUNTER_REPLY_TIMEOUT, CounterBody,
    CounterCommand, CounterLedger, CounterRequestPhase, CounterRequestState, handle_connection,
    process_counter_command_at_adoption, process_counter_commands,
};
use dbflux_ipc::{
    framing,
    protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse},
};
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, WeakEntity};
use std::{
    io::Read as _,
    os::unix::net::UnixStream,
    sync::{
        Arc,
        atomic::AtomicBool,
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

/// Positive-path exchanges wait far longer than any scheduler pause, so only the
/// explicit timeout test depends on the production deadline.
const TEST_REPLY_WAIT: Duration = Duration::from_secs(30);

fn owner(cx: &mut TestAppContext, mode: &str) -> Entity<AppStateEntity> {
    cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("private storage");
            let mut app = AppStateEntity::new_with_storage_runtime(runtime).expect("app");
            if mode != "missing" {
                app.initialize_telemetry(
                    dbflux_app::telemetry::TelemetryConfig::from_values(None, None, false)
                        .expect("config"),
                    chrono::Utc::now(),
                )
                .expect("scheduler");
                let scheduler = app.telemetry_mut().expect("scheduler");
                // Basic is the default level, so "off" must be set explicitly.
                scheduler
                    .set_basic(mode != "off", chrono::Utc::now())
                    .expect("level");
                if mode != "undisclosed" {
                    scheduler
                        .disclosure_dismissed(chrono::Utc::now())
                        .expect("disclosure");
                }
            }
            if mode == "audit_disabled" {
                app.audit_service().set_enabled(false);
            }
            app
        })
    })
}

struct Route {
    owner: WeakEntity<AppStateEntity>,
    sender: SyncSender<CounterCommand>,
    receiver: Receiver<CounterCommand>,
    ledger: CounterLedger,
    now: Instant,
}
impl Route {
    fn new(owner: &Entity<AppStateEntity>) -> Self {
        let (sender, receiver) = mpsc::sync_channel(COUNTER_CAPACITY);
        Self {
            owner: owner.downgrade(),
            sender,
            receiver,
            ledger: CounterLedger::new(),
            now: Instant::now(),
        }
    }
    fn tick(&mut self, cx: &mut TestAppContext) {
        cx.update(|cx| {
            process_counter_commands(&self.receiver, &self.owner, &mut self.ledger, self.now, cx)
        });
    }
    fn slots(&self) -> usize {
        self.ledger
            .entries
            .iter()
            .filter(|entry| entry.is_some())
            .count()
    }
    fn exchange(&mut self, body: IpcMessage, cx: &mut TestAppContext) -> IpcResponse {
        let (mut client, server) = UnixStream::pair().expect("private socket pair");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read bound");
        let (legacy, _receive) = mpsc::channel();
        let sender = self.sender.clone();
        let worker = thread::spawn(move || {
            handle_connection(server, &legacy, "fixture-token", &sender, TEST_REPLY_WAIT)
                .expect("handler")
        });
        framing::send_msg(
            &mut client,
            &AppControlRequest::new(1, Some("fixture-token".into()), body),
        )
        .expect("request");
        let deadline = Instant::now() + Duration::from_secs(2);
        let command = loop {
            if let Ok(command) = self.receiver.try_recv() {
                break command;
            }
            assert!(Instant::now() < deadline, "handler must enqueue");
            thread::yield_now();
        };
        // Merely enqueueing must not acknowledge the operation.
        client.set_nonblocking(true).expect("nonblocking probe");
        assert_eq!(
            client
                .read(&mut [0_u8; 1])
                .expect_err("no foreground reply")
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        client.set_nonblocking(false).expect("blocking test read");
        self.sender
            .try_send(command)
            .expect("restore queued command");
        self.tick(cx);
        let response: AppControlResponse = framing::recv_msg(&mut client).expect("reply");
        worker.join().expect("worker");
        response.body
    }
    fn begin(&mut self, session: Uuid, cx: &mut TestAppContext) -> Option<Uuid> {
        match self.exchange(
            IpcMessage::BeginMcpToolUsage {
                session_nonce: session,
            },
            cx,
        ) {
            IpcResponse::McpToolUsageCapability { capability } => capability,
            _ => panic!("typed begin response"),
        }
    }
    fn finish(
        &mut self,
        session: Uuid,
        capability: Uuid,
        success: bool,
        cx: &mut TestAppContext,
    ) -> bool {
        match self.exchange(
            IpcMessage::FinishMcpToolUsage {
                session_nonce: session,
                capability,
                success,
            },
            cx,
        ) {
            IpcResponse::McpToolUsageFinished { accepted } => accepted,
            _ => panic!("typed finish response"),
        }
    }
}

fn counts(app: &Entity<AppStateEntity>, cx: &mut TestAppContext) -> serde_json::Value {
    cx.update(|cx| {
        app.update(cx, |app, _| {
            let scheduler = app.telemetry_mut().expect("scheduler");
            let _preparation = scheduler
                .prepare(chrono::Utc::now(), "en")
                .expect("foreground persistence");
            serde_json::to_value(scheduler.state()).expect("state")["counters"].clone()
        })
    })
}
fn assert_counts(app: &Entity<AppStateEntity>, cx: &mut TestAppContext, expected: u64) {
    let value = counts(app, cx);
    let counters = value.as_object().expect("counter map");
    assert_eq!(
        counters
            .get("mcp_tool")
            .map(|value| value.as_u64().expect("counter"))
            .unwrap_or(0),
        expected
    );
    for feature in [
        "query_run",
        "visual_query_builder",
        "dashboard_open",
        "script_run",
        "schema_inspect",
    ] {
        assert_eq!(
            counters
                .get(feature)
                .map(|value| value.as_u64().expect("counter"))
                .unwrap_or(0),
            0,
            "other producer {feature}"
        );
    }
}

#[gpui::test]
fn ipc_authenticated_begin_has_foreground_capability(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let session = Uuid::new_v4();
    let capability = route
        .begin(session, cx)
        .expect("foreground acquired capability");
    assert_eq!(route.slots(), 1);
    assert_counts(&app, cx, 0);
    assert!(!route.finish(Uuid::new_v4(), capability, true, cx));
    assert_eq!(route.slots(), 1, "wrong session preserves rightful entry");
    assert!(route.finish(session, capability, true, cx));
    assert!(!route.finish(session, capability, true, cx));
    assert!(!route.finish(session, Uuid::new_v4(), true, cx));
    let failed = route.begin(session, cx).expect("second operation");
    assert!(!route.finish(session, failed, false, cx));
    assert!(!route.finish(session, failed, true, cx));
    assert_eq!(route.slots(), 0);
    assert_counts(&app, cx, 1);
}

#[gpui::test]
fn ipc_existing_foreground_loop_acquires_and_records(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(dbflux_ui_base::keymap::init_keymap);
    let app = owner(cx, "basic");
    let weak = app.downgrade();
    let captured = std::rc::Rc::new(std::cell::RefCell::new(None));
    let holder = captured.clone();
    let app_for_window = app.clone();
    let (_, window) = cx.add_window_view(move |window, cx| {
        let workspace =
            cx.new(|cx| crate::ui::views::workspace::Workspace::new(app_for_window, window, cx));
        holder.replace(Some(workspace.clone()));
        gpui_component::Root::new(workspace, window, cx)
    });
    let handle = window.update(|window, _| window.window_handle());
    window.run_until_parked();
    let workspace = captured.borrow_mut().take().expect("workspace");
    let (legacy_sender, legacy_receiver) = mpsc::channel();
    let (sender, receiver) = mpsc::sync_channel(COUNTER_CAPACITY);
    let foreground = cx.spawn(move |cx| {
        super::process_commands(legacy_receiver, workspace, handle, receiver, weak, cx)
    });
    let session = Uuid::new_v4();
    let (reply, receive) = mpsc::sync_channel(1);
    sender
        .try_send(CounterCommand {
            body: CounterBody::Begin { session },
            reply,
            request: Arc::new(CounterRequestState::default()),
        })
        .expect("queue");
    assert!(
        matches!(receive.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "enqueue is not acceptance"
    );
    cx.run_until_parked();
    let capability = match receive.try_recv().expect("foreground reply") {
        IpcResponse::McpToolUsageCapability {
            capability: Some(capability),
        } => capability,
        _ => panic!("foreground capability"),
    };
    let (reply, receive) = mpsc::sync_channel(1);
    sender
        .try_send(CounterCommand {
            body: CounterBody::Finish {
                session,
                capability,
                success: true,
            },
            reply,
            request: Arc::new(CounterRequestState::default()),
        })
        .expect("finish queue");
    cx.executor().advance_clock(Duration::from_millis(10));
    cx.run_until_parked();
    assert!(matches!(
        receive.try_recv().expect("finish reply"),
        IpcResponse::McpToolUsageFinished { accepted: true }
    ));
    assert_counts(&app, cx, 1);
    drop(legacy_sender);
    cx.executor().advance_clock(Duration::from_millis(10));
    cx.run_until_parked();
    drop(foreground);
}

/// The identity is stable, so time passing is not an epoch change and a
/// reservation survives it. An explicit reset is what reclaims the ledger.
#[gpui::test]
fn ipc_stable_identity_keeps_the_ledger_and_reset_reclaims_it(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let session = Uuid::new_v4();
    let capability = route.begin(session, cx).expect("begin");
    cx.update(|cx| {
        app.update(cx, |app, _| {
            let _source =
                app.telemetry_usage_source(chrono::Utc::now() + chrono::Duration::days(91));
        })
    });
    route.tick(cx);
    assert_eq!(route.slots(), 1, "a stable identity keeps the reservation");
    cx.update(|cx| {
        app.update(cx, |app, _| {
            app.reset_telemetry_install_id(chrono::Utc::now())
                .expect("reset");
        })
    });
    route.tick(cx);
    assert_eq!(route.slots(), 0, "a reset reclaims stale tickets");
    assert!(!route.finish(session, capability, true, cx));
    assert_counts(&app, cx, 0);
}

#[gpui::test]
fn ipc_source_unavailable_and_actual_weak_owner_drop(cx: &mut TestAppContext) {
    for mode in ["missing", "off", "undisclosed", "audit_disabled"] {
        let app = owner(cx, mode);
        let mut route = Route::new(&app);
        assert!(route.begin(Uuid::new_v4(), cx).is_none(), "{mode}");
        assert_eq!(route.slots(), 0);
    }
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let session = Uuid::new_v4();
    let capability = route.begin(session, cx).expect("capability");
    drop(app);
    cx.update(|_| {});
    assert!(route.owner.upgrade().is_none(), "actual entity destroyed");
    assert!(!route.finish(session, capability, true, cx));
    assert_eq!(route.slots(), 0);
    assert!(route.begin(session, cx).is_none());
}

#[gpui::test]
fn ipc_ledger_capacity_expiry_and_stale_epoch_reclaim(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let session = Uuid::new_v4();
    let mut first = None;
    for _ in 0..COUNTER_CAPACITY {
        first = first.or(route.begin(session, cx));
    }
    assert_eq!(route.slots(), COUNTER_CAPACITY);
    assert!(route.begin(session, cx).is_none());
    route.now += COUNTER_LEASE;
    assert!(!route.finish(session, first.expect("first"), true, cx));
    assert_eq!(route.slots(), 0);
    for _ in 0..COUNTER_CAPACITY {
        assert!(route.begin(session, cx).is_some());
    }
    cx.update(|cx| {
        app.update(cx, |app, _| {
            app.telemetry_mut()
                .expect("scheduler")
                .reset(chrono::Utc::now())
                .expect("reset");
        })
    });
    route.tick(cx);
    assert_eq!(route.slots(), 0, "reset reclaims without waiting 24 hours");
    assert!(route.begin(session, cx).is_some());
    cx.update(|cx| {
        app.update(cx, |app, _| {
            app.telemetry_mut()
                .expect("scheduler")
                .set_basic(false, chrono::Utc::now())
                .expect("Off");
        })
    });
    route.tick(cx);
    assert_eq!(route.slots(), 0);
    assert!(route.begin(session, cx).is_none());
    assert_counts(&app, cx, 0);
}

#[gpui::test]
fn ipc_audit_withdrawal_prunes_and_dropped_reply_releases_reservation(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let session = Uuid::new_v4();
    let capability = route.begin(session, cx).expect("begin");
    cx.update(|cx| app.read(cx).audit_service().set_enabled(false));
    route.tick(cx);
    assert_eq!(route.slots(), 0);
    assert!(!route.finish(session, capability, true, cx));
    cx.update(|cx| app.read(cx).audit_service().set_enabled(true));
    let (reply, receive) = mpsc::sync_channel(1);
    drop(receive);
    route
        .sender
        .try_send(CounterCommand {
            body: CounterBody::Begin { session },
            reply,
            request: Arc::new(CounterRequestState::default()),
        })
        .expect("queue");
    route.tick(cx);
    assert_eq!(route.slots(), 0, "failed oneshot send discards reservation");
    assert_counts(&app, cx, 0);
}

#[gpui::test]
fn ipc_channel_full_and_handler_timeout_do_not_block_foreground(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    for _ in 0..COUNTER_CAPACITY {
        let (reply, _receive) = mpsc::sync_channel(1);
        route
            .sender
            .try_send(CounterCommand {
                body: CounterBody::Begin {
                    session: Uuid::new_v4(),
                },
                reply,
                request: Arc::new(CounterRequestState::default()),
            })
            .expect("fill channel");
    }
    let (mut client, server) = UnixStream::pair().expect("socket pair");
    let sender = route.sender.clone();
    let (legacy, _receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        handle_connection(
            server,
            &legacy,
            "fixture-token",
            &sender,
            COUNTER_REPLY_TIMEOUT,
        )
        .expect("handler")
    });
    framing::send_msg(
        &mut client,
        &AppControlRequest::new(
            1,
            Some("fixture-token".into()),
            IpcMessage::BeginMcpToolUsage {
                session_nonce: Uuid::new_v4(),
            },
        ),
    )
    .expect("request");
    let response: AppControlResponse = framing::recv_msg(&mut client).expect("full reply");
    assert!(matches!(
        response.body,
        IpcResponse::McpToolUsageCapability { capability: None }
    ));
    worker.join().expect("worker");
    for _ in 0..COUNTER_CAPACITY {
        route.tick(cx);
    }
    assert_eq!(route.slots(), 0);
    let (mut client, server) = UnixStream::pair().expect("timeout pair");
    let sender = route.sender.clone();
    let (legacy, _receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        handle_connection(
            server,
            &legacy,
            "fixture-token",
            &sender,
            COUNTER_REPLY_TIMEOUT,
        )
        .expect("handler")
    });
    framing::send_msg(
        &mut client,
        &AppControlRequest::new(
            1,
            Some("fixture-token".into()),
            IpcMessage::BeginMcpToolUsage {
                session_nonce: Uuid::new_v4(),
            },
        ),
    )
    .expect("request");
    let response: AppControlResponse =
        framing::recv_msg(&mut client).expect("bounded timeout reply");
    assert!(matches!(
        response.body,
        IpcResponse::McpToolUsageCapability { capability: None }
    ));
    worker.join().expect("worker");
    route.tick(cx);
    assert_eq!(route.slots(), 0, "timed-out command creates no reservation");
    assert_counts(&app, cx, 0);
}

#[test]
fn ipc_poisoned_ticket_pruning_is_fail_closed() {
    let poison = Arc::new(AtomicBool::new(false));
    let mut controller = dbflux_core::observability::usage::UsageController::with_permission({
        let poison = poison.clone();
        Arc::new(move || {
            assert!(
                !poison.load(std::sync::atomic::Ordering::SeqCst),
                "fixture permission panic"
            );
            true
        })
    });
    controller.set_enabled(true).expect("enable");
    let ticket = controller.source().ticket().expect("gate").expect("ticket");
    let mut ledger = CounterLedger::new();
    assert!(
        ledger
            .begin(Uuid::new_v4(), ticket.clone(), Instant::now())
            .is_some()
    );
    poison.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ticket.is_current())).is_err()
    );
    assert_eq!(
        ticket.is_current(),
        Err(dbflux_core::observability::usage::UsageError::Poisoned)
    );
    ledger.prune(Instant::now());
    assert!(ledger.entries.iter().all(Option::is_none));
}

#[gpui::test]
fn ipc_finish_receiver_abandoned_before_adoption(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let session = Uuid::new_v4();
    let capability = route.begin(session, cx).expect("authenticated begin");
    let (command, receive) = CounterCommand::new(CounterBody::Finish {
        session,
        capability,
        success: true,
    });
    let request = command.request.clone();
    route.sender.try_send(command).expect("finish queue");
    drop(receive);
    route.tick(cx);
    assert_counts(&app, cx, 0);
    assert_eq!(
        request.phase.load(std::sync::atomic::Ordering::Acquire),
        CounterRequestPhase::Cancelled as u8
    );
    assert_eq!(route.slots(), 1, "unadopted finish preserves reservation");
}

#[gpui::test]
fn ipc_cancelled_finish_never_credits(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let session = Uuid::new_v4();
    let capability = route.begin(session, cx).expect("begin");
    let (command, receive) = CounterCommand::new(CounterBody::Finish {
        session,
        capability,
        success: true,
    });
    let request = command.request.clone();
    route.sender.try_send(command).expect("queue finish");
    assert!(matches!(
        receive.wait(Duration::ZERO),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    assert_eq!(
        request.phase.load(std::sync::atomic::Ordering::Acquire),
        CounterRequestPhase::Cancelled as u8
    );
    drop(receive);
    route.tick(cx);
    assert_counts(&app, cx, 0);
    route.now += COUNTER_LEASE;
    route.tick(cx);
    assert_eq!(route.slots(), 0);
}

#[gpui::test]
fn ipc_finish_cancellation_at_atomic_adoption_boundary(cx: &mut TestAppContext) {
    for cancellation_first in [true, false] {
        for timeout in [false, true] {
            let app = owner(cx, "basic");
            let mut route = Route::new(&app);
            let session = Uuid::new_v4();
            let capability = route.begin(session, cx).expect("authenticated begin");
            let (command, receive) = CounterCommand::new(CounterBody::Finish {
                session,
                capability,
                success: true,
            });
            let request = command.request.clone();
            route.sender.try_send(command).expect("queue finish");
            let command = route.receiver.try_recv().expect("actual queued command");
            let abandon: Box<dyn FnOnce()> = Box::new(move || {
                if timeout {
                    assert!(matches!(
                        receive.wait(Duration::ZERO),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    ));
                }
                drop(receive);
            });
            let hooks = if cancellation_first {
                AdoptionHooks {
                    before: Some(abandon),
                    after: None,
                }
            } else {
                AdoptionHooks {
                    before: None,
                    after: Some(abandon),
                }
            };
            // This seam brackets the same CAS used by the real processor;
            // it does not change the foreground task's scheduling.
            cx.update(|cx| {
                process_counter_command_at_adoption(
                    command,
                    &route.owner,
                    &mut route.ledger,
                    route.now,
                    hooks,
                    cx,
                );
            });
            let phase = if cancellation_first {
                CounterRequestPhase::Cancelled
            } else {
                CounterRequestPhase::Adopted
            };
            assert_eq!(
                request.phase.load(std::sync::atomic::Ordering::Acquire),
                phase as u8
            );
            assert_counts(&app, cx, u64::from(!cancellation_first));
            assert_eq!(route.slots(), usize::from(cancellation_first));
            assert_eq!(
                route.finish(session, capability, true, cx),
                cancellation_first
            );
            assert!(!route.finish(session, capability, true, cx));
            assert_counts(&app, cx, 1);
        }
    }
}

#[gpui::test]
fn ipc_begin_adopted_reply_loss_discards_reservation(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let (command, receive) = CounterCommand::new(CounterBody::Begin {
        session: Uuid::new_v4(),
    });
    let request = command.request.clone();
    route.sender.try_send(command).expect("queue begin");
    let command = route.receiver.try_recv().expect("actual queued begin");
    let hooks = AdoptionHooks {
        before: None,
        after: Some(Box::new(move || drop(receive))),
    };
    cx.update(|cx| {
        process_counter_command_at_adoption(
            command,
            &route.owner,
            &mut route.ledger,
            route.now,
            hooks,
            cx,
        );
    });
    assert_eq!(
        request.phase.load(std::sync::atomic::Ordering::Acquire),
        CounterRequestPhase::Adopted as u8
    );
    assert_eq!(route.slots(), 0, "undelivered adopted Begin is discarded");
    assert_counts(&app, cx, 0);
}

#[test]
fn ipc_request_guard_unwind_cancels_pending() {
    let (command, receive) = CounterCommand::new(CounterBody::Begin {
        session: Uuid::new_v4(),
    });
    let request = command.request.clone();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _receive = receive;
            panic!("fixture request unwind");
        }))
        .is_err()
    );
    assert_eq!(
        request.phase.load(std::sync::atomic::Ordering::Acquire),
        CounterRequestPhase::Cancelled as u8
    );
    assert!(!request.adopt());
}

#[test]
fn ipc_atomic_request_claim_has_one_winner_at_barriers() {
    for cancellation_first in [true, false] {
        let request = Arc::new(CounterRequestState::default());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker = thread::spawn({
            let request = request.clone();
            let barrier = barrier.clone();
            move || {
                if cancellation_first {
                    assert!(request.cancel_pending());
                    barrier.wait();
                } else {
                    barrier.wait();
                    assert!(!request.cancel_pending(), "adoption cannot be stolen");
                }
            }
        });
        if cancellation_first {
            barrier.wait();
            assert!(!request.adopt());
        } else {
            assert!(request.adopt());
            barrier.wait();
        }
        worker.join().expect("claim worker");
        assert!(!request.adopt(), "no second adoption");
        assert!(!request.cancel_pending(), "no second cancellation");
    }
}

#[gpui::test]
fn ipc_auth_version_and_legacy_requests_keep_ordering(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let route = Route::new(&app);
    for (auth, major, minor, body, denied) in [
        (
            None,
            1,
            1,
            IpcMessage::BeginMcpToolUsage {
                session_nonce: Uuid::new_v4(),
            },
            true,
        ),
        (
            Some("wrong"),
            1,
            1,
            IpcMessage::BeginMcpToolUsage {
                session_nonce: Uuid::new_v4(),
            },
            true,
        ),
        (
            Some("fixture-token"),
            2,
            1,
            IpcMessage::BeginMcpToolUsage {
                session_nonce: Uuid::new_v4(),
            },
            true,
        ),
        (
            Some("fixture-token"),
            1,
            0,
            IpcMessage::BeginMcpToolUsage {
                session_nonce: Uuid::new_v4(),
            },
            true,
        ),
        (Some("fixture-token"), 1, 0, IpcMessage::Ping, false),
        (Some("fixture-token"), 1, 0, IpcMessage::Focus, false),
        (
            Some("fixture-token"),
            1,
            0,
            IpcMessage::OpenScript {
                path: "nonexistent-fixture.lua".into(),
            },
            false,
        ),
    ] {
        let (mut client, server) = UnixStream::pair().expect("pair");
        let (legacy, receive) = mpsc::channel();
        let sender = route.sender.clone();
        let worker = thread::spawn(move || {
            handle_connection(
                server,
                &legacy,
                "fixture-token",
                &sender,
                COUNTER_REPLY_TIMEOUT,
            )
            .expect("handler")
        });
        let mut request = AppControlRequest::new(1, auth.map(str::to_string), body.clone());
        request.protocol_version = dbflux_ipc::ProtocolVersion::new(major, minor);
        framing::send_msg(&mut client, &request).expect("request");
        let response: AppControlResponse = framing::recv_msg(&mut client).expect("reply");
        worker.join().expect("worker");
        assert_eq!(matches!(response.body, IpcResponse::Error { .. }), denied);
        assert!(
            route.receiver.try_recv().is_err(),
            "denial and legacy requests never enqueue counters"
        );
        assert_eq!(
            receive.try_recv().is_ok(),
            matches!(body, IpcMessage::Focus | IpcMessage::OpenScript { .. })
        );
    }
}

/// Reads a prepared request and fails every write, like a client that closed
/// its socket after its own deadline.
struct ClosedReply(std::io::Cursor<Vec<u8>>);
impl std::io::Read for ClosedReply {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buffer)
    }
}
impl std::io::Write for ClosedReply {
    fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
        Err(std::io::ErrorKind::BrokenPipe.into())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[gpui::test]
fn a_begin_reply_that_cannot_be_written_releases_its_reservation(cx: &mut TestAppContext) {
    let app = owner(cx, "basic");
    let mut route = Route::new(&app);
    let mut request = Vec::new();
    framing::send_msg(
        &mut request,
        &AppControlRequest::new(
            1,
            Some("fixture-token".into()),
            IpcMessage::BeginMcpToolUsage {
                session_nonce: Uuid::new_v4(),
            },
        ),
    )
    .expect("request bytes");
    let sender = route.sender.clone();
    let (legacy, _receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        handle_connection(
            ClosedReply(std::io::Cursor::new(request)),
            &legacy,
            "fixture-token",
            &sender,
            TEST_REPLY_WAIT,
        )
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while route.slots() == 0 {
        assert!(Instant::now() < deadline, "the begin must reserve a slot");
        route.tick(cx);
        thread::yield_now();
    }
    assert!(worker.join().expect("worker").is_err());
    route.tick(cx);
    assert_eq!(route.slots(), 0, "an undelivered capability holds no slot");
}
