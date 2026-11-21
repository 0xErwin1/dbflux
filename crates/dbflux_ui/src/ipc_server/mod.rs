use crate::ui::views::workspace::Workspace;
use dbflux_ipc::{
    APP_CONTROL_VERSION, framing,
    protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse},
};
use gpui::*;
use interprocess::local_socket::{Listener as IpcListener, traits::Listener as _};
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

#[cfg(feature = "telemetry")]
use dbflux_core::observability::usage::{UsageFeature, UsageTicket};
#[cfg(feature = "telemetry")]
use dbflux_ui_base::AppStateEntity;
#[cfg(feature = "telemetry")]
use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicUsize, Ordering},
    mpsc::SyncSender,
};
#[cfg(feature = "telemetry")]
use std::time::Instant;
#[cfg(feature = "telemetry")]
use uuid::Uuid;

pub struct IpcServer;

#[cfg(feature = "telemetry")]
const COUNTER_CAPACITY: usize = 64;
#[cfg(feature = "telemetry")]
const COUNTER_REPLY_TIMEOUT: Duration = Duration::from_millis(250);
// This best-effort lease bounds abandoned reservations, not MCP execution time.
#[cfg(feature = "telemetry")]
const COUNTER_LEASE: Duration = Duration::from_secs(24 * 60 * 60);

#[cfg(feature = "telemetry")]
enum CounterBody {
    Begin {
        session: Uuid,
    },
    Finish {
        session: Uuid,
        capability: Uuid,
        success: bool,
    },
    /// The socket write of an issued capability failed, so the client never
    /// received it and the reservation can be dropped.
    Release {
        capability: Uuid,
    },
}
#[cfg(feature = "telemetry")]
impl CounterBody {
    fn unavailable(&self) -> IpcResponse {
        match self {
            Self::Begin { .. } => IpcResponse::McpToolUsageCapability { capability: None },
            Self::Finish { .. } | Self::Release { .. } => {
                IpcResponse::McpToolUsageFinished { accepted: false }
            }
        }
    }
}
#[cfg(feature = "telemetry")]
struct CounterCommand {
    body: CounterBody,
    reply: SyncSender<IpcResponse>,
    request: Arc<CounterRequestState>,
}
#[cfg(feature = "telemetry")]
#[repr(u8)]
enum CounterRequestPhase {
    Pending,
    Adopted,
    Cancelled,
}
#[cfg(feature = "telemetry")]
#[derive(Default)]
struct CounterRequestState {
    phase: AtomicU8,
}
#[cfg(feature = "telemetry")]
impl CounterRequestState {
    // Adoption and abandonment compete for Pending exactly once. A lost
    // reply cannot withdraw foreground work that has already been adopted.
    fn adopt(&self) -> bool {
        self.phase
            .compare_exchange(
                CounterRequestPhase::Pending as u8,
                CounterRequestPhase::Adopted as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
    fn cancel_pending(&self) -> bool {
        self.phase
            .compare_exchange(
                CounterRequestPhase::Pending as u8,
                CounterRequestPhase::Cancelled as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}
#[cfg(feature = "telemetry")]
struct CounterReplyReceiver {
    receiver: Receiver<IpcResponse>,
    request: Arc<CounterRequestState>,
}
#[cfg(feature = "telemetry")]
impl CounterReplyReceiver {
    fn wait(&self, timeout: Duration) -> Result<IpcResponse, mpsc::RecvTimeoutError> {
        let response = self.receiver.recv_timeout(timeout);
        if response.is_err() {
            self.request.cancel_pending();
        }
        response
    }
}
#[cfg(feature = "telemetry")]
impl Drop for CounterReplyReceiver {
    fn drop(&mut self) {
        self.request.cancel_pending();
    }
}
#[cfg(feature = "telemetry")]
impl CounterCommand {
    fn new(body: CounterBody) -> (Self, CounterReplyReceiver) {
        let (reply, receiver) = mpsc::sync_channel(1);
        let request = Arc::new(CounterRequestState::default());
        (
            Self {
                body,
                reply,
                request: request.clone(),
            },
            CounterReplyReceiver { receiver, request },
        )
    }
}
#[cfg(feature = "telemetry")]
struct CounterEntry {
    ticket: UsageTicket,
    session: Uuid,
    capability: Uuid,
    expires: Instant,
}
#[cfg(feature = "telemetry")]
struct CounterLedger {
    entries: [Option<CounterEntry>; COUNTER_CAPACITY],
}
#[cfg(feature = "telemetry")]
impl CounterLedger {
    fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
        }
    }
    fn clear(&mut self) {
        for entry in &mut self.entries {
            *entry = None;
        }
    }
    fn prune(&mut self, now: Instant) {
        for slot in &mut self.entries {
            let keep = slot.as_ref().is_some_and(|entry| {
                if now >= entry.expires {
                    return false;
                }
                match entry.ticket.is_current() {
                    Ok(current) => current,
                    Err(_) => {
                        log::warn!("IPC usage ticket validation failed");
                        false
                    }
                }
            });
            if !keep {
                *slot = None;
            }
        }
    }
    fn begin(&mut self, session: Uuid, ticket: UsageTicket, now: Instant) -> Option<Uuid> {
        let slot = self.entries.iter_mut().find(|slot| slot.is_none())?;
        let capability = Uuid::new_v4();
        *slot = Some(CounterEntry {
            ticket,
            session,
            capability,
            expires: now + COUNTER_LEASE,
        });
        Some(capability)
    }
    fn discard(&mut self, capability: Uuid) {
        if let Some(slot) = self.entries.iter_mut().find(|slot| {
            slot.as_ref()
                .is_some_and(|entry| entry.capability == capability)
        }) {
            *slot = None;
        }
    }
    fn finish(&mut self, session: Uuid, capability: Uuid, success: bool) -> bool {
        let Some(slot) = self.entries.iter_mut().find(|slot| {
            slot.as_ref()
                .is_some_and(|entry| entry.capability == capability && entry.session == session)
        }) else {
            return false;
        };
        let Some(entry) = slot.take() else {
            return false;
        };
        if !success {
            return false;
        }
        match entry.ticket.record(UsageFeature::McpTool) {
            Ok(recorded) => recorded,
            Err(_) => {
                log::warn!("IPC usage credit failed");
                false
            }
        }
    }
}

#[cfg(feature = "telemetry")]
fn counter_request(
    sender: &SyncSender<CounterCommand>,
    body: CounterBody,
    reply_timeout: Duration,
) -> IpcResponse {
    let unavailable = body.unavailable();
    let (command, receive) = CounterCommand::new(body);
    if sender.try_send(command).is_err() {
        return unavailable;
    }
    receive.wait(reply_timeout).unwrap_or(unavailable)
}

#[cfg(test)]
#[cfg(feature = "telemetry")]
#[derive(Default)]
struct AdoptionHooks<'a> {
    before: Option<Box<dyn FnOnce() + 'a>>,
    after: Option<Box<dyn FnOnce() + 'a>>,
}

// Called exclusively inside the existing foreground cx.update, never on the socket thread.
#[cfg(feature = "telemetry")]
fn process_counter_command(
    command: CounterCommand,
    owner: &WeakEntity<AppStateEntity>,
    ledger: &mut CounterLedger,
    now: Instant,
    cx: &mut App,
) {
    process_counter_command_at_adoption(
        command,
        owner,
        ledger,
        now,
        #[cfg(test)]
        AdoptionHooks::default(),
        cx,
    );
}

#[cfg(feature = "telemetry")]
fn process_counter_command_at_adoption(
    command: CounterCommand,
    owner: &WeakEntity<AppStateEntity>,
    ledger: &mut CounterLedger,
    now: Instant,
    #[cfg(test)] mut hooks: AdoptionHooks<'_>,
    cx: &mut App,
) {
    ledger.prune(now);
    let Some(app) = owner.upgrade() else {
        ledger.clear();
        if command.reply.try_send(command.body.unavailable()).is_err() {
            log::debug!("IPC usage reply recipient unavailable");
        }
        return;
    };
    #[cfg(test)]
    if let Some(before) = hooks.before.take() {
        before();
    }
    if !command.request.adopt() {
        return;
    }
    #[cfg(test)]
    if let Some(after) = hooks.after.take() {
        after();
    }
    let response = match command.body {
        CounterBody::Begin { session } => {
            let ticket = app.update(cx, |app, _| {
                app.telemetry_usage_source(chrono::Utc::now())
                    .and_then(|source| match source.ticket() {
                        Ok(ticket) => ticket,
                        Err(_) => {
                            log::warn!("IPC usage ticket acquisition failed");
                            None
                        }
                    })
            });
            let capability = ticket.and_then(|ticket| ledger.begin(session, ticket, now));
            IpcResponse::McpToolUsageCapability { capability }
        }
        CounterBody::Finish {
            session,
            capability,
            success,
        } => IpcResponse::McpToolUsageFinished {
            accepted: ledger.finish(session, capability, success),
        },
        CounterBody::Release { capability } => {
            ledger.discard(capability);
            IpcResponse::McpToolUsageFinished { accepted: false }
        }
    };
    let reservation = match &response {
        IpcResponse::McpToolUsageCapability { capability } => *capability,
        _ => None,
    };
    if command.reply.try_send(response).is_err() {
        if let Some(capability) = reservation {
            ledger.discard(capability);
        }
        log::debug!("IPC usage reply recipient unavailable");
    }
}

#[cfg(feature = "telemetry")]
fn process_counter_commands(
    receiver: &Receiver<CounterCommand>,
    owner: &WeakEntity<AppStateEntity>,
    ledger: &mut CounterLedger,
    now: Instant,
    cx: &mut App,
) {
    if owner.upgrade().is_none() {
        ledger.clear();
    } else {
        ledger.prune(now);
    }
    // A bounded quantum keeps both command queues and window work responsive.
    if let Ok(command) = receiver.try_recv() {
        process_counter_command(command, owner, ledger, now, cx);
    }
}

enum IpcCommand {
    OpenScript { path: PathBuf },
    Focus,
}

impl IpcServer {
    pub fn start_with_listener(
        listener: IpcListener,
        workspace: Entity<Workspace>,
        window: AnyWindowHandle,
        auth_token: String,
        #[cfg(feature = "telemetry")] app_state: WeakEntity<AppStateEntity>,
        cx: &mut App,
    ) {
        let (cmd_tx, cmd_rx): (Sender<IpcCommand>, Receiver<IpcCommand>) = mpsc::channel();

        #[cfg(feature = "telemetry")]
        let (counter_tx, counter_rx) = mpsc::sync_channel(COUNTER_CAPACITY);

        thread::spawn(move || {
            accept_loop(
                listener,
                cmd_tx,
                std::sync::Arc::new(auth_token),
                #[cfg(feature = "telemetry")]
                counter_tx,
            );
        });

        cx.spawn(async move |cx| {
            process_commands(
                cmd_rx,
                workspace,
                window,
                #[cfg(feature = "telemetry")]
                counter_rx,
                #[cfg(feature = "telemetry")]
                app_state,
                cx.clone(),
            )
            .await;
        })
        .detach();
    }
}

/// Connections served at once. A counter request can wait for the foreground,
/// so each connection gets its own short-lived thread, within this bound;
/// beyond it the accept thread serves the connection itself.
#[cfg(feature = "telemetry")]
const MAX_CONCURRENT_CONNECTIONS: usize = 8;

fn accept_loop(
    listener: IpcListener,
    cmd_tx: Sender<IpcCommand>,
    auth_token: std::sync::Arc<String>,
    #[cfg(feature = "telemetry")] counter_tx: SyncSender<CounterCommand>,
) {
    #[cfg(feature = "telemetry")]
    let in_flight = Arc::new(AtomicUsize::new(0));
    loop {
        match listener.accept() {
            Ok(stream) => {
                #[cfg(feature = "telemetry")]
                if in_flight.fetch_add(1, Ordering::AcqRel) < MAX_CONCURRENT_CONNECTIONS {
                    let cmd_tx = cmd_tx.clone();
                    let auth_token = auth_token.clone();
                    let counter_tx = counter_tx.clone();
                    let in_flight = in_flight.clone();
                    thread::spawn(move || {
                        serve_connection(stream, &cmd_tx, &auth_token, &counter_tx);
                        in_flight.fetch_sub(1, Ordering::AcqRel);
                    });
                    continue;
                } else {
                    in_flight.fetch_sub(1, Ordering::AcqRel);
                }
                serve_connection(
                    stream,
                    &cmd_tx,
                    &auth_token,
                    #[cfg(feature = "telemetry")]
                    &counter_tx,
                );
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                log::warn!("IPC accept error: {}", e);
                break;
            }
        }
    }
}

fn serve_connection<S: io::Read + io::Write>(
    stream: S,
    cmd_tx: &Sender<IpcCommand>,
    auth_token: &str,
    #[cfg(feature = "telemetry")] counter_tx: &SyncSender<CounterCommand>,
) {
    if let Err(error) = handle_connection(
        stream,
        cmd_tx,
        auth_token,
        #[cfg(feature = "telemetry")]
        counter_tx,
        #[cfg(feature = "telemetry")]
        COUNTER_REPLY_TIMEOUT,
    ) {
        log::warn!("IPC connection error: {}", error);
    }
}

fn handle_connection<S: io::Read + io::Write>(
    mut stream: S,
    cmd_tx: &Sender<IpcCommand>,
    expected_auth_token: &str,
    #[cfg(feature = "telemetry")] counter_tx: &SyncSender<CounterCommand>,
    #[cfg(feature = "telemetry")] counter_reply_timeout: Duration,
) -> io::Result<()> {
    let request: AppControlRequest = framing::recv_msg(&mut stream)?;
    let request_id = request.request_id;

    if !request
        .protocol_version
        .is_compatible_with(APP_CONTROL_VERSION)
    {
        let response = AppControlResponse::ok(
            request_id,
            IpcResponse::Error {
                message: "incompatible app-control protocol version".to_string(),
            },
        );
        framing::send_msg(&mut stream, &response)?;
        return Ok(());
    }

    if request.auth_token.as_deref() != Some(expected_auth_token) {
        let response = AppControlResponse::ok(
            request_id,
            IpcResponse::Error {
                message: "unauthorized app-control request".to_string(),
            },
        );
        framing::send_msg(&mut stream, &response)?;
        return Ok(());
    }

    #[cfg(feature = "telemetry")]
    if request.protocol_version.minor < 1
        && matches!(
            &request.body,
            IpcMessage::BeginMcpToolUsage { .. } | IpcMessage::FinishMcpToolUsage { .. }
        )
    {
        framing::send_msg(
            &mut stream,
            &AppControlResponse::ok(
                request_id,
                IpcResponse::Error {
                    message: "incompatible app-control protocol version".into(),
                },
            ),
        )?;
        return Ok(());
    }

    let response_body = match request.body {
        #[cfg(feature = "telemetry")]
        IpcMessage::BeginMcpToolUsage { session_nonce } => counter_request(
            counter_tx,
            CounterBody::Begin {
                session: session_nonce,
            },
            counter_reply_timeout,
        ),
        #[cfg(feature = "telemetry")]
        IpcMessage::FinishMcpToolUsage {
            session_nonce,
            capability,
            success,
        } => counter_request(
            counter_tx,
            CounterBody::Finish {
                session: session_nonce,
                capability,
                success,
            },
            counter_reply_timeout,
        ),
        IpcMessage::Ping => IpcResponse::Pong {
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        IpcMessage::OpenScript { path } => {
            let path = path.canonicalize().unwrap_or(path);
            if cmd_tx.send(IpcCommand::OpenScript { path }).is_ok() {
                IpcResponse::Ok
            } else {
                IpcResponse::Error {
                    message: "failed to send command".into(),
                }
            }
        }
        IpcMessage::Focus => {
            if cmd_tx.send(IpcCommand::Focus).is_ok() {
                IpcResponse::Ok
            } else {
                IpcResponse::Error {
                    message: "failed to send command".into(),
                }
            }
        }
    };

    #[cfg(feature = "telemetry")]
    let issued = match &response_body {
        IpcResponse::McpToolUsageCapability {
            capability: Some(capability),
        } => Some(*capability),
        _ => None,
    };
    let response = AppControlResponse::ok(request_id, response_body);

    let sent = framing::send_msg(&mut stream, &response);
    // A capability the client never received would hold its slot until the
    // lease expires, so the foreground is told to drop it.
    #[cfg(feature = "telemetry")]
    if sent.is_err()
        && let Some(capability) = issued
    {
        let (release, _reply) = CounterCommand::new(CounterBody::Release { capability });
        if counter_tx.try_send(release).is_err() {
            log::debug!("IPC usage release could not be queued");
        }
    }
    sent?;
    Ok(())
}

async fn process_commands(
    cmd_rx: Receiver<IpcCommand>,
    workspace: Entity<Workspace>,
    window: AnyWindowHandle,
    #[cfg(feature = "telemetry")] counter_rx: Receiver<CounterCommand>,
    #[cfg(feature = "telemetry")] app_state: WeakEntity<AppStateEntity>,
    cx: AsyncApp,
) {
    #[cfg(feature = "telemetry")]
    let mut ledger = CounterLedger::new();
    loop {
        #[cfg(feature = "telemetry")]
        cx.update(|cx| {
            process_counter_commands(&counter_rx, &app_state, &mut ledger, Instant::now(), cx)
        });
        match cmd_rx.try_recv() {
            Ok(cmd) => {
                cx.update(|cx| match cmd {
                    IpcCommand::OpenScript { path } => {
                        workspace.update(cx, |ws, cx| {
                            ws.open_script_from_path(path, cx);
                        });
                    }
                    IpcCommand::Focus => {
                        if let Err(e) = window.update(cx, |_root, window, _cx| {
                            window.activate_window();
                        }) {
                            log::warn!("Failed to focus main window via IPC: {:?}", e);
                        }
                    }
                });
            }
            Err(mpsc::TryRecvError::Empty) => {
                cx.background_executor()
                    .timer(Duration::from_millis(10))
                    .await;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

#[cfg(test)]
#[cfg(all(feature = "telemetry", unix))]
mod tests;
