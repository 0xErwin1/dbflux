pub mod category;
pub mod layer;
pub mod queue;
pub mod writer;

pub use writer::FmtWriter;

use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

use crate::observability::source::EventSink;
use crate::observability::types::EventSeverity;

use self::layer::AuditLayer;
use self::queue::BridgeQueue;

const DEFAULT_QUEUE_CAPACITY: usize = 512;
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(2);

/// Guards against double-initialization of the global tracing subscriber.
///
/// `tracing_log::LogTracer::init()` panics on second call, and the subscriber
/// can only be installed once per process — flipping this flag first lets us
/// return a typed error instead of crashing.
static INIT_GUARD: AtomicBool = AtomicBool::new(false);

// ============================================================================
// LevelCode
// ============================================================================

/// Numeric representation of a severity level stored in an `AtomicU8`.
///
/// The ordinal matches logical ordering: higher means more severe, so a gate
/// `event_code >= min_code` passes more severe events.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelCode {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl From<EventSeverity> for LevelCode {
    fn from(s: EventSeverity) -> Self {
        match s {
            EventSeverity::Trace => LevelCode::Trace,
            EventSeverity::Debug => LevelCode::Debug,
            EventSeverity::Info => LevelCode::Info,
            EventSeverity::Warn => LevelCode::Warn,
            EventSeverity::Error | EventSeverity::Fatal => LevelCode::Error,
        }
    }
}

// ============================================================================
// BridgeConfig
// ============================================================================

/// Configuration for initializing the tracing bridge.
pub struct BridgeConfig {
    /// Minimum severity for audit capture.
    pub min_level: EventSeverity,
    /// Capacity of the bounded channel between `AuditLayer` and the drain thread.
    pub queue_capacity: usize,
    /// Whether to compose the `AuditLayer` into the subscriber.
    ///
    /// Set `false` for the driver host, which has no SQLite store.
    pub include_audit_layer: bool,
    /// Where the fmt layer writes its output.
    pub fmt_writer: FmtWriter,
    /// Default `EnvFilter` directive string (e.g. `"info,hyper=warn"`).
    pub env_filter_default: &'static str,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        BridgeConfig {
            min_level: EventSeverity::Info,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            include_audit_layer: true,
            fmt_writer: FmtWriter::Stderr,
            env_filter_default: "info",
        }
    }
}

// ============================================================================
// BridgeHandle
// ============================================================================

/// Handle returned by `init_tracing`.
///
/// The caller must keep this alive for the full process lifetime.
/// Dropping it without calling `shutdown()` first abandons the drain thread.
pub struct BridgeHandle {
    pub min_level: Arc<AtomicU8>,
    pub drop_counter: Arc<AtomicU64>,
    pub audit_slot: Arc<OnceLock<Arc<dyn EventSink>>>,
    in_flight: Arc<AtomicUsize>,
    drain_stop: Arc<AtomicBool>,
    drain_thread: Option<JoinHandle<()>>,
    _fmt_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

impl BridgeHandle {
    /// Installs the audit sink, enabling the drain thread to forward records.
    pub fn install_sink(&self, sink: Arc<dyn EventSink>) -> Result<(), AlreadySetError> {
        self.audit_slot.set(sink).map_err(|_| AlreadySetError)
    }

    /// Updates the capture threshold without reinitializing the subscriber.
    pub fn set_min_level(&self, level: EventSeverity) {
        let code = LevelCode::from(level) as u8;
        self.min_level.store(code, Ordering::Relaxed);
    }

    /// Returns the current count of dropped events (queue overflow or pre-sink-install).
    pub fn drop_count(&self) -> u64 {
        self.drop_counter.load(Ordering::Relaxed)
    }

    /// Returns the current estimate of events in the channel awaiting drain.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Signals the drain thread to flush remaining events and exit.
    ///
    /// Waits up to `SHUTDOWN_BUDGET` (2 seconds). Returns `Err` on timeout or panic.
    pub fn shutdown(mut self) -> Result<(), ShutdownError> {
        self.drain_stop.store(true, Ordering::Relaxed);

        let thread = match self.drain_thread.take() {
            Some(t) => t,
            None => return Ok(()),
        };

        let deadline = Instant::now() + SHUTDOWN_BUDGET;
        loop {
            if thread.is_finished() {
                return thread.join().map_err(|_| ShutdownError::JoinPanic);
            }
            if Instant::now() >= deadline {
                let remaining = self.in_flight.load(Ordering::Relaxed);
                return Err(ShutdownError::DrainTimeout {
                    remaining_in_flight: remaining,
                });
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

// ============================================================================
// Error types
// ============================================================================

#[derive(Debug)]
pub enum InitError {
    AlreadyInitialized,
    LogTracerInit(tracing_log::log_tracer::SetLoggerError),
    SubscriberInstall(tracing_subscriber::util::TryInitError),
    WriterIo(io::Error),
    DrainThreadSpawn(io::Error),
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InitError::AlreadyInitialized => write!(f, "tracing already initialized"),
            InitError::LogTracerInit(e) => write!(f, "log tracer init failed: {e}"),
            InitError::SubscriberInstall(e) => write!(f, "subscriber install failed: {e}"),
            InitError::WriterIo(e) => write!(f, "writer io error: {e}"),
            InitError::DrainThreadSpawn(e) => write!(f, "drain thread spawn failed: {e}"),
        }
    }
}

impl std::error::Error for InitError {}

#[derive(Debug)]
pub enum ShutdownError {
    DrainTimeout { remaining_in_flight: usize },
    JoinPanic,
}

impl std::fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownError::DrainTimeout {
                remaining_in_flight,
            } => {
                write!(f, "drain timeout, {remaining_in_flight} events remaining")
            }
            ShutdownError::JoinPanic => write!(f, "drain thread panicked"),
        }
    }
}

impl std::error::Error for ShutdownError {}

#[derive(Debug)]
pub struct AlreadySetError;

impl std::fmt::Display for AlreadySetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "audit sink already installed")
    }
}

impl std::error::Error for AlreadySetError {}

// ============================================================================
// init_tracing — core entry point
// ============================================================================

/// Initializes the global tracing subscriber.
///
/// Must be called exactly once per process, before any `log::*!` or
/// `tracing::*!` calls. A second call returns `Err(InitError::AlreadyInitialized)`.
pub fn init_tracing(config: BridgeConfig) -> Result<BridgeHandle, InitError> {
    if INIT_GUARD
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        debug_assert!(
            false,
            "init_tracing called more than once — each binary main must call it exactly once"
        );
        return Err(InitError::AlreadyInitialized);
    }

    let min_level_arc = Arc::new(AtomicU8::new(LevelCode::from(config.min_level) as u8));
    let drop_counter_arc = Arc::new(AtomicU64::new(0));
    let in_flight_arc = Arc::new(AtomicUsize::new(0));
    let audit_slot: Arc<OnceLock<Arc<dyn EventSink>>> = Arc::new(OnceLock::new());
    let drain_stop = Arc::new(AtomicBool::new(false));

    let mut fmt_guard: Option<tracing_appender::non_blocking::WorkerGuard> = None;
    let mut drain_thread: Option<JoinHandle<()>> = None;

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.env_filter_default));

    match config.fmt_writer {
        FmtWriter::Stderr => {
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(true)
                .with_target(true);

            if config.include_audit_layer {
                let (bridge_queue, rx) = BridgeQueue::new(config.queue_capacity);
                let audit_layer = AuditLayer {
                    min_level: min_level_arc.clone(),
                    drop_counter: drop_counter_arc.clone(),
                    queue_tx: bridge_queue.sender.clone(),
                    in_flight: in_flight_arc.clone(),
                };
                drain_thread = Some(
                    queue::spawn_drain_thread(
                        rx,
                        audit_slot.clone(),
                        drop_counter_arc.clone(),
                        in_flight_arc.clone(),
                        drain_stop.clone(),
                    )
                    .map_err(InitError::DrainThreadSpawn)?,
                );
                Registry::default()
                    .with(env_filter)
                    .with(fmt_layer)
                    .with(audit_layer)
                    .try_init()
                    .map_err(InitError::SubscriberInstall)?;
            } else {
                Registry::default()
                    .with(env_filter)
                    .with(fmt_layer)
                    .try_init()
                    .map_err(InitError::SubscriberInstall)?;
            }
        }

        FmtWriter::File(path) => {
            // Use non-blocking for file too — the Mutex-based approach with
            // tracing_subscriber requires MakeWriter which needs Clone on the guard.
            // non_blocking handles this cleanly.
            let file = open_log_file(&path)?;
            let (non_blocking, guard) = tracing_appender::non_blocking(file);
            fmt_guard = Some(guard);

            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(true);

            if config.include_audit_layer {
                let (bridge_queue, rx) = BridgeQueue::new(config.queue_capacity);
                let audit_layer = AuditLayer {
                    min_level: min_level_arc.clone(),
                    drop_counter: drop_counter_arc.clone(),
                    queue_tx: bridge_queue.sender.clone(),
                    in_flight: in_flight_arc.clone(),
                };
                drain_thread = Some(
                    queue::spawn_drain_thread(
                        rx,
                        audit_slot.clone(),
                        drop_counter_arc.clone(),
                        in_flight_arc.clone(),
                        drain_stop.clone(),
                    )
                    .map_err(InitError::DrainThreadSpawn)?,
                );
                Registry::default()
                    .with(env_filter)
                    .with(fmt_layer)
                    .with(audit_layer)
                    .try_init()
                    .map_err(InitError::SubscriberInstall)?;
            } else {
                Registry::default()
                    .with(env_filter)
                    .with(fmt_layer)
                    .try_init()
                    .map_err(InitError::SubscriberInstall)?;
            }
        }

        FmtWriter::NonBlockingFile(path) => {
            let file = open_log_file(&path)?;
            let (non_blocking, guard) = tracing_appender::non_blocking(file);
            fmt_guard = Some(guard);

            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(true);

            if config.include_audit_layer {
                let (bridge_queue, rx) = BridgeQueue::new(config.queue_capacity);
                let audit_layer = AuditLayer {
                    min_level: min_level_arc.clone(),
                    drop_counter: drop_counter_arc.clone(),
                    queue_tx: bridge_queue.sender.clone(),
                    in_flight: in_flight_arc.clone(),
                };
                drain_thread = Some(
                    queue::spawn_drain_thread(
                        rx,
                        audit_slot.clone(),
                        drop_counter_arc.clone(),
                        in_flight_arc.clone(),
                        drain_stop.clone(),
                    )
                    .map_err(InitError::DrainThreadSpawn)?,
                );
                Registry::default()
                    .with(env_filter)
                    .with(fmt_layer)
                    .with(audit_layer)
                    .try_init()
                    .map_err(InitError::SubscriberInstall)?;
            } else {
                Registry::default()
                    .with(env_filter)
                    .with(fmt_layer)
                    .try_init()
                    .map_err(InitError::SubscriberInstall)?;
            }
        }
    }

    // `LogTracer::init()` registers the global `log` backend so that `log::*!`
    // macros are forwarded to the tracing subscriber.  A `SetLoggerError` means
    // another logger is already registered (e.g. the Rust test runner or a dep
    // that initialised first), which is acceptable — the tracing subscriber is
    // still installed and tracing events still work.
    let _ = LogTracer::init();

    Ok(BridgeHandle {
        min_level: min_level_arc,
        drop_counter: drop_counter_arc,
        audit_slot,
        in_flight: in_flight_arc,
        drain_stop,
        drain_thread,
        _fmt_guard: fmt_guard,
    })
}

fn open_log_file(path: &PathBuf) -> Result<std::fs::File, InitError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(InitError::WriterIo)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(InitError::WriterIo)
}
