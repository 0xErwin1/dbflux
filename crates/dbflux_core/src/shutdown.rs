use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Phase of the graceful shutdown process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ShutdownPhase {
    NotStarted = 0,
    SignalSent = 1,
    CancellingTasks = 2,
    ClosingConnections = 3,
    FlushingLogs = 4,
    Complete = 5,
    Failed = 6,
}

impl ShutdownPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => ShutdownPhase::NotStarted,
            1 => ShutdownPhase::SignalSent,
            2 => ShutdownPhase::CancellingTasks,
            3 => ShutdownPhase::ClosingConnections,
            4 => ShutdownPhase::FlushingLogs,
            5 => ShutdownPhase::Complete,
            _ => ShutdownPhase::Failed,
        }
    }

    /// Returns a human-readable message describing the current phase.
    pub fn message(&self) -> &'static str {
        match self {
            ShutdownPhase::NotStarted => "",
            ShutdownPhase::SignalSent => "Shutting down...",
            ShutdownPhase::CancellingTasks => "Cancelling tasks...",
            ShutdownPhase::ClosingConnections => "Closing connections...",
            ShutdownPhase::FlushingLogs => "Flushing logs...",
            ShutdownPhase::Complete => "Shutdown complete",
            ShutdownPhase::Failed => "Shutdown failed",
        }
    }

    /// Returns true if this phase should show a spinner in the UI.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            ShutdownPhase::SignalSent
                | ShutdownPhase::CancellingTasks
                | ShutdownPhase::ClosingConnections
                | ShutdownPhase::FlushingLogs
        )
    }
}

/// Coordinates graceful shutdown across the application.
///
/// Thread-safe and can be cloned to share across async tasks.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    shutdown_requested: Arc<AtomicBool>,
    phase: Arc<AtomicU8>,
    start_time: Arc<RwLock<Option<Instant>>>,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        Self {
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            phase: Arc::new(AtomicU8::new(ShutdownPhase::NotStarted as u8)),
            start_time: Arc::new(RwLock::new(None)),
        }
    }

    /// Request application shutdown and start the timer.
    ///
    /// Returns `true` if this call initiated shutdown, `false` if already shutting down.
    pub fn request_shutdown(&self) -> bool {
        let was_requested = self.shutdown_requested.swap(true, Ordering::SeqCst);

        if !was_requested {
            self.phase
                .store(ShutdownPhase::SignalSent as u8, Ordering::SeqCst);

            if let Ok(mut guard) = self.start_time.write() {
                *guard = Some(Instant::now());
            }
            true
        } else {
            false
        }
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    /// Get the current shutdown phase.
    pub fn phase(&self) -> ShutdownPhase {
        ShutdownPhase::from_u8(self.phase.load(Ordering::SeqCst))
    }

    /// Set the current shutdown phase.
    pub fn set_phase(&self, phase: ShutdownPhase) {
        self.phase.store(phase as u8, Ordering::SeqCst);
    }

    /// Advance to next phase only if currently in the expected phase.
    ///
    /// Returns `true` if the transition succeeded, `false` if the current phase
    /// didn't match the expected phase.
    pub fn advance_phase(&self, expected: ShutdownPhase, next: ShutdownPhase) -> bool {
        match self.phase.compare_exchange(
            expected as u8,
            next as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => true,
            Err(actual) => {
                log::warn!(
                    "Invalid phase transition: expected {:?}, was {:?}",
                    expected,
                    ShutdownPhase::from_u8(actual)
                );
                false
            }
        }
    }

    /// Check if shutdown has completed (successfully or with failure).
    pub fn is_complete(&self) -> bool {
        matches!(
            self.phase(),
            ShutdownPhase::Complete | ShutdownPhase::Failed
        )
    }

    /// Get elapsed time since shutdown was requested.
    ///
    /// Returns `None` if shutdown hasn't been requested yet.
    pub fn elapsed(&self) -> Option<std::time::Duration> {
        self.start_time
            .read()
            .ok()
            .and_then(|guard| guard.map(|start| start.elapsed()))
    }

    /// Mark shutdown as complete.
    pub fn complete(&self) {
        self.set_phase(ShutdownPhase::Complete);
    }

    /// Mark shutdown as failed.
    pub fn fail(&self) {
        self.set_phase(ShutdownPhase::Failed);
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state() {
        let coord = ShutdownCoordinator::new();
        assert!(!coord.is_shutdown_requested());
        assert_eq!(coord.phase(), ShutdownPhase::NotStarted);
        assert!(!coord.is_complete());
        assert!(coord.elapsed().is_none());
    }

    #[test]
    fn request_shutdown() {
        let coord = ShutdownCoordinator::new();
        assert!(coord.request_shutdown());
        assert!(coord.is_shutdown_requested());
        assert_eq!(coord.phase(), ShutdownPhase::SignalSent);
        assert!(coord.elapsed().is_some());
    }

    #[test]
    fn request_shutdown_idempotent() {
        let coord = ShutdownCoordinator::new();
        assert!(coord.request_shutdown());
        assert!(!coord.request_shutdown());
    }

    #[test]
    fn phase_transitions() {
        let coord = ShutdownCoordinator::new();
        coord.request_shutdown();

        coord.set_phase(ShutdownPhase::CancellingTasks);
        assert_eq!(coord.phase(), ShutdownPhase::CancellingTasks);

        coord.set_phase(ShutdownPhase::ClosingConnections);
        assert_eq!(coord.phase(), ShutdownPhase::ClosingConnections);

        coord.complete();
        assert!(coord.is_complete());
    }

    #[test]
    fn clone_shares_state() {
        let coord1 = ShutdownCoordinator::new();
        let coord2 = coord1.clone();

        coord1.request_shutdown();
        assert!(coord2.is_shutdown_requested());

        coord2.set_phase(ShutdownPhase::Complete);
        assert!(coord1.is_complete());
    }

    #[test]
    fn advance_phase_valid_transition() {
        let coord = ShutdownCoordinator::new();
        coord.request_shutdown();
        assert_eq!(coord.phase(), ShutdownPhase::SignalSent);

        assert!(coord.advance_phase(ShutdownPhase::SignalSent, ShutdownPhase::CancellingTasks));
        assert_eq!(coord.phase(), ShutdownPhase::CancellingTasks);

        assert!(coord.advance_phase(
            ShutdownPhase::CancellingTasks,
            ShutdownPhase::ClosingConnections
        ));
        assert_eq!(coord.phase(), ShutdownPhase::ClosingConnections);
    }

    #[test]
    fn advance_phase_invalid_transition() {
        let coord = ShutdownCoordinator::new();
        coord.request_shutdown();

        // Try to skip CancellingTasks phase
        assert!(!coord.advance_phase(
            ShutdownPhase::CancellingTasks,
            ShutdownPhase::ClosingConnections
        ));

        // Phase should remain SignalSent
        assert_eq!(coord.phase(), ShutdownPhase::SignalSent);
    }
}
