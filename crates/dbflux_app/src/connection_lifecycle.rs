/// The app-owned lifecycle contract shared by normal and ephemeral connections.
///
/// Concrete adapters bridge this policy to GPUI tasks, driver connections, and
/// hook execution without allowing those layers to choose phase order or cleanup.
pub trait LifecycleAdapter {
    type Error;

    fn run_phase(&mut self, phase: LifecyclePhase) -> Result<LifecyclePhaseOutcome, Self::Error>;

    fn establish(&mut self) -> Result<(), Self::Error>;

    fn release_connection(&mut self) -> Result<(), Self::Error>;

    fn cancel_detached_hooks(&mut self);

    fn join_detached_hooks(&mut self) -> Result<(), Self::Error>;

    fn describe_error(&self, error: &Self::Error) -> String;

    /// Receives exactly one terminal result after resource cleanup completes.
    fn publish_terminal(&mut self, terminal: &LifecycleTerminal);
}

/// The phase sequence is deliberately app-owned so normal and ephemeral
/// connection flows cannot drift in their hook policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecyclePhase {
    PreConnect,
    PostConnect,
    PreDisconnect,
    PostDisconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecyclePhaseOutcome {
    Continue { warnings: Vec<String> },
    Aborted(String),
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleTerminal {
    Completed {
        warnings: Vec<String>,
    },
    Aborted {
        error: String,
        warnings: Vec<String>,
    },
    Cancelled {
        warnings: Vec<String>,
    },
    Failed {
        error: String,
        warnings: Vec<String>,
    },
}

pub struct ConnectionLifecycle;

/// App-state-owned registry for concrete GPUI task handles.
///
/// The owner retains each handle independently of any observer entity, so a
/// window or sidebar closing cannot cancel lifecycle cleanup prematurely.
#[derive(Default)]
pub struct LifecycleTaskOwner<T> {
    tasks: std::collections::HashMap<dbflux_core::TaskId, T>,
}

impl<T> LifecycleTaskOwner<T> {
    /// Retains a lifecycle task until its awaited cleanup has completed.
    ///
    /// The original task is preserved when a caller accidentally attempts to
    /// register the same ID twice. Replacing it could drop the only handle that
    /// keeps cleanup alive.
    pub fn retain(&mut self, task_id: dbflux_core::TaskId, task: T) -> Result<(), T> {
        if self.tasks.contains_key(&task_id) {
            return Err(task);
        }

        self.tasks.insert(task_id, task);
        Ok(())
    }

    pub fn release(&mut self, task_id: dbflux_core::TaskId) -> Option<T> {
        self.tasks.remove(&task_id)
    }

    pub fn contains(&self, task_id: dbflux_core::TaskId) -> bool {
        self.tasks.contains_key(&task_id)
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl ConnectionLifecycle {
    pub fn run<A: LifecycleAdapter>(adapter: &mut A) -> LifecycleTerminal {
        let mut warnings = Vec::new();
        let mut established = false;

        let mut terminal = match Self::run_phase(adapter, LifecyclePhase::PreConnect, &mut warnings)
        {
            Ok(Some(terminal)) => terminal,
            Ok(None) => match adapter.establish() {
                Ok(()) => {
                    established = true;
                    match Self::run_phase(adapter, LifecyclePhase::PostConnect, &mut warnings) {
                        Ok(Some(terminal)) => terminal,
                        Ok(None) => LifecycleTerminal::Completed {
                            warnings: warnings.clone(),
                        },
                        Err(error) => LifecycleTerminal::Failed {
                            error: adapter.describe_error(&error),
                            warnings: warnings.clone(),
                        },
                    }
                }
                Err(error) => LifecycleTerminal::Failed {
                    error: adapter.describe_error(&error),
                    warnings: warnings.clone(),
                },
            },
            Err(error) => LifecycleTerminal::Failed {
                error: adapter.describe_error(&error),
                warnings: warnings.clone(),
            },
        };

        if established {
            terminal = Self::disconnect(adapter, terminal, &mut warnings);
        }

        if let Err(error) = Self::cleanup(adapter) {
            terminal = terminal.with_cleanup_error(adapter.describe_error(&error));
        }

        adapter.publish_terminal(&terminal);
        terminal
    }

    fn run_phase<A: LifecycleAdapter>(
        adapter: &mut A,
        phase: LifecyclePhase,
        warnings: &mut Vec<String>,
    ) -> Result<Option<LifecycleTerminal>, A::Error> {
        match adapter.run_phase(phase)? {
            LifecyclePhaseOutcome::Continue {
                warnings: phase_warnings,
            } => {
                warnings.extend(phase_warnings);
                Ok(None)
            }
            LifecyclePhaseOutcome::Aborted(error) => Ok(Some(LifecycleTerminal::Aborted {
                error,
                warnings: warnings.clone(),
            })),
            LifecyclePhaseOutcome::Cancelled => Ok(Some(LifecycleTerminal::Cancelled {
                warnings: warnings.clone(),
            })),
        }
    }

    fn disconnect<A: LifecycleAdapter>(
        adapter: &mut A,
        terminal: LifecycleTerminal,
        warnings: &mut Vec<String>,
    ) -> LifecycleTerminal {
        let mut disconnect_warnings = Vec::new();

        if let Err(error) = Self::run_phase(adapter, LifecyclePhase::PreDisconnect, warnings) {
            disconnect_warnings.push(adapter.describe_error(&error));
        }

        if let Err(error) = adapter.release_connection() {
            disconnect_warnings.push(adapter.describe_error(&error));
        }

        if let Err(error) = Self::run_phase(adapter, LifecyclePhase::PostDisconnect, warnings) {
            disconnect_warnings.push(adapter.describe_error(&error));
        }

        for warning in disconnect_warnings {
            warnings.push(format!("Disconnect cleanup failed: {warning}"));
        }

        terminal.with_warnings(warnings.clone())
    }

    fn cleanup<A: LifecycleAdapter>(adapter: &mut A) -> Result<(), A::Error> {
        adapter.cancel_detached_hooks();
        adapter.join_detached_hooks()
    }
}

impl LifecycleTerminal {
    fn with_warnings(self, warnings: Vec<String>) -> Self {
        match self {
            Self::Completed { .. } => Self::Completed { warnings },
            Self::Aborted { error, .. } => Self::Aborted { error, warnings },
            Self::Cancelled { .. } => Self::Cancelled { warnings },
            Self::Failed { error, .. } => Self::Failed { error, warnings },
        }
    }

    fn with_cleanup_error(self, cleanup_error: String) -> Self {
        let cleanup_warning = format!("Lifecycle cleanup failed: {cleanup_error}");

        match self {
            Self::Completed { mut warnings } => {
                warnings.push(cleanup_warning);
                Self::Failed {
                    error: cleanup_error,
                    warnings,
                }
            }
            Self::Aborted {
                error,
                mut warnings,
            } => {
                warnings.push(cleanup_warning);
                Self::Aborted { error, warnings }
            }
            Self::Cancelled { mut warnings } => {
                warnings.push(cleanup_warning);
                Self::Cancelled { warnings }
            }
            Self::Failed {
                error,
                mut warnings,
            } => {
                warnings.push(cleanup_warning);
                Self::Failed { error, warnings }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionLifecycle, LifecycleAdapter, LifecyclePhase, LifecyclePhaseOutcome,
        LifecycleTaskOwner, LifecycleTerminal,
    };
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingAdapter {
        events: Vec<&'static str>,
        outcomes: Vec<LifecyclePhaseOutcome>,
        terminal_updates: Vec<LifecycleTerminal>,
        establish_error: Option<&'static str>,
        release_error: Option<&'static str>,
        join_error: Option<&'static str>,
    }

    impl LifecycleAdapter for RecordingAdapter {
        type Error = &'static str;

        fn run_phase(
            &mut self,
            phase: LifecyclePhase,
        ) -> Result<LifecyclePhaseOutcome, Self::Error> {
            self.events.push(match phase {
                LifecyclePhase::PreConnect => "pre-connect",
                LifecyclePhase::PostConnect => "post-connect",
                LifecyclePhase::PreDisconnect => "pre-disconnect",
                LifecyclePhase::PostDisconnect => "post-disconnect",
            });
            Ok(self.outcomes.remove(0))
        }

        fn establish(&mut self) -> Result<(), Self::Error> {
            self.events.push("connect");
            self.establish_error.map_or(Ok(()), Err)
        }

        fn release_connection(&mut self) -> Result<(), Self::Error> {
            self.events.push("release");
            self.release_error.map_or(Ok(()), Err)
        }

        fn cancel_detached_hooks(&mut self) {
            self.events.push("cancel-detached");
        }

        fn join_detached_hooks(&mut self) -> Result<(), Self::Error> {
            self.events.push("join-detached");
            self.join_error.map_or(Ok(()), Err)
        }

        fn describe_error(&self, error: &Self::Error) -> String {
            error.to_string()
        }

        fn publish_terminal(&mut self, terminal: &LifecycleTerminal) {
            self.terminal_updates.push(terminal.clone());
        }
    }

    #[test]
    fn task_owner_rejects_duplicate_ids_without_replacing_the_original_task() {
        let task_id = Uuid::new_v4();
        let mut owner = LifecycleTaskOwner::default();

        assert_eq!(owner.retain(task_id, "original"), Ok(()));
        assert_eq!(owner.retain(task_id, "duplicate"), Err("duplicate"));
        assert_eq!(owner.release(task_id), Some("original"));
        assert!(owner.is_empty());
    }

    #[test]
    fn successful_lifecycle_runs_ordered_phases_and_releases_everything() {
        let mut adapter = RecordingAdapter {
            outcomes: vec![
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
                LifecyclePhaseOutcome::Continue {
                    warnings: vec!["post-connect warning".to_string()],
                },
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
            ],
            ..Default::default()
        };

        let terminal = ConnectionLifecycle::run(&mut adapter);

        assert_eq!(
            terminal,
            LifecycleTerminal::Completed {
                warnings: vec!["post-connect warning".to_string()],
            }
        );
        assert_eq!(adapter.terminal_updates, vec![terminal]);
        assert_eq!(
            adapter.events,
            [
                "pre-connect",
                "connect",
                "post-connect",
                "pre-disconnect",
                "release",
                "post-disconnect",
                "cancel-detached",
                "join-detached",
            ]
        );
    }

    #[test]
    fn abort_before_establishment_skips_disconnect_and_cleans_up() {
        let mut adapter = RecordingAdapter {
            outcomes: vec![LifecyclePhaseOutcome::Aborted(
                "pre-connect failed".to_string(),
            )],
            ..Default::default()
        };

        let terminal = ConnectionLifecycle::run(&mut adapter);

        assert_eq!(
            terminal,
            LifecycleTerminal::Aborted {
                error: "pre-connect failed".to_string(),
                warnings: vec![],
            }
        );
        assert_eq!(adapter.terminal_updates, vec![terminal]);
        assert_eq!(
            adapter.events,
            ["pre-connect", "cancel-detached", "join-detached"]
        );
    }

    #[test]
    fn failed_establishment_skips_disconnect_and_publishes_one_terminal_update() {
        let mut adapter = RecordingAdapter {
            outcomes: vec![LifecyclePhaseOutcome::Continue { warnings: vec![] }],
            establish_error: Some("connection failed"),
            ..Default::default()
        };

        let terminal = ConnectionLifecycle::run(&mut adapter);

        assert_eq!(
            terminal,
            LifecycleTerminal::Failed {
                error: "connection failed".to_string(),
                warnings: vec![],
            }
        );
        assert_eq!(adapter.terminal_updates, vec![terminal]);
        assert_eq!(
            adapter.events,
            ["pre-connect", "connect", "cancel-detached", "join-detached"]
        );
    }

    #[test]
    fn cancellation_after_establishment_runs_disconnect_and_returns_one_terminal() {
        let mut adapter = RecordingAdapter {
            outcomes: vec![
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
                LifecyclePhaseOutcome::Cancelled,
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
            ],
            ..Default::default()
        };

        let terminal = ConnectionLifecycle::run(&mut adapter);

        assert_eq!(terminal, LifecycleTerminal::Cancelled { warnings: vec![] });
        assert_eq!(adapter.terminal_updates, vec![terminal]);
        assert_eq!(
            adapter.events,
            [
                "pre-connect",
                "connect",
                "post-connect",
                "pre-disconnect",
                "release",
                "post-disconnect",
                "cancel-detached",
                "join-detached",
            ]
        );
    }

    #[test]
    fn disconnect_warning_and_ignored_failure_complete_cleanup() {
        let mut adapter = RecordingAdapter {
            outcomes: vec![
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
                LifecyclePhaseOutcome::Continue {
                    warnings: vec!["disconnect warning".to_string()],
                },
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
            ],
            ..Default::default()
        };

        let terminal = ConnectionLifecycle::run(&mut adapter);

        assert_eq!(
            terminal,
            LifecycleTerminal::Completed {
                warnings: vec!["disconnect warning".to_string()],
            }
        );
        assert!(adapter.events.contains(&"release"));
        assert_eq!(adapter.terminal_updates, vec![terminal]);
    }

    #[test]
    fn disconnect_failures_do_not_replace_the_primary_error_or_skip_release() {
        let mut adapter = RecordingAdapter {
            outcomes: vec![
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
                LifecyclePhaseOutcome::Aborted("post-connect failed".to_string()),
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
                LifecyclePhaseOutcome::Continue { warnings: vec![] },
            ],
            release_error: Some("release failed"),
            ..Default::default()
        };

        let terminal = ConnectionLifecycle::run(&mut adapter);

        assert_eq!(
            terminal,
            LifecycleTerminal::Aborted {
                error: "post-connect failed".to_string(),
                warnings: vec!["Disconnect cleanup failed: release failed".to_string()],
            }
        );
        assert!(adapter.events.contains(&"release"));
        assert_eq!(adapter.terminal_updates, vec![terminal]);
    }

    #[test]
    fn detached_join_failure_preserves_primary_error_and_publishes_once() {
        let mut adapter = RecordingAdapter {
            outcomes: vec![LifecyclePhaseOutcome::Aborted(
                "pre-connect failed".to_string(),
            )],
            join_error: Some("detached join failed"),
            ..Default::default()
        };

        let terminal = ConnectionLifecycle::run(&mut adapter);

        assert_eq!(
            terminal,
            LifecycleTerminal::Aborted {
                error: "pre-connect failed".to_string(),
                warnings: vec!["Lifecycle cleanup failed: detached join failed".to_string()],
            }
        );
        assert_eq!(adapter.terminal_updates, vec![terminal]);
        assert_eq!(
            adapter.events,
            ["pre-connect", "cancel-detached", "join-detached"]
        );
    }
}
