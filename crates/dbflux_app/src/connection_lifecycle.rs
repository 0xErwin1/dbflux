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
    pub fn retain(&mut self, task_id: dbflux_core::TaskId, task: T) {
        self.tasks.insert(task_id, task);
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
            terminal = LifecycleTerminal::Failed {
                error: adapter.describe_error(&error),
                warnings,
            };
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
        let pre_disconnect = Self::run_phase(adapter, LifecyclePhase::PreDisconnect, warnings)
            .unwrap_or_else(|error| {
                Some(LifecycleTerminal::Failed {
                    error: adapter.describe_error(&error),
                    warnings: warnings.clone(),
                })
            });

        if let Err(error) = adapter.release_connection() {
            return LifecycleTerminal::Failed {
                error: adapter.describe_error(&error),
                warnings: warnings.clone(),
            };
        }

        let post_disconnect = Self::run_phase(adapter, LifecyclePhase::PostDisconnect, warnings)
            .unwrap_or_else(|error| {
                Some(LifecycleTerminal::Failed {
                    error: adapter.describe_error(&error),
                    warnings: warnings.clone(),
                })
            });

        post_disconnect
            .or(pre_disconnect)
            .unwrap_or_else(|| terminal.with_warnings(warnings.clone()))
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
            Ok(())
        }

        fn cancel_detached_hooks(&mut self) {
            self.events.push("cancel-detached");
        }

        fn join_detached_hooks(&mut self) -> Result<(), Self::Error> {
            self.events.push("join-detached");
            Ok(())
        }

        fn describe_error(&self, error: &Self::Error) -> String {
            error.to_string()
        }

        fn publish_terminal(&mut self, terminal: &LifecycleTerminal) {
            self.terminal_updates.push(terminal.clone());
        }
    }

    #[test]
    fn task_owner_retains_work_until_the_app_owner_releases_it() {
        let task_id = Uuid::new_v4();
        let mut owner = LifecycleTaskOwner::default();

        owner.retain(task_id, "concrete-gpui-task");

        assert!(owner.contains(task_id));
        assert_eq!(owner.len(), 1);
        assert_eq!(owner.release(task_id), Some("concrete-gpui-task"));
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
}
