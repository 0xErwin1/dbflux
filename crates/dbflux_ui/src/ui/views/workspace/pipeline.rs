use gpui::prelude::*;
use gpui::{Context, EventEmitter, Window, div, px};
use gpui_component::ActiveTheme;

use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Radii, Spacing};

use dbflux_core::{PipelineState, StateWatcher};

/// Events emitted by the pipeline progress entity.
#[derive(Debug, Clone)]
pub enum PipelineProgressEvent {
    /// Pipeline state changed.
    StateChanged(PipelineState),

    /// Pipeline completed — caller should register the connection.
    Completed,

    /// Pipeline failed — caller should display the error.
    Failed { stage: String, error: String },

    /// Pipeline was cancelled by the user.
    Cancelled,

    /// Pipeline state watcher channel closed.
    WatchClosed { last_state: PipelineState },
}

/// GPUI entity that observes a `StateWatcher` and renders pipeline progress.
///
/// Created when a pipeline-enabled profile starts connecting. The entity
/// spawns a foreground task that polls the watch channel and calls
/// `cx.notify()` on each state change.
pub struct PipelineProgress {
    state: PipelineState,
    completed_stages: Vec<String>,
    profile_name: String,
    _poll_task: Option<gpui::Task<()>>,
}

impl PipelineProgress {
    pub fn new(profile_name: String, watcher: StateWatcher, cx: &mut Context<Self>) -> Self {
        let poll_task = cx.spawn({
            let mut watcher = watcher;
            async move |this, cx| {
                loop {
                    let changed = watcher.changed().await;
                    if changed.is_err() {
                        if let Err(error) = cx.update(|cx| {
                            this.update(cx, |this, cx| {
                                this.handle_watch_closed(cx);
                            })
                            .ok();
                        }) {
                            log::warn!("Failed to handle pipeline watcher closure: {:?}", error);
                        }
                        break;
                    }

                    let new_state = watcher.borrow().clone();
                    let is_terminal = matches!(
                        new_state,
                        PipelineState::Connected
                            | PipelineState::Failed { .. }
                            | PipelineState::Cancelled
                    );

                    if let Err(error) = cx.update(|cx| {
                        this.update(cx, |this, cx| {
                            this.apply_state(new_state, cx);
                        })
                        .ok();
                    }) {
                        log::warn!("Failed to apply pipeline state change: {:?}", error);
                        break;
                    }

                    if is_terminal {
                        break;
                    }
                }
            }
        });

        Self {
            state: PipelineState::Idle,
            completed_stages: Vec::new(),
            profile_name,
            _poll_task: Some(poll_task),
        }
    }

    #[allow(dead_code)]
    pub fn state(&self) -> &PipelineState {
        &self.state
    }

    pub fn profile_name(&self) -> &str {
        &self.profile_name
    }

    fn apply_state(&mut self, new_state: PipelineState, cx: &mut Context<Self>) {
        // Track completed stages for the progress display
        if let Some(label) = completed_stage_label(&self.state)
            && !self.completed_stages.contains(&label)
        {
            self.completed_stages.push(label);
        }

        cx.emit(PipelineProgressEvent::StateChanged(new_state.clone()));

        // Emit events for terminal states
        match &new_state {
            PipelineState::Connected => {
                cx.emit(PipelineProgressEvent::Completed);
            }
            PipelineState::Failed { stage, error } => {
                cx.emit(PipelineProgressEvent::Failed {
                    stage: stage.clone(),
                    error: error.clone(),
                });
            }
            PipelineState::Cancelled => {
                cx.emit(PipelineProgressEvent::Cancelled);
            }
            _ => {}
        }

        self.state = new_state;
        cx.notify();
    }

    fn handle_watch_closed(&mut self, cx: &mut Context<Self>) {
        cx.emit(PipelineProgressEvent::WatchClosed {
            last_state: self.state.clone(),
        });
    }
}

impl EventEmitter<PipelineProgressEvent> for PipelineProgress {}

impl Render for PipelineProgress {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let current_label = active_stage_label(&self.state);
        let is_waiting_sso = matches!(self.state, PipelineState::WaitingForLogin { .. });

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .p(Spacing::SM)
            .bg(theme.secondary)
            .rounded(Radii::LG)
            .border_1()
            .border_color(theme.border)
            .child(
                // Header row
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .text_size(FontSizes::SM)
                    .font_weight(gpui::FontWeight::MEDIUM)
                    .text_color(theme.foreground)
                    .child(
                        gpui::svg()
                            .path(AppIcon::Loader.path())
                            .size(px(14.0))
                            .text_color(theme.primary),
                    )
                    .child(format!("Connecting: {}", self.profile_name)),
            )
            // Completed stages (checkmarks)
            .children(self.completed_stages.iter().map(|stage| {
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .child(
                        gpui::svg()
                            .path(AppIcon::CircleCheck.path())
                            .size(px(12.0))
                            .text_color(theme.success),
                    )
                    .child(stage.clone())
            }))
            // Current in-progress stage
            .when(current_label.is_some(), |el| {
                let label = current_label.unwrap_or_default();
                el.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .text_size(FontSizes::XS)
                        .text_color(theme.foreground)
                        .child(
                            gpui::svg()
                                .path(AppIcon::Loader.path())
                                .size(px(12.0))
                                .text_color(theme.info),
                        )
                        .child(label),
                )
            })
            // SSO waiting message
            .when(is_waiting_sso, |el| {
                el.child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.muted_foreground)
                        .italic()
                        .child("Waiting for SSO login in browser..."),
                )
            })
    }
}

/// Label for a completed stage (shown with a checkmark).
fn completed_stage_label(state: &PipelineState) -> Option<String> {
    match state {
        PipelineState::Authenticating { provider_name } => {
            Some(format!("Authenticated ({})", provider_name))
        }
        PipelineState::WaitingForLogin { provider_name, .. } => {
            Some(format!("SSO login completed ({})", provider_name))
        }
        PipelineState::ResolvingValues { total, .. } => {
            Some(format!("Resolved {} value(s)", total))
        }
        PipelineState::OpeningAccess { method_label } => {
            Some(format!("{} established", method_label))
        }
        PipelineState::Connecting { driver_name } => Some(format!("Connected to {}", driver_name)),
        PipelineState::FetchingSchema => Some("Schema fetched".to_string()),
        _ => None,
    }
}

/// Label for the currently active stage (shown with a spinner).
fn active_stage_label(state: &PipelineState) -> Option<String> {
    match state {
        PipelineState::Idle => None,
        PipelineState::Authenticating { provider_name } => {
            Some(format!("Authenticating ({})...", provider_name))
        }
        PipelineState::WaitingForLogin { provider_name, .. } => {
            Some(format!("Waiting for {} login...", provider_name))
        }
        PipelineState::ResolvingValues { total, resolved } => {
            Some(format!("Resolving values ({}/{})...", resolved, total))
        }
        PipelineState::OpeningAccess { method_label } => {
            Some(format!("Opening {}...", method_label))
        }
        PipelineState::Connecting { driver_name } => {
            Some(format!("Connecting to {}...", driver_name))
        }
        PipelineState::FetchingSchema => Some("Fetching schema...".to_string()),
        PipelineState::Connected | PipelineState::Failed { .. } | PipelineState::Cancelled => None,
    }
}
