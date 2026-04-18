use crate::app::{AppStateChanged, AppStateEntity, McpRuntimeEventRaised};
use dbflux_components::controls::Button;
use dbflux_components::primitives::Text;
use dbflux_mcp::{PendingExecutionDetail, PendingExecutionSummary};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

use super::chrome::{compact_labeled_control, compact_top_bar, workspace_footer_bar};

pub struct McpApprovalsView {
    app_state: Entity<AppStateEntity>,
    pending: Vec<PendingExecutionSummary>,
    selected_id: Option<String>,
    selected_detail: Option<PendingExecutionDetail>,
    status_message: Option<String>,
}

impl McpApprovalsView {
    pub fn new(app_state: Entity<AppStateEntity>) -> Self {
        Self {
            app_state,
            pending: Vec::new(),
            selected_id: None,
            selected_detail: None,
            status_message: None,
        }
    }

    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        match self.app_state.read(cx).list_mcp_pending_executions() {
            Ok(mut pending) => {
                pending.sort_by(|left, right| left.id.cmp(&right.id));
                self.pending = pending;
                self.status_message = None;

                if let Some(selected_id) = self.selected_id.clone() {
                    self.load_detail(&selected_id, cx);
                }
            }
            Err(error) => {
                self.pending.clear();
                self.selected_detail = None;
                self.status_message = Some(error);
            }
        }
    }

    fn load_detail(&mut self, pending_id: &str, cx: &mut Context<Self>) {
        self.selected_id = Some(pending_id.to_string());

        match self
            .app_state
            .read(cx)
            .get_mcp_pending_execution(pending_id)
        {
            Ok(detail) => {
                self.selected_detail = Some(detail);
                self.status_message = None;
            }
            Err(error) => {
                self.selected_detail = None;
                self.status_message = Some(format!(
                    "Failed to load pending execution {}: {}",
                    pending_id, error
                ));
            }
        }
    }

    fn semantics_preview(detail: &PendingExecutionDetail) -> String {
        format!(
            "requester: {} | connection: {} | classification: {}",
            detail.summary.actor_id,
            detail.summary.connection_id,
            Self::classification_label(detail.summary.classification)
        )
    }

    fn classification_label(
        classification: dbflux_policy::ExecutionClassification,
    ) -> &'static str {
        match classification {
            dbflux_policy::ExecutionClassification::Metadata => "metadata",
            dbflux_policy::ExecutionClassification::Read => "read",
            dbflux_policy::ExecutionClassification::Write => "write",
            dbflux_policy::ExecutionClassification::Destructive => "destructive",
            dbflux_policy::ExecutionClassification::Admin => "admin",
            dbflux_policy::ExecutionClassification::AdminSafe => "admin_safe",
            dbflux_policy::ExecutionClassification::AdminDestructive => "admin_destructive",
        }
    }

    fn approve_selected(&mut self, cx: &mut Context<Self>) {
        let Some(pending_id) = self.selected_id.clone() else {
            return;
        };

        let mut result: Result<(), String> = Ok(());

        self.app_state.update(cx, |state, cx| {
            result = state.approve_mcp_pending_execution(&pending_id).map(|_| ());

            if result.is_ok() {
                for event in state.drain_mcp_runtime_events() {
                    cx.emit(McpRuntimeEventRaised { event });
                }

                cx.emit(AppStateChanged);
            }
        });

        if let Err(error) = result {
            self.status_message = Some(error);
            cx.notify();
            return;
        }

        self.refresh(cx);
    }

    fn reject_selected(&mut self, cx: &mut Context<Self>) {
        let Some(pending_id) = self.selected_id.clone() else {
            return;
        };

        let mut result: Result<(), String> = Ok(());

        self.app_state.update(cx, |state, cx| {
            result = state.reject_mcp_pending_execution(&pending_id).map(|_| ());

            if result.is_ok() {
                for event in state.drain_mcp_runtime_events() {
                    cx.emit(McpRuntimeEventRaised { event });
                }

                cx.emit(AppStateChanged);
            }
        });

        if let Err(error) = result {
            self.status_message = Some(error);
            cx.notify();
            return;
        }

        self.refresh(cx);
    }
}

impl Render for McpApprovalsView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .size_full()
            .flex()
            .overflow_hidden()
            .child(
                div()
                    .w(px(340.0))
                    .h_full()
                    .border_r_1()
                    .border_color(theme.border)
                    .p_3()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        Button::new("mcp-approvals-refresh", "Refresh Pending")
                            .small()
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.refresh(cx);
                            })),
                    )
                    .child(
                        div()
                            .flex_1()
                            .overflow_y_scrollbar()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .when(self.pending.is_empty(), |root| {
                                root.child(Text::muted("No pending executions"))
                            })
                            .children(self.pending.iter().map(|entry| {
                                let entry_id = entry.id.clone();
                                let is_selected =
                                    self.selected_id.as_deref() == Some(entry.id.as_str());

                                div()
                                    .id(SharedString::from(format!("pending-{}", entry.id)))
                                    .p_2()
                                    .rounded(px(4.0))
                                    .border_1()
                                    .border_color(if is_selected {
                                        theme.primary
                                    } else {
                                        transparent_black()
                                    })
                                    .bg(if is_selected {
                                        theme.secondary
                                    } else {
                                        transparent_black()
                                    })
                                    .cursor_pointer()
                                    .hover({
                                        let secondary = theme.secondary;
                                        move |div| div.bg(secondary)
                                    })
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.load_detail(&entry_id, cx);
                                        cx.notify();
                                    }))
                                    .child(
                                        Text::body(entry.tool_id.clone())
                                            .font_weight(FontWeight::MEDIUM),
                                    )
                                    .child(Text::caption(format!("actor: {}", entry.actor_id)))
                            })),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .h_full()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .when_some(self.selected_detail.clone(), |root, detail| {
                        root.child(Text::heading(format!("Pending {}", detail.summary.id)))
                            .child(
                                div()
                                    .child(Text::caption("Approval context"))
                                    .child(Text::body(Self::semantics_preview(&detail))),
                            )
                            .child(
                                div()
                                    .child(Text::caption("Execution plan"))
                                    .child(Text::body(detail.plan.to_string())),
                            )
                            .child(
                                div()
                                    .flex()
                                    .gap_2()
                                    .child(
                                        Button::new("mcp-approval-approve", "Approve")
                                            .small()
                                            .primary()
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.approve_selected(cx);
                                            })),
                                    )
                                    .child(
                                        Button::new("mcp-approval-reject", "Reject")
                                            .small()
                                            .danger()
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.reject_selected(cx);
                                            })),
                                    ),
                            )
                    })
                    .when(self.selected_detail.is_none(), |root| {
                        root.child(Text::muted("Select a pending request to review details."))
                    })
                    .when_some(self.status_message.clone(), |root, message| {
                        root.child(Text::body(message).text_color(theme.danger))
                    }),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::McpApprovalsView;
    use dbflux_mcp::{PendingExecutionDetail, PendingExecutionSummary};

    #[test]
    fn semantics_preview_shows_actual_approval_context() {
        let detail = PendingExecutionDetail {
            summary: PendingExecutionSummary {
                id: "pending-1".to_string(),
                actor_id: "agent-a".to_string(),
                connection_id: "conn-a".to_string(),
                tool_id: "request_execution".to_string(),
                classification: dbflux_policy::ExecutionClassification::Write,
                status: "pending".to_string(),
                created_at_epoch_ms: 0,
            },
            plan: serde_json::json!({"session": "one-shot", "scope": "connection"}),
        };

        let preview = McpApprovalsView::semantics_preview(&detail);
        assert_eq!(
            preview,
            "requester: agent-a | connection: conn-a | classification: write"
        );
    }

    #[test]
    fn semantics_preview_does_not_depend_on_optional_payload_fields() {
        let detail = PendingExecutionDetail {
            summary: PendingExecutionSummary {
                id: "pending-2".to_string(),
                actor_id: "agent-a".to_string(),
                connection_id: "conn-a".to_string(),
                tool_id: "request_execution".to_string(),
                classification: dbflux_policy::ExecutionClassification::Write,
                status: "pending".to_string(),
                created_at_epoch_ms: 0,
            },
            plan: serde_json::json!({}),
        };

        let preview = McpApprovalsView::semantics_preview(&detail);
        assert_eq!(
            preview,
            "requester: agent-a | connection: conn-a | classification: write"
        );
    }
}
