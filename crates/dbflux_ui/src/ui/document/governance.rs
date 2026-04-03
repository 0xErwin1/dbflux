use crate::app::{AppStateChanged, AppStateEntity, McpRuntimeEventRaised};
use crate::ui::icons::AppIcon;
use dbflux_export::export_text_payload;
use dbflux_mcp::{
    AuditEntry, AuditExportFormat, AuditQuery, PendingExecutionDetail, PendingExecutionSummary,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Input, InputEvent, InputState};
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

        self.selected_detail = self
            .app_state
            .read(cx)
            .get_mcp_pending_execution(pending_id)
            .ok();
    }

    fn semantics_preview(detail: &PendingExecutionDetail) -> String {
        let plan = &detail.plan;
        let session = plan
            .get("session")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("n/a");
        let scope = plan
            .get("scope")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("n/a");

        format!("session: {} | scope: {}", session, scope)
    }

    fn approve_selected(&mut self, cx: &mut Context<Self>) {
        let Some(pending_id) = self.selected_id.clone() else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            let _ = state.approve_mcp_pending_execution(&pending_id);

            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }

            cx.emit(AppStateChanged);
        });

        self.refresh(cx);
    }

    fn reject_selected(&mut self, cx: &mut Context<Self>) {
        let Some(pending_id) = self.selected_id.clone() else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            let _ = state.reject_mcp_pending_execution(&pending_id);

            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }

            cx.emit(AppStateChanged);
        });

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
                        Button::new("mcp-approvals-refresh")
                            .label("Refresh Pending")
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
                                root.child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child("No pending executions"),
                                )
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
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::MEDIUM)
                                            .child(entry.tool_id.clone()),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child(format!("actor: {}", entry.actor_id)),
                                    )
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
                        root.child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::SEMIBOLD)
                                .child(format!("Pending {}", detail.summary.id)),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child("Decision semantics")
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.foreground)
                                        .child(Self::semantics_preview(&detail)),
                                ),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child("Execution plan")
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.foreground)
                                        .child(detail.plan.to_string()),
                                ),
                        )
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .child(
                                    Button::new("mcp-approval-approve")
                                        .label("Approve")
                                        .small()
                                        .primary()
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.approve_selected(cx);
                                        })),
                                )
                                .child(
                                    Button::new("mcp-approval-reject")
                                        .label("Reject")
                                        .small()
                                        .danger()
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.reject_selected(cx);
                                        })),
                                ),
                        )
                    })
                    .when(self.selected_detail.is_none(), |root| {
                        root.child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("Select a pending request to review details."),
                        )
                    })
                    .when_some(self.status_message.clone(), |root, message| {
                        root.child(div().text_sm().text_color(theme.danger).child(message))
                    }),
            )
    }
}

pub struct McpAuditView {
    app_state: Entity<AppStateEntity>,
    input_actor: Entity<InputState>,
    input_tool: Entity<InputState>,
    input_start_epoch: Entity<InputState>,
    input_end_epoch: Entity<InputState>,
    entries: Vec<AuditEntry>,
    status_message: Option<String>,
    _subscriptions: Vec<Subscription>,
}

impl McpAuditView {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_actor = cx.new(|cx| InputState::new(window, cx).placeholder("actor filter"));
        let input_tool = cx.new(|cx| InputState::new(window, cx).placeholder("tool filter"));
        let input_start_epoch =
            cx.new(|cx| InputState::new(window, cx).placeholder("start epoch ms"));
        let input_end_epoch = cx.new(|cx| InputState::new(window, cx).placeholder("end epoch ms"));

        let actor_sub = cx.subscribe(&input_actor, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::PressEnter { secondary: false }) {
                this.refresh(cx);
            }
        });

        let tool_sub = cx.subscribe(&input_tool, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::PressEnter { secondary: false }) {
                this.refresh(cx);
            }
        });

        Self {
            app_state,
            input_actor,
            input_tool,
            input_start_epoch,
            input_end_epoch,
            entries: Vec::new(),
            status_message: None,
            _subscriptions: vec![actor_sub, tool_sub],
        }
    }

    fn current_query(&self, cx: &App) -> AuditQuery {
        let actor = self.input_actor.read(cx).value().trim().to_string();
        let tool = self.input_tool.read(cx).value().trim().to_string();
        let start_epoch_ms = self
            .input_start_epoch
            .read(cx)
            .value()
            .trim()
            .parse::<i64>()
            .ok();
        let end_epoch_ms = self
            .input_end_epoch
            .read(cx)
            .value()
            .trim()
            .parse::<i64>()
            .ok();

        AuditQuery {
            actor_id: (!actor.is_empty()).then_some(actor),
            tool_id: (!tool.is_empty()).then_some(tool),
            decision: None,
            start_epoch_ms,
            end_epoch_ms,
            limit: Some(200),
        }
    }

    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        let query = self.current_query(cx);
        match self.app_state.read(cx).query_mcp_audit_entries(&query) {
            Ok(entries) => {
                self.entries = entries;
                self.status_message = None;
            }
            Err(error) => {
                self.entries.clear();
                self.status_message = Some(error);
            }
        }
    }

    fn export(&mut self, format: AuditExportFormat, cx: &mut Context<Self>) {
        let query = self.current_query(cx);

        match self
            .app_state
            .read(cx)
            .export_mcp_audit_entries(&query, format)
        {
            Ok(payload) => {
                let extension = match format {
                    AuditExportFormat::Csv => "csv",
                    AuditExportFormat::Json => "json",
                };

                let path =
                    std::env::temp_dir().join(format!("dbflux-mcp-audit-export.{extension}"));
                match std::fs::File::create(&path) {
                    Ok(mut file) => {
                        if let Err(error) = export_text_payload(&payload, &mut file) {
                            self.status_message = Some(format!("Export failed: {}", error));
                            return;
                        }

                        self.status_message = Some(format!("Exported to {}", path.display()));
                    }
                    Err(error) => {
                        self.status_message = Some(format!("Export failed: {}", error));
                    }
                }
            }
            Err(error) => {
                self.status_message = Some(error);
            }
        }
    }
}

impl Render for McpAuditView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .size_full()
            .flex()
            .flex_col()
            .child(compact_top_bar(
                theme,
                vec![
                    compact_labeled_control(
                        "Actor:",
                        div()
                            .w(px(220.0))
                            .child(Input::new(&self.input_actor).small()),
                        theme,
                    )
                    .into_any_element(),
                    compact_labeled_control(
                        "Tool:",
                        div()
                            .w(px(220.0))
                            .child(Input::new(&self.input_tool).small()),
                        theme,
                    )
                    .into_any_element(),
                    compact_labeled_control(
                        "Start:",
                        div()
                            .w(px(160.0))
                            .child(Input::new(&self.input_start_epoch).small()),
                        theme,
                    )
                    .into_any_element(),
                    compact_labeled_control(
                        "End:",
                        div()
                            .w(px(160.0))
                            .child(Input::new(&self.input_end_epoch).small()),
                        theme,
                    )
                    .into_any_element(),
                    Button::new("mcp-audit-filter-apply")
                        .label("Apply Filters")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.refresh(cx);
                        }))
                        .into_any_element(),
                    div().flex_1().into_any_element(),
                    Button::new("mcp-audit-export-csv")
                        .label("Export CSV")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.export(AuditExportFormat::Csv, cx);
                        }))
                        .into_any_element(),
                    Button::new("mcp-audit-export-json")
                        .label("Export JSON")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.export(AuditExportFormat::Json, cx);
                        }))
                        .into_any_element(),
                ],
            ))
            .child(
                div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .p_4()
                    .when(self.entries.is_empty(), |root| {
                        root.child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No audit entries match current filters."),
                        )
                    })
                    .children(self.entries.iter().map(|entry| {
                        div()
                            .id(SharedString::from(format!("audit-entry-{}", entry.id)))
                            .p_2()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(theme.border)
                            .child(
                                div()
                                    .flex()
                                    .justify_between()
                                    .items_center()
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::MEDIUM)
                                            .child(entry.tool_id.clone()),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child(entry.decision.clone()),
                                    ),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child(format!("actor: {}", entry.actor_id)),
                            )
                    })),
            )
            .child(workspace_footer_bar(
                theme,
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(
                        svg()
                            .path(AppIcon::Rows3.path())
                            .size_3()
                            .text_color(theme.muted_foreground),
                    )
                    .child(format!("{} entries", self.entries.len())),
                div(),
                div().when_some(self.status_message.clone(), |right, message| {
                    right.child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(message),
                    )
                }),
            ))
    }
}

#[cfg(test)]
mod tests {
    use super::McpApprovalsView;
    use dbflux_mcp::{PendingExecutionDetail, PendingExecutionSummary};

    #[test]
    fn semantics_preview_reads_session_and_scope_fields() {
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
        assert_eq!(preview, "session: one-shot | scope: connection");
    }

    #[test]
    fn semantics_preview_defaults_missing_fields() {
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
        assert_eq!(preview, "session: n/a | scope: n/a");
    }
}
