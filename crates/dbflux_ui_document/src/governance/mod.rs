mod pane;

use dbflux_components::controls::{Button, ButtonVariant, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Chamfer, Icon, Kbd, Text,
};
use dbflux_components::tokens::{
    ApprovalsMetrics, ChamferCut, ChromeColors, DocumentMetrics, SyntaxColors, TreeMetrics,
};
use dbflux_components::typography::AppFonts;
use dbflux_core::keymap_types::{Command, ContextId};
use dbflux_mcp::{PendingExecutionDetail, PendingExecutionSummary};
use dbflux_policy::ExecutionClassification;
use dbflux_ui_base::keymap::shortcut_label;
use dbflux_ui_base::{AppStateChanged, AppStateEntity, McpRuntimeEventRaised};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

use super::chrome::{detail_field, document_bar, document_subtitle, document_title};
use super::handle::DocumentEvent;
use super::syntax_runs::json_highlights;
use super::types::{DocumentId, DocumentState};

/// The MCP approvals document (P1Approvals): the pending agent calls on the
/// left, the selected call's context and payload on the right, and the
/// rejection reason with the Reject and Approve actions at the bottom.
/// Opened as a singleton tab.
pub struct McpApprovalsView {
    id: DocumentId,
    app_state: Entity<AppStateEntity>,
    pending: Vec<PendingExecutionSummary>,
    selected_id: Option<String>,
    selected_detail: Option<PendingExecutionDetail>,
    status_message: Option<String>,
    reject_reason: Entity<InputState>,
    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for McpApprovalsView {}

impl McpApprovalsView {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let runtime_events = cx.subscribe(
            &app_state,
            |this, _app_state, _event: &McpRuntimeEventRaised, cx| {
                this.refresh(cx);
            },
        );

        let reject_reason = cx.new(|cx| InputState::new(window, cx));

        let mut view = Self {
            id: DocumentId::new(),
            app_state,
            pending: Vec::new(),
            selected_id: None,
            selected_detail: None,
            status_message: None,
            reject_reason,
            focus_handle: cx.focus_handle(),
            _subscriptions: vec![runtime_events],
        };

        view.refresh(cx);
        view
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        dbflux_i18n::t!("document.governance.title")
    }

    pub fn state(&self) -> DocumentState {
        if self.status_message.is_some() {
            DocumentState::Error
        } else {
            DocumentState::Clean
        }
    }

    /// Reloads the pending list and takes keyboard focus for the list keys.
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.refresh(cx);
        self.focus_handle.focus(window, cx);
    }

    /// The approvals keys apply across the view. Their default predicate
    /// leaves the letters to the reason field while it has focus.
    pub fn active_context(&self) -> ContextId {
        ContextId::McpApprovals
    }

    /// Runs a keymap command on the pending list or the decision.
    pub fn dispatch_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::SelectNext => self.move_selection(1, cx),
            Command::SelectPrev => self.move_selection(-1, cx),
            Command::SelectFirst => self.move_selection(isize::MIN, cx),
            Command::SelectLast => self.move_selection(isize::MAX, cx),
            Command::ApproveExecution => self.approve_selected(window, cx),
            Command::RejectExecution => self.reject_selected(window, cx),
            Command::Execute => self.focus_reject_reason(window, cx),
            Command::RefreshSchema => self.refresh(cx),
            Command::Cancel => {
                if !self.reason_has_focus(window, cx) {
                    return false;
                }
                self.focus_handle.focus(window, cx);
            }
            _ => return false,
        }

        true
    }

    /// The decision buttons and the reason field, for the pane actions menu.
    pub(crate) fn pane_actions(&self, this: &Entity<Self>) -> Vec<crate::pane::PaneAction> {
        use crate::pane::PaneAction;

        let context = ContextId::McpApprovals;
        let has_selection = self.selected_id.is_some();
        let reason_target = this.downgrade();

        vec![
            PaneAction::command(
                "approval-approve",
                dbflux_i18n::t!("document.governance.approve"),
                Command::ApproveExecution,
                context,
            )
            .icon(AppIcon::Check)
            .enabled(has_selection),
            PaneAction::command(
                "approval-reject",
                dbflux_i18n::t!("document.governance.reject"),
                Command::RejectExecution,
                context,
            )
            .icon(AppIcon::CircleX)
            .enabled(has_selection),
            PaneAction::callback(
                "approval-reason",
                dbflux_i18n::t!("document.governance.pane_actions.reason"),
                move |window, cx| {
                    if let Some(view) = reason_target.upgrade() {
                        view.update(cx, |view, cx| view.focus_reject_reason(window, cx));
                    }
                },
            )
            .icon(AppIcon::Pencil),
            PaneAction::command(
                "approval-refresh",
                dbflux_i18n::t!("document.governance.refresh"),
                Command::RefreshSchema,
                context,
            )
            .icon(AppIcon::RefreshCcw),
        ]
    }

    fn focus_reject_reason(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let handle = self.reject_reason.read(cx).focus_handle(cx);
        handle.focus(window, cx);
    }

    fn reason_has_focus(&self, window: &Window, cx: &App) -> bool {
        self.reject_reason
            .read(cx)
            .focus_handle(cx)
            .is_focused(window)
    }

    /// Reloads the pending list. A call another view asked to show (a
    /// notification's "Review") is selected when it is still pending;
    /// otherwise the current selection is kept, or the first call selected.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        let requested = self
            .app_state
            .update(cx, |state, _| state.pending_approval_focus.take());

        match self.app_state.read(cx).list_mcp_pending_executions() {
            Ok(mut pending) => {
                pending.sort_by(|left, right| left.id.cmp(&right.id));
                self.pending = pending;
                self.status_message = None;

                let is_pending = |id: &String| self.pending.iter().any(|entry| &entry.id == id);

                let next_selection = requested
                    .filter(|id| is_pending(id))
                    .or_else(|| self.selected_id.clone().filter(|id| is_pending(id)))
                    .or_else(|| self.pending.first().map(|entry| entry.id.clone()));

                match next_selection {
                    Some(selected_id) => self.load_detail(&selected_id, cx),
                    None => {
                        self.selected_id = None;
                        self.selected_detail = None;
                    }
                }
            }
            Err(error) => {
                self.pending.clear();
                self.selected_detail = None;
                self.status_message = Some(error);
            }
        }

        cx.notify();
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
                self.status_message = Some(dbflux_i18n::t!(
                    "document.governance.load_failed",
                    id = pending_id,
                    error = error
                ));
            }
        }
    }

    /// Moves the selection `step` entries along the pending list, clamped
    /// to its ends.
    fn move_selection(&mut self, step: isize, cx: &mut Context<Self>) {
        if self.pending.is_empty() {
            return;
        }

        let current = self
            .selected_id
            .as_ref()
            .and_then(|id| self.pending.iter().position(|entry| &entry.id == id));

        let next = match current {
            Some(index) => index
                .saturating_add_signed(step)
                .min(self.pending.len() - 1),
            None => 0,
        };

        let next_id = self.pending[next].id.clone();
        self.load_detail(&next_id, cx);
        cx.notify();
    }

    /// Translated name of a classification, as its badge shows it.
    pub fn classification_display(classification: ExecutionClassification) -> String {
        match classification {
            ExecutionClassification::Metadata => {
                dbflux_i18n::t!("document.governance.classification.metadata")
            }
            ExecutionClassification::Read => {
                dbflux_i18n::t!("document.governance.classification.read")
            }
            ExecutionClassification::Write => {
                dbflux_i18n::t!("document.governance.classification.write")
            }
            ExecutionClassification::Destructive => {
                dbflux_i18n::t!("document.governance.classification.destructive")
            }
            ExecutionClassification::Admin => {
                dbflux_i18n::t!("document.governance.classification.admin")
            }
            ExecutionClassification::AdminSafe => {
                dbflux_i18n::t!("document.governance.classification.admin_safe")
            }
            ExecutionClassification::AdminDestructive => {
                dbflux_i18n::t!("document.governance.classification.admin_destructive")
            }
        }
    }

    /// Badge tone of a classification: reads in blue, writes in amber, and
    /// anything that can lose data or change the schema in red.
    pub fn classification_tone(classification: ExecutionClassification) -> BadgeTone {
        match classification {
            ExecutionClassification::Metadata => BadgeTone::Neutral,
            ExecutionClassification::Read => BadgeTone::Info,
            ExecutionClassification::Write | ExecutionClassification::AdminSafe => {
                BadgeTone::Warning
            }
            ExecutionClassification::Destructive
            | ExecutionClassification::Admin
            | ExecutionClassification::AdminDestructive => BadgeTone::Danger,
        }
    }

    /// How long a call has waited, from its creation time.
    fn waiting_label(created_at_epoch_ms: i64, now_ms: i64) -> String {
        let minutes = (now_ms - created_at_epoch_ms).max(0) / 60_000;

        if minutes < 1 {
            dbflux_i18n::t!("document.governance.waiting.just_now")
        } else if minutes < 60 {
            dbflux_i18n::t!("document.governance.waiting.minutes", count = minutes)
        } else {
            dbflux_i18n::t!("document.governance.waiting.hours", count = minutes / 60)
        }
    }

    /// Name of the connection a call targets: the profile name when the id
    /// matches a saved connection, the id otherwise, and an em dash for a
    /// tool without a connection.
    fn connection_name(&self, connection_id: &str, cx: &App) -> String {
        if connection_id.is_empty() {
            return "—".to_string();
        }

        self.app_state
            .read(cx)
            .profiles()
            .iter()
            .find(|profile| profile.id.to_string() == connection_id)
            .map(|profile| profile.name.clone())
            .unwrap_or_else(|| connection_id.to_string())
    }

    fn pending_tool_text(text: impl Into<SharedString>) -> Text {
        Text::code(text)
    }

    fn clear_reject_reason(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.reject_reason
            .update(cx, |input, cx| input.set_value("", window, cx));
    }

    fn approve_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
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

        self.clear_reject_reason(window, cx);
        self.refresh(cx);
    }

    /// Rejects the selected call with the typed reason, which the runtime
    /// trims, caps and sends back to the agent.
    fn reject_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(pending_id) = self.selected_id.clone() else {
            return;
        };

        let reason = self.reject_reason.read(cx).value().to_string();
        let mut result: Result<(), String> = Ok(());

        self.app_state.update(cx, |state, cx| {
            result = state
                .reject_mcp_pending_execution(&pending_id, Some(&reason))
                .map(|_| ());

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

        self.clear_reject_reason(window, cx);
        self.refresh(cx);
    }

    fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let tint = ChromeColors::tint(cx.theme());

        document_bar(DocumentMetrics::HEADER_HEIGHT, cx)
            .child(document_title(
                AppIcon::Bot,
                tint,
                dbflux_i18n::t!("document.governance.title"),
                cx,
            ))
            .child(document_subtitle(
                dbflux_i18n::t!("document.governance.subtitle"),
                cx,
            ))
            .child(div().flex_1())
            .child(
                Button::new(
                    "mcp-approvals-refresh",
                    dbflux_i18n::t!("document.governance.refresh"),
                )
                .icon(AppIcon::RefreshCcw)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.refresh(cx);
                })),
            )
    }

    fn render_pending_row(
        &self,
        entry: &PendingExecutionSummary,
        now_ms: i64,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let is_selected = self.selected_id.as_deref() == Some(entry.id.as_str());
        let entry_id = entry.id.clone();
        let hover = theme.list_hover;

        div()
            .id(SharedString::from(format!("pending-{}", entry.id)))
            .relative()
            .flex()
            .flex_col()
            .gap(ApprovalsMetrics::ROW_GAP)
            .px(ApprovalsMetrics::LIST_PADDING_X)
            .py(ApprovalsMetrics::LIST_PADDING_Y)
            .border_b_1()
            .border_color(theme.table_row_border)
            .cursor_pointer()
            .when(is_selected, |row| {
                row.bg(tint.opacity(ApprovalsMetrics::SELECTED_ALPHA))
                    .child(
                        div()
                            .absolute()
                            .left_0()
                            .top_0()
                            .bottom_0()
                            .w(TreeMetrics::SELECTION_BAR)
                            .bg(tint),
                    )
            })
            .when(!is_selected, |row| row.hover(move |row| row.bg(hover)))
            .on_click(cx.listener(move |this, _, window, cx| {
                this.load_detail(&entry_id, cx);
                this.focus_handle.focus(window, cx);
                cx.notify();
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .child(
                        Icon::new(AppIcon::Bot)
                            .size(ApprovalsMetrics::ROW_ICON)
                            .color(tint),
                    )
                    .child(
                        Self::pending_tool_text(entry.tool_id.clone())
                            .color(ChromeColors::strong(theme))
                            .font_weight(FontWeight::BOLD),
                    )
                    .child(div().flex_1())
                    .child(Badge::new(
                        Self::classification_display(entry.classification),
                        Self::classification_tone(entry.classification),
                    )),
            )
            .child(
                div()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(theme.foreground)
                    .truncate()
                    .child(self.connection_name(&entry.connection_id, cx)),
            )
            .child(
                div()
                    .text_size(ApprovalsMetrics::META_FONT)
                    .text_color(theme.muted_foreground)
                    .truncate()
                    .child(format!(
                        "{} · {}",
                        entry.actor_id,
                        Self::waiting_label(entry.created_at_epoch_ms, now_ms)
                    )),
            )
    }

    fn render_pending_list(&self, now_ms: i64, cx: &mut Context<Self>) -> impl IntoElement {
        let rows = self
            .pending
            .iter()
            .map(|entry| {
                self.render_pending_row(entry, now_ms, cx)
                    .into_any_element()
            })
            .collect::<Vec<_>>();

        let theme = cx.theme();

        div()
            .w(ApprovalsMetrics::LIST_WIDTH)
            .flex_shrink_0()
            .h_full()
            .flex()
            .flex_col()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.background)
            .child(
                div()
                    .flex()
                    .items_center()
                    .px(ApprovalsMetrics::LIST_PADDING_X)
                    .py(ApprovalsMetrics::LIST_PADDING_Y)
                    .child(
                        Text::label(dbflux_i18n::t!(
                            "document.governance.pending_count",
                            count = self.pending.len()
                        ))
                        .font_size(ApprovalsMetrics::SECTION_LABEL_FONT),
                    ),
            )
            .child(
                div()
                    .id("mcp-approvals-list")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .when(self.pending.is_empty(), |list| {
                        list.child(
                            div()
                                .px(ApprovalsMetrics::LIST_PADDING_X)
                                .child(Text::caption(dbflux_i18n::t!(
                                    "document.governance.no_pending"
                                ))),
                        )
                    })
                    .children(rows),
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .items_center()
                    .gap(ApprovalsMetrics::HINT_GAP)
                    .px(ApprovalsMetrics::LIST_PADDING_X)
                    .py(ApprovalsMetrics::LIST_PADDING_Y)
                    .border_t_1()
                    .border_color(theme.border)
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(theme.muted_foreground)
                    .child(Kbd::new("j"))
                    .child(Kbd::new("k"))
                    .child(dbflux_i18n::t!("document.governance.list_hint")),
            )
    }

    fn render_detail(
        &self,
        detail: PendingExecutionDetail,
        now_ms: i64,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let summary = &detail.summary;

        let icon_value = |icon: AppIcon, color: Hsla, value: String| {
            div()
                .flex()
                .items_center()
                .gap(ApprovalsMetrics::VALUE_ICON_GAP)
                .child(
                    Icon::new(icon)
                        .size(ApprovalsMetrics::VALUE_ICON)
                        .color(color),
                )
                .child(value)
        };

        let fields: Vec<AnyElement> = vec![
            detail_field(
                dbflux_i18n::t!("document.governance.requested_by"),
                icon_value(AppIcon::Bot, tint, summary.actor_id.clone()),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.governance.connection"),
                icon_value(
                    AppIcon::Database,
                    theme.info,
                    self.connection_name(&summary.connection_id, cx),
                ),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.governance.classification_label"),
                Badge::new(
                    Self::classification_display(summary.classification),
                    Self::classification_tone(summary.classification),
                ),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.governance.tool"),
                summary.tool_id.clone(),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.governance.requested_at"),
                dbflux_components::common::time_range::format_timestamp_ms(
                    summary.created_at_epoch_ms,
                    dbflux_components::common::time_range::TimestampDisplayMode::Local,
                ),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.governance.waiting_label"),
                Self::waiting_label(summary.created_at_epoch_ms, now_ms),
                cx,
            )
            .into_any_element(),
        ];

        let mut grid_rows = Vec::new();
        let mut fields = fields.into_iter().peekable();

        while fields.peek().is_some() {
            let mut row = div().flex().gap(ApprovalsMetrics::GRID_GAP);

            for _ in 0..ApprovalsMetrics::GRID_COLUMNS {
                let cell = div().flex_1().min_w_0();
                row = row.child(match fields.next() {
                    Some(field) => cell.child(field),
                    None => cell,
                });
            }

            grid_rows.push(row);
        }

        let payload =
            serde_json::to_string_pretty(&detail.plan).unwrap_or_else(|_| detail.plan.to_string());
        let highlights = json_highlights(&payload, &SyntaxColors::for_current(cx));

        let payload_block = div()
            .relative()
            .px(ApprovalsMetrics::CODE_PADDING_X)
            .py(ApprovalsMetrics::CODE_PADDING_Y)
            .font_family(dbflux_components::fonts::editor_family(cx))
            .text_size(ApprovalsMetrics::CODE_FONT)
            .line_height(relative(ApprovalsMetrics::CODE_LINE_HEIGHT))
            .text_color(ChromeColors::strong(theme))
            .child(
                Chamfer::new(ChamferCut::INPUT)
                    .fill(theme.background)
                    .border(theme.border),
            )
            .child(StyledText::new(payload).with_highlights(highlights));

        div()
            .flex_1()
            .min_w_0()
            .h_full()
            .flex()
            .flex_col()
            .child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(ApprovalsMetrics::TITLE_GAP)
                    .h(ApprovalsMetrics::TITLE_HEIGHT)
                    .px(ApprovalsMetrics::DETAIL_PADDING_X)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        Icon::new(AppIcon::Bot)
                            .size(ApprovalsMetrics::TITLE_ICON)
                            .color(tint),
                    )
                    .child(
                        div()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .text_size(ApprovalsMetrics::TITLE_FONT)
                            .font_weight(FontWeight::BOLD)
                            .text_color(ChromeColors::strong(theme))
                            .child(summary.tool_id.clone()),
                    )
                    .child(Badge::new(
                        dbflux_i18n::t!("document.governance.status.pending"),
                        BadgeTone::Accent,
                    ))
                    .child(div().flex_1())
                    .child(
                        div()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .text_size(ApprovalsMetrics::META_FONT)
                            .text_color(theme.muted_foreground)
                            .child(dbflux_i18n::t!(
                                "document.governance.exec_id",
                                id = summary.id.clone()
                            )),
                    ),
            )
            .child(
                div()
                    .id("mcp-approval-detail")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .gap(ApprovalsMetrics::SECTION_GAP)
                    .px(ApprovalsMetrics::DETAIL_PADDING_X)
                    .py(ApprovalsMetrics::SECTION_GAP)
                    .when_some(self.status_message.clone(), |detail, message| {
                        detail.child(BannerBlock::new(BannerVariant::Danger, message))
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap(ApprovalsMetrics::GRID_GAP)
                            .children(grid_rows),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap(ApprovalsMetrics::SECTION_TITLE_GAP)
                            .child(
                                Text::label(dbflux_i18n::t!("document.governance.execution_plan"))
                                    .font_size(ApprovalsMetrics::SECTION_LABEL_FONT),
                            )
                            .child(payload_block),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(ApprovalsMetrics::TITLE_GAP)
                    .px(ApprovalsMetrics::DETAIL_PADDING_X)
                    .py(ApprovalsMetrics::FOOTER_PADDING_Y)
                    .border_t_1()
                    .border_color(theme.border)
                    .child(
                        div().flex_1().min_w_0().child(
                            Input::new(&self.reject_reason)
                                .id("approval-reject-reason")
                                .w_full()
                                .placeholder(dbflux_i18n::t!(
                                    "document.governance.reject_reason_placeholder"
                                ))
                                .prefix(
                                    Icon::new(AppIcon::Pencil)
                                        .size(ApprovalsMetrics::VALUE_ICON)
                                        .color(theme.muted_foreground),
                                ),
                        ),
                    )
                    .child(
                        Button::new(
                            "mcp-approval-reject",
                            dbflux_i18n::t!("document.governance.reject"),
                        )
                        .danger()
                        .icon(AppIcon::CircleX)
                        .when_some(
                            shortcut_label(ContextId::McpApprovals, Command::RejectExecution),
                            |button, keys| button.kbd(keys),
                        )
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.reject_selected(window, cx);
                        })),
                    )
                    .child(
                        Button::new(
                            "mcp-approval-approve",
                            dbflux_i18n::t!("document.governance.approve"),
                        )
                        .variant(ButtonVariant::Primary)
                        .icon(AppIcon::Check)
                        .when_some(
                            shortcut_label(ContextId::McpApprovals, Command::ApproveExecution),
                            |button, keys| button.kbd(keys),
                        )
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.approve_selected(window, cx);
                        })),
                    ),
            )
    }
}

impl Focusable for McpApprovalsView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for McpApprovalsView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0);

        let detail: AnyElement = match self.selected_detail.clone() {
            Some(detail) => self.render_detail(detail, now_ms, cx).into_any_element(),
            None => div()
                .flex_1()
                .h_full()
                .flex()
                .flex_col()
                .items_center()
                .justify_center()
                .gap(DocumentMetrics::GAP)
                .p(ApprovalsMetrics::DETAIL_PADDING_X)
                .when_some(self.status_message.clone(), |empty, message| {
                    empty.child(BannerBlock::new(BannerVariant::Danger, message))
                })
                .child(Text::caption(dbflux_i18n::t!(
                    "document.governance.select_prompt"
                )))
                .into_any_element(),
        };

        div()
            .id("mcp-approvals")
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(self.render_header(cx))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .child(self.render_pending_list(now_ms, cx))
                    .child(detail),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::McpApprovalsView;
    use dbflux_components::primitives::{BadgeTone, TextColorSelection, TextDefaultColor};
    use dbflux_components::typography::AppFonts;
    use dbflux_policy::ExecutionClassification;
    use gpui::Focusable as _;

    #[test]
    fn pending_tool_names_use_the_code_role() {
        let tool = McpApprovalsView::pending_tool_text("request_execution").inspect();

        assert_eq!(tool.family, AppFonts::MONO);
        assert_eq!(tool.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(tool.size_override, None);
        assert_eq!(
            tool.color_selection,
            TextColorSelection::RoleDefault(TextDefaultColor::Foreground)
        );
    }

    #[test]
    fn classifications_that_can_lose_data_read_as_danger() {
        assert_eq!(
            McpApprovalsView::classification_tone(ExecutionClassification::Read),
            BadgeTone::Info
        );
        assert_eq!(
            McpApprovalsView::classification_tone(ExecutionClassification::Write),
            BadgeTone::Warning
        );

        for classification in [
            ExecutionClassification::Destructive,
            ExecutionClassification::Admin,
            ExecutionClassification::AdminDestructive,
        ] {
            assert_eq!(
                McpApprovalsView::classification_tone(classification),
                BadgeTone::Danger
            );
        }
    }

    #[test]
    fn waiting_label_rounds_down_to_minutes_then_hours() {
        let minute = 60_000;

        assert_eq!(
            McpApprovalsView::waiting_label(0, 30_000),
            dbflux_i18n::t!("document.governance.waiting.just_now")
        );
        assert_eq!(
            McpApprovalsView::waiting_label(0, 4 * minute),
            dbflux_i18n::t!("document.governance.waiting.minutes", count = 4)
        );
        assert_eq!(
            McpApprovalsView::waiting_label(0, 125 * minute),
            dbflux_i18n::t!("document.governance.waiting.hours", count = 2)
        );
    }

    fn approvals_view_with_one_pending_call(
        cx: &mut gpui::TestAppContext,
    ) -> (
        gpui::Entity<McpApprovalsView>,
        gpui::Entity<dbflux_ui_base::AppStateEntity>,
        String,
        &mut gpui::VisualTestContext,
    ) {
        use gpui::AppContext as _;

        crate::keyboard_test_support::init_keyboard_runtime(cx);

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("isolated storage runtime");
                dbflux_ui_base::AppStateEntity::new_with_storage_runtime(storage_runtime)
                    .expect("test storage setup")
            })
        });

        let pending = app_state.update(cx, |state, _| {
            state
                .request_mcp_execution(
                    "agent-a".to_string(),
                    "conn-a".to_string(),
                    "delete_records".to_string(),
                    ExecutionClassification::Destructive,
                    serde_json::json!({ "table": "items" }),
                )
                .expect("queue a pending execution")
        });

        let (host, window) = crate::keyboard_test_support::host_document(
            cx,
            {
                let app_state = app_state.clone();
                move |window, cx| cx.new(|cx| McpApprovalsView::new(app_state, window, cx))
            },
            |view, _cx| view.active_context(),
            |view, command, window, cx| view.dispatch_command(command, window, cx),
        );
        let view = window.update(|_, cx| host.read(cx).document.clone());

        (view, app_state, pending.id, window)
    }

    /// Two pending calls, the first selected. `keymap_keys_move_over_the_pending_calls`
    /// proves the keys, and the workspace test `the_approvals_keys_reach_the_approvals_tab`
    /// the way in.
    #[gpui::test]
    fn the_approvals_tab_is_covered(cx: &mut gpui::TestAppContext) {
        use crate::keyboard_coverage::MCP_APPROVALS;
        use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

        let (view, app_state, _pending_id, window) = approvals_view_with_one_pending_call(cx);
        app_state.update(window, |state, _| {
            state
                .request_mcp_execution(
                    "agent-b".to_string(),
                    "conn-b".to_string(),
                    "update_records".to_string(),
                    ExecutionClassification::Write,
                    serde_json::json!({ "table": "items" }),
                )
                .expect("queue a second pending execution")
        });
        window.update(|_, cx| view.update(cx, |view, cx| view.refresh(cx)));
        window.run_until_parked();

        let menu: Vec<String> = window.update(|_, cx| {
            view.read(cx)
                .pane_actions(&view)
                .into_iter()
                .map(|action| action.id.to_string())
                .collect()
        });

        let capture = FrameCapture::observe(window);
        let checked = Coverage::new(MCP_APPROVALS)
            .with_menu_entries(menu)
            .assert_covered(&capture.frame(window));
        assert!(
            checked.iter().any(|id| id == "mcp-approval-approve"),
            "{checked:?}"
        );
    }

    #[gpui::test]
    fn refresh_selects_the_call_a_notification_asked_for(cx: &mut gpui::TestAppContext) {
        let (view, app_state, first_id, window) = approvals_view_with_one_pending_call(cx);
        assert_eq!(
            window.update(|_, cx| view.read(cx).selected_id.clone()),
            Some(first_id)
        );

        let second = app_state.update(window, |state, _| {
            state
                .request_mcp_execution(
                    "agent-b".to_string(),
                    "conn-b".to_string(),
                    "update_records".to_string(),
                    ExecutionClassification::Write,
                    serde_json::json!({ "table": "items" }),
                )
                .expect("queue a second pending execution")
        });

        window.update(|_, cx| {
            app_state.update(cx, |state, _| {
                state.pending_approval_focus = Some(second.id.clone());
            });
            view.update(cx, |view, cx| view.refresh(cx));
        });

        let (selected, request_left) = window.update(|_, cx| {
            (
                view.read(cx).selected_id.clone(),
                app_state.read(cx).pending_approval_focus.clone(),
            )
        });
        assert_eq!(selected, Some(second.id));
        assert_eq!(request_left, None, "the request is consumed");
    }

    #[gpui::test]
    fn shortcut_letters_type_into_the_reason_field(cx: &mut gpui::TestAppContext) {
        let (view, app_state, pending_id, window) = approvals_view_with_one_pending_call(cx);

        window.update(|window, cx| {
            let reason_focus = view.read(cx).reject_reason.read(cx).focus_handle(cx);
            reason_focus.focus(window, cx);
        });
        window.simulate_keystrokes("r a");
        window.run_until_parked();

        let typed = window.update(|_, cx| view.read(cx).reject_reason.read(cx).value().to_string());
        assert_eq!(typed, "ra");

        let still_pending = window.update(|_, cx| {
            app_state
                .read(cx)
                .list_mcp_pending_executions()
                .expect("list pending executions")
        });
        assert!(
            still_pending.iter().any(|entry| entry.id == pending_id),
            "typing r or a in the reason field must neither reject nor approve"
        );
    }

    #[gpui::test]
    fn reject_sends_the_typed_reason_and_clears_the_field(cx: &mut gpui::TestAppContext) {
        let (view, app_state, pending_id, window) = approvals_view_with_one_pending_call(cx);

        window.update(|window, cx| {
            view.update(cx, |view, cx| {
                view.reject_reason.update(cx, |input, cx| {
                    input.set_value("wrong table", window, cx);
                });
                view.focus_handle.focus(window, cx);
            });
        });
        window.simulate_keystrokes("r");
        window.run_until_parked();

        let (remaining, reason_left) = window.update(|_, cx| {
            (
                app_state
                    .read(cx)
                    .list_mcp_pending_executions()
                    .expect("list pending executions"),
                view.read(cx).reject_reason.read(cx).value().to_string(),
            )
        });
        assert!(remaining.iter().all(|entry| entry.id != pending_id));
        assert!(
            reason_left.is_empty(),
            "the field is cleared after a decision"
        );

        let rejections = window.update(|_, cx| {
            app_state
                .read(cx)
                .audit_service()
                .query_extended(&dbflux_audit::query::AuditQueryFilter {
                    action: Some(
                        dbflux_core::observability::actions::MCP_REJECT_EXECUTION
                            .as_str()
                            .to_string(),
                    ),
                    ..Default::default()
                })
                .expect("query the audit log")
        });
        assert_eq!(rejections.len(), 1);
        assert_eq!(rejections[0].error_message.as_deref(), Some("wrong table"));
    }

    /// Two pending calls in a view hosted like the workspace hosts it, with
    /// the app keymap and the view's key context.
    fn keyboard_approvals_view(
        cx: &mut gpui::TestAppContext,
    ) -> (
        gpui::Entity<McpApprovalsView>,
        gpui::Entity<dbflux_ui_base::AppStateEntity>,
        Vec<String>,
        &mut gpui::VisualTestContext,
    ) {
        use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
        use gpui::AppContext as _;

        init_keyboard_runtime(cx);

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("isolated storage runtime");
                dbflux_ui_base::AppStateEntity::new_with_storage_runtime(storage_runtime)
                    .expect("test storage setup")
            })
        });

        let mut ids: Vec<String> = ["delete_records", "update_records"]
            .into_iter()
            .map(|tool| {
                app_state.update(cx, |state, _| {
                    state
                        .request_mcp_execution(
                            "agent-a".to_string(),
                            "conn-a".to_string(),
                            tool.to_string(),
                            ExecutionClassification::Write,
                            serde_json::json!({ "table": "items" }),
                        )
                        .expect("queue a pending execution")
                        .id
                })
            })
            .collect();
        ids.sort();

        let (host, window) = host_document(
            cx,
            {
                let app_state = app_state.clone();
                move |window, cx| cx.new(|cx| McpApprovalsView::new(app_state, window, cx))
            },
            |view, _cx| view.active_context(),
            |view, command, window, cx| view.dispatch_command(command, window, cx),
        );

        let view = window.update(|_, cx| host.read(cx).document.clone());
        window.update(|window, cx| view.update(cx, |view, cx| view.focus(window, cx)));
        window.run_until_parked();

        (view, app_state, ids, window)
    }

    #[gpui::test]
    fn keymap_keys_move_over_the_pending_calls(cx: &mut gpui::TestAppContext) {
        let (view, _app_state, ids, window) = keyboard_approvals_view(cx);
        let selected = |window: &mut gpui::VisualTestContext| {
            window.update(|_, cx| view.read(cx).selected_id.clone())
        };

        assert_eq!(selected(window).as_ref(), Some(&ids[0]));

        window.simulate_keystrokes("j");
        assert_eq!(
            selected(window).as_ref(),
            Some(&ids[1]),
            "j selects the next call"
        );

        window.simulate_keystrokes("k");
        assert_eq!(
            selected(window).as_ref(),
            Some(&ids[0]),
            "k selects the previous call"
        );

        window.simulate_keystrokes("shift-g");
        assert_eq!(
            selected(window).as_ref(),
            Some(&ids[1]),
            "Shift+G selects the last call"
        );

        window.simulate_keystrokes("g");
        assert_eq!(
            selected(window).as_ref(),
            Some(&ids[0]),
            "g selects the first call"
        );
    }

    #[gpui::test]
    fn keymap_keys_type_the_reason_reject_and_approve(cx: &mut gpui::TestAppContext) {
        let (view, app_state, ids, window) = keyboard_approvals_view(cx);
        let pending = |window: &mut gpui::VisualTestContext| {
            window.update(|_, cx| {
                app_state
                    .read(cx)
                    .list_mcp_pending_executions()
                    .expect("list pending executions")
                    .into_iter()
                    .map(|entry| entry.id)
                    .collect::<Vec<_>>()
            })
        };

        window.simulate_keystrokes("enter");
        window.simulate_keystrokes("r a");
        window.run_until_parked();
        assert_eq!(
            window.update(|_, cx| view.read(cx).reject_reason.read(cx).value().to_string()),
            "ra",
            "Enter hands the keyboard to the reason field, which types r and a"
        );
        assert_eq!(pending(window).len(), 2, "typing decides nothing");

        window.simulate_keystrokes("escape");
        window.simulate_keystrokes("r");
        window.run_until_parked();
        assert_eq!(
            pending(window),
            vec![ids[1].clone()],
            "r rejects the selected call"
        );

        window.simulate_keystrokes("a");
        window.run_until_parked();
        assert!(pending(window).is_empty(), "a approves the selected call");
    }

    #[gpui::test]
    fn the_pane_actions_list_the_decisions(cx: &mut gpui::TestAppContext) {
        let (view, _app_state, _ids, window) = keyboard_approvals_view(cx);

        let ids: Vec<String> = window.update(|_, cx| {
            view.read(cx)
                .pane_actions(&view)
                .into_iter()
                .map(|action| action.id.to_string())
                .collect()
        });

        assert_eq!(
            ids,
            [
                "approval-approve",
                "approval-reject",
                "approval-reason",
                "approval-refresh"
            ]
        );
    }

    #[test]
    fn approvals_keys_resolve_in_every_locale() {
        for key in [
            "document.governance.title",
            "document.governance.subtitle",
            "document.governance.list_hint",
            "document.governance.requested_by",
            "document.governance.classification.admin_destructive",
            "document.governance.waiting.just_now",
            "document.governance.pane_actions.reason",
        ] {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty() && value != format!("{locale}.{key}"),
                    "{key} missing in {locale}"
                );
            }
        }
    }
}
