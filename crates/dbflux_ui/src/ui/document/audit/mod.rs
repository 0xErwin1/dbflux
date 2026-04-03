//! Audit document module.

mod filters;
mod source_adapter;

pub use filters::{AuditFilters, TimeRange};
pub use source_adapter::AuditSourceAdapter;

use std::collections::HashSet;

use crate::app::AppStateEntity;
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::components::toast::{PendingToast, flush_pending_toast};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    Pagination, RefreshPolicy,
    observability::{EventCategory, EventOutcome, EventSeverity},
};
use dbflux_storage::repositories::audit::{AuditEventDto, AuditQueryFilter};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::scroll::ScrollableElement;

use super::chrome::{compact_labeled_control, compact_top_bar, workspace_footer_bar};
use super::types::{DocumentIcon, DocumentId, DocumentKind, DocumentState};

/// Events emitted by AuditDocument.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum AuditDocumentEvent {
    Refresh,
}

const DEFAULT_PAGE_SIZE: u32 = 100;

/// Audit event viewer document.
pub struct AuditDocument {
    adapter: AuditSourceAdapter,
    filters: AuditFilters,
    events: Vec<AuditEventDto>,
    total_events: u64,
    expanded_event_ids: HashSet<i64>,
    pagination: Pagination,
    status_message: Option<String>,
    is_loading: bool,
    id: DocumentId,
    title: String,
    pending_initial_load: bool,
    pending_toast: Option<PendingToast>,
    export_menu_open: bool,
    search_input: Entity<InputState>,
    dropdown_time_range: Entity<Dropdown>,
    dropdown_level: Entity<Dropdown>,
    dropdown_category: Entity<Dropdown>,
    dropdown_outcome: Entity<Dropdown>,
    refresh_policy: RefreshPolicy,
    refresh_dropdown: Entity<Dropdown>,
    load_request_id: u64,
    _refresh_timer: Option<Task<()>>,
    _subscriptions: Vec<Subscription>,
}

impl AuditDocument {
    /// Creates a new audit document.
    pub fn new(app_state: Entity<AppStateEntity>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder("Search events..."));

        let dropdown_time_range = cx.new(|_cx| {
            Dropdown::new("audit-time-range")
                .placeholder("Last 24 h")
                .items(Self::time_range_items())
                .selected_index(Some(2))
                .toolbar_style(true)
        });

        let dropdown_level = cx.new(|_cx| {
            Dropdown::new("audit-level")
                .placeholder("All")
                .items(Self::level_items())
                .selected_index(Some(0))
                .toolbar_style(true)
        });

        let dropdown_category = cx.new(|_cx| {
            Dropdown::new("audit-category")
                .placeholder("All")
                .items(Self::category_items())
                .selected_index(Some(0))
                .toolbar_style(true)
        });

        let dropdown_outcome = cx.new(|_cx| {
            Dropdown::new("audit-outcome")
                .placeholder("All")
                .items(Self::outcome_items())
                .selected_index(Some(0))
                .toolbar_style(true)
        });

        let audit_repo = app_state.read(cx).storage_runtime().audit();
        let adapter = AuditSourceAdapter::new(audit_repo);

        let search_sub = cx.subscribe(&search_input, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::PressEnter { secondary: false }) {
                this.handle_search_submit(cx);
            }
        });

        let time_range_sub = cx.subscribe(
            &dropdown_time_range,
            |this, _, event: &DropdownSelectionChanged, cx| {
                if let Some(range) = Self::time_range_for_index(event.index) {
                    let (start_ms, end_ms) = range.to_filter_values();
                    this.filters.start_ms = start_ms;
                    this.filters.end_ms = end_ms;
                    this.reset_pagination();
                    this.load_events(cx);
                }
            },
        );

        let level_sub = cx.subscribe(
            &dropdown_level,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.filters.level = Self::level_for_index(event.index);
                this.reset_pagination();
                this.load_events(cx);
            },
        );

        let category_sub = cx.subscribe(
            &dropdown_category,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.filters.category = Self::category_for_index(event.index);
                this.reset_pagination();
                this.load_events(cx);
            },
        );

        let outcome_sub = cx.subscribe(
            &dropdown_outcome,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.filters.outcome = Self::outcome_for_index(event.index);
                this.reset_pagination();
                this.load_events(cx);
            },
        );

        // Refresh policy dropdown — identical construction to DataGridPanel.
        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|policy| DropdownItem::new(policy.label()))
                .collect();

            Dropdown::new("audit-auto-refresh")
                .items(items)
                .selected_index(Some(RefreshPolicy::Manual.index()))
                .compact_trigger(true)
        });

        let refresh_dropdown_sub = cx.subscribe(
            &refresh_dropdown,
            |this, _, event: &DropdownSelectionChanged, cx| {
                let policy = RefreshPolicy::from_index(event.index);
                this.set_refresh_policy(policy, cx);
            },
        );

        Self {
            adapter,
            filters: Self::default_filters(),
            events: Vec::new(),
            total_events: 0,
            expanded_event_ids: HashSet::new(),
            pagination: Pagination::Offset {
                limit: DEFAULT_PAGE_SIZE,
                offset: 0,
            },
            status_message: None,
            is_loading: false,
            id: DocumentId::new(),
            title: "Audit".to_string(),
            pending_initial_load: true,
            pending_toast: None,
            export_menu_open: false,
            search_input,
            dropdown_time_range,
            dropdown_level,
            dropdown_category,
            dropdown_outcome,
            refresh_policy: RefreshPolicy::Manual,
            refresh_dropdown,
            load_request_id: 0,
            _refresh_timer: None,
            _subscriptions: vec![
                search_sub,
                time_range_sub,
                level_sub,
                category_sub,
                outcome_sub,
                refresh_dropdown_sub,
            ],
        }
    }

    fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        if self.refresh_policy == policy {
            return;
        }
        self.refresh_policy = policy;
        self.update_refresh_timer(cx);
        cx.notify();
    }

    fn update_refresh_timer(&mut self, cx: &mut Context<Self>) {
        // Drop existing timer.
        self._refresh_timer = None;

        let Some(duration) = self.refresh_policy.duration() else {
            return;
        };

        self._refresh_timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(duration).await;

                let _ = cx.update(|cx| {
                    let Some(entity) = this.upgrade() else {
                        return;
                    };
                    entity.update(cx, |doc, cx| {
                        if !doc.refresh_policy.is_auto() || doc.is_loading {
                            return;
                        }
                        doc.load_events(cx);
                    });
                });
            }
        }));
    }

    fn default_filters() -> AuditFilters {
        let mut filters = AuditFilters::default();
        let (start_ms, end_ms) = TimeRange::Last24h.to_filter_values();
        filters.start_ms = start_ms;
        filters.end_ms = end_ms;
        filters
    }

    fn active_filter(&self, limit: Option<usize>, offset: Option<usize>) -> AuditQueryFilter {
        let level_str = self
            .filters
            .level
            .as_ref()
            .map(|level| level.as_str().to_string());
        let category_str = self
            .filters
            .category
            .as_ref()
            .map(|category| category.as_str().to_string());
        let categories_str = self.filters.categories.as_ref().map(|categories| {
            categories
                .iter()
                .map(|category| category.as_str().to_string())
                .collect()
        });
        let source_str = self
            .filters
            .source
            .as_ref()
            .map(|source| source.as_str().to_string());

        AuditQueryFilter {
            id: None,
            actor_id: self.filters.actor.clone(),
            tool_id: None,
            decision: None,
            profile_id: None,
            classification: None,
            start_epoch_ms: self.filters.start_ms,
            end_epoch_ms: self.filters.end_ms,
            limit,
            offset,
            level: level_str,
            category: category_str,
            action: None,
            categories: categories_str,
            source_id: source_str,
            outcome: self
                .filters
                .outcome
                .as_ref()
                .map(|outcome| outcome.as_str().to_string()),
            connection_id: self.filters.connection_id.clone(),
            driver_id: self.filters.driver_id.clone(),
            actor_type: self
                .filters
                .actor_type
                .as_ref()
                .map(|actor_type| actor_type.as_str().to_string()),
            object_type: None,
            free_text: self.filters.free_text.clone(),
            correlation_id: self.filters.correlation_id.clone(),
            session_id: None,
        }
    }

    fn pagination_limit(&self) -> usize {
        self.pagination.limit() as usize
    }

    fn pagination_offset(&self) -> usize {
        self.pagination.offset() as usize
    }

    fn reset_pagination(&mut self) {
        self.pagination = self.pagination.reset_offset();
    }

    fn current_page_range(&self) -> Option<(u64, u64)> {
        if self.events.is_empty() || self.total_events == 0 {
            return None;
        }

        let start = self.pagination.offset() + 1;
        let end = self.pagination.offset() + self.events.len() as u64;
        Some((start, end))
    }

    fn total_pages(&self) -> Option<u64> {
        if self.total_events == 0 {
            return None;
        }

        Some(self.total_events.div_ceil(self.pagination.limit() as u64))
    }

    fn can_go_prev(&self) -> bool {
        !self.pagination.is_first_page()
    }

    fn can_go_next(&self) -> bool {
        self.pagination.offset() + (self.events.len() as u64) < self.total_events
    }

    fn load_events(&mut self, cx: &mut Context<Self>) {
        self.load_request_id += 1;
        let request_id = self.load_request_id;
        self.is_loading = true;
        self.export_menu_open = false;
        self.status_message = Some("Loading audit events...".to_string());
        cx.notify();

        let page_filter = self.active_filter(
            Some(self.pagination_limit()),
            Some(self.pagination_offset()),
        );
        let count_filter = self.active_filter(None, None);
        let adapter = self.adapter.clone();

        let task = cx.background_executor().spawn(async move {
            let events = adapter.query_filter(&page_filter)?;
            let total = adapter.count_filter(&count_filter)?;
            Ok::<_, String>((events, total))
        });

        cx.spawn(async move |this, cx| match task.await {
            Ok((events, total_events)) => {
                let _ = cx.update(|cx| {
                    this.update(cx, |doc, cx| {
                        if doc.load_request_id != request_id {
                            return;
                        }

                        let visible_ids: HashSet<i64> =
                            events.iter().map(|event| event.id).collect();

                        doc.events = events;
                        doc.total_events = total_events;
                        doc.is_loading = false;
                        doc.status_message = None;
                        doc.expanded_event_ids
                            .retain(|event_id| visible_ids.contains(event_id));

                        cx.notify();
                    })
                });
            }
            Err(error) => {
                let _ = cx.update(|cx| {
                    this.update(cx, |doc, cx| {
                        if doc.load_request_id != request_id {
                            return;
                        }

                        doc.events.clear();
                        doc.total_events = 0;
                        doc.expanded_event_ids.clear();
                        doc.is_loading = false;
                        doc.status_message = Some(format!("Error loading events: {}", error));

                        cx.notify();
                    })
                });
            }
        })
        .detach();
    }

    fn handle_search_submit(&mut self, cx: &mut Context<Self>) {
        let search_text = self.search_input.read(cx).value().trim().to_string();
        self.filters.free_text = (!search_text.is_empty()).then_some(search_text);
        self.reset_pagination();
        self.load_events(cx);
    }

    pub fn clear_filters(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.filters = Self::default_filters();
        self.reset_pagination();
        self.export_menu_open = false;

        self.dropdown_time_range
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(2), cx));
        self.dropdown_level
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(0), cx));
        self.dropdown_category
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(0), cx));
        self.dropdown_outcome
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(0), cx));
        self.search_input
            .update(cx, |state, cx| state.set_value("", window, cx));

        self.load_events(cx);
    }

    fn toggle_event_expanded(&mut self, event_id: i64, cx: &mut Context<Self>) {
        if !self.expanded_event_ids.insert(event_id) {
            self.expanded_event_ids.remove(&event_id);
        }

        cx.notify();
    }

    fn go_to_prev_page(&mut self, cx: &mut Context<Self>) {
        let Some(prev) = self.pagination.prev_page() else {
            return;
        };

        self.pagination = prev;
        self.load_events(cx);
    }

    fn go_to_next_page(&mut self, cx: &mut Context<Self>) {
        if !self.can_go_next() {
            return;
        }

        self.pagination = self.pagination.next_page();
        self.load_events(cx);
    }

    fn toggle_export_menu(&mut self, cx: &mut Context<Self>) {
        self.export_menu_open = !self.export_menu_open;
        cx.notify();
    }

    fn export_with_format(&mut self, format: &'static str, cx: &mut Context<Self>) {
        self.export_menu_open = false;
        self.do_export(format.to_string(), cx);
    }

    fn time_range_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new("Last 15 min"),
            DropdownItem::new("Last hour"),
            DropdownItem::new("Last 24 h"),
            DropdownItem::new("Last 7 days"),
        ]
    }

    fn time_range_for_index(index: usize) -> Option<TimeRange> {
        match index {
            0 => Some(TimeRange::Last15min),
            1 => Some(TimeRange::LastHour),
            2 => Some(TimeRange::Last24h),
            3 => Some(TimeRange::Last7Days),
            _ => None,
        }
    }

    fn level_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new("All"),
            DropdownItem::new("Error"),
            DropdownItem::new("Warn"),
            DropdownItem::new("Info"),
        ]
    }

    fn level_for_index(index: usize) -> Option<EventSeverity> {
        match index {
            1 => Some(EventSeverity::Error),
            2 => Some(EventSeverity::Warn),
            3 => Some(EventSeverity::Info),
            _ => None,
        }
    }

    fn category_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new("All"),
            DropdownItem::new("Config"),
            DropdownItem::new("Connection"),
            DropdownItem::new("Query"),
            DropdownItem::new("Hook"),
            DropdownItem::new("Script"),
            DropdownItem::new("System"),
            DropdownItem::new("MCP"),
            DropdownItem::new("Governance"),
        ]
    }

    fn category_for_index(index: usize) -> Option<EventCategory> {
        match index {
            1 => Some(EventCategory::Config),
            2 => Some(EventCategory::Connection),
            3 => Some(EventCategory::Query),
            4 => Some(EventCategory::Hook),
            5 => Some(EventCategory::Script),
            6 => Some(EventCategory::System),
            7 => Some(EventCategory::Mcp),
            8 => Some(EventCategory::Governance),
            _ => None,
        }
    }

    fn outcome_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new("All"),
            DropdownItem::new("Success"),
            DropdownItem::new("Failure"),
            DropdownItem::new("Cancelled"),
        ]
    }

    fn outcome_for_index(index: usize) -> Option<EventOutcome> {
        match index {
            1 => Some(EventOutcome::Success),
            2 => Some(EventOutcome::Failure),
            3 => Some(EventOutcome::Cancelled),
            _ => None,
        }
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    #[allow(dead_code)]
    pub fn kind(&self) -> DocumentKind {
        DocumentKind::Audit
    }

    #[allow(dead_code)]
    pub fn icon(&self) -> DocumentIcon {
        DocumentIcon::Audit
    }

    pub fn state(&self) -> DocumentState {
        if self.is_loading {
            DocumentState::Loading
        } else if self.status_message.is_some() && self.events.is_empty() {
            DocumentState::Error
        } else {
            DocumentState::Clean
        }
    }

    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.load_events(cx);
    }

    fn short_category_label(category: Option<&str>) -> &'static str {
        match category {
            Some("config") => "CONFIG",
            Some("connection") => "CONN",
            Some("query") => "QUERY",
            Some("hook") => "HOOK",
            Some("script") => "SCRIPT",
            Some("system") => "SYS",
            Some("mcp") => "MCP",
            Some("governance") => "GOV",
            _ => "---",
        }
    }

    fn level_color(level: Option<&str>, theme: &gpui_component::Theme) -> Hsla {
        match level {
            Some("error") => theme.danger,
            Some("warn") => theme.warning,
            Some("info") => theme.primary,
            _ => theme.muted_foreground,
        }
    }

    fn level_bg_color(level: Option<&str>, theme: &gpui_component::Theme) -> Hsla {
        match level {
            Some("error") => theme.danger.opacity(0.15),
            Some("warn") => theme.warning.opacity(0.15),
            Some("info") => theme.primary.opacity(0.15),
            _ => theme.muted_foreground.opacity(0.15),
        }
    }

    fn format_timestamp_ms(ms: i64) -> String {
        let secs = ms / 1000;
        let millis = ms % 1000;
        let hours = (secs / 3600) % 24;
        let minutes = (secs / 60) % 60;
        let secs = secs % 60;
        format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, secs, millis)
    }

    fn format_connection_driver(
        connection_id: &Option<String>,
        driver_id: &Option<String>,
    ) -> Option<String> {
        let connection = connection_id.as_deref().filter(|value| !value.is_empty());
        let driver = driver_id.as_deref().filter(|value| !value.is_empty());

        match (connection, driver) {
            (Some(connection), Some(driver)) => Some(format!("{} / {}", connection, driver)),
            (Some(connection), None) => Some(connection.to_string()),
            (None, Some(driver)) => Some(driver.to_string()),
            _ => None,
        }
    }

    fn pretty_json(json: &str) -> String {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(json) {
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| json.to_string())
        } else {
            json.to_string()
        }
    }

    fn filter_by_correlation(&mut self, correlation_id: String, cx: &mut Context<Self>) {
        self.filters.correlation_id = Some(correlation_id);
        self.reset_pagination();
        self.load_events(cx);
    }

    fn do_export(&mut self, format: String, cx: &mut Context<Self>) {
        let adapter = self.adapter.clone();
        let filter = self.active_filter(None, None);
        let format_for_task = format.clone();

        let task = cx.background_executor().spawn(async move {
            let event_count = adapter.count_filter(&filter)?;
            let bytes = adapter.export_filtered(&filter, &format_for_task)?;
            Ok::<_, String>((event_count, bytes))
        });

        cx.spawn(async move |this, cx| match task.await {
            Ok((event_count, bytes)) => {
                let extension = if format == "csv" { "csv" } else { "json" };
                let filename = format!("audit_export.{}", extension);
                let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());

                let path = if std::fs::create_dir_all(format!("{}/Downloads", home)).is_ok() {
                    format!("{}/Downloads/{}", home, filename)
                } else {
                    format!("{}/{}", home, filename)
                };

                let message = match std::fs::write(&path, &bytes) {
                    Ok(_) => PendingToast {
                        message: format!("Exported {} events to {}", event_count, path),
                        is_error: false,
                    },
                    Err(error) => PendingToast {
                        message: format!("Export succeeded but failed to write file: {}", error),
                        is_error: true,
                    },
                };

                let _ = cx.update(|cx| {
                    this.update(cx, |doc, cx| {
                        doc.pending_toast = Some(message);
                        cx.notify();
                    })
                });
            }
            Err(error) => {
                let _ = cx.update(|cx| {
                    this.update(cx, |doc, cx| {
                        doc.pending_toast = Some(PendingToast {
                            message: format!("Export failed: {}", error),
                            is_error: true,
                        });
                        cx.notify();
                    })
                });
            }
        })
        .detach();
    }

    fn render_toolbar(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();

        // Search input — plain inline input like the WHERE filter in DataGridPanel.
        let search_control = div()
            .flex()
            .items_center()
            .w(px(220.0))
            .rounded(Radii::SM)
            .child(div().flex_1().child(Input::new(&self.search_input).small()));

        // Refresh split button — identical to DataGridPanel:
        // left part: action button; right part: compact_trigger dropdown for auto-refresh policy.
        let refresh_label = if self.refresh_policy.is_auto() {
            self.refresh_policy.label()
        } else {
            "Refresh"
        };
        let refresh_icon = if self.refresh_policy.is_auto() {
            AppIcon::Clock.path()
        } else {
            AppIcon::RefreshCcw.path()
        };

        let refresh_btn = div()
            .id("audit-refresh-control")
            .h(Heights::BUTTON)
            .flex()
            .items_center()
            .gap_0()
            .rounded(Radii::SM)
            .bg(theme.background)
            .border_1()
            .border_color(theme.input)
            // left: action
            .child(
                div()
                    .id("audit-refresh-action")
                    .h_full()
                    .px(Spacing::SM)
                    .flex()
                    .items_center()
                    .gap_1()
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.accent.opacity(0.08)))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.load_events(cx);
                    }))
                    .child(
                        svg()
                            .path(refresh_icon)
                            .size_4()
                            .text_color(theme.foreground),
                    )
                    .child(refresh_label),
            )
            // divider
            .child(div().w(px(1.0)).h_full().bg(theme.input))
            // right: policy dropdown
            .child(
                div()
                    .w(px(28.0))
                    .h_full()
                    .child(self.refresh_dropdown.clone()),
            );

        // Clear — plain text action, same hover pattern as other muted controls.
        let clear_btn = div()
            .id("audit-clear-btn")
            .h(Heights::BUTTON)
            .flex()
            .items_center()
            .px(Spacing::SM)
            .rounded(Radii::SM)
            .text_size(FontSizes::SM)
            .text_color(theme.muted_foreground)
            .cursor_pointer()
            .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
            .on_click(cx.listener(|this, _, window, cx| {
                this.clear_filters(window, cx);
            }))
            .child("Clear");

        let _ = window; // suppress unused warning

        compact_top_bar(
            &theme,
            vec![
                compact_labeled_control("Search:", search_control, &theme).into_any_element(),
                compact_labeled_control("Time:", self.dropdown_time_range.clone(), &theme)
                    .into_any_element(),
                compact_labeled_control("Level:", self.dropdown_level.clone(), &theme)
                    .into_any_element(),
                compact_labeled_control("Category:", self.dropdown_category.clone(), &theme)
                    .into_any_element(),
                compact_labeled_control("Outcome:", self.dropdown_outcome.clone(), &theme)
                    .into_any_element(),
                div().flex_1().into_any_element(),
                refresh_btn.into_any_element(),
                clear_btn.into_any_element(),
            ],
        )
    }

    fn render_event_list(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        if self.events.is_empty() && self.is_loading {
            return div()
                .flex_1()
                .items_center()
                .justify_center()
                .child(
                    div()
                        .text_sm()
                        .text_color(theme.muted_foreground)
                        .child("Loading audit events..."),
                )
                .into_any_element();
        }

        if self.events.is_empty()
            && self.status_message.is_some()
            && self.state() == DocumentState::Error
        {
            return div()
                .flex_1()
                .flex()
                .flex_col()
                .items_center()
                .justify_center()
                .gap_3()
                .child(
                    div()
                        .text_color(theme.danger)
                        .text_lg()
                        .font_weight(FontWeight::SEMIBOLD)
                        .child("Failed to load audit events"),
                )
                .child(
                    div()
                        .text_color(theme.muted_foreground)
                        .text_sm()
                        .text_center()
                        .child(self.status_message.clone().unwrap_or_default()),
                )
                .child(
                    Button::new("audit-retry")
                        .label("Retry")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
                )
                .into_any_element();
        }

        if self.events.is_empty() {
            return div()
                .flex_1()
                .flex()
                .flex_col()
                .items_center()
                .justify_center()
                .gap_3()
                .child(
                    div()
                        .text_color(theme.muted_foreground)
                        .text_sm()
                        .child("No audit events match the current filters."),
                )
                .into_any_element();
        }

        let mut rows = Vec::with_capacity(self.events.len());
        for event in self.events.iter().cloned() {
            rows.push(self.render_event_row(event, cx).into_any_element());
        }

        div()
            .id("audit-event-list")
            .flex_1()
            .overflow_y_scrollbar()
            .flex()
            .flex_col()
            .children(rows)
            .into_any_element()
    }

    fn render_event_row(&self, event: AuditEventDto, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let event_id = event.id;
        let is_expanded = self.expanded_event_ids.contains(&event_id);
        let timestamp = Self::format_timestamp_ms(event.created_at_epoch_ms);
        let level = event.level.as_deref().unwrap_or("---");
        let summary = event.summary.clone().unwrap_or_default();
        let summary_display = if summary.is_empty() {
            "-".to_string()
        } else {
            summary
        };
        let category = Self::short_category_label(event.category.as_deref());
        let connection_driver =
            Self::format_connection_driver(&event.connection_id, &event.driver_id);

        div()
            .w_full()
            .border_b_1()
            .border_color(theme.border.opacity(0.5))
            .child(
                div()
                    .id(SharedString::from(format!("audit-event-{}", event_id)))
                    .flex()
                    .items_center()
                    .gap_3()
                    .px_3()
                    .py_1p5()
                    .cursor_pointer()
                    .bg(if is_expanded {
                        theme.primary.opacity(0.08)
                    } else {
                        gpui::transparent_black()
                    })
                    .hover(|style| style.bg(theme.list_hover))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, _, cx| {
                            this.toggle_event_expanded(event_id, cx);
                        }),
                    )
                    .child(
                        svg()
                            .path(if is_expanded {
                                AppIcon::ChevronDown.path()
                            } else {
                                AppIcon::ChevronRight.path()
                            })
                            .size_3()
                            .text_color(theme.muted_foreground),
                    )
                    .child(
                        div()
                            .text_xs()
                            .font_family("monospace")
                            .text_color(theme.muted_foreground)
                            .flex_shrink_0()
                            .child(timestamp),
                    )
                    .child(
                        div()
                            .px_1p5()
                            .py_px()
                            .rounded(px(3.0))
                            .text_xs()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(Self::level_color(event.level.as_deref(), theme))
                            .bg(Self::level_bg_color(event.level.as_deref(), theme))
                            .flex_shrink_0()
                            .child(level.to_uppercase()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .flex_shrink_0()
                            .child(category),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .flex_1()
                            .truncate()
                            .child(summary_display),
                    )
                    .when_some(
                        connection_driver.filter(|value| !value.is_empty()),
                        |row, value| {
                            row.child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .flex_shrink_0()
                                    .child(value),
                            )
                        },
                    ),
            )
            .when(is_expanded, |root| {
                root.child(self.render_inline_detail(event, cx))
            })
    }

    fn render_detail_field(
        &self,
        label: &'static str,
        value: String,
        theme: &gpui_component::Theme,
    ) -> Div {
        div()
            .flex_col()
            .gap_1p5()
            .min_w(px(120.0))
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(theme.muted_foreground)
                    .child(label),
            )
            .child(div().text_sm().text_color(theme.foreground).child(value))
    }

    fn render_inline_detail(
        &self,
        event: AuditEventDto,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let timestamp = Self::format_timestamp_ms(event.created_at_epoch_ms);
        let level = event.level.as_deref().unwrap_or("-").to_string();
        let category = Self::short_category_label(event.category.as_deref()).to_string();
        let outcome = event.outcome.as_deref().unwrap_or("-").to_string();
        let actor = if event
            .actor_type
            .as_deref()
            .filter(|actor_type| !actor_type.is_empty() && *actor_type != "system")
            .is_some()
        {
            format!(
                "{} ({})",
                event.actor_id,
                event.actor_type.as_deref().unwrap_or("")
            )
        } else {
            event.actor_id.clone()
        };
        let action = event.action.as_deref().unwrap_or("-").to_string();
        let source = event.source_id.as_deref().unwrap_or("-").to_string();
        let connection_driver =
            Self::format_connection_driver(&event.connection_id, &event.driver_id);
        let duration = event
            .duration_ms
            .map(|duration_ms| format!("{} ms", duration_ms));
        let summary = event.summary.clone().filter(|value| !value.is_empty());
        let error_message = event
            .error_message
            .clone()
            .filter(|value| !value.is_empty());
        let details_json = event.details_json.clone().filter(|value| !value.is_empty());
        let correlation_id = event
            .correlation_id
            .clone()
            .filter(|value| !value.is_empty());

        div()
            .px_4()
            .pb_3()
            .pt_1()
            .flex()
            .flex_col()
            .gap_3()
            .bg(theme.secondary.opacity(0.35))
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_4()
                    .children(vec![
                        self.render_detail_field("Time", timestamp, theme)
                            .into_any_element(),
                        self.render_detail_field("Level", level, theme)
                            .into_any_element(),
                        self.render_detail_field("Category", category, theme)
                            .into_any_element(),
                        self.render_detail_field("Outcome", outcome, theme)
                            .into_any_element(),
                        self.render_detail_field("Actor", actor, theme)
                            .into_any_element(),
                        self.render_detail_field("Action", action, theme)
                            .into_any_element(),
                        self.render_detail_field("Source", source, theme)
                            .into_any_element(),
                    ])
                    .when_some(connection_driver, |row, value| {
                        row.child(self.render_detail_field("Connection/Driver", value, theme))
                    })
                    .when_some(duration, |row, value| {
                        row.child(self.render_detail_field("Duration", value, theme))
                    }),
            )
            .when_some(summary, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(
                            div()
                                .text_xs()
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(theme.muted_foreground)
                                .child("Summary"),
                        )
                        .child(div().text_sm().text_color(theme.foreground).child(value)),
                )
            })
            .when_some(error_message, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(
                            div()
                                .text_xs()
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(theme.danger)
                                .child("Error"),
                        )
                        .child(div().text_sm().text_color(theme.danger).child(value)),
                )
            })
            .when_some(details_json, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(
                            div()
                                .text_xs()
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(theme.muted_foreground)
                                .child("Details"),
                        )
                        .child(
                            div()
                                .text_xs()
                                .font_family("monospace")
                                .text_color(theme.foreground)
                                .bg(theme.secondary)
                                .p_2()
                                .rounded(px(4.0))
                                .child(Self::pretty_json(&value)),
                        ),
                )
            })
            .when_some(correlation_id, |root, value| {
                let correlation_id_for_click = value.clone();

                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(
                            div()
                                .text_xs()
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(theme.muted_foreground)
                                .child("Correlation ID"),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.primary)
                                .cursor_pointer()
                                .hover(|style| style.underline())
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(move |this, _, _, cx| {
                                        this.filter_by_correlation(
                                            correlation_id_for_click.clone(),
                                            cx,
                                        );
                                    }),
                                )
                                .child(value),
                        ),
                )
            })
    }

    fn render_export_button(
        &self,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let menu_open = self.export_menu_open;

        // Identical to DataGridPanel::render_export_button.
        div()
            .id("audit-export-trigger")
            .relative()
            .flex()
            .items_center()
            .gap_1()
            .px(Spacing::XS)
            .rounded(Radii::SM)
            .text_size(FontSizes::XS)
            .cursor_pointer()
            .text_color(theme.muted_foreground)
            .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
            .on_click(cx.listener(|this, _, _, cx| {
                this.toggle_export_menu(cx);
            }))
            .child(
                svg()
                    .path(AppIcon::FileSpreadsheet.path())
                    .size_4()
                    .text_color(theme.muted_foreground),
            )
            .child("Export")
            .child(
                svg()
                    .path(AppIcon::ChevronDown.path())
                    .size_3()
                    .text_color(theme.muted_foreground),
            )
            .when(menu_open, |trigger| {
                trigger.child(self.render_export_menu(theme, cx))
            })
    }

    fn render_export_menu(
        &self,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let items = [("CSV", "csv"), ("JSON", "json")]
            .into_iter()
            .enumerate()
            .map(|(index, (label, format))| {
                // Identical to DataGridPanel::render_export_menu items.
                div()
                    .id(SharedString::from(format!("audit-export-{}", index)))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.export_with_format(format, cx);
                    }))
                    .child(label)
                    .into_any_element()
            })
            .collect::<Vec<_>>();

        // Identical to DataGridPanel::render_export_menu container.
        deferred(
            div()
                .absolute()
                .bottom_full()
                .right_0()
                .mb(Spacing::XS)
                .w(px(160.0))
                .bg(theme.popover)
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::MD)
                .shadow_lg()
                .py(Spacing::XS)
                .occlude()
                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                    cx.stop_propagation();
                })
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.export_menu_open = false;
                    cx.notify();
                }))
                .children(items),
        )
        .with_priority(1)
    }

    fn render_status_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let can_prev = self.can_go_prev();
        let can_next = self.can_go_next();

        // Left: row count with icon — same as DataGridPanel.
        let left = {
            let row_count_label = if let Some((start, end)) = self.current_page_range() {
                format!("{}-{} of {} rows", start, end, self.total_events)
            } else {
                format!("{} rows", self.total_events)
            };

            div()
                .flex()
                .items_center()
                .gap_1()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(
                    svg()
                        .path(AppIcon::Rows3.path())
                        .size_3()
                        .text_color(theme.muted_foreground),
                )
                .child(row_count_label)
        };

        // Center: pagination — identical to DataGridPanel.
        let center = div().flex().items_center().gap(Spacing::SM).when_some(
            self.total_pages(),
            |pagination, total_pages| {
                let page = self.pagination.current_page();
                let offset = self.pagination.offset();
                let start = offset + 1;
                let end = offset + self.events.len() as u64;

                pagination
                    .child(
                        div()
                            .id("audit-prev-page")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::XS)
                            .rounded(Radii::SM)
                            .text_size(FontSizes::XS)
                            .when(can_prev, |d| {
                                d.cursor_pointer()
                                    .text_color(theme.foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.go_to_prev_page(cx);
                                    }))
                            })
                            .when(!can_prev, |d| {
                                d.text_color(theme.muted_foreground).opacity(0.5)
                            })
                            .child(svg().path(AppIcon::ChevronLeft.path()).size_3().text_color(
                                if can_prev {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child("Prev"),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(if total_pages > 1 {
                                format!("Page {}/{} ({}-{})", page, total_pages, start, end)
                            } else {
                                format!("Page {}/{}", page, total_pages)
                            }),
                    )
                    .child(
                        div()
                            .id("audit-next-page")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::XS)
                            .rounded(Radii::SM)
                            .text_size(FontSizes::XS)
                            .when(can_next, |d| {
                                d.cursor_pointer()
                                    .text_color(theme.foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.go_to_next_page(cx);
                                    }))
                            })
                            .when(!can_next, |d| {
                                d.text_color(theme.muted_foreground).opacity(0.5)
                            })
                            .child("Next")
                            .child(
                                svg()
                                    .path(AppIcon::ChevronRight.path())
                                    .size_3()
                                    .text_color(if can_next {
                                        theme.foreground
                                    } else {
                                        theme.muted_foreground
                                    }),
                            ),
                    )
            },
        );

        // Right: export + loading indicator — same as DataGridPanel.
        let right = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .when(self.total_events > 0, |d| {
                d.child(self.render_export_button(&theme, cx))
            })
            .when_some(
                self.status_message.clone().filter(|_| self.is_loading),
                |d, _| {
                    d.child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground.opacity(0.5))
                            .child("Loading..."),
                    )
                },
            );

        workspace_footer_bar(&theme, left, center, right)
    }
}

impl Focusable for AuditDocument {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        cx.focus_handle()
    }
}

impl EventEmitter<AuditDocumentEvent> for AuditDocument {}

impl Render for AuditDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_initial_load {
            self.pending_initial_load = false;
            self.load_events(cx);
        }

        flush_pending_toast(self.pending_toast.take(), window, cx);

        let theme = cx.theme().clone();

        div()
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .bg(theme.background)
            .child(self.render_toolbar(window, cx))
            .child(
                div()
                    .flex_1()
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .child(self.render_event_list(cx))
                    .child(self.render_status_bar(cx)),
            )
    }
}
