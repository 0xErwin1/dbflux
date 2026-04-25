//! Audit document module.

mod filters;
mod source_adapter;

pub use filters::{AuditFilters, TimeRange};
pub use source_adapter::AuditSourceAdapter;

use std::collections::{HashMap, HashSet};

use crate::app::AppStateEntity;
use crate::keymap::{Command, ContextId};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::components::filter_bar::{FilterBarItem, FilterBarMode, FilterBarState};
use crate::ui::components::multi_select::{MultiSelect, MultiSelectChanged};
use crate::ui::components::toast::{PendingToast, flush_pending_toast};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_components::controls::{GpuiInput as Input, InputEvent, InputState};
use dbflux_components::primitives::{Icon, Label, Text, surface_raised};
use dbflux_core::{
    CollectionBrowseRequest, CollectionRef, Pagination, RefreshPolicy, Value,
    observability::{EventCategory, EventOutcome, EventSeverity},
};
use dbflux_storage::repositories::audit::{AuditEventDto, AuditQueryFilter};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::scroll::ScrollableElement;
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use super::chrome::{compact_top_bar, workspace_footer_bar};
use super::types::{DocumentIcon, DocumentId, DocumentKind, DocumentState};

// ── Context menu ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditContextMenuAction {
    CopyRowAsCsv,
    CopySummary,
    FilterByCorrelation,
}

#[derive(Debug, Clone)]
struct AuditContextMenuState {
    /// Index into `AuditDocument::events` that the menu targets.
    row: usize,
    /// Index of the highlighted item in the menu (for keyboard nav).
    selected_index: usize,
    /// Screen position where the menu should appear.
    ///
    /// - Right-click: the actual mouse position from `event.position`.
    /// - Keyboard (m): approximated from `row * row_height`.
    position: Point<Pixels>,
}

/// Flat list of context menu items.  Separators carry `action: None`.
#[derive(Clone, Copy)]
struct AuditMenuItem {
    label: &'static str,
    action: Option<AuditContextMenuAction>,
    icon: Option<AppIcon>,
}

impl AuditMenuItem {
    const fn item(label: &'static str, action: AuditContextMenuAction, icon: AppIcon) -> Self {
        Self {
            label,
            action: Some(action),
            icon: Some(icon),
        }
    }

    const fn separator() -> Self {
        Self {
            label: "",
            action: None,
            icon: None,
        }
    }

    fn is_separator(self) -> bool {
        self.action.is_none()
    }
}

/// Events emitted by AuditDocument.
#[derive(Clone, Debug)]
pub enum AuditDocumentEvent {
    Refresh,
    /// The document was interacted with and wants workspace focus.
    RequestFocus,
}

const DEFAULT_PAGE_SIZE: u32 = 100;

#[derive(Clone)]
enum AuditDocumentSource {
    Internal {
        adapter: AuditSourceAdapter,
    },
    CloudWatchLogGroup {
        profile_id: Uuid,
        collection: CollectionRef,
    },
    CloudWatchLogStream {
        profile_id: Uuid,
        collection: CollectionRef,
        log_stream: String,
    },
}

#[derive(Clone, Copy)]
enum ToolbarSlot {
    Search,
    Time,
    Level,
    Category,
    Outcome,
    Refresh,
    RefreshPolicy,
    Clear,
}

struct CloudWatchLoadResult {
    events: Vec<AuditEventDto>,
    total_events: u64,
}

/// Audit event viewer document.
pub struct AuditDocument {
    app_state: Entity<AppStateEntity>,
    source: AuditDocumentSource,
    filters: AuditFilters,
    events: Vec<AuditEventDto>,
    total_events: u64,
    expanded_event_ids: HashSet<i64>,
    cloudwatch_message_inputs: HashMap<i64, Entity<InputState>>,
    cloudwatch_details_inputs: HashMap<i64, Entity<InputState>>,
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
    multi_select_level: Entity<MultiSelect>,
    multi_select_category: Entity<MultiSelect>,
    multi_select_outcome: Entity<MultiSelect>,
    refresh_policy: RefreshPolicy,
    refresh_dropdown: Entity<Dropdown>,
    load_request_id: u64,
    _refresh_timer: Option<Task<()>>,
    _subscriptions: Vec<Subscription>,

    suppress_load: bool,

    // ── Keyboard navigation state ─────────────────────────────────────────
    focus_handle: FocusHandle,
    /// Currently highlighted row (0-based index into `events`).
    selected_row: Option<usize>,
    /// Open context menu, if any.
    context_menu: Option<AuditContextMenuState>,
    /// Toolbar focus-ring navigation (search input is item 0).
    filter_bar: FilterBarState,
    /// Absolute position of the document panel's top-left corner in window
    /// coordinates. Updated each frame via a canvas element, identical to
    /// `DataGridPanel::panel_origin`. Used to convert `event.position`
    /// (window-absolute) to panel-local coordinates for context menu placement.
    panel_origin: Point<Pixels>,
    /// Whether this document or any of its children currently hold GPUI focus.
    /// Updated in `Render` before rows are rendered, so row highlights are
    /// suppressed when focus moves to the sidebar or another panel.
    has_focus: bool,
}

impl AuditDocument {
    /// Creates a new audit document.
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let audit_repo = app_state.read(cx).storage_runtime().audit();

        Self::new_with_source(
            app_state,
            AuditDocumentSource::Internal {
                adapter: AuditSourceAdapter::new(audit_repo),
            },
            "Audit".to_string(),
            "Search events...",
            window,
            cx,
        )
    }

    pub fn new_for_cloudwatch_log_group(
        profile_id: Uuid,
        collection: CollectionRef,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let title = collection.name.clone();

        Self::new_with_source(
            app_state,
            AuditDocumentSource::CloudWatchLogGroup {
                profile_id,
                collection,
            },
            title,
            "Filter pattern...",
            window,
            cx,
        )
    }

    pub fn new_for_cloudwatch_log_stream(
        profile_id: Uuid,
        collection: CollectionRef,
        log_stream: String,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let title = format!("{} · {}", collection.name, log_stream);

        Self::new_with_source(
            app_state,
            AuditDocumentSource::CloudWatchLogStream {
                profile_id,
                collection,
                log_stream,
            },
            title,
            "Filter pattern...",
            window,
            cx,
        )
    }

    fn new_with_source(
        app_state: Entity<AppStateEntity>,
        source: AuditDocumentSource,
        title: String,
        search_placeholder: &'static str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let focus_handle = cx.focus_handle();

        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder(search_placeholder));

        let initial_time_range = Self::initial_time_range(&source);
        let time_range_placeholder =
            if matches!(source, AuditDocumentSource::CloudWatchLogStream { .. }) {
                "All time"
            } else {
                "Last 24 h"
            };

        let dropdown_time_range = cx.new(|_cx| {
            Dropdown::new("audit-time-range")
                .placeholder(time_range_placeholder)
                .items(Self::time_range_items())
                .selected_index(initial_time_range)
        });

        let multi_select_level = cx.new(|cx| {
            let items: Vec<DropdownItem> = Self::level_items();
            let mut ms = MultiSelect::new("audit-level").placeholder("Level");
            ms.set_items(items, cx);
            ms
        });

        let multi_select_category = cx.new(|cx| {
            let items: Vec<DropdownItem> = Self::category_items();
            let mut ms = MultiSelect::new("audit-category").placeholder("Category");
            ms.set_items(items, cx);
            ms
        });

        let multi_select_outcome = cx.new(|cx| {
            let items: Vec<DropdownItem> = Self::outcome_items();
            let mut ms = MultiSelect::new("audit-outcome").placeholder("Outcome");
            ms.set_items(items, cx);
            ms
        });

        let search_sub = cx.subscribe(&search_input, |this, _, event: &InputEvent, cx| {
            match event {
                InputEvent::PressEnter { secondary: false } => {
                    this.handle_search_submit(cx);
                }
                // When the input loses focus (e.g. user presses Escape inside the input),
                // transition the filter bar from Editing back to Navigating so the focus
                // ring stays visible and the user can press Escape again to exit the toolbar.
                InputEvent::Blur => {
                    this.filter_bar.exit_editing();
                    cx.notify();
                }
                _ => {}
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
            &multi_select_level,
            |this, entity, _event: &MultiSelectChanged, cx| {
                if this.suppress_load {
                    return;
                }

                let levels: Vec<EventSeverity> = entity
                    .read(cx)
                    .selected_values()
                    .iter()
                    .filter_map(|v| EventSeverity::from_str_repr(v.as_ref()))
                    .collect();

                if levels.is_empty() {
                    this.filters.levels = None;
                    this.filters.level = None;
                } else {
                    this.filters.levels = Some(levels);
                    this.filters.level = None;
                }

                this.reset_pagination();
                this.load_events(cx);
            },
        );

        let category_sub = cx.subscribe(
            &multi_select_category,
            |this, entity, _event: &MultiSelectChanged, cx| {
                if this.suppress_load {
                    return;
                }

                let categories: Vec<EventCategory> = entity
                    .read(cx)
                    .selected_values()
                    .iter()
                    .filter_map(|v| Self::category_for_value(v.as_ref()))
                    .collect();

                if categories.is_empty() {
                    this.filters.categories = None;
                    this.filters.category = None;
                } else {
                    this.filters.categories = Some(categories);
                    this.filters.category = None;
                }

                this.reset_pagination();
                this.load_events(cx);
            },
        );

        let outcome_sub = cx.subscribe(
            &multi_select_outcome,
            |this, _entity, event: &MultiSelectChanged, cx| {
                if this.suppress_load {
                    return;
                }

                let outcomes: Vec<EventOutcome> = event
                    .selected_values
                    .iter()
                    .filter_map(|v| EventOutcome::from_str_repr(v.as_ref()))
                    .collect();

                if outcomes.is_empty() {
                    this.filters.outcomes = None;
                    this.filters.outcome = None;
                } else {
                    this.filters.outcomes = Some(outcomes);
                    this.filters.outcome = None;
                }

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

        let mut toolbar_items = vec![
            FilterBarItem::input("Search:", search_input.clone()),
            FilterBarItem::dropdown("Time:", dropdown_time_range.clone()),
        ];

        if matches!(source, AuditDocumentSource::Internal { .. }) {
            toolbar_items.extend([
                FilterBarItem::button("Level"),
                FilterBarItem::button("Category"),
                FilterBarItem::button("Outcome"),
            ]);
        }

        toolbar_items.extend([
            FilterBarItem::button_with_icon("Refresh", AppIcon::RefreshCcw),
            FilterBarItem::dropdown("Auto-refresh:", refresh_dropdown.clone()),
            FilterBarItem::button("Clear"),
        ]);

        let filter_bar = FilterBarState::new(toolbar_items);

        let filters = Self::default_filters_for_source(&source);

        Self {
            app_state,
            source,
            filters,
            events: Vec::new(),
            total_events: 0,
            expanded_event_ids: HashSet::new(),
            cloudwatch_message_inputs: HashMap::new(),
            cloudwatch_details_inputs: HashMap::new(),
            pagination: Pagination::Offset {
                limit: DEFAULT_PAGE_SIZE,
                offset: 0,
            },
            status_message: None,
            is_loading: false,
            id: DocumentId::new(),
            title,
            pending_initial_load: true,
            pending_toast: None,
            export_menu_open: false,
            search_input,
            dropdown_time_range,
            multi_select_level,
            multi_select_category,
            multi_select_outcome,
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
            suppress_load: false,
            focus_handle,
            selected_row: None,
            context_menu: None,
            filter_bar,
            panel_origin: Point::default(),
            has_focus: false,
        }
    }

    /// Creates a new audit document with a category pre-filter applied.
    ///
    /// This is the entry point for opening the audit viewer focused on a specific
    /// category (e.g., MCP events from the governance panel). The dropdown is synced
    /// to reflect the pre-selected category.
    pub fn new_with_category(
        category: EventCategory,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut doc = Self::new(app_state, window, cx);

        doc.set_category_filter(Some(category), cx);
        doc.pending_initial_load = false;

        doc
    }

    pub fn is_cloudwatch_log_group(&self, profile_id: Uuid, collection: &CollectionRef) -> bool {
        match &self.source {
            AuditDocumentSource::CloudWatchLogGroup {
                profile_id: doc_profile_id,
                collection: doc_collection,
            } => *doc_profile_id == profile_id && doc_collection == collection,
            AuditDocumentSource::Internal { .. } => false,
            AuditDocumentSource::CloudWatchLogStream { .. } => false,
        }
    }

    pub fn is_cloudwatch_log_stream(
        &self,
        profile_id: Uuid,
        collection: &CollectionRef,
        log_stream: &str,
    ) -> bool {
        match &self.source {
            AuditDocumentSource::CloudWatchLogStream {
                profile_id: doc_profile_id,
                collection: doc_collection,
                log_stream: doc_log_stream,
            } => {
                *doc_profile_id == profile_id
                    && doc_collection == collection
                    && doc_log_stream == log_stream
            }
            AuditDocumentSource::Internal { .. }
            | AuditDocumentSource::CloudWatchLogGroup { .. } => false,
        }
    }

    fn is_cloudwatch_source(&self) -> bool {
        matches!(
            self.source,
            AuditDocumentSource::CloudWatchLogGroup { .. }
                | AuditDocumentSource::CloudWatchLogStream { .. }
        )
    }

    fn toolbar_index(&self, slot: ToolbarSlot) -> Option<usize> {
        match (&self.source, slot) {
            (_, ToolbarSlot::Search) => Some(0),
            (_, ToolbarSlot::Time) => Some(1),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Level) => Some(2),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Category) => Some(3),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Outcome) => Some(4),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Refresh) => Some(5),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::RefreshPolicy) => Some(6),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Clear) => Some(7),
            (AuditDocumentSource::CloudWatchLogGroup { .. }, ToolbarSlot::Refresh)
            | (AuditDocumentSource::CloudWatchLogStream { .. }, ToolbarSlot::Refresh) => Some(2),
            (AuditDocumentSource::CloudWatchLogGroup { .. }, ToolbarSlot::RefreshPolicy)
            | (AuditDocumentSource::CloudWatchLogStream { .. }, ToolbarSlot::RefreshPolicy) => {
                Some(3)
            }
            (AuditDocumentSource::CloudWatchLogGroup { .. }, ToolbarSlot::Clear)
            | (AuditDocumentSource::CloudWatchLogStream { .. }, ToolbarSlot::Clear) => Some(4),
            _ => None,
        }
    }

    fn slot_has_ring(&self, slot: ToolbarSlot) -> bool {
        self.filter_bar.mode() == FilterBarMode::Navigating
            && self.toolbar_index(slot) == Some(self.filter_bar.focused_index())
    }

    pub fn set_category_filter(&mut self, category: Option<EventCategory>, cx: &mut Context<Self>) {
        match category {
            Some(cat) => {
                let value = cat.as_str().to_string();
                self.multi_select_category.update(cx, |ms, cx| {
                    ms.set_selected_values(&[value], cx);
                });
                self.filters.categories = Some(vec![cat]);
                self.filters.category = None;
            }
            None => {
                self.suppress_load = true;
                self.multi_select_category
                    .update(cx, |ms, cx| ms.clear_selection(cx));
                self.suppress_load = false;
                self.filters.categories = None;
                self.filters.category = None;
            }
        }

        self.reset_pagination();
        self.load_events(cx);
    }

    pub fn category_filter(&self) -> Option<EventCategory> {
        self.filters
            .categories
            .as_ref()
            .and_then(|cats| cats.first().copied())
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

    fn initial_time_range(source: &AuditDocumentSource) -> Option<usize> {
        match source {
            AuditDocumentSource::CloudWatchLogStream { .. } => None,
            AuditDocumentSource::Internal { .. }
            | AuditDocumentSource::CloudWatchLogGroup { .. } => Some(2),
        }
    }

    fn default_filters_for_source(source: &AuditDocumentSource) -> AuditFilters {
        let mut filters = AuditFilters::default();

        if !matches!(source, AuditDocumentSource::CloudWatchLogStream { .. }) {
            let (start_ms, end_ms) = TimeRange::Last24h.to_filter_values();
            filters.start_ms = start_ms;
            filters.end_ms = end_ms;
        }

        filters
    }

    fn source_loading_label(&self) -> &'static str {
        if self.is_cloudwatch_source() {
            "Loading log events..."
        } else {
            "Loading audit events..."
        }
    }

    fn source_error_heading(&self) -> &'static str {
        if self.is_cloudwatch_source() {
            "Failed to load log events"
        } else {
            "Failed to load audit events"
        }
    }

    fn source_empty_label(&self) -> &'static str {
        if self.is_cloudwatch_source() {
            "No log events match the current filters."
        } else {
            "No audit events match the current filters."
        }
    }

    fn source_row_label(&self) -> &'static str {
        if self.is_cloudwatch_source() {
            "events"
        } else {
            "rows"
        }
    }

    fn cloudwatch_browse_request(
        collection: CollectionRef,
        filter_pattern: Option<String>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        pagination: Pagination,
    ) -> CollectionBrowseRequest {
        let mut request = CollectionBrowseRequest::new(collection).with_pagination(pagination);

        let mut filter = serde_json::Map::new();

        if let Some(pattern) = filter_pattern.filter(|value| !value.trim().is_empty()) {
            filter.insert("filter_pattern".to_string(), JsonValue::String(pattern));
        }

        if let Some(start_ms) = start_ms {
            filter.insert("start_ms".to_string(), json!(start_ms));
        }

        if let Some(end_ms) = end_ms {
            filter.insert("end_ms".to_string(), json!(end_ms));
        }

        if !filter.is_empty() {
            request = request.with_filter(JsonValue::Object(filter));
        }

        request
    }

    fn cloudwatch_stream_browse_request(
        collection: CollectionRef,
        log_stream: String,
        filter_pattern: Option<String>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        pagination: Pagination,
    ) -> CollectionBrowseRequest {
        let mut request = CollectionBrowseRequest::new(collection).with_pagination(pagination);

        let mut filter = serde_json::Map::new();

        if let Some(pattern) = filter_pattern.filter(|value| !value.trim().is_empty()) {
            filter.insert("filter_pattern".to_string(), JsonValue::String(pattern));
        }

        if let Some(start_ms) = start_ms {
            filter.insert("start_ms".to_string(), json!(start_ms));
        }

        if let Some(end_ms) = end_ms {
            filter.insert("end_ms".to_string(), json!(end_ms));
        }

        filter.insert("log_stream_names".to_string(), json!([log_stream]));
        filter.insert("most_recent".to_string(), JsonValue::Bool(true));

        request = request.with_filter(JsonValue::Object(filter));
        request
    }

    fn cloudwatch_result_to_page(
        result: dbflux_core::QueryResult,
        collection: CollectionRef,
        pagination_offset: u64,
    ) -> CloudWatchLoadResult {
        let has_next_page = result.next_page_token.is_some();
        let events = result
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                Self::cloudwatch_row_to_event(row, &collection, pagination_offset, index)
            })
            .collect::<Vec<_>>();
        let total_events = pagination_offset + events.len() as u64 + u64::from(has_next_page);

        CloudWatchLoadResult {
            events,
            total_events,
        }
    }

    fn cloudwatch_row_to_event(
        row: &[Value],
        collection: &CollectionRef,
        pagination_offset: u64,
        row_index: usize,
    ) -> AuditEventDto {
        let timestamp_ms = Self::int_cell(row, 0).unwrap_or_default();
        let ingestion_time_ms = Self::int_cell(row, 1);
        let log_stream_name = Self::text_cell(row, 2);
        let message = Self::text_cell(row, 3);
        let event_id = Self::text_cell(row, 4);
        let message_details = Self::cloudwatch_message_details(message.as_deref());

        let details_json = json!({
            "log_group": collection.name,
            "timestamp_ms": timestamp_ms,
            "ingestion_time_ms": ingestion_time_ms,
            "log_stream_name": log_stream_name,
            "message": message_details,
            "event_id": event_id,
        })
        .to_string();

        AuditEventDto {
            id: (pagination_offset + row_index as u64 + 1) as i64,
            actor_id: String::new(),
            tool_id: "cloudwatch_log_event".to_string(),
            decision: String::new(),
            reason: None,
            profile_id: None,
            classification: None,
            duration_ms: None,
            created_at: timestamp_ms.to_string(),
            created_at_epoch_ms: timestamp_ms,
            level: None,
            category: None,
            action: log_stream_name,
            outcome: None,
            actor_type: None,
            source_id: None,
            summary: message,
            connection_id: Some(collection.name.clone()),
            database_name: Some(collection.database.clone()),
            driver_id: Some("cloudwatch".to_string()),
            object_type: Some("log_event".to_string()),
            object_id: event_id,
            details_json: Some(details_json),
            error_code: None,
            error_message: ingestion_time_ms.map(|value| value.to_string()),
            session_id: None,
            correlation_id: None,
        }
    }

    fn int_cell(row: &[Value], index: usize) -> Option<i64> {
        match row.get(index) {
            Some(Value::Int(value)) => Some(*value),
            Some(Value::Text(value)) => value.parse().ok(),
            _ => None,
        }
    }

    fn text_cell(row: &[Value], index: usize) -> Option<String> {
        match row.get(index) {
            Some(Value::Text(value)) if !value.is_empty() => Some(value.clone()),
            Some(Value::Int(value)) => Some(value.to_string()),
            Some(Value::Null) | None => None,
            Some(value) => Some(value.to_string()),
        }
    }

    fn active_filter(&self, limit: Option<usize>, offset: Option<usize>) -> AuditQueryFilter {
        let level_str = self
            .filters
            .level
            .as_ref()
            .map(|level| level.as_str().to_string());
        let levels_str = self.filters.levels.as_ref().map(|levels| {
            levels
                .iter()
                .map(|level| level.as_str().to_string())
                .collect()
        });
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
            levels: levels_str,
            category: category_str,
            action: None,
            categories: categories_str,
            source_id: source_str,
            outcome: self
                .filters
                .outcome
                .as_ref()
                .map(|outcome| outcome.as_str().to_string()),
            outcomes: self.filters.outcomes.as_ref().map(|outcomes| {
                outcomes
                    .iter()
                    .map(|outcome| outcome.as_str().to_string())
                    .collect()
            }),
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
        self.status_message = Some(self.source_loading_label().to_string());
        cx.notify();

        let page_filter = self.active_filter(
            Some(self.pagination_limit()),
            Some(self.pagination_offset()),
        );
        let count_filter = self.active_filter(None, None);

        let task = match &self.source {
            AuditDocumentSource::Internal { adapter } => {
                let adapter = adapter.clone();

                cx.background_executor().spawn(async move {
                    let events = adapter.query_filter(&page_filter)?;
                    let total = adapter.count_filter(&count_filter)?;

                    Ok::<_, String>(CloudWatchLoadResult {
                        events,
                        total_events: total,
                    })
                })
            }
            AuditDocumentSource::CloudWatchLogGroup {
                profile_id,
                collection,
            } => {
                let Some(connection) = self
                    .app_state
                    .read(cx)
                    .connections()
                    .get(profile_id)
                    .map(|connected| connected.connection.clone())
                else {
                    self.events.clear();
                    self.total_events = 0;
                    self.expanded_event_ids.clear();
                    self.is_loading = false;
                    self.status_message =
                        Some("Connection not found for this log group".to_string());
                    cx.notify();
                    return;
                };

                let browse_request = Self::cloudwatch_browse_request(
                    collection.clone(),
                    self.filters.free_text.clone(),
                    self.filters.start_ms,
                    self.filters.end_ms,
                    self.pagination.clone(),
                );
                let collection_for_task = collection.clone();
                let pagination_offset = self.pagination.offset();

                cx.background_executor().spawn(async move {
                    let result = connection
                        .browse_collection(&browse_request)
                        .map_err(|error| format!("cloudwatch browse failed: {error}"))?;

                    Ok::<_, String>(Self::cloudwatch_result_to_page(
                        result,
                        collection_for_task,
                        pagination_offset,
                    ))
                })
            }
            AuditDocumentSource::CloudWatchLogStream {
                profile_id,
                collection,
                log_stream,
            } => {
                let Some(connection) = self
                    .app_state
                    .read(cx)
                    .connections()
                    .get(profile_id)
                    .map(|connected| connected.connection.clone())
                else {
                    self.events.clear();
                    self.total_events = 0;
                    self.expanded_event_ids.clear();
                    self.is_loading = false;
                    self.status_message =
                        Some("Connection not found for this log stream".to_string());
                    cx.notify();
                    return;
                };

                let browse_request = Self::cloudwatch_stream_browse_request(
                    collection.clone(),
                    log_stream.clone(),
                    self.filters.free_text.clone(),
                    self.filters.start_ms,
                    self.filters.end_ms,
                    self.pagination.clone(),
                );
                let collection_for_task = collection.clone();
                let pagination_offset = self.pagination.offset();

                cx.background_executor().spawn(async move {
                    let result = connection
                        .browse_collection(&browse_request)
                        .map_err(|error| format!("cloudwatch stream browse failed: {error}"))?;

                    Ok::<_, String>(Self::cloudwatch_result_to_page(
                        result,
                        collection_for_task,
                        pagination_offset,
                    ))
                })
            }
        };

        cx.spawn(async move |this, cx| match task.await {
            Ok(page) => {
                let _ = cx.update(|cx| {
                    this.update(cx, |doc, cx| {
                        if doc.load_request_id != request_id {
                            return;
                        }

                        let visible_ids: HashSet<i64> =
                            page.events.iter().map(|event| event.id).collect();

                        doc.events = page.events;
                        doc.total_events = page.total_events;
                        doc.is_loading = false;
                        doc.status_message = None;
                        doc.expanded_event_ids
                            .retain(|event_id| visible_ids.contains(event_id));
                        doc.retain_cloudwatch_inline_inputs(&visible_ids);

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
                        doc.clear_cloudwatch_inline_inputs();
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
        self.filters = Self::default_filters_for_source(&self.source);
        self.reset_pagination();
        self.export_menu_open = false;

        self.suppress_load = true;
        self.dropdown_time_range
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(2), cx));
        self.multi_select_level
            .update(cx, |ms, cx| ms.clear_selection(cx));
        self.multi_select_category
            .update(cx, |ms, cx| ms.clear_selection(cx));
        self.multi_select_outcome
            .update(cx, |ms, cx| ms.clear_selection(cx));
        self.search_input
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.suppress_load = false;

        self.load_events(cx);
    }

    fn toggle_event_expanded(&mut self, event_id: i64, cx: &mut Context<Self>) {
        if !self.expanded_event_ids.insert(event_id) {
            self.expanded_event_ids.remove(&event_id);
        }

        cx.notify();
    }

    fn retain_cloudwatch_inline_inputs(&mut self, visible_ids: &HashSet<i64>) {
        Self::retain_cloudwatch_input_cache(&mut self.cloudwatch_message_inputs, visible_ids);
        Self::retain_cloudwatch_input_cache(&mut self.cloudwatch_details_inputs, visible_ids);
    }

    fn retain_cloudwatch_input_cache<T>(cache: &mut HashMap<i64, T>, visible_ids: &HashSet<i64>) {
        cache.retain(|event_id, _| visible_ids.contains(event_id));
    }

    fn clear_cloudwatch_inline_inputs(&mut self) {
        self.cloudwatch_message_inputs.clear();
        self.cloudwatch_details_inputs.clear();
    }

    fn ensure_cloudwatch_message_input(
        &mut self,
        event_id: i64,
        message: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<InputState> {
        Self::ensure_cloudwatch_text_input(
            &mut self.cloudwatch_message_inputs,
            event_id,
            message,
            None,
            window,
            cx,
        )
    }

    fn ensure_cloudwatch_details_input(
        &mut self,
        event_id: i64,
        details_json: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<InputState> {
        Self::ensure_cloudwatch_text_input(
            &mut self.cloudwatch_details_inputs,
            event_id,
            details_json,
            Some("json"),
            window,
            cx,
        )
    }

    fn ensure_cloudwatch_text_input(
        cache: &mut HashMap<i64, Entity<InputState>>,
        event_id: i64,
        value: &str,
        editor_mode: Option<&'static str>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<InputState> {
        let value = value.to_string();
        let rows = if editor_mode.is_some() {
            Self::cloudwatch_text_rows(&value, 4)
        } else {
            Self::cloudwatch_text_rows(&value, 2)
        };

        let input = cache
            .entry(event_id)
            .or_insert_with(|| {
                let initial_value = value.clone();
                let initial_rows = rows;

                cx.new(|cx| {
                    let mut state = if let Some(editor_mode) = editor_mode {
                        InputState::new(window, cx)
                            .code_editor(editor_mode)
                            .line_number(false)
                            .rows(initial_rows)
                            .soft_wrap(true)
                    } else {
                        InputState::new(window, cx)
                            .multi_line(true)
                            .rows(initial_rows)
                            .soft_wrap(true)
                    };

                    state.set_value(&initial_value, window, cx);
                    state
                })
            })
            .clone();

        if input.read(cx).value().to_string() != value {
            input.update(cx, |state, cx| state.set_value(value, window, cx));
        }

        input
    }

    fn cloudwatch_text_rows(value: &str, min_rows: usize) -> usize {
        let line_rows = value.lines().count().max(1);
        let wrap_rows = value
            .lines()
            .map(|line| (line.chars().count() / 120).saturating_add(1))
            .sum::<usize>()
            .max(1);

        line_rows.max(wrap_rows).max(min_rows)
    }

    fn cloudwatch_text_height(rows: usize) -> Pixels {
        px((rows as f32 * 22.0) + 16.0)
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
            DropdownItem::with_value("Error", "error"),
            DropdownItem::with_value("Warn", "warn"),
            DropdownItem::with_value("Info", "info"),
        ]
    }

    #[allow(dead_code)]
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
            DropdownItem::with_value("Config", "config"),
            DropdownItem::with_value("Connection", "connection"),
            DropdownItem::with_value("Query", "query"),
            DropdownItem::with_value("Hook", "hook"),
            DropdownItem::with_value("Script", "script"),
            DropdownItem::with_value("System", "system"),
            DropdownItem::with_value("MCP", "mcp"),
            DropdownItem::with_value("Governance", "governance"),
        ]
    }

    #[allow(dead_code)]
    fn category_index(category: Option<EventCategory>) -> usize {
        match category {
            Some(EventCategory::Config) => 1,
            Some(EventCategory::Connection) => 2,
            Some(EventCategory::Query) => 3,
            Some(EventCategory::Hook) => 4,
            Some(EventCategory::Script) => 5,
            Some(EventCategory::System) => 6,
            Some(EventCategory::Mcp) => 7,
            Some(EventCategory::Governance) => 8,
            None => 0,
        }
    }

    #[allow(dead_code)]
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

    /// Maps a category string value (as stored in DropdownItem.value) to EventCategory.
    fn category_for_value(value: &str) -> Option<EventCategory> {
        EventCategory::from_str_repr(value)
    }

    fn outcome_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::with_value("Success", "success"),
            DropdownItem::with_value("Failure", "failure"),
            DropdownItem::with_value("Cancelled", "cancelled"),
        ]
    }

    #[allow(dead_code)]
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

    /// Renders a null placeholder matching the DataTable convention: italic muted "NULL".
    fn null_display(_theme: &gpui_component::Theme) -> Div {
        div()
            .italic()
            .child(Text::caption("NULL").muted_foreground())
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
            _ => "NULL",
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

    fn cloudwatch_message_details(message: Option<&str>) -> serde_json::Value {
        message
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .unwrap_or_else(|| json!(message))
    }

    fn filter_by_correlation(&mut self, correlation_id: String, cx: &mut Context<Self>) {
        self.filters.correlation_id = Some(correlation_id);
        self.reset_pagination();
        self.load_events(cx);
    }

    // ── Focus ─────────────────────────────────────────────────────────────

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    /// Execute the button action for the currently focused FilterBar item.
    /// Only called when `activate_input` returned `false` (Button variant).
    fn execute_filter_bar_button(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let focused_index = self.filter_bar.focused_index();

        if self.toolbar_index(ToolbarSlot::Refresh) == Some(focused_index) {
            self.refresh(cx);
            self.filter_bar.deactivate();
            self.focus_handle.focus(window);
        } else if self.toolbar_index(ToolbarSlot::Clear) == Some(focused_index) {
            self.clear_filters(window, cx);
            self.filter_bar.deactivate();
            self.focus_handle.focus(window);
        } else if self.toolbar_index(ToolbarSlot::Level) == Some(focused_index) {
            self.multi_select_level
                .update(cx, |ms, cx| ms.toggle_open(cx));
        } else if self.toolbar_index(ToolbarSlot::Category) == Some(focused_index) {
            self.multi_select_category
                .update(cx, |ms, cx| ms.toggle_open(cx));
        } else if self.toolbar_index(ToolbarSlot::Outcome) == Some(focused_index) {
            self.multi_select_outcome
                .update(cx, |ms, cx| ms.toggle_open(cx));
        }
    }

    /// Returns the active `ContextId` for keyboard dispatch.
    ///
    /// Priority (highest first):
    /// - `ContextMenu` — while the context menu is open
    /// - `TextInput`   — while the search input has keyboard focus (Editing)
    /// - `Audit`       — row list or toolbar focus-ring navigation
    pub fn active_context(&self) -> ContextId {
        if self.context_menu.is_some() {
            return ContextId::ContextMenu;
        }

        if self.filter_bar.is_editing() {
            return ContextId::TextInput;
        }

        ContextId::Audit
    }

    // ── Row cursor navigation ─────────────────────────────────────────────

    #[allow(dead_code)]
    fn row_count(&self) -> usize {
        self.events.len()
    }

    fn select_row(&mut self, row: usize, cx: &mut Context<Self>) {
        if self.events.is_empty() {
            return;
        }

        let row = row.min(self.events.len().saturating_sub(1));
        self.selected_row = Some(row);
        cx.notify();
    }

    fn select_next_row(&mut self, cx: &mut Context<Self>) {
        let next = match self.selected_row {
            None => 0,
            Some(r) => (r + 1).min(self.events.len().saturating_sub(1)),
        };
        self.selected_row = Some(next);
        cx.notify();
    }

    fn select_prev_row(&mut self, cx: &mut Context<Self>) {
        let prev = match self.selected_row {
            None => 0,
            Some(0) => 0,
            Some(r) => r - 1,
        };
        self.selected_row = Some(prev);
        cx.notify();
    }

    fn select_first_row(&mut self, cx: &mut Context<Self>) {
        if !self.events.is_empty() {
            self.selected_row = Some(0);
            cx.notify();
        }
    }

    fn select_last_row(&mut self, cx: &mut Context<Self>) {
        if !self.events.is_empty() {
            self.selected_row = Some(self.events.len() - 1);
            cx.notify();
        }
    }

    /// Jump down by a partial page (same feel as Ctrl+D in Results).
    fn page_down_rows(&mut self, cx: &mut Context<Self>) {
        let step = (DEFAULT_PAGE_SIZE / 4) as usize;
        let next = match self.selected_row {
            None => step.min(self.events.len().saturating_sub(1)),
            Some(r) => (r + step).min(self.events.len().saturating_sub(1)),
        };
        self.selected_row = Some(next);
        cx.notify();
    }

    /// Jump up by a partial page.
    fn page_up_rows(&mut self, cx: &mut Context<Self>) {
        let step = (DEFAULT_PAGE_SIZE / 4) as usize;
        let prev = match self.selected_row {
            None => 0,
            Some(r) => r.saturating_sub(step),
        };
        self.selected_row = Some(prev);
        cx.notify();
    }

    /// Toggle expand/collapse for the selected row (Execute / Space).
    fn toggle_selected_row_expanded(&mut self, cx: &mut Context<Self>) {
        if let Some(row) = self.selected_row
            && let Some(event) = self.events.get(row)
        {
            self.toggle_event_expanded(event.id, cx);
        }
    }

    // ── Context menu ──────────────────────────────────────────────────────

    /// Static menu item table — separators have `action: None`.
    fn context_menu_items(has_correlation: bool) -> Vec<AuditMenuItem> {
        let mut items = vec![
            AuditMenuItem::item(
                "Copy Row as CSV",
                AuditContextMenuAction::CopyRowAsCsv,
                AppIcon::Layers,
            ),
            AuditMenuItem::item(
                "Copy Summary",
                AuditContextMenuAction::CopySummary,
                AppIcon::Layers,
            ),
        ];

        if has_correlation {
            items.push(AuditMenuItem::separator());
            items.push(AuditMenuItem::item(
                "Filter by Correlation",
                AuditContextMenuAction::FilterByCorrelation,
                AppIcon::ListFilter,
            ));
        }

        items
    }

    fn open_context_menu_at_selection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(row) = self.selected_row else {
            return;
        };

        if row >= self.events.len() {
            return;
        }

        // Keyboard-triggered: approximate position from row index.
        const AUDIT_ROW_HEIGHT: f32 = 30.0;
        let y = row as f32 * AUDIT_ROW_HEIGHT + AUDIT_ROW_HEIGHT;
        let position = Point::new(px(8.0), px(y));

        self.context_menu = Some(AuditContextMenuState {
            row,
            selected_index: 0,
            position,
        });
        // Keep focus on the document's own handle so on_key_down continues
        // to receive events while the context menu is open.
        self.focus_handle.focus(window);
        cx.notify();
    }

    fn open_context_menu_at_mouse(
        &mut self,
        row: usize,
        mouse_position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if row >= self.events.len() {
            return;
        }

        self.select_row(row, cx);

        // Convert from window-absolute coordinates to panel-local coordinates,
        // exactly as DataGridPanel does: `menu_x = position.x - panel_origin.x`.
        let local_position = Point::new(
            mouse_position.x - self.panel_origin.x,
            mouse_position.y - self.panel_origin.y,
        );

        self.context_menu = Some(AuditContextMenuState {
            row,
            selected_index: 0,
            position: local_position,
        });
        self.focus_handle.focus(window);
        cx.notify();
    }

    fn close_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.context_menu.is_some() {
            self.context_menu = None;
            self.focus_handle.focus(window);
            cx.notify();
        }
    }

    #[allow(dead_code)]
    fn context_menu_item_count(&self) -> usize {
        let Some(menu) = &self.context_menu else {
            return 0;
        };

        let event = self.events.get(menu.row);
        let has_correlation = event
            .and_then(|e| e.correlation_id.as_deref())
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        Self::context_menu_items(has_correlation)
            .iter()
            .filter(|i| !i.is_separator())
            .count()
    }

    fn navigate_menu_down(&mut self, cx: &mut Context<Self>) {
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let event = self.events.get(menu.row);
        let has_correlation = event
            .and_then(|e| e.correlation_id.as_deref())
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        let items = Self::context_menu_items(has_correlation);
        let navigable: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, i)| !i.is_separator())
            .map(|(idx, _)| idx)
            .collect();

        if navigable.is_empty() {
            return;
        }

        let current_pos = navigable
            .iter()
            .position(|&idx| idx == menu.selected_index)
            .unwrap_or(0);

        let next_pos = (current_pos + 1) % navigable.len();
        menu.selected_index = navigable[next_pos];
        cx.notify();
    }

    fn navigate_menu_up(&mut self, cx: &mut Context<Self>) {
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let event = self.events.get(menu.row);
        let has_correlation = event
            .and_then(|e| e.correlation_id.as_deref())
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        let items = Self::context_menu_items(has_correlation);
        let navigable: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, i)| !i.is_separator())
            .map(|(idx, _)| idx)
            .collect();

        if navigable.is_empty() {
            return;
        }

        let current_pos = navigable
            .iter()
            .position(|&idx| idx == menu.selected_index)
            .unwrap_or(0);

        let prev_pos = if current_pos == 0 {
            navigable.len() - 1
        } else {
            current_pos - 1
        };

        menu.selected_index = navigable[prev_pos];
        cx.notify();
    }

    fn execute_selected_menu_item(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(menu) = self.context_menu.clone() else {
            return;
        };

        let event = self.events.get(menu.row).cloned();
        let has_correlation = event
            .as_ref()
            .and_then(|e| e.correlation_id.as_deref())
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        let items = Self::context_menu_items(has_correlation);
        let Some(item) = items.get(menu.selected_index) else {
            return;
        };

        let Some(action) = item.action else {
            return;
        };

        self.close_context_menu(window, cx);

        match action {
            AuditContextMenuAction::CopyRowAsCsv => {
                if let Some(event) = event {
                    let csv = Self::event_to_csv_row(&event);
                    cx.write_to_clipboard(ClipboardItem::new_string(csv));
                }
            }
            AuditContextMenuAction::CopySummary => {
                if let Some(event) = event {
                    let summary = event.summary.clone().unwrap_or_default();
                    cx.write_to_clipboard(ClipboardItem::new_string(summary));
                }
            }
            AuditContextMenuAction::FilterByCorrelation => {
                if let Some(event) = event
                    && let Some(correlation_id) = event.correlation_id.clone()
                {
                    self.filter_by_correlation(correlation_id, cx);
                }
            }
        }
    }

    /// Dispatch a keyboard command to the document.
    ///
    /// Returns `true` if the command was handled.
    ///
    /// Structure mirrors `DataGridPanel::dispatch_command`: the toolbar block
    /// runs first and either handles the command, exits early, or falls through
    /// to the list commands below.
    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // While the context menu is open, all commands go to the menu.
        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

        // ── Open dropdown in toolbar ──────────────────────────────────────
        // When the focused filter bar item is a Dropdown and it is open,
        // route navigation commands directly to that dropdown. This mirrors
        // how other list-based overlays (context menu, command palette) own
        // the keyboard while they are visible.
        if let Some(entity) = self.filter_bar.focused_dropdown_entity()
            && entity.read(cx).is_open()
        {
            return match cmd {
                Command::SelectNext => {
                    entity.update(cx, |d, cx| d.select_next_item(cx));
                    true
                }
                Command::SelectPrev => {
                    entity.update(cx, |d, cx| d.select_prev_item(cx));
                    true
                }
                Command::Execute => {
                    entity.update(cx, |d, cx| d.accept_selection(cx));
                    true
                }
                Command::Cancel => {
                    entity.update(cx, |d, cx| d.close(cx));
                    true
                }
                // Consume everything else so the list doesn't react while the
                // dropdown is open.
                _ => true,
            };
        }

        // ── Toolbar mode (Navigating or Editing) ─────────────────────────
        // This block mirrors the `if self.focus_mode == GridFocusMode::Toolbar`
        // block in DataGridPanel. When the filter bar is active:
        //   - Navigation commands (h/l, ←/→) move the ring between items.
        //   - Enter activates the focused item.
        //   - Escape / FocusUp exits toolbar and returns to the list.
        //   - All list commands (j/k, g/G, etc.) are consumed without effect
        //     so the list does not move while the toolbar is focused.
        if self.filter_bar.is_active() {
            if self.filter_bar.is_editing() {
                // The input has GPUI focus; only Cancel/Escape is intercepted
                // here to exit editing mode. Everything else goes to the input.
                if cmd == Command::Cancel {
                    self.filter_bar.exit_editing();
                    self.focus_handle.focus(window);
                    cx.notify();
                    return true;
                }
                return false;
            }

            // Navigating mode: ring is visible, no input has GPUI focus.
            return match cmd {
                Command::ColumnLeft | Command::FocusLeft => {
                    self.filter_bar.move_left();
                    cx.notify();
                    true
                }
                Command::ColumnRight | Command::FocusRight => {
                    self.filter_bar.move_right();
                    cx.notify();
                    true
                }
                Command::Execute => {
                    let activated = self.filter_bar.activate_input(window, cx);
                    if !activated {
                        // Button item: execute the action for this index.
                        self.execute_filter_bar_button(window, cx);
                    }
                    cx.notify();
                    true
                }
                Command::Cancel | Command::FocusUp => {
                    self.filter_bar.deactivate();
                    self.focus_handle.focus(window);
                    cx.notify();
                    true
                }
                // Consume all other list-navigation commands so the list
                // does not respond while the toolbar ring is active.
                _ => true,
            };
        }

        // ── List mode ────────────────────────────────────────────────────
        match cmd {
            Command::SelectNext => {
                self.select_next_row(cx);
                true
            }
            Command::SelectPrev => {
                self.select_prev_row(cx);
                true
            }
            Command::SelectFirst => {
                self.select_first_row(cx);
                true
            }
            Command::SelectLast => {
                self.select_last_row(cx);
                true
            }
            Command::PageDown => {
                self.page_down_rows(cx);
                true
            }
            Command::PageUp => {
                self.page_up_rows(cx);
                true
            }
            Command::ResultsNextPage => {
                self.go_to_next_page(cx);
                true
            }
            Command::ResultsPrevPage => {
                self.go_to_prev_page(cx);
                true
            }
            Command::ExpandCollapse | Command::Execute => {
                self.toggle_selected_row_expanded(cx);
                true
            }
            Command::OpenContextMenu => {
                self.open_context_menu_at_selection(window, cx);
                true
            }
            Command::RefreshSchema => {
                self.refresh(cx);
                true
            }
            Command::FocusToolbar | Command::FocusSearch => {
                self.filter_bar.enter(0);
                cx.notify();
                true
            }
            _ => false,
        }
    }

    fn dispatch_menu_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::MenuDown | Command::SelectNext => {
                self.navigate_menu_down(cx);
                true
            }
            Command::MenuUp | Command::SelectPrev => {
                self.navigate_menu_up(cx);
                true
            }
            Command::MenuSelect | Command::Execute => {
                self.execute_selected_menu_item(window, cx);
                true
            }
            Command::MenuBack | Command::Cancel => {
                self.close_context_menu(window, cx);
                true
            }
            _ => false,
        }
    }

    // ── CSV formatting ────────────────────────────────────────────────────

    /// Formats a single audit event as a CSV row with a header embedded.
    ///
    /// The format matches the full export schema so it is consistent with
    /// what the "Export CSV" button produces.
    fn event_to_csv_row(event: &AuditEventDto) -> String {
        let header = "id,timestamp,level,category,outcome,actor_id,actor_type,action,source_id,\
                      connection_id,driver_id,duration_ms,summary,error_message,correlation_id";

        let escape_csv = |s: &str| -> String {
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.to_string()
            }
        };

        let row = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            event.id,
            event.created_at_epoch_ms,
            escape_csv(event.level.as_deref().unwrap_or("")),
            escape_csv(event.category.as_deref().unwrap_or("")),
            escape_csv(event.outcome.as_deref().unwrap_or("")),
            escape_csv(&event.actor_id),
            escape_csv(event.actor_type.as_deref().unwrap_or("")),
            escape_csv(event.action.as_deref().unwrap_or("")),
            escape_csv(event.source_id.as_deref().unwrap_or("")),
            escape_csv(event.connection_id.as_deref().unwrap_or("")),
            escape_csv(event.driver_id.as_deref().unwrap_or("")),
            event.duration_ms.map(|d| d.to_string()).unwrap_or_default(),
            escape_csv(event.summary.as_deref().unwrap_or("")),
            escape_csv(event.error_message.as_deref().unwrap_or("")),
            escape_csv(event.correlation_id.as_deref().unwrap_or("")),
        );

        format!("{}\n{}", header, row)
    }

    fn do_export(&mut self, format: String, cx: &mut Context<Self>) {
        let AuditDocumentSource::Internal { adapter } = &self.source else {
            self.pending_toast = Some(PendingToast {
                message: "Export is only available for the built-in audit viewer".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        };

        let adapter = adapter.clone();
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

    fn render_context_menu(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let menu = self.context_menu.as_ref()?;
        let theme = cx.theme().clone();

        let event = self.events.get(menu.row)?;
        let has_correlation = event
            .correlation_id
            .as_deref()
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        let items = Self::context_menu_items(has_correlation);
        let selected_index = menu.selected_index;

        let mut menu_elements: Vec<AnyElement> = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            if item.is_separator() {
                menu_elements.push(
                    div()
                        .h(px(1.0))
                        .mx(Spacing::SM)
                        .my(Spacing::XS)
                        .bg(theme.border)
                        .into_any_element(),
                );
                continue;
            }

            let Some(action) = item.action else {
                continue;
            };

            let is_selected = idx == selected_index;
            let label = item.label;
            let icon = item.icon;

            // Icon color follows the DataGridPanel context menu convention.
            let icon_color = if is_selected {
                theme.accent_foreground
            } else {
                theme.muted_foreground
            };

            menu_elements.push(
                div()
                    .id(SharedString::from(format!("audit-ctx-{}", idx)))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .when(is_selected, |d| d.bg(theme.accent))
                    .when(!is_selected, |d| d.hover(|d| d.bg(theme.secondary)))
                    // Icon or indent to keep label alignment consistent.
                    .when_some(icon, |d, icon| {
                        d.child(Icon::new(icon).size(px(16.0)).color(icon_color))
                    })
                    .when(icon.is_none(), |d| d.pl(px(20.0)))
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu
                            && menu.selected_index != idx
                        {
                            menu.selected_index = idx;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        // Resolve the action again — the menu may have changed.
                        let has_corr = this
                            .context_menu
                            .as_ref()
                            .and_then(|m| this.events.get(m.row))
                            .and_then(|e| e.correlation_id.as_deref())
                            .map(|c| !c.is_empty())
                            .unwrap_or(false);
                        let items = Self::context_menu_items(has_corr);
                        if let Some(item) = items.get(idx)
                            && item.action == Some(action)
                            && let Some(menu) = this.context_menu.clone()
                        {
                            let event = this.events.get(menu.row).cloned();
                            this.close_context_menu(window, cx);
                            match action {
                                AuditContextMenuAction::CopyRowAsCsv => {
                                    if let Some(event) = event {
                                        let csv = Self::event_to_csv_row(&event);
                                        cx.write_to_clipboard(ClipboardItem::new_string(csv));
                                    }
                                }
                                AuditContextMenuAction::CopySummary => {
                                    if let Some(event) = event {
                                        let summary = event.summary.clone().unwrap_or_default();
                                        cx.write_to_clipboard(ClipboardItem::new_string(summary));
                                    }
                                }
                                AuditContextMenuAction::FilterByCorrelation => {
                                    if let Some(event) = event
                                        && let Some(correlation_id) = event.correlation_id.clone()
                                    {
                                        this.filter_by_correlation(correlation_id, cx);
                                    }
                                }
                            }
                        }
                    }))
                    .child(Text::caption(label).color(if is_selected {
                        theme.accent_foreground
                    } else {
                        theme.foreground
                    }))
                    .into_any_element(),
            );
        }

        let position = menu.position;

        let element = deferred(
            surface_raised(cx)
                .absolute()
                .top(position.y)
                .left(position.x)
                .w(px(200.0))
                .shadow_lg()
                .py(Spacing::XS)
                .occlude()
                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                    cx.stop_propagation();
                })
                .on_mouse_down_out(cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }))
                .children(menu_elements),
        )
        .with_priority(2)
        .into_any_element();

        Some(element)
    }

    fn render_toolbar(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();

        // Search input.
        let search_control = div()
            .flex()
            .items_center()
            .w(px(220.0))
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Search), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(
                div()
                    .flex_1()
                    .child(Input::new(&self.search_input).small().h(Heights::BUTTON)),
            );

        // Dropdown wrappers — ring goes around the whole labeled control.
        let time_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Time), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.dropdown_time_range.clone());

        let level_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Level), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.multi_select_level.clone());

        let category_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Category), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.multi_select_category.clone());

        let outcome_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Outcome), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.multi_select_outcome.clone());

        // Refresh split button.
        let refresh_label = if self.refresh_policy.is_auto() {
            self.refresh_policy.label()
        } else {
            "Refresh"
        };
        let refresh_icon = if self.refresh_policy.is_auto() {
            AppIcon::Clock
        } else {
            AppIcon::RefreshCcw
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
            .border_color(if self.slot_has_ring(ToolbarSlot::Refresh) {
                theme.ring
            } else {
                theme.input
            })
            .child(
                div()
                    .id("audit-refresh-action")
                    .h_full()
                    .px(Spacing::SM)
                    .flex()
                    .items_center()
                    .gap_1()
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.accent.opacity(0.08)))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.load_events(cx);
                    }))
                    .child(
                        Icon::new(refresh_icon)
                            .size(px(16.0))
                            .color(theme.foreground),
                    )
                    .child(Text::caption(refresh_label)),
            )
            .child(div().w(px(1.0)).h_full().bg(theme.input))
            .child(
                div()
                    .w(px(28.0))
                    .h_full()
                    .rounded_r(Radii::SM)
                    .when(self.slot_has_ring(ToolbarSlot::RefreshPolicy), |d| {
                        d.border_1().border_color(theme.ring)
                    })
                    .child(self.refresh_dropdown.clone()),
            );

        // Clear button.
        let clear_btn = div()
            .id("audit-clear-btn")
            .h(Heights::BUTTON)
            .flex()
            .items_center()
            .px(Spacing::SM)
            .rounded(Radii::SM)
            .border_1()
            .border_color(if self.slot_has_ring(ToolbarSlot::Clear) {
                theme.ring
            } else {
                gpui::transparent_black()
            })
            .cursor_pointer()
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(|this, _, window, cx| {
                this.clear_filters(window, cx);
            }))
            .child(Text::caption("Clear"));

        let _ = window;

        compact_top_bar(&theme, {
            let mut items = vec![
                search_control.into_any_element(),
                time_control.into_any_element(),
            ];

            if !self.is_cloudwatch_source() {
                items.extend([
                    level_control.into_any_element(),
                    category_control.into_any_element(),
                    outcome_control.into_any_element(),
                ]);
            }

            items.extend([
                div().flex_1().into_any_element(),
                refresh_btn.into_any_element(),
                clear_btn.into_any_element(),
            ]);

            items
        })
    }

    fn render_event_list(&mut self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        if self.events.is_empty() && self.is_loading {
            return div()
                .flex_1()
                .items_center()
                .justify_center()
                .child(Text::muted(self.source_loading_label()))
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
                .child(Text::heading(self.source_error_heading()).danger())
                .child(Text::muted(self.status_message.clone().unwrap_or_default()))
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
                .child(Text::muted(self.source_empty_label()))
                .into_any_element();
        }

        let events = self.events.clone();
        let mut rows = Vec::with_capacity(events.len());

        for (row_index, event) in events.into_iter().enumerate() {
            rows.push(
                self.render_event_row(row_index, event, window, cx)
                    .into_any_element(),
            );
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

    fn render_event_row(
        &mut self,
        row_index: usize,
        event: AuditEventDto,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let event_id = event.id;
        let is_expanded = self.expanded_event_ids.contains(&event_id);
        // Only highlight the selected row when this document has GPUI focus.
        // When focus moves to the sidebar, the highlight disappears so the
        // user isn't confused by three simultaneous focus indicators.
        let is_selected = self.has_focus && self.selected_row == Some(row_index);
        let timestamp = Self::format_timestamp_ms(event.created_at_epoch_ms);
        let summary = event.summary.clone().unwrap_or_default();
        let summary_display: AnyElement = if summary.is_empty() {
            Self::null_display(&theme).into_any_element()
        } else {
            Text::body(summary).into_any_element()
        };
        let connection_driver =
            Self::format_connection_driver(&event.connection_id, &event.driver_id);
        let log_stream = event.action.clone();
        let cloudwatch_event_id = event.object_id.clone();

        // Background priority: selected (keyboard cursor) > expanded > default.
        // Use theme.list_active for the selected row — same token as key_value and sidebar.
        let row_bg = if is_selected {
            theme.list_active
        } else if is_expanded {
            theme.primary.opacity(0.08)
        } else {
            gpui::transparent_black()
        };

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
                    .bg(row_bg)
                    // Selected rows get a left-border accent to match other list views.
                    .when(is_selected, |d| d.border_l_2().border_color(theme.accent))
                    .hover(|style| style.bg(theme.list_hover))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            // Signal the workspace to update focus_target → Document so that
                            // Ctrl+H and other panel-navigation bindings work correctly.
                            cx.emit(AuditDocumentEvent::RequestFocus);
                            this.select_row(row_index, cx);
                            this.toggle_event_expanded(event_id, cx);
                            this.focus_handle.focus(window);
                        }),
                    )
                    .on_mouse_down(
                        MouseButton::Right,
                        cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                            // Right-click: open at the actual mouse position.
                            this.open_context_menu_at_mouse(row_index, event.position, window, cx);
                        }),
                    )
                    .child(
                        Icon::new(if is_expanded {
                            AppIcon::ChevronDown
                        } else {
                            AppIcon::ChevronRight
                        })
                        .size(px(12.0))
                        .muted(),
                    )
                    .child(Text::code(timestamp))
                    .when(self.is_cloudwatch_source(), |row| {
                        row.when_some(log_stream.clone(), |row, value| {
                            row.child(
                                div()
                                    .px_1p5()
                                    .py_px()
                                    .rounded(px(3.0))
                                    .bg(theme.primary.opacity(0.15))
                                    .max_w(px(240.0))
                                    .child(
                                        div()
                                            .truncate()
                                            .child(Text::label_sm(value).font_size(FontSizes::XS)),
                                    ),
                            )
                        })
                    })
                    .when(!self.is_cloudwatch_source(), |row| {
                        let level = event.level.as_deref();
                        let level_display: AnyElement = match level {
                            Some(l) => div()
                                .px_1p5()
                                .py_px()
                                .rounded(px(3.0))
                                .bg(Self::level_bg_color(Some(l), &theme))
                                .flex_shrink_0()
                                .child(
                                    Text::label_sm(l.to_uppercase())
                                        .font_size(FontSizes::XS)
                                        .color(Self::level_color(Some(l), &theme)),
                                )
                                .into_any_element(),
                            None => Self::null_display(&theme)
                                .flex_shrink_0()
                                .into_any_element(),
                        };
                        let category = Self::short_category_label(event.category.as_deref());

                        row.child(level_display)
                            .child(Text::caption(category.to_string()))
                    })
                    .child(div().text_sm().flex_1().truncate().child(summary_display))
                    .when_some(
                        cloudwatch_event_id.filter(|_| self.is_cloudwatch_source()),
                        |row, value| row.child(Text::caption(value)),
                    )
                    .when_some(
                        connection_driver.filter(|value| !value.is_empty()),
                        |row, value| row.child(Text::caption(value)),
                    ),
            )
            .when(is_expanded, |root| {
                root.child(self.render_inline_detail(event, window, cx))
            })
    }

    fn render_detail_field(
        &self,
        label: &'static str,
        value: Option<String>,
        theme: &gpui_component::Theme,
    ) -> Div {
        let value_element: AnyElement = match value {
            Some(ref v) if !v.is_empty() => Text::body(v.clone()).into_any_element(),
            _ => Self::null_display(theme).into_any_element(),
        };
        div()
            .flex_col()
            .gap_1p5()
            .min_w(px(120.0))
            .child(Label::new(label))
            .child(value_element)
    }

    fn render_inline_detail(
        &mut self,
        event: AuditEventDto,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if self.is_cloudwatch_source() {
            return self.render_cloudwatch_inline_detail(event, window, cx);
        }

        let theme = cx.theme().clone();
        let timestamp = Self::format_timestamp_ms(event.created_at_epoch_ms);
        let level = event.level.clone();
        let category = match Self::short_category_label(event.category.as_deref()) {
            "NULL" => None,
            label => Some(label.to_string()),
        };
        let outcome = event.outcome.clone();
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
        let action = event.action.clone();
        let source = event.source_id.clone();
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
                        self.render_detail_field("Time", Some(timestamp), &theme)
                            .into_any_element(),
                        self.render_detail_field("Level", level, &theme)
                            .into_any_element(),
                        self.render_detail_field("Category", category, &theme)
                            .into_any_element(),
                        self.render_detail_field("Outcome", outcome, &theme)
                            .into_any_element(),
                        self.render_detail_field("Actor", Some(actor), &theme)
                            .into_any_element(),
                        self.render_detail_field("Action", action, &theme)
                            .into_any_element(),
                        self.render_detail_field("Source", source, &theme)
                            .into_any_element(),
                    ])
                    .when_some(connection_driver, |row, value| {
                        row.child(self.render_detail_field(
                            "Connection/Driver",
                            Some(value),
                            &theme,
                        ))
                    })
                    .when_some(duration, |row, value| {
                        row.child(self.render_detail_field("Duration", Some(value), &theme))
                    }),
            )
            .when_some(summary, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Summary"))
                        .child(Text::body(value)),
                )
            })
            .when_some(error_message, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Error").text_color(theme.danger))
                        .child(Text::body(value).danger()),
                )
            })
            .when_some(details_json, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Details"))
                        .child(
                            div()
                                .bg(theme.secondary)
                                .p_2()
                                .rounded(px(4.0))
                                .child(Text::code(Self::pretty_json(&value))),
                        ),
                )
            })
            .when_some(correlation_id, |root, value| {
                let correlation_id_for_click = value.clone();

                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Correlation ID"))
                        .child(
                            div()
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
                                .child(Text::body(value.clone()).primary()),
                        ),
                )
            })
            .into_any_element()
    }

    fn render_cloudwatch_inline_detail(
        &mut self,
        event: AuditEventDto,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let row_event_id = event.id;
        let timestamp = Self::format_timestamp_ms(event.created_at_epoch_ms);
        let log_group = event.connection_id.clone();
        let log_stream = event.action.clone();
        let log_event_id = event.object_id.clone();
        let ingestion_time = event
            .error_message
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok())
            .map(Self::format_timestamp_ms);
        let message = event.summary.clone().filter(|value| !value.is_empty());
        let details_json = event.details_json.clone().filter(|value| !value.is_empty());

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
                        self.render_detail_field("Time", Some(timestamp), &theme)
                            .into_any_element(),
                        self.render_detail_field("Log Group", log_group, &theme)
                            .into_any_element(),
                        self.render_detail_field("Log Stream", log_stream, &theme)
                            .into_any_element(),
                        self.render_detail_field("Event ID", log_event_id, &theme)
                            .into_any_element(),
                    ])
                    .when_some(ingestion_time, |row, value| {
                        row.child(self.render_detail_field("Ingestion Time", Some(value), &theme))
                    }),
            )
            .when_some(message, |root, value| {
                let message_input =
                    self.ensure_cloudwatch_message_input(row_event_id, &value, window, cx);
                let message_rows = Self::cloudwatch_text_rows(&value, 2);

                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Message"))
                        .child(
                            div().h(Self::cloudwatch_text_height(message_rows)).child(
                                Input::new(&message_input)
                                    .appearance(false)
                                    .w_full()
                                    .h_full(),
                            ),
                        ),
                )
            })
            .when_some(details_json, |root, value| {
                let pretty_details = Self::pretty_json(&value);
                let details_input =
                    self.ensure_cloudwatch_details_input(row_event_id, &pretty_details, window, cx);
                let details_rows = Self::cloudwatch_text_rows(&pretty_details, 4);

                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Details"))
                        .child(
                            div()
                                .bg(theme.secondary)
                                .p_2()
                                .rounded(px(4.0))
                                .h(Self::cloudwatch_text_height(details_rows))
                                .child(
                                    Input::new(&details_input)
                                        .appearance(false)
                                        .w_full()
                                        .h_full(),
                                ),
                        ),
                )
            })
            .into_any_element()
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
            .cursor_pointer()
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(|this, _, _, cx| {
                this.toggle_export_menu(cx);
            }))
            .child(Icon::new(AppIcon::FileSpreadsheet).size(px(16.0)).muted())
            .child(Text::caption("Export"))
            .child(Icon::new(AppIcon::ChevronDown).size(px(12.0)).muted())
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
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.export_with_format(format, cx);
                    }))
                    .child(Text::body(label))
                    .into_any_element()
            })
            .collect::<Vec<_>>();

        // Identical to DataGridPanel::render_export_menu container.
        deferred(
            surface_raised(cx)
                .absolute()
                .bottom_full()
                .right_0()
                .mb(Spacing::XS)
                .w(px(160.0))
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
                format!(
                    "{}-{} of {} {}",
                    start,
                    end,
                    self.total_events,
                    self.source_row_label()
                )
            } else {
                format!("{} {}", self.total_events, self.source_row_label())
            };

            div()
                .flex()
                .items_center()
                .gap_1()
                .child(Icon::new(AppIcon::Rows3).size(px(12.0)).muted())
                .child(Text::caption(row_count_label))
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
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.go_to_prev_page(cx);
                                    }))
                            })
                            .when(!can_prev, |d| d.opacity(0.5))
                            .child(Icon::new(AppIcon::ChevronLeft).size(px(12.0)).color(
                                if can_prev {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child(Text::caption("Prev").color(if can_prev {
                                theme.foreground
                            } else {
                                theme.muted_foreground
                            })),
                    )
                    .child(Text::caption(if total_pages > 1 {
                        format!("Page {}/{} ({}-{})", page, total_pages, start, end)
                    } else {
                        format!("Page {}/{}", page, total_pages)
                    }))
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
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.go_to_next_page(cx);
                                    }))
                            })
                            .when(!can_next, |d| d.opacity(0.5))
                            .child(Text::caption("Next").color(if can_next {
                                theme.foreground
                            } else {
                                theme.muted_foreground
                            }))
                            .child(Icon::new(AppIcon::ChevronRight).size(px(12.0)).color(
                                if can_next {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            )),
                    )
            },
        );

        // Right: export + loading indicator — same as DataGridPanel.
        let right = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .when(self.total_events > 0 && !self.is_cloudwatch_source(), |d| {
                d.child(self.render_export_button(&theme, cx))
            })
            .when_some(
                self.status_message.clone().filter(|_| self.is_loading),
                |d, _| d.child(Text::dim("Loading...")),
            );

        workspace_footer_bar(&theme, left, center, right)
    }
}

impl Focusable for AuditDocument {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
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

        // Update focus state before rendering rows so the selection highlight
        // is suppressed when focus moves to the sidebar or another panel.
        self.has_focus = self.focus_handle.contains_focused(window, cx);

        let theme = cx.theme().clone();
        let context_menu = self.render_context_menu(cx);
        let focus_handle = self.focus_handle.clone();

        div()
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .bg(theme.background)
            // Capture panel origin for context-menu coordinate conversion,
            // identical to DataGridPanel.
            .child({
                let this_entity = cx.entity().clone();
                canvas(
                    move |bounds, _, cx| {
                        this_entity.update(cx, |this, _cx| {
                            this.panel_origin = bounds.origin;
                        });
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full()
            })
            // track_focus keeps the handle alive so focus() works correctly
            // when the workspace calls doc.focus() via set_focus(Document).
            // There is NO on_key_down here — the workspace on_key_down is the
            // single source of truth for keyboard dispatch, exactly as in
            // DataGridPanel and CodeDocument. Adding a second on_key_down would
            // cause both to fire with different context IDs, breaking navigation.
            .track_focus(&focus_handle)
            .child(self.render_toolbar(window, cx))
            .child(
                div()
                    .relative()
                    .flex_1()
                    .overflow_hidden()
                    .flex()
                    .flex_col()
                    .child(self.render_event_list(window, cx))
                    .children(context_menu),
            )
            .child(self.render_status_bar(cx))
    }
}

#[cfg(test)]
mod tests {
    use super::AuditDocument;
    use std::collections::{HashMap, HashSet};

    use dbflux_core::{CollectionRef, Pagination, Value, observability::EventCategory};
    use serde_json::json;

    #[test]
    fn category_index_maps_none_to_all() {
        assert_eq!(AuditDocument::category_index(None), 0);
    }

    #[test]
    fn category_index_maps_mcp_to_mcp_dropdown_entry() {
        assert_eq!(AuditDocument::category_index(Some(EventCategory::Mcp)), 7);
    }

    #[test]
    fn cloudwatch_browse_request_uses_filter_pattern_and_range() {
        let request = AuditDocument::cloudwatch_browse_request(
            CollectionRef::new("logs", "/aws/lambda/app"),
            Some("ERROR".to_string()),
            Some(10),
            Some(20),
            Pagination::Offset {
                limit: 100,
                offset: 0,
            },
        );

        assert_eq!(
            request.filter,
            Some(json!({
                "filter_pattern": "ERROR",
                "start_ms": 10,
                "end_ms": 20,
            }))
        );
    }

    #[test]
    fn cloudwatch_row_maps_into_audit_style_event() {
        let event = AuditDocument::cloudwatch_row_to_event(
            &[
                Value::Int(1000),
                Value::Int(2000),
                Value::Text("2026/04/25/[$LATEST]abc".to_string()),
                Value::Text("hello from cloudwatch".to_string()),
                Value::Text("event-123".to_string()),
            ],
            &CollectionRef::new("logs", "/aws/lambda/app"),
            200,
            3,
        );

        assert_eq!(event.id, 204);
        assert_eq!(event.created_at_epoch_ms, 1000);
        assert_eq!(event.action.as_deref(), Some("2026/04/25/[$LATEST]abc"));
        assert_eq!(event.summary.as_deref(), Some("hello from cloudwatch"));
        assert_eq!(event.object_id.as_deref(), Some("event-123"));
        assert_eq!(event.connection_id.as_deref(), Some("/aws/lambda/app"));
        assert_eq!(event.driver_id.as_deref(), Some("cloudwatch"));
    }

    #[test]
    fn cloudwatch_row_embeds_valid_json_message_as_pretty_printable_details() {
        let event = AuditDocument::cloudwatch_row_to_event(
            &[
                Value::Int(1000),
                Value::Int(2000),
                Value::Text("2026/04/25/[$LATEST]abc".to_string()),
                Value::Text("{\"level\":\"info\",\"nested\":{\"ok\":true}}".to_string()),
                Value::Text("event-123".to_string()),
            ],
            &CollectionRef::new("logs", "/aws/lambda/app"),
            0,
            0,
        );

        let details =
            serde_json::from_str::<serde_json::Value>(event.details_json.as_deref().unwrap())
                .unwrap();

        assert_eq!(details["message"]["level"], json!("info"));
        assert_eq!(details["message"]["nested"]["ok"], json!(true));
    }

    #[test]
    fn cloudwatch_row_keeps_plain_text_message_as_string_in_details() {
        let event = AuditDocument::cloudwatch_row_to_event(
            &[
                Value::Int(1000),
                Value::Int(2000),
                Value::Text("2026/04/25/[$LATEST]abc".to_string()),
                Value::Text("plain text message".to_string()),
                Value::Text("event-123".to_string()),
            ],
            &CollectionRef::new("logs", "/aws/lambda/app"),
            0,
            0,
        );

        let details =
            serde_json::from_str::<serde_json::Value>(event.details_json.as_deref().unwrap())
                .unwrap();

        assert_eq!(details["message"], json!("plain text message"));
    }

    #[test]
    fn cloudwatch_stream_browse_request_targets_exact_stream_and_latest_page() {
        let request = AuditDocument::cloudwatch_stream_browse_request(
            CollectionRef::new("logs", "/aws/lambda/app"),
            "2026/04/25/[$LATEST]abc".to_string(),
            None,
            None,
            None,
            Pagination::Offset {
                limit: 100,
                offset: 0,
            },
        );

        assert_eq!(
            request.filter,
            Some(json!({
                "log_stream_names": ["2026/04/25/[$LATEST]abc"],
                "most_recent": true,
            }))
        );
    }

    #[test]
    fn retain_cloudwatch_input_cache_drops_non_visible_entries() {
        let mut cache = HashMap::from([(1_i64, "message"), (2_i64, "details"), (3_i64, "extra")]);
        let visible_ids = HashSet::from([2_i64, 3_i64]);

        AuditDocument::retain_cloudwatch_input_cache(&mut cache, &visible_ids);

        assert_eq!(cache, HashMap::from([(2_i64, "details"), (3_i64, "extra")]));
    }
}
