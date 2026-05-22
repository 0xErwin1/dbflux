//! Audit document module.

mod chart_view;
mod commands;
mod filters;
pub mod pane;
mod render;
mod source_adapter;
pub mod view;

pub use chart_view::{AuditChartState, AuditViewMode};
pub use filters::{AuditFilters, TimeRange, TimestampDisplayMode};
pub use source_adapter::AuditSourceAdapter;
pub use view::LogStreamView;

use std::collections::{HashMap, HashSet};

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::common::time_range::view::{TimeRangeChanged, TimeRangePanel};
use dbflux_components::components::filter_bar::{FilterBarItem, FilterBarMode, FilterBarState};
use dbflux_components::components::multi_select::{MultiSelect, MultiSelectChanged};
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::controls::{GpuiInput as Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_core::{
    CollectionRef, EventQuery, EventRecord, EventStreamTarget, Pagination, RefreshPolicy,
    observability::{EventCategory, EventOutcome, EventSeverity},
};
use dbflux_storage::repositories::audit::{AuditEventDto, AuditQueryFilter};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{PendingToast, flush_pending_toast};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::calendar::Date;
use gpui_component::date_picker::DatePickerState;
use uuid::Uuid;

use super::handle::DocumentEvent;
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

const DEFAULT_PAGE_SIZE: u32 = 100;

#[derive(Clone)]
enum AuditDocumentSource {
    Internal {
        adapter: AuditSourceAdapter,
    },
    ExternalEventStream {
        profile_id: Uuid,
        target: EventStreamTarget,
    },
}

#[derive(Clone, Copy)]
enum ToolbarSlot {
    Search,
    Time,
    CustomStart,
    CustomEnd,
    CustomApply,
    Timezone,
    Level,
    Category,
    Outcome,
    Refresh,
    RefreshPolicy,
    Clear,
}

struct LoadedEventPage {
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
    external_message_inputs: HashMap<i64, Entity<InputState>>,
    external_details_inputs: HashMap<i64, Entity<InputState>>,
    pagination: Pagination,
    status_message: Option<String>,
    is_loading: bool,
    id: DocumentId,
    title: String,
    pending_initial_load: bool,
    pending_toast: Option<PendingToast>,
    export_menu_open: bool,
    search_input: Entity<InputState>,
    /// Owns the time-range dropdown, date picker, and hour/minute dropdowns.
    /// The audit document reads sub-entities from it for rendering and
    /// subscribes to `TimeRangeChanged` for filter updates.
    time_range_panel: Entity<TimeRangePanel>,
    // Entity handles extracted from `time_range_panel` at construction.
    // Kept here so rendering and `FilterBarItem` wiring can access them
    // without needing `cx` on every render path.
    custom_date_range_picker: Entity<DatePickerState>,
    custom_start_hour_dropdown: Entity<Dropdown>,
    custom_start_minute_dropdown: Entity<Dropdown>,
    custom_end_hour_dropdown: Entity<Dropdown>,
    custom_end_minute_dropdown: Entity<Dropdown>,
    dropdown_time_range: Entity<Dropdown>,
    dropdown_timestamp_mode: Entity<Dropdown>,
    selected_time_range: Option<TimeRange>,
    timestamp_mode: TimestampDisplayMode,
    multi_select_level: Entity<MultiSelect>,
    multi_select_category: Entity<MultiSelect>,
    multi_select_outcome: Entity<MultiSelect>,
    refresh_policy: RefreshPolicy,
    refresh_dropdown: Entity<Dropdown>,
    load_request_id: u64,
    _refresh_timer: Option<Task<()>>,
    _subscriptions: Vec<Subscription>,

    suppress_load: bool,

    // ── Chart view-mode ──────────────────────────────────────────────────
    /// Current view mode: Table (default) or Chart.
    pub view_mode: AuditViewMode,
    /// Chart-specific state. Only used when `view_mode == Chart`.
    pub chart: AuditChartState,
    /// Group-by selector shown in the toolbar when in Chart mode.
    dropdown_chart_group_by: Entity<Dropdown>,

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

    pub fn new_for_event_stream(
        profile_id: Uuid,
        target: EventStreamTarget,
        title: String,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        Self::new_with_source(
            app_state,
            AuditDocumentSource::ExternalEventStream { profile_id, target },
            title,
            "Filter events...",
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
            if matches!(source, AuditDocumentSource::ExternalEventStream { .. }) {
                "All time"
            } else {
                "Last 12 h"
            };

        // Construct the reusable time-range panel.  Sub-entities are extracted
        // below so rendering and FilterBarItem wiring can reference them
        // without going through cx on every access.
        let time_range_panel = cx
            .new(|cx| TimeRangePanel::new(time_range_placeholder, initial_time_range, window, cx));

        let (
            custom_date_range_picker,
            custom_start_hour_dropdown,
            custom_start_minute_dropdown,
            custom_end_hour_dropdown,
            custom_end_minute_dropdown,
            dropdown_time_range,
        ) = {
            let panel = time_range_panel.read(cx);
            (
                panel.custom_date_range_picker.clone(),
                panel.custom_start_hour_dropdown.clone(),
                panel.custom_start_minute_dropdown.clone(),
                panel.custom_end_hour_dropdown.clone(),
                panel.custom_end_minute_dropdown.clone(),
                panel.dropdown_time_range.clone(),
            )
        };

        let dropdown_timestamp_mode = cx.new(|_cx| {
            Dropdown::new("audit-timestamp-mode")
                .placeholder("Local")
                .items(Self::timestamp_mode_items())
                .selected_index(Some(0))
                .toolbar_style(true)
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

        // Single subscription replaces the six individual time-range
        // subscriptions (date picker, four hour/minute dropdowns, preset
        // dropdown).  The panel emits TimeRangeChanged only when the
        // effective window actually changes (preset or custom apply).
        let time_range_sub = cx.subscribe(
            &time_range_panel,
            |this, panel, event: &TimeRangeChanged, cx| {
                let selected = panel.read(cx).selected_time_range;
                this.selected_time_range = selected;
                this.status_message = None;
                this.refresh_filter_bar_items();

                this.filters.start_ms = event.start_ms;
                this.filters.end_ms = event.end_ms;
                this.reset_pagination();
                this.load_events(cx);

                if matches!(this.view_mode, AuditViewMode::Chart) {
                    this.trigger_chart_aggregate(cx);
                }
            },
        );

        // The panel emits `TimeRangeChanged` only when the effective window
        // changes, which never happens for the "Custom…" selection (it waits
        // for Apply). Subscribe directly to the preset dropdown so the toolbar
        // reveals the custom date/time inputs as soon as Custom is picked.
        let preset_selection_sub = cx.subscribe(
            &dropdown_time_range,
            |this, _, event: &DropdownSelectionChanged, cx| {
                let Some(range) = TimeRangePanel::time_range_for_index(event.index) else {
                    return;
                };

                this.selected_time_range = Some(range);
                this.refresh_filter_bar_items();
                cx.notify();
            },
        );

        let timestamp_mode_sub = cx.subscribe(
            &dropdown_timestamp_mode,
            |this, _, event: &DropdownSelectionChanged, cx| {
                if let Some(mode) = Self::timestamp_mode_for_index(event.index) {
                    this.timestamp_mode = mode;
                    cx.notify();
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

                if matches!(this.view_mode, AuditViewMode::Chart) {
                    this.trigger_chart_aggregate(cx);
                }
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

                if matches!(this.view_mode, AuditViewMode::Chart) {
                    this.trigger_chart_aggregate(cx);
                }
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

                if matches!(this.view_mode, AuditViewMode::Chart) {
                    this.trigger_chart_aggregate(cx);
                }
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

        let selected_time_range = initial_time_range.and_then(Self::time_range_for_index);
        let filter_bar = FilterBarState::new(Self::toolbar_items_for_state(
            &source,
            selected_time_range,
            &search_input,
            &dropdown_time_range,
            &dropdown_timestamp_mode,
            &custom_date_range_picker,
            &custom_start_hour_dropdown,
            &custom_start_minute_dropdown,
            &custom_end_hour_dropdown,
            &custom_end_minute_dropdown,
            &refresh_dropdown,
        ));

        let filters = Self::default_filters_for_source(&source);

        // Chart group-by dropdown — visible only when in Chart mode.
        let dropdown_chart_group_by = cx.new(|_cx| {
            Dropdown::new("audit-chart-group-by")
                .items(vec![
                    DropdownItem::new("Category"),
                    DropdownItem::new("Outcome"),
                    DropdownItem::new("Level"),
                ])
                .selected_index(Some(0))
                .toolbar_style(true)
        });

        let chart_group_by_sub = cx.subscribe(
            &dropdown_chart_group_by,
            |this, _, event: &DropdownSelectionChanged, cx| {
                use dbflux_components::chart::AuditGroupBy;
                let group_by = match event.index {
                    0 => AuditGroupBy::Category,
                    1 => AuditGroupBy::Outcome,
                    2 => AuditGroupBy::Level,
                    _ => AuditGroupBy::Category,
                };
                this.chart.group_by = group_by;
                // Reset binding seeded flag so the new group-by gets a fresh
                // BindingSpec seed after the next aggregate result arrives.
                this.chart.binding_seeded = false;
                if matches!(this.view_mode, AuditViewMode::Chart) {
                    this.trigger_chart_aggregate(cx);
                }
            },
        );

        Self {
            app_state,
            source,
            filters,
            events: Vec::new(),
            total_events: 0,
            expanded_event_ids: HashSet::new(),
            external_message_inputs: HashMap::new(),
            external_details_inputs: HashMap::new(),
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
            time_range_panel,
            custom_date_range_picker,
            custom_start_hour_dropdown,
            custom_start_minute_dropdown,
            custom_end_hour_dropdown,
            custom_end_minute_dropdown,
            dropdown_time_range,
            dropdown_timestamp_mode,
            selected_time_range,
            timestamp_mode: TimestampDisplayMode::Local,
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
                preset_selection_sub,
                timestamp_mode_sub,
                level_sub,
                category_sub,
                outcome_sub,
                refresh_dropdown_sub,
                chart_group_by_sub,
            ],
            suppress_load: false,
            view_mode: AuditViewMode::Table,
            chart: AuditChartState::new(cx),
            dropdown_chart_group_by,
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

    pub fn matches_event_stream(&self, profile_id: Uuid, target: &EventStreamTarget) -> bool {
        match &self.source {
            AuditDocumentSource::ExternalEventStream {
                profile_id: doc_profile_id,
                target: doc_target,
            } => *doc_profile_id == profile_id && doc_target == target,
            AuditDocumentSource::Internal { .. } => false,
        }
    }

    fn is_external_event_stream(&self) -> bool {
        matches!(self.source, AuditDocumentSource::ExternalEventStream { .. })
    }

    /// Returns `true` when this document is backed by the built-in audit
    /// store (as opposed to an external event stream). Used for deduplication:
    /// only one internal audit viewer may be open at a time.
    pub fn is_internal(&self) -> bool {
        matches!(self.source, AuditDocumentSource::Internal { .. })
    }

    #[allow(clippy::too_many_arguments)]
    fn toolbar_items_for_state(
        source: &AuditDocumentSource,
        selected_time_range: Option<TimeRange>,
        search_input: &Entity<InputState>,
        dropdown_time_range: &Entity<Dropdown>,
        dropdown_timestamp_mode: &Entity<Dropdown>,
        custom_date_range_picker: &Entity<DatePickerState>,
        custom_start_hour_dropdown: &Entity<Dropdown>,
        custom_start_minute_dropdown: &Entity<Dropdown>,
        custom_end_hour_dropdown: &Entity<Dropdown>,
        custom_end_minute_dropdown: &Entity<Dropdown>,
        refresh_dropdown: &Entity<Dropdown>,
    ) -> Vec<FilterBarItem> {
        let mut toolbar_items = vec![
            FilterBarItem::input("Search:", search_input.clone()),
            FilterBarItem::dropdown("Time:", dropdown_time_range.clone()),
            FilterBarItem::dropdown("Time zone:", dropdown_timestamp_mode.clone()),
        ];

        if selected_time_range == Some(TimeRange::Custom) {
            toolbar_items.extend([
                FilterBarItem::date_picker("Range:", custom_date_range_picker.clone()),
                FilterBarItem::dropdown("Start hour:", custom_start_hour_dropdown.clone()),
                FilterBarItem::dropdown("Start minute:", custom_start_minute_dropdown.clone()),
                FilterBarItem::dropdown("End hour:", custom_end_hour_dropdown.clone()),
                FilterBarItem::dropdown("End minute:", custom_end_minute_dropdown.clone()),
                FilterBarItem::button("Apply"),
            ]);
        }

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

        toolbar_items
    }

    fn refresh_filter_bar_items(&mut self) {
        self.filter_bar.set_items(Self::toolbar_items_for_state(
            &self.source,
            self.selected_time_range,
            &self.search_input,
            &self.dropdown_time_range,
            &self.dropdown_timestamp_mode,
            &self.custom_date_range_picker,
            &self.custom_start_hour_dropdown,
            &self.custom_start_minute_dropdown,
            &self.custom_end_hour_dropdown,
            &self.custom_end_minute_dropdown,
            &self.refresh_dropdown,
        ));
    }

    fn toolbar_index(&self, slot: ToolbarSlot) -> Option<usize> {
        let custom_offset = if self.selected_time_range == Some(TimeRange::Custom) {
            6
        } else {
            0
        };

        match (&self.source, slot) {
            (_, ToolbarSlot::Search) => Some(0),
            (_, ToolbarSlot::Time) => Some(1),
            (_, ToolbarSlot::Timezone) => Some(2),
            (_, ToolbarSlot::CustomStart) if custom_offset > 0 => Some(3),
            (_, ToolbarSlot::CustomEnd) if custom_offset > 0 => Some(6),
            (_, ToolbarSlot::CustomApply) if custom_offset > 0 => Some(8),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Level) => Some(3 + custom_offset),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Category) => {
                Some(4 + custom_offset)
            }
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Outcome) => Some(5 + custom_offset),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Refresh) => Some(6 + custom_offset),
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::RefreshPolicy) => {
                Some(7 + custom_offset)
            }
            (AuditDocumentSource::Internal { .. }, ToolbarSlot::Clear) => Some(8 + custom_offset),
            (AuditDocumentSource::ExternalEventStream { .. }, ToolbarSlot::Refresh) => {
                Some(3 + custom_offset)
            }
            (AuditDocumentSource::ExternalEventStream { .. }, ToolbarSlot::RefreshPolicy) => {
                Some(4 + custom_offset)
            }
            (AuditDocumentSource::ExternalEventStream { .. }, ToolbarSlot::Clear) => {
                Some(5 + custom_offset)
            }
            _ => None,
        }
    }

    fn slot_has_ring(&self, slot: ToolbarSlot) -> bool {
        if self.filter_bar.mode() != FilterBarMode::Navigating {
            return false;
        }

        let focused_index = self.filter_bar.focused_index();

        if self.selected_time_range == Some(TimeRange::Custom) {
            match slot {
                ToolbarSlot::CustomStart => return (3..=5).contains(&focused_index),
                ToolbarSlot::CustomEnd => return (6..=7).contains(&focused_index),
                _ => {}
            }
        }

        self.filter_bar.mode() == FilterBarMode::Navigating
            && self.toolbar_index(slot) == Some(focused_index)
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

                        if matches!(doc.view_mode, AuditViewMode::Chart) {
                            doc.trigger_chart_aggregate(cx);
                        }
                    });
                });
            }
        }));
    }

    fn initial_time_range(source: &AuditDocumentSource) -> Option<usize> {
        match source {
            AuditDocumentSource::ExternalEventStream { .. } => None,
            // Index 3 = Last24Hours in the new preset mapping.
            AuditDocumentSource::Internal { .. } => Some(3),
        }
    }

    fn default_filters_for_source(source: &AuditDocumentSource) -> AuditFilters {
        let mut filters = AuditFilters::default();

        if !matches!(source, AuditDocumentSource::ExternalEventStream { .. }) {
            let (start_ms, end_ms) = TimeRange::Last24Hours.to_filter_values();
            filters.start_ms = start_ms;
            filters.end_ms = end_ms;
        }

        filters
    }

    fn source_loading_label(&self) -> &'static str {
        if self.is_external_event_stream() {
            "Loading events..."
        } else {
            "Loading audit events..."
        }
    }

    fn source_error_heading(&self) -> &'static str {
        if self.is_external_event_stream() {
            "Failed to load events"
        } else {
            "Failed to load audit events"
        }
    }

    fn source_empty_label(&self) -> &'static str {
        if self.is_external_event_stream() {
            "No events match the current filters."
        } else {
            "No audit events match the current filters."
        }
    }

    fn source_row_label(&self) -> &'static str {
        if self.is_external_event_stream() {
            "events"
        } else {
            "rows"
        }
    }

    fn external_page_to_loaded(page: dbflux_core::EventPage) -> LoadedEventPage {
        let events = page
            .events
            .into_iter()
            .map(Self::audit_event_from_record)
            .collect::<Vec<_>>();
        let total_events = page
            .total
            .map(|value| value as u64)
            .unwrap_or_else(|| page.offset as u64 + events.len() as u64 + u64::from(page.has_more));

        LoadedEventPage {
            events,
            total_events,
        }
    }

    fn audit_event_from_record(record: EventRecord) -> AuditEventDto {
        AuditEventDto {
            id: record.id.unwrap_or_default(),
            actor_id: record.actor_id.unwrap_or_default(),
            tool_id: record.action.clone(),
            decision: String::new(),
            reason: None,
            profile_id: None,
            classification: None,
            duration_ms: record.duration_ms,
            created_at: record.ts_ms.to_string(),
            created_at_epoch_ms: record.ts_ms,
            level: Some(record.level.as_str().to_string()),
            category: Some(record.category.as_str().to_string()),
            action: Some(record.action),
            outcome: Some(record.outcome.as_str().to_string()),
            actor_type: Some(record.actor_type.as_str().to_string()),
            source_id: Some(record.source_id.as_str().to_string()),
            summary: Some(record.summary),
            connection_id: record.connection_id,
            database_name: record.database_name,
            driver_id: record.driver_id,
            object_type: record.object_type,
            object_id: record.object_id,
            details_json: record.details_json,
            error_code: record.error_code,
            error_message: record.error_message,
            session_id: record.session_id,
            correlation_id: record.correlation_id,
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
        let task_id = match &self.source {
            AuditDocumentSource::ExternalEventStream { profile_id, .. } => {
                Some(self.app_state.update(cx, |state, _| {
                    let (task_id, _) = state.start_task_for_profile(
                        dbflux_core::TaskKind::Query,
                        format!("Loading event stream: {}", self.title),
                        Some(*profile_id),
                    );
                    task_id
                }))
            }
            AuditDocumentSource::Internal { .. } => None,
        };

        let task = match &self.source {
            AuditDocumentSource::Internal { adapter } => {
                let adapter = adapter.clone();

                cx.background_executor().spawn(async move {
                    let events = adapter.query_filter(&page_filter)?;
                    let total = adapter.count_filter(&count_filter)?;

                    Ok::<_, String>(LoadedEventPage {
                        events,
                        total_events: total,
                    })
                })
            }
            AuditDocumentSource::ExternalEventStream { profile_id, target } => {
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
                        Some("Connection not found for this event source".to_string());
                    cx.notify();
                    return;
                };

                let target = target.clone();
                let query = EventQuery {
                    from_ts_ms: self.filters.start_ms,
                    to_ts_ms: self.filters.end_ms,
                    free_text: self.filters.free_text.clone(),
                    limit: Some(self.pagination_limit()),
                    offset: Some(self.pagination_offset()),
                    ..EventQuery::default()
                };

                cx.background_executor().spawn(async move {
                    let page = connection
                        .browse_event_stream(&target, &query)
                        .map_err(|error| format!("external event stream browse failed: {error}"))?;

                    Ok::<_, String>(Self::external_page_to_loaded(page))
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
                        doc.retain_external_inline_inputs(&visible_ids);

                        if let Some(task_id) = task_id {
                            doc.app_state.update(cx, |state, _| {
                                state.complete_task(task_id);
                            });
                        }

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
                        doc.clear_external_inline_inputs();
                        doc.is_loading = false;
                        doc.status_message = Some(format!("Error loading events: {}", error));

                        if let Some(task_id) = task_id {
                            let details = format!("Error loading events: {}", error);
                            doc.app_state.update(cx, |state, _| {
                                state.fail_task_with_details(task_id, error.clone(), details);
                            });
                        }

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

    fn custom_date_range(
        &self,
        cx: &App,
    ) -> Option<(
        dbflux_core::chrono::NaiveDate,
        dbflux_core::chrono::NaiveDate,
    )> {
        match self.custom_date_range_picker.read(cx).date() {
            Date::Range(Some(start), Some(end)) => Some((start, end)),
            _ => None,
        }
    }

    fn selected_dropdown_number(dropdown: &Entity<Dropdown>, cx: &App) -> Option<u32> {
        dropdown.read(cx).selected_value()?.parse::<u32>().ok()
    }

    fn custom_time_parts(&self, cx: &App) -> Option<(u32, u32, u32, u32)> {
        Some((
            Self::selected_dropdown_number(&self.custom_start_hour_dropdown, cx)?,
            Self::selected_dropdown_number(&self.custom_start_minute_dropdown, cx)?,
            Self::selected_dropdown_number(&self.custom_end_hour_dropdown, cx)?,
            Self::selected_dropdown_number(&self.custom_end_minute_dropdown, cx)?,
        ))
    }

    fn can_apply_custom_time_range(&self, cx: &App) -> bool {
        self.custom_date_range(cx).is_some() && self.custom_time_parts(cx).is_some()
    }

    fn apply_custom_time_range(&mut self, cx: &mut Context<Self>) {
        // Sync the panel's timestamp_mode before delegating so the panel
        // validates using the same timezone the audit doc is displaying.
        let timestamp_mode = self.timestamp_mode;
        self.time_range_panel.update(cx, |panel, _cx| {
            panel.timestamp_mode = timestamp_mode;
        });

        match self
            .time_range_panel
            .update(cx, |panel, cx| panel.apply_custom_range(cx))
        {
            Ok((start_ms, end_ms)) => {
                self.filters.start_ms = Some(start_ms);
                self.filters.end_ms = Some(end_ms);
                self.selected_time_range = Some(TimeRange::Custom);
                self.status_message = None;
                self.reset_pagination();
                self.load_events(cx);
            }
            Err(error) => {
                self.status_message = Some(error.clone());
                self.pending_toast = Some(PendingToast {
                    message: error,
                    is_error: true,
                });
                cx.notify();
            }
        }
    }

    pub fn clear_filters(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.filters = Self::default_filters_for_source(&self.source);
        self.reset_pagination();
        self.export_menu_open = false;

        self.suppress_load = true;
        let selected_time_range = Self::initial_time_range(&self.source);
        let selected_range_variant = selected_time_range.and_then(Self::time_range_for_index);
        self.selected_time_range = selected_range_variant;
        self.time_range_panel.update(cx, |panel, _cx| {
            panel.selected_time_range = selected_range_variant;
        });
        self.dropdown_time_range.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(selected_time_range, cx)
        });
        self.refresh_filter_bar_items();
        self.multi_select_level
            .update(cx, |ms, cx| ms.clear_selection(cx));
        self.multi_select_category
            .update(cx, |ms, cx| ms.clear_selection(cx));
        self.multi_select_outcome
            .update(cx, |ms, cx| ms.clear_selection(cx));
        self.search_input
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.custom_date_range_picker.update(cx, |picker, cx| {
            picker.set_date(Date::Range(None, None), window, cx);
        });
        self.custom_start_hour_dropdown
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(0), cx));
        self.custom_start_minute_dropdown
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(0), cx));
        self.custom_end_hour_dropdown
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(23), cx));
        self.custom_end_minute_dropdown
            .update(cx, |dropdown, cx| dropdown.set_selected_index(Some(59), cx));
        self.suppress_load = false;

        self.load_events(cx);
    }

    fn toggle_event_expanded(&mut self, event_id: i64, cx: &mut Context<Self>) {
        if !self.expanded_event_ids.insert(event_id) {
            self.expanded_event_ids.remove(&event_id);
        }

        cx.notify();
    }

    fn retain_external_inline_inputs(&mut self, visible_ids: &HashSet<i64>) {
        Self::retain_event_input_cache(&mut self.external_message_inputs, visible_ids);
        Self::retain_event_input_cache(&mut self.external_details_inputs, visible_ids);
    }

    fn retain_event_input_cache<T>(cache: &mut HashMap<i64, T>, visible_ids: &HashSet<i64>) {
        cache.retain(|event_id, _| visible_ids.contains(event_id));
    }

    fn clear_external_inline_inputs(&mut self) {
        self.external_message_inputs.clear();
        self.external_details_inputs.clear();
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

    fn time_range_for_index(index: usize) -> Option<TimeRange> {
        match index {
            0 => Some(TimeRange::Last15min),
            1 => Some(TimeRange::LastHour),
            2 => Some(TimeRange::Last6Hours),
            3 => Some(TimeRange::Last24Hours),
            4 => Some(TimeRange::Last7Days),
            5 => Some(TimeRange::Custom),
            _ => None,
        }
    }

    fn timestamp_mode_items() -> Vec<DropdownItem> {
        vec![DropdownItem::new("Local"), DropdownItem::new("UTC")]
    }

    fn timestamp_mode_for_index(index: usize) -> Option<TimestampDisplayMode> {
        match index {
            0 => Some(TimestampDisplayMode::Local),
            1 => Some(TimestampDisplayMode::Utc),
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

    /// Returns the current auto-refresh policy.
    pub fn current_refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    /// Applies a new refresh policy; no-op if the policy is unchanged.
    pub fn apply_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        self.set_refresh_policy(policy, cx);
    }

    // ── Focus ─────────────────────────────────────────────────────────────

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub(self) fn filter_by_correlation(&mut self, correlation_id: String, cx: &mut Context<Self>) {
        self.filters.correlation_id = Some(correlation_id);
        self.reset_pagination();
        self.load_events(cx);
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
}

impl Focusable for AuditDocument {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<DocumentEvent> for AuditDocument {}

impl Render for AuditDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_initial_load {
            self.pending_initial_load = false;
            self.load_events(cx);
        }

        // Drain any pending chart aggregate result before rendering.
        self.drain_chart_pending_result(cx);

        flush_pending_toast(self.pending_toast.take(), window, cx);

        // Update focus state before rendering rows so the selection highlight
        // is suppressed when focus moves to the sidebar or another panel.
        self.has_focus = self.focus_handle.contains_focused(window, cx);

        let theme = cx.theme().clone();
        let focus_handle = self.focus_handle.clone();

        // Build the content area based on the current view mode.
        let content_area: AnyElement = match self.view_mode {
            AuditViewMode::Table => {
                let context_menu = self.render_context_menu(cx);
                div()
                    .relative()
                    .flex_1()
                    .overflow_hidden()
                    .flex()
                    .flex_col()
                    .child(self.render_event_list(window, cx))
                    .children(context_menu)
                    .into_any_element()
            }
            AuditViewMode::Chart => self.render_chart_area(window, cx),
        };

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
            .child(content_area)
            .when(self.view_mode == AuditViewMode::Table, |d| {
                d.child(self.render_status_bar(cx))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::AuditDocument;
    use std::collections::{HashMap, HashSet};

    use dbflux_core::{
        EventActorType, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSourceId,
    };

    #[test]
    fn category_index_maps_none_to_all() {
        assert_eq!(AuditDocument::category_index(None), 0);
    }

    #[test]
    fn category_index_maps_mcp_to_mcp_dropdown_entry() {
        assert_eq!(AuditDocument::category_index(Some(EventCategory::Mcp)), 7);
    }

    #[test]
    fn audit_event_from_record_preserves_event_stream_fields() {
        let event = AuditDocument::audit_event_from_record(EventRecord {
            id: Some(7),
            ts_ms: 1000,
            level: EventSeverity::Info,
            category: EventCategory::System,
            action: "partition-a".to_string(),
            outcome: EventOutcome::Success,
            actor_type: EventActorType::System,
            actor_id: None,
            source_id: EventSourceId::System,
            connection_id: Some("source-a".to_string()),
            database_name: Some("logs".to_string()),
            driver_id: Some("cloudwatch".to_string()),
            object_type: Some("event_stream".to_string()),
            object_id: Some("event-123".to_string()),
            summary: "hello".to_string(),
            details_json: Some("{\"message\":\"hello\"}".to_string()),
            error_code: None,
            error_message: Some("2000".to_string()),
            duration_ms: None,
            session_id: None,
            correlation_id: None,
        });

        assert_eq!(event.id, 7);
        assert_eq!(event.created_at_epoch_ms, 1000);
        assert_eq!(event.action.as_deref(), Some("partition-a"));
        assert_eq!(event.summary.as_deref(), Some("hello"));
        assert_eq!(event.object_id.as_deref(), Some("event-123"));
        assert_eq!(event.connection_id.as_deref(), Some("source-a"));
    }

    #[test]
    fn retain_event_input_cache_drops_non_visible_entries() {
        let mut cache = HashMap::from([(1_i64, "message"), (2_i64, "details"), (3_i64, "extra")]);
        let visible_ids = HashSet::from([2_i64, 3_i64]);

        AuditDocument::retain_event_input_cache(&mut cache, &visible_ids);

        assert_eq!(cache, HashMap::from([(2_i64, "details"), (3_i64, "extra")]));
    }

    #[test]
    fn event_text_rows_adds_wrap_height_for_long_lines() {
        let long_line = "x".repeat(161);

        assert_eq!(AuditDocument::event_text_rows(&long_line, 2), 3);
    }

    #[test]
    fn event_code_rows_is_more_conservative_for_pretty_json_blocks() {
        let line = "a\nb\nc";

        assert_eq!(AuditDocument::event_code_rows(line, 2), 3);
    }
}
