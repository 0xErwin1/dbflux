use crate::keymap::{RunCommand, run_command};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::actions::{ScrollDown, ScrollToBottom, ScrollToTop, ScrollUp};
use dbflux_components::chart::ChartKind;
use dbflux_components::composites::{inline_tab, inline_tab_bar};
use dbflux_components::controls::{Button, GpuiInput as Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::modal::{Modal, ModalFocus};
use dbflux_components::primitives::Text;
use dbflux_components::saved_chart::SavedChart;
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_components::vim::{VimBinding, VimHost};
use dbflux_core::{LogErr, MetricDescriptor};
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Context, Entity, EventEmitter, FocusHandle, Focusable, FontWeight,
    IntoElement, Render, SharedString, Subscription, Window, div, px,
};
use gpui_component::ActiveTheme;
use gpui_component::input::EditorState;
use gpui_component::scroll::ScrollableElement;
use std::collections::HashMap;
use uuid::Uuid;

impl VimHost for ModalAddPanelPicker {
    fn vim(&self) -> Option<&VimBinding> {
        Some(&self.query_vim)
    }

    fn vim_mut(&mut self) -> Option<&mut VimBinding> {
        Some(&mut self.query_vim)
    }
}

/// Outcome emitted when the user resolves the add-panel picker.
#[derive(Clone, Debug)]
pub enum AddPanelOutcome {
    /// User confirmed selection of one or more existing saved charts.
    Confirmed {
        dashboard_id: Uuid,
        chart_ids: Vec<Uuid>,
    },
    /// User submitted the "From query" tab and wants a new query-backed panel.
    CreateFromQuery {
        dashboard_id: Uuid,
        profile_id: Uuid,
        name: String,
        query: String,
        chart_kind: ChartKind,
    },
    /// User submitted the "From metric" tab and wants a new metric-backed panel.
    CreateFromMetric {
        dashboard_id: Uuid,
        profile_id: Uuid,
        name: String,
        namespace: String,
        metric_name: String,
        dimensions: Vec<(String, String)>,
        period_seconds: u32,
        statistic: String,
    },
    Cancelled,
}

/// Side-channel event: modal asks the host to load metrics for a namespace.
///
/// The host (workspace) fetches them via the active connection's metric
/// catalog and feeds them back through `set_metrics_for_namespace`.
#[derive(Clone, Debug)]
pub struct RequestMetricsForNamespace {
    pub profile_id: Uuid,
    pub namespace: String,
}

/// Request payload for opening the add-panel picker.
#[derive(Clone, Debug)]
pub struct AddPanelRequest {
    pub dashboard_id: Uuid,
    /// Profile of the active connection backing this dashboard.
    pub profile_id: Uuid,
    /// All saved charts available for this profile's connection.
    pub candidates: Vec<SavedChart>,
    /// Whether the connection advertises `DriverCapabilities::METRIC_CATALOG`.
    pub has_metric_catalog: bool,
    /// Pre-loaded namespaces (empty when no catalog or when still loading).
    pub metric_namespaces: Vec<String>,
    /// True while a background fetch for `metric_namespaces` is in flight.
    /// The modal renders a "Loading namespaces…" placeholder while true.
    pub metric_namespaces_loading: bool,
}

/// Active tab in the picker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AddPanelTab {
    #[default]
    Saved,
    Query,
    Metric,
}

/// The list of the Metric tab the keyboard is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetricList {
    Namespaces,
    Metrics,
}

/// Pure helper: submit button label given an active tab and current
/// saved-tab selection count. Extracted to keep tests GPUI-free.
pub fn submit_label_for(tab: AddPanelTab, saved_selection_count: usize) -> String {
    match tab {
        AddPanelTab::Saved => match saved_selection_count {
            0 => dbflux_i18n::t!("modals.add_panel_picker.submit.zero"),
            1 => dbflux_i18n::t!("modals.add_panel_picker.submit.one"),
            n => dbflux_i18n::t!("modals.add_panel_picker.submit.many", count = n),
        },
        AddPanelTab::Query | AddPanelTab::Metric => {
            dbflux_i18n::t!("modals.add_panel_picker.submit.create")
        }
    }
}

/// Pure helper: catalog key for a chart kind's translated label.
fn chart_kind_label_key(kind: ChartKind) -> &'static str {
    match kind {
        ChartKind::Line => "modals.add_panel_picker.chart_kind.line",
        ChartKind::Bar => "modals.add_panel_picker.chart_kind.bar",
        ChartKind::Area => "modals.add_panel_picker.chart_kind.area",
        ChartKind::Scatter => "modals.add_panel_picker.chart_kind.scatter",
        _ => "modals.add_panel_picker.chart_kind.line",
    }
}

/// Translated label for a chart kind, using the active process-wide locale.
fn chart_kind_label(kind: ChartKind) -> String {
    dbflux_i18n::t!(chart_kind_label_key(kind))
}

/// CloudWatch statistic identifiers. These are API values, not user-facing
/// prose, so they stay untranslated.
const METRIC_STATISTICS: [&str; 5] = ["Average", "Sum", "Minimum", "Maximum", "SampleCount"];

/// Pure helper: which tabs are visible given the metric-catalog capability.
pub fn visible_tabs_for(has_metric_catalog: bool) -> Vec<AddPanelTab> {
    let mut tabs = vec![AddPanelTab::Saved, AddPanelTab::Query];
    if has_metric_catalog {
        tabs.push(AddPanelTab::Metric);
    }
    tabs
}

/// Pure helper: query-tab form validity.
pub fn query_tab_valid(name: &str, query: &str) -> bool {
    !name.trim().is_empty() && !query.trim().is_empty()
}

/// Pure helper: metric-tab form validity.
pub fn metric_tab_valid(
    name: &str,
    namespace_selected: bool,
    metric_selected: bool,
    period_text: &str,
) -> bool {
    let period_ok = period_text
        .trim()
        .parse::<u32>()
        .map(|p| p > 0)
        .unwrap_or(false);
    !name.trim().is_empty() && namespace_selected && metric_selected && period_ok
}

/// Modal entity for adding panels to a dashboard.
///
/// Three tabs:
/// - `Saved` — pick from existing saved charts (current behaviour).
/// - `Query` — type a SQL query and pick a chart kind to spawn a new panel.
/// - `Metric` — pick a CloudWatch-style namespace + metric (only visible when
///   the connection advertises `METRIC_CATALOG`).
pub struct ModalAddPanelPicker {
    request: Option<AddPanelRequest>,
    visible: bool,
    active_tab: AddPanelTab,
    focus: ModalFocus,

    // Saved-tab state.
    search_input: Entity<InputState>,
    selected_ids: Vec<Uuid>,
    /// Row of the chart list the keyboard is on (into the filtered list).
    saved_highlight: usize,
    /// Tab stop of the chart list: the list keys work while it has focus.
    saved_list_focus: FocusHandle,

    // Query-tab state.
    query_name_input: Entity<InputState>,
    query_input: Entity<EditorState>,
    /// Vim mode for the query editor.
    query_vim: VimBinding,
    query_chart_kind: ChartKind,

    // Metric-tab state.
    metric_name_input: Entity<InputState>,
    metric_namespace_filter_input: Entity<InputState>,
    /// Free-text filter applied to the metric list in the right column of the
    /// metric picker tab. Case-insensitive substring match against
    /// `metric_name` and against each `dimension key=value` pair.
    metric_metric_filter_input: Entity<InputState>,
    metric_namespace_selected: Option<String>,
    metric_metrics_for_namespace: HashMap<String, Vec<MetricDescriptor>>,
    metric_metric_selected: Option<usize>,
    metric_period_input: Entity<InputState>,
    metric_statistic: String,
    /// Rows of the namespace and metric lists the keyboard is on (into the
    /// filtered lists).
    namespace_highlight: usize,
    metric_highlight: usize,
    /// Tab stops of the namespace and metric lists.
    namespace_list_focus: FocusHandle,
    metric_list_focus: FocusHandle,

    _subscriptions: Vec<Subscription>,
}

impl ModalAddPanelPicker {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "modals.add_panel_picker.saved.search_placeholder"
            ))
        });
        let query_name_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("modals.add_panel_picker.name_placeholder"))
        });
        let query_input = cx.new(|cx| {
            // soft_wrap defaults to true in 0.6.1, so the old explicit builder is gone.
            EditorState::new(window, cx)
                .language("sql")
                .line_number(true)
                .placeholder(dbflux_i18n::t!("modals.add_panel_picker.query.placeholder"))
        });
        let metric_name_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("modals.add_panel_picker.name_placeholder"))
        });
        let metric_namespace_filter_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "modals.add_panel_picker.metric.namespace_filter_placeholder"
            ))
        });
        let metric_metric_filter_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "modals.add_panel_picker.metric.metric_filter_placeholder"
            ))
        });
        let metric_period_input = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "modals.add_panel_picker.metric.period_label"
            ));
            s.set_value("60", window, cx);
            s
        });

        let query_vim = VimBinding::new(query_input.clone(), window, cx);

        let mut picker = Self {
            request: None,
            visible: false,
            active_tab: AddPanelTab::default(),
            focus: ModalFocus::new(cx),
            search_input,
            selected_ids: Vec::new(),
            saved_highlight: 0,
            saved_list_focus: cx.focus_handle().tab_stop(true),
            query_name_input,
            query_input,
            query_vim,
            query_chart_kind: ChartKind::Line,
            metric_name_input,
            metric_namespace_filter_input,
            metric_metric_filter_input,
            metric_namespace_selected: None,
            metric_metrics_for_namespace: HashMap::new(),
            metric_metric_selected: None,
            metric_period_input,
            metric_statistic: "Average".to_string(),
            namespace_highlight: 0,
            metric_highlight: 0,
            namespace_list_focus: cx.focus_handle().tab_stop(true),
            metric_list_focus: cx.focus_handle().tab_stop(true),
            _subscriptions: Vec::new(),
        };

        VimBinding::follow_setting(&mut picker, cx);
        picker
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn active_tab(&self) -> AddPanelTab {
        self.active_tab
    }

    pub fn set_active_tab(&mut self, tab: AddPanelTab, cx: &mut Context<Self>) {
        self.active_tab = tab;
        cx.notify();
    }

    pub fn open(&mut self, request: AddPanelRequest, window: &mut Window, cx: &mut Context<Self>) {
        let has_metric_catalog = request.has_metric_catalog;
        self.request = Some(request);
        self.visible = true;
        self.active_tab = AddPanelTab::Saved;
        self.selected_ids.clear();
        self.saved_highlight = 0;
        self.namespace_highlight = 0;
        self.metric_highlight = 0;
        self.metric_namespace_selected = None;
        self.metric_metric_selected = None;
        self.metric_metrics_for_namespace.clear();
        self.query_chart_kind = ChartKind::Line;
        self.metric_statistic = "Average".to_string();

        self.search_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.query_name_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.query_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.metric_name_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.metric_namespace_filter_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.metric_metric_filter_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.metric_period_input.update(cx, |state, cx| {
            state.set_value("60", window, cx);
        });

        let mut subs = Vec::new();
        for input in [
            &self.search_input,
            &self.query_name_input,
            &self.metric_name_input,
            &self.metric_namespace_filter_input,
            &self.metric_metric_filter_input,
            &self.metric_period_input,
        ] {
            let sub = cx.subscribe_in(input, window, |_this, _, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
            });
            subs.push(sub);
        }
        // EditorState is a distinct entity type in 0.6.1, so the query input
        // subscribes separately with the same handler.
        subs.push(cx.subscribe_in(
            &self.query_input,
            window,
            |_this, _, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
            },
        ));
        self._subscriptions = subs;

        let search_focus = self.search_input.read(cx).focus_handle(cx);
        self.focus.focus(Some(&search_focus), window, cx);

        let _ = has_metric_catalog;
        let _ = has_metric_catalog;
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.selected_ids.clear();
        self.metric_namespace_selected = None;
        self.metric_metric_selected = None;
        self.metric_metrics_for_namespace.clear();
        self.focus.restore(cx);
        cx.notify();
    }

    /// Dismiss the picker without adding anything.
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(AddPanelOutcome::Cancelled);
        self.close(cx);
    }

    pub fn toggle_chart(&mut self, chart_id: Uuid, cx: &mut Context<Self>) {
        if let Some(pos) = self.selected_ids.iter().position(|id| *id == chart_id) {
            self.selected_ids.remove(pos);
        } else {
            self.selected_ids.push(chart_id);
        }
        cx.notify();
    }

    pub fn select_namespace(
        &mut self,
        namespace: String,
        cx: &mut Context<Self>,
    ) -> Option<RequestMetricsForNamespace> {
        let profile_id = self.request.as_ref().map(|r| r.profile_id)?;
        self.metric_namespace_selected = Some(namespace.clone());
        self.metric_metric_selected = None;

        let needs_fetch = !self.metric_metrics_for_namespace.contains_key(&namespace);
        cx.notify();

        if needs_fetch {
            Some(RequestMetricsForNamespace {
                profile_id,
                namespace,
            })
        } else {
            None
        }
    }

    /// Cache metrics for a namespace (called by the host after fetching).
    pub fn set_metrics_for_namespace(
        &mut self,
        namespace: String,
        metrics: Vec<MetricDescriptor>,
        cx: &mut Context<Self>,
    ) {
        self.metric_metrics_for_namespace.insert(namespace, metrics);
        cx.notify();
    }

    /// Replace the cached metric namespaces and clear the loading flag.
    ///
    /// Called by the host after the background `list_namespaces()` task
    /// completes (success or failure). Pass an empty vec on failure to keep
    /// the picker out of the "loading" state.
    pub fn set_metric_namespaces(&mut self, namespaces: Vec<String>, cx: &mut Context<Self>) {
        if let Some(req) = self.request.as_mut() {
            req.metric_namespaces = namespaces;
            req.metric_namespaces_loading = false;
            cx.notify();
        }
    }

    /// Returns the submit button label based on the active tab and state.
    pub fn submit_label(&self) -> String {
        submit_label_for(self.active_tab, self.selected_ids.len())
    }

    /// Returns true when the current tab's form passes validation.
    pub fn can_confirm(&self, cx: &App) -> bool {
        match self.active_tab {
            AddPanelTab::Saved => !self.selected_ids.is_empty(),
            AddPanelTab::Query => {
                let name = self.query_name_input.read(cx).value().to_string();
                let query = self.query_input.read(cx).value().to_string();
                query_tab_valid(&name, &query)
            }
            AddPanelTab::Metric => {
                let name = self.metric_name_input.read(cx).value().to_string();
                let period = self.metric_period_input.read(cx).value().to_string();
                metric_tab_valid(
                    &name,
                    self.metric_namespace_selected.is_some(),
                    self.metric_metric_selected.is_some(),
                    &period,
                )
            }
        }
    }

    /// Returns the visible tabs for the current request — filters out
    /// `Metric` when the connection has no metric catalog.
    pub fn visible_tabs(&self) -> Vec<AddPanelTab> {
        let has_metric = self
            .request
            .as_ref()
            .map(|r| r.has_metric_catalog)
            .unwrap_or(false);
        visible_tabs_for(has_metric)
    }

    /// Returns filtered candidates matching the current search query (case-insensitive).
    fn filtered_candidates<'a>(candidates: &'a [SavedChart], query: &str) -> Vec<&'a SavedChart> {
        if query.is_empty() {
            return candidates.iter().collect();
        }
        let lower = query.to_lowercase();
        candidates
            .iter()
            .filter(|c| c.name.to_lowercase().contains(&lower))
            .collect()
    }

    /// Submit the active tab, as the submit button does. Does nothing while
    /// the tab's form is incomplete.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        if !self.can_confirm(cx) {
            return;
        }
        let Some(ref request) = self.request else {
            return;
        };

        match self.active_tab {
            AddPanelTab::Saved => {
                cx.emit(AddPanelOutcome::Confirmed {
                    dashboard_id: request.dashboard_id,
                    chart_ids: self.selected_ids.clone(),
                });
            }
            AddPanelTab::Query => {
                let name = self.query_name_input.read(cx).value().to_string();
                let query = self.query_input.read(cx).value().to_string();
                cx.emit(AddPanelOutcome::CreateFromQuery {
                    dashboard_id: request.dashboard_id,
                    profile_id: request.profile_id,
                    name: name.trim().to_string(),
                    query,
                    chart_kind: self.query_chart_kind,
                });
            }
            AddPanelTab::Metric => {
                let name = self.metric_name_input.read(cx).value().to_string();
                let period = self
                    .metric_period_input
                    .read(cx)
                    .value()
                    .to_string()
                    .trim()
                    .parse::<u32>()
                    .unwrap_or(60);
                let namespace = self.metric_namespace_selected.clone().unwrap_or_default();
                let (metric_name, dimensions) = self
                    .metric_metric_selected
                    .and_then(|idx| {
                        self.metric_metrics_for_namespace
                            .get(&namespace)
                            .and_then(|list| list.get(idx))
                            .map(|m| (m.metric_name.clone(), m.dimensions.clone()))
                    })
                    .unwrap_or_default();

                cx.emit(AddPanelOutcome::CreateFromMetric {
                    dashboard_id: request.dashboard_id,
                    profile_id: request.profile_id,
                    name: name.trim().to_string(),
                    namespace,
                    metric_name,
                    dimensions,
                    period_seconds: period,
                    statistic: self.metric_statistic.clone(),
                });
            }
        }

        self.close(cx);
    }

    // ---- keyboard ----

    /// Ids of the saved charts the search leaves in the list, in order.
    fn filtered_saved_ids(&self, cx: &App) -> Vec<Uuid> {
        let Some(request) = self.request.as_ref() else {
            return Vec::new();
        };
        let query = self.search_input.read(cx).value().to_string();

        Self::filtered_candidates(&request.candidates, &query)
            .into_iter()
            .map(|chart| chart.id)
            .collect()
    }

    /// Namespaces the filter leaves in the namespace list, in order.
    fn filtered_namespaces(&self, cx: &App) -> Vec<String> {
        let Some(request) = self.request.as_ref() else {
            return Vec::new();
        };
        let filter = self
            .metric_namespace_filter_input
            .read(cx)
            .value()
            .to_lowercase();

        request
            .metric_namespaces
            .iter()
            .filter(|namespace| filter.is_empty() || namespace.to_lowercase().contains(&filter))
            .cloned()
            .collect()
    }

    /// Indices (into the cached metric list) the filter leaves in the metric
    /// list, in order.
    fn filtered_metric_indices(&self, cx: &App) -> Vec<usize> {
        let Some(metrics) = self
            .metric_namespace_selected
            .as_ref()
            .and_then(|namespace| self.metric_metrics_for_namespace.get(namespace))
        else {
            return Vec::new();
        };
        let filter = self
            .metric_metric_filter_input
            .read(cx)
            .value()
            .to_lowercase();

        metrics
            .iter()
            .enumerate()
            .filter(|(_, metric)| {
                filter.is_empty()
                    || metric.metric_name.to_lowercase().contains(&filter)
                    || metric.dimensions.iter().any(|(key, value)| {
                        format!("{key}={value}").to_lowercase().contains(&filter)
                    })
            })
            .map(|(index, _)| index)
            .collect()
    }

    /// The Metric tab list the keyboard works: the focused list, or the one
    /// under the focused filter field, otherwise the namespaces.
    fn active_metric_list(&self, window: &Window, cx: &App) -> MetricList {
        let metric_filter_focused = self
            .metric_metric_filter_input
            .read(cx)
            .focus_handle(cx)
            .is_focused(window);

        if self.metric_list_focus.is_focused(window) || metric_filter_focused {
            MetricList::Metrics
        } else {
            MetricList::Namespaces
        }
    }

    /// Moves the keyboard into a list of the Metric tab.
    fn focus_metric_list(&mut self, list: MetricList, window: &mut Window, cx: &mut Context<Self>) {
        match list {
            MetricList::Namespaces => self.namespace_list_focus.focus(window, cx),
            MetricList::Metrics => self.metric_list_focus.focus(window, cx),
        }
        cx.notify();
    }

    /// Selects the tab `delta` places away among the visible ones, wrapping,
    /// and moves the keyboard to its first field: the field that had it is
    /// no longer drawn.
    fn step_tab(&mut self, delta: isize, window: &mut Window, cx: &mut Context<Self>) {
        let tabs = self.visible_tabs();
        let current = tabs
            .iter()
            .position(|tab| *tab == self.active_tab)
            .unwrap_or(0);
        let next = tabs[(current as isize + delta).rem_euclid(tabs.len() as isize) as usize];

        self.set_active_tab(next, cx);

        let first_field = match next {
            AddPanelTab::Saved => self.search_input.clone(),
            AddPanelTab::Query => self.query_name_input.clone(),
            AddPanelTab::Metric => self.metric_name_input.clone(),
        };
        first_field.update(cx, |input, cx| input.focus(window, cx));
    }

    /// Moves the highlighted row of the list the keyboard works by `delta`,
    /// or to its first (`Some(false)`) or last (`Some(true)`) row.
    fn move_highlight(
        &mut self,
        delta: isize,
        to_edge: Option<bool>,
        window: &Window,
        cx: &mut Context<Self>,
    ) {
        let (count, highlight) = match self.active_tab {
            AddPanelTab::Saved => (self.filtered_saved_ids(cx).len(), &mut self.saved_highlight),
            AddPanelTab::Metric => match self.active_metric_list(window, cx) {
                MetricList::Namespaces => (
                    self.filtered_namespaces(cx).len(),
                    &mut self.namespace_highlight,
                ),
                MetricList::Metrics => (
                    self.filtered_metric_indices(cx).len(),
                    &mut self.metric_highlight,
                ),
            },
            AddPanelTab::Query => return,
        };

        if count == 0 {
            return;
        }

        *highlight = match to_edge {
            Some(true) => count - 1,
            Some(false) => 0,
            None => (*highlight)
                .min(count - 1)
                .saturating_add_signed(delta)
                .min(count - 1),
        };
        cx.notify();
    }

    /// Space: checks or unchecks the highlighted chart, or picks the
    /// highlighted namespace or metric.
    fn toggle_highlighted(&mut self, window: &Window, cx: &mut Context<Self>) {
        match self.active_tab {
            AddPanelTab::Saved => {
                if let Some(id) = self
                    .filtered_saved_ids(cx)
                    .get(self.saved_highlight)
                    .copied()
                {
                    self.toggle_chart(id, cx);
                }
            }
            AddPanelTab::Metric => self.pick_highlighted_metric_row(window, cx),
            AddPanelTab::Query => {}
        }
    }

    fn pick_highlighted_metric_row(&mut self, window: &Window, cx: &mut Context<Self>) {
        match self.active_metric_list(window, cx) {
            MetricList::Namespaces => {
                let namespace = self
                    .filtered_namespaces(cx)
                    .get(self.namespace_highlight)
                    .cloned();

                if let Some(namespace) = namespace {
                    self.metric_highlight = 0;
                    if let Some(request) = self.select_namespace(namespace, cx) {
                        cx.emit(request);
                    }
                }
            }
            MetricList::Metrics => {
                if let Some(index) = self
                    .filtered_metric_indices(cx)
                    .get(self.metric_highlight)
                    .copied()
                {
                    self.metric_metric_selected = Some(index);
                    cx.notify();
                }
            }
        }
    }

    /// Whether Enter has something to do: the tab's form is complete, or a
    /// highlighted row Enter would pick first.
    fn can_confirm_from_keys(&self, cx: &App) -> bool {
        self.can_confirm(cx)
            || match self.active_tab {
                AddPanelTab::Saved => {
                    self.selected_ids.is_empty() && !self.filtered_saved_ids(cx).is_empty()
                }
                AddPanelTab::Metric => true,
                AddPanelTab::Query => false,
            }
    }

    /// Enter: in the chart list with nothing checked, adds the highlighted
    /// chart; in a Metric tab list, picks its highlighted row first. Then
    /// submits, as the submit button does, once the form is complete.
    fn confirm_from_keys(&mut self, window: &Window, cx: &mut Context<Self>) {
        match self.active_tab {
            AddPanelTab::Saved if self.selected_ids.is_empty() => {
                if let Some(id) = self
                    .filtered_saved_ids(cx)
                    .get(self.saved_highlight)
                    .copied()
                {
                    self.selected_ids.push(id);
                }
            }
            AddPanelTab::Metric
                if self.namespace_list_focus.is_focused(window)
                    || self.metric_list_focus.is_focused(window) =>
            {
                self.pick_highlighted_metric_row(window, cx);
            }
            _ => {}
        }

        self.confirm(cx);
    }

    /// `/`: moves the keyboard to the search or filter field of the tab.
    fn focus_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = match self.active_tab {
            AddPanelTab::Saved => self.search_input.clone(),
            AddPanelTab::Query => self.query_name_input.clone(),
            AddPanelTab::Metric => match self.active_metric_list(window, cx) {
                MetricList::Namespaces => self.metric_namespace_filter_input.clone(),
                MetricList::Metrics => self.metric_metric_filter_input.clone(),
            },
        };

        input.update(cx, |input, cx| input.focus(window, cx));
    }

    /// Runs a keymap command of the dialog's own layer (see the module's
    /// `AddPanelPicker` keys). Returns whether it applied.
    fn run_key(&mut self, command: Command, window: &mut Window, cx: &mut Context<Self>) -> bool {
        match command {
            Command::NextPanelTab => self.step_tab(1, window, cx),
            Command::PrevPanelTab => self.step_tab(-1, window, cx),
            Command::SelectNext => self.move_highlight(1, None, window, cx),
            Command::SelectPrev => self.move_highlight(-1, None, window, cx),
            Command::SelectFirst => self.move_highlight(0, Some(false), window, cx),
            Command::SelectLast => self.move_highlight(0, Some(true), window, cx),
            Command::ExpandCollapse => self.toggle_highlighted(window, cx),
            Command::FocusSearch => self.focus_search(window, cx),
            // Only the Metric tab has two lists; elsewhere the keys do
            // nothing rather than reach the dashboard behind the dialog.
            Command::ColumnLeft | Command::ColumnRight => {
                if self.active_tab == AddPanelTab::Metric {
                    let list = if command == Command::ColumnLeft {
                        MetricList::Namespaces
                    } else {
                        MetricList::Metrics
                    };
                    self.focus_metric_list(list, window, cx);
                }
            }
            _ => return false,
        }

        true
    }

    fn render_chart_row(
        chart_id: uuid::Uuid,
        chart_name: String,
        is_selected: bool,
        is_highlighted: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();

        let checkbox_bg = if is_selected {
            theme.primary
        } else {
            theme.background
        };
        let checkbox_border = if is_selected {
            theme.primary
        } else {
            theme.input
        };

        let mut checkbox = div()
            .w(Heights::ICON_SM)
            .h(Heights::ICON_SM)
            .border_1()
            .border_color(checkbox_border)
            .rounded_sm()
            .flex()
            .items_center()
            .justify_center()
            .bg(checkbox_bg);

        if is_selected {
            checkbox = checkbox.child(
                dbflux_components::primitives::Text::caption("✓").color(theme.primary_foreground),
            );
        }

        let row_bg = if is_selected {
            theme.accent.opacity(0.18)
        } else {
            gpui::transparent_black()
        };
        let hover_bg = theme.secondary;

        let row_id = ("add-panel-chart-row", chart_id.as_u128() as u64);

        div()
            .id(row_id)
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .rounded(dbflux_components::tokens::Radii::SM)
            .bg(row_bg)
            .border_1()
            .border_color(if is_highlighted {
                theme.ring
            } else {
                gpui::transparent_black()
            })
            .when(!is_selected, |el| el.hover(move |d| d.bg(hover_bg)))
            .cursor_pointer()
            .on_click(cx.listener(move |this, _, _, cx| {
                this.toggle_chart(chart_id, cx);
            }))
            .child(checkbox.into_any_element())
            .child(div().flex_1().text_sm().child(chart_name))
            .into_any_element()
    }

    fn render_tab_strip(&self, cx: &mut Context<Self>) -> AnyElement {
        let visible = self.visible_tabs();
        let active = self.active_tab;

        let tabs: Vec<AnyElement> = visible
            .into_iter()
            .map(|tab| {
                let label = match tab {
                    AddPanelTab::Saved => dbflux_i18n::t!("modals.add_panel_picker.tab.saved"),
                    AddPanelTab::Query => dbflux_i18n::t!("modals.add_panel_picker.tab.query"),
                    AddPanelTab::Metric => dbflux_i18n::t!("modals.add_panel_picker.tab.metric"),
                };
                let id = match tab {
                    AddPanelTab::Saved => "add-panel-tab-saved",
                    AddPanelTab::Query => "add-panel-tab-query",
                    AddPanelTab::Metric => "add-panel-tab-metric",
                };
                let is_active = tab == active;

                inline_tab(id, is_active, cx)
                    .role(gpui::Role::Tab)
                    .aria_selected(is_active)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.set_active_tab(tab, cx);
                    }))
                    .child(Text::body(label))
                    .into_any_element()
            })
            .collect();

        inline_tab_bar(cx)
            .id("add-panel-tab-list")
            .role(gpui::Role::TabList)
            .children(tabs)
            .into_any_element()
    }

    fn render_saved_tab(&self, candidates: &[SavedChart], cx: &mut Context<Self>) -> AnyElement {
        if candidates.is_empty() {
            return Text::body(dbflux_i18n::t!("modals.add_panel_picker.saved.empty"))
                .into_any_element();
        }

        let query = self.search_input.read(cx).value().to_string();
        let filtered = Self::filtered_candidates(candidates, &query);
        let selected_ids = self.selected_ids.clone();

        let search_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body(dbflux_i18n::t!(
                    "modals.add_panel_picker.saved.search_label"
                ))
                .into_any_element(),
            )
            .child(Input::new(&self.search_input))
            .into_any_element();

        let highlight = self.saved_highlight.min(filtered.len().saturating_sub(1));
        let chart_rows: Vec<AnyElement> = filtered
            .iter()
            .enumerate()
            .map(|(row, chart)| {
                let is_selected = selected_ids.contains(&chart.id);
                Self::render_chart_row(
                    chart.id,
                    chart.name.clone(),
                    is_selected,
                    row == highlight,
                    cx,
                )
            })
            .collect();

        // A tab stop after the search: J / K, Space and Enter work the list
        // from here, and the arrows work it from the search too.
        let chart_list = div()
            .id("add-panel-chart-list")
            .track_focus(&self.saved_list_focus)
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .children(chart_rows)
            .into_any_element();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(search_row)
            .child(chart_list)
            .into_any_element()
    }

    fn render_chart_kind_picker(&self, cx: &mut Context<Self>) -> AnyElement {
        let kinds = [
            (ChartKind::Line, "add-panel-kind-line"),
            (ChartKind::Bar, "add-panel-kind-bar"),
            (ChartKind::Area, "add-panel-kind-area"),
            (ChartKind::Scatter, "add-panel-kind-scatter"),
        ];
        let current = self.query_chart_kind;

        let buttons: Vec<AnyElement> = kinds
            .into_iter()
            .map(|(kind, id)| {
                let label = chart_kind_label(kind);
                let is_active = kind == current;
                let mut btn =
                    Button::new(id, label).on_click(cx.listener(move |this, _, _, cx| {
                        this.query_chart_kind = kind;
                        cx.notify();
                    }));
                if is_active {
                    btn = btn.primary();
                } else {
                    btn = btn.ghost();
                }
                btn.into_any_element()
            })
            .collect();

        div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .children(buttons)
            .into_any_element()
    }

    fn render_query_tab(&self, cx: &mut Context<Self>) -> AnyElement {
        // Vim's listeners on the query editor's container. In Insert mode the
        // dialog's Escape leaves Insert mode instead of cancelling, and in
        // Normal mode Enter moves down instead of submitting.
        let query_container = VimBinding::wire(div(), cx);
        let query_container = VimBinding::capture_action::<dbflux_components::actions::Cancel, _>(
            query_container,
            cx,
        );
        let query_container = VimBinding::capture_action::<dbflux_components::actions::Execute, _>(
            query_container,
            cx,
        );

        let name_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body(dbflux_i18n::t!("modals.add_panel_picker.name_label"))
                    .into_any_element(),
            )
            .child(Input::new(&self.query_name_input))
            .into_any_element();

        let theme = cx.theme();
        let query_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body(dbflux_i18n::t!("modals.add_panel_picker.query.label"))
                    .into_any_element(),
            )
            .child(
                query_container
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::SM)
                    .bg(theme.background)
                    .h(px(320.0))
                    .p(Spacing::SM)
                    .overflow_hidden()
                    .child(
                        self.query_vim
                            .editor(false)
                            .w_full()
                            .h_full()
                            .font_family(AppFonts::MONO)
                            .font_weight(FontWeight::MEDIUM)
                            .text_size(FontSizes::BASE),
                    )
                    .into_any_element(),
            )
            .children(self.query_vim.render_indicator(cx))
            .into_any_element();

        let kind_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body(dbflux_i18n::t!("modals.add_panel_picker.chart_kind.label"))
                    .into_any_element(),
            )
            .child(self.render_chart_kind_picker(cx))
            .into_any_element();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::LG)
            .child(name_row)
            .child(query_row)
            .child(kind_row)
            .into_any_element()
    }

    fn render_namespace_list(&self, cx: &mut Context<Self>) -> AnyElement {
        let Some(request) = self.request.as_ref() else {
            return div().into_any_element();
        };
        let theme = cx.theme();

        // While the host is still fetching the namespace list show a clear
        // in-modal loading state. Users opened the modal expecting feedback;
        // an empty list with no signal was previously indistinguishable from
        // "this connection has no metrics".
        if request.metric_namespaces_loading {
            return div()
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::SM)
                .h(px(260.0))
                .flex()
                .items_center()
                .justify_center()
                .child(Text::caption(dbflux_i18n::t!(
                    "modals.add_panel_picker.metric.loading_namespaces"
                )))
                .into_any_element();
        }

        let filter = self
            .metric_namespace_filter_input
            .read(cx)
            .value()
            .to_string()
            .to_lowercase();
        let selected = self.metric_namespace_selected.clone();

        let highlight = self.namespace_highlight;
        let rows: Vec<AnyElement> = request
            .metric_namespaces
            .iter()
            .filter(|ns| filter.is_empty() || ns.to_lowercase().contains(&filter))
            .cloned()
            .enumerate()
            .map(|(row, ns)| {
                let is_selected = selected.as_deref() == Some(ns.as_str());
                let is_highlighted = row == highlight;
                let ns_for_listener = ns.clone();
                div()
                    .id(gpui::ElementId::Name(
                        format!("add-panel-namespace-{ns}").into(),
                    ))
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .text_sm()
                    .cursor_pointer()
                    .border_1()
                    .border_color(if is_highlighted {
                        theme.ring
                    } else {
                        gpui::transparent_black()
                    })
                    .when(is_selected, |el| {
                        el.bg(theme.accent)
                            .text_color(theme.accent_foreground)
                            .font_weight(FontWeight::SEMIBOLD)
                    })
                    .when(!is_selected, |el| el.hover(|s| s.bg(theme.muted)))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if let Some(req) = this.select_namespace(ns_for_listener.clone(), cx) {
                            cx.emit(req);
                        }
                    }))
                    .child(ns)
                    .into_any_element()
            })
            .collect();

        div()
            .id("add-panel-namespace-list")
            .track_focus(&self.namespace_list_focus)
            .border_1()
            .border_color(theme.border)
            .rounded(Radii::SM)
            .h(px(260.0))
            .overflow_y_scrollbar()
            .children(rows)
            .into_any_element()
    }

    fn render_metric_list(&self, cx: &mut Context<Self>) -> AnyElement {
        let Some(namespace) = self.metric_namespace_selected.clone() else {
            return Text::body(dbflux_i18n::t!(
                "modals.add_panel_picker.metric.select_namespace_hint"
            ))
            .into_any_element();
        };

        let theme = cx.theme();
        let Some(metrics) = self.metric_metrics_for_namespace.get(&namespace) else {
            return div()
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::SM)
                .h(px(260.0))
                .p(Spacing::SM)
                .child(
                    Text::body(dbflux_i18n::t!("modals.add_panel_picker.metric.loading"))
                        .into_any_element(),
                )
                .into_any_element();
        };

        if metrics.is_empty() {
            return div()
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::SM)
                .h(px(260.0))
                .p(Spacing::SM)
                .child(
                    Text::body(dbflux_i18n::t!("modals.add_panel_picker.metric.no_metrics"))
                        .into_any_element(),
                )
                .into_any_element();
        }

        // Apply the user-supplied filter (case-insensitive substring match)
        // against the metric name AND against each `key=value` dimension pair.
        // We preserve the original index so selection state still maps onto
        // the cached `metric_metrics_for_namespace` entry.
        let filter = self
            .metric_metric_filter_input
            .read(cx)
            .value()
            .to_string()
            .to_lowercase();

        let filtered: Vec<(usize, &MetricDescriptor)> = metrics
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                if filter.is_empty() {
                    return true;
                }
                if m.metric_name.to_lowercase().contains(&filter) {
                    return true;
                }
                m.dimensions
                    .iter()
                    .any(|(k, v)| format!("{k}={v}").to_lowercase().contains(&filter))
            })
            .collect();

        if filtered.is_empty() {
            return div()
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::SM)
                .h(px(260.0))
                .p(Spacing::SM)
                .child(
                    Text::body(dbflux_i18n::t!(
                        "modals.add_panel_picker.metric.no_metrics_filtered"
                    ))
                    .into_any_element(),
                )
                .into_any_element();
        }

        let selected = self.metric_metric_selected;
        let highlight = self.metric_highlight;
        let rows: Vec<AnyElement> = filtered
            .iter()
            .enumerate()
            .map(|(row, (idx, m))| {
                let idx = *idx;
                let is_selected = selected == Some(idx);
                let is_highlighted = row == highlight;
                let dim_summary = if m.dimensions.is_empty() {
                    String::new()
                } else {
                    let parts: Vec<String> = m
                        .dimensions
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect();
                    format!("  [{}]", parts.join(", "))
                };
                let label = format!("{}{}", m.metric_name, dim_summary);

                div()
                    .id(("add-panel-metric-row", idx))
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .text_sm()
                    .cursor_pointer()
                    .border_1()
                    .border_color(if is_highlighted {
                        theme.ring
                    } else {
                        gpui::transparent_black()
                    })
                    .when(is_selected, |el| {
                        el.bg(theme.accent)
                            .text_color(theme.accent_foreground)
                            .font_weight(FontWeight::SEMIBOLD)
                    })
                    .when(!is_selected, |el| el.hover(|s| s.bg(theme.muted)))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.metric_metric_selected = Some(idx);
                        cx.notify();
                    }))
                    .child(label)
                    .into_any_element()
            })
            .collect();

        div()
            .id("add-panel-metric-list")
            .track_focus(&self.metric_list_focus)
            .border_1()
            .border_color(theme.border)
            .rounded(Radii::SM)
            .h(px(260.0))
            .overflow_y_scrollbar()
            .children(rows)
            .into_any_element()
    }

    fn render_metric_tab(&self, cx: &mut Context<Self>) -> AnyElement {
        let name_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body(dbflux_i18n::t!("modals.add_panel_picker.name_label"))
                    .into_any_element(),
            )
            .child(Input::new(&self.metric_name_input))
            .into_any_element();

        // Two-column row: Namespace (with filter) on the left, Metric on the right.
        let namespace_column = div()
            .flex()
            .flex_1()
            .min_w_0()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body(dbflux_i18n::t!(
                    "modals.add_panel_picker.metric.namespace_label"
                ))
                .into_any_element(),
            )
            .child(Input::new(&self.metric_namespace_filter_input))
            .child(self.render_namespace_list(cx))
            .into_any_element();

        let metric_column = div()
            .flex()
            .flex_1()
            .min_w_0()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body(dbflux_i18n::t!(
                    "modals.add_panel_picker.metric.metric_label"
                ))
                .into_any_element(),
            )
            .child(Input::new(&self.metric_metric_filter_input))
            .child(self.render_metric_list(cx))
            .into_any_element();

        let picker_row = div()
            .flex()
            .items_start()
            .gap(Spacing::MD)
            .child(namespace_column)
            .child(metric_column)
            .into_any_element();

        let period_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .w(px(180.0))
            .child(
                Text::body(dbflux_i18n::t!(
                    "modals.add_panel_picker.metric.period_label"
                ))
                .into_any_element(),
            )
            .child(Input::new(&self.metric_period_input))
            .into_any_element();

        let current_stat = self.metric_statistic.clone();
        let stat_buttons: Vec<AnyElement> = METRIC_STATISTICS
            .iter()
            .map(|s| {
                let s_owned = s.to_string();
                let is_active = current_stat == *s;
                let id_string: SharedString = format!("add-panel-stat-{}", s).into();
                let mut btn =
                    Button::new(id_string, *s).on_click(cx.listener(move |this, _, _, cx| {
                        this.metric_statistic = s_owned.clone();
                        cx.notify();
                    }));
                if is_active {
                    btn = btn.primary();
                } else {
                    btn = btn.ghost();
                }
                btn.into_any_element()
            })
            .collect();

        let stat_column = div()
            .flex()
            .flex_1()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body(dbflux_i18n::t!(
                    "modals.add_panel_picker.metric.statistic_label"
                ))
                .into_any_element(),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .children(stat_buttons)
                    .into_any_element(),
            )
            .into_any_element();

        let period_stat_row = div()
            .flex()
            .items_end()
            .gap(Spacing::MD)
            .child(period_row)
            .child(stat_column)
            .into_any_element();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::LG)
            .child(name_row)
            .child(picker_row)
            .child(period_stat_row)
            .into_any_element()
    }
}

impl EventEmitter<AddPanelOutcome> for ModalAddPanelPicker {}
impl EventEmitter<RequestMetricsForNamespace> for ModalAddPanelPicker {}

impl Render for ModalAddPanelPicker {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let candidates = request.candidates.clone();
        let submit_label = self.submit_label();
        let can_confirm = self.can_confirm(cx);
        let can_confirm_from_keys = self.can_confirm_from_keys(cx);

        let tab_strip = self.render_tab_strip(cx);
        let body_inner: AnyElement = match self.active_tab {
            AddPanelTab::Saved => self.render_saved_tab(&candidates, cx),
            AddPanelTab::Query => self.render_query_tab(cx),
            AddPanelTab::Metric => self.render_metric_tab(cx),
        };

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::LG)
            .child(tab_strip)
            .child(body_inner)
            .into_any_element();

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| this.cancel(cx));
        let on_confirm = cx.listener(|this, _: &gpui::ClickEvent, _, cx| this.confirm(cx));

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "add-panel-cancel",
                    dbflux_i18n::t!("modals.add_panel_picker.cancel"),
                )
                .on_click(on_cancel),
            )
            .child(
                Button::new("add-panel-confirm", submit_label)
                    .primary()
                    .disabled(!can_confirm)
                    .on_click(on_confirm),
            );

        let modal = Modal::new(dbflux_i18n::t!("modals.add_panel_picker.title"))
            .body(body)
            .footer(footer)
            .icon(AppIcon::Plus)
            .width(gpui::px(900.0))
            .focus_handle(self.focus.handle())
            .key_context(ContextId::AddPanelPicker.as_gpui_context())
            .on_close({
                let entity = cx.entity().downgrade();
                move |_, cx| {
                    entity.update(cx, |this, cx| this.cancel(cx)).log_err();
                }
            })
            .on_confirm({
                let entity = cx.entity().downgrade();
                move |window, cx| {
                    entity
                        .update(cx, |this, cx| this.confirm_from_keys(window, cx))
                        .log_err();
                }
            })
            .confirm_enabled(can_confirm_from_keys);

        // The dialog's keys: its own layer's commands, and the arrow and
        // Home / End keys the modal answers only when it scrolls, which move
        // through the lists here (also from the search field).
        div()
            .id("add-panel-picker")
            .absolute()
            .inset_0()
            .on_action(cx.listener(|this, action: &RunCommand, window, cx| {
                let handled =
                    run_command(action).is_some_and(|command| this.run_key(command, window, cx));

                if !handled {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &ScrollDown, window, cx| {
                this.move_highlight(1, None, window, cx);
            }))
            .on_action(cx.listener(|this, _: &ScrollUp, window, cx| {
                this.move_highlight(-1, None, window, cx);
            }))
            .on_action(cx.listener(|this, _: &ScrollToTop, window, cx| {
                this.move_highlight(0, Some(false), window, cx);
            }))
            .on_action(cx.listener(|this, _: &ScrollToBottom, window, cx| {
                this.move_highlight(0, Some(true), window, cx);
            }))
            .child(modal)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_components::chart::ChartSpec;
    use dbflux_components::saved_chart::{SavedChart, SavedChartRefreshPolicy, SavedChartSource};

    fn test_uuid() -> Uuid {
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    fn make_chart(name: &str, profile_id: Uuid) -> SavedChart {
        let chart_spec: ChartSpec = serde_json::from_str(
            r#"{"x_axis":{"column_index":0,"label":"t","kind":"Time","unit":null},"series":[]}"#,
        )
        .unwrap();

        SavedChart {
            id: Uuid::new_v4(),
            name: name.to_string(),
            profile_id,
            source: SavedChartSource::Query {
                query: "SELECT 1".to_string(),
            },
            chart_spec,
            bindings: Default::default(),
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    // ----- existing-style tests -----

    #[test]
    fn submit_label_updates_live_based_on_selection_count() {
        let ids_0: Vec<Uuid> = vec![];
        let ids_1 = [Uuid::new_v4()];
        let ids_3 = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        assert_eq!(
            submit_label_for(AddPanelTab::Saved, ids_0.len()),
            dbflux_i18n::t!("modals.add_panel_picker.submit.zero")
        );
        assert_eq!(
            submit_label_for(AddPanelTab::Saved, ids_1.len()),
            dbflux_i18n::t!("modals.add_panel_picker.submit.one")
        );
        assert_eq!(
            submit_label_for(AddPanelTab::Saved, ids_3.len()),
            dbflux_i18n::t!("modals.add_panel_picker.submit.many", count = ids_3.len())
        );
    }

    #[test]
    fn modal_add_panel_picker_filter_is_case_insensitive() {
        let profile_id = test_uuid();
        let candidates = vec![
            make_chart("Foo metric", profile_id),
            make_chart("FOO dashboard", profile_id),
            make_chart("bar chart", profile_id),
        ];

        let filtered = ModalAddPanelPicker::filtered_candidates(&candidates, "foo");
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|c| c.name == "Foo metric"));
        assert!(filtered.iter().any(|c| c.name == "FOO dashboard"));
    }

    // ----- new tests for multi-tab behaviour (pure helpers — no GPUI required) -----

    #[test]
    fn submit_label_per_tab_pure() {
        let zero = dbflux_i18n::t!("modals.add_panel_picker.submit.zero");
        let one = dbflux_i18n::t!("modals.add_panel_picker.submit.one");
        let many_5 = dbflux_i18n::t!("modals.add_panel_picker.submit.many", count = 5);
        let create = dbflux_i18n::t!("modals.add_panel_picker.submit.create");

        assert_eq!(submit_label_for(AddPanelTab::Saved, 0), zero);
        assert_eq!(submit_label_for(AddPanelTab::Saved, 1), one);
        assert_eq!(submit_label_for(AddPanelTab::Saved, 5), many_5);
        assert_eq!(submit_label_for(AddPanelTab::Query, 0), create);
        assert_eq!(submit_label_for(AddPanelTab::Query, 99), create);
        assert_eq!(submit_label_for(AddPanelTab::Metric, 0), create);
    }

    #[test]
    fn query_tab_rejects_empty_name_or_query() {
        assert!(!query_tab_valid("", ""), "empty must be invalid");
        assert!(!query_tab_valid("Panel A", ""), "name only is not enough");
        assert!(!query_tab_valid("", "SELECT 1"), "query only is not enough");
        assert!(
            query_tab_valid("Panel A", "SELECT 1"),
            "name + query must be valid"
        );
        assert!(
            !query_tab_valid("   ", "SELECT 1"),
            "whitespace-only name must be invalid"
        );
        assert!(
            !query_tab_valid("Panel A", "  \n  "),
            "whitespace-only query must be invalid"
        );
    }

    #[test]
    fn metric_tab_rejects_unfinished_form() {
        assert!(!metric_tab_valid("", false, false, "60"));
        assert!(
            !metric_tab_valid("My metric", false, false, "60"),
            "namespace missing"
        );
        assert!(
            !metric_tab_valid("My metric", true, false, "60"),
            "metric missing"
        );
        assert!(metric_tab_valid("My metric", true, true, "60"), "all set");
        assert!(
            !metric_tab_valid("My metric", true, true, "0"),
            "period must be > 0"
        );
        assert!(!metric_tab_valid("My metric", true, true, "not a number"));
        assert!(metric_tab_valid("My metric", true, true, "300"));
        assert!(
            !metric_tab_valid("   ", true, true, "60"),
            "whitespace-only name"
        );
    }

    #[test]
    fn metric_tab_hidden_without_capability() {
        let with_metric = visible_tabs_for(true);
        assert_eq!(with_metric.len(), 3);
        assert!(with_metric.contains(&AddPanelTab::Metric));

        let without_metric = visible_tabs_for(false);
        assert_eq!(without_metric, vec![AddPanelTab::Saved, AddPanelTab::Query]);
        assert!(!without_metric.contains(&AddPanelTab::Metric));
    }

    #[test]
    fn metric_descriptor_cache_keys_by_namespace() {
        // Validates the data shape we cache in `metric_metrics_for_namespace`:
        // a HashMap keyed by namespace can hold multiple namespaces concurrently
        // (no overwrite when keys differ).
        let mut cache: std::collections::HashMap<String, Vec<MetricDescriptor>> =
            std::collections::HashMap::new();

        cache.insert(
            "AWS/EC2".to_string(),
            vec![MetricDescriptor {
                metric_name: "CPUUtilization".to_string(),
                dimensions: vec![],
            }],
        );
        cache.insert(
            "AWS/RDS".to_string(),
            vec![MetricDescriptor {
                metric_name: "CPUUtilization".to_string(),
                dimensions: vec![("DBInstanceIdentifier".to_string(), "db-1".to_string())],
            }],
        );

        assert!(cache.contains_key("AWS/EC2"));
        assert!(cache.contains_key("AWS/RDS"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn outcome_variants_are_distinct() {
        // Pins the AddPanelOutcome enum shape so future refactors don't
        // silently collapse the three paths back into one.
        let dashboard_id = test_uuid();
        let profile_id = Uuid::new_v4();

        let confirmed = AddPanelOutcome::Confirmed {
            dashboard_id,
            chart_ids: vec![Uuid::new_v4()],
        };
        let from_query = AddPanelOutcome::CreateFromQuery {
            dashboard_id,
            profile_id,
            name: "Panel A".to_string(),
            query: "SELECT 1".to_string(),
            chart_kind: ChartKind::Bar,
        };
        let from_metric = AddPanelOutcome::CreateFromMetric {
            dashboard_id,
            profile_id,
            name: "CPU panel".to_string(),
            namespace: "AWS/EC2".to_string(),
            metric_name: "CPUUtilization".to_string(),
            dimensions: vec![("InstanceId".to_string(), "i-abc".to_string())],
            period_seconds: 120,
            statistic: "Sum".to_string(),
        };
        let cancelled = AddPanelOutcome::Cancelled;

        // Match exhaustively so adding a future variant forces a compile error.
        for outcome in [&confirmed, &from_query, &from_metric, &cancelled] {
            match outcome {
                AddPanelOutcome::Confirmed { .. }
                | AddPanelOutcome::CreateFromQuery { .. }
                | AddPanelOutcome::CreateFromMetric { .. }
                | AddPanelOutcome::Cancelled => {}
            }
        }

        // Field round-trip on CreateFromQuery.
        if let AddPanelOutcome::CreateFromQuery {
            dashboard_id: d,
            profile_id: p,
            name,
            query,
            chart_kind,
        } = &from_query
        {
            assert_eq!(*d, dashboard_id);
            assert_eq!(*p, profile_id);
            assert_eq!(name, "Panel A");
            assert_eq!(query, "SELECT 1");
            assert_eq!(*chart_kind, ChartKind::Bar);
        } else {
            panic!("from_query did not match CreateFromQuery");
        }

        // Field round-trip on CreateFromMetric.
        if let AddPanelOutcome::CreateFromMetric {
            dashboard_id: d,
            profile_id: p,
            name,
            namespace,
            metric_name,
            dimensions,
            period_seconds,
            statistic,
        } = &from_metric
        {
            assert_eq!(*d, dashboard_id);
            assert_eq!(*p, profile_id);
            assert_eq!(name, "CPU panel");
            assert_eq!(namespace, "AWS/EC2");
            assert_eq!(metric_name, "CPUUtilization");
            assert_eq!(dimensions.len(), 1);
            assert_eq!(*period_seconds, 120u32);
            assert_eq!(statistic, "Sum");
        } else {
            panic!("from_metric did not match CreateFromMetric");
        }
    }

    #[test]
    fn request_metrics_for_namespace_carries_profile_and_namespace() {
        let profile_id = Uuid::new_v4();
        let ev = RequestMetricsForNamespace {
            profile_id,
            namespace: "AWS/EC2".to_string(),
        };
        assert_eq!(ev.profile_id, profile_id);
        assert_eq!(ev.namespace, "AWS/EC2");
    }

    // ----- i18n catalog coverage -----

    const ADD_PANEL_PICKER_KEYS: &[&str] = &[
        "modals.add_panel_picker.title",
        "modals.add_panel_picker.submit.zero",
        "modals.add_panel_picker.submit.one",
        "modals.add_panel_picker.submit.many",
        "modals.add_panel_picker.submit.create",
        "modals.add_panel_picker.saved.empty",
        "modals.add_panel_picker.saved.search_label",
        "modals.add_panel_picker.saved.search_placeholder",
        "modals.add_panel_picker.tab.saved",
        "modals.add_panel_picker.tab.query",
        "modals.add_panel_picker.tab.metric",
        "modals.add_panel_picker.name_placeholder",
        "modals.add_panel_picker.name_label",
        "modals.add_panel_picker.query.placeholder",
        "modals.add_panel_picker.query.label",
        "modals.add_panel_picker.chart_kind.label",
        "modals.add_panel_picker.chart_kind.line",
        "modals.add_panel_picker.chart_kind.bar",
        "modals.add_panel_picker.chart_kind.area",
        "modals.add_panel_picker.chart_kind.scatter",
        "modals.add_panel_picker.metric.namespace_filter_placeholder",
        "modals.add_panel_picker.metric.metric_filter_placeholder",
        "modals.add_panel_picker.metric.namespace_label",
        "modals.add_panel_picker.metric.metric_label",
        "modals.add_panel_picker.metric.period_label",
        "modals.add_panel_picker.metric.statistic_label",
        "modals.add_panel_picker.metric.loading_namespaces",
        "modals.add_panel_picker.metric.select_namespace_hint",
        "modals.add_panel_picker.metric.loading",
        "modals.add_panel_picker.metric.no_metrics",
        "modals.add_panel_picker.metric.no_metrics_filtered",
        "modals.add_panel_picker.cancel",
    ];

    #[test]
    fn add_panel_picker_catalog_keys_resolve() {
        for key in ADD_PANEL_PICKER_KEYS {
            let en = dbflux_i18n::t!(key);
            assert!(!en.is_empty(), "empty English translation for {key}");
            assert_ne!(en, *key, "missing English translation for {key}");

            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!es.is_empty(), "empty Spanish translation for {key}");
            assert_ne!(es, *key, "missing Spanish translation for {key}");
        }
    }

    #[test]
    fn chart_kind_labels_resolve() {
        for kind in [
            ChartKind::Line,
            ChartKind::Bar,
            ChartKind::Area,
            ChartKind::Scatter,
        ] {
            let key = chart_kind_label_key(kind);
            let en = dbflux_i18n::t!(key);
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty());
            assert!(!es.is_empty());
            assert_ne!(en, es, "{kind:?} label should differ between locales");
        }
    }

    #[test]
    fn add_panel_picker_title_differs_between_locales() {
        let en = dbflux_i18n::t!("modals.add_panel_picker.title");
        let es = dbflux_i18n::t!("modals.add_panel_picker.title", locale = "es");
        assert_ne!(en, es);
    }

    #[test]
    fn submit_label_for_zero_uses_zero_bucket() {
        assert_eq!(
            submit_label_for(AddPanelTab::Saved, 0),
            dbflux_i18n::t!("modals.add_panel_picker.submit.zero")
        );
    }

    #[test]
    fn submit_label_for_one_uses_one_bucket() {
        assert_eq!(
            submit_label_for(AddPanelTab::Saved, 1),
            dbflux_i18n::t!("modals.add_panel_picker.submit.one")
        );
    }

    #[test]
    fn submit_label_for_many_interpolates_count() {
        let label = submit_label_for(AddPanelTab::Saved, 5);
        assert_eq!(
            label,
            dbflux_i18n::t!("modals.add_panel_picker.submit.many", count = 5)
        );
        assert!(label.contains('5'));
    }

    #[test]
    fn metric_statistics_are_not_translated() {
        assert_eq!(
            METRIC_STATISTICS,
            ["Average", "Sum", "Minimum", "Maximum", "SampleCount"]
        );
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{AddPanelOutcome, AddPanelRequest, AddPanelTab, ModalAddPanelPicker};
    use crate::modals::test_host::{click_backdrop, has_focus, host_modal};
    use dbflux_components::chart::ChartSpec;
    use dbflux_components::saved_chart::{SavedChart, SavedChartRefreshPolicy, SavedChartSource};
    use gpui::{Entity, FocusHandle, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;
    use uuid::Uuid;

    type Outcomes = Rc<RefCell<Vec<AddPanelOutcome>>>;

    fn chart() -> SavedChart {
        let chart_spec: ChartSpec = serde_json::from_str(
            r#"{"x_axis":{"column_index":0,"label":"t","kind":"Time","unit":null},"series":[]}"#,
        )
        .expect("valid chart spec");

        SavedChart {
            id: Uuid::nil(),
            name: "Latency".to_string(),
            profile_id: Uuid::nil(),
            source: SavedChartSource::Query {
                query: "SELECT 1".to_string(),
            },
            chart_spec,
            bindings: Default::default(),
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    fn open_modal(
        cx: &mut TestAppContext,
    ) -> (
        Entity<ModalAddPanelPicker>,
        FocusHandle,
        &mut VisualTestContext,
        Outcomes,
    ) {
        let (modal, outside, window) = host_modal(cx, ModalAddPanelPicker::new);

        let outcomes: Outcomes = Rc::default();
        window.update(|window, cx| {
            let sink = outcomes.clone();
            cx.subscribe(&modal, move |_, outcome: &AddPanelOutcome, _| {
                sink.borrow_mut().push(outcome.clone());
            })
            .detach();

            modal.update(cx, |modal, cx| {
                modal.open(
                    AddPanelRequest {
                        dashboard_id: Uuid::nil(),
                        profile_id: Uuid::nil(),
                        candidates: vec![chart()],
                        has_metric_catalog: false,
                        metric_namespaces: Vec::new(),
                        metric_namespaces_loading: false,
                    },
                    window,
                    cx,
                );
            });
        });
        window.run_until_parked();

        (modal, outside, window, outcomes)
    }

    /// Enter adds the checked charts, or with none checked the highlighted
    /// one; with no chart left in the list it does nothing.
    #[gpui::test]
    fn enter_adds_the_highlighted_chart_when_none_is_checked(cx: &mut TestAppContext) {
        let (modal, _outside, window, outcomes) = open_modal(cx);
        assert_eq!(
            window.update(|_, cx| modal.read(cx).active_tab()),
            AddPanelTab::Saved
        );

        window.simulate_keystrokes("x enter");
        assert!(
            outcomes.borrow().is_empty(),
            "a search that leaves no chart adds nothing"
        );
        assert!(window.update(|_, cx| modal.read(cx).is_visible()));

        window.simulate_keystrokes("backspace enter");

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [AddPanelOutcome::Confirmed { chart_ids, .. }] if chart_ids == &[Uuid::nil()]
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    /// The same picker, open on the Metric tab's connection, with two saved
    /// charts.
    fn open_modal_with(
        cx: &mut TestAppContext,
        candidates: Vec<SavedChart>,
        has_metric_catalog: bool,
    ) -> (
        Entity<ModalAddPanelPicker>,
        &mut VisualTestContext,
        Outcomes,
    ) {
        let (modal, _outside, window) = host_modal(cx, ModalAddPanelPicker::new);

        let outcomes: Outcomes = Rc::default();
        window.update(|window, cx| {
            let sink = outcomes.clone();
            cx.subscribe(&modal, move |_, outcome: &AddPanelOutcome, _| {
                sink.borrow_mut().push(outcome.clone());
            })
            .detach();

            modal.update(cx, |modal, cx| {
                modal.open(
                    AddPanelRequest {
                        dashboard_id: Uuid::nil(),
                        profile_id: Uuid::nil(),
                        candidates,
                        has_metric_catalog,
                        metric_namespaces: vec!["AWS/EC2".to_string(), "AWS/RDS".to_string()],
                        metric_namespaces_loading: false,
                    },
                    window,
                    cx,
                );
            });
        });
        window.run_until_parked();

        (modal, window, outcomes)
    }

    fn named_chart(name: &str) -> SavedChart {
        SavedChart {
            id: Uuid::new_v4(),
            name: name.to_string(),
            ..chart()
        }
    }

    /// Alt+L and Alt+H switch the tabs from the search field, wrapping.
    #[gpui::test]
    fn alt_l_and_alt_h_switch_the_tabs(cx: &mut TestAppContext) {
        let (modal, window, _outcomes) = open_modal_with(cx, vec![chart()], true);
        let tab =
            |window: &mut VisualTestContext| window.update(|_, cx| modal.read(cx).active_tab());

        window.simulate_keystrokes("alt-l");
        assert_eq!(tab(window), AddPanelTab::Query);

        window.simulate_keystrokes("alt-l alt-l");
        assert_eq!(tab(window), AddPanelTab::Saved, "the tabs wrap around");

        window.simulate_keystrokes("alt-h");
        assert_eq!(tab(window), AddPanelTab::Metric);
    }

    /// From the search field Down moves the highlighted chart and Enter adds
    /// it when nothing is checked.
    #[gpui::test]
    fn down_and_enter_add_the_highlighted_chart(cx: &mut TestAppContext) {
        let first = named_chart("Latency");
        let second = named_chart("Throughput");
        let second_id = second.id;
        let (_modal, window, outcomes) = open_modal_with(cx, vec![first, second], false);

        window.simulate_keystrokes("down enter");

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [AddPanelOutcome::Confirmed { chart_ids, .. }] if chart_ids == &[second_id]
        ));
    }

    /// In the chart list (the tab stop after the search) J moves and Space
    /// checks charts; Enter adds the checked ones.
    #[gpui::test]
    fn j_and_space_check_charts_in_the_list(cx: &mut TestAppContext) {
        let first = named_chart("Latency");
        let second = named_chart("Throughput");
        let ids = [first.id, second.id];
        let (modal, window, outcomes) = open_modal_with(cx, vec![first, second], false);

        window.update(|window, cx| {
            let list = modal.read(cx).saved_list_focus.clone();
            list.focus(window, cx);
        });
        window.run_until_parked();

        window.simulate_keystrokes("space j space");
        assert_eq!(
            window.update(|_, cx| modal.read(cx).selected_ids.clone()),
            ids.to_vec()
        );

        window.simulate_keystrokes("enter");
        assert!(matches!(
            outcomes.borrow().as_slice(),
            [AddPanelOutcome::Confirmed { chart_ids, .. }] if chart_ids == &ids.to_vec()
        ));
    }

    /// On the Metric tab, the namespace list takes J and Space: Space picks
    /// the highlighted namespace.
    #[gpui::test]
    fn the_metric_namespace_list_is_driven_by_keys(cx: &mut TestAppContext) {
        let (modal, window, _outcomes) = open_modal_with(cx, vec![chart()], true);

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.set_active_tab(AddPanelTab::Metric, cx);
                modal.focus_metric_list(super::MetricList::Namespaces, window, cx);
            })
        });
        window.run_until_parked();

        window.simulate_keystrokes("j space");
        assert_eq!(
            window.update(|_, cx| modal.read(cx).metric_namespace_selected.clone()),
            Some("AWS/RDS".to_string())
        );
    }

    #[gpui::test]
    fn escape_cancels_and_gives_focus_back(cx: &mut TestAppContext) {
        let (modal, outside, window, outcomes) = open_modal(cx);

        window.simulate_keystrokes("escape");

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [AddPanelOutcome::Cancelled]
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert!(has_focus(window, &outside));
    }

    #[gpui::test]
    fn a_backdrop_click_cancels(cx: &mut TestAppContext) {
        let (_modal, _outside, window, outcomes) = open_modal(cx);

        click_backdrop(window);

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [AddPanelOutcome::Cancelled]
        ));
    }

    /// With Vim mode on, the query editor keeps its keys: `j` moves down,
    /// the first Escape leaves Insert mode, Enter in Normal mode never
    /// submits, and Escape in Normal mode cancels the dialog.
    #[gpui::test]
    fn the_query_editor_takes_vim_keys(cx: &mut TestAppContext) {
        cx.update(|cx| dbflux_components::vim::set_vim_enabled(cx, true));
        let (modal, _outside, window, outcomes) = open_modal(cx);

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.active_tab = AddPanelTab::Query;
                modal.query_name_input.update(cx, |state, cx| {
                    state.set_value("Latency", window, cx);
                });
                modal.query_input.update(cx, |state, cx| {
                    state.set_value("SELECT 1\nFROM t", window, cx);
                    state.set_selected_range(0..0, cx);
                    state.focus(window, cx);
                });
                cx.notify();
            });
        });
        window.run_until_parked();

        let text_and_cursor = |window: &mut VisualTestContext| {
            window.update(|_, cx| {
                let state = modal.read(cx).query_input.read(cx);
                (state.value().to_string(), state.cursor())
            })
        };

        window.simulate_keystrokes("j");
        window.run_until_parked();
        assert_eq!(text_and_cursor(window), ("SELECT 1\nFROM t".into(), 9));

        window.simulate_keystrokes("enter");
        window.run_until_parked();
        assert!(
            outcomes.borrow().is_empty(),
            "Enter in Normal mode submitted"
        );

        window.simulate_keystrokes("i");
        window.simulate_input("X");
        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert_eq!(text_and_cursor(window).0, "SELECT 1\nXFROM t");
        assert!(
            window.update(|_, cx| modal.read(cx).is_visible()),
            "the first Escape only leaves Insert mode"
        );

        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }
}
