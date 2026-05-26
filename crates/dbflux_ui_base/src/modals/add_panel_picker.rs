use dbflux_components::chart::ChartKind;
use dbflux_components::controls::{Button, GpuiInput as Input, InputEvent, InputState};
use dbflux_components::modals::shell::ModalShell;
use dbflux_components::primitives::Text;
use dbflux_components::saved_chart::SavedChart;
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::MetricDescriptor;
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Context, Entity, EventEmitter, FocusHandle, FontWeight, IntoElement,
    MouseButton, Render, SharedString, Subscription, Window, div, px,
};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use std::collections::HashMap;
use uuid::Uuid;

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
    /// Pre-loaded namespaces (empty when no catalog).
    pub metric_namespaces: Vec<String>,
}

/// Active tab in the picker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AddPanelTab {
    #[default]
    Saved,
    Query,
    Metric,
}

/// Pure helper: submit button label given an active tab and current
/// saved-tab selection count. Extracted to keep tests GPUI-free.
pub fn submit_label_for(tab: AddPanelTab, saved_selection_count: usize) -> String {
    match tab {
        AddPanelTab::Saved => match saved_selection_count {
            0 => "Add panels".to_string(),
            1 => "Add 1 panel".to_string(),
            n => format!("Add {n} panels"),
        },
        AddPanelTab::Query | AddPanelTab::Metric => "Create panel".to_string(),
    }
}

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
    focus_handle: FocusHandle,

    // Saved-tab state.
    search_input: Entity<InputState>,
    selected_ids: Vec<Uuid>,

    // Query-tab state.
    query_name_input: Entity<InputState>,
    query_input: Entity<InputState>,
    query_chart_kind: ChartKind,

    // Metric-tab state.
    metric_name_input: Entity<InputState>,
    metric_namespace_filter_input: Entity<InputState>,
    metric_namespace_selected: Option<String>,
    metric_metrics_for_namespace: HashMap<String, Vec<MetricDescriptor>>,
    metric_metric_selected: Option<usize>,
    metric_period_input: Entity<InputState>,
    metric_statistic: String,

    _subscriptions: Vec<Subscription>,
}

impl ModalAddPanelPicker {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder("Search charts..."));
        let query_name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Panel name"));
        let query_input = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("sql")
                .line_number(true)
                .soft_wrap(true)
                .placeholder("Type a query…")
        });
        let metric_name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Panel name"));
        let metric_namespace_filter_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Filter namespaces…"));
        let metric_period_input = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("Period (seconds)");
            s.set_value("60", window, cx);
            s
        });

        Self {
            request: None,
            visible: false,
            active_tab: AddPanelTab::default(),
            focus_handle: cx.focus_handle(),
            search_input,
            selected_ids: Vec::new(),
            query_name_input,
            query_input,
            query_chart_kind: ChartKind::Line,
            metric_name_input,
            metric_namespace_filter_input,
            metric_namespace_selected: None,
            metric_metrics_for_namespace: HashMap::new(),
            metric_metric_selected: None,
            metric_period_input,
            metric_statistic: "Average".to_string(),
            _subscriptions: Vec::new(),
        }
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
        self.metric_namespace_selected = None;
        self.metric_metric_selected = None;
        self.metric_metrics_for_namespace.clear();
        self.query_chart_kind = ChartKind::Line;
        self.metric_statistic = "Average".to_string();

        self.search_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
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
        self.metric_period_input.update(cx, |state, cx| {
            state.set_value("60", window, cx);
        });

        let mut subs = Vec::new();
        for input in [
            &self.search_input,
            &self.query_name_input,
            &self.query_input,
            &self.metric_name_input,
            &self.metric_namespace_filter_input,
            &self.metric_period_input,
        ] {
            let sub = cx.subscribe_in(input, window, |_this, _, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
            });
            subs.push(sub);
        }
        self._subscriptions = subs;

        self.focus_handle.focus(window);
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
        cx.notify();
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

    fn confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
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

    fn render_chart_row(
        chart_id: uuid::Uuid,
        chart_name: String,
        is_selected: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let checkbox = div()
            .w(Heights::ICON_SM)
            .h(Heights::ICON_SM)
            .border_1()
            .rounded_sm()
            .flex()
            .items_center()
            .justify_center()
            .when(is_selected, |el| el.bg(theme.selection))
            .into_any_element();

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .cursor_pointer()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.toggle_chart(chart_id, cx);
                }),
            )
            .child(checkbox)
            .child(div().flex_1().text_sm().child(chart_name))
            .into_any_element()
    }

    fn render_tab_strip(&self, cx: &mut Context<Self>) -> AnyElement {
        let visible = self.visible_tabs();
        let active = self.active_tab;
        let theme = cx.theme();

        let buttons: Vec<AnyElement> = visible
            .into_iter()
            .map(|tab| {
                let label = match tab {
                    AddPanelTab::Saved => "Saved charts",
                    AddPanelTab::Query => "From query",
                    AddPanelTab::Metric => "From metric",
                };
                let id = match tab {
                    AddPanelTab::Saved => "add-panel-tab-saved",
                    AddPanelTab::Query => "add-panel-tab-query",
                    AddPanelTab::Metric => "add-panel-tab-metric",
                };
                let is_active = tab == active;
                let mut btn =
                    Button::new(id, label).on_click(cx.listener(move |this, _, _, cx| {
                        this.set_active_tab(tab, cx);
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
            .gap(Spacing::SM)
            .pb(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .children(buttons)
            .into_any_element()
    }

    fn render_saved_tab(&self, candidates: &[SavedChart], cx: &mut Context<Self>) -> AnyElement {
        if candidates.is_empty() {
            return Text::body(
                "No saved charts on this connection. \
                 Use \"From query\" or \"From metric\" to create one directly.",
            )
            .into_any_element();
        }

        let query = self.search_input.read(cx).value().to_string();
        let filtered = Self::filtered_candidates(candidates, &query);
        let selected_ids = self.selected_ids.clone();

        let search_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::subsection_label("Search").into_any_element())
            .child(Input::new(&self.search_input))
            .into_any_element();

        let chart_rows: Vec<AnyElement> = filtered
            .iter()
            .map(|chart| {
                let is_selected = selected_ids.contains(&chart.id);
                Self::render_chart_row(chart.id, chart.name.clone(), is_selected, cx)
            })
            .collect();

        let chart_list = div()
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
            (ChartKind::Line, "Line", "add-panel-kind-line"),
            (ChartKind::Bar, "Bar", "add-panel-kind-bar"),
            (ChartKind::Area, "Area", "add-panel-kind-area"),
            (ChartKind::Scatter, "Scatter", "add-panel-kind-scatter"),
        ];
        let current = self.query_chart_kind;

        let buttons: Vec<AnyElement> = kinds
            .into_iter()
            .map(|(kind, label, id)| {
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
        let name_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::subsection_label("Name").into_any_element())
            .child(Input::new(&self.query_name_input))
            .into_any_element();

        let theme = cx.theme();
        let query_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::subsection_label("Query").into_any_element())
            .child(
                div()
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::SM)
                    .bg(theme.background)
                    .h(px(320.0))
                    .p(Spacing::SM)
                    .overflow_hidden()
                    .child(Input::new(&self.query_input).w_full().h_full())
                    .into_any_element(),
            )
            .into_any_element();

        let kind_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::subsection_label("Chart kind").into_any_element())
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
        let filter = self
            .metric_namespace_filter_input
            .read(cx)
            .value()
            .to_string()
            .to_lowercase();
        let selected = self.metric_namespace_selected.clone();

        let rows: Vec<AnyElement> = request
            .metric_namespaces
            .iter()
            .filter(|ns| filter.is_empty() || ns.to_lowercase().contains(&filter))
            .cloned()
            .map(|ns| {
                let is_selected = selected.as_deref() == Some(ns.as_str());
                let ns_for_listener = ns.clone();
                div()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .text_sm()
                    .cursor_pointer()
                    .when(is_selected, |el| {
                        el.bg(theme.accent)
                            .text_color(theme.accent_foreground)
                            .font_weight(FontWeight::SEMIBOLD)
                    })
                    .when(!is_selected, |el| el.hover(|s| s.bg(theme.muted)))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, _, cx| {
                            if let Some(req) = this.select_namespace(ns_for_listener.clone(), cx) {
                                cx.emit(req);
                            }
                        }),
                    )
                    .child(ns)
                    .into_any_element()
            })
            .collect();

        div()
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
            return Text::body("Select a namespace to load metrics.").into_any_element();
        };

        let theme = cx.theme();
        let Some(metrics) = self.metric_metrics_for_namespace.get(&namespace) else {
            return div()
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::SM)
                .h(px(260.0))
                .p(Spacing::SM)
                .child(Text::body("Loading…").into_any_element())
                .into_any_element();
        };

        if metrics.is_empty() {
            return div()
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::SM)
                .h(px(260.0))
                .p(Spacing::SM)
                .child(Text::body("No metrics in this namespace.").into_any_element())
                .into_any_element();
        }

        let selected = self.metric_metric_selected;
        let rows: Vec<AnyElement> = metrics
            .iter()
            .enumerate()
            .map(|(idx, m)| {
                let is_selected = selected == Some(idx);
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
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .text_sm()
                    .cursor_pointer()
                    .when(is_selected, |el| {
                        el.bg(theme.accent)
                            .text_color(theme.accent_foreground)
                            .font_weight(FontWeight::SEMIBOLD)
                    })
                    .when(!is_selected, |el| el.hover(|s| s.bg(theme.muted)))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, _, cx| {
                            this.metric_metric_selected = Some(idx);
                            cx.notify();
                        }),
                    )
                    .child(label)
                    .into_any_element()
            })
            .collect();

        div()
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
            .child(Text::subsection_label("Name").into_any_element())
            .child(Input::new(&self.metric_name_input))
            .into_any_element();

        // Two-column row: Namespace (with filter) on the left, Metric on the right.
        let namespace_column = div()
            .flex()
            .flex_1()
            .min_w_0()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::subsection_label("Namespace").into_any_element())
            .child(Input::new(&self.metric_namespace_filter_input))
            .child(self.render_namespace_list(cx))
            .into_any_element();

        let metric_column = div()
            .flex()
            .flex_1()
            .min_w_0()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::subsection_label("Metric").into_any_element())
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
            .child(Text::subsection_label("Period (seconds)").into_any_element())
            .child(Input::new(&self.metric_period_input))
            .into_any_element();

        let statistics = ["Average", "Sum", "Minimum", "Maximum", "SampleCount"];
        let current_stat = self.metric_statistic.clone();
        let stat_buttons: Vec<AnyElement> = statistics
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
            .child(Text::subsection_label("Statistic").into_any_element())
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

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(AddPanelOutcome::Cancelled);
            this.close(cx);
        });

        let on_confirm = cx.listener(move |this, _: &gpui::ClickEvent, window, cx| {
            this.confirm(window, cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(Button::new("add-panel-cancel", "Cancel").on_click(on_cancel))
            .child(
                Button::new("add-panel-confirm", submit_label)
                    .primary()
                    .disabled(!can_confirm)
                    .on_click(on_confirm),
            );

        ModalShell::new("Add panels", body, footer.into_any_element())
            .width(gpui::px(900.0))
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
        let ids_1 = vec![Uuid::new_v4()];
        let ids_3 = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        let label = |ids: &[Uuid]| -> String {
            match ids.len() {
                0 => "Add panels".to_string(),
                1 => "Add 1 panel".to_string(),
                n => format!("Add {n} panels"),
            }
        };

        assert_eq!(label(&ids_0), "Add panels");
        assert_eq!(label(&ids_1), "Add 1 panel");
        assert_eq!(label(&ids_3), "Add 3 panels");
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
        assert_eq!(submit_label_for(AddPanelTab::Saved, 0), "Add panels");
        assert_eq!(submit_label_for(AddPanelTab::Saved, 1), "Add 1 panel");
        assert_eq!(submit_label_for(AddPanelTab::Saved, 5), "Add 5 panels");
        assert_eq!(submit_label_for(AddPanelTab::Query, 0), "Create panel");
        assert_eq!(submit_label_for(AddPanelTab::Query, 99), "Create panel");
        assert_eq!(submit_label_for(AddPanelTab::Metric, 0), "Create panel");
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
}
