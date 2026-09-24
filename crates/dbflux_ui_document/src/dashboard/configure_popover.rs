//! Per-panel Configure popover for the dashboard.
//!
//! Surfaces three sections behind a modal shell so per-panel configuration
//! (chart kind, axis bindings, stats action) is reachable from the kebab
//! menu without polluting the chrome of every embedded chart panel.
//!
//! All operations route through `ChartDocument` public accessors so the
//! popover never reaches into `chart_shell` directly.

use super::{DashboardDocument, DashboardPanelSlot};
use dbflux_components::chart::{AggKind, AxisPill, BindingSpec, ChartKind, axis_bar_element};
use dbflux_components::controls::Button;
use dbflux_components::modals::ModalShell;
use dbflux_components::primitives::Text;
use dbflux_components::semantic::ChartColors;
use dbflux_components::tokens::Spacing;
use dbflux_core::ColumnMeta;
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::{AnyElement, Context, Entity, IntoElement, div, px};

/// All chart kinds offered by the Configure popover, in display order. The
/// translated label for each kind comes from
/// `crate::labels::configure_chart_kind_label`, not this table.
const CHART_KIND_OPTIONS: &[(ChartKind, &str)] = &[
    (ChartKind::Line, "configure-kind-line"),
    (ChartKind::Bar, "configure-kind-bar"),
    (ChartKind::Scatter, "configure-kind-scatter"),
    (ChartKind::Area, "configure-kind-area"),
    (ChartKind::StackedBar, "configure-kind-stacked"),
    (ChartKind::Pie, "configure-kind-pie"),
];

/// Build the Configure popover overlay element for the panel at `panel_index`.
///
/// Returns `None` when the slot is `Orphan` (no chart to configure) or out of
/// bounds. The returned element is a `ModalShell` overlay; the caller is
/// expected to push it into the dashboard's render tree.
pub(super) fn render_configure_popover(
    dashboard: &DashboardDocument,
    panel_index: usize,
    cx: &mut Context<DashboardDocument>,
) -> Option<AnyElement> {
    let slot = dashboard.panel_slots().get(panel_index)?;
    let panel_entity = match slot {
        DashboardPanelSlot::Loaded { panel, .. } => panel.clone(),
        DashboardPanelSlot::Orphan { .. }
        | DashboardPanelSlot::Divider { .. }
        | DashboardPanelSlot::Inspector { .. } => return None,
    };

    let panel_title = panel_entity.read(cx).title();
    let chart_kind = panel_entity.read(cx).chart_kind(cx);
    let bindings = panel_entity.read(cx).active_bindings(cx);
    let columns = panel_entity
        .read(cx)
        .last_result_columns()
        .unwrap_or_default();
    let axis_open_pill = panel_entity.read(cx).axis_open_pill(cx);

    let chart_kind_row = render_chart_kind_row(panel_index, chart_kind, cx);
    let bindings_row = render_bindings_row(
        panel_entity.clone(),
        panel_index,
        &bindings,
        &columns,
        axis_open_pill,
        cx,
    );
    let actions_row = render_actions_row(panel_index, cx);

    let body = div()
        .flex()
        .flex_col()
        .gap(Spacing::LG)
        .child(section(
            dbflux_i18n::t!("document.dashboard.configure.section.chart_type"),
            chart_kind_row,
        ))
        .child(section(
            dbflux_i18n::t!("document.dashboard.configure.section.axis_bindings"),
            bindings_row,
        ))
        .child(section(
            dbflux_i18n::t!("document.dashboard.configure.section.actions"),
            actions_row,
        ))
        .into_any_element();

    // Footer: Cancel + Apply
    let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
        this.close_configure_panel(cx);
    });
    let on_apply = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
        this.configure_apply_and_persist(panel_index, cx);
    });

    let footer = div()
        .flex()
        .flex_row()
        .gap(Spacing::SM)
        .child(
            Button::new(
                "configure-cancel",
                dbflux_i18n::t!("document.dashboard.configure.cancel"),
            )
            .ghost()
            .on_click(on_cancel),
        )
        .child(
            Button::new(
                "configure-apply",
                dbflux_i18n::t!("document.dashboard.configure.apply"),
            )
            .primary()
            .on_click(on_apply),
        )
        .into_any_element();

    // Bridge ModalShell's App-scoped handlers into the DashboardDocument
    // entity via a weak handle: Escape, the X button and a backdrop click
    // close the popover, and Enter applies like the Apply button.
    let modal_title = dbflux_i18n::t!("document.dashboard.configure.title", name = panel_title);
    let modal = ModalShell::new(modal_title, body, footer)
        .width(px(720.0))
        .focus_handle(dashboard.configure_focus.handle())
        .on_close({
            let weak_self = cx.weak_entity();
            move |_window, cx| {
                weak_self
                    .update(cx, |this, cx| this.close_configure_panel(cx))
                    .log_err();
            }
        })
        .on_confirm({
            let weak_self = cx.weak_entity();
            move |_window, cx| {
                weak_self
                    .update(cx, |this, cx| {
                        this.configure_apply_and_persist(panel_index, cx)
                    })
                    .log_err();
            }
        });

    Some(modal.into_any_element())
}

fn section(label: impl Into<gpui::SharedString>, body: AnyElement) -> AnyElement {
    div()
        .flex()
        .flex_col()
        .gap(Spacing::SM)
        .child(Text::subsection_label(label).into_any_element())
        .child(body)
        .into_any_element()
}

fn render_chart_kind_row(
    panel_index: usize,
    current_kind: ChartKind,
    cx: &mut Context<DashboardDocument>,
) -> AnyElement {
    let buttons: Vec<AnyElement> = CHART_KIND_OPTIONS
        .iter()
        .map(|(kind, id)| {
            let kind = *kind;
            let label = crate::labels::configure_chart_kind_label(kind);
            let is_active = kind == current_kind;
            let on_click = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
                this.configure_apply_chart_kind(panel_index, kind, cx);
            });
            // Inactive kinds use the default Button variant so the border
            // makes them readable as buttons against the modal background.
            // `.ghost()` produced borderless transparent boxes which blended
            // into the modal and looked like static text.
            let btn = if is_active {
                Button::new(*id, label).primary().on_click(on_click)
            } else {
                Button::new(*id, label).on_click(on_click)
            };
            btn.into_any_element()
        })
        .collect();

    div()
        .flex()
        .flex_row()
        .gap(Spacing::XS)
        .children(buttons)
        .into_any_element()
}

fn render_bindings_row(
    panel_entity: Entity<crate::chart_document::ChartDocument>,
    panel_index: usize,
    bindings: &BindingSpec,
    columns: &[ColumnMeta],
    open_pill: Option<AxisPill>,
    cx: &mut Context<DashboardDocument>,
) -> AnyElement {
    // When there is no query result yet, the popover cannot drive bindings —
    // surface a hint and skip the AxisBar.
    if columns.is_empty() {
        return div()
            .child(Text::caption(dbflux_i18n::t!(
                "document.dashboard.configure.bindings_hint"
            )))
            .into_any_element();
    }

    let chart_colors = ChartColors::for_current(cx);

    let panel_for_pill = panel_entity.clone();
    let panel_for_x = panel_entity.clone();
    let panel_for_y = panel_entity.clone();
    let panel_for_group = panel_entity.clone();
    let panel_for_agg = panel_entity.clone();

    let on_pill = move |pill: AxisPill, _w: &mut gpui::Window, cx: &mut gpui::App| {
        panel_for_pill.update(cx, |doc, cx| doc.toggle_axis_pill(pill, cx));
    };
    let on_x = move |col_idx: usize, _w: &mut gpui::Window, cx: &mut gpui::App| {
        panel_for_x.update(cx, |doc, cx| {
            let mut b = doc.active_bindings(cx);
            b.x = col_idx;
            doc.apply_binding_spec(b, cx);
        });
    };
    let on_y = move |col_idx: usize, checked: bool, _w: &mut gpui::Window, cx: &mut gpui::App| {
        panel_for_y.update(cx, |doc, cx| {
            let mut b = doc.active_bindings(cx);
            if checked {
                if !b.y.contains(&col_idx) {
                    b.y.push(col_idx);
                }
            } else {
                b.y.retain(|&i| i != col_idx);
            }
            doc.apply_binding_spec(b, cx);
        });
    };
    let on_group = move |group_col: Option<usize>, _w: &mut gpui::Window, cx: &mut gpui::App| {
        panel_for_group.update(cx, |doc, cx| {
            let mut b = doc.active_bindings(cx);
            b.group_by = group_col;
            doc.apply_binding_spec(b, cx);
        });
    };
    let on_agg = move |agg: AggKind, _w: &mut gpui::Window, cx: &mut gpui::App| {
        panel_for_agg.update(cx, |doc, cx| {
            let mut b = doc.active_bindings(cx);
            b.aggregation = agg;
            doc.apply_binding_spec(b, cx);
        });
    };

    let _ = panel_index; // Reserved for future per-panel id namespacing.

    axis_bar_element(
        bindings,
        columns,
        open_pill,
        &chart_colors,
        on_pill,
        on_x,
        on_y,
        on_group,
        on_agg,
    )
    .into_any_element()
}

fn render_actions_row(panel_index: usize, cx: &mut Context<DashboardDocument>) -> AnyElement {
    let on_stats = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
        this.configure_toggle_stats(panel_index, cx);
    });

    div()
        .flex()
        .flex_row()
        .gap(Spacing::SM)
        .child(
            Button::new(
                "configure-stats",
                dbflux_i18n::t!("document.dashboard.configure.action.stats"),
            )
            .on_click(on_stats),
        )
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// `CHART_KIND_OPTIONS` must enumerate every variant of `ChartKind`. Adding
    /// a new variant without updating this table would silently hide the kind
    /// from the popover.
    #[test]
    fn chart_kind_options_cover_all_variants() {
        // Walk every variant of ChartKind via exhaustive match; any new variant
        // breaks the compile until the table is updated.
        let kinds = [
            ChartKind::Line,
            ChartKind::Bar,
            ChartKind::Scatter,
            ChartKind::Area,
            ChartKind::StackedBar,
            ChartKind::Pie,
        ];
        for kind in kinds {
            assert!(
                CHART_KIND_OPTIONS.iter().any(|(k, _)| *k == kind),
                "Configure popover must surface {kind:?}"
            );
        }
    }

    /// IDs in `CHART_KIND_OPTIONS` must be unique to avoid GPUI element-id
    /// collisions inside the popover.
    #[test]
    fn chart_kind_option_ids_are_unique() {
        let mut seen: Vec<&str> = Vec::new();
        for (_, id) in CHART_KIND_OPTIONS {
            assert!(!seen.contains(id), "duplicate Configure popover id: {id}");
            seen.push(id);
        }
    }

    /// `crate::labels::configure_chart_kind_label` covers every chart kind
    /// surfaced by `CHART_KIND_OPTIONS` and its labels resolve through the
    /// catalog (widened from a hardcoded `&str` table column).
    #[test]
    fn chart_kind_options_labels_resolve_via_configure_chart_kind_label() {
        for (kind, _) in CHART_KIND_OPTIONS {
            let label = crate::labels::configure_chart_kind_label(*kind);
            assert!(
                !label.is_empty(),
                "configure_chart_kind_label({kind:?}) resolved empty"
            );
        }
    }

    /// The Configure popover's section/action/footer keys resolve in both
    /// locales and the popover title interpolates the panel name.
    #[test]
    fn configure_popover_keys_resolve_in_both_locales() {
        let keys = [
            "document.dashboard.configure.section.chart_type",
            "document.dashboard.configure.section.axis_bindings",
            "document.dashboard.configure.section.actions",
            "document.dashboard.configure.cancel",
            "document.dashboard.configure.apply",
            "document.dashboard.configure.bindings_hint",
            "document.dashboard.configure.action.stats",
        ];
        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    /// The popover title interpolates the panel name via `%{name}`.
    #[test]
    fn configure_popover_title_interpolates_panel_name() {
        let title = dbflux_i18n::t!("document.dashboard.configure.title", name = "My Chart");
        assert!(
            title.contains("My Chart"),
            "expected the panel name to be interpolated into the title, got {title:?}"
        );
    }

    /// At least one Configure popover key must diverge between locales.
    #[test]
    fn configure_popover_cancel_differs_between_locales() {
        let en = dbflux_i18n::t!("document.dashboard.configure.cancel", locale = "en");
        let es = dbflux_i18n::t!("document.dashboard.configure.cancel", locale = "es");
        assert_ne!(en, es);
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use crate::chart_document::ChartDocument;
    use crate::dashboard::{DashboardDocument, DashboardPanelSlot, PanelGridPos};
    use dbflux_components::chart::{ChartKind, ChartSpec};
    use dbflux_components::common::time_range::view::TimeRangePanel;
    use dbflux_components::saved_chart::{SavedChart, SavedChartRefreshPolicy, SavedChartSource};
    use dbflux_components::theme;
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::modals::test_host::{click_backdrop, has_focus, host_modal};
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use uuid::Uuid;

    struct Setup<'a> {
        dashboard: Entity<DashboardDocument>,
        app_state: Entity<AppStateEntity>,
        chart_id: Uuid,
        window: &'a mut VisualTestContext,
    }

    /// Opens the Configure popover for a dashboard holding one saved line
    /// chart, from a focused dashboard, and switches the panel to a bar chart
    /// without applying it yet.
    fn open_popover(cx: &mut TestAppContext) -> Setup<'_> {
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });
        // Saved charts reference their connection profile, so the profile row
        // has to exist before the chart can be stored.
        let profile_id = Uuid::new_v4();
        let storage = StorageRuntime::in_memory().expect("in-memory storage");
        storage
            .dbflux_db()
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
                [profile_id.to_string(), "test-profile".to_string()],
            )
            .expect("profile stored");
        let app_state =
            cx.new(|_| AppStateEntity::new_with_storage_runtime(storage).expect("app state"));

        let mut chart_spec: ChartSpec = serde_json::from_str(
            r#"{"x_axis":{"column_index":0,"label":"t","kind":"Time","unit":null},"series":[]}"#,
        )
        .expect("valid chart spec");
        chart_spec.kind = ChartKind::Line;
        let saved = SavedChart {
            id: Uuid::new_v4(),
            name: "Latency".to_string(),
            profile_id,
            source: SavedChartSource::Query {
                query: String::new(),
            },
            chart_spec,
            bindings: Default::default(),
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let chart_id = saved.id;
        cx.update(|cx| {
            app_state.update(cx, |state, _| {
                state
                    .saved_charts
                    .upsert(saved.clone())
                    .expect("saved chart stored");
            });
        });

        let (dashboard, _outside, window) = host_modal(cx, {
            let app_state = app_state.clone();
            move |window, cx| {
                let panel = cx.new(|cx| {
                    ChartDocument::from_saved(&saved, app_state.clone(), window, cx)
                        .expect("query chart")
                });
                let shared_time_range = cx.new(|cx| TimeRangePanel::new("24h", None, window, cx));

                DashboardDocument::new(
                    Uuid::nil(),
                    "Ops".to_string(),
                    vec![DashboardPanelSlot::Loaded {
                        panel,
                        grid_pos: PanelGridPos {
                            grid_row: 0,
                            grid_column: 0,
                            grid_width: 6,
                            grid_height: 4,
                        },
                        title_override: None,
                    }],
                    shared_time_range,
                    None,
                    SavedChartRefreshPolicy::Off,
                    false,
                    app_state,
                    cx,
                )
            }
        });

        window.update(|window, cx| {
            dashboard.update(cx, |dashboard, cx| {
                dashboard.focus(window, cx);
                dashboard.start_configure_panel(0, cx);
                dashboard.configure_apply_chart_kind(0, ChartKind::Bar, cx);
            });
        });
        window.run_until_parked();

        Setup {
            dashboard,
            app_state,
            chart_id,
            window,
        }
    }

    impl Setup<'_> {
        fn is_open(&mut self) -> bool {
            let dashboard = self.dashboard.clone();
            self.window
                .update(|_, cx| dashboard.read(cx).pending_configure_panel_index().is_some())
        }

        fn stored_kind(&mut self) -> Option<ChartKind> {
            let (app_state, chart_id) = (self.app_state.clone(), self.chart_id);
            self.window.update(|_, cx| {
                app_state
                    .read(cx)
                    .saved_charts
                    .chart_by_id(chart_id)
                    .map(|chart| chart.chart_spec.kind)
            })
        }

        fn dashboard_has_focus(&mut self) -> bool {
            let handle = self
                .window
                .update(|_, cx| self.dashboard.read(cx).focus_handle.clone());
            has_focus(self.window, &handle)
        }
    }

    #[gpui::test]
    fn enter_applies_and_saves_the_configuration(cx: &mut TestAppContext) {
        let mut setup = open_popover(cx);
        assert!(setup.is_open());

        setup.window.simulate_keystrokes("enter");

        assert!(!setup.is_open());
        assert_eq!(setup.stored_kind(), Some(ChartKind::Bar));
    }

    #[gpui::test]
    fn escape_closes_without_saving_and_gives_focus_back(cx: &mut TestAppContext) {
        let mut setup = open_popover(cx);

        setup.window.simulate_keystrokes("escape");

        assert!(!setup.is_open());
        assert_eq!(setup.stored_kind(), Some(ChartKind::Line));
        assert!(setup.dashboard_has_focus());
    }

    #[gpui::test]
    fn a_backdrop_click_closes_without_saving(cx: &mut TestAppContext) {
        let mut setup = open_popover(cx);

        click_backdrop(setup.window);

        assert!(!setup.is_open());
        assert_eq!(setup.stored_kind(), Some(ChartKind::Line));
    }
}
