//! Per-panel Configure popover for the dashboard.
//!
//! Surfaces three sections behind a modal shell so per-panel configuration
//! (chart kind, axis bindings, stats/PNG actions) is reachable from the kebab
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
use gpui::prelude::*;
use gpui::{AnyElement, Context, Entity, IntoElement, div, px};

/// All chart kinds offered by the Configure popover, in display order.
const CHART_KIND_OPTIONS: &[(ChartKind, &str, &str)] = &[
    (ChartKind::Line, "Line", "configure-kind-line"),
    (ChartKind::Bar, "Bar", "configure-kind-bar"),
    (ChartKind::Scatter, "Scatter", "configure-kind-scatter"),
    (ChartKind::Area, "Area", "configure-kind-area"),
    (ChartKind::StackedBar, "Stacked", "configure-kind-stacked"),
    (ChartKind::Pie, "Pie", "configure-kind-pie"),
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
        .child(section("Chart type", chart_kind_row))
        .child(section("Axis bindings", bindings_row))
        .child(section("Actions", actions_row))
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
            Button::new("configure-cancel", "Cancel")
                .ghost()
                .on_click(on_cancel),
        )
        .child(
            Button::new("configure-apply", "Apply")
                .primary()
                .on_click(on_apply),
        )
        .into_any_element();

    // Bridge ModalShell's App-scoped on_close into the DashboardDocument
    // entity via a weak handle so the X button closes the popover.
    let weak_self = cx.weak_entity();
    let modal = ModalShell::new(format!("Configure panel: {panel_title}"), body, footer)
        .width(px(720.0))
        .on_close(move |_window, cx| {
            if let Some(this) = weak_self.upgrade() {
                this.update(cx, |this, cx| this.close_configure_panel(cx));
            }
        });

    Some(modal.into_any_element())
}

fn section(label: &'static str, body: AnyElement) -> AnyElement {
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
        .map(|(kind, label, id)| {
            let kind = *kind;
            let is_active = kind == current_kind;
            let on_click = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
                this.configure_apply_chart_kind(panel_index, kind, cx);
            });
            // Inactive kinds use the default Button variant so the border
            // makes them readable as buttons against the modal background.
            // `.ghost()` produced borderless transparent boxes which blended
            // into the modal and looked like static text.
            let btn = if is_active {
                Button::new(*id, *label).primary().on_click(on_click)
            } else {
                Button::new(*id, *label).on_click(on_click)
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
            .child(Text::caption(
                "Run the chart at least once to configure bindings.",
            ))
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
    let on_png = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
        this.configure_export_png(panel_index, cx);
    });

    div()
        .flex()
        .flex_row()
        .gap(Spacing::SM)
        .child(Button::new("configure-stats", "Stats").on_click(on_stats))
        .child(Button::new("configure-png", "Export PNG").on_click(on_png))
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
                CHART_KIND_OPTIONS.iter().any(|(k, _, _)| *k == kind),
                "Configure popover must surface {kind:?}"
            );
        }
    }

    /// IDs in `CHART_KIND_OPTIONS` must be unique to avoid GPUI element-id
    /// collisions inside the popover.
    #[test]
    fn chart_kind_option_ids_are_unique() {
        let mut seen: Vec<&str> = Vec::new();
        for (_, _, id) in CHART_KIND_OPTIONS {
            assert!(!seen.contains(id), "duplicate Configure popover id: {id}");
            seen.push(id);
        }
    }
}
