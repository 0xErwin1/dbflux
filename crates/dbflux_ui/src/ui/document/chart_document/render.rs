//! Render implementation for `ChartDocument`.
//!
//! Layout:
//!   ┌──────────────────────────────────────────────┐
//!   │ header: title · Save button                  │
//!   ├──────────────────────────────────────────────┤
//!   │ collapsible editor drawer                    │
//!   │   [chevron] Query  [Run]                     │
//!   │   ┌───────────────────────────────────────┐  │
//!   │   │ SQL editor (when expanded)            │  │
//!   │   └───────────────────────────────────────┘  │
//!   ├──────────────────────────────────────────────┤
//!   │ chart area (fills remaining space)           │
//!   └──────────────────────────────────────────────┘

use super::ChartDocument;
use crate::ui::components::toast::flush_pending_toast;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Heights, Spacing};
use dbflux_components::chart::ChartDetection;
use dbflux_components::controls::Input;
use dbflux_components::primitives::{Icon, Text};
use gpui::prelude::*;
use gpui::*;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
use gpui_component::{ActiveTheme, Disableable, Sizable};

impl Render for ChartDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // -- Flush pending toasts --
        flush_pending_toast(self.pending_toast.take(), window, cx);

        // -- Apply pending query result --
        if let Some(pending) = self.pending_result.take() {
            self.apply_result(pending, cx);
        }

        // -- Auto-run on first render --
        if self.pending_run_on_first_render {
            self.pending_run_on_first_render = false;
            self.request_reexecute(window, cx);
        }

        let theme = cx.theme().clone();

        let is_executing = self.exec_state == super::ExecState::Running;
        let drawer_open = self.editor_drawer_open;
        let editor_input = self.editor_input.clone();
        let focus_handle = self.focus_handle.clone();
        let title = self.title.clone();
        let show_name_prompt = self.name_prompt.is_some();

        // -- Ensure chart view is built for the current result --
        // This must happen before reading shell state so ensure_chart_view
        // has a chance to construct the ChartView entity.
        if let Some(result) = self.last_result.clone() {
            self.chart_shell.update(cx, |shell, cx| {
                shell.ensure_chart_view(&result, cx);
            });
        }

        // -- Read chart view entity from shell --
        let chart_view_entity = self
            .chart_shell
            .read(cx)
            .chart_view()
            .cloned();

        let chart_detection = self
            .chart_shell
            .read(cx)
            .chart_detection
            .clone();

        // -- Chart area content --
        let chart_area: AnyElement = if let Some(chart_entity) = chart_view_entity {
            div().size_full().child(chart_entity).into_any_element()
        } else {
            // Degraded state: show a placeholder based on detection result.
            let msg = match &chart_detection {
                Some(ChartDetection::EmptyResult) | None => "Run the query to populate the chart.",
                Some(ChartDetection::NoTimeColumn) => "No time column detected in result.",
                Some(ChartDetection::NoNumericSeries) => "No numeric series detected in result.",
                Some(ChartDetection::Ok { .. }) => "Chart build failed.",
            };
            div()
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .child(Text::muted(msg))
                .into_any_element()
        };

        // -- Name prompt modal --
        let name_prompt_element = show_name_prompt.then(|| {
            let input = self.name_prompt.as_ref().unwrap().input.clone();

            div()
                .absolute()
                .inset_0()
                .flex()
                .items_center()
                .justify_center()
                .bg(theme.background.opacity(0.6))
                .child(
                    div()
                        .bg(theme.secondary)
                        .border_1()
                        .border_color(theme.border)
                        .p(Spacing::LG)
                        .w(px(360.0))
                        .flex()
                        .flex_col()
                        .gap(Spacing::MD)
                        .child(Text::label("Save chart"))
                        .child(Input::new(&input).placeholder("Chart name"))
                        .child(
                            div()
                                .flex()
                                .flex_row()
                                .gap(Spacing::SM)
                                .justify_end()
                                .child(
                                    Button::new("cancel-save")
                                        .label("Cancel")
                                        .small()
                                        .on_click(cx.listener(|this, _, _window, cx| {
                                            this.cancel_save(cx);
                                        })),
                                )
                                .child(
                                    Button::new("confirm-save")
                                        .label("Save")
                                        .small()
                                        .with_variant(ButtonVariant::Primary)
                                        .on_click(cx.listener(|this, _, _window, cx| {
                                            this.confirm_save(cx);
                                        })),
                                ),
                        ),
                )
        });

        // -- Editor drawer --
        let chevron_icon = if drawer_open {
            AppIcon::ChevronDown
        } else {
            AppIcon::ChevronRight
        };

        let drawer = div()
            .flex()
            .flex_col()
            .border_b_1()
            .border_color(theme.border)
            .child(
                // Drawer header: click the whole bar to toggle.
                div()
                    .id("chart-doc-drawer-toggle")
                    .flex()
                    .flex_row()
                    .items_center()
                    .h(Heights::TOOLBAR)
                    .px(Spacing::MD)
                    .gap(Spacing::SM)
                    .cursor_pointer()
                    .on_click(cx.listener(|this, _, _window, cx| {
                        this.toggle_editor_drawer(cx);
                    }))
                    .child(Icon::new(chevron_icon).small())
                    .child(Text::muted("Query"))
                    .child(div().flex_grow())
                    .child(
                        Button::new("run-query")
                            .label(if is_executing { "Running…" } else { "Run" })
                            .small()
                            .with_variant(ButtonVariant::Primary)
                            .disabled(is_executing)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.request_reexecute(window, cx);
                            })),
                    ),
            )
            .when(drawer_open, |el| {
                el.child(
                    div()
                        .h(px(180.0))
                        .border_t_1()
                        .border_color(theme.border)
                        .child(Input::new(&editor_input).placeholder("Enter SQL query")),
                )
            });

        // -- Document header --
        let header = div()
            .flex()
            .flex_row()
            .items_center()
            .h(Heights::TOOLBAR)
            .px(Spacing::MD)
            .gap(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .child(Text::label(title))
            .child(div().flex_grow())
            .child(
                Button::new("save-chart")
                    .label("Save")
                    .small()
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.open_name_prompt(window, cx);
                    })),
            );

        // -- Root container --
        div()
            .flex()
            .flex_col()
            .size_full()
            .track_focus(&focus_handle)
            .child(header)
            .child(drawer)
            .child(div().flex_grow().min_h_0().child(chart_area))
            .when_some(name_prompt_element, |el, modal| el.child(modal))
    }
}
