use gpui::{Context, IntoElement, SharedString, div};
use gpui_component::ActiveTheme;

use dbflux_components::controls::{Button, ButtonVariant, Input};
use dbflux_components::tokens::{FontSizes, Spacing};

use crate::data_grid_panel::mutation_executor::{CountState, CountUnknownReason, ExecutionMode};
use crate::query_builder::panel::QueryBuilderPanel;

/// Returns a human-readable label for a `CountState`.
fn count_label(state: &CountState) -> String {
    match state {
        CountState::Counting => "Counting rows\u{2026}".to_string(),
        CountState::Done(n) => format!("{} rows estimated", n),
        CountState::Unknown { reason } => match reason {
            CountUnknownReason::TimedOut => {
                "Row count timed out — chunked mode recommended".to_string()
            }
            CountUnknownReason::Failed(msg) => format!("Row count failed: {}", msg),
        },
    }
}

/// Returns the display label for an `ExecutionMode`.
fn mode_label(mode: ExecutionMode) -> &'static str {
    match mode {
        ExecutionMode::SingleTransaction => "Single TX",
        ExecutionMode::ChunkedTransaction => "Chunked TX",
        ExecutionMode::DirectAutocommit => "Direct",
    }
}

/// Renders the execution mode section.
///
/// Shows:
/// - A 3-button segmented control for SingleTransaction / ChunkedTransaction / DirectAutocommit
/// - A chunk-size input (greyed out unless ChunkedTransaction is selected)
/// - A lock-timeout input (greyed out for DirectAutocommit)
/// - The row-count state label
pub fn render_execution(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::prelude::*;

    let theme = cx.theme().clone();

    let (current_mode, count_state) = match panel.mutation_state.as_ref() {
        Some(s) => (s.exec_options.mode, s.count_state.clone()),
        None => return div().into_any_element(),
    };

    let count_text = count_label(&count_state);

    let modes = [
        ExecutionMode::SingleTransaction,
        ExecutionMode::ChunkedTransaction,
        ExecutionMode::DirectAutocommit,
    ];

    let mut mode_row = div().flex().flex_row().gap_1().items_center().child(
        div()
            .text_size(FontSizes::SM)
            .text_color(theme.muted_foreground)
            .child("Mode:"),
    );

    for mode in modes {
        let is_active = mode == current_mode;
        let variant = if is_active {
            ButtonVariant::Primary
        } else {
            ButtonVariant::Default
        };
        mode_row = mode_row.child(
            Button::new(("qb-exec-mode", mode as usize), mode_label(mode))
                .variant(variant)
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    if let Some(state) = this.mutation_state.as_mut() {
                        state.exec_options.mode = mode;
                    }
                    cx.notify();
                })),
        );
    }

    // Chunk size row (enabled only in ChunkedTransaction mode)
    let chunk_enabled = current_mode == ExecutionMode::ChunkedTransaction;
    let chunk_label_color = if chunk_enabled {
        theme.foreground
    } else {
        theme.muted_foreground
    };

    let chunk_row = div()
        .flex()
        .flex_row()
        .gap_1()
        .items_center()
        .child(
            div()
                .text_size(FontSizes::SM)
                .text_color(chunk_label_color)
                .child("Chunk size:"),
        )
        .child(div().w(gpui::px(100.0)).child(
            // guardrail-allow: explicit pixel width for input field
            if let Some(cs_state) = panel.exec_chunk_size_input.as_ref().cloned() {
                div().child(
                    Input::new(&cs_state)
                        .placeholder("1000")
                        .disabled(!chunk_enabled),
                )
            } else {
                div().child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child("1000"),
                )
            },
        ));

    // Lock timeout row (disabled for DirectAutocommit)
    let lock_enabled = current_mode != ExecutionMode::DirectAutocommit;
    let lock_label_color = if lock_enabled {
        theme.foreground
    } else {
        theme.muted_foreground
    };

    let lock_row = div()
        .flex()
        .flex_row()
        .gap_1()
        .items_center()
        .child(
            div()
                .text_size(FontSizes::SM)
                .text_color(lock_label_color)
                .child("Lock timeout (ms):"),
        )
        .child(div().w(gpui::px(100.0)).child(
            // guardrail-allow: explicit pixel width for input field
            if let Some(lt_state) = panel.exec_lock_timeout_input.as_ref().cloned() {
                div().child(
                    Input::new(&lt_state)
                        .placeholder("none")
                        .disabled(!lock_enabled),
                )
            } else {
                div().child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child("none"),
                )
            },
        ));

    // Row count label
    let count_row = div()
        .text_size(FontSizes::SM)
        .text_color(theme.muted_foreground)
        .child(SharedString::from(count_text));

    div()
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .child(mode_row)
        .child(chunk_row)
        .child(lock_row)
        .child(count_row)
        .into_any_element()
}
