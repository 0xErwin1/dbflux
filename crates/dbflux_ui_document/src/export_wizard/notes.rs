//! Blocks the export and import wizards share: the sentence that says why
//! something is unavailable, the mark on an invalid field, the reason beside
//! a disabled Continue, and the per-table result list of a finished run.
//!
//! Every block is drawn from shared components and tokens; nothing here has a
//! style of its own.

use dbflux_components::icons::AppIcon;
use dbflux_components::modals::modal_frame;
use dbflux_components::primitives::{Chamfer, Icon};
use dbflux_components::tokens::{ChamferCut, ChromeColors, Feedback, ModalMetrics, Spacing, ui};
use dbflux_transfer::TableTransferStatus;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

/// Icon of a hint line and of the footer reason. (12 px)
const HINT_ICON: Rems = ui(12.0);

/// Icon of a result row. (14 px)
const ROW_ICON: Rems = ui(14.0);

/// An icon beside a 12 px line of text, both starting at the top.
fn icon_line(
    icon: AppIcon,
    icon_color: Hsla,
    text_color: Hsla,
    text: impl Into<SharedString>,
) -> Div {
    div()
        .flex()
        .items_start()
        .gap(Spacing::XXS)
        .text_size(ModalMetrics::FIELD_LABEL_FONT)
        .line_height(relative(1.5))
        .text_color(text_color)
        .child(
            div()
                .flex_shrink_0()
                .mt(px(2.0))
                .child(Icon::new(icon).size(HINT_ICON).color(icon_color)),
        )
        .child(div().min_w_0().child(text.into()))
}

/// An info line: the tint info icon and the text in the body colour. Says why
/// an option is not offered, or what to do next.
pub(crate) fn info_line(
    text: impl Into<SharedString>,
    selector: &'static str,
    cx: &App,
) -> AnyElement {
    let theme = cx.theme();

    icon_line(
        AppIcon::Info,
        ChromeColors::tint(theme),
        theme.foreground,
        text,
    )
    .debug_selector(|| selector.to_string())
    .into_any_element()
}

/// The hint under an invalid field: an alert icon and the message in the
/// danger colour, so the state does not rely on colour alone.
pub(crate) fn alert_hint(
    text: impl Into<SharedString>,
    selector: &'static str,
    cx: &App,
) -> AnyElement {
    let danger = cx.theme().danger;

    icon_line(AppIcon::CircleAlert, danger, danger, text)
        .debug_selector(|| selector.to_string())
        .into_any_element()
}

/// The reason beside a disabled Continue, at the start of the footer.
pub(crate) fn footer_reason(
    text: impl Into<SharedString>,
    selector: &'static str,
    cx: &App,
) -> AnyElement {
    let theme = cx.theme();

    icon_line(
        AppIcon::Info,
        ChromeColors::tint(theme),
        theme.muted_foreground,
        text,
    )
    .mr_auto()
    .min_w_0()
    .debug_selector(|| selector.to_string())
    .into_any_element()
}

/// Width of the label column of a wizard form row. Wide enough for the longest
/// label ("Rows per INSERT") on one line at any UI font size the shared
/// 120 px column wraps it at. (140 px at the base font size)
const FORM_LABEL_WIDTH: Rems = ui(140.0);

/// A wizard form row: the label in a fixed column that never wraps, the
/// control and its optional hint beside it. Same layout as the shared
/// `modal_form_row`, with a column that fits every label of the export and
/// import wizards. The label is addressed `form-label-<label>`.
pub(crate) fn form_row(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    hint: Option<AnyElement>,
    cx: &App,
) -> Div {
    let label: SharedString = label.into();
    let selector = format!("form-label-{label}");

    div()
        .flex()
        .items_start()
        .gap(ModalMetrics::FORM_ROW_GAP)
        .py(ModalMetrics::FORM_ROW_PADDING_Y)
        .child(
            div()
                .debug_selector(move || selector)
                .w(FORM_LABEL_WIDTH)
                .flex_shrink_0()
                .pt(ModalMetrics::FORM_LABEL_OFFSET)
                .whitespace_nowrap()
                .text_color(cx.theme().foreground)
                .child(label),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .flex()
                .flex_col()
                .gap(ModalMetrics::FIELD_GAP)
                .child(control)
                .children(hint),
        )
}

/// A control with the danger edge drawn over its frame when `invalid`. The
/// wrapper takes the control's own width, so the edge wraps the input and
/// not the row.
pub(crate) fn with_danger_edge(control: impl IntoElement, invalid: bool, cx: &App) -> Div {
    let danger = cx.theme().danger;

    div().relative().child(control).when(invalid, |frame| {
        frame.child(Chamfer::new(ChamferCut::CONTROL).border(danger))
    })
}

/// How a table ended in a finished run, which decides its icon and wash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultKind {
    Completed,
    Skipped,
    Failed,
    Cancelled,
    NotAttempted,
}

impl ResultKind {
    pub(crate) fn of(status: &TableTransferStatus) -> Self {
        match status {
            TableTransferStatus::Completed { .. } => Self::Completed,
            TableTransferStatus::Skipped => Self::Skipped,
            TableTransferStatus::Failed { .. } => Self::Failed,
            TableTransferStatus::Cancelled { .. } => Self::Cancelled,
            TableTransferStatus::NotStarted => Self::NotAttempted,
        }
    }

    fn slug(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::NotAttempted => "not-attempted",
        }
    }
}

/// One table's line in the result list of a finished run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResultRow {
    pub(crate) kind: ResultKind,
    pub(crate) text: String,
}

/// The framed list of per-table results: one row per table with a status
/// icon, and the failed rows on the danger wash. Rows are addressed
/// `<prefix>-<index>-<kind>`.
pub(crate) fn render_result_rows(rows: &[ResultRow], prefix: &'static str, cx: &App) -> Div {
    let theme = cx.theme();
    let last = rows.len().saturating_sub(1);

    let items = rows.iter().enumerate().map(|(index, row)| {
        let (icon, icon_color) = match row.kind {
            ResultKind::Completed => (AppIcon::Check, theme.success),
            ResultKind::Failed => (AppIcon::CircleAlert, theme.danger),
            ResultKind::Skipped => (AppIcon::Minus, theme.muted_foreground),
            ResultKind::Cancelled => (AppIcon::CircleX, theme.muted_foreground),
            ResultKind::NotAttempted => (AppIcon::CircleAlert, theme.muted_foreground),
        };
        let selector = format!("{prefix}-{index}-{}", row.kind.slug());

        div()
            .debug_selector(move || selector)
            .flex()
            .items_start()
            .flex_shrink_0()
            .gap(ModalMetrics::LIST_ROW_GAP)
            .px(ModalMetrics::LIST_ROW_PADDING_X)
            .py(px(9.0))
            .text_size(ModalMetrics::FIELD_LABEL_FONT)
            .line_height(relative(1.5))
            .text_color(theme.foreground)
            .when(index < last, |item| {
                item.border_b_1().border_color(theme.table_row_border)
            })
            .when(row.kind == ResultKind::Failed, |item| {
                item.bg(theme.danger.opacity(Feedback::BANNER_FILL_ALPHA))
            })
            .child(
                div()
                    .flex_shrink_0()
                    .mt(px(2.0))
                    .child(Icon::new(icon).size(ROW_ICON).color(icon_color)),
            )
            .child(div().min_w_0().child(SharedString::from(row.text.clone())))
    });

    modal_frame(cx).child(div().flex().flex_col().children(items))
}
