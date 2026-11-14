//! Form field rows: a label beside the control it names, or above it in a
//! narrow column, with an optional muted description under the control.
//!
//! [`FieldRow`] builds every shape the free functions draw, plus the locked
//! look, for a row whose control cannot take input right now.

use gpui::prelude::*;
use gpui::{AnyElement, App, Div, Pixels, SharedString, div, px};

use crate::primitives::{Label, Text};
use crate::tokens::{Fields, Spacing};

/// Default label width for horizontal field rows.
const DEFAULT_LABEL_WIDTH: Pixels = px(140.0);

/// Selector every field row records, so a test can read the row's own box.
const FIELD_ROW_SELECTOR: &str = "field-row";

/// Render a standard form field row: label on the left, control on the right.
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
pub fn field_row(label: impl Into<SharedString>, control: impl IntoElement, cx: &App) -> Div {
    FieldRow::new(label, control).build(cx)
}

/// Render a form field row with a muted description below the control.
pub fn field_row_with_desc(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    desc: impl Into<SharedString>,
    cx: &App,
) -> Div {
    FieldRow::new(label, control).desc(desc).build(cx)
}

/// Render a form field row with a custom label width.
///
/// Use this when the default 140px label width doesn't fit the content.
pub fn field_row_with_label_width(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    label_width: Pixels,
    cx: &App,
) -> Div {
    FieldRow::new(label, control)
        .label_width(label_width)
        .build(cx)
}

/// Render a vertical form field row: label above the control.
///
/// Use this for narrow containers or when controls need full width.
pub fn field_row_vertical(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    cx: &App,
) -> Div {
    FieldRow::new(label, control).vertical().build(cx)
}

/// Render a vertical form field row with a muted description below the control.
pub fn field_row_vertical_with_desc(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    desc: impl Into<SharedString>,
    cx: &App,
) -> Div {
    FieldRow::new(label, control)
        .vertical()
        .desc(desc)
        .build(cx)
}

/// A form field row assembled option by option, for the shapes the free
/// functions do not cover: a description under a custom label width, a
/// vertical row with a description, and the locked look.
///
/// ```ignore
/// FieldRow::new("Port", control)
///     .desc("Left empty, the default applies")
///     .disabled(port_locked)
///     .build(cx)
/// ```
pub struct FieldRow {
    label: SharedString,
    control: AnyElement,
    desc: Option<SharedString>,
    label_width: Pixels,
    vertical: bool,
    disabled: bool,
}

impl FieldRow {
    pub fn new(label: impl Into<SharedString>, control: impl IntoElement) -> Self {
        Self {
            label: label.into(),
            control: control.into_any_element(),
            desc: None,
            label_width: DEFAULT_LABEL_WIDTH,
            vertical: false,
            disabled: false,
        }
    }

    /// Draws a muted description below the control.
    pub fn desc(mut self, desc: impl Into<SharedString>) -> Self {
        self.desc = Some(desc.into());
        self
    }

    /// Sets the width of the label column; 140 px by default.
    pub fn label_width(mut self, label_width: Pixels) -> Self {
        self.label_width = label_width;
        self
    }

    /// Stacks the label above the control instead of drawing them side by
    /// side, for a narrow container or a control that needs the full width.
    pub fn vertical(mut self) -> Self {
        self.vertical = true;
        self
    }

    /// Draws the row locked: dimmed at `Fields::DISABLED_OPACITY`, with the
    /// not-allowed cursor, and covered by a plate that hides the row's own
    /// content from the pointer, so a click on what the row draws does
    /// nothing.
    ///
    /// What it blocks is pointer input on the row's content. It does not reach
    /// into what it draws: a control the row holds keeps its own disabled
    /// state, and an overlay a control opened — a `Dropdown`'s menu, a popup —
    /// is drawn outside the row and still takes clicks. A host that locks a row
    /// around such a control disables it (`Dropdown::set_disabled`) or closes
    /// the overlay for as long as the row is locked.
    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    /// Lays the row out; the caller chains more attributes on the `Div` it
    /// returns, as it does on the ones the free functions return.
    pub fn build(self, _cx: &App) -> Div {
        let FieldRow {
            label,
            control,
            desc,
            label_width,
            vertical,
            disabled,
        } = self;

        let label = Label::new(label);
        let row = if vertical {
            vertical_row(label, control, desc)
        } else {
            horizontal_row(label, control, desc, label_width)
        };
        let row = row.debug_selector(|| FIELD_ROW_SELECTOR.to_string());

        if !disabled {
            return row;
        }

        row.opacity(Fields::DISABLED_OPACITY)
            .cursor_not_allowed()
            .relative()
            .child(div().absolute().inset_0().occlude())
    }
}

fn horizontal_row(
    label: impl IntoElement,
    control: impl IntoElement,
    desc: Option<SharedString>,
    label_width: Pixels,
) -> Div {
    let mut control_col = div().flex_1().child(control);

    if let Some(desc_text) = desc {
        control_col = control_col.child(desc_line(desc_text));
    }

    div()
        .flex()
        .items_start()
        .gap(Spacing::MD)
        .child(
            div()
                .w(label_width)
                .pt(Spacing::XXS)
                .flex_shrink_0()
                .child(label),
        )
        .child(control_col)
}

fn vertical_row(
    label: impl IntoElement,
    control: impl IntoElement,
    desc: Option<SharedString>,
) -> Div {
    let mut col = div()
        .flex_col()
        .gap(Spacing::XS)
        .child(label)
        .child(div().w_full().child(control));

    if let Some(desc_text) = desc {
        col = col.child(desc_line(desc_text));
    }

    div().flex().flex_col().child(col)
}

fn desc_line(desc: SharedString) -> Div {
    div().mt(px(2.0)).child(Text::caption(desc))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use gpui::{
        Bounds, Context, IntoElement, Modifiers, ParentElement as _, Pixels, Render, Styled as _,
        TestAppContext, VisualTestContext, Window, div,
    };

    use super::{DEFAULT_LABEL_WIDTH, FieldRow, field_row};
    use crate::controls::Button;
    use crate::tokens::Spacing;

    struct RowHost {
        clicks: Rc<Cell<usize>>,
        disabled: bool,
        built: bool,
    }

    impl Render for RowHost {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            let clicks = self.clicks.clone();
            let control = Button::new("row-button", "Get new token")
                .on_click(move |_, _, _| clicks.set(clicks.get() + 1));
            let row = if self.built {
                FieldRow::new("Token", control)
                    .disabled(self.disabled)
                    .build(cx)
            } else {
                field_row("Token", control, cx)
            };

            div().size_full().child(row)
        }
    }

    fn open_row(
        disabled: bool,
        built: bool,
        cx: &mut TestAppContext,
    ) -> (Rc<Cell<usize>>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            crate::theme::install(
                crate::brand::Brand::neutral(),
                crate::palette::PaletteMode::Dark,
                crate::density::Density::Default,
                cx,
            );
        });

        let clicks = Rc::new(Cell::new(0));
        let clicks_for_host = clicks.clone();
        let (_host, window) = cx.add_window_view(move |_, _| RowHost {
            clicks: clicks_for_host,
            disabled,
            built,
        });
        window.run_until_parked();

        (clicks, window)
    }

    fn control_bounds(window: &mut VisualTestContext) -> Bounds<Pixels> {
        window
            .debug_bounds("row-button")
            .expect("the control of the row is laid out")
    }

    fn click_control(window: &mut VisualTestContext) {
        let center = window
            .debug_bounds("row-button")
            .expect("the control of the row is laid out")
            .center();
        window.simulate_click(center, Modifiers::default());
        window.run_until_parked();
    }

    #[gpui::test]
    fn a_locked_row_ignores_a_click_on_its_control(cx: &mut TestAppContext) {
        let (clicks, window) = open_row(true, true, cx);

        click_control(window);

        assert_eq!(clicks.get(), 0, "a locked row takes no click");
    }

    #[gpui::test]
    fn a_row_that_is_not_locked_keeps_its_control_clickable(cx: &mut TestAppContext) {
        let (clicks, window) = open_row(false, true, cx);

        click_control(window);

        assert_eq!(clicks.get(), 1);
    }

    #[gpui::test]
    fn a_horizontal_row_starts_its_control_after_the_label_column(cx: &mut TestAppContext) {
        let (_clicks, window) = open_row(false, true, cx);

        let row = window
            .debug_bounds("field-row")
            .expect("the row records its selector");
        let control = control_bounds(window);

        assert_eq!(
            control.left() - row.left(),
            DEFAULT_LABEL_WIDTH + Spacing::MD
        );
    }

    #[gpui::test]
    fn the_free_function_draws_the_control_where_the_builder_does(cx: &mut TestAppContext) {
        let (_clicks, window) = open_row(false, false, cx);
        let drawn_by_function = control_bounds(window);

        let (_clicks, window) = open_row(false, true, cx);
        let drawn_by_builder = control_bounds(window);

        assert_eq!(drawn_by_function, drawn_by_builder);
    }
}
