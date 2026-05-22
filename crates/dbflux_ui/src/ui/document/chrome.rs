use crate::ui::tokens::{Heights, Radii, Spacing};
use dbflux_components::icon::IconSource;
use dbflux_components::primitives::{Icon, Text};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::theme::Theme;
use gpui_component::tooltip::Tooltip;

/// Toolbar bar that matches the DataGridPanel toolbar exactly:
/// `h(Heights::TOOLBAR)` / `bg(theme.secondary)` / `border_b_1`.
///
/// `flex_shrink_0` ensures that when toolbar items wrap onto multiple rows the
/// bar claims its full content-driven height before the sibling `flex_1`
/// content area gets the residual space.  Without it the parent `flex_col`
/// keeps the bar at `min_h(TOOLBAR)` and clipped wrapped rows are invisible.
///
/// `py(Spacing::XS)` adds breathing room between wrapped rows.
pub(crate) fn compact_top_bar(
    theme: &Theme,
    children: impl IntoIterator<Item = AnyElement>,
) -> Div {
    div()
        .flex()
        .flex_wrap()
        .flex_shrink_0()
        .items_center()
        .gap(Spacing::SM)
        .min_h(Heights::TOOLBAR)
        .py(Spacing::XS)
        .px(Spacing::SM)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.secondary)
        .children(children)
}

/// Labeled control pair matching the `WHERE`/`LIMIT` style in DataGridPanel:
/// muted label text + control inline.
#[allow(dead_code)]
pub(crate) fn compact_labeled_control(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    _theme: &Theme,
) -> Div {
    div()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .child(Text::caption(label.into()))
        .child(control)
}

/// Status/footer bar that matches the DataGridPanel status bar exactly:
/// `h(Heights::ROW_COMPACT)` / `bg(theme.tab_bar)` / `border_t_1`.
pub(crate) fn workspace_footer_bar(
    theme: &Theme,
    left: impl IntoElement,
    center: impl IntoElement,
    right: impl IntoElement,
) -> Div {
    div()
        .flex()
        .items_center()
        .justify_between()
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .border_t_1()
        .border_color(theme.border)
        .bg(theme.tab_bar)
        .child(div().flex().items_center().gap(Spacing::SM).child(left))
        .child(div().flex().items_center().gap(Spacing::SM).child(center))
        .child(div().flex().items_center().gap(Spacing::SM).child(right))
}

/// Boxed click handler for [`ToolbarButton`], produced by `cx.listener(...)`.
type ToolbarClickHandler = Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>;

/// Visual variants for [`ToolbarButton`], codifying the DataGridPanel reference
/// style so every toolbar shares one button language.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolbarButtonVariant {
    /// Bordered neutral control — the data-grid reference look.
    Default,
    /// Filled accent button for the primary action of a toolbar.
    Primary,
    /// Filled danger button for inline destructive or cancel actions.
    Danger,
    /// Borderless low-emphasis button.
    Ghost,
}

/// Shared toolbar button matching the DataGridPanel reference style: 28 px tall,
/// square corners, a 16 px icon, a caption-sized label, and a focus ring driven
/// by the caller's keyboard-focus state.
///
/// Callers supply behavior (`on_click`, `disabled`, `focused`) and content
/// (`icon`, `label`, `tooltip`); the variant decides the color treatment.
#[derive(IntoElement)]
pub(crate) struct ToolbarButton {
    id: ElementId,
    icon: Option<IconSource>,
    label: Option<SharedString>,
    variant: ToolbarButtonVariant,
    focused: bool,
    disabled: bool,
    tooltip: Option<SharedString>,
    on_click: Option<ToolbarClickHandler>,
}

impl ToolbarButton {
    pub(crate) fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            icon: None,
            label: None,
            variant: ToolbarButtonVariant::Default,
            focused: false,
            disabled: false,
            tooltip: None,
            on_click: None,
        }
    }

    pub(crate) fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    pub(crate) fn label(mut self, label: impl Into<SharedString>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub(crate) fn variant(mut self, variant: ToolbarButtonVariant) -> Self {
        self.variant = variant;
        self
    }

    pub(crate) fn focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        self
    }

    pub(crate) fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    pub(crate) fn tooltip(mut self, tooltip: impl Into<SharedString>) -> Self {
        self.tooltip = Some(tooltip.into());
        self
    }

    pub(crate) fn on_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_click = Some(Box::new(handler));
        self
    }
}

impl RenderOnce for ToolbarButton {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        let foreground = theme.foreground;
        let muted = theme.muted_foreground;
        let background = theme.background;
        let secondary = theme.secondary;
        let accent_hover = theme.accent.opacity(0.08);
        let ring = theme.ring;
        let input = theme.input;
        let primary = theme.primary;
        let danger = theme.danger;
        let transparent = gpui::transparent_black();

        let ToolbarButton {
            id,
            icon,
            label,
            variant,
            focused,
            disabled,
            tooltip,
            on_click,
        } = self;

        let content_color = match variant {
            ToolbarButtonVariant::Default | ToolbarButtonVariant::Ghost => {
                if disabled {
                    muted
                } else {
                    foreground
                }
            }
            ToolbarButtonVariant::Primary => {
                if disabled {
                    muted
                } else {
                    background
                }
            }
            ToolbarButtonVariant::Danger => background,
        };

        let bg_color = match variant {
            ToolbarButtonVariant::Default => background,
            ToolbarButtonVariant::Ghost => transparent,
            ToolbarButtonVariant::Primary => {
                if disabled {
                    secondary
                } else {
                    primary
                }
            }
            ToolbarButtonVariant::Danger => danger,
        };

        let border_color = match (variant, focused) {
            (_, true) => ring,
            (ToolbarButtonVariant::Default, false) => input,
            _ => transparent,
        };

        let mut el = div()
            .id(id)
            .flex()
            .items_center()
            .gap_1()
            .h(Heights::CONTROL)
            .px(Spacing::SM)
            .rounded(Radii::SM)
            .border_1()
            .border_color(border_color)
            .bg(bg_color)
            .text_color(content_color);

        if !disabled {
            el = match variant {
                ToolbarButtonVariant::Primary | ToolbarButtonVariant::Danger => {
                    el.hover(|d| d.opacity(0.9))
                }
                ToolbarButtonVariant::Default => el.hover(move |d| d.bg(accent_hover)),
                ToolbarButtonVariant::Ghost => {
                    el.hover(move |d| d.bg(secondary).text_color(foreground))
                }
            };
        }

        if disabled {
            el = el.cursor_not_allowed();
        } else if let Some(handler) = on_click {
            el = el.cursor_pointer().on_click(handler);
        }

        if let Some(tip) = tooltip {
            el = el.tooltip(move |window, cx| Tooltip::new(tip.clone()).build(window, cx));
        }

        if let Some(icon_source) = icon {
            el = el.child(Icon::new(icon_source).small().color(content_color));
        }

        if let Some(label) = label {
            el = el.child(Text::caption(label).color(content_color));
        }

        el
    }
}
