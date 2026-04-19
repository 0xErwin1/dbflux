use gpui::prelude::*;
use gpui::{App, ClickEvent, Hsla, Pixels, SharedString, Stateful, Window, div};
use gpui_component::ActiveTheme;
use gpui_component::IconName;

use crate::icon::IconSource;
use crate::primitives::{Icon, SurfaceRole};
use crate::tokens::{FontSizes, Heights, Spacing};
use crate::typography::{MonoCaption, MonoTextInspection};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PanelHeaderVariant {
    Standard,
    WorkspaceTasks,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PanelHeaderBackground {
    Surface(SurfaceRole),
    ThemeTabBar,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PanelHeaderTitleColor {
    Foreground,
    Primary,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PanelHeaderInspection {
    pub background: PanelHeaderBackground,
    pub hover_background: Option<PanelHeaderBackground>,
    pub height: Pixels,
    pub horizontal_padding: Pixels,
    pub shows_chevron: bool,
    pub shows_leading_icon: bool,
    pub has_actions: bool,
    pub title: MonoTextInspection,
    pub base_title_color: PanelHeaderTitleColor,
    pub focus_title_color: Option<PanelHeaderTitleColor>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PanelHeaderContract {
    background: PanelHeaderBackground,
    hover_background: Option<PanelHeaderBackground>,
    height: Pixels,
    horizontal_padding: Pixels,
    base_title_color: PanelHeaderTitleColor,
    focus_title_color: Option<PanelHeaderTitleColor>,
    supports_leading_icon: bool,
}

fn panel_header_contract(variant: PanelHeaderVariant) -> PanelHeaderContract {
    match variant {
        PanelHeaderVariant::Standard => PanelHeaderContract {
            background: PanelHeaderBackground::Surface(SurfaceRole::Card),
            hover_background: None,
            height: Heights::TOOLBAR,
            horizontal_padding: Spacing::SM,
            base_title_color: PanelHeaderTitleColor::Foreground,
            focus_title_color: None,
            supports_leading_icon: false,
        },
        PanelHeaderVariant::WorkspaceTasks => PanelHeaderContract {
            background: PanelHeaderBackground::ThemeTabBar,
            hover_background: Some(PanelHeaderBackground::Surface(SurfaceRole::Card)),
            height: Heights::ROW_COMPACT,
            horizontal_padding: Spacing::SM,
            base_title_color: PanelHeaderTitleColor::Foreground,
            focus_title_color: Some(PanelHeaderTitleColor::Primary),
            supports_leading_icon: true,
        },
    }
}

fn title_color(selection: PanelHeaderTitleColor, theme: &gpui_component::Theme) -> Hsla {
    match selection {
        PanelHeaderTitleColor::Foreground => theme.foreground,
        PanelHeaderTitleColor::Primary => theme.primary,
    }
}

fn background_color(selection: PanelHeaderBackground, theme: &gpui_component::Theme) -> Hsla {
    match selection {
        PanelHeaderBackground::Surface(SurfaceRole::Panel) => theme.background,
        PanelHeaderBackground::Surface(SurfaceRole::Card) => theme.secondary,
        PanelHeaderBackground::Surface(SurfaceRole::Raised) => theme.popover,
        PanelHeaderBackground::Surface(SurfaceRole::ModalContainer) => theme.popover,
        PanelHeaderBackground::Surface(SurfaceRole::Scrim) => gpui::black().opacity(0.5),
        PanelHeaderBackground::ThemeTabBar => theme.tab_bar,
    }
}

fn title_inspection(focused: bool) -> MonoTextInspection {
    let mut title = MonoCaption::new("Panel").font_size(FontSizes::SM);

    title = if focused {
        title.font_weight(gpui::FontWeight::BOLD)
    } else {
        title.font_weight(gpui::FontWeight::MEDIUM)
    };

    title.inspect()
}

pub fn inspect_panel_header(
    variant: PanelHeaderVariant,
    collapsible: bool,
    focused: bool,
    has_actions: bool,
) -> PanelHeaderInspection {
    let contract = panel_header_contract(variant);
    let shows_chevron = collapsible || matches!(variant, PanelHeaderVariant::WorkspaceTasks);

    PanelHeaderInspection {
        background: contract.background,
        hover_background: contract.hover_background,
        height: contract.height,
        horizontal_padding: contract.horizontal_padding,
        shows_chevron,
        shows_leading_icon: contract.supports_leading_icon,
        has_actions,
        title: title_inspection(focused),
        base_title_color: contract.base_title_color,
        focus_title_color: contract.focus_title_color,
    }
}

pub fn panel_header_variant(
    title: impl Into<SharedString>,
    variant: PanelHeaderVariant,
    cx: &App,
) -> gpui::Div {
    panel_header_layout(title.into(), variant, false, false, None, Vec::new(), cx)
}

pub fn panel_header_variant_with_actions(
    title: impl Into<SharedString>,
    variant: PanelHeaderVariant,
    actions: Vec<impl IntoElement>,
    cx: &App,
) -> gpui::Div {
    let actions = actions
        .into_iter()
        .map(|action| action.into_any_element())
        .collect();

    panel_header_layout(title.into(), variant, false, false, None, actions, cx)
}

pub fn panel_header_collapsible_variant(
    id: impl Into<gpui::ElementId>,
    title: impl Into<SharedString>,
    variant: PanelHeaderVariant,
    collapsed: bool,
    leading_icon: Option<IconName>,
    on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    cx: &App,
) -> Stateful<gpui::Div> {
    panel_header_layout_stateful(
        id.into(),
        title.into(),
        variant,
        true,
        false,
        leading_icon,
        Some(Box::new(on_toggle)),
        collapsed,
        cx,
    )
}

/// Render a toolbar-height panel header with a title.
pub fn panel_header(title: impl Into<SharedString>, cx: &App) -> gpui::Div {
    panel_header_variant(title, PanelHeaderVariant::Standard, cx)
}

/// Render a panel header with right-aligned action elements.
pub fn panel_header_with_actions(
    title: impl Into<SharedString>,
    actions: Vec<impl IntoElement>,
    cx: &App,
) -> gpui::Div {
    panel_header_variant_with_actions(title, PanelHeaderVariant::Standard, actions, cx)
}

/// Render a collapsible panel header with a chevron toggle and click handler.
pub fn panel_header_collapsible(
    id: impl Into<gpui::ElementId>,
    title: impl Into<SharedString>,
    collapsed: bool,
    on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    cx: &App,
) -> Stateful<gpui::Div> {
    panel_header_collapsible_variant(
        id,
        title,
        PanelHeaderVariant::Standard,
        collapsed,
        None,
        on_toggle,
        cx,
    )
}

/// Compatibility shim while callers migrate to sanctioned variants.
#[allow(clippy::too_many_arguments)]
pub fn panel_header_custom(
    id: impl Into<gpui::ElementId>,
    title: impl Into<SharedString>,
    collapsed: bool,
    leading_icon: Option<IconName>,
    height: Pixels,
    bg: Hsla,
    on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    cx: &App,
) -> Stateful<gpui::Div> {
    panel_header_custom_stateful(
        id.into(),
        title.into(),
        collapsed,
        leading_icon,
        height,
        bg,
        Box::new(on_toggle),
        cx,
    )
}

#[allow(clippy::too_many_arguments)]
fn panel_header_layout(
    title: SharedString,
    variant: PanelHeaderVariant,
    collapsible: bool,
    focused: bool,
    leading_icon: Option<IconName>,
    actions: Vec<gpui::AnyElement>,
    cx: &App,
) -> gpui::Div {
    let contract = panel_header_contract(variant);
    let theme = cx.theme();

    let mut left = div().flex().items_center().gap(Spacing::SM);

    if collapsible {
        let chevron = if focused {
            IconName::ChevronDown
        } else {
            IconName::ChevronRight
        };

        left = left.child(
            Icon::new(IconSource::Named(chevron))
                .size(Heights::ICON_SM)
                .color(title_color(
                    contract
                        .focus_title_color
                        .filter(|_| focused)
                        .unwrap_or(contract.base_title_color),
                    theme,
                )),
        );
    }

    if contract.supports_leading_icon {
        if let Some(leading_icon) = leading_icon {
            left = left.child(
                Icon::new(IconSource::Named(leading_icon))
                    .size(Heights::ICON_SM)
                    .color(title_color(
                        contract
                            .focus_title_color
                            .filter(|_| focused)
                            .unwrap_or(contract.base_title_color),
                        theme,
                    )),
            );
        }
    }

    let title = MonoCaption::new(title)
        .font_size(FontSizes::SM)
        .font_weight(if focused {
            gpui::FontWeight::BOLD
        } else {
            gpui::FontWeight::MEDIUM
        })
        .color(title_color(
            contract
                .focus_title_color
                .filter(|_| focused)
                .unwrap_or(contract.base_title_color),
            theme,
        ));

    left = left.child(title);

    let mut header = div()
        .flex()
        .items_center()
        .justify_between()
        .h(contract.height)
        .px(contract.horizontal_padding)
        .bg(background_color(contract.background, theme))
        .border_b_1()
        .border_color(theme.border)
        .child(left);

    if let Some(hover_background) = contract.hover_background {
        let hover_color = background_color(hover_background, theme);
        header = header.hover(move |style| style.bg(hover_color));
    }

    if !actions.is_empty() {
        header = header.child(
            div()
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .children(actions),
        );
    }

    header
}

#[allow(clippy::too_many_arguments)]
fn panel_header_layout_stateful(
    id: gpui::ElementId,
    title: SharedString,
    variant: PanelHeaderVariant,
    collapsible: bool,
    focused: bool,
    leading_icon: Option<IconName>,
    on_click: Option<Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync>>,
    collapsed: bool,
    cx: &App,
) -> Stateful<gpui::Div> {
    let contract = panel_header_contract(variant);
    let theme = cx.theme();

    let mut left = div().flex().items_center().gap(Spacing::SM);

    if collapsible {
        let chevron = if collapsed {
            IconName::ChevronRight
        } else {
            IconName::ChevronDown
        };

        let tone = contract
            .focus_title_color
            .filter(|_| focused)
            .unwrap_or(contract.base_title_color);

        left = left.child(
            Icon::new(IconSource::Named(chevron))
                .size(Heights::ICON_SM)
                .color(title_color(tone, theme)),
        );
    }

    if contract.supports_leading_icon {
        if let Some(leading_icon) = leading_icon {
            let tone = contract
                .focus_title_color
                .filter(|_| focused)
                .unwrap_or(contract.base_title_color);

            left = left.child(
                Icon::new(IconSource::Named(leading_icon))
                    .size(Heights::ICON_SM)
                    .color(title_color(tone, theme)),
            );
        }
    }

    let tone = contract
        .focus_title_color
        .filter(|_| focused)
        .unwrap_or(contract.base_title_color);

    let title = MonoCaption::new(title)
        .font_size(FontSizes::SM)
        .font_weight(if focused {
            gpui::FontWeight::BOLD
        } else {
            gpui::FontWeight::MEDIUM
        })
        .color(title_color(tone, theme));

    left = left.child(title);

    let mut header = div()
        .id(id)
        .flex()
        .items_center()
        .justify_between()
        .h(contract.height)
        .px(contract.horizontal_padding)
        .bg(background_color(contract.background, theme))
        .border_b_1()
        .border_color(theme.border)
        .cursor_pointer()
        .child(left);

    if let Some(hover_background) = contract.hover_background {
        let hover_color = background_color(hover_background, theme);
        header = header.hover(move |style| style.bg(hover_color));
    }

    if let Some(on_click) = on_click {
        header = header.on_click(on_click);
    }

    header
}

#[allow(clippy::too_many_arguments)]
fn panel_header_custom_stateful(
    id: gpui::ElementId,
    title: SharedString,
    collapsed: bool,
    leading_icon: Option<IconName>,
    height: Pixels,
    bg: Hsla,
    on_click: Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync>,
    cx: &App,
) -> Stateful<gpui::Div> {
    let theme = cx.theme();

    let chevron = if collapsed {
        IconName::ChevronRight
    } else {
        IconName::ChevronDown
    };

    let mut left = div().flex().items_center().gap(Spacing::SM).child(
        Icon::new(IconSource::Named(chevron))
            .size(Heights::ICON_SM)
            .color(theme.muted_foreground),
    );

    if let Some(leading_icon) = leading_icon {
        left = left.child(
            Icon::new(IconSource::Named(leading_icon))
                .size(Heights::ICON_SM)
                .color(theme.muted_foreground),
        );
    }

    left = left.child(
        MonoCaption::new(title)
            .font_size(FontSizes::SM)
            .font_weight(gpui::FontWeight::MEDIUM)
            .color(theme.foreground),
    );

    div()
        .id(id)
        .flex()
        .items_center()
        .justify_between()
        .h(height)
        .px(Spacing::SM)
        .bg(bg)
        .border_b_1()
        .border_color(theme.border)
        .cursor_pointer()
        .child(left)
        .on_click(on_click)
}

#[cfg(test)]
mod tests {
    use super::{
        PanelHeaderBackground, PanelHeaderTitleColor, PanelHeaderVariant, inspect_panel_header,
    };
    use crate::primitives::SurfaceRole;
    use crate::tokens::{FontSizes, Heights, Spacing};
    use crate::typography::AppFonts;
    use gpui::FontWeight;

    #[test]
    fn default_panel_header_keeps_toolbar_metrics_and_mono_title_contract() {
        let inspection = inspect_panel_header(PanelHeaderVariant::Standard, false, false, false);

        assert_eq!(inspection.height, Heights::TOOLBAR);
        assert_eq!(inspection.horizontal_padding, Spacing::SM);
        assert_eq!(
            inspection.background,
            PanelHeaderBackground::Surface(SurfaceRole::Card)
        );
        assert_eq!(inspection.title.family, Some(AppFonts::MONO));
        assert_eq!(inspection.title.size_override, Some(FontSizes::SM));
        assert_eq!(inspection.title.weight_override, Some(FontWeight::MEDIUM));
        assert_eq!(
            inspection.base_title_color,
            PanelHeaderTitleColor::Foreground
        );
    }

    #[test]
    fn workspace_panel_header_focus_and_collapse_state_stay_in_the_shared_contract() {
        let collapsed =
            inspect_panel_header(PanelHeaderVariant::WorkspaceTasks, true, false, false);
        assert_eq!(collapsed.height, Heights::ROW_COMPACT);
        assert!(collapsed.shows_chevron);
        assert!(collapsed.shows_leading_icon);
        assert!(!collapsed.has_actions);
        assert_eq!(collapsed.title.weight_override, Some(FontWeight::MEDIUM));

        let focused = inspect_panel_header(PanelHeaderVariant::WorkspaceTasks, false, true, true);
        assert!(focused.shows_chevron);
        assert!(focused.shows_leading_icon);
        assert!(focused.has_actions);
        assert_eq!(focused.title.weight_override, Some(FontWeight::BOLD));
        assert_eq!(focused.base_title_color, PanelHeaderTitleColor::Foreground);
        assert_eq!(
            focused.focus_title_color,
            Some(PanelHeaderTitleColor::Primary)
        );
        assert_eq!(
            focused.hover_background,
            Some(PanelHeaderBackground::Surface(SurfaceRole::Card))
        );
    }
}
