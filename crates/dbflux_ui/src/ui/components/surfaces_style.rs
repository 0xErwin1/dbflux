use crate::ui::tokens::Heights;
use gpui::Pixels;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThemeColorSlot {
    Background,
    Secondary,
    SecondaryHover,
    TabBar,
    Border,
    MutedForeground,
    Foreground,
    Primary,
    Accent,
    Transparent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BorderEdge {
    None,
    Top,
    Bottom,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PanelBarStyleKind {
    Toolbar,
    RowCompact,
    Footer,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PanelBarStyleSpec {
    pub background: ThemeColorSlot,
    pub border_color: ThemeColorSlot,
    pub border_edge: BorderEdge,
    pub height: Option<Pixels>,
    pub hover_background: Option<ThemeColorSlot>,
}

pub fn panel_bar_style_defaults(kind: PanelBarStyleKind) -> PanelBarStyleSpec {
    match kind {
        PanelBarStyleKind::Toolbar => PanelBarStyleSpec {
            background: ThemeColorSlot::TabBar,
            border_color: ThemeColorSlot::Border,
            border_edge: BorderEdge::Bottom,
            height: Some(Heights::TOOLBAR),
            hover_background: None,
        },
        PanelBarStyleKind::RowCompact => PanelBarStyleSpec {
            background: ThemeColorSlot::TabBar,
            border_color: ThemeColorSlot::Border,
            border_edge: BorderEdge::Bottom,
            height: Some(Heights::ROW_COMPACT),
            hover_background: None,
        },
        PanelBarStyleKind::Footer => PanelBarStyleSpec {
            background: ThemeColorSlot::Background,
            border_color: ThemeColorSlot::Border,
            border_edge: BorderEdge::Top,
            height: Some(Heights::TOOLBAR),
            hover_background: None,
        },
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TabTriggerStyleSpec {
    pub active_border: ThemeColorSlot,
    pub active_text: ThemeColorSlot,
    pub active_background: Option<ThemeColorSlot>,
    pub inactive_border: ThemeColorSlot,
    pub inactive_text: ThemeColorSlot,
    pub inactive_background: Option<ThemeColorSlot>,
    pub hover_background: Option<ThemeColorSlot>,
}

pub fn tab_trigger_style_defaults() -> TabTriggerStyleSpec {
    TabTriggerStyleSpec {
        active_border: ThemeColorSlot::Primary,
        active_text: ThemeColorSlot::Foreground,
        active_background: None,
        inactive_border: ThemeColorSlot::Transparent,
        inactive_text: ThemeColorSlot::MutedForeground,
        inactive_background: None,
        hover_background: Some(ThemeColorSlot::Secondary),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BorderEdge, PanelBarStyleKind, ThemeColorSlot, panel_bar_style_defaults,
        tab_trigger_style_defaults,
    };
    use crate::ui::tokens::Heights;

    #[test]
    fn toolbar_panel_bar_defaults_match_workspace_toolbar() {
        let style = panel_bar_style_defaults(PanelBarStyleKind::Toolbar);

        assert_eq!(style.background, ThemeColorSlot::TabBar);
        assert_eq!(style.border_color, ThemeColorSlot::Border);
        assert_eq!(style.border_edge, BorderEdge::Bottom);
        assert_eq!(style.height, Some(Heights::TOOLBAR));
    }

    #[test]
    fn footer_panel_bar_defaults_match_status_bar() {
        let style = panel_bar_style_defaults(PanelBarStyleKind::Footer);

        assert_eq!(style.background, ThemeColorSlot::Background);
        assert_eq!(style.border_color, ThemeColorSlot::Border);
        assert_eq!(style.border_edge, BorderEdge::Top);
        assert_eq!(style.height, Some(Heights::TOOLBAR));
    }

    #[test]
    fn tab_trigger_defaults_match_connection_manager_tabs() {
        let style = tab_trigger_style_defaults();

        assert_eq!(style.active_border, ThemeColorSlot::Primary);
        assert_eq!(style.active_text, ThemeColorSlot::Foreground);
        assert_eq!(style.active_background, None);
        assert_eq!(style.inactive_border, ThemeColorSlot::Transparent);
        assert_eq!(style.inactive_text, ThemeColorSlot::MutedForeground);
        assert_eq!(style.inactive_background, None);
        assert_eq!(style.hover_background, Some(ThemeColorSlot::Secondary));
    }
}
