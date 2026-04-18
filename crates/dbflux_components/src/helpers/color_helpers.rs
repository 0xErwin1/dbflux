use gpui::Hsla;
use gpui_component::Theme;

pub fn text_color_for_active(is_active: bool, theme: &Theme) -> Hsla {
    if is_active {
        theme.foreground
    } else {
        theme.muted_foreground
    }
}

pub fn text_color_for_selected(is_selected: bool, theme: &Theme) -> Hsla {
    if is_selected {
        theme.primary_foreground
    } else {
        theme.muted_foreground
    }
}

pub fn text_color_for_enabled(is_enabled: bool, theme: &Theme) -> Hsla {
    if is_enabled {
        theme.foreground
    } else {
        theme.muted_foreground
    }
}

pub fn text_color_for_danger(is_danger: bool, theme: &Theme) -> Hsla {
    if is_danger {
        theme.danger
    } else {
        theme.muted_foreground
    }
}

pub fn text_color_for_has_changes(has_changes: bool, theme: &Theme) -> Hsla {
    if has_changes {
        theme.foreground
    } else {
        theme.muted_foreground
    }
}
