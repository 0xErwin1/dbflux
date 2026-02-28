use dbflux_core::ThemeSetting;
use gpui::{hsla, App, Hsla, Window};
use gpui_component::theme::{Theme, ThemeMode};

pub fn init(cx: &mut App) {
    gpui_component::init(cx);
    apply_theme(ThemeSetting::Dark, None, cx);
}

pub fn apply_theme(setting: ThemeSetting, window: Option<&mut Window>, cx: &mut App) {
    match setting {
        ThemeSetting::Dark => {
            Theme::change(ThemeMode::Dark, window, cx);
            apply_ayu_dark(cx);
        }
        ThemeSetting::Light => {
            Theme::change(ThemeMode::Light, window, cx);
            apply_ayu_light(cx);
        }
    }
}

fn rgb_to_hsla(hex: u32) -> Hsla {
    let r = ((hex >> 16) & 0xFF) as f32 / 255.0;
    let g = ((hex >> 8) & 0xFF) as f32 / 255.0;
    let b = (hex & 0xFF) as f32 / 255.0;

    let max = r.max(g).max(b);
    let min = r.min(g).min(b);
    let l = (max + min) / 2.0;

    if (max - min).abs() < f32::EPSILON {
        return hsla(0.0, 0.0, l, 1.0);
    }

    let d = max - min;
    let s = if l > 0.5 {
        d / (2.0 - max - min)
    } else {
        d / (max + min)
    };

    let h = if (max - r).abs() < f32::EPSILON {
        let mut h = (g - b) / d;
        if g < b {
            h += 6.0;
        }
        h
    } else if (max - g).abs() < f32::EPSILON {
        (b - r) / d + 2.0
    } else {
        (r - g) / d + 4.0
    };

    hsla(h / 6.0, s, l, 1.0)
}

fn rgb_to_hsla_alpha(hex: u32, alpha: f32) -> Hsla {
    let mut hsla = rgb_to_hsla(hex);
    hsla.a = alpha;
    hsla
}

fn apply_ayu_dark(cx: &mut App) {
    let theme = Theme::global_mut(cx);

    // Ayu Dark base colors
    let background = rgb_to_hsla(0x0A0E14);
    let panel = rgb_to_hsla(0x0F1419);
    let foreground = rgb_to_hsla(0xB3B1AD);
    let muted = rgb_to_hsla(0x5C6773);
    let accent = rgb_to_hsla(0xFFB454);
    let border = rgb_to_hsla(0x1F2430);

    let raised = rgb_to_hsla(0x151E2B);
    let selection = rgb_to_hsla(0x273747);

    let error = rgb_to_hsla(0xF07178);
    let success = rgb_to_hsla(0xAAD94C);
    let warning = rgb_to_hsla(0xFFB454);
    let info = rgb_to_hsla(0x59C2FF);

    // Core colors
    theme.background = background;
    theme.foreground = foreground;
    theme.border = border;
    theme.caret = accent;

    // Muted
    theme.muted = muted;
    theme.muted_foreground = muted;

    // Primary (accent color)
    theme.primary = accent;
    theme.primary_hover = rgb_to_hsla(0xE6A34C);
    theme.primary_active = rgb_to_hsla(0xCC9143);
    theme.primary_foreground = rgb_to_hsla(0x0A0E14);

    // Secondary
    theme.secondary = raised;
    theme.secondary_hover = rgb_to_hsla(0x1A2535);
    theme.secondary_active = rgb_to_hsla(0x1F2A3F);
    theme.secondary_foreground = foreground;

    // Accent (hover states)
    theme.accent = rgb_to_hsla_alpha(0xB3B1AD, 0.05);
    theme.accent_foreground = foreground;

    // Semantic colors - Danger
    theme.danger = error;
    theme.danger_hover = rgb_to_hsla(0xD8656B);
    theme.danger_active = rgb_to_hsla(0xC05A5E);
    theme.danger_foreground = rgb_to_hsla(0xFFFFFF);

    // Semantic colors - Success
    theme.success = success;
    theme.success_hover = rgb_to_hsla(0x99C444);
    theme.success_active = rgb_to_hsla(0x88AF3D);
    theme.success_foreground = rgb_to_hsla(0x0A0E14);

    // Semantic colors - Warning
    theme.warning = warning;
    theme.warning_hover = rgb_to_hsla(0xE6A34C);
    theme.warning_active = rgb_to_hsla(0xCC9143);
    theme.warning_foreground = rgb_to_hsla(0x0A0E14);

    // Semantic colors - Info
    theme.info = info;
    theme.info_hover = rgb_to_hsla(0x50AFE6);
    theme.info_active = rgb_to_hsla(0x479ACC);
    theme.info_foreground = rgb_to_hsla(0x0A0E14);

    // Popover
    theme.popover = panel;
    theme.popover_foreground = foreground;

    // Selection
    theme.selection = selection;

    // Focus ring
    theme.ring = rgb_to_hsla_alpha(0xFFB454, 0.75);

    // Input
    theme.input = rgb_to_hsla_alpha(0xB3B1AD, 0.14);

    // Scrollbar
    theme.scrollbar = background;
    theme.scrollbar_thumb = rgb_to_hsla_alpha(0xB3B1AD, 0.15);
    theme.scrollbar_thumb_hover = rgb_to_hsla_alpha(0xB3B1AD, 0.25);

    // Sidebar
    theme.sidebar = panel;
    theme.sidebar_foreground = foreground;
    theme.sidebar_border = border;
    theme.sidebar_accent = rgb_to_hsla_alpha(0xB3B1AD, 0.05);
    theme.sidebar_accent_foreground = foreground;
    theme.sidebar_primary = accent;
    theme.sidebar_primary_foreground = rgb_to_hsla(0x0A0E14);

    // Tab bar
    theme.tab = panel;
    theme.tab_bar = panel;
    theme.tab_foreground = muted;
    theme.tab_active = background;
    theme.tab_active_foreground = foreground;
    theme.tab_bar_segmented = raised;

    // Table
    theme.table = background;
    theme.table_head = panel;
    theme.table_head_foreground = muted;
    theme.table_even = rgb_to_hsla_alpha(0xB3B1AD, 0.02);
    theme.table_hover = rgb_to_hsla_alpha(0xB3B1AD, 0.05);
    theme.table_active = rgb_to_hsla_alpha(0x59C2FF, 0.15);
    theme.table_active_border = rgb_to_hsla_alpha(0x59C2FF, 0.5);
    theme.table_row_border = border;

    // List
    theme.list = background;
    theme.list_head = panel;
    theme.list_even = rgb_to_hsla_alpha(0xB3B1AD, 0.02);
    theme.list_hover = rgb_to_hsla_alpha(0xB3B1AD, 0.05);
    theme.list_active = selection;
    theme.list_active_border = accent;

    // Accordion
    theme.accordion = panel;
    theme.accordion_hover = raised;

    // Title bar
    theme.title_bar = panel;
    theme.title_bar_border = border;

    // Tiles
    theme.tiles = rgb_to_hsla(0x111823);

    // Overlay
    theme.overlay = rgb_to_hsla_alpha(0x000000, 0.55);

    // Window border (Linux only)
    theme.window_border = border;

    // Link
    theme.link = info;
    theme.link_hover = rgb_to_hsla(0x6BCFFF);
    theme.link_active = rgb_to_hsla(0x50AFE6);

    // Switch
    theme.switch = muted;
    theme.switch_thumb = foreground;

    // Slider
    theme.slider_bar = muted;
    theme.slider_thumb = accent;

    // Progress bar
    theme.progress_bar = accent;

    // Skeleton
    theme.skeleton = raised;

    // Description list
    theme.description_list_label = panel;
    theme.description_list_label_foreground = muted;

    // Drag and drop
    theme.drag_border = accent;
    theme.drop_target = rgb_to_hsla_alpha(0xFFB454, 0.1);

    // Group box
    theme.group_box = panel;
    theme.group_box_foreground = foreground;

    // Chart colors
    theme.chart_1 = rgb_to_hsla(0x59C2FF);
    theme.chart_2 = rgb_to_hsla(0xAAD94C);
    theme.chart_3 = rgb_to_hsla(0xFFB454);
    theme.chart_4 = rgb_to_hsla(0xF07178);
    theme.chart_5 = rgb_to_hsla(0xD2A6FF);

    // Candlestick
    theme.bullish = success;
    theme.bearish = error;

    // Base colors
    theme.red = error;
    theme.red_light = rgb_to_hsla(0xF8A5AA);
    theme.green = success;
    theme.green_light = rgb_to_hsla(0xC5E88B);
    theme.blue = info;
    theme.blue_light = rgb_to_hsla(0x8DD6FF);
    theme.yellow = warning;
    theme.yellow_light = rgb_to_hsla(0xFFCC80);
    theme.magenta = rgb_to_hsla(0xD2A6FF);
    theme.magenta_light = rgb_to_hsla(0xE4CCFF);
    theme.cyan = rgb_to_hsla(0x95E6CB);
    theme.cyan_light = rgb_to_hsla(0xBBF0DF);
}

fn apply_ayu_light(cx: &mut App) {
    let theme = Theme::global_mut(cx);

    let background = rgb_to_hsla(0xFAFAFA);
    let panel = rgb_to_hsla(0xF3F3F3);
    let foreground = rgb_to_hsla(0x5C6166);
    let muted = rgb_to_hsla(0xABB0B6);
    let accent = rgb_to_hsla(0xFF9940);
    let border = rgb_to_hsla(0xDCDCDC);

    let raised = rgb_to_hsla(0xEEEEEE);
    let selection = rgb_to_hsla(0xD3E8F8);

    let error = rgb_to_hsla(0xE65050);
    let success = rgb_to_hsla(0x86B300);
    let warning = rgb_to_hsla(0xF2AE49);
    let info = rgb_to_hsla(0x399EE6);

    theme.background = background;
    theme.foreground = foreground;
    theme.border = border;
    theme.caret = accent;

    theme.muted = muted;
    theme.muted_foreground = muted;

    theme.primary = accent;
    theme.primary_hover = rgb_to_hsla(0xE68A3A);
    theme.primary_active = rgb_to_hsla(0xCC7A33);
    theme.primary_foreground = rgb_to_hsla(0xFFFFFF);

    theme.secondary = raised;
    theme.secondary_hover = rgb_to_hsla(0xE4E4E4);
    theme.secondary_active = rgb_to_hsla(0xDADADA);
    theme.secondary_foreground = foreground;

    theme.accent = rgb_to_hsla_alpha(0x5C6166, 0.06);
    theme.accent_foreground = foreground;

    theme.danger = error;
    theme.danger_hover = rgb_to_hsla(0xCF4848);
    theme.danger_active = rgb_to_hsla(0xB84040);
    theme.danger_foreground = rgb_to_hsla(0xFFFFFF);

    theme.success = success;
    theme.success_hover = rgb_to_hsla(0x79A100);
    theme.success_active = rgb_to_hsla(0x6D9000);
    theme.success_foreground = rgb_to_hsla(0xFFFFFF);

    theme.warning = warning;
    theme.warning_hover = rgb_to_hsla(0xDA9D42);
    theme.warning_active = rgb_to_hsla(0xC28C3B);
    theme.warning_foreground = rgb_to_hsla(0xFFFFFF);

    theme.info = info;
    theme.info_hover = rgb_to_hsla(0x338ECF);
    theme.info_active = rgb_to_hsla(0x2D7EB8);
    theme.info_foreground = rgb_to_hsla(0xFFFFFF);

    theme.popover = rgb_to_hsla(0xFFFFFF);
    theme.popover_foreground = foreground;

    theme.selection = selection;

    theme.ring = rgb_to_hsla_alpha(0xFF9940, 0.5);

    theme.input = rgb_to_hsla_alpha(0x5C6166, 0.08);

    theme.scrollbar = background;
    theme.scrollbar_thumb = rgb_to_hsla_alpha(0x5C6166, 0.15);
    theme.scrollbar_thumb_hover = rgb_to_hsla_alpha(0x5C6166, 0.3);

    theme.sidebar = panel;
    theme.sidebar_foreground = foreground;
    theme.sidebar_border = border;
    theme.sidebar_accent = rgb_to_hsla_alpha(0x5C6166, 0.06);
    theme.sidebar_accent_foreground = foreground;
    theme.sidebar_primary = accent;
    theme.sidebar_primary_foreground = rgb_to_hsla(0xFFFFFF);

    theme.tab = panel;
    theme.tab_bar = panel;
    theme.tab_foreground = muted;
    theme.tab_active = background;
    theme.tab_active_foreground = foreground;
    theme.tab_bar_segmented = raised;

    theme.table = background;
    theme.table_head = panel;
    theme.table_head_foreground = muted;
    theme.table_even = rgb_to_hsla_alpha(0x5C6166, 0.03);
    theme.table_hover = rgb_to_hsla_alpha(0x5C6166, 0.06);
    theme.table_active = rgb_to_hsla_alpha(0x399EE6, 0.12);
    theme.table_active_border = rgb_to_hsla_alpha(0x399EE6, 0.4);
    theme.table_row_border = border;

    theme.list = background;
    theme.list_head = panel;
    theme.list_even = rgb_to_hsla_alpha(0x5C6166, 0.03);
    theme.list_hover = rgb_to_hsla_alpha(0x5C6166, 0.06);
    theme.list_active = selection;
    theme.list_active_border = accent;

    theme.accordion = panel;
    theme.accordion_hover = raised;

    theme.title_bar = panel;
    theme.title_bar_border = border;

    theme.tiles = rgb_to_hsla(0xE8E8E8);

    theme.overlay = rgb_to_hsla_alpha(0x000000, 0.3);

    theme.window_border = border;

    theme.link = info;
    theme.link_hover = rgb_to_hsla(0x4CADF0);
    theme.link_active = rgb_to_hsla(0x338ECF);

    theme.switch = muted;
    theme.switch_thumb = rgb_to_hsla(0xFFFFFF);

    theme.slider_bar = muted;
    theme.slider_thumb = accent;

    theme.progress_bar = accent;

    theme.skeleton = raised;

    theme.description_list_label = panel;
    theme.description_list_label_foreground = muted;

    theme.drag_border = accent;
    theme.drop_target = rgb_to_hsla_alpha(0xFF9940, 0.1);

    theme.group_box = panel;
    theme.group_box_foreground = foreground;

    theme.chart_1 = rgb_to_hsla(0x399EE6);
    theme.chart_2 = rgb_to_hsla(0x86B300);
    theme.chart_3 = rgb_to_hsla(0xFF9940);
    theme.chart_4 = rgb_to_hsla(0xE65050);
    theme.chart_5 = rgb_to_hsla(0xA37ACC);

    theme.bullish = success;
    theme.bearish = error;

    theme.red = error;
    theme.red_light = rgb_to_hsla(0xF09090);
    theme.green = success;
    theme.green_light = rgb_to_hsla(0xB8D96E);
    theme.blue = info;
    theme.blue_light = rgb_to_hsla(0x73B8F0);
    theme.yellow = warning;
    theme.yellow_light = rgb_to_hsla(0xF5C880);
    theme.magenta = rgb_to_hsla(0xA37ACC);
    theme.magenta_light = rgb_to_hsla(0xC4A6E0);
    theme.cyan = rgb_to_hsla(0x4CBF99);
    theme.cyan_light = rgb_to_hsla(0x86D9BF);
}
