use crate::semantic::ThemeSettingGlobal;
use crate::tokens::SyntaxColors;
pub use crate::typography::AppFonts;
use crate::typography::load_bundled_fonts;
use dbflux_core::{AppStyle, ThemeSetting};
use gpui::{App, Hsla, SharedString, Window, WindowAppearance, hsla, px};
use gpui_component::{
    highlighter::{HighlightTheme, ThemeStyle},
    theme::{Theme, ThemeMode, ThemeTokens},
};
use std::{rc::Rc, sync::Arc};

/// Structural separator between major UI regions; resolves to the palette line.
pub fn ghost_border_color(theme: &Theme) -> Hsla {
    crate::tokens::ChromeColors::ghost_border(theme)
}

pub fn init(cx: &mut App) {
    gpui_component::init(cx);
    crate::highlighting::register_languages();
    crate::controls::register_input_overrides(cx);
    crate::modals::register_modal_keybindings(cx);
    load_bundled_fonts(cx);
    apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx);
}

/// Initialize the theme and density global from persisted settings.
///
/// Call this after `init` and after the config has been loaded, before
/// the first window opens. This sets up the correct radius tokens and
/// density global for the first frame.
pub fn init_with_settings(setting: ThemeSetting, style: AppStyle, cx: &mut App) {
    crate::density::init(cx, style);
    apply_theme(setting, style, None, cx);
}

/// Apply the Bolt Byzantium palette for `setting`.
///
/// `ThemeSetting::System` resolves through the OS appearance (the window's when
/// one is given, which is more reliable on Linux). The resolved Dark or Light
/// variant is published through `ThemeSettingGlobal` so semantic color
/// accessors follow the palette that is actually on screen.
pub fn apply_theme(
    setting: ThemeSetting,
    style: AppStyle,
    window: Option<&mut Window>,
    cx: &mut App,
) {
    let resolved = match setting {
        ThemeSetting::System => {
            let appearance = window
                .as_ref()
                .map(|window| window.appearance())
                .unwrap_or_else(|| cx.window_appearance());
            variant_for_appearance(appearance)
        }
        ThemeSetting::Dark | ThemeSetting::Light => setting,
    };

    let palette = match resolved {
        ThemeSetting::Light => Palette::light(),
        ThemeSetting::Dark | ThemeSetting::System => Palette::dark(),
    };

    Theme::change(palette.mode, window, cx);
    apply_palette(&palette, style, cx);

    // The palette mutates legacy color fields directly, which bypasses the
    // resolution `Theme::change` performed; refresh every resolved token
    // surface before handing the theme to the widgets.
    let theme = Theme::global_mut(cx);
    sync_component_tokens(theme);
    Theme::sync_base(cx);

    ThemeSettingGlobal::set(cx, resolved);
}

/// Map an OS appearance to the palette variant that matches it.
pub fn variant_for_appearance(appearance: WindowAppearance) -> ThemeSetting {
    match appearance {
        WindowAppearance::Dark | WindowAppearance::VibrantDark => ThemeSetting::Dark,
        WindowAppearance::Light | WindowAppearance::VibrantLight => ThemeSetting::Light,
    }
}

/// Re-derive the component-level tokens from the semantic colors the palette
/// assigns.
///
/// `Theme::change` resolves `Theme.tokens` and the legacy `button_*` fields
/// from gpui-component's built-in default theme. Mutating the legacy color
/// fields afterwards (as `apply_palette` does) bypasses that resolution, so
/// widgets reading `tokens.button_primary` (primary buttons) or
/// `tokens.primary` (checked checkboxes) would keep the default palette's
/// background — white in dark mode — instead of the palette accent.
///
/// Preserve the gpui-component 0.5 button appearance: default buttons use
/// secondary colors, and semantic variants use solid fills and their matching
/// foregrounds. These intentionally differ from 0.6's neutral and tinted-button
/// fallbacks. Rebuild the token snapshot after resolving the palette colors.
fn sync_component_tokens(theme: &mut Theme) {
    theme.button = theme.secondary;
    theme.button_hover = theme.secondary_hover;
    theme.button_active = theme.secondary_active;
    theme.button_foreground = theme.foreground;

    theme.button_primary = theme.primary;
    theme.button_primary_hover = theme.primary_hover;
    theme.button_primary_active = theme.primary_active;
    theme.button_primary_foreground = theme.primary_foreground;

    theme.button_secondary = theme.secondary;
    theme.button_secondary_hover = theme.secondary_hover;
    theme.button_secondary_active = theme.secondary_active;
    theme.button_secondary_foreground = theme.secondary_foreground;

    theme.button_danger = theme.danger;
    theme.button_danger_hover = theme.danger_hover;
    theme.button_danger_active = theme.danger_active;
    theme.button_danger_foreground = theme.danger_foreground;

    theme.button_success = theme.success;
    theme.button_success_hover = theme.success_hover;
    theme.button_success_active = theme.success_active;
    theme.button_success_foreground = theme.success_foreground;

    theme.button_warning = theme.warning;
    theme.button_warning_hover = theme.warning_hover;
    theme.button_warning_active = theme.warning_active;
    theme.button_warning_foreground = theme.warning_foreground;

    theme.button_info = theme.info;
    theme.button_info_hover = theme.info_hover;
    theme.button_info_active = theme.info_active;
    theme.button_info_foreground = theme.info_foreground;

    theme.table_foot = theme.list_head;
    theme.table_foot_foreground = theme.muted_foreground;
    theme.status_bar = theme.title_bar;
    theme.status_bar_border = theme.title_bar_border;

    theme.tokens = ThemeTokens::from(&theme.colors);
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

/// Persist custom font families into the stored ThemeConfig so that
/// `Theme::change()` (triggered by ThemeRegistry observer) preserves them.
/// Without this, `apply_config()` resets font_family to ".SystemUIFont".
fn persist_font_config(theme: &mut Theme) {
    let mut dark = (*theme.dark_theme).clone();
    dark.font_family = Some(SharedString::from(AppFonts::BODY));
    dark.mono_font_family = Some(SharedString::from(AppFonts::MONO));
    theme.dark_theme = Rc::new(dark);

    let mut light = (*theme.light_theme).clone();
    light.font_family = Some(SharedString::from(AppFonts::BODY));
    light.mono_font_family = Some(SharedString::from(AppFonts::MONO));
    theme.light_theme = Rc::new(light);

    // Also set the immediate values
    theme.font_family = SharedString::from(AppFonts::BODY);
    theme.mono_font_family = SharedString::from(AppFonts::MONO);
}

/// Apply border-radius values to the theme based on the active `AppStyle`.
///
/// - `AppStyle::Default` — square corners (0 px), the project's flat chrome
///   baseline.
/// - `AppStyle::Compact` — subtle radii: 2 px (`radius`) and 3 px (`radius_lg`),
///   matching the Design System token values.
fn apply_style_radius(theme: &mut Theme, style: AppStyle) {
    match style {
        AppStyle::Default => {
            theme.radius = px(0.0);
            theme.radius_lg = px(0.0);
        }
        AppStyle::Compact => {
            theme.radius = px(2.0);
            theme.radius_lg = px(3.0);
        }
    }
}

fn apply_editor_chrome(
    theme: &mut Theme,
    background: Hsla,
    active_line: Hsla,
    line_number: Hsla,
    active_line_number: Hsla,
) {
    let mut highlight_theme = (*theme.highlight_theme).clone();
    highlight_theme.style.editor_background = Some(background);
    highlight_theme.style.editor_active_line = Some(active_line);
    highlight_theme.style.editor_line_number = Some(line_number);
    highlight_theme.style.editor_active_line_number = Some(active_line_number);
    theme.highlight_theme = Arc::new(HighlightTheme {
        name: highlight_theme.name.clone(),
        appearance: highlight_theme.appearance,
        style: highlight_theme.style,
    });
}

/// Recolor the code editor's tree-sitter captures with the palette's syntax
/// roles. Captures without a role mapping keep the gpui-component default.
fn apply_syntax_colors(theme: &mut Theme, colors: &SyntaxColors) {
    let mut highlight_theme = (*theme.highlight_theme).clone();
    let syntax = &mut highlight_theme.style.syntax;

    let assignments = [
        (&mut syntax.keyword, colors.keyword),
        (&mut syntax.string, colors.string),
        (&mut syntax.string_escape, colors.string),
        (&mut syntax.string_regex, colors.string),
        (&mut syntax.string_special, colors.string),
        (&mut syntax.string_special_symbol, colors.string),
        (&mut syntax.number, colors.number),
        (&mut syntax.boolean, colors.number),
        (&mut syntax.constant, colors.number),
        (&mut syntax.comment, colors.comment),
        (&mut syntax.comment_doc, colors.comment),
        (&mut syntax.type_, colors.type_name),
        (&mut syntax.constructor, colors.type_name),
        (&mut syntax.enum_, colors.type_name),
        (&mut syntax.variable_special, colors.type_name),
        (&mut syntax.function, colors.function),
        (&mut syntax.operator, colors.operator),
        (&mut syntax.punctuation, colors.operator),
        (&mut syntax.punctuation_bracket, colors.operator),
        (&mut syntax.punctuation_delimiter, colors.operator),
        (&mut syntax.punctuation_special, colors.operator),
        (&mut syntax.variable, colors.plain),
        (&mut syntax.property, colors.plain),
        (&mut syntax.primary, colors.plain),
    ];

    for (slot, color) in assignments {
        match recolored_style(*slot, color) {
            Ok(style) => *slot = Some(style),
            Err(error) => log::warn!("Failed to recolor editor syntax style: {error}"),
        }
    }

    theme.highlight_theme = Arc::new(highlight_theme);
}

/// Return `existing` (or an empty style) with its color replaced.
///
/// gpui-component keeps `ThemeStyle` fields private and only exposes serde, so
/// the style goes through JSON; font style and weight are preserved.
fn recolored_style(existing: Option<ThemeStyle>, color: Hsla) -> serde_json::Result<ThemeStyle> {
    let mut value = match existing {
        Some(style) => serde_json::to_value(style)?,
        None => serde_json::Value::Object(serde_json::Map::new()),
    };

    if let Some(object) = value.as_object_mut() {
        object.insert("color".to_string(), serde_json::to_value(color)?);
    }

    serde_json::from_value(value)
}

/// Hand-picked color roles of one Bolt Byzantium variant.
///
/// Every value is taken from the design board; `apply_palette` maps the roles
/// onto the gpui-component `Theme` fields so both variants share one mapping.
struct Palette {
    mode: ThemeMode,

    background: Hsla,
    panel: Hsla,
    raised: Hsla,
    line: Hsla,
    line_strong: Hsla,
    row_divider: Hsla,

    text_strong: Hsla,
    text_body: Hsla,
    text_muted: Hsla,

    byzantine: Hsla,
    byzantine_hover: Hsla,
    byzantine_deep: Hsla,
    tint: Hsla,
    ink: Hsla,

    success: Hsla,
    info: Hsla,
    warning: Hsla,
    danger: Hsla,
    cyan: Hsla,
    /// Text drawn on top of solid success/info/warning/danger fills.
    semantic_foreground: Hsla,

    hover_wash: Hsla,
    alternating_row_wash: Hsla,
    selected_row_wash: Hsla,
    selected_item_wash: Hsla,
    overlay: Hsla,
    progress: Hsla,

    syntax: SyntaxColors,
}

impl Palette {
    fn dark() -> Self {
        let tint = rgb_to_hsla(0xD48CC8);
        let byzantine = rgb_to_hsla(0x702963);

        Self {
            mode: ThemeMode::Dark,

            background: rgb_to_hsla(0x09090B),
            panel: rgb_to_hsla(0x100F13),
            raised: rgb_to_hsla(0x1A181E),
            line: rgb_to_hsla(0x232128),
            line_strong: rgb_to_hsla(0x37333D),
            row_divider: rgb_to_hsla(0x18161B),

            text_strong: rgb_to_hsla(0xF7F4F7),
            text_body: rgb_to_hsla(0xC6C3CC),
            text_muted: rgb_to_hsla(0x8E8996),

            byzantine,
            byzantine_hover: rgb_to_hsla(0x7F3171),
            byzantine_deep: rgb_to_hsla(0x4A1B41),
            tint,
            ink: rgb_to_hsla(0xFFFFFF),

            success: rgb_to_hsla(0x7BE0A0),
            info: rgb_to_hsla(0x6EA8FF),
            warning: rgb_to_hsla(0xFFC23D),
            danger: rgb_to_hsla(0xFF6B5E),
            cyan: rgb_to_hsla(0x6FD3D8),
            semantic_foreground: rgb_to_hsla(0x09090B),

            hover_wash: rgb_to_hsla_alpha(0xFFFFFF, 0.04),
            alternating_row_wash: rgb_to_hsla_alpha(0xFFFFFF, 0.012),
            selected_row_wash: rgb_to_hsla_alpha(0xD48CC8, 0.07),
            selected_item_wash: rgb_to_hsla_alpha(0xD48CC8, 0.12),
            overlay: rgb_to_hsla_alpha(0x000000, 0.60),
            progress: tint,

            syntax: SyntaxColors::dark(),
        }
    }

    fn light() -> Self {
        let byzantine = rgb_to_hsla(0x702963);

        Self {
            mode: ThemeMode::Light,

            background: rgb_to_hsla(0xF6F4F7),
            panel: rgb_to_hsla(0xFFFFFF),
            raised: rgb_to_hsla(0xEEEAF0),
            line: rgb_to_hsla(0xE3DEE6),
            line_strong: rgb_to_hsla(0xCBC4D1),
            row_divider: rgb_to_hsla(0xF0ECF2),

            text_strong: rgb_to_hsla(0x141118),
            text_body: rgb_to_hsla(0x3B3740),
            text_muted: rgb_to_hsla(0x6B6572),

            byzantine,
            byzantine_hover: rgb_to_hsla(0x7F3171),
            byzantine_deep: rgb_to_hsla(0x4A1B41),
            // Byzantine doubles as the text accent on light surfaces.
            tint: byzantine,
            ink: rgb_to_hsla(0xFFFFFF),

            success: rgb_to_hsla(0x1C7F45),
            info: rgb_to_hsla(0x1F5FD1),
            warning: rgb_to_hsla(0xB7791F),
            danger: rgb_to_hsla(0xC7362B),
            cyan: rgb_to_hsla(0x0F7C82),
            semantic_foreground: rgb_to_hsla(0xFFFFFF),

            hover_wash: rgb_to_hsla_alpha(0x141118, 0.04),
            alternating_row_wash: rgb_to_hsla_alpha(0x141118, 0.015),
            selected_row_wash: rgb_to_hsla_alpha(0x702963, 0.07),
            selected_item_wash: rgb_to_hsla_alpha(0x702963, 0.14),
            overlay: rgb_to_hsla_alpha(0x141118, 0.35),
            progress: byzantine,

            syntax: SyntaxColors::light(),
        }
    }
}

/// Lightness offset between a semantic fill and its hover state.
const HOVER_LIGHTNESS_STEP: f32 = 0.05;

/// Lightness offset between a semantic fill and its pressed state.
const ACTIVE_LIGHTNESS_STEP: f32 = -0.06;

/// Share of the remaining distance to white used for the `*_light` base colors.
const LIGHT_VARIANT_MIX: f32 = 0.35;

fn shift_lightness(color: Hsla, amount: f32) -> Hsla {
    Hsla {
        l: (color.l + amount).clamp(0.0, 1.0),
        ..color
    }
}

fn lighter_variant(color: Hsla) -> Hsla {
    Hsla {
        l: color.l + (1.0 - color.l) * LIGHT_VARIANT_MIX,
        ..color
    }
}

fn with_alpha(color: Hsla, alpha: f32) -> Hsla {
    Hsla { a: alpha, ..color }
}

fn apply_palette(palette: &Palette, style: AppStyle, cx: &mut App) {
    let theme = Theme::global_mut(cx);

    persist_font_config(theme);
    apply_style_radius(theme, style);
    apply_editor_chrome(
        theme,
        palette.background,
        palette.hover_wash,
        palette.text_muted,
        palette.text_strong,
    );
    apply_syntax_colors(theme, &palette.syntax);

    theme.background = palette.background;
    theme.foreground = palette.text_body;
    theme.border = palette.line;
    theme.caret = palette.tint;
    theme.window_border = palette.line;

    theme.muted = palette.line_strong;
    theme.muted_foreground = palette.text_muted;

    theme.primary = palette.byzantine;
    theme.primary_hover = palette.byzantine_hover;
    theme.primary_active = palette.byzantine_deep;
    theme.primary_foreground = palette.ink;

    theme.secondary = palette.raised;
    theme.secondary_hover = palette.line;
    theme.secondary_active = palette.line_strong;
    theme.secondary_foreground = palette.text_body;

    theme.accent = palette.hover_wash;
    theme.accent_foreground = palette.text_strong;

    theme.danger = palette.danger;
    theme.danger_hover = shift_lightness(palette.danger, HOVER_LIGHTNESS_STEP);
    theme.danger_active = shift_lightness(palette.danger, ACTIVE_LIGHTNESS_STEP);
    theme.danger_foreground = palette.semantic_foreground;

    theme.success = palette.success;
    theme.success_hover = shift_lightness(palette.success, HOVER_LIGHTNESS_STEP);
    theme.success_active = shift_lightness(palette.success, ACTIVE_LIGHTNESS_STEP);
    theme.success_foreground = palette.semantic_foreground;

    theme.warning = palette.warning;
    theme.warning_hover = shift_lightness(palette.warning, HOVER_LIGHTNESS_STEP);
    theme.warning_active = shift_lightness(palette.warning, ACTIVE_LIGHTNESS_STEP);
    theme.warning_foreground = palette.semantic_foreground;

    theme.info = palette.info;
    theme.info_hover = shift_lightness(palette.info, HOVER_LIGHTNESS_STEP);
    theme.info_active = shift_lightness(palette.info, ACTIVE_LIGHTNESS_STEP);
    theme.info_foreground = palette.semantic_foreground;

    theme.popover = palette.panel;
    theme.popover_foreground = palette.text_body;

    theme.selection = with_alpha(palette.tint, 0.25);
    theme.ring = palette.tint;
    theme.input = palette.line_strong;

    theme.scrollbar = hsla(0.0, 0.0, 0.0, 0.0);
    theme.scrollbar_thumb = with_alpha(palette.text_muted, 0.30);
    theme.scrollbar_thumb_hover = with_alpha(palette.text_muted, 0.50);

    theme.sidebar = palette.background;
    theme.sidebar_foreground = palette.text_body;
    theme.sidebar_border = palette.line;
    theme.sidebar_accent = palette.selected_item_wash;
    theme.sidebar_accent_foreground = palette.text_strong;
    theme.sidebar_primary = palette.tint;
    theme.sidebar_primary_foreground = palette.ink;

    theme.tab = palette.background;
    theme.tab_bar = palette.background;
    theme.tab_foreground = palette.text_muted;
    theme.tab_active = palette.panel;
    theme.tab_active_foreground = palette.text_strong;
    theme.tab_bar_segmented = palette.raised;

    theme.table = palette.background;
    theme.table_head = palette.background;
    theme.table_head_foreground = palette.text_strong;
    theme.table_even = palette.alternating_row_wash;
    theme.table_hover = palette.hover_wash;
    theme.table_active = palette.selected_row_wash;
    theme.table_active_border = palette.tint;
    theme.table_row_border = palette.row_divider;

    theme.colors.list = palette.background;
    theme.list_head = palette.panel;
    theme.list_even = palette.alternating_row_wash;
    theme.list_hover = palette.hover_wash;
    theme.list_active = palette.selected_item_wash;
    theme.list_active_border = palette.tint;

    theme.accordion = palette.panel;
    theme.title_bar = palette.background;
    theme.title_bar_border = palette.line;
    theme.tiles = palette.panel;
    theme.overlay = palette.overlay;

    theme.link = palette.info;
    theme.link_hover = shift_lightness(palette.info, HOVER_LIGHTNESS_STEP);
    theme.link_active = shift_lightness(palette.info, ACTIVE_LIGHTNESS_STEP);

    theme.switch = palette.line_strong;
    theme.switch_thumb = palette.text_strong;
    theme.slider_bar = palette.line_strong;
    theme.slider_thumb = palette.tint;
    theme.progress_bar = palette.progress;
    theme.skeleton = palette.raised;

    theme.description_list_label = palette.panel;
    theme.description_list_label_foreground = palette.text_muted;

    theme.drag_border = palette.tint;
    theme.drop_target = with_alpha(palette.tint, 0.10);

    theme.group_box = palette.panel;
    theme.group_box_foreground = palette.text_body;

    theme.chart_1 = palette.tint;
    theme.chart_2 = palette.info;
    theme.chart_3 = palette.success;
    theme.chart_4 = palette.warning;
    theme.chart_5 = palette.danger;
    theme.chart_bullish = palette.success;
    theme.chart_bearish = palette.danger;

    theme.red = palette.danger;
    theme.red_light = lighter_variant(palette.danger);
    theme.green = palette.success;
    theme.green_light = lighter_variant(palette.success);
    theme.blue = palette.info;
    theme.blue_light = lighter_variant(palette.info);
    theme.yellow = palette.warning;
    theme.yellow_light = lighter_variant(palette.warning);
    theme.magenta = palette.tint;
    theme.magenta_light = lighter_variant(palette.tint);
    theme.cyan = palette.cyan;
    theme.cyan_light = lighter_variant(palette.cyan);
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;

    /// Byzantine is the primary fill of both variants.
    fn expected_primary() -> Hsla {
        rgb_to_hsla(0x702963)
    }

    /// Primary buttons fill from `tokens.button_primary` and checked
    /// checkboxes from `tokens.primary`; both must carry the byzantine fill
    /// of the applied setting, not the dependency's default (white in dark
    /// mode). Exercises the production `apply_theme` path.
    #[gpui::test]
    fn component_tokens_follow_byzantine_primary_across_all_settings(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        for setting in [
            ThemeSetting::System,
            ThemeSetting::Dark,
            ThemeSetting::Light,
        ] {
            cx.update(|cx| apply_theme(setting, AppStyle::Default, None, cx));
            cx.update(|cx| {
                let theme = Theme::global(cx);
                let expected = expected_primary();

                assert_eq!(
                    theme.tokens.primary.color, expected,
                    "{setting:?}: checkbox fill"
                );
                assert_eq!(
                    theme.tokens.button_primary.color, expected,
                    "{setting:?}: primary button fill"
                );
                assert_eq!(
                    theme.tokens.button_primary_hover.color, theme.colors.primary_hover,
                    "{setting:?}: primary button hover"
                );
                assert_eq!(
                    theme.tokens.button_primary_active.color, theme.colors.primary_active,
                    "{setting:?}: primary button active"
                );
                assert_eq!(
                    theme.tokens.button_primary_foreground.color, theme.colors.primary_foreground,
                    "{setting:?}: primary button foreground"
                );
                assert_eq!(
                    theme.colors.button_primary, expected,
                    "{setting:?}: legacy button field"
                );
            });
        }
    }

    /// Every button family and the surfaces whose dependency fallbacks derive
    /// from overridden colors must resolve to the applied palette.
    #[gpui::test]
    fn button_families_and_surfaces_follow_semantic_colors(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);

            assert_eq!(theme.colors.button, theme.colors.secondary);
            assert_eq!(theme.colors.button_hover, theme.colors.secondary_hover);
            assert_eq!(theme.colors.button_active, theme.colors.secondary_active);
            assert_eq!(theme.colors.button_foreground, theme.colors.foreground);
            assert_eq!(theme.colors.button_secondary, theme.colors.secondary);
            assert_eq!(
                theme.colors.button_secondary_hover,
                theme.colors.secondary_hover
            );
            assert_eq!(theme.colors.button_danger, theme.colors.danger);
            assert_eq!(
                theme.colors.button_danger_foreground,
                theme.colors.danger_foreground
            );
            assert_eq!(theme.colors.button_success, theme.colors.success);
            assert_eq!(theme.colors.button_warning, theme.colors.warning);
            assert_eq!(theme.colors.button_info, theme.colors.info);
            assert_eq!(theme.tokens.button_secondary.color, theme.colors.secondary);
            assert_eq!(theme.tokens.button_danger.color, theme.colors.danger);
            assert_eq!(theme.tokens.button_success.color, theme.colors.success);
            assert_eq!(theme.tokens.button_warning.color, theme.colors.warning);
            assert_eq!(theme.tokens.button_info.color, theme.colors.info);
            assert_eq!(theme.colors.status_bar, theme.colors.title_bar);
            assert_eq!(theme.colors.table_foot, theme.colors.list_head);
        });
    }

    /// Switching settings re-resolves the tokens every time; no palette may
    /// leak into another.
    #[gpui::test]
    fn theme_switches_keep_component_tokens_in_sync(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| apply_theme(ThemeSetting::Light, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.colors.background, rgb_to_hsla(0xF6F4F7));
            assert_eq!(theme.tokens.button_primary.color, expected_primary());
            assert_eq!(theme.tokens.primary.color, expected_primary());
            assert_eq!(theme.colors.ring, rgb_to_hsla(0x702963));
        });

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.colors.background, rgb_to_hsla(0x09090B));
            assert_eq!(theme.tokens.button_primary.color, expected_primary());
            assert_eq!(theme.tokens.primary.color, expected_primary());
            assert_eq!(theme.colors.ring, rgb_to_hsla(0xD48CC8));
        });
    }

    #[test]
    fn os_appearance_maps_to_the_matching_variant() {
        assert_eq!(
            variant_for_appearance(WindowAppearance::Dark),
            ThemeSetting::Dark
        );
        assert_eq!(
            variant_for_appearance(WindowAppearance::VibrantDark),
            ThemeSetting::Dark
        );
        assert_eq!(
            variant_for_appearance(WindowAppearance::Light),
            ThemeSetting::Light
        );
        assert_eq!(
            variant_for_appearance(WindowAppearance::VibrantLight),
            ThemeSetting::Light
        );
    }

    /// The semantic color accessors read `ThemeSettingGlobal`, so every
    /// `apply_theme` call must publish the variant it put on screen. The test
    /// platform reports a light appearance, which `System` must follow.
    #[gpui::test]
    fn apply_theme_publishes_the_resolved_variant(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Dark));

        cx.update(|cx| apply_theme(ThemeSetting::Light, AppStyle::Default, None, cx));
        cx.update(|cx| assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Light));

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| apply_theme(ThemeSetting::System, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let expected = variant_for_appearance(cx.window_appearance());
            assert_eq!(ThemeSettingGlobal::get(cx), expected);
            assert_eq!(
                Theme::global(cx).colors.background == rgb_to_hsla(0xF6F4F7),
                expected == ThemeSetting::Light
            );
        });
    }

    fn hex_of(color: Hsla) -> u32 {
        let rgba = gpui::Rgba::from(color);
        let channel = |value: f32| (value * 255.0).round() as u32;

        (channel(rgba.r) << 16) | (channel(rgba.g) << 8) | channel(rgba.b)
    }

    fn syntax_hex(theme: &Theme, capture: &str) -> Option<u32> {
        theme
            .highlight_theme
            .style
            .syntax
            .style(capture)
            .and_then(|style| style.color)
            .map(hex_of)
    }

    /// The SQL editor's highlight theme and the tint accessor follow the
    /// palette's syntax roles in both variants.
    #[gpui::test]
    fn editor_syntax_and_tint_follow_the_palette(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        for (setting, expected) in [
            (ThemeSetting::Dark, SyntaxColors::dark()),
            (ThemeSetting::Light, SyntaxColors::light()),
        ] {
            cx.update(|cx| apply_theme(setting, AppStyle::Default, None, cx));
            cx.update(|cx| {
                let theme = Theme::global(cx);

                for (capture, color) in [
                    ("keyword", expected.keyword),
                    ("string", expected.string),
                    ("number", expected.number),
                    ("comment", expected.comment),
                    ("type", expected.type_name),
                    ("function", expected.function),
                    ("operator", expected.operator),
                    ("punctuation.delimiter", expected.operator),
                    ("variable", expected.plain),
                ] {
                    assert_eq!(
                        syntax_hex(theme, capture),
                        Some(hex_of(color)),
                        "{setting:?}: {capture}"
                    );
                }

                assert_eq!(
                    hex_of(crate::tokens::ChromeColors::tint(theme)),
                    hex_of(expected.keyword),
                    "{setting:?}: tint"
                );
            });

            cx.update(|cx| assert_eq!(SyntaxColors::for_current(cx), expected));
        }
    }
}
