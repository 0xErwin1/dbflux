//! The theme engine: one palette model, applied for whatever brand a product
//! installs.
//!
//! The neutral chrome colors of the dark and light palettes live here; the
//! accent roles come from the installed [`Brand`], so no product hue is
//! hardcoded. A product maps its own settings onto [`PaletteMode`] and
//! [`Density`], installs its [`Brand`], and calls [`install`] or
//! [`apply_theme`]. Screens never read a `gpui_component::Theme` field for an
//! accent role: they read [`crate::brand::brand`] and [`crate::brand::colors`].
//!
//! `System` is a request, not a palette: [`apply_theme`] resolves it against
//! the platform appearance and publishes `Dark` or `Light` once it knows
//! which one it installed.

use std::{rc::Rc, sync::Arc};

use gpui::{App, Hsla, Window, WindowAppearance, hsla, px};
use gpui_component::{
    highlighter::{HighlightTheme, ThemeStyle},
    theme::{Theme, ThemeConfig, ThemeMode, ThemeTokens},
};

use crate::{
    brand::{AccentWashes, Brand},
    density::Density,
    fonts::FontSettings,
    palette::{self, PaletteMode},
    tokens::{BASE_REM, ChromeColors, SyntaxColors},
};

/// Structural separator between major UI regions; resolves to the palette line.
pub fn ghost_border_color(theme: &Theme) -> Hsla {
    ChromeColors::ghost_border(theme)
}

/// Installs `brand` and applies the theme for `mode` and `density` in one
/// call, the startup path for a product that has no further settings to
/// apply first.
///
/// The product initializes `gpui_component` and its own assets (languages,
/// bundled fonts) before calling this.
pub fn install(brand: Brand, mode: PaletteMode, density: Density, cx: &mut App) {
    crate::brand::install(brand, cx);
    apply_theme(mode, density, None, cx);
}

/// Apply the palette for `mode` and the installed brand.
///
/// `PaletteMode::System` resolves through the OS appearance (the window's when
/// one is given, which is more reliable on Linux). Returns the resolved
/// `Dark` or `Light` mode so the caller can publish what is actually on
/// screen.
pub fn apply_theme(
    mode: PaletteMode,
    density: Density,
    window: Option<&mut Window>,
    cx: &mut App,
) -> PaletteMode {
    let resolved = match mode {
        PaletteMode::System => {
            let appearance = window
                .as_ref()
                .map(|window| window.appearance())
                .unwrap_or_else(|| cx.window_appearance());
            resolve_mode(mode, appearance)
        }
        PaletteMode::Dark | PaletteMode::Light => mode,
    };

    let brand = crate::brand::brand(cx);
    let palette = match resolved {
        PaletteMode::Light => Palette::light(&brand),
        PaletteMode::Dark | PaletteMode::System => Palette::dark(&brand),
    };

    Theme::change(palette.mode, window, cx);
    apply_palette(&palette, density, cx);

    // The palette mutates legacy color fields directly, which bypasses the
    // resolution `Theme::change` performed; refresh every resolved token
    // surface before handing the theme to the widgets.
    let theme = Theme::global_mut(cx);
    sync_component_tokens(theme);
    Theme::sync_base(cx);

    palette::set_mode(cx, resolved);
    resolved
}

/// Map an OS appearance to the palette variant that matches it, unless
/// `mode` already names one.
pub fn resolve_mode(mode: PaletteMode, appearance: WindowAppearance) -> PaletteMode {
    match mode {
        PaletteMode::System => match appearance {
            WindowAppearance::Dark | WindowAppearance::VibrantDark => PaletteMode::Dark,
            WindowAppearance::Light | WindowAppearance::VibrantLight => PaletteMode::Light,
        },
        resolved => resolved,
    }
}

/// Write the active font settings into the global theme without changing
/// its palette, so open windows pick them up on their next render.
///
/// Does nothing before the theme has been initialized.
pub fn apply_fonts(cx: &mut App) {
    if !cx.has_global::<Theme>() {
        return;
    }

    let fonts = crate::fonts::current(cx);
    persist_font_config(Theme::global_mut(cx), &fonts);
    Theme::sync_base(cx);
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

/// `hex` as an opaque color.
pub fn rgb_to_hsla(hex: u32) -> Hsla {
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

/// `rgb_to_hsla` with `alpha` in the fourth channel.
pub fn rgb_to_hsla_alpha(hex: u32, alpha: f32) -> Hsla {
    let mut hsla = rgb_to_hsla(hex);
    hsla.a = alpha;
    hsla
}

/// Persist the active fonts into the stored ThemeConfigs so that
/// `Theme::change()` (triggered by ThemeRegistry observer) preserves them.
/// Without this, `apply_config()` resets font_family to ".SystemUIFont".
///
/// `font_size` is the rem size every `Root` applies to its window; it keeps
/// gpui-component's 16 px at the default interface size and follows the
/// interface scale.
fn persist_font_config(theme: &mut Theme, fonts: &FontSettings) {
    let rem_size = BASE_REM * fonts.ui_size / crate::fonts::DEFAULT_UI_FONT_SIZE;

    let apply = |config: &mut ThemeConfig| {
        config.font_family = Some(fonts.ui_family.clone());
        config.font_size = Some(rem_size);
        config.mono_font_family = Some(fonts.editor_family.clone());
        config.mono_font_size = Some(fonts.editor_size);
    };

    let mut dark = (*theme.dark_theme).clone();
    apply(&mut dark);
    theme.dark_theme = Rc::new(dark);

    let mut light = (*theme.light_theme).clone();
    apply(&mut light);
    theme.light_theme = Rc::new(light);

    theme.font_family = fonts.ui_family.clone();
    theme.font_size = px(rem_size);
    theme.mono_font_family = fonts.editor_family.clone();
    theme.mono_font_size = px(fonts.editor_size);
}

/// Apply border-radius values to the theme for the active `Density`.
///
/// - `Density::Default` — square corners (0 px), the flat chrome baseline.
/// - `Density::Compact` — subtle radii: 2 px (`radius`) and 3 px (`radius_lg`),
///   matching the Design System token values.
fn apply_style_radius(theme: &mut Theme, density: Density) {
    theme.radius = density.radius();
    theme.radius_lg = density.radius_lg();
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

    // Keywords and functions are bold and aliases italic, so the roles of a
    // query differ in more than color.
    let roles = [
        ("keyword", colors.keyword, true, false),
        ("function", colors.function, true, false),
        ("namespace", colors.namespace, false, false),
        ("field", colors.field, false, false),
        ("type.alias", colors.type_name, false, true),
        ("variable.alias", colors.type_name, false, true),
        ("variable.column_alias", colors.field, false, true),
    ];
    match styled_syntax(syntax, &roles) {
        Ok(styled) => *syntax = styled,
        Err(error) => log::warn!("Failed to style editor syntax roles: {error}"),
    }

    theme.highlight_theme = Arc::new(highlight_theme);
}

/// `syntax` with each `(name, color, bold, italic)` role replaced. The fields
/// of the style types are private, so the change goes through JSON, as in
/// `recolored_style`.
fn styled_syntax(
    syntax: &gpui_component::highlighter::SyntaxColors,
    roles: &[(&str, Hsla, bool, bool)],
) -> serde_json::Result<gpui_component::highlighter::SyntaxColors> {
    let mut value = serde_json::to_value(syntax)?;

    if let Some(object) = value.as_object_mut() {
        for &(name, color, bold, italic) in roles {
            let mut style = serde_json::Map::new();
            style.insert("color".to_string(), serde_json::to_value(color)?);
            if bold {
                style.insert("font_weight".to_string(), 700.into());
            }
            if italic {
                style.insert("font_style".to_string(), "italic".into());
            }
            object.insert(name.to_string(), serde_json::Value::Object(style));
        }
    }

    serde_json::from_value(value)
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

/// Hand-picked color roles of one palette variant.
///
/// The neutral chrome values are shared by every product; the accent roles
/// come from the installed brand. `apply_palette` maps the roles onto the
/// gpui-component `Theme` fields so both variants share one mapping.
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

    accent: Hsla,
    accent_hover: Hsla,
    accent_pressed: Hsla,
    tint: Hsla,
    on_accent: Hsla,

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
    text_selection: Hsla,
    drop_target: Hsla,
    overlay: Hsla,
    progress: Hsla,

    syntax: SyntaxColors,
}

impl Palette {
    fn dark(brand: &Brand) -> Self {
        let washes = AccentWashes::for_brand(brand, PaletteMode::Dark);

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

            accent: brand.accent,
            accent_hover: brand.accent_hover,
            accent_pressed: brand.accent_pressed,
            tint: brand.accent_tint,
            on_accent: brand.on_accent,

            success: rgb_to_hsla(0x7BE0A0),
            info: rgb_to_hsla(0x6EA8FF),
            warning: rgb_to_hsla(0xFFC23D),
            danger: rgb_to_hsla(0xFF6B5E),
            cyan: rgb_to_hsla(0x6FD3D8),
            semantic_foreground: rgb_to_hsla(0x09090B),

            hover_wash: rgb_to_hsla_alpha(0xFFFFFF, 0.04),
            alternating_row_wash: rgb_to_hsla_alpha(0xFFFFFF, 0.012),
            selected_row_wash: washes.selected_row,
            selected_item_wash: washes.selected_item,
            text_selection: washes.text_selection,
            drop_target: washes.drop_target,
            overlay: rgb_to_hsla_alpha(0x050507, 0.62),
            progress: brand.accent_tint,

            syntax: SyntaxColors::dark(),
        }
    }

    fn light(brand: &Brand) -> Self {
        let washes = AccentWashes::for_brand(brand, PaletteMode::Light);

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

            accent: brand.accent,
            accent_hover: brand.accent_hover,
            accent_pressed: brand.accent_pressed,
            // The accent doubles as the text accent on light surfaces.
            tint: brand.accent,
            on_accent: brand.on_accent,

            success: rgb_to_hsla(0x1C7F45),
            info: rgb_to_hsla(0x1F5FD1),
            warning: rgb_to_hsla(0xB7791F),
            danger: rgb_to_hsla(0xC7362B),
            cyan: rgb_to_hsla(0x0F7C82),
            semantic_foreground: rgb_to_hsla(0xFFFFFF),

            hover_wash: rgb_to_hsla_alpha(0x141118, 0.04),
            alternating_row_wash: rgb_to_hsla_alpha(0x141118, 0.015),
            selected_row_wash: washes.selected_row,
            selected_item_wash: washes.selected_item,
            text_selection: washes.text_selection,
            drop_target: washes.drop_target,
            overlay: rgb_to_hsla_alpha(0x1E1423, 0.28),
            progress: brand.accent,

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

fn apply_palette(palette: &Palette, density: Density, cx: &mut App) {
    let fonts = crate::fonts::current(cx);
    let theme = Theme::global_mut(cx);

    persist_font_config(theme, &fonts);
    apply_style_radius(theme, density);
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

    theme.primary = palette.accent;
    theme.primary_hover = palette.accent_hover;
    theme.primary_active = palette.accent_pressed;
    theme.primary_foreground = palette.on_accent;

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

    theme.selection = palette.text_selection;
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
    theme.sidebar_primary_foreground = palette.on_accent;

    theme.tab = palette.background;
    theme.tab_bar = palette.background;
    theme.tab_foreground = palette.text_muted;
    theme.tab_active = palette.panel;
    theme.tab_active_foreground = palette.text_strong;
    theme.tab_bar_segmented = palette.raised;

    theme.table = palette.panel;
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
    // The sidebar tree marks its selected row with the tint wash and a 2 px
    // tint bar on the left; gpui-component's rectangular
    // outline around the selected list item is not part of the design.
    theme.list_active_border = gpui::transparent_black();

    theme.accordion = palette.panel;
    theme.title_bar = palette.background;
    theme.title_bar_border = palette.line;
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
    theme.drop_target = palette.drop_target;

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

    /// A synthetic brand, deliberately far from any product's hue.
    fn test_brand() -> Brand {
        Brand {
            name: "Synthetic".into(),
            accent: rgb_to_hsla(0x2255AA),
            accent_hover: rgb_to_hsla(0x3366BB),
            accent_pressed: rgb_to_hsla(0x114499),
            accent_tint: rgb_to_hsla(0x99CCFF),
            on_accent: rgb_to_hsla(0x000000),
        }
    }

    /// Primary buttons fill from `tokens.button_primary` and checked
    /// checkboxes from `tokens.primary`; both must carry the installed
    /// brand's accent, not the dependency's default (white in dark mode).
    #[gpui::test]
    fn component_tokens_follow_the_installed_accent_across_all_modes(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        let brand = test_brand();

        for mode in [PaletteMode::Dark, PaletteMode::Light] {
            cx.update(|cx| install(brand.clone(), mode, Density::Default, cx));
            cx.update(|cx| {
                let theme = Theme::global(cx);

                assert_eq!(
                    theme.tokens.primary.color, brand.accent,
                    "{mode:?}: checkbox fill"
                );
                assert_eq!(
                    theme.tokens.button_primary.color, brand.accent,
                    "{mode:?}: primary button fill"
                );
                assert_eq!(
                    theme.tokens.button_primary_hover.color, theme.colors.primary_hover,
                    "{mode:?}: primary button hover"
                );
                assert_eq!(
                    theme.tokens.button_primary_active.color, theme.colors.primary_active,
                    "{mode:?}: primary button active"
                );
                assert_eq!(
                    theme.tokens.button_primary_foreground.color, theme.colors.primary_foreground,
                    "{mode:?}: primary button foreground"
                );
                assert_eq!(
                    theme.colors.button_primary, brand.accent,
                    "{mode:?}: legacy button field"
                );
            });
        }
    }

    /// Every button family and the surfaces whose dependency fallbacks derive
    /// from overridden colors must resolve to the applied palette.
    #[gpui::test]
    fn button_families_and_surfaces_follow_semantic_colors(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| install(test_brand(), PaletteMode::Dark, Density::Default, cx));
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

    #[gpui::test]
    fn syntax_roles_are_styled_in_both_variants(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        for (mode, colors) in [
            (PaletteMode::Dark, SyntaxColors::dark()),
            (PaletteMode::Light, SyntaxColors::light()),
        ] {
            cx.update(|cx| {
                install(test_brand(), mode, Density::Default, cx);
                let syntax = &Theme::global(cx).highlight_theme.style.syntax;
                let style = |name: &str| syntax.style(name).expect(name);

                assert_eq!(style("keyword").color, Some(colors.keyword), "{mode:?}");
                assert_eq!(style("keyword").font_weight, Some(gpui::FontWeight::BOLD));
                assert_eq!(style("function").font_weight, Some(gpui::FontWeight::BOLD));
                assert_eq!(style("namespace").color, Some(colors.namespace));
                assert_eq!(style("field").color, Some(colors.field));
                for alias in ["type.alias", "variable.alias", "variable.column_alias"] {
                    assert_eq!(
                        style(alias).font_style,
                        Some(gpui::FontStyle::Italic),
                        "{mode:?} {alias}"
                    );
                }
                assert_eq!(style("variable.alias").color, Some(colors.type_name));
                assert_eq!(style("variable.column_alias").color, Some(colors.field));
            });
        }
    }

    /// Switching modes re-resolves the tokens every time; the brand accent
    /// and the neutral chrome must follow without leaking between variants.
    #[gpui::test]
    fn theme_switches_keep_the_brand_and_chrome_in_sync(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        let brand = test_brand();

        cx.update(|cx| install(brand.clone(), PaletteMode::Dark, Density::Default, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.colors.background, rgb_to_hsla(0x09090B));
            assert_eq!(theme.tokens.button_primary.color, brand.accent);
            assert_eq!(theme.tokens.primary.color, brand.accent);
            assert_eq!(theme.colors.ring, brand.accent_tint);
        });

        cx.update(|cx| apply_theme(PaletteMode::Light, Density::Default, None, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.colors.background, rgb_to_hsla(0xF6F4F7));
            assert_eq!(theme.tokens.button_primary.color, brand.accent);
            assert_eq!(theme.tokens.primary.color, brand.accent);
            assert_eq!(theme.colors.ring, brand.accent);
        });
    }

    /// The editor's background, active line and line numbers follow the
    /// palette's neutral chrome in both variants.
    #[gpui::test]
    fn editor_chrome_follows_the_palette(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        for (mode, background, line_number) in [
            (PaletteMode::Dark, 0x09090B, 0x8E8996),
            (PaletteMode::Light, 0xF6F4F7, 0x6B6572),
        ] {
            cx.update(|cx| install(test_brand(), mode, Density::Default, cx));
            cx.update(|cx| {
                let style = &Theme::global(cx).highlight_theme.style;

                assert_eq!(style.editor_background, Some(rgb_to_hsla(background)));
                assert_eq!(style.editor_line_number, Some(rgb_to_hsla(line_number)));
            });
        }
    }

    #[gpui::test]
    fn radius_follows_the_density(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| install(test_brand(), PaletteMode::Dark, Density::Default, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.radius, px(0.0));
            assert_eq!(theme.radius_lg, px(0.0));
        });

        cx.update(|cx| {
            apply_theme(PaletteMode::Dark, Density::Compact, None, cx);
            let theme = Theme::global(cx);
            assert_eq!(theme.radius, px(2.0));
            assert_eq!(theme.radius_lg, px(3.0));
        });
    }

    /// The active font settings land in the theme and its stored configs, and
    /// survive a palette change.
    #[gpui::test]
    fn font_settings_persist_into_the_theme_and_survive_a_palette_change(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| {
            crate::fonts::init(
                cx,
                FontSettings {
                    ui_family: "Inter".into(),
                    ui_size: 19.5,
                    editor_family: "Fira Code".into(),
                    editor_size: 20.0,
                    ..FontSettings::default()
                },
            );
            install(test_brand(), PaletteMode::Dark, Density::Default, cx);

            let theme = Theme::global(cx);
            assert_eq!(theme.font_family, "Inter");
            assert_eq!(theme.font_size, px(16.0 * 19.5 / 13.0));
            assert_eq!(theme.mono_font_family, "Fira Code");
            assert_eq!(theme.mono_font_size, px(20.0));
            for config in [&theme.dark_theme, &theme.light_theme] {
                assert_eq!(config.font_family, Some("Inter".into()));
                assert_eq!(config.font_size, Some(16.0 * 19.5 / 13.0));
                assert_eq!(config.mono_font_family, Some("Fira Code".into()));
                assert_eq!(config.mono_font_size, Some(20.0));
            }
        });

        cx.update(|cx| {
            apply_theme(PaletteMode::Light, Density::Compact, None, cx);
            let theme = Theme::global(cx);
            assert_eq!(theme.font_family, "Inter");
            assert_eq!(theme.font_size, px(16.0 * 19.5 / 13.0));
        });
    }

    #[test]
    fn an_os_appearance_resolves_a_system_request() {
        assert_eq!(
            resolve_mode(PaletteMode::System, WindowAppearance::Dark),
            PaletteMode::Dark
        );
        assert_eq!(
            resolve_mode(PaletteMode::System, WindowAppearance::VibrantDark),
            PaletteMode::Dark
        );
        assert_eq!(
            resolve_mode(PaletteMode::System, WindowAppearance::Light),
            PaletteMode::Light
        );
        assert_eq!(
            resolve_mode(PaletteMode::System, WindowAppearance::VibrantLight),
            PaletteMode::Light
        );
        assert_eq!(
            resolve_mode(PaletteMode::Light, WindowAppearance::Dark),
            PaletteMode::Light
        );
    }
}
