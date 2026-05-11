use crate::keymap::ContextId;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Radii, Spacing};
use dbflux_components::controls::{GpuiInput as Input, InputEvent, InputState};
#[cfg(test)]
use dbflux_components::helpers::text_color_for_selected;
use dbflux_components::primitives::{Chord, Icon, overlay_bg, surface_modal_container};
use dbflux_components::tokens::BannerColors;
use dbflux_components::typography::{Body, MonoCaption, MonoLabel};
use dbflux_core::{CollectionRef, TableRef};
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::{ActiveTheme, Sizable};
use std::path::PathBuf;
use uuid::Uuid;

actions!(command_palette, [SelectNext, SelectPrev, Close, Execute]);

pub fn command_palette_keybindings() -> Vec<KeyBinding> {
    let ctx = Some(ContextId::CommandPalette.as_gpui_context());
    vec![
        KeyBinding::new("up", SelectPrev, ctx),
        KeyBinding::new("down", SelectNext, ctx),
        KeyBinding::new("ctrl-k", SelectPrev, ctx),
        KeyBinding::new("ctrl-j", SelectNext, ctx),
        KeyBinding::new("escape", Close, ctx),
        KeyBinding::new("enter", Execute, ctx),
    ]
}

/// A searchable item in the command palette.
#[derive(Clone)]
pub enum PaletteItem {
    Action {
        id: &'static str,
        name: &'static str,
        category: &'static str,
        shortcut: Option<&'static str>,
    },
    Connection {
        profile_id: Uuid,
        name: String,
        is_connected: bool,
    },
    Resource(ResourceItem),
    Script {
        /// Absolute filesystem path (used to open the script).
        path: PathBuf,
        /// File name (e.g., "health-check.sql").
        name: String,
        /// Path relative to the scripts root directory (for display/search).
        relative_path: String,
    },
}

/// Schema resource variants surfaced by connected profiles.
#[derive(Clone)]
pub enum ResourceItem {
    Table {
        profile_id: Uuid,
        profile_name: String,
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    Collection {
        profile_id: Uuid,
        profile_name: String,
        database: String,
        name: String,
    },
    View {
        profile_id: Uuid,
        profile_name: String,
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    KeyValueDb {
        profile_id: Uuid,
        profile_name: String,
        database: String,
    },
}

impl PaletteItem {
    /// Text searched by `SkimMatcherV2`.
    pub fn search_text(&self) -> String {
        match self {
            Self::Action { category, name, .. } => format!("{} {}", category, name),
            Self::Connection { name, .. } => format!("Connection {}", name),
            Self::Resource(r) => match r {
                ResourceItem::Table {
                    profile_name,
                    database,
                    schema,
                    name,
                    ..
                } => {
                    let mut parts = format!("Table {} {}", profile_name, name);
                    if let Some(db) = database {
                        parts.push_str(&format!(" {}", db));
                    }
                    if let Some(s) = schema {
                        parts.push_str(&format!(" {}", s));
                    }
                    parts
                }
                ResourceItem::Collection {
                    profile_name,
                    database,
                    name,
                    ..
                } => format!("Collection {} {} {}", profile_name, name, database),
                ResourceItem::View {
                    profile_name,
                    database,
                    schema,
                    name,
                    ..
                } => {
                    let mut parts = format!("View {} {}", profile_name, name);
                    if let Some(db) = database {
                        parts.push_str(&format!(" {}", db));
                    }
                    if let Some(s) = schema {
                        parts.push_str(&format!(" {}", s));
                    }
                    parts
                }
                ResourceItem::KeyValueDb {
                    profile_name,
                    database,
                    ..
                } => format!("Keyspace {} {}", profile_name, database),
            },
            Self::Script {
                name,
                relative_path,
                ..
            } => {
                format!("Script {} {}", name, relative_path)
            }
        }
    }

    /// Returns `(category_label, display_name)`.
    pub fn display_label(&self) -> (String, String) {
        match self {
            Self::Action { category, name, .. } => (category.to_string(), name.to_string()),
            Self::Connection { name, .. } => ("Connection".to_string(), name.clone()),
            Self::Resource(r) => match r {
                ResourceItem::Table { name, .. } => ("Table".to_string(), name.clone()),
                ResourceItem::Collection { name, .. } => ("Collection".to_string(), name.clone()),
                ResourceItem::View { name, .. } => ("View".to_string(), name.clone()),
                ResourceItem::KeyValueDb { database, .. } => {
                    ("Keyspace".to_string(), database.clone())
                }
            },
            Self::Script { name, .. } => ("Script".to_string(), name.clone()),
        }
    }

    /// Type priority for tiebreaking (lower = higher priority).
    pub fn type_priority(&self) -> u8 {
        match self {
            Self::Action { .. } => 0,
            Self::Connection { .. } => 1,
            Self::Resource(_) => 2,
            Self::Script { .. } => 3,
        }
    }

    /// Optional qualifier text shown after the item name.
    pub fn qualifier(&self) -> Option<String> {
        match self {
            Self::Action { shortcut, .. } => shortcut.map(|s| s.to_string()),
            Self::Resource(r) => match r {
                ResourceItem::Table {
                    profile_name,
                    database,
                    schema,
                    ..
                }
                | ResourceItem::View {
                    profile_name,
                    database,
                    schema,
                    ..
                } => {
                    let mut parts = profile_name.clone();
                    if let Some(db) = database {
                        parts.push_str(&format!(" / {}", db));
                    }
                    if let Some(s) = schema {
                        parts.push_str(&format!(" / {}", s));
                    }
                    Some(parts)
                }
                ResourceItem::Collection {
                    profile_name,
                    database,
                    ..
                } => Some(format!("{} / {}", profile_name, database)),
                ResourceItem::KeyValueDb { profile_name, .. } => Some(profile_name.clone()),
            },
            Self::Script { relative_path, .. } => {
                if relative_path.contains('/') {
                    Some(relative_path.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Legacy static command descriptor kept for `default_commands()` backwards compat.
#[derive(Clone)]
pub struct PaletteCommand {
    pub id: &'static str,
    pub name: &'static str,
    pub category: &'static str,
    pub shortcut: Option<&'static str>,
}

impl PaletteCommand {
    pub const fn new(id: &'static str, name: &'static str, category: &'static str) -> Self {
        Self {
            id,
            name,
            category,
            shortcut: None,
        }
    }

    pub const fn with_shortcut(mut self, shortcut: &'static str) -> Self {
        self.shortcut = Some(shortcut);
        self
    }
}

impl From<PaletteCommand> for PaletteItem {
    fn from(cmd: PaletteCommand) -> Self {
        PaletteItem::Action {
            id: cmd.id,
            name: cmd.name,
            category: cmd.category,
            shortcut: cmd.shortcut,
        }
    }
}

struct FilteredItem {
    index: usize,
    score: i64,
}

const VISIBLE_ITEMS: usize = 8;

/// Section grouping for the rendered palette list.
///
/// The order here is the visual order in the palette. Sections render only
/// when at least one matching item exists for that section. Section headers
/// themselves are not selectable — they are a render-only concern.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PaletteSection {
    Connections,
    Commands,
    Tables,
    Scripts,
}

impl PaletteSection {
    fn label(self) -> &'static str {
        match self {
            Self::Connections => "Connections",
            Self::Commands => "Commands",
            Self::Tables => "Tables",
            Self::Scripts => "Scripts",
        }
    }

    fn for_item(item: &PaletteItem) -> Self {
        match item {
            PaletteItem::Connection { .. } => Self::Connections,
            PaletteItem::Action { .. } => Self::Commands,
            PaletteItem::Resource(_) => Self::Tables,
            PaletteItem::Script { .. } => Self::Scripts,
        }
    }
}

/// Render row produced by section grouping.
///
/// `Item.display_idx` is the position in the filtered list (i.e. the value
/// `selected_index` compares against). `palette_idx` is the index into
/// `items`. `SectionHeader` rows are not selectable.
enum PaletteRow {
    SectionHeader(SharedString),
    Item {
        display_idx: usize,
        palette_idx: usize,
    },
}

fn palette_item_name(
    item: &PaletteItem,
    name: impl Into<SharedString>,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
) -> AnyElement {
    // Selected rows use the banner-style amber background, so the row name
    // mirrors `theme.primary` for visual emphasis. Inactive rows keep the
    // regular foreground.
    let color = if is_selected {
        theme.primary
    } else {
        theme.foreground
    };

    match item {
        PaletteItem::Resource(_) | PaletteItem::Script { .. } => {
            MonoLabel::new(name).color(color).into_any_element()
        }
        _ => Body::new(name).color(color).into_any_element(),
    }
}

fn palette_category_text(
    label: impl Into<SharedString>,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
) -> MonoCaption {
    MonoCaption::new(label).color(if is_selected {
        theme.primary.opacity(0.75)
    } else {
        theme.muted_foreground
    })
}

fn palette_qualifier_text(
    label: impl Into<SharedString>,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
) -> MonoCaption {
    MonoCaption::new(label).color(if is_selected {
        theme.primary.opacity(0.65)
    } else {
        theme.muted_foreground
    })
}

#[cfg(test)]
fn palette_shortcut_text(
    shortcut: impl Into<SharedString>,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
) -> MonoCaption {
    MonoCaption::new(shortcut).color(text_color_for_selected(is_selected, theme))
}

/// Split a shortcut string like "ctrl-shift-k" into Chord parts.
///
/// Recognizes the canonical modifier tokens used in `KeyBinding` strings
/// (`ctrl`, `shift`, `alt`, `cmd`) and capitalizes them for display. The
/// final segment is treated as the key name and uppercased.
fn palette_shortcut_parts(shortcut: &str) -> Vec<SharedString> {
    let tokens: Vec<&str> = shortcut.split('-').collect();

    if tokens.is_empty() {
        return Vec::new();
    }

    let mut parts: Vec<SharedString> = Vec::with_capacity(tokens.len());
    let last_idx = tokens.len() - 1;

    for (idx, token) in tokens.iter().enumerate() {
        let display = if idx == last_idx {
            token.to_uppercase()
        } else {
            match token.to_lowercase().as_str() {
                "ctrl" => "Ctrl".to_string(),
                "shift" => "Shift".to_string(),
                "alt" => "Alt".to_string(),
                "cmd" | "command" | "super" | "platform" => "Cmd".to_string(),
                other => {
                    let mut chars = other.chars();
                    match chars.next() {
                        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
                        None => String::new(),
                    }
                }
            }
        };
        parts.push(SharedString::from(display));
    }

    parts
}

pub struct CommandPalette {
    visible: bool,
    items: Vec<PaletteItem>,
    filtered: Vec<FilteredItem>,
    selected_index: usize,
    scroll_offset: usize,
    input_state: Entity<InputState>,
    matcher: SkimMatcherV2,
}

#[cfg(test)]
mod tests {
    use super::{PaletteCommand, palette_qualifier_text, palette_shortcut_text};
    use crate::ui::theme;
    use dbflux_components::tokens::FontSizes;
    use dbflux_components::typography::AppFonts;
    use gpui::TestAppContext;
    use gpui_component::theme::Theme;
    use std::fs;

    fn command_palette_source() -> String {
        let source = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/ui/overlays/command_palette.rs"
        ))
        .unwrap_or_else(|error| panic!("failed to read command_palette.rs: {error}"));

        source
            .rsplit("impl Render for CommandPalette")
            .next()
            .map(|render_impl| format!("impl Render for CommandPalette{render_impl}"))
            .expect("command_palette.rs should contain a render implementation")
    }

    fn command_palette_overlay_source() -> String {
        let source = command_palette_source();
        let start = source
            .find(".id(\"command-palette-overlay\")")
            .expect("command_palette render should define the overlay container");

        let remaining = &source[start..];
        let end = remaining
            .find(".child(\n                        div()\n                            .id(\"command-palette-list\")")
            .unwrap_or(remaining.len());

        remaining[..end].to_string()
    }

    #[gpui::test]
    fn action_shortcuts_use_mono_caption_instead_of_bold_key_hint(cx: &mut TestAppContext) {
        cx.update(theme::init);

        let theme = cx.update(|cx| Theme::global(cx).clone());

        let shortcut = palette_shortcut_text("ctrl-k", false, &theme).inspect();
        let selected_shortcut = palette_shortcut_text("enter", true, &theme).inspect();

        for inspection in [shortcut, selected_shortcut] {
            assert_eq!(inspection.family, Some(AppFonts::MONO));
            assert_eq!(inspection.fallbacks, &[AppFonts::MONO_FALLBACK]);
            assert_eq!(inspection.size_override, Some(FontSizes::XS));
            assert_eq!(inspection.weight_override, None);
            assert!(inspection.has_custom_color_override);
            assert!(!inspection.uses_muted_foreground_override);
        }
    }

    #[test]
    fn palette_commands_still_preserve_explicit_shortcuts() {
        let command = PaletteCommand::new("id", "Open", "Action").with_shortcut("ctrl-k");

        assert_eq!(command.shortcut, Some("ctrl-k"));
    }

    #[gpui::test]
    fn qualifiers_use_mono_caption_role(cx: &mut TestAppContext) {
        cx.update(theme::init);

        let theme = cx.update(|cx| Theme::global(cx).clone());

        let qualifier = palette_qualifier_text("prod / analytics", false, &theme).inspect();
        let selected = palette_qualifier_text("scripts/admin", true, &theme).inspect();

        for inspection in [qualifier, selected] {
            assert_eq!(inspection.family, Some(AppFonts::MONO));
            assert_eq!(inspection.fallbacks, &[AppFonts::MONO_FALLBACK]);
            assert_eq!(inspection.size_override, Some(FontSizes::XS));
            assert_eq!(inspection.weight_override, None);
            assert!(inspection.has_custom_color_override);
        }
    }

    #[test]
    fn command_palette_overlay_uses_canonical_scrim_and_modal_container_contracts() {
        let source = command_palette_source();

        assert!(source.contains(".bg(overlay_bg(theme))"));
        assert!(source.contains("surface_modal_container(cx)"));
        assert!(!source.contains(".bg(gpui::black().opacity(0.5))"));
        assert!(!source.contains("surface_panel(cx)"));
    }

    #[test]
    fn command_palette_render_keeps_overlay_identity_and_close_behavior() {
        let source = command_palette_source();

        assert!(source.contains(".id(\"command-palette-overlay\")"));
        assert!(source.contains(".key_context(ContextId::CommandPalette.as_gpui_context())"));
        assert!(source.contains("this.hide(cx);"));
    }

    #[test]
    fn command_palette_overlay_chain_starts_from_the_shared_modal_container() {
        let source = command_palette_overlay_source();

        assert!(source.contains("surface_modal_container(cx)"));
        assert!(source.contains(".id(\"command-palette-container\")"));
        assert!(!source.contains("surface_panel(cx)"));
    }

    #[test]
    fn command_palette_overlay_chain_keeps_the_shared_scrim_close_path() {
        let source = command_palette_overlay_source();

        assert!(source.contains(".bg(overlay_bg(theme))"));
        assert!(source.contains("this.hide(cx);"));
        assert!(!source.contains(".bg(gpui::black().opacity(0.5))"));
    }
}

/// Event emitted when the user selects a palette item.
pub enum PaletteSelection {
    Command {
        id: &'static str,
    },
    Connect {
        profile_id: Uuid,
    },
    OpenTable {
        profile_id: Uuid,
        table: TableRef,
        database: Option<String>,
    },
    OpenCollection {
        profile_id: Uuid,
        collection: CollectionRef,
    },
    OpenKeyValue {
        profile_id: Uuid,
        database: String,
    },
    FocusConnection {
        profile_id: Uuid,
    },
    OpenScript {
        path: PathBuf,
    },
}

pub struct CommandPaletteClosed;

impl CommandPalette {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Search commands, connections, tables, scripts...")
        });

        cx.subscribe_in(
            &input_state,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::Change => {
                    let query = this.input_state.read(cx).value().to_string();
                    this.update_filter(&query, cx);
                }
                InputEvent::PressEnter { .. } => {
                    this.execute_selected(window, cx);
                }
                _ => {}
            },
        )
        .detach();

        Self {
            visible: false,
            items: Vec::new(),
            filtered: Vec::new(),
            selected_index: 0,
            scroll_offset: 0,
            input_state,
            matcher: SkimMatcherV2::default(),
        }
    }

    /// Set items and reset filter state. Called by Workspace on each toggle.
    pub fn open_with_items(
        &mut self,
        items: Vec<PaletteItem>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.items = items;
        self.filtered = self
            .items
            .iter()
            .enumerate()
            .map(|(index, _)| FilteredItem { index, score: 0 })
            .collect();

        self.visible = true;
        self.selected_index = 0;
        self.scroll_offset = 0;

        self.input_state.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });

        cx.notify();
    }

    pub fn register_commands(&mut self, _commands: Vec<PaletteCommand>) {
        // No-op; items are now set via open_with_items.
        // Kept to avoid breaking the call site during migration.
    }

    pub fn toggle(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = !self.visible;

        if self.visible {
            self.input_state.update(cx, |state, cx| {
                state.set_value("", window, cx);
                state.focus(window, cx);
            });
            self.selected_index = 0;
            self.scroll_offset = 0;
            self.filtered = self
                .items
                .iter()
                .enumerate()
                .map(|(index, _)| FilteredItem { index, score: 0 })
                .collect();
        }

        cx.notify();
    }

    pub fn hide(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.emit(CommandPaletteClosed);
        cx.notify();
    }

    #[allow(dead_code)]
    pub fn is_visible(&self) -> bool {
        self.visible
    }

    fn update_filter(&mut self, query: &str, cx: &mut Context<Self>) {
        if query.is_empty() {
            self.filtered = self
                .items
                .iter()
                .enumerate()
                .map(|(index, _)| FilteredItem { index, score: 0 })
                .collect();
        } else {
            let mut scored: Vec<FilteredItem> = self
                .items
                .iter()
                .enumerate()
                .filter_map(|(index, item)| {
                    let search_text = item.search_text();
                    self.matcher
                        .fuzzy_match(&search_text, query)
                        .map(|score| FilteredItem { index, score })
                })
                .collect();

            scored.sort_by(|a, b| {
                b.score.cmp(&a.score).then_with(|| {
                    self.items[a.index]
                        .type_priority()
                        .cmp(&self.items[b.index].type_priority())
                })
            });

            self.filtered = scored;
        }

        self.selected_index = 0;
        self.scroll_offset = 0;
        cx.notify();
    }

    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        if !self.filtered.is_empty() {
            self.selected_index = (self.selected_index + 1) % self.filtered.len();
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        if !self.filtered.is_empty() {
            self.selected_index = if self.selected_index == 0 {
                self.filtered.len() - 1
            } else {
                self.selected_index - 1
            };
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    fn ensure_selected_visible(&mut self) {
        if self.selected_index < self.scroll_offset {
            self.scroll_offset = self.selected_index;
        }
        if self.selected_index >= self.scroll_offset + VISIBLE_ITEMS {
            self.scroll_offset = self.selected_index - VISIBLE_ITEMS + 1;
        }
    }

    fn scroll_down(&mut self, cx: &mut Context<Self>) {
        if self.selected_index < self.filtered.len().saturating_sub(1) {
            self.selected_index += 1;
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    fn scroll_up(&mut self, cx: &mut Context<Self>) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    fn execute_selected(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(filtered) = self.filtered.get(self.selected_index)
            && let Some(item) = self.items.get(filtered.index)
        {
            let selection = match item {
                PaletteItem::Action { id, .. } => PaletteSelection::Command { id },
                PaletteItem::Connection {
                    profile_id,
                    is_connected,
                    ..
                } => {
                    if *is_connected {
                        PaletteSelection::FocusConnection {
                            profile_id: *profile_id,
                        }
                    } else {
                        PaletteSelection::Connect {
                            profile_id: *profile_id,
                        }
                    }
                }
                PaletteItem::Resource(r) => match r {
                    ResourceItem::Table {
                        profile_id,
                        schema,
                        name,
                        database,
                        ..
                    }
                    | ResourceItem::View {
                        profile_id,
                        schema,
                        name,
                        database,
                        ..
                    } => PaletteSelection::OpenTable {
                        profile_id: *profile_id,
                        table: TableRef {
                            schema: schema.clone(),
                            name: name.clone(),
                        },
                        database: database.clone(),
                    },
                    ResourceItem::Collection {
                        profile_id,
                        database,
                        name,
                        ..
                    } => PaletteSelection::OpenCollection {
                        profile_id: *profile_id,
                        collection: CollectionRef {
                            database: database.clone(),
                            name: name.clone(),
                        },
                    },
                    ResourceItem::KeyValueDb {
                        profile_id,
                        database,
                        ..
                    } => PaletteSelection::OpenKeyValue {
                        profile_id: *profile_id,
                        database: database.clone(),
                    },
                },
                PaletteItem::Script { path, .. } => {
                    PaletteSelection::OpenScript { path: path.clone() }
                }
            };

            self.visible = false;
            cx.emit(selection);
            cx.notify();
        }
    }

    fn render_palette_item(
        idx: usize,
        item: &PaletteItem,
        is_selected: bool,
        theme: &gpui_component::theme::Theme,
    ) -> Stateful<Div> {
        let (category, name) = item.display_label();

        // Right column: action shortcuts use a Chord; resources/scripts/
        // connections use their qualifier text. Selected rows additionally
        // surface an `Enter` glyph to reinforce the run affordance.
        let right_el: Option<AnyElement> = match item {
            PaletteItem::Action { shortcut, .. } => shortcut.map(|s| {
                let parts = palette_shortcut_parts(s);
                Chord::new(parts).into_any_element()
            }),
            PaletteItem::Connection { .. }
            | PaletteItem::Resource(_)
            | PaletteItem::Script { .. } => item
                .qualifier()
                .map(|q| palette_qualifier_text(q, is_selected, theme).into_any_element()),
        };

        let right_column = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .when_some(right_el, |d, el| d.child(el))
            .when(is_selected, |d| {
                d.child(Chord::new(vec![SharedString::from("\u{21B5}")]))
            });

        div()
            .id(("cmd", idx))
            .w_full()
            .px(Spacing::MD)
            .py(Spacing::SM)
            .flex()
            .items_center()
            .justify_between()
            .rounded(Radii::SM)
            .cursor_pointer()
            .border_l_2()
            .when(is_selected, |d| {
                d.bg(BannerColors::warning_bg(theme))
                    .border_color(theme.primary)
            })
            .when(!is_selected, |d| {
                d.border_color(gpui::transparent_black())
                    .hover(|d| d.bg(theme.secondary))
            })
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(palette_category_text(category, is_selected, theme))
                    .child(palette_item_name(item, name, is_selected, theme)),
            )
            .child(right_column)
    }
}

impl EventEmitter<PaletteSelection> for CommandPalette {}
impl EventEmitter<CommandPaletteClosed> for CommandPalette {}

impl Render for CommandPalette {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let input_state = self.input_state.clone();

        // Build the windowed list (scroll_offset..+VISIBLE_ITEMS) then group
        // the resulting items by section. Section headers are interleaved
        // before the first item of each section but are NOT counted in the
        // `display_idx` that compares against `selected_index`.
        let windowed: Vec<(usize, PaletteItem)> = self
            .filtered
            .iter()
            .enumerate()
            .skip(self.scroll_offset)
            .take(VISIBLE_ITEMS)
            .map(|(idx, filtered)| (idx, self.items[filtered.index].clone()))
            .collect();

        let section_order = [
            PaletteSection::Connections,
            PaletteSection::Commands,
            PaletteSection::Tables,
            PaletteSection::Scripts,
        ];

        let mut rows: Vec<PaletteRow> = Vec::with_capacity(windowed.len() + section_order.len());
        for section in section_order {
            let mut header_pushed = false;
            for (display_idx, item) in windowed.iter() {
                if PaletteSection::for_item(item) != section {
                    continue;
                }
                if !header_pushed {
                    rows.push(PaletteRow::SectionHeader(SharedString::from(
                        section.label().to_uppercase(),
                    )));
                    header_pushed = true;
                }
                // `palette_idx` is the original index into `self.items` for
                // click handlers that re-trigger `execute_selected`.
                let palette_idx = self.filtered[*display_idx].index;
                rows.push(PaletteRow::Item {
                    display_idx: *display_idx,
                    palette_idx,
                });
            }
        }

        let total_count = self.items.len();
        let filtered_count = self.filtered.len();

        div()
            .id("command-palette-overlay")
            .key_context(ContextId::CommandPalette.as_gpui_context())
            .absolute()
            .inset_0()
            .flex()
            .justify_center()
            .pt(px(80.0))
            .bg(overlay_bg(theme))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.hide(cx);
                }),
            )
            .on_action(cx.listener(|this, _: &SelectPrev, _window, cx| {
                this.select_prev(cx);
            }))
            .on_action(cx.listener(|this, _: &SelectNext, _window, cx| {
                this.select_next(cx);
            }))
            .on_action(cx.listener(|this, _: &Close, _window, cx| {
                this.hide(cx);
            }))
            .on_action(cx.listener(|this, _: &Execute, window, cx| {
                this.execute_selected(window, cx);
            }))
            .child(
                surface_modal_container(cx)
                    .id("command-palette-container")
                    // Force the deepest Ayu Dark background; the default
                    // ModalContainer surface is a raised popover tone which
                    // read as too warm / Mirage-like inside this palette.
                    .bg(theme.background)
                    .w_full()
                    .max_w(px(560.0))
                    .max_h(px(440.0))
                    .shadow_lg()
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(
                        div()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_b_1()
                            .border_color(theme.border)
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(Icon::new(AppIcon::Search).small().muted())
                            .child(
                                div()
                                    .flex_1()
                                    .child(Input::new(&input_state).small().cleanable(true)),
                            )
                            .child(MonoCaption::new(format!(
                                "{} / {}",
                                filtered_count, total_count
                            )))
                            .child(Chord::new(vec![SharedString::from("Esc")])),
                    )
                    .child(
                        div()
                            .id("command-palette-list")
                            .flex_1()
                            .overflow_y_hidden()
                            .p(Spacing::XS)
                            .on_scroll_wheel(cx.listener(
                                |this, event: &ScrollWheelEvent, _window, cx| {
                                    let delta = event.delta.pixel_delta(px(1.0));
                                    if delta.y < px(0.0) {
                                        this.scroll_down(cx);
                                    } else if delta.y > px(0.0) {
                                        this.scroll_up(cx);
                                    }
                                },
                            ))
                            .children(rows.into_iter().map(|row| {
                                match row {
                                    PaletteRow::SectionHeader(label) => div()
                                        .px(Spacing::MD)
                                        .py(Spacing::XS)
                                        .child(
                                            MonoCaption::new(label)
                                                .color(theme.muted_foreground)
                                                .into_any_element(),
                                        )
                                        .into_any_element(),
                                    PaletteRow::Item {
                                        display_idx,
                                        palette_idx,
                                    } => {
                                        let is_selected = display_idx == self.selected_index;
                                        let item = self.items[palette_idx].clone();
                                        Self::render_palette_item(
                                            display_idx,
                                            &item,
                                            is_selected,
                                            theme,
                                        )
                                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                            cx.stop_propagation();
                                        })
                                        .on_click(cx.listener(move |this, _, window, cx| {
                                            this.selected_index = display_idx;
                                            this.execute_selected(window, cx);
                                        }))
                                        .into_any_element()
                                    }
                                }
                            }))
                            .when(self.filtered.is_empty(), |d| {
                                d.child(
                                    div()
                                        .w_full()
                                        .py(Spacing::LG)
                                        .flex()
                                        .justify_center()
                                        .child(Body::new("No matching items").muted(cx)),
                                )
                            }),
                    )
                    .child(
                        div()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_t_1()
                            .border_color(theme.border)
                            .flex()
                            .items_center()
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::MD)
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap(Spacing::XS)
                                            .child(Chord::new(vec![
                                                SharedString::from("\u{2191}"),
                                                SharedString::from("\u{2193}"),
                                            ]))
                                            .child(
                                                MonoCaption::new("navigate")
                                                    .color(theme.muted_foreground),
                                            ),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap(Spacing::XS)
                                            .child(Chord::new(vec![SharedString::from("\u{21B5}")]))
                                            .child(
                                                MonoCaption::new("run")
                                                    .color(theme.muted_foreground),
                                            ),
                                    )
                                    // Aspirational: "open in new tab" has no
                                    // wired Tab+Enter handler in the palette
                                    // yet. Rendered at half opacity to flag
                                    // it as a forthcoming affordance rather
                                    // than a live shortcut.
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap(Spacing::XS)
                                            .opacity(0.5)
                                            .child(Chord::new(vec![
                                                SharedString::from("\u{21E5}"),
                                                SharedString::from("\u{21B5}"),
                                            ]))
                                            .child(
                                                MonoCaption::new("open in new tab")
                                                    .color(theme.muted_foreground),
                                            ),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }
}
