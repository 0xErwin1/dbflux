use crate::keymap::{Command, ContextId};
use crate::ui::icons::AppIcon;
use dbflux_app::keymap::default_slots;
use dbflux_components::controls::{GpuiInput as Input, InputEvent, InputState};
use dbflux_components::icons::DriverIconTone;
use dbflux_components::primitives::{
    Chamfer, Icon, Kbd, SurfaceRole, Text, inspect_surface_role, overlay_bg,
};
use dbflux_components::tokens::{ChamferCut, ChromeColors, PaletteMetrics};
use dbflux_core::{CollectionRef, TableRef};
use dbflux_ui_base::keymap::{
    RunCommand, chord_display_parts, default_keymap, effective_keymap, run_command,
};
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::ops::Range;
use std::path::PathBuf;
use uuid::Uuid;

/// A searchable item in the command palette.
#[derive(Clone)]
pub enum PaletteItem {
    Action {
        id: &'static str,
        name: SharedString,
        category: SharedString,
        shortcut: Option<&'static str>,
    },
    Connection {
        profile_id: Uuid,
        name: String,
        is_connected: bool,
        /// The driver's logo and tone, when the driver is registered.
        icon: Option<(AppIcon, DriverIconTone)>,
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
    /// A saved chart record surfaced by the "Open chart..." command.
    SavedChart {
        id: Uuid,
        name: String,
        profile_name: String,
        profile_id: Uuid,
        /// `true` when the chart's source is `Collection` (browse mode).
        /// The palette appends a `[browse]` suffix to help users distinguish
        /// collection charts from query charts.
        is_collection_source: bool,
    },
    /// "Import Dashboard from JSON" action (shown only when the active connection
    /// has the `DASHBOARD_IMPORT` capability).
    ImportDashboard,
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
    ///
    /// Each kind arm embeds the English proxy word (so the filter always
    /// matches on the English name, regardless of the active locale)
    /// followed by the process-locale translated kind word (so it also
    /// matches on the localized name).
    pub fn search_text(&self) -> String {
        match self {
            Self::Action {
                category, name, id, ..
            } => {
                format!("{} {} {}", category, name, id.replace('_', " "))
            }
            Self::Connection { name, .. } => {
                format!(
                    "Connection {} {}",
                    dbflux_i18n::t!("palette.kind.connection"),
                    name
                )
            }
            Self::SavedChart {
                name, profile_name, ..
            } => format!(
                "Chart {} {} {}",
                dbflux_i18n::t!("palette.kind.chart"),
                name,
                profile_name
            ),
            Self::Resource(r) => match r {
                ResourceItem::Table {
                    profile_name,
                    database,
                    schema,
                    name,
                    ..
                } => {
                    let mut parts = format!(
                        "Table {} {} {}",
                        dbflux_i18n::t!("palette.kind.table"),
                        profile_name,
                        name
                    );
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
                } => format!(
                    "Collection {} {} {} {}",
                    dbflux_i18n::t!("palette.kind.collection"),
                    profile_name,
                    name,
                    database
                ),
                ResourceItem::View {
                    profile_name,
                    database,
                    schema,
                    name,
                    ..
                } => {
                    let mut parts = format!(
                        "View {} {} {}",
                        dbflux_i18n::t!("palette.kind.view"),
                        profile_name,
                        name
                    );
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
                } => format!(
                    "Keyspace {} {} {}",
                    dbflux_i18n::t!("palette.kind.keyspace"),
                    profile_name,
                    database
                ),
            },
            Self::Script {
                name,
                relative_path,
                ..
            } => {
                format!(
                    "Script {} {} {}",
                    dbflux_i18n::t!("palette.kind.script"),
                    name,
                    relative_path
                )
            }
            Self::ImportDashboard => {
                format!(
                    "Charts {}",
                    dbflux_i18n::t!("palette.import_dashboard.name")
                )
            }
        }
    }

    /// Returns `(category_label, display_name)`.
    pub fn display_label(&self) -> (String, String) {
        match self {
            Self::Action { category, name, .. } => (category.to_string(), name.to_string()),
            Self::Connection { name, .. } => {
                (dbflux_i18n::t!("palette.kind.connection"), name.clone())
            }
            Self::SavedChart {
                name,
                is_collection_source,
                ..
            } => {
                let display = if *is_collection_source {
                    format!(
                        "{} {}",
                        name,
                        dbflux_i18n::t!("palette.chart.browse_suffix")
                    )
                } else {
                    name.clone()
                };
                (dbflux_i18n::t!("palette.kind.chart"), display)
            }
            Self::Resource(r) => match r {
                ResourceItem::Table { name, .. } => {
                    (dbflux_i18n::t!("palette.kind.table"), name.clone())
                }
                ResourceItem::Collection { name, .. } => {
                    (dbflux_i18n::t!("palette.kind.collection"), name.clone())
                }
                ResourceItem::View { name, .. } => {
                    (dbflux_i18n::t!("palette.kind.view"), name.clone())
                }
                ResourceItem::KeyValueDb { database, .. } => {
                    (dbflux_i18n::t!("palette.kind.keyspace"), database.clone())
                }
            },
            Self::Script { name, .. } => (dbflux_i18n::t!("palette.kind.script"), name.clone()),
            Self::ImportDashboard => (
                dbflux_i18n::t!("palette.section.charts"),
                dbflux_i18n::t!("palette.import_dashboard.name"),
            ),
        }
    }

    /// Type priority for tiebreaking (lower = higher priority).
    pub fn type_priority(&self) -> u8 {
        match self {
            Self::Action { .. } => 0,
            Self::Connection { .. } => 1,
            Self::SavedChart { .. } => 2,
            Self::ImportDashboard => 2,
            Self::Resource(_) => 3,
            Self::Script { .. } => 4,
        }
    }

    /// Right-aligned mono qualifier: where a resource lives, a script's
    /// folder, a chart's connection, or "connected" for an open connection.
    /// Commands show their shortcut keycaps there instead.
    pub fn qualifier(&self) -> Option<String> {
        match self {
            Self::Action { .. } => None,
            Self::Connection { is_connected, .. } => {
                is_connected.then(|| dbflux_i18n::t!("palette.connection.connected"))
            }
            Self::SavedChart { profile_name, .. } => Some(profile_name.clone()),
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

impl PaletteItem {
    /// Leading icon and, for a connection, the driver's tone. Rows without a
    /// tone draw the icon muted, or tinted while selected.
    fn icon(&self) -> (AppIcon, Option<DriverIconTone>) {
        match self {
            Self::Action { id, .. } => (command_icon(id), None),
            Self::Connection { icon, .. } => match icon {
                Some((icon, tone)) => (*icon, Some(*tone)),
                None => (AppIcon::Database, None),
            },
            Self::Resource(ResourceItem::Table { .. }) => (AppIcon::Table, None),
            Self::Resource(ResourceItem::View { .. }) => (AppIcon::Eye, None),
            Self::Resource(ResourceItem::Collection { .. }) => (AppIcon::Box, None),
            Self::Resource(ResourceItem::KeyValueDb { .. }) => (AppIcon::KeyRound, None),
            Self::Script { .. } => (AppIcon::FileCode, None),
            Self::SavedChart { .. } => (AppIcon::ChartSpline, None),
            Self::ImportDashboard => (AppIcon::Download, None),
        }
    }

    /// Muted qualifier drawn right after a command's name: its category.
    fn inline_qualifier(&self) -> Option<SharedString> {
        match self {
            Self::Action { category, .. } => Some(category.clone()),
            _ => None,
        }
    }
}

/// Icon of a palette command, by command id.
fn command_icon(id: &str) -> AppIcon {
    match id {
        "new_query_tab" => AppIcon::Plus,
        "run_query" | "run_query_in_new_tab" => AppIcon::Play,
        "save_query" | "save_file_as" => AppIcon::Save,
        "open_script_file" | "add_external_scripts_folder" => AppIcon::Folder,
        "toggle_comment" | "focus_editor" => AppIcon::Code,
        "open_history" => AppIcon::History,
        "cancel_query" | "close_tab" => AppIcon::CircleX,
        "next_tab" => AppIcon::ChevronRight,
        "prev_tab" => AppIcon::ChevronLeft,
        "export_results" => AppIcon::Download,
        "open_connection_manager" => AppIcon::Cable,
        "disconnect" => AppIcon::Unplug,
        "refresh_schema" => AppIcon::RefreshCcw,
        "focus_sidebar" | "toggle_sidebar" => AppIcon::Database,
        "focus_results" | "toggle_results" => AppIcon::Rows3,
        "focus_tasks" | "toggle_tasks" => AppIcon::Loader,
        "toggle_editor" => AppIcon::SquareTerminal,
        "open_settings" => AppIcon::Settings,
        "open_login_modal" | "open_sso_wizard" => AppIcon::KeyRound,
        "open_mcp_approvals" | "refresh_mcp_governance" => AppIcon::Bot,
        "open_audit_viewer" => AppIcon::FingerprintPattern,
        "open_saved_chart" => AppIcon::ChartSpline,
        "new_dashboard" => AppIcon::ChartColumnBig,
        "analyze_dump_file" => AppIcon::FileSpreadsheet,
        _ => AppIcon::Zap,
    }
}

/// Legacy static command descriptor kept for `default_commands()` backwards compat.
#[derive(Clone)]
pub struct PaletteCommand {
    pub id: &'static str,
    pub name: SharedString,
    pub category: SharedString,
    pub shortcut: Option<&'static str>,
}

impl PaletteCommand {
    pub fn new(
        id: &'static str,
        name: impl Into<SharedString>,
        category: impl Into<SharedString>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            category: category.into(),
            shortcut: None,
        }
    }

    pub fn with_shortcut(mut self, shortcut: &'static str) -> Self {
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

/// Most commands a mixed query (no `>` or `@` prefix) keeps, so the
/// connections and tables it also matches stay on screen (P1Palette).
const MIXED_QUERY_COMMAND_LIMIT: usize = 3;

/// Section grouping for the rendered palette list.
///
/// The order here is the visual order in the palette. Sections render only
/// when at least one matching item exists for that section. Section headers
/// themselves are not selectable — they are a render-only concern.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PaletteSection {
    Commands,
    Connections,
    Charts,
    Tables,
    Scripts,
}

impl PaletteSection {
    const ORDER: [PaletteSection; 5] = [
        Self::Commands,
        Self::Connections,
        Self::Charts,
        Self::Tables,
        Self::Scripts,
    ];

    fn label(self) -> String {
        match self {
            Self::Connections => dbflux_i18n::t!("palette.section.connections"),
            Self::Commands => dbflux_i18n::t!("palette.section.commands"),
            Self::Charts => dbflux_i18n::t!("palette.section.charts"),
            Self::Tables => dbflux_i18n::t!("palette.section.tables"),
            Self::Scripts => dbflux_i18n::t!("palette.section.scripts"),
        }
    }

    fn for_item(item: &PaletteItem) -> Self {
        match item {
            PaletteItem::Connection { .. } => Self::Connections,
            PaletteItem::Action { .. } => Self::Commands,
            PaletteItem::SavedChart { .. } | PaletteItem::ImportDashboard => Self::Charts,
            PaletteItem::Resource(_) => Self::Tables,
            PaletteItem::Script { .. } => Self::Scripts,
        }
    }

    /// Visual ordering key, the position in [`Self::ORDER`], so keyboard
    /// navigation walks the list in the same order the user sees it.
    fn sort_order(self) -> usize {
        Self::ORDER
            .iter()
            .position(|section| *section == self)
            .unwrap_or(Self::ORDER.len())
    }
}

/// What a query searches, chosen by its first character: `>` keeps only
/// commands, `@` only tables and collections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PaletteScope {
    All,
    Commands,
    Tables,
}

impl PaletteScope {
    /// Splits a typed query into its scope and the text to match.
    fn parse(query: &str) -> (Self, &str) {
        if let Some(rest) = query.strip_prefix('>') {
            (Self::Commands, rest.trim_start())
        } else if let Some(rest) = query.strip_prefix('@') {
            (Self::Tables, rest.trim_start())
        } else {
            (Self::All, query)
        }
    }

    fn includes(self, item: &PaletteItem) -> bool {
        match self {
            Self::All => true,
            Self::Commands => matches!(item, PaletteItem::Action { .. }),
            Self::Tables => matches!(item, PaletteItem::Resource(_)),
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

/// Byte ranges of `name` that fuzzy-match `query`, merged into runs, for the
/// tinted match highlight. Empty when the query is empty or only matched
/// other fields of the item.
fn match_ranges(matcher: &SkimMatcherV2, name: &str, query: &str) -> Vec<Range<usize>> {
    if query.is_empty() {
        return Vec::new();
    }

    let Some((_, char_indices)) = matcher.fuzzy_indices(name, query) else {
        return Vec::new();
    };

    let mut ranges: Vec<Range<usize>> = Vec::new();

    for (char_index, (byte_index, character)) in name.char_indices().enumerate() {
        if !char_indices.contains(&char_index) {
            continue;
        }

        let byte_end = byte_index + character.len_utf8();

        match ranges.last_mut() {
            Some(last) if last.end == byte_index => last.end = byte_end,
            _ => ranges.push(byte_index..byte_end),
        }
    }

    ranges
}

/// Contexts searched, in order, for the chord a palette command shows.
const PALETTE_SHORTCUT_CONTEXTS: [ContextId; 5] = [
    ContextId::Global,
    ContextId::Editor,
    ContextId::Results,
    ContextId::Sidebar,
    ContextId::BackgroundTasks,
];

/// Keycap of the keys that run `command` inside the palette.
fn palette_shortcut(command: Command) -> Option<SharedString> {
    dbflux_ui_base::keymap::shortcut_label(ContextId::CommandPalette, command)
}

/// Keycaps of a palette command.
///
/// A command the default keymap binds shows the keys the effective keymap
/// gives it, so a rebinding shows at once and a removed shortcut shows
/// none: one keycap per key of a single chord, one keycap per chord of a
/// key sequence. Any other command shows its explicit `shortcut`, if it has
/// one.
fn palette_command_keycaps(id: &str, shortcut: Option<&str>) -> Vec<SharedString> {
    let command = Command::from_palette_id(id).or_else(|| {
        Command::all_variants()
            .into_iter()
            .find(|command| command.id() == id)
    });

    let keymap_binds_command = command.is_some_and(|command| {
        default_slots(default_keymap())
            .iter()
            .any(|slot| slot.command == command)
    });

    match command {
        Some(command) if keymap_binds_command => {
            let keymap = effective_keymap();

            PALETTE_SHORTCUT_CONTEXTS
                .iter()
                .find_map(|context| keymap.keys_for_command(*context, command))
                .map(|keys| {
                    if keys.is_single() {
                        chord_display_parts(keys.first())
                    } else {
                        keys.chords()
                            .iter()
                            .map(|chord| chord_display_parts(chord).join(" ").into())
                            .collect()
                    }
                })
                .unwrap_or_default()
        }
        _ => shortcut.map(palette_shortcut_parts).unwrap_or_default(),
    }
}

/// Split a shortcut string like "ctrl-shift-k" into one label per keycap.
///
/// Recognizes the canonical modifier tokens used in `KeyBinding` strings
/// (`ctrl`, `shift`, `alt`, `cmd`) and capitalizes them for display. The
/// final segment is the key name: uppercased after a modifier (`Ctrl E`),
/// kept as typed on its own (`x`).
fn palette_shortcut_parts(shortcut: &str) -> Vec<SharedString> {
    let tokens: Vec<&str> = shortcut.split('-').collect();

    if tokens.is_empty() {
        return Vec::new();
    }

    let mut parts: Vec<SharedString> = Vec::with_capacity(tokens.len());
    let last_idx = tokens.len() - 1;

    for (idx, token) in tokens.iter().enumerate() {
        let display = if idx == last_idx && last_idx == 0 {
            token.to_string()
        } else if idx == last_idx {
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

/// The items a typed query keeps, with their fuzzy-match scores. A leading
/// `>` or `@` narrows the search to commands or to tables and collections.
fn filter_items(items: &[PaletteItem], matcher: &SkimMatcherV2, query: &str) -> Vec<FilteredItem> {
    let (scope, text) = PaletteScope::parse(query);

    items
        .iter()
        .enumerate()
        .filter(|(_, item)| scope.includes(item))
        .filter_map(|(index, item)| {
            if text.is_empty() {
                return Some(FilteredItem { index, score: 0 });
            }

            matcher
                .fuzzy_match(&item.search_text(), text)
                .map(|score| FilteredItem { index, score })
        })
        .collect()
}

/// Orders `filtered` the way the list shows it: by section, then by score,
/// then by kind. A mixed query keeps only its best
/// [`MIXED_QUERY_COMMAND_LIMIT`] commands so every kind it matches shows.
fn arrange_results(
    items: &[PaletteItem],
    mut filtered: Vec<FilteredItem>,
    query: &str,
) -> Vec<FilteredItem> {
    filtered.sort_by(|a, b| {
        let item_a = &items[a.index];
        let item_b = &items[b.index];
        let section_a = PaletteSection::for_item(item_a).sort_order();
        let section_b = PaletteSection::for_item(item_b).sort_order();

        section_a
            .cmp(&section_b)
            .then_with(|| b.score.cmp(&a.score))
            .then_with(|| item_a.type_priority().cmp(&item_b.type_priority()))
    });

    let (scope, text) = PaletteScope::parse(query);

    if scope == PaletteScope::All && !text.trim().is_empty() {
        let mut commands_kept = 0;

        filtered.retain(|filtered_item| {
            if !matches!(items[filtered_item.index], PaletteItem::Action { .. }) {
                return true;
            }

            commands_kept += 1;
            commands_kept <= MIXED_QUERY_COMMAND_LIMIT
        });
    }

    filtered
}

/// Where the chosen item opens: `Enter` reuses the tab already showing it,
/// the new-tab chord (`Ctrl ↵`) always opens another one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenPlacement {
    ReuseTab,
    NewTab,
}

pub struct CommandPalette {
    visible: bool,
    items: Vec<PaletteItem>,
    filtered: Vec<FilteredItem>,
    selected_index: usize,
    scroll_offset: usize,
    input_state: Entity<InputState>,
    matcher: SkimMatcherV2,
    /// The typed query without its scope prefix, for the match highlight.
    match_query: String,
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
        /// Open another tab even when one already shows this table.
        new_tab: bool,
    },
    OpenCollection {
        profile_id: Uuid,
        collection: CollectionRef,
        new_tab: bool,
    },
    OpenKeyValue {
        profile_id: Uuid,
        database: String,
        new_tab: bool,
    },
    FocusConnection {
        profile_id: Uuid,
    },
    OpenScript {
        path: PathBuf,
    },
    OpenSavedChart {
        chart_id: Uuid,
    },
    /// The user selected the "Import Dashboard from JSON" entry.
    ImportDashboard,
}

pub struct CommandPaletteClosed;

impl CommandPalette {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!("palette.search.placeholder"))
        });

        cx.subscribe_in(
            &input_state,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::Change => {
                    let query = this.input_state.read(cx).value().to_string();
                    this.update_filter(&query, cx);
                }
                // The search input owns Enter and the primary-modifier Enter
                // while it has focus, so the new-tab chord arrives here as a
                // secondary Enter rather than through the palette keymap.
                InputEvent::PressEnter { secondary, .. } => {
                    let placement = if *secondary {
                        OpenPlacement::NewTab
                    } else {
                        OpenPlacement::ReuseTab
                    };

                    this.execute_selected(placement, window, cx);
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
            match_query: String::new(),
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
        self.match_query.clear();
        self.filtered = arrange_results(&self.items, std::mem::take(&mut self.filtered), "");

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
            self.match_query.clear();
            self.filtered = self
                .items
                .iter()
                .enumerate()
                .map(|(index, _)| FilteredItem { index, score: 0 })
                .collect();
            self.filtered = arrange_results(&self.items, std::mem::take(&mut self.filtered), "");
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
        let filtered = filter_items(&self.items, &self.matcher, query);
        self.filtered = arrange_results(&self.items, filtered, query);
        self.match_query = PaletteScope::parse(query).1.to_string();

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

    fn execute_selected(
        &mut self,
        placement: OpenPlacement,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let new_tab = placement == OpenPlacement::NewTab;

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
                        new_tab,
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
                        new_tab,
                    },
                    ResourceItem::KeyValueDb {
                        profile_id,
                        database,
                        ..
                    } => PaletteSelection::OpenKeyValue {
                        profile_id: *profile_id,
                        database: database.clone(),
                        new_tab,
                    },
                },
                PaletteItem::Script { path, .. } => {
                    PaletteSelection::OpenScript { path: path.clone() }
                }
                PaletteItem::SavedChart { id, .. } => {
                    PaletteSelection::OpenSavedChart { chart_id: *id }
                }
                PaletteItem::ImportDashboard => PaletteSelection::ImportDashboard,
            };

            self.visible = false;
            cx.emit(selection);
            cx.notify();
        }
    }

    fn render_palette_item(
        &self,
        display_idx: usize,
        item: &PaletteItem,
        is_selected: bool,
        cx: &App,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let (_, name) = item.display_label();

        let (icon, tone) = item.icon();
        let icon_color = match tone {
            Some(tone) => tone.resolve(cx),
            None if is_selected => tint,
            None => theme.muted_foreground,
        };

        let name_color = if is_selected {
            ChromeColors::strong(theme)
        } else {
            theme.foreground
        };

        let highlights: Vec<(Range<usize>, HighlightStyle)> =
            match_ranges(&self.matcher, &name, &self.match_query)
                .into_iter()
                .map(|range| {
                    (
                        range,
                        HighlightStyle {
                            color: Some(tint),
                            font_weight: Some(FontWeight::BOLD),
                            ..Default::default()
                        },
                    )
                })
                .collect();

        let keycaps: Vec<AnyElement> = match item {
            PaletteItem::Action { id, shortcut, .. } => palette_command_keycaps(id, *shortcut)
                .into_iter()
                .map(|part| Kbd::new(part).into_any_element())
                .collect(),
            _ => Vec::new(),
        };

        let wash = tint.opacity(PaletteMetrics::SELECTED_ALPHA);

        div()
            .id(("cmd", display_idx))
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(PaletteMetrics::ROW_GAP)
            .h(PaletteMetrics::ROW_HEIGHT)
            .px(PaletteMetrics::PADDING_X)
            .cursor_pointer()
            .when(is_selected, |row| {
                row.bg(wash).child(
                    div()
                        .absolute()
                        .left_0()
                        .top_0()
                        .bottom_0()
                        .w(PaletteMetrics::SELECTION_BAR)
                        .bg(tint),
                )
            })
            .when(!is_selected, |row| {
                row.hover(|row| row.bg(theme.list_hover))
            })
            .child(
                Icon::new(icon)
                    .size(PaletteMetrics::ROW_ICON)
                    .color(icon_color),
            )
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .text_size(PaletteMetrics::ROW_FONT)
                    .text_color(name_color)
                    .child(StyledText::new(name).with_highlights(highlights)),
            )
            .when_some(item.inline_qualifier(), |row, qualifier| {
                row.child(
                    div()
                        .flex_shrink_0()
                        .text_size(PaletteMetrics::QUALIFIER_FONT)
                        .text_color(theme.muted_foreground)
                        .child(qualifier),
                )
            })
            .child(div().flex_1())
            .when_some(item.qualifier(), |row, qualifier| {
                row.child(
                    div()
                        .flex_shrink_0()
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .text_size(PaletteMetrics::QUALIFIER_FONT)
                        .text_color(theme.muted_foreground)
                        .child(qualifier),
                )
            })
            .children(keycaps)
    }

    fn render_search_row(&self, cx: &App) -> Div {
        let theme = cx.theme();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(PaletteMetrics::SEARCH_GAP)
            .h(PaletteMetrics::SEARCH_HEIGHT)
            .px(PaletteMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Search)
                    .size(PaletteMetrics::SEARCH_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                div().flex_1().min_w_0().child(
                    Input::new(&self.input_state)
                        .appearance(false)
                        .text_size(PaletteMetrics::QUERY_FONT)
                        .text_color(ChromeColors::strong(theme))
                        .px_0(),
                ),
            )
            .child(
                div()
                    .flex_shrink_0()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(PaletteMetrics::COUNT_FONT)
                    .text_color(theme.muted_foreground)
                    .child(format!("{} / {}", self.filtered.len(), self.items.len())),
            )
            .when_some(palette_shortcut(Command::Cancel), |header, label| {
                header.child(Kbd::new(label))
            })
    }

    fn render_footer(cx: &App) -> Div {
        let theme = cx.theme();
        let navigate_keys = palette_navigate_label(
            palette_shortcut(Command::SelectPrev),
            palette_shortcut(Command::SelectNext),
        );

        let hint = |label: String| div().flex_shrink_0().child(label);

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(PaletteMetrics::FOOTER_GAP)
            .h(PaletteMetrics::FOOTER_HEIGHT)
            .px(PaletteMetrics::PADDING_X)
            .border_t_1()
            .border_color(theme.border)
            .text_size(PaletteMetrics::FOOTER_FONT)
            .text_color(theme.muted_foreground)
            .when_some(navigate_keys, |footer, keys| {
                footer
                    .child(Kbd::new(keys))
                    .child(hint(dbflux_i18n::t!("palette.footer.navigate")))
            })
            .when_some(palette_shortcut(Command::Execute), |footer, label| {
                footer
                    .child(Kbd::new(footer_key_label(&label)))
                    .child(hint(dbflux_i18n::t!("palette.footer.run")))
            })
            .when_some(
                palette_shortcut(Command::RunQueryInNewTab),
                |footer, label| {
                    footer
                        .child(Kbd::new(label))
                        .child(hint(dbflux_i18n::t!("palette.footer.open_in_new_tab")))
                },
            )
            .child(div().flex_1())
            .child(Kbd::new(">"))
            .child(hint(dbflux_i18n::t!("palette.footer.commands_only")))
            .child(Kbd::new("@"))
            .child(hint(dbflux_i18n::t!("palette.footer.tables_only")))
    }
}

/// The footer's move keycap: the previous and next keys side by side in one
/// keycap (`↑↓`), or whichever of them is bound.
fn palette_navigate_label(
    previous: Option<SharedString>,
    next: Option<SharedString>,
) -> Option<SharedString> {
    match (previous, next) {
        (None, None) => None,
        (previous, next) => Some(
            previous
                .into_iter()
                .chain(next)
                .map(|label| label.to_string())
                .collect::<String>()
                .into(),
        ),
    }
}

/// A lone Enter reads `↵` in the footer, like the new-tab chord next to it
/// (`Ctrl ↵`).
fn footer_key_label(label: &str) -> SharedString {
    if label == "Enter" {
        "\u{21b5}".into()
    } else {
        label.to_string().into()
    }
}

impl EventEmitter<PaletteSelection> for CommandPalette {}
impl EventEmitter<CommandPaletteClosed> for CommandPalette {}

impl Render for CommandPalette {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();

        // Build the windowed list (scroll_offset..+VISIBLE_ITEMS) then group
        // the resulting items by section. Section headers are interleaved
        // before the first item of each section but are NOT counted in the
        // `display_idx` that compares against `selected_index`.
        let windowed: Vec<(usize, usize)> = self
            .filtered
            .iter()
            .enumerate()
            .skip(self.scroll_offset)
            .take(VISIBLE_ITEMS)
            .map(|(display_idx, filtered)| (display_idx, filtered.index))
            .collect();

        let mut rows: Vec<PaletteRow> =
            Vec::with_capacity(windowed.len() + PaletteSection::ORDER.len());

        for section in PaletteSection::ORDER {
            let mut header_pushed = false;

            for &(display_idx, palette_idx) in &windowed {
                if PaletteSection::for_item(&self.items[palette_idx]) != section {
                    continue;
                }

                if !header_pushed {
                    rows.push(PaletteRow::SectionHeader(section.label().into()));
                    header_pushed = true;
                }

                rows.push(PaletteRow::Item {
                    display_idx,
                    palette_idx,
                });
            }
        }

        let list_rows: Vec<AnyElement> = rows
            .into_iter()
            .map(|row| match row {
                PaletteRow::SectionHeader(label) => div()
                    .pt(PaletteMetrics::SECTION_PADDING_TOP)
                    .pb(PaletteMetrics::SECTION_PADDING_BOTTOM)
                    .px(PaletteMetrics::PADDING_X)
                    .child(Text::label(label).font_size(PaletteMetrics::SECTION_FONT))
                    .into_any_element(),
                PaletteRow::Item {
                    display_idx,
                    palette_idx,
                } => {
                    let is_selected = display_idx == self.selected_index;

                    self.render_palette_item(display_idx, &self.items[palette_idx], is_selected, cx)
                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                            cx.stop_propagation();
                        })
                        .on_click(cx.listener(move |this, _, window, cx| {
                            this.selected_index = display_idx;
                            this.execute_selected(OpenPlacement::ReuseTab, window, cx);
                        }))
                        .into_any_element()
                }
            })
            .collect();

        let surface = inspect_surface_role(SurfaceRole::Modal);
        let card_shape = Chamfer::new(ChamferCut::CARD)
            .fill(surface.fill.resolve(theme))
            .border(surface.border.resolve(theme));

        let max_height = window.viewport_size().height - PaletteMetrics::TOP_OFFSET * 2.0;

        div()
            .id("command-palette-overlay")
            .key_context(ContextId::CommandPalette.as_gpui_context())
            .absolute()
            .inset_0()
            .flex()
            .justify_center()
            .items_start()
            .pt(PaletteMetrics::TOP_OFFSET)
            .bg(overlay_bg(theme))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.hide(cx);
                }),
            )
            // The keymap's CommandPalette layer binds its keys to these
            // commands; anything else goes on to the workspace.
            .on_action(cx.listener(|this, action: &RunCommand, window, cx| {
                match run_command(action) {
                    Some(Command::SelectPrev) => this.select_prev(cx),
                    Some(Command::SelectNext) => this.select_next(cx),
                    Some(Command::Cancel) => this.hide(cx),
                    Some(Command::Execute) => {
                        this.execute_selected(OpenPlacement::ReuseTab, window, cx)
                    }
                    Some(Command::RunQueryInNewTab) => {
                        this.execute_selected(OpenPlacement::NewTab, window, cx)
                    }
                    _ => cx.propagate(),
                }
            }))
            .child(
                div()
                    .id("command-palette-container")
                    .relative()
                    .w(PaletteMetrics::WIDTH)
                    .max_w_full()
                    .max_h(max_height)
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(card_shape)
                    .child(self.render_search_row(cx))
                    .child(
                        div()
                            .id("command-palette-list")
                            .flex()
                            .flex_col()
                            .flex_1()
                            .min_h_0()
                            .overflow_y_hidden()
                            .pb(PaletteMetrics::LIST_PADDING_BOTTOM)
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
                            .children(list_rows)
                            .when(self.filtered.is_empty(), |list| {
                                list.child(
                                    div()
                                        .flex()
                                        .justify_center()
                                        .h(PaletteMetrics::SEARCH_HEIGHT)
                                        .items_center()
                                        .child(
                                            Text::body(dbflux_i18n::t!("palette.empty"))
                                                .muted_foreground(),
                                        ),
                                )
                            }),
                    )
                    .child(Self::render_footer(cx)),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MIXED_QUERY_COMMAND_LIMIT, PaletteCommand, PaletteItem, PaletteScope, PaletteSection,
        ResourceItem, arrange_results, filter_items, footer_key_label, match_ranges,
        palette_navigate_label, palette_shortcut_parts,
    };
    use fuzzy_matcher::skim::SkimMatcherV2;
    use std::fs;
    use uuid::Uuid;

    fn command_palette_source() -> String {
        let source = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/ui/overlays/command_palette.rs"
        ))
        .unwrap_or_else(|error| panic!("failed to read command_palette.rs: {error}"));

        // Extract only the `impl Render` body. The marker is anchored on the
        // ` {` brace so it matches the real implementation and not the string
        // literals inside this helper, and the slice stops at the test module
        // so the assertions never inspect their own source text.
        let impl_start = source
            .find("impl Render for CommandPalette {")
            .expect("command_palette.rs should contain a render implementation");
        let after_impl = &source[impl_start..];
        let render_end = after_impl.find("#[cfg(test)]").unwrap_or(after_impl.len());

        after_impl[..render_end].to_string()
    }

    fn action(id: &'static str, name: &str) -> PaletteItem {
        PaletteItem::Action {
            id,
            name: name.to_string().into(),
            category: "Results".into(),
            shortcut: None,
        }
    }

    fn table(name: &str) -> PaletteItem {
        PaletteItem::Resource(ResourceItem::Table {
            profile_id: Uuid::new_v4(),
            profile_name: "shop-pg".to_string(),
            database: None,
            schema: Some("public".to_string()),
            name: name.to_string(),
        })
    }

    fn connection(name: &str, is_connected: bool) -> PaletteItem {
        PaletteItem::Connection {
            profile_id: Uuid::new_v4(),
            name: name.to_string(),
            is_connected,
            icon: None,
        }
    }

    #[test]
    fn palette_commands_still_preserve_explicit_shortcuts() {
        let command = PaletteCommand::new("id", "Open", "Action").with_shortcut("ctrl-k");

        assert_eq!(command.shortcut, Some("ctrl-k"));
    }

    #[test]
    fn shortcut_parts_give_one_keycap_per_key() {
        assert_eq!(
            palette_shortcut_parts("ctrl-shift-2"),
            vec!["Ctrl", "Shift", "2"]
        );
        assert_eq!(palette_shortcut_parts("ctrl-e"), vec!["Ctrl", "E"]);
        assert_eq!(palette_shortcut_parts("x"), vec!["x"]);
    }

    #[test]
    fn commands_come_first_then_connections_then_tables() {
        assert!(PaletteSection::Commands.sort_order() < PaletteSection::Connections.sort_order());
        assert!(PaletteSection::Connections.sort_order() < PaletteSection::Tables.sort_order());
        assert!(PaletteSection::Tables.sort_order() < PaletteSection::Scripts.sort_order());
    }

    #[test]
    fn scope_prefix_is_split_from_the_query() {
        assert_eq!(PaletteScope::parse("orders"), (PaletteScope::All, "orders"));
        assert_eq!(
            PaletteScope::parse("> export"),
            (PaletteScope::Commands, "export")
        );
        assert_eq!(PaletteScope::parse("@ord"), (PaletteScope::Tables, "ord"));
    }

    #[test]
    fn command_scope_keeps_only_commands_and_table_scope_only_tables() {
        let matcher = SkimMatcherV2::default();
        let items = vec![
            action("export_results", "Export results"),
            connection("orders-db", true),
            table("orders"),
        ];

        let commands = filter_items(&items, &matcher, ">");
        assert_eq!(
            commands.iter().map(|item| item.index).collect::<Vec<_>>(),
            vec![0]
        );

        let tables = filter_items(&items, &matcher, "@or");
        assert_eq!(
            tables.iter().map(|item| item.index).collect::<Vec<_>>(),
            vec![2]
        );

        let everything = filter_items(&items, &matcher, "");
        assert_eq!(everything.len(), 3);
    }

    #[test]
    fn a_mixed_query_caps_commands_so_connections_and_tables_show() {
        let matcher = SkimMatcherV2::default();
        let items = vec![
            action("export_results", "Export orders"),
            action("focus_editor", "Focus orders editor"),
            action("open_history", "Open orders history"),
            action("close_tab", "Close orders tab"),
            action("next_tab", "Next orders tab"),
            connection("orders-db", true),
            table("orders"),
            table("order_items"),
        ];

        let arranged = arrange_results(&items, filter_items(&items, &matcher, "or"), "or");
        let kinds: Vec<PaletteSection> = arranged
            .iter()
            .map(|filtered| PaletteSection::for_item(&items[filtered.index]))
            .collect();

        let command_count = kinds
            .iter()
            .filter(|section| **section == PaletteSection::Commands)
            .count();

        assert_eq!(command_count, MIXED_QUERY_COMMAND_LIMIT);
        assert!(kinds.contains(&PaletteSection::Connections));
        assert_eq!(
            kinds
                .iter()
                .filter(|section| **section == PaletteSection::Tables)
                .count(),
            2
        );
    }

    #[test]
    fn prefixed_and_empty_queries_keep_every_command() {
        let matcher = SkimMatcherV2::default();
        let items = vec![
            action("export_results", "Export orders"),
            action("focus_editor", "Focus orders editor"),
            action("open_history", "Open orders history"),
            action("close_tab", "Close orders tab"),
            table("orders"),
        ];

        let commands_only = arrange_results(&items, filter_items(&items, &matcher, "> or"), "> or");
        assert_eq!(commands_only.len(), 4);

        let everything = arrange_results(&items, filter_items(&items, &matcher, ""), "");
        assert_eq!(everything.len(), 5);
    }

    #[test]
    fn footer_shows_arrows_in_one_keycap_and_enter_as_a_glyph() {
        assert_eq!(
            palette_navigate_label(Some("↑".into()), Some("↓".into())).as_deref(),
            Some("↑↓")
        );
        assert_eq!(palette_navigate_label(None, None), None);
        assert_eq!(footer_key_label("Enter").as_ref(), "\u{21b5}");
        assert_eq!(footer_key_label("Ctrl ↵").as_ref(), "Ctrl ↵");
    }

    #[test]
    fn match_ranges_cover_the_matched_characters_of_the_name() {
        let matcher = SkimMatcherV2::default();

        assert_eq!(match_ranges(&matcher, "orders", "or"), vec![0..2]);
        assert!(match_ranges(&matcher, "orders", "").is_empty());
        assert!(match_ranges(&matcher, "orders", "xyz").is_empty());
    }

    #[test]
    fn connection_qualifier_says_connected_only_when_connected() {
        assert_eq!(
            connection("cache-redis", true).qualifier().as_deref(),
            Some("connected")
        );
        assert_eq!(connection("cache-redis", false).qualifier(), None);
        assert_eq!(action("export_results", "Export results").qualifier(), None);
    }

    #[test]
    fn command_palette_card_uses_the_modal_surface_on_the_shared_scrim() {
        let source = command_palette_source();

        assert!(source.contains(".bg(overlay_bg(theme))"));
        assert!(source.contains("inspect_surface_role(SurfaceRole::Modal)"));
        assert!(source.contains("Chamfer::new(ChamferCut::CARD)"));
        assert!(source.contains(".id(\"command-palette-container\")"));
    }

    #[test]
    fn command_palette_render_keeps_overlay_identity_and_close_behavior() {
        let source = command_palette_source();

        assert!(source.contains(".id(\"command-palette-overlay\")"));
        assert!(source.contains(".key_context(ContextId::CommandPalette.as_gpui_context())"));
        assert!(source.contains("this.hide(cx);"));
    }

    // R.1 — Import label cleanup

    #[test]
    fn command_palette_import_label_contains_no_cloudwatch() {
        let (_category, label) = PaletteItem::ImportDashboard.display_label();
        assert!(
            !label.contains("CloudWatch"),
            "ImportDashboard display label must not reference CloudWatch; got: {label:?}"
        );
        let search = PaletteItem::ImportDashboard.search_text();
        assert!(
            !search.contains("CloudWatch"),
            "ImportDashboard search_text must not reference CloudWatch; got: {search:?}"
        );
    }

    #[test]
    fn command_palette_import_label_is_exactly_correct() {
        let (_category, label) = PaletteItem::ImportDashboard.display_label();
        assert_eq!(label, "Import dashboard from JSON…");
    }

    // R.2 — New palette entries

    #[test]
    fn command_palette_contains_no_cloudwatch_substring_in_any_action_label() {
        // All PaletteItem::Action entries come from default_commands(), which are
        // turned into PaletteItem::Action. We verify none reference "CloudWatch".
        use super::super::super::views::workspace::Workspace;

        let commands = Workspace::palette_commands_for_test();
        for cmd in &commands {
            assert!(
                !cmd.name.contains("CloudWatch"),
                "Command {:?} name must not reference CloudWatch",
                cmd.name
            );
            assert!(
                !cmd.category.contains("CloudWatch"),
                "Command {:?} category must not reference CloudWatch",
                cmd.category
            );
        }
    }

    #[test]
    fn command_palette_includes_new_dashboard_entry() {
        use super::super::super::views::workspace::Workspace;

        let commands = Workspace::palette_commands_for_test();
        let expected_name = dbflux_i18n::t!("palette.command.new_dashboard.name");
        let expected_category = dbflux_i18n::t!("palette.category.dashboards");
        let found = commands
            .iter()
            .any(|c| c.name == expected_name && c.category == expected_category);
        assert!(
            found,
            "Palette must include 'Dashboards: New dashboard…' entry"
        );
    }

    #[test]
    fn command_palette_does_not_include_import_dashboard_in_commands() {
        // ImportDashboard is a PaletteItem::ImportDashboard, not a PaletteCommand.
        // It should never appear in default_commands().
        use super::super::super::views::workspace::Workspace;

        let commands = Workspace::palette_commands_for_test();
        let import_in_commands = commands
            .iter()
            .any(|c| c.name.contains("Import") && c.name.contains("Dashboard"));
        assert!(
            !import_in_commands,
            "ImportDashboard must not appear in default_commands(); found: {:?}",
            commands
                .iter()
                .filter(|c| c.name.contains("Import"))
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        );
    }

    // i18n — command palette foundation

    const PALETTE_CATALOG_KEYS: &[&str] = &[
        "palette.search.placeholder",
        "palette.empty",
        "palette.section.connections",
        "palette.section.commands",
        "palette.section.charts",
        "palette.section.tables",
        "palette.section.scripts",
        "palette.kind.connection",
        "palette.kind.chart",
        "palette.kind.table",
        "palette.kind.collection",
        "palette.kind.view",
        "palette.kind.keyspace",
        "palette.kind.script",
        "palette.chart.browse_suffix",
        "palette.import_dashboard.name",
        "palette.footer.navigate",
        "palette.footer.run",
        "palette.footer.open_in_new_tab",
        "palette.footer.commands_only",
        "palette.footer.tables_only",
        "palette.connection.connected",
        "palette.chart.no_saved_charts",
    ];

    #[test]
    fn palette_keys_resolve_in_both_locales() {
        for locale in ["en", "es", "ko", "zh_Hans"] {
            for key in PALETTE_CATALOG_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn palette_section_label_differs_between_locales() {
        let english = dbflux_i18n::t!("palette.section.connections", locale = "en");
        let spanish = dbflux_i18n::t!("palette.section.connections", locale = "es");

        assert_eq!(english, "Connections");
        assert_eq!(spanish, "Conexiones");
        assert_ne!(english, spanish);
    }

    #[test]
    fn search_text_matches_english_and_translated_kind() {
        // `search_text` intentionally stays bilingual: the leading kind word
        // is the English proxy the filter always understands, followed by
        // the process-locale translated kind word so a table item also
        // matches on the active-locale name. In the default "en" test
        // locale the two words are identical, so a correct implementation
        // embeds "Table" twice; a regression that drops the translated
        // lookup only embeds it once.
        let item = PaletteItem::Resource(ResourceItem::Table {
            profile_id: Uuid::new_v4(),
            profile_name: "prod".to_string(),
            database: None,
            schema: None,
            name: "orders".to_string(),
        });

        let search_text = item.search_text();

        assert!(search_text.contains("Table"));
        assert_eq!(
            search_text.matches("Table").count(),
            2,
            "expected the English proxy word and the translated kind word both present, got: {search_text:?}"
        );
    }

    // i18n — command palette command copy

    const PALETTE_COMMAND_CATEGORIES: &[&str] = &[
        "editor",
        "tabs",
        "results",
        "connections",
        "focus",
        "view",
        "charts",
        "dashboards",
    ];

    #[test]
    fn palette_command_keys_resolve_in_both_locales() {
        use super::super::super::views::workspace::Workspace;

        let commands = Workspace::palette_commands_for_test();

        for locale in ["en", "es"] {
            for command in &commands {
                let key = format!("palette.command.{}.name", command.id);
                let value = dbflux_i18n::t!(&key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }

            for category in PALETTE_COMMAND_CATEGORIES {
                let key = format!("palette.category.{category}");
                let value = dbflux_i18n::t!(&key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn palette_command_name_differs_between_locales() {
        let english = dbflux_i18n::t!("palette.command.run_query.name", locale = "en");
        let spanish = dbflux_i18n::t!("palette.command.run_query.name", locale = "es");

        assert_eq!(english, "Run query");
        assert_eq!(spanish, "Ejecutar consulta");
        assert_ne!(english, spanish);
    }
}
