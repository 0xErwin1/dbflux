//! The command palette's product half: the items DBFlux offers, the
//! selection each one yields, and the keymap facts its keys run.
//!
//! The palette engine — fuzzy query, section grouping, key cursor, footer
//! hints, the card on the shared scrim — lives in `bolt` and is re-exported
//! here through [`dbflux_components::command_palette`], so call sites keep
//! compiling. What stays here is the product vocabulary: the
//! [`PaletteItem`] catalogue and its [`PaletteEntry`] impl, the
//! [`PaletteSelection`] it yields, and the [`PaletteHost`] over
//! `dbflux_ui_base`'s keymap. The engine's words come from the text seam;
//! the item kind words and the palette command copy are translated here,
//! through `dbflux_i18n` directly.

use crate::keymap::{Command, ContextId};
use crate::ui::icons::AppIcon;
use dbflux_app::keymap::default_slots;
use dbflux_components::command_palette::{
    PaletteEntry, PaletteHost, PaletteSection, RunCommandHandler, palette_shortcut_parts,
};
use dbflux_components::icon::IconSource;
use dbflux_components::icons::DriverIconTone;
use dbflux_core::{CollectionRef, TableRef};
use dbflux_ui_base::keymap::{
    RunCommand, chord_display_parts, default_keymap, effective_keymap, run_command, shortcut_label,
};
use gpui::{App, Div, EventEmitter, InteractiveElement as _, SharedString, Stateful};
use std::path::PathBuf;
use uuid::Uuid;

pub type CommandPalette =
    dbflux_components::command_palette::CommandPalette<PaletteItem, DbfluxPaletteHost>;

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
    /// Text searched by the palette's fuzzy matcher.
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

    /// Leading icon and, for a connection, the driver's tone.
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
        "toggle_results_position" => AppIcon::PanelRight,
        "increase_interface_size" => AppIcon::ZoomIn,
        "decrease_interface_size" => AppIcon::ZoomOut,
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

/// Event emitted when the palette closes without a selection.
#[derive(Default)]
pub struct CommandPaletteClosed;

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

/// The section a [`PaletteItem`] groups under.
fn item_section(item: &PaletteItem) -> PaletteSection {
    match item {
        PaletteItem::Connection { .. } => PaletteSection::Connections,
        PaletteItem::Action { .. } => PaletteSection::Commands,
        PaletteItem::SavedChart { .. } | PaletteItem::ImportDashboard => PaletteSection::Charts,
        PaletteItem::Resource(_) => PaletteSection::Tables,
        PaletteItem::Script { .. } => PaletteSection::Scripts,
    }
}

impl PaletteEntry for PaletteItem {
    type Selection = PaletteSelection;

    fn search_text(&self) -> String {
        PaletteItem::search_text(self)
    }

    fn display_label(&self) -> (String, String) {
        PaletteItem::display_label(self)
    }

    fn type_priority(&self) -> u8 {
        PaletteItem::type_priority(self)
    }

    fn qualifier(&self) -> Option<String> {
        PaletteItem::qualifier(self)
    }

    fn inline_qualifier(&self) -> Option<SharedString> {
        PaletteItem::inline_qualifier(self)
    }

    fn icon(&self, cx: &App) -> (IconSource, Option<gpui::Hsla>) {
        let (icon, tone) = self.icon();
        (icon.into(), tone.map(|tone| tone.resolve(cx)))
    }

    fn section(&self) -> PaletteSection {
        item_section(self)
    }

    fn keycaps(&self) -> Vec<SharedString> {
        match self {
            PaletteItem::Action { id, shortcut, .. } => palette_command_keycaps(id, *shortcut),
            _ => Vec::new(),
        }
    }

    fn select(&self, new_tab: bool) -> PaletteSelection {
        match self {
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
            PaletteItem::Script { path, .. } => PaletteSelection::OpenScript { path: path.clone() },
            PaletteItem::SavedChart { id, .. } => {
                PaletteSelection::OpenSavedChart { chart_id: *id }
            }
            PaletteItem::ImportDashboard => PaletteSelection::ImportDashboard,
        }
    }
}

/// Contexts searched, in order, for the chord a palette command shows.
const PALETTE_SHORTCUT_CONTEXTS: [ContextId; 5] = [
    ContextId::Global,
    ContextId::Editor,
    ContextId::Results,
    ContextId::Sidebar,
    ContextId::BackgroundTasks,
];

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

/// DBFlux's palette host: the key context, keycap labels and run-command
/// dispatch of the product's keymap, as the palette engine reads them.
pub struct DbfluxPaletteHost;

impl PaletteHost for DbfluxPaletteHost {
    type Closed = CommandPaletteClosed;

    fn key_context() -> &'static str {
        ContextId::CommandPalette.as_gpui_context()
    }

    fn key_label(command: dbflux_components::keymap::Command) -> Option<SharedString> {
        let command = match command {
            dbflux_components::keymap::Command::SelectPrev => Command::SelectPrev,
            dbflux_components::keymap::Command::SelectNext => Command::SelectNext,
            dbflux_components::keymap::Command::Execute => Command::Execute,
            dbflux_components::keymap::Command::Cancel => Command::Cancel,
            dbflux_components::keymap::Command::OpenInNewTab => Command::RunQueryInNewTab,
            _ => return None,
        };

        shortcut_label(ContextId::CommandPalette, command)
    }

    fn attach_run_command(element: Stateful<Div>, on_command: RunCommandHandler) -> Stateful<Div> {
        element.on_action(move |action: &RunCommand, window, cx: &mut App| {
            let handled = run_command(action)
                .map(|command| on_command(command.id(), window, cx))
                .unwrap_or(false);

            if !handled {
                cx.propagate();
            }
        })
    }
}

// The events the palette emits, named for the concrete palette the
// workspace keeps.
impl EventEmitter<PaletteSelection> for CommandPalette {}
impl EventEmitter<CommandPaletteClosed> for CommandPalette {}

#[cfg(test)]
mod tests {
    use super::{
        PaletteCommand, PaletteEntry, PaletteItem, PaletteSection, PaletteSelection, ResourceItem,
    };
    use uuid::Uuid;

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

    /// The section mapping moved with the engine's grouping: every product
    /// kind must land where the palette draws it.
    #[test]
    fn every_item_kind_groups_into_its_section() {
        assert_eq!(
            action("export_results", "Export").section(),
            PaletteSection::Commands
        );
        assert_eq!(
            connection("orders-db", true).section(),
            PaletteSection::Connections
        );
        assert_eq!(table("orders").section(), PaletteSection::Tables);
        assert_eq!(
            PaletteItem::Script {
                path: "/tmp/x.sql".into(),
                name: "x.sql".into(),
                relative_path: "x.sql".into(),
            }
            .section(),
            PaletteSection::Scripts
        );
        assert_eq!(
            PaletteItem::SavedChart {
                id: Uuid::new_v4(),
                name: "chart".into(),
                profile_name: "prod".into(),
                profile_id: Uuid::new_v4(),
                is_collection_source: false,
            }
            .section(),
            PaletteSection::Charts
        );
        assert_eq!(
            PaletteItem::ImportDashboard.section(),
            PaletteSection::Charts
        );
    }

    /// The selection mapping moved with the engine's confirm: a resource
    /// item must yield the same table reference it always did.
    #[test]
    fn a_table_item_selects_the_table_it_shows() {
        let item = table("orders");
        let PaletteSelection::OpenTable {
            table,
            database,
            new_tab,
            ..
        } = item.select(false)
        else {
            panic!("a table item must open a table");
        };

        assert_eq!(table.name, "orders");
        assert_eq!(table.schema.as_deref(), Some("public"));
        assert_eq!(database, None);
        assert!(!new_tab);
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
        use crate::ui::views::workspace::Workspace;

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
        use crate::ui::views::workspace::Workspace;

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
    fn command_palette_lists_toggle_full_screen_under_view() {
        use crate::ui::views::workspace::Workspace;

        let commands = Workspace::palette_commands_for_test();
        let entry = commands
            .iter()
            .find(|command| command.id == "toggle_full_screen")
            .expect("the palette must list Toggle full screen");
        assert_eq!(entry.name, "Toggle full screen");
        assert_eq!(entry.category, "View");
    }

    #[test]
    fn command_palette_lists_the_interface_size_commands_under_view() {
        use crate::ui::views::workspace::Workspace;

        let commands = Workspace::palette_commands_for_test();

        for (id, name) in [
            ("increase_interface_size", "Increase interface size"),
            ("decrease_interface_size", "Decrease interface size"),
            ("reset_interface_size", "Reset interface size"),
        ] {
            let entry = commands
                .iter()
                .find(|command| command.id == id)
                .unwrap_or_else(|| panic!("the palette must list {name}"));
            assert_eq!(entry.name, name);
            assert_eq!(entry.category, "View");
        }
    }

    #[test]
    fn command_palette_does_not_include_import_dashboard_in_commands() {
        // ImportDashboard is a PaletteItem::ImportDashboard, not a PaletteCommand.
        // It should never appear in default_commands().
        use crate::ui::views::workspace::Workspace;

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
    //
    // These assert through `dbflux_i18n` directly: the seam resolves the
    // same catalogue at runtime, and these pin the keys themselves.

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
        use crate::ui::views::workspace::Workspace;

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
