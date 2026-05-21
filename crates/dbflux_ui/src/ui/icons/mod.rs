use dbflux_components::icon::IconSource;
use dbflux_core::Icon;

/// App-specific icons embedded from resources/icons/
///
/// This enum centralizes all SVG icon references used throughout DBFlux.
/// Icons are loaded via GPUI's AssetSource using the `path()` method.
///
/// Usage:
/// ```rust,ignore
/// Icon::new(AppIcon::Folder).size_4().color(theme.foreground)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum AppIcon {
    // Chevrons / Navigation
    ChevronDown,
    ChevronLeft,
    ChevronRight,
    ChevronUp,

    // Actions
    Play,
    SquarePlay,
    Plus,
    Power,
    Save,
    Delete,
    Pencil,
    Copy,
    RefreshCcw,
    RotateCcw,
    Download,
    Search,
    Settings,
    History,
    Undo,
    Redo,
    X,

    // UI elements
    Eye,
    EyeOff,
    Loader,
    Info,
    Check,
    CircleAlert,
    CircleCheck,
    CircleX,
    TriangleAlert,
    ExternalLink,
    Globe,
    Code,
    Table,
    Columns,
    Rows3,
    ArrowUp,
    ArrowDown,
    Star,
    Clock,
    Zap,
    Hash,
    Lock,
    Layers,
    Keyboard,
    FingerprintPattern,
    PanelBottomClose,
    PanelBottomOpen,
    FileSpreadsheet,
    KeyRound,
    Link2,
    CaseSensitive,
    ScrollText,
    ListFilter,
    ArrowUpDown,
    Bot,
    BrainCircuit,

    // Connection / Network
    Plug,
    Unplug,
    Server,
    HardDrive,

    // Files / Folders
    FileCode,
    Folder,
    Box,
    Braces,
    SquareTerminal,
    Parentheses,
    Sigma,

    // Database generic
    Database,
    DatabaseZap,

    // Zoom / View
    ZoomIn,
    ZoomOut,
    Minus,
    Maximize2,
    Minimize2,
    Grid3x3,
    Snowflake,
    Scale,
    ArrowLeftRight,

    // Clipboard
    Clipboard,

    // Generic non-database data sources
    Logs,

    // Charts
    ChartSpline,
    ChartArea,
    ChartColumnBig,
    ChartBar,
    ChartPie,
    ChartNetwork,

    // Database brands (SimpleIcons)
    BrandPostgres,
    BrandMysql,
    BrandMariadb,
    BrandSqlite,
    BrandMongodb,
    BrandRedis,

    // Language brands (for script file icons)
    BrandLua,
    BrandPython,
    BrandBash,
    BrandJavaScript,
    BrandInfluxDb,

    // App branding
    DbFlux,
}

impl AppIcon {
    /// Returns the asset path for this icon.
    pub const fn path(self) -> &'static str {
        match self {
            Self::ChevronDown => "icons/ui/chevron-down.svg",
            Self::ChevronLeft => "icons/ui/chevron-left.svg",
            Self::ChevronRight => "icons/ui/chevron-right.svg",
            Self::ChevronUp => "icons/ui/chevron-up.svg",
            Self::Play => "icons/ui/play.svg",
            Self::SquarePlay => "icons/ui/square-play.svg",
            Self::Plus => "icons/ui/plus.svg",
            Self::Power => "icons/ui/power.svg",
            Self::Save => "icons/ui/save.svg",
            Self::Delete => "icons/ui/delete.svg",
            Self::Pencil => "icons/ui/pencil.svg",
            Self::Copy => "icons/ui/copy.svg",
            Self::RefreshCcw => "icons/ui/refresh-ccw.svg",
            Self::RotateCcw => "icons/ui/rotate-ccw.svg",
            Self::Download => "icons/ui/download.svg",
            Self::Search => "icons/ui/search.svg",
            Self::Settings => "icons/ui/settings.svg",
            Self::History => "icons/ui/history.svg",
            Self::Undo => "icons/ui/undo.svg",
            Self::Redo => "icons/ui/redo.svg",
            Self::X => "icons/ui/x.svg",
            Self::Eye => "icons/ui/eye.svg",
            Self::EyeOff => "icons/ui/eye-off.svg",
            Self::Loader => "icons/ui/loader.svg",
            Self::Info => "icons/ui/info.svg",
            Self::Check => "icons/ui/check.svg",
            Self::CircleAlert => "icons/ui/circle-alert.svg",
            Self::CircleCheck => "icons/ui/circle-check.svg",
            Self::CircleX => "icons/ui/circle-x.svg",
            Self::TriangleAlert => "icons/ui/triangle-alert.svg",
            Self::ExternalLink => "icons/ui/external-link.svg",
            Self::Globe => "icons/ui/globe.svg",
            Self::Code => "icons/ui/code.svg",
            Self::Table => "icons/ui/table.svg",
            Self::Columns => "icons/ui/columns.svg",
            Self::Rows3 => "icons/ui/rows-3.svg",
            Self::ArrowUp => "icons/ui/arrow-up.svg",
            Self::ArrowDown => "icons/ui/arrow-down.svg",
            Self::Star => "icons/ui/star.svg",
            Self::Clock => "icons/ui/clock.svg",
            Self::Zap => "icons/ui/zap.svg",
            Self::Hash => "icons/ui/hash.svg",
            Self::Lock => "icons/ui/lock.svg",
            Self::Layers => "icons/ui/layers.svg",
            Self::Keyboard => "icons/ui/keyboard.svg",
            Self::FingerprintPattern => "icons/ui/fingerprint-pattern.svg",
            Self::PanelBottomClose => "icons/ui/panel-bottom-close.svg",
            Self::PanelBottomOpen => "icons/ui/panel-bottom-open.svg",
            Self::FileSpreadsheet => "icons/ui/file-spreadsheet.svg",
            Self::KeyRound => "icons/ui/key-round.svg",
            Self::Link2 => "icons/ui/link-2.svg",
            Self::CaseSensitive => "icons/ui/case-sensitive.svg",
            Self::ScrollText => "icons/ui/scroll-text.svg",
            Self::ListFilter => "icons/ui/list-filter.svg",
            Self::ArrowUpDown => "icons/ui/arrow-up-down.svg",
            Self::Plug => "icons/ui/plug.svg",
            Self::Unplug => "icons/ui/unplug.svg",
            Self::Server => "icons/ui/server.svg",
            Self::HardDrive => "icons/ui/hard-drive.svg",
            Self::FileCode => "icons/ui/file-code-corner.svg",
            Self::Folder => "icons/ui/folder.svg",
            Self::Box => "icons/ui/box.svg",
            Self::Braces => "icons/ui/braces.svg",
            Self::SquareTerminal => "icons/ui/square-terminal.svg",
            Self::Parentheses => "icons/ui/parentheses.svg",
            Self::Sigma => "icons/ui/sigma.svg",
            Self::Database => "icons/ui/database.svg",
            Self::Logs => "icons/ui/logs.svg",
            Self::DatabaseZap => "icons/ui/database-zap.svg",
            Self::ZoomIn => "icons/ui/zoom-in.svg",
            Self::ZoomOut => "icons/ui/zoom-out.svg",
            Self::Minus => "icons/ui/minus.svg",
            Self::Maximize2 => "icons/ui/maximize-2.svg",
            Self::Minimize2 => "icons/ui/minimize-2.svg",
            Self::Grid3x3 => "icons/ui/grid-3x3.svg",
            Self::Snowflake => "icons/ui/snowflake.svg",
            Self::Scale => "icons/ui/scale.svg",
            Self::ArrowLeftRight => "icons/ui/arrow-left-right.svg",
            Self::Clipboard => "icons/ui/clipboard.svg",
            Self::ChartSpline => "icons/ui/chart-spline.svg",
            Self::ChartArea => "icons/ui/chart-area.svg",
            Self::ChartColumnBig => "icons/ui/chart-column-big.svg",
            Self::ChartBar => "icons/ui/chart-bar.svg",
            Self::ChartPie => "icons/ui/chart-pie.svg",
            Self::ChartNetwork => "icons/ui/chart-network.svg",
            Self::BrandPostgres => "icons/brand/postgresql.svg",
            Self::BrandMysql => "icons/brand/mysql.svg",
            Self::BrandMariadb => "icons/brand/mariadb.svg",
            Self::BrandSqlite => "icons/brand/sqlite.svg",
            Self::BrandMongodb => "icons/brand/mongodb.svg",
            Self::BrandRedis => "icons/brand/redis.svg",
            Self::BrandLua => "icons/brand/lua.svg",
            Self::BrandPython => "icons/brand/python.svg",
            Self::BrandBash => "icons/brand/gnubash.svg",
            Self::BrandJavaScript => "icons/brand/javascript.svg",
            Self::BrandInfluxDb => "icons/brand/influxdb.svg",
            Self::DbFlux => "icons/dbflux.svg",
            Self::BrainCircuit => "icons/ui/brain-circuit.svg",
            Self::Bot => "icons/ui/bot.svg",
        }
    }

    pub fn embedded_bytes(self) -> &'static [u8] {
        match self {
            Self::ChevronDown => {
                include_bytes!("../../../../../resources/icons/ui/chevron-down.svg")
            }
            Self::ChevronLeft => {
                include_bytes!("../../../../../resources/icons/ui/chevron-left.svg")
            }
            Self::ChevronRight => {
                include_bytes!("../../../../../resources/icons/ui/chevron-right.svg")
            }
            Self::ChevronUp => include_bytes!("../../../../../resources/icons/ui/chevron-up.svg"),
            Self::Play => include_bytes!("../../../../../resources/icons/ui/play.svg"),
            Self::SquarePlay => include_bytes!("../../../../../resources/icons/ui/square-play.svg"),
            Self::Plus => include_bytes!("../../../../../resources/icons/ui/plus.svg"),
            Self::Power => include_bytes!("../../../../../resources/icons/ui/power.svg"),
            Self::Save => include_bytes!("../../../../../resources/icons/ui/save.svg"),
            Self::Delete => include_bytes!("../../../../../resources/icons/ui/delete.svg"),
            Self::Pencil => include_bytes!("../../../../../resources/icons/ui/pencil.svg"),
            Self::Copy => include_bytes!("../../../../../resources/icons/ui/copy.svg"),
            Self::RefreshCcw => include_bytes!("../../../../../resources/icons/ui/refresh-ccw.svg"),
            Self::RotateCcw => include_bytes!("../../../../../resources/icons/ui/rotate-ccw.svg"),
            Self::Download => include_bytes!("../../../../../resources/icons/ui/download.svg"),
            Self::Search => include_bytes!("../../../../../resources/icons/ui/search.svg"),
            Self::Settings => include_bytes!("../../../../../resources/icons/ui/settings.svg"),
            Self::History => include_bytes!("../../../../../resources/icons/ui/history.svg"),
            Self::Undo => include_bytes!("../../../../../resources/icons/ui/undo.svg"),
            Self::Redo => include_bytes!("../../../../../resources/icons/ui/redo.svg"),
            Self::X => include_bytes!("../../../../../resources/icons/ui/x.svg"),
            Self::Eye => include_bytes!("../../../../../resources/icons/ui/eye.svg"),
            Self::EyeOff => include_bytes!("../../../../../resources/icons/ui/eye-off.svg"),
            Self::Loader => include_bytes!("../../../../../resources/icons/ui/loader.svg"),
            Self::Info => include_bytes!("../../../../../resources/icons/ui/info.svg"),
            Self::CircleAlert => {
                include_bytes!("../../../../../resources/icons/ui/circle-alert.svg")
            }
            Self::CircleCheck => {
                include_bytes!("../../../../../resources/icons/ui/circle-check.svg")
            }
            Self::CircleX => include_bytes!("../../../../../resources/icons/ui/circle-x.svg"),
            Self::Check => include_bytes!("../../../../../resources/icons/ui/check.svg"),
            Self::ExternalLink => {
                include_bytes!("../../../../../resources/icons/ui/external-link.svg")
            }
            Self::Globe => include_bytes!("../../../../../resources/icons/ui/globe.svg"),
            Self::TriangleAlert => {
                include_bytes!("../../../../../resources/icons/ui/triangle-alert.svg")
            }
            Self::Code => include_bytes!("../../../../../resources/icons/ui/code.svg"),
            Self::Table => include_bytes!("../../../../../resources/icons/ui/table.svg"),
            Self::Columns => include_bytes!("../../../../../resources/icons/ui/columns.svg"),
            Self::Rows3 => include_bytes!("../../../../../resources/icons/ui/rows-3.svg"),
            Self::ArrowUp => include_bytes!("../../../../../resources/icons/ui/arrow-up.svg"),
            Self::ArrowDown => include_bytes!("../../../../../resources/icons/ui/arrow-down.svg"),
            Self::Star => include_bytes!("../../../../../resources/icons/ui/star.svg"),
            Self::Clock => include_bytes!("../../../../../resources/icons/ui/clock.svg"),
            Self::Zap => include_bytes!("../../../../../resources/icons/ui/zap.svg"),
            Self::Hash => include_bytes!("../../../../../resources/icons/ui/hash.svg"),
            Self::Lock => include_bytes!("../../../../../resources/icons/ui/lock.svg"),
            Self::Layers => include_bytes!("../../../../../resources/icons/ui/layers.svg"),
            Self::Keyboard => include_bytes!("../../../../../resources/icons/ui/keyboard.svg"),
            Self::FingerprintPattern => {
                include_bytes!("../../../../../resources/icons/ui/fingerprint-pattern.svg")
            }
            Self::PanelBottomClose => {
                include_bytes!("../../../../../resources/icons/ui/panel-bottom-close.svg")
            }
            Self::PanelBottomOpen => {
                include_bytes!("../../../../../resources/icons/ui/panel-bottom-open.svg")
            }
            Self::FileSpreadsheet => {
                include_bytes!("../../../../../resources/icons/ui/file-spreadsheet.svg")
            }
            Self::KeyRound => include_bytes!("../../../../../resources/icons/ui/key-round.svg"),
            Self::Link2 => include_bytes!("../../../../../resources/icons/ui/link-2.svg"),
            Self::CaseSensitive => {
                include_bytes!("../../../../../resources/icons/ui/case-sensitive.svg")
            }
            Self::ScrollText => include_bytes!("../../../../../resources/icons/ui/scroll-text.svg"),
            Self::ListFilter => include_bytes!("../../../../../resources/icons/ui/list-filter.svg"),
            Self::ArrowUpDown => {
                include_bytes!("../../../../../resources/icons/ui/arrow-up-down.svg")
            }
            Self::Plug => include_bytes!("../../../../../resources/icons/ui/plug.svg"),
            Self::Unplug => include_bytes!("../../../../../resources/icons/ui/unplug.svg"),
            Self::Server => include_bytes!("../../../../../resources/icons/ui/server.svg"),
            Self::HardDrive => include_bytes!("../../../../../resources/icons/ui/hard-drive.svg"),
            Self::FileCode => {
                include_bytes!("../../../../../resources/icons/ui/file-code-corner.svg")
            }
            Self::Folder => include_bytes!("../../../../../resources/icons/ui/folder.svg"),
            Self::Box => include_bytes!("../../../../../resources/icons/ui/box.svg"),
            Self::Braces => include_bytes!("../../../../../resources/icons/ui/braces.svg"),
            Self::SquareTerminal => {
                include_bytes!("../../../../../resources/icons/ui/square-terminal.svg")
            }
            Self::Parentheses => {
                include_bytes!("../../../../../resources/icons/ui/parentheses.svg")
            }
            Self::Sigma => include_bytes!("../../../../../resources/icons/ui/sigma.svg"),
            Self::Database => include_bytes!("../../../../../resources/icons/ui/database.svg"),
            Self::Logs => include_bytes!("../../../../../resources/icons/ui/logs.svg"),
            Self::DatabaseZap => {
                include_bytes!("../../../../../resources/icons/ui/database-zap.svg")
            }
            Self::ZoomIn => include_bytes!("../../../../../resources/icons/ui/zoom-in.svg"),
            Self::ZoomOut => include_bytes!("../../../../../resources/icons/ui/zoom-out.svg"),
            Self::Minus => include_bytes!("../../../../../resources/icons/ui/minus.svg"),
            Self::Maximize2 => include_bytes!("../../../../../resources/icons/ui/maximize-2.svg"),
            Self::Minimize2 => include_bytes!("../../../../../resources/icons/ui/minimize-2.svg"),
            Self::Grid3x3 => include_bytes!("../../../../../resources/icons/ui/grid-3x3.svg"),
            Self::Snowflake => include_bytes!("../../../../../resources/icons/ui/snowflake.svg"),
            Self::Scale => include_bytes!("../../../../../resources/icons/ui/scale.svg"),
            Self::ArrowLeftRight => {
                include_bytes!("../../../../../resources/icons/ui/arrow-left-right.svg")
            }
            Self::Clipboard => include_bytes!("../../../../../resources/icons/ui/clipboard.svg"),
            Self::ChartSpline => {
                include_bytes!("../../../../../resources/icons/ui/chart-spline.svg")
            }
            Self::ChartArea => include_bytes!("../../../../../resources/icons/ui/chart-area.svg"),
            Self::ChartColumnBig => {
                include_bytes!("../../../../../resources/icons/ui/chart-column-big.svg")
            }
            Self::ChartBar => include_bytes!("../../../../../resources/icons/ui/chart-bar.svg"),
            Self::ChartPie => include_bytes!("../../../../../resources/icons/ui/chart-pie.svg"),
            Self::ChartNetwork => {
                include_bytes!("../../../../../resources/icons/ui/chart-network.svg")
            }
            Self::BrandPostgres => {
                include_bytes!("../../../../../resources/icons/brand/postgresql.svg")
            }
            Self::BrandMysql => include_bytes!("../../../../../resources/icons/brand/mysql.svg"),
            Self::BrandMariadb => {
                include_bytes!("../../../../../resources/icons/brand/mariadb.svg")
            }
            Self::BrandSqlite => include_bytes!("../../../../../resources/icons/brand/sqlite.svg"),
            Self::BrandMongodb => {
                include_bytes!("../../../../../resources/icons/brand/mongodb.svg")
            }
            Self::BrandRedis => include_bytes!("../../../../../resources/icons/brand/redis.svg"),
            Self::BrandLua => include_bytes!("../../../../../resources/icons/brand/lua.svg"),
            Self::BrandPython => include_bytes!("../../../../../resources/icons/brand/python.svg"),
            Self::BrandBash => include_bytes!("../../../../../resources/icons/brand/gnubash.svg"),
            Self::BrandJavaScript => {
                include_bytes!("../../../../../resources/icons/brand/javascript.svg")
            }
            Self::BrandInfluxDb => {
                include_bytes!("../../../../../resources/icons/brand/influxdb.svg")
            }
            Self::DbFlux => include_bytes!("../../../../../resources/icons/dbflux.svg"),
            Self::BrainCircuit => {
                include_bytes!("../../../../../resources/icons/ui/brain-circuit.svg")
            }
            Self::Bot => include_bytes!("../../../../../resources/icons/ui/bot.svg"),
        }
    }

    /// Returns the best icon for a given query language.
    ///
    /// Languages with a dedicated brand SVG get their own icon. Languages that
    /// are DB-specific (SQL, MongoDB query, Redis, Cypher, CQL, InfluxQL) reuse
    /// the corresponding DB brand icon when one exists, or fall back to a
    /// generic file icon.
    pub fn for_language(lang: &dbflux_core::QueryLanguage) -> Self {
        use dbflux_core::QueryLanguage;
        match lang {
            QueryLanguage::Lua => Self::BrandLua,
            QueryLanguage::Python => Self::BrandPython,
            QueryLanguage::Bash => Self::BrandBash,
            QueryLanguage::MongoQuery => Self::BrandMongodb,
            QueryLanguage::RedisCommands => Self::BrandRedis,
            QueryLanguage::InfluxQuery | QueryLanguage::Flux => Self::BrandInfluxDb,
            QueryLanguage::Sql
            | QueryLanguage::CloudWatchLogsInsightsQl
            | QueryLanguage::OpenSearchPpl
            | QueryLanguage::OpenSearchSql
            | QueryLanguage::Cql => Self::Database,
            QueryLanguage::Cypher => Self::Database,
            QueryLanguage::Custom(_) => Self::FileCode,
        }
    }

    /// Returns the icon that best represents a given chart kind.
    ///
    /// Used for chart tabs and any chart-kind-specific affordance so the UI
    /// stays agnostic to the concrete `ChartKind` variants.
    pub const fn for_chart_kind(kind: dbflux_components::chart::ChartKind) -> Self {
        use dbflux_components::chart::ChartKind;
        match kind {
            ChartKind::Line => Self::ChartSpline,
            ChartKind::Bar => Self::ChartColumnBig,
            ChartKind::Scatter => Self::ChartNetwork,
            ChartKind::Area => Self::ChartArea,
            ChartKind::StackedBar => Self::ChartColumnBig,
            ChartKind::Pie => Self::ChartPie,
        }
    }

    /// Maps a core Icon to the corresponding AppIcon.
    pub const fn from_icon(icon: Icon) -> Self {
        match icon {
            Icon::Postgres => Self::BrandPostgres,
            Icon::Mysql => Self::BrandMysql,
            Icon::Mariadb => Self::BrandMariadb,
            Icon::Sqlite => Self::BrandSqlite,
            Icon::Mongodb => Self::BrandMongodb,
            Icon::Redis => Self::BrandRedis,
            Icon::Dynamodb => Self::Database,
            // TODO(influxdb-icon): real brand SVG already exists at icons/brand/influxdb.svg
            Icon::Influxdb => Self::BrandInfluxDb,
            Icon::Logs => Self::Logs,
            Icon::Database => Self::Database,
        }
    }
}

pub const ALL_ICONS: &[AppIcon] = &[
    AppIcon::ChevronDown,
    AppIcon::ChevronLeft,
    AppIcon::ChevronRight,
    AppIcon::ChevronUp,
    AppIcon::Play,
    AppIcon::SquarePlay,
    AppIcon::Plus,
    AppIcon::Power,
    AppIcon::Save,
    AppIcon::Delete,
    AppIcon::Pencil,
    AppIcon::Copy,
    AppIcon::RefreshCcw,
    AppIcon::RotateCcw,
    AppIcon::Download,
    AppIcon::Search,
    AppIcon::Settings,
    AppIcon::History,
    AppIcon::Undo,
    AppIcon::Redo,
    AppIcon::X,
    AppIcon::Eye,
    AppIcon::EyeOff,
    AppIcon::Loader,
    AppIcon::Info,
    AppIcon::Check,
    AppIcon::CircleAlert,
    AppIcon::CircleCheck,
    AppIcon::CircleX,
    AppIcon::TriangleAlert,
    AppIcon::ExternalLink,
    AppIcon::Globe,
    AppIcon::Code,
    AppIcon::Table,
    AppIcon::Columns,
    AppIcon::Rows3,
    AppIcon::ArrowUp,
    AppIcon::ArrowDown,
    AppIcon::Star,
    AppIcon::Clock,
    AppIcon::Zap,
    AppIcon::Hash,
    AppIcon::Lock,
    AppIcon::Layers,
    AppIcon::Keyboard,
    AppIcon::FingerprintPattern,
    AppIcon::Maximize2,
    AppIcon::Minimize2,
    AppIcon::PanelBottomClose,
    AppIcon::PanelBottomOpen,
    AppIcon::FileSpreadsheet,
    AppIcon::KeyRound,
    AppIcon::Link2,
    AppIcon::CaseSensitive,
    AppIcon::ScrollText,
    AppIcon::ListFilter,
    AppIcon::ArrowUpDown,
    AppIcon::Plug,
    AppIcon::Unplug,
    AppIcon::Server,
    AppIcon::HardDrive,
    AppIcon::FileCode,
    AppIcon::Folder,
    AppIcon::Box,
    AppIcon::Braces,
    AppIcon::SquareTerminal,
    AppIcon::Parentheses,
    AppIcon::Sigma,
    AppIcon::Database,
    AppIcon::Logs,
    AppIcon::DatabaseZap,
    AppIcon::ZoomIn,
    AppIcon::ZoomOut,
    AppIcon::Minus,
    AppIcon::Grid3x3,
    AppIcon::Snowflake,
    AppIcon::Scale,
    AppIcon::ArrowLeftRight,
    AppIcon::Clipboard,
    AppIcon::ChartSpline,
    AppIcon::ChartArea,
    AppIcon::ChartColumnBig,
    AppIcon::ChartBar,
    AppIcon::ChartPie,
    AppIcon::ChartNetwork,
    AppIcon::BrainCircuit,
    AppIcon::Bot,
    AppIcon::BrandPostgres,
    AppIcon::BrandMysql,
    AppIcon::BrandMariadb,
    AppIcon::BrandSqlite,
    AppIcon::BrandMongodb,
    AppIcon::BrandRedis,
    AppIcon::BrandLua,
    AppIcon::BrandPython,
    AppIcon::BrandBash,
    AppIcon::BrandJavaScript,
    AppIcon::BrandInfluxDb,
    AppIcon::DbFlux,
];

impl From<AppIcon> for IconSource {
    fn from(icon: AppIcon) -> Self {
        IconSource::Svg(icon.path().into())
    }
}

impl From<AppIcon> for gpui_component::Icon {
    fn from(icon: AppIcon) -> Self {
        gpui_component::Icon::default().path(icon.path())
    }
}
