use dbflux_core::Icon;

/// App-specific icons embedded from resources/icons/
///
/// This enum centralizes all SVG icon references used throughout DBFlux.
/// Icons are loaded via GPUI's AssetSource using the `path()` method.
///
/// Usage:
/// ```rust,ignore
/// svg().path(AppIcon::Folder.path()).size_4().text_color(theme.foreground)
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
    CircleAlert,
    CircleCheck,
    TriangleAlert,
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
    Maximize2,
    Minimize2,
    PanelBottomClose,
    PanelBottomOpen,
    FileSpreadsheet,
    KeyRound,
    Link2,
    CaseSensitive,
    ScrollText,

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

    // Database generic
    Database,

    // Database brands (SimpleIcons)
    BrandPostgres,
    BrandMysql,
    BrandMariadb,
    BrandSqlite,
    BrandMongodb,
    BrandRedis,

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
            Self::CircleAlert => "icons/ui/circle-alert.svg",
            Self::CircleCheck => "icons/ui/circle-check.svg",
            Self::TriangleAlert => "icons/ui/triangle-alert.svg",
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
            Self::Maximize2 => "icons/ui/maximize-2.svg",
            Self::Minimize2 => "icons/ui/minimize-2.svg",
            Self::PanelBottomClose => "icons/ui/panel-bottom-close.svg",
            Self::PanelBottomOpen => "icons/ui/panel-bottom-open.svg",
            Self::FileSpreadsheet => "icons/ui/file-spreadsheet.svg",
            Self::KeyRound => "icons/ui/key-round.svg",
            Self::Link2 => "icons/ui/link-2.svg",
            Self::CaseSensitive => "icons/ui/case-sensitive.svg",
            Self::ScrollText => "icons/ui/scroll-text.svg",
            Self::Plug => "icons/ui/plug.svg",
            Self::Unplug => "icons/ui/unplug.svg",
            Self::Server => "icons/ui/server.svg",
            Self::HardDrive => "icons/ui/hard-drive.svg",
            Self::FileCode => "icons/ui/file-code-corner.svg",
            Self::Folder => "icons/ui/folder.svg",
            Self::Box => "icons/ui/box.svg",
            Self::Braces => "icons/ui/braces.svg",
            Self::SquareTerminal => "icons/ui/square-terminal.svg",
            Self::Database => "icons/ui/database.svg",
            Self::BrandPostgres => "icons/brand/postgresql.svg",
            Self::BrandMysql => "icons/brand/mysql.svg",
            Self::BrandMariadb => "icons/brand/mariadb.svg",
            Self::BrandSqlite => "icons/brand/sqlite.svg",
            Self::BrandMongodb => "icons/brand/mongodb.svg",
            Self::BrandRedis => "icons/brand/redis.svg",
            Self::DbFlux => "icons/dbflux.svg",
        }
    }

    /// Maps a core Icon to the corresponding AppIcon.
    ///
    /// This is the preferred way to get database brand icons from driver metadata.
    pub const fn from_icon(icon: Icon) -> Self {
        match icon {
            Icon::Postgres => Self::BrandPostgres,
            Icon::Mysql => Self::BrandMysql,
            Icon::Mariadb => Self::BrandMariadb,
            Icon::Sqlite => Self::BrandSqlite,
            Icon::Mongodb => Self::BrandMongodb,
            Icon::Redis => Self::BrandRedis,
            Icon::Database => Self::Database,
        }
    }
}
