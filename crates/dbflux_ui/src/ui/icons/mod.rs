pub use dbflux_components::icons::AppIcon;

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

/// Returns the embedded bytes for the given icon.
///
/// The `include_bytes!` paths are relative to this source file, which stays at
/// `crates/dbflux_ui/src/ui/icons/mod.rs`. They must not change when `AppIcon`
/// moves to `dbflux_components` because the icon resources live under
/// `crates/dbflux_ui/resources/`.
pub(crate) fn embedded_bytes(icon: AppIcon) -> &'static [u8] {
    match icon {
        AppIcon::ChevronDown => {
            include_bytes!("../../../../../resources/icons/ui/chevron-down.svg")
        }
        AppIcon::ChevronLeft => {
            include_bytes!("../../../../../resources/icons/ui/chevron-left.svg")
        }
        AppIcon::ChevronRight => {
            include_bytes!("../../../../../resources/icons/ui/chevron-right.svg")
        }
        AppIcon::ChevronUp => include_bytes!("../../../../../resources/icons/ui/chevron-up.svg"),
        AppIcon::Play => include_bytes!("../../../../../resources/icons/ui/play.svg"),
        AppIcon::SquarePlay => include_bytes!("../../../../../resources/icons/ui/square-play.svg"),
        AppIcon::Plus => include_bytes!("../../../../../resources/icons/ui/plus.svg"),
        AppIcon::Power => include_bytes!("../../../../../resources/icons/ui/power.svg"),
        AppIcon::Save => include_bytes!("../../../../../resources/icons/ui/save.svg"),
        AppIcon::Delete => include_bytes!("../../../../../resources/icons/ui/delete.svg"),
        AppIcon::Pencil => include_bytes!("../../../../../resources/icons/ui/pencil.svg"),
        AppIcon::Copy => include_bytes!("../../../../../resources/icons/ui/copy.svg"),
        AppIcon::RefreshCcw => include_bytes!("../../../../../resources/icons/ui/refresh-ccw.svg"),
        AppIcon::RotateCcw => include_bytes!("../../../../../resources/icons/ui/rotate-ccw.svg"),
        AppIcon::Download => include_bytes!("../../../../../resources/icons/ui/download.svg"),
        AppIcon::Search => include_bytes!("../../../../../resources/icons/ui/search.svg"),
        AppIcon::Settings => include_bytes!("../../../../../resources/icons/ui/settings.svg"),
        AppIcon::History => include_bytes!("../../../../../resources/icons/ui/history.svg"),
        AppIcon::Undo => include_bytes!("../../../../../resources/icons/ui/undo.svg"),
        AppIcon::Redo => include_bytes!("../../../../../resources/icons/ui/redo.svg"),
        AppIcon::X => include_bytes!("../../../../../resources/icons/ui/x.svg"),
        AppIcon::Eye => include_bytes!("../../../../../resources/icons/ui/eye.svg"),
        AppIcon::EyeOff => include_bytes!("../../../../../resources/icons/ui/eye-off.svg"),
        AppIcon::Loader => include_bytes!("../../../../../resources/icons/ui/loader.svg"),
        AppIcon::Info => include_bytes!("../../../../../resources/icons/ui/info.svg"),
        AppIcon::CircleAlert => {
            include_bytes!("../../../../../resources/icons/ui/circle-alert.svg")
        }
        AppIcon::CircleCheck => {
            include_bytes!("../../../../../resources/icons/ui/circle-check.svg")
        }
        AppIcon::CircleX => include_bytes!("../../../../../resources/icons/ui/circle-x.svg"),
        AppIcon::Check => include_bytes!("../../../../../resources/icons/ui/check.svg"),
        AppIcon::ExternalLink => {
            include_bytes!("../../../../../resources/icons/ui/external-link.svg")
        }
        AppIcon::Globe => include_bytes!("../../../../../resources/icons/ui/globe.svg"),
        AppIcon::TriangleAlert => {
            include_bytes!("../../../../../resources/icons/ui/triangle-alert.svg")
        }
        AppIcon::Code => include_bytes!("../../../../../resources/icons/ui/code.svg"),
        AppIcon::Table => include_bytes!("../../../../../resources/icons/ui/table.svg"),
        AppIcon::Columns => include_bytes!("../../../../../resources/icons/ui/columns.svg"),
        AppIcon::Rows3 => include_bytes!("../../../../../resources/icons/ui/rows-3.svg"),
        AppIcon::ArrowUp => include_bytes!("../../../../../resources/icons/ui/arrow-up.svg"),
        AppIcon::ArrowDown => include_bytes!("../../../../../resources/icons/ui/arrow-down.svg"),
        AppIcon::Star => include_bytes!("../../../../../resources/icons/ui/star.svg"),
        AppIcon::Clock => include_bytes!("../../../../../resources/icons/ui/clock.svg"),
        AppIcon::Zap => include_bytes!("../../../../../resources/icons/ui/zap.svg"),
        AppIcon::Hash => include_bytes!("../../../../../resources/icons/ui/hash.svg"),
        AppIcon::Lock => include_bytes!("../../../../../resources/icons/ui/lock.svg"),
        AppIcon::Layers => include_bytes!("../../../../../resources/icons/ui/layers.svg"),
        AppIcon::Keyboard => include_bytes!("../../../../../resources/icons/ui/keyboard.svg"),
        AppIcon::FingerprintPattern => {
            include_bytes!("../../../../../resources/icons/ui/fingerprint-pattern.svg")
        }
        AppIcon::Maximize2 => include_bytes!("../../../../../resources/icons/ui/maximize-2.svg"),
        AppIcon::Minimize2 => include_bytes!("../../../../../resources/icons/ui/minimize-2.svg"),
        AppIcon::PanelBottomClose => {
            include_bytes!("../../../../../resources/icons/ui/panel-bottom-close.svg")
        }
        AppIcon::PanelBottomOpen => {
            include_bytes!("../../../../../resources/icons/ui/panel-bottom-open.svg")
        }
        AppIcon::FileSpreadsheet => {
            include_bytes!("../../../../../resources/icons/ui/file-spreadsheet.svg")
        }
        AppIcon::KeyRound => include_bytes!("../../../../../resources/icons/ui/key-round.svg"),
        AppIcon::Link2 => include_bytes!("../../../../../resources/icons/ui/link-2.svg"),
        AppIcon::CaseSensitive => {
            include_bytes!("../../../../../resources/icons/ui/case-sensitive.svg")
        }
        AppIcon::ScrollText => include_bytes!("../../../../../resources/icons/ui/scroll-text.svg"),
        AppIcon::ListFilter => include_bytes!("../../../../../resources/icons/ui/list-filter.svg"),
        AppIcon::ArrowUpDown => {
            include_bytes!("../../../../../resources/icons/ui/arrow-up-down.svg")
        }
        AppIcon::Plug => include_bytes!("../../../../../resources/icons/ui/plug.svg"),
        AppIcon::Unplug => include_bytes!("../../../../../resources/icons/ui/unplug.svg"),
        AppIcon::Server => include_bytes!("../../../../../resources/icons/ui/server.svg"),
        AppIcon::HardDrive => include_bytes!("../../../../../resources/icons/ui/hard-drive.svg"),
        AppIcon::FileCode => {
            include_bytes!("../../../../../resources/icons/ui/file-code-corner.svg")
        }
        AppIcon::Folder => include_bytes!("../../../../../resources/icons/ui/folder.svg"),
        AppIcon::Box => include_bytes!("../../../../../resources/icons/ui/box.svg"),
        AppIcon::Braces => include_bytes!("../../../../../resources/icons/ui/braces.svg"),
        AppIcon::SquareTerminal => {
            include_bytes!("../../../../../resources/icons/ui/square-terminal.svg")
        }
        AppIcon::Parentheses => {
            include_bytes!("../../../../../resources/icons/ui/parentheses.svg")
        }
        AppIcon::Sigma => include_bytes!("../../../../../resources/icons/ui/sigma.svg"),
        AppIcon::Database => include_bytes!("../../../../../resources/icons/ui/database.svg"),
        AppIcon::Logs => include_bytes!("../../../../../resources/icons/ui/logs.svg"),
        AppIcon::ChartSpline => {
            include_bytes!("../../../../../resources/icons/ui/chart-spline.svg")
        }
        AppIcon::ChartArea => include_bytes!("../../../../../resources/icons/ui/chart-area.svg"),
        AppIcon::ChartColumnBig => {
            include_bytes!("../../../../../resources/icons/ui/chart-column-big.svg")
        }
        AppIcon::ChartBar => include_bytes!("../../../../../resources/icons/ui/chart-bar.svg"),
        AppIcon::ChartPie => include_bytes!("../../../../../resources/icons/ui/chart-pie.svg"),
        AppIcon::ChartNetwork => {
            include_bytes!("../../../../../resources/icons/ui/chart-network.svg")
        }
        AppIcon::BrandPostgres => {
            include_bytes!("../../../../../resources/icons/brand/postgresql.svg")
        }
        AppIcon::BrandMysql => include_bytes!("../../../../../resources/icons/brand/mysql.svg"),
        AppIcon::BrandMariadb => {
            include_bytes!("../../../../../resources/icons/brand/mariadb.svg")
        }
        AppIcon::BrandSqlite => include_bytes!("../../../../../resources/icons/brand/sqlite.svg"),
        AppIcon::BrandMongodb => {
            include_bytes!("../../../../../resources/icons/brand/mongodb.svg")
        }
        AppIcon::BrandRedis => include_bytes!("../../../../../resources/icons/brand/redis.svg"),
        AppIcon::BrandLua => include_bytes!("../../../../../resources/icons/brand/lua.svg"),
        AppIcon::BrandPython => include_bytes!("../../../../../resources/icons/brand/python.svg"),
        AppIcon::BrandBash => include_bytes!("../../../../../resources/icons/brand/gnubash.svg"),
        AppIcon::BrandJavaScript => {
            include_bytes!("../../../../../resources/icons/brand/javascript.svg")
        }
        AppIcon::BrandInfluxDb => {
            include_bytes!("../../../../../resources/icons/brand/influxdb.svg")
        }
        AppIcon::DbFlux => dbflux_mark_bytes(),
        AppIcon::BrainCircuit => {
            include_bytes!("../../../../../resources/icons/ui/brain-circuit.svg")
        }
        AppIcon::Bot => include_bytes!("../../../../../resources/icons/ui/bot.svg"),
    }
}

/// Returns the DBFlux brand mark for the running release channel. Nightly ships
/// a visually distinct mark so it can be told apart from stable at a glance.
fn dbflux_mark_bytes() -> &'static [u8] {
    match dbflux_core::ReleaseChannel::current() {
        dbflux_core::ReleaseChannel::Nightly => {
            include_bytes!("../../../../../resources/branding/nightly/mark.svg")
        }
        dbflux_core::ReleaseChannel::Stable | dbflux_core::ReleaseChannel::Rc => {
            include_bytes!("../../../../../resources/branding/stable/mark.svg")
        }
    }
}
