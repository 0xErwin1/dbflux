pub use dbflux_components::icons::AppIcon;

/// Declares every embedded icon once, producing both `ALL_ICONS` (the list the
/// asset source searches by path) and `embedded_bytes`.
///
/// Generating both from one table keeps them in lockstep: `embedded_bytes` is an
/// exhaustive match, so adding an `AppIcon` variant without an entry here fails
/// to compile instead of silently rendering an empty glyph at runtime.
///
/// File paths are relative to the workspace `resources/` directory.
macro_rules! embedded_icons {
    ($($icon:ident => $file:literal),* $(,)?) => {
        pub const ALL_ICONS: &[AppIcon] = &[$(AppIcon::$icon),*];

        /// Returns the embedded bytes for the given icon.
        pub(crate) fn embedded_bytes(icon: AppIcon) -> &'static [u8] {
            match icon {
                $(AppIcon::$icon => include_bytes!(concat!("../../../../../resources/", $file)),)*
            }
        }
    };
}

embedded_icons! {
    ChevronDown => "icons/ui/chevron-down.svg",
    ChevronLeft => "icons/ui/chevron-left.svg",
    ChevronRight => "icons/ui/chevron-right.svg",
    ChevronUp => "icons/ui/chevron-up.svg",
    Play => "icons/ui/play.svg",
    SquarePlay => "icons/ui/square-play.svg",
    Plus => "icons/ui/plus.svg",
    Power => "icons/ui/power.svg",
    Save => "icons/ui/save.svg",
    Delete => "icons/ui/delete.svg",
    Pencil => "icons/ui/pencil.svg",
    Copy => "icons/ui/copy.svg",
    RefreshCcw => "icons/ui/refresh-ccw.svg",
    RotateCcw => "icons/ui/rotate-ccw.svg",
    Download => "icons/ui/download.svg",
    Search => "icons/ui/search.svg",
    Settings => "icons/ui/settings.svg",
    History => "icons/ui/history.svg",
    Undo => "icons/ui/undo.svg",
    Redo => "icons/ui/redo.svg",
    X => "icons/ui/x.svg",
    Eye => "icons/ui/eye.svg",
    EyeOff => "icons/ui/eye-off.svg",
    Loader => "icons/ui/loader.svg",
    Info => "icons/ui/info.svg",
    CircleAlert => "icons/ui/circle-alert.svg",
    CircleCheck => "icons/ui/circle-check.svg",
    CircleX => "icons/ui/circle-x.svg",
    Check => "icons/ui/check.svg",
    ExternalLink => "icons/ui/external-link.svg",
    Globe => "icons/ui/globe.svg",
    TriangleAlert => "icons/ui/triangle-alert.svg",
    Code => "icons/ui/code.svg",
    Table => "icons/ui/table.svg",
    Columns => "icons/ui/columns.svg",
    Rows3 => "icons/ui/rows-3.svg",
    ArrowUp => "icons/ui/arrow-up.svg",
    ArrowDown => "icons/ui/arrow-down.svg",
    Star => "icons/ui/star.svg",
    Clock => "icons/ui/clock.svg",
    Zap => "icons/ui/zap.svg",
    Hash => "icons/ui/hash.svg",
    Lock => "icons/ui/lock.svg",
    Layers => "icons/ui/layers.svg",
    Keyboard => "icons/ui/keyboard.svg",
    FingerprintPattern => "icons/ui/fingerprint-pattern.svg",
    Maximize2 => "icons/ui/maximize-2.svg",
    Minimize2 => "icons/ui/minimize-2.svg",
    PanelBottomClose => "icons/ui/panel-bottom-close.svg",
    PanelBottomOpen => "icons/ui/panel-bottom-open.svg",
    FileSpreadsheet => "icons/ui/file-spreadsheet.svg",
    KeyRound => "icons/ui/key-round.svg",
    Cable => "icons/ui/cable.svg",
    Link2 => "icons/ui/link-2.svg",
    CaseSensitive => "icons/ui/case-sensitive.svg",
    ScrollText => "icons/ui/scroll-text.svg",
    ListFilter => "icons/ui/list-filter.svg",
    Tag => "icons/ui/tag.svg",
    Activity => "icons/ui/activity.svg",
    FileDown => "icons/ui/file-down.svg",
    SquareFunction => "icons/ui/square-function.svg",
    ArrowUpDown => "icons/ui/arrow-up-down.svg",
    Plug => "icons/ui/plug.svg",
    Unplug => "icons/ui/unplug.svg",
    Server => "icons/ui/server.svg",
    HardDrive => "icons/ui/hard-drive.svg",
    File => "icons/ui/file.svg",
    FileCode => "icons/ui/file-code-corner.svg",
    Image => "icons/ui/image.svg",
    Folder => "icons/ui/folder.svg",
    Box => "icons/ui/box.svg",
    Boxes => "icons/ui/boxes.svg",
    Braces => "icons/ui/braces.svg",
    SquareTerminal => "icons/ui/square-terminal.svg",
    Parentheses => "icons/ui/parentheses.svg",
    Sigma => "icons/ui/sigma.svg",
    Database => "icons/ui/database.svg",
    DatabaseZap => "icons/ui/database-zap.svg",
    ZoomIn => "icons/ui/zoom-in.svg",
    ZoomOut => "icons/ui/zoom-out.svg",
    Minus => "icons/ui/minus.svg",
    Grid3x3 => "icons/ui/grid-3x3.svg",
    Snowflake => "icons/ui/snowflake.svg",
    Scale => "icons/ui/scale.svg",
    ArrowLeftRight => "icons/ui/arrow-left-right.svg",
    Clipboard => "icons/ui/clipboard.svg",
    Pin => "icons/ui/pin.svg",
    Logs => "icons/ui/logs.svg",
    ChartSpline => "icons/ui/chart-spline.svg",
    ChartArea => "icons/ui/chart-area.svg",
    ChartColumnBig => "icons/ui/chart-column-big.svg",
    ChartNoAxesColumn => "icons/ui/chart-no-axes-column.svg",
    ChartBar => "icons/ui/chart-bar.svg",
    ChartPie => "icons/ui/chart-pie.svg",
    ChartNetwork => "icons/ui/chart-network.svg",
    BrandPostgres => "icons/brand/postgresql.svg",
    BrandMysql => "icons/brand/mysql.svg",
    BrandMariadb => "icons/brand/mariadb.svg",
    BrandSqlite => "icons/brand/sqlite.svg",
    BrandMongodb => "icons/brand/mongodb.svg",
    BrandRedis => "icons/brand/redis.svg",
    BrandClickhouse => "icons/brand/clickhouse.svg",
    BrandTurso => "icons/brand/turso.svg",
    BrandLua => "icons/brand/lua.svg",
    BrandPython => "icons/brand/python.svg",
    BrandBash => "icons/brand/gnubash.svg",
    BrandJavaScript => "icons/brand/javascript.svg",
    BrandInfluxDb => "icons/brand/influxdb.svg",
    DbFlux => "branding/glyph.svg",
    BrainCircuit => "icons/ui/brain-circuit.svg",
    Bot => "icons/ui/bot.svg",
    Bell => "icons/ui/bell.svg",
}

#[cfg(test)]
mod tests {
    use super::{ALL_ICONS, AppIcon, embedded_bytes};

    #[test]
    fn semantic_driver_fallback_assets_are_registered() {
        for icon in [
            AppIcon::ChartNoAxesColumn,
            AppIcon::Boxes,
            AppIcon::BrandClickhouse,
            AppIcon::BrandTurso,
        ] {
            assert!(ALL_ICONS.contains(&icon));
            assert!(embedded_bytes(icon).starts_with(b"<svg"));
        }
    }

    #[test]
    fn schema_toolbar_icons_are_registered() {
        for icon in [
            AppIcon::ZoomIn,
            AppIcon::ZoomOut,
            AppIcon::Grid3x3,
            AppIcon::Minus,
            AppIcon::Snowflake,
            AppIcon::DatabaseZap,
        ] {
            assert!(ALL_ICONS.contains(&icon), "{icon:?} is not registered");
        }
    }

    #[test]
    fn every_registered_icon_has_a_unique_path_and_svg_bytes() {
        let mut paths = std::collections::HashSet::new();

        for icon in ALL_ICONS {
            assert!(paths.insert(icon.path()), "duplicate path for {icon:?}");

            let bytes = embedded_bytes(*icon);
            let has_svg_root = bytes.windows(4).any(|window| window == b"<svg");
            assert!(has_svg_root, "{icon:?} does not embed an SVG document");
        }
    }
}
