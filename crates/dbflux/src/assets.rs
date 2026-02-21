use gpui::{AssetSource, SharedString};
use std::borrow::Cow;

/// Embedded assets for DBFlux application.
///
/// This struct implements GPUI's AssetSource trait to provide embedded SVG icons
/// and other assets. Icons are embedded at compile time using `include_bytes!`.
pub struct Assets;

impl AssetSource for Assets {
    fn load(&self, path: &str) -> gpui::Result<Option<Cow<'static, [u8]>>> {
        // Map asset paths to embedded bytes
        let bytes: Option<&'static [u8]> = match path {
            // UI icons (Lucide)
            "icons/ui/folder.svg" => Some(include_bytes!("../../../resources/icons/ui/folder.svg")),
            "icons/ui/database.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/database.svg"))
            }
            "icons/ui/chevron-down.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/chevron-down.svg"
            )),
            "icons/ui/chevron-right.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/chevron-right.svg"
            )),
            "icons/ui/chevron-up.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/chevron-up.svg"))
            }
            "icons/ui/chevron-left.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/chevron-left.svg"
            )),
            "icons/ui/plus.svg" => Some(include_bytes!("../../../resources/icons/ui/plus.svg")),
            "icons/ui/square-terminal.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/square-terminal.svg"
            )),
            "icons/ui/server.svg" => Some(include_bytes!("../../../resources/icons/ui/server.svg")),
            "icons/ui/hard-drive.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/hard-drive.svg"))
            }
            "icons/ui/braces.svg" => Some(include_bytes!("../../../resources/icons/ui/braces.svg")),
            "icons/ui/box.svg" => Some(include_bytes!("../../../resources/icons/ui/box.svg")),
            "icons/ui/settings.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/settings.svg"))
            }
            "icons/ui/search.svg" => Some(include_bytes!("../../../resources/icons/ui/search.svg")),
            "icons/ui/eye.svg" => Some(include_bytes!("../../../resources/icons/ui/eye.svg")),
            "icons/ui/eye-off.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/eye-off.svg"))
            }
            "icons/ui/loader.svg" => Some(include_bytes!("../../../resources/icons/ui/loader.svg")),
            "icons/ui/download.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/download.svg"))
            }
            "icons/ui/x.svg" => Some(include_bytes!("../../../resources/icons/ui/x.svg")),
            "icons/ui/history.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/history.svg"))
            }
            "icons/ui/undo.svg" => Some(include_bytes!("../../../resources/icons/ui/undo.svg")),
            "icons/ui/redo.svg" => Some(include_bytes!("../../../resources/icons/ui/redo.svg")),
            "icons/ui/info.svg" => Some(include_bytes!("../../../resources/icons/ui/info.svg")),
            "icons/ui/circle-alert.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/circle-alert.svg"
            )),
            "icons/ui/triangle-alert.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/triangle-alert.svg"
            )),
            "icons/ui/plug.svg" => Some(include_bytes!("../../../resources/icons/ui/plug.svg")),
            "icons/ui/unplug.svg" => Some(include_bytes!("../../../resources/icons/ui/unplug.svg")),
            "icons/ui/fingerprint-pattern.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/fingerprint-pattern.svg"
            )),
            "icons/ui/keyboard.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/keyboard.svg"))
            }
            "icons/ui/layers.svg" => Some(include_bytes!("../../../resources/icons/ui/layers.svg")),
            "icons/ui/table.svg" => Some(include_bytes!("../../../resources/icons/ui/table.svg")),
            "icons/ui/columns.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/columns.svg"))
            }
            "icons/ui/hash.svg" => Some(include_bytes!("../../../resources/icons/ui/hash.svg")),
            "icons/ui/key-round.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/key-round.svg"))
            }
            "icons/ui/lock.svg" => Some(include_bytes!("../../../resources/icons/ui/lock.svg")),
            "icons/ui/code.svg" => Some(include_bytes!("../../../resources/icons/ui/code.svg")),
            "icons/ui/zap.svg" => Some(include_bytes!("../../../resources/icons/ui/zap.svg")),
            "icons/ui/clock.svg" => Some(include_bytes!("../../../resources/icons/ui/clock.svg")),
            "icons/ui/power.svg" => Some(include_bytes!("../../../resources/icons/ui/power.svg")),
            "icons/ui/star.svg" => Some(include_bytes!("../../../resources/icons/ui/star.svg")),
            "icons/ui/pencil.svg" => Some(include_bytes!("../../../resources/icons/ui/pencil.svg")),
            "icons/ui/delete.svg" => Some(include_bytes!("../../../resources/icons/ui/delete.svg")),
            "icons/ui/refresh-ccw.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/refresh-ccw.svg"
            )),
            "icons/ui/rows-3.svg" => Some(include_bytes!("../../../resources/icons/ui/rows-3.svg")),
            "icons/ui/arrow-up.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/arrow-up.svg"))
            }
            "icons/ui/arrow-down.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/arrow-down.svg"))
            }
            "icons/ui/play.svg" => Some(include_bytes!("../../../resources/icons/ui/play.svg")),
            "icons/ui/square-play.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/square-play.svg"
            )),
            "icons/ui/save.svg" => Some(include_bytes!("../../../resources/icons/ui/save.svg")),
            "icons/ui/maximize-2.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/maximize-2.svg"))
            }
            "icons/ui/minimize-2.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/minimize-2.svg"))
            }
            "icons/ui/panel-bottom-close.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/panel-bottom-close.svg"
            )),
            "icons/ui/panel-bottom-open.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/panel-bottom-open.svg"
            )),
            "icons/ui/file-spreadsheet.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/file-spreadsheet.svg"
            )),
            "icons/ui/case-sensitive.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/case-sensitive.svg"
            )),
            "icons/ui/scroll-text.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/scroll-text.svg"
            )),
            "icons/ui/circle-check.svg" => Some(include_bytes!(
                "../../../resources/icons/ui/circle-check.svg"
            )),
            "icons/ui/link-2.svg" => Some(include_bytes!("../../../resources/icons/ui/link-2.svg")),
            "icons/ui/rotate-ccw.svg" => {
                Some(include_bytes!("../../../resources/icons/ui/rotate-ccw.svg"))
            }

            // App icon
            "icons/dbflux.svg" => Some(include_bytes!("../../../resources/icons/dbflux.svg")),

            // Database brand icons (SimpleIcons)
            "icons/brand/postgresql.svg" => Some(include_bytes!(
                "../../../resources/icons/brand/postgresql.svg"
            )),
            "icons/brand/mysql.svg" => {
                Some(include_bytes!("../../../resources/icons/brand/mysql.svg"))
            }
            "icons/brand/mariadb.svg" => {
                Some(include_bytes!("../../../resources/icons/brand/mariadb.svg"))
            }
            "icons/brand/sqlite.svg" => {
                Some(include_bytes!("../../../resources/icons/brand/sqlite.svg"))
            }
            "icons/brand/mongodb.svg" => {
                Some(include_bytes!("../../../resources/icons/brand/mongodb.svg"))
            }
            "icons/brand/redis.svg" => {
                Some(include_bytes!("../../../resources/icons/brand/redis.svg"))
            }

            // Unknown path
            _ => None,
        };

        Ok(bytes.map(Cow::Borrowed))
    }

    fn list(&self, path: &str) -> gpui::Result<Vec<SharedString>> {
        match path {
            "icons/ui" | "icons/ui/" => Ok(vec![
                "icons/ui/folder.svg".into(),
                "icons/ui/database.svg".into(),
                "icons/ui/chevron-down.svg".into(),
                "icons/ui/chevron-right.svg".into(),
                "icons/ui/chevron-up.svg".into(),
                "icons/ui/chevron-left.svg".into(),
                "icons/ui/plus.svg".into(),
                "icons/ui/square-terminal.svg".into(),
                "icons/ui/server.svg".into(),
                "icons/ui/hard-drive.svg".into(),
                "icons/ui/braces.svg".into(),
                "icons/ui/box.svg".into(),
                "icons/ui/settings.svg".into(),
                "icons/ui/search.svg".into(),
                "icons/ui/eye.svg".into(),
                "icons/ui/eye-off.svg".into(),
                "icons/ui/loader.svg".into(),
                "icons/ui/download.svg".into(),
                "icons/ui/x.svg".into(),
                "icons/ui/history.svg".into(),
                "icons/ui/undo.svg".into(),
                "icons/ui/redo.svg".into(),
                "icons/ui/info.svg".into(),
                "icons/ui/circle-alert.svg".into(),
                "icons/ui/triangle-alert.svg".into(),
                "icons/ui/plug.svg".into(),
                "icons/ui/unplug.svg".into(),
                "icons/ui/fingerprint-pattern.svg".into(),
                "icons/ui/keyboard.svg".into(),
                "icons/ui/layers.svg".into(),
                "icons/ui/table.svg".into(),
                "icons/ui/columns.svg".into(),
                "icons/ui/hash.svg".into(),
                "icons/ui/lock.svg".into(),
                "icons/ui/code.svg".into(),
                "icons/ui/zap.svg".into(),
                "icons/ui/clock.svg".into(),
                "icons/ui/power.svg".into(),
                "icons/ui/star.svg".into(),
                "icons/ui/pencil.svg".into(),
                "icons/ui/delete.svg".into(),
                "icons/ui/refresh-ccw.svg".into(),
                "icons/ui/rows-3.svg".into(),
                "icons/ui/arrow-up.svg".into(),
                "icons/ui/arrow-down.svg".into(),
                "icons/ui/play.svg".into(),
                "icons/ui/square-play.svg".into(),
                "icons/ui/save.svg".into(),
                "icons/ui/maximize-2.svg".into(),
                "icons/ui/minimize-2.svg".into(),
                "icons/ui/panel-bottom-close.svg".into(),
                "icons/ui/panel-bottom-open.svg".into(),
                "icons/ui/file-spreadsheet.svg".into(),
                "icons/ui/case-sensitive.svg".into(),
                "icons/ui/scroll-text.svg".into(),
                "icons/ui/circle-check.svg".into(),
                "icons/ui/link-2.svg".into(),
                "icons/ui/rotate-ccw.svg".into(),
            ]),
            "icons/brand" | "icons/brand/" => Ok(vec![
                "icons/brand/postgresql.svg".into(),
                "icons/brand/mysql.svg".into(),
                "icons/brand/mariadb.svg".into(),
                "icons/brand/sqlite.svg".into(),
                "icons/brand/mongodb.svg".into(),
                "icons/brand/redis.svg".into(),
            ]),
            _ => Ok(vec![]),
        }
    }
}
