use gpui::{AssetSource, SharedString};
use std::borrow::Cow;

use crate::ui::icons::{ALL_ICONS, embedded_bytes};

pub struct Assets;

impl AssetSource for Assets {
    fn load(&self, path: &str) -> gpui::Result<Option<Cow<'static, [u8]>>> {
        // Full-color brand marks, served for `img(...)`. Pre-rendered PNGs are
        // used rather than the SVG because gpui's `img` SVG path does not apply
        // the display scale factor, so a runtime-rasterized SVG looks blurry on
        // HiDPI; a high-resolution PNG downscales crisply.
        match path {
            "branding/stable/mark-256.png" => {
                return Ok(Some(Cow::Borrowed(include_bytes!(
                    "../../../resources/branding/stable/mark-256.png"
                ))));
            }
            "branding/nightly/mark-256.png" => {
                return Ok(Some(Cow::Borrowed(include_bytes!(
                    "../../../resources/branding/nightly/mark-256.png"
                ))));
            }
            _ => {}
        }

        if let Some(icon) = ALL_ICONS.iter().find(|icon| icon.path() == path) {
            return Ok(Some(Cow::Borrowed(embedded_bytes(*icon))));
        }

        // gpui_component icons resolve via paths like "icons/<file>.svg" without
        // the "ui/" namespace we use locally. Map those onto our "icons/ui/" set
        // so IconName::* (chevrons, loader, circle-x, etc.) renders correctly.
        if let Some(rest) = path.strip_prefix("icons/")
            && !rest.starts_with("ui/")
            && !rest.starts_with("brand/")
        {
            let aliased = format!("icons/ui/{rest}");
            if let Some(icon) = ALL_ICONS.iter().find(|icon| icon.path() == aliased) {
                return Ok(Some(Cow::Borrowed(embedded_bytes(*icon))));
            }
        }

        Ok(None)
    }

    fn list(&self, path: &str) -> gpui::Result<Vec<SharedString>> {
        let entries: Vec<SharedString> = ALL_ICONS
            .iter()
            .filter(|icon| {
                let p = icon.path();
                if let Some(dir) = p.rfind('/') {
                    let parent = &p[..dir];
                    let trimmed = path.trim_end_matches('/');
                    parent == trimmed
                } else {
                    false
                }
            })
            .map(|icon| SharedString::from(icon.path()))
            .collect();

        Ok(entries)
    }
}
