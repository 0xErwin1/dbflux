use gpui::{AssetSource, SharedString};
use std::borrow::Cow;

use crate::ui::icons::ALL_ICONS;

pub struct Assets;

impl AssetSource for Assets {
    fn load(&self, path: &str) -> gpui::Result<Option<Cow<'static, [u8]>>> {
        let bytes = ALL_ICONS
            .iter()
            .find(|icon| icon.path() == path)
            .map(|icon| icon.embedded_bytes());

        Ok(bytes.map(Cow::Borrowed))
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
