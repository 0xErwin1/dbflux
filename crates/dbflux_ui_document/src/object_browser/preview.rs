//! Preview pane for `ObjectBrowserDocument`.
//!
//! Layout, top to bottom: header (file-type icon, object name, close), the
//! preview body — which for now only reports why an object cannot be
//! previewed — the object metadata section, and the action bar. Preview
//! *content* (image, text editor, PDF fallback) lands with the preview tasks;
//! this file owns the chrome and the metadata rows around it.

use super::metadata::{
    ObjectMetadataState, ObjectVersionsState, PreviewGate, format_size_detail, short_version_id,
    versioning_tracks_history,
};
use super::render::{format_modified, object_icon};
use super::{ObjectAction, ObjectBrowserDocument};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::ObjectVersionSummary;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Width of the preview pane when a selection is being previewed.
pub(super) const PREVIEW_WIDTH: Pixels = px(320.0);

/// Label column of the metadata rows. Narrow enough to leave the values room
/// inside a 320 px pane.
const METADATA_LABEL_WIDTH: Pixels = px(92.0);

const UNKNOWN: &str = "—";

impl ObjectBrowserDocument {
    pub(super) fn render_preview_pane(
        &self,
        key: &str,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .w(PREVIEW_WIDTH)
            .flex()
            .flex_col()
            .border_l_1()
            .border_color(theme.border)
            .bg(theme.background)
            .child(self.render_preview_header(key, cx))
            .child(
                div()
                    .flex_1()
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .child(self.render_preview_body(cx))
                    .child(self.render_metadata_section(key, cx)),
            )
            .child(self.render_preview_actions(key, cx))
    }

    fn render_preview_header(&self, key: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let name = object_display_name(key);

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .flex_1()
                    .items_center()
                    .gap(Spacing::XS)
                    .overflow_hidden()
                    .child(Icon::new(object_icon(name)).small().muted())
                    .child(
                        div()
                            .overflow_hidden()
                            .text_ellipsis()
                            .whitespace_nowrap()
                            .child(Text::code(name.to_string())),
                    ),
            )
            .child(
                div()
                    .id("object-browser-preview-close")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size(Heights::CONTROL)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.close_preview(cx);
                    }))
                    .child(Icon::new(AppIcon::X).small().muted()),
            )
    }

    /// Body area above the metadata rows: the loading/error state of the
    /// `head_object` call, or the reason the object's bytes were not fetched.
    fn render_preview_body(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();

        let (icon, message, is_error) = match &self.metadata {
            None | Some(ObjectMetadataState::Loading) => {
                (AppIcon::Loader, "Loading metadata…".to_string(), false)
            }
            Some(ObjectMetadataState::Error(message)) => {
                (AppIcon::TriangleAlert, message.clone(), true)
            }
            Some(ObjectMetadataState::Loaded { gate, .. }) => match gate {
                PreviewGate::Allowed => (AppIcon::Eye, "No preview".to_string(), false),
                PreviewGate::Archived => (AppIcon::Lock, gate.message().unwrap_or_default(), false),
                PreviewGate::TooLarge { .. } => (
                    AppIcon::TriangleAlert,
                    gate.message().unwrap_or_default(),
                    false,
                ),
            },
        };

        let archived = matches!(
            &self.metadata,
            Some(ObjectMetadataState::Loaded {
                gate: PreviewGate::Archived,
                ..
            })
        );

        div()
            .flex_1()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(Spacing::SM)
            .p(Spacing::MD)
            .bg(theme.secondary)
            .child(if is_error {
                Icon::new(icon).size(Heights::ICON_LG).danger()
            } else if archived {
                Icon::new(icon).size(Heights::ICON_LG).warning()
            } else {
                Icon::new(icon).size(Heights::ICON_LG).muted()
            })
            .child(if is_error {
                Text::caption(message).danger()
            } else {
                Text::caption(message).muted_foreground()
            })
            .into_any_element()
    }

    /// Object metadata section (S3-3's "Object" block): one key/value row per
    /// field, with the ETag dimmed and versions fetched only on request.
    fn render_metadata_section(&self, key: &str, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        let Some(ObjectMetadataState::Loaded { metadata, gate: _ }) = &self.metadata else {
            return div().into_any_element();
        };

        div()
            .flex()
            .flex_col()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_t_1()
            .border_color(theme.border)
            .child(
                div()
                    .pb(Spacing::XS)
                    .child(Text::subsection_label("Object")),
            )
            .child(self.metadata_row("Key", Text::code(metadata.key.clone()).into_any_element()))
            .child(self.metadata_row(
                "Size",
                Text::code(format_size_detail(metadata.size_bytes)).into_any_element(),
            ))
            .child(self.metadata_row(
                "Content-Type",
                Text::code(optional_value(metadata.content_type.as_deref())).into_any_element(),
            ))
            .child(self.metadata_row(
                "Last modified",
                Text::code(format_modified(metadata.last_modified)).into_any_element(),
            ))
            .child(
                self.metadata_row(
                    "ETag",
                    Text::code(optional_value(metadata.etag.as_deref()))
                        .muted_foreground()
                        .into_any_element(),
                ),
            )
            .child(self.metadata_row(
                "Storage class",
                self.render_storage_class(metadata.storage_class.as_deref(), cx),
            ))
            .child(self.metadata_row(
                "Encryption",
                Text::code(optional_value(metadata.encryption.as_deref())).into_any_element(),
            ))
            .child(self.metadata_row(
                "Versions",
                self.render_versions_value(key, metadata.version_count, cx),
            ))
            .child(self.render_versions_list(cx))
            .into_any_element()
    }

    fn metadata_row(&self, label: &'static str, value: AnyElement) -> impl IntoElement {
        div()
            .flex()
            .items_start()
            .gap(Spacing::SM)
            .py(Spacing::XXS)
            .child(
                div()
                    .w(METADATA_LABEL_WIDTH)
                    .flex_shrink_0()
                    .child(Text::caption(label).muted_foreground()),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(value),
            )
    }

    /// Versions value: a count when the driver reported one, otherwise an
    /// on-demand lookup for buckets that keep version history.
    fn render_versions_value(
        &self,
        key: &str,
        version_count: Option<u64>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if let Some(count) = version_count {
            return Text::code(count.to_string()).into_any_element();
        }

        match &self.versions {
            ObjectVersionsState::Loading => Text::caption("Loading…")
                .muted_foreground()
                .into_any_element(),
            ObjectVersionsState::Loaded(versions) => {
                let word = if versions.len() == 1 {
                    "version"
                } else {
                    "versions"
                };
                Text::code(format!("{} {word}", versions.len())).into_any_element()
            }
            ObjectVersionsState::Error(message) => {
                Text::caption(message.clone()).danger().into_any_element()
            }
            ObjectVersionsState::Idle => {
                if !versioning_tracks_history(&self.bucket_details) {
                    return Text::code(UNKNOWN.to_string())
                        .muted_foreground()
                        .into_any_element();
                }

                let key = key.to_string();

                div()
                    .id("object-browser-view-versions")
                    .cursor_pointer()
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.load_object_versions(key.clone(), cx);
                    }))
                    .child(Text::caption("View versions").primary())
                    .into_any_element()
            }
        }
    }

    fn render_versions_list(&self, cx: &Context<Self>) -> AnyElement {
        let ObjectVersionsState::Loaded(versions) = &self.versions else {
            return div().into_any_element();
        };

        if versions.is_empty() {
            return div().into_any_element();
        }

        let theme = cx.theme();

        div()
            .flex()
            .flex_col()
            .mt(Spacing::XS)
            .pt(Spacing::XS)
            .border_t_1()
            .border_color(theme.border)
            .children(
                versions
                    .iter()
                    .map(|version| self.render_version_row(version)),
            )
            .into_any_element()
    }

    fn render_version_row(&self, version: &ObjectVersionSummary) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .py(Spacing::XXS)
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(if version.is_latest {
                        Text::code(short_version_id(&version.version_id)).primary()
                    } else {
                        Text::code(short_version_id(&version.version_id)).muted_foreground()
                    }),
            )
            .child(Text::caption(format_modified(version.last_modified)).muted_foreground())
    }

    /// Action bar (S3-3 footer). Copy S3 URI acts immediately; the remaining
    /// actions raise intents drained by their flow owners.
    fn render_preview_actions(&self, key: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(self.preview_action_button(
                "object-browser-download",
                AppIcon::Download,
                "Download",
                false,
                {
                    let key = key.to_string();
                    move |this, cx| {
                        this.request_object_action(ObjectAction::Download { key: key.clone() }, cx)
                    }
                },
                cx,
            ))
            .child(self.preview_action_button(
                "object-browser-copy-uri",
                AppIcon::Copy,
                "Copy URI",
                false,
                {
                    let key = key.to_string();
                    move |this, cx| this.copy_object_uri(&key, cx)
                },
                cx,
            ))
            .child(self.preview_action_button(
                "object-browser-presign",
                AppIcon::Link2,
                "Presign",
                false,
                {
                    let key = key.to_string();
                    move |this, cx| {
                        this.request_object_action(ObjectAction::Presign { key: key.clone() }, cx)
                    }
                },
                cx,
            ))
            .child(self.preview_action_button(
                "object-browser-delete",
                AppIcon::Delete,
                "Delete",
                true,
                {
                    let key = key.to_string();
                    move |this, cx| {
                        this.request_object_action(ObjectAction::Delete { key: key.clone() }, cx)
                    }
                },
                cx,
            ))
    }

    fn preview_action_button(
        &self,
        id: &'static str,
        icon: AppIcon,
        label: &'static str,
        destructive: bool,
        on_activate: impl Fn(&mut Self, &mut Context<Self>) + 'static,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .id(id)
            .flex()
            .flex_1()
            .items_center()
            .justify_center()
            .gap(Spacing::XS)
            .h(Heights::CONTROL)
            .px(Spacing::XS)
            .rounded(Radii::SM)
            .cursor_pointer()
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                on_activate(this, cx);
            }))
            .child(if destructive {
                Icon::new(icon).small().danger()
            } else {
                Icon::new(icon).small().muted()
            })
            .child(if destructive {
                Text::caption(label).danger()
            } else {
                Text::caption(label)
            })
    }
}

fn object_display_name(key: &str) -> &str {
    key.rsplit_once('/').map(|(_, name)| name).unwrap_or(key)
}

fn optional_value(value: Option<&str>) -> String {
    value.unwrap_or(UNKNOWN).to_string()
}

#[cfg(test)]
mod tests {
    // Deliberately narrow imports: `use super::*` would pull in the module's
    // `gpui::*` glob, whose `test` attribute macro would shadow the standard
    // `#[test]` attribute below.
    use super::{object_display_name, optional_value};

    /// T27: the header shows the last path segment, not the full key.
    #[test]
    fn header_name_drops_the_prefix() {
        assert_eq!(object_display_name("logs/2026/app.log"), "app.log");
        assert_eq!(object_display_name("app.log"), "app.log");
    }

    /// T27: absent metadata fields render as the em-dash placeholder rather
    /// than an empty row.
    #[test]
    fn missing_metadata_values_render_as_placeholders() {
        assert_eq!(optional_value(None), "—");
        assert_eq!(optional_value(Some("AES256")), "AES256");
    }
}
