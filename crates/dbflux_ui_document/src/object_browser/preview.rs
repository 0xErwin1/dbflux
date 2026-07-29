//! Preview pane for `ObjectBrowserDocument`.
//!
//! Layout, top to bottom: header (file-type icon, object name, open-externally,
//! close), the preview body — the rendered image, or the reason there is
//! nothing to render — the object metadata section, and the action bar. The
//! inline text editor lands with its own task; everything the pane cannot
//! render falls back to metadata plus the download / open-externally actions.

use super::metadata::{
    ObjectMetadataState, ObjectVersionsState, PreviewGate, format_size_detail, short_version_id,
    versioning_tracks_history,
};
use super::preview_content::{ImagePreview, PreviewContentState, PreviewKind};
use super::render::{format_modified, object_icon};
use super::{ObjectAction, ObjectBrowserDocument};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::ObjectVersionSummary;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Preferred width of the preview pane when a selection is being previewed.
pub(super) const PREVIEW_WIDTH: Pixels = px(320.0);

/// Preferred width while the inline editor is open: 320 px leaves too little
/// room to read, let alone edit, a line of text next to its line numbers.
pub(super) const PREVIEW_EDITOR_WIDTH: Pixels = px(520.0);

/// Floor for the preview pane. Below this the metadata rows and the action
/// bar stop being readable, so the pane keeps this much even on a narrow
/// window.
const PREVIEW_MIN_WIDTH: Pixels = px(240.0);

/// Share of the document width the preview pane may claim. The preferred
/// widths above are absolute, so on a narrow window they would leave the
/// listing a sliver; capping the pane relative to the available width keeps
/// the listing usable at every window size.
const PREVIEW_MAX_WIDTH_FRACTION: f32 = 0.55;

/// Label column of the metadata rows. Narrow enough to leave the values room
/// inside a 320 px pane.
const METADATA_LABEL_WIDTH: Pixels = px(92.0);

/// Vertical room reserved for the image itself, so the meta strip and the
/// metadata rows below it never jump as images of different shapes load.
const IMAGE_VIEWPORT_HEIGHT: Pixels = px(220.0);

const UNKNOWN: &str = "—";

/// Severity of a body notice, which drives the icon and text treatment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NoticeTone {
    Neutral,
    Warning,
    Danger,
}

impl ObjectBrowserDocument {
    pub(super) fn render_preview_pane(
        &self,
        key: &str,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let editing = self.editor_for(key).is_some();

        div()
            .w(if editing {
                PREVIEW_EDITOR_WIDTH
            } else {
                PREVIEW_WIDTH
            })
            .min_w(PREVIEW_MIN_WIDTH)
            .max_w(relative(PREVIEW_MAX_WIDTH_FRACTION))
            .flex()
            .flex_col()
            .border_l_1()
            .border_color(theme.border)
            .bg(theme.background)
            .child(self.render_preview_header(key, cx))
            .when(editing, |this| this.child(self.render_editor_meta(key, cx)))
            .child(
                div()
                    .flex_1()
                    .flex()
                    .flex_col()
                    .min_h_0()
                    .overflow_hidden()
                    .child(self.render_preview_body(key, cx))
                    .child(self.render_metadata_section(key, cx)),
            )
            .child(self.render_preview_actions(key, cx))
    }

    /// Meta line under the header while editing: what the object is, how big
    /// it is, and how its text is encoded.
    fn render_editor_meta(&self, key: &str, cx: &Context<Self>) -> AnyElement {
        let Some(editor) = self.editor_for(key) else {
            return div().into_any_element();
        };

        let theme = cx.theme();

        div()
            .flex()
            .items_center()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .child(Text::caption(editor.meta_line()).muted_foreground())
            .into_any_element()
    }

    fn render_preview_header(&self, key: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let name = object_display_name(key);
        let shows_image = matches!(self.preview_content(), PreviewContentState::Image(_));
        let is_dirty = self.editor_for(key).is_some_and(|editor| editor.dirty);

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
                    )
                    .when(is_dirty, |this| this.child(self.render_dirty_badge(cx))),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XXS)
                    .when(shows_image, |this| {
                        let key = key.to_string();

                        this.child(
                            div()
                                .id("object-browser-open-external")
                                .flex()
                                .items_center()
                                .justify_center()
                                .size(Heights::CONTROL)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(|d| d.bg(theme.secondary))
                                .tooltip(|window, cx| {
                                    gpui_component::tooltip::Tooltip::new("Open in system viewer")
                                        .build(window, cx)
                                })
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.open_object_externally(key.clone(), cx);
                                }))
                                .child(Icon::new(AppIcon::ExternalLink).small().muted()),
                        )
                    })
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
                    ),
            )
    }

    /// Body area above the metadata rows: the rendered image, the inline text
    /// editor, or the reason there is nothing to render.
    fn render_preview_body(&self, key: &str, cx: &mut Context<Self>) -> AnyElement {
        match self.preview_content() {
            PreviewContentState::Image(preview) => self.render_image_body(preview, cx),
            PreviewContentState::Text => self.render_text_editor(key, cx),
            _ => self.render_body_notice(cx),
        }
    }

    /// The S3-3 image block: the image itself over a neutral backdrop, its
    /// dimensions/format/size meta strip, and the fit + transfer-timing row.
    fn render_image_body(&self, preview: &ImagePreview, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();

        let timing = self
            .last_operation
            .as_ref()
            .filter(|timing| timing.label == "GetObject")
            .map(|timing| format!("{} · {} ms", timing.label, timing.millis));

        div()
            .flex()
            .flex_col()
            .child(
                div()
                    .h(IMAGE_VIEWPORT_HEIGHT)
                    .flex()
                    .items_center()
                    .justify_center()
                    .overflow_hidden()
                    .p(Spacing::SM)
                    .bg(theme.secondary)
                    .child(
                        img(preview.image.clone())
                            .max_w_full()
                            .max_h_full()
                            .object_fit(ObjectFit::Contain),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_center()
                    .py(Spacing::XS)
                    .border_t_1()
                    .border_color(theme.border)
                    .child(Text::caption(preview.meta_line()).muted_foreground()),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap(Spacing::SM)
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .border_t_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(Icon::new(AppIcon::Maximize2).small().muted())
                            .child(Text::caption("Fit to width").muted_foreground()),
                    )
                    .when_some(timing, |this, timing| {
                        this.child(Text::caption(timing).muted_foreground())
                    }),
            )
            .into_any_element()
    }

    /// Everything that is not a rendered image: still loading, refused by the
    /// gate, undecodable, or simply not previewable in-app.
    fn render_body_notice(&self, cx: &Context<Self>) -> AnyElement {
        if let PreviewContentState::Loading = self.preview_content() {
            return self.render_notice(
                AppIcon::Loader,
                "Loading preview…",
                NoticeTone::Neutral,
                cx,
            );
        }

        if let PreviewContentState::Failed(message) = self.preview_content() {
            return self.render_notice(AppIcon::TriangleAlert, message, NoticeTone::Warning, cx);
        }

        let (icon, message, tone) = match &self.metadata {
            None | Some(ObjectMetadataState::Loading) => (
                AppIcon::Loader,
                "Loading metadata…".to_string(),
                NoticeTone::Neutral,
            ),
            Some(ObjectMetadataState::Error(message)) => {
                (AppIcon::TriangleAlert, message.clone(), NoticeTone::Danger)
            }
            Some(ObjectMetadataState::Loaded { gate, .. }) => match gate {
                PreviewGate::Allowed => (
                    AppIcon::Eye,
                    self.unpreviewable_message(),
                    NoticeTone::Neutral,
                ),
                PreviewGate::Archived => (
                    AppIcon::Lock,
                    gate.message().unwrap_or_default(),
                    NoticeTone::Warning,
                ),
                PreviewGate::TooLarge { .. } => (
                    AppIcon::TriangleAlert,
                    gate.message().unwrap_or_default(),
                    NoticeTone::Warning,
                ),
            },
        };

        self.render_notice(icon, &message, tone, cx)
    }

    /// Copy for an object the gate allows but the pane cannot render itself.
    fn unpreviewable_message(&self) -> String {
        match self.preview_kind() {
            Some(PreviewKind::Pdf) => {
                "PDFs are not rendered in-app. Download this object or open it in your system \
                 viewer."
                    .to_string()
            }
            _ => "This file type has no in-app preview. Download this object or open it in your \
                  system viewer."
                .to_string(),
        }
    }

    fn render_notice(
        &self,
        icon: AppIcon,
        message: &str,
        tone: NoticeTone,
        cx: &Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();

        div()
            .flex_1()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(Spacing::SM)
            .p(Spacing::MD)
            .bg(theme.secondary)
            .child(match tone {
                NoticeTone::Danger => Icon::new(icon).size(Heights::ICON_LG).danger(),
                NoticeTone::Warning => Icon::new(icon).size(Heights::ICON_LG).warning(),
                NoticeTone::Neutral => Icon::new(icon).size(Heights::ICON_LG).muted(),
            })
            .child(match tone {
                NoticeTone::Danger => Text::caption(message.to_string()).danger(),
                _ => Text::caption(message.to_string()).muted_foreground(),
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

    /// Action bar (S3-3 footer). Download, Open externally, and Copy S3 URI act
    /// immediately; the remaining actions raise intents drained by their flow
    /// owners.
    ///
    /// The row wraps instead of clipping: at the pane's minimum width five
    /// labelled buttons do not fit on one line, and a clipped Delete is worse
    /// than a two-line bar. `w_full` is required for the wrap to trigger at
    /// all — see `dbflux_components::result_panel`'s chrome row.
    fn render_preview_actions(&self, key: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .flex_wrap()
            .items_center()
            .w_full()
            .gap(Spacing::XS)
            .min_h(Heights::TOOLBAR)
            .py(Spacing::XS)
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
                    move |this, cx| this.download_object(key.clone(), cx)
                },
                cx,
            ))
            .child(self.preview_action_button(
                "object-browser-open-externally",
                AppIcon::ExternalLink,
                "Open",
                false,
                {
                    let key = key.to_string();
                    move |this, cx| this.open_object_externally(key.clone(), cx)
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
            .flex_shrink_0()
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
