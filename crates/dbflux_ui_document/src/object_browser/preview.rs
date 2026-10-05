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
use super::preview_content::{
    EncodingChoice, ImagePreview, OVERRIDABLE_ENCODINGS, PreviewContentState, PreviewKind,
    encoding_label,
};
use super::render::object_icon_color;
use super::render::{format_modified, object_icon};
use super::{ObjectAction, ObjectBrowserDocument, ObjectBrowserFocusMode};
use crate::handle::DocumentEvent;
use crate::labels::object_browser_versions_count_label;
use crate::pane::DocumentSidePanel;
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{
    ChromeColors, DocumentMetrics, Fields, Heights, IslandMetrics, ObjectStoreMetrics,
    PreviewRailMetrics, Spacing,
};
use dbflux_components::typography::AppFonts;
use dbflux_core::{Encoding, ObjectVersionSummary};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Preferred width of the preview pane when a selection is being previewed
/// (P1Objects).
pub(super) const PREVIEW_WIDTH: Pixels = px(460.0);

/// Preferred width while the inline editor is open, so a line of text reads
/// next to its line numbers.
pub(super) const PREVIEW_EDITOR_WIDTH: Pixels = px(520.0);

/// Floor for the preview pane. Below this the metadata rows and the action
/// bar stop being readable, so the pane keeps this much even on a narrow
/// window.
const PREVIEW_MIN_WIDTH: Pixels = px(240.0);

/// Share of the window width the preview island may claim. The preferred
/// widths above are absolute, so on a narrow window they would leave the
/// listing a sliver; capping the island relative to the window keeps the
/// listing usable at every window size.
const PREVIEW_MAX_WIDTH_FRACTION: f32 = 0.4;

/// Ceiling for a user-dragged pane width; the relative cap above still
/// applies, so the listing keeps room even below this.
const PREVIEW_DRAG_MAX_WIDTH: Pixels = px(1200.0);

/// Hit target of the resize grip over the preview island's left edge.
const PREVIEW_GRIP_WIDTH: Pixels = IslandMetrics::GAP;

/// Label column of the metadata rows. (110 px)
const METADATA_LABEL_WIDTH: Pixels = px(110.0);

/// Vertical room reserved for the image itself, so the meta strip and the
/// metadata rows below it never jump as images of different shapes load.
const IMAGE_VIEWPORT_HEIGHT: Pixels = px(220.0);

const UNKNOWN: &str = "—";

/// Width of the preview island: the preferred or dragged width, capped at
/// `PREVIEW_MAX_WIDTH_FRACTION` of the window and never below
/// `PREVIEW_MIN_WIDTH`.
fn preview_panel_width(preferred: Pixels, viewport_width: Pixels) -> Pixels {
    let cap = (viewport_width * PREVIEW_MAX_WIDTH_FRACTION).max(PREVIEW_MIN_WIDTH);

    preferred.clamp(PREVIEW_MIN_WIDTH, cap)
}

/// Severity of a body notice, which drives the icon and text treatment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NoticeTone {
    Neutral,
    Warning,
    Danger,
}

impl ObjectBrowserDocument {
    pub(super) fn begin_preview_resize(&mut self, start_x: Pixels, cx: &mut Context<Self>) {
        let current = self.current_preview_width();
        self.preview_resize_start = Some((start_x, current));
        cx.notify();
    }

    /// Dragging the left-edge grip leftwards grows the pane, so the delta is
    /// inverted relative to the sidebar dock's right-edge grip.
    pub(super) fn handle_preview_resize_move(
        &mut self,
        position_x: Pixels,
        cx: &mut Context<Self>,
    ) {
        let Some((start_x, start_width)) = self.preview_resize_start else {
            return;
        };

        let new_width =
            (start_width + (start_x - position_x)).clamp(PREVIEW_MIN_WIDTH, PREVIEW_DRAG_MAX_WIDTH);
        self.preview_custom_width = Some(new_width);
        cx.notify();
    }

    pub(super) fn finish_preview_resize(&mut self, cx: &mut Context<Self>) {
        if self.preview_resize_start.is_some() {
            self.preview_resize_start = None;
            cx.notify();
        }
    }

    fn current_preview_width(&self) -> Pixels {
        self.preview_custom_width.unwrap_or(
            if self
                .preview_key
                .as_deref()
                .and_then(|key| self.editor_for(key))
                .is_some()
            {
                PREVIEW_EDITOR_WIDTH
            } else {
                PREVIEW_WIDTH
            },
        )
    }

    /// The preview as a side panel for the workspace, which draws it as a
    /// full-height island beside the document island (IslObjects). `None`
    /// while nothing is selected for preview.
    pub(super) fn preview_side_panel(
        &self,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> Option<DocumentSidePanel> {
        let key = self.preview_key.clone()?;
        let width = preview_panel_width(self.current_preview_width(), window.viewport_size().width);

        Some(DocumentSidePanel {
            id: "object-preview".into(),
            width,
            content: self.render_preview_pane(&key, cx).into_any_element(),
        })
    }

    fn render_preview_pane(&self, key: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let grip_hover = cx.theme().accent.opacity(0.3);
        let grip_active = ChromeColors::tint(cx.theme());
        let editing = self.editor_for(key).is_some();
        let resizing = self.preview_resize_start.is_some();

        let resize_listeners = resizing.then(|| {
            let entity = cx.entity().clone();

            // Same pattern as the sidebar dock: element listeners lose the
            // drag once the cursor leaves the grip, so the drag is tracked
            // with window-level listeners registered during paint.
            canvas(
                |_, _, _| {},
                move |_, _, window, _| {
                    window.on_mouse_event({
                        let entity = entity.clone();
                        move |event: &MouseMoveEvent, phase, _, cx| {
                            if phase.bubble() {
                                entity.update(cx, |doc, cx| {
                                    doc.handle_preview_resize_move(event.position.x, cx);
                                });
                            }
                        }
                    });

                    window.on_mouse_event({
                        let entity = entity.clone();
                        move |_: &MouseUpEvent, phase, _, cx| {
                            if phase.bubble() {
                                entity.update(cx, |doc, cx| doc.finish_preview_resize(cx));
                            }
                        }
                    });
                },
            )
            .absolute()
            .size_full()
        });

        div()
            .id("object-preview")
            .relative()
            .size_full()
            .flex()
            .flex_col()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = ObjectBrowserFocusMode::Listing;
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .when_some(resize_listeners, |el, listeners| el.child(listeners))
            .child(self.render_preview_header(key, cx))
            .when(editing, |this| this.child(self.render_editor_meta(key, cx)))
            .child(self.render_encoding_override_row(cx))
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
            // The grip lies over the island's left edge, next to the desk gap
            // that separates it from the listing.
            .child(
                div()
                    .id("object-preview-grip")
                    .absolute()
                    .top_0()
                    .bottom_0()
                    .left_0()
                    .w(PREVIEW_GRIP_WIDTH)
                    .cursor_col_resize()
                    .hover(move |el| el.bg(grip_hover))
                    .when(resizing, |el| el.bg(grip_active))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, event: &MouseDownEvent, _, cx| {
                            cx.stop_propagation();
                            this.begin_preview_resize(event.position.x, cx);
                        }),
                    ),
            )
    }

    /// Meta line under the header while editing: what the object is, how big
    /// it is, and how its text is encoded.
    fn render_editor_meta(&self, key: &str, cx: &Context<Self>) -> AnyElement {
        let Some(editor) = self.editor_for(key) else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let decode_label = editor.decode_label();
        let is_editable = editor.is_editable();

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .child(Text::caption(editor.meta_line()).muted_foreground())
            .when_some(decode_label, |this, label| {
                this.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .child(Text::caption(label).primary())
                        .when(!is_editable, |this| {
                            this.child(
                                Text::caption(dbflux_i18n::t!(
                                    "document.object_browser.preview.body.decoded_read_only"
                                ))
                                .muted_foreground(),
                            )
                        }),
                )
            })
            .into_any_element()
    }

    /// Whether the preview of `key` offers to open it in its own tab.
    ///
    /// The pinned pane is narrow by design, and the same buffer can be taken
    /// to a full-size tab. A text object is offered only once it decoded into
    /// a buffer here, which is exactly the gate the editor tab applies. A CSV
    /// or TSV object opens as a table that reads it in pages, so it is
    /// offered whatever its size.
    pub(super) fn offers_open_in_editor(&self, key: &str) -> bool {
        self.editor_for(key).is_some()
            || crate::file_format::file_document_format(std::path::Path::new(key)).is_some()
    }

    fn render_preview_header(&self, key: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let name = object_display_name(key);
        let icon = object_icon(name);
        let icon_color = object_icon_color(icon, cx);
        let is_dirty = self.editor_for(key).is_some_and(|editor| editor.dirty);

        let open_in_editor = self.offers_open_in_editor(key).then(|| {
            let key = key.to_string();

            Button::new("object-browser-open-in-editor", "")
                .icon(AppIcon::Maximize2)
                .icon_only()
                .tooltip(dbflux_i18n::t!(
                    "document.object_browser.preview.header.open_in_editor"
                ))
                .tab_stop(false)
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.request_open_object_editor(key.clone(), cx);
                }))
        });

        let open_external = {
            let key = key.to_string();

            Button::new("object-browser-open-external", "")
                .icon(AppIcon::ExternalLink)
                .icon_only()
                .tooltip(dbflux_i18n::t!(
                    "document.object_browser.preview.header.open_in_system_viewer"
                ))
                .tab_stop(false)
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.open_object_externally(key.clone(), cx);
                }))
        };

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(PreviewRailMetrics::HEADER_GAP)
            .h(DocumentMetrics::HEADER_HEIGHT)
            .px(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(icon)
                    .size(ObjectStoreMetrics::NAME_ICON)
                    .color(icon_color),
            )
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::strong(theme))
                    .child(name.to_string()),
            )
            .when(is_dirty, |this| this.child(self.render_dirty_badge(cx)))
            .child(div().flex_1())
            .children(open_in_editor)
            .child(open_external)
            .child(
                Button::new("object-browser-preview-close", "")
                    .icon(AppIcon::X)
                    .icon_only()
                    .tooltip(dbflux_i18n::t!(
                        "document.object_browser.preview.header.close"
                    ))
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.close_preview(cx);
                    })),
            )
    }

    /// Body area above the metadata rows: the rendered image, the inline text
    /// editor, or the reason there is nothing to render.
    fn render_preview_body(&self, key: &str, cx: &mut Context<Self>) -> AnyElement {
        match self.preview_content() {
            PreviewContentState::Image(preview) => self.render_image_body(preview, cx),
            PreviewContentState::Text => self.render_text_editor(key, cx),
            _ => self.render_body_notice(key, cx),
        }
    }

    /// "Interpret as" row: lets the user override the auto-detected encoding
    /// once a body has been fetched, regardless of how it is currently shown.
    /// Absent until a body exists, since there is nothing yet to reinterpret.
    pub(super) fn render_encoding_override_row(&self, cx: &mut Context<Self>) -> AnyElement {
        if self.preview_raw_bytes.is_none() {
            return div().into_any_element();
        }

        let theme = cx.theme();

        let choice_id = |choice: Option<EncodingChoice>| -> SharedString {
            match choice {
                None => "object-browser-encoding-auto".into(),
                Some(EncodingChoice::Raw) => "object-browser-encoding-raw".into(),
                Some(EncodingChoice::Encoding(encoding)) => {
                    format!("object-browser-encoding-{}", encoding as u8).into()
                }
            }
        };

        let mut choices: Vec<(Option<EncodingChoice>, String)> = vec![
            (
                None,
                dbflux_i18n::t!("document.object_browser.preview.body.encoding_auto"),
            ),
            (
                Some(EncodingChoice::Raw),
                dbflux_i18n::t!("document.object_browser.preview.body.encoding_raw"),
            ),
        ];
        choices.extend(OVERRIDABLE_ENCODINGS.iter().map(|encoding| {
            (
                Some(EncodingChoice::Encoding(*encoding)),
                encoding_label(*encoding).to_string(),
            )
        }));

        let items = choices
            .iter()
            .map(|(choice, label)| SegmentedItem::new(choice_id(*choice), label.clone()))
            .collect();

        let weak_self = cx.weak_entity();
        let control = SegmentedControl::new(
            items,
            choice_id(self.encoding_override),
            move |id, _, cx| {
                let Some((choice, _)) =
                    choices.iter().find(|(choice, _)| choice_id(*choice) == *id)
                else {
                    return;
                };
                let choice = *choice;

                if let Some(doc) = weak_self.upgrade() {
                    doc.update(cx, |this, cx| this.set_encoding_override(choice, cx));
                }
            },
        );

        div()
            .flex()
            .flex_wrap()
            .flex_shrink_0()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .min_h(PreviewRailMetrics::INTERPRET_HEIGHT)
            .py(PreviewRailMetrics::INTERPRET_PADDING_Y)
            .px(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .text_size(DocumentMetrics::TABLE_META_FONT)
            .text_color(theme.muted_foreground)
            .child(dbflux_i18n::t!(
                "document.object_browser.preview.body.interpret_as"
            ))
            .child(control)
            .into_any_element()
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
                            .child(
                                Text::caption(dbflux_i18n::t!(
                                    "document.object_browser.preview.body.fit_to_width"
                                ))
                                .muted_foreground(),
                            ),
                    )
                    .when_some(timing, |this, timing| {
                        this.child(Text::caption(timing).muted_foreground())
                    }),
            )
            .into_any_element()
    }

    /// Everything that is not a rendered image: still loading, refused by the
    /// gate, undecodable, or simply not previewable in-app.
    fn render_body_notice(&self, key: &str, cx: &mut Context<Self>) -> AnyElement {
        if let PreviewContentState::Loading = self.preview_content() {
            return self.render_notice(
                AppIcon::Loader,
                &dbflux_i18n::t!("document.object_browser.preview.body.loading"),
                NoticeTone::Neutral,
                None,
                cx,
            );
        }

        if let PreviewContentState::Failed(message) = self.preview_content() {
            return self.render_notice(
                AppIcon::TriangleAlert,
                message,
                NoticeTone::Warning,
                None,
                cx,
            );
        }

        if let PreviewContentState::DecodeFailed { encoding, reason } = self.preview_content() {
            let message = dbflux_i18n::t!(
                "document.object_browser.preview.body.decode_failed",
                encoding = encoding_label(*encoding),
                reason = reason.as_str()
            );
            return self.render_notice(
                AppIcon::TriangleAlert,
                &message,
                NoticeTone::Warning,
                None,
                cx,
            );
        }

        if let PreviewContentState::DecodeTooLarge {
            encoding,
            limit_bytes,
        } = self.preview_content()
        {
            let message = dbflux_i18n::t!(
                "document.object_browser.preview.body.decode_too_large",
                encoding = encoding_label(*encoding),
                limit = crate::buckets_table::format_bytes(*limit_bytes as u64).as_str()
            );
            return self.render_notice(
                AppIcon::TriangleAlert,
                &message,
                NoticeTone::Warning,
                None,
                cx,
            );
        }

        let (icon, message, tone, action) = match &self.metadata {
            None | Some(ObjectMetadataState::Loading) => (
                AppIcon::Loader,
                dbflux_i18n::t!("document.object_browser.preview.body.loading_metadata"),
                NoticeTone::Neutral,
                None,
            ),
            Some(ObjectMetadataState::Error(message)) => (
                AppIcon::TriangleAlert,
                message.clone(),
                NoticeTone::Danger,
                None,
            ),
            Some(ObjectMetadataState::Loaded { gate, .. }) => match gate {
                PreviewGate::Allowed => (
                    AppIcon::Eye,
                    self.unpreviewable_message(),
                    NoticeTone::Neutral,
                    None,
                ),
                PreviewGate::Archived => (
                    AppIcon::Lock,
                    gate.message().unwrap_or_default(),
                    NoticeTone::Warning,
                    None,
                ),
                PreviewGate::TooLarge { .. } => (
                    AppIcon::TriangleAlert,
                    gate.message().unwrap_or_default(),
                    NoticeTone::Warning,
                    Some(self.render_load_anyway_button(key, cx)),
                ),
            },
        };

        self.render_notice(icon, &message, tone, action, cx)
    }

    /// Copy for an object the gate allows but the pane cannot render itself.
    fn unpreviewable_message(&self) -> String {
        match self.preview_kind() {
            Some(PreviewKind::Pdf) => {
                dbflux_i18n::t!("document.object_browser.preview.body.unpreviewable.pdf")
            }
            _ => dbflux_i18n::t!("document.object_browser.preview.body.unpreviewable.generic"),
        }
    }

    /// "Load anyway" action offered under a `PreviewGate::TooLarge` refusal:
    /// bypasses the gate once, for this object only.
    fn render_load_anyway_button(&self, key: &str, cx: &mut Context<Self>) -> AnyElement {
        let key = key.to_string();

        Button::new(
            "object-browser-load-anyway",
            dbflux_i18n::t!("document.object_browser.preview.body.load_anyway"),
        )
        .icon(AppIcon::Download)
        .tab_stop(false)
        .on_click(cx.listener(move |this, _, _, cx| {
            this.load_preview_body_override(key.clone(), cx);
        }))
        .into_any_element()
    }

    fn render_notice(
        &self,
        icon: AppIcon,
        message: &str,
        tone: NoticeTone,
        action: Option<AnyElement>,
        cx: &mut Context<Self>,
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
            .bg(theme.background)
            .child(match tone {
                NoticeTone::Danger => Icon::new(icon).size(Heights::ICON_LG).danger(),
                NoticeTone::Warning => Icon::new(icon).size(Heights::ICON_LG).warning(),
                NoticeTone::Neutral => Icon::new(icon).size(Heights::ICON_LG).muted(),
            })
            .child(match tone {
                NoticeTone::Danger => Text::caption(message.to_string()).danger(),
                _ => Text::caption(message.to_string()).muted_foreground(),
            })
            .when_some(action, |this, action| this.child(action))
            .into_any_element()
    }

    /// Object metadata section (S3-3's "Object" block): one key/value row per
    /// field, with the ETag dimmed and versions fetched only on request.
    fn render_metadata_section(&self, key: &str, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        let Some(ObjectMetadataState::Loaded { metadata, gate: _ }) = &self.metadata else {
            return div().into_any_element();
        };

        let value = |text: String| {
            div()
                .truncate()
                .font_family(dbflux_components::fonts::editor_family(cx))
                .text_color(ChromeColors::strong(theme))
                .child(text)
                .into_any_element()
        };

        div()
            .flex()
            .flex_col()
            .flex_shrink_0()
            .px(DocumentMetrics::PADDING_X)
            .py(PreviewRailMetrics::SECTION_PADDING_Y)
            .border_t_1()
            .border_color(theme.border)
            .child(
                Text::label(dbflux_i18n::t!("document.object_browser.metadata.section"))
                    .font_size(PreviewRailMetrics::SECTION_LABEL_FONT),
            )
            .child(self.metadata_row(
                dbflux_i18n::t!("document.object_browser.metadata.key"),
                value(metadata.key.clone()),
                cx,
            ))
            .child(self.metadata_row(
                dbflux_i18n::t!("document.object_browser.metadata.size"),
                value(format_size_detail(metadata.size_bytes)),
                cx,
            ))
            .child(self.metadata_row(
                dbflux_i18n::t!("document.object_browser.metadata.content_type"),
                value(optional_value(metadata.content_type.as_deref())),
                cx,
            ))
            .child(self.metadata_row(
                dbflux_i18n::t!("document.object_browser.metadata.last_modified"),
                value(format_modified(metadata.last_modified)),
                cx,
            ))
            .child(self.metadata_row(
                dbflux_i18n::t!("document.object_browser.metadata.etag"),
                value(optional_value(metadata.etag.as_deref())),
                cx,
            ))
            .child(self.metadata_row(
                dbflux_i18n::t!("document.object_browser.metadata.storage_class"),
                value(super::render::storage_class_label(
                    metadata.storage_class.as_deref(),
                )),
                cx,
            ))
            .child(self.metadata_row(
                dbflux_i18n::t!("document.object_browser.metadata.encryption"),
                value(optional_value(metadata.encryption.as_deref())),
                cx,
            ))
            .child(self.metadata_row(
                dbflux_i18n::t!("document.object_browser.metadata.versions"),
                self.render_versions_value(key, metadata.version_count, cx),
                cx,
            ))
            .child(self.render_versions_list(cx))
            .into_any_element()
    }

    fn metadata_row(
        &self,
        label: impl Into<SharedString>,
        value: AnyElement,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_start()
            .py(PreviewRailMetrics::ROW_PADDING_Y)
            .text_size(Fields::TEXT)
            .child(
                div()
                    .w(METADATA_LABEL_WIDTH)
                    .flex_shrink_0()
                    .text_color(cx.theme().muted_foreground)
                    .child(label.into()),
            )
            .child(div().flex_1().min_w_0().overflow_hidden().child(value))
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
            ObjectVersionsState::Loading => Text::caption(dbflux_i18n::t!(
                "document.object_browser.preview.versions.loading"
            ))
            .muted_foreground()
            .into_any_element(),
            ObjectVersionsState::Loaded(versions) => {
                Text::code(object_browser_versions_count_label(versions.len())).into_any_element()
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
                    .child(
                        Text::caption(dbflux_i18n::t!(
                            "document.object_browser.preview.versions.view"
                        ))
                        .primary(),
                    )
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

    /// Action bar (S3-3 footer): Download, Copy URI, Presign and Delete, the
    /// four that fit one row at the pane's default width. Download and Copy
    /// URI act immediately; the others raise intents drained by their flow
    /// owners. Opening in the system viewer lives in the header.
    ///
    /// The row still wraps instead of clipping at the pane's minimum width: a
    /// clipped Delete is worse than a two-line bar. `w_full` is required for
    /// the wrap to trigger at all — see `dbflux_components::result_panel`'s
    /// chrome row.
    fn render_preview_actions(&self, key: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let action = |id: &'static str, icon: AppIcon, label: String| {
            Button::new(id, label).icon(icon).tab_stop(false)
        };

        div()
            .flex()
            .flex_wrap()
            .flex_shrink_0()
            .items_center()
            .w_full()
            .gap(PreviewRailMetrics::ACTIONS_GAP)
            .px(DocumentMetrics::PADDING_X)
            .py(PreviewRailMetrics::ACTIONS_PADDING_Y)
            .border_t_1()
            .border_color(theme.border)
            .child(
                action(
                    "object-browser-download",
                    AppIcon::Download,
                    dbflux_i18n::t!("document.object_browser.preview.action.download"),
                )
                .primary()
                .on_click({
                    let key = key.to_string();
                    cx.listener(move |this, _, _, cx| this.download_object(key.clone(), cx))
                }),
            )
            .child(
                action(
                    "object-browser-copy-uri",
                    AppIcon::Copy,
                    dbflux_i18n::t!("document.object_browser.preview.action.copy_uri"),
                )
                .on_click({
                    let key = key.to_string();
                    cx.listener(move |this, _, _, cx| this.copy_object_uri(&key, cx))
                }),
            )
            .child(
                action(
                    "object-browser-presign",
                    AppIcon::Link2,
                    dbflux_i18n::t!("document.object_browser.preview.action.presign"),
                )
                .on_click({
                    let key = key.to_string();
                    cx.listener(move |this, _, _, cx| {
                        this.request_object_action(ObjectAction::Presign { key: key.clone() }, cx)
                    })
                }),
            )
            .child(
                action(
                    "object-browser-delete",
                    AppIcon::Delete,
                    dbflux_i18n::t!("document.object_browser.preview.action.delete"),
                )
                .danger()
                .on_click({
                    let key = key.to_string();
                    cx.listener(move |this, _, _, cx| {
                        this.request_object_action(ObjectAction::Delete { key: key.clone() }, cx)
                    })
                }),
            )
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
    use super::{
        PREVIEW_MIN_WIDTH, PREVIEW_WIDTH, object_display_name, optional_value, preview_panel_width,
    };

    /// The preview island keeps its preferred width on a wide window, gives
    /// way on a narrow one so the listing stays usable, and never drops
    /// below its floor.
    #[test]
    fn the_preview_island_width_follows_the_window() {
        assert_eq!(
            preview_panel_width(PREVIEW_WIDTH, gpui::px(1920.0)),
            PREVIEW_WIDTH
        );
        assert_eq!(
            preview_panel_width(PREVIEW_WIDTH, gpui::px(1000.0)),
            gpui::px(400.0)
        );
        assert_eq!(
            preview_panel_width(PREVIEW_WIDTH, gpui::px(300.0)),
            PREVIEW_MIN_WIDTH
        );
    }

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
