//! Layout for `ObjectEditorDocument`.
//!
//! Top to bottom: a header naming the object, the buffer (or why there is no
//! buffer), and the action footer. The footer mirrors the preview pane's
//! editor footer so the two surfaces share their controls and their shortcut
//! hints.

use super::{LoadRefusal, LoadState, ObjectEditorDocument};
use crate::chrome::{document_bar, document_footer, footer_item};
use crate::handle::DocumentEvent;
use crate::object_browser::decode_label;
use crate::object_browser::{object_icon, object_icon_color};
use crate::object_text::{
    FIND_SHORTCUT_HINT, SAVE_SHORTCUT_HINT, body_meta_line, cursor_label, keycap_text,
};
use dbflux_components::composites::EmptyState;
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone, Icon, SegmentedControl, SegmentedItem};
use dbflux_components::tokens::{ChromeColors, DocumentMetrics, ObjectStoreMetrics};
use dbflux_components::typography::AppFonts;
use dbflux_components::vim::VimBinding;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

impl Render for ObjectEditorDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Building the buffer needs a `Window`, which only this pass has.
        if let Some(pending) = self.pending_body.take() {
            self.install_buffer(pending, window, cx);
        }

        div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .bg(cx.theme().popover)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|_this, _, _, cx| {
                    cx.emit(DocumentEvent::RequestFocus);
                }),
            )
            .child(self.render_header(cx))
            .child(self.render_body(cx))
            .child(self.render_footer(cx))
    }
}

impl ObjectEditorDocument {
    /// Header row: the object's icon and URI, the modified badge, the
    /// Auto/Raw interpretation switch, Find, Discard and the primary Save.
    fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_dirty = self.is_dirty();
        let is_saving = self.saving;
        let is_editable = self.is_editable();
        let can_act = is_editable && is_dirty && !is_saving;
        let has_buffer = self.buffer.is_some();
        let has_raw_override = self.has_raw_override();
        let icon = object_icon(&self.key);
        let icon_color = object_icon_color(icon, cx);

        // Raw is offered while looking at a decoded (never dirty) view, and
        // Auto is the way back from a Raw override; `set_raw_override`
        // refuses to leave Raw while it holds edits.
        let interpretation = ((has_buffer && !is_editable) || has_raw_override).then(|| {
            let weak_self = cx.weak_entity();

            SegmentedControl::new(
                vec![
                    SegmentedItem::new(
                        "object-editor-interpret-auto",
                        dbflux_i18n::t!("document.object_browser.preview.body.encoding_auto"),
                    ),
                    SegmentedItem::new(
                        "object-editor-interpret-raw",
                        dbflux_i18n::t!("document.object_browser.preview.body.encoding_raw"),
                    ),
                ],
                if has_raw_override {
                    "object-editor-interpret-raw"
                } else {
                    "object-editor-interpret-auto"
                },
                move |id, _, cx| {
                    let raw = id.as_ref() == "object-editor-interpret-raw";

                    if let Some(doc) = weak_self.upgrade() {
                        doc.update(cx, |this, cx| {
                            if this.has_raw_override() != raw {
                                this.set_raw_override(raw, cx);
                            }
                        });
                    }
                },
            )
        });

        let find = has_buffer.then(|| {
            Button::new(
                "object-editor-find",
                dbflux_i18n::t!("document.object_browser.editor.footer.find"),
            )
            .icon(AppIcon::Search)
            .kbd(keycap_text(FIND_SHORTCUT_HINT))
            .tab_stop(false)
            .on_click(cx.listener(|this, _, window, cx| {
                this.open_find(window, cx);
            }))
        });

        let discard = is_editable.then(|| {
            Button::new(
                "object-editor-discard",
                dbflux_i18n::t!("document.object_browser.editor.footer.discard"),
            )
            .icon(AppIcon::RotateCcw)
            .disabled(!can_act)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, window, cx| {
                this.discard_edits(window, cx);
            }))
        });

        let save = is_editable.then(|| {
            Button::new(
                "object-editor-save",
                if is_saving {
                    dbflux_i18n::t!("document.object_browser.editor.footer.saving")
                } else {
                    dbflux_i18n::t!("document.object_browser.editor.footer.save")
                },
            )
            .primary()
            .icon(if is_saving {
                AppIcon::Loader
            } else {
                AppIcon::Save
            })
            .kbd(keycap_text(SAVE_SHORTCUT_HINT))
            .disabled(!can_act)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| {
                this.save(cx);
            }))
        });

        document_bar(DocumentMetrics::HEADER_HEIGHT_TALL, cx)
            .child(
                Icon::new(icon)
                    .size(DocumentMetrics::TITLE_ICON)
                    .color(icon_color),
            )
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .font_family(AppFonts::MONO)
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::strong(theme))
                    .child(format!("s3://{}/{}", self.bucket, self.key)),
            )
            .when(is_dirty, |this| {
                this.child(Badge::new(
                    dbflux_i18n::t!("document.object_browser.editor.dirty_badge"),
                    BadgeTone::Warning,
                ))
            })
            .child(div().flex_1())
            .children(interpretation)
            .children(find)
            .children(discard)
            .children(save)
    }

    fn render_body(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        match (&self.load, self.buffer.as_ref()) {
            (LoadState::Failed(refusal), _) => self.render_refusal(refusal, cx),
            (_, Some(buffer)) => {
                let editor = div()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .pt(ObjectStoreMetrics::EDITOR_PADDING_TOP)
                    .bg(theme.background)
                    .child(
                        buffer
                            .vim
                            .editor(!buffer.is_editable())
                            .appearance(false)
                            .w_full()
                            .h_full(),
                    );
                let indicator = buffer.vim.render_indicator(cx);
                let wrapper = div().flex_1().min_h_0().flex().flex_col();

                let input = buffer.vim.input_id();

                VimBinding::capture_run_command(VimBinding::wire(wrapper, input, cx), input, cx)
                    .child(editor)
                    .children(indicator)
                    .into_any_element()
            }
            (LoadState::Loading, None) => self.render_notice(
                dbflux_i18n::t!("document.object_editor.status.loading"),
                false,
                None,
                cx,
            ),
            (LoadState::Ready, None) => self.render_notice(
                dbflux_i18n::t!("document.object_editor.status.preparing"),
                false,
                None,
                cx,
            ),
        }
    }

    fn render_refusal(&self, refusal: &LoadRefusal, cx: &mut Context<Self>) -> AnyElement {
        let action = refusal.is_too_large().then(|| {
            Button::new(
                "object-editor-load-anyway",
                dbflux_i18n::t!("document.object_browser.preview.body.load_anyway"),
            )
            .icon(AppIcon::Download)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| {
                this.load_anyway(cx);
            }))
            .into_any_element()
        });

        self.render_notice(refusal.message().to_string(), true, action, cx)
    }

    fn render_notice(
        &self,
        message: String,
        is_error: bool,
        action: Option<AnyElement>,
        cx: &Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();

        let mut empty = EmptyState::new(
            if is_error {
                AppIcon::TriangleAlert
            } else {
                AppIcon::Loader
            },
            message,
        );

        if is_error {
            empty = empty.danger();
        }

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(DocumentMetrics::GAP)
            .p(DocumentMetrics::PADDING_X)
            .bg(theme.background)
            .child(empty)
            .when_some(action, |this, action| this.child(action))
            .into_any_element()
    }

    /// Footer: what the buffer is (type, encoding, size), how it decoded,
    /// and the cursor position.
    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let is_editable = self.is_editable();
        let has_buffer = self.buffer.is_some();

        let position = self
            .buffer
            .as_ref()
            .map(|buffer| buffer.input.read(cx).cursor_position());

        let meta = self.buffer.as_ref().map(|buffer| {
            body_meta_line(
                buffer.content_type.as_deref(),
                buffer.byte_len,
                buffer.line_ending,
            )
        });

        let decoded_label = self
            .buffer
            .as_ref()
            .and_then(|buffer| decode_label(&buffer.baseline, buffer.source));

        let tint = ChromeColors::tint(cx.theme());

        document_footer(cx)
            .h(ObjectStoreMetrics::EDITOR_FOOTER_HEIGHT)
            .gap(ObjectStoreMetrics::EDITOR_FOOTER_GAP)
            .when_some(meta, |footer, meta| {
                footer.child(footer_item(AppIcon::File, meta, cx))
            })
            .when_some(decoded_label, |footer, label| {
                footer.child(div().text_color(tint).child(label))
            })
            .when(has_buffer && !is_editable, |footer| {
                footer.child(dbflux_i18n::t!(
                    "document.object_browser.preview.body.decoded_read_only"
                ))
            })
            .child(div().flex_1())
            .when_some(position, |footer, position| {
                footer.child(
                    div()
                        .font_family(AppFonts::MONO)
                        .child(cursor_label(position)),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    /// The tab's own loading/preparing notices resolve in both locales and
    /// diverge from English to Spanish.
    #[test]
    fn status_keys_resolve_in_both_locales() {
        for key in [
            "document.object_editor.status.loading",
            "document.object_editor.status.preparing",
            "document.object_browser.error.connection_unavailable",
            "document.object_browser.error.api_unavailable",
        ] {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");

            assert!(!en.is_empty());
            assert_ne!(en, key);
            assert_ne!(en, format!("en.{key}"));
            assert_ne!(en, es);
        }
    }

    /// The footer, dirty-badge, and toolbar controls reuse the same
    /// `document.object_browser.editor.*` catalog entries the pinned
    /// preview-pane editor uses, so the two surfaces read identically.
    #[test]
    fn footer_and_dirty_badge_reuse_the_shared_editor_catalog_entries() {
        for key in [
            "document.object_browser.editor.dirty_badge",
            "document.object_browser.editor.footer.saving",
            "document.object_browser.editor.footer.save",
            "document.object_browser.editor.footer.discard",
            "document.object_browser.editor.footer.find",
        ] {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");

            assert!(!en.is_empty());
            assert_ne!(en, es);
        }
    }
}
