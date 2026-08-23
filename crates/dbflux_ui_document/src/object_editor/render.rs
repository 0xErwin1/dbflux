//! Layout for `ObjectEditorDocument`.
//!
//! Top to bottom: a header naming the object, the buffer (or why there is no
//! buffer), and the action footer. The footer mirrors the preview pane's
//! editor footer so the two surfaces share their controls and their shortcut
//! hints.

use super::{LoadState, ObjectEditorDocument};
use crate::handle::DocumentEvent;
use crate::object_text::{FIND_SHORTCUT_HINT, SAVE_SHORTCUT_HINT, body_meta_line, cursor_label};
use dbflux_components::controls::GpuiInput;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Diameter of the dirty indicator inside the "modified" pill.
const DIRTY_DOT: Pixels = px(7.0);

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
            .bg(cx.theme().background)
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
    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_dirty = self.is_dirty();

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(Icon::new(AppIcon::FileCode).small().muted())
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .child(Text::code(format!("s3://{}/{}", self.bucket, self.key))),
            )
            .when(is_dirty, |this| {
                this.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .px(Spacing::XS)
                        .rounded(Radii::SM)
                        .border_1()
                        .border_color(theme.warning)
                        .child(div().size(DIRTY_DOT).rounded(Radii::FULL).bg(theme.warning))
                        .child(
                            Text::caption(dbflux_i18n::t!(
                                "document.object_browser.editor.dirty_badge"
                            ))
                            .warning(),
                        ),
                )
            })
    }

    fn render_body(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        match (&self.load, self.buffer.as_ref()) {
            (LoadState::Failed(message), _) => self.render_notice(message.clone(), true, cx),
            (_, Some(buffer)) => div()
                .flex_1()
                .min_h_0()
                .overflow_hidden()
                .bg(theme.background)
                .child(
                    GpuiInput::new(&buffer.input)
                        .appearance(false)
                        .w_full()
                        .h_full(),
                )
                .into_any_element(),
            (LoadState::Loading, None) => self.render_notice(
                dbflux_i18n::t!("document.object_editor.status.loading"),
                false,
                cx,
            ),
            (LoadState::Ready, None) => self.render_notice(
                dbflux_i18n::t!("document.object_editor.status.preparing"),
                false,
                cx,
            ),
        }
    }

    fn render_notice(&self, message: String, is_error: bool, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(Spacing::SM)
            .p(Spacing::LG)
            .child(
                Icon::new(if is_error {
                    AppIcon::TriangleAlert
                } else {
                    AppIcon::Loader
                })
                .size(Heights::ICON_MD)
                .color(if is_error {
                    theme.danger
                } else {
                    theme.muted_foreground
                }),
            )
            .child(Text::muted(message))
            .into_any_element()
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let is_dirty = self.is_dirty();
        let is_saving = self.saving;
        let can_act = is_dirty && !is_saving;
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

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .id("object-editor-save")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .bg(theme.primary)
                            .when(!can_act, |d| d.opacity(0.5))
                            .when(can_act, |d| {
                                d.cursor_pointer()
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.save(cx);
                                    }))
                            })
                            .child(
                                Icon::new(if is_saving {
                                    AppIcon::Loader
                                } else {
                                    AppIcon::Save
                                })
                                .small()
                                .color(theme.primary_foreground),
                            )
                            .child(
                                Text::caption(if is_saving {
                                    dbflux_i18n::t!("document.object_browser.editor.footer.saving")
                                } else {
                                    dbflux_i18n::t!("document.object_browser.editor.footer.save")
                                })
                                .color(theme.primary_foreground),
                            )
                            .child(
                                Text::key_hint(SAVE_SHORTCUT_HINT).color(theme.primary_foreground),
                            ),
                    )
                    .child(
                        div()
                            .id("object-editor-discard")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .when(!can_act, |d| d.opacity(0.5))
                            .when(can_act, |d| {
                                d.cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.discard_edits(window, cx);
                                    }))
                            })
                            .child(Icon::new(AppIcon::RotateCcw).small().muted())
                            .child(Text::caption(dbflux_i18n::t!(
                                "document.object_browser.editor.footer.discard"
                            ))),
                    )
                    .when(has_buffer, |this| {
                        this.child(
                            div()
                                .id("object-editor-find")
                                .flex()
                                .items_center()
                                .gap(Spacing::XS)
                                .h(Heights::CONTROL)
                                .px(Spacing::SM)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.open_find(window, cx);
                                }))
                                .child(Icon::new(AppIcon::Search).small().muted())
                                .child(Text::caption(dbflux_i18n::t!(
                                    "document.object_browser.editor.footer.find"
                                )))
                                .child(Text::key_hint(FIND_SHORTCUT_HINT)),
                        )
                    }),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .when_some(meta, |this, meta| {
                        this.child(Text::caption(meta).muted_foreground())
                    })
                    .when_some(position, |this, position| {
                        this.child(Text::caption(cursor_label(position)).muted_foreground())
                    }),
            )
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
