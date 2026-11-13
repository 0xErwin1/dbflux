//! The value section of the details pane: the retrieve control, the
//! key/value and plaintext presentations and a failed retrieval.

use super::{DetailsState, SecretView, ValueState};
use super::{VIEWER_GUTTER_WIDTH, VIEWER_VISIBLE_LINES, section_title};
use crate::buckets_table::format_bytes;
use crate::secret_store::render::listing_shortcut;
use crate::secret_store::value::{MASK, RetrievedValue, ValueContent, ValueFailure, ValueView};
use dbflux_app::keymap::Command;
use dbflux_components::controls::{Button, Input};
use dbflux_components::fonts;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Chamfer, Icon, SegmentedControl, SegmentedItem,
    Text,
};
use dbflux_components::tokens::{ChamferCut, DocumentMetrics, SecretStoreMetrics};
use dbflux_core::{SecretStoreCapabilities, SecretSummary};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::Scrollbar;

impl SecretView {
    pub(super) fn render_value_section(
        &self,
        summary: &SecretSummary,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let header = div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .child(section_title(
                dbflux_i18n::t!("document.secret_store.value.title"),
                cx,
            ))
            .when_some(self.value_kind_label(), |header, label| {
                header.child(Badge::new(label, BadgeTone::Accent).id("secret-value-kind"))
            })
            .child(div().flex_1())
            .when(!self.in_tab(), |header| {
                header.when_some(self.value_header_meta(), |header, meta| {
                    header.child(Text::caption(meta).muted_foreground())
                })
            })
            .when(self.in_tab(), |header| {
                header.children(self.value_card_actions(cx))
            });

        let body: AnyElement = if let Some(date) = self.deletion_of(summary) {
            self.render_pending_deletion(summary, date, cx)
                .into_any_element()
        } else {
            match &self.value {
                ValueState::NotLoaded => self.render_value_not_loaded(cx).into_any_element(),
                ValueState::Loading => {
                    Text::caption(dbflux_i18n::t!("document.secret_store.value.retrieving"))
                        .muted_foreground()
                        .into_any_element()
                }
                ValueState::Loaded(value) => self.render_value_loaded(value, cx).into_any_element(),
                ValueState::Failed(failure) => render_value_failed(failure, cx).into_any_element(),
            }
        };

        if self.in_tab() {
            let mut rows = Vec::new();

            if let Some(version) = self.shown_older_version().map(str::to_string) {
                rows.push(self.render_version_banner(version, cx).into_any_element());
            }
            rows.push(body);

            return div().id("secret-value-section").child(super::card::card(
                "secret-document-value-card",
                header,
                rows,
                cx,
            ));
        }

        div()
            .id("secret-value-section")
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::ROW_GAP)
            .child(header)
            .when_some(
                self.shown_older_version().map(str::to_string),
                |section, version| section.child(self.render_version_banner(version, cx)),
            )
            .child(body)
    }

    /// Says which version the value section shows when it is not the
    /// current one, with the one action that goes back.
    pub(super) fn render_version_banner(
        &self,
        version: String,
        cx: &mut Context<Self>,
    ) -> BannerBlock {
        let back = Button::new(
            "secret-version-back-to-current",
            dbflux_i18n::t!("document.secret_store.versions.back_to_current"),
        )
        .icon(AppIcon::RotateCcw)
        .tab_stop(false)
        .disabled(!self.can_return_to_current())
        .on_click(cx.listener(|this, _, _, cx| this.back_to_current_version(cx)));

        BannerBlock::new(
            BannerVariant::Info,
            dbflux_i18n::t!("document.secret_store.versions.shown", version = version),
        )
        .with_body(if self.current_version_id().is_some() {
            dbflux_i18n::t!("document.secret_store.versions.not_current")
        } else {
            dbflux_i18n::t!("document.secret_store.versions.current_unknown")
        })
        .with_actions(back)
    }

    /// "dotenv · 9 lines" once a value is shown: counts only, no text.
    pub(in crate::secret_store) fn value_kind_label(&self) -> Option<String> {
        match &self.value {
            ValueState::Loaded(value) => Some(value.kind_label()),
            _ => None,
        }
    }

    /// "Retrieved 14:02 · audited" once a value is shown.
    pub(super) fn value_header_meta(&self) -> Option<String> {
        let ValueState::Loaded(value) = &self.value else {
            return None;
        };

        Some(dbflux_i18n::t!(
            "document.secret_store.value.retrieved_at",
            time = value.retrieved_at.format("%H:%M").to_string()
        ))
    }

    pub(super) fn retrieve_button(&self, cx: &mut Context<Self>) -> Button {
        let mut button = Button::new(
            "secret-retrieve-value",
            dbflux_i18n::t!("document.secret_store.value.retrieve"),
        )
        .primary()
        .icon(AppIcon::Eye)
        .tab_stop(false)
        .disabled(!self.can_retrieve_value())
        .on_click(cx.listener(|this, _, _, cx| this.retrieve_selected_value(cx)));

        if let Some(key) = listing_shortcut(Command::ToggleValuePanel) {
            button = button.kbd(key);
        }

        button
    }

    pub(super) fn render_value_not_loaded(&self, cx: &mut Context<Self>) -> Stateful<Div> {
        let encryption_key = match &self.details {
            DetailsState::Loaded(details)
                if self.store.supports(SecretStoreCapabilities::TYPED_VALUES) =>
            {
                details.encryption_key.clone()
            }
            _ => None,
        };

        let explanation = match &encryption_key {
            Some(_) => dbflux_i18n::t!("document.secret_store.value.encrypted_hint"),
            None => dbflux_i18n::t!("document.secret_store.value.not_loaded_hint"),
        };

        let theme = cx.theme();
        let fill = theme.popover;
        let border = theme.border;
        let muted = theme.muted_foreground;

        div()
            .id("secret-value-card")
            .relative()
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::ROW_GAP)
            .when(!self.in_tab(), |card| {
                card.px(DocumentMetrics::DETAIL_BLOCK_PADDING_X)
                    .py(DocumentMetrics::DETAIL_BLOCK_PADDING_Y)
            })
            .when(!self.in_tab(), |card| {
                card.child(Chamfer::new(ChamferCut::CARD).fill(fill).border(border))
            })
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .child(
                        div().id("secret-value-card-lock").child(
                            Icon::new(AppIcon::Lock)
                                .size(DocumentMetrics::TITLE_ICON)
                                .color(muted),
                        ),
                    )
                    .child(div().font_weight(FontWeight::SEMIBOLD).child(Text::body(
                        dbflux_i18n::t!("document.secret_store.value.not_loaded"),
                    ))),
            )
            .child(Text::caption(explanation).muted_foreground())
            .when_some(encryption_key, |body, key| {
                body.child(
                    Text::caption(dbflux_i18n::t!(
                        "document.secret_store.value.needs_decrypt",
                        key = key
                    ))
                    .muted_foreground(),
                )
            })
            .child(div().flex().child(self.retrieve_button(cx)))
    }

    pub(super) fn render_value_view_switch(
        &self,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let entity = cx.entity().downgrade();
        let active = match value.view {
            ValueView::KeyValue => "key-value",
            ValueView::Plaintext => "plaintext",
        };

        let switch = SegmentedControl::new(
            value_view_items(value.content.has_key_value_view()),
            active,
            move |id, _window, cx| {
                let view = if id.as_ref() == "plaintext" {
                    ValueView::Plaintext
                } else {
                    ValueView::KeyValue
                };

                if let Some(doc) = entity.upgrade() {
                    doc.update(cx, |doc, cx| doc.set_value_view(view, cx));
                }
            },
        )
        .group("secret-value-view");

        div().child(switch).into_any_element()
    }

    pub(super) fn render_value_loaded(
        &self,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> Div {
        let mut body = div().flex().flex_col().gap(SecretStoreMetrics::ROW_GAP);

        // The open editor replaces the value's presentation: the stored text
        // in an editable surface, with Save and Discard, drawn inside the
        // redaction boundary the multi-line viewer uses.
        if self.is_editing_value() {
            return self.render_value_editor(cx);
        }

        body = match &value.content {
            ValueContent::KeyValue { .. } | ValueContent::Plaintext(_) => {
                body.child(self.render_value_switch_row(value, cx))
            }
            ValueContent::Binary { size, .. } => body.child(
                div()
                    .flex()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .child(Text::body(dbflux_i18n::t!(
                        "document.secret_store.value.binary_size",
                        size = format_bytes(*size as u64)
                    )))
                    .child(Badge::new(
                        dbflux_i18n::t!("document.secret_store.value.read_only"),
                        BadgeTone::Neutral,
                    )),
            ),
        };

        let fields: Vec<AnyElement> = value
            .fields()
            .iter()
            .enumerate()
            .map(|(index, (key, _))| {
                self.render_value_field(
                    index,
                    key.map(str::to_string),
                    value.is_revealed(index),
                    value,
                    cx,
                )
            })
            .collect();

        body = body.children(fields);

        if value.content.is_binary() {
            body = body.child(
                Text::caption(dbflux_i18n::t!("document.secret_store.value.binary_note"))
                    .muted_foreground(),
            );
        }

        // The tab keeps the actions in the card's title row and says the
        // value is tab-only in its footer.
        if self.in_tab() {
            return body;
        }

        body.child(self.render_value_actions(value, cx)).child(
            Text::caption(dbflux_i18n::t!("document.secret_store.value.tab_only"))
                .muted_foreground(),
        )
    }

    /// The Key/value | Plaintext switch, with "Retrieved 14:02 · audited" at
    /// its right in the tab.
    fn render_value_switch_row(&self, value: &RetrievedValue, cx: &mut Context<Self>) -> Div {
        let meta = self.in_tab().then(|| self.value_header_meta()).flatten();

        div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .child(self.render_value_view_switch(value, cx))
            .child(div().flex_1())
            .children(meta.map(|meta| Text::caption(meta).muted_foreground()))
    }

    /// One value field: its key on a key/value secret, the value in a
    /// secret-marked read-only input, and its reveal and copy buttons. A value
    /// with more than one line is drawn in the multi-line viewer instead.
    pub(super) fn render_value_field(
        &self,
        index: usize,
        key: Option<String>,
        revealed: bool,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if value.is_multiline() {
            return self
                .render_multiline_value_field(index, key, revealed, value, cx)
                .into_any_element();
        }

        let theme = cx.theme();
        let aria = key
            .clone()
            .unwrap_or_else(|| dbflux_i18n::t!("document.secret_store.value.title"));

        let field: AnyElement = match self.value_inputs.get(index) {
            Some(input) => Input::new(input)
                .id(SharedString::from(format!("secret-value-field-{index}")))
                .aria_label(aria)
                .secret(true)
                .appearance(false)
                .disabled(true)
                .into_any_element(),
            None => div()
                .text_color(theme.muted_foreground)
                .child(MASK)
                .into_any_element(),
        };

        div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .h(SecretStoreMetrics::VALUE_ROW_HEIGHT)
            .font_family(fonts::editor_family(cx))
            .text_size(DocumentMetrics::TABLE_META_FONT)
            .when_some(key, |row, key| {
                row.child(
                    div()
                        .w(SecretStoreMetrics::VALUE_KEY_WIDTH)
                        .flex_shrink_0()
                        .truncate()
                        .text_color(theme.muted_foreground)
                        .child(key),
                )
            })
            .child(div().flex_1().min_w_0().child(field))
            .children(value_field_buttons(index, revealed, cx))
            .into_any_element()
    }

    /// A value the viewer draws: its key and actions on one row, then the
    /// bounded viewer box.
    pub(super) fn render_multiline_value_field(
        &self,
        index: usize,
        key: Option<String>,
        revealed: bool,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let muted = cx.theme().muted_foreground;
        let family = fonts::editor_family(cx);
        let lines = value.viewer_lines(revealed);

        let header = div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .text_size(DocumentMetrics::TABLE_META_FONT)
            .when_some(key, |header, key| {
                header.child(
                    div()
                        .min_w_0()
                        .truncate()
                        .font_family(family)
                        .text_color(muted)
                        .child(key),
                )
            })
            .child(div().flex_1())
            .children(value_field_buttons(index, revealed, cx));

        div()
            .id(SharedString::from(format!("secret-value-field-{index}")))
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::ROW_GAP)
            .child(header)
            .child(render_value_viewer(&lines, &self.value_viewer_scroll, cx))
    }

    pub(super) fn render_value_actions(
        &self,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> Div {
        div()
            .flex()
            .flex_wrap()
            .gap(DocumentMetrics::GAP)
            .children(self.value_action_buttons(value, cx))
    }

    /// The actions of a loaded value shown in the tab's value card: Reveal
    /// all, Copy JSON and Edit value. Empty while no value is loaded or the
    /// editor is open.
    fn value_card_actions(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        match &self.value {
            ValueState::Loaded(value) if !self.is_editing_value() => {
                self.value_action_buttons(value, cx)
            }
            _ => Vec::new(),
        }
    }

    fn value_action_buttons(
        &self,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let copy_label = if value.content.has_key_value_view() {
            dbflux_i18n::t!("document.secret_store.value.copy_json")
        } else {
            dbflux_i18n::t!("document.secret_store.value.copy_all")
        };

        let copy_all = Button::new("secret-value-copy-all", copy_label)
            .inline()
            .icon(AppIcon::Copy)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.copy_full_value(cx)));

        let (reveal_key, reveal_icon) = if value.all_revealed() {
            ("document.secret_store.value.hide_all", AppIcon::EyeOff)
        } else {
            ("document.secret_store.value.show_all", AppIcon::Eye)
        };

        let reveal_all = Button::new("secret-value-hide-all", dbflux_i18n::t!(reveal_key))
            .inline()
            .icon(reveal_icon)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.toggle_reveal_all(cx)));

        let mut buttons = vec![reveal_all.into_any_element(), copy_all.into_any_element()];

        // The pane's bottom bar owns Edit value; the tab has no bar.
        if self.in_tab() && self.can_edit_value() {
            buttons.push(
                Button::new(
                    "secret-value-edit",
                    dbflux_i18n::t!("document.secret_store.edit.open"),
                )
                .primary()
                .icon(AppIcon::Pencil)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.start_value_edit(window, cx);
                }))
                .into_any_element(),
            );
        }

        buttons
    }
}

/// The two tabs of the value view. Key/value is muted, with the reason in
/// its tooltip, on a value that does not open as a one-level JSON object.
pub(in crate::secret_store) fn value_view_items(key_value_available: bool) -> Vec<SegmentedItem> {
    let key_value = SegmentedItem::new(
        "key-value",
        dbflux_i18n::t!("document.secret_store.value.view.key_value"),
    );

    let key_value = if key_value_available {
        key_value
    } else {
        key_value.disabled_reason(dbflux_i18n::t!(
            "document.secret_store.value.view.key_value_unavailable"
        ))
    };

    vec![
        key_value,
        SegmentedItem::new(
            "plaintext",
            dbflux_i18n::t!("document.secret_store.value.view.plaintext"),
        ),
    ]
}

/// The reveal and copy buttons of one value field, shared by the single-line
/// field and the multi-line viewer.
pub(super) fn value_field_buttons(
    index: usize,
    revealed: bool,
    cx: &mut Context<SecretView>,
) -> [AnyElement; 2] {
    [
        Button::new(
            SharedString::from(format!("secret-value-reveal-{index}")),
            if revealed {
                dbflux_i18n::t!("document.secret_store.value.hide")
            } else {
                dbflux_i18n::t!("document.secret_store.value.reveal")
            },
        )
        .ghost()
        .icon(if revealed {
            AppIcon::EyeOff
        } else {
            AppIcon::Eye
        })
        .icon_only()
        .tab_stop(false)
        .on_click(cx.listener(move |this, _, _, cx| this.toggle_reveal(index, cx)))
        .into_any_element(),
        Button::new(
            SharedString::from(format!("secret-value-copy-{index}")),
            dbflux_i18n::t!("document.secret_store.value.copy"),
        )
        .ghost()
        .icon(AppIcon::Copy)
        .icon_only()
        .tab_stop(false)
        .on_click(cx.listener(move |this, _, _, cx| this.copy_value_field(index, cx)))
        .into_any_element(),
    ]
}

/// The bounded viewer of a multi-line value: a monospace box with a
/// line-number gutter that keeps every line break and wraps long lines at the
/// pane width. The box grows with its lines up to [`VIEWER_VISIBLE_LINES`]
/// and scrolls past that, so a short value does not push the sections below
/// it down and a long one does not run away.
///
/// The lines it draws are hidden from the accessibility tree and redacted in
/// the rendered frame UI automation reads, so neither a masked nor a revealed
/// value leaves the value behind.
pub(super) fn render_value_viewer(lines: &[String], scroll_handle: &ScrollHandle, cx: &App) -> Div {
    let theme = cx.theme();
    let muted = theme.muted_foreground;
    let foreground = theme.foreground;
    let fill = theme.popover;
    let border = theme.border;

    let line_height = fonts::ui_px(cx, DocumentMetrics::DETAIL_BLOCK_FONT)
        * DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT;

    let rows = lines.iter().enumerate().map(|(index, line)| {
        div()
            .id(SharedString::from(format!(
                "secret-value-viewer-line-{index}"
            )))
            .relative()
            .flex_shrink_0()
            .w_full()
            .min_h(line_height)
            .pl(VIEWER_GUTTER_WIDTH)
            .child(
                div()
                    .absolute()
                    .left_0()
                    .top_0()
                    .w(VIEWER_GUTTER_WIDTH)
                    .text_color(muted)
                    .child((index + 1).to_string()),
            )
            .child(div().w_full().child(line.clone()))
    });

    div()
        .relative()
        .px(DocumentMetrics::DETAIL_BLOCK_PADDING_X)
        .py(DocumentMetrics::DETAIL_BLOCK_PADDING_Y)
        .font_family(fonts::editor_family(cx))
        .text_size(DocumentMetrics::DETAIL_BLOCK_FONT)
        .line_height(relative(DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT))
        .text_color(foreground)
        .child(Chamfer::new(ChamferCut::INPUT).fill(fill).border(border))
        .child(
            div()
                .id("secret-value-viewer")
                .flex()
                .flex_col()
                .max_h(line_height * VIEWER_VISIBLE_LINES)
                .aria_hidden(true)
                .frame_redacted(true)
                .track_scroll(scroll_handle)
                .children(rows)
                .overflow_y_scroll(),
        )
        .child(Scrollbar::vertical(scroll_handle))
}

pub(super) fn render_value_failed(failure: &ValueFailure, cx: &mut Context<SecretView>) -> Div {
    let title = if failure.decrypt_denied {
        dbflux_i18n::t!("document.secret_store.value.failed_decrypt")
    } else {
        dbflux_i18n::t!("document.secret_store.value.failed")
    };

    let actions = div()
        .flex()
        .gap(DocumentMetrics::GAP)
        .child(
            Button::new(
                "secret-value-copy-error",
                dbflux_i18n::t!("document.secret_store.value.copy_error"),
            )
            .icon(AppIcon::Copy)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.copy_value_error(cx))),
        )
        .child(
            Button::new(
                "secret-value-view-in-audit",
                dbflux_i18n::t!("document.secret_store.value.view_in_audit"),
            )
            .icon(AppIcon::ScrollText)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.view_value_error_in_audit(cx))),
        );

    div()
        .flex()
        .flex_col()
        .gap(SecretStoreMetrics::ROW_GAP)
        .child(
            BannerBlock::new(BannerVariant::Danger, title)
                .with_pre(failure.message.clone())
                .with_actions(actions),
        )
        .child(
            Text::caption(dbflux_i18n::t!(
                "document.secret_store.value.metadata_loaded"
            ))
            .muted_foreground(),
        )
}
