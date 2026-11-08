//! The drawing of the value editor: the header row with its controls, the
//! gutter and the text, and the footer.
//!
//! The editor element and the gutter beside it sit inside the redaction
//! boundary the multi-line viewer uses. The gutter holds digits and change
//! markers only and is hidden from the accessibility tree; the header and
//! the footer carry counts, sizes and the version id, never the text.

use dbflux_app::keymap::Command;
use dbflux_components::controls::Button;
use dbflux_components::fonts;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, Chamfer, SegmentedControl, SegmentedItem, Text,
};
use dbflux_components::tokens::{ChamferCut, DocumentMetrics, SecretStoreMetrics};
use dbflux_core::secrecy::ExposeSecret;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Textarea, TextareaState};

use super::line_diff::{LineChanges, changed_lines};
use super::{EditMode, ValueEditState};
use crate::buckets_table::format_bytes;
use crate::object_text::{FIND_SHORTCUT_HINT, keycap_text};
use crate::secret_store::details::VIEWER_GUTTER_WIDTH;
use crate::secret_store::details::VIEWER_VISIBLE_LINES;
use crate::secret_store::{SecretStoreDocument, ValueState};

/// Width of the change-marker bar beside a gutter number.
const MARKER_WIDTH: Pixels = px(3.0);

/// How much of the row the changed-line tint covers.
const TINT_OPACITY: f32 = 0.08;

impl ValueEditState {
    /// The text Save compares the edit with: what the mode being edited
    /// shows for the stored value.
    fn baseline_for_mode(&self) -> String {
        match self.mode {
            EditMode::Formatted => self.formatted_baseline(),
            EditMode::Raw => self.original.expose_secret().to_string(),
        }
    }
}

impl SecretStoreDocument {
    /// The editor of a plaintext value: the header row, the gutter beside
    /// the text, the footer, and the hint.
    pub(in crate::secret_store) fn render_value_editor(&self, cx: &mut Context<Self>) -> Div {
        let Some(edit) = &self.value_edit else {
            return div();
        };

        let editor = edit.editor.clone();
        let text = editor.read(cx).value().to_string();
        let line_count = text.split('\n').count();
        // The chip, the markers and the count follow what Save would write,
        // so indentation Formatted added is not a change.
        let changes = if edit.is_changed(cx) {
            changed_lines(&edit.baseline_for_mode(), &text)
        } else {
            LineChanges::unchanged(line_count)
        };
        let write_size = edit.text_to_write(&text).len();
        let hint_key = match self.edit_mode() {
            Some(EditMode::Formatted) => "document.secret_store.edit.hint_formatted",
            _ => "document.secret_store.edit.hint",
        };

        div()
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::ROW_GAP)
            .child(self.render_editor_header(cx))
            .child(self.render_editor_body(&editor, &changes, cx))
            .child(self.render_editor_footer(&editor, &changes, write_size, cx))
            .child(Text::caption(dbflux_i18n::t!(hint_key)).muted_foreground())
    }

    fn render_editor_header(&self, cx: &mut Context<Self>) -> Div {
        let Some(edit) = &self.value_edit else {
            return div();
        };

        let unchanged = !edit.is_changed(cx);
        let saving = edit.saving;
        let modified = !unchanged;
        let toggle = self
            .edit_mode()
            .map(|mode| self.render_edit_mode_toggle(mode, saving, cx));
        let save_shortcut = dbflux_ui_base::effective_keymap()
            .shortcut_for_command(self.active_context(), Command::SaveQuery);

        let mut save = Button::new(
            "secret-value-editor-save",
            dbflux_i18n::t!("document.secret_store.edit.save"),
        )
        .primary()
        .tab_stop(false)
        .disabled(unchanged || saving)
        .on_click(cx.listener(|this, _, _, cx| this.save_value_edit(cx)));

        if let Some(shortcut) = save_shortcut {
            save = save.kbd(shortcut);
        }

        div()
            .flex()
            .flex_wrap()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .when(modified, |header| {
                header.child(
                    Badge::new(
                        dbflux_i18n::t!("document.secret_store.edit.modified"),
                        BadgeTone::Warning,
                    )
                    .id("secret-value-editor-modified"),
                )
            })
            .child(div().flex_1())
            .when_some(toggle, |header, toggle| header.child(toggle))
            .child(
                Button::new(
                    "secret-value-editor-find",
                    dbflux_i18n::t!("document.secret_store.edit.find"),
                )
                .icon(AppIcon::Search)
                .kbd(keycap_text(FIND_SHORTCUT_HINT))
                .tab_stop(false)
                .on_click(cx.listener(|this, _, window, cx| this.find_in_value(window, cx))),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .child(
                        Button::new(
                            "secret-value-editor-discard",
                            dbflux_i18n::t!("document.secret_store.edit.discard"),
                        )
                        .tab_stop(false)
                        .disabled(saving)
                        .on_click(cx.listener(|this, _, _, cx| this.discard_value_edit(cx))),
                    )
                    .child(save),
            )
    }

    /// The Formatted / Raw switch of a value stored as JSON.
    fn render_edit_mode_toggle(
        &self,
        mode: EditMode,
        disabled: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let entity = cx.entity().downgrade();
        let active = match mode {
            EditMode::Formatted => "formatted",
            EditMode::Raw => "raw",
        };

        SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    "formatted",
                    dbflux_i18n::t!("document.secret_store.edit.mode.formatted"),
                ),
                SegmentedItem::new(
                    "raw",
                    dbflux_i18n::t!("document.secret_store.edit.mode.raw"),
                ),
            ],
            active,
            move |id, window, cx| {
                let mode = if id.as_ref() == "raw" {
                    EditMode::Raw
                } else {
                    EditMode::Formatted
                };

                if let Some(doc) = entity.upgrade() {
                    doc.update(cx, |doc, cx| doc.set_edit_mode(mode, window, cx));
                }
            },
        )
        .group("secret-editor-mode")
        .disabled(disabled)
        .into_any_element()
    }

    fn render_editor_body(
        &self,
        editor: &Entity<TextareaState>,
        changes: &LineChanges,
        cx: &App,
    ) -> Div {
        let theme = cx.theme();
        let foreground = theme.foreground;
        let fill = theme.popover;
        let border = theme.border;
        let muted = theme.muted_foreground;
        let warning = theme.warning;

        let state = editor.read(cx);
        let line_count = changes.marked.len().max(1);
        let offset = state.scroll_offset().y;
        let line_height = state
            .line_height()
            .unwrap_or_else(|| super::fallback_line_height(cx));
        // Only the rows the viewport can show are drawn, however long the
        // value is; the margin covers the frame the scroll lags behind.
        let first_row = (-offset / line_height).floor().max(0.0) as usize;
        let last_row = (first_row + VIEWER_VISIBLE_LINES as usize + 2).min(line_count);
        let first_row = first_row.min(last_row);
        let rows_top = offset + line_height * first_row as f32;
        let rows_origin = self
            .value_edit
            .as_ref()
            .map(|edit| edit.rows_origin.clone())
            .unwrap_or_default();
        // The text viewport inside the row area: lower than the top when the
        // Find panel is open. Rows are placed and clipped against it.
        // `text_bounds()` already includes the scroll offset, so the origin
        // comes from the unscrolled input bounds. Before the first layout
        // there are none: the text starts at the textarea's top padding.
        let viewport_top = match state.text_bounds() {
            Some(_) => (state.input_bounds().origin.y - rows_origin.get()).max(px(0.0)),
            None => gpui_component::Size::default().input_py(),
        };
        let viewport_height = match state.input_bounds().size.height {
            height if height > px(0.0) => height,
            _ => line_height * VIEWER_VISIBLE_LINES,
        };
        let focus = self.value_edit.as_ref().map(|edit| edit.focus.clone());

        let tint = div()
            .id("secret-value-editor-tint")
            .absolute()
            .top(viewport_top)
            .left_0()
            .w_full()
            .h(viewport_height)
            .overflow_hidden()
            .child(
                div()
                    .absolute()
                    .top(rows_top)
                    .left_0()
                    .w_full()
                    .flex()
                    .flex_col()
                    .children(
                        changes
                            .marked
                            .iter()
                            .skip(first_row)
                            .take(last_row - first_row)
                            .map(|marked| {
                                div()
                                    .h(line_height)
                                    .w_full()
                                    .when(*marked, |row| row.bg(warning.opacity(TINT_OPACITY)))
                            }),
                    ),
            );

        let gutter = div()
            .id("secret-value-editor-gutter")
            .relative()
            .flex_shrink_0()
            .aria_hidden(true)
            .text_color(muted)
            .child(
                div()
                    .absolute()
                    .top(viewport_top)
                    .left_0()
                    .w_full()
                    .h(viewport_height)
                    .overflow_hidden()
                    .child(
                        div()
                            .absolute()
                            .top(rows_top)
                            .left_0()
                            .flex()
                            .flex_col()
                            .children((first_row..last_row).map(|index| {
                                let marked = changes.marked.get(index).copied().unwrap_or(false);

                                div()
                                    .id(SharedString::from(format!(
                                        "secret-value-editor-line-{index}"
                                    )))
                                    .flex()
                                    .h(line_height)
                                    .child(
                                        div()
                                            .w(VIEWER_GUTTER_WIDTH)
                                            .pr(DocumentMetrics::GAP)
                                            .flex()
                                            .justify_end()
                                            .child(
                                                div()
                                                    .id(SharedString::from(format!(
                                                        "secret-value-editor-line-{index}-number"
                                                    )))
                                                    .child((index + 1).to_string()),
                                            ),
                                    )
                                    .child(
                                        div()
                                            .w(MARKER_WIDTH)
                                            .mr(DocumentMetrics::GAP)
                                            .h_full()
                                            .when(marked, |bar| {
                                                bar.child(
                                            div()
                                                .id(SharedString::from(format!(
                                                    "secret-value-editor-line-{index}-marker"
                                                )))
                                                .size_full()
                                                .bg(warning),
                                        )
                                            }),
                                    )
                            })),
                    ),
            )
            // Reserves the gutter's width; its rows are positioned over it.
            .child(
                div()
                    .invisible()
                    .flex()
                    .h(line_height)
                    .child(div().w(VIEWER_GUTTER_WIDTH).pr(DocumentMetrics::GAP))
                    .child(div().w(MARKER_WIDTH).mr(DocumentMetrics::GAP)),
            );

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
                    .relative()
                    .flex()
                    .when_some(focus, |block, focus| block.track_focus(&focus))
                    .child(
                        canvas(
                            {
                                let rows_origin = rows_origin.clone();
                                move |bounds, window, _| {
                                    if rows_origin.replace(bounds.origin.y) != bounds.origin.y {
                                        window.refresh();
                                    }
                                }
                            },
                            |_, _, _, _| {},
                        )
                        .absolute()
                        .size_full(),
                    )
                    .child(tint)
                    .child(gutter)
                    .child(
                        div()
                            .id("secret-value-editor")
                            .flex_1()
                            .min_w_0()
                            .aria_hidden(true)
                            .frame_redacted(true)
                            .child(
                                Textarea::new(editor)
                                    .redacted(true)
                                    .appearance(false)
                                    .bordered(false)
                                    .w_full(),
                            ),
                    ),
            )
    }

    /// Encoding, size of what Save would write, the version being edited,
    /// the number of changed lines and the caret. Counts and ids only.
    fn render_editor_footer(
        &self,
        editor: &Entity<TextareaState>,
        changes: &LineChanges,
        write_size: usize,
        cx: &App,
    ) -> Stateful<Div> {
        let muted = cx.theme().muted_foreground;
        let family = fonts::editor_family(cx);
        let size = format_bytes(write_size as u64);

        let version = match &self.value {
            ValueState::Loaded(value) => value.version.clone(),
            _ => None,
        };
        let summary = dbflux_i18n::t!(
            "document.secret_store.edit.footer.summary_unversioned",
            size = size
        );

        let changed = changes.changed();
        let changed_label = match changed {
            0 => None,
            1 => Some(dbflux_i18n::t!(
                "document.secret_store.edit.footer.changed_one"
            )),
            count => Some(dbflux_i18n::t!(
                "document.secret_store.edit.footer.changed_other",
                count = count.to_string()
            )),
        };

        let caret = editor.read(cx).cursor_position();
        let caret_label = dbflux_i18n::t!(
            "document.secret_store.edit.footer.caret",
            line = (caret.line + 1).to_string(),
            column = (caret.character + 1).to_string()
        );

        // The footer is two groups that wrap as wholes, so a wrapped line
        // breaks between groups. Inside a group each item after the first
        // carries its own separator, so no line starts with a bare "·".
        let group = |id: &'static str| {
            div()
                .id(id)
                .flex()
                .flex_shrink_0()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .whitespace_nowrap()
        };
        let has_changes = changed_label.is_some();

        div()
            .id("secret-value-editor-footer")
            .flex()
            .flex_wrap()
            .items_center()
            .gap_x(DocumentMetrics::GAP)
            .text_size(DocumentMetrics::TABLE_META_FONT)
            .text_color(muted)
            .child(
                group("secret-value-editor-footer-details")
                    .child(
                        div()
                            .id("secret-value-editor-footer-summary")
                            .child(summary),
                    )
                    .when_some(version, |details, version| {
                        let short = if version.parse::<u64>().is_ok() {
                            crate::secret_store::render::version_label(Some(&version))
                        } else {
                            crate::secret_store::details::short_version_id(&version)
                        };
                        let label = dbflux_i18n::t!(
                            "document.secret_store.edit.footer.version",
                            version = short
                        );

                        details.child(
                            div()
                                .id("secret-value-editor-footer-version")
                                .role(gpui::accesskit::Role::Label)
                                .aria_label(version.clone())
                                .tooltip(move |window, cx| {
                                    gpui_component::tooltip::Tooltip::new(version.clone())
                                        .build(window, cx)
                                })
                                .child(format!("· {label}")),
                        )
                    }),
            )
            .child(div().flex_1())
            .child(
                group("secret-value-editor-footer-position")
                    .when_some(changed_label, |position, label| {
                        position.child(div().id("secret-value-editor-footer-changes").child(label))
                    })
                    .child(
                        div()
                            .id("secret-value-editor-footer-caret")
                            .font_family(family)
                            .child(if has_changes {
                                format!("· {caret_label}")
                            } else {
                                caret_label
                            }),
                    ),
            )
    }
}
