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
    Badge, BadgeTone, BannerBlock, BannerVariant, Chamfer, SegmentedControl, SegmentedItem, Text,
};
use dbflux_components::tokens::{ChamferCut, DocumentMetrics, SecretStoreMetrics};
use dbflux_components::vim::VimBinding;
use dbflux_core::secrecy::ExposeSecret;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::EditorState;

use super::line_diff::{LineChanges, changed_lines};
use super::{EditMode, ValueEditState};
use crate::buckets_table::format_bytes;
use crate::object_text::{FIND_SHORTCUT_HINT, keycap_text};
use crate::secret_store::ValueState;
use crate::secret_store::view::SecretView;
use crate::secret_store::view::details::VIEWER_GUTTER_WIDTH;
use crate::secret_store::view::details::VIEWER_VISIBLE_LINES;

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

impl SecretView {
    /// The editor of a plaintext value: the header row, the editor, the
    /// footer, and the hint.
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
            .child(self.render_editor_body(false, cx))
            .child(self.render_editor_footer(&editor, &changes, write_size, cx))
            .children(self.editor_changed_elsewhere().then(|| {
                div().id("secret-value-editor-changed-elsewhere").child(
                    Text::caption(dbflux_i18n::t!(
                        "document.secret_store.edit.changed_elsewhere"
                    ))
                    .warning(),
                )
            }))
            .child(Text::caption(dbflux_i18n::t!(hint_key)).muted_foreground())
    }

    fn render_editor_header(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .flex_wrap()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .children(self.editor_modified_badge(cx))
            .child(div().flex_1())
            .child(self.render_editor_actions(cx))
    }

    /// The "modified" badge, while the text differs from the stored one.
    fn editor_modified_badge(&self, cx: &App) -> Option<AnyElement> {
        let edit = self.value_edit.as_ref()?;

        edit.is_changed(cx).then(|| {
            Badge::new(
                dbflux_i18n::t!("document.secret_store.edit.modified"),
                BadgeTone::Warning,
            )
            .id("secret-value-editor-modified")
            .into_any_element()
        })
    }

    /// The editor's controls: Formatted | Raw, Find, Discard and Save.
    fn render_editor_actions(&self, cx: &mut Context<Self>) -> Div {
        let Some(edit) = &self.value_edit else {
            return div();
        };

        let unchanged = !edit.is_changed(cx);
        let saving = edit.saving;
        let locked = self.draft_is_locked();
        let toggle = self
            .edit_mode()
            .map(|mode| self.render_edit_mode_toggle(mode, saving || locked, cx));
        let save_shortcut = dbflux_ui_base::effective_keymap()
            .shortcut_for_command(self.host_context, Command::SaveQuery);

        let mut save = Button::new(
            "secret-value-editor-save",
            dbflux_i18n::t!("document.secret_store.edit.save"),
        )
        .primary()
        .tab_stop(false)
        .disabled(unchanged || saving || locked)
        .on_click(cx.listener(|this, _, _, cx| this.save_value_edit(cx)));

        if let Some(shortcut) = save_shortcut {
            save = save.kbd(shortcut);
        }

        div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
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

    /// The editor component itself, wired for Vim and kept out of the
    /// accessibility tree and the rendered frame: its text is the secret.
    /// In the tab it fills the room it is given; in the pane it keeps a
    /// capped height so the sections below it keep their place.
    fn render_editor_body(&self, fill: bool, cx: &mut Context<Self>) -> AnyElement {
        let Some(edit) = &self.value_edit else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let background = theme.background;
        let border = theme.border;
        let input_id = edit.editor.entity_id();
        let saving = edit.saving;
        let focus = edit.focus.clone();
        let line_height = fonts::editor_line_height(cx);

        let editor = div()
            .id("secret-value-editor")
            .flex_1()
            .min_h_0()
            .min_w_0()
            .aria_hidden(true)
            .frame_redacted(true)
            .bg(background)
            .child(
                edit.vim
                    .editor(saving || self.draft_is_locked())
                    .redacted(true)
                    .appearance(false)
                    .w_full()
                    .h_full(),
            );
        let indicator = edit.vim.render_indicator(cx);
        let wrapper = edit
            .vim
            .leader_scope(div().flex_1().min_h_0().flex().flex_col(), cx);

        let body =
            VimBinding::capture_run_command(VimBinding::wire(wrapper, input_id, cx), input_id, cx)
                .track_focus(&focus)
                .child(editor)
                .children(indicator);

        if fill {
            return body.size_full().into_any_element();
        }

        div()
            .h(line_height * VIEWER_VISIBLE_LINES)
            .flex()
            .flex_col()
            .border_1()
            .border_color(border)
            .child(body)
            .into_any_element()
    }

    /// Encoding, size of what Save would write, the version being edited,
    /// the number of changed lines and the caret. Counts and ids only.
    fn render_editor_footer(
        &self,
        editor: &Entity<EditorState>,
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

        let changed_label = changed_lines_label(changes.changed());

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
                        details.child(footer_version(version))
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

/// "N changed lines", `None` while nothing differs from the stored text.
fn changed_lines_label(changed: usize) -> Option<String> {
    match changed {
        0 => None,
        1 => Some(dbflux_i18n::t!(
            "document.secret_store.edit.footer.changed_one"
        )),
        count => Some(dbflux_i18n::t!(
            "document.secret_store.edit.footer.changed_other",
            count = count.to_string()
        )),
    }
}

/// The version being edited, short in the footer and whole in its tooltip.
fn footer_version(version: String) -> Stateful<Div> {
    let short = if version.parse::<u64>().is_ok() {
        crate::secret_store::render::version_label(Some(&version))
    } else {
        crate::secret_store::view::details::short_version_id(&version)
    };
    let label = dbflux_i18n::t!("document.secret_store.edit.footer.version", version = short);

    div()
        .id("secret-value-editor-footer-version")
        .role(gpui::accesskit::Role::Label)
        .aria_label(version.clone())
        .tooltip(move |window, cx| {
            gpui_component::tooltip::Tooltip::new(version.clone()).build(window, cx)
        })
        .child(format!("· {label}"))
}

impl SecretView {
    /// The tab while a value is edited, as artboard 06 draws it: one header
    /// row (name, type, where it lives, "modified", and the controls), the
    /// editor taking the rest of the tab, and a status bar. Nothing else.
    pub(crate) fn render_tab_editor(
        &self,
        name: &str,
        subtitle: String,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(edit) = &self.value_edit else {
            return div().flex_1().into_any_element();
        };

        let name_for_notice = name.to_string();
        let removed = edit.removed;
        let scheduled = !removed && self.deletion_pending();
        let editor = edit.editor.clone();
        let text = editor.read(cx).value().to_string();
        let line_count = text.split('\n').count();
        let changes = if edit.is_changed(cx) {
            changed_lines(&edit.baseline_for_mode(), &text)
        } else {
            LineChanges::unchanged(line_count)
        };
        let write_size = edit.text_to_write(&text).len();

        let theme = cx.theme();
        let border = theme.border;
        let warning = theme.warning;
        let muted = theme.muted_foreground;
        let strong = dbflux_components::tokens::ChromeColors::strong(theme);
        let kind = self.value_kind_label();

        let header = div()
            .id("secret-document-header")
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .px(SecretStoreMetrics::DETAILS_PADDING_X)
            .py(SecretStoreMetrics::DETAILS_PADDING_Y)
            .border_b_1()
            .border_color(border)
            .child(
                dbflux_components::primitives::Icon::new(AppIcon::KeyRound)
                    .size(DocumentMetrics::TITLE_ICON)
                    .color(warning),
            )
            .child(
                div()
                    .id("secret-document-name")
                    .truncate()
                    .font_family(fonts::editor_family(cx))
                    .font_weight(FontWeight::BOLD)
                    .text_size(SecretStoreMetrics::NAME_FONT)
                    .text_color(strong)
                    .child(name.to_string()),
            )
            .children(kind.map(|kind| Badge::new(kind, BadgeTone::Accent).id("secret-value-kind")))
            .child(
                div()
                    .id("secret-document-subtitle")
                    .min_w_0()
                    .truncate()
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(muted)
                    .child(subtitle),
            )
            .children(self.editor_modified_badge(cx))
            .child(div().flex_1())
            .child(self.render_editor_actions(cx));

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(header)
            .children(removed.then(|| {
                div().id("secret-document-removed").child(
                    BannerBlock::new(
                        BannerVariant::Warning,
                        dbflux_i18n::t!("document.secret.removed.draft_title"),
                    )
                    .with_body(dbflux_i18n::t!(
                        "document.secret.removed.draft_body",
                        name = name
                    )),
                )
            }))
            .children(scheduled.then(|| self.render_draft_scheduled_notice(&name_for_notice, cx)))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .child(self.render_editor_body(true, cx)),
            )
            .child(self.render_tab_editor_status(&editor, &changes, write_size, cx))
            .into_any_element()
    }

    /// The status bar of the tab's editor: type, encoding, size and version
    /// (with the labels it holds), the changed lines, what saving does, and
    /// the caret on the right. Counts and ids only, never the text.
    fn render_tab_editor_status(
        &self,
        editor: &Entity<EditorState>,
        changes: &LineChanges,
        write_size: usize,
        cx: &App,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let border = theme.border;
        let family = fonts::editor_family(cx);

        let kind = self
            .value_kind_label()
            .map(|label| label.split(" · ").next().unwrap_or_default().to_string());
        let summary = dbflux_i18n::t!(
            "document.secret_store.edit.footer.summary_unversioned",
            size = format_bytes(write_size as u64)
        );
        let version = match &self.value {
            ValueState::Loaded(value) => value.version.clone(),
            _ => None,
        };
        let labels = version.as_deref().and_then(|id| self.version_labels_of(id));

        let caret = editor.read(cx).cursor_position();
        let caret_label = dbflux_i18n::t!(
            "document.secret_store.edit.footer.caret",
            line = (caret.line + 1).to_string(),
            column = (caret.character + 1).to_string()
        );

        let separated = |id: &'static str, text: String| {
            div().id(id).whitespace_nowrap().child(format!("· {text}"))
        };

        div()
            .id("secret-document-editor-status")
            .flex()
            .flex_shrink_0()
            .flex_wrap()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .px(SecretStoreMetrics::DETAILS_PADDING_X)
            .py(SecretStoreMetrics::DETAILS_PADDING_Y)
            .border_t_1()
            .border_color(border)
            .text_size(DocumentMetrics::TABLE_META_FONT)
            .text_color(muted)
            .children(kind.map(|kind| div().id("secret-document-editor-kind").child(kind)))
            .child(separated("secret-value-editor-footer-summary", summary))
            .when_some(version, |status, version| {
                status.child(footer_version_with_labels(version, labels))
            })
            .children(
                changed_lines_label(changes.changed())
                    .map(|label| separated("secret-value-editor-footer-changes", label)),
            )
            .children(self.editor_changed_elsewhere().then(|| {
                div()
                    .id("secret-value-editor-changed-elsewhere")
                    .text_color(cx.theme().warning)
                    .whitespace_nowrap()
                    .child(format!(
                        "· {}",
                        dbflux_i18n::t!("document.secret_store.edit.changed_elsewhere")
                    ))
            }))
            .child(separated(
                "secret-document-editor-saving-note",
                dbflux_i18n::t!("document.secret_store.edit.footer.saving_note"),
            ))
            .child(div().flex_1())
            .child(
                div()
                    .id("secret-value-editor-footer-caret")
                    .font_family(family)
                    .child(caret_label),
            )
    }

    /// Whether the open editor was told the secret changed in another view.
    fn editor_changed_elsewhere(&self) -> bool {
        self.value_edit
            .as_ref()
            .is_some_and(|edit| edit.changed_elsewhere)
    }

    /// The staging labels the history holds for `version`, joined, when it
    /// has any.
    fn version_labels_of(&self, version: &str) -> Option<String> {
        let crate::secret_store::VersionsState::Loaded(versions) = &self.versions else {
            return None;
        };

        versions
            .iter()
            .find(|candidate| candidate.id == version)
            .filter(|candidate| !candidate.labels.is_empty())
            .map(|candidate| candidate.labels.join(", "))
    }
}

/// [`footer_version`] with the version's labels after it: "· version a41c09e2
/// (AWSCURRENT)".
fn footer_version_with_labels(version: String, labels: Option<String>) -> Stateful<Div> {
    let short = if version.parse::<u64>().is_ok() {
        crate::secret_store::render::version_label(Some(&version))
    } else {
        crate::secret_store::view::details::short_version_id(&version)
    };
    let label = dbflux_i18n::t!("document.secret_store.edit.footer.version", version = short);
    let text = match labels {
        Some(labels) => format!("· {label} ({labels})"),
        None => format!("· {label}"),
    };

    div()
        .id("secret-value-editor-footer-version")
        .role(gpui::accesskit::Role::Label)
        .aria_label(version.clone())
        .tooltip(move |window, cx| {
            gpui_component::tooltip::Tooltip::new(version.clone()).build(window, cx)
        })
        .whitespace_nowrap()
        .child(text)
}

impl SecretView {
    /// The notice over a read-only draft whose secret is scheduled for
    /// deletion, with Restore: the draft becomes editable again once it is.
    fn render_draft_scheduled_notice(&self, name: &str, cx: &mut Context<Self>) -> Stateful<Div> {
        let restore = self.supports_restore().then(|| {
            Button::new(
                "secret-restore",
                dbflux_i18n::t!("document.secret_store.restore.button"),
            )
            .primary()
            .icon(AppIcon::RotateCcw)
            .tab_stop(false)
            .disabled(!self.can_restore_selected())
            .on_click(cx.listener(|this, _, _, cx| this.restore_selected(cx)))
        });
        let mut banner = BannerBlock::new(
            BannerVariant::Warning,
            dbflux_i18n::t!("document.secret.scheduled.draft_title"),
        )
        .with_body(dbflux_i18n::t!(
            "document.secret.scheduled.draft_body",
            name = name
        ));

        if let Some(restore) = restore {
            banner = banner.with_actions(restore);
        }

        div().id("secret-document-deletion-scheduled").child(banner)
    }
}
