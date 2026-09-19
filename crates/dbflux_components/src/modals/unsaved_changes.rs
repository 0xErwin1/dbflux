use crate::modals::shell::{ModalShell, ModalVariant};
use crate::primitives::Text;
use crate::tokens::{FontSizes, Spacing};
use dbflux_core::document_id::DocumentId;
use gpui::prelude::*;
use gpui::{Context, EventEmitter, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::button::{Button, ButtonVariants};
use std::collections::HashMap;

/// Event emitted when the user resolves the modal.
#[derive(Clone, Debug)]
pub enum UnsavedChangesOutcome {
    /// User chose "Don't save" — caller should discard changes and close the
    /// listed documents. Carries exactly the documents the modal was opened
    /// for, so a caller asked about one tab cannot close the others.
    DiscardAll(Vec<DocumentId>),
    /// User chose "Cancel" — abort the close/quit flow.
    Cancelled,
    /// User chose "Save selected" — caller should save the given document IDs.
    SaveSelected(Vec<DocumentId>),
}

/// What closing a document does with its pending edits.
///
/// The dialog names the action per entry: saving a document's own file and
/// applying a table's staged edits are not the same thing to the reader, even
/// when both mean "keep my work".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseAction {
    /// Write the document's own file.
    Save,
    /// Run the grid's staged edits against the database.
    Apply,
}

/// One dirty document entry passed when opening the modal.
#[derive(Clone, Debug)]
pub struct DirtySummaryEntry {
    pub id: DocumentId,
    pub name: String,
    pub summary: String,
    /// What this entry's pending edits are brought to when the dialog's confirm
    /// action runs.
    pub action: CloseAction,
}

/// Request payload for `pending_modal_open` on the workspace.
#[derive(Clone, Debug)]
pub struct UnsavedChangesRequest {
    pub entries: Vec<DirtySummaryEntry>,
}

/// Modal entity for the "unsaved changes" confirmation.
///
/// Uses `ModalShell::Default` (520 px).
pub struct ModalUnsavedChanges {
    entries: Vec<DirtySummaryEntry>,
    selected: HashMap<DocumentId, bool>,
    visible: bool,
}

impl ModalUnsavedChanges {
    pub fn new(_cx: &mut Context<Self>) -> Self {
        Self {
            entries: Vec::new(),
            selected: HashMap::new(),
            visible: false,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: UnsavedChangesRequest, cx: &mut Context<Self>) {
        self.selected = request.entries.iter().map(|e| (e.id, true)).collect();
        self.entries = request.entries;
        self.visible = true;
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.entries.clear();
        self.selected.clear();
        cx.notify();
    }

    /// Number of currently-selected entries.
    pub fn selected_count(&self) -> usize {
        self.selected.values().filter(|&&v| v).count()
    }

    /// Resolve the modal as if "Save selected" was clicked: emit the checked
    /// ids and close. The keyboard path (ConfirmModal keymap) uses this so
    /// Enter resolves the modal through the same outcome handler as a mouse
    /// click.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        // The Save button is disabled at zero selections; Enter must not close
        // the dialog over an operation the user cannot trigger either.
        if self.selected_count() == 0 {
            return;
        }

        let ids = self.selected_ids();
        cx.emit(UnsavedChangesOutcome::SaveSelected(ids));
        self.close(cx);
    }

    /// Resolve the modal as if the cancel button was clicked.
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(UnsavedChangesOutcome::Cancelled);
        self.close(cx);
    }

    fn toggle(&mut self, id: DocumentId, cx: &mut Context<Self>) {
        let entry = self.selected.entry(id).or_insert(false);
        *entry = !*entry;
        cx.notify();
    }

    fn selected_ids(&self) -> Vec<DocumentId> {
        self.selected
            .iter()
            .filter_map(|(id, &checked)| if checked { Some(*id) } else { None })
            .collect()
    }

    /// The action of every selected entry, in no particular order.
    fn selected_actions(&self) -> Vec<CloseAction> {
        self.entries
            .iter()
            .filter(|entry| self.selected.get(&entry.id).copied().unwrap_or(false))
            .map(|entry| entry.action)
            .collect()
    }
}

/// What an entry's pending edits are, with the verb that names the action.
fn action_summary(action: CloseAction, summary: &str) -> String {
    let verb = match action {
        CloseAction::Save => dbflux_i18n::t!("modals.unsaved_changes.action.save"),
        CloseAction::Apply => dbflux_i18n::t!("modals.unsaved_changes.action.apply"),
    };

    format!("{verb} · {summary}")
}

/// Label for the confirm button, with the selected count interpolated.
///
/// The verb follows what the selected entries do: a selection of grids is
/// applied, not saved. Any other selection keeps the save wording the footer has
/// always used, including a mixed one, because each entry's own line is what
/// names the action it takes.
///
/// Uses the singular catalog bucket only for exactly one selected document;
/// every other count, including zero, uses the plural bucket.
fn confirm_label(actions: &[CloseAction]) -> String {
    let count = actions.len();
    let applies = !actions.is_empty() && actions.iter().all(|action| *action == CloseAction::Apply);

    match (applies, count == 1) {
        (true, true) => dbflux_i18n::t!("modals.unsaved_changes.apply_selected.one", count = count),
        (true, false) => {
            dbflux_i18n::t!("modals.unsaved_changes.apply_selected.many", count = count)
        }
        (false, true) => dbflux_i18n::t!("modals.unsaved_changes.save_selected.one", count = count),
        (false, false) => {
            dbflux_i18n::t!("modals.unsaved_changes.save_selected.many", count = count)
        }
    }
}

impl EventEmitter<UnsavedChangesOutcome> for ModalUnsavedChanges {}

impl Render for ModalUnsavedChanges {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let selected_count = self.selected_count();

        let mut rows = div().flex().flex_col().gap(Spacing::XS);

        for (row_idx, entry) in self.entries.iter().enumerate() {
            let id = entry.id;
            let is_checked = self.selected.get(&id).copied().unwrap_or(false);
            let name = entry.name.clone();
            let summary = action_summary(entry.action, &entry.summary);
            let check_color = if is_checked {
                theme.primary
            } else {
                theme.border
            };

            rows = rows.child(
                div()
                    .id(("unsaved-row", row_idx))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(px(3.0))
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.list_active))
                    .on_click(cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
                        this.toggle(id, cx);
                    }))
                    // Checkbox indicator
                    .child(
                        div()
                            .w(px(14.0))
                            .h(px(14.0))
                            .rounded(px(2.0))
                            .border_1()
                            .border_color(check_color)
                            .flex()
                            .items_center()
                            .justify_center()
                            .when(is_checked, |el| {
                                el.bg(theme.primary).child(
                                    div()
                                        .w(Spacing::SM)
                                        .h(Spacing::SM)
                                        .text_size(FontSizes::XS)
                                        .text_color(theme.background)
                                        .child("✓"),
                                )
                            }),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .flex_1()
                            .gap(px(1.0))
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.foreground)
                                    .child(name),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child(summary),
                            ),
                    ),
            );
        }

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(Text::body(dbflux_i18n::t!("modals.unsaved_changes.prompt")).into_any_element())
            .child(rows);

        let on_discard = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            let ids = this.entries.iter().map(|entry| entry.id).collect();
            cx.emit(UnsavedChangesOutcome::DiscardAll(ids));
            this.close(cx);
        });

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(UnsavedChangesOutcome::Cancelled);
            this.close(cx);
        });

        let save_label = confirm_label(&self.selected_actions());
        let save_disabled = selected_count == 0;

        let on_save = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
            let ids = this.selected_ids();
            cx.emit(UnsavedChangesOutcome::SaveSelected(ids));
            this.close(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new("unsaved-discard")
                    .label(dbflux_i18n::t!("modals.unsaved_changes.dont_save"))
                    .ghost()
                    .on_click(on_discard),
            )
            .child(div().flex_1())
            .child(
                Button::new("unsaved-cancel")
                    .label(dbflux_i18n::t!("modals.unsaved_changes.cancel"))
                    .on_click(on_cancel),
            )
            .child(if save_disabled {
                div()
                    .flex()
                    .items_center()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(px(4.0)) // guardrail-allow: border radius, not spacing
                    .opacity(0.4)
                    .bg(theme.primary)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.background)
                            .child(save_label),
                    )
                    .into_any_element()
            } else {
                Button::new("unsaved-save")
                    .label(save_label)
                    .primary()
                    .on_click(on_save)
                    .into_any_element()
            });

        ModalShell::new(
            dbflux_i18n::t!("modals.unsaved_changes.title"),
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Default)
        .width(px(520.0))
        .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Unit tests — pure logic, no GPUI context required
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn make_id() -> DocumentId {
        DocumentId(Uuid::new_v4())
    }

    fn make_entry(id: DocumentId, name: &str) -> DirtySummaryEntry {
        DirtySummaryEntry {
            id,
            name: name.to_string(),
            summary: "+3/-1 lines".to_string(),
            action: CloseAction::Save,
        }
    }

    fn modal_with(entries: Vec<DirtySummaryEntry>) -> ModalUnsavedChanges {
        let selected: HashMap<DocumentId, bool> = entries.iter().map(|e| (e.id, true)).collect();
        ModalUnsavedChanges {
            entries,
            selected,
            visible: true,
        }
    }

    #[test]
    fn selected_count_all_checked() {
        let a = make_id();
        let b = make_id();
        let modal = modal_with(vec![make_entry(a, "query.sql"), make_entry(b, "notes.sql")]);
        assert_eq!(modal.selected_count(), 2);
    }

    #[test]
    fn selected_count_none_checked() {
        let a = make_id();
        let mut modal = modal_with(vec![make_entry(a, "query.sql")]);
        modal.selected.insert(a, false);
        assert_eq!(modal.selected_count(), 0);
    }

    #[test]
    fn save_selected_disabled_when_zero_checked() {
        let a = make_id();
        let mut modal = modal_with(vec![make_entry(a, "query.sql")]);
        modal.selected.insert(a, false);
        assert_eq!(
            modal.selected_count(),
            0,
            "count zero means button is disabled"
        );
    }

    #[test]
    fn selected_ids_returns_only_checked() {
        let a = make_id();
        let b = make_id();
        let mut modal = modal_with(vec![make_entry(a, "a.sql"), make_entry(b, "b.sql")]);
        modal.selected.insert(b, false);
        let ids = modal.selected_ids();
        assert_eq!(ids, vec![a]);
    }

    #[test]
    fn toggle_flips_state() {
        // Toggle without a GPUI context — we test the HashMap directly.
        let a = make_id();
        let mut modal = modal_with(vec![make_entry(a, "a.sql")]);
        assert!(modal.selected[&a]);
        *modal.selected.entry(a).or_insert(false) ^= true;
        assert!(!modal.selected[&a]);
        *modal.selected.entry(a).or_insert(false) ^= true;
        assert!(modal.selected[&a]);
    }

    #[test]
    fn unsaved_changes_keys_resolve_in_both_locales() {
        let keys = [
            "modals.unsaved_changes.title",
            "modals.unsaved_changes.prompt",
            "modals.unsaved_changes.save_selected.one",
            "modals.unsaved_changes.save_selected.many",
            "modals.unsaved_changes.cannot_save.one",
            "modals.unsaved_changes.cannot_save.many",
            "modals.unsaved_changes.dont_save",
            "modals.unsaved_changes.cancel",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn unsaved_changes_dont_save_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.unsaved_changes.dont_save", locale = "en");
        let es = dbflux_i18n::t!("modals.unsaved_changes.dont_save", locale = "es");
        assert_ne!(en, es);
    }

    #[test]
    fn confirm_label_uses_singular_bucket_for_one() {
        let label = confirm_label(&[CloseAction::Save]);
        assert!(label.contains('1'));
        assert_eq!(
            label,
            dbflux_i18n::t!("modals.unsaved_changes.save_selected.one", count = 1)
        );
    }

    #[test]
    fn confirm_label_uses_the_plural_bucket_for_zero_and_many() {
        let zero = confirm_label(&[]);
        assert!(zero.contains('0'));

        let many = confirm_label(&[CloseAction::Save, CloseAction::Save]);
        assert!(many.contains('2'));
        assert_eq!(
            many,
            dbflux_i18n::t!("modals.unsaved_changes.save_selected.many", count = 2)
        );
    }

    /// A selection of grids is applied, and the footer is where that verb shows.
    #[test]
    fn confirm_label_uses_the_apply_verb_for_a_selection_of_grids() {
        let label = confirm_label(&[CloseAction::Apply, CloseAction::Apply]);

        assert_eq!(
            label,
            dbflux_i18n::t!("modals.unsaved_changes.apply_selected.many", count = 2),
            "two grids are applied, not saved"
        );
        assert_eq!(
            confirm_label(&[CloseAction::Apply]),
            dbflux_i18n::t!("modals.unsaved_changes.apply_selected.one", count = 1)
        );
    }

    /// A file-backed document keeps the save wording, and a mixed selection keeps
    /// it too: each entry's own line is what names its action.
    #[test]
    fn confirm_label_keeps_the_save_verb_for_every_other_selection() {
        let saved = confirm_label(&[CloseAction::Save]);
        let mixed = confirm_label(&[CloseAction::Save, CloseAction::Apply]);

        assert_eq!(
            saved,
            dbflux_i18n::t!("modals.unsaved_changes.save_selected.one", count = 1)
        );
        assert_eq!(
            mixed,
            dbflux_i18n::t!("modals.unsaved_changes.save_selected.many", count = 2)
        );
    }

    /// The entry's own line carries the verb, so a reader can tell a grid from a
    /// file without opening either.
    #[test]
    fn an_entry_line_names_the_action_before_what_is_pending() {
        let applied = action_summary(CloseAction::Apply, "3 edits · 1 delete");
        let saved = action_summary(CloseAction::Save, "1 statement");

        assert!(
            applied.starts_with(&dbflux_i18n::t!("modals.unsaved_changes.action.apply")),
            "a grid's line starts with its own verb: {applied}"
        );
        assert!(applied.ends_with("3 edits · 1 delete"), "{applied}");
        assert!(
            saved.starts_with(&dbflux_i18n::t!("modals.unsaved_changes.action.save")),
            "a file's line starts with its own verb: {saved}"
        );
        assert_ne!(applied, saved);
    }
}

#[cfg(test)]
mod confirm_keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{
        CloseAction, DirtySummaryEntry, ModalUnsavedChanges, UnsavedChangesOutcome,
        UnsavedChangesRequest,
    };
    use dbflux_core::document_id::DocumentId;
    use gpui::{AppContext, TestAppContext};
    use std::cell::RefCell;
    use std::rc::Rc;
    use uuid::Uuid;

    /// Enter must behave like the Save button: with nothing checked the button
    /// is disabled, so the dialog stays open instead of resolving to a save of
    /// nothing.
    #[gpui::test]
    fn enter_with_nothing_checked_keeps_the_dialog_open(cx: &mut TestAppContext) {
        let id = DocumentId(Uuid::new_v4());
        let modal = cx.new(ModalUnsavedChanges::new);

        cx.update(|cx| {
            modal.update(cx, |modal, cx| {
                modal.open(
                    UnsavedChangesRequest {
                        entries: vec![DirtySummaryEntry {
                            id,
                            name: "query.sql".to_string(),
                            summary: "+1/-1 lines".to_string(),
                            action: CloseAction::Save,
                        }],
                    },
                    cx,
                );
                modal.selected.insert(id, false);
            });
        });

        let outcomes: Rc<RefCell<Vec<UnsavedChangesOutcome>>> = Rc::new(RefCell::new(Vec::new()));
        let sink = outcomes.clone();
        cx.update(|cx| {
            cx.subscribe(&modal, move |_, event: &UnsavedChangesOutcome, _| {
                sink.borrow_mut().push(event.clone());
            })
            .detach();
        });

        cx.update(|cx| {
            modal.update(cx, |modal, cx| modal.confirm(cx));
        });

        assert!(
            outcomes.borrow().is_empty(),
            "Enter must not resolve the dialog while nothing is selected"
        );
        let visible = cx.update(|cx| modal.read(cx).is_visible());
        assert!(visible, "the dialog stays open for the user to choose");
    }
}
