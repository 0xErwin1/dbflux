//! Edit tags: add, change and remove the tags of the selected secret.
//!
//! The dialog lists the secret's tags as editable key and value rows with a
//! remove button each and an Add tag button. Saving sends only the
//! difference — the tags that are new or changed, and the keys that are gone —
//! through [`update_secret_tags`](crate::secret_store::update_secret_tags).
//! The limits the services share (key and value length, characters, the
//! reserved `aws:` prefix, the quota of 50 tags) are the seam's own checks:
//! the dialog runs them while the user types and shows their message under
//! the rows, and sends nothing until the request passes.

use super::{
    DialogShell, FooterSpec, Landing, WriteDialog, WriteEffect, dialog_footer, dialog_modal,
};
use crate::secret_store::DetailsState;
use crate::secret_store::SecretOperation;
use crate::secret_store::view::SecretView;
use crate::secret_store::write::update_secret_tags;
use dbflux_components::controls::{Button, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{ModalMetrics, Spacing};
use dbflux_core::{MAX_TAGS, SecretStoreCapabilities, SecretTag, SecretTagsUpdate};
use gpui::prelude::*;
use gpui::*;

/// The capabilities Edit tags needs: the same pair the seam requires.
const TAGS_CAPABILITIES: SecretStoreCapabilities =
    SecretStoreCapabilities::WRITE.union(SecretStoreCapabilities::TAGS);

/// One row of the dialog: a tag being edited.
pub(in crate::secret_store) struct TagRow {
    pub(in crate::secret_store) key: Entity<InputState>,
    pub(in crate::secret_store) value: Entity<InputState>,
    _subscriptions: [Subscription; 2],
}

/// The Edit tags dialog.
pub(in crate::secret_store) struct TagsDialog {
    pub name: String,

    /// The tags the secret has now.
    original: Vec<SecretTag>,
    pub rows: Vec<TagRow>,

    /// A row was edited since the dialog opened, so a problem is worth
    /// reporting.
    touched: bool,
}

/// Builds one editable tag row. `on_change` runs on the owner of the row
/// after every edit of either field, so the owner's dialog can mark itself
/// touched.
pub(in crate::secret_store) fn new_tag_row<H: 'static>(
    key: &str,
    value: &str,
    window: &mut Window,
    cx: &mut Context<H>,
    on_change: fn(&mut H, &mut Context<H>),
) -> TagRow {
    let build = |window: &mut Window, cx: &mut Context<H>, placeholder: String, text: &str| {
        cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(placeholder);

            if !text.is_empty() {
                state.set_value(text, window, cx);
            }

            state
        })
    };

    let key_input = build(
        window,
        cx,
        dbflux_i18n::t!("document.secret_store.tags_edit.key_placeholder"),
        key,
    );
    let value_input = build(
        window,
        cx,
        dbflux_i18n::t!("document.secret_store.tags_edit.value_placeholder"),
        value,
    );

    // Every keystroke feeds the validation shown under the rows.
    let watch = |input: &Entity<InputState>, cx: &mut Context<H>| {
        cx.subscribe(input, move |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                on_change(this, cx);
                cx.notify();
            }
        })
    };
    let subscriptions = [watch(&key_input, cx), watch(&value_input, cx)];

    TagRow {
        key: key_input,
        value: value_input,
        _subscriptions: subscriptions,
    }
}

impl TagRow {
    /// The row's tag, `None` for a row left entirely empty.
    pub(in crate::secret_store) fn tag(&self, cx: &App) -> Option<SecretTag> {
        let key = self.key.read(cx).value().to_string();
        let value = self.value.read(cx).value().to_string();

        (!key.is_empty() || !value.is_empty()).then(|| SecretTag::new(key, value))
    }
}

impl TagsDialog {
    /// The tags the rows hold now, without the rows left empty.
    fn current(&self, cx: &App) -> Vec<SecretTag> {
        self.rows.iter().filter_map(|row| row.tag(cx)).collect()
    }

    /// What the rows change: the tags that are new or have a new value, and
    /// the keys that are gone. A key that appears twice is not a change the
    /// seam could take, so it is named instead.
    fn request(&self, cx: &App) -> Result<SecretTagsUpdate, TagsProblem> {
        let current = self.current(cx);

        for (index, tag) in current.iter().enumerate() {
            if current
                .iter()
                .take(index)
                .any(|earlier| earlier.key == tag.key)
            {
                return Err(TagsProblem::DuplicateKey(tag.key.clone()));
            }
        }

        let mut update = SecretTagsUpdate::new(self.name.clone());

        for tag in &current {
            if !self.original.iter().any(|known| known == tag) {
                update.set.push(tag.clone());
            }
        }

        for known in &self.original {
            if !current.iter().any(|tag| tag.key == known.key) {
                update.remove.push(known.key.clone());
            }
        }

        Ok(update)
    }

    /// Why the request cannot be sent yet.
    fn problem(&self, cx: &App) -> Option<TagsProblem> {
        let update = match self.request(cx) {
            Ok(update) => update,
            Err(problem) => return Some(problem),
        };

        if update.set.is_empty() && update.remove.is_empty() {
            return Some(TagsProblem::NoChange);
        }

        let existing: Vec<String> = self.original.iter().map(|tag| tag.key.clone()).collect();

        let refusal = update.validate().err();
        let refusal = refusal.or_else(|| update.check_quota(&existing).err());

        refusal.map(|error| TagsProblem::Refused(error.to_string()))
    }
}

/// Why the Edit tags dialog cannot send its request yet.
enum TagsProblem {
    /// Nothing differs from the secret's tags.
    NoChange,

    /// Two rows hold this key.
    DuplicateKey(String),

    /// The seam's own validation message.
    Refused(String),
}

impl TagsProblem {
    fn message(&self) -> String {
        match self {
            Self::NoChange => dbflux_i18n::t!("document.secret_store.tags_edit.no_change"),
            Self::DuplicateKey(key) => {
                dbflux_i18n::t!("document.secret_store.tags_edit.duplicate", key = key)
            }
            Self::Refused(message) => message.clone(),
        }
    }
}

impl SecretView {
    /// Whether the store can edit tags at all.
    pub(crate) fn supports_tags(&self) -> bool {
        self.store_allows(TAGS_CAPABILITIES)
    }

    /// Whether Edit tags may open now: the secret's details are loaded, so the
    /// dialog starts from the tags the store reports and not from a guess.
    pub(crate) fn can_edit_tags(&self) -> bool {
        self.supports_tags()
            && self.connection_present
            && !self.write_in_flight
            && !self.dialog_is_open()
            && self.write_refusal_reason().is_none()
            && self.name.is_some()
            && matches!(self.details, DetailsState::Loaded(_))
    }

    /// Opens the Edit tags dialog on the selected secret's tags. Nothing is
    /// written until it is confirmed.
    pub(crate) fn open_edit_tags(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_edit_tags() {
            return;
        }

        let DetailsState::Loaded(details) = &self.details else {
            return;
        };

        let name = details.summary.name.clone();
        let original = details.tags.clone();

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        let rows: Vec<TagRow> = original
            .iter()
            .map(|tag| new_tag_row(&tag.key, &tag.value, window, cx, Self::touch_tags_dialog))
            .collect();
        let focus = rows.first().map(|row| row.key.read(cx).focus_handle(cx));

        self.open_dialog(
            WriteDialog::Tags(TagsDialog {
                name,
                original,
                rows,
                touched: false,
            }),
            focus,
            window,
            cx,
        );
    }

    /// A row of the open dialog was edited: its problems are worth showing.
    fn touch_tags_dialog(&mut self, _cx: &mut Context<Self>) {
        if let Some(WriteDialog::Tags(dialog)) = &mut self.dialog {
            dialog.touched = true;
        }
    }

    /// Adds an empty row to the open dialog and moves the keyboard to its
    /// key.
    pub(crate) fn add_tag_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.write_in_flight {
            return;
        }

        let Some(WriteDialog::Tags(dialog)) = &self.dialog else {
            return;
        };

        if dialog.rows.len() >= MAX_TAGS {
            return;
        }

        let row = new_tag_row("", "", window, cx, Self::touch_tags_dialog);
        let focus = row.key.read(cx).focus_handle(cx);

        if let Some(WriteDialog::Tags(dialog)) = &mut self.dialog {
            dialog.rows.push(row);
            dialog.touched = true;
        }

        focus.focus(window, cx);
        cx.notify();
    }

    /// Removes the row at `index` from the open dialog.
    pub(crate) fn remove_tag_row(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.write_in_flight {
            return;
        }

        if let Some(WriteDialog::Tags(dialog)) = &mut self.dialog
            && index < dialog.rows.len()
        {
            dialog.rows.remove(index);
            dialog.touched = true;
            cx.notify();
        }
    }

    /// Writes the tag edit the dialog confirmed, through the seam.
    pub(in crate::secret_store) fn submit_tags(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(WriteDialog::Tags(dialog)) = &self.dialog else {
            return;
        };

        // A request the seam would refuse is not sent: the dialog already
        // shows why.
        if dialog.problem(cx).is_some() {
            return;
        }

        let Ok(update) = dialog.request(cx) else {
            return;
        };
        let name = dialog.name.clone();

        let Some(session) = self.write_session(cx) else {
            return;
        };

        self.run_governed_write(
            session,
            SecretOperation::EditTags,
            name.clone(),
            move |context| Landing::of(update_secret_tags(context, &update)),
            move |doc, landing, cx| doc.land_tags(&name, landing, cx),
            cx,
        );
    }

    fn land_tags(&mut self, name: &str, landing: Landing<()>, cx: &mut Context<Self>) {
        match landing {
            Landing::Done(()) => {
                self.dismiss_dialog(cx);
                self.toast_written(
                    dbflux_i18n::t!("document.secret_store.tags_edit.done", name = name),
                    cx,
                );
                self.refresh_after_write(name, WriteEffect::Metadata, cx);
            }
            Landing::Stands(failure) => {
                self.dismiss_dialog(cx);
                self.report_write_failure(&failure, cx);
                self.refresh_after_write(name, WriteEffect::Metadata, cx);
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }

    pub(in crate::secret_store) fn render_tags_dialog(
        &self,
        dialog: &TagsDialog,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let problem = dialog.problem(cx);
        let at_limit = dialog.rows.len() >= MAX_TAGS;

        // A form nobody has touched is not told off for having nothing to
        // save yet.
        let shown = problem
            .as_ref()
            .filter(|problem| dialog.touched || !matches!(problem, TagsProblem::NoChange))
            .map(TagsProblem::message);

        let body = self.render_tags_body(dialog, shown, at_limit, cx);

        let footer = dialog_footer(
            FooterSpec {
                cancel_id: "secret-tags-cancel",
                confirm_id: "secret-tags-confirm",
                confirm_label: dbflux_i18n::t!("document.secret_store.tags_edit.confirm"),
                confirm_icon: AppIcon::Check,
                danger: false,
                enabled: problem.is_none(),
                submitting: self.write_in_flight,
            },
            cx,
        );

        dialog_modal(
            DialogShell {
                id: "secret-tags-dialog",
                title: dbflux_i18n::t!(
                    "document.secret_store.tags_edit.title",
                    name = dialog.name.as_str()
                ),
                icon: AppIcon::Tag,
                danger: false,
                confirm_enabled: problem.is_none() && !self.write_in_flight,
            },
            body,
            footer,
            self.dialog_focus.handle(),
            cx,
        )
    }

    /// The lead, the rows, the Add tag button and the problem line.
    fn render_tags_body(
        &self,
        dialog: &TagsDialog,
        shown: Option<String>,
        at_limit: bool,
        cx: &mut Context<Self>,
    ) -> Div {
        // The request carries the rows as they were when Save was pressed, so
        // the form keeps them until the write answers.
        let locked = self.write_in_flight;
        let rows: Vec<AnyElement> = dialog
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| render_tag_row(index, row, locked, cx))
            .collect();

        let mut body = div().flex().flex_col().gap(ModalMetrics::HEADER_GAP).child(
            Text::caption(dbflux_i18n::t!("document.secret_store.tags_edit.lead"))
                .muted_foreground(),
        );

        if dialog.rows.is_empty() {
            body = body.child(
                Text::caption(dbflux_i18n::t!("document.secret_store.tags.none"))
                    .muted_foreground(),
            );
        }

        body = body.children(rows).child(
            div().flex().child(
                Button::new(
                    "secret-tags-add",
                    dbflux_i18n::t!("document.secret_store.tags_edit.add"),
                )
                .icon(AppIcon::Plus)
                .disabled(at_limit || locked)
                .on_click(cx.listener(|this, _, window, cx| this.add_tag_row(window, cx))),
            ),
        );

        if let Some(message) = shown {
            body = body.child(
                div()
                    .id("secret-tags-problem")
                    .child(Text::caption(message).danger()),
            );
        }

        body
    }
}

/// One editable tag: its key, its value and the button that removes it.
fn render_tag_row(
    index: usize,
    row: &TagRow,
    locked: bool,
    cx: &mut Context<SecretView>,
) -> AnyElement {
    div()
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .child(
            div().flex_1().min_w_0().child(
                Input::new(&row.key)
                    .id(SharedString::from(format!("secret-tags-key-{index}")))
                    .aria_label(dbflux_i18n::t!(
                        "document.secret_store.tags_edit.key_label",
                        row = index + 1
                    ))
                    .disabled(locked)
                    .w_full(),
            ),
        )
        .child(
            div().flex_1().min_w_0().child(
                Input::new(&row.value)
                    .id(SharedString::from(format!("secret-tags-value-{index}")))
                    .aria_label(dbflux_i18n::t!(
                        "document.secret_store.tags_edit.value_label",
                        row = index + 1
                    ))
                    .disabled(locked)
                    .w_full(),
            ),
        )
        .child(
            Button::new(
                SharedString::from(format!("secret-tags-remove-{index}")),
                dbflux_i18n::t!("document.secret_store.tags_edit.remove"),
            )
            .ghost()
            .icon(AppIcon::X)
            .icon_only()
            .disabled(locked)
            .on_click(cx.listener(move |this, _, _, cx| this.remove_tag_row(index, cx))),
        )
        .into_any_element()
}
