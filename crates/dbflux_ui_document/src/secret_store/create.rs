//! New secret and New parameter: creates a secret through
//! [`create_secret`](crate::secret_store::create_secret).
//!
//! The dialog asks for the name, the value, a description, the tags and, for
//! a store whose values are typed, the type, the tier and the encryption key.
//! A hierarchical store starts the name at the path being listed. The value
//! is typed into the redacted text area the editor uses: it never reaches an
//! element id, the accessibility tree or a rendered frame. A name the listing
//! already holds is refused before anything is sent, and a name the listing
//! does not show is refused by the store, never overwritten.

use crate::secret_store::view::{
    DialogHost, DialogShell, FooterSpec, TagRow, WriteDialog, WriteEffect, dialog_footer,
    dialog_modal, new_tag_row,
};
use crate::secret_store::{SecretRowKey, SecretStoreDocument};
use dbflux_components::controls::{Button, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::parts::modal_field;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{ModalMetrics, Spacing};
use dbflux_core::{MAX_TAGS, SecretCreateChoices, SecretCreateRequest, SecretStoreCapabilities};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Textarea, TextareaState};

/// The capabilities a creation needs: the same pair the seam requires.
const CREATE_CAPABILITIES: SecretStoreCapabilities =
    SecretStoreCapabilities::WRITE.union(SecretStoreCapabilities::CREATE);

/// What the document showed when a create was sent: the level, the filter and
/// the selection, so its landing can tell that the user moved on.
#[derive(PartialEq, Eq)]
pub(super) struct ListingView {
    path: Option<String>,
    path_mode: crate::secret_store::PathMode,
    name_filter: String,
    selected: Option<SecretRowKey>,
}

/// The New secret dialog.
pub(super) struct CreateDialog {
    pub(in crate::secret_store) name: Entity<InputState>,
    pub(in crate::secret_store) value: Entity<TextareaState>,
    description: Entity<InputState>,
    key: Entity<InputState>,
    tags: Vec<TagRow>,

    /// What the store declares for a create: present when its values are
    /// typed, so the type and the tier are asked from it.
    choices: Option<SecretCreateChoices>,
    value_type: String,
    pub(in crate::secret_store) tier: Option<String>,

    /// A field was edited since the dialog opened, so an incomplete request
    /// is worth reporting.
    pub(super) touched: bool,
    _subscriptions: Vec<Subscription>,
}

/// Why the New secret dialog cannot send its request yet.
enum CreateProblem {
    /// The listing already holds the name.
    Exists(String),

    /// The seam's own validation message.
    Refused(String),
}

impl CreateProblem {
    fn message(&self) -> String {
        match self {
            Self::Exists(name) => {
                dbflux_i18n::t!("document.secret_store.create.exists", name = name)
            }
            Self::Refused(message) => message.clone(),
        }
    }
}

impl CreateDialog {
    /// Whether the encryption key applies: always on a store whose values are
    /// not typed, and for the type the store marks encrypted on one whose are.
    fn takes_key(&self) -> bool {
        self.choices
            .as_ref()
            .is_none_or(|choices| choices.takes_encryption_key(&self.value_type))
    }

    /// Whether the store declares value types to pick from.
    fn offers_value_types(&self) -> bool {
        self.choices
            .as_ref()
            .is_some_and(|choices| !choices.value_types().is_empty())
    }

    /// The text of a single-line field, `None` when it is empty.
    fn field(input: &Entity<InputState>, cx: &App) -> Option<String> {
        let text = input.read(cx).value().trim().to_string();

        (!text.is_empty()).then_some(text)
    }

    /// The request the dialog would send.
    fn request(&self, cx: &App) -> SecretCreateRequest {
        let name = self.name.read(cx).value().trim().to_string();
        let value = self.value.read(cx).value().to_string();
        let mut request = SecretCreateRequest::text(name, value);

        if let Some(description) = Self::field(&self.description, cx) {
            request = request.with_description(description);
        }

        if self.offers_value_types() {
            request = request.with_value_type(self.value_type.as_str());
        }

        if let Some(tier) = &self.tier {
            request = request.with_tier(tier.as_str());
        }

        if self.takes_key()
            && let Some(key) = Self::field(&self.key, cx)
        {
            request = request.with_encryption_key(key);
        }

        for tag in self.tags.iter().filter_map(|row| row.tag(cx)) {
            request = request.with_tag(tag.key, tag.value);
        }

        request
    }
}

impl SecretStoreDocument {
    /// Whether the store can create secrets at all.
    pub fn supports_create(&self) -> bool {
        self.store.supports(CREATE_CAPABILITIES)
    }

    /// Whether New may open its dialog now.
    pub fn can_open_create(&self, cx: &App) -> bool {
        let view = self.view.read(cx);

        self.supports_create()
            && view.is_connected()
            && !view.write_in_flight()
            && !view.dialog_is_open()
    }

    /// Why the dialog cannot send its request yet: a name the listing holds,
    /// or the seam's own validation message.
    fn create_problem(&self, dialog: &CreateDialog, cx: &App) -> Option<CreateProblem> {
        let request = dialog.request(cx);

        if self
            .listing
            .entries
            .iter()
            .any(|entry| entry.name == request.name)
        {
            return Some(CreateProblem::Exists(request.name));
        }

        request
            .validate()
            .err()
            .map(|error| CreateProblem::Refused(error.to_string()))
    }

    /// Opens the New secret dialog. A hierarchical store starts the name at
    /// the path being listed. Nothing is written until it is confirmed.
    pub fn open_create(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_open_create(cx) {
            return;
        }

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        let single_line =
            |window: &mut Window, cx: &mut Context<Self>, placeholder: String, text: &str| {
                cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder(placeholder);

                    if !text.is_empty() {
                        state.set_value(text, window, cx);
                    }

                    state
                })
            };

        let start = if self.hierarchical() {
            self.path.clone().unwrap_or_else(|| "/".to_string())
        } else {
            String::new()
        };

        let name = single_line(
            window,
            cx,
            dbflux_i18n::t!("document.secret_store.create.name_placeholder"),
            &start,
        );
        let description = single_line(
            window,
            cx,
            dbflux_i18n::t!("document.secret_store.create.description_placeholder"),
            "",
        );
        let key = single_line(
            window,
            cx,
            dbflux_i18n::t!("document.secret_store.create.key_placeholder"),
            "",
        );
        let value = cx.new(|cx| {
            TextareaState::new(window, cx)
                .auto_grow(3, 8)
                .soft_wrap(true)
        });

        let mut subscriptions = Vec::new();

        for input in [&name, &description, &key] {
            subscriptions.push(cx.subscribe(input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.touch_create_dialog(cx);
                }
            }));
        }
        // The text area notifies for more than text; the form counts as
        // touched once it holds a value, which the render checks.
        subscriptions.push(cx.observe(&value, |_, _, cx| cx.notify()));

        let focus = name.read(cx).focus_handle(cx);
        let choices = self.store.create_choices.clone();

        self.create_dialog = Some(CreateDialog {
            name,
            value,
            description,
            key,
            tags: Vec::new(),
            value_type: choices
                .as_ref()
                .map(|choices| choices.default_value_type().to_string())
                .unwrap_or_default(),
            tier: choices
                .as_ref()
                .and_then(|choices| choices.default_tier().map(str::to_string)),
            choices,
            touched: false,
            _subscriptions: subscriptions,
        });

        self.view.update(cx, |view, cx| {
            view.open_dialog(WriteDialog::Create, Some(focus), window, cx)
        });

        // The view refuses a dialog while a region switch or an editor save
        // is pending, and the dialog it did not open is not kept.
        if !self.view.read(cx).dialog_is_open() {
            self.create_dialog = None;
        }
    }

    /// Drops the New secret dialog's fields once the view closed its slot.
    pub(super) fn drop_create_dialog(&mut self) {
        self.create_dialog = None;
    }

    /// A field of the open dialog changed: its problems are worth showing.
    fn touch_create_dialog(&mut self, cx: &mut Context<Self>) {
        if let Some(dialog) = &mut self.create_dialog {
            dialog.touched = true;
        }

        cx.notify();
    }

    /// Picks the value type of the open dialog.
    pub fn set_create_value_type(&mut self, value_type: &str, cx: &mut Context<Self>) {
        if self.view.read(cx).write_in_flight() {
            return;
        }

        if let Some(dialog) = &mut self.create_dialog
            && let Some(choices) = &dialog.choices
            && let Some(chosen) = choices
                .value_types()
                .iter()
                .find(|known| known.as_str() == value_type)
        {
            dialog.value_type = chosen.clone();
            dialog.touched = true;
            cx.notify();
        }
    }

    /// Picks the tier of the open dialog.
    pub fn set_create_tier(&mut self, tier: &str, cx: &mut Context<Self>) {
        if self.view.read(cx).write_in_flight() {
            return;
        }

        if let Some(dialog) = &mut self.create_dialog
            && let Some(choices) = &dialog.choices
            && let Some(chosen) = choices.tiers().iter().find(|known| known.as_str() == tier)
        {
            dialog.tier = Some(chosen.clone());
            dialog.touched = true;
            cx.notify();
        }
    }

    /// Adds an empty tag row to the open dialog.
    pub fn add_create_tag(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.view.read(cx).write_in_flight() {
            return;
        }

        let Some(dialog) = &self.create_dialog else {
            return;
        };

        if dialog.tags.len() >= MAX_TAGS {
            return;
        }

        let row = new_tag_row("", "", window, cx, Self::touch_create_dialog);
        let focus = row.key.read(cx).focus_handle(cx);

        if let Some(dialog) = &mut self.create_dialog {
            dialog.tags.push(row);
            dialog.touched = true;
        }

        focus.focus(window, cx);
        cx.notify();
    }

    /// Removes the tag row at `index` from the open dialog.
    pub fn remove_create_tag(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.view.read(cx).write_in_flight() {
            return;
        }

        if let Some(dialog) = &mut self.create_dialog
            && index < dialog.tags.len()
        {
            dialog.tags.remove(index);
            dialog.touched = true;
            cx.notify();
        }
    }

    /// Sends the creation the dialog confirmed, through the seam.
    pub(super) fn submit_create(&mut self, cx: &mut Context<Self>) {
        let Some(dialog) = &self.create_dialog else {
            return;
        };

        // A request the seam would refuse, or a name the listing holds, is
        // not sent: the dialog already shows why.
        if self.create_problem(dialog, cx).is_some() {
            return;
        }

        let request = dialog.request(cx);
        let origin = self.listing_view();

        let started = self
            .view
            .update(cx, |view, cx| view.start_create(request, cx));

        if started {
            self.create_origin = Some(origin);
        }
    }

    /// The create the view sent landed: tells the user and shows the new
    /// secret, unless the user moved on while it was in flight.
    pub(super) fn land_create(&mut self, name: &str, stands: bool, cx: &mut Context<Self>) {
        let Some(origin) = self.create_origin.take() else {
            return;
        };

        if !stands {
            dbflux_ui_base::toast::Toast::success(dbflux_i18n::t!(
                "document.secret_store.create.done",
                name = name
            ))
            .meta_right(dbflux_ui_base::toast::now_hms())
            .push(cx);
        }

        self.show_created(name, &origin, cx);
    }

    /// What the user is looking at: the level, the filter and the selection.
    fn listing_view(&self) -> ListingView {
        ListingView {
            path: self.path.clone(),
            path_mode: self.path_mode,
            name_filter: self.name_filter.clone(),
            selected: self.selected.clone(),
        }
    }

    /// Selects the new secret and reads the listing again, so it shows up in
    /// its row with its details. A user who navigated or selected something
    /// else while the create was in flight keeps what they chose: the listing
    /// is refreshed and the new secret is left unselected, since it may not
    /// even be in the listing in view.
    fn show_created(&mut self, name: &str, sent_from: &ListingView, cx: &mut Context<Self>) {
        if self.listing_view() != *sent_from {
            self.view.update(cx, |view, cx| {
                view.refresh_after_write(name, WriteEffect::Metadata, cx)
            });
            return;
        }

        self.widen_scope_to(name);
        self.select_row(SecretRowKey::Secret(name.to_string()), cx);
        self.view.update(cx, |view, cx| {
            view.refresh_after_write(name, WriteEffect::Metadata, cx)
        });
    }

    /// Moves the listing's scope to where `name` shows up: the name filter is
    /// cleared, and a hierarchical store lists the level the name is under.
    fn widen_scope_to(&mut self, name: &str) {
        if !self.name_filter.is_empty() {
            self.name_filter.clear();
            self.clear_search_input = true;
        }

        if !self.hierarchical() {
            return;
        }

        let level = name.rfind('/').map(|index| name[..=index].to_string());
        let visible = match (&self.path, self.path_mode) {
            (Some(path), crate::secret_store::PathMode::Tree) => name
                .strip_prefix(path.as_str())
                .is_some_and(|rest| !rest.contains('/')),
            (Some(path), crate::secret_store::PathMode::Flat) => name.starts_with(path.as_str()),
            // The root lists one level too: a nested name is below it.
            (None, crate::secret_store::PathMode::Tree) => {
                !name.trim_start_matches('/').contains('/')
            }
            (None, crate::secret_store::PathMode::Flat) => true,
        };

        if !visible {
            self.path = level;
        }
    }

    pub(in crate::secret_store) fn render_create_dialog(
        &self,
        dialog: &CreateDialog,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let problem = self.create_problem(dialog, cx);
        let body = self.render_create_body(dialog, problem.as_ref(), cx);
        let submitting = self.view.read(cx).write_in_flight();
        let focus = self.view.read(cx).dialog_focus_handle().clone();

        let title_key = if self.hierarchical() {
            "document.secret_store.create.title_parameter"
        } else {
            "document.secret_store.create.title_secret"
        };

        let footer = dialog_footer(
            FooterSpec {
                cancel_id: "secret-create-cancel",
                confirm_id: "secret-create-confirm",
                confirm_label: dbflux_i18n::t!("document.secret_store.create.confirm"),
                confirm_icon: AppIcon::Plus,
                danger: false,
                enabled: problem.is_none(),
                submitting,
            },
            cx,
        );

        dialog_modal(
            DialogShell {
                id: "secret-create-dialog",
                title: dbflux_i18n::t!(title_key),
                icon: AppIcon::Plus,
                danger: false,
                confirm_enabled: problem.is_none() && !submitting,
            },
            body,
            footer,
            &focus,
            cx,
        )
    }

    /// The fields of the dialog and the problem line under them.
    fn render_create_body(
        &self,
        dialog: &CreateDialog,
        problem: Option<&CreateProblem>,
        cx: &mut Context<Self>,
    ) -> Div {
        let locked = self.view.read(cx).write_in_flight();
        let touched = dialog.touched || !dialog.value.read(cx).value().is_empty();
        let shown = problem
            .filter(|problem| touched || matches!(problem, CreateProblem::Exists(_)))
            .map(CreateProblem::message);

        let mut body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_field(
                dbflux_i18n::t!("document.secret_store.create.name"),
                Input::new(&dialog.name)
                    .id("secret-create-name-input")
                    .aria_label(dbflux_i18n::t!("document.secret_store.create.name"))
                    .disabled(locked)
                    .w_full(),
                cx,
            ));

        body = body.children(self.render_create_type_field(dialog, locked, cx));

        body = body.child(modal_field(
            dbflux_i18n::t!("document.secret_store.create.value"),
            render_value_input(&dialog.value, locked, cx),
            cx,
        ));

        body = body.child(modal_field(
            dbflux_i18n::t!("document.secret_store.create.description"),
            Input::new(&dialog.description)
                .id("secret-create-description-input")
                .aria_label(dbflux_i18n::t!("document.secret_store.create.description"))
                .disabled(locked)
                .w_full(),
            cx,
        ));

        body = body.children(self.render_create_tier_field(dialog, locked, cx));

        if dialog.takes_key() {
            body = body.child(modal_field(
                dbflux_i18n::t!("document.secret_store.create.key"),
                Input::new(&dialog.key)
                    .id("secret-create-key-input")
                    .aria_label(dbflux_i18n::t!("document.secret_store.create.key"))
                    .disabled(locked)
                    .w_full(),
                cx,
            ));
        }

        body = body.child(self.render_create_tags(dialog, locked, cx));

        if let Some(message) = shown {
            body = body.child(
                div()
                    .id("secret-create-problem")
                    .child(Text::caption(message).danger()),
            );
        }

        body
    }

    /// The value type picker, for a store that declares value types.
    fn render_create_type_field(
        &self,
        dialog: &CreateDialog,
        locked: bool,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        let choices = dialog
            .choices
            .as_ref()
            .filter(|_| dialog.offers_value_types())?;

        Some(modal_field(
            dbflux_i18n::t!("document.secret_store.create.type"),
            self.render_choice_group(
                "secret-create-type",
                choices.value_types(),
                &dialog.value_type,
                |this, choice, cx| this.set_create_value_type(choice, cx),
                locked,
                cx,
            ),
            cx,
        ))
    }

    /// The tier picker, for a store that declares tiers.
    fn render_create_tier_field(
        &self,
        dialog: &CreateDialog,
        locked: bool,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        let choices = dialog
            .choices
            .as_ref()
            .filter(|choices| !choices.tiers().is_empty())?;

        Some(modal_field(
            dbflux_i18n::t!("document.secret_store.create.tier"),
            self.render_choice_group(
                "secret-create-tier",
                choices.tiers(),
                dialog.tier.as_deref().unwrap_or_default(),
                |this, choice, cx| this.set_create_tier(choice, cx),
                locked,
                cx,
            ),
            cx,
        ))
    }

    /// A row of buttons of which one is chosen: the type and the tier. Each
    /// is a button of the dialog, so Tab reaches it and Space or Enter picks
    /// it.
    fn render_choice_group(
        &self,
        id: &'static str,
        choices: &[String],
        chosen: &str,
        on_pick: fn(&mut SecretStoreDocument, &str, &mut Context<SecretStoreDocument>),
        locked: bool,
        cx: &mut Context<Self>,
    ) -> Div {
        let theme = cx.theme();
        let fill = theme.secondary;
        let border = theme.border;

        let buttons: Vec<Button> = choices
            .iter()
            .map(|choice| {
                let picked = choice.clone();

                Button::new(SharedString::from(format!("{id}-{choice}")), choice.clone())
                    .ghost()
                    .selected(choice == chosen)
                    .toggled(choice == chosen)
                    .disabled(locked)
                    .on_click(cx.listener(move |this, _, _, cx| on_pick(this, &picked, cx)))
            })
            .collect();

        div().flex().child(
            div()
                .id(id)
                .flex()
                .flex_wrap()
                .items_center()
                .gap(Spacing::XS)
                .p(Spacing::XS)
                .bg(fill)
                .border_1()
                .border_color(border)
                .children(buttons),
        )
    }

    /// The tag rows of the dialog and the button that adds one.
    fn render_create_tags(
        &self,
        dialog: &CreateDialog,
        locked: bool,
        cx: &mut Context<Self>,
    ) -> Div {
        let rows: Vec<AnyElement> = dialog
            .tags
            .iter()
            .enumerate()
            .map(|(index, row)| {
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        div().flex_1().min_w_0().child(
                            Input::new(&row.key)
                                .id(SharedString::from(format!("secret-create-tag-key-{index}")))
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
                                .id(SharedString::from(format!(
                                    "secret-create-tag-value-{index}"
                                )))
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
                            SharedString::from(format!("secret-create-tag-remove-{index}")),
                            dbflux_i18n::t!("document.secret_store.tags_edit.remove"),
                        )
                        .ghost()
                        .icon(AppIcon::X)
                        .icon_only()
                        .disabled(locked)
                        .on_click(
                            cx.listener(move |this, _, _, cx| this.remove_create_tag(index, cx)),
                        ),
                    )
                    .into_any_element()
            })
            .collect();

        modal_field(
            dbflux_i18n::t!("document.secret_store.create.tags"),
            div()
                .flex()
                .flex_col()
                .gap(ModalMetrics::HEADER_GAP)
                .children(rows)
                .child(
                    div().flex().child(
                        Button::new(
                            "secret-create-tag-add",
                            dbflux_i18n::t!("document.secret_store.tags_edit.add"),
                        )
                        .icon(AppIcon::Plus)
                        .disabled(dialog.tags.len() >= MAX_TAGS || locked)
                        .on_click(
                            cx.listener(|this, _, window, cx| this.add_create_tag(window, cx)),
                        ),
                    ),
                ),
            cx,
        )
    }
}

/// The value's text area, inside the redaction boundary the editor uses: the
/// element is hidden from the accessibility tree and redacted in the frame
/// automation reads, so neither the typed value nor its length leaves the
/// dialog.
fn render_value_input(value: &Entity<TextareaState>, locked: bool, cx: &App) -> Stateful<Div> {
    let theme = cx.theme();

    div()
        .id("secret-create-value")
        .aria_hidden(true)
        .frame_redacted(true)
        .p(Spacing::SM)
        .border_1()
        .border_color(theme.border)
        .child(
            Textarea::new(value)
                .disabled(locked)
                .redacted(true)
                .appearance(false)
                .bordered(false)
                .w_full(),
        )
}

impl SecretStoreDocument {
    /// Whether a write dialog is open over the document.
    pub fn dialog_is_open(&self, cx: &App) -> bool {
        self.view.read(cx).dialog_is_open()
    }

    /// Closes the open dialog without writing, and hands the keyboard back.
    pub fn close_dialog(&mut self, cx: &mut Context<Self>) {
        self.view.update(cx, |view, cx| view.close_dialog(cx));
    }

    /// Confirms the open dialog: the one primary action of whichever dialog
    /// is up, as its button and Enter both run it. The view sends its own
    /// dialogs; New secret is the document's to send.
    pub fn confirm_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let create_confirmed = self
            .view
            .update(cx, |view, cx| view.confirm_dialog(window, cx));

        if create_confirmed {
            self.submit_create(cx);
        }
    }

    /// The dialog drawn over the document: New secret when it is open, else
    /// whichever the selected secret's view has open.
    pub(super) fn render_dialog(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        if let Some(dialog) = &self.create_dialog {
            return Some(self.render_create_dialog(dialog, cx));
        }

        self.view
            .update(cx, |view, cx| view.render_write_dialog(cx))
    }
}

impl DialogHost for SecretStoreDocument {
    fn close_dialog_from_modal(&mut self, cx: &mut Context<Self>) {
        self.close_dialog(cx);
    }

    fn confirm_dialog_from_modal(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.confirm_dialog(window, cx);
    }
}
