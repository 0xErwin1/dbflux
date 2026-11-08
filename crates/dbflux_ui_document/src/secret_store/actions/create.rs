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

use super::tags::TagRow;
use super::{
    DialogShell, FooterSpec, Landing, WriteDialog, WriteEffect, dialog_footer, dialog_modal,
};
use crate::secret_store::write::create_secret;
use crate::secret_store::{SecretRowKey, SecretStoreDocument};
use dbflux_components::controls::{Button, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::parts::modal_field;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{ModalMetrics, Spacing};
use dbflux_core::{MAX_TAGS, SecretCreateRequest, SecretCreated, SecretStoreCapabilities};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Textarea, TextareaState};

/// The capabilities a creation needs: the same pair the seam requires.
const CREATE_CAPABILITIES: SecretStoreCapabilities =
    SecretStoreCapabilities::WRITE.union(SecretStoreCapabilities::CREATE);

/// The value types a typed store takes, and the one a new value starts as.
/// The store checks the choice, and refuses a name it does not know.
const VALUE_TYPES: [&str; 3] = ["String", "StringList", "SecureString"];
const DEFAULT_VALUE_TYPE: &str = "SecureString";

/// The type whose value is encrypted, the only one that takes an encryption
/// key on a typed store.
const ENCRYPTED_VALUE_TYPE: &str = "SecureString";

/// The tiers a typed store takes, and the one a new value starts in.
const TIERS: [&str; 3] = ["Standard", "Advanced", "Intelligent-Tiering"];
const DEFAULT_TIER: &str = "Standard";

/// The New secret dialog.
pub(in crate::secret_store) struct CreateDialog {
    pub(in crate::secret_store) name: Entity<InputState>,
    pub(in crate::secret_store) value: Entity<TextareaState>,
    description: Entity<InputState>,
    key: Entity<InputState>,
    tags: Vec<TagRow>,

    /// The store's values are typed, so the type and the tier are asked.
    typed: bool,
    value_type: &'static str,
    tier: &'static str,

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
    /// not typed, and for the type that is encrypted on one whose are.
    fn takes_key(&self) -> bool {
        !self.typed || self.value_type == ENCRYPTED_VALUE_TYPE
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

        if self.typed {
            request = request
                .with_value_type(self.value_type)
                .with_tier(self.tier);
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
        self.store_allows(CREATE_CAPABILITIES)
    }

    /// Whether New may open its dialog now.
    pub fn can_open_create(&self) -> bool {
        self.supports_create() && !self.write_in_flight && !self.dialog_is_open()
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
        if !self.can_open_create() {
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

        self.open_dialog(
            WriteDialog::Create(CreateDialog {
                name,
                value,
                description,
                key,
                tags: Vec::new(),
                typed: self.store_allows(SecretStoreCapabilities::TYPED_VALUES),
                value_type: DEFAULT_VALUE_TYPE,
                tier: DEFAULT_TIER,
                touched: false,
                _subscriptions: subscriptions,
            }),
            Some(focus),
            window,
            cx,
        );
    }

    /// A field of the open dialog changed: its problems are worth showing.
    fn touch_create_dialog(&mut self, cx: &mut Context<Self>) {
        if let Some(WriteDialog::Create(dialog)) = &mut self.dialog {
            dialog.touched = true;
        }

        cx.notify();
    }

    /// Picks the value type of the open dialog.
    pub fn set_create_value_type(&mut self, value_type: &str, cx: &mut Context<Self>) {
        let Some(chosen) = VALUE_TYPES.iter().find(|known| **known == value_type) else {
            return;
        };

        if let Some(WriteDialog::Create(dialog)) = &mut self.dialog
            && dialog.typed
        {
            dialog.value_type = chosen;
            dialog.touched = true;
            cx.notify();
        }
    }

    /// Picks the tier of the open dialog.
    pub fn set_create_tier(&mut self, tier: &str, cx: &mut Context<Self>) {
        let Some(chosen) = TIERS.iter().find(|known| **known == tier) else {
            return;
        };

        if let Some(WriteDialog::Create(dialog)) = &mut self.dialog
            && dialog.typed
        {
            dialog.tier = chosen;
            dialog.touched = true;
            cx.notify();
        }
    }

    /// Adds an empty tag row to the open dialog.
    pub fn add_create_tag(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(WriteDialog::Create(dialog)) = &self.dialog else {
            return;
        };

        if dialog.tags.len() >= MAX_TAGS {
            return;
        }

        let row = Self::new_tag_row("", "", window, cx);
        let focus = row.key.read(cx).focus_handle(cx);

        if let Some(WriteDialog::Create(dialog)) = &mut self.dialog {
            dialog.tags.push(row);
            dialog.touched = true;
        }

        focus.focus(window, cx);
        cx.notify();
    }

    /// Removes the tag row at `index` from the open dialog.
    pub fn remove_create_tag(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(WriteDialog::Create(dialog)) = &mut self.dialog
            && index < dialog.tags.len()
        {
            dialog.tags.remove(index);
            dialog.touched = true;
            cx.notify();
        }
    }

    /// Sends the creation the dialog confirmed, through the seam.
    pub(in crate::secret_store) fn submit_create(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(WriteDialog::Create(dialog)) = &self.dialog else {
            return;
        };

        // A request the seam would refuse, or a name the listing holds, is
        // not sent: the dialog already shows why.
        if self.create_problem(dialog, cx).is_some() {
            return;
        }

        let request = dialog.request(cx);
        let name = request.name.clone();

        let Some(session) = self.write_session(cx) else {
            return;
        };

        self.run_governed_write(
            session,
            move |context| Landing::of(create_secret(context, &request)),
            move |doc, landing, cx| doc.land_create(&name, landing, cx),
            cx,
        );
    }

    fn land_create(&mut self, name: &str, landing: Landing<SecretCreated>, cx: &mut Context<Self>) {
        match landing {
            Landing::Done(_) => {
                self.dismiss_dialog(cx);
                self.toast_written(
                    dbflux_i18n::t!("document.secret_store.create.done", name = name),
                    cx,
                );
                self.show_created(name, cx);
            }
            Landing::Stands(failure) => {
                self.dismiss_dialog(cx);
                self.report_write_failure(&failure, cx);
                self.show_created(name, cx);
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }

    /// Selects the new secret and reads the listing again, so it shows up in
    /// its row with its details.
    fn show_created(&mut self, name: &str, cx: &mut Context<Self>) {
        self.select_row(SecretRowKey::Secret(name.to_string()), cx);
        self.refresh_after_write(name, WriteEffect::Metadata, cx);
    }

    pub(in crate::secret_store) fn render_create_dialog(
        &self,
        dialog: &CreateDialog,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let problem = self.create_problem(dialog, cx);
        let body = self.render_create_body(dialog, problem.as_ref(), cx);

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
                submitting: self.write_in_flight,
            },
            cx,
        );

        dialog_modal(
            DialogShell {
                id: "secret-create-dialog",
                title: dbflux_i18n::t!(title_key),
                icon: AppIcon::Plus,
                danger: false,
                confirm_enabled: problem.is_none() && !self.write_in_flight,
            },
            body,
            footer,
            self.dialog_focus.handle(),
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
                    .w_full(),
                cx,
            ));

        if dialog.typed {
            body = body.child(modal_field(
                dbflux_i18n::t!("document.secret_store.create.type"),
                self.render_choice_group(
                    "secret-create-type",
                    &VALUE_TYPES,
                    dialog.value_type,
                    |this, choice, cx| this.set_create_value_type(choice, cx),
                    cx,
                ),
                cx,
            ));
        }

        body = body.child(modal_field(
            dbflux_i18n::t!("document.secret_store.create.value"),
            render_value_input(&dialog.value, cx),
            cx,
        ));

        body = body.child(modal_field(
            dbflux_i18n::t!("document.secret_store.create.description"),
            Input::new(&dialog.description)
                .id("secret-create-description-input")
                .aria_label(dbflux_i18n::t!("document.secret_store.create.description"))
                .w_full(),
            cx,
        ));

        if dialog.typed {
            body = body.child(modal_field(
                dbflux_i18n::t!("document.secret_store.create.tier"),
                self.render_choice_group(
                    "secret-create-tier",
                    &TIERS,
                    dialog.tier,
                    |this, choice, cx| this.set_create_tier(choice, cx),
                    cx,
                ),
                cx,
            ));
        }

        if dialog.takes_key() {
            body = body.child(modal_field(
                dbflux_i18n::t!("document.secret_store.create.key"),
                Input::new(&dialog.key)
                    .id("secret-create-key-input")
                    .aria_label(dbflux_i18n::t!("document.secret_store.create.key"))
                    .w_full(),
                cx,
            ));
        }

        body = body.child(self.render_create_tags(dialog, cx));

        if let Some(message) = shown {
            body = body.child(
                div()
                    .id("secret-create-problem")
                    .child(Text::caption(message).danger()),
            );
        }

        body
    }

    /// A row of buttons of which one is chosen: the type and the tier. Each
    /// is a button of the dialog, so Tab reaches it and Space or Enter picks
    /// it.
    fn render_choice_group(
        &self,
        id: &'static str,
        choices: &[&'static str],
        chosen: &str,
        on_pick: fn(&mut SecretStoreDocument, &str, &mut Context<SecretStoreDocument>),
        cx: &mut Context<Self>,
    ) -> Div {
        let theme = cx.theme();
        let fill = theme.secondary;
        let border = theme.border;

        let buttons: Vec<Button> = choices
            .iter()
            .map(|choice| {
                let choice = *choice;

                Button::new(SharedString::from(format!("{id}-{choice}")), choice)
                    .ghost()
                    .selected(choice == chosen)
                    .toggled(choice == chosen)
                    .on_click(cx.listener(move |this, _, _, cx| on_pick(this, choice, cx)))
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
    fn render_create_tags(&self, dialog: &CreateDialog, cx: &mut Context<Self>) -> Div {
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
                        .disabled(dialog.tags.len() >= MAX_TAGS)
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
fn render_value_input(value: &Entity<TextareaState>, cx: &App) -> Stateful<Div> {
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
                .redacted(true)
                .appearance(false)
                .bordered(false)
                .w_full(),
        )
}
