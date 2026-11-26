//! Settings > Snippets: the list of code snippets and the form that edits
//! one. The snippets live in `AppStateEntity::snippets`; this section holds
//! the list as last read and the unsaved form.

mod actions;
mod keys;
mod placeholders;
mod render;

use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::FormSection;
use super::section_trait::SectionFocusEvent;
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_components::modals::{ConfirmOutcome, ModalConfirm};
use dbflux_storage::SnippetRecord;
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme as _;
use gpui_component::input::{EditorState, RangeDecorationCollection, TextDecorationCollection};

#[cfg(test)]
mod tests;

/// A change of the form that would drop the unsaved edits, held back until
/// the user confirms discarding them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PendingSnippetAction {
    /// Open this saved snippet.
    Select(String),
    /// Open a blank form.
    New,
}

/// Stable id of the SQL language, as snippets store it.
pub(super) const SQL_LANGUAGE_ID: &str = "sql";

/// Languages the form offers. Snippets of any other stored language stay
/// listed and keep their language when edited.
const OFFERED_LANGUAGES: &[&str] = &[SQL_LANGUAGE_ID];

/// Name shown for a language id: SQL has a translated entry, any other id is
/// shown upper-cased.
pub(super) fn language_label(language: &str) -> String {
    match language {
        SQL_LANGUAGE_ID => dbflux_i18n::t!("settings.snippets.language.sql"),
        other => other.to_uppercase(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SnippetFocus {
    List,
    Form,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum SnippetFormField {
    Name,
    Prefix,
    Language,
    Body,
    DeleteButton,
    CancelButton,
    SaveButton,
}

/// What the form held when it was validated, so an error goes away once the
/// field it belongs to is edited.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct FormValues {
    pub(super) name: String,
    pub(super) prefix: String,
    pub(super) body: String,
}

/// Inline errors under the form fields, set by Save.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct SnippetFormErrors {
    pub(super) name: Option<String>,
    pub(super) prefix: Option<String>,
    pub(super) body: Option<String>,
}

impl SnippetFormErrors {
    pub(super) fn is_empty(&self) -> bool {
        self.name.is_none() && self.prefix.is_none() && self.body.is_none()
    }

    /// The first field, in form order, that carries an error.
    pub(super) fn first_field(&self) -> Option<SnippetFormField> {
        if self.name.is_some() {
            Some(SnippetFormField::Name)
        } else if self.prefix.is_some() {
            Some(SnippetFormField::Prefix)
        } else if self.body.is_some() {
            Some(SnippetFormField::Body)
        } else {
            None
        }
    }
}

pub(super) struct SnippetsSection {
    pub(super) app_state: Entity<AppStateEntity>,
    /// Every snippet, as the manager last returned them.
    pub(super) snippets: Vec<SnippetRecord>,
    /// The saved snippet the form edits; `None` while it holds a new one.
    pub(super) editing_id: Option<String>,
    /// Snippet that was open when "New snippet" was pressed, for Cancel.
    pub(super) return_to_id: Option<String>,
    /// Whether the form holds a snippet that is not saved yet.
    pub(super) new_form_open: bool,
    pub(super) pending_delete_id: Option<String>,
    /// Confirmation of the delete, opened by `render` while one is pending.
    pub(super) delete_confirm: Entity<ModalConfirm>,
    /// Outcome of `delete_confirm`, drained in `render` where a `Window` is
    /// available (the outcome subscription records it without one).
    pending_delete_confirm_resolution: Option<ConfirmOutcome>,
    /// The form change waiting for the user to confirm discarding the edits.
    pub(super) pending_discard_action: Option<PendingSnippetAction>,
    /// Confirmation of the discard, opened by `render` while one is pending.
    pub(super) discard_confirm: Entity<ModalConfirm>,
    pending_discard_confirm_resolution: Option<ConfirmOutcome>,
    pub(super) input_name: Entity<InputState>,
    pub(super) input_prefix: Entity<InputState>,
    pub(super) language_dropdown: Entity<Dropdown>,
    pub(super) input_body: Entity<EditorState>,
    /// Chips drawn over the placeholders of the body.
    placeholder_chips: RangeDecorationCollection,
    placeholder_text: TextDecorationCollection,
    pub(super) errors: SnippetFormErrors,
    pub(super) error_values: FormValues,
    pub(super) focus: SnippetFocus,
    pub(super) list_idx: Option<usize>,
    pub(super) list_scroll_handle: ScrollHandle,
    pub(super) pending_scroll_idx: Option<usize>,
    pub(super) form_field: SnippetFormField,
    pub(super) editing_field: bool,
    pub(super) content_focused: bool,
    pub(super) switching_input: bool,
    _subscriptions: Vec<Subscription>,
}

impl SnippetsSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_name = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.snippets.form.name_placeholder"))
        });
        let input_prefix = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.snippets.form.prefix_placeholder"))
        });
        let language_dropdown = cx.new(|_cx| {
            Dropdown::new("snippet-language")
                .items(language_items(None))
                .selected_index(Some(0))
        });
        let input_body = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("sql")
                .line_number(true)
        });

        let (placeholder_chips, placeholder_text) = input_body.update(cx, |state, cx| {
            (
                state.create_range_decorations_collection(Vec::new(), cx),
                state.create_decorations_collection(Vec::new(), cx),
            )
        });

        let name_sub = notify_on_input_change(&input_name, window, cx, |this, cx| {
            this.drop_stale_error(SnippetFormField::Name, cx);
        });
        let prefix_sub = notify_on_input_change(&input_prefix, window, cx, |this, cx| {
            this.drop_stale_error(SnippetFormField::Prefix, cx);
        });
        let body_sub = cx.subscribe_in(
            &input_body,
            window,
            |this, _, event: &InputEvent, _, cx| match event {
                InputEvent::Change => {
                    this.refresh_placeholder_chips(cx);
                    this.drop_stale_error(SnippetFormField::Body, cx);
                    cx.notify();
                }
                InputEvent::Blur => {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }

                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
                _ => {}
            },
        );
        let language_sub = cx.subscribe_in(
            &language_dropdown,
            window,
            |_, _, _: &DropdownSelectionChanged, _, cx| cx.notify(),
        );

        let delete_confirm = cx.new(ModalConfirm::new);
        let delete_confirm_subscription =
            cx.subscribe(&delete_confirm, |this, _, outcome: &ConfirmOutcome, cx| {
                this.pending_delete_confirm_resolution = Some(*outcome);
                cx.notify();
            });

        let discard_confirm = cx.new(ModalConfirm::new);
        let discard_confirm_subscription =
            cx.subscribe(&discard_confirm, |this, _, outcome: &ConfirmOutcome, cx| {
                this.pending_discard_confirm_resolution = Some(*outcome);
                cx.notify();
            });

        let mut section = Self {
            app_state,
            snippets: Vec::new(),
            editing_id: None,
            return_to_id: None,
            new_form_open: false,
            pending_delete_id: None,
            delete_confirm,
            pending_delete_confirm_resolution: None,
            pending_discard_action: None,
            discard_confirm,
            pending_discard_confirm_resolution: None,
            input_name,
            input_prefix,
            language_dropdown,
            input_body,
            placeholder_chips,
            placeholder_text,
            errors: SnippetFormErrors::default(),
            error_values: FormValues::default(),
            focus: SnippetFocus::List,
            list_idx: None,
            list_scroll_handle: ScrollHandle::new(),
            pending_scroll_idx: None,
            form_field: SnippetFormField::Name,
            editing_field: false,
            content_focused: false,
            switching_input: false,
            _subscriptions: vec![
                name_sub,
                prefix_sub,
                body_sub,
                language_sub,
                delete_confirm_subscription,
                discard_confirm_subscription,
            ],
        };

        section.reload_snippets(cx);

        if let Some(first_id) = section.snippets.first().map(|snippet| snippet.id.clone()) {
            section.select_snippet(&first_id, window, cx);
            section.focus = SnippetFocus::List;
        }

        section
    }
}

impl SnippetsSection {
    /// Redraws the placeholder chips for the body as it is now. `set_value`
    /// emits no change, so the callers that load a body call this too.
    pub(super) fn refresh_placeholder_chips(&self, cx: &mut Context<Self>) {
        let body = self.input_body.read(cx).value().to_string();
        let colors = placeholders::PlaceholderColors::from_theme(cx.theme());
        let decorations = placeholders::placeholder_decorations(&body, colors);

        self.placeholder_chips.set(decorations.chips, cx);
        self.placeholder_text.set(decorations.text, cx);
    }
}

/// Dropdown items: the offered languages, plus `extra` when a stored snippet
/// uses one the form does not offer, so editing it never changes its language.
pub(super) fn language_items(extra: Option<&str>) -> Vec<DropdownItem> {
    OFFERED_LANGUAGES
        .iter()
        .copied()
        .chain(extra.filter(|extra| !OFFERED_LANGUAGES.contains(extra)))
        .map(|language| DropdownItem::with_value(language_label(language), language.to_string()))
        .collect()
}

fn notify_on_input_change(
    input: &Entity<InputState>,
    window: &mut Window,
    cx: &mut Context<SnippetsSection>,
    on_change: impl Fn(&mut SnippetsSection, &mut Context<SnippetsSection>) + 'static,
) -> Subscription {
    cx.subscribe_in(
        input,
        window,
        move |this, _, event: &InputEvent, _window, cx| {
            if matches!(event, InputEvent::Change) {
                on_change(this, cx);
                cx.notify();
            }

            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }

                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        },
    )
}

impl FormSection for SnippetsSection {
    type Focus = SnippetFocus;
    type FormField = SnippetFormField;

    fn focus_area(&self) -> Self::Focus {
        self.focus
    }

    fn set_focus_area(&mut self, focus: Self::Focus) {
        self.focus = focus;
    }

    fn form_field(&self) -> Self::FormField {
        self.form_field
    }

    fn set_form_field(&mut self, field: Self::FormField) {
        self.form_field = field;
    }

    fn editing_field(&self) -> bool {
        self.editing_field
    }

    fn set_editing_field(&mut self, editing: bool) {
        self.editing_field = editing;
    }

    fn switching_input(&self) -> bool {
        self.switching_input
    }

    fn set_switching_input(&mut self, switching: bool) {
        self.switching_input = switching;
    }

    fn content_focused(&self) -> bool {
        self.content_focused
    }

    fn list_focus() -> Self::Focus {
        SnippetFocus::List
    }

    fn form_focus() -> Self::Focus {
        SnippetFocus::Form
    }

    fn first_form_field() -> Self::FormField {
        SnippetFormField::Name
    }

    fn form_rows(&self) -> Vec<Vec<Self::FormField>> {
        vec![
            vec![SnippetFormField::Name],
            vec![SnippetFormField::Prefix],
            vec![SnippetFormField::Language],
            vec![SnippetFormField::Body],
            if self.editing_id.is_some() {
                vec![SnippetFormField::DeleteButton, SnippetFormField::SaveButton]
            } else {
                vec![SnippetFormField::CancelButton, SnippetFormField::SaveButton]
            },
        ]
    }

    fn is_input_field(field: Self::FormField) -> bool {
        matches!(
            field,
            SnippetFormField::Name | SnippetFormField::Prefix | SnippetFormField::Body
        )
    }

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        SnippetsSection::focus_form_field(self, window, cx);
    }

    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        SnippetsSection::activate_form_field(self, window, cx);
    }
}

impl SettingsSection for SnippetsSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Snippets
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        SnippetsSection::handle_key_event(self, event, window, cx);
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.editing_field = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_changes(cx)
    }

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        self.render_footer_buttons(cx)
    }

    fn render_footer_leading_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        self.render_footer_leading_buttons(cx)
    }

    fn save_from_shortcut(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.save_snippet(window, cx);
    }
}

impl EventEmitter<SectionFocusEvent> for SnippetsSection {}

impl Render for SnippetsSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(outcome) = self.pending_delete_confirm_resolution.take() {
            match outcome {
                ConfirmOutcome::Confirmed => self.confirm_delete_snippet(window, cx),
                ConfirmOutcome::Cancelled => self.cancel_delete_snippet(cx),
            }
        }

        if let Some(outcome) = self.pending_discard_confirm_resolution.take() {
            match outcome {
                ConfirmOutcome::Confirmed => self.confirm_discard_changes(window, cx),
                ConfirmOutcome::Cancelled => self.cancel_discard_changes(cx),
            }
        }

        self.open_delete_confirmation(cx);
        self.open_discard_confirmation(cx);

        div()
            .size_full()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(self.render_snippets_section(cx))
            .child(self.delete_confirm.clone())
            .child(self.discard_confirm.clone())
    }
}
