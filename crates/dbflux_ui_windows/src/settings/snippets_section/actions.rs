use super::{
    FormValues, SnippetFocus, SnippetFormErrors, SnippetFormField, SnippetsSection, language_items,
    language_label,
};
use dbflux_components::modals::{ConfirmRequest, ModalVariant};
use dbflux_core::{SnippetParseError, SnippetParseErrorKind, parse_snippet};
use dbflux_storage::{SnippetDraft, SnippetError, SnippetRecord};
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

/// Message for a body the snippet parser rejected, with its position and the
/// offending source.
pub(super) fn parse_error_message(error: &SnippetParseError) -> String {
    let line = error.line;
    let column = error.column;
    let excerpt = error.excerpt.as_str();

    match error.kind {
        SnippetParseErrorKind::UnclosedPlaceholder => dbflux_i18n::t!(
            "settings.snippets.validation.parse.unclosed_placeholder",
            line = line,
            column = column,
            excerpt = excerpt
        ),
        SnippetParseErrorKind::NestedPlaceholder => dbflux_i18n::t!(
            "settings.snippets.validation.parse.nested_placeholder",
            line = line,
            column = column,
            excerpt = excerpt
        ),
        SnippetParseErrorKind::UnsupportedChoice => dbflux_i18n::t!(
            "settings.snippets.validation.parse.unsupported_choice",
            line = line,
            column = column,
            excerpt = excerpt
        ),
        SnippetParseErrorKind::UnsupportedVariable => dbflux_i18n::t!(
            "settings.snippets.validation.parse.unsupported_variable",
            line = line,
            column = column,
            excerpt = excerpt
        ),
        SnippetParseErrorKind::InvalidPlaceholder => dbflux_i18n::t!(
            "settings.snippets.validation.parse.invalid_placeholder",
            line = line,
            column = column,
            excerpt = excerpt
        ),
    }
}

/// The errors Save shows for `values`, before anything reaches the store.
pub(super) fn validate_form(values: &FormValues) -> SnippetFormErrors {
    let mut errors = SnippetFormErrors::default();

    if values.name.trim().is_empty() {
        errors.name = Some(dbflux_i18n::t!(
            "settings.snippets.validation.name_required"
        ));
    }

    if values.prefix.trim().is_empty() {
        errors.prefix = Some(dbflux_i18n::t!(
            "settings.snippets.validation.prefix_required"
        ));
    }

    if values.body.trim().is_empty() {
        errors.body = Some(dbflux_i18n::t!(
            "settings.snippets.validation.body_required"
        ));
    } else if let Err(error) = parse_snippet(&values.body) {
        errors.body = Some(parse_error_message(&error));
    }

    errors
}

impl SnippetsSection {
    pub(super) fn reload_snippets(&mut self, cx: &mut Context<Self>) {
        let result = self.app_state.update(cx, |state, _| state.snippets.all());

        match result {
            Ok(snippets) => self.snippets = snippets,
            Err(error) => report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.snippets.error.load"),
                )
                .with_cause(error.to_string()),
                cx,
            ),
        }

        self.sync_list_index();
        cx.notify();
    }

    fn sync_list_index(&mut self) {
        self.list_idx = self
            .editing_id
            .as_ref()
            .and_then(|id| self.snippets.iter().position(|snippet| &snippet.id == id));
    }

    pub(super) fn form_values(&self, cx: &App) -> FormValues {
        FormValues {
            name: self.input_name.read(cx).value().to_string(),
            prefix: self.input_prefix.read(cx).value().to_string(),
            body: self.input_body.read(cx).value().to_string(),
        }
    }

    fn form_language(&self, cx: &App) -> String {
        self.language_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string())
            .unwrap_or_else(|| super::SQL_LANGUAGE_ID.to_string())
    }

    fn set_language(&self, language: &str, cx: &mut Context<Self>) {
        let items = language_items(Some(language));
        let index = items
            .iter()
            .position(|item| item.value.as_ref() == language);

        self.language_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(items, cx);
            dropdown.set_selected_index(index.or(Some(0)), cx);
        });
    }

    fn write_form(
        &mut self,
        values: &FormValues,
        language: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.input_name.update(cx, |input, cx| {
            input.set_value(values.name.as_str(), window, cx)
        });
        self.input_prefix.update(cx, |input, cx| {
            input.set_value(values.prefix.as_str(), window, cx)
        });
        self.input_body.update(cx, |input, cx| {
            input.set_value(values.body.as_str(), window, cx)
        });
        self.set_language(language, cx);
        self.errors = SnippetFormErrors::default();
        self.form_field = SnippetFormField::Name;
        self.editing_field = false;
    }

    pub(super) fn select_snippet(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let Some(record) = self
            .snippets
            .iter()
            .find(|snippet| snippet.id == id)
            .cloned()
        else {
            return;
        };

        self.editing_id = Some(record.id.clone());
        self.new_form_open = false;
        self.return_to_id = None;
        self.write_form(
            &FormValues {
                name: record.name.clone(),
                prefix: record.prefix.clone(),
                body: record.body.clone(),
            },
            &record.language,
            window,
            cx,
        );
        self.sync_list_index();
        self.pending_scroll_idx = self.list_idx;
        cx.notify();
    }

    pub(super) fn select_index(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(id) = self.snippets.get(index).map(|snippet| snippet.id.clone()) {
            self.select_snippet(&id, window, cx);
        }
    }

    fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_id = None;
        self.list_idx = None;
        self.write_form(&FormValues::default(), super::SQL_LANGUAGE_ID, window, cx);
        cx.notify();
    }

    /// Opens a blank form. Cancel goes back to the snippet that was open.
    pub(in crate::settings) fn begin_new_snippet(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.editing_id.is_some() {
            self.return_to_id = self.editing_id.clone();
        }

        self.clear_form(window, cx);
        self.new_form_open = true;
    }

    pub(super) fn cancel_new_snippet(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let target = self
            .return_to_id
            .take()
            .filter(|id| self.snippets.iter().any(|snippet| &snippet.id == id))
            .or_else(|| self.snippets.first().map(|snippet| snippet.id.clone()));

        match target {
            Some(id) => self.select_snippet(&id, window, cx),
            None => {
                self.clear_form(window, cx);
                self.new_form_open = false;
            }
        }
    }

    pub(super) fn has_unsaved_changes(&self, cx: &App) -> bool {
        let values = self.form_values(cx);
        let saved = self
            .editing_id
            .as_ref()
            .and_then(|id| self.snippets.iter().find(|snippet| &snippet.id == id));

        match saved {
            Some(saved) => {
                values.name != saved.name
                    || values.prefix != saved.prefix
                    || values.body != saved.body
                    || self.form_language(cx) != saved.language
            }
            None => {
                self.new_form_open
                    && (!values.name.trim().is_empty()
                        || !values.prefix.trim().is_empty()
                        || !values.body.trim().is_empty())
            }
        }
    }

    /// Clears the error of `field` once its text no longer is what was
    /// validated.
    pub(super) fn drop_stale_error(&mut self, field: SnippetFormField, cx: &mut Context<Self>) {
        let current = self.form_values(cx);

        match field {
            SnippetFormField::Name if current.name != self.error_values.name => {
                self.errors.name = None;
            }
            SnippetFormField::Prefix if current.prefix != self.error_values.prefix => {
                self.errors.prefix = None;
            }
            SnippetFormField::Body if current.body != self.error_values.body => {
                self.errors.body = None;
            }
            _ => {}
        }
    }

    fn show_errors(
        &mut self,
        errors: SnippetFormErrors,
        values: FormValues,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let first = errors.first_field();
        self.errors = errors;
        self.error_values = values;

        if let Some(field) = first {
            self.focus = SnippetFocus::Form;
            self.form_field = field;
            self.focus_form_field(window, cx);
        }

        cx.notify();
    }

    pub(super) fn save_snippet(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.new_form_open && self.editing_id.is_none() {
            return;
        }

        let values = self.form_values(cx);
        let errors = validate_form(&values);

        if !errors.is_empty() {
            self.show_errors(errors, values, window, cx);
            return;
        }

        let draft = SnippetDraft {
            name: values.name.trim().to_string(),
            prefix: values.prefix.trim().to_string(),
            language: self.form_language(cx),
            body: values.body.clone(),
        };
        let editing_id = self.editing_id.clone();
        let result = self
            .app_state
            .update(cx, |state, _| match editing_id.as_deref() {
                Some(id) => state.snippets.update(id, &draft),
                None => state.snippets.create(&draft),
            });

        match result {
            Ok(record) => self.finish_save(record, window, cx),
            Err(error) => self.report_save_error(error, values, window, cx),
        }
    }

    fn finish_save(&mut self, record: SnippetRecord, window: &mut Window, cx: &mut Context<Self>) {
        self.reload_snippets(cx);
        self.select_snippet(&record.id, window, cx);
        self.focus = SnippetFocus::Form;

        Toast::success(dbflux_i18n::t!("settings.snippets.toast.saved"))
            .meta_right(now_hms())
            .push(cx);
    }

    fn report_save_error(
        &mut self,
        error: SnippetError,
        values: FormValues,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let mut errors = SnippetFormErrors::default();

        match error {
            SnippetError::DuplicatePrefix { language, prefix } => {
                errors.prefix = Some(dbflux_i18n::t!(
                    "settings.snippets.validation.duplicate_prefix",
                    language = language_label(&language),
                    prefix = prefix
                ));
            }
            SnippetError::EmptyName => {
                errors.name = Some(dbflux_i18n::t!(
                    "settings.snippets.validation.name_required"
                ));
            }
            SnippetError::EmptyPrefix => {
                errors.prefix = Some(dbflux_i18n::t!(
                    "settings.snippets.validation.prefix_required"
                ));
            }
            SnippetError::EmptyBody => {
                errors.body = Some(dbflux_i18n::t!(
                    "settings.snippets.validation.body_required"
                ));
            }
            other => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        dbflux_i18n::t!("settings.snippets.error.save"),
                    )
                    .with_cause(other.to_string()),
                    cx,
                );
                return;
            }
        }

        self.show_errors(errors, values, window, cx);
    }

    pub(super) fn request_delete_snippet(&mut self, id: String, cx: &mut Context<Self>) {
        self.pending_delete_id = Some(id);
        cx.notify();
    }

    pub(super) fn cancel_delete_snippet(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_id = None;
        cx.notify();
    }

    pub(super) fn confirm_delete_snippet(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(id) = self.pending_delete_id.take() else {
            return;
        };

        let result = self
            .app_state
            .update(cx, |state, _| state.snippets.delete(&id));

        if let Err(error) = result {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.snippets.error.delete"),
                )
                .with_cause(error.to_string()),
                cx,
            );
            return;
        }

        self.reload_snippets(cx);

        if self.editing_id.as_deref() == Some(id.as_str()) {
            self.new_form_open = false;
            self.return_to_id = None;
            self.clear_form(window, cx);
        }

        Toast::success(dbflux_i18n::t!("settings.snippets.toast.deleted"))
            .meta_right(now_hms())
            .push(cx);
        cx.notify();
    }

    /// Opens the delete confirmation while a delete is pending. Opening only
    /// when the request changed keeps later renders from stealing focus back.
    pub(super) fn open_delete_confirmation(&mut self, cx: &mut Context<Self>) {
        let Some(id) = self.pending_delete_id.clone() else {
            return;
        };
        let Some(name) = self
            .snippets
            .iter()
            .find(|snippet| snippet.id == id)
            .map(|snippet| snippet.name.clone())
        else {
            self.pending_delete_id = None;
            return;
        };

        let body = dbflux_i18n::t!("settings.snippets.delete.message", name = name);
        let needs_open = !self.delete_confirm.read(cx).is_visible()
            || self.delete_confirm.read(cx).current_body() != Some(body.as_str());

        if needs_open {
            self.delete_confirm.update(cx, |confirm, cx| {
                confirm.open(
                    ConfirmRequest {
                        title: dbflux_i18n::t!("settings.snippets.delete.title"),
                        body,
                        variant: ModalVariant::Danger,
                        confirm_label: dbflux_i18n::t!("settings.snippets.action.delete"),
                    },
                    cx,
                );
            });
        }
    }

    pub(super) fn focus_form_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_field = true;

        match self.form_field {
            SnippetFormField::Name => {
                self.input_name
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            SnippetFormField::Prefix => {
                self.input_prefix
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            SnippetFormField::Body => {
                self.input_body
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            _ => self.editing_field = false,
        }
    }

    pub(super) fn activate_form_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.form_field {
            SnippetFormField::Name | SnippetFormField::Prefix | SnippetFormField::Body => {
                self.focus_form_field(window, cx);
            }
            SnippetFormField::Language => {
                self.language_dropdown
                    .update(cx, |dropdown, cx| dropdown.toggle_open(cx));
            }
            SnippetFormField::SaveButton => self.save_snippet(window, cx),
            SnippetFormField::DeleteButton => {
                if let Some(id) = self.editing_id.clone() {
                    self.request_delete_snippet(id, cx);
                }
            }
            SnippetFormField::CancelButton => self.cancel_new_snippet(window, cx),
        }
    }
}
