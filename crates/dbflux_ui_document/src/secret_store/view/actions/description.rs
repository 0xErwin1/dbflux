//! Edit description: changes the description of the selected secret through
//! [`update_secret_description`](crate::secret_store::update_secret_description).
//!
//! A store that keeps its versions as a plain history changes a description
//! only by writing the value again, which creates a version; the dialog says
//! so before it is confirmed, and the landing names the version the change
//! created. A store with staging labels changes the description alone.

use super::{
    DialogShell, FooterSpec, Landing, WriteDialog, WriteEffect, dialog_footer, dialog_modal,
};
use crate::secret_store::DetailsState;
use crate::secret_store::SecretOperation;
use crate::secret_store::view::SecretView;
use crate::secret_store::write::update_secret_description;
use dbflux_components::controls::{Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::parts::modal_field;
use dbflux_components::primitives::{BannerBlock, BannerVariant};
use dbflux_components::tokens::ModalMetrics;
use dbflux_core::{SecretDescriptionChange, SecretDescriptionUpdate, SecretStoreCapabilities};
use gpui::prelude::*;
use gpui::*;

/// The capabilities Edit description needs: the same pair the seam requires.
const DESCRIPTION_CAPABILITIES: SecretStoreCapabilities =
    SecretStoreCapabilities::WRITE.union(SecretStoreCapabilities::DESCRIPTION);

/// The Edit description dialog.
pub(in crate::secret_store) struct DescriptionDialog {
    pub name: String,

    /// The description the secret has now.
    original: String,
    pub input: Entity<InputState>,

    /// Whether saving writes the value again as a new version.
    rewrites_value: bool,
    _subscription: Subscription,
}

impl DescriptionDialog {
    fn text(&self, cx: &App) -> String {
        self.input.read(cx).value().to_string()
    }

    fn is_changed(&self, cx: &App) -> bool {
        self.text(cx) != self.original
    }
}

impl SecretView {
    /// Whether the store can change descriptions at all.
    pub(crate) fn supports_description_edit(&self) -> bool {
        self.store_allows(DESCRIPTION_CAPABILITIES)
    }

    /// Whether changing the description writes the value again as a new
    /// version. The capability set does not say it, so a store counts as one
    /// that rewrites when it keeps a plain version history — versions with no
    /// staging labels to move — which is a store where a version is a
    /// complete record of the value and its description. Where the guess is
    /// wrong, the dialog over-warns: the landing reports the version the
    /// change actually created.
    pub(crate) fn description_edit_rewrites_value(&self) -> bool {
        self.store_allows(SecretStoreCapabilities::VERSIONS)
            && !self.store_allows(SecretStoreCapabilities::VERSION_LABELS)
    }

    /// Whether Edit description may open now.
    pub(crate) fn can_edit_description(&self) -> bool {
        self.supports_description_edit()
            && self.connection_present
            && !self.write_in_flight
            && !self.dialog_is_open()
            && self.write_refusal_reason().is_none()
            && self.name.is_some()
            && matches!(self.details, DetailsState::Loaded(_))
    }

    /// Opens the Edit description dialog on the selected secret's
    /// description. Nothing is written until it is confirmed.
    pub(crate) fn open_edit_description(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_edit_description() {
            return;
        }

        let DetailsState::Loaded(details) = &self.details else {
            return;
        };

        let name = details.summary.name.clone();
        let original = details.summary.description.clone().unwrap_or_default();

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        let input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.secret_store.description_edit.placeholder"
            ));

            if !original.is_empty() {
                state.set_value(&original, window, cx);
            }

            state
        });
        let subscription = cx.subscribe(&input, |_, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                cx.notify();
            }
        });
        let focus = input.read(cx).focus_handle(cx);
        let rewrites_value = self.description_edit_rewrites_value();

        self.open_dialog(
            WriteDialog::Description(DescriptionDialog {
                name,
                original,
                input,
                rewrites_value,
                _subscription: subscription,
            }),
            Some(focus),
            window,
            cx,
        );
    }

    /// Writes the description the dialog confirmed, through the seam.
    pub(in crate::secret_store) fn submit_description(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(WriteDialog::Description(dialog)) = &self.dialog else {
            return;
        };

        // An unchanged description sends nothing: on a store that rewrites
        // the value, it would create a version for no change.
        if !dialog.is_changed(cx) {
            return;
        }

        let update = SecretDescriptionUpdate::new(dialog.name.clone(), dialog.text(cx));
        let name = dialog.name.clone();

        let Some(session) = self.write_session(cx) else {
            return;
        };

        self.run_governed_write(
            session,
            SecretOperation::EditDescription,
            name.clone(),
            move |context| Landing::of(update_secret_description(context, &update)),
            move |doc, landing, cx| doc.land_description(&name, landing, cx),
            cx,
        );
    }

    fn land_description(
        &mut self,
        name: &str,
        landing: Landing<SecretDescriptionChange>,
        cx: &mut Context<Self>,
    ) {
        match landing {
            Landing::Done(change) => {
                self.dismiss_dialog(cx);

                let (message, effect) = match change.version {
                    Some(version) => (
                        dbflux_i18n::t!(
                            "document.secret_store.description_edit.done_version",
                            name = name,
                            version = version
                        ),
                        WriteEffect::Versions,
                    ),
                    None => (
                        dbflux_i18n::t!("document.secret_store.description_edit.done", name = name),
                        WriteEffect::Metadata,
                    ),
                };

                self.toast_written(message, cx);
                self.refresh_after_write(name, effect, cx);
            }
            Landing::Stands(failure) => {
                self.dismiss_dialog(cx);
                self.report_write_failure(&failure, cx);
                self.refresh_after_write(name, WriteEffect::Versions, cx);
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }

    pub(in crate::secret_store) fn render_description_dialog(
        &self,
        dialog: &DescriptionDialog,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let changed = dialog.is_changed(cx);

        let mut body = div().flex().flex_col().gap(ModalMetrics::BODY_GAP);

        if dialog.rewrites_value {
            body = body.child(
                div().id("secret-description-rewrites").child(
                    BannerBlock::new(
                        BannerVariant::Warning,
                        dbflux_i18n::t!("document.secret_store.description_edit.rewrites_title"),
                    )
                    .with_body(dbflux_i18n::t!(
                        "document.secret_store.description_edit.rewrites_body"
                    )),
                ),
            );
        }

        body = body.child(modal_field(
            dbflux_i18n::t!("document.secret_store.description_edit.label"),
            Input::new(&dialog.input)
                .id("secret-description-input")
                .aria_label(dbflux_i18n::t!(
                    "document.secret_store.description_edit.label"
                ))
                .w_full(),
            cx,
        ));

        let footer = dialog_footer(
            FooterSpec {
                cancel_id: "secret-description-cancel",
                confirm_id: "secret-description-confirm",
                confirm_label: if dialog.rewrites_value {
                    dbflux_i18n::t!("document.secret_store.description_edit.confirm_version")
                } else {
                    dbflux_i18n::t!("document.secret_store.description_edit.confirm")
                },
                confirm_icon: AppIcon::Check,
                danger: false,
                enabled: changed,
                submitting: self.write_in_flight,
            },
            cx,
        );

        dialog_modal(
            DialogShell {
                id: "secret-description-dialog",
                title: dbflux_i18n::t!(
                    "document.secret_store.description_edit.title",
                    name = dialog.name.as_str()
                ),
                icon: AppIcon::Pencil,
                danger: false,
                confirm_enabled: changed && !self.write_in_flight,
            },
            body,
            footer,
            self.dialog_focus.handle(),
            cx,
        )
    }
}
