//! The entries of the context menu of the shown secret. The menu itself
//! belongs to the listing; what each entry offers, and why one is disabled,
//! follows the secret's state, which only the view knows.

use super::{DetailsState, SecretView};
use crate::secret_store::context_menu::{SecretMenuAction, SecretMenuItem, menu_shortcut};
use dbflux_app::keymap::Command;
use dbflux_components::icons::AppIcon;
use dbflux_core::chrono::{DateTime, Local, Utc};
use gpui::SharedString;

/// A deletion date the way the listing's badge spells it.
fn short_date(date: DateTime<Utc>) -> String {
    date.with_timezone(&Local).format("%Y-%m-%d").to_string()
}

impl SecretView {
    /// The entries of the menu of the shown secret. Parameter rows (the
    /// entries of a hierarchical store) offer no ARN item: their entries
    /// carry no resource identifier the document copies.
    pub(in crate::secret_store) fn secret_menu_items(&self, name: &str) -> Vec<SecretMenuItem> {
        let mut items = vec![
            SecretMenuItem::new(
                SecretMenuAction::OpenInTab,
                dbflux_i18n::t!("document.secret_store.context_menu.open_in_tab").into(),
                AppIcon::Maximize2,
            ),
            self.retrieve_item(name),
            SecretMenuItem::new(
                SecretMenuAction::CopyName,
                dbflux_i18n::t!("document.secret_store.context_menu.copy_name").into(),
                AppIcon::Copy,
            ),
        ];

        if !self.hierarchical() {
            items.push(self.copy_arn_item());
        }

        // The design's Edit value entry: offered when an edit can be asked
        // for, like the bar's button. An unretrieved value is retrieved first,
        // through the audited path.
        if self.can_request_edit_value() {
            items.push(
                SecretMenuItem::new(
                    SecretMenuAction::EditValue,
                    dbflux_i18n::t!("document.secret_store.edit.open").into(),
                    AppIcon::Pencil,
                )
                .with_shortcut(Command::EditValue),
            );
        }

        // Rotate now starts a rotation of a secret that rotates.
        if self.supports_rotation() {
            let rotate = SecretMenuItem::new(
                SecretMenuAction::RotateNow,
                dbflux_i18n::t!("document.secret_store.rotate.button").into(),
                AppIcon::Zap,
            );

            items.push(match self.rotate_now_block_reason() {
                Some(reason) => rotate.disabled_with_reason(reason.into()),
                None => self.write_item(rotate, self.can_rotate_now(), true),
            });
        }

        // Edit tags opens the dialog the Tags section's button opens.
        if self.supports_tags() {
            items.push(self.write_item(
                SecretMenuItem::new(
                    SecretMenuAction::EditTags,
                    dbflux_i18n::t!("document.secret_store.tags_edit.button").into(),
                    AppIcon::Tag,
                ),
                self.can_edit_tags(),
                true,
            ));
        }

        // Restore cancels a scheduled deletion; Delete schedules or
        // makes one. Each is drawn disabled, with the reason, when
        // the secret's own state refuses it.
        if self.supports_restore()
            && self
                .selected_summary()
                .is_some_and(|summary| self.is_deletion_pending(summary))
        {
            items.push(self.write_item(
                SecretMenuItem::new(
                    SecretMenuAction::Restore,
                    dbflux_i18n::t!("document.secret_store.restore.button").into(),
                    AppIcon::RotateCcw,
                ),
                self.can_restore_selected(),
                false,
            ));
        }

        if self.supports_delete() {
            items.push(
                self.write_item(
                    SecretMenuItem::new(
                        SecretMenuAction::Delete,
                        dbflux_i18n::t!("document.secret_store.delete.button").into(),
                        AppIcon::Delete,
                    )
                    .danger()
                    .with_shortcut(Command::Delete),
                    self.can_delete_selected(),
                    true,
                ),
            );
        }

        items
    }

    /// A write entry: enabled when `allowed`; otherwise disabled with the
    /// reason the secret's own state gives, when it has one. `refuse_state`
    /// asks for that reason (a restore is for a secret whose deletion is
    /// scheduled, so that state is no reason to refuse it).
    fn write_item(
        &self,
        item: SecretMenuItem,
        allowed: bool,
        refuse_state: bool,
    ) -> SecretMenuItem {
        if allowed {
            return item;
        }

        match refuse_state.then(|| self.write_refusal_reason()).flatten() {
            Some(reason) => item.disabled_with_reason(reason.into()),
            None => SecretMenuItem {
                disabled: true,
                ..item
            },
        }
    }

    fn retrieve_item(&self, name: &str) -> SecretMenuItem {
        let deletion = self
            .listed_summary()
            .filter(|summary| summary.name == name)
            .and_then(|summary| self.deletion_of(summary));

        // A scheduled deletion keeps the value unreachable: the retrieval is
        // already blocked by the document, so the entry says why instead of
        // running into that block.
        let (detail, disabled) = match deletion {
            Some(date) => (
                Some(
                    match date {
                        Some(date) => dbflux_i18n::t!(
                            "document.secret_store.value.deletion_scheduled",
                            date = short_date(date)
                        ),
                        None => dbflux_i18n::t!(
                            "document.secret_store.value.deletion_scheduled_unknown"
                        ),
                    }
                    .into(),
                ),
                true,
            ),
            None => (None, !self.can_retrieve_value()),
        };

        SecretMenuItem {
            action: SecretMenuAction::RetrieveValue,
            label: dbflux_i18n::t!("document.secret_store.value.retrieve").into(),
            icon: AppIcon::Eye,
            shortcut: menu_shortcut(Command::ToggleValuePanel),
            detail,
            disabled,
            danger: false,
        }
    }

    /// Why the resource identifier cannot be copied now: still arriving,
    /// failed to load, or not reported by the store.
    pub(super) fn resource_id_unavailable_reason(&self) -> String {
        match &self.details {
            DetailsState::Loaded(_) => {
                dbflux_i18n::t!("document.secret_store.context_menu.arn_missing")
            }
            DetailsState::Failed(error) => dbflux_i18n::t!(
                "document.secret_store.context_menu.arn_failed",
                error = error.as_str()
            ),
            _ => dbflux_i18n::t!("document.secret_store.context_menu.arn_loading"),
        }
        .to_string()
    }

    /// Copy ARN is only meaningful once the describe delivered the resource
    /// identifier. Until then it stays disabled and names what is missing —
    /// an identifier still arriving is not the same as a describe that
    /// failed, and a failure is said the way the value section says its own.
    fn copy_arn_item(&self) -> SecretMenuItem {
        let known = matches!(
            &self.details,
            DetailsState::Loaded(details) if details.resource_id.is_some()
        );

        if known {
            return SecretMenuItem::new(
                SecretMenuAction::CopyArn,
                dbflux_i18n::t!("document.secret_store.action.copy_resource_id").into(),
                AppIcon::Copy,
            );
        }

        let reason: SharedString = self.resource_id_unavailable_reason().into();

        SecretMenuItem::new(
            SecretMenuAction::CopyArn,
            dbflux_i18n::t!("document.secret_store.action.copy_resource_id").into(),
            AppIcon::Copy,
        )
        .disabled_with_reason(reason)
    }
}
