//! The pane actions of one secret, for the menu `m` opens. A host splices
//! each group into its own list at the place the action sits in the menu.

use super::SecretView;
use super::{DetailsState, ValueState, VersionsState};
use crate::pane::PaneAction;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::icons::AppIcon;
use dbflux_core::SecretStoreCapabilities;
use gpui::{Context, WeakEntity};

impl SecretView {
    /// The value section's actions, from Retrieve value to the editor's
    /// Discard.
    pub(crate) fn value_pane_actions(&self, target: &WeakEntity<Self>) -> Vec<PaneAction> {
        let context = ContextId::Results;
        let value_loaded = matches!(self.value, ValueState::Loaded(_));
        let value_failed = matches!(self.value, ValueState::Failed(_));
        let has_resource_id = matches!(
            &self.details,
            DetailsState::Loaded(details) if details.resource_id.is_some()
        );

        let command = |id: &'static str, key: &str, command: Command| {
            PaneAction::command(id, dbflux_i18n::t!(key), command, context)
        };

        vec![
            command(
                "secret-retrieve-value",
                "document.secret_store.value.retrieve",
                Command::ToggleValuePanel,
            )
            .icon(AppIcon::Eye)
            .enabled(self.can_retrieve_value()),
            command(
                "secret-copy-resource-id",
                "document.secret_store.action.copy_resource_id",
                Command::ResultsCopyCell,
            )
            .enabled(has_resource_id),
            command(
                "secret-copy-value",
                "document.secret_store.value.copy_all",
                Command::ResultsCopyRow,
            )
            .enabled(value_loaded),
            command(
                "secret-reveal",
                "document.secret_store.value.reveal_all",
                Command::ExpandCollapse,
            )
            .enabled(value_loaded),
            callback_action(
                target,
                "secret-hide-all",
                "document.secret_store.value.hide_all",
                Self::hide_all,
            )
            .enabled(value_loaded),
            callback_action(
                target,
                "secret-copy-error",
                "document.secret_store.value.copy_error",
                Self::copy_value_error,
            )
            .enabled(value_failed),
            callback_action(
                target,
                "secret-view-in-audit",
                "document.secret_store.value.view_in_audit",
                Self::view_value_error_in_audit,
            )
            .enabled(value_failed),
            callback_action(
                target,
                "secret-editor-save",
                "document.secret_store.edit.save",
                Self::save_value_edit,
            )
            .enabled(self.is_editing_value() && !self.is_saving_value_edit()),
            callback_action(
                target,
                "secret-editor-discard",
                "document.secret_store.edit.discard",
                Self::discard_value_edit,
            )
            .enabled(self.is_editing_value() && !self.is_saving_value_edit()),
        ]
    }

    /// Copy name, offered when the view shows a secret.
    pub(crate) fn copy_name_pane_actions(&self, target: &WeakEntity<Self>) -> Vec<PaneAction> {
        let mut actions = Vec::new();

        // Each copy entry names the selection it acts on: Copy name for a
        // secret, Copy path for a sub-path. The entry is only offered when a
        // selection of its kind exists.
        if self.selected_summary().is_some() {
            actions.push(callback_action(
                target,
                "secret-copy-name",
                "document.secret_store.context_menu.copy_name",
                Self::copy_selected_name,
            ));
        }

        actions
    }

    /// One entry per version of the history, and Back to current.
    pub(crate) fn history_pane_actions(&self, target: &WeakEntity<Self>) -> Vec<PaneAction> {
        let mut actions = Vec::new();

        // Each version of the history is a clickable row, and Back to current
        // a button of the value section: both reachable from the menu.
        if let VersionsState::Loaded(versions) = &self.versions {
            for (index, version) in versions.iter().enumerate() {
                let retrieve_target = target.clone();
                let history_generation = self.versions_generation;
                actions.push(
                    PaneAction::callback(
                        format!("secret-version-retrieve-{index}"),
                        dbflux_i18n::t!(
                            "document.secret_store.versions.retrieve",
                            version = super::details::short_version_id(&version.id)
                        ),
                        move |_window, cx| {
                            if let Some(view) = retrieve_target.upgrade() {
                                view.update(cx, |view, cx| {
                                    // The menu was built over another history.
                                    if view.versions_generation == history_generation {
                                        view.activate_version(index, cx);
                                    }
                                });
                            }
                        },
                    )
                    .icon(AppIcon::History)
                    .enabled(self.can_retrieve_value()),
                );

                // Roll back is a button on every version that is not
                // current, so each one has its own entry.
                if self.supports_roll_back() && !self.is_current_version(&version.id) {
                    let roll_back_target = target.clone();
                    let history_generation = self.versions_generation;
                    let mut roll_back = PaneAction::callback(
                        format!("secret-version-rollback-{index}"),
                        dbflux_i18n::t!(
                            "document.secret_store.rollback.action",
                            version = super::details::short_version_id(&version.id)
                        ),
                        move |window, cx| {
                            if let Some(view) = roll_back_target.upgrade() {
                                view.update(cx, |view, cx| {
                                    if view.versions_generation == history_generation {
                                        view.open_roll_back(index, window, cx);
                                    }
                                });
                            }
                        },
                    )
                    .icon(AppIcon::RotateCcw)
                    .enabled(self.can_roll_back_version(index));
                    roll_back.shortcut = dbflux_ui_base::keymap::shortcut_label(
                        ContextId::Results,
                        Command::RollBackVersion,
                    )
                    .filter(|_| self.previous_version_index() == Some(index));
                    actions.push(roll_back);
                }
            }
        }

        // Registered whatever the history is doing: the banner's button is
        // drawn from the value, not from the history, so it needs its
        // keyboard path even while the history loads or fails.
        if self.store.supports(SecretStoreCapabilities::VERSIONS) {
            actions.push(
                callback_action(
                    target,
                    "secret-version-current",
                    "document.secret_store.versions.back_to_current",
                    Self::back_to_current_version,
                )
                .icon(AppIcon::RotateCcw)
                .enabled(self.can_return_to_current()),
            );
        }

        actions
    }

    /// The editor's switch and Find, Edit value and Delete.
    pub(crate) fn edit_pane_actions(&self, target: &WeakEntity<Self>) -> Vec<PaneAction> {
        let mut actions = Vec::new();

        // The Formatted / Raw switch is a clickable control of the editor, so
        // the pane actions are its keyboard path too.
        if self.can_toggle_edit_mode() {
            let target = target.clone();
            actions.push(
                crate::pane::PaneAction::callback(
                    "secret-editor-mode",
                    dbflux_i18n::t!("document.secret_store.edit.mode.toggle"),
                    move |window, cx| {
                        if let Some(view) = target.upgrade() {
                            view.update(cx, |view, cx| view.cycle_edit_mode(window, cx));
                        }
                    },
                )
                .enabled(self.can_toggle_edit_mode()),
            );
        }

        if self.is_editing_value() {
            let target = target.clone();
            actions.push(
                crate::pane::PaneAction::callback(
                    "secret-editor-find",
                    dbflux_i18n::t!("document.secret_store.edit.find"),
                    move |window, cx| {
                        if let Some(view) = target.upgrade() {
                            view.update(cx, |view, cx| view.find_in_value(window, cx));
                        }
                    },
                )
                .icon(AppIcon::Search),
            );
        }

        // Edit value opens the editor of a plaintext value, retrieving the
        // value first when it is not loaded; the action needs the window the
        // editor's state is built with. The bottom bar's button and the row
        // menu's entry are covered by it.
        if self.can_request_edit_value() || self.can_edit_value() {
            let target = target.clone();
            let mut edit_value = crate::pane::PaneAction::callback(
                "secret-edit-value",
                dbflux_i18n::t!("document.secret_store.edit.open"),
                move |window, cx| {
                    if let Some(view) = target.upgrade() {
                        view.update(cx, |view, cx| view.request_edit_value(window, cx));
                    }
                },
            )
            .icon(AppIcon::Pencil)
            .enabled(self.can_request_edit_value());
            edit_value.shortcut =
                dbflux_ui_base::keymap::shortcut_label(ContextId::Results, Command::EditValue);
            actions.push(edit_value);
        }

        // Delete asks first; Restore cancels a scheduled deletion. Both are
        // buttons of the details pane, so both have an entry here.
        if self.supports_delete() && self.name.is_some() {
            let target = target.clone();
            let mut delete = PaneAction::callback(
                "secret-delete",
                dbflux_i18n::t!("document.secret_store.delete.button"),
                move |window, cx| {
                    if let Some(view) = target.upgrade() {
                        view.update(cx, |view, cx| view.open_delete(window, cx));
                    }
                },
            )
            .icon(AppIcon::Delete)
            .enabled(self.can_delete_selected());
            delete.shortcut =
                dbflux_ui_base::keymap::shortcut_label(ContextId::Results, Command::Delete);
            actions.push(delete);
        }

        actions
    }

    /// Edit tags, Edit description, the rotation actions and Restore.
    pub(crate) fn write_pane_actions(&self, target: &WeakEntity<Self>) -> Vec<PaneAction> {
        let mut actions = Vec::new();

        // Edit tags and Edit description are buttons of the Tags and Details
        // sections (the description's of the bottom bar on a store that
        // rewrites the value for it).
        if self.name.is_some() {
            if self.supports_tags() {
                let tags_target = target.clone();
                actions.push(
                    PaneAction::callback(
                        "secret-edit-tags",
                        dbflux_i18n::t!("document.secret_store.tags_edit.button"),
                        move |window, cx| {
                            if let Some(view) = tags_target.upgrade() {
                                view.update(cx, |view, cx| view.open_edit_tags(window, cx));
                            }
                        },
                    )
                    .icon(AppIcon::Tag)
                    .enabled(self.can_edit_tags()),
                );
            }

            if self.supports_description_edit() {
                let description_target = target.clone();
                actions.push(
                    PaneAction::callback(
                        "secret-edit-description",
                        dbflux_i18n::t!("document.secret_store.description_edit.button"),
                        move |window, cx| {
                            if let Some(view) = description_target.upgrade() {
                                view.update(cx, |view, cx| view.open_edit_description(window, cx));
                            }
                        },
                    )
                    .icon(AppIcon::Pencil)
                    .enabled(self.can_edit_description()),
                );
            }
        }

        // Rotate now and Edit rotation are buttons of the Rotation section,
        // the bottom bar and the row menu.
        if self.supports_rotation() && self.name.is_some() {
            let rotate_target = target.clone();
            actions.push(
                PaneAction::callback(
                    "secret-rotate-now",
                    dbflux_i18n::t!("document.secret_store.rotate.button"),
                    move |window, cx| {
                        if let Some(view) = rotate_target.upgrade() {
                            view.update(cx, |view, cx| view.rotate_selected_now(window, cx));
                        }
                    },
                )
                .icon(AppIcon::Zap)
                .enabled(self.can_rotate_now()),
            );

            let edit_target = target.clone();
            actions.push(
                PaneAction::callback(
                    "secret-edit-rotation",
                    dbflux_i18n::t!("document.secret_store.rotation_edit.button"),
                    move |window, cx| {
                        if let Some(view) = edit_target.upgrade() {
                            view.update(cx, |view, cx| view.open_edit_rotation(window, cx));
                        }
                    },
                )
                .icon(AppIcon::Pencil)
                .enabled(self.can_edit_rotation()),
            );
        }

        if self.supports_restore()
            && self
                .selected_summary()
                .is_some_and(|summary| self.is_deletion_pending(summary))
        {
            actions.push(
                callback_action(
                    target,
                    "secret-restore",
                    "document.secret_store.restore.button",
                    Self::restore_selected,
                )
                .icon(AppIcon::RotateCcw)
                .enabled(self.can_restore_selected()),
            );
        }

        actions
    }
}

/// A pane action that runs `run` on the entity `target`, for an action no
/// command covers.
pub(in crate::secret_store) fn callback_action<T: 'static>(
    target: &WeakEntity<T>,
    id: &'static str,
    key: &str,
    run: fn(&mut T, &mut Context<T>),
) -> PaneAction {
    let target = target.clone();

    PaneAction::callback(id, dbflux_i18n::t!(key), move |_window, cx| {
        if let Some(entity) = target.upgrade() {
            entity.update(cx, run);
        }
    })
}
