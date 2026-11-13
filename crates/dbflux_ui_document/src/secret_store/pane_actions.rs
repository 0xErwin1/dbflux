//! The pane actions of the listing document, for the menu `m` opens. The
//! actions of the selected secret come from its view and are spliced in at
//! the place the menu shows them.

use super::{RegionListState, SecretRowKey, SecretStoreDocument, view};
use crate::pane::PaneAction;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::icons::AppIcon;
use gpui::{App, Entity};

impl SecretStoreDocument {
    /// The controls of the toolbar and the details pane. The listing's
    /// actions are drawn here and the selected secret's come from its view,
    /// in the order the menu shows them.
    pub(crate) fn pane_actions(&self, this: &Entity<Self>, cx: &App) -> Vec<PaneAction> {
        let view = self.view.read(cx);
        let view_target = self.view.downgrade();

        let mut actions = view.value_pane_actions(&view_target);
        actions.extend(self.paging_pane_actions());

        // Each copy entry names the selection it acts on: Copy name for a
        // secret, Copy path for a sub-path. The entry is only offered when a
        // selection of its kind exists.
        actions.extend(view.copy_name_pane_actions(&view_target));
        actions.extend(self.selection_pane_actions(this));
        actions.extend(view.history_pane_actions(&view_target));
        actions.extend(view.edit_pane_actions(&view_target));
        actions.extend(self.create_pane_action(this, cx));
        actions.extend(view.write_pane_actions(&view_target));
        actions.extend(self.region_pane_action());

        actions
    }

    fn paging_pane_actions(&self) -> Vec<PaneAction> {
        let command = |id: &'static str, key: &str, command: Command| {
            PaneAction::command(id, dbflux_i18n::t!(key), command, ContextId::Results)
        };

        vec![
            command(
                "secret-load-more",
                "document.secret_store.load_more",
                Command::ResultsNextPage,
            )
            .enabled(self.listing.has_more()),
            command(
                "secret-refresh",
                "document.secret_store.toolbar.refresh",
                Command::RefreshSchema,
            )
            .icon(AppIcon::RefreshCcw),
        ]
    }

    /// Close and Copy path for the selected row, and the level controls of a
    /// hierarchical store.
    fn selection_pane_actions(&self, this: &Entity<Self>) -> Vec<PaneAction> {
        let target = this.downgrade();
        let mut actions = Vec::new();

        if self.selected.is_some() {
            let target = target.clone();
            actions.push(PaneAction::callback(
                "secret-details-close",
                dbflux_i18n::t!("document.secret_store.details.close"),
                move |window, cx| {
                    if let Some(doc) = target.upgrade() {
                        doc.update(cx, |doc, cx| doc.close_details(window, cx));
                    }
                },
            ));
        }

        if matches!(self.selected, Some(SecretRowKey::Secret(_))) {
            actions.push(
                view::callback_action(
                    &target,
                    "secret-open-in-tab",
                    "document.secret_store.context_menu.open_in_tab",
                    Self::open_selected_in_tab,
                )
                .icon(AppIcon::Maximize2),
            );
        }

        if matches!(self.selected, Some(SecretRowKey::Path(_))) {
            actions.push(view::callback_action(
                &target,
                "secret-copy-path",
                "document.secret_store.context_menu.copy_path",
                Self::copy_selected_path,
            ));
        }

        if self.hierarchical() {
            let command = |id: &'static str, key: &str, command: Command| {
                PaneAction::command(id, dbflux_i18n::t!(key), command, ContextId::Results)
            };

            actions.push(
                command(
                    "secret-path-mode",
                    "document.secret_store.path_mode.toggle",
                    Command::CycleDocumentView,
                )
                .icon(AppIcon::Folder),
            );
            actions.push(
                command(
                    "secret-path-up",
                    "document.secret_store.path.up",
                    Command::ColumnLeft,
                )
                .enabled(self.path.is_some()),
            );
        }

        actions
    }

    /// New secret / New parameter is a button of the toolbar.
    fn create_pane_action(&self, this: &Entity<Self>, cx: &App) -> Option<PaneAction> {
        if !self.supports_create() {
            return None;
        }

        let target = this.downgrade();

        Some(
            PaneAction::callback(
                "secret-new",
                dbflux_i18n::t!(if self.hierarchical() {
                    "document.secret_store.create.button_parameter"
                } else {
                    "document.secret_store.create.button_secret"
                }),
                move |window, cx| {
                    if let Some(doc) = target.upgrade() {
                        doc.update(cx, |doc, cx| doc.open_create(window, cx));
                    }
                },
            )
            .icon(AppIcon::Plus)
            .enabled(self.can_open_create(cx)),
        )
    }

    /// The region picker is a clickable toolbar control, so the pane actions
    /// are its keyboard path too.
    fn region_pane_action(&self) -> Option<PaneAction> {
        if !self.region_switchable {
            return None;
        }

        let dropdown = self.region_dropdown.clone()?;

        Some(
            PaneAction::callback(
                "secret-region",
                dbflux_i18n::t!("document.secret_store.region.label"),
                move |window, cx| {
                    dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
                },
            )
            .icon(AppIcon::Globe)
            .enabled(
                matches!(&self.regions, RegionListState::Loaded(regions) if !regions.is_empty()),
            ),
        )
    }
}
