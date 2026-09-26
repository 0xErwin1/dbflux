use super::*;

use crate::ui::document::DocumentId;

/// What the Migrate action does for a sidebar selection. Only one migration
/// runs at a time, as with the former modal wizard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrateOpenPlan {
    /// No migration is running: focus the tab already open for this
    /// selection, or open a new one.
    Open { existing: Option<DocumentId> },
    /// A migration is running in a tab: focus that tab. `warn` is set when
    /// it was opened for another selection, so the user learns why their
    /// selection did not open.
    FocusRunning { id: DocumentId, warn: bool },
    /// A migration is running but its tab was closed: it continues in the
    /// Tasks panel, and no wizard opens.
    RunningWithoutTab,
}

/// Decides [`MigrateOpenPlan`] from the Migrate task in flight, the tab that
/// owns a running migration (with whether it matches the requested
/// selection), and the tab already open for that selection.
fn plan_migrate_open(
    migrate_task_running: bool,
    running_tab: Option<(DocumentId, bool)>,
    existing: Option<DocumentId>,
) -> MigrateOpenPlan {
    match running_tab {
        Some((id, matches_selection)) => MigrateOpenPlan::FocusRunning {
            id,
            warn: !matches_selection,
        },
        None if migrate_task_running => MigrateOpenPlan::RunningWithoutTab,
        None => MigrateOpenPlan::Open { existing },
    }
}

impl Workspace {
    /// Opens the Migrate-data wizard as a document tab, pre-populated with the
    /// sidebar's table selection. Deduplicated by the selection it was opened
    /// for, so repeating the same Migrate action focuses the existing tab.
    /// While a migration runs, no other wizard opens: the tab that owns the
    /// run is focused instead and a toast says why.
    pub(in crate::ui::views::workspace) fn open_migrate_wizard(
        &mut self,
        profile_id: uuid::Uuid,
        database: Option<String>,
        tables: Vec<dbflux_core::TableRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::{DocumentKey, DocumentKind, DocumentState};
        use dbflux_ui_document::migrate_wizard::MigrateWizard;

        let key = DocumentKey::MigrateWizard {
            profile_id,
            database: database.clone(),
            tables: tables.clone(),
        };

        let migrate_task_running = self
            .app_state
            .read(cx)
            .running_tasks()
            .iter()
            .any(|task| task.kind == dbflux_core::TaskKind::Migrate);

        let running_tab = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .find(|tab| {
                tab.kind() == DocumentKind::MigrateWizard
                    && tab.meta_snapshot(cx).state == DocumentState::Executing
            })
            .map(|tab| (tab.id(), tab.matches_dedup_key(&key, cx)));

        let existing = self.tab_manager.read(cx).find_by_key(&key, cx);

        match plan_migrate_open(migrate_task_running, running_tab, existing) {
            MigrateOpenPlan::Open { existing: Some(id) } => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(id, cx);
                });
            }
            MigrateOpenPlan::Open { existing: None } => {
                let app_state = self.app_state.clone();
                let doc = cx.new(|cx| {
                    let mut wizard = MigrateWizard::new(app_state, cx);
                    wizard.open(profile_id, database, tables, window, cx);
                    wizard
                });
                let pane = MigrateWizard::into_pane(doc, cx);

                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
            }
            MigrateOpenPlan::FocusRunning { id, warn } => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(id, cx);
                });

                if warn {
                    Toast::warning(dbflux_i18n::t!("document.migrate_wizard.already_running"))
                        .meta_right(now_hms())
                        .push(cx);
                }
            }
            MigrateOpenPlan::RunningWithoutTab => {
                Toast::warning(dbflux_i18n::t!(
                    "document.migrate_wizard.already_running_in_tasks"
                ))
                .meta_right(now_hms())
                .push(cx);
                return;
            }
        }

        self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use super::{MigrateOpenPlan, plan_migrate_open};
    use crate::ui::document::DocumentId;
    use crate::ui::views::workspace::Workspace;
    use dbflux_core::{TableRef, TaskKind};
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn without_a_running_migration_the_selection_opens_or_refocuses() {
        let existing = DocumentId::new();

        assert_eq!(
            plan_migrate_open(false, None, None),
            MigrateOpenPlan::Open { existing: None }
        );
        assert_eq!(
            plan_migrate_open(false, None, Some(existing)),
            MigrateOpenPlan::Open {
                existing: Some(existing)
            }
        );
    }

    #[test]
    fn a_running_migration_takes_the_focus_instead_of_another_selection() {
        let running = DocumentId::new();
        let other_selection_tab = DocumentId::new();

        assert_eq!(
            plan_migrate_open(true, Some((running, false)), Some(other_selection_tab)),
            MigrateOpenPlan::FocusRunning {
                id: running,
                warn: true
            }
        );
        assert_eq!(
            plan_migrate_open(true, Some((running, true)), Some(running)),
            MigrateOpenPlan::FocusRunning {
                id: running,
                warn: false
            }
        );
        assert_eq!(
            plan_migrate_open(true, None, None),
            MigrateOpenPlan::RunningWithoutTab
        );
    }

    fn new_workspace(
        cx: &mut TestAppContext,
    ) -> (
        Entity<Workspace>,
        Entity<AppStateEntity>,
        &mut VisualTestContext,
    ) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::new(RefCell::new(None));
        let workspace_ref = holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
            workspace_ref.replace(Some(workspace.clone()));
            gpui_component::Root::new(workspace, window, cx)
        });

        let workspace = holder
            .borrow()
            .clone()
            .expect("workspace should be created");
        (workspace, app_state, window)
    }

    #[gpui::test]
    fn migrate_does_not_open_a_second_wizard_while_a_migration_runs(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);

        window.update(|_, cx| {
            app_state.update(cx, |state, _| {
                state.start_task_for_target(TaskKind::Migrate, "Migrate 1 table".to_string(), None);
            });
        });

        let tabs_before = window.update(|_, cx| workspace.read(cx).tab_manager.read(cx).len());

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_migrate_wizard(
                    uuid::Uuid::new_v4(),
                    Some("app".to_string()),
                    vec![TableRef::new("users")],
                    window,
                    cx,
                );
            });
        });
        window.run_until_parked();

        let tabs_after = window.update(|_, cx| workspace.read(cx).tab_manager.read(cx).len());
        assert_eq!(
            tabs_after, tabs_before,
            "a running migration must block a second wizard"
        );

        let last_toast = window.update(|_, cx| {
            cx.global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .read(cx)
                .last_toast_title()
        });
        assert_eq!(
            last_toast,
            Some(dbflux_i18n::t!(
                "document.migrate_wizard.already_running_in_tasks"
            )),
            "the user is told why the wizard did not open"
        );
    }
}
