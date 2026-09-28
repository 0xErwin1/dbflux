//! Keyboard access to the schema diff document.
//!
//! The document reports the Results key context, so J/K and the arrows move
//! a cursor over its rows (the comparison mode, the reference databases,
//! connections or snapshots, Compute, every applicable change and the
//! Preview / Apply footer), H and L move between a row's buttons, Enter or I
//! presses the button under the cursor and Space checks a change. M lists
//! the document's buttons through the workspace's pane actions menu, and F5
//! computes the diff again.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{
    RailMenuEntry, RailNav, RailOutcome, RailOwner, RailRow, RailTarget, rail_command,
};
use dbflux_components::icons::AppIcon;
use gpui::{App, Context, Entity, FocusHandle, SharedString, Window};

use super::diff_source::{DiffMode, TableActionOutcome};
use super::view::SchemaDiffDocument;
use crate::pane::PaneAction;

pub(super) const MODE_ROW: &str = "mode";
pub(super) const MODE_LIVE_FIELD: &str = "mode-live";
pub(super) const MODE_SNAPSHOT_FIELD: &str = "mode-snapshot";
pub(super) const COMPUTE_ROW: &str = "compute";
pub(super) const COMPUTE_FIELD: &str = "compute-diff";
pub(super) const FOOTER_ROW: &str = "footer";
pub(super) const PREVIEW_FIELD: &str = "preview-ddl";
pub(super) const APPLY_FIELD: &str = "apply-ddl";

/// The field of a one-button row (a reference or a change).
const SELECT_FIELD: &str = "select";

pub(super) fn change_row_id(group_index: usize, change_index: usize) -> String {
    format!("chk-{group_index}-{change_index}")
}

pub(super) fn table_action_row_id(group_index: usize) -> String {
    format!("chk-table-{group_index}")
}

/// The rows the cursor stops on, in the order the document draws them.
pub(super) fn rail_rows(
    document: &SchemaDiffDocument,
    cx: &App,
) -> Vec<RailRow<SchemaDiffDocument>> {
    let mut rows = vec![
        RailRow::new(MODE_ROW)
            .field(
                MODE_LIVE_FIELD,
                RailTarget::run(|this: &mut SchemaDiffDocument, _window, cx| {
                    this.set_mode(DiffMode::LiveVsLive, cx)
                }),
            )
            .field(
                MODE_SNAPSHOT_FIELD,
                RailTarget::run(|this: &mut SchemaDiffDocument, _window, cx| {
                    this.set_mode(DiffMode::SnapshotVsLive, cx)
                }),
            ),
    ];

    match document.picker().mode {
        DiffMode::LiveVsLive => {
            let (databases, connections) = document.live_reference_candidates(cx);

            for database in databases {
                let id = format!("ref-db-{database}");
                rows.push(select_row(id, move |this, cx| {
                    this.select_same_connection_database(database.clone(), cx)
                }));
            }

            for (profile_id, _name) in connections {
                rows.push(select_row(
                    format!("ref-conn-{profile_id}"),
                    move |this, cx| this.select_reference_connection(profile_id, cx),
                ));
            }
        }
        DiffMode::SnapshotVsLive => {
            for (snapshot_id, _label) in document.snapshot_options(cx) {
                rows.push(select_row(
                    format!("snap-{snapshot_id}"),
                    move |this, cx| this.select_snapshot(snapshot_id, cx),
                ));
            }
        }
    }

    rows.push(RailRow::new(COMPUTE_ROW).field(
        COMPUTE_FIELD,
        RailTarget::run(|this: &mut SchemaDiffDocument, _window, cx| {
            this.compute_from_keyboard(cx)
        }),
    ));

    for (group_index, group) in document.groups().iter().enumerate() {
        for change_index in 0..group.applicable.len() {
            let toggle = move |this: &mut SchemaDiffDocument,
                               _: &mut Window,
                               cx: &mut Context<SchemaDiffDocument>| {
                this.toggle_selection(group_index, change_index, cx)
            };
            rows.push(
                RailRow::new(change_row_id(group_index, change_index))
                    .field(SELECT_FIELD, RailTarget::run(toggle))
                    .on_toggle(toggle),
            );
        }

        if matches!(
            group.table_action,
            Some(TableActionOutcome::Applicable { .. })
        ) {
            let toggle = move |this: &mut SchemaDiffDocument,
                               _: &mut Window,
                               cx: &mut Context<SchemaDiffDocument>| {
                this.toggle_table_action_selection(group_index, cx)
            };
            rows.push(
                RailRow::new(table_action_row_id(group_index))
                    .field(SELECT_FIELD, RailTarget::run(toggle))
                    .on_toggle(toggle),
            );
        }
    }

    rows.push(
        RailRow::new(FOOTER_ROW)
            .field(
                PREVIEW_FIELD,
                RailTarget::run(|this: &mut SchemaDiffDocument, _window, cx| {
                    if this.can_act_on_selection() {
                        this.open_preview(cx);
                    }
                }),
            )
            .field(
                APPLY_FIELD,
                RailTarget::run(|this: &mut SchemaDiffDocument, _window, cx| {
                    if this.can_act_on_selection() {
                        this.request_apply(cx);
                    }
                }),
            ),
    );

    rows
}

fn select_row(
    id: String,
    select: impl Fn(&mut SchemaDiffDocument, &mut Context<SchemaDiffDocument>) + 'static,
) -> RailRow<SchemaDiffDocument> {
    RailRow::new(id).field(
        SELECT_FIELD,
        RailTarget::run(move |this: &mut SchemaDiffDocument, _window, cx| select(this, cx)),
    )
}

impl RailOwner for SchemaDiffDocument {
    fn rail_nav(&mut self) -> &mut RailNav<Self> {
        &mut self.rail
    }

    fn rail_rows(&self, cx: &App) -> Vec<RailRow<Self>> {
        rail_rows(self, cx)
    }

    /// The document's buttons are listed by the workspace's pane actions
    /// menu instead of a rail menu.
    fn rail_actions(&self, _cx: &App) -> Vec<RailMenuEntry<Self>> {
        Vec::new()
    }

    fn rail_focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle_ref().clone()
    }
}

impl SchemaDiffDocument {
    pub(super) fn keyboard_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            // Left to the workspace, which lists `pane_actions`.
            Command::OpenPaneActions | Command::OpenContextMenu => false,
            Command::RefreshSchema => {
                self.compute_from_keyboard(cx);
                true
            }
            _ => match rail_command(self, command, window, cx) {
                RailOutcome::Handled | RailOutcome::Leave => true,
                RailOutcome::Unhandled => false,
            },
        }
    }

    /// Computes the diff when the Compute button would take a click.
    fn compute_from_keyboard(&mut self, cx: &mut Context<Self>) {
        if self.can_compute() && !self.is_busy() {
            self.compute_diff(cx);
        }
    }

    /// Whether Preview DDL and Apply would take a click.
    fn can_act_on_selection(&self) -> bool {
        self.has_selection() && !self.is_busy()
    }

    /// The mode chips, Compute, Preview DDL and Apply, each enabled when its
    /// button is.
    pub(crate) fn pane_actions(&self, this: &Entity<Self>) -> Vec<PaneAction> {
        let entry =
            |id: &'static str,
             label: String,
             icon: AppIcon,
             enabled: bool,
             run: fn(&mut SchemaDiffDocument, &mut Context<SchemaDiffDocument>)| {
                let target = this.downgrade();
                PaneAction::callback(SharedString::from(id), label, move |_window, cx| {
                    if let Some(document) = target.upgrade() {
                        document.update(cx, run);
                    }
                })
                .icon(icon)
                .enabled(enabled)
            };

        let mut compute = entry(
            "schema-diff-compute",
            dbflux_i18n::t!("document.schema_diff.action.compute_diff"),
            AppIcon::Play,
            self.can_compute() && !self.is_busy(),
            |document, cx| document.compute_from_keyboard(cx),
        );
        compute.shortcut =
            dbflux_ui_base::keymap::shortcut_label(ContextId::Results, Command::RefreshSchema);

        vec![
            compute,
            entry(
                "schema-diff-preview",
                dbflux_i18n::t!("document.schema_diff.action.preview_ddl"),
                AppIcon::Eye,
                self.can_act_on_selection(),
                |document, cx| document.open_preview(cx),
            ),
            entry(
                "schema-diff-apply",
                dbflux_i18n::t!("document.schema_diff.action.apply"),
                AppIcon::Check,
                self.can_act_on_selection(),
                |document, cx| document.request_apply(cx),
            ),
            entry(
                "schema-diff-mode-live",
                dbflux_i18n::t!("document.schema_diff.view.mode.live"),
                AppIcon::Database,
                self.picker().mode != DiffMode::LiveVsLive,
                |document, cx| document.set_mode(DiffMode::LiveVsLive, cx),
            ),
            entry(
                "schema-diff-mode-snapshot",
                dbflux_i18n::t!("document.schema_diff.view.mode.snapshot"),
                AppIcon::History,
                self.picker().mode != DiffMode::SnapshotVsLive,
                |document, cx| document.set_mode(DiffMode::SnapshotVsLive, cx),
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::super::diff_source::DiffMode;
    use super::super::view::SchemaDiffDocument;
    use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
    use dbflux_core::{ConnectionProfile, DbConfig, SchemaSnapshotRecord, SnapshotDepth};
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::sync::Arc;
    use uuid::Uuid;

    /// A schema diff of a saved SQLite profile with one stored snapshot,
    /// hosted like the workspace hosts it.
    fn open(cx: &mut TestAppContext) -> (Entity<SchemaDiffDocument>, Uuid, &mut VisualTestContext) {
        init_keyboard_runtime(cx);

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("storage"),
                )
                .expect("app state")
            })
        });
        let profile = ConnectionProfile::new("test", DbConfig::default_sqlite());
        let profile_id = profile.id;
        app_state.update(cx, |state, _| state.add_profile_in_folder(profile, None));

        let snapshot_id = Uuid::now_v7();
        let repo = cx.update(|cx| Arc::clone(&app_state.read(cx).inner.schema_snapshot_repo));
        repo.insert(&SchemaSnapshotRecord {
            id: snapshot_id,
            profile_id,
            database: Some("db".into()),
            captured_at: 1,
            fingerprint: "fingerprint".into(),
            depth: SnapshotDepth::Shallow,
            tables: Vec::new(),
            creation_metadata: Vec::new(),
        })
        .expect("snapshot");

        let (host, window) = host_document(
            cx,
            move |window, cx| {
                cx.new(|cx| {
                    SchemaDiffDocument::new(profile_id, Some("db".into()), app_state, window, cx)
                })
            },
            |document, _cx| document.active_context(),
            |document, command, window, cx| document.dispatch_command(command, window, cx),
        );

        let document = window.update(|_, cx| host.read(cx).document.clone());
        window.update(|window, cx| document.update(cx, |d, cx| d.focus(window, cx)));
        window.run_until_parked();

        (document, snapshot_id, window)
    }

    fn keys(window: &mut VisualTestContext, keystrokes: &str) {
        for keystroke in keystrokes.split(' ') {
            window.simulate_keystrokes(keystroke);
            window.run_until_parked();
        }
    }

    #[gpui::test]
    fn the_cursor_picks_the_mode_and_the_snapshot(cx: &mut TestAppContext) {
        let (document, snapshot_id, window) = open(cx);

        keys(window, "l enter");
        assert_eq!(
            window.update(|_, cx| document.read(cx).picker().mode),
            DiffMode::SnapshotVsLive,
            "l then Enter presses the Snapshot chip"
        );

        keys(window, "j enter");
        assert_eq!(
            window.update(|_, cx| document.read(cx).picker().selected_snapshot),
            Some(snapshot_id),
            "j then Enter selects the listed snapshot"
        );

        keys(window, "j");
        assert_eq!(
            window.update(|_, cx| document.read(cx).rail.cursor_row().cloned()),
            Some(super::COMPUTE_ROW.into()),
            "the row after the references is Compute"
        );
    }

    #[gpui::test]
    fn the_pane_actions_list_the_buttons(cx: &mut TestAppContext) {
        let (document, _snapshot_id, window) = open(cx);

        let actions = window.update(|_, cx| document.read(cx).pane_actions(&document));
        let listed: Vec<(String, bool)> = actions
            .iter()
            .map(|action| (action.id.to_string(), action.enabled))
            .collect();

        assert_eq!(
            listed,
            [
                ("schema-diff-compute".to_string(), false),
                ("schema-diff-preview".to_string(), false),
                ("schema-diff-apply".to_string(), false),
                ("schema-diff-mode-live".to_string(), false),
                ("schema-diff-mode-snapshot".to_string(), true),
            ]
        );
    }
}
