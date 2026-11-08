//! The write actions of the secret store document, driven through the
//! rendered UI: the test clicks the drawn button, types into the drawn
//! dialog and confirms it, and the fake stores record what reached them.
//! Every action goes through the governed seam, so each test also reads the
//! audit trail the seam wrote.

use super::super::actions::DeleteKind;
use super::super::actions::WriteDialog;
use super::*;
use dbflux_core::SecretDeleteRequest;
use dbflux_core::SecretVersionStage;
use dbflux_test_support::fake_driver::{FAKE_ROTATION_FUNCTION, FakeSecretMutation};

impl Opened<'_> {
    /// Makes the window tall enough that the whole details pane is drawn,
    /// so a button at the bottom of it can be clicked.
    fn tall(&mut self) {
        self.window
            .simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(1400.0)));
        self.window.run_until_parked();
    }

    /// The bounds of the drawn element `id`.
    fn bounds_of(&mut self, id: &str) -> gpui::Bounds<gpui::Pixels> {
        let frame = self.observe_frame();

        frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.bounds())
            .unwrap_or_else(|| panic!("`{id}` is drawn"))
    }

    fn is_drawn(&mut self, id: &str) -> bool {
        let frame = self.observe_frame();

        frame.nodes().any(|(_, node)| node.id() == id)
    }

    /// Whether the drawn element `id` is disabled for the accessibility
    /// tree, which is what a screen reader and UI automation see.
    fn is_disabled(&mut self, id: &str) -> bool {
        let frame = self.observe_frame();

        frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .and_then(|(_, node)| frame.accessibility_node(node))
            .map(|node| node.is_disabled())
            .unwrap_or_else(|| panic!("`{id}` is drawn"))
    }

    /// Types `text` where the keyboard is, the way a user does.
    fn type_text(&mut self, text: &str) {
        self.window.simulate_input(text);
        self.window.run_until_parked();
    }

    /// Opens the secret store of `store_id` on the parameter path
    /// `/app/prod/db/` and selects `name`.
    fn open_parameter(&mut self, name: &str) {
        self.tall();
        self.load_everything();
        self.select(name);
        self.window.run_until_parked();
    }

    /// Replaces the text of the drawn input `id` the way a user does: click
    /// it, select everything and type.
    fn fill_input(&mut self, id: &str, text: &str) {
        self.click(id);
        self.window.simulate_keystrokes("ctrl-a");
        self.window.run_until_parked();
        self.type_text(text);
    }

    /// Left-clicks the drawn element `id`, press and release on its bounds.
    fn click(&mut self, id: &str) {
        let bounds = self.bounds_of(id);

        self.window
            .simulate_click(bounds.center(), gpui::Modifiers::default());
        self.window.run_until_parked();
    }

    /// Every write operation that reached the stores, accepted or not.
    fn mutations(&mut self) -> Vec<FakeSecretMutation> {
        self.driver
            .secret_store_calls()
            .mutation_attempts
            .into_iter()
            .map(|(_, mutation)| mutation)
            .collect()
    }

    /// The actions of the audit events the seam recorded, oldest first.
    fn audit_actions(&mut self) -> Vec<String> {
        self.audit_records()
            .iter()
            .filter_map(|event| event.action.clone())
            .collect()
    }

    fn dialog_open(&mut self) -> bool {
        self.read(|doc| doc.dialog_is_open())
    }

    /// Selects `name` and lets its details and history load.
    fn open_secret(&mut self, name: &str) {
        self.tall();
        self.select(name);
        self.window.run_until_parked();
    }
}

// -- Roll back -----------------------------------------------------------------

/// Every version that is not current carries a Roll back button, the
/// current one does not, and the footnote says what a roll back does.
#[gpui::test]
fn roll_back_is_offered_on_every_version_but_the_current_one(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    assert!(
        !opened.is_drawn("secret-version-rollback-0"),
        "the current version has nothing to roll back to"
    );
    assert!(opened.is_drawn("secret-version-rollback-1"));
    assert!(opened.is_drawn("secret-version-rollback-2"));

    let text = opened.rendered_text();
    assert!(text.contains("No value is rewritten"), "{text}");
}

/// A store without version labels, or without writes, draws no Roll back
/// button: the gate is the store's capabilities, never its id.
#[gpui::test]
fn a_store_without_version_labels_draws_no_roll_back(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    opened.tall();
    opened.load_everything();
    let name = opened
        .entry_names()
        .into_iter()
        .next()
        .expect("the store lists a parameter");
    opened.select(&name);
    assert!(
        opened.read(|doc| doc.selected_summary().is_some()),
        "the parameter is selected and its details are drawn"
    );

    let text = opened.rendered_text();
    assert!(!text.contains("No value is rewritten"), "{text}");
    assert!(!opened.is_drawn("secret-version-rollback-1"));
}

/// Clicking Roll back asks first: the dialog names the version, nothing
/// reaches the store, and the row's own retrieval does not run.
#[gpui::test]
fn roll_back_asks_before_it_writes_and_names_the_version(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    opened.click("secret-version-rollback-1");

    assert!(opened.dialog_open());
    assert!(opened.is_drawn("secret-rollback-dialog"));
    let text = opened.rendered_text();
    assert!(text.contains("v-previous"), "{text}");
    assert!(text.contains(KEY_VALUE_SECRET), "{text}");
    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
    assert!(
        opened
            .driver
            .secret_store_calls()
            .value_version_requests
            .is_empty(),
        "the button must not also retrieve the version"
    );
}

/// Cancelling the dialog writes nothing and audits nothing.
#[gpui::test]
fn cancelling_the_roll_back_writes_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-version-rollback-1");

    opened.click("secret-rollback-cancel");

    assert!(!opened.dialog_open());
    assert!(opened.mutations().is_empty());
    assert!(opened.audit_actions().is_empty());
}

/// Escape and the dialog's close button cancel it as well.
#[gpui::test]
fn the_roll_back_dialog_closes_on_its_close_button(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-version-rollback-1");

    opened.click("modal-close");

    assert!(!opened.dialog_open());
    assert!(opened.mutations().is_empty());
}

/// Confirming writes through the seam — one roll back request naming the
/// chosen version, one audit event — and the history reads from the store
/// again, so the chosen version is current.
#[gpui::test]
fn confirming_the_roll_back_writes_through_the_seam_and_refreshes_the_history(
    cx: &mut TestAppContext,
) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-version-rollback-1");

    opened.click("secret-rollback-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(
            &mutations[0],
            FakeSecretMutation::RollBack(request)
                if request.name == KEY_VALUE_SECRET && request.version_id == "v-previous"
        ),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_rolled_back"]);
    assert!(!opened.dialog_open());

    let current = opened.read(|doc| match doc.versions() {
        VersionsState::Loaded(versions) => versions
            .iter()
            .find(|version| version.is_current)
            .map(|version| version.id.clone()),
        _ => None,
    });
    assert_eq!(current.as_deref(), Some("v-previous"));
    assert!(
        opened
            .driver
            .secret_store_calls()
            .value_version_requests
            .is_empty(),
        "a roll back rewrites and retrieves no value"
    );
}

/// A read-only connection refuses the roll back at the seam: the refusal is
/// reported once, the store is never called, the dialog stays open and the
/// refusal is audited.
#[gpui::test]
fn a_read_only_connection_refuses_the_roll_back_and_reports_it_once(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );
    opened.click("secret-version-rollback-1");

    opened.click("secret-rollback-confirm");

    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("read-only"), "{}", errors[0].1);
    assert!(
        opened.dialog_open(),
        "the dialog stays open after a refusal"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_rollback_refused"]);
}

/// A store failure is reported once with its correlation id and keeps the
/// dialog open.
#[gpui::test]
fn a_failed_roll_back_is_reported_once_and_keeps_the_dialog(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_write_error(KEY_VALUE_SECRET, "not authorized to roll back");
    let mut opened = open_with(cx, driver, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-version-rollback-1");

    opened.click("secret-rollback-confirm");

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("not authorized"), "{}", errors[0].1);
    assert!(opened.dialog_open());
    assert_eq!(opened.audit_actions(), vec!["secret_rollback_failed"]);
}

/// A secret another service manages, and one whose deletion is scheduled,
/// refuse every write with the reason the pane shows; the roll back key opens
/// nothing for them.
#[gpui::test]
fn a_managed_or_deleting_secret_refuses_the_roll_back(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();

    opened.open_secret(MANAGED_SECRET);
    let reason = opened.read(|doc| doc.write_refusal_reason());
    assert!(
        reason
            .as_deref()
            .is_some_and(|reason| reason.contains("rds")),
        "{reason:?}"
    );
    assert!(!opened.read(|doc| doc.can_roll_back_version(0)));

    opened.open_secret(PENDING_DELETION_SECRET);
    let reason = opened.read(|doc| doc.write_refusal_reason());
    assert!(
        reason
            .as_deref()
            .is_some_and(|reason| reason.contains("Deletion is scheduled")),
        "{reason:?}"
    );

    opened.command(Command::RollBackVersion);
    assert!(!opened.dialog_open());
    assert!(opened.mutations().is_empty());
}

/// `b` rolls back to the most recent earlier version: the same dialog, with
/// nothing written until it is confirmed.
#[gpui::test]
fn the_b_key_opens_the_roll_back_of_the_previous_version(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    assert!(opened.command(Command::RollBackVersion));

    assert!(opened.dialog_open());
    let text = opened.rendered_text();
    assert!(text.contains("v-previous"), "{text}");
    assert!(opened.mutations().is_empty());
}

/// Roll back's buttons are covered: each one has its pane action, and the
/// dialog's controls answer Enter and Escape.
#[gpui::test]
fn the_roll_back_controls_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::Coverage;

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    let entries = opened.pane_action_ids();
    assert!(
        entries.iter().any(|id| id == "secret-version-rollback-1"),
        "{entries:?}"
    );

    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries.clone())
        .assert_covered(&capture.frame(opened.window));
    assert!(
        checked.iter().any(|id| id == "secret-version-rollback-1"),
        "{checked:?}"
    );

    opened.click("secret-version-rollback-1");
    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));
    assert!(
        checked.iter().any(|id| id == "secret-rollback-confirm"),
        "{checked:?}"
    );
}

// -- Delete and restore ------------------------------------------------------

/// The same Delete is reachable from the pane header, the bottom bar and the
/// row menu, each opening the one dialog and writing nothing.
#[gpui::test]
fn delete_is_reachable_from_the_header_the_bar_and_the_row_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    for id in ["secret-details-delete", "secret-action-delete"] {
        assert!(opened.is_drawn(id), "{id}");
        opened.click(id);
        assert!(opened.is_drawn("secret-delete-dialog"), "{id} opens it");
        opened.click("secret-delete-cancel");
        assert!(!opened.dialog_open());
    }

    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    let actions = opened.menu_actions();
    let index = actions
        .iter()
        .position(|action| action == "Delete")
        .unwrap_or_else(|| panic!("the menu offers Delete: {actions:?}"));
    opened.click_menu_item(&format!("secret-menu-delete-item-{index}"));
    assert!(opened.is_drawn("secret-delete-dialog"));

    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
}

/// The `x` key opens the same dialog for the selected secret.
#[gpui::test]
fn the_x_key_opens_the_delete_dialog(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    assert!(opened.command(Command::Delete));

    assert!(opened.is_drawn("secret-delete-dialog"));
    assert!(opened.mutations().is_empty());
}

/// A store with a recovery window offers the window, 7 to 30 days with 30
/// picked, and never an option to delete without recovery.
#[gpui::test]
fn a_store_with_a_recovery_window_never_offers_deleting_without_recovery(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");

    for days in [7, 14, 30] {
        assert!(
            opened.is_drawn(&format!("secret-delete-window-{days}")),
            "{days}"
        );
    }
    for days in [0, 1, 6, 31, 365] {
        assert!(
            !opened.is_drawn(&format!("secret-delete-window-{days}")),
            "{days} is outside what the service accepts"
        );
    }

    let kind = opened.read(|doc| match &doc.dialog {
        Some(WriteDialog::Delete(dialog)) => Some(dialog.kind),
        _ => None,
    });
    assert_eq!(
        kind,
        Some(DeleteKind::Scheduled { window_days: 30 }),
        "30 days is the default"
    );

    let text = opened.rendered_text().to_lowercase();
    assert!(text.contains("recovery window"), "{text}");
    assert!(!text.contains("permanent"), "{text}");
    assert!(!text.contains("cannot be undone"), "{text}");
}

/// The Delete button stays disabled until the exact name is typed, for a
/// store with a window as for one without.
#[gpui::test]
fn delete_waits_for_the_exact_name_and_nothing_is_written_before(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");

    assert!(opened.is_disabled("secret-delete-confirm"));

    opened.type_text("prod/api/credential");
    assert!(
        opened.is_disabled("secret-delete-confirm"),
        "a name that is one letter short does not confirm"
    );

    opened.type_text("s ");
    assert!(
        opened.is_disabled("secret-delete-confirm"),
        "a trailing space does not confirm"
    );

    // Enter and a click on the disabled button both write nothing.
    opened.window.simulate_keystrokes("enter");
    opened.window.run_until_parked();
    opened.click("secret-delete-confirm");
    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
    assert!(opened.dialog_open());

    // The dialog's own confirm is the last line: whatever reaches it before
    // the name matches sends nothing.
    let doc = opened.doc.clone();
    opened
        .window
        .update(|window, cx| doc.update(cx, |doc, cx| doc.confirm_dialog(window, cx)));
    opened.window.run_until_parked();
    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
}

/// Typing the name, picking 14 days and confirming schedules the deletion
/// through the seam, audited once; the secret stays listed with its state
/// and a Restore action.
#[gpui::test]
fn confirming_a_scheduled_deletion_writes_the_window_and_shows_the_state(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");
    opened.click("secret-delete-window-14");
    opened.type_text(KEY_VALUE_SECRET);
    assert!(!opened.is_disabled("secret-delete-confirm"));

    opened.click("secret-delete-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(
            &mutations[0],
            FakeSecretMutation::Delete(SecretDeleteRequest::Scheduled {
                name,
                recovery_window_days: 14,
            }) if name == KEY_VALUE_SECRET
        ),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_delete_scheduled"]);
    assert!(!opened.dialog_open());

    // The listing and the details read the store again: the secret is
    // scheduled, shows the deletion and offers Restore.
    assert!(opened.read(|doc| {
        doc.selected_summary()
            .is_some_and(|summary| summary.is_pending_deletion())
    }));
    assert!(opened.is_drawn("secret-restore"));
    let text = opened.rendered_text();
    assert!(text.contains("Deletion scheduled for"), "{text}");
    assert!(text.contains("14 day recovery window"), "{text}");
}

/// Cancelling the dialog writes nothing and audits nothing.
#[gpui::test]
fn cancelling_the_delete_writes_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");
    opened.type_text(KEY_VALUE_SECRET);

    opened.click("secret-delete-cancel");

    assert!(!opened.dialog_open());
    assert!(opened.mutations().is_empty());
    assert!(opened.audit_actions().is_empty());
}

/// A store without a recovery window says the deletion cannot be undone,
/// takes the typed name and sends it as the confirmation of an irreversible
/// request on the selected row's name.
#[gpui::test]
fn a_parameter_is_deleted_only_after_its_exact_name_is_typed(cx: &mut TestAppContext) {
    let name = "/app/prod/db/host";
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.open_parameter(name);
    opened.click("secret-action-delete");

    assert!(opened.is_drawn("secret-delete-dialog"));
    assert!(
        !opened.is_drawn("secret-delete-window-7"),
        "no window to pick on a store without one"
    );
    let text = opened.rendered_text();
    assert!(text.contains("cannot be undone"), "{text}");
    assert!(text.contains("Delete parameter"), "{text}");
    assert!(opened.is_disabled("secret-delete-confirm"));

    opened.type_text("/app/prod/db/hos");
    assert!(opened.is_disabled("secret-delete-confirm"));
    opened.type_text("t");
    assert!(!opened.is_disabled("secret-delete-confirm"));

    opened.click("secret-delete-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(
            &mutations[0],
            FakeSecretMutation::Delete(SecretDeleteRequest::Irreversible { name: sent, confirmation })
                if sent == name && confirmation == name
        ),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_deleted"]);

    // The parameter is gone: the listing no longer has it and nothing of it
    // stays selected.
    assert!(!opened.entry_names().iter().any(|entry| entry == name));
    assert!(opened.read(|doc| doc.selected().is_none()));
}

/// A refusal at the seam is reported once and keeps the dialog; so is a
/// store failure.
#[gpui::test]
fn a_refused_or_failed_delete_is_reported_once_and_keeps_the_dialog(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );
    opened.click("secret-action-delete");
    opened.type_text(KEY_VALUE_SECRET);

    opened.click("secret-delete-confirm");

    assert!(opened.mutations().is_empty());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("read-only"), "{}", errors[0].1);
    assert!(opened.dialog_open());
    assert_eq!(opened.audit_actions(), vec!["secret_delete_refused"]);
}

#[gpui::test]
fn a_failed_delete_names_its_cause_once(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_write_error(KEY_VALUE_SECRET, "not authorized to delete");
    let mut opened = open_with(cx, driver, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");
    opened.type_text(KEY_VALUE_SECRET);

    opened.click("secret-delete-confirm");

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("not authorized"), "{}", errors[0].1);
    assert!(opened.dialog_open());
    assert_eq!(opened.audit_actions(), vec!["secret_delete_failed"]);
}

/// A managed secret and one already scheduled cannot be deleted: the buttons
/// are disabled and the key reports why without opening anything.
#[gpui::test]
fn a_managed_secret_cannot_be_deleted(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(MANAGED_SECRET);

    assert!(opened.is_disabled("secret-action-delete"));
    assert!(opened.is_disabled("secret-details-delete"));

    opened.command(Command::Delete);

    assert!(!opened.dialog_open());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("rds"), "{}", errors[0].1);
    assert!(opened.mutations().is_empty());
}

/// A scheduled deletion shows its state — when it was requested, when it
/// takes effect and the window — and Restore cancels it through the seam.
#[gpui::test]
fn a_scheduled_deletion_shows_its_state_and_restore_cancels_it(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);

    let text = opened.rendered_text();
    assert!(text.contains("Deletion scheduled for"), "{text}");
    assert!(text.contains("recovery window"), "{text}");
    assert!(text.contains("Deletes on"), "{text}");
    assert!(opened.is_drawn("secret-restore"));
    assert!(
        opened.is_disabled("secret-action-delete"),
        "a deletion that is already scheduled cannot be scheduled again"
    );

    opened.click("secret-restore");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(&mutations[0], FakeSecretMutation::Restore { name } if name == PENDING_DELETION_SECRET),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_restored"]);
    assert!(!opened.is_drawn("secret-restore"));
    assert!(opened.read(|doc| {
        doc.selected_summary()
            .is_some_and(|summary| !summary.is_pending_deletion())
    }));
}

/// A read-only connection refuses Restore at the seam, once.
#[gpui::test]
fn a_read_only_connection_refuses_restore(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );

    opened.click("secret-restore");

    assert!(opened.mutations().is_empty());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(opened.audit_actions(), vec!["secret_restore_refused"]);
    assert!(opened.is_drawn("secret-restore"), "still scheduled");
}

/// The bottom bar's Edit value retrieves the value first when it is not
/// loaded, then opens the editor on it.
#[gpui::test]
fn the_bars_edit_value_retrieves_the_value_and_opens_the_editor(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(PLAINTEXT_SECRET);
    assert!(!opened.read(|doc| doc.is_editing_value()));

    opened.click("secret-action-edit-value");

    assert!(opened.read(|doc| doc.is_editing_value()));
    assert_eq!(opened.editor_text(), FAKE_SECRET_PLAINTEXT);
    assert!(
        opened.mutations().is_empty(),
        "editing alone writes nothing"
    );
}

/// Every control the delete and restore actions draw has a keyboard path,
/// the dialog's included.
#[gpui::test]
fn the_delete_and_restore_controls_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);

    let entries = opened.pane_action_ids();
    assert!(
        entries.iter().any(|id| id == "secret-delete"),
        "{entries:?}"
    );

    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries.clone())
        .assert_covered(&capture.frame(opened.window));
    for id in [
        "secret-details-delete",
        "secret-action-delete",
        "secret-action-edit-value",
    ] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }

    opened.click("secret-action-delete");
    // A disabled button is no clickable element, so the name is typed to
    // enable the confirm button the registry must cover.
    opened.type_text(KEY_VALUE_SECRET);
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));
    for id in [
        "secret-delete-confirm",
        "secret-delete-cancel",
        "secret-delete-window-14",
    ] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }
    opened.click("secret-delete-cancel");

    // A scheduled secret: Restore is a pane action.
    opened.open_secret(PENDING_DELETION_SECRET);
    let entries = opened.pane_action_ids();
    assert!(
        entries.iter().any(|id| id == "secret-restore"),
        "{entries:?}"
    );
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));
    assert!(
        checked.iter().any(|id| id == "secret-restore"),
        "{checked:?}"
    );
}

// -- Rotation ------------------------------------------------------------------

const ROTATION_ARN: &str = "arn:aws:lambda:us-east-1:123456789012:function:rotate-hooks";

/// Rotate now and Edit rotation are drawn in the Rotation section, the bottom
/// bar and the row menu.
#[gpui::test]
fn rotation_actions_are_offered_in_the_section_the_bar_and_the_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    for id in [
        "secret-rotation-edit",
        "secret-rotation-rotate-now",
        "secret-action-rotate-now",
    ] {
        assert!(opened.is_drawn(id), "{id}");
    }
    let text = opened.rendered_text();
    assert!(text.contains("Rotate immediately"), "{text}");
    assert!(text.contains("Edit rotation"), "{text}");

    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    assert!(opened.menu_actions().contains(&"RotateNow".to_string()));
}

/// A store without rotation draws none of it.
#[gpui::test]
fn a_store_without_rotation_draws_no_rotation_action(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.open_parameter("/app/prod/db/host");

    for id in [
        "secret-rotation-edit",
        "secret-rotation-rotate-now",
        "secret-action-rotate-now",
    ] {
        assert!(!opened.is_drawn(id), "{id}");
    }
}

/// Rotate now starts the rotation through the seam: one request, one audit
/// event, and the history shows the version it created.
#[gpui::test]
fn rotate_now_starts_a_rotation_through_the_seam(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    let before = opened.read(|doc| match doc.versions() {
        VersionsState::Loaded(versions) => versions.len(),
        _ => 0,
    });

    opened.click("secret-rotation-rotate-now");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(&mutations[0], FakeSecretMutation::RotateNow { name } if name == KEY_VALUE_SECRET),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_rotation_started"]);
    let after = opened.read(|doc| match doc.versions() {
        VersionsState::Loaded(versions) => versions.len(),
        _ => 0,
    });
    assert_eq!(after, before + 1, "the history lists the new version");
    assert!(opened.reported_errors().is_empty());
}

/// The bar's and the menu's Rotate now run the same action.
#[gpui::test]
fn the_bar_and_the_menu_start_the_same_rotation(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-rotate-now");
    assert_eq!(opened.mutations().len(), 1);

    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    let index = opened
        .menu_actions()
        .iter()
        .position(|action| action == "RotateNow")
        .expect("the menu offers Rotate now");
    opened.click_menu_item(&format!("secret-menu-rotate-now-item-{index}"));

    // The second rotation is refused by the store while the first is
    // unfinished or accepted again; either way both reached the seam.
    assert_eq!(opened.mutations().len(), 2, "{:?}", opened.mutations());
}

/// A secret whose rotation is off is refused with the reason, and the store is
/// never called.
#[gpui::test]
fn rotate_now_on_a_secret_without_rotation_shows_the_refusal_and_calls_nothing(
    cx: &mut TestAppContext,
) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(PLAINTEXT_SECRET);

    opened.click("secret-action-rotate-now");

    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("Rotation is off"), "{}", errors[0].1);
    assert!(opened.audit_actions().is_empty());

    opened.click("secret-rotation-rotate-now");
    assert!(opened.mutations().is_empty());
    assert_eq!(opened.reported_errors().len(), 2, "each press is told why");
}

/// A read-only connection refuses Rotate now at the seam.
#[gpui::test]
fn a_read_only_connection_refuses_rotate_now(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );

    opened.click("secret-rotation-rotate-now");

    assert!(opened.mutations().is_empty());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("read-only"), "{}", errors[0].1);
    assert_eq!(opened.audit_actions(), vec!["secret_rotate_refused"]);
}

/// A managed secret cannot be rotated from here.
#[gpui::test]
fn a_managed_secret_cannot_be_rotated_or_edited(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(MANAGED_SECRET);

    for id in [
        "secret-action-rotate-now",
        "secret-rotation-rotate-now",
        "secret-rotation-edit",
    ] {
        assert!(opened.is_disabled(id), "{id}");
    }
}

/// Edit rotation opens on the secret's own settings, and Save sends one
/// request with them changed.
#[gpui::test]
fn edit_rotation_changes_the_schedule_through_the_seam(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-rotation-edit");

    assert!(opened.is_drawn("secret-rotation-dialog"));
    assert!(!opened.is_disabled("secret-rotation-confirm"));
    assert!(opened.mutations().is_empty());

    opened.fill_input("secret-rotation-schedule-input", "rate(7 days)");
    opened.click("secret-rotation-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(
            &mutations[0],
            FakeSecretMutation::UpdateRotation(update)
                if update.enabled
                    && update.schedule.as_deref() == Some("rate(7 days)")
                    && update.window.as_deref() == Some("4h")
                    && update.function.as_deref() == Some(FAKE_ROTATION_FUNCTION)
        ),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_rotation_updated"]);
    assert!(!opened.dialog_open());
    let text = opened.rendered_text();
    assert!(text.contains("Every 7 days"), "{text}");
}

/// Turning rotation on for a secret that has none: the fields start empty,
/// say nothing until touched, and the request carries what was typed.
#[gpui::test]
fn edit_rotation_turns_rotation_on_with_what_was_typed(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(PLAINTEXT_SECRET);
    opened.click("secret-rotation-edit");

    assert!(
        opened.is_disabled("secret-rotation-confirm"),
        "nothing to save while rotation stays off"
    );
    assert!(
        opened.rendered_text().contains("already off"),
        "{}",
        opened.rendered_text()
    );

    opened.click("secret-rotation-enabled");
    assert!(
        opened.is_disabled("secret-rotation-confirm"),
        "no schedule yet"
    );

    opened.fill_input("secret-rotation-schedule-input", "rate(7 days)");
    opened.fill_input("secret-rotation-window-input", "2h");
    opened.fill_input("secret-rotation-function-input", ROTATION_ARN);
    assert!(!opened.is_disabled("secret-rotation-confirm"));

    opened.click("secret-rotation-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(
            &mutations[0],
            FakeSecretMutation::UpdateRotation(update)
                if update.enabled
                    && update.schedule.as_deref() == Some("rate(7 days)")
                    && update.window.as_deref() == Some("2h")
                    && update.function.as_deref() == Some(ROTATION_ARN)
        ),
        "{mutations:?}"
    );
    assert!(opened.rendered_text().contains("Enabled"));
}

/// The seam's validation message shows under the fields, Save stays disabled
/// and nothing is sent.
#[gpui::test]
fn a_rotation_the_seam_would_refuse_shows_its_message_and_sends_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-rotation-edit");

    opened.fill_input("secret-rotation-schedule-input", "every day");
    assert!(opened.is_disabled("secret-rotation-confirm"));
    assert!(opened.is_drawn("secret-rotation-problem"));
    let text = opened.rendered_text();
    assert!(text.contains("is not a valid schedule"), "{text}");

    opened.fill_input("secret-rotation-schedule-input", "rate(4 hours)");
    opened.fill_input("secret-rotation-window-input", "12h");
    let text = opened.rendered_text();
    assert!(text.contains("longer than the 4-hour interval"), "{text}");
    assert!(opened.is_disabled("secret-rotation-confirm"));

    opened.fill_input("secret-rotation-window-input", "2h");
    opened.fill_input("secret-rotation-function-input", "not-an-arn");
    let text = opened.rendered_text();
    assert!(text.contains("is not a Lambda function ARN"), "{text}");
    assert!(opened.is_disabled("secret-rotation-confirm"));

    opened.click("secret-rotation-confirm");
    opened.window.simulate_keystrokes("enter");
    opened.window.run_until_parked();

    // The dialog's own confirm is the last line, whatever reaches it.
    let doc = opened.doc.clone();
    opened
        .window
        .update(|window, cx| doc.update(cx, |doc, cx| doc.confirm_dialog(window, cx)));
    opened.window.run_until_parked();

    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
    assert!(
        opened.audit_actions().is_empty(),
        "the dialog sends nothing, so not even a refusal reaches the seam"
    );
    assert!(opened.dialog_open());
}

/// Cancelling the dialog writes nothing.
#[gpui::test]
fn cancelling_edit_rotation_writes_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-rotation-edit");
    opened.fill_input("secret-rotation-schedule-input", "rate(9 days)");

    opened.click("secret-rotation-cancel");

    assert!(!opened.dialog_open());
    assert!(opened.mutations().is_empty());
    assert!(opened.audit_actions().is_empty());
}

/// Turning rotation off sends a request without settings, and a rotation the
/// store reports unfinished is shown as a warning naming its version.
#[gpui::test]
fn cancelling_rotation_names_the_version_a_rotation_left_pending(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    // A rotation that never finished: a version holding the pending stage.
    opened
        .governed_write(
            &SecretPutRequest::text(KEY_VALUE_SECRET, "{}").with_stage(SecretVersionStage::Pending),
        )
        .expect("the fake accepts a staged write");
    opened.update(|doc, cx| {
        doc.request_versions(
            KEY_VALUE_SECRET.to_string(),
            super::super::HistoryLoad::Automatic,
            cx,
        )
    });
    let audit_before = opened.audit_actions().len();

    opened.click("secret-rotation-edit");
    opened.click("secret-rotation-enabled");
    opened.click("secret-rotation-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(
            &mutations[0],
            FakeSecretMutation::UpdateRotation(update)
                if !update.enabled
                    && update.schedule.is_none()
                    && update.window.is_none()
                    && update.function.is_none()
        ),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions().len(), audit_before + 1);

    let version = opened
        .read(|doc| {
            doc.rotation_notice
                .as_ref()
                .map(|notice| notice.version.clone())
        })
        .expect("the store reported an unfinished rotation");
    assert!(opened.is_drawn("secret-rotation-notice"));
    let text = opened.rendered_text();
    assert!(text.contains("left version"), "{text}");
    assert!(text.contains(&version), "{text}");
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains(&version), "{}", errors[0].1);

    // Moving to another secret drops the notice.
    opened.select(PLAINTEXT_SECRET);
    assert!(!opened.is_drawn("secret-rotation-notice"));
}

/// A failed rotation edit is reported once and keeps the dialog.
#[gpui::test]
fn a_failed_rotation_edit_is_reported_once_and_keeps_the_dialog(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_write_error(KEY_VALUE_SECRET, "not authorized to edit rotation");
    let mut opened = open_with(cx, driver, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-rotation-edit");
    opened.fill_input("secret-rotation-schedule-input", "rate(7 days)");

    opened.click("secret-rotation-confirm");

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("not authorized"), "{}", errors[0].1);
    assert!(opened.dialog_open());
    assert_eq!(
        opened.audit_actions(),
        vec!["secret_rotation_update_failed"]
    );
}

/// The rotation controls and the dialog's are covered.
#[gpui::test]
fn the_rotation_controls_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    let entries = opened.pane_action_ids();
    for id in ["secret-rotate-now", "secret-edit-rotation"] {
        assert!(entries.iter().any(|entry| entry == id), "{id}: {entries:?}");
    }

    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries.clone())
        .assert_covered(&capture.frame(opened.window));
    for id in [
        "secret-rotation-edit",
        "secret-rotation-rotate-now",
        "secret-action-rotate-now",
    ] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }

    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries.clone())
        .assert_covered(&capture.frame(opened.window));
    assert!(
        checked
            .iter()
            .any(|id| id.starts_with("secret-menu-rotate-now")),
        "{checked:?}"
    );
    opened.command(Command::Cancel);

    opened.click("secret-rotation-edit");
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));
    for id in [
        "secret-rotation-confirm",
        "secret-rotation-cancel",
        "secret-rotation-enabled",
    ] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }
}
