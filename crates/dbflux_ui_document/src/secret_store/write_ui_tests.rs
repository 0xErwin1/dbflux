//! The write actions of the secret store document, driven through the
//! rendered UI: the test clicks the drawn button, types into the drawn
//! dialog and confirms it, and the fake stores record what reached them.
//! Every action goes through the governed seam, so each test also reads the
//! audit trail the seam wrote.

use super::super::actions::DeleteKind;
use super::super::actions::WriteDialog;
use super::*;
use dbflux_core::SecretDeleteRequest;
use dbflux_test_support::fake_driver::FakeSecretMutation;

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
