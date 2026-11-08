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

/// The effect line wraps inside the dialog body, from the same left edge as
/// the lead paragraph, instead of running past the dialog's right edge.
#[gpui::test]
fn the_roll_back_effect_line_wraps_inside_the_dialog(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-version-rollback-1");

    let effect = opened.bounds_of("secret-rollback-effect");
    let lead = opened.bounds_of("secret-rollback-lead");
    let confirm = opened.bounds_of("secret-rollback-confirm");

    assert!(
        effect.right() <= confirm.right(),
        "the effect line {effect:?} runs past the dialog edge {confirm:?}"
    );
    assert_eq!(effect.left(), lead.left(), "same left edge as the lead");
    assert!(
        f32::from(effect.size.height) > 20.0,
        "a long sentence wraps onto more than one line: {effect:?}"
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

// -- Versions table at the default width ---------------------------------------

/// At the default details width the table's cells sit side by side: no cell
/// overlaps its neighbour, the label chips stay inside their column and the
/// header is one line.
#[gpui::test]
fn the_versions_table_fits_the_default_details_width(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    let edge = |value: gpui::Pixels| f32::from(value);
    let header_labels = opened.bounds_of("secret-versions-col-labels");
    let one_line = f32::from(opened.bounds_of("secret-versions-col-version").size.height);
    assert!(
        f32::from(header_labels.size.height) <= one_line + 1.0,
        "the Staging labels header is one line: {header_labels:?} vs {one_line}"
    );

    for index in 0..3 {
        let id = opened.bounds_of(&format!("secret-version-{index}-id"));
        let labels = opened.bounds_of(&format!("secret-version-{index}-labels"));
        let created = opened.bounds_of(&format!("secret-version-{index}-created"));

        assert!(
            edge(id.right()) <= edge(labels.left()) + 0.5,
            "row {index}: id/labels"
        );
        assert!(
            edge(labels.right()) <= edge(created.left()) + 0.5,
            "row {index}: labels {labels:?} overlap created {created:?}"
        );

        if opened.is_drawn(&format!("secret-version-{index}-action")) {
            let action = opened.bounds_of(&format!("secret-version-{index}-action"));
            assert!(
                edge(created.right()) <= edge(action.left()) + 0.5,
                "row {index}: created/action"
            );
        }

        for chip in 0..2 {
            let chip_id = format!("secret-version-{index}-label-{chip}");

            if opened.is_drawn(&chip_id) {
                let chip = opened.bounds_of(&chip_id);
                assert!(
                    edge(chip.right()) <= edge(labels.right()) + 0.5,
                    "row {index}: chip {chip:?} runs out of its column {labels:?}"
                );
            }
        }
    }
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

/// The confirmation input's placeholder is a short hint, never the name
/// itself: a name drawn in the field reads as text that is already typed.
#[gpui::test]
fn the_delete_input_does_not_show_the_name_as_its_placeholder(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");

    let frame = opened.observe_frame();
    let placeholder = frame
        .nodes()
        .find(|(_, node)| node.id() == "secret-delete-name-input")
        .and_then(|(_, node)| frame.accessibility_node(node))
        .and_then(|node| node.placeholder().map(str::to_string))
        .unwrap_or_default();

    assert!(!placeholder.contains(KEY_VALUE_SECRET), "{placeholder}");
    assert!(!placeholder.is_empty(), "the input still has a hint");
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

/// A secret whose rotation is off draws Rotate now disabled, with the reason
/// in its accessible name, in the bar, the Rotation section, the row menu and
/// the pane actions; clicking calls nothing. The seam-side refusal stays the
/// backstop for a caller that gets past the button.
#[gpui::test]
fn rotate_now_is_disabled_with_the_reason_when_rotation_is_off(cx: &mut TestAppContext) {
    use dbflux_ui_base::keyboard_coverage::FrameCapture;

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(PLAINTEXT_SECRET);

    for id in ["secret-action-rotate-now", "secret-rotation-rotate-now"] {
        assert!(opened.is_disabled(id), "{id} is disabled");

        let capture = FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);
        let name = frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .and_then(|(_, node)| frame.accessibility_node(node))
            .and_then(|node| node.label().map(str::to_string))
            .unwrap_or_default();
        assert!(name.contains("Rotation is off"), "{id}: {name}");

        opened.click(id);
    }

    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
    assert!(
        opened.reported_errors().is_empty(),
        "nothing to refuse after"
    );

    opened.right_click(&format!("secret-row-{PLAINTEXT_SECRET}"));
    let rotate = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .iter()
            .find(|item| item.action == super::super::context_menu::SecretMenuAction::RotateNow)
            .map(|item| (item.disabled, item.detail.clone()))
            .expect("the menu offers Rotate now")
    });
    assert!(rotate.0, "the menu entry is disabled");
    assert!(
        rotate
            .1
            .is_some_and(|detail| detail.contains("Rotation is off")),
        "and says why"
    );
    opened.command(Command::Cancel);

    let enabled = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc)
            .into_iter()
            .find(|action| action.id == "secret-rotate-now")
            .map(|action| action.enabled)
    });
    assert_eq!(enabled, Some(false));

    // The backstop: a direct call is still refused with the reason, and the
    // store is not called.
    opened.update(|doc, cx| doc.rotate_selected_now(cx));
    assert!(opened.mutations().is_empty());
    assert_eq!(opened.reported_errors().len(), 1);
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

// -- Tags --------------------------------------------------------------------

/// Edit tags is a button of the Tags section and an entry of the row menu, on
/// a flat and a hierarchical store alike.
#[gpui::test]
fn edit_tags_is_offered_in_the_section_and_the_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    assert!(opened.is_drawn("secret-tags-edit"));
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    assert!(opened.menu_actions().contains(&"EditTags".to_string()));

    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.open_parameter("/app/prod/db/host");
    assert!(opened.is_drawn("secret-tags-edit"));
    opened.right_click("secret-row-/app/prod/db/host");
    assert!(opened.menu_actions().contains(&"EditTags".to_string()));
}

/// The dialog opens on the secret's tags, writes nothing on its own, and
/// Save sends only the difference: the tags that are new or changed and the
/// keys that are gone.
#[gpui::test]
fn edit_tags_sends_the_difference_through_the_seam(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-tags-edit");

    assert!(opened.is_drawn("secret-tags-dialog"));
    assert!(opened.is_drawn("secret-tags-key-0"));
    assert!(opened.is_drawn("secret-tags-key-1"));
    assert!(
        opened.is_disabled("secret-tags-confirm"),
        "nothing changed yet"
    );
    assert!(opened.mutations().is_empty());

    opened.fill_input("secret-tags-value-0", "billing");
    opened.click("secret-tags-remove-1");
    opened.click("secret-tags-add");
    opened.fill_input("secret-tags-key-1", "cost-center");
    opened.fill_input("secret-tags-value-1", "4021");
    assert!(!opened.is_disabled("secret-tags-confirm"));

    opened.click("secret-tags-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    let FakeSecretMutation::UpdateTags(update) = &mutations[0] else {
        panic!("{mutations:?}");
    };
    let set: Vec<(&str, &str)> = update
        .set
        .iter()
        .map(|tag| (tag.key.as_str(), tag.value.as_str()))
        .collect();
    assert_eq!(set, vec![("team", "billing"), ("cost-center", "4021")]);
    assert_eq!(update.remove, vec!["env".to_string()]);
    assert_eq!(opened.audit_actions(), vec!["secret_tags_updated"]);
    assert!(!opened.dialog_open());

    let text = opened.rendered_text();
    assert!(text.contains("team=billing"), "{text}");
    assert!(text.contains("cost-center=4021"), "{text}");
    assert!(!text.contains("env=prod"), "{text}");
}

/// The services' limits are the seam's own checks, shown under the rows:
/// the reserved prefix, an over-long value and a key in two rows. Save stays
/// disabled and nothing is sent.
#[gpui::test]
fn a_tag_the_seam_would_refuse_shows_its_message_and_sends_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-tags-edit");

    opened.fill_input("secret-tags-key-0", "aws:team");
    assert!(opened.is_disabled("secret-tags-confirm"));
    assert!(opened.is_drawn("secret-tags-problem"));
    let text = opened.rendered_text();
    assert!(text.contains("reserved prefix"), "{text}");

    opened.fill_input("secret-tags-key-0", "team");
    opened.fill_input("secret-tags-value-0", &"x".repeat(257));
    let text = opened.rendered_text();
    assert!(text.contains("longer than 256 characters"), "{text}");
    assert!(opened.is_disabled("secret-tags-confirm"));

    opened.fill_input("secret-tags-value-0", "payments");
    opened.fill_input("secret-tags-key-1", "team");
    let text = opened.rendered_text();
    assert!(text.contains("appears in more than one row"), "{text}");
    assert!(opened.is_disabled("secret-tags-confirm"));

    opened.click("secret-tags-confirm");
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
fn cancelling_edit_tags_writes_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-tags-edit");
    opened.click("secret-tags-remove-0");

    opened.click("secret-tags-cancel");

    assert!(!opened.dialog_open());
    assert!(opened.mutations().is_empty());
    assert!(opened.audit_actions().is_empty());
}

/// A parameter's last tag can be removed: the request is a removal only.
#[gpui::test]
fn removing_the_last_tag_of_a_parameter_sends_a_removal_only(cx: &mut TestAppContext) {
    let name = "/app/prod/db/host";
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.open_parameter(name);
    opened.click("secret-tags-edit");
    opened.click("secret-tags-remove-0");

    opened.click("secret-tags-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(
            &mutations[0],
            FakeSecretMutation::UpdateTags(update)
                if update.name == name && update.set.is_empty() && update.remove == vec!["app".to_string()]
        ),
        "{mutations:?}"
    );
    assert!(opened.rendered_text().contains("No tags"));
}

/// A refusal at the seam and a store failure are each reported once and keep
/// the dialog.
#[gpui::test]
fn a_refused_or_failed_tag_edit_is_reported_once(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );
    opened.click("secret-tags-edit");
    opened.click("secret-tags-remove-0");

    opened.click("secret-tags-confirm");

    assert!(opened.mutations().is_empty());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("read-only"), "{}", errors[0].1);
    assert!(opened.dialog_open());
    assert_eq!(opened.audit_actions(), vec!["secret_tags_update_refused"]);
}

#[gpui::test]
fn a_failed_tag_edit_names_its_cause_once(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_write_error(KEY_VALUE_SECRET, "not authorized to tag");
    let mut opened = open_with(cx, driver, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-tags-edit");
    opened.click("secret-tags-remove-0");

    opened.click("secret-tags-confirm");

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("not authorized"), "{}", errors[0].1);
    assert!(opened.dialog_open());
    assert_eq!(opened.audit_actions(), vec!["secret_tags_update_failed"]);
}

/// A managed secret takes no tag edit.
#[gpui::test]
fn a_managed_secret_cannot_have_its_tags_or_description_edited(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(MANAGED_SECRET);

    assert!(opened.is_disabled("secret-tags-edit"));
    assert!(opened.is_disabled("secret-details-edit"));
}

// -- Description ---------------------------------------------------------------

const NEW_DESCRIPTION: &str = "Rotated by the platform team";

/// A store that changes a description alone draws Edit in the Details
/// section; the dialog says nothing about a new version, Save waits for a
/// change, and the write creates no version.
#[gpui::test]
fn the_description_of_a_secret_is_edited_without_a_new_version(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    assert!(opened.is_drawn("secret-details-edit"));
    assert!(!opened.is_drawn("secret-action-edit-description"));
    let versions_before = opened.read(|doc| match doc.versions() {
        VersionsState::Loaded(versions) => versions.len(),
        _ => 0,
    });

    opened.click("secret-details-edit");

    assert!(opened.is_drawn("secret-description-dialog"));
    assert!(!opened.is_drawn("secret-description-rewrites"));
    assert!(opened.is_disabled("secret-description-confirm"));
    assert!(opened.mutations().is_empty());

    opened.fill_input("secret-description-input", NEW_DESCRIPTION);
    opened.click("secret-description-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(
            &mutations[0],
            FakeSecretMutation::UpdateDescription(update)
                if update.name == KEY_VALUE_SECRET && update.description == NEW_DESCRIPTION
        ),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_description_updated"]);
    assert!(!opened.dialog_open());
    assert!(opened.rendered_text().contains(NEW_DESCRIPTION));
    let versions_after = opened.read(|doc| match doc.versions() {
        VersionsState::Loaded(versions) => versions.len(),
        _ => 0,
    });
    assert_eq!(versions_after, versions_before);

    // The description text is not an audit field.
    let audit = format!("{:?}", opened.audit_records());
    assert!(!audit.contains(NEW_DESCRIPTION), "{audit}");
}

/// A store that changes a description by writing the value again says so in
/// the dialog before it is confirmed, draws Edit description in the bottom
/// bar, and the write names the version it created.
#[gpui::test]
fn a_parameters_description_edit_says_it_writes_a_new_version(cx: &mut TestAppContext) {
    let name = "/app/prod/db/host";
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.open_parameter(name);
    assert!(opened.is_drawn("secret-action-edit-description"));
    assert!(!opened.is_drawn("secret-details-edit"));
    let versions_before = opened.read(|doc| match doc.versions() {
        VersionsState::Loaded(versions) => versions.len(),
        _ => 0,
    });

    opened.click("secret-action-edit-description");

    assert!(opened.is_drawn("secret-description-rewrites"));
    let text = opened.rendered_text();
    assert!(
        text.contains("writes the value again as a new version"),
        "{text}"
    );
    assert!(text.contains("Save as new version"), "{text}");

    opened.fill_input("secret-description-input", NEW_DESCRIPTION);
    opened.click("secret-description-confirm");

    let mutations = opened.mutations();
    assert_eq!(mutations.len(), 1, "{mutations:?}");
    assert!(
        matches!(&mutations[0], FakeSecretMutation::UpdateDescription(update) if update.name == name),
        "{mutations:?}"
    );
    assert_eq!(opened.audit_actions(), vec!["secret_description_updated"]);
    let versions_after = opened.read(|doc| match doc.versions() {
        VersionsState::Loaded(versions) => versions.len(),
        _ => 0,
    });
    assert_eq!(versions_after, versions_before + 1, "the history lists it");
}

/// Cancelling writes nothing, and so does saving without a change.
#[gpui::test]
fn cancelling_edit_description_writes_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-details-edit");
    opened.fill_input("secret-description-input", NEW_DESCRIPTION);

    opened.click("secret-description-cancel");

    assert!(!opened.dialog_open());
    assert!(opened.mutations().is_empty());
    assert!(opened.audit_actions().is_empty());

    opened.click("secret-details-edit");
    let doc = opened.doc.clone();
    opened
        .window
        .update(|window, cx| doc.update(cx, |doc, cx| doc.confirm_dialog(window, cx)));
    opened.window.run_until_parked();
    assert!(
        opened.mutations().is_empty(),
        "an unchanged text sends nothing"
    );
}

/// A refusal at the seam and a store failure are each reported once and keep
/// the dialog; a store that cannot clear a description says so.
#[gpui::test]
fn a_refused_or_failed_description_edit_is_reported_once(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );
    opened.click("secret-details-edit");
    opened.fill_input("secret-description-input", NEW_DESCRIPTION);

    opened.click("secret-description-confirm");

    assert!(opened.mutations().is_empty());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("read-only"), "{}", errors[0].1);
    assert!(opened.dialog_open());
    assert_eq!(
        opened.audit_actions(),
        vec!["secret_description_update_refused"]
    );
}

#[gpui::test]
fn a_description_a_store_cannot_clear_is_reported_once(cx: &mut TestAppContext) {
    let name = "/app/prod/db/host";
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.open_parameter(name);

    // Give the parameter a description first, so clearing it is a change.
    opened.click("secret-action-edit-description");
    opened.fill_input("secret-description-input", NEW_DESCRIPTION);
    opened.click("secret-description-confirm");
    assert!(!opened.dialog_open());
    assert!(opened.reported_errors().is_empty());

    opened.click("secret-action-edit-description");
    opened.click("secret-description-input");
    opened.window.simulate_keystrokes("ctrl-a");
    opened.window.simulate_keystrokes("backspace");
    opened.window.run_until_parked();
    assert!(
        !opened.is_disabled("secret-description-confirm"),
        "a cleared description differs from the stored one"
    );

    opened.click("secret-description-confirm");

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("cannot be cleared"), "{}", errors[0].1);
    assert!(opened.dialog_open(), "the dialog stays for another text");
}

/// Every control of the tag and description dialogs has a keyboard path.
#[gpui::test]
fn the_tag_and_description_controls_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);

    let entries = opened.pane_action_ids();
    for id in ["secret-edit-tags", "secret-edit-description"] {
        assert!(entries.iter().any(|entry| entry == id), "{id}: {entries:?}");
    }

    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries.clone())
        .assert_covered(&capture.frame(opened.window));
    for id in ["secret-tags-edit", "secret-details-edit"] {
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
            .any(|id| id.starts_with("secret-menu-edit-tags")),
        "{checked:?}"
    );
    opened.command(Command::Cancel);

    opened.click("secret-tags-edit");
    opened.click("secret-tags-add");
    opened.fill_input("secret-tags-key-2", "owner");
    opened.fill_input("secret-tags-value-2", "platform");
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries.clone())
        .assert_covered(&capture.frame(opened.window));
    for id in [
        "secret-tags-confirm",
        "secret-tags-cancel",
        "secret-tags-add",
        "secret-tags-remove-0",
    ] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }
    opened.click("secret-tags-cancel");

    opened.click("secret-details-edit");
    opened.fill_input("secret-description-input", NEW_DESCRIPTION);
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));
    for id in ["secret-description-confirm", "secret-description-cancel"] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }
    opened.click("secret-description-cancel");

    // A store that rewrites the value for a description has the bar's button.
    let mut parameters = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    parameters.open_parameter("/app/prod/db/host");
    let entries = parameters.pane_action_ids();
    let capture = FrameCapture::observe(parameters.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(parameters.window));
    assert!(
        checked
            .iter()
            .any(|id| id == "secret-action-edit-description"),
        "{checked:?}"
    );
}

// -- Reload --------------------------------------------------------------------

/// A write reloads the whole listing, not only its first page: the store pages
/// at four entries here, and the rows past the first page are still listed.
#[gpui::test]
fn a_write_reloads_every_page_of_the_listing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    let before = opened.entry_names();
    assert_eq!(before.len(), 6, "{before:?}");
    opened.open_secret(KEY_VALUE_SECRET);

    opened.click("secret-tags-edit");
    opened.click("secret-tags-remove-0");
    opened.click("secret-tags-confirm");

    assert_eq!(opened.mutations().len(), 1);
    let after = opened.entry_names();
    assert_eq!(after.len(), 6, "{after:?}");
}

// -- Create --------------------------------------------------------------------

const CREATED_VALUE: &str = "created-secret-value-9021";

impl Opened<'_> {
    /// Replaces the text of the open create dialog's value area.
    fn set_create_value(&mut self, text: &str) {
        let value = self.read(|doc| match &doc.dialog {
            Some(WriteDialog::Create(dialog)) => dialog.value.clone(),
            _ => panic!("the create dialog is open"),
        });
        let text = text.to_string();

        self.window.update(|window, cx| {
            value.update(cx, |state, cx| state.set_value(&text, window, cx));
        });
        self.window.run_until_parked();
    }

    /// The create requests the fake store accepted, with the value read out.
    fn created(&mut self) -> Vec<(dbflux_core::SecretCreateRequest, String)> {
        use dbflux_core::secrecy::ExposeSecret as _;

        self.driver
            .secret_store_calls()
            .mutations
            .into_iter()
            .filter_map(|(_, mutation)| match mutation {
                FakeSecretMutation::Create(request) => {
                    let value = match &request.payload {
                        SecretPayload::Text(text) => text.expose_secret().to_string(),
                        SecretPayload::Binary(_) => String::new(),
                    };
                    Some((request, value))
                }
                _ => None,
            })
            .collect()
    }
}

/// New secret and New parameter are the toolbar's button, named by what the
/// store calls its entries.
#[gpui::test]
fn the_toolbar_offers_new_secret_or_new_parameter(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    assert!(opened.is_drawn("secret-new"));
    assert!(opened.rendered_text().contains("New secret"));

    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    opened.tall();
    assert!(opened.is_drawn("secret-new"));
    assert!(opened.rendered_text().contains("New parameter"));
}

/// As the listing narrows the New button gives way to its icon and then to
/// the pane actions menu, so the toolbar's other controls keep their room;
/// New stays reachable from the keyboard at every width.
#[gpui::test]
fn the_new_button_gives_way_as_the_listing_narrows(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));

    // The width of the button, `None` when the toolbar leaves it out.
    let width_at = |opened: &mut Opened<'_>, width: f32| -> Option<f32> {
        opened
            .window
            .simulate_resize(gpui::size(gpui::px(width), gpui::px(800.0)));
        opened.window.run_until_parked();

        opened
            .is_drawn("secret-new")
            .then(|| f32::from(opened.bounds_of("secret-new").size.width))
    };

    let wide = width_at(&mut opened, 1600.0).expect("drawn when wide");
    let narrower = width_at(&mut opened, 1100.0).expect("drawn as an icon");
    assert!(narrower < wide, "{narrower} < {wide}: the label gave way");
    assert_eq!(width_at(&mut opened, 700.0), None, "left out");
    assert!(
        opened.pane_action_ids().iter().any(|id| id == "secret-new"),
        "New stays in the pane actions"
    );
}

/// A parameter is created under the path being listed: the name starts at
/// it, the type, tier, description and tags go with the value, and the new
/// parameter is selected with its details.
#[gpui::test]
fn a_parameter_is_created_under_the_current_path(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.tall();
    opened.click("secret-new");

    assert!(opened.is_drawn("secret-create-dialog"));
    let name_input = opened.read(|doc| match &doc.dialog {
        Some(WriteDialog::Create(dialog)) => dialog.name.clone(),
        _ => panic!("the create dialog is open"),
    });
    let prefilled = opened
        .window
        .update(|_, cx| name_input.read(cx).value().to_string());
    assert_eq!(prefilled, "/app/prod/db/", "the name starts at the path");
    assert!(opened.is_disabled("secret-create-confirm"), "no value yet");
    assert!(opened.mutations().is_empty());

    opened.type_text("token");
    opened.click("secret-create-type-String");
    opened.click("secret-create-tier-Advanced");
    assert!(
        !opened.is_drawn("secret-create-key-input"),
        "only an encrypted value takes a key"
    );
    opened.set_create_value(CREATED_VALUE);
    opened.fill_input("secret-create-description-input", "A new token");
    opened.click("secret-create-tag-add");
    opened.fill_input("secret-create-tag-key-0", "owner");
    opened.fill_input("secret-create-tag-value-0", "platform");
    assert!(!opened.is_disabled("secret-create-confirm"));

    opened.click("secret-create-confirm");

    let created = opened.created();
    assert_eq!(created.len(), 1, "{:?}", opened.mutations());
    let (request, value) = &created[0];
    assert_eq!(request.name, "/app/prod/db/token");
    assert_eq!(value, CREATED_VALUE);
    assert_eq!(request.value_type.as_deref(), Some("String"));
    assert_eq!(request.tier.as_deref(), Some("Advanced"));
    assert_eq!(request.description.as_deref(), Some("A new token"));
    assert_eq!(request.encryption_key, None);
    assert_eq!(
        request
            .tags
            .iter()
            .map(|tag| (tag.key.as_str(), tag.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("owner", "platform")]
    );
    assert_eq!(opened.audit_actions(), vec!["secret_created"]);
    assert!(!opened.dialog_open());

    assert!(
        opened
            .entry_names()
            .contains(&"/app/prod/db/token".to_string())
    );
    assert_eq!(
        opened.read(|doc| doc.selected().cloned()),
        Some(SecretRowKey::Secret("/app/prod/db/token".to_string()))
    );
    let text = opened.rendered_text();
    assert!(text.contains("A new token"), "{text}");
}

/// An encrypted parameter takes an encryption key.
#[gpui::test]
fn an_encrypted_parameter_takes_the_key_that_was_typed(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.tall();
    opened.click("secret-new");
    opened.type_text("secure");
    assert!(
        opened.is_drawn("secret-create-key-input"),
        "SecureString is the default type and takes a key"
    );
    opened.set_create_value(CREATED_VALUE);
    opened.fill_input("secret-create-key-input", "alias/team");

    opened.click("secret-create-confirm");

    let created = opened.created();
    assert_eq!(created.len(), 1, "{:?}", opened.mutations());
    assert_eq!(created[0].0.value_type.as_deref(), Some("SecureString"));
    assert_eq!(created[0].0.encryption_key.as_deref(), Some("alias/team"));
}

/// A store whose values are not typed asks for no type and no tier, and sends
/// neither.
#[gpui::test]
fn a_secret_is_created_without_a_type_or_a_tier(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    opened.click("secret-new");

    assert!(!opened.is_drawn("secret-create-type-String"));
    assert!(!opened.is_drawn("secret-create-tier-Standard"));

    opened.type_text("prod/new/credentials");
    opened.set_create_value(CREATED_VALUE);
    opened.click("secret-create-confirm");

    let created = opened.created();
    assert_eq!(created.len(), 1, "{:?}", opened.mutations());
    let (request, value) = &created[0];
    assert_eq!(request.name, "prod/new/credentials");
    assert_eq!(value, CREATED_VALUE);
    assert_eq!(request.value_type, None);
    assert_eq!(request.tier, None);
    assert_eq!(opened.audit_actions(), vec!["secret_created"]);
    assert!(
        opened
            .entry_names()
            .contains(&"prod/new/credentials".to_string())
    );
    assert_eq!(
        opened.read(|doc| doc.selected().cloned()),
        Some(SecretRowKey::Secret("prod/new/credentials".to_string()))
    );
}

/// A name the listing already holds is refused before anything is sent.
#[gpui::test]
fn a_name_that_exists_is_refused_before_anything_is_sent(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    opened.click("secret-new");
    opened.type_text(KEY_VALUE_SECRET);
    opened.set_create_value(CREATED_VALUE);

    assert!(opened.is_disabled("secret-create-confirm"));
    assert!(opened.is_drawn("secret-create-problem"));
    let text = opened.rendered_text();
    assert!(text.contains("already exists"), "{text}");

    let doc = opened.doc.clone();
    opened
        .window
        .update(|window, cx| doc.update(cx, |doc, cx| doc.confirm_dialog(window, cx)));
    opened.window.run_until_parked();

    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
    assert!(opened.audit_actions().is_empty());
}

/// A name the listing does not show is refused by the store, and the secret
/// that has it is not overwritten.
#[gpui::test]
fn a_name_the_store_has_is_refused_and_never_overwrites(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    // The listing shows nothing, so only the store knows the name.
    opened.update(|doc, cx| doc.apply_name_filter("zzz".to_string(), cx));
    assert!(opened.entry_names().is_empty());

    opened.click("secret-new");
    opened.type_text(KEY_VALUE_SECRET);
    opened.set_create_value(CREATED_VALUE);
    opened.click("secret-create-confirm");

    assert!(opened.created().is_empty(), "nothing was created");
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        !errors[0].1.contains(CREATED_VALUE),
        "the refusal never carries the value: {}",
        errors[0].1
    );
    assert!(opened.dialog_open(), "the dialog stays for another name");
    assert_eq!(opened.audit_actions(), vec!["secret_create_failed"]);

    // The secret that has the name still reads its own value.
    opened.update(|doc, cx| doc.apply_name_filter(String::new(), cx));
    opened.select(KEY_VALUE_SECRET);
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    let text = opened.rendered_text();
    assert!(!text.contains(CREATED_VALUE), "{text}");
}

/// The value reaches no element id, no accessibility node and no rendered
/// frame while it is typed, after the write and in the audit trail.
#[gpui::test]
fn the_created_value_reaches_no_node_of_the_accessibility_update(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    opened.click("secret-new");
    opened.type_text("prod/new/credentials");
    opened.set_create_value(CREATED_VALUE);

    let typing_text = opened.accessibility_update_text();
    let typing_paths = opened.rendered_paths();
    assert!(!typing_text.contains(CREATED_VALUE), "{typing_text}");
    assert!(
        !typing_paths.iter().any(|path| path.contains(CREATED_VALUE)),
        "{typing_paths:?}"
    );

    opened.click("secret-create-confirm");

    let written_text = opened.accessibility_update_text();
    let written_paths = opened.rendered_paths();
    assert!(!written_text.contains(CREATED_VALUE), "{written_text}");
    assert!(
        !written_paths
            .iter()
            .any(|path| path.contains(CREATED_VALUE)),
        "{written_paths:?}"
    );
    let audit = format!("{:?}", opened.audit_records());
    assert!(!audit.contains(CREATED_VALUE), "{audit}");
    assert_eq!(opened.created()[0].1, CREATED_VALUE);
}

/// An incomplete request says what is missing under the fields, after the
/// first edit, and sends nothing.
#[gpui::test]
fn an_incomplete_create_shows_the_seams_message(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    opened.click("secret-new");
    assert!(
        !opened.is_drawn("secret-create-problem"),
        "an untouched form is not told off"
    );

    opened.type_text("prod/new/credentials");
    let text = opened.rendered_text();
    assert!(text.contains("needs a value"), "{text}");
    assert!(opened.is_disabled("secret-create-confirm"));
    assert!(opened.mutations().is_empty());
}

/// Cancelling writes nothing.
#[gpui::test]
fn cancelling_create_writes_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    opened.click("secret-new");
    opened.type_text("prod/new/credentials");
    opened.set_create_value(CREATED_VALUE);

    opened.click("secret-create-cancel");

    assert!(!opened.dialog_open());
    assert!(opened.mutations().is_empty());
    assert!(opened.audit_actions().is_empty());
}

/// A refusal at the seam and a store failure are each reported once and keep
/// the dialog, without the value.
#[gpui::test]
fn a_refused_or_failed_create_is_reported_once(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );
    opened.click("secret-new");
    opened.type_text("prod/new/credentials");
    opened.set_create_value(CREATED_VALUE);

    opened.click("secret-create-confirm");

    assert!(opened.mutations().is_empty());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("read-only"), "{}", errors[0].1);
    assert!(!errors[0].1.contains(CREATED_VALUE));
    assert!(opened.dialog_open());
    assert_eq!(opened.audit_actions(), vec!["secret_create_refused"]);
}

#[gpui::test]
fn a_failed_create_names_its_cause_once(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_write_error("prod/new/credentials", "not authorized to create");
    let mut opened = open_with(cx, driver, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    opened.click("secret-new");
    opened.type_text("prod/new/credentials");
    opened.set_create_value(CREATED_VALUE);

    opened.click("secret-create-confirm");

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("not authorized"), "{}", errors[0].1);
    assert!(!errors[0].1.contains(CREATED_VALUE));
    assert!(opened.dialog_open());
    assert_eq!(opened.audit_actions(), vec!["secret_create_failed"]);
}

/// Every control of the create dialog and the toolbar button has a keyboard
/// path.
#[gpui::test]
fn the_create_controls_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.tall();

    let entries = opened.pane_action_ids();
    assert!(
        entries.iter().any(|entry| entry == "secret-new"),
        "{entries:?}"
    );
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries.clone())
        .assert_covered(&capture.frame(opened.window));
    assert!(checked.iter().any(|id| id == "secret-new"), "{checked:?}");

    opened.click("secret-new");
    opened.type_text("covered");
    opened.set_create_value(CREATED_VALUE);
    opened.click("secret-create-tag-add");
    opened.fill_input("secret-create-tag-key-0", "owner");
    opened.fill_input("secret-create-tag-value-0", "platform");
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));
    for id in [
        "secret-create-confirm",
        "secret-create-cancel",
        "secret-create-type-String",
        "secret-create-tier-Standard",
        "secret-create-tag-add",
        "secret-create-tag-remove-0",
    ] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }
}

// -- Dialogs are pinned to the session and the secret they opened on -----------

impl Opened<'_> {
    /// Confirms the open dialog the way Enter does.
    fn confirm_open_dialog(&mut self) {
        let doc = self.doc.clone();
        self.window
            .update(|window, cx| doc.update(cx, |doc, cx| doc.confirm_dialog(window, cx)));
        self.window.run_until_parked();
    }

    /// Asserts that a confirm after a connection replacement wrote nothing to
    /// either session and reported the refusal once.
    fn assert_pinned_refusal(&mut self, replacement: &FakeDriver) {
        assert!(self.mutations().is_empty(), "old: {:?}", self.mutations());
        assert!(
            replacement
                .secret_store_calls()
                .mutation_attempts
                .is_empty(),
            "new: {:?}",
            replacement.secret_store_calls().mutation_attempts
        );
        assert!(replacement.secret_store_calls().put_attempts.is_empty());

        let errors = self.reported_errors();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].1.contains("was replaced"), "{}", errors[0].1);
        assert!(
            !self.dialog_open(),
            "a dialog that can no longer write closes"
        );
    }
}

#[gpui::test]
fn a_delete_dialog_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let name = "/app/prod/db/host";
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.open_parameter(name);
    opened.click("secret-action-delete");
    opened.type_text(name);
    let replacement = opened.replace_connection();

    opened.confirm_open_dialog();

    opened.assert_pinned_refusal(&replacement);
}

#[gpui::test]
fn a_scheduled_delete_dialog_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");
    opened.type_text(KEY_VALUE_SECRET);
    let replacement = opened.replace_connection();

    opened.confirm_open_dialog();

    opened.assert_pinned_refusal(&replacement);
}

#[gpui::test]
fn a_tags_dialog_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-tags-edit");
    opened.click("secret-tags-remove-0");
    let replacement = opened.replace_connection();

    opened.confirm_open_dialog();

    opened.assert_pinned_refusal(&replacement);
}

#[gpui::test]
fn a_description_dialog_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-details-edit");
    opened.fill_input("secret-description-input", NEW_DESCRIPTION);
    let replacement = opened.replace_connection();

    opened.confirm_open_dialog();

    opened.assert_pinned_refusal(&replacement);
}

#[gpui::test]
fn a_rotation_dialog_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-rotation-edit");
    opened.fill_input("secret-rotation-schedule-input", "rate(7 days)");
    let replacement = opened.replace_connection();

    opened.confirm_open_dialog();

    opened.assert_pinned_refusal(&replacement);
}

#[gpui::test]
fn a_create_dialog_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.tall();
    opened.click("secret-new");
    opened.type_text("prod/new/credentials");
    opened.set_create_value(CREATED_VALUE);
    let replacement = opened.replace_connection();

    opened.confirm_open_dialog();

    opened.assert_pinned_refusal(&replacement);
}

#[gpui::test]
fn a_roll_back_dialog_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-version-rollback-1");
    let replacement = opened.replace_connection();

    opened.confirm_open_dialog();

    assert!(opened.mutations().is_empty());
    assert!(
        replacement
            .secret_store_calls()
            .mutation_attempts
            .is_empty()
    );
    assert!(!opened.dialog_open());
}

/// Moving to another secret closes a dialog that names the first one, so a
/// confirm can never target a secret other than the one the dialog names.
#[gpui::test]
fn a_selection_change_closes_the_delete_dialog(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");
    assert!(opened.dialog_open());

    opened.select(PLAINTEXT_SECRET);
    assert!(!opened.dialog_open(), "the dialog named another secret");

    opened.confirm_open_dialog();
    assert!(opened.mutations().is_empty(), "{:?}", opened.mutations());
}

/// A reload of the listing closes an open dialog too.
#[gpui::test]
fn a_reload_closes_the_open_dialog(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.click("secret-action-delete");
    assert!(opened.dialog_open());

    opened.update(|doc, cx| doc.refresh(cx));

    assert!(!opened.dialog_open());
}

// -- The details pane scrolls above its action bar -------------------------------

/// At the default window height a secret with versions, rotation and tags is
/// taller than the pane: every control is reachable by wheel scrolling, lies
/// inside the scroll viewport, and none runs under the action bar.
#[gpui::test]
fn every_control_of_the_details_pane_is_reachable_above_the_action_bar(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened
        .window
        .simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(700.0)));
    opened.window.run_until_parked();
    opened.select(KEY_VALUE_SECRET);
    opened.window.run_until_parked();

    let viewport = opened.bounds_of("secret-details-body");
    let bar = opened.bounds_of("secret-action-bar");
    assert!(
        viewport.bottom() <= bar.top() + gpui::px(0.5),
        "the scroll area ends above the bar: {viewport:?} / {bar:?}"
    );

    let controls = [
        "secret-retrieve-value",
        "secret-details-edit",
        "secret-version-rollback-1",
        "secret-rotation-edit",
        "secret-rotation-rotate-now",
        "secret-tags-edit",
    ];

    for id in controls {
        let mut steps = 0;

        loop {
            let bounds = opened.bounds_of(id);
            let inside = bounds.top() >= viewport.top() && bounds.bottom() <= viewport.bottom();

            if inside {
                assert!(
                    bounds.bottom() <= bar.top() + gpui::px(0.5),
                    "{id} {bounds:?} runs under the bar {bar:?}"
                );
                break;
            }

            assert!(steps < 40, "{id} never became reachable: {bounds:?}");
            steps += 1;

            opened.window.simulate_event(gpui::ScrollWheelEvent {
                position: viewport.center(),
                delta: gpui::ScrollDelta::Pixels(gpui::point(
                    gpui::px(0.0),
                    gpui::px(if bounds.top() < viewport.top() {
                        120.0
                    } else {
                        -120.0
                    }),
                )),
                ..Default::default()
            });
            opened.window.run_until_parked();
        }
    }
}
