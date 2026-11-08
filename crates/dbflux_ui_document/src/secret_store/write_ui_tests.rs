//! The write actions of the secret store document, driven through the
//! rendered UI: the test clicks the drawn button, types into the drawn
//! dialog and confirms it, and the fake stores record what reached them.
//! Every action goes through the governed seam, so each test also reads the
//! audit trail the seam wrote.

use super::*;
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
