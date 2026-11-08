//! Follow-ups to the extracted single-secret view: what a disconnect does to
//! a retrieved value, how a stale answer is discarded, which version the
//! Versions table highlights, and how a restore reconciles the guards.

use super::super::view::Landing;
use super::*;
use std::cell::Cell;
use std::rc::Rc;

impl Opened<'_> {
    /// Drops the profile's live connection, as a disconnect does.
    fn disconnect(&mut self) {
        let app_state = self.app_state.clone();
        let profile_id = self.read(|doc| doc.profile_id);

        self.window.update(|_, cx| {
            app_state.update(cx, |state, _| state.connections_mut().remove(&profile_id))
        });
    }

    /// Counts the times the view asks for a redraw from now on.
    fn count_view_notifications(&mut self) -> Rc<Cell<usize>> {
        let doc = self.doc.clone();
        let counter = Rc::new(Cell::new(0));
        let seen = counter.clone();

        self.window.update(|_, cx| {
            let view = doc.read(cx).view.clone();
            cx.observe(&view, move |_, _| seen.set(seen.get() + 1))
                .detach();
        });

        counter
    }
}

/// A value retrieved from a connection the profile no longer holds is
/// dropped when the profile disconnects, so it cannot be revealed or copied.
#[gpui::test]
fn a_disconnect_drops_the_retrieved_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert!(opened.shown_password().is_some(), "the value was retrieved");

    opened.disconnect();
    opened.announce_state_change();

    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert_eq!(opened.shown_password(), None);
    assert!(!opened.read_view(|view| view.can_edit_value()));
}

/// A retrieval that lands after the disconnect leaves no value, and the
/// section does not stay loading.
#[gpui::test]
fn a_retrieval_landing_after_a_disconnect_leaves_no_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.start_retrieval();
    opened.disconnect();
    opened.window.run_until_parked();

    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert_eq!(opened.shown_password(), None);
}

/// The stale answer is discarded with a redraw, so the section stops
/// showing "retrieving" at once.
#[gpui::test]
fn discarding_a_stale_retrieval_redraws_the_view(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.start_retrieval();
    opened.replace_connection();
    let redraws = opened.count_view_notifications();
    opened.window.run_until_parked();

    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert!(redraws.get() > 0, "the view was never told to redraw");
}

/// The retrieval is audited against the driver of the session it read,
/// whether or not the profile still holds a connection.
#[gpui::test]
fn the_retrieval_audit_names_the_driver_of_the_originating_session(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    let driver_id = opened.live_connection().metadata().id.clone();
    assert!(!driver_id.is_empty());

    opened.start_retrieval();
    opened.disconnect();
    opened.window.run_until_parked();

    let recorded: Vec<Option<String>> = opened
        .audit_records()
        .into_iter()
        .filter(|event| event.action.as_deref() == Some(VALUE_RETRIEVED_ACTION))
        .map(|event| event.driver_id)
        .collect();
    assert_eq!(recorded, vec![Some(driver_id)]);
}

/// The Versions table highlights the version of the value on display, which
/// stays the retrieved one after a rotation made another current.
#[gpui::test]
fn the_versions_highlight_follows_the_displayed_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    let shown = opened
        .read_view(|view| match view.value() {
            ValueState::Loaded(value) => value.version.clone(),
            _ => None,
        })
        .expect("the retrieval returned its version");

    opened.rotate_current(KEY_VALUE_SECRET, "rotated");
    opened.update_view(|view, cx| {
        view.describe(KEY_VALUE_SECRET.to_string(), cx);
        view.load_versions(KEY_VALUE_SECRET.to_string(), cx);
    });

    assert!(opened.read_view(|view| view.shown_older_version().is_some()));
    assert_eq!(
        opened.read_view(|view| view.highlighted_version().map(str::to_string)),
        Some(shown)
    );
}

/// A store without a version history keeps saying so for a secret whose
/// deletion is scheduled.
#[gpui::test]
fn a_store_without_versions_stays_unsupported_for_a_pending_secret(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.update_view(|view, _| {
        view.store
            .capabilities
            .remove(SecretStoreCapabilities::VERSIONS)
    });
    opened.open_secret(PENDING_DELETION_SECRET);

    assert!(opened.read_view(|view| view.deletion_pending()));
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotSupported)));
}

/// A confirmed restore lifts the deletion from the view's copies of the row
/// at once, so the retrieval and history guards do not wait for reads that
/// may fail or come back stale.
#[gpui::test]
fn a_confirmed_restore_unblocks_the_guards_before_any_read_lands(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    assert!(!opened.read_view(|view| view.can_retrieve_value()));

    let doc = opened.doc.clone();
    opened.window.update(|_, cx| {
        let view = doc.read(cx).view.clone();
        view.update(cx, |view, cx| {
            view.land_restore(PENDING_DELETION_SECRET, Landing::Done(()), cx)
        });
    });

    assert!(!opened.read_view(|view| view.deletion_pending()));
    assert!(opened.read_view(|view| view.can_retrieve_value()));
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Loading)));
}

/// A listing that shows the secret active again is the authority over the
/// describe it outdates: the history resumes.
#[gpui::test]
fn a_listing_showing_the_secret_active_resumes_the_history(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    assert!(
        opened.read_view(|view| matches!(view.versions(), VersionsState::ScheduledForDeletion))
    );

    opened
        .live_connection()
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("store")
        .restore_secret(PENDING_DELETION_SECRET)
        .expect("restored");
    let mut active = opened
        .read_view(|view| view.listed_summary().cloned())
        .expect("the listing holds the row");
    active.deletion_scheduled_for = None;
    opened.update_view(|view, cx| view.set_listing_row(Some(active), true, cx));

    assert!(!opened.version_ids().is_empty(), "the history loaded");
    assert!(opened.reported_errors().is_empty());
}

/// A store that reports only the date the deletion takes effect shows that
/// date as "Deletes on" and invents neither a "Deleted" moment nor a window.
#[gpui::test]
fn a_scheduled_deletion_without_a_request_moment_shows_only_its_date(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    opened.update_view(|view, cx| {
        if let DetailsState::Loaded(details) = &mut view.details {
            details.deleted_at = None;
        }
        cx.notify();
    });

    let text = opened.rendered_text();

    assert!(text.contains("Deletion scheduled for"), "{text}");
    assert!(text.contains("Deletes on"), "{text}");
    assert!(!text.contains("recovery window"), "{text}");
    assert!(!text.contains("was deleted on"), "{text}");
    assert!(!text.contains("Deleted"), "{text}");
}

/// When the store does report the moment the deletion was requested, the
/// details and the banner keep showing it with the window it implies.
#[gpui::test]
fn a_scheduled_deletion_with_a_request_moment_keeps_showing_it(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);

    let text = opened.rendered_text();

    assert!(text.contains("Deletes on"), "{text}");
    assert!(text.contains("Deleted"), "{text}");
    assert!(text.contains("recovery window"), "{text}");
}

// -- A disconnect is not an error ---------------------------------------------

impl Opened<'_> {
    /// Removes the profile's connection and returns it, so a test can bring it back.
    fn take_connection(&mut self) -> dbflux_app::app_state::ConnectedProfile {
        let app_state = self.app_state.clone();
        let profile_id = self.read(|doc| doc.profile_id);

        self.window
            .update(|_, cx| {
                app_state.update(cx, |state, _| state.connections_mut().remove(&profile_id))
            })
            .expect("the profile was connected")
    }

    fn restore_connection(&mut self, connected: dbflux_app::app_state::ConnectedProfile) {
        let app_state = self.app_state.clone();
        let profile_id = self.read(|doc| doc.profile_id);

        self.window.update(|_, cx| {
            app_state.update(cx, |state, _| {
                state.connections_mut().insert(profile_id, connected);
            })
        });
    }
}

/// A user-initiated disconnect reports nothing, asks for nothing, and the
/// Versions section says the profile is not connected.
#[gpui::test]
fn a_disconnect_reports_nothing_and_shows_a_neutral_history(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    let listings = opened.driver.secret_store_calls().version_listings.len();

    let _connected = opened.take_connection();
    opened.announce_state_change();

    assert!(
        opened.reported_errors().is_empty(),
        "{:?}",
        opened.reported_errors()
    );
    assert_eq!(opened.toast_count(), 0);
    assert!(
        opened
            .audit_records()
            .iter()
            .all(|event| event.action.as_deref() != Some("user_error"))
    );
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotConnected)));
    assert_eq!(
        opened.driver.secret_store_calls().version_listings.len(),
        listings
    );
    let text = opened.rendered_text();
    assert!(text.contains("Not connected"), "{text}");
    assert!(!text.contains("could not be loaded"), "{text}");
    assert!(!text.contains("not available"), "{text}");
}

/// The history loads again when the connection returns.
#[gpui::test]
fn the_history_loads_again_on_reconnect(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    let connected = opened.take_connection();
    opened.announce_state_change();
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotConnected)));

    opened.restore_connection(connected);
    opened.announce_state_change();

    assert!(!opened.version_ids().is_empty());
    assert!(opened.reported_errors().is_empty());
}

/// Every action that needs the connection is disabled while there is none.
#[gpui::test]
fn the_actions_that_need_the_connection_are_disabled_while_disconnected(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let enabled = |opened: &mut Opened<'_>| {
        opened.read_view(|view| {
            [
                view.can_retrieve_value(),
                view.can_request_edit_value(),
                view.can_delete_selected(),
                view.can_rotate_now(),
                view.can_edit_rotation(),
                view.can_edit_tags(),
                view.can_edit_description(),
                view.can_roll_back_version(1),
            ]
        })
    };
    assert_eq!(
        enabled(&mut opened),
        [true; 8],
        "all offered while connected"
    );

    let connected = opened.take_connection();
    opened.announce_state_change();

    assert_eq!(enabled(&mut opened), [false; 8]);

    opened.restore_connection(connected);
    opened.announce_state_change();
    assert!(opened.read_view(|view| view.can_retrieve_value()));
}

/// Restore waits for a connection too.
#[gpui::test]
fn restore_is_disabled_while_disconnected(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    assert!(opened.read_view(|view| view.can_restore_selected()));

    let _connected = opened.take_connection();
    opened.announce_state_change();

    assert!(!opened.read_view(|view| view.can_restore_selected()));
}

/// New secret waits for a connection.
#[gpui::test]
fn new_secret_is_disabled_while_disconnected(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    assert!(opened.read_cx(|doc, cx| doc.can_open_create(cx)));

    let _connected = opened.take_connection();
    opened.announce_state_change();

    assert!(!opened.read_cx(|doc, cx| doc.can_open_create(cx)));
}

/// A write pressed after the connection dropped, before the view heard of it,
/// is still reported.
#[gpui::test]
fn a_write_pressed_as_the_connection_drops_is_still_reported(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    let _connected = opened.take_connection();

    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    assert_eq!(opened.reported_errors().len(), 1);
    assert_eq!(opened.toast_count(), 1);
}

/// A history the user asked for, with no connection to ask, is reported once.
#[gpui::test]
fn a_requested_history_without_a_connection_is_reported_once(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    let _connected = opened.take_connection();

    opened.update_view(|view, cx| view.load_versions(KEY_VALUE_SECRET.to_string(), cx));

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].1.contains(&dbflux_i18n::t!(
            "document.secret_store.error.connection_unavailable"
        )),
        "{errors:?}"
    );
}
