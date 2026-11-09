//! Follow-ups to the extracted single-secret view: what a disconnect does to
//! a retrieved value, how a stale answer is discarded, which version the
//! Versions table highlights, and how a restore reconciles the guards.

use super::super::view::{DeleteKind, Landing};
use super::*;
use crate::secret_store::SecretWriteFailureKind;
use dbflux_core::SecretSummary;
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
    opened.update_view(|view, cx| view.set_listing_row(Some(active), true, None, cx));

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

// -- A write confirmed in this view outranks stale reads -----------------------

use dbflux_core::SecretDetails;

impl Opened<'_> {
    /// A listing row for the shown secret as a lagging read reports it.
    fn stale_row(&mut self, scheduled: bool) -> SecretSummary {
        let mut row = self
            .read_view(|view| view.selected_summary().cloned())
            .expect("a shown secret");
        row.deletion_scheduled_for = scheduled.then(|| row.last_changed.unwrap_or_default());
        row
    }

    fn land_stale_listing(&mut self, row: SecretSummary) {
        let generation = self.listing_generation();
        self.update_view(|view, cx| view.set_listing_row(Some(row), true, Some(generation), cx));
    }

    /// The generation of the latest listing request the host issued.
    fn listing_generation(&mut self) -> u64 {
        self.read(|doc| doc.listing.generation())
    }

    /// Lands `row` as the answer of a listing request with `generation`.
    fn land_listing_of(&mut self, row: SecretSummary, generation: u64) {
        self.update_view(|view, cx| view.set_listing_row(Some(row), true, Some(generation), cx));
    }

    /// Whether the confirmed state still outranks `row`: a scheduled row is
    /// read as active after a confirmed restore.
    fn row_reads_as_pending(&mut self, row: &SecretSummary) -> bool {
        self.read_view(|view| view.is_deletion_pending(row))
    }

    fn land_stale_describe(&mut self, scheduled: bool) {
        self.window.run_until_parked();
        let mut details: SecretDetails = self
            .read_view(|view| match view.details() {
                DetailsState::Loaded(details) => Some((**details).clone()),
                _ => None,
            })
            .expect("loaded details");
        details.summary.deletion_scheduled_for =
            scheduled.then(|| details.summary.last_changed.unwrap_or_default());
        self.update_view(|view, cx| {
            let generation = view.details_generation;
            let name = details.summary.name.clone();
            view.apply_details(generation, &name, Ok(details), 0, cx)
        });
    }

    fn last_toast_title(&mut self) -> Option<String> {
        self.window.update(|_, cx| {
            let host = cx
                .global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .clone();
            host.read(cx).last_toast_title()
        })
    }

    fn pending(&mut self) -> bool {
        self.read_view(|view| view.deletion_pending())
    }

    /// Lands a confirmed scheduled delete of `name` without letting the
    /// window settle, so the reads that follow it come from a store that has
    /// not caught up: it still says the secret is active.
    fn confirm_delete(&mut self, name: &str) {
        let doc = self.doc.clone();
        let date = dbflux_core::chrono::Utc::now() + dbflux_core::chrono::Duration::days(7);
        self.window.update(|_, cx| {
            let view = doc.read(cx).view.clone();
            view.update(cx, |view, cx| {
                view.land_delete(
                    name,
                    DeleteKind::Scheduled { window_days: 7 },
                    Landing::Done(dbflux_core::SecretDeletion {
                        deletion_scheduled_for: Some(date),
                    }),
                    cx,
                )
            });
        });
    }

    /// The same for a confirmed restore.
    fn confirm_restore(&mut self, name: &str) {
        let doc = self.doc.clone();
        self.window.update(|_, cx| {
            let view = doc.read(cx).view.clone();
            view.update(cx, |view, cx| {
                view.land_restore(name, Landing::Done(()), cx)
            });
        });
    }
}

/// A listing that still says active after a confirmed delete keeps the
/// secret blocked, with Restore on offer and the history withheld.
#[gpui::test]
fn a_stale_active_listing_after_a_confirmed_delete_keeps_the_blocks(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.confirm_delete(KEY_VALUE_SECRET);
    assert!(opened.pending());

    let stale = opened.stale_row(false);
    opened.land_stale_listing(stale);

    assert!(
        opened.pending(),
        "the confirmed delete outranks the listing"
    );
    assert!(!opened.read_view(|view| view.can_retrieve_value()));
    assert!(opened.read_view(|view| view.can_restore_selected()));
    assert!(!opened.read_view(|view| view.can_delete_selected()));
    assert!(
        opened.read_view(|view| matches!(view.versions(), VersionsState::ScheduledForDeletion))
    );
}

/// A describe that lags after a confirmed delete is ignored the same way.
#[gpui::test]
fn a_stale_active_describe_after_a_confirmed_delete_keeps_the_blocks(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.confirm_delete(KEY_VALUE_SECRET);

    opened.land_stale_describe(false);

    assert!(opened.pending());
    assert!(opened.read_view(|view| view.can_restore_selected()));
}

/// A describe that lags after a confirmed restore keeps the secret restored.
#[gpui::test]
fn a_stale_scheduled_describe_after_a_confirmed_restore_keeps_it_restored(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    opened.confirm_restore(PENDING_DELETION_SECRET);
    assert!(!opened.pending());

    opened.land_stale_describe(true);
    let stale = opened.stale_row(true);
    opened.land_stale_listing(stale);

    assert!(
        !opened.pending(),
        "the confirmed restore outranks the reads"
    );
    assert!(opened.read_view(|view| view.can_retrieve_value()));
    assert!(!opened.read_view(|view| view.can_restore_selected()));
}

fn failure(kind: SecretWriteFailureKind) -> crate::secret_store::SecretWriteFailure {
    crate::secret_store::SecretWriteFailure {
        error: DbError::query_failed("audit trail unavailable"),
        correlation_id: None,
        kind,
        audit_unrecorded: false,
    }
}

/// A restore whose only loss is its audit row did restore the secret.
#[gpui::test]
fn a_restore_with_a_lost_audit_row_reconciles(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);

    opened.update_view(|view, cx| {
        view.land_restore(
            PENDING_DELETION_SECRET,
            Landing::Stands(failure(SecretWriteFailureKind::AuditLost)),
            cx,
        )
    });
    opened.land_stale_describe(true);

    assert!(!opened.pending());
}

/// An indeterminate restore says nothing, so the view keeps what it read.
#[gpui::test]
fn an_indeterminate_restore_confirms_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);

    opened.update_view(|view, cx| {
        view.land_restore(
            PENDING_DELETION_SECRET,
            Landing::Stands(failure(SecretWriteFailureKind::Indeterminate)),
            cx,
        )
    });
    opened.land_stale_describe(true);

    assert!(opened.pending());
}

/// A delete whose only loss is its audit row did schedule the deletion: the
/// secret stays blocked with Restore on offer, and no date is invented.
#[gpui::test]
fn a_delete_with_a_lost_audit_row_blocks_without_a_date(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    // The fake store did not delete: what the view believes is the write's.
    opened.update_view(|view, cx| {
        view.land_delete(
            KEY_VALUE_SECRET,
            DeleteKind::Scheduled { window_days: 7 },
            Landing::Stands(failure(SecretWriteFailureKind::AuditLost)),
            cx,
        )
    });
    let stale = opened.stale_row(false);
    opened.land_stale_listing(stale);
    opened.land_stale_describe(false);

    assert!(opened.pending());
    assert!(!opened.read_view(|view| view.can_retrieve_value()));
    assert!(opened.read_view(|view| view.can_restore_selected()));
    let text = opened.rendered_text();
    assert!(text.contains("Deletion scheduled"), "{text}");
    assert!(!text.contains("Deletion scheduled for"), "{text}");
    assert!(!text.contains("Deletes on 19"), "{text}");
    assert!(!text.contains("1970"), "{text}");
    assert!(!text.contains("Deletes on"), "{text}");
}

/// An indeterminate delete confirms nothing.
#[gpui::test]
fn an_indeterminate_delete_confirms_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);

    opened.update_view(|view, cx| {
        view.land_delete(
            KEY_VALUE_SECRET,
            DeleteKind::Scheduled { window_days: 7 },
            Landing::Stands(failure(SecretWriteFailureKind::Indeterminate)),
            cx,
        )
    });
    let stale = opened.stale_row(false);
    opened.land_stale_listing(stale);

    assert!(!opened.pending());
}

/// A store without a version history still reconciles a restore made
/// elsewhere when the listing shows the secret active.
#[gpui::test]
fn a_store_without_versions_reconciles_an_external_restore(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.update_view(|view, _| {
        view.store
            .capabilities
            .remove(SecretStoreCapabilities::VERSIONS)
    });
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    assert!(opened.pending());
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotSupported)));

    let active = opened.stale_row(false);
    opened.land_stale_listing(active);

    assert!(!opened.pending());
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotSupported)));
}

/// An explicit refresh believes the store over a confirmed write.
#[gpui::test]
fn an_explicit_refresh_honors_the_store(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.confirm_delete(KEY_VALUE_SECRET);
    assert!(opened.pending());

    opened.window.run_until_parked();
    assert!(opened.pending(), "the lagging store's reads are ignored");

    opened.update(|doc, cx| doc.refresh(cx));
    opened.load_everything();
    // Refreshing drops the selection; the store is believed on the next look.
    opened.open_secret(KEY_VALUE_SECRET);

    assert!(!opened.pending());
}

// -- The confirmed deletion, second round --------------------------------------

/// A scheduled delete that lands without a date still scheduled the deletion.
#[gpui::test]
fn a_scheduled_delete_without_a_date_is_confirmed(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);

    opened.update_view(|view, cx| {
        view.land_delete(
            KEY_VALUE_SECRET,
            DeleteKind::Scheduled { window_days: 7 },
            Landing::Done(dbflux_core::SecretDeletion {
                deletion_scheduled_for: None,
            }),
            cx,
        )
    });
    let stale = opened.stale_row(false);
    opened.land_stale_listing(stale);
    opened.land_stale_describe(false);

    assert!(opened.pending());
    assert!(!opened.read_view(|view| view.can_retrieve_value()));
    assert!(opened.read_view(|view| view.can_restore_selected()));
    let text = opened.rendered_text();
    assert!(!text.contains("1970"), "{text}");
}

/// An irreversible delete confirms nothing: the secret is gone, not scheduled.
#[gpui::test]
fn an_irreversible_delete_confirms_no_scheduled_state(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    let active = opened.stale_row(false);

    opened.update_view(|view, cx| {
        view.land_delete(
            KEY_VALUE_SECRET,
            DeleteKind::Irreversible,
            Landing::Done(dbflux_core::SecretDeletion::default()),
            cx,
        )
    });

    assert!(!opened.read_view(|view| view.is_deletion_pending(&active)));
}

/// After a delete, a describe that agrees does not end the override while the
/// listing still disagrees: a stale listing cannot re-enable the actions.
#[gpui::test]
fn an_agreeing_describe_alone_does_not_end_a_confirmed_delete(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.confirm_delete(KEY_VALUE_SECRET);

    opened.land_stale_describe(true);
    let stale = opened.stale_row(false);
    opened.land_stale_listing(stale);

    assert!(opened.pending(), "the listing has not agreed yet");
    assert!(!opened.read_view(|view| view.can_retrieve_value()));
    assert!(opened.read_view(|view| view.can_restore_selected()));
    assert!(
        opened.read_view(|view| matches!(view.versions(), VersionsState::ScheduledForDeletion))
    );

    let agreeing = opened.stale_row(true);
    opened.land_stale_listing(agreeing);
    opened.land_stale_describe(false);

    assert!(
        !opened.pending(),
        "both reads agreed, so the store is believed again"
    );
}

/// The same after a restore, in the other order: an agreeing listing alone
/// does not let a stale describe suppress the restored secret.
#[gpui::test]
fn an_agreeing_listing_alone_does_not_end_a_confirmed_restore(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    opened.confirm_restore(PENDING_DELETION_SECRET);
    // The reads the restore asked for land first; the store they read has not
    // been restored, so they disagree and the lagging ones below come after.
    opened.window.run_until_parked();

    let agreeing = opened.stale_row(false);
    opened.land_stale_listing(agreeing);
    opened.land_stale_describe(true);

    assert!(!opened.pending(), "the describe has not agreed yet");
    assert!(opened.read_view(|view| view.can_retrieve_value()));

    opened.land_stale_describe(false);
    opened.land_stale_describe(true);

    assert!(
        opened.pending(),
        "both reads agreed, so the store is believed"
    );
}

/// A confirmed restore conforms the listing row, so no stale scheduled date
/// is left on it for the row to warn about.
#[gpui::test]
fn a_confirmed_restore_conforms_the_listing_row(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    opened.confirm_restore(PENDING_DELETION_SECRET);

    let stale = opened.stale_row(true);
    opened.land_stale_listing(stale);

    assert_eq!(
        opened.read_view(|view| view
            .listed_summary()
            .and_then(|row| row.deletion_scheduled_for)),
        None
    );
    assert_eq!(
        opened.read_view(|view| view
            .selected_summary()
            .and_then(|row| row.deletion_scheduled_for)),
        None
    );
    let text = opened.rendered_text();
    assert!(!text.contains("Deletes"), "{text}");
}

/// A confirmation belongs to the connection session that made it.
#[gpui::test]
fn a_session_change_drops_the_confirmed_deletion(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.confirm_delete(KEY_VALUE_SECRET);
    assert!(opened.pending());

    let _other_account = opened.replace_connection();
    opened.announce_state_change();

    assert!(
        !opened.pending(),
        "the new session's same-named active secret is not scheduled"
    );
    assert!(opened.read_view(|view| view.can_retrieve_value()));
}

/// Selecting a secret conforms its row like a pushed listing row does.
#[gpui::test]
fn selecting_a_secret_conforms_its_row_to_a_confirmed_delete(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.confirm_delete(KEY_VALUE_SECRET);
    let stale = opened.stale_row(false);

    opened.update_view(|view, cx| view.show_secret(KEY_VALUE_SECRET.to_string(), Some(stale), cx));

    assert!(
        opened
            .read_view(|view| view
                .listed_summary()
                .and_then(|row| row.deletion_scheduled_for))
            .is_some(),
        "the row carries the confirmed deletion"
    );
    assert!(opened.pending());
    assert!(!opened.read_view(|view| view.can_retrieve_value()));
}

/// A retrieval that finds the connection gone before it starts reports that
/// and records no audit event against an empty driver id.
#[gpui::test]
fn a_retrieval_without_a_live_connection_records_no_audit_row(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    let before = opened.audit_events().len();
    // The view has not heard of the disconnect yet.
    opened.disconnect();
    assert!(opened.read_view(|view| view.is_connected()));

    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.window.run_until_parked();

    assert_eq!(opened.audit_events().len(), before, "nothing was retrieved");
    assert_eq!(opened.reported_errors().len(), 1);
    assert!(!opened.read_view(|view| matches!(view.value(), ValueState::Loading)));
}

/// An explicit retrieval command with no connection says why.
#[gpui::test]
fn a_retrieval_command_without_a_connection_is_reported_once(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    let _connected = opened.take_connection();
    opened.announce_state_change();
    assert!(!opened.read_view(|view| view.can_retrieve_value()));
    assert!(opened.reported_errors().is_empty());

    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains(&dbflux_i18n::t!(
        "document.secret_store.error.connection_unavailable"
    )));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
}

/// The disconnect emits no `user_error` event, so no audit row follows; the
/// request that does report emits exactly one.
#[gpui::test]
fn a_disconnect_emits_no_user_error_event(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.open_secret(KEY_VALUE_SECRET);
    let _connected = opened.take_connection();

    let ((), events) = dbflux_ui_base::user_error::capture::capture_user_error_events(|| {
        opened.announce_state_change();
    });
    assert!(events.is_empty(), "{events:?}");

    let ((), events) = dbflux_ui_base::user_error::capture::capture_user_error_events(|| {
        opened.update_view(|view, cx| view.load_versions(KEY_VALUE_SECRET.to_string(), cx));
    });
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(
        events[0].get("action").map(String::as_str),
        Some("user_error")
    );
}

/// A view bound to a region sees no connection while the profile is on
/// another one: nothing is read and nothing is written, and the view says
/// the connection is there but elsewhere. Binding it to the region the
/// profile is on brings the connection back.
#[gpui::test]
fn a_view_bound_to_another_region_sees_no_connection(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    opened.open_secret(KEY_VALUE_SECRET);
    // A dirty editor, so Save has something to refuse.
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(r#"{"changed": 1}"#);

    opened.update_view(|view, cx| view.set_region_pin(Some(OTHER_REGION.to_string()), cx));

    assert!(opened.read_view(|view| !view.can_retrieve_value()));
    let (mismatch, connected) =
        opened.update_view(|view, cx| (view.region_mismatch(cx), view.connection(cx).is_some()));
    assert!(mismatch && !connected);

    let reads_before = opened.driver.secret_store_calls().value_requests.len();
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert_eq!(
        opened.driver.secret_store_calls().value_requests.len(),
        reads_before,
        "no read while the view is bound elsewhere"
    );
    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());

    // Save is refused before anything is sent, and silently: the view says
    // the connection is elsewhere, Save adds no error of its own (the
    // retrieval above is what reported one).
    let reported_before = opened.reported_errors().len();
    opened.update_view(|view, cx| view.save_value_edit(cx));
    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
    assert_eq!(opened.reported_errors().len(), reported_before);

    opened.update_view(|view, cx| view.set_region_pin(Some(CURRENT_REGION.to_string()), cx));
    assert!(opened.read_view(|view| view.can_retrieve_value()));
    assert!(!opened.update_view(|view, cx| view.region_mismatch(cx)));
}

// -- The confirmed deletion, third round ---------------------------------------

/// A source's latest observation is what counts: an agreeing listing that a
/// later listing contradicts no longer counts as agreement.
#[gpui::test]
fn a_disagreeing_listing_clears_that_sources_agreement(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    opened.confirm_restore(PENDING_DELETION_SECRET);
    let scheduled = opened.stale_row(true);

    let agreeing = opened.stale_row(false);
    opened.land_stale_listing(agreeing);
    opened.land_stale_listing(scheduled.clone());
    opened.land_stale_describe(false);

    assert!(
        !opened.row_reads_as_pending(&scheduled),
        "the listing's latest word disagrees, so the restore still outranks the reads"
    );
}

/// The same for a describe that a later describe contradicts.
#[gpui::test]
fn a_disagreeing_describe_clears_that_sources_agreement(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    opened.confirm_restore(PENDING_DELETION_SECRET);
    let scheduled = opened.stale_row(true);

    opened.land_stale_describe(false);
    opened.land_stale_describe(true);
    let agreeing = opened.stale_row(false);
    opened.land_stale_listing(agreeing);

    assert!(!opened.row_reads_as_pending(&scheduled));
}

/// A listing issued before the write cannot have seen it, whatever it says.
#[gpui::test]
fn a_listing_issued_before_the_write_does_not_count(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    let issued_before = opened.listing_generation();
    opened.confirm_restore(PENDING_DELETION_SECRET);
    let scheduled = opened.stale_row(true);

    let agreeing = opened.stale_row(false);
    opened.land_listing_of(agreeing, issued_before);
    opened.land_stale_describe(false);

    assert!(
        !opened.row_reads_as_pending(&scheduled),
        "only the describe counted"
    );
}

/// A row the selection brings along, or a row a failed reload retained, is a
/// copy of an older listing and no observation.
#[gpui::test]
fn a_retained_row_is_not_an_observation(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.confirm_delete(KEY_VALUE_SECRET);
    let agreeing = opened.stale_row(true);
    let active = opened.stale_row(false);

    opened.update_view(|view, cx| {
        view.show_secret(KEY_VALUE_SECRET.to_string(), Some(agreeing.clone()), cx)
    });
    opened.land_stale_describe(true);

    assert!(
        opened.row_reads_as_pending(&active),
        "the selection's copy of the row did not count as the listing agreeing"
    );
}

/// The summaries a confirmation conformed belong to its session: a session
/// replacement leaves none of them behind, even when nothing reads again.
#[gpui::test]
fn a_session_change_drops_the_conformed_summaries(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);
    opened.confirm_delete(KEY_VALUE_SECRET);
    let stale = opened.stale_row(false);
    opened.land_stale_listing(stale);
    assert!(
        opened
            .read_view(|view| view
                .selected_summary()
                .and_then(|row| row.deletion_scheduled_for))
            .is_some()
    );

    let _other_account = opened.replace_connection();
    opened.update_view(|view, cx| view.sync_connection_presence(cx));

    assert_eq!(
        opened.read_view(|view| view
            .selected_summary()
            .and_then(|row| row.deletion_scheduled_for)),
        None,
        "the previous session's date is gone"
    );
}

/// A scheduled delete the store answered without a date still says it was
/// scheduled, and never that the secret is gone.
#[gpui::test]
fn a_scheduled_delete_without_a_date_toasts_a_scheduled_message(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(KEY_VALUE_SECRET);

    opened.update_view(|view, cx| {
        view.land_delete(
            KEY_VALUE_SECRET,
            DeleteKind::Scheduled { window_days: 7 },
            Landing::Done(dbflux_core::SecretDeletion {
                deletion_scheduled_for: None,
            }),
            cx,
        )
    });

    let title = opened.last_toast_title().expect("a toast");
    assert_eq!(
        title,
        dbflux_i18n::t!(
            "document.secret_store.delete.done_scheduled_undated",
            name = KEY_VALUE_SECRET
        )
    );
    assert_ne!(
        title,
        dbflux_i18n::t!("document.secret_store.delete.done", name = KEY_VALUE_SECRET)
    );
}

// -- The confirmed deletion, fourth round ---------------------------------------

/// Every row a listing page returns counts for the override, not only the
/// selected secret's: a restore reload that returns A while B is selected
/// still leaves A's listing agreeing.
#[gpui::test]
fn a_listing_page_is_observed_for_every_row_it_returns(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    let scheduled = opened.stale_row(true);
    let active = opened.stale_row(false);
    opened.confirm_restore(PENDING_DELETION_SECRET);
    opened.select(KEY_VALUE_SECRET);

    // The listing holds the restored (active) row, as a reload after the
    // restore would have left it.
    let generation = opened.listing_generation();
    let doc = opened.doc.clone();
    opened.window.update(|_, cx| {
        doc.update(cx, |doc, cx| {
            for entry in doc.listing.entries.iter_mut() {
                if entry.name == PENDING_DELETION_SECRET {
                    entry.deletion_scheduled_for = None;
                }
            }
            let page = dbflux_core::SecretListPage {
                entries: vec![active.clone()],
                child_paths: Vec::new(),
                next_page_token: None,
            };
            doc.apply_list(generation, Ok(page), 0, cx)
        })
    });

    opened.select(PENDING_DELETION_SECRET);
    opened.land_stale_describe(false);

    assert!(
        opened.row_reads_as_pending(&scheduled),
        "listing and describe both agreed with the restore, so the override ended"
    );
}

/// A describe read from a session the profile has since replaced is not
/// applied: it is read again from the live one.
#[gpui::test]
fn a_describe_from_a_replaced_session_is_not_applied(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    let mut details = opened
        .read_view(|view| match view.details() {
            DetailsState::Loaded(details) => Some((**details).clone()),
            _ => None,
        })
        .expect("loaded details");
    details.summary.deletion_scheduled_for = Some(dbflux_core::chrono::Utc::now());
    let doc = opened.doc.clone();

    let generation = opened.window.update(|_, cx| {
        let view = doc.read(cx).view.clone();
        view.update(cx, |view, cx| {
            view.describe(PENDING_DELETION_SECRET.to_string(), cx);
            view.details_generation
        })
    });
    let _other_account = opened.replace_connection();
    opened.window.update(|_, cx| {
        let view = doc.read(cx).view.clone();
        view.update(cx, |view, cx| {
            view.confirm_restored(PENDING_DELETION_SECRET, cx);
            view.apply_details(generation, PENDING_DELETION_SECRET, Ok(details), 0, cx)
        })
    });

    assert!(
        opened.read_view(|view| matches!(view.details(), DetailsState::Loading)),
        "the previous session's describe must not become the details"
    );

    opened.window.run_until_parked();
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Loaded(_))));
    assert!(opened.update_view(|view, cx| view.details_session_is_live(cx)));
}

/// A session change while a write is in flight cannot leave the previous
/// session's details on screen; the write's landing reads them again.
#[gpui::test]
fn a_session_change_during_a_write_invalidates_the_loaded_details(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Loaded(_))));
    let doc = opened.doc.clone();

    opened.window.update(|_, cx| {
        let view = doc.read(cx).view.clone();
        view.update(cx, |view, cx| view.restore_selected(cx));
    });
    assert!(opened.read_view(|view| view.write_in_flight()));

    let _other_account = opened.replace_connection();
    opened.window.update(|_, cx| {
        let view = doc.read(cx).view.clone();
        view.update(cx, |view, cx| {
            view.begin_connection_change(cx);
            view.finish_connection_change(cx);
        })
    });

    assert!(
        opened.read_view(|view| matches!(view.details(), DetailsState::Loading)),
        "details of the previous session stayed loaded"
    );

    opened.window.run_until_parked();
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Loaded(_))));
    assert!(opened.update_view(|view, cx| view.details_session_is_live(cx)));
}

/// A page that repeats a secret keeps the listing's earlier row, so the
/// repeated row, which the listing discarded, is no observation.
#[gpui::test]
fn a_discarded_duplicate_row_is_not_a_listing_observation(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.open_secret(PENDING_DELETION_SECRET);
    let scheduled = opened.stale_row(true);
    let active = opened.stale_row(false);
    opened.confirm_restore(PENDING_DELETION_SECRET);
    opened.select(KEY_VALUE_SECRET);

    let generation = opened.listing_generation();
    let doc = opened.doc.clone();
    opened.window.update(|_, cx| {
        doc.update(cx, |doc, cx| {
            let page = dbflux_core::SecretListPage {
                entries: vec![active.clone()],
                child_paths: Vec::new(),
                next_page_token: None,
            };
            doc.apply_list(generation, Ok(page), 0, cx)
        })
    });

    opened.select(PENDING_DELETION_SECRET);
    opened.land_stale_describe(false);

    assert!(
        !opened.row_reads_as_pending(&scheduled),
        "the retained row still says scheduled, so the listing does not agree with the restore"
    );
}
