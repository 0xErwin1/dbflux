//! The region picker of a connection whose driver can switch regions: the
//! offered regions, fetched once per document on the background executor,
//! and the switch that reconnects the live connection in place. The state
//! the picker draws lives on the document (`SecretStoreDocument::regions`);
//! this module owns how it is loaded, shown and switched.

use super::SecretStoreDocument;
use crate::object_text::db_error_to_user_facing;
use dbflux_components::controls::DropdownItem;
use dbflux_core::{DbError, SwitchRegionResult};
use dbflux_ui_base::AppStateChanged;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::Context;

/// The regions the connection offers, fetched once per document on the
/// background executor. `Failed` keeps the store-side error, so a failed
/// listing renders as an error with a retry and never as an empty picker.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RegionListState {
    #[default]
    Idle,
    Loading,
    Loaded(Vec<String>),
    Failed(String),
}

impl SecretStoreDocument {
    // -- Region -----------------------------------------------------------

    /// Fetches the connection's offered regions, on the background executor:
    /// the seam is synchronous and may block on the network, so it never runs
    /// on the foreground. A retry calls the connection again.
    pub(crate) fn load_regions(&mut self, cx: &mut Context<Self>) {
        if !self.region_switchable || matches!(self.regions, RegionListState::Loading) {
            return;
        }

        self.regions_generation = self.regions_generation.wrapping_add(1);
        let generation = self.regions_generation;

        let Some(connection) = self
            .app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|connected| connected.connection.clone())
        else {
            self.regions = RegionListState::Failed(dbflux_i18n::t!(
                "document.secret_store.error.connection_unavailable"
            ));
            cx.notify();
            return;
        };

        self.regions = RegionListState::Loading;
        cx.notify();

        let entity = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { connection.available_regions() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;
            cx.update(|cx| {
                entity.update(cx, |doc, cx| doc.apply_regions(generation, result, cx));
            });
        })
        .detach();
    }

    /// The generation of the region discovery in flight, for tests that need
    /// to land a result the way a replaced connection would.
    #[cfg(test)]
    pub(crate) fn region_discovery_generation(&self) -> u64 {
        self.regions_generation
    }

    /// Lands one discovery. A result from a discovery a newer one replaced —
    /// or from a connection that was replaced under it — belongs to a session
    /// that no longer exists, so it is dropped instead of shown.
    pub(crate) fn apply_regions(
        &mut self,
        generation: u64,
        result: Result<Vec<String>, DbError>,
        cx: &mut Context<Self>,
    ) {
        if generation != self.regions_generation {
            return;
        }

        self.regions = match result {
            Ok(mut regions) => {
                regions.sort();
                RegionListState::Loaded(regions)
            }
            Err(error) => {
                // First catch site of the discovery and of the user-triggered
                // retry: report through the error seam and keep the inline
                // retry, which renders below with the store's message.
                report_error(db_error_to_user_facing(&error), cx);
                RegionListState::Failed(error.to_string())
            }
        };
        self.sync_region_dropdown(cx);
        cx.notify();
    }

    /// Moves the picker's items and selection to the document's region
    /// state; also the way a failed switch puts the old region back.
    fn sync_region_dropdown(&mut self, cx: &mut Context<Self>) {
        let Some(dropdown) = self.region_dropdown.clone() else {
            return;
        };

        let items: Vec<DropdownItem> = match &self.regions {
            RegionListState::Loaded(regions) => regions
                .iter()
                .map(|region| DropdownItem::with_value(region.clone(), region.clone()))
                .collect(),
            _ => Vec::new(),
        };

        let selected = self
            .region
            .as_ref()
            .and_then(|region| items.iter().position(|item| item.value.as_ref() == region));

        dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(items, cx);
            dropdown.set_selected_index(selected, cx);
        });
    }

    /// Switches the live connection to `region`: prepares on the foreground,
    /// reconnects on the background executor and installs the result in
    /// place, so the sidebar and the other documents of the profile reload.
    /// A second switch while one is in flight, or a pick of the region
    /// already in use, is ignored; a failure leaves the connection, its
    /// stores and the selected region untouched.
    pub fn switch_region(&mut self, region: String, cx: &mut Context<Self>) {
        if !self.region_switchable
            || self.region_switch_in_flight
            || self.region.as_deref() == Some(region.as_str())
        {
            return;
        }

        let params = match self
            .app_state
            .read(cx)
            .prepare_switch_region(self.profile_id, &region)
        {
            Ok(params) => params,
            Err(error) => {
                self.report_region_failure(&region, error, cx);
                return;
            }
        };

        self.region_switch_in_flight = true;
        cx.notify();

        let entity = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;
            cx.update(|cx| {
                entity.update(cx, |doc, cx| doc.apply_region_switch(region, result, cx));
            });
        })
        .detach();
    }

    fn apply_region_switch(
        &mut self,
        region: String,
        result: Result<SwitchRegionResult, String>,
        cx: &mut Context<Self>,
    ) {
        self.region_switch_in_flight = false;

        match result {
            Ok(switch) => {
                let installed = self.app_state.update(cx, |state, cx| {
                    let installed = state.apply_switch_region(
                        switch.profile_id,
                        switch.original_profile,
                        switch.connection,
                        switch.schema,
                        switch.session_generation,
                    );
                    if installed {
                        cx.emit(AppStateChanged);
                    }
                    installed
                });

                if !installed {
                    // The profile's session moved on while the switch was
                    // reconnecting: the connection was replaced or the profile
                    // disconnected elsewhere, so installing would overwrite
                    // whatever the newer session reached. Leave the region,
                    // the listing, the selection and the value as they are;
                    // the in-flight flag is already cleared, and the picker
                    // goes back to the region the document still shows
                    // instead of keeping the pick that did not happen.
                    self.sync_region_dropdown(cx);
                    cx.notify();
                    return;
                }

                self.region = Some(region);
                self.sync_region_dropdown(cx);
                self.load_first_page(cx);
            }
            Err(error) => self.report_region_failure(&region, error, cx),
        }
    }

    /// Reports a failed region switch with the region in the message.
    fn report_region_failure(&mut self, region: &str, error: String, cx: &mut Context<Self>) {
        report_error(
            UserFacingError::new(
                ErrorKind::Network,
                dbflux_i18n::t!(
                    "document.secret_store.region.switch_failed",
                    region = region,
                    error = error.as_str()
                ),
            ),
            cx,
        );
        self.sync_region_dropdown(cx);
        cx.notify();
    }

    /// A region change this document did not ask for (the profile was
    /// reconnected elsewhere, or another document of the same profile
    /// switched) leaves the connection on a region the listing was not loaded
    /// under. Adopting the live region reloads the first page and drops the
    /// selection, its details and any retrieved value, so the tab never shows
    /// one region's secrets under another region's connection. A document
    /// that switched for itself already shows the new region and does not
    /// reload again.
    pub(crate) fn adopt_connection_region(&mut self, cx: &mut Context<Self>) {
        let Some(live_region) = self
            .app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .and_then(|connected| connected.connection.current_region())
        else {
            return;
        };

        if self.region.as_deref() == Some(live_region.as_str()) {
            return;
        }

        self.region = Some(live_region);

        // A discovery still running was asked of the previous connection, so
        // its answer is dropped rather than shown. The offered regions belong
        // to the credentials, not to the region, so a list already loaded
        // stays: asking again on every switch would be a call for nothing.
        self.regions_generation = self.regions_generation.wrapping_add(1);

        if !matches!(self.regions, RegionListState::Loaded(_)) {
            self.regions = RegionListState::Idle;
            self.load_regions(cx);
        }

        self.sync_region_dropdown(cx);
        self.load_first_page(cx);
        cx.notify();
    }
}
