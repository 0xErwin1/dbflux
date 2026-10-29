//! The region picker of a connection whose driver can switch regions: the
//! offered regions, fetched once per document on the background executor,
//! and the switch that reconnects the live connection in place. The state
//! the picker draws lives on the document (`SecretStoreDocument::regions`);
//! this module owns how it is loaded, shown and switched.

use super::SecretStoreDocument;
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
                entity.update(cx, |doc, cx| doc.apply_regions(result, cx));
            });
        })
        .detach();
    }

    fn apply_regions(&mut self, result: Result<Vec<String>, DbError>, cx: &mut Context<Self>) {
        self.regions = match result {
            Ok(mut regions) => {
                regions.sort();
                RegionListState::Loaded(regions)
            }
            Err(error) => RegionListState::Failed(error.to_string()),
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
                self.app_state.update(cx, |state, cx| {
                    state.apply_switch_region(
                        switch.profile_id,
                        switch.original_profile,
                        switch.connection,
                        switch.schema,
                    );
                    cx.emit(AppStateChanged);
                });

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
}
