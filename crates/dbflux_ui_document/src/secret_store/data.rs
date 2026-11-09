//! Listing calls of `SecretStoreDocument`. Each one runs on the background
//! executor, because the seam is synchronous, and lands on the foreground
//! through a generation check, so a result that a newer call replaced is
//! dropped. The first catch site of a user-triggered failure reports it.

use super::listing::{LARGEST_PAGE_ENTRIES, PathMode, keeps_loading};
use super::{CallTiming, SecretRowKey, SecretStoreDocument, StoreCall};
use crate::object_text::db_error_to_user_facing;
use dbflux_core::{Connection, DbError, SecretListPage, SecretListRequest};
use dbflux_ui_base::user_error::report_error_async;
use gpui::Context;
use std::sync::Arc;
use std::time::Instant;

impl SecretStoreDocument {
    pub(crate) fn connection(&self, cx: &Context<Self>) -> Option<Arc<dyn Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|connected| connected.connection.clone())
    }

    /// The request for the current level: its path and mode on a
    /// hierarchical store, and the name filter.
    pub(crate) fn list_request(&self, page_token: Option<String>) -> SecretListRequest {
        let hierarchical = self.hierarchical();
        let name_filter = Some(self.name_filter.clone()).filter(|filter| !filter.is_empty());

        SecretListRequest {
            path: if hierarchical {
                self.path.clone()
            } else {
                None
            },
            recursive: hierarchical && self.path_mode == PathMode::Flat,
            name_filter,
            page_token,
            page_size: Some(LARGEST_PAGE_ENTRIES),
        }
    }

    /// Lists the current level from its first page. The selection, its
    /// details and any retrieved value are dropped with the old listing, and
    /// a menu open over it would act on rows that no longer exist — none of
    /// which may happen under an editor holding unsaved text.
    pub fn load_first_page(&mut self, cx: &mut Context<Self>) {
        if self.block_unsaved_value_edits(cx) {
            return;
        }

        self.selected = None;
        self.view.update(cx, |view, cx| view.clear_selection(cx));
        self.drop_context_menu();

        let generation = self.listing.begin(true);
        self.spawn_list(generation, None, cx);
    }

    /// Lists the current level again after a write, leaving the selection,
    /// its details and its value where they are: the rows are replaced when
    /// the first page lands, so a changed rotation, description or deletion
    /// date reads from the store instead of from the listing the write
    /// outdated.
    pub(crate) fn reload_listing_in_place(&mut self, cx: &mut Context<Self>) {
        let generation = self.listing.begin_reload();
        self.spawn_list(generation, None, cx);
    }

    /// Loads the next page and appends it. A no-op on the last page or while
    /// a page is loading.
    pub fn load_more(&mut self, cx: &mut Context<Self>) {
        if self.listing.loading {
            return;
        }

        let Some(token) = self.listing.next_page_token.clone() else {
            return;
        };

        let generation = self.listing.begin(false);
        self.spawn_list(generation, Some(token), cx);
    }

    /// Refresh: lists the level again and drops the retrieved value.
    ///
    /// A region discovery that failed is asked again, so granting
    /// `ec2:DescribeRegions` brings the picker back without reopening the
    /// document.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.block_unsaved_value_edits(cx) {
            return;
        }

        // An explicit refresh believes the store over what a write confirmed.
        self.view
            .update(cx, |view, _| view.clear_confirmed_deletions());

        if matches!(self.regions, super::RegionListState::Failed(_)) {
            self.load_regions(cx);
        }

        self.load_first_page(cx);
    }

    fn spawn_list(&mut self, generation: u64, page_token: Option<String>, cx: &mut Context<Self>) {
        cx.notify();
        self.view
            .update(cx, |view, _| view.note_listing_issued(generation));

        let Some(connection) = self.connection(cx) else {
            self.listing.apply_error(
                generation,
                dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
            );
            cx.notify();
            return;
        };

        let request = self.list_request(page_token);
        let store_id = self.store.id.clone();
        let entity = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            let started = Instant::now();
            let result = match connection.secret_store(&store_id) {
                Some(store) => store.list(&request),
                None => Err(store_unavailable(&store_id)),
            };

            (result, started.elapsed().as_millis())
        });

        cx.spawn(async move |_this, cx| {
            let (result, millis) = task.await;

            if let Err(error) = &result {
                report_error_async(db_error_to_user_facing(error), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| doc.apply_list(generation, result, millis, cx));
            });
        })
        .detach();
    }

    pub(crate) fn apply_list(
        &mut self,
        generation: u64,
        result: Result<SecretListPage, DbError>,
        millis: u128,
        cx: &mut Context<Self>,
    ) {
        // A reload that replaces the rows starts from nothing, so a page that
        // is shorter than the listing it replaces still made progress.
        let replacing = self.listing.replaces_on_next_page();
        let loaded_before = if replacing {
            0
        } else {
            self.listing.entries.len()
        };
        let paths_before = if replacing {
            0
        } else {
            self.listing.child_paths.len()
        };

        let applied = match result {
            Ok(page) => {
                let count = page.entries.len();
                let page_names: Vec<String> = page
                    .entries
                    .iter()
                    .map(|entry| entry.name.clone())
                    .collect();
                let applied = self.listing.apply_page(generation, page);

                if applied {
                    // The listing keeps the earlier row of a repeated secret, so
                    // what it retained is its word, not the discarded duplicate.
                    let retained: Vec<_> = page_names
                        .iter()
                        .filter_map(|name| self.listing.find(name).cloned())
                        .collect();

                    self.view.update(cx, |view, cx| {
                        view.observe_listed_rows(&retained, generation, cx)
                    });

                    self.last_call = Some(CallTiming {
                        call: StoreCall::List {
                            count,
                            hierarchical: self.hierarchical(),
                        },
                        millis,
                    });
                }

                applied
            }
            Err(error) => self.listing.apply_error(generation, error.to_string()),
        };

        if !applied {
            return;
        }

        self.push_listing_row(Some(generation), cx);
        cx.notify();

        // A failed page ends the chain: the retained rows are not progress, and
        // asking for the old next page would clear the error and mix pages of
        // two different loads.
        if self.listing.error.is_some() {
            return;
        }

        // A page that landed under the limit asks for the next one instead of
        // making the user click for it; the manual row remains for the rest.
        //
        // A page that added nothing stops the chain regardless: a store that
        // keeps handing back a token while returning no entries would
        // otherwise be asked forever.
        let made_progress = self.listing.entries.len() > loaded_before
            || self.listing.child_paths.len() > paths_before;

        if made_progress && keeps_loading(self.listing.entries.len(), self.listing.has_more()) {
            self.load_more(cx);
        }
    }

    /// Gives the view the listing's row of the selected secret. A failed page
    /// keeps the rows but does not refresh what the view remembers of them.
    /// `observed_by` is the generation of the load that just landed, `None`
    /// when no load did.
    pub(super) fn push_listing_row(&mut self, observed_by: Option<u64>, cx: &mut Context<Self>) {
        let row = match &self.selected {
            Some(SecretRowKey::Secret(name)) => self.listing.find(name).cloned(),
            _ => None,
        };
        let refresh_cache = self.listing.error.is_none();

        self.view.update(cx, |view, cx| {
            view.set_listing_row(row, refresh_cache, observed_by, cx)
        });
    }
}

/// Why the history is being listed: something the user did, or a reload the
/// view runs on its own after the connection changed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryLoad {
    Requested,
    Automatic,
}

pub(super) fn store_unavailable(store_id: &str) -> DbError {
    DbError::NotSupported(dbflux_i18n::t!(
        "document.secret_store.error.store_unavailable",
        store = store_id
    ))
}
