use dbflux_core::{
    Connection, DbError, DriverCapabilities, RootSectionDef, RootSectionPage, TaskKind,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::{App, Context};
use std::collections::HashSet;
use uuid::Uuid;

use super::{
    DECLARATIONS_RETRY_PREFIX, LISTING_RETRY_PREFIX, PendingRootSectionFetch, ProfileRootSections,
    RootSectionFetchKey, RootSectionListing,
};
use crate::Sidebar;

/// The declarations a connection can show, and the ids of the declarations
/// rejected because they cannot be embedded in a node id or repeat the id of
/// an earlier declaration, whose node they would share.
struct DeclaredSections {
    sections: Vec<RootSectionDef>,
    invalid_ids: Vec<String>,
}

/// Reads the declarations of `connection` and keeps the ones it can show.
///
/// A section whose presentation opens an object-store document is hidden on
/// a connection without `object_store_api`, since every load of that document
/// would fail. A section id with the node id separator is rejected, since
/// every node built from it would be misparsed. A repeated section id is
/// rejected and the first declaration kept, since both would share one node.
#[allow(clippy::result_large_err)]
fn declared_sections(connection: &dyn Connection) -> Result<DeclaredSections, DbError> {
    let source = connection
        .root_section_source()
        .ok_or_else(|| DbError::NotSupported("connection declares no root sections".to_string()))?;
    let has_object_store = connection.object_store_api().is_some();

    let mut declared = DeclaredSections {
        sections: Vec::new(),
        invalid_ids: Vec::new(),
    };
    let mut seen_ids = HashSet::new();
    for section in source.sections()? {
        if !section.has_valid_id() || !seen_ids.insert(section.id.clone()) {
            declared.invalid_ids.push(section.id);
        } else if section.requires_object_store() && !has_object_store {
            log::warn!(
                "Hiding root section {}: its presentation needs an object store the connection does not expose",
                section.id
            );
        } else {
            declared.sections.push(section);
        }
    }

    Ok(declared)
}

impl Sidebar {
    /// Whether `profile_id` is connected through a driver that declares root
    /// sections.
    pub(crate) fn profile_has_root_sections(&self, profile_id: Uuid, cx: &App) -> bool {
        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|connected| {
                connected
                    .connection
                    .metadata()
                    .capabilities
                    .contains(DriverCapabilities::ROOT_SECTIONS)
            })
    }

    /// Whether the declarations of `profile_id` are still to arrive: neither
    /// loaded nor failed.
    pub(crate) fn root_section_declarations_pending(&self, profile_id: Uuid, cx: &App) -> bool {
        self.profile_has_root_sections(profile_id, cx)
            && self.root_section_cache.get(&profile_id).is_none()
            && !self
                .root_section_cache
                .declaration_errors
                .contains_key(&profile_id)
    }

    /// Loads the section declarations of `profile_id`, then the first page of
    /// every section not listed yet. Deduplicated against the cache and the
    /// fetches in flight, so an expand/collapse burst lists each once. What
    /// failed is fetched again only from its retry row.
    pub(crate) fn spawn_fetch_root_sections(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        if !self.profile_has_root_sections(profile_id, cx) {
            return;
        }

        if let Some(cached) = self.root_section_cache.get(&profile_id) {
            let unlisted: Vec<String> = cached
                .sections
                .iter()
                .filter(|section| {
                    !cached.listings.contains_key(&section.id)
                        && !cached.listing_errors.contains_key(&section.id)
                })
                .map(|section| section.id.clone())
                .collect();

            for section_id in unlisted {
                self.spawn_fetch_root_section_page(profile_id, &section_id, None, cx);
            }
            return;
        }

        if self
            .root_section_cache
            .declaration_errors
            .contains_key(&profile_id)
        {
            return;
        }

        let key = RootSectionFetchKey {
            profile_id,
            section_id: None,
        };
        if self.pending_root_section_fetches.contains_key(&key) {
            return;
        }

        let Some(connection) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.connection.clone())
        else {
            return;
        };

        let background_task = cx
            .background_executor()
            .spawn(async move { declared_sections(connection.as_ref()) });

        let task = cx.spawn(async move |this, cx| {
            let result = background_task.await;

            if let Err(error) = this.update(cx, |sidebar, cx| {
                sidebar.apply_root_section_declarations(profile_id, result, cx);
            }) {
                log::warn!("Sidebar dropped before root sections of {profile_id} loaded: {error}");
            }
        });

        self.pending_root_section_fetches.insert(
            key,
            PendingRootSectionFetch {
                _task: task,
                load_task_id: None,
            },
        );
    }

    /// Caches the declarations that loaded, or records why they did not, and
    /// opens the section a connection row activation was waiting for.
    fn apply_root_section_declarations(
        &mut self,
        profile_id: Uuid,
        result: Result<DeclaredSections, DbError>,
        cx: &mut Context<Self>,
    ) {
        self.pending_root_section_fetches
            .remove(&RootSectionFetchKey {
                profile_id,
                section_id: None,
            });

        match result {
            Ok(declared) => {
                let activation_pending = self.pending_root_section_activations.remove(&profile_id);

                for section_id in &declared.invalid_ids {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Driver,
                            dbflux_i18n::t!(
                                "sidebar.toast.root_section_invalid_id",
                                section = section_id
                            ),
                        ),
                        cx,
                    );
                }

                self.root_section_cache
                    .declaration_errors
                    .remove(&profile_id);
                self.root_section_cache.profiles.insert(
                    profile_id,
                    ProfileRootSections {
                        sections: declared.sections,
                        ..ProfileRootSections::default()
                    },
                );
                self.spawn_fetch_root_sections(profile_id, cx);

                if activation_pending
                    && let Some(section_id) = self.profile_activation_section(profile_id)
                {
                    self.activate_root_section(profile_id, &section_id, cx);
                }
            }
            Err(error) => {
                log::warn!("Failed to load the root sections of {profile_id}: {error}");
                self.root_section_cache
                    .declaration_errors
                    .insert(profile_id, error.to_string());
            }
        }

        self.rebuild_tree_with_overrides(cx);
    }

    /// Lists one page of `section_id`: the first page when `page_token` is
    /// `None`, otherwise the page after the items already listed, which it
    /// appends to them.
    pub(crate) fn spawn_fetch_root_section_page(
        &mut self,
        profile_id: Uuid,
        section_id: &str,
        page_token: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let key = RootSectionFetchKey {
            profile_id,
            section_id: Some(section_id.to_string()),
        };
        if self.pending_root_section_fetches.contains_key(&key) {
            return;
        }

        let Some(section) = self
            .root_section_cache
            .get(&profile_id)
            .and_then(|cached| cached.section(section_id))
            .cloned()
        else {
            return;
        };

        let Some((connection, profile_name)) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| (connected.connection.clone(), connected.profile.name.clone()))
        else {
            return;
        };

        let section_label = super::resolve_text(&section.label);
        let app_state = self.app_state.clone();
        let load_task_id = app_state.update(cx, |state, _| {
            let (task_id, _) = state.start_task_for_profile(
                TaskKind::LoadSchema,
                crate::labels::listing_root_section_task_label(&section_label, &profile_name),
                Some(profile_id),
            );
            state.tasks_mut().mark_uncancellable(task_id);
            task_id
        });

        let background_task = cx.background_executor().spawn({
            let section_id = section_id.to_string();
            let page_token = page_token.clone();
            async move {
                let source = connection.root_section_source().ok_or_else(|| {
                    DbError::NotSupported("connection declares no root sections".to_string())
                })?;
                source.list_items(&section_id, page_token.as_deref())
            }
        });

        let task = cx.spawn({
            let key = key.clone();
            async move |this, cx| {
                let result = background_task.await.map_err(|error| error.to_string());

                cx.update(|cx| match &result {
                    Ok(_) => {
                        app_state.update(cx, |state, _| state.complete_task(load_task_id));
                    }
                    Err(message) => {
                        let details =
                            crate::labels::list_root_section_failed_label(&section_label, message);
                        app_state.update(cx, |state, _| {
                            state.fail_task_with_details(load_task_id, message.clone(), details);
                        });
                    }
                });

                if let Err(error) = this.update(cx, |sidebar, cx| {
                    sidebar.pending_root_section_fetches.remove(&key);
                    sidebar.apply_root_section_page(&key, page_token.is_some(), result);
                    sidebar.rebuild_tree_with_overrides(cx);
                }) {
                    log::warn!("Sidebar dropped before a root section page loaded: {error}");
                }
            }
        });

        self.pending_root_section_fetches.insert(
            key,
            PendingRootSectionFetch {
                _task: task,
                load_task_id: Some(load_task_id),
            },
        );
    }

    /// Stores a listed page. A failed first page of a section with nothing
    /// listed yet is recorded for its retry row; any other failure keeps the
    /// items already listed and is left to the failed task.
    fn apply_root_section_page(
        &mut self,
        key: &RootSectionFetchKey,
        appends: bool,
        result: Result<RootSectionPage, String>,
    ) {
        let Some(section_id) = key.section_id.as_deref() else {
            return;
        };
        let Some(cached) = self.root_section_cache.get_mut(&key.profile_id) else {
            return;
        };

        let page = match result {
            Ok(page) => page,
            Err(message) => {
                log::warn!(
                    "Failed to list root section {section_id} of {}: {message}",
                    key.profile_id
                );
                if !appends && !cached.listings.contains_key(section_id) {
                    cached
                        .listing_errors
                        .insert(section_id.to_string(), message);
                }
                return;
            }
        };

        cached.listing_errors.remove(section_id);
        let listing = cached
            .listings
            .entry(section_id.to_string())
            .or_insert_with(RootSectionListing::default);

        if !appends {
            listing.items.clear();
        }
        listing.items.extend(page.items);
        listing.next_page_token = page.next_page_token;
    }

    /// Loads the next page of `section_id` when its listing has one.
    pub(crate) fn load_more_root_section_items(
        &mut self,
        profile_id: Uuid,
        section_id: &str,
        cx: &mut Context<Self>,
    ) {
        let page_token = self
            .root_section_cache
            .get(&profile_id)
            .and_then(|cached| cached.listings.get(section_id))
            .and_then(|listing| listing.next_page_token.clone());

        if let Some(page_token) = page_token {
            self.spawn_fetch_root_section_page(profile_id, section_id, Some(page_token), cx);
        }
    }

    /// Lists `section_id` again from its first page, replacing the cached
    /// items once the page arrives. A fetch of that section still in flight
    /// is abandoned and its task settled.
    pub(crate) fn refresh_root_section(
        &mut self,
        profile_id: Uuid,
        section_id: &str,
        cx: &mut Context<Self>,
    ) {
        let key = RootSectionFetchKey {
            profile_id,
            section_id: Some(section_id.to_string()),
        };
        if let Some(abandoned) = self.pending_root_section_fetches.remove(&key) {
            self.settle_abandoned_root_section_fetch(abandoned, cx);
        }
        if let Some(cached) = self.root_section_cache.get_mut(&profile_id) {
            cached.listing_errors.remove(section_id);
        }

        self.spawn_fetch_root_section_page(profile_id, section_id, None, cx);
    }

    /// Cancels the tasks panel entry of a fetch whose completion was dropped.
    /// The blocking driver call may still run to its end in the background;
    /// its result is discarded.
    fn settle_abandoned_root_section_fetch(
        &mut self,
        abandoned: PendingRootSectionFetch,
        cx: &mut Context<Self>,
    ) {
        if let Some(task_id) = abandoned.load_task_id {
            self.app_state
                .update(cx, |state, _| state.cancel_task(task_id));
        }
    }

    /// Whether `item_id` is a root section retry row.
    pub(crate) fn is_root_section_retry_row(item_id: &str) -> bool {
        item_id.starts_with(DECLARATIONS_RETRY_PREFIX) || item_id.starts_with(LISTING_RETRY_PREFIX)
    }

    /// Activates the retry row `item_id` when it is one: fetches the failed
    /// declarations or first page again. Returns whether it was one.
    pub(crate) fn retry_root_section_row(&mut self, item_id: &str, cx: &mut Context<Self>) -> bool {
        if let Some(profile_id) = item_id.strip_prefix(DECLARATIONS_RETRY_PREFIX) {
            if let Ok(profile_id) = profile_id.parse::<Uuid>() {
                self.root_section_cache
                    .declaration_errors
                    .remove(&profile_id);
                self.spawn_fetch_root_sections(profile_id, cx);
                self.rebuild_tree_with_overrides(cx);
            }
            return true;
        }

        if let Some(rest) = item_id.strip_prefix(LISTING_RETRY_PREFIX) {
            if let Some((profile_id, section_id)) = rest.split_once('|')
                && let Ok(profile_id) = profile_id.parse::<Uuid>()
            {
                self.refresh_root_section(profile_id, section_id, cx);
                self.rebuild_tree_with_overrides(cx);
            }
            return true;
        }

        false
    }

    /// Drops the root sections of `profile_id`, so the next expansion loads
    /// them again, and settles the fetches it abandons.
    pub(crate) fn clear_root_section_cache(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.root_section_cache.remove(&profile_id);
        self.pending_root_section_activations.remove(&profile_id);

        let abandoned_keys: Vec<RootSectionFetchKey> = self
            .pending_root_section_fetches
            .keys()
            .filter(|key| key.profile_id == profile_id)
            .cloned()
            .collect();
        for key in abandoned_keys {
            if let Some(abandoned) = self.pending_root_section_fetches.remove(&key) {
                self.settle_abandoned_root_section_fetch(abandoned, cx);
            }
        }
    }
}
