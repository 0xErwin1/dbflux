use dbflux_core::{DbError, DriverCapabilities, RootSectionDef, RootSectionPage, TaskKind};
use gpui::{App, Context};
use uuid::Uuid;

use super::{ProfileRootSections, RootSectionFetchKey, RootSectionListing};
use crate::Sidebar;

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

    /// Loads the section declarations of `profile_id`, then the first page of
    /// every section not listed yet. Deduplicated against the cache and the
    /// fetches in flight, so an expand/collapse burst lists each once.
    pub(crate) fn spawn_fetch_root_sections(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        if !self.profile_has_root_sections(profile_id, cx) {
            return;
        }

        if let Some(cached) = self.root_section_cache.get(&profile_id) {
            let unlisted: Vec<String> = cached
                .sections
                .iter()
                .filter(|section| !cached.listings.contains_key(&section.id))
                .map(|section| section.id.clone())
                .collect();

            for section_id in unlisted {
                self.spawn_fetch_root_section_page(profile_id, &section_id, None, cx);
            }
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

        let background_task = cx.background_executor().spawn(async move {
            connection
                .root_section_source()
                .map(|source| source.sections())
                .unwrap_or_default()
        });

        let task = cx.spawn(async move |this, cx| {
            let sections: Vec<RootSectionDef> = background_task.await;

            if let Err(error) = this.update(cx, |sidebar, cx| {
                sidebar
                    .pending_root_section_fetches
                    .remove(&RootSectionFetchKey {
                        profile_id,
                        section_id: None,
                    });
                sidebar.root_section_cache.insert(
                    profile_id,
                    ProfileRootSections {
                        sections,
                        listings: Default::default(),
                    },
                );
                sidebar.spawn_fetch_root_sections(profile_id, cx);
                sidebar.rebuild_tree_with_overrides(cx);
            }) {
                log::warn!("Sidebar dropped before root sections of {profile_id} loaded: {error}");
            }
        });

        self.pending_root_section_fetches.insert(key, task);
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

        self.pending_root_section_fetches.insert(key, task);
    }

    fn apply_root_section_page(
        &mut self,
        key: &RootSectionFetchKey,
        appends: bool,
        result: Result<RootSectionPage, String>,
    ) {
        let Some(section_id) = key.section_id.as_deref() else {
            return;
        };

        let page = match result {
            Ok(page) => page,
            Err(message) => {
                log::warn!(
                    "Failed to list root section {section_id} of {}: {message}",
                    key.profile_id
                );
                return;
            }
        };

        let Some(cached) = self.root_section_cache.get_mut(&key.profile_id) else {
            return;
        };

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
    /// items once the page arrives.
    pub(crate) fn refresh_root_section(
        &mut self,
        profile_id: Uuid,
        section_id: &str,
        cx: &mut Context<Self>,
    ) {
        self.pending_root_section_fetches
            .remove(&RootSectionFetchKey {
                profile_id,
                section_id: Some(section_id.to_string()),
            });
        self.spawn_fetch_root_section_page(profile_id, section_id, None, cx);
    }

    /// Drops the root sections of `profile_id`, so the next expansion loads
    /// them again.
    pub(crate) fn clear_root_section_cache(&mut self, profile_id: Uuid) {
        self.root_section_cache.remove(&profile_id);
        self.pending_root_section_fetches
            .retain(|key, _| key.profile_id != profile_id);
    }
}
