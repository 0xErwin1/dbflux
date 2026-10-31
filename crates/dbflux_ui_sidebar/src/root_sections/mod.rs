//! Driver-declared root sections (`RootSectionSource`): one folder per
//! section at the root of a connection, with the listed items as its rows.
//!
//! Every section renders through the builders here, gated on
//! `DriverCapabilities::ROOT_SECTIONS`. Labels, presentations and actions all
//! come from the declaration, so the sidebar never inspects the driver id.

mod actions;
mod fetch;

pub(crate) use actions::PendingRootSectionAction;
pub use actions::{RootSectionActionConfirm, RootSectionActionRequest};

use dbflux_core::{LocalizedText, RootSectionDef, RootSectionItem, SchemaNodeId, TaskId};
use gpui::Task;
use gpui_component::tree::TreeItem;
use std::collections::HashMap;
use uuid::Uuid;

use crate::Sidebar;

/// The items listed so far for one section, in listing order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootSectionListing {
    pub(crate) items: Vec<RootSectionItem>,
    /// Token of the next page; `None` once the last page is listed.
    pub(crate) next_page_token: Option<String>,
}

/// The declared sections of one connection and the listings loaded for them.
/// A section with neither a listing nor a listing error is still loading.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProfileRootSections {
    pub(crate) sections: Vec<RootSectionDef>,
    pub(crate) listings: HashMap<String, RootSectionListing>,
    /// The error of a first page that failed, keyed by section id. Shown as a
    /// retry row and never retried on its own, so a failing listing cannot
    /// refetch in a loop.
    pub(crate) listing_errors: HashMap<String, String>,
}

impl ProfileRootSections {
    pub(crate) fn section(&self, section_id: &str) -> Option<&RootSectionDef> {
        self.sections
            .iter()
            .find(|section| section.id == section_id)
    }
}

/// Session cache of the root sections of every connected profile.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootSectionCache {
    /// Profiles whose declarations loaded.
    pub(crate) profiles: HashMap<Uuid, ProfileRootSections>,
    /// The error of the last declaration fetch of each profile whose fetch
    /// failed. A failure is never cached as "no sections": it shows a retry
    /// row and is asked again from there.
    pub(crate) declaration_errors: HashMap<Uuid, String>,
}

impl RootSectionCache {
    pub(crate) fn get(&self, profile_id: &Uuid) -> Option<&ProfileRootSections> {
        self.profiles.get(profile_id)
    }

    pub(crate) fn get_mut(&mut self, profile_id: &Uuid) -> Option<&mut ProfileRootSections> {
        self.profiles.get_mut(profile_id)
    }

    /// Every profile with loaded declarations or a failed declaration fetch.
    pub(crate) fn profile_ids(&self) -> impl Iterator<Item = &Uuid> {
        self.profiles.keys().chain(self.declaration_errors.keys())
    }

    pub(crate) fn remove(&mut self, profile_id: &Uuid) {
        self.profiles.remove(profile_id);
        self.declaration_errors.remove(profile_id);
    }
}

impl From<HashMap<Uuid, ProfileRootSections>> for RootSectionCache {
    fn from(profiles: HashMap<Uuid, ProfileRootSections>) -> Self {
        Self {
            profiles,
            declaration_errors: HashMap::new(),
        }
    }
}

/// What a root section fetch loads: the declarations (`None`) or the next
/// page of one section's listing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RootSectionFetchKey {
    pub(crate) profile_id: Uuid,
    pub(crate) section_id: Option<String>,
}

/// One root section fetch in flight.
pub(crate) struct PendingRootSectionFetch {
    /// Dropping it abandons the fetch's foreground completion.
    pub(crate) _task: Task<()>,
    /// The tasks panel entry of a page fetch, which whoever abandons the fetch
    /// must settle, since the dropped completion never will.
    pub(crate) load_task_id: Option<TaskId>,
}

/// In-flight root section fetches.
pub(crate) type PendingRootSectionFetches = HashMap<RootSectionFetchKey, PendingRootSectionFetch>;

/// Row ids of the retry rows, which are not node ids: activating one fetches
/// again what failed.
pub(crate) const DECLARATIONS_RETRY_PREFIX: &str = "root-sections-retry|";
pub(crate) const LISTING_RETRY_PREFIX: &str = "root-section-retry|";

/// The text of a driver-declared label: its catalog entry when the catalog
/// knows the key, the declared fallback otherwise.
pub(crate) fn resolve_text(text: &LocalizedText) -> String {
    text.key
        .as_deref()
        .and_then(dbflux_i18n::try_translate)
        .unwrap_or_else(|| text.fallback.clone())
}

/// The folder label: the plain section label while the listing loads, then
/// the label with the number of items listed so far.
pub(crate) fn section_folder_label(section: &RootSectionDef, count: Option<usize>) -> String {
    let label = resolve_text(&section.label);

    let Some(count) = count else {
        return label;
    };

    match section
        .counted_label_key
        .as_deref()
        .and_then(dbflux_i18n::try_translate)
        .filter(|counted| counted.contains("%{count}"))
    {
        Some(counted) => counted.replace("%{count}", &count.to_string()),
        None => dbflux_i18n::t!(
            "sidebar.tree.status.remote_listing_count",
            label = label,
            count = count
        ),
    }
}

/// The rows root sections add under a connection: one folder per declared
/// section, in declaration order, once the declarations are loaded; a retry
/// row when their fetch failed; a loading row until then.
pub(crate) fn build_root_section_folders(
    profile_id: Uuid,
    cache: &RootSectionCache,
) -> Vec<TreeItem> {
    let Some(profile_sections) = cache.get(&profile_id) else {
        let row = match cache.declaration_errors.get(&profile_id) {
            Some(error) => Sidebar::error_retry_placeholder(
                &format!("{DECLARATIONS_RETRY_PREFIX}{profile_id}"),
                error,
            ),
            None => TreeItem::new(
                format!("root-sections-loading|{profile_id}"),
                dbflux_i18n::t!("sidebar.tree.status.loading"),
            ),
        };
        return vec![row];
    };

    profile_sections
        .sections
        .iter()
        .map(|section| {
            build_root_section_folder(
                profile_id,
                section,
                profile_sections.listings.get(&section.id),
                profile_sections
                    .listing_errors
                    .get(&section.id)
                    .map(String::as_str),
            )
        })
        .collect()
}

/// The folder of one section. A first page that failed shows a retry row in
/// place of the items.
pub(crate) fn build_root_section_folder(
    profile_id: Uuid,
    section: &RootSectionDef,
    listing: Option<&RootSectionListing>,
    listing_error: Option<&str>,
) -> TreeItem {
    let label = section_folder_label(section, listing.map(|listing| listing.items.len()));
    let children = match (listing, listing_error) {
        (None, Some(error)) => vec![Sidebar::error_retry_placeholder(
            &format!("{LISTING_RETRY_PREFIX}{profile_id}|{}", section.id),
            error,
        )],
        _ => build_root_section_children(profile_id, section, listing),
    };

    TreeItem::new(
        SchemaNodeId::RootSection {
            profile_id,
            section_id: section.id.clone(),
        }
        .to_string(),
        label,
    )
    .expanded(true)
    .children(children)
}

/// The rows of a section: a loading row until the first page resolves, an
/// empty row for an empty listing, otherwise one leaf per item and a load
/// more row while pages remain.
pub(crate) fn build_root_section_children(
    profile_id: Uuid,
    section: &RootSectionDef,
    listing: Option<&RootSectionListing>,
) -> Vec<TreeItem> {
    let Some(listing) = listing else {
        return vec![Sidebar::loading_placeholder(
            profile_id,
            &section.id,
            "root-section-loading",
        )];
    };

    if listing.items.is_empty() && listing.next_page_token.is_none() {
        return vec![TreeItem::new(
            format!("root-section-empty|{profile_id}|{}", section.id),
            resolve_text(&section.empty_label),
        )];
    }

    let mut rows: Vec<TreeItem> = listing
        .items
        .iter()
        .map(|item| {
            TreeItem::new(
                SchemaNodeId::RootSectionItem {
                    profile_id,
                    section_id: section.id.clone(),
                    item_id: item.id.clone(),
                }
                .to_string(),
                item.label.clone(),
            )
        })
        .collect();

    if listing.next_page_token.is_some() {
        rows.push(TreeItem::new(
            SchemaNodeId::RootSectionLoadMore {
                profile_id,
                section_id: section.id.clone(),
            }
            .to_string(),
            dbflux_i18n::t!("sidebar.tree.status.load_more"),
        ));
    }

    rows
}

#[cfg(test)]
pub(crate) mod tests;
