//! Driver-declared root sections (`RootSectionSource`): one folder per
//! section at the root of a connection, with the listed items as its rows.
//!
//! Every section renders through the builders here, gated on
//! `DriverCapabilities::ROOT_SECTIONS`. Labels, presentations and actions all
//! come from the declaration, so the sidebar never inspects the driver id.

mod actions;
mod fetch;

pub use actions::{RootSectionActionConfirm, RootSectionActionRequest};

use dbflux_core::{LocalizedText, RootSectionDef, RootSectionItem, SchemaNodeId};
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
/// A section without a listing is still loading.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProfileRootSections {
    pub(crate) sections: Vec<RootSectionDef>,
    pub(crate) listings: HashMap<String, RootSectionListing>,
}

impl ProfileRootSections {
    pub(crate) fn section(&self, section_id: &str) -> Option<&RootSectionDef> {
        self.sections
            .iter()
            .find(|section| section.id == section_id)
    }
}

/// Session cache of the root sections of every connected profile.
pub(crate) type RootSectionCache = HashMap<Uuid, ProfileRootSections>;

/// What a root section fetch loads: the declarations (`None`) or the next
/// page of one section's listing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RootSectionFetchKey {
    pub(crate) profile_id: Uuid,
    pub(crate) section_id: Option<String>,
}

/// In-flight root section fetches.
pub(crate) type PendingRootSectionFetches = HashMap<RootSectionFetchKey, Task<()>>;

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
    {
        Some(counted) => counted.replace("%{count}", &count.to_string()),
        None => dbflux_i18n::t!(
            "sidebar.tree.status.remote_listing_count",
            label = label,
            count = count
        ),
    }
}

/// One folder per declared section, in declaration order.
pub(crate) fn build_root_section_folders(
    profile_id: Uuid,
    cache: &RootSectionCache,
) -> Vec<TreeItem> {
    let Some(profile_sections) = cache.get(&profile_id) else {
        return Vec::new();
    };

    profile_sections
        .sections
        .iter()
        .map(|section| {
            build_root_section_folder(
                profile_id,
                section,
                profile_sections.listings.get(&section.id),
            )
        })
        .collect()
}

pub(crate) fn build_root_section_folder(
    profile_id: Uuid,
    section: &RootSectionDef,
    listing: Option<&RootSectionListing>,
) -> TreeItem {
    let label = section_folder_label(section, listing.map(|listing| listing.items.len()));

    TreeItem::new(
        SchemaNodeId::RootSection {
            profile_id,
            section_id: section.id.clone(),
        }
        .to_string(),
        label,
    )
    .expanded(true)
    .children(build_root_section_children(profile_id, section, listing))
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
