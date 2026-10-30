//! Driver-declared root sections: object kinds shown at the root of a
//! connection in the sidebar (S3 buckets, and later Kafka topics or consumer
//! groups), each with its own item listing, presentation and actions.
//!
//! The sidebar renders every declared section through one generic builder and
//! never inspects the driver id. Node ids carry the section id, so a new
//! object kind needs no new `SchemaNodeKind` variant.

use serde::{Deserialize, Serialize};

use crate::{DbError, ExecutionClassification};

/// Text shown in the UI for a driver-declared label.
///
/// These types cross the driver RPC protocol, which encodes with `postcard`,
/// so fields are never skipped on serialization; `#[serde(default)]` only
/// helps hand-written JSON.
///
/// `key` names an entry of the host's i18n catalog; the UI resolves it when the
/// catalog knows it and shows `fallback` otherwise. Drivers do not depend on
/// the catalog, and external RPC drivers usually send only the fallback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalizedText {
    #[serde(default)]
    pub key: Option<String>,
    pub fallback: String,
}

impl LocalizedText {
    pub fn plain(fallback: impl Into<String>) -> Self {
        Self {
            key: None,
            fallback: fallback.into(),
        }
    }

    pub fn keyed(key: impl Into<String>, fallback: impl Into<String>) -> Self {
        Self {
            key: Some(key.into()),
            fallback: fallback.into(),
        }
    }
}

/// What opens when the user activates a section's folder row.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootSectionPresentation {
    /// The object-storage containers table of the connection.
    ObjectContainerTable,
}

/// What opens when the user activates one item of a section.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootItemPresentation {
    /// The object browser rooted at the container whose name is the item id.
    ObjectBrowser,
}

/// An operation the user can run on one item of a section.
///
/// `classification` decides how the host gates the action: `Destructive`,
/// `Admin` and `AdminDestructive` ask for confirmation, and every run is
/// audited with its classification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootSectionAction {
    /// Stable driver-scoped id, passed back to `RootSectionSource::execute_action`.
    pub id: String,
    pub label: LocalizedText,
    /// Shown in the confirmation dialog.
    #[serde(default)]
    pub description: Option<LocalizedText>,
    pub classification: ExecutionClassification,
}

impl RootSectionAction {
    pub fn requires_confirmation(&self) -> bool {
        matches!(
            self.classification,
            ExecutionClassification::Destructive
                | ExecutionClassification::Admin
                | ExecutionClassification::AdminDestructive
        )
    }
}

/// One section shown at the root of a connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootSectionDef {
    /// Stable driver-scoped id. It is embedded in sidebar node ids, so it must
    /// not contain `|`.
    pub id: String,
    pub label: LocalizedText,
    /// Catalog key that formats the label with the loaded item count through a
    /// `count` argument. Without it the host shows `"<label> (<count>)"`.
    #[serde(default)]
    pub counted_label_key: Option<String>,
    /// Shown as the only child when the listing is empty.
    pub empty_label: LocalizedText,
    #[serde(default)]
    pub presentation: Option<RootSectionPresentation>,
    pub item_presentation: RootItemPresentation,
    /// Activating the connection row opens this section's presentation.
    #[serde(default)]
    pub opens_on_profile_activation: bool,
    #[serde(default)]
    pub item_actions: Vec<RootSectionAction>,
}

impl RootSectionDef {
    pub fn item_action(&self, action_id: &str) -> Option<&RootSectionAction> {
        self.item_actions
            .iter()
            .find(|action| action.id == action_id)
    }
}

/// One listed item of a section. `id` is what the driver and the item
/// presentation receive; `label` is what the sidebar shows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootSectionItem {
    pub id: String,
    pub label: String,
}

/// One page of a section listing. `next_page_token` is `None` on the last page.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootSectionPage {
    pub items: Vec<RootSectionItem>,
    #[serde(default)]
    pub next_page_token: Option<String>,
}

/// Driver seam behind `Connection::root_section_source`.
///
/// Methods block and must run on a background executor. A driver that
/// implements it advertises `DriverCapabilities::ROOT_SECTIONS`.
pub trait RootSectionSource: Send + Sync {
    /// The sections of this connection, in display order.
    fn sections(&self) -> Vec<RootSectionDef>;

    /// Lists one page of `section_id`. `page_token` is `None` for the first
    /// page and the previous page's `next_page_token` afterwards.
    fn list_items(
        &self,
        section_id: &str,
        page_token: Option<&str>,
    ) -> Result<RootSectionPage, DbError>;

    /// Runs the item action `action_id` on `item_id`. The host has already
    /// confirmed it when `RootSectionAction::requires_confirmation` holds.
    fn execute_action(
        &self,
        _section_id: &str,
        _item_id: &str,
        action_id: &str,
    ) -> Result<(), DbError> {
        Err(DbError::NotSupported(format!(
            "root section action '{action_id}' is not supported"
        )))
    }
}

#[cfg(test)]
mod tests;
