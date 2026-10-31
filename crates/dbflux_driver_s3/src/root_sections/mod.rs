//! The S3 connection's sidebar root sections: one `buckets` section whose
//! items are the account's buckets and open in the object browser.

use dbflux_core::{
    DbError, ExecutionClassification, LocalizedText, ObjectStoreConnection, RootItemPresentation,
    RootSectionAction, RootSectionDef, RootSectionItem, RootSectionPage, RootSectionPresentation,
    RootSectionSource,
};

use crate::driver::S3Connection;

pub(crate) const BUCKETS_SECTION_ID: &str = "buckets";
pub(crate) const DELETE_BUCKET_ACTION_ID: &str = "delete_bucket";

/// The bucket operations the sections need, split out of
/// `ObjectStoreConnection` so the dispatch is testable without an endpoint.
pub(crate) trait BucketCatalog {
    fn bucket_names(&self) -> Result<Vec<String>, DbError>;
    fn remove_bucket(&self, bucket: &str) -> Result<(), DbError>;
}

impl<T: ObjectStoreConnection + ?Sized> BucketCatalog for T {
    fn bucket_names(&self) -> Result<Vec<String>, DbError> {
        Ok(self
            .list_buckets()?
            .into_iter()
            .map(|bucket| bucket.name)
            .collect())
    }

    fn remove_bucket(&self, bucket: &str) -> Result<(), DbError> {
        self.delete_bucket(bucket)
    }
}

pub(crate) fn buckets_section() -> RootSectionDef {
    RootSectionDef {
        id: BUCKETS_SECTION_ID.to_string(),
        label: LocalizedText::keyed("sidebar.tree.folder.buckets", "Buckets"),
        counted_label_key: Some("sidebar.tree.container.object_storage".to_string()),
        empty_label: LocalizedText::keyed("sidebar.tree.empty.buckets", "No buckets"),
        presentation: Some(RootSectionPresentation::ObjectContainerTable),
        item_presentation: RootItemPresentation::ObjectBrowser,
        opens_on_profile_activation: true,
        item_actions: vec![RootSectionAction {
            id: DELETE_BUCKET_ACTION_ID.to_string(),
            label: LocalizedText::keyed(
                "sidebar.root_section.action.delete_bucket",
                "Delete bucket",
            ),
            description: Some(LocalizedText::keyed(
                "sidebar.root_section.action.delete_bucket_description",
                "Deletes the bucket. Only an empty bucket can be deleted, and this cannot be undone.",
            )),
            classification: ExecutionClassification::AdminDestructive,
        }],
    }
}

fn unknown_section(section_id: &str) -> DbError {
    DbError::NotSupported(format!("S3 has no root section '{section_id}'"))
}

/// S3 lists every bucket in one call, so the section has a single page.
pub(crate) fn list_section_items(
    catalog: &dyn BucketCatalog,
    section_id: &str,
) -> Result<RootSectionPage, DbError> {
    if section_id != BUCKETS_SECTION_ID {
        return Err(unknown_section(section_id));
    }

    let items = catalog
        .bucket_names()?
        .into_iter()
        .map(|name| RootSectionItem {
            id: name.clone(),
            label: name,
        })
        .collect();

    Ok(RootSectionPage {
        items,
        next_page_token: None,
    })
}

pub(crate) fn execute_section_action(
    catalog: &dyn BucketCatalog,
    section_id: &str,
    item_id: &str,
    action_id: &str,
) -> Result<(), DbError> {
    if section_id != BUCKETS_SECTION_ID {
        return Err(unknown_section(section_id));
    }

    match action_id {
        DELETE_BUCKET_ACTION_ID => catalog.remove_bucket(item_id),
        _ => Err(DbError::NotSupported(format!(
            "S3 buckets have no action '{action_id}'"
        ))),
    }
}

impl RootSectionSource for S3Connection {
    fn sections(&self) -> Vec<RootSectionDef> {
        vec![buckets_section()]
    }

    fn list_items(
        &self,
        section_id: &str,
        _page_token: Option<&str>,
    ) -> Result<RootSectionPage, DbError> {
        list_section_items(self, section_id)
    }

    fn execute_action(
        &self,
        section_id: &str,
        item_id: &str,
        action_id: &str,
    ) -> Result<(), DbError> {
        execute_section_action(self, section_id, item_id, action_id)
    }
}

#[cfg(test)]
mod tests;
