use std::cell::RefCell;

use dbflux_core::{
    DbError, DriverCapabilities, ExecutionClassification, RootItemPresentation,
    RootSectionPresentation,
};

use super::*;
use crate::S3_METADATA;

#[derive(Default)]
struct FakeCatalog {
    names: Vec<String>,
    deleted: RefCell<Vec<String>>,
}

impl BucketCatalog for FakeCatalog {
    fn bucket_names(&self) -> Result<Vec<String>, DbError> {
        Ok(self.names.clone())
    }

    fn remove_bucket(&self, bucket: &str) -> Result<(), DbError> {
        self.deleted.borrow_mut().push(bucket.to_string());
        Ok(())
    }
}

#[test]
fn metadata_advertises_root_sections() {
    assert!(
        S3_METADATA
            .capabilities
            .contains(DriverCapabilities::ROOT_SECTIONS)
    );
}

#[test]
fn buckets_section_declares_the_sidebar_contract() {
    let section = buckets_section();

    assert_eq!(section.id, "buckets");
    assert_eq!(
        section.label.key.as_deref(),
        Some("sidebar.tree.folder.buckets")
    );
    assert_eq!(section.label.fallback, "Buckets");
    assert_eq!(
        section.counted_label_key.as_deref(),
        Some("sidebar.tree.container.object_storage")
    );
    assert_eq!(
        section.empty_label.key.as_deref(),
        Some("sidebar.tree.empty.buckets")
    );
    assert_eq!(section.empty_label.fallback, "No buckets");
    assert_eq!(
        section.presentation,
        Some(RootSectionPresentation::ObjectContainerTable)
    );
    assert_eq!(
        section.item_presentation,
        RootItemPresentation::ObjectBrowser
    );
    assert!(section.opens_on_profile_activation);

    let delete = section
        .item_action("delete_bucket")
        .expect("delete_bucket action");
    assert_eq!(
        delete.classification,
        ExecutionClassification::AdminDestructive
    );
    assert!(delete.requires_confirmation());
    assert_eq!(section.item_actions.len(), 1);
}

#[test]
fn list_items_returns_every_bucket_in_one_page() {
    let catalog = FakeCatalog {
        names: vec!["alpha".into(), "beta".into()],
        ..FakeCatalog::default()
    };

    let page = list_section_items(&catalog, "buckets").unwrap();

    let ids: Vec<_> = page.items.iter().map(|item| item.id.as_str()).collect();
    let labels: Vec<_> = page.items.iter().map(|item| item.label.as_str()).collect();
    assert_eq!(ids, ["alpha", "beta"]);
    assert_eq!(labels, ["alpha", "beta"]);
    assert_eq!(page.next_page_token, None);
}

#[test]
fn list_items_rejects_an_unknown_section() {
    let error = list_section_items(&FakeCatalog::default(), "topics").unwrap_err();

    assert!(matches!(error, DbError::NotSupported(_)), "{error:?}");
}

#[test]
fn delete_bucket_action_deletes_the_item() {
    let catalog = FakeCatalog::default();

    execute_section_action(&catalog, "buckets", "old-bucket", "delete_bucket").unwrap();

    assert_eq!(*catalog.deleted.borrow(), ["old-bucket"]);
}

#[test]
fn unknown_section_or_action_is_not_supported_and_deletes_nothing() {
    let catalog = FakeCatalog::default();

    let unknown_action =
        execute_section_action(&catalog, "buckets", "old-bucket", "empty_bucket").unwrap_err();
    let unknown_section =
        execute_section_action(&catalog, "topics", "old-bucket", "delete_bucket").unwrap_err();

    assert!(matches!(unknown_action, DbError::NotSupported(_)));
    assert!(matches!(unknown_section, DbError::NotSupported(_)));
    assert!(catalog.deleted.borrow().is_empty());
}
