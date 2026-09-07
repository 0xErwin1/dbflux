#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    BucketCreateOptions, BucketEncryption, ConnectionProfile, DbConfig, DbError, DbKind,
    ObjectStoreConnection, PresignMethod, VersioningStatus,
};
use dbflux_driver_s3::S3Driver;
use dbflux_test_support::containers::{self, MinioConfig};
use std::time::Duration;

fn minio_profile(minio: &MinioConfig) -> ConnectionProfile {
    ConnectionProfile::new_with_driver(
        "live-minio",
        DbKind::S3,
        "builtin:s3",
        DbConfig::S3 {
            region: minio.region.clone(),
            profile: None,
            access_key_id: Some(minio.access_key_id.clone()),
            endpoint: Some(minio.endpoint.clone()),
            path_style: true,
        },
    )
}

fn connect_minio(minio: &MinioConfig) -> Result<Box<dyn ObjectStoreConnection>, DbError> {
    let driver = S3Driver::new();
    let profile = minio_profile(minio);
    let secret = SecretString::from(minio.secret_access_key.clone());

    containers::retry_db_operation(Duration::from_secs(30), || {
        driver.connect_object_store(&profile, Some(&secret))
    })
}

fn create_bucket(store: &dyn ObjectStoreConnection, bucket: &str) -> Result<(), DbError> {
    let outcome = store.create_bucket(
        bucket,
        BucketCreateOptions {
            region: "us-east-1".to_string(),
            versioning: false,
            block_public_access: false,
            object_lock: false,
            encryption: BucketEncryption::None,
        },
    )?;

    assert!(
        outcome.warnings.is_empty(),
        "unexpected warnings creating bucket {bucket}: {:?}",
        outcome.warnings
    );

    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn minio_connect_and_list_buckets_round_trips() -> Result<(), DbError> {
    containers::with_minio_endpoint(|minio| {
        let store = connect_minio(&minio)?;

        let before = store.list_buckets()?;
        assert!(before.is_empty(), "fresh MinIO instance should start empty");

        create_bucket(store.as_ref(), "dbflux-connect-test")?;

        let after = store.list_buckets()?;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].name, "dbflux-connect-test");

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn minio_create_bucket_degrades_unsupported_options_into_warnings() -> Result<(), DbError> {
    containers::with_minio_endpoint(|minio| {
        let store = connect_minio(&minio)?;

        let outcome = store.create_bucket(
            "dbflux-degradation-test",
            BucketCreateOptions {
                region: "us-east-1".to_string(),
                versioning: true,
                block_public_access: true,
                object_lock: true,
                encryption: BucketEncryption::SseS3,
            },
        )?;

        // MinIO's exact support surface for object-lock/block-public-access
        // drifts across releases; the contract under test is that whichever
        // options this MinIO build rejects come back as non-fatal warnings
        // instead of failing bucket creation.
        let buckets = store.list_buckets()?;
        assert!(buckets.iter().any(|b| b.name == "dbflux-degradation-test"));

        for warning in &outcome.warnings {
            assert!(
                warning.contains("is not supported by this endpoint"),
                "unexpected warning shape: {warning}"
            );
        }

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn minio_list_objects_paginates_one_level_with_prefixes() -> Result<(), DbError> {
    containers::with_minio_endpoint(|minio| {
        let store = connect_minio(&minio)?;

        let bucket = "dbflux-listing-test";
        create_bucket(store.as_ref(), bucket)?;

        store.put_object(bucket, "root-file.txt", b"root".to_vec(), None)?;
        store.put_object(bucket, "folder/nested-a.txt", b"a".to_vec(), None)?;
        store.put_object(bucket, "folder/nested-b.txt", b"b".to_vec(), None)?;

        let page = store.list_objects(bucket, "", None)?;
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "root-file.txt");
        assert_eq!(page.common_prefixes, vec!["folder/".to_string()]);
        assert!(page.next_continuation_token.is_none());

        let nested_page = store.list_objects(bucket, "folder/", None)?;
        assert_eq!(nested_page.objects.len(), 2);
        assert!(nested_page.common_prefixes.is_empty());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn minio_put_get_upload_copy_and_delete_object_round_trip() -> Result<(), DbError> {
    containers::with_minio_endpoint(|minio| {
        let store = connect_minio(&minio)?;

        let bucket = "dbflux-object-lifecycle-test";
        create_bucket(store.as_ref(), bucket)?;

        store.put_object(
            bucket,
            "docs/original.txt",
            b"hello from dbflux".to_vec(),
            Some("text/plain"),
        )?;

        let body = store.get_object(bucket, "docs/original.txt")?;
        assert_eq!(body, b"hello from dbflux");

        let temp_dir = std::env::temp_dir().join(format!(
            "dbflux-s3-upload-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&temp_dir)
            .map_err(|e| DbError::query_failed(format!("failed to create temp dir: {e}")))?;
        let source_path = temp_dir.join("uploaded.txt");
        std::fs::write(&source_path, b"uploaded via streaming path")
            .map_err(|e| DbError::query_failed(format!("failed to write temp file: {e}")))?;

        store.upload_object(
            bucket,
            "docs/uploaded.txt",
            &source_path,
            Some("text/plain"),
        )?;
        let uploaded_body = store.get_object(bucket, "docs/uploaded.txt")?;
        assert_eq!(uploaded_body, b"uploaded via streaming path");

        std::fs::remove_dir_all(&temp_dir).ok();

        // Rename-shaped composition: copy then delete the original, exactly
        // as `ObjectBrowserDocument`'s rename flow will do (DEC-13).
        store.copy_object(bucket, "docs/original.txt", "docs/renamed.txt")?;
        let copied_body = store.get_object(bucket, "docs/renamed.txt")?;
        assert_eq!(copied_body, b"hello from dbflux");

        store.delete_object(bucket, "docs/original.txt")?;
        let after_rename = store.list_objects(bucket, "docs/", None)?;
        let keys: Vec<&str> = after_rename
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect();
        assert!(!keys.contains(&"docs/original.txt"));
        assert!(keys.contains(&"docs/renamed.txt"));
        assert!(keys.contains(&"docs/uploaded.txt"));

        store.delete_object(bucket, "docs/renamed.txt")?;
        store.delete_object(bucket, "docs/uploaded.txt")?;

        let empty = store.list_objects(bucket, "docs/", None)?;
        assert!(empty.objects.is_empty());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn minio_delete_prefix_batches_more_than_one_thousand_objects() -> Result<(), DbError> {
    containers::with_minio_endpoint(|minio| {
        let store = connect_minio(&minio)?;

        let bucket = "dbflux-bulk-delete-test";
        create_bucket(store.as_ref(), bucket)?;

        // Exercises the driver's DELETE_BATCH_SIZE=1000 chunked-loop path —
        // one page short of two full batches, and small bodies so the seed
        // step stays fast.
        const OBJECT_COUNT: usize = 1_250;
        for index in 0..OBJECT_COUNT {
            store.put_object(
                bucket,
                &format!("bulk/object-{index:05}.txt"),
                b"x".to_vec(),
                None,
            )?;
        }

        let outside_scope_key = "outside-bulk-scope.txt";
        store.put_object(bucket, outside_scope_key, b"keep me".to_vec(), None)?;

        let outcome = store.delete_prefix(bucket, "bulk/")?;
        assert_eq!(outcome.deleted_count, OBJECT_COUNT as u64);

        let remaining_under_prefix = store.list_objects(bucket, "bulk/", None)?;
        assert!(remaining_under_prefix.objects.is_empty());

        let remaining_outside = store.list_objects(bucket, "", None)?;
        assert!(
            remaining_outside
                .objects
                .iter()
                .any(|object| object.key == outside_scope_key),
            "delete_prefix must not remove keys outside the deleted scope"
        );

        store.delete_object(bucket, outside_scope_key)?;

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn minio_delete_bucket_requires_empty_bucket() -> Result<(), DbError> {
    containers::with_minio_endpoint(|minio| {
        let store = connect_minio(&minio)?;

        let bucket = "dbflux-empty-only-delete-test";
        create_bucket(store.as_ref(), bucket)?;

        store.put_object(bucket, "blocking-object.txt", b"still here".to_vec(), None)?;

        // The driver's `delete_bucket` is a thin `DeleteBucket` call with no
        // empty-only guard of its own — S3/MinIO itself rejects deleting a
        // non-empty bucket, which is the contract this asserts.
        let non_empty_result = store.delete_bucket(bucket);
        assert!(
            non_empty_result.is_err(),
            "deleting a non-empty bucket must fail"
        );

        store.delete_object(bucket, "blocking-object.txt")?;
        store.delete_bucket(bucket)?;

        let buckets = store.list_buckets()?;
        assert!(!buckets.iter().any(|b| b.name == bucket));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn minio_presign_get_url_is_well_formed_and_not_logged() -> Result<(), DbError> {
    containers::with_minio_endpoint(|minio| {
        let store = connect_minio(&minio)?;

        let bucket = "dbflux-presign-test";
        create_bucket(store.as_ref(), bucket)?;
        store.put_object(bucket, "presign-target.txt", b"presigned".to_vec(), None)?;

        let expiry = Duration::from_secs(3600);
        let url = store.presign(bucket, "presign-target.txt", PresignMethod::Get, expiry)?;

        // Assert URL *shape* only — the value itself is intentionally never
        // included in any assertion failure message or logged output below,
        // matching DEC-15's "never logged, persisted, or audited" contract.
        let is_well_formed = url.starts_with("http://") || url.starts_with("https://");
        assert!(is_well_formed, "presigned URL has an unexpected scheme");
        assert!(
            url.contains("X-Amz-Expires=3600"),
            "presigned URL must encode the requested expiry"
        );
        assert!(
            url.contains(bucket),
            "presigned URL must reference the target bucket"
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn minio_estimate_bucket_size_and_list_object_versions() -> Result<(), DbError> {
    containers::with_minio_endpoint(|minio| {
        let store = connect_minio(&minio)?;

        let bucket = "dbflux-size-and-versions-test";
        create_bucket(store.as_ref(), bucket)?;

        store.put_object(bucket, "sized/a.txt", vec![0u8; 100], None)?;
        store.put_object(bucket, "sized/b.txt", vec![0u8; 250], None)?;

        let estimate = store.estimate_bucket_size(bucket, 10_000)?;
        assert_eq!(estimate.object_count, 2);
        assert_eq!(estimate.total_bytes, 350);
        assert!(!estimate.truncated);

        let capped_estimate = store.estimate_bucket_size(bucket, 1)?;
        assert!(capped_estimate.truncated);
        assert!(capped_estimate.object_count >= 1);

        let details = store.get_bucket_details(bucket)?;
        match details.versioning {
            VersioningStatus::Disabled => {
                let versions = store.list_object_versions(bucket, "sized/a.txt")?;
                assert!(
                    versions.is_empty() || versions.len() == 1,
                    "an unversioned bucket should expose at most the current version"
                );
            }
            VersioningStatus::Enabled | VersioningStatus::Suspended => {
                let versions = store.list_object_versions(bucket, "sized/a.txt")?;
                assert!(!versions.is_empty());
            }
        }

        Ok(())
    })
}

#[test]
fn minio_endpoint_failures_are_actionable() {
    let driver = S3Driver::new();
    let profile = ConnectionProfile::new_with_driver(
        "minio-invalid-endpoint",
        DbKind::S3,
        "builtin:s3",
        DbConfig::S3 {
            region: "us-east-1".to_string(),
            profile: None,
            access_key_id: Some("minioadmin".to_string()),
            endpoint: Some("http://127.0.0.1:9".to_string()),
            path_style: true,
        },
    );
    let secret = SecretString::from("minioadmin".to_string());

    let error = driver
        .connect_object_store(&profile, Some(&secret))
        .err()
        .expect("connecting to an unavailable endpoint should fail");

    let text = error.to_string().to_ascii_lowercase();
    assert!(
        text.contains("endpoint") || text.contains("connection") || text.contains("timed out"),
        "unexpected failure text: {text}"
    );
}
