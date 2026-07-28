//! S3-compatible object storage driver for DBFlux.
//!
//! Implements the `ObjectStoreConnection` capability (defined in `dbflux_core`)
//! against AWS S3 and S3-compatible endpoints such as Cloudflare R2 and MinIO,
//! using either AWS profile/SSO auth or static access-key credentials.
#![allow(clippy::result_large_err)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
    )
)]
