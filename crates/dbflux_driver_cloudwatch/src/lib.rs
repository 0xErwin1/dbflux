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

pub mod dashboard_import;
pub mod driver;
pub mod metric_catalog;

pub use dashboard_import::CloudWatchDashboardImporter;
pub use driver::{CLOUDWATCH_METADATA, CloudWatchDriver};
