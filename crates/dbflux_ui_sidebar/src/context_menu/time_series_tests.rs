//! The measurement menu actions follow object facets and the database layout,
//! never the connection's paradigm.

use super::{collection_has_time_series_facet, lists_measurements};
use crate::tree_builder::tree_shape_tests::connected_profile;
use dbflux_core::{
    DatabaseCategory, DocumentSchema, DriverCapabilities, KeyValueSchema, RelationalSchema,
    SchemaLoadingStrategy, SchemaSnapshot, TimeSeriesSchema,
};

fn profile(category: DatabaseCategory, schema: SchemaSnapshot) -> dbflux_core::ConnectedProfile {
    connected_profile(
        category,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::LazyPerDatabase,
        schema,
    )
}

#[test]
fn measurements_carry_the_time_series_facet() {
    let time_series = profile(
        DatabaseCategory::TimeSeries,
        SchemaSnapshot::time_series(TimeSeriesSchema::default()),
    );
    let document = profile(
        DatabaseCategory::Document,
        SchemaSnapshot::document(DocumentSchema::default()),
    );

    assert!(collection_has_time_series_facet(
        &time_series,
        "monitoring",
        "cpu"
    ));
    assert!(!collection_has_time_series_facet(
        &document, "shop", "orders"
    ));
}

#[test]
fn only_measurement_folders_offer_a_database_query() {
    let time_series = profile(
        DatabaseCategory::TimeSeries,
        SchemaSnapshot::time_series(TimeSeriesSchema::default()),
    );
    assert!(lists_measurements(&time_series));

    for (category, schema) in [
        (
            DatabaseCategory::Document,
            SchemaSnapshot::document(DocumentSchema::default()),
        ),
        (
            DatabaseCategory::Relational,
            SchemaSnapshot::relational(RelationalSchema::default()),
        ),
        (
            DatabaseCategory::KeyValue,
            SchemaSnapshot::key_value(KeyValueSchema::default()),
        ),
    ] {
        assert!(!lists_measurements(&profile(category, schema)));
    }

    let mut unloaded = time_series;
    unloaded.schema = None;
    assert!(!lists_measurements(&unloaded));
}
