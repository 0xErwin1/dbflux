//! InfluxDB query generator.
//!
//! Produces native InfluxQL and Flux query templates for use in the UI's
//! context menu ("SELECT *", "SHOW MEASUREMENTS", etc.) and MCP previews.

use dbflux_core::{
    CollectionBrowseRequest, CollectionTemplateRequest, GeneratedQuery, InfluxVersion,
    MutationCategory, MutationRequest, QueryGenerator, QueryLanguage, ReadTemplateRequest,
};

use crate::connection::{escape_flux_string, escape_influxql_ident};

/// InfluxDB query generator — produces InfluxQL or Flux templates.
///
/// Query language is determined by the `version` field:
/// - V1 → InfluxQL only.
/// - V2 → InfluxQL and Flux depending on the requested language.
pub struct InfluxQueryGenerator {
    pub version: InfluxVersion,
    pub default_language: QueryLanguage,
    /// Default bucket or database name, used in Flux `from(bucket: ...)` templates.
    ///
    /// When `None`, Flux templates use a `"<bucket>"` placeholder so the user
    /// can fill in the correct bucket name.
    pub default_bucket: Option<String>,
}

impl InfluxQueryGenerator {
    pub fn new(
        version: InfluxVersion,
        default_language: QueryLanguage,
        default_bucket: Option<String>,
    ) -> Self {
        Self {
            version,
            default_language,
            default_bucket,
        }
    }

    /// Generate a `SELECT * FROM "<name>" LIMIT <limit>` InfluxQL statement.
    pub fn select_all_influxql(measurement: &str, limit: u32) -> String {
        format!("SELECT * FROM \"{measurement}\" LIMIT {limit}")
    }

    /// Generate a Flux query that selects all fields for a measurement.
    pub fn select_all_flux(bucket: &str, measurement: &str, limit: u32) -> String {
        format!(
            "from(bucket: \"{bucket}\")\n  |> range(start: -1h)\n  |> filter(fn: (r) => r._measurement == \"{measurement}\")\n  |> limit(n: {limit})"
        )
    }

    /// Generate `SHOW MEASUREMENTS` InfluxQL.
    pub fn show_measurements() -> &'static str {
        "SHOW MEASUREMENTS"
    }

    /// Generate a time-bounded InfluxQL query template for a specific measurement.
    ///
    /// Format: `SELECT * FROM "<bucket>"."autogen"."<measurement>" WHERE time > now() - 1h LIMIT 100`
    pub fn query_measurement_influxql(bucket: &str, measurement: &str) -> String {
        let bucket_escaped = bucket.replace('"', "\"\"");
        let measurement_escaped = measurement.replace('"', "\"\"");
        format!(
            "SELECT * FROM \"{bucket_escaped}\".\"autogen\".\"{measurement_escaped}\" WHERE time > now() - 1h LIMIT 100"
        )
    }

    /// Generate a time-bounded Flux query template for a specific measurement.
    ///
    /// Format: `from(bucket: "<bucket>") |> range(start: -1h) |> filter(...)`
    pub fn query_measurement_flux(bucket: &str, measurement: &str) -> String {
        let bucket_escaped = bucket.replace('\\', "\\\\").replace('"', "\\\"");
        let measurement_escaped = measurement.replace('\\', "\\\\").replace('"', "\\\"");
        format!(
            "from(bucket: \"{bucket_escaped}\")\n  |> range(start: -1h)\n  |> filter(fn: (r) => r._measurement == \"{measurement_escaped}\")"
        )
    }

    /// InfluxQL statement that reads one page of a measurement, newest first.
    pub fn browse_measurement_influxql(measurement: &str, limit: u32, offset: u64) -> String {
        let measurement = escape_influxql_ident(measurement);

        format!("SELECT * FROM {measurement} ORDER BY time DESC LIMIT {limit} OFFSET {offset}")
    }

    /// Flux query that reads one page of a measurement from the last 24 hours,
    /// newest first.
    ///
    /// Flux has no offset, so a later page fetches `offset + limit` rows and
    /// keeps the last `limit` of them with `tail`.
    pub fn browse_measurement_flux(
        bucket: &str,
        measurement: &str,
        limit: u32,
        offset: u64,
    ) -> String {
        let bucket = escape_flux_string(bucket);
        let measurement = escape_flux_string(measurement);

        let head = format!(
            "from(bucket: \"{bucket}\")\
             \n  |> range(start: -24h)\
             \n  |> filter(fn: (r) => r._measurement == \"{measurement}\")\
             \n  |> sort(columns: [\"_time\"], desc: true)"
        );

        if offset == 0 {
            format!("{head}\n  |> limit(n: {limit})")
        } else {
            let fetch = offset + u64::from(limit);
            format!("{head}\n  |> limit(n: {fetch})\n  |> tail(n: {limit})")
        }
    }

    /// The query `InfluxConnection::browse_collection` runs for `request`.
    ///
    /// Flux is used only on a v2 connection whose default language is Flux.
    /// Every other connection browses with InfluxQL. The bucket or database
    /// comes from the collection reference, not the profile default.
    pub fn browse_query(&self, request: &CollectionBrowseRequest) -> GeneratedQuery {
        let measurement = &request.collection.name;
        let limit = request.pagination.limit();
        let offset = request.pagination.offset();

        if self.version == InfluxVersion::V2 && self.default_language == QueryLanguage::Flux {
            GeneratedQuery {
                language: QueryLanguage::Flux,
                text: Self::browse_measurement_flux(
                    &request.collection.database,
                    measurement,
                    limit,
                    offset,
                ),
            }
        } else {
            GeneratedQuery {
                language: QueryLanguage::InfluxQuery,
                text: Self::browse_measurement_influxql(measurement, limit, offset),
            }
        }
    }
}

impl QueryGenerator for InfluxQueryGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        // InfluxDB does not support INSERT/UPDATE/DELETE via the query API.
        &[]
    }

    fn generate_mutation(&self, _mutation: &MutationRequest) -> Option<GeneratedQuery> {
        None
    }

    fn generate_read_template(&self, request: &ReadTemplateRequest<'_>) -> Option<GeneratedQuery> {
        let measurement = request.table;

        match self.default_language {
            QueryLanguage::Flux if self.version == InfluxVersion::V2 => Some(GeneratedQuery {
                language: QueryLanguage::Flux,
                // Use the configured default bucket or a placeholder when none is set.
                text: Self::select_all_flux(
                    self.default_bucket.as_deref().unwrap_or("<bucket>"),
                    measurement,
                    100,
                ),
            }),
            _ => Some(GeneratedQuery {
                language: QueryLanguage::InfluxQuery,
                text: Self::select_all_influxql(measurement, 100),
            }),
        }
    }

    fn collection_browse_query(&self, request: &CollectionBrowseRequest) -> Option<GeneratedQuery> {
        Some(self.browse_query(request))
    }

    fn template_for_collection(
        &self,
        request: &CollectionTemplateRequest<'_>,
    ) -> Option<GeneratedQuery> {
        match self.default_language {
            QueryLanguage::Flux if self.version == InfluxVersion::V2 => Some(GeneratedQuery {
                language: QueryLanguage::Flux,
                text: Self::query_measurement_flux(request.database, request.collection),
            }),
            _ => Some(GeneratedQuery {
                language: QueryLanguage::InfluxQuery,
                text: Self::query_measurement_influxql(request.database, request.collection),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (C.6.1 – C.6.3)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // C.6.1
    #[test]
    fn select_all_influxql_format() {
        let q = InfluxQueryGenerator::select_all_influxql("cpu", 100);
        assert_eq!(q, "SELECT * FROM \"cpu\" LIMIT 100");
    }

    // C.6.2
    #[test]
    fn select_all_flux_format() {
        let q = InfluxQueryGenerator::select_all_flux("my-bucket", "cpu", 100);
        assert!(
            q.contains("from(bucket: \"my-bucket\")"),
            "must include from: {q}"
        );
        assert!(
            q.contains("|> range(start: -1h)"),
            "must include range: {q}"
        );
        assert!(
            q.contains("r._measurement == \"cpu\""),
            "must filter by measurement: {q}"
        );
        assert!(q.contains("limit(n: 100)"), "must include limit: {q}");
    }

    // C.6.3
    #[test]
    fn show_measurements_returns_expected_query() {
        assert_eq!(
            InfluxQueryGenerator::show_measurements(),
            "SHOW MEASUREMENTS"
        );
    }

    // C.6.4 — query_measurement_influxql produces the expected WHERE+LIMIT template
    #[test]
    fn query_measurement_influxql_format() {
        let q = InfluxQueryGenerator::query_measurement_influxql("mydb", "cpu");
        assert!(q.contains("SELECT * FROM"), "must select all: {q}");
        assert!(
            q.contains("\"mydb\".\"autogen\".\"cpu\""),
            "must include quoted db.rp.measurement: {q}"
        );
        assert!(
            q.contains("WHERE time > now() - 1h"),
            "must include time filter: {q}"
        );
        assert!(q.contains("LIMIT 100"), "must include limit: {q}");
    }

    // C.6.5 — query_measurement_flux produces the expected Flux template
    #[test]
    fn query_measurement_flux_format() {
        let q = InfluxQueryGenerator::query_measurement_flux("my-bucket", "temperature");
        assert!(
            q.contains("from(bucket: \"my-bucket\")"),
            "must start from bucket: {q}"
        );
        assert!(
            q.contains("|> range(start: -1h)"),
            "must include range: {q}"
        );
        assert!(
            q.contains("r._measurement == \"temperature\""),
            "must filter by measurement: {q}"
        );
    }

    // C.6.6 — template_for_collection dispatches by version + language
    #[test]
    fn template_for_collection_v1_returns_influxql() {
        let qg = InfluxQueryGenerator::new(
            InfluxVersion::V1,
            QueryLanguage::InfluxQuery,
            Some("mydb".to_string()),
        );

        let request = dbflux_core::CollectionTemplateRequest {
            collection: "cpu",
            database: "mydb",
        };

        let result = qg
            .template_for_collection(&request)
            .expect("must produce template");
        assert_eq!(result.language, QueryLanguage::InfluxQuery);
        assert!(
            result.text.contains("SELECT * FROM"),
            "v1 must use InfluxQL: {}",
            result.text
        );
        assert!(
            result.text.contains("mydb"),
            "must reference bucket: {}",
            result.text
        );
    }

    #[test]
    fn template_for_collection_v2_flux_returns_flux() {
        let qg = InfluxQueryGenerator::new(
            InfluxVersion::V2,
            QueryLanguage::Flux,
            Some("my-bucket".to_string()),
        );

        let request = dbflux_core::CollectionTemplateRequest {
            collection: "temperature",
            database: "my-bucket",
        };

        let result = qg
            .template_for_collection(&request)
            .expect("must produce template");
        assert_eq!(result.language, QueryLanguage::Flux);
        assert!(
            result.text.contains("from(bucket:"),
            "v2/Flux must use Flux: {}",
            result.text
        );
        assert!(
            result.text.contains("temperature"),
            "must reference measurement: {}",
            result.text
        );
    }

    // C.6.7 — special characters in measurement names are properly escaped
    #[test]
    fn query_measurement_influxql_escapes_embedded_quotes() {
        let q = InfluxQueryGenerator::query_measurement_influxql("my\"db", "my\"measurement");
        // Embedded double quotes are escaped by doubling in InfluxQL
        assert!(
            q.contains("\"my\"\"db\""),
            "bucket embedded quote must be doubled: {q}"
        );
        assert!(
            q.contains("\"my\"\"measurement\""),
            "measurement embedded quote must be doubled: {q}"
        );
    }

    #[test]
    fn query_measurement_flux_escapes_embedded_quotes() {
        let q = InfluxQueryGenerator::query_measurement_flux("my\"bucket", "my\"measurement");
        // Embedded double quotes are escaped with backslash in Flux string literals
        assert!(q.contains("\\\""), "Flux must escape embedded quotes: {q}");
    }

    fn browse_request(
        database: &str,
        measurement: &str,
        limit: u32,
        offset: u64,
    ) -> CollectionBrowseRequest {
        CollectionBrowseRequest::new(dbflux_core::CollectionRef::new(database, measurement))
            .with_pagination(dbflux_core::Pagination::Offset { limit, offset })
    }

    #[test]
    fn browse_measurement_influxql_reads_newest_page_first() {
        let query = InfluxQueryGenerator::browse_measurement_influxql("cpu usage", 50, 100);

        assert_eq!(
            query,
            "SELECT * FROM \"cpu usage\" ORDER BY time DESC LIMIT 50 OFFSET 100"
        );
    }

    #[test]
    fn browse_measurement_flux_first_page_limits_without_tail() {
        let query =
            InfluxQueryGenerator::browse_measurement_flux("my_bucket", "temperature", 25, 0);

        assert_eq!(
            query,
            "from(bucket: \"my_bucket\")\n  |> range(start: -24h)\n  |> filter(fn: (r) => r._measurement == \"temperature\")\n  |> sort(columns: [\"_time\"], desc: true)\n  |> limit(n: 25)"
        );
    }

    #[test]
    fn browse_measurement_flux_later_page_overfetches_and_tails() {
        let query =
            InfluxQueryGenerator::browse_measurement_flux("my_bucket", "temperature", 25, 50);

        assert!(
            query.ends_with("|> limit(n: 75)\n  |> tail(n: 25)"),
            "{query}"
        );
    }

    #[test]
    fn browse_measurement_escapes_names_for_each_language() {
        let influxql = InfluxQueryGenerator::browse_measurement_influxql("a\"b", 10, 0);
        let flux = InfluxQueryGenerator::browse_measurement_flux("b\\k", "a\"b", 10, 0);

        assert!(influxql.contains("FROM \"a\"\"b\""), "{influxql}");
        assert!(flux.contains("from(bucket: \"b\\\\k\")"), "{flux}");
        assert!(flux.contains("r._measurement == \"a\\\"b\""), "{flux}");
    }

    #[test]
    fn collection_browse_query_is_influxql_on_v1() {
        let generator =
            InfluxQueryGenerator::new(InfluxVersion::V1, QueryLanguage::InfluxQuery, None);

        let query = generator
            .collection_browse_query(&browse_request("metrics", "system", 100, 0))
            .expect("InfluxDB always describes its browse query");

        assert_eq!(query.language, QueryLanguage::InfluxQuery);
        assert_eq!(
            query.text,
            "SELECT * FROM \"system\" ORDER BY time DESC LIMIT 100 OFFSET 0"
        );
    }

    #[test]
    fn collection_browse_query_follows_the_v2_default_language() {
        let request = browse_request("metrics", "system", 100, 0);

        let influxql =
            InfluxQueryGenerator::new(InfluxVersion::V2, QueryLanguage::InfluxQuery, None)
                .collection_browse_query(&request)
                .expect("InfluxDB always describes its browse query");
        assert_eq!(influxql.language, QueryLanguage::InfluxQuery);
        assert!(
            influxql.text.starts_with("SELECT * FROM \"system\""),
            "{}",
            influxql.text
        );

        let flux = InfluxQueryGenerator::new(
            InfluxVersion::V2,
            QueryLanguage::Flux,
            Some("profile-default".to_string()),
        )
        .collection_browse_query(&request)
        .expect("InfluxDB always describes its browse query");
        assert_eq!(flux.language, QueryLanguage::Flux);
        assert!(
            flux.text.starts_with("from(bucket: \"metrics\")"),
            "the sidebar bucket must win over the profile default: {}",
            flux.text
        );
    }
}
