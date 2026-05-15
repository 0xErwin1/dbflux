//! InfluxDB query generator.
//!
//! Produces native InfluxQL and Flux query templates for use in the UI's
//! context menu ("SELECT *", "SHOW MEASUREMENTS", etc.) and MCP previews.

use dbflux_core::{
    CollectionTemplateRequest, GeneratedQuery, InfluxVersion, MutationCategory, MutationRequest,
    QueryGenerator, QueryLanguage, ReadTemplateRequest,
};

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
}
