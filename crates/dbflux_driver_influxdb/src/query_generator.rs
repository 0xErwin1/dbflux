//! InfluxDB query generator.
//!
//! Produces native InfluxQL and Flux query templates for use in the UI's
//! context menu ("SELECT *", "SHOW MEASUREMENTS", etc.) and MCP previews.

use dbflux_core::{
    GeneratedQuery, InfluxVersion, MutationCategory, MutationRequest, QueryGenerator,
    QueryLanguage, ReadTemplateRequest,
};

/// InfluxDB query generator — produces InfluxQL or Flux templates.
///
/// Query language is determined by the `version` field:
/// - V1 → InfluxQL only.
/// - V2 → InfluxQL and Flux depending on the requested language.
pub struct InfluxQueryGenerator {
    pub version: InfluxVersion,
    pub default_language: QueryLanguage,
    /// Bucket or database name, used in Flux `from(bucket: ...)` templates.
    pub bucket_or_db: String,
}

impl InfluxQueryGenerator {
    pub fn new(
        version: InfluxVersion,
        default_language: QueryLanguage,
        bucket_or_db: String,
    ) -> Self {
        Self {
            version,
            default_language,
            bucket_or_db,
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
                text: Self::select_all_flux(&self.bucket_or_db, measurement, 100),
            }),
            _ => Some(GeneratedQuery {
                language: QueryLanguage::InfluxQuery,
                text: Self::select_all_influxql(measurement, 100),
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
}
