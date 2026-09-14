use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    DatabaseCategory, DbConfig, DbDriver, DbError, DbKind, DeploymentClass, DriverCapabilities,
    DriverFormDef, DriverKey, DriverMetadata, FormFieldKind, FormSection, FormTab, FormValues,
    Icon, MutationRequest, OrderByMode, PaginationStyle, PlaceholderStyle, QueryCapabilities,
    QueryGenError, QueryGenerator, QueryLanguage, ReadTemplateRequest, SelectQuery,
    SqlMutationGenerator, SyntaxInfo, TransferFamily, VisualQuerySpec, WhereOperator, field,
    field_password, field_required, with_default, with_help,
};

use crate::connection::ClickHouseConnection;
use crate::dialect::CLICKHOUSE_DIALECT;
use crate::http::ClickHouseHttpClient;

const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
const MAX_REQUEST_TIMEOUT_SECONDS: u64 = i32::MAX as u64;

pub static METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "clickhouse".to_string(),
    display_name: "ClickHouse".to_string(),
    description: "Column-oriented analytical database over HTTP".to_string(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: Some(DeploymentClass::SelfHosted),
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::MULTIPLE_DATABASES.bits()
            | DriverCapabilities::SSL.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::VIEWS.bits()
            | DriverCapabilities::PAGINATION.bits()
            | DriverCapabilities::SORTING.bits()
            | DriverCapabilities::FILTERING.bits()
            | DriverCapabilities::CHART_AUTHORING.bits()
            | DriverCapabilities::EXPORT_CSV.bits()
            | DriverCapabilities::EXPORT_JSON.bits()
            | DriverCapabilities::INSTANCE_METRICS.bits()
            | DriverCapabilities::INSTANCE_INSPECTOR.bits(),
    ),
    default_port: Some(8123),
    uri_scheme: "http".to_string(),
    icon: Icon::Clickhouse,
    syntax: Some(SyntaxInfo {
        identifier_quote: '`',
        string_quote: '\'',
        placeholder_style: PlaceholderStyle::QuestionMark,
        supports_schemas: false,
        default_schema: None,
        case_sensitive_identifiers: true,
    }),
    query: Some(QueryCapabilities {
        pagination: vec![PaginationStyle::Offset],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::Like,
            WhereOperator::Null,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        order_by_mode: OrderByMode::AnyColumns,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: true,
        supports_limit: true,
        supports_offset: true,
        supports_joins: true,
        supports_subqueries: true,
        supports_union: true,
        supports_intersect: true,
        supports_except: true,
        supports_case_expressions: true,
        supports_window_functions: true,
        supports_ctes: true,
        supports_explain: true,
        max_query_parameters: 0,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: None,
    ddl: None,
    transactions: None,
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

pub static CLICKHOUSE_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".to_string(),
        label: "Main".to_string(),
        sections: vec![
            FormSection {
                title: "Server".to_string(),
                fields: vec![
                    with_default(
                        field_required(
                            "url",
                            "HTTP URL",
                            FormFieldKind::Text,
                            "http://localhost:8123",
                        ),
                        "http://localhost:8123",
                    ),
                    with_default(
                        field_required("database", "Database", FormFieldKind::Text, "default"),
                        "default",
                    ),
                    with_default(
                        field(
                            "request_timeout_seconds",
                            "Request Timeout (seconds)",
                            FormFieldKind::Number,
                            "30",
                        ),
                        "30",
                    ),
                ],
            },
            FormSection {
                title: "Authentication".to_string(),
                fields: vec![
                    with_default(
                        field_required("user", "User", FormFieldKind::Text, "default"),
                        "default",
                    ),
                    with_help(
                        field_password(),
                        "Stored securely; sent using HTTP Basic Auth",
                    ),
                ],
            },
        ],
    }],
});

pub struct ClickHouseDriver;

impl ClickHouseDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClickHouseDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for ClickHouseDriver {
    fn kind(&self) -> DbKind {
        DbKind::ClickHouse
    }

    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn form_definition(&self) -> &DriverFormDef {
        &CLICKHOUSE_FORM
    }

    fn driver_key(&self) -> DriverKey {
        "builtin:clickhouse".into()
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let url = required_value(values, "url", "HTTP URL")?;
        ClickHouseHttpClient::validate_endpoint(&url)
            .map_err(|error| DbError::InvalidProfile(error.to_string()))?;
        let user = required_value(values, "user", "User")?;
        let database = required_value(values, "database", "Database")?;
        let request_timeout_seconds = values
            .get("request_timeout_seconds")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| {
                value.parse::<u64>().map_err(|_| {
                    DbError::InvalidProfile("Request timeout must be a whole number".to_string())
                })
            })
            .transpose()?;
        if request_timeout_seconds == Some(0) {
            return Err(DbError::InvalidProfile(
                "Request timeout must be greater than zero".to_string(),
            ));
        }
        if request_timeout_seconds.is_some_and(|timeout| timeout > MAX_REQUEST_TIMEOUT_SECONDS) {
            return Err(DbError::InvalidProfile(format!(
                "Request timeout must not exceed {MAX_REQUEST_TIMEOUT_SECONDS} seconds"
            )));
        }
        Ok(DbConfig::ClickHouse {
            url,
            user,
            database,
            request_timeout_seconds,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();
        if let DbConfig::ClickHouse {
            url,
            user,
            database,
            request_timeout_seconds,
        } = config
        {
            values.insert("url".to_string(), url.clone());
            values.insert("user".to_string(), user.clone());
            values.insert("database".to_string(), database.clone());
            values.insert(
                "request_timeout_seconds".to_string(),
                request_timeout_seconds
                    .unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECONDS)
                    .to_string(),
            );
        }
        values
    }

    fn connect_with_secrets(
        &self,
        profile: &dbflux_core::ConnectionProfile,
        password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
        let DbConfig::ClickHouse {
            url,
            user,
            database,
            request_timeout_seconds,
        } = &profile.config
        else {
            return Err(DbError::InvalidProfile(
                "Expected ClickHouse configuration".to_string(),
            ));
        };
        let timeout = request_timeout_seconds.unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECONDS);
        if timeout == 0 || timeout > MAX_REQUEST_TIMEOUT_SECONDS {
            return Err(DbError::InvalidProfile(format!(
                "Request timeout must be between 1 and {MAX_REQUEST_TIMEOUT_SECONDS} seconds"
            )));
        }
        let client = ClickHouseHttpClient::new(
            url,
            user.clone(),
            password.cloned(),
            database.clone(),
            Duration::from_secs(timeout),
        )
        .map_err(|error| {
            crate::error_formatter::ClickHouseErrorFormatter::into_connection_error(&error)
        })?;
        let connection = ClickHouseConnection::new(client, database.clone());
        connection.validate_connection()?;
        Ok(Box::new(connection))
    }

    fn test_connection(&self, profile: &dbflux_core::ConnectionProfile) -> Result<(), DbError> {
        self.connect(profile).map(|_| ())
    }
}

fn required_value(values: &FormValues, key: &str, label: &str) -> Result<String, DbError> {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| DbError::InvalidProfile(format!("{label} is required")))
}

pub(crate) struct ReadOnlyClickHouseGenerator {
    inner: SqlMutationGenerator,
}

impl ReadOnlyClickHouseGenerator {
    const fn new() -> Self {
        Self {
            inner: SqlMutationGenerator::new(&CLICKHOUSE_DIALECT),
        }
    }
}

impl QueryGenerator for ReadOnlyClickHouseGenerator {
    fn supported_categories(&self) -> &'static [dbflux_core::MutationCategory] {
        &[]
    }

    fn generate_mutation(
        &self,
        _mutation: &MutationRequest,
    ) -> Option<dbflux_core::GeneratedQuery> {
        None
    }

    fn generate_read_template(
        &self,
        request: &ReadTemplateRequest<'_>,
    ) -> Option<dbflux_core::GeneratedQuery> {
        self.inner.generate_read_template(request)
    }

    fn generate_select(
        &self,
        spec: &VisualQuerySpec,
    ) -> Result<Option<SelectQuery>, QueryGenError> {
        self.inner.generate_select(spec).map(|query| {
            query.map(|query| SelectQuery {
                sql: query.materialize_for_editor(&CLICKHOUSE_DIALECT),
                params: Vec::new(),
            })
        })
    }

    fn materialize_select_for_editor(&self, query: &SelectQuery) -> String {
        query.sql.clone()
    }
}

pub(crate) static READ_ONLY_GENERATOR: ReadOnlyClickHouseGenerator =
    ReadOnlyClickHouseGenerator::new();

#[cfg(test)]
mod tests {
    use super::{ClickHouseDriver, MAX_REQUEST_TIMEOUT_SECONDS, METADATA, READ_ONLY_GENERATOR};
    use dbflux_core::{
        Comparator, DbConfig, DbDriver, DriverCapabilities, FilterNode, FormValues, LiteralValue,
        Predicate, PredicateValue, Projection, QueryGenerator, SourceTable, VisualQuerySpec,
    };

    #[test]
    fn metadata_is_conservative_and_has_no_mutation_contract() {
        for capability in [
            DriverCapabilities::INSERT,
            DriverCapabilities::UPDATE,
            DriverCapabilities::DELETE,
            DriverCapabilities::TRANSACTIONS,
            DriverCapabilities::PREPARED_STATEMENTS,
            DriverCapabilities::QUERY_CANCELLATION,
            DriverCapabilities::MULTI_STATEMENT,
        ] {
            assert!(!METADATA.capabilities.contains(capability));
        }
        assert!(METADATA.mutation.is_none());
        assert!(METADATA.transactions.is_none());
        assert!(
            METADATA
                .capabilities
                .contains(DriverCapabilities::CHART_AUTHORING)
        );
    }

    #[test]
    fn config_round_trips_without_password() {
        let driver = ClickHouseDriver::new();
        let values = FormValues::from([
            ("url".to_string(), "https://ch.example.com".to_string()),
            ("user".to_string(), "analytics".to_string()),
            ("database".to_string(), "events".to_string()),
            ("request_timeout_seconds".to_string(), "45".to_string()),
        ]);
        let config = driver.build_config(&values).expect("valid config");
        let DbConfig::ClickHouse {
            request_timeout_seconds,
            ..
        } = &config
        else {
            panic!("expected ClickHouse config");
        };
        assert_eq!(*request_timeout_seconds, Some(45));
        let extracted = driver.extract_values(&config);
        assert!(!extracted.contains_key("password"));
        assert_eq!(
            extracted.get("database").map(String::as_str),
            Some("events")
        );
    }

    #[test]
    fn rejects_zero_timeout() {
        let driver = ClickHouseDriver::new();
        let values = FormValues::from([
            ("url".to_string(), "http://localhost:8123".to_string()),
            ("user".to_string(), "default".to_string()),
            ("database".to_string(), "default".to_string()),
            ("request_timeout_seconds".to_string(), "0".to_string()),
        ]);
        assert!(driver.build_config(&values).is_err());
    }

    #[test]
    fn rejects_unsafe_endpoint_and_unpersistable_timeout() {
        let driver = ClickHouseDriver::new();
        let mut values = FormValues::from([
            (
                "url".to_string(),
                "https://user:secret@clickhouse.example.com".to_string(),
            ),
            ("user".to_string(), "default".to_string()),
            ("database".to_string(), "default".to_string()),
            ("request_timeout_seconds".to_string(), "30".to_string()),
        ]);
        assert!(driver.build_config(&values).is_err());

        values.insert(
            "url".to_string(),
            "https://clickhouse.example.com".to_string(),
        );
        values.insert(
            "request_timeout_seconds".to_string(),
            (MAX_REQUEST_TIMEOUT_SECONDS + 1).to_string(),
        );
        assert!(driver.build_config(&values).is_err());
    }

    #[test]
    fn visual_select_inlines_escaped_values_without_params() {
        let spec = VisualQuerySpec {
            source: SourceTable {
                schema: Some("analytics".to_string()),
                table: "events".to_string(),
                alias: "events".to_string(),
            },
            projection: Projection::All,
            joins: Vec::new(),
            filter: Some(FilterNode::Predicate(Predicate {
                source_alias: "events".to_string(),
                column: "name".to_string(),
                comparator: Comparator::Eq,
                value: PredicateValue::Single(LiteralValue::Text("O'Reilly\\docs".to_string())),
                node_id: 0,
            })),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            having: None,
            sort: Vec::new(),
            limit: Some(10),
            offset: 0,
        };

        let query = READ_ONLY_GENERATOR
            .generate_select(&spec)
            .expect("valid spec")
            .expect("SELECT supported");
        assert!(query.params.is_empty());
        assert!(query.sql.contains("'O\\'Reilly\\\\docs'"));
        assert!(!query.sql.contains('?'));
    }
}
