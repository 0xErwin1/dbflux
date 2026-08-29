use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Instant;

use std::sync::Arc;

use redis::cluster_routing::{RoutingInfo, SingleNodeRoutingInfo};

use dbflux_core::secrecy::{ExposeSecret, SecretString};

use crate::language_service::RedisLanguageService;
use dbflux_core::{
    ColumnKind, ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionExt, ConnectionProfile,
    DatabaseCategory, DatabaseInfo, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo,
    DdlCapabilities, DefaultSqlDialect, DeploymentClass, DiagnosticSeverity, DocumentConnection,
    DriverCapabilities, DriverFormDef, DriverLimits, DriverMetadata, EditorDiagnostic,
    ExecutionSourceContext, FormFieldDef, FormFieldKind, FormSection, FormTab, FormValues,
    FormattedError, HashDeleteRequest, HashSetRequest, Icon, InstanceCatalog, KeyBulkGetRequest,
    KeyDeleteRequest, KeyEntry, KeyExistsRequest, KeyExpireRequest, KeyGetRequest, KeyGetResult,
    KeyLoadState, KeyPersistRequest, KeyRenameRequest, KeyScanPage, KeyScanRequest, KeySetRequest,
    KeySpaceInfo, KeyTtlRequest, KeyType, KeyTypeRequest, KeyValueApi, KeyValueConnection,
    KeyValueSchema, LanguageService, ListEnd, ListPushRequest, ListRemoveRequest, ListSetRequest,
    MutationCapabilities, OrderByColumn, PaginationStyle, QueryCapabilities, QueryErrorFormatter,
    QueryGenerator, QueryHandle, QueryLanguage, QueryRequest, QueryResult, RelationalConnection,
    SchemaDropTarget, SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan, SemanticRequest,
    SetAddRequest, SetCondition, SetRemoveRequest, SqlDialect, SshTunnelConfig, StreamAddRequest,
    StreamDeleteRequest, StreamEntryId, TextPosition, TextPositionRange, TransactionCapabilities,
    TransferFamily, Value, ValueRepr, ZSetAddRequest, ZSetRemoveRequest, field, field_password,
    field_required, field_use_uri, sanitize_uri, ssh_tab, when_checked, when_unchecked,
    with_default,
};
use dbflux_ssh::SshTunnel;

use crate::transport::{
    ClusterScanCursor, ConfiguredTopology, MasterRoleSanity, RedisTransport, RoleClassification,
    TopologyProbe, classify_role_reply, evaluate_master_role_sanity, is_connection_level_error,
    parse_cluster_enabled, parse_cluster_slots_masters, parse_configured_topology, parse_node_list,
    split_host_port, validate_cluster_database,
};
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};

pub static REDIS_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![
        FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![
                FormSection {
                    title: "Server".into(),
                    fields: vec![
                        field_use_uri(),
                        when_checked(
                            field_required(
                                "uri",
                                "Connection URI",
                                FormFieldKind::Text,
                                "redis://localhost:6379/0",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("host", "Host", FormFieldKind::Text, "localhost"),
                                "localhost",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("port", "Port", FormFieldKind::Number, "6379"),
                                "6379",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field("database", "Database Index", FormFieldKind::Number, "0"),
                                "0",
                            ),
                            "use_uri",
                        ),
                    ],
                },
                FormSection {
                    title: "Authentication".into(),
                    fields: vec![
                        when_unchecked(
                            field("user", "User", FormFieldKind::Text, "optional"),
                            "use_uri",
                        ),
                        field_password(),
                    ],
                },
            ],
        },
        ssh_tab(),
    ],
});

fn plan_redis_mutation(mutation: &dbflux_core::MutationRequest) -> Result<SemanticPlan, DbError> {
    static GENERATOR: crate::command_generator::RedisCommandGenerator =
        crate::command_generator::RedisCommandGenerator;

    GENERATOR.plan_mutation(mutation).ok_or_else(|| {
        DbError::NotSupported("Redis semantic planning does not support this mutation".into())
    })
}

fn plan_redis_semantic_request(request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
    match request {
        SemanticRequest::Mutation(mutation) => plan_redis_mutation(mutation),
        SemanticRequest::TableBrowse(_)
        | SemanticRequest::TableCount(_)
        | SemanticRequest::Aggregate(_)
        | SemanticRequest::CollectionBrowse(_)
        | SemanticRequest::CollectionCount(_) => Err(DbError::NotSupported(
            "Redis semantic planning does not support relational or collection browse/count requests"
                .into(),
        )),
        SemanticRequest::Explain(_) | SemanticRequest::Describe(_) => Err(DbError::NotSupported(
            "Redis semantic planning does not support explain or describe requests".into(),
        )),
    }
}

/// Redis driver metadata.
pub static REDIS_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "redis".into(),
    display_name: "Redis".into(),
    description: "In-memory key-value database".into(),
    category: DatabaseCategory::KeyValue,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: Some(DeploymentClass::SelfHosted),
    query_language: QueryLanguage::RedisCommands,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::KEYVALUE_BASE.bits()
            | DriverCapabilities::MULTIPLE_DATABASES.bits()
            | DriverCapabilities::KV_TTL.bits()
            | DriverCapabilities::KV_KEY_TYPES.bits()
            | DriverCapabilities::KV_VALUE_SIZE.bits()
            | DriverCapabilities::KV_RENAME.bits()
            | DriverCapabilities::KV_BULK_GET.bits()
            | DriverCapabilities::KV_STREAM_RANGE.bits()
            | DriverCapabilities::KV_STREAM_ADD.bits()
            | DriverCapabilities::KV_STREAM_DELETE.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::SSH_TUNNEL.bits()
            | DriverCapabilities::SSL.bits()
            | DriverCapabilities::INSTANCE_METRICS.bits()
            | DriverCapabilities::INSTANCE_INSPECTOR.bits()
            | DriverCapabilities::CHART_AUTHORING.bits(),
    ),
    default_port: Some(6379),
    uri_scheme: "redis".into(),
    icon: Icon::Redis,
    syntax: None,
    query: Some(QueryCapabilities {
        pagination: vec![PaginationStyle::Cursor],
        where_operators: vec![],
        supports_order_by: false,
        order_by_mode: dbflux_core::OrderByMode::None,
        supports_group_by: false,
        supports_having: false,
        supports_distinct: false,
        supports_limit: false,
        supports_offset: false,
        supports_joins: false,
        supports_subqueries: false,
        supports_union: false,
        supports_intersect: false,
        supports_except: false,
        supports_case_expressions: false,
        supports_window_functions: false,
        supports_ctes: false,
        supports_explain: false,
        max_query_parameters: 0,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: Some(MutationCapabilities {
        supports_insert: true,
        supports_update: true,
        supports_delete: true,
        supports_upsert: false,
        supports_returning: false,
        supports_batch: true,
        supports_bulk_update: false,
        supports_bulk_delete: true,
        max_insert_values: 0,
    }),
    ddl: Some(DdlCapabilities {
        supports_create_database: false,
        supports_drop_database: false,
        supports_create_table: false,
        supports_drop_table: false,
        supports_alter_table: false,
        supports_create_index: false,
        supports_drop_index: false,
        supports_create_view: false,
        supports_drop_view: false,
        supports_create_trigger: false,
        supports_drop_trigger: false,
        transactional_ddl: false,
        supports_add_column: false,
        supports_drop_column: false,
        supports_rename_column: false,
        supports_alter_column: false,
        supports_add_constraint: false,
        supports_drop_constraint: false,
    }),
    transactions: Some(TransactionCapabilities {
        supports_transactions: true,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: false,
        supports_deferrable: false,
    }),
    limits: Some(DriverLimits {
        max_query_length: 0,
        max_parameters: 0,
        max_result_rows: 0,
        max_connections: 0,
        max_nested_subqueries: 0,
        max_identifier_length: 0,
        max_columns: 0,
        max_indexes_per_table: 0,
        max_bulk_insert_rows: 0,
    }),
    ssl_modes: Some(&[
        dbflux_core::SslModeOption {
            id: "off",
            label: "off",
        },
        dbflux_core::SslModeOption {
            id: "on",
            label: "on",
        },
        dbflux_core::SslModeOption {
            id: "verify",
            label: "verify",
        },
    ]),
    ssl_cert_fields: Some(dbflux_core::SslCertFields {
        root_cert: true,
        client_cert: true,
    }),
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

pub struct RedisDriver;

impl RedisDriver {
    pub fn new() -> Self {
        Self
    }

    fn connect_direct(
        &self,
        params: DirectConnectParams<'_>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let tls = redis_ssl_mode_to_config(
            params.ssl_mode,
            params.ssl_root_cert_path,
            params.ssl_client_cert_path,
            params.ssl_client_key_path,
        )
        .map_err(DbError::InvalidProfile)?;

        // Build the base URI according to the selected TLS mode. The Redis crate
        // accepts the `#insecure` fragment to skip certificate verification when
        // using `rediss://`. For `verify` mode we feed PEM bytes through
        // `Client::build_with_tls` so the rustls config trusts the supplied root
        // CA and / or sends the client certificate.
        let scheme = if matches!(tls, RedisTlsConfig::Plain) {
            "redis"
        } else {
            "rediss"
        };
        let mut uri = format!("{}://{}:{}/", scheme, params.host, params.port);
        if matches!(tls, RedisTlsConfig::TlsInsecure) {
            uri.push_str("#insecure");
        }

        if params.topology == ConfiguredTopology::Sentinel {
            let sentinel_nodes = build_sentinel_node_uris(&uri, params.additional_nodes);

            return connect_sentinel_master(
                sentinel_nodes,
                params.sentinel_master_name,
                params.user,
                params.password,
                params.database,
                params.ssh_tunnel,
                |e| format_redis_error(e, params.host, params.port),
            );
        }

        if params.topology == ConfiguredTopology::Cluster {
            validate_cluster_database(params.database)?;

            let mut cluster_connection = build_cluster_connection(
                &uri,
                params.additional_nodes,
                &tls,
                params.user,
                params.password,
            )
            .map_err(|e| format_redis_error(&e, params.host, params.port))?;

            set_client_name(&mut cluster_connection);

            redis::cmd("PING")
                .query::<String>(&mut cluster_connection)
                .map_err(|e| format_redis_error(&e, params.host, params.port))?;

            return Ok(Box::new(RedisConnection {
                connection: Arc::new(Mutex::new(RedisTransport::Cluster(Box::new(
                    cluster_connection,
                )))),
                active_database: Mutex::new(None),
                _ssh_tunnel: params.ssh_tunnel,
            }));
        }

        let client = match &tls {
            RedisTlsConfig::Plain | RedisTlsConfig::TlsInsecure => {
                redis::Client::open(uri.as_str())
                    .map_err(|e| format_redis_error(&e, params.host, params.port))?
            }
            RedisTlsConfig::TlsVerify(certs) => redis::Client::build_with_tls(
                uri.as_str(),
                redis::TlsCertificates {
                    client_tls: certs.client_tls.clone(),
                    root_cert: certs.root_cert.clone(),
                },
            )
            .map_err(|e| format_redis_error(&e, params.host, params.port))?,
        };

        let mut connection = client
            .get_connection()
            .map_err(|e| format_redis_error(&e, params.host, params.port))?;

        authenticate(&mut connection, params.user, params.password)
            .map_err(|e| format_redis_error(&e, params.host, params.port))?;

        match detect_topology(&mut connection)
            .map_err(|e| format_redis_error(&e, params.host, params.port))?
        {
            TopologyProbe::Standalone => {
                if let Some(db) = params.database {
                    select_db(&mut connection, db)
                        .map_err(|e| format_redis_error(&e, params.host, params.port))?;
                }

                set_client_name(&mut connection);

                redis::cmd("PING")
                    .query::<String>(&mut connection)
                    .map_err(|e| format_redis_error(&e, params.host, params.port))?;

                Ok(Box::new(RedisConnection {
                    connection: Arc::new(Mutex::new(RedisTransport::Standalone(Arc::new(
                        Mutex::new(connection),
                    )))),
                    active_database: Mutex::new(params.database),
                    _ssh_tunnel: params.ssh_tunnel,
                }))
            }
            TopologyProbe::Cluster => {
                validate_cluster_database(params.database)?;

                let mut cluster_connection =
                    build_cluster_connection(&uri, &[], &tls, params.user, params.password)
                        .map_err(|e| format_redis_error(&e, params.host, params.port))?;

                set_client_name(&mut cluster_connection);

                redis::cmd("PING")
                    .query::<String>(&mut cluster_connection)
                    .map_err(|e| format_redis_error(&e, params.host, params.port))?;

                Ok(Box::new(RedisConnection {
                    connection: Arc::new(Mutex::new(RedisTransport::Cluster(Box::new(
                        cluster_connection,
                    )))),
                    active_database: Mutex::new(None),
                    _ssh_tunnel: params.ssh_tunnel,
                }))
            }
            TopologyProbe::SentinelService => Err(unconfigured_sentinel_topology()),
        }
    }

    fn connect_with_uri(
        &self,
        params: UriConnectParams<'_>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let UriConnectParams {
            uri,
            user,
            password,
            database,
            topology,
            sentinel_master_name,
            additional_nodes,
        } = params;

        let has_credentials = uri_authority_has_credentials(uri);

        if topology == ConfiguredTopology::Sentinel {
            let sentinel_nodes = build_sentinel_node_uris(uri, additional_nodes);
            let sentinel_password = if has_credentials { None } else { password };
            let sentinel_user = if has_credentials { None } else { user };

            return connect_sentinel_master(
                sentinel_nodes,
                sentinel_master_name,
                sentinel_user,
                sentinel_password,
                database,
                None,
                |e| format_redis_uri_error(e, uri),
            );
        }

        if topology == ConfiguredTopology::Cluster {
            validate_cluster_database(database)?;

            // URI mode carries no separate TLS cert configuration; the
            // scheme (`redis://` / `rediss://`) alone decides TLS, same
            // as the plain `Client::open(uri)` call above.
            let cluster_password = if has_credentials { None } else { password };
            let cluster_user = if has_credentials { None } else { user };

            let mut cluster_connection = build_cluster_connection(
                uri,
                additional_nodes,
                &RedisTlsConfig::Plain,
                cluster_user,
                cluster_password,
            )
            .map_err(|e| format_redis_uri_error(&e, uri))?;

            set_client_name(&mut cluster_connection);

            redis::cmd("PING")
                .query::<String>(&mut cluster_connection)
                .map_err(|e| format_redis_uri_error(&e, uri))?;

            return Ok(Box::new(RedisConnection {
                connection: Arc::new(Mutex::new(RedisTransport::Cluster(Box::new(
                    cluster_connection,
                )))),
                active_database: Mutex::new(None),
                _ssh_tunnel: None,
            }));
        }

        let client = redis::Client::open(uri).map_err(|e| format_redis_uri_error(&e, uri))?;
        let mut connection = client
            .get_connection()
            .map_err(|e| format_redis_uri_error(&e, uri))?;

        if !has_credentials {
            authenticate(&mut connection, user, password)
                .map_err(|e| format_redis_uri_error(&e, uri))?;
        }

        match detect_topology(&mut connection).map_err(|e| format_redis_uri_error(&e, uri))? {
            TopologyProbe::Standalone => {
                if let Some(db) = database {
                    select_db(&mut connection, db).map_err(|e| format_redis_uri_error(&e, uri))?;
                }

                set_client_name(&mut connection);

                redis::cmd("PING")
                    .query::<String>(&mut connection)
                    .map_err(|e| format_redis_uri_error(&e, uri))?;

                Ok(Box::new(RedisConnection {
                    connection: Arc::new(Mutex::new(RedisTransport::Standalone(Arc::new(
                        Mutex::new(connection),
                    )))),
                    active_database: Mutex::new(database),
                    _ssh_tunnel: None,
                }))
            }
            TopologyProbe::Cluster => {
                validate_cluster_database(database)?;

                // URI mode carries no separate TLS cert configuration; the
                // scheme (`redis://` / `rediss://`) alone decides TLS, same
                // as the plain `Client::open(uri)` call above.
                let cluster_password = if has_credentials { None } else { password };
                let cluster_user = if has_credentials { None } else { user };

                let mut cluster_connection = build_cluster_connection(
                    uri,
                    &[],
                    &RedisTlsConfig::Plain,
                    cluster_user,
                    cluster_password,
                )
                .map_err(|e| format_redis_uri_error(&e, uri))?;

                set_client_name(&mut cluster_connection);

                redis::cmd("PING")
                    .query::<String>(&mut cluster_connection)
                    .map_err(|e| format_redis_uri_error(&e, uri))?;

                Ok(Box::new(RedisConnection {
                    connection: Arc::new(Mutex::new(RedisTransport::Cluster(Box::new(
                        cluster_connection,
                    )))),
                    active_database: Mutex::new(None),
                    _ssh_tunnel: None,
                }))
            }
            TopologyProbe::SentinelService => Err(unconfigured_sentinel_topology()),
        }
    }

    fn connect_via_ssh_tunnel(
        &self,
        tunnel_config: &SshTunnelConfig,
        config: &ExtractedRedisConfig,
        ssh_secret: Option<&str>,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let ssh_session = dbflux_ssh::establish_session(tunnel_config, ssh_secret)?;
        let tunnel = SshTunnel::start(ssh_session, config.host.clone(), config.port)?;
        let local_port = tunnel.local_port();

        self.connect_direct(DirectConnectParams {
            host: "127.0.0.1",
            port: local_port,
            ssl_mode: config.ssl_mode.as_deref(),
            ssl_root_cert_path: config.ssl_root_cert_path.as_deref(),
            ssl_client_cert_path: config.ssl_client_cert_path.as_deref(),
            ssl_client_key_path: config.ssl_client_key_path.as_deref(),
            user: config.user.as_deref(),
            password,
            database: config.database,
            topology: config.topology,
            sentinel_master_name: config.sentinel_master_name.as_deref(),
            additional_nodes: &config.additional_nodes,
            ssh_tunnel: Some(tunnel),
        })
    }
}

impl Default for RedisDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for RedisDriver {
    fn kind(&self) -> DbKind {
        DbKind::Redis
    }

    fn metadata(&self) -> &DriverMetadata {
        &REDIS_METADATA
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:redis".into()
    }

    fn settings_schema(&self) -> Option<Arc<DriverFormDef>> {
        Some(Arc::new(DriverFormDef {
            tabs: vec![FormTab {
                id: "settings".into(),
                label: "Settings".into(),
                sections: vec![
                    FormSection {
                        title: "Key Scanning".into(),
                        fields: vec![
                            FormFieldDef {
                                id: "scan_batch_size".into(),
                                label: "Scan batch size".into(),
                                kind: FormFieldKind::Number,
                                placeholder: "100".into(),
                                required: false,
                                default_value: "100".into(),
                                enabled_when_checked: None,
                                enabled_when_unchecked: None,
                                disabled_when_field_set: None,
                                help: None,
                            },
                            FormFieldDef {
                                id: "stream_preview_limit".into(),
                                label: "Stream preview limit".into(),
                                kind: FormFieldKind::Number,
                                placeholder: "50".into(),
                                required: false,
                                default_value: "50".into(),
                                enabled_when_checked: None,
                                enabled_when_unchecked: None,
                                disabled_when_field_set: None,
                                help: None,
                            },
                        ],
                    },
                    FormSection {
                        title: "Safety".into(),
                        fields: vec![FormFieldDef {
                            id: "allow_flush".into(),
                            label: "Allow FLUSHALL / FLUSHDB".into(),
                            kind: FormFieldKind::Checkbox,
                            placeholder: String::new(),
                            required: false,
                            default_value: "false".into(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                            disabled_when_field_set: None,
                            help: None,
                        }],
                    },
                ],
            }],
        }))
    }

    fn form_definition(&self) -> &DriverFormDef {
        &REDIS_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let use_uri = values.get("use_uri").map(|s| s == "true").unwrap_or(false);
        let uri = values.get("uri").filter(|s| !s.is_empty()).cloned();
        let user = values.get("user").filter(|s| !s.is_empty()).cloned();
        let database = values
            .get("database")
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<u32>())
            .transpose()
            .map_err(|_| DbError::InvalidProfile("Invalid database index".to_string()))?;

        if use_uri {
            if uri.is_none() {
                return Err(DbError::InvalidProfile(
                    "Connection URI is required when using URI mode".to_string(),
                ));
            }

            return Ok(DbConfig::Redis {
                use_uri,
                uri,
                host: String::new(),
                port: 6379,
                user,
                database,
                tls: false,
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
                // Batch 4 wires topology/sentinel_master_name/additional_nodes form
                // fields into `build_config`; until then every profile built through
                // the form is standalone-with-detection.
                topology: None,
                sentinel_master_name: None,
                additional_nodes: None,
            });
        }

        let host = values
            .get("host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Host is required".to_string()))?
            .clone();
        let port = values
            .get("port")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Port is required".to_string()))?
            .parse::<u16>()
            .map_err(|_| DbError::InvalidProfile("Invalid port number".to_string()))?;

        Ok(DbConfig::Redis {
            use_uri,
            uri: None,
            host,
            port,
            user,
            database,
            tls: false,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: None,
            sentinel_master_name: None,
            additional_nodes: None,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::Redis {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            ..
        } = config
        {
            values.insert(
                "use_uri".to_string(),
                if *use_uri { "true" } else { "" }.to_string(),
            );
            values.insert("uri".to_string(), uri.clone().unwrap_or_default());
            values.insert("host".to_string(), host.clone());
            values.insert("port".to_string(), port.to_string());
            values.insert("user".to_string(), user.clone().unwrap_or_default());
            values.insert(
                "database".to_string(),
                database.map(|d| d.to_string()).unwrap_or_default(),
            );
        }

        values
    }

    fn build_uri(&self, values: &FormValues, password: &str) -> Option<String> {
        let host = values
            .get("host")
            .map(String::as_str)
            .unwrap_or("localhost");
        let port = values.get("port").map(String::as_str).unwrap_or("6379");
        let user = values.get("user").map(String::as_str).unwrap_or("");
        let db_index = values.get("database").map(String::as_str).unwrap_or("");

        // The form no longer carries a `tls` checkbox — TLS is determined by the
        // driver-level SSL mode selection (see `connect_with_secrets`). When
        // building a preview URI we default to plain `redis://`.
        let scheme = "redis";
        let auth = if !user.is_empty() {
            if password.is_empty() {
                format!("{}@", urlencoding::encode(user))
            } else {
                format!(
                    "{}:{}@",
                    urlencoding::encode(user),
                    urlencoding::encode(password)
                )
            }
        } else if !password.is_empty() {
            format!(":{}@", urlencoding::encode(password))
        } else {
            String::new()
        };

        let path = if db_index.is_empty() {
            String::new()
        } else {
            format!("/{}", db_index)
        };

        Some(format!("{}://{}{}:{}{}", scheme, auth, host, port, path))
    }

    fn parse_uri(&self, uri: &str) -> Option<FormValues> {
        fn decode_lossy(s: &str) -> String {
            urlencoding::decode(s)
                .map(std::borrow::Cow::into_owned)
                .unwrap_or_else(|_| s.to_string())
        }

        let (scheme, rest) = uri.split_once("://")?;
        if scheme != "redis" && scheme != "rediss" {
            return None;
        }

        let mut values = HashMap::new();
        values.insert("use_uri".to_string(), "true".to_string());
        values.insert("uri".to_string(), uri.to_string());

        let (authority, path) = match rest.split_once('/') {
            Some((a, p)) => (a, p),
            None => (rest, ""),
        };

        let host_port = if let Some((auth, hp)) = authority.rsplit_once('@') {
            if let Some((user, pass)) = auth.split_once(':') {
                if !user.is_empty() {
                    values.insert("user".to_string(), decode_lossy(user));
                }
                if !pass.is_empty() {
                    values.insert("password".to_string(), decode_lossy(pass));
                }
            } else if !auth.is_empty() {
                values.insert("user".to_string(), decode_lossy(auth));
            }
            hp
        } else {
            authority
        };

        if let Some((host, port)) = host_port.rsplit_once(':') {
            values.insert("host".to_string(), host.to_string());
            values.insert("port".to_string(), port.to_string());
        } else {
            values.insert("host".to_string(), host_port.to_string());
            values.insert("port".to_string(), "6379".to_string());
        }

        let db = path.split('/').next().unwrap_or_default();
        if !db.is_empty() {
            values.insert("database".to_string(), db.to_string());
        }

        Some(values)
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_redis_config(&profile.config)?;

        let password = password.map(|value| value.expose_secret());
        let ssh_secret = ssh_secret.map(|value| value.expose_secret());

        if config.use_uri {
            if config.ssh_tunnel.is_some() {
                return Err(DbError::InvalidProfile(
                    "SSH tunnel is not supported when URI mode is enabled for Redis".to_string(),
                ));
            }

            return self.connect_with_uri(UriConnectParams {
                uri: config.uri.as_deref().unwrap_or_default(),
                user: config.user.as_deref(),
                password,
                database: config.database,
                topology: config.topology,
                sentinel_master_name: config.sentinel_master_name.as_deref(),
                additional_nodes: &config.additional_nodes,
            });
        }

        if let Some(tunnel_config) = config.ssh_tunnel.as_ref() {
            self.connect_via_ssh_tunnel(tunnel_config, &config, ssh_secret, password)
        } else {
            self.connect_direct(DirectConnectParams {
                host: &config.host,
                port: config.port,
                ssl_mode: config.ssl_mode.as_deref(),
                ssl_root_cert_path: config.ssl_root_cert_path.as_deref(),
                ssl_client_cert_path: config.ssl_client_cert_path.as_deref(),
                ssl_client_key_path: config.ssl_client_key_path.as_deref(),
                user: config.user.as_deref(),
                password,
                database: config.database,
                topology: config.topology,
                sentinel_master_name: config.sentinel_master_name.as_deref(),
                additional_nodes: &config.additional_nodes,
                ssh_tunnel: None,
            })
        }
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let conn = self.connect_with_secrets(profile, None, None)?;
        conn.ping()
    }
}

#[derive(Debug)]
struct ExtractedRedisConfig {
    use_uri: bool,
    uri: Option<String>,
    host: String,
    port: u16,
    user: Option<String>,
    database: Option<u32>,
    ssl_mode: Option<String>,
    ssl_root_cert_path: Option<String>,
    ssl_client_cert_path: Option<String>,
    ssl_client_key_path: Option<String>,
    ssh_tunnel: Option<SshTunnelConfig>,
    topology: ConfiguredTopology,
    sentinel_master_name: Option<String>,
    additional_nodes: Vec<(String, u16)>,
}

struct DirectConnectParams<'a> {
    host: &'a str,
    port: u16,
    ssl_mode: Option<&'a str>,
    ssl_root_cert_path: Option<&'a str>,
    ssl_client_cert_path: Option<&'a str>,
    ssl_client_key_path: Option<&'a str>,
    user: Option<&'a str>,
    password: Option<&'a str>,
    database: Option<u32>,
    topology: ConfiguredTopology,
    sentinel_master_name: Option<&'a str>,
    additional_nodes: &'a [(String, u16)],
    ssh_tunnel: Option<SshTunnel>,
}

struct UriConnectParams<'a> {
    uri: &'a str,
    user: Option<&'a str>,
    password: Option<&'a str>,
    database: Option<u32>,
    topology: ConfiguredTopology,
    sentinel_master_name: Option<&'a str>,
    additional_nodes: &'a [(String, u16)],
}

fn extract_redis_config(config: &DbConfig) -> Result<ExtractedRedisConfig, DbError> {
    match config {
        DbConfig::Redis {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            tls,
            ssl_mode,
            ssl_root_cert_path,
            ssl_client_cert_path,
            ssl_client_key_path,
            ssh_tunnel,
            topology,
            sentinel_master_name,
            additional_nodes,
            ..
        } => {
            // Migrate the legacy boolean `tls` flag: when older saves don't carry
            // an `ssl_mode`, fall back to the previous TLS-without-verification
            // semantics (`tls=true` → `"on"`, `tls=false` → `"off"`).
            let resolved_ssl_mode = match ssl_mode.clone() {
                Some(mode) => Some(mode),
                None => Some(if *tls {
                    "on".to_string()
                } else {
                    "off".to_string()
                }),
            };

            let configured_topology = parse_configured_topology(topology.as_deref())?;

            let sentinel_master_name = sentinel_master_name
                .clone()
                .filter(|value| !value.trim().is_empty());

            if configured_topology == ConfiguredTopology::Sentinel && sentinel_master_name.is_none()
            {
                return Err(DbError::InvalidProfile(
                    "Redis Sentinel topology requires a master/service name".to_string(),
                ));
            }

            let additional_nodes = additional_nodes
                .as_deref()
                .map(parse_node_list)
                .transpose()?
                .unwrap_or_default();

            Ok(ExtractedRedisConfig {
                use_uri: *use_uri,
                uri: uri.clone(),
                host: host.clone(),
                port: *port,
                user: user.clone(),
                database: *database,
                ssl_mode: resolved_ssl_mode,
                ssl_root_cert_path: ssl_root_cert_path.clone(),
                ssl_client_cert_path: ssl_client_cert_path.clone(),
                ssl_client_key_path: ssl_client_key_path.clone(),
                ssh_tunnel: ssh_tunnel.clone(),
                topology: configured_topology,
                sentinel_master_name,
                additional_nodes,
            })
        }
        _ => Err(DbError::InvalidProfile(
            "Expected Redis configuration".to_string(),
        )),
    }
}

/// Native TLS configuration derived from an SSL mode id and optional cert paths.
///
/// Returned by `redis_ssl_mode_to_config` and consumed by `connect_direct`.
enum RedisTlsConfig {
    /// Plain TCP (`redis://`). Used when the mode is `"off"`.
    Plain,
    /// TLS without certificate verification (`rediss://...#insecure`). Used when
    /// the mode is `"on"`.
    TlsInsecure,
    /// TLS with verification, optionally with a custom root CA and / or mTLS.
    TlsVerify(RedisTlsCerts),
}

impl std::fmt::Debug for RedisTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisTlsConfig::Plain => f.write_str("Plain"),
            RedisTlsConfig::TlsInsecure => f.write_str("TlsInsecure"),
            RedisTlsConfig::TlsVerify(_) => f.write_str("TlsVerify"),
        }
    }
}

#[derive(Default)]
struct RedisTlsCerts {
    client_tls: Option<redis::ClientTlsConfig>,
    root_cert: Option<Vec<u8>>,
}

/// Maps a Redis driver SSL mode id (`"off"` / `"on"` / `"verify"`) to a native
/// `RedisTlsConfig`. Optional certificate paths are loaded from disk when the
/// mode requires them.
///
/// Returns `Err` with a static message for unknown mode ids or unreadable cert
/// files.
fn redis_ssl_mode_to_config(
    mode_id: Option<&str>,
    root_cert_path: Option<&str>,
    client_cert_path: Option<&str>,
    client_key_path: Option<&str>,
) -> Result<RedisTlsConfig, String> {
    let mode = mode_id.unwrap_or("off");

    match mode {
        "" | "off" => Ok(RedisTlsConfig::Plain),
        "on" => Ok(RedisTlsConfig::TlsInsecure),
        "verify" => {
            let root_cert =
                match root_cert_path.and_then(non_empty) {
                    Some(path) => Some(std::fs::read(path).map_err(|e| {
                        format!("Failed to read Redis root CA at '{}': {}", path, e)
                    })?),
                    None => None,
                };

            let client_tls = match (
                client_cert_path.and_then(non_empty),
                client_key_path.and_then(non_empty),
            ) {
                (Some(cert), Some(key)) => {
                    let client_cert = std::fs::read(cert).map_err(|e| {
                        format!("Failed to read Redis client cert at '{}': {}", cert, e)
                    })?;
                    let client_key = std::fs::read(key).map_err(|e| {
                        format!("Failed to read Redis client key at '{}': {}", key, e)
                    })?;
                    Some(redis::ClientTlsConfig {
                        client_cert,
                        client_key,
                    })
                }
                (None, None) => None,
                _ => {
                    return Err(
                        "Redis mTLS requires both a client cert and a client key".to_string()
                    );
                }
            };

            Ok(RedisTlsConfig::TlsVerify(RedisTlsCerts {
                client_tls,
                root_cert,
            }))
        }
        other => Err(format!("Unknown Redis SSL mode: '{}'", other)),
    }
}

fn non_empty(s: &str) -> Option<&str> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

pub struct RedisConnection {
    connection: Arc<Mutex<RedisTransport>>,
    active_database: Mutex<Option<u32>>,
    _ssh_tunnel: Option<SshTunnel>,
}

impl RedisConnection {
    fn active_db_index(&self) -> Result<Option<u32>, DbError> {
        self.active_database
            .lock()
            .map(|db| *db)
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))
    }

    fn set_active_db_index(&self, database: Option<u32>) -> Result<(), DbError> {
        let mut active = self
            .active_database
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;
        *active = database;
        Ok(())
    }

    /// `f` must be callable more than once (`Fn`, not `FnOnce`) so that
    /// `RedisTransport::with_connection_like` can retry it once against a
    /// freshly re-resolved Sentinel master after a connection-class failure.
    fn with_connection<T>(
        &self,
        keyspace: Option<u32>,
        f: impl Fn(&mut dyn redis::ConnectionLike) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let mut transport = self
            .connection
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let active = self.active_db_index()?;
        let target_db = keyspace.or(active);

        if matches!(&*transport, RedisTransport::Cluster(_)) {
            validate_cluster_database(target_db)?;
        }

        transport.with_connection_like(|conn| {
            if let Some(db) = target_db {
                select_db(conn, db).map_err(|e| format_redis_query_error(&e))?;
            }

            let result = f(conn);

            // Restore the active database if we temporarily switched to a different one
            if keyspace.is_some()
                && keyspace != active
                && let Some(db) = active
            {
                let _ = select_db(conn, db);
            }

            result
        })
    }

    /// Fans a key scan out across every Redis Cluster master, since a plain
    /// `SCAN` is unroutable against a `ClusterConnection` (redis-rs has no
    /// single-node "current" concept for it, unlike single-key commands).
    ///
    /// The page budget (`request.limit`, defaulting to 100) is split evenly
    /// across the masters still pending this round, rounded up so every
    /// node gets at least one slot per page. Each node's returned cursor is
    /// tracked independently and round-tripped via `ClusterScanCursor`; the
    /// overall scan is exhausted once every node has reported cursor 0.
    fn scan_keys_cluster(&self, request: &KeyScanRequest) -> Result<KeyScanPage, DbError> {
        validate_cluster_database(request.keyspace)?;

        let mut transport = self
            .connection
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        let RedisTransport::Cluster(conn) = &mut *transport else {
            return Err(DbError::query_failed(
                "Redis Cluster scan requested on a non-cluster connection".to_string(),
            ));
        };
        let conn: &mut redis::cluster::ClusterConnection = conn.as_mut();

        let mut cursor_state = match request.cursor.as_deref() {
            None => ClusterScanCursor::fresh(&fetch_cluster_masters(conn)?),
            Some(raw) => {
                let mut decoded = ClusterScanCursor::decode(raw)?;
                let masters = fetch_cluster_masters(conn)?;
                let known: HashSet<String> = masters
                    .iter()
                    .map(|(host, port)| format!("{host}:{port}"))
                    .collect();
                decoded.retain_known_nodes(&known);
                decoded
            }
        };

        if cursor_state.is_exhausted() {
            return Ok(KeyScanPage {
                entries: Vec::new(),
                next_cursor: None,
            });
        }

        let total_limit = if request.limit == 0 {
            100
        } else {
            request.limit
        } as usize;
        let addresses = cursor_state.addresses();
        let per_node_count = total_limit.div_ceil(addresses.len()).max(1) as u32;

        let mut entries = Vec::new();

        for address in &addresses {
            let (host, port) = split_host_port(address)?;
            let node_cursor = cursor_state.cursor_for(address);

            let mut command = redis::cmd("SCAN");
            command.arg(node_cursor);

            if let Some(filter) = request.filter.as_ref()
                && !filter.is_empty()
            {
                command.arg("MATCH").arg(filter);
            }

            command.arg("COUNT").arg(per_node_count);

            let value = conn
                .route_command(
                    &command,
                    RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress { host, port }),
                )
                .map_err(|e| format_redis_query_error(&e))?;

            let (next_cursor, keys): (u64, Vec<String>) =
                redis::FromRedisValue::from_redis_value(&value)
                    .map_err(|e| format_redis_query_error(&e))?;

            cursor_state.record_result(address, next_cursor);

            // Each key routes to its own node by slot through `ConnectionLike`
            // dispatch regardless of which master's SCAN page produced it, so
            // this works unchanged from the standalone TYPE lookup below.
            for key in keys {
                let type_name = redis::cmd("TYPE")
                    .arg(&key)
                    .query::<String>(conn)
                    .map_err(|e| format_redis_query_error(&e))?;

                entries.push(KeyEntry {
                    key,
                    key_type: Some(parse_key_type(&type_name)),
                    ttl_seconds: None,
                    size_bytes: None,
                });
            }
        }

        Ok(KeyScanPage {
            entries,
            next_cursor: cursor_state.encode(),
        })
    }
}

impl Connection for RedisConnection {
    fn metadata(&self) -> &DriverMetadata {
        &REDIS_METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        self.with_connection(None, |conn| {
            redis::cmd("PING")
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(())
        })
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn instance_catalog(&self) -> Option<Box<dyn InstanceCatalog>> {
        let transport = self.connection.lock().ok()?;
        let standalone = transport.standalone()?;

        Some(Box::new(
            crate::instance_catalog::RedisInstanceCatalog::new_probed(standalone),
        ))
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        if let Some(source) = req
            .execution_context
            .as_ref()
            .and_then(|ctx| ctx.source.as_ref())
        {
            match source {
                ExecutionSourceContext::InstanceMetricQuery { metric_id, .. } => {
                    let transport = self.connection.lock().map_err(|_| {
                        DbError::QueryFailed("redis connection mutex poisoned".to_string().into())
                    })?;
                    let standalone = transport.standalone().ok_or_else(|| {
                        DbError::NotSupported(
                            "Instance metrics are not supported for Redis Cluster connections"
                                .to_string(),
                        )
                    })?;
                    let mut conn = standalone.lock().map_err(|_| {
                        DbError::QueryFailed("redis connection mutex poisoned".to_string().into())
                    })?;
                    return crate::instance_catalog::dispatch_metric_series(&mut conn, metric_id);
                }
                ExecutionSourceContext::InstanceInspectorQuery { metric_id } => {
                    let transport = self.connection.lock().map_err(|_| {
                        DbError::QueryFailed("redis connection mutex poisoned".to_string().into())
                    })?;
                    let standalone = transport.standalone().ok_or_else(|| {
                        DbError::NotSupported(
                            "Instance inspectors are not supported for Redis Cluster connections"
                                .to_string(),
                        )
                    })?;
                    let mut conn = standalone.lock().map_err(|_| {
                        DbError::QueryFailed("redis connection mutex poisoned".to_string().into())
                    })?;
                    return crate::instance_catalog::dispatch_inspector_snapshot(
                        &mut conn, metric_id,
                    );
                }
                _ => {}
            }
        }

        let start = Instant::now();
        let parts = parse_command(req.sql.trim())?;

        if parts.is_empty() {
            return Ok(QueryResult::empty());
        }

        let query_db = req
            .database
            .as_deref()
            .map(parse_database_name)
            .transpose()?;

        let value = self.with_connection(query_db, |conn| {
            // Invariant: parts is non-empty — guarded by `if parts.is_empty()` above.
            #[allow(clippy::indexing_slicing)]
            let mut command = redis::cmd(&parts[0]);
            for arg in parts.iter().skip(1) {
                command.arg(arg);
            }

            command
                .query::<redis::Value>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })?;

        Ok(redis_value_to_result(value, start.elapsed()))
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation not supported for Redis".to_string(),
        ))
    }

    fn language_service(&self) -> &dyn LanguageService {
        &RedisLanguageService
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        {
            let mut transport = self
                .connection
                .lock()
                .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

            if let RedisTransport::Cluster(conn) = &mut *transport {
                let conn: &mut redis::cluster::ClusterConnection = conn.as_mut();
                let masters = fetch_cluster_masters(conn)?;
                let stats = fetch_cluster_keyspace_stats(conn, &masters)?;

                return Ok(SchemaSnapshot::key_value(KeyValueSchema {
                    keyspaces: vec![KeySpaceInfo {
                        db_index: 0,
                        key_count: Some(stats.key_count),
                        memory_bytes: None,
                        avg_ttl_seconds: stats.avg_ttl_seconds,
                    }],
                    current_keyspace: Some(0),
                }));
            }
        }

        self.with_connection(None, |conn| {
            let current_db = self.active_db_index()?.unwrap_or(0);
            let keyspace_stats = fetch_keyspace_stats(conn)?;
            let db_count = fetch_database_count(conn).ok().unwrap_or_else(|| {
                keyspace_stats
                    .keys()
                    .copied()
                    .max()
                    .map(|max| max + 1)
                    .unwrap_or(current_db + 1)
            });

            let keyspaces = (0..db_count)
                .map(|db_index| {
                    let stats = keyspace_stats.get(&db_index);
                    KeySpaceInfo {
                        db_index,
                        key_count: stats.map(|s| s.key_count),
                        memory_bytes: None,
                        avg_ttl_seconds: stats.and_then(|s| s.avg_ttl_seconds),
                    }
                })
                .collect();

            Ok(SchemaSnapshot::key_value(KeyValueSchema {
                keyspaces,
                current_keyspace: Some(current_db),
            }))
        })
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let schema = self.schema()?;
        let current = schema.as_key_value().and_then(|s| s.current_keyspace);

        Ok(schema
            .keyspaces()
            .iter()
            .map(|space| DatabaseInfo {
                name: format!("db{}", space.db_index),
                is_current: Some(space.db_index) == current,
            })
            .collect())
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        let db_index = parse_database_name(database)?;

        self.with_connection(Some(db_index), |conn| {
            redis::cmd("DBSIZE")
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            Ok(DbSchemaInfo {
                name: database.to_string(),
                tables: Vec::new(),
                views: Vec::new(),
                custom_types: None,
            })
        })
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        let target = database.map(parse_database_name).transpose()?;

        let mut transport = self
            .connection
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        if let Some(db) = target {
            if matches!(&*transport, RedisTransport::Cluster(_)) {
                validate_cluster_database(Some(db))?;
            }

            transport.with_connection_like(|conn| {
                select_db(conn, db).map_err(|e| format_redis_query_error(&e))
            })?;
        }

        drop(transport);
        self.set_active_db_index(target)
    }

    fn active_database(&self) -> Option<String> {
        self.active_db_index()
            .ok()
            .flatten()
            .map(|db| format!("db{}", db))
    }

    fn drop_schema_object(
        &self,
        target: &SchemaDropTarget,
        _cascade: bool,
        _if_exists: bool,
    ) -> Result<(), DbError> {
        Err(DbError::NotSupported(format!(
            "Redis does not support dropping schema objects via drop_schema_object (requested {:?} '{}')",
            target.kind, target.name
        )))
    }

    fn kind(&self) -> DbKind {
        DbKind::Redis
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::LazyPerDatabase
    }

    fn key_value_api(&self) -> Option<&dyn KeyValueApi> {
        Some(self)
    }

    fn dialect(&self) -> &dyn SqlDialect {
        static DIALECT: DefaultSqlDialect = DefaultSqlDialect;
        &DIALECT
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        static GENERATOR: crate::command_generator::RedisCommandGenerator =
            crate::command_generator::RedisCommandGenerator;
        Some(&GENERATOR)
    }

    fn plan_semantic_request(&self, request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
        plan_redis_semantic_request(request)
    }

    fn build_select_sql(
        &self,
        _table: &str,
        _columns: &[String],
        _filter: Option<&Value>,
        _order_by: &[OrderByColumn],
        _limit: u32,
        _offset: u32,
    ) -> String {
        // Redis doesn't use SQL - this is for SQL-based drivers
        "SELECT * FROM key WHERE filter LIMIT offset".to_string()
    }

    fn build_insert_sql(
        &self,
        _table: &str,
        _columns: &[String],
        _values: &[Value],
    ) -> (String, Vec<Value>) {
        // Redis doesn't use SQL - this is for SQL-based drivers
        ("SET key value".to_string(), Vec::new())
    }

    fn build_update_sql(
        &self,
        _table: &str,
        _set: &[(String, Value)],
        _filter: Option<&Value>,
    ) -> (String, Vec<Value>) {
        // Redis doesn't use SQL - this is for SQL-based drivers
        ("SET key value".to_string(), Vec::new())
    }

    fn build_delete_sql(&self, _table: &str, _filter: Option<&Value>) -> (String, Vec<Value>) {
        // Redis doesn't use SQL - this is for SQL-based drivers
        ("DEL key".to_string(), Vec::new())
    }

    fn build_upsert_sql(
        &self,
        _table: &str,
        _columns: &[String],
        _values: &[Value],
        _conflict_columns: &[String],
        _update_columns: &[String],
    ) -> (String, Vec<Value>) {
        // Redis doesn't use SQL - this is for SQL-based drivers
        ("SET key value".to_string(), Vec::new())
    }

    fn build_count_sql(&self, _table: &str, _filter: Option<&Value>) -> String {
        // Redis doesn't use SQL - this is for SQL-based drivers
        "DBSIZE".to_string()
    }

    fn build_truncate_sql(&self, _table: &str) -> String {
        // Redis doesn't support TRUNCATE - use FLUSHDB with caution
        "FLUSHDB".to_string()
    }

    fn build_drop_index_sql(
        &self,
        _index_name: &str,
        _table_name: Option<&str>,
        _if_exists: bool,
    ) -> String {
        // Redis doesn't have named indexes like SQL databases
        "DROP INDEX not_applicable".to_string()
    }

    fn version_query(&self) -> &'static str {
        // Redis uses INFO command, not SQL
        "INFO server | grep redis_version"
    }

    fn supports_transactional_ddl(&self) -> bool {
        false
    }

    fn translate_filter(&self, _filter: &Value) -> Result<String, DbError> {
        // Redis doesn't use SQL WHERE clauses
        Err(DbError::NotSupported(
            "translate_filter is not applicable to Redis - it uses key-based access and commands, not SQL".to_string(),
        ))
    }
}

impl KeyValueConnection for RedisConnection {}

impl ConnectionExt for RedisConnection {
    fn as_relational(&self) -> Option<&dyn RelationalConnection> {
        None
    }

    fn as_document(&self) -> Option<&dyn DocumentConnection> {
        None
    }

    fn as_keyvalue(&self) -> Option<&dyn KeyValueConnection> {
        Some(self)
    }
}

impl KeyValueApi for RedisConnection {
    fn scan_keys(&self, request: &KeyScanRequest) -> Result<KeyScanPage, DbError> {
        let is_cluster = {
            let transport = self
                .connection
                .lock()
                .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;
            matches!(&*transport, RedisTransport::Cluster(_))
        };

        if is_cluster {
            return self.scan_keys_cluster(request);
        }

        let cursor = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<u64>()
            .map_err(|_| DbError::InvalidProfile("Invalid key scan cursor".to_string()))?;

        let count = if request.limit == 0 {
            100
        } else {
            request.limit
        };

        self.with_connection(request.keyspace, |conn| {
            let mut command = redis::cmd("SCAN");
            command.arg(cursor);

            if let Some(filter) = request.filter.as_ref()
                && !filter.is_empty()
            {
                command.arg("MATCH").arg(filter);
            }

            command.arg("COUNT").arg(count);

            let (next_cursor, keys): (u64, Vec<String>) = command
                .query(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let entries = keys
                .into_iter()
                .map(|key| {
                    let type_name = redis::cmd("TYPE")
                        .arg(&key)
                        .query::<String>(conn)
                        .map_err(|e| format_redis_query_error(&e))?;

                    Ok(KeyEntry {
                        key,
                        key_type: Some(parse_key_type(&type_name)),
                        ttl_seconds: None,
                        size_bytes: None,
                    })
                })
                .collect::<Result<Vec<_>, DbError>>()?;

            let next_cursor = if next_cursor == 0 {
                None
            } else {
                Some(next_cursor.to_string())
            };

            Ok(KeyScanPage {
                entries,
                next_cursor,
            })
        })
    }

    fn get_key(&self, request: &KeyGetRequest) -> Result<KeyGetResult, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let key_type_name = redis::cmd("TYPE")
                .arg(&request.key)
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let key_type = parse_key_type(&key_type_name);
            if key_type == KeyType::Unknown && key_type_name.eq_ignore_ascii_case("none") {
                return Err(DbError::object_not_found(format!(
                    "Key '{}' not found",
                    request.key
                )));
            }

            let (value, repr, load_state) =
                fetch_key_payload(conn, &request.key, key_type, request.max_value_bytes)?;

            let ttl_seconds = if request.include_ttl {
                let ttl = redis::cmd("TTL")
                    .arg(&request.key)
                    .query::<i64>(conn)
                    .map_err(|e| format_redis_query_error(&e))?;

                if ttl >= 0 { Some(ttl) } else { None }
            } else {
                None
            };

            let key_type = normalize_key_type_for_payload(key_type, repr);

            // `size_bytes` reflects the true value size even when gated: the
            // `TooLarge` STRLEN probe already knows it without fetching.
            let size_bytes = match load_state {
                KeyLoadState::TooLarge { size_bytes, .. } => Some(size_bytes),
                _ => Some(value.len() as u64),
            };

            let entry = KeyEntry {
                key: request.key.clone(),
                key_type: if request.include_type {
                    Some(key_type)
                } else {
                    None
                },
                ttl_seconds,
                size_bytes: if request.include_size {
                    size_bytes
                } else {
                    None
                },
            };

            Ok(KeyGetResult {
                entry,
                value,
                repr,
                load_state,
            })
        })
    }

    fn set_key(&self, request: &KeySetRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut command = redis::cmd("SET");
            command.arg(&request.key).arg(&request.value);

            if let Some(ttl_seconds) = request.ttl_seconds {
                command.arg("EX").arg(ttl_seconds);
            }

            match request.condition {
                SetCondition::Always => {}
                SetCondition::IfNotExists => {
                    command.arg("NX");
                }
                SetCondition::IfExists => {
                    command.arg("XX");
                }
            }

            let response = command
                .query::<Option<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            if response.is_none() {
                return Err(DbError::query_failed(
                    "SET condition was not satisfied".to_string(),
                ));
            }

            Ok(())
        })
    }

    fn delete_key(&self, request: &KeyDeleteRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let deleted = redis::cmd("DEL")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(deleted > 0)
        })
    }

    fn exists_key(&self, request: &KeyExistsRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let exists = redis::cmd("EXISTS")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(exists > 0)
        })
    }

    fn key_type(&self, request: &KeyTypeRequest) -> Result<KeyType, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let type_name = redis::cmd("TYPE")
                .arg(&request.key)
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let key_type = parse_key_type(&type_name);
            if key_type == KeyType::Unknown && type_name.eq_ignore_ascii_case("none") {
                return Err(DbError::object_not_found(format!(
                    "Key '{}' not found",
                    request.key
                )));
            }

            Ok(key_type)
        })
    }

    fn key_ttl(&self, request: &KeyTtlRequest) -> Result<Option<i64>, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let ttl = redis::cmd("TTL")
                .arg(&request.key)
                .query::<i64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            if ttl == -2 {
                return Err(DbError::object_not_found(format!(
                    "Key '{}' not found",
                    request.key
                )));
            }

            if ttl < 0 { Ok(None) } else { Ok(Some(ttl)) }
        })
    }

    fn expire_key(&self, request: &KeyExpireRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let changed = redis::cmd("EXPIRE")
                .arg(&request.key)
                .arg(request.ttl_seconds)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(changed > 0)
        })
    }

    fn persist_key(&self, request: &KeyPersistRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let changed = redis::cmd("PERSIST")
                .arg(&request.key)
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(changed > 0)
        })
    }

    fn rename_key(&self, request: &KeyRenameRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            redis::cmd("RENAME")
                .arg(&request.from_key)
                .arg(&request.to_key)
                .query::<String>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(())
        })
    }

    fn bulk_get(&self, request: &KeyBulkGetRequest) -> Result<Vec<Option<KeyGetResult>>, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut values = Vec::with_capacity(request.keys.len());

            for key in &request.keys {
                let type_name = redis::cmd("TYPE")
                    .arg(key)
                    .query::<String>(conn)
                    .map_err(|e| format_redis_query_error(&e))?;

                let key_type = parse_key_type(&type_name);
                if key_type == KeyType::Unknown && type_name.eq_ignore_ascii_case("none") {
                    values.push(None);
                    continue;
                }

                // Bulk fetches carry no per-key byte budget, so the whole
                // payload is always transferred here.
                let (payload, repr, load_state) =
                    if matches!(key_type, KeyType::String | KeyType::Json | KeyType::Unknown) {
                        let fetched = redis::cmd("GET")
                            .arg(key)
                            .query::<Option<Vec<u8>>>(conn)
                            .map_err(|e| format_redis_query_error(&e))?;

                        match fetched {
                            Some(v) => {
                                let repr = detect_value_repr(&v);
                                (v, repr, KeyLoadState::Loaded)
                            }
                            None => {
                                values.push(None);
                                continue;
                            }
                        }
                    } else {
                        fetch_key_payload(conn, key, key_type, None)?
                    };

                let ttl_seconds = if request.include_ttl {
                    let ttl = redis::cmd("TTL")
                        .arg(key)
                        .query::<i64>(conn)
                        .map_err(|e| format_redis_query_error(&e))?;

                    if ttl >= 0 { Some(ttl) } else { None }
                } else {
                    None
                };

                let key_type = normalize_key_type_for_payload(key_type, repr);

                values.push(Some(KeyGetResult {
                    entry: KeyEntry {
                        key: key.clone(),
                        key_type: if request.include_type {
                            Some(key_type)
                        } else {
                            None
                        },
                        ttl_seconds,
                        size_bytes: if request.include_size {
                            Some(payload.len() as u64)
                        } else {
                            None
                        },
                    },
                    value: payload,
                    repr,
                    load_state,
                }));
            }

            Ok(values)
        })
    }

    // -- Hash member operations --

    fn hash_set(&self, request: &HashSetRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("HSET");
            cmd.arg(&request.key);
            for (field, value) in &request.fields {
                cmd.arg(field).arg(value);
            }
            cmd.query::<()>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })
    }

    fn hash_delete(&self, request: &HashDeleteRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("HDEL");
            cmd.arg(&request.key);
            for field in &request.fields {
                cmd.arg(field);
            }
            let removed = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- List member operations --

    fn list_set(&self, request: &ListSetRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            redis::cmd("LSET")
                .arg(&request.key)
                .arg(request.index)
                .arg(&request.value)
                .query::<()>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })
    }

    fn list_push(&self, request: &ListPushRequest) -> Result<(), DbError> {
        self.with_connection(request.keyspace, |conn| {
            let cmd_name = match request.end {
                ListEnd::Head => "LPUSH",
                ListEnd::Tail => "RPUSH",
            };

            let mut cmd = redis::cmd(cmd_name);
            cmd.arg(&request.key);
            for value in &request.values {
                cmd.arg(value);
            }
            cmd.query::<()>(conn)
                .map_err(|e| format_redis_query_error(&e))
        })
    }

    fn list_remove(&self, request: &ListRemoveRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let removed = redis::cmd("LREM")
                .arg(&request.key)
                .arg(request.count)
                .arg(&request.value)
                .query::<i64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- Set member operations --

    fn set_add(&self, request: &SetAddRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("SADD");
            cmd.arg(&request.key);
            for member in &request.members {
                cmd.arg(member);
            }
            let added = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(added > 0)
        })
    }

    fn set_remove(&self, request: &SetRemoveRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("SREM");
            cmd.arg(&request.key);
            for member in &request.members {
                cmd.arg(member);
            }
            let removed = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- Sorted Set member operations --

    fn zset_add(&self, request: &ZSetAddRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("ZADD");
            cmd.arg(&request.key);
            for (member, score) in &request.members {
                cmd.arg(*score).arg(member);
            }
            let added = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(added > 0)
        })
    }

    fn zset_remove(&self, request: &ZSetRemoveRequest) -> Result<bool, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("ZREM");
            cmd.arg(&request.key);
            for member in &request.members {
                cmd.arg(member);
            }
            let removed = cmd
                .query::<u64>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok(removed > 0)
        })
    }

    // -- Stream operations --

    fn stream_add(&self, request: &StreamAddRequest) -> Result<String, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("XADD");
            cmd.arg(&request.key);

            if let Some(maxlen) = &request.maxlen {
                cmd.arg("MAXLEN");
                if maxlen.approximate {
                    cmd.arg("~");
                }
                cmd.arg(maxlen.count);
            }

            match &request.id {
                StreamEntryId::Auto => {
                    cmd.arg("*");
                }
                StreamEntryId::Explicit(id) => {
                    cmd.arg(id);
                }
            }

            for (field, value) in &request.fields {
                cmd.arg(field).arg(value);
            }

            let entry_id: String = cmd.query(conn).map_err(|e| format_redis_query_error(&e))?;

            Ok(entry_id)
        })
    }

    fn stream_delete(&self, request: &StreamDeleteRequest) -> Result<u64, DbError> {
        self.with_connection(request.keyspace, |conn| {
            let mut cmd = redis::cmd("XDEL");
            cmd.arg(&request.key);

            for id in &request.ids {
                cmd.arg(id);
            }

            let deleted: u64 = cmd.query(conn).map_err(|e| format_redis_query_error(&e))?;

            Ok(deleted)
        })
    }
}

struct RedisErrorFormatter;

impl RedisErrorFormatter {
    fn format_connection_message(source: &str, host: &str, port: u16) -> String {
        let lower = source.to_ascii_lowercase();

        if lower.contains("connection refused") {
            format!("Connection refused. Is Redis running at {}:{}?", host, port)
        } else if lower.contains("timed out") {
            "Connection timed out".to_string()
        } else if lower.contains("noauth") || lower.contains("wrongpass") {
            "Authentication failed. Check credentials.".to_string()
        } else {
            source.to_string()
        }
    }
}

impl QueryErrorFormatter for RedisErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        FormattedError::new(error.to_string())
    }
}

impl ConnectionErrorFormatter for RedisErrorFormatter {
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        host: &str,
        port: u16,
    ) -> FormattedError {
        let source = error.to_string();
        let message = Self::format_connection_message(&source, host, port);
        FormattedError::new(message)
    }

    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError {
        let source = error.to_string();
        let lower = source.to_ascii_lowercase();

        if lower.contains("connection refused") {
            return FormattedError::new(format!(
                "Connection refused. Check URI: {}",
                sanitized_uri
            ));
        }

        if lower.contains("noauth") || lower.contains("wrongpass") {
            return FormattedError::new("Authentication failed. Check credentials.");
        }

        if lower.contains("timed out") {
            return FormattedError::new("Connection timed out");
        }

        FormattedError::new(source)
    }
}

// -- Redis Value → QueryResult --

fn redis_value_to_result(value: redis::Value, execution_time: std::time::Duration) -> QueryResult {
    match value {
        redis::Value::Nil => QueryResult::text("(nil)".to_string(), execution_time),

        redis::Value::Int(i) => QueryResult::text(format!("(integer) {}", i), execution_time),

        redis::Value::BulkString(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(s) => QueryResult::text(s, execution_time),
            Err(_) => QueryResult::binary(bytes, execution_time),
        },

        redis::Value::SimpleString(s) => QueryResult::text(s, execution_time),

        redis::Value::Array(items) => redis_array_to_result(items, execution_time),

        redis::Value::Map(entries) => {
            let mut lines = Vec::with_capacity(entries.len());
            for (k, v) in entries {
                let key_str = redis_value_to_display(&k);
                let val_str = redis_value_to_display(&v);
                lines.push(format!("{}: {}", key_str, val_str));
            }
            QueryResult::text(lines.join("\n"), execution_time)
        }

        redis::Value::Boolean(b) => QueryResult::text(
            if b { "(true)" } else { "(false)" }.to_string(),
            execution_time,
        ),

        redis::Value::Double(f) => QueryResult::text(format!("(double) {}", f), execution_time),

        redis::Value::BigNumber(n) => {
            QueryResult::text(format!("(big number) {}", n), execution_time)
        }

        redis::Value::VerbatimString { format: _, text } => QueryResult::text(text, execution_time),

        redis::Value::Set(items) => redis_array_to_result(items, execution_time),

        redis::Value::Okay => QueryResult::text("OK".to_string(), execution_time),

        redis::Value::ServerError(e) => QueryResult::text(
            format!("(error) {}", e.details().unwrap_or("unknown")),
            execution_time,
        ),

        redis::Value::Push { kind: _, data } => redis_array_to_result(data, execution_time),

        redis::Value::Attribute {
            data,
            attributes: _,
        } => redis_value_to_result(*data, execution_time),
    }
}

/// Try to present a redis array as a table (if elements are uniform key-value pairs)
/// or fall back to numbered text lines.
fn redis_array_to_result(
    items: Vec<redis::Value>,
    execution_time: std::time::Duration,
) -> QueryResult {
    if items.is_empty() {
        return QueryResult::text("(empty array)".to_string(), execution_time);
    }

    // Check if all items are simple scalars → table with index + value columns
    let all_scalar = items.iter().all(|v| {
        matches!(
            v,
            redis::Value::Int(_)
                | redis::Value::BulkString(_)
                | redis::Value::SimpleString(_)
                | redis::Value::Nil
                | redis::Value::Boolean(_)
                | redis::Value::Double(_)
                | redis::Value::Okay
        )
    });

    if all_scalar {
        let columns = vec![
            ColumnMeta {
                name: "#".to_string(),
                type_name: "int".to_string(),
                kind: ColumnKind::Integer,
                nullable: false,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "value".to_string(),
                type_name: "redis".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
        ];

        let rows: Vec<Vec<Value>> = items
            .into_iter()
            .enumerate()
            .map(|(i, v)| vec![Value::Int(i as i64), redis_scalar_to_value(v)])
            .collect();

        return QueryResult::table(columns, rows, None, execution_time);
    }

    // Fallback: numbered text dump
    let lines: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, v)| format!("{}) {}", i + 1, redis_value_to_display(v)))
        .collect();

    QueryResult::text(lines.join("\n"), execution_time)
}

fn redis_scalar_to_value(v: redis::Value) -> Value {
    match v {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(i) => Value::Int(i),
        redis::Value::BulkString(bytes) => match String::from_utf8(bytes) {
            Ok(s) => Value::Text(s),
            Err(e) => Value::Bytes(e.into_bytes()),
        },
        redis::Value::SimpleString(s) => Value::Text(s),
        redis::Value::Boolean(b) => Value::Bool(b),
        redis::Value::Double(f) => Value::Float(f),
        redis::Value::Okay => Value::Text("OK".to_string()),
        _ => Value::Text(redis_value_to_display(&v)),
    }
}

fn redis_value_to_display(v: &redis::Value) -> String {
    match v {
        redis::Value::Nil => "(nil)".to_string(),
        redis::Value::Int(i) => i.to_string(),
        redis::Value::BulkString(bytes) => {
            String::from_utf8(bytes.clone()).unwrap_or_else(|_| format!("<{} bytes>", bytes.len()))
        }
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Array(items) | redis::Value::Set(items) => {
            let inner: Vec<String> = items.iter().map(redis_value_to_display).collect();
            format!("[{}]", inner.join(", "))
        }
        redis::Value::Map(entries) => {
            let inner: Vec<String> = entries
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: {}",
                        redis_value_to_display(k),
                        redis_value_to_display(v)
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
        redis::Value::Boolean(b) => b.to_string(),
        redis::Value::Double(f) => f.to_string(),
        redis::Value::BigNumber(n) => n.to_string(),
        redis::Value::VerbatimString { text, .. } => text.clone(),
        redis::Value::Okay => "OK".to_string(),
        redis::Value::ServerError(e) => {
            format!("ERR {}", e.details().unwrap_or("unknown"))
        }
        redis::Value::Push { data, .. } => {
            let inner: Vec<String> = data.iter().map(redis_value_to_display).collect();
            format!("PUSH[{}]", inner.join(", "))
        }
        redis::Value::Attribute { data, .. } => redis_value_to_display(data),
    }
}

static REDIS_ERROR_FORMATTER: RedisErrorFormatter = RedisErrorFormatter;

fn format_redis_error(error: &redis::RedisError, host: &str, port: u16) -> DbError {
    let formatted = REDIS_ERROR_FORMATTER.format_connection_error(error, host, port);
    formatted.into_connection_error()
}

fn format_redis_uri_error(error: &redis::RedisError, uri: &str) -> DbError {
    let sanitized = sanitize_uri(uri);
    let formatted = REDIS_ERROR_FORMATTER.format_uri_error(error, &sanitized);
    formatted.into_connection_error()
}

/// Formats a query-time `redis::RedisError` into a `DbError`.
///
/// A connection-class error (see `is_connection_level_error`) is classified as
/// `DbError::ConnectionFailed` rather than `DbError::QueryFailed`. This is what
/// lets `RedisTransport::with_connection_like`'s Sentinel failover retry
/// (`should_retry_sentinel_command`) recognize it without needing the raw
/// `redis::RedisError` at that choke point — command closures already convert
/// to `DbError` via this function before it is reached.
fn format_redis_query_error(error: &redis::RedisError) -> DbError {
    let formatted = REDIS_ERROR_FORMATTER.format_query_error(error);

    if is_connection_level_error(error) {
        formatted.into_connection_error()
    } else {
        formatted.into_query_error()
    }
}

fn authenticate(
    conn: &mut redis::Connection,
    user: Option<&str>,
    password: Option<&str>,
) -> redis::RedisResult<()> {
    if let Some(password) = password {
        let mut command = redis::cmd("AUTH");
        if let Some(user) = user
            && !user.is_empty()
        {
            command.arg(user);
        }
        command.arg(password);
        command.query::<String>(conn)?;
    }

    Ok(())
}

fn select_db(conn: &mut dyn redis::ConnectionLike, db_index: u32) -> redis::RedisResult<()> {
    redis::cmd("SELECT").arg(db_index).query::<String>(conn)?;
    Ok(())
}

/// Detects the topology of the server behind `conn` via `ROLE` and, when
/// that does not identify a Sentinel, `INFO cluster`.
///
/// Called once at connect time, before the connection is wrapped in a
/// `RedisTransport`. Only a genuine transport/IO error on either probe
/// aborts detection; a `ROLE` reply that some managed providers reject as an
/// unsupported command is treated as "not a Sentinel" (see
/// `classify_role_reply`) and detection falls through to `INFO cluster`.
fn detect_topology(conn: &mut redis::Connection) -> redis::RedisResult<TopologyProbe> {
    let role_reply = redis::cmd("ROLE").query::<redis::Role>(conn);

    match (classify_role_reply(&role_reply), role_reply) {
        (RoleClassification::Sentinel, _) => Ok(TopologyProbe::SentinelService),
        (RoleClassification::Aborted, Err(error)) => Err(error),
        // Unreachable in practice: `classify_role_reply` only returns `Aborted`
        // from the `Err(_)` arm. Fall through to the INFO check defensively
        // instead of panicking on a value that cannot occur.
        (RoleClassification::Aborted, Ok(_)) | (RoleClassification::NotSentinel, _) => {
            detect_topology_via_cluster_info(conn)
        }
    }
}

fn detect_topology_via_cluster_info(
    conn: &mut redis::Connection,
) -> redis::RedisResult<TopologyProbe> {
    let info: String = redis::cmd("INFO").arg("cluster").query(conn)?;

    Ok(if parse_cluster_enabled(&info) {
        TopologyProbe::Cluster
    } else {
        TopologyProbe::Standalone
    })
}

/// The user pointed a standalone or auto-detecting connection at a Redis
/// Sentinel node. Detection alone cannot proceed (a Sentinel has no data of
/// its own), so this tells the user exactly what to configure instead of the
/// batch 2 placeholder ("select the Sentinel topology mode... once Sentinel
/// support ships").
fn unconfigured_sentinel_topology() -> DbError {
    DbError::NotSupported(
        "dbflux detected a Redis Sentinel deployment at this address. Set this connection's \
         topology to Sentinel and provide the Sentinel master/service name to connect through it."
            .to_string(),
    )
}

/// Builds a live `ClusterConnection` from a seed node URI plus any additional
/// configured seed nodes.
///
/// `ClusterClient` only needs one reachable node to discover the rest of the
/// topology via `CLUSTER SLOTS`, so `additional_nodes` is a resilience
/// improvement (tolerating the primary node being down at connect time)
/// rather than a correctness requirement. TLS mode is derived by the builder
/// from `node_uri`'s scheme (and `#insecure` fragment); `additional_nodes` are
/// always plain `redis://` — configuring per-node TLS for extra seeds is not
/// supported by this batch (see the driver README).
fn build_cluster_connection(
    node_uri: &str,
    additional_nodes: &[(String, u16)],
    tls: &RedisTlsConfig,
    user: Option<&str>,
    password: Option<&str>,
) -> redis::RedisResult<redis::cluster::ClusterConnection> {
    let mut nodes = vec![node_uri.to_string()];
    nodes.extend(
        additional_nodes
            .iter()
            .map(|(host, port)| format!("redis://{host}:{port}/")),
    );

    let mut builder = redis::cluster::ClusterClientBuilder::new(nodes);

    if let Some(user) = user.filter(|value| !value.is_empty()) {
        builder = builder.username(user.to_string());
    }

    if let Some(password) = password {
        builder = builder.password(password.to_string());
    }

    if let RedisTlsConfig::TlsVerify(certs) = tls {
        builder = builder.certs(redis::TlsCertificates {
            client_tls: certs.client_tls.clone(),
            root_cert: certs.root_cert.clone(),
        });
    }

    builder.build()?.get_connection()
}

/// Builds the list of Sentinel node URIs: the primary node followed by any
/// configured additional nodes, all as plain `redis://` addresses.
///
/// Per-node TLS for Sentinel nodes is not supported by this batch (see the
/// driver README); `primary_uri` is used as typed by the caller (it may carry
/// a `rediss://` scheme in direct-connect mode, but `SentinelClient` only uses
/// it to reach the Sentinel node itself, not the resolved master).
fn build_sentinel_node_uris(primary_uri: &str, additional_nodes: &[(String, u16)]) -> Vec<String> {
    let mut nodes = vec![primary_uri.to_string()];
    nodes.extend(
        additional_nodes
            .iter()
            .map(|(host, port)| format!("redis://{host}:{port}/")),
    );
    nodes
}

/// Resolves a Sentinel master, runs the standard post-connect sequence, and
/// wraps the result in a `RedisConnection` backed by `RedisTransport::SentinelMaster`.
///
/// Credentials (`user`/`password`) apply only to the resolved MASTER
/// connection, via `SentinelNodeConnectionInfo`; the Sentinel nodes themselves
/// are contacted without authentication (see the driver README limitation).
/// `format_error` adapts a raw `redis::RedisError` into the caller's
/// context-appropriate `DbError` (host/port for direct connect, URI for URI
/// mode).
fn connect_sentinel_master(
    sentinel_node_uris: Vec<String>,
    master_name: Option<&str>,
    user: Option<&str>,
    password: Option<&str>,
    database: Option<u32>,
    ssh_tunnel: Option<SshTunnel>,
    format_error: impl Fn(&redis::RedisError) -> DbError,
) -> Result<Box<dyn Connection>, DbError> {
    let master_name = master_name
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            DbError::InvalidProfile(
                "Redis Sentinel topology requires a configured master/service name".to_string(),
            )
        })?;

    let node_connection_info = SentinelNodeConnectionInfo {
        tls_mode: None,
        redis_connection_info: Some(redis::RedisConnectionInfo {
            db: database.map(i64::from).unwrap_or(0),
            username: user.filter(|value| !value.is_empty()).map(str::to_string),
            password: password.map(str::to_string),
            ..Default::default()
        }),
    };

    let mut resolver = SentinelClient::build(
        sentinel_node_uris,
        master_name.to_string(),
        Some(node_connection_info),
        SentinelServerType::Master,
    )
    .map_err(|e| format_error(&e))?;

    let mut connection = resolver.get_connection().map_err(|e| format_error(&e))?;

    set_client_name(&mut connection);

    redis::cmd("PING")
        .query::<String>(&mut connection)
        .map_err(|e| format_error(&e))?;

    let role_reply = redis::cmd("ROLE").query::<redis::Role>(&mut connection);
    if evaluate_master_role_sanity(&role_reply) == MasterRoleSanity::ResolvedReplica {
        return Err(DbError::query_failed(
            "Redis Sentinel resolved a replica; failover in progress, retry".to_string(),
        ));
    }

    Ok(Box::new(RedisConnection {
        connection: Arc::new(Mutex::new(RedisTransport::SentinelMaster {
            connection: Arc::new(Mutex::new(connection)),
            resolver: Mutex::new(resolver),
        })),
        active_database: Mutex::new(database),
        _ssh_tunnel: ssh_tunnel,
    }))
}

/// Issues `CLUSTER SLOTS` and returns the unique master `(host, port)` pairs
/// backing the cluster. There is no public redis-rs API to enumerate
/// masters directly, so this parses the raw reply via
/// `parse_cluster_slots_masters`.
fn fetch_cluster_masters(
    conn: &mut redis::cluster::ClusterConnection,
) -> Result<Vec<(String, u16)>, DbError> {
    let reply = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query::<redis::Value>(conn)
        .map_err(|e| format_redis_query_error(&e))?;

    parse_cluster_slots_masters(&reply)
}

/// Aggregates `db0` key/TTL stats across every Redis Cluster master.
///
/// A plain `INFO keyspace` on a `ClusterConnection` fans out to every master
/// (`ResponsePolicy::Special`) and returns a `Value::Map` keyed by node
/// address rather than a single string, so it cannot reuse
/// `fetch_keyspace_stats`'s standalone `.query::<String>()` call. Each
/// master is queried individually via `route_command` instead, and the
/// per-node results are summed by `aggregate_keyspace_stats`.
fn fetch_cluster_keyspace_stats(
    conn: &mut redis::cluster::ClusterConnection,
    masters: &[(String, u16)],
) -> Result<KeyspaceStats, DbError> {
    let mut per_master = Vec::with_capacity(masters.len());

    for (host, port) in masters {
        let mut command = redis::cmd("INFO");
        command.arg("keyspace");

        let reply = conn
            .route_command(
                &command,
                RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                    host: host.clone(),
                    port: *port,
                }),
            )
            .map_err(|e| format_redis_query_error(&e))?;

        let info_text: String = redis::FromRedisValue::from_redis_value(&reply)
            .map_err(|e| format_redis_query_error(&e))?;

        per_master.push(parse_keyspace_info(&info_text));
    }

    Ok(aggregate_keyspace_stats(&per_master))
}

/// Reports the client identity to the server via `CLIENT SETNAME` so it is
/// visible in `CLIENT LIST`/`CLIENT INFO`. Some managed Redis providers
/// restrict the `CLIENT` command family, so a failure here must not fail the
/// connection.
///
/// On a Redis Cluster connection this broadcasts to every node (redis-rs
/// routes `CLIENT SETNAME` as `AllNodes`/`AllSucceeded`), so the identity is
/// visible from any node's `CLIENT LIST`.
fn set_client_name(conn: &mut dyn redis::ConnectionLike) {
    if let Err(error) = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg(dbflux_core::client_identity())
        .query::<String>(conn)
    {
        log::warn!("Redis CLIENT SETNAME failed (server may restrict CLIENT commands): {error}");
    }
}

fn uri_authority_has_credentials(uri: &str) -> bool {
    if let Some((_, rest)) = uri.split_once("://") {
        let authority = rest.split('/').next().unwrap_or_default();
        return authority.contains('@');
    }

    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KeyspaceStats {
    key_count: u64,
    avg_ttl_seconds: Option<u64>,
}

fn parse_database_name(database: &str) -> Result<u32, DbError> {
    let trimmed = database.trim();
    let digits = trimmed.strip_prefix("db").unwrap_or(trimmed);

    digits.parse::<u32>().map_err(|_| {
        DbError::InvalidProfile(format!(
            "Invalid database name '{}': expected dbN",
            database
        ))
    })
}

fn fetch_database_count(conn: &mut dyn redis::ConnectionLike) -> Result<u32, DbError> {
    let values: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("databases")
        .query(conn)
        .map_err(|e| format_redis_query_error(&e))?;

    if values.len() < 2 {
        return Err(DbError::query_failed(
            "Invalid CONFIG GET databases response",
        ));
    }

    // Invariant: len >= 2 is guaranteed by the early-return guard above.
    #[allow(clippy::indexing_slicing)]
    let db_count = values[1]
        .parse::<u32>()
        .map_err(|_| DbError::query_failed("Invalid Redis databases count"));
    db_count
}

fn fetch_keyspace_stats(
    conn: &mut dyn redis::ConnectionLike,
) -> Result<HashMap<u32, KeyspaceStats>, DbError> {
    let info = redis::cmd("INFO")
        .arg("keyspace")
        .query::<String>(conn)
        .map_err(|e| format_redis_query_error(&e))?;

    Ok(parse_keyspace_info(&info))
}

/// Parses an `INFO keyspace` reply body into per-database key/TTL stats.
///
/// Extracted as a pure function so both the standalone path (a single
/// `INFO keyspace` call) and the Redis Cluster path (one call per master,
/// aggregated by `aggregate_keyspace_stats`) share the same parsing logic.
fn parse_keyspace_info(info: &str) -> HashMap<u32, KeyspaceStats> {
    let mut stats = HashMap::new();

    for line in info.lines() {
        let line = line.trim();
        if !line.starts_with("db") {
            continue;
        }

        let Some((db_part, fields_part)) = line.split_once(':') else {
            continue;
        };

        let Ok(db_index) = db_part.trim_start_matches("db").parse::<u32>() else {
            continue;
        };

        let mut key_count = 0_u64;
        let mut avg_ttl_seconds = None;

        for field in fields_part.split(',') {
            let Some((name, value)) = field.split_once('=') else {
                continue;
            };

            if name == "keys" {
                key_count = value.parse::<u64>().unwrap_or(0);
            }

            if name == "avg_ttl" {
                let avg_ttl_ms = value.parse::<u64>().unwrap_or(0);
                avg_ttl_seconds = if avg_ttl_ms == 0 {
                    None
                } else {
                    Some(avg_ttl_ms / 1000)
                };
            }
        }

        stats.insert(
            db_index,
            KeyspaceStats {
                key_count,
                avg_ttl_seconds,
            },
        );
    }

    stats
}

/// Sums per-master `db0` key counts into a single aggregate entry, since a
/// Redis Cluster's `INFO keyspace` only ever reports the local master's
/// slice of `db0` (there is no cluster-wide `INFO` aggregation for
/// keyspace stats, unlike `DBSIZE`'s built-in cluster-wide sum).
///
/// `avg_ttl_seconds` is combined as a key-count-weighted average across the
/// masters that reported one; nodes with no TTL data are skipped rather than
/// treated as zero.
fn aggregate_keyspace_stats(per_master: &[HashMap<u32, KeyspaceStats>]) -> KeyspaceStats {
    let mut key_count = 0_u64;
    let mut weighted_ttl_sum = 0_u128;
    let mut weighted_ttl_count = 0_u64;

    for stats in per_master {
        let Some(db0) = stats.get(&0) else {
            continue;
        };

        key_count += db0.key_count;

        if let Some(avg_ttl_seconds) = db0.avg_ttl_seconds {
            weighted_ttl_sum += u128::from(avg_ttl_seconds) * u128::from(db0.key_count);
            weighted_ttl_count += db0.key_count;
        }
    }

    let avg_ttl_seconds = if weighted_ttl_count == 0 {
        None
    } else {
        Some((weighted_ttl_sum / u128::from(weighted_ttl_count)) as u64)
    };

    KeyspaceStats {
        key_count,
        avg_ttl_seconds,
    }
}

/// Number of entries requested per `XRANGE` call. Kept as an existing safety
/// cap on stream reads; `fetch_key_payload` now reports `KeyLoadState::Truncated`
/// when a stream returns exactly this many entries, since the stream may hold
/// more that were not fetched.
const STREAM_FETCH_COUNT: usize = 50;

/// Outcome of comparing a probed value size against an optional byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SizeGateDecision {
    Fetch,
    TooLarge { size_bytes: u64, limit_bytes: u64 },
}

/// Pure size-gate decision: whether a whole-payload value may be fetched
/// given its known size and an optional byte budget.
///
/// `None` budget always allows the fetch (unbounded read path). A size
/// exactly at the budget is allowed; only sizes strictly over it are gated.
fn gate_decision(size_bytes: u64, max_value_bytes: Option<u64>) -> SizeGateDecision {
    match max_value_bytes {
        Some(limit_bytes) if size_bytes > limit_bytes => SizeGateDecision::TooLarge {
            size_bytes,
            limit_bytes,
        },
        _ => SizeGateDecision::Fetch,
    }
}

fn fetch_key_payload(
    conn: &mut dyn redis::ConnectionLike,
    key: &str,
    key_type: KeyType,
    max_value_bytes: Option<u64>,
) -> Result<(Vec<u8>, ValueRepr, KeyLoadState), DbError> {
    match key_type {
        KeyType::String | KeyType::Json | KeyType::Unknown => {
            if max_value_bytes.is_some() {
                let size_bytes = redis::cmd("STRLEN")
                    .arg(key)
                    .query::<u64>(conn)
                    .map_err(|e| format_redis_query_error(&e))?;

                if let SizeGateDecision::TooLarge {
                    size_bytes,
                    limit_bytes,
                } = gate_decision(size_bytes, max_value_bytes)
                {
                    let load_state = KeyLoadState::TooLarge {
                        size_bytes,
                        limit_bytes,
                    };
                    return Ok((Vec::new(), ValueRepr::Binary, load_state));
                }
            }

            let fetched = redis::cmd("GET")
                .arg(key)
                .query::<Option<Vec<u8>>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let value = fetched
                .ok_or_else(|| DbError::object_not_found(format!("Key '{}' not found", key)))?;
            let repr = detect_value_repr(&value);
            Ok((value, repr, KeyLoadState::Loaded))
        }
        KeyType::Hash => {
            let entries = redis::cmd("HGETALL")
                .arg(key)
                .query::<Vec<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let mut object = serde_json::Map::new();
            for chunk in entries.chunks(2) {
                if let [field, value] = chunk {
                    object.insert(field.clone(), serde_json::Value::String(value.clone()));
                }
            }

            let value = serde_json::to_vec(&serde_json::Value::Object(object))
                .map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Structured, KeyLoadState::Loaded))
        }
        KeyType::List => {
            let entries = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let value =
                serde_json::to_vec(&entries).map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Structured, KeyLoadState::Loaded))
        }
        KeyType::Set => {
            let entries = redis::cmd("SMEMBERS")
                .arg(key)
                .query::<Vec<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let value =
                serde_json::to_vec(&entries).map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Structured, KeyLoadState::Loaded))
        }
        KeyType::SortedSet => {
            let entries = redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES")
                .query::<Vec<String>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let items: Vec<serde_json::Value> = entries
                .chunks(2)
                .filter_map(|chunk| {
                    if let [member, score] = chunk {
                        Some(serde_json::json!({"member": member, "score": score}))
                    } else {
                        None
                    }
                })
                .collect();

            let value =
                serde_json::to_vec(&items).map_err(|e| DbError::query_failed(e.to_string()))?;
            Ok((value, ValueRepr::Structured, KeyLoadState::Loaded))
        }
        KeyType::Stream => {
            let raw_entries: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
                .arg(key)
                .arg("-")
                .arg("+")
                .arg("COUNT")
                .arg(STREAM_FETCH_COUNT)
                .query(conn)
                .map_err(|e| format_redis_query_error(&e))?;

            let hit_fetch_cap = raw_entries.len() == STREAM_FETCH_COUNT;

            let entries: Vec<serde_json::Value> = raw_entries
                .into_iter()
                .map(|(id, fields)| {
                    let mut map = serde_json::Map::new();
                    for chunk in fields.chunks(2) {
                        if let [f, v] = chunk {
                            map.insert(f.clone(), serde_json::Value::String(v.clone()));
                        }
                    }
                    serde_json::json!({ "id": id, "fields": map })
                })
                .collect();

            let value =
                serde_json::to_vec(&entries).map_err(|e| DbError::query_failed(e.to_string()))?;

            let load_state = if hit_fetch_cap {
                KeyLoadState::Truncated {
                    returned_bytes: value.len() as u64,
                    total_bytes: None,
                }
            } else {
                KeyLoadState::Loaded
            };

            Ok((value, ValueRepr::Stream, load_state))
        }
        KeyType::Bytes => {
            let payload = redis::cmd("DUMP")
                .arg(key)
                .query::<Vec<u8>>(conn)
                .map_err(|e| format_redis_query_error(&e))?;
            Ok((payload, ValueRepr::Binary, KeyLoadState::Loaded))
        }
    }
}

fn parse_key_type(type_name: &str) -> KeyType {
    let normalized = type_name.trim().to_ascii_lowercase();

    match normalized.as_str() {
        "string" => KeyType::String,
        "hash" => KeyType::Hash,
        "list" => KeyType::List,
        "set" => KeyType::Set,
        "zset" => KeyType::SortedSet,
        "stream" => KeyType::Stream,
        "json" | "rejson-rl" => KeyType::Json,
        _ if normalized.contains("json") => KeyType::Json,
        _ => KeyType::Unknown,
    }
}

fn normalize_key_type_for_payload(key_type: KeyType, repr: ValueRepr) -> KeyType {
    if key_type == KeyType::String && repr == ValueRepr::Binary {
        KeyType::Bytes
    } else {
        key_type
    }
}

fn detect_value_repr(value: &[u8]) -> ValueRepr {
    if let Ok(text) = std::str::from_utf8(value) {
        if serde_json::from_str::<serde_json::Value>(text).is_ok() {
            ValueRepr::Json
        } else {
            ValueRepr::Text
        }
    } else {
        ValueRepr::Binary
    }
}

fn split_command(input: &str) -> Result<Vec<String>, DbError> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        if ch == '\\' {
            escaped = true;
            continue;
        }

        if ch == '\'' && !in_double {
            in_single = !in_single;
            continue;
        }

        if ch == '"' && !in_single {
            in_double = !in_double;
            continue;
        }

        if ch.is_whitespace() && !in_single && !in_double {
            if !current.is_empty() {
                items.push(std::mem::take(&mut current));
            }
            continue;
        }

        current.push(ch);
    }

    if escaped {
        return Err(DbError::query_failed(
            "Dangling escape character in command",
        ));
    }

    if in_single {
        return Err(DbError::query_failed("Unterminated single-quoted string"));
    }

    if in_double {
        return Err(DbError::query_failed("Unterminated double-quoted string"));
    }

    if !current.is_empty() {
        items.push(current);
    }

    Ok(items)
}

pub(crate) fn parse_command(input: &str) -> Result<Vec<String>, DbError> {
    let cleaned = input
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with('#')
        })
        .collect::<Vec<_>>()
        .join("\n");

    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        return Ok(Vec::new());
    }

    let cleaned = cleaned.trim_end_matches(';').trim();
    split_command(cleaned)
}

/// Arity rule for a Redis command.
///
/// - `min`: minimum number of arguments (excluding the command name itself)
/// - `max`: maximum number of arguments, or `None` for variadic commands
enum Arity {
    Exact(usize),
    AtLeast(usize),
    Range(usize, usize),
}

/// Look up the arity expectation for a known Redis command. Returns `None`
/// for unknown commands (no arity check performed).
fn command_arity(command: &str) -> Option<Arity> {
    match command {
        // Key inspection / manipulation
        "GET" => Some(Arity::Exact(1)),
        "SET" => Some(Arity::Range(2, 7)),
        "SETNX" => Some(Arity::Exact(2)),
        "GETSET" => Some(Arity::Exact(2)),
        "GETRANGE" => Some(Arity::Exact(3)),
        "SETRANGE" => Some(Arity::Exact(3)),
        "APPEND" => Some(Arity::Exact(2)),
        "STRLEN" => Some(Arity::Exact(1)),
        "MGET" => Some(Arity::AtLeast(1)),
        "MSET" => Some(Arity::AtLeast(2)),
        "DEL" => Some(Arity::AtLeast(1)),
        "EXISTS" => Some(Arity::AtLeast(1)),
        "EXPIRE" => Some(Arity::Range(2, 3)),
        "TTL" => Some(Arity::Exact(1)),
        "PTTL" => Some(Arity::Exact(1)),
        "TYPE" => Some(Arity::Exact(1)),
        "PERSIST" => Some(Arity::Exact(1)),
        "RENAME" => Some(Arity::Exact(2)),
        "INCR" => Some(Arity::Exact(1)),
        "DECR" => Some(Arity::Exact(1)),
        "INCRBY" => Some(Arity::Exact(2)),
        "DECRBY" => Some(Arity::Exact(2)),
        "DUMP" => Some(Arity::Exact(1)),
        "OBJECT" => Some(Arity::AtLeast(1)),
        "KEYS" => Some(Arity::Exact(1)),
        "SCAN" => Some(Arity::AtLeast(1)),
        "SELECT" => Some(Arity::Exact(1)),

        // Hash
        "HGET" => Some(Arity::Exact(2)),
        "HSET" => Some(Arity::AtLeast(3)),
        "HDEL" => Some(Arity::AtLeast(2)),
        "HGETALL" => Some(Arity::Exact(1)),
        "HLEN" => Some(Arity::Exact(1)),

        // List
        "LPUSH" => Some(Arity::AtLeast(2)),
        "RPUSH" => Some(Arity::AtLeast(2)),
        "LPOP" => Some(Arity::Range(1, 2)),
        "RPOP" => Some(Arity::Range(1, 2)),
        "LRANGE" => Some(Arity::Exact(3)),
        "LLEN" => Some(Arity::Exact(1)),
        "LINDEX" => Some(Arity::Exact(2)),
        "LSET" => Some(Arity::Exact(3)),

        // Set
        "SADD" => Some(Arity::AtLeast(2)),
        "SREM" => Some(Arity::AtLeast(2)),
        "SMEMBERS" => Some(Arity::Exact(1)),
        "SCARD" => Some(Arity::Exact(1)),
        "SISMEMBER" => Some(Arity::Exact(2)),

        // Sorted Set
        "ZADD" => Some(Arity::AtLeast(3)),
        "ZREM" => Some(Arity::AtLeast(2)),
        "ZRANGE" => Some(Arity::Range(3, 7)),
        "ZCARD" => Some(Arity::Exact(1)),
        "ZSCORE" => Some(Arity::Exact(2)),
        "ZRANK" => Some(Arity::Exact(2)),

        // Server
        "PING" => Some(Arity::Range(0, 1)),
        "INFO" => Some(Arity::Range(0, 1)),

        _ => None,
    }
}

/// Check argument count for a parsed Redis command, returning diagnostics if
/// the arity is wrong.
pub(crate) fn check_redis_arity(tokens: &[String], query: &str) -> Vec<EditorDiagnostic> {
    if tokens.is_empty() {
        return vec![];
    }

    // Invariant: tokens is non-empty — guarded by `if tokens.is_empty()` above.
    #[allow(clippy::indexing_slicing)]
    let command = tokens[0].to_uppercase();
    let arg_count = tokens.len() - 1;

    let Some(arity) = command_arity(&command) else {
        return vec![];
    };

    let problem = match arity {
        Arity::Exact(n) if arg_count != n => Some(if n == 1 {
            format!("{command} requires exactly {n} argument, got {arg_count}")
        } else {
            format!("{command} requires exactly {n} arguments, got {arg_count}")
        }),

        Arity::AtLeast(n) if arg_count < n => Some(if n == 1 {
            format!("{command} requires at least {n} argument, got {arg_count}")
        } else {
            format!("{command} requires at least {n} arguments, got {arg_count}")
        }),

        Arity::Range(min, max) if arg_count < min || arg_count > max => Some(format!(
            "{command} accepts {min}–{max} arguments, got {arg_count}"
        )),

        _ => None,
    };

    if let Some(message) = problem {
        return vec![EditorDiagnostic {
            severity: DiagnosticSeverity::Warning,
            message,
            range: redis_first_line_range(query),
        }];
    }

    if let Some(pairing_msg) = check_pairing(&command, arg_count) {
        return vec![EditorDiagnostic {
            severity: DiagnosticSeverity::Warning,
            message: pairing_msg,
            range: redis_first_line_range(query),
        }];
    }

    vec![]
}

/// Commands that require arguments in pairs (key/value or field/value).
fn check_pairing(command: &str, arg_count: usize) -> Option<String> {
    match command {
        // MSET key value [key value ...] — total args must be even
        "MSET" | "MSETNX" if !arg_count.is_multiple_of(2) => Some(format!(
            "{command} requires key-value pairs (even number of arguments), got {arg_count}"
        )),

        // HSET key field value [field value ...] — args after the key must be in pairs
        "HSET" | "HMSET" if arg_count >= 3 && !(arg_count - 1).is_multiple_of(2) => Some(format!(
            "{command} requires a key followed by field-value pairs, got {arg_count} arguments"
        )),

        _ => None,
    }
}

pub(crate) fn redis_first_line_range(query: &str) -> TextPositionRange {
    let first_line_len = query
        .lines()
        .next()
        .map(|line| line.chars().count())
        .unwrap_or(1) as u32;
    let end_col = first_line_len.max(1);

    TextPositionRange::new(TextPosition::new(0, 0), TextPosition::new(0, end_col))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        DatabaseCategory, DbDriver, KeySetRequest, MutationRequest, QueryLanguage,
        SemanticPlanKind, SemanticRequest, TableBrowseRequest, TableRef, ValidationResult,
    };

    #[test]
    fn gate_decision_allows_fetch_when_size_under_budget() {
        assert_eq!(gate_decision(50, Some(100)), SizeGateDecision::Fetch);
    }

    #[test]
    fn gate_decision_allows_fetch_when_size_equals_budget() {
        assert_eq!(gate_decision(100, Some(100)), SizeGateDecision::Fetch);
    }

    #[test]
    fn gate_decision_rejects_fetch_when_size_over_budget() {
        assert_eq!(
            gate_decision(101, Some(100)),
            SizeGateDecision::TooLarge {
                size_bytes: 101,
                limit_bytes: 100,
            }
        );
    }

    #[test]
    fn gate_decision_allows_fetch_when_budget_is_unbounded() {
        assert_eq!(gate_decision(u64::MAX, None), SizeGateDecision::Fetch);
    }

    #[test]
    fn build_config_requires_uri_when_uri_mode_enabled() {
        let driver = RedisDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "true".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn build_config_rejects_invalid_database_index() {
        let driver = RedisDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "localhost".to_string());
        values.insert("port".to_string(), "6379".to_string());
        values.insert("database".to_string(), "nope".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn build_config_requires_host_and_valid_port_in_manual_mode() {
        let driver = RedisDriver::new();

        let mut missing_host = FormValues::new();
        missing_host.insert("port".to_string(), "6379".to_string());
        let result = driver.build_config(&missing_host);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));

        let mut bad_port = FormValues::new();
        bad_port.insert("host".to_string(), "localhost".to_string());
        bad_port.insert("port".to_string(), "not-a-port".to_string());
        let result = driver.build_config(&bad_port);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn extract_values_includes_database_and_omits_tls() {
        let driver = RedisDriver::new();
        let config = DbConfig::Redis {
            use_uri: false,
            uri: None,
            host: "cache.local".to_string(),
            port: 6380,
            user: Some("svc".to_string()),
            database: Some(3),
            tls: false,
            ssl_mode: Some("on".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: None,
            sentinel_master_name: None,
            additional_nodes: None,
        };

        let values = driver.extract_values(&config);
        assert_eq!(values.get("host").map(String::as_str), Some("cache.local"));
        assert_eq!(values.get("port").map(String::as_str), Some("6380"));
        assert_eq!(values.get("database").map(String::as_str), Some("3"));
        // SSL mode is owned by the generic TRANSPORT control, not by the form values.
        assert!(!values.contains_key("tls"));
    }

    #[test]
    fn build_uri_keeps_user_and_db_without_tls_field() {
        let driver = RedisDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "cache.local".to_string());
        values.insert("port".to_string(), "6380".to_string());
        values.insert("user".to_string(), "service user".to_string());
        values.insert("database".to_string(), "2".to_string());

        let uri = driver
            .build_uri(&values, "s3cr@t")
            .expect("redis driver should support uri build");
        assert_eq!(uri, "redis://service%20user:s3cr%40t@cache.local:6380/2");

        let parsed = driver.parse_uri(&uri).expect("uri should parse");
        assert_eq!(parsed.get("host").map(String::as_str), Some("cache.local"));
        assert_eq!(parsed.get("port").map(String::as_str), Some("6380"));
        assert_eq!(parsed.get("user").map(String::as_str), Some("service user"));
        assert_eq!(parsed.get("database").map(String::as_str), Some("2"));
    }

    #[test]
    fn ssl_mode_off_returns_plain() {
        let cfg = redis_ssl_mode_to_config(Some("off"), None, None, None)
            .expect("off should map to plain");
        assert!(matches!(cfg, RedisTlsConfig::Plain));
    }

    #[test]
    fn ssl_mode_default_when_none_is_plain() {
        let cfg =
            redis_ssl_mode_to_config(None, None, None, None).expect("None should map to plain");
        assert!(matches!(cfg, RedisTlsConfig::Plain));
    }

    #[test]
    fn ssl_mode_on_returns_tls_insecure() {
        let cfg = redis_ssl_mode_to_config(Some("on"), None, None, None)
            .expect("on should map to insecure TLS");
        assert!(matches!(cfg, RedisTlsConfig::TlsInsecure));
    }

    #[test]
    fn ssl_mode_verify_without_certs_is_verify_with_no_overrides() {
        let cfg = redis_ssl_mode_to_config(Some("verify"), None, None, None)
            .expect("verify without certs should be valid");
        let certs = match cfg {
            RedisTlsConfig::TlsVerify(c) => c,
            other => panic!("expected TlsVerify, got {:?}", other),
        };
        assert!(certs.client_tls.is_none());
        assert!(certs.root_cert.is_none());
    }

    #[test]
    fn ssl_mode_verify_rejects_partial_client_cert() {
        let err = redis_ssl_mode_to_config(Some("verify"), None, Some("/tmp/cert.pem"), None)
            .expect_err("client cert without key should fail");
        assert!(err.contains("client cert and a client key"));
    }

    #[test]
    fn ssl_mode_unknown_id_returns_error() {
        let err = redis_ssl_mode_to_config(Some("bogus"), None, None, None)
            .expect_err("unknown mode should fail");
        assert!(err.contains("Unknown Redis SSL mode"));
    }

    #[test]
    fn extract_redis_config_migrates_legacy_tls_true() {
        let config = DbConfig::Redis {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 6379,
            user: None,
            database: Some(0),
            tls: true,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: None,
            sentinel_master_name: None,
            additional_nodes: None,
        };
        let extracted = extract_redis_config(&config).expect("redis config should extract");
        assert_eq!(extracted.ssl_mode.as_deref(), Some("on"));
    }

    #[test]
    fn extract_redis_config_migrates_legacy_tls_false() {
        let config = DbConfig::Redis {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 6379,
            user: None,
            database: Some(0),
            tls: false,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: None,
            sentinel_master_name: None,
            additional_nodes: None,
        };
        let extracted = extract_redis_config(&config).expect("redis config should extract");
        assert_eq!(extracted.ssl_mode.as_deref(), Some("off"));
    }

    #[test]
    fn extract_redis_config_prefers_explicit_ssl_mode_over_legacy_tls() {
        let config = DbConfig::Redis {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 6379,
            user: None,
            database: Some(0),
            // Legacy `tls: true` but explicit `ssl_mode: "verify"` — the new field wins.
            tls: true,
            ssl_mode: Some("verify".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: None,
            sentinel_master_name: None,
            additional_nodes: None,
        };
        let extracted = extract_redis_config(&config).expect("redis config should extract");
        assert_eq!(extracted.ssl_mode.as_deref(), Some("verify"));
    }

    fn base_redis_config() -> DbConfig {
        DbConfig::Redis {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 6379,
            user: None,
            database: Some(0),
            tls: false,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: None,
            sentinel_master_name: None,
            additional_nodes: None,
        }
    }

    #[test]
    fn extract_redis_config_defaults_to_standalone_topology() {
        let extracted =
            extract_redis_config(&base_redis_config()).expect("redis config should extract");
        assert_eq!(extracted.topology, ConfiguredTopology::Standalone);
        assert!(extracted.additional_nodes.is_empty());
    }

    #[test]
    fn extract_redis_config_rejects_unknown_topology() {
        let mut config = base_redis_config();
        if let DbConfig::Redis { topology, .. } = &mut config {
            *topology = Some("replica-set".to_string());
        }

        let error = extract_redis_config(&config).unwrap_err();
        assert!(matches!(error, DbError::InvalidProfile(_)));
    }

    #[test]
    fn extract_redis_config_sentinel_requires_master_name() {
        let mut config = base_redis_config();
        if let DbConfig::Redis { topology, .. } = &mut config {
            *topology = Some("sentinel".to_string());
        }

        let error = extract_redis_config(&config).unwrap_err();
        assert!(matches!(error, DbError::InvalidProfile(_)));
    }

    #[test]
    fn extract_redis_config_sentinel_accepts_configured_master_name() {
        let mut config = base_redis_config();
        if let DbConfig::Redis {
            topology,
            sentinel_master_name,
            ..
        } = &mut config
        {
            *topology = Some("sentinel".to_string());
            *sentinel_master_name = Some("mymaster".to_string());
        }

        let extracted = extract_redis_config(&config).expect("redis config should extract");
        assert_eq!(extracted.topology, ConfiguredTopology::Sentinel);
        assert_eq!(extracted.sentinel_master_name.as_deref(), Some("mymaster"));
    }

    #[test]
    fn extract_redis_config_parses_additional_nodes() {
        let mut config = base_redis_config();
        if let DbConfig::Redis {
            topology,
            additional_nodes,
            ..
        } = &mut config
        {
            *topology = Some("cluster".to_string());
            *additional_nodes = Some("10.0.0.2:6379, 10.0.0.3:6379".to_string());
        }

        let extracted = extract_redis_config(&config).expect("redis config should extract");
        assert_eq!(extracted.topology, ConfiguredTopology::Cluster);
        assert_eq!(
            extracted.additional_nodes,
            vec![
                ("10.0.0.2".to_string(), 6379),
                ("10.0.0.3".to_string(), 6379)
            ]
        );
    }

    #[test]
    fn extract_redis_config_rejects_malformed_additional_nodes() {
        let mut config = base_redis_config();
        if let DbConfig::Redis {
            topology,
            additional_nodes,
            ..
        } = &mut config
        {
            *topology = Some("cluster".to_string());
            *additional_nodes = Some("not-a-node".to_string());
        }

        let error = extract_redis_config(&config).unwrap_err();
        assert!(matches!(error, DbError::InvalidProfile(_)));
    }

    #[test]
    fn parse_uri_rejects_unsupported_scheme() {
        let driver = RedisDriver::new();
        assert!(driver.parse_uri("http://localhost:6379").is_none());
    }

    #[test]
    fn parse_uri_defaults_port_when_missing() {
        let driver = RedisDriver::new();
        let parsed = driver
            .parse_uri("redis://localhost/0")
            .expect("uri should parse");

        assert_eq!(parsed.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(parsed.get("port").map(String::as_str), Some("6379"));
        assert_eq!(parsed.get("database").map(String::as_str), Some("0"));
    }

    #[test]
    fn parse_database_name_supports_prefix_and_plain_numbers() {
        assert_eq!(parse_database_name("db3").unwrap(), 3);
        assert_eq!(parse_database_name(" 7 ").unwrap(), 7);
    }

    #[test]
    fn parse_database_name_rejects_invalid_values() {
        let error = parse_database_name("dbx").expect_err("invalid db name should fail");
        assert!(matches!(error, DbError::InvalidProfile(_)));
    }

    #[test]
    fn parse_command_strips_comments_and_semicolon() {
        let tokens = parse_command("# comment\nGET my_key;").expect("command should parse");
        assert_eq!(tokens, vec!["GET", "my_key"]);
    }

    #[test]
    fn parse_command_handles_quotes_and_escapes() {
        let tokens =
            parse_command("SET \"my key\" 'hello world'\\n").expect("quoted command should parse");
        assert_eq!(tokens, vec!["SET", "my key", "hello worldn"]);
    }

    #[test]
    fn parse_command_reports_unterminated_quote() {
        let error = parse_command("SET 'abc").expect_err("unterminated quote should fail");
        assert!(matches!(error, DbError::QueryFailed(_)));
    }

    #[test]
    fn check_pairing_detects_mset_odd_arguments() {
        let message = check_pairing("MSET", 3).expect("odd mset arity should warn");
        assert!(message.contains("even number of arguments"));
    }

    #[test]
    fn check_redis_arity_reports_exact_argument_mismatch() {
        let diagnostics = check_redis_arity(&["GET".to_string()], "GET");
        assert_eq!(diagnostics.len(), 1);
        assert!(
            diagnostics[0]
                .message
                .contains("requires exactly 1 argument")
        );
    }

    #[test]
    fn language_service_flags_sql_as_wrong_language() {
        let service = RedisLanguageService;
        let validation = service.validate("SELECT * FROM users");

        assert!(matches!(validation, ValidationResult::WrongLanguage { .. }));
    }

    #[test]
    fn uri_authority_has_credentials_detects_at_symbol() {
        assert!(uri_authority_has_credentials(
            "redis://:pass@localhost:6379/0"
        ));
        assert!(!uri_authority_has_credentials("redis://localhost:6379/0"));
    }

    #[test]
    fn redis_parse_uri_decodes_username_and_password() {
        let driver = RedisDriver::new();
        let v = driver
            .parse_uri("redis://user%40domain.com:p%40ss@localhost:6379/0")
            .expect("URI should parse");
        assert_eq!(v.get("user").map(String::as_str), Some("user@domain.com"));
        assert_eq!(v.get("password").map(String::as_str), Some("p@ss"));
        assert_eq!(v.get("host").map(String::as_str), Some("localhost"));
        assert_eq!(v.get("port").map(String::as_str), Some("6379"));
    }

    #[test]
    fn parse_uri_no_userinfo() {
        let driver = RedisDriver::new();
        let v = driver
            .parse_uri("redis://host:6379/0")
            .expect("URI should parse");
        assert_eq!(v.get("host").map(String::as_str), Some("host"));
        assert_eq!(v.get("port").map(String::as_str), Some("6379"));
        assert!(!v.contains_key("user"), "no user key expected");
        assert!(!v.contains_key("password"), "no password key expected");
    }

    #[test]
    fn parse_uri_invalid_percent_is_lossy() {
        let driver = RedisDriver::new();
        // %GG is an invalid percent sequence; parse_uri must return Some (no panic, no None)
        // and the username segment must equal the original raw text.
        let v = driver
            .parse_uri("redis://%GGuser:pass@host:6379/0")
            .expect("URI should parse even with invalid percent sequence");
        assert_eq!(
            v.get("user").map(String::as_str),
            Some("%GGuser"),
            "lossy fallback must return the original raw segment"
        );
    }

    #[test]
    fn metadata_and_form_definition_match_redis_contract() {
        let driver = RedisDriver::new();
        let metadata = driver.metadata();

        assert_eq!(metadata.category, DatabaseCategory::KeyValue);
        assert_eq!(metadata.query_language, QueryLanguage::RedisCommands);
        assert_eq!(metadata.default_port, Some(6379));
        assert_eq!(metadata.uri_scheme, "redis");
        assert!(!driver.form_definition().tabs.is_empty());
    }

    #[test]
    fn settings_schema_exposes_scan_and_safety_fields() {
        let driver = RedisDriver::new();
        let schema = driver
            .settings_schema()
            .expect("redis should have a settings schema");

        assert_eq!(schema.tabs.len(), 1);
        assert_eq!(schema.tabs[0].sections.len(), 2);

        let scanning = &schema.tabs[0].sections[0];
        assert_eq!(scanning.title, "Key Scanning");
        assert_eq!(scanning.fields.len(), 2);
        assert_eq!(scanning.fields[0].id, "scan_batch_size");
        assert_eq!(scanning.fields[0].default_value, "100");
        assert_eq!(scanning.fields[1].id, "stream_preview_limit");
        assert_eq!(scanning.fields[1].default_value, "50");

        let safety = &schema.tabs[0].sections[1];
        assert_eq!(safety.title, "Safety");
        assert_eq!(safety.fields.len(), 1);
        assert_eq!(safety.fields[0].id, "allow_flush");
        assert_eq!(safety.fields[0].default_value, "false");
    }

    #[test]
    fn driver_key_is_builtin_redis() {
        let driver = RedisDriver::new();
        assert_eq!(driver.driver_key(), "builtin:redis");
    }

    #[test]
    fn semantic_planner_wraps_key_value_mutation_preview() {
        let plan = plan_redis_semantic_request(&SemanticRequest::Mutation(
            MutationRequest::KeyValueSet(KeySetRequest::new("session:1", b"alive".to_vec())),
        ))
        .expect("key-value mutation should plan");

        assert_eq!(plan.kind, SemanticPlanKind::MutationPreview);
        assert_eq!(plan.queries[0].language, QueryLanguage::RedisCommands);
        assert_eq!(plan.queries[0].text, "SET session:1 alive");
    }

    #[test]
    fn semantic_planner_rejects_relational_browse_requests() {
        let error = plan_redis_semantic_request(&SemanticRequest::TableBrowse(
            TableBrowseRequest::new(TableRef::new("users")),
        ))
        .expect_err("redis should reject relational browse planning");

        assert!(matches!(error, DbError::NotSupported(_)));
        assert!(error.to_string().contains("browse/count"));
    }

    #[test]
    fn semantic_planner_rejects_explain_requests_explicitly() {
        let error = plan_redis_semantic_request(&SemanticRequest::Explain(
            dbflux_core::ExplainRequest::new(TableRef::new("sessions")).with_query("GET sessions"),
        ))
        .expect_err("redis should reject explain planning");

        assert!(matches!(error, DbError::NotSupported(_)));
        assert!(error.to_string().contains("explain or describe"));
    }

    #[test]
    fn redis_metadata_advertises_chart_authoring() {
        assert!(
            REDIS_METADATA
                .capabilities
                .contains(DriverCapabilities::CHART_AUTHORING),
            "CHART_AUTHORING must be set: Redis advertises INSTANCE_METRICS and needs \
             CHART_AUTHORING so the sidebar surfaces Dashboards / Saved Charts folders"
        );
    }

    #[test]
    fn redis_metadata_advertises_instance_metrics() {
        assert!(
            REDIS_METADATA
                .capabilities
                .contains(DriverCapabilities::INSTANCE_METRICS),
            "INSTANCE_METRICS must remain set on Redis driver"
        );
    }

    #[test]
    fn parse_keyspace_info_extracts_key_count_and_avg_ttl() {
        let info = "# Keyspace\r\ndb0:keys=42,expires=1,avg_ttl=5000\r\n";
        let stats = parse_keyspace_info(info);

        assert_eq!(
            stats.get(&0),
            Some(&KeyspaceStats {
                key_count: 42,
                avg_ttl_seconds: Some(5),
            })
        );
    }

    #[test]
    fn parse_keyspace_info_treats_zero_avg_ttl_as_none() {
        let info = "db0:keys=10,expires=0,avg_ttl=0\r\n";
        let stats = parse_keyspace_info(info);

        assert_eq!(
            stats.get(&0),
            Some(&KeyspaceStats {
                key_count: 10,
                avg_ttl_seconds: None,
            })
        );
    }

    #[test]
    fn parse_keyspace_info_empty_when_no_databases_reported() {
        let info = "# Keyspace\r\n";
        assert!(parse_keyspace_info(info).is_empty());
    }

    #[test]
    fn aggregate_keyspace_stats_sums_key_counts_across_masters() {
        let per_master = vec![
            HashMap::from([(
                0,
                KeyspaceStats {
                    key_count: 10,
                    avg_ttl_seconds: None,
                },
            )]),
            HashMap::from([(
                0,
                KeyspaceStats {
                    key_count: 5,
                    avg_ttl_seconds: None,
                },
            )]),
        ];

        let aggregated = aggregate_keyspace_stats(&per_master);
        assert_eq!(aggregated.key_count, 15);
        assert_eq!(aggregated.avg_ttl_seconds, None);
    }

    #[test]
    fn aggregate_keyspace_stats_weights_avg_ttl_by_key_count() {
        let per_master = vec![
            HashMap::from([(
                0,
                KeyspaceStats {
                    key_count: 10,
                    avg_ttl_seconds: Some(100),
                },
            )]),
            HashMap::from([(
                0,
                KeyspaceStats {
                    key_count: 30,
                    avg_ttl_seconds: Some(200),
                },
            )]),
        ];

        // Weighted average: (10*100 + 30*200) / 40 = 175.
        let aggregated = aggregate_keyspace_stats(&per_master);
        assert_eq!(aggregated.key_count, 40);
        assert_eq!(aggregated.avg_ttl_seconds, Some(175));
    }

    #[test]
    fn aggregate_keyspace_stats_skips_masters_without_db0() {
        let per_master = vec![
            HashMap::new(),
            HashMap::from([(
                0,
                KeyspaceStats {
                    key_count: 7,
                    avg_ttl_seconds: Some(50),
                },
            )]),
        ];

        let aggregated = aggregate_keyspace_stats(&per_master);
        assert_eq!(aggregated.key_count, 7);
        assert_eq!(aggregated.avg_ttl_seconds, Some(50));
    }

    #[test]
    fn aggregate_keyspace_stats_empty_input_reports_zero() {
        let aggregated = aggregate_keyspace_stats(&[]);
        assert_eq!(aggregated.key_count, 0);
        assert_eq!(aggregated.avg_ttl_seconds, None);
    }
}
