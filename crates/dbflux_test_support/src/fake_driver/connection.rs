use super::*;

pub(super) struct FakeConnection {
    kind: DbKind,
    state: Arc<FakeDriverState>,
    metadata: Arc<DriverMetadata>,
    active_database: RwLock<Option<String>>,
    current_region: Option<String>,
    secret_stores: Vec<FakeSecretStore>,
}

impl FakeConnection {
    pub(super) fn new(
        kind: DbKind,
        profile: &ConnectionProfile,
        state: Arc<FakeDriverState>,
        metadata: Arc<DriverMetadata>,
    ) -> Self {
        let secret_stores = match kind {
            DbKind::AwsSecrets => fake_secret_stores(&state),
            _ => Vec::new(),
        };

        let current_region = match &profile.config {
            DbConfig::AwsSecrets { region, .. } => Some(region.clone()),
            _ => None,
        };

        Self {
            kind,
            state,
            metadata,
            active_database: RwLock::new(active_database_from_profile(profile)),
            current_region,
            secret_stores,
        }
    }

    fn execute_internal(&self, request: &QueryRequest) -> Result<QueryResult, Box<DbError>> {
        mutex_lock(&self.state.executed_requests).push(request.clone());

        let refuses_read_only = !self.metadata.enforces_read_only()
            || self.state.refuse_read_only.load(Ordering::Relaxed);
        if refuses_read_only {
            request
                .refuse_read_only_enforcement("Fake driver")
                .map_err(Box::new)?;
        }

        if let Some(database) = request.database.clone() {
            *rwlock_write(&self.active_database) = Some(database);
        }

        if let Some(outcome) = rwlock_read(&self.state.query_outcomes)
            .get(&request.sql)
            .cloned()
        {
            return outcome.to_result();
        }

        if let Some(outcome) = rwlock_read(&self.state.default_outcome).clone() {
            return outcome.to_result();
        }

        Ok(QueryResult::empty())
    }
}

impl Connection for FakeConnection {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn ping(&self) -> Result<(), DbError> {
        if let Some(message) = rwlock_read(&self.state.ping_error).clone() {
            return Err(DbError::connection_failed(message));
        }

        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        self.state.close_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.execute_internal(req).map_err(|error| *error)
    }

    fn query_plan(
        &self,
        request: &dbflux_core::QueryPlanRequest,
    ) -> Result<dbflux_core::QueryPlan, DbError> {
        mutex_lock(&self.state.query_plan_requests).push(request.clone());

        let analyzed = match request.mode {
            dbflux_core::QueryPlanMode::Analyze => {
                rwlock_read(&self.state.analyzed_plan_outcome).clone()
            }
            dbflux_core::QueryPlanMode::Estimate => None,
        };
        let Some(outcome) = analyzed.or_else(|| rwlock_read(&self.state.plan_outcome).clone())
        else {
            return Err(DbError::NotSupported(
                "Query plans are not supported by this driver".to_string(),
            ));
        };

        match request.mode {
            dbflux_core::QueryPlanMode::Estimate => {
                dbflux_core::single_plan_statement(&request.query)?;
            }
            dbflux_core::QueryPlanMode::Analyze => {
                dbflux_core::analyze_statement(&dbflux_core::QueryLanguage::Sql, &request.query)?;
            }
        }

        outcome.to_result()
    }

    fn execute_with_handle(
        &self,
        req: &QueryRequest,
    ) -> Result<(QueryHandle, QueryResult), DbError> {
        let handle = QueryHandle::new();
        let result = self.execute_internal(req).map_err(|error| *error)?;
        Ok((handle, result))
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        mutex_lock(&self.state.cancelled_handles).push(handle.clone());
        Ok(())
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.state
            .cancel_active_calls
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(rwlock_read(&self.state.schema).clone())
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        Ok(rwlock_read(&self.state.schema).databases().to_vec())
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        *rwlock_write(&self.active_database) = database.map(std::string::ToString::to_string);
        Ok(())
    }

    fn active_database(&self) -> Option<String> {
        rwlock_read(&self.active_database).clone()
    }

    fn current_region(&self) -> Option<String> {
        self.current_region.clone()
    }

    fn available_regions(&self) -> Result<Vec<String>, DbError> {
        if !self.state.region_switch_enabled.load(Ordering::Relaxed) {
            return Err(DbError::NotSupported(
                "region listing not supported".to_string(),
            ));
        }

        self.state
            .available_regions_calls
            .fetch_add(1, Ordering::Relaxed);

        if let Some(message) = rwlock_read(&self.state.region_list_error).clone() {
            return Err(DbError::PermissionDenied(message.into()));
        }

        Ok(rwlock_read(&self.state.available_regions).clone())
    }

    fn kind(&self) -> DbKind {
        self.kind
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        match self.kind {
            DbKind::MySQL | DbKind::MariaDB | DbKind::SqlServer => {
                SchemaLoadingStrategy::LazyPerDatabase
            }
            DbKind::Postgres | DbKind::Redshift => SchemaLoadingStrategy::ConnectionPerDatabase,
            DbKind::SQLite | DbKind::MongoDB | DbKind::Redis => {
                SchemaLoadingStrategy::SingleDatabase
            }
            DbKind::DynamoDB | DbKind::CloudWatchLogs | DbKind::S3 | DbKind::AwsSecrets => {
                SchemaLoadingStrategy::SingleDatabase
            }
            DbKind::ClickHouse | DbKind::DuckDB | DbKind::InfluxDB => {
                SchemaLoadingStrategy::LazyPerDatabase
            }
            DbKind::Turso => SchemaLoadingStrategy::SingleDatabase,
        }
    }

    fn table_details(
        &self,
        _database: &str,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<dbflux_core::TableInfo, DbError> {
        let call = self
            .state
            .table_details_calls
            .fetch_add(1, Ordering::Relaxed);

        match rwlock_read(&self.state.table_details).get(table) {
            Some(outcome) => fake_table_details_answer(outcome, call, table),
            None => Err(DbError::NotSupported(
                "table_details not supported".to_string(),
            )),
        }
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        mutex_lock(&self.state.crud_ops).push(CrudOp::Update(patch.clone()));
        Ok(CrudResult::new(1, None))
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        mutex_lock(&self.state.crud_ops).push(CrudOp::Insert(insert.clone()));
        Ok(CrudResult::new(1, None))
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if let dbflux_core::RecordIdentity::Composite { values, .. } = &delete.identity
            && let Some((_, message)) = rwlock_read(&self.state.delete_row_errors)
                .iter()
                .find(|(value, _)| values.contains(value))
        {
            return Err(DbError::query_failed(message.clone()));
        }

        mutex_lock(&self.state.crud_ops).push(CrudOp::Delete(delete.clone()));
        Ok(CrudResult::new(1, None))
    }

    fn language_service(&self) -> &dyn dbflux_core::LanguageService {
        match self.kind {
            DbKind::Redis => &REDIS_LANGUAGE_SERVICE,
            DbKind::MongoDB => &MONGO_LANGUAGE_SERVICE,
            _ => &SQL_LANGUAGE_SERVICE,
        }
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &DEFAULT_SQL_DIALECT
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        static GENERATOR: dbflux_core::SqlMutationGenerator =
            dbflux_core::SqlMutationGenerator::new(&DEFAULT_SQL_DIALECT);
        Some(&GENERATOR)
    }

    fn secret_stores(&self) -> Vec<&dyn SecretStoreSource> {
        self.secret_stores
            .iter()
            .map(|store| store as &dyn SecretStoreSource)
            .collect()
    }
}

fn fake_table_details_answer(
    outcome: &FakeTableDetails,
    call: usize,
    table: &str,
) -> Result<dbflux_core::TableInfo, DbError> {
    match outcome {
        FakeTableDetails::Info(info) => Ok((**info).clone()),
        FakeTableDetails::NotFound => Err(DbError::object_not_found(format!(
            "table {table} not found"
        ))),
        FakeTableDetails::Failure(message) => Err(DbError::connection_failed(message.clone())),
        FakeTableDetails::Sequence(answers) => match answers.get(call).or(answers.last()) {
            Some(answer) => fake_table_details_answer(answer, call, table),
            None => Err(DbError::NotSupported(
                "table_details not supported".to_string(),
            )),
        },
    }
}
