use super::*;

impl DataGridPanel {
    /// Fetch table details to get PK columns if not already cached.
    pub(super) fn fetch_table_details_for_pk(
        &mut self,
        profile_id: Uuid,
        table: &TableRef,
        cx: &mut Context<Self>,
    ) {
        let source_database = match &self.source {
            DataSource::Table { database, .. } => database.clone(),
            _ => None,
        };

        let database = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                // `prepare_fetch_table_details` rejects a disconnected profile,
                // so there is no key to invent and no fetch to attempt.
                log::warn!(
                    "[PK] Cannot fetch table details: profile {} is not connected",
                    profile_id
                );
                return;
            };
            Self::table_details_database(connected, source_database.as_deref())
        };

        // Details cached under this key must be used rather than fetched again:
        // `prepare_fetch_table_details` refuses a second fetch for a key that is
        // already populated, and treating that refusal as a failure is what left
        // a reopened table read-only. An entry carrying only `sample_fields` is
        // such a populated key as well, so its absence of columns is read as
        // "no primary keys here" rather than as an excuse to fetch again.
        let cached_pk_names = self
            .app_state
            .read(cx)
            .get_table_details(profile_id, &database, table.schema.as_deref(), &table.name)
            .map(|details| Self::primary_key_names(details.columns.as_deref().unwrap_or(&[])));

        if let Some(pk_names) = cached_pk_names {
            log::info!(
                "[PK] Using cached table details for {}.{}",
                database,
                table.qualified_name()
            );
            self.apply_pk_details(pk_names, cx);
            return;
        }

        // Logged only for a cache miss: printing it above the check made a warm
        // cache look like a fetch, which is what made #634's evidence ambiguous.
        log::info!(
            "[PK] Fetching table details for PK columns: {}.{}",
            database,
            table.qualified_name()
        );

        let params = match self.app_state.read(cx).prepare_fetch_table_details(
            profile_id,
            &database,
            table.schema.as_deref(),
            &table.name,
        ) {
            Ok(p) => p,
            Err(e) => {
                // Returning silently here is how #634 presented: a read-only
                // grid and nothing but a log line.
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::Driver,
                        crate::labels::pk_details_fetch_failed_error(&e.to_string()),
                    ),
                    cx,
                );
                return;
            }
        };

        self.pk_details_pending = true;
        cx.notify();

        let entity = cx.entity().clone();
        let app_state = self.app_state.clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move { params.execute() })
                .await;

            cx.update(|cx| {
                let fetch_result = match result {
                    Ok(r) => r,
                    Err(e) => {
                        dbflux_ui_base::user_error::report_error(
                            dbflux_ui_base::user_error::UserFacingError::new(
                                dbflux_ui_base::user_error::ErrorKind::Driver,
                                crate::labels::pk_details_fetch_failed_error(&e.to_string()),
                            ),
                            cx,
                        );
                        entity.update(cx, |panel, cx| {
                            panel.pk_details_pending = false;
                            cx.notify();
                        });
                        return;
                    }
                };

                // Extract PK columns
                let pk_names =
                    Self::primary_key_names(fetch_result.details.columns.as_deref().unwrap_or(&[]));

                // Store in cache
                app_state.update(cx, |state, _| {
                    state.set_table_details(
                        fetch_result.profile_id,
                        fetch_result.database.clone(),
                        fetch_result.schema.clone(),
                        fetch_result.table.clone(),
                        fetch_result.details,
                    );
                    state.set_dependents(
                        fetch_result.profile_id,
                        fetch_result.database,
                        fetch_result.schema,
                        fetch_result.table,
                        fetch_result.dependents,
                    );
                });

                entity.update(cx, |panel, cx| {
                    panel.apply_pk_details(pk_names, cx);
                });
            });
        })
        .detach();
    }

    /// Whether the "no primary key" banner has to wait: the key is still being
    /// fetched, or the table is being read again now that it is known.
    pub(crate) fn primary_key_still_resolving(&self) -> bool {
        self.pk_details_pending || self.pk_requery_pending
    }

    pub(super) fn primary_key_names(columns: &[dbflux_core::ColumnInfo]) -> Vec<String> {
        columns
            .iter()
            .filter(|column| column.is_primary_key)
            .map(|column| column.name.clone())
            .collect()
    }

    /// Applies primary-key columns the panel just learned about and rebuilds the
    /// table, so editability and any committed builder binding see them.
    pub(super) fn apply_pk_details(&mut self, pk_names: Vec<String>, cx: &mut Context<Self>) {
        self.pk_details_pending = false;
        // The details carry the table's facets (a hypertable's time axis).
        self.sync_source_facets(cx);
        cx.notify();
        if !pk_names.is_empty() {
            self.pk_columns = pk_names;
        }

        // A projection chosen before the key was known may leave key columns
        // out; they come back, and the table is read again with them.
        let restored_keys = self.keep_keys_in_table_projection(cx);

        // Everything below needs the table this panel reads, so the source is
        // destructured once and both the binding recompute and the requery
        // decision work from that.
        let source_fields = match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                pagination,
                order_by,
                total_rows,
            } => Some((
                *profile_id,
                database.clone(),
                table.clone(),
                pagination.clone(),
                *total_rows,
                order_by.is_empty(),
            )),
            _ => None,
        };

        let Some((profile_id, database, table, pagination, total_rows, unordered)) = source_fields
        else {
            self.pending.rebuild = true;
            cx.notify();
            return;
        };

        // Cold-cache upgrade: if a committed visual spec exists, recompute
        // the binding now that details are available. This upgrades a
        // previously read-only builder result to editable without requiring
        // the user to re-run the query.
        if let Some(spec) = self.builder.current_visual_spec.clone() {
            let binding =
                self.compute_builder_binding(Some(&spec), profile_id, database.as_deref(), cx);
            if let Some(binding) = &binding {
                self.pk_columns = binding.pk_columns.clone();
            }
            self.builder.builder_editable_binding = binding;
        }

        // The first page was issued before these key columns were known, so it
        // carried no `ORDER BY` and `LIMIT/OFFSET` paging can repeat or skip
        // rows. Rewrite the source with the order just learned and requery — the
        // shape `handle_sort_clear` uses for the same reason.
        if !unordered && !restored_keys {
            self.pending.rebuild = true;
            cx.notify();
            return;
        }

        let order_by = Self::get_primary_key_columns(
            &self.app_state,
            profile_id,
            database.as_deref(),
            &table,
            cx,
        );

        if order_by.is_empty() && !restored_keys {
            self.pending.rebuild = true;
            cx.notify();
            return;
        }

        let filter_value = self.filter_bar.filter_input.read(cx).value();
        let filter = if filter_value.trim().is_empty() {
            None
        } else {
            Some(filter_value.to_string())
        };

        self.source = DataSource::Table {
            profile_id,
            database: database.clone(),
            table: table.clone(),
            pagination: pagination.clone(),
            order_by: order_by.clone(),
            total_rows,
        };
        self.pk_requery_pending = true;
        self.pending.requery = Some(PendingRequery {
            profile_id,
            database,
            table,
            pagination,
            order_by,
            filter,
            total_rows,
        });

        cx.notify();
    }

    // === Helpers ===

    /// Primary-key columns for `table`, read from the connection's cached table
    /// details.
    ///
    /// `database` is the database the table was opened from. The cache key is
    /// built by [`DataGridPanel::table_details_database`], the same key
    /// `fetch_table_details_for_pk` writes the entry under.
    ///
    /// That key is not the sidebar's. `ItemIdParts::cache_database`
    /// (`dbflux_ui_sidebar`) falls back to the schema name for a node that
    /// carries no database, so one table reaches the cache as `"main"` there and
    /// as `"default"` here: two entries that can go stale independently. A single
    /// fallback cannot reconcile them, because the string carries two roles at
    /// once — a component of the cache key and the database the fetch targets.
    /// `new_internal`'s filter completion cache is the one reader left on a third
    /// chain, through `table.schema`.
    pub(super) fn get_primary_key_columns(
        app_state: &Entity<AppStateEntity>,
        profile_id: Uuid,
        database: Option<&str>,
        table: &TableRef,
        cx: &Context<Self>,
    ) -> Vec<OrderByColumn> {
        let state = app_state.read(cx);
        let Some(connected) = state.connections().get(&profile_id) else {
            return Vec::new();
        };

        // Check table_details cache first (populated when table is expanded)
        let cache_key = (
            Self::table_details_database(connected, database),
            table.schema.clone(),
            table.name.clone(),
        );
        if let Some(table_info) = connected.table_details.get(&cache_key) {
            let columns = table_info.columns.as_deref().unwrap_or(&[]);
            return columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| OrderByColumn::asc(&c.name))
                .collect();
        }

        // Check database_schemas (MySQL/MariaDB lazy loading)
        if let Some(schema_name) = &table.schema
            && let Some(db_schema) = connected.database_schemas.get(schema_name)
        {
            for t in &db_schema.tables {
                if t.name == table.name {
                    let columns = t.columns.as_deref().unwrap_or(&[]);
                    return columns
                        .iter()
                        .filter(|c| c.is_primary_key)
                        .map(|c| OrderByColumn::asc(&c.name))
                        .collect();
                }
            }
        }

        // Fall back to schema.schemas (PostgreSQL/SQLite)
        let Some(schema) = &connected.schema else {
            return Vec::new();
        };

        for db_schema in schema.schemas() {
            if table.schema.as_deref() == Some(&db_schema.name) || table.schema.is_none() {
                for t in &db_schema.tables {
                    if t.name == table.name {
                        let columns = t.columns.as_deref().unwrap_or(&[]);
                        return columns
                            .iter()
                            .filter(|c| c.is_primary_key)
                            .map(|c| OrderByColumn::asc(&c.name))
                            .collect();
                    }
                }
            }
        }

        Vec::new()
    }

    /// Returns `true` when row mutations (add, edit, delete) are permitted on
    /// the current result.
    ///
    /// Mutations are disabled when the current result is grouped (aggregated
    /// rows have no row identity) or when no primary key columns are known
    /// (preventing UPDATE/DELETE target identification).
    pub fn mutations_enabled(&self) -> bool {
        // Neighbor rows carry a computed `nearest_distance` column the table lacks.
        if self.is_grouped_result() || self.nearest_neighbors.is_some() {
            return false;
        }

        !self.pk_columns.is_empty()
    }
}
