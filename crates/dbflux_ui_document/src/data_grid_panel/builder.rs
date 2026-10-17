use super::*;

impl DataGridPanel {
    /// Records that the builder no longer owns the inspector rail.
    ///
    /// A draft the user never ran is dropped from the read path, so the raw
    /// filter that comes back drives the next reload instead of the unrun
    /// draft. The draft itself stays in the builder panel for the next open.
    pub(super) fn mark_builder_closed(&mut self) {
        self.builder.builder_open = false;

        if !self.builder.filter_input_hidden {
            self.builder.visual_select = None;
        }
    }

    /// Whether the raw WHERE filter input is shown in the toolbar. It is hidden
    /// while the builder is open or while an applied builder spec drives the
    /// rows.
    pub(crate) fn filter_input_visible(&self) -> bool {
        !self.builder.filter_input_hidden && !self.builder.builder_open
    }

    /// Whether the filter row shows the "rows come from the builder query"
    /// notice in place of the WHERE input: the builder is closed while a spec
    /// it ran still drives the rows. A relational WHERE filter lowers to a
    /// spec as well, but it keeps its own chip instead of this notice.
    pub(crate) fn builder_notice_visible(&self) -> bool {
        self.builder.filter_input_hidden
            && !self.builder.builder_open
            && !matches!(
                self.builder.relational_filter_state,
                filter_bar::RelationalFilterState::Active { .. }
            )
    }

    /// Drops the builder spec and its panel, restores the WHERE filter and
    /// reloads the rows through the plain table read.
    ///
    /// Returns `false` without changing anything when unsaved edits block
    /// the reload.
    pub(crate) fn reset_builder_query(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.reload_blocked_by_pending_edits(cx) {
            return false;
        }

        self.clear_builder_draft_spec(cx);
        self.builder.builder_panel = None;
        self.builder._builder_subscriptions.clear();
        self.builder.builder_open = false;
        self.refresh(window, cx);

        true
    }

    // ---- Visual Query Builder integration ----

    /// Stores the given spec and re-computes the cached `SelectQuery`.
    ///
    /// Called when the user presses Run inside the `QueryBuilderPanel`. Does
    /// NOT immediately execute the query; sets `pending_refresh = true` so the
    /// next render tick triggers `run_table_query`, which will find
    /// `visual_select` ready to use.
    pub fn apply_builder_draft_spec(&mut self, spec: VisualQuerySpec, cx: &mut Context<Self>) {
        let select = match self.build_visual_select(&spec, cx) {
            Ok(select) => select,
            Err(message) => {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::Driver,
                        message,
                    ),
                    cx,
                );
                None
            }
        };

        self.builder.builder_draft_spec = Some(spec);
        self.builder.visual_select = select;
        self.builder.filter_input_hidden = true;
        self.pending.refresh = true;

        cx.notify();
    }

    /// Produces the executable read for a builder spec.
    ///
    /// Relational drivers materialize a parameterized `SelectQuery` via
    /// `generate_select`. Document drivers (whose `generate_select` yields
    /// `None`) fall back to the generic `generate_read_from_spec`, whose opaque
    /// query text is wrapped as a parameter-less `SelectQuery` so the existing
    /// execution path runs it unchanged. Selection is by which generator method
    /// returns a query, never by a driver id.
    ///
    /// Returns `Ok(None)` when no generator is available or neither path emits a
    /// query, and `Err(message)` when a generator actively rejects the spec
    /// (e.g. `generate_read_from_spec` returns `InvalidSpec`) so the caller can
    /// surface the failure instead of silently producing an empty read.
    pub(super) fn build_visual_select(
        &self,
        spec: &VisualQuerySpec,
        cx: &App,
    ) -> Result<Option<dbflux_core::SelectQuery>, String> {
        let Some(generator) = self.connection_generator(cx) else {
            return Ok(None);
        };

        match generator.generate_select(spec) {
            Ok(Some(select)) => return Ok(Some(select)),
            // No structured SELECT for this generator (Document drivers return the
            // default `Ok(None)`; `Unsupported` is the same intent made explicit).
            // Either way, fall through to `generate_read_from_spec`.
            Ok(None) => {}
            Err(dbflux_core::QueryGenError::Unsupported) => {}
            Err(error) => return Err(error.to_string()),
        }

        match generator.generate_read_from_spec(spec) {
            Ok(Some(generated)) => Ok(Some(dbflux_core::SelectQuery {
                sql: generated.text,
                params: Vec::new(),
            })),
            Ok(None) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }

    /// Clears the visual spec and restores the raw filter-input chrome.
    ///
    /// Called by the builder's Reset action. The next query falls back to
    /// the `TableBrowseRequest` path.
    pub fn clear_builder_draft_spec(&mut self, cx: &mut Context<Self>) {
        self.builder.builder_draft_spec = None;
        self.builder.visual_select = None;
        self.builder.current_visual_spec = None;
        self.builder.filter_input_hidden = false;

        cx.notify();
    }

    /// Compute the `EditableBinding` for the given committed visual spec by
    /// looking up PK columns from the schema cache.
    ///
    /// The pk_lookup reads the AppState connection cache synchronously; it
    /// returns `None` on a cold cache so the result stays read-only without
    /// triggering any blocking fetch. The binding is recomputed when
    /// table-details arrive (cold → warm upgrade).
    pub(crate) fn compute_builder_binding(
        &self,
        committed_spec: Option<&VisualQuerySpec>,
        profile_id: uuid::Uuid,
        database: Option<&str>,
        cx: &App,
    ) -> Option<dbflux_core::EditableBinding> {
        let spec = committed_spec?;

        let app_state = self.app_state.read(cx);
        let connected = app_state.connections().get(&profile_id)?;
        let db = Self::table_details_database(connected, database);

        spec.compute_editable_binding(|source| {
            let cache_key = (db.clone(), source.schema.clone(), source.table.clone());
            if let Some(table_info) = connected.table_details.get(&cache_key) {
                let cols = table_info.columns.as_deref().unwrap_or(&[]);
                return Some(Self::primary_key_names(cols));
            }

            // Also check database_schemas (MySQL/MariaDB lazy loading).
            if let Some(schema_name) = &source.schema
                && let Some(db_schema) = connected.database_schemas.get(schema_name)
            {
                for t in &db_schema.tables {
                    if t.name == source.table {
                        let cols = t.columns.as_deref().unwrap_or(&[]);
                        return Some(Self::primary_key_names(cols));
                    }
                }
            }

            // Check schema.schemas (PostgreSQL/SQLite).
            if let Some(schema) = &connected.schema {
                for db_schema in schema.schemas() {
                    if source.schema.as_deref() == Some(&db_schema.name) || source.schema.is_none()
                    {
                        for t in &db_schema.tables {
                            if t.name == source.table {
                                let cols = t.columns.as_deref().unwrap_or(&[]);
                                return Some(Self::primary_key_names(cols));
                            }
                        }
                    }
                }
            }

            None
        })
    }

    /// Returns whether the toolbar's "Open in Builder" button should be shown.
    ///
    /// Availability is capability-driven, never keyed off a driver id: the
    /// source must be a relational `Table` or a document `Collection`, the
    /// driver's `category` must be `Relational` or `Document`, and the driver
    /// must publish at least one non-logical predicate operator. A `Collection`
    /// is only eligible when its driver declares the `Document` facet (relational
    /// drivers open as `Table`; key-value drivers publish no predicate operators
    /// and are thus excluded on both counts).
    pub fn can_open_builder(&self, cx: &App) -> bool {
        let (profile_id, is_collection) = match &self.source {
            DataSource::Table { profile_id, .. } => (*profile_id, false),
            DataSource::Collection { profile_id, .. } => (*profile_id, true),
            DataSource::QueryResult { .. } => return false,
        };

        let Some(connected) = self.app_state.read(cx).connections().get(&profile_id) else {
            return false;
        };

        let metadata = connected.connection.metadata();

        let category_ok = matches!(
            metadata.category,
            DatabaseCategory::Relational | DatabaseCategory::Document
        );

        // A collection source only makes sense when its driver declares it holds
        // documents; a relational table source is served by `DataSource::Table`.
        if is_collection && !self.has_document_facet(cx) {
            return false;
        }

        let has_predicates = metadata
            .query
            .as_ref()
            .map(|q| {
                q.where_operators.iter().any(|op| {
                    !matches!(
                        op,
                        WhereOperator::And | WhereOperator::Or | WhereOperator::Not
                    )
                })
            })
            .unwrap_or(false);

        category_ok && has_predicates
    }

    /// Opens (or re-opens) the `QueryBuilderPanel` inspector for this grid.
    ///
    /// Constructs the panel entity on first open, or re-hydrates it from
    /// `builder_draft_spec` when the inspector is opened again after being closed.
    pub fn open_query_builder(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Relational tables map their schema/name into the spec source; document
        // collections map their database/name. Both feed the same builder; the
        // driver's `QueryGenerator` decides which read form is emitted.
        let (profile_id, database, source) = match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                ..
            } => {
                let source = dbflux_core::SourceTable {
                    schema: table.schema.clone(),
                    table: table.name.clone(),
                    alias: table.name.clone(),
                };
                (*profile_id, database.clone(), source)
            }
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => {
                let source = dbflux_core::SourceTable {
                    schema: Some(collection.database.clone()),
                    table: collection.name.clone(),
                    alias: collection.name.clone(),
                };
                (*profile_id, Some(collection.database.clone()), source)
            }
            DataSource::QueryResult { .. } => return,
        };

        // The builder and row inspector share one rail. Opening the builder
        // intentionally ends row-follow mode so tab switches cannot replace
        // the builder with a row snapshot.
        self.clear_inspector_state(cx);

        let source_schema = source.schema.clone();

        let initial_spec = self.builder.builder_draft_spec.clone();

        let weak_self = cx.entity().downgrade();

        let connection_arc: Option<std::sync::Arc<dyn dbflux_core::Connection>> = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone());

        let generate_preview: Box<dyn Fn(&VisualQuerySpec) -> String + Send + Sync> =
            if let Some(conn) = connection_arc.clone() {
                Box::new(move |spec: &VisualQuerySpec| {
                    let Some(generator) = conn.query_generator() else {
                        return String::new();
                    };

                    match generator.generate_select(spec) {
                        Ok(Some(select)) => return select.materialize_for_editor(conn.dialect()),
                        Ok(None) => {}
                        Err(dbflux_core::QueryGenError::Unsupported) => {}
                        Err(error) => return format!("-- {error}"),
                    }

                    match generator.generate_read_from_spec(spec) {
                        Ok(Some(generated)) => generated.text,
                        Ok(None) => String::new(),
                        // Surface the rejection inline so the preview is never a
                        // silently-empty box when the generator refuses the spec.
                        Err(error) => format!("-- {error}"),
                    }
                })
            } else {
                Box::new(|_spec: &VisualQuerySpec| String::new())
            };

        let generate_mutation_preview: Box<
            dyn Fn(&dbflux_core::VisualMutationSpec) -> String + Send + Sync,
        > = if let Some(conn) = connection_arc {
            Box::new(move |spec: &dbflux_core::VisualMutationSpec| {
                conn.query_generator()
                    .and_then(|qgen| {
                        use dbflux_core::MutationKind;
                        let generated = match &spec.kind {
                            MutationKind::Delete => qgen.generate_delete_from_spec(spec).ok(),
                            MutationKind::Update { .. } => {
                                qgen.generate_update_from_spec(spec).ok()
                            }
                        };
                        generated.map(|m| m.materialize_for_editor(conn.dialect()))
                    })
                    .unwrap_or_default()
            })
        } else {
            Box::new(|_spec: &dbflux_core::VisualMutationSpec| String::new())
        };

        let available_columns: Vec<String> =
            self.result.columns.iter().map(|c| c.name.clone()).collect();

        let column_kinds: std::collections::HashMap<String, dbflux_core::ColumnKind> = self
            .result
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.kind))
            .collect();

        // Resolve the orderable key once, from the source's key-schema metadata as
        // it appears in the browse result the builder opens from — not from the
        // live result, which a later builder-generated read can replace with one
        // that no longer carries key markers. Stores that order on a single key
        // (e.g. DynamoDB's sort key) emit their primary-key columns partition-key
        // first and sort-key last, so the trailing primary-key column is the
        // orderable key — but only when more than one primary-key column exists.
        // A single primary-key column is a partition-only key with nothing to
        // order on, so no key is seeded and no ORDER BY is emitted.
        let sort_key_column = resolve_orderable_sort_key(&self.result.columns);

        let panel = if let Some(existing) = &self.builder.builder_panel {
            existing.update(cx, |p, cx| {
                if let Some(spec) = initial_spec.clone() {
                    p.set_spec(spec, cx);
                }
                p.available_columns = available_columns.clone();
                p.column_kinds = column_kinds.clone();
                // Only refresh the cached key when the current result still
                // carries one: a builder-generated read can replace the grid with
                // a result that has no key markers, and re-opening the builder
                // must not clobber a previously resolved sort key with None.
                if let Some(key) = &sort_key_column {
                    p.cached_sort_key_column = Some(key.clone());
                }
            });
            existing.clone()
        } else {
            let new_panel = cx.new(|cx| {
                QueryBuilderPanel::new(
                    source,
                    initial_spec,
                    Some(weak_self.clone()),
                    available_columns,
                    self.app_state.clone(),
                    profile_id,
                    generate_preview,
                    generate_mutation_preview,
                    window,
                    cx,
                )
            });

            new_panel.update(cx, |p, _| {
                p.column_kinds = column_kinds;
                p.cached_sort_key_column = sort_key_column;
            });

            let run_sub = cx.subscribe_in(
                &new_panel,
                window,
                |this, _panel, event: &BuilderEvent, window, cx| {
                    this.handle_builder_event(event, window, cx);
                },
            );

            self.builder._builder_subscriptions = vec![run_sub];
            self.builder.builder_panel = Some(new_panel.clone());
            new_panel
        };

        if self.builder.visual_select.is_none()
            && let Some(spec) = self.builder.builder_draft_spec.clone()
        {
            self.builder.visual_select = self.build_visual_select(&spec, cx).unwrap_or(None);
        }

        self.builder.builder_open = true;

        self.spawn_fk_fetch_for_builder(panel.clone(), profile_id, database, source_schema, cx);

        let view: AnyView = AnyView::from(panel);
        cx.emit(DataGridEvent::OpenInspector {
            title: "Query Builder".into(),
            content: view,
            content_has_header: true,
        });
    }

    /// Applies a successful FK fetch result to the panel's `fk_cache`.
    ///
    /// When the filter bar was waiting for FK metadata (`Resolving` state),
    /// sets `pending_refresh` so the render cycle re-evaluates the relational
    /// filter now that the cache is populated.
    pub(crate) fn apply_fk_result(
        &mut self,
        foreign_keys: Vec<dbflux_core::SchemaForeignKeyInfo>,
        cx: &mut Context<Self>,
    ) {
        self.builder.fk_cache = if foreign_keys.is_empty() {
            FkLoadState::Unavailable
        } else {
            FkLoadState::Ready(foreign_keys)
        };

        self.refresh_filter_fk_links();

        if matches!(
            self.builder.relational_filter_state,
            filter_bar::RelationalFilterState::Resolving
        ) {
            if self.reload_blocked_by_pending_edits(cx) {
                // The re-run will not happen, so the filter bar must stop
                // showing that it is waiting for one.
                self.builder.relational_filter_state = filter_bar::RelationalFilterState::Inactive;
            } else {
                self.pending.refresh = true;
            }
        }

        cx.notify();
    }

    /// Rebuild `filter_completion_cache.fk_links` from `fk_cache`.
    ///
    /// Single-hop only: maps each FK column on the source table to its
    /// referenced table. Multi-hop traversal (e.g. `created_by.organization.name`)
    /// would require recursive FK metadata and is deferred.
    pub(super) fn refresh_filter_fk_links(&mut self) {
        let Some(cache) = self.filter_bar.filter_completion_cache.as_ref() else {
            return;
        };

        let DataSource::Table { table, .. } = &self.source else {
            return;
        };

        let fks = match &self.builder.fk_cache {
            FkLoadState::Ready(fks) => fks,
            _ => return,
        };

        let source_table_lower = table.name.to_lowercase();
        let mut links: HashMap<String, FkLink> = HashMap::new();

        for fk in fks {
            if fk.table_name.to_lowercase() != source_table_lower {
                continue;
            }

            let Some(col) = fk.columns.first() else {
                continue;
            };

            links.insert(
                col.to_lowercase(),
                FkLink {
                    referenced_schema: fk.referenced_schema.clone(),
                    referenced_table: fk.referenced_table.clone(),
                },
            );
        }

        cache.borrow_mut().fk_links = links;
    }

    /// If the filter text contains a `<col>.` qualifier whose left side is an
    /// FK column on the source table, kick off a background fetch of the
    /// referenced table's columns so dotted-path completion has data to show.
    pub(super) fn ensure_filter_fk_columns_loaded(&mut self, text: &str, cx: &mut Context<Self>) {
        let Some(cache_rc) = self.filter_bar.filter_completion_cache.clone() else {
            return;
        };

        let qualifier = match Self::extract_filter_qualifier(text) {
            Some(q) => q,
            None => return,
        };

        let qualifier_lower = qualifier.to_lowercase();
        let (ref_schema, ref_table) = {
            let cache = cache_rc.borrow();
            match cache.fk_links.get(&qualifier_lower) {
                Some(link) => (
                    link.referenced_schema.clone(),
                    link.referenced_table.clone(),
                ),
                None => return,
            }
        };

        let key = (
            ref_schema.as_ref().map(|s| s.to_lowercase()),
            ref_table.to_lowercase(),
        );

        {
            let cache = cache_rc.borrow();
            if cache.joined_columns.contains_key(&key)
                || cache.fetching.contains(&key)
                || cache.failed.contains(&key)
            {
                return;
            }
        }

        let (profile_id, database, source_table_schema) = match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                ..
            } => (*profile_id, database.clone(), table.schema.clone()),
            _ => return,
        };

        let database = database
            .or_else(|| {
                self.app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .and_then(|c| c.active_database.clone())
            })
            .or(source_table_schema)
            .unwrap_or_else(|| "default".to_string());

        let Some(conn) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection_for_database(&database))
        else {
            return;
        };

        cache_rc.borrow_mut().fetching.insert(key.clone());

        let ref_table_for_task = ref_table.clone();
        let ref_schema_for_task = ref_schema.clone();
        let database_for_task = database.clone();

        let task = cx.background_executor().spawn(async move {
            conn.table_details(
                &database_for_task,
                ref_schema_for_task.as_deref(),
                &ref_table_for_task,
            )
        });

        let key_for_finish = key.clone();
        let cache_for_finish = cache_rc.clone();

        let ref_table_for_log = ref_table.clone();
        let database_for_log = database.clone();
        cx.spawn(async move |_this, _cx| {
            let result = task.await;
            let mut cache = cache_for_finish.borrow_mut();
            cache.fetching.remove(&key_for_finish);
            match result {
                Ok(details) => {
                    if let Some(cols) = details.columns {
                        cache.joined_columns.insert(key_for_finish, cols);
                    } else {
                        log::warn!(
                            "autocomplete: FK-target table_details returned no columns for \
                             {}.{}",
                            database_for_log,
                            ref_table_for_log
                        );
                        cache.failed.insert(key_for_finish);
                    }
                }
                Err(err) => {
                    log::warn!(
                        "autocomplete: failed to fetch FK-target columns for {}.{}: {}",
                        database_for_log,
                        ref_table_for_log,
                        err
                    );
                    cache.failed.insert(key_for_finish);
                }
            }
        })
        .detach();
    }

    /// If the filter cache has no source columns yet, kick off a background
    /// fetch of `table_details` for the source table. The result is written
    /// into the cache so subsequent completion calls have data, and is also
    /// pushed into `AppState` so other panels benefit.
    pub(super) fn ensure_filter_source_columns_loaded(&mut self, cx: &mut Context<Self>) {
        let Some(cache_rc) = self.filter_bar.filter_completion_cache.clone() else {
            return;
        };

        let (profile_id, table, database) = match &self.source {
            DataSource::Table {
                profile_id,
                table,
                database,
                ..
            } => (*profile_id, table.clone(), database.clone()),
            _ => return,
        };

        let key: (Option<String>, String) = (
            table.schema.as_ref().map(|s| s.to_lowercase()),
            table.name.to_lowercase(),
        );

        {
            let cache = cache_rc.borrow();
            if !cache.source_columns.is_empty()
                || cache.fetching.contains(&key)
                || cache.failed.contains(&key)
            {
                return;
            }
        }

        let database = database
            .or_else(|| {
                self.app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .and_then(|c| c.active_database.clone())
            })
            .or_else(|| table.schema.clone())
            .unwrap_or_else(|| "default".to_string());

        let Some(conn) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection_for_database(&database))
        else {
            return;
        };

        cache_rc.borrow_mut().fetching.insert(key.clone());

        let table_for_task = table.clone();
        let database_for_task = database.clone();

        let task = cx.background_executor().spawn(async move {
            conn.table_details(
                &database_for_task,
                table_for_task.schema.as_deref(),
                &table_for_task.name,
            )
        });

        let key_for_finish = key.clone();
        let cache_for_finish = cache_rc.clone();
        let app_state_weak = self.app_state.downgrade();
        let database_for_finish = database;
        let table_for_finish = table.clone();

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cache_for_finish
                .borrow_mut()
                .fetching
                .remove(&key_for_finish);

            match result {
                Ok(details) => {
                    if let Some(cols) = details.columns.clone() {
                        cache_for_finish.borrow_mut().source_columns = cols;

                        if let Some(app) = app_state_weak.upgrade() {
                            cx.update(|cx| {
                                app.update(cx, |state, _| {
                                    state.set_table_details(
                                        profile_id,
                                        database_for_finish.clone(),
                                        table_for_finish.schema.clone(),
                                        table_for_finish.name.clone(),
                                        details,
                                    );
                                });
                            });
                        }
                    } else {
                        log::warn!(
                            "autocomplete: table_details returned no columns for {}.{}",
                            database_for_finish,
                            table_for_finish.qualified_name()
                        );
                        cache_for_finish.borrow_mut().failed.insert(key_for_finish);
                    }
                }
                Err(err) => {
                    log::warn!(
                        "autocomplete: failed to fetch columns for {}.{}: {}",
                        database_for_finish,
                        table_for_finish.qualified_name(),
                        err
                    );
                    cache_for_finish.borrow_mut().failed.insert(key_for_finish);
                }
            }
        })
        .detach();
    }
    /// Extract the identifier just before a trailing `.` (or the dot before
    /// the active prefix at end-of-string) from filter text.
    ///
    /// Returns `Some("created_by")` for inputs like `created_by.`,
    /// `created_by.ema`, or `name = 'x' AND created_by.`. Returns `None`
    /// when there is no dot-qualified identifier at the current cursor
    /// position (end of string for autocomplete typing).
    // bytes[i - 1] and bytes[start - 1] are each guarded by i > 0 / start > 0
    // in their loop or condition; the bound is not re-checked per access.
    #[expect(
        clippy::indexing_slicing,
        reason = "five reads: bytes[i - 1] twice in the first while (i > 0 &&) and once behind the i == 0 || short-circuit, bytes[start - 1] twice in the second while (start > 0 &&); i and start begin at or below bytes.len() and only decrease"
    )]
    pub(super) fn extract_filter_qualifier(text: &str) -> Option<String> {
        let bytes = text.as_bytes();
        let mut i = bytes.len();

        while i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
            i -= 1;
        }

        if i == 0 || bytes[i - 1] != b'.' {
            return None;
        }

        let dot_pos = i - 1;
        let mut start = dot_pos;
        while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
            start -= 1;
        }

        if start == dot_pos {
            return None;
        }

        Some(text[start..dot_pos].to_string())
    }

    /// Transitions the panel's `fk_cache` to `Unavailable`.
    pub(crate) fn mark_fk_unavailable(&mut self, cx: &mut Context<Self>) {
        self.builder.fk_cache = FkLoadState::Unavailable;
        cx.notify();
    }

    /// Trigger a FK metadata fetch if the cache has not yet been populated.
    ///
    /// Only fires when `fk_cache == Loading`. All other states (`Ready`,
    /// `Unavailable`) are treated as terminal and this method is a no-op.
    /// Must be called from a `DataSource::Table` context; non-Table sources
    /// return immediately.
    pub(crate) fn ensure_fk_cache_loaded(&mut self, cx: &mut Context<Self>) {
        if !matches!(self.builder.fk_cache, FkLoadState::Loading) {
            return;
        }

        let (profile_id, database, schema) = match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                ..
            } => (*profile_id, database.clone(), table.schema.clone()),
            _ => return,
        };

        let Some(database) = database else {
            self.mark_fk_unavailable(cx);
            return;
        };

        let Some(conn) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone())
        else {
            self.mark_fk_unavailable(cx);
            return;
        };

        let schema_for_task = schema;
        let task = cx
            .background_executor()
            .spawn(async move { conn.schema_foreign_keys(&database, schema_for_task.as_deref()) });

        cx.spawn(async move |this, cx| {
            let result = task.await;
            cx.update(|cx| {
                this.update(cx, |grid, cx| match result {
                    Ok(fks) => grid.apply_fk_result(fks, cx),
                    Err(_) => grid.mark_fk_unavailable(cx),
                })
                .ok();
            });
        })
        .detach();
    }

    /// Loads foreign-key metadata for the builder's source table on a
    /// background task, then applies it to both the `DataGridPanel`'s `fk_cache`
    /// and the given `QueryBuilderPanel`.
    pub(super) fn spawn_fk_fetch_for_builder(
        &self,
        panel: Entity<QueryBuilderPanel>,
        profile_id: uuid::Uuid,
        database: Option<String>,
        schema: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let Some(database) = database else {
            panel.update(cx, |p, cx| p.mark_fk_unavailable(cx));
            return;
        };

        let Some(conn) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone())
        else {
            panel.update(cx, |p, cx| p.mark_fk_unavailable(cx));
            return;
        };

        let schema_for_task = schema.clone();
        let task = cx
            .background_executor()
            .spawn(async move { conn.schema_foreign_keys(&database, schema_for_task.as_deref()) });

        let panel_weak = panel.downgrade();
        cx.spawn(async move |this, cx| {
            let result = task.await;
            cx.update(|cx| {
                if let Some(panel) = panel_weak.upgrade() {
                    panel.update(cx, |p, cx| match &result {
                        Ok(fks) => p.apply_fk_result(fks.clone(), cx),
                        Err(_) => p.mark_fk_unavailable(cx),
                    });
                }

                this.update(cx, |grid, cx| match result {
                    Ok(fks) => grid.apply_fk_result(fks, cx),
                    Err(_) => grid.mark_fk_unavailable(cx),
                })
                .ok();
            });
        })
        .detach();
    }

    /// Handles events emitted by the builder panel.
    pub(super) fn handle_builder_event(
        &mut self,
        event: &BuilderEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            BuilderEvent::RunRequested => {
                if let Some(spec) = self.builder.builder_draft_spec.clone().or_else(|| {
                    self.builder
                        .builder_panel
                        .as_ref()
                        .map(|p| p.read(cx).current_spec().clone())
                }) {
                    if self.reload_blocked_by_pending_edits(cx) {
                        return;
                    }

                    self.apply_builder_draft_spec(spec, cx);
                    self.refresh(window, cx);
                }
            }

            BuilderEvent::SpecChanged(spec) => {
                // Live edit: cache the read for the next Run. A rejected spec is a
                // transient editing state, so it caches `None` without a toast here;
                // the failure is surfaced when the user actually Runs (see
                // `apply_builder_draft_spec`), avoiding a toast on every keystroke.
                self.builder.visual_select = self.build_visual_select(spec, cx).unwrap_or(None);
                self.builder.builder_draft_spec = Some(*spec.clone());
            }

            BuilderEvent::ResetRequested => {
                if self.reset_builder_query(window, cx) {
                    cx.emit(DataGridEvent::CloseInspector);
                }
            }

            BuilderEvent::OpenInEditorRequested => {
                self.open_builder_in_editor(cx);
            }

            BuilderEvent::CloseRequested => {
                self.mark_builder_closed();
                cx.emit(DataGridEvent::CloseInspector);
                cx.notify();
            }

            BuilderEvent::SaveRequested { name } => {
                self.save_builder_query(name.clone(), cx);
            }

            BuilderEvent::SaveAsRequested { name } => {
                self.save_builder_query(name.clone(), cx);
            }

            BuilderEvent::ImportRequested { source_id } => {
                self.import_builder_query(source_id.clone(), cx);
            }

            BuilderEvent::MutationRunRequested {
                spec,
                opts,
                est_rows,
            } => {
                self.on_mutation_run_requested(
                    spec.as_ref().clone(),
                    opts.as_ref().clone(),
                    *est_rows,
                    window,
                    cx,
                );
            }
        }
    }

    /// Produces the editor-ready SQL by inlining literals into the parameterized
    /// query, then opens a new code editor tab with that SQL.
    pub(super) fn open_builder_in_editor(&mut self, cx: &mut Context<Self>) {
        let Some(select) = &self.builder.visual_select else {
            return;
        };

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return,
        };

        let generator = self.connection_generator(cx);
        let sql = generator
            .map(|qgen| qgen.materialize_select_for_editor(select))
            .unwrap_or_else(|| select.sql.clone());

        cx.emit(DataGridEvent::OpenEditorWithContent { profile_id, sql });
    }

    /// Saves the current builder spec under `name` for the panel's profile.
    pub(super) fn save_builder_query(&mut self, name: String, cx: &mut Context<Self>) {
        let Some(spec) = self.builder.builder_draft_spec.clone() else {
            return;
        };

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => profile_id.to_string(),
            _ => return,
        };

        let result = self.app_state.update(cx, |app, _cx| {
            app.saved_queries.save(&profile_id, &name, &spec)
        });

        match result {
            Ok(summary) => {
                if let Some(panel) = &self.builder.builder_panel {
                    panel.update(cx, |p, _| {
                        p.loaded_id = Some(summary.id);
                    });
                }
                dbflux_ui_base::toast::Toast::success(crate::labels::saved_query_saved_as_toast(
                    &name,
                ))
                .meta_right(dbflux_ui_base::toast::now_hms())
                .push(cx);
            }
            Err(e) => {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::Storage,
                        crate::labels::saved_query_already_exists_error(&name),
                    )
                    .with_cause(e.to_string()),
                    cx,
                );
            }
        }
    }

    /// Imports a saved query from another connection into this panel's profile.
    pub(super) fn import_builder_query(&mut self, source_id: String, cx: &mut Context<Self>) {
        use dbflux_ui_base::saved_query_manager::ConnectionTableProbe;

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return,
        };

        let profile_id_str = profile_id.to_string();

        let conn = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::User,
                        dbflux_i18n::t!(
                            "document.data.saved_query.error.target_connection_unavailable"
                        ),
                    ),
                    cx,
                );
                return;
            };
            connected.connection.clone()
        };

        let database = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|c| c.active_database.clone())
            .unwrap_or_default();

        let probe = ConnectionTableProbe::new(conn.as_ref(), &database);

        let result = self.app_state.update(cx, |app, _cx| {
            app.saved_queries
                .import_to(&source_id, &profile_id_str, &probe)
        });

        match result {
            Ok(_summary) => {
                dbflux_ui_base::toast::Toast::success(dbflux_i18n::t!(
                    "document.data.grid.toast.query_imported"
                ))
                .meta_right(dbflux_ui_base::toast::now_hms())
                .push(cx);
            }
            Err(e) => {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::User,
                        dbflux_i18n::t!("document.data.saved_query.error.import_failed"),
                    )
                    .with_cause(e.to_string()),
                    cx,
                );
            }
        }
    }

    /// Returns a reference to the driver's `QueryGenerator`, if connected.
    pub(super) fn connection_generator<'a>(
        &self,
        cx: &'a App,
    ) -> Option<&'a dyn dbflux_core::QueryGenerator> {
        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return None,
        };

        let state = self.app_state.read(cx);
        let connected = state.connections().get(&profile_id)?;

        connected.connection.query_generator()
    }
}
