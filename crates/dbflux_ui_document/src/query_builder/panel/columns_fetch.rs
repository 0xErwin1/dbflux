use super::*;

impl QueryBuilderPanel {
    /// Builds the alias binding list from the current spec's source and join rows.
    ///
    /// Used when re-attaching providers after the join list changes.
    pub(crate) fn make_alias_bindings(&self) -> Vec<AliasBinding> {
        let mut bindings = vec![AliasBinding {
            alias: self.current_spec.source.alias.clone(),
            schema: self.current_spec.source.schema.clone(),
            table: self.current_spec.source.table.clone(),
            is_source: true,
        }];

        for row in &self.join_rows {
            if !row.to_table.is_empty() {
                bindings.push(AliasBinding {
                    alias: row.to_alias.clone(),
                    schema: row.to_schema.clone(),
                    table: row.to_table.clone(),
                    is_source: false,
                });
            }
        }

        bindings
    }

    /// Fetches column metadata for a joined table in the background and stores
    /// it in `self.schema_cache`.
    ///
    /// Idempotent: returns immediately if the columns are already cached, a
    /// fetch is in flight, or the fetch previously failed. Fetch failures are
    /// silent (the popover shows aliases only for that join) and stored in the
    /// `failed` set to prevent retries.
    /// Background-fetch column metadata for the builder's source table.
    ///
    /// Called from the panel constructor when the source-table columns are
    /// not yet cached in `AppState`. Writes the fetched columns into the
    /// shared `SchemaCache` so attached completion providers see them as
    /// soon as the fetch resolves; failures are silent (autocomplete is not
    /// a user-facing operation).
    pub(crate) fn spawn_source_columns_fetch(
        schema_cache: Rc<RefCell<SchemaCache>>,
        app_state_weak: gpui::WeakEntity<AppStateEntity>,
        profile_id: uuid::Uuid,
        source_schema: Option<String>,
        source_table: String,
        cx: &mut Context<Self>,
    ) {
        use crate::completion_support::normalize_identifier;

        let key = (
            source_schema.as_ref().map(|s| normalize_identifier(s)),
            normalize_identifier(&source_table),
        );

        {
            let cache = schema_cache.borrow();
            if cache.fetching.contains(&key) || cache.failed.contains(&key) {
                return;
            }
        }

        let Some(app) = app_state_weak.upgrade() else {
            return;
        };

        let db_name = app
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|c| c.active_database.clone())
            .or_else(|| source_schema.clone())
            .unwrap_or_else(|| "default".to_string());

        let Some(conn) = app
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection_for_database(&db_name))
        else {
            return;
        };

        schema_cache.borrow_mut().fetching.insert(key.clone());

        let schema_owned = source_schema;
        let table_owned = source_table;
        let db_for_task = db_name;
        let key_for_task = key.clone();
        let db_for_log = db_for_task.clone();
        let table_for_log = table_owned.clone();

        let task = cx.background_executor().spawn(async move {
            conn.table_details(&db_for_task, schema_owned.as_deref(), &table_owned)
        });

        let schema_cache_for_finish = schema_cache.clone();
        cx.spawn(async move |_this, cx| {
            let result = task.await;
            let _ = cx.update(|cx| {
                {
                    let mut cache = schema_cache_for_finish.borrow_mut();
                    cache.fetching.remove(&key_for_task);
                    match result {
                        Ok(info) => {
                            if let Some(cols) = info.columns {
                                cache.source_columns = cols;
                            } else {
                                log::warn!(
                                    "autocomplete: builder source table_details returned no \
                                     columns for {}.{}",
                                    db_for_log,
                                    table_for_log
                                );
                                cache.failed.insert(key_for_task);
                            }
                        }
                        Err(err) => {
                            log::warn!(
                                "autocomplete: failed to fetch builder source columns for \
                                 {}.{}: {}",
                                db_for_log,
                                table_for_log,
                                err
                            );
                            cache.failed.insert(key_for_task);
                        }
                    }
                }
                _this.update(cx, |_panel, cx| cx.notify()).ok();
            });
        })
        .detach();
    }

    pub(crate) fn ensure_joined_columns(
        &self,
        schema: Option<&str>,
        table: &str,
        cx: &mut Context<Self>,
    ) {
        use crate::completion_support::normalize_identifier;

        let key = (
            schema.map(normalize_identifier),
            normalize_identifier(table),
        );

        {
            let cache = self.schema_cache.borrow();
            if cache.joined_columns.contains_key(&key)
                || cache.fetching.contains(&key)
                || cache.failed.contains(&key)
            {
                return;
            }
        }

        self.schema_cache.borrow_mut().fetching.insert(key.clone());

        let db_name = self
            .app_state_weak
            .upgrade()
            .as_ref()
            .and_then(|app| {
                app.read(cx)
                    .connections()
                    .get(&self.schema_profile_id)
                    .and_then(|c| c.active_database.clone())
            })
            .or_else(|| schema.map(|s| s.to_string()))
            .unwrap_or_else(|| "default".to_string());

        let Some(conn) = self.app_state_weak.upgrade().as_ref().and_then(|app| {
            app.read(cx)
                .connections()
                .get(&self.schema_profile_id)
                .map(|c| c.connection_for_database(&db_name))
        }) else {
            self.schema_cache.borrow_mut().fetching.remove(&key);
            self.schema_cache.borrow_mut().failed.insert(key);
            return;
        };

        let schema_owned = schema.map(|s| s.to_string());
        let table_owned = table.to_string();
        let key_for_task = key.clone();
        let db_for_log = db_name.clone();
        let table_for_log = table_owned.clone();

        let task = cx.background_executor().spawn(async move {
            conn.table_details(&db_name, schema_owned.as_deref(), &table_owned)
        });

        let schema_cache = self.schema_cache.clone();
        cx.spawn(async move |_this, cx| {
            let result = task.await;
            cx.update(|cx| {
                {
                    let mut cache = schema_cache.borrow_mut();
                    cache.fetching.remove(&key_for_task);
                    match result {
                        Ok(table_info) => {
                            let cols = table_info.columns.unwrap_or_default();
                            cache.joined_columns.insert(key_for_task, cols);
                        }
                        Err(err) => {
                            log::warn!(
                                "autocomplete: failed to fetch joined-table columns for \
                                 {}.{}: {}",
                                db_for_log,
                                table_for_log,
                                err
                            );
                            cache.failed.insert(key_for_task);
                        }
                    }
                }
                _this.update(cx, |_panel, cx| cx.notify()).ok();
            })
            .ok();
        })
        .detach();
    }
}
