use super::*;

impl SqlQueryDocument {
    // === Context dropdown creation ===

    pub(super) fn create_connection_dropdown(
        app_state: &Entity<AppState>,
        exec_ctx: &ExecutionContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<Dropdown>, Subscription) {
        let connections = app_state.read(cx).connections();
        let items: Vec<DropdownItem> = connections
            .values()
            .map(|c| DropdownItem::with_value(&c.profile.name, c.profile.id.to_string()))
            .collect();

        let selected_index = exec_ctx
            .connection_id
            .and_then(|id| connections.values().position(|c| c.profile.id == id));

        let dropdown = cx.new(|_cx| {
            Dropdown::new("ctx-connection")
                .items(items)
                .selected_index(selected_index)
                .placeholder("No connection")
        });

        let sub = cx.subscribe_in(
            &dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.on_connection_changed(event.index, cx);
            },
        );

        (dropdown, sub)
    }

    pub(super) fn create_database_dropdown(
        app_state: &Entity<AppState>,
        exec_ctx: &ExecutionContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<Dropdown>, Subscription) {
        let items = Self::database_items_for_connection(app_state, exec_ctx.connection_id, cx);

        let selected_index = exec_ctx
            .database
            .as_ref()
            .and_then(|db| items.iter().position(|item| item.value.as_ref() == db));

        let dropdown = cx.new(|_cx| {
            Dropdown::new("ctx-database")
                .items(items)
                .selected_index(selected_index)
                .placeholder("Database")
        });

        let sub = cx.subscribe_in(
            &dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.on_database_changed(&event.item, cx);
            },
        );

        (dropdown, sub)
    }

    pub(super) fn create_schema_dropdown(
        app_state: &Entity<AppState>,
        exec_ctx: &ExecutionContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<Dropdown>, Subscription) {
        let items = Self::schema_items_for_connection(app_state, exec_ctx, cx);

        let selected_index = exec_ctx
            .schema
            .as_ref()
            .and_then(|s| items.iter().position(|item| item.value.as_ref() == s));

        let dropdown = cx.new(|_cx| {
            Dropdown::new("ctx-schema")
                .items(items)
                .selected_index(selected_index)
                .placeholder("Schema")
        });

        let sub = cx.subscribe_in(
            &dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.on_schema_changed(&event.item, cx);
            },
        );

        (dropdown, sub)
    }

    // === Event handlers for context changes ===

    fn on_connection_changed(&mut self, index: usize, cx: &mut Context<Self>) {
        let connections = self.app_state.read(cx).connections();
        let conn = connections.values().nth(index);

        let Some(conn) = conn else {
            return;
        };

        let new_conn_id = conn.profile.id;
        self.exec_ctx.connection_id = Some(new_conn_id);
        self.connection_id = Some(new_conn_id);

        // Reset dependent dropdowns
        self.exec_ctx.database = conn.active_database.clone();
        self.exec_ctx.schema = None;
        self.exec_ctx.container = None;

        // Update the task runner
        self.runner.set_profile_id(new_conn_id);

        // Refresh database dropdown items
        let db_items = Self::database_items_for_connection(&self.app_state, Some(new_conn_id), cx);
        let db_selected = self
            .exec_ctx
            .database
            .as_ref()
            .and_then(|db| db_items.iter().position(|item| item.value.as_ref() == db));

        self.database_dropdown.update(cx, |dd, cx| {
            dd.set_items(db_items, cx);
            dd.set_selected_index(db_selected, cx);
        });

        // Refresh schema dropdown with default pre-selection
        self.refresh_schema_dropdown_with_default(cx);

        // Update completion provider
        let completion_provider: Rc<dyn CompletionProvider> =
            Rc::new(QueryCompletionProvider::new(
                self.query_language,
                self.app_state.clone(),
                Some(new_conn_id),
            ));
        self.input_state.update(cx, |state, _cx| {
            state.lsp.completion_provider = Some(completion_provider);
        });

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    fn on_database_changed(&mut self, item: &DropdownItem, cx: &mut Context<Self>) {
        let db_name = item.value.to_string();

        // Save previous state so we can revert on connection failure.
        let prev_database = self.exec_ctx.database.clone();
        let prev_schema = self.exec_ctx.schema.clone();

        self.exec_ctx.database = Some(db_name.clone());
        self.exec_ctx.schema = None;

        if let Some(conn_id) = self.exec_ctx.connection_id {
            let needs_connection = self
                .app_state
                .read(cx)
                .connections()
                .get(&conn_id)
                .is_some_and(|c| {
                    let strategy = c.connection.schema_loading_strategy();
                    strategy == SchemaLoadingStrategy::ConnectionPerDatabase
                        && c.database_connection(&db_name).is_none()
                        && c.schema
                            .as_ref()
                            .and_then(|s| s.current_database())
                            .is_none_or(|current| current != db_name)
                });

            if needs_connection {
                self.connect_to_database(conn_id, db_name.clone(), prev_database, prev_schema, cx);
            }
        }

        self.refresh_schema_dropdown_with_default(cx);

        cx.notify();
    }

    fn on_schema_changed(&mut self, item: &DropdownItem, cx: &mut Context<Self>) {
        self.exec_ctx.schema = Some(item.value.to_string());
        cx.notify();
    }

    /// Refresh the schema dropdown and pre-select the default schema ("public" for PG).
    fn refresh_schema_dropdown_with_default(&mut self, cx: &mut Context<Self>) {
        let schema_items = Self::schema_items_for_connection(&self.app_state, &self.exec_ctx, cx);

        let default_index = schema_items
            .iter()
            .position(|item| item.value.as_ref() == "public");

        if default_index.is_some() {
            self.exec_ctx.schema = Some("public".to_string());
        }

        self.schema_dropdown.update(cx, |dd, cx| {
            dd.set_items(schema_items, cx);
            dd.set_selected_index(default_index, cx);
        });
    }

    /// Connect to a specific database. Reverts `exec_ctx` on failure.
    fn connect_to_database(
        &mut self,
        profile_id: Uuid,
        database: String,
        prev_database: Option<String>,
        prev_schema: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let params = match self.app_state.read(cx).prepare_database_connection(profile_id, &database) {
            Ok(p) => p,
            Err(e) => {
                log::warn!("Cannot connect to database {}: {}", database, e);
                self.revert_database_selection(prev_database, prev_schema, cx);
                return;
            }
        };

        let app_state = self.app_state.clone();
        let target_db = database.clone();

        let task = cx.background_executor().spawn(async move {
            params.execute()
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            match result {
                Ok(switch_result) => {
                    cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.add_database_connection(
                                profile_id,
                                target_db.clone(),
                                switch_result.connection,
                                switch_result.schema,
                            );
                            cx.emit(AppStateChanged);
                        });

                        this.update(cx, |doc, cx| {
                            doc.refresh_schema_dropdown_with_default(cx);
                            cx.notify();
                        })
                        .ok();
                    })
                    .ok();
                }
                Err(e) => {
                    log::error!("Failed to connect to database {}: {}", target_db, e);
                    cx.update(|cx| {
                        this.update(cx, |doc, cx| {
                            doc.revert_database_selection(prev_database, prev_schema, cx);

                            doc.pending_error = Some(format!(
                                "Failed to connect to database '{}': {}",
                                target_db, e
                            ));
                            cx.notify();
                        })
                        .ok();
                    })
                    .ok();
                }
            }
        })
        .detach();
    }

    /// Revert the database dropdown and exec_ctx to the previous state.
    fn revert_database_selection(
        &mut self,
        prev_database: Option<String>,
        prev_schema: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.exec_ctx.database = prev_database.clone();
        self.exec_ctx.schema = prev_schema;

        let db_items =
            Self::database_items_for_connection(&self.app_state, self.exec_ctx.connection_id, cx);

        let db_selected = prev_database
            .as_ref()
            .and_then(|db| db_items.iter().position(|item| item.value.as_ref() == db));

        self.database_dropdown.update(cx, |dd, cx| {
            dd.set_items(db_items, cx);
            dd.set_selected_index(db_selected, cx);
        });

        self.refresh_schema_dropdown_with_default(cx);
    }

    // === Data fetching helpers ===

    fn database_items_for_connection(
        app_state: &Entity<AppState>,
        connection_id: Option<Uuid>,
        cx: &App,
    ) -> Vec<DropdownItem> {
        let Some(conn_id) = connection_id else {
            return Vec::new();
        };

        let Some(connected) = app_state.read(cx).connections().get(&conn_id) else {
            return Vec::new();
        };

        let Some(schema) = &connected.schema else {
            return Vec::new();
        };

        schema
            .databases()
            .iter()
            .map(|db| DropdownItem::with_value(&db.name, &db.name))
            .collect()
    }

    pub(super) fn schema_items_for_connection(
        app_state: &Entity<AppState>,
        exec_ctx: &ExecutionContext,
        cx: &App,
    ) -> Vec<DropdownItem> {
        let Some(conn_id) = exec_ctx.connection_id else {
            return Vec::new();
        };

        let Some(connected) = app_state.read(cx).connections().get(&conn_id) else {
            return Vec::new();
        };

        if !connected
            .connection
            .metadata()
            .capabilities
            .contains(DriverCapabilities::SCHEMAS)
        {
            return Vec::new();
        }

        let schema = exec_ctx
            .database
            .as_deref()
            .and_then(|db| connected.schema_for_target_database(db))
            .or(connected.schema.as_ref());

        let Some(schema) = schema else {
            return Vec::new();
        };

        schema
            .schemas()
            .iter()
            .map(|s| DropdownItem::with_value(&s.name, &s.name))
            .collect()
    }

    // === Visibility helpers for render ===

    pub(super) fn should_show_database_dropdown(&self, cx: &App) -> bool {
        let Some(conn_id) = self.exec_ctx.connection_id else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .map(|c| {
                c.connection
                    .metadata()
                    .capabilities
                    .contains(DriverCapabilities::MULTIPLE_DATABASES)
            })
            .unwrap_or(false)
    }

    pub(super) fn should_show_schema_dropdown(&self, cx: &App) -> bool {
        let Some(conn_id) = self.exec_ctx.connection_id else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .map(|c| {
                c.connection
                    .metadata()
                    .capabilities
                    .contains(DriverCapabilities::SCHEMAS)
            })
            .unwrap_or(false)
    }

    /// Refresh context dropdowns when connections change externally.
    #[allow(dead_code)]
    pub fn refresh_context_dropdowns(&mut self, cx: &mut Context<Self>) {
        let connections = self.app_state.read(cx).connections();
        let items: Vec<DropdownItem> = connections
            .values()
            .map(|c| DropdownItem::with_value(&c.profile.name, c.profile.id.to_string()))
            .collect();

        let selected_index = self
            .exec_ctx
            .connection_id
            .and_then(|id| connections.values().position(|c| c.profile.id == id));

        self.connection_dropdown.update(cx, |dd, cx| {
            dd.set_items(items, cx);
            dd.set_selected_index(selected_index, cx);
        });
    }

    // === Render the context bar ===

    pub(super) fn render_context_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let show_db = self.should_show_database_dropdown(cx);
        let show_schema = self.should_show_schema_dropdown(cx);

        div()
            .id("exec-context-bar")
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(
                        svg()
                            .path(AppIcon::Database.path())
                            .size_3()
                            .text_color(theme.muted_foreground),
                    )
                    .child("Connection:"),
            )
            .child(
                div()
                    .min_w(px(140.0))
                    .child(self.connection_dropdown.clone()),
            )
            .when(show_db, |el| {
                el.child(
                    div()
                        .flex()
                        .items_center()
                        .gap_1()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child("Database:"),
                )
                .child(div().min_w(px(120.0)).child(self.database_dropdown.clone()))
            })
            .when(show_schema, |el| {
                el.child(
                    div()
                        .flex()
                        .items_center()
                        .gap_1()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child("Schema:"),
                )
                .child(div().min_w(px(100.0)).child(self.schema_dropdown.clone()))
            })
            .child(div().flex_1())
            .when_some(self.path.as_ref(), |el, path| {
                el.child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .overflow_x_hidden()
                        .child(path.display().to_string()),
                )
            })
    }
}
