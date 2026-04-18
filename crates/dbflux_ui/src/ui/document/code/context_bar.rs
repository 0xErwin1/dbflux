use super::*;
use crate::ui::AsyncUpdateResultExt;
use dbflux_components::composites::control_shell;
use dbflux_components::primitives::{Icon, Text, focus_frame};

fn context_dropdown_min_width(index: usize) -> Pixels {
    match index {
        0 => px(140.0),
        1 => px(120.0),
        _ => px(100.0),
    }
}

fn context_dropdown_is_keyboard_focused(
    focus_mode: SqlQueryFocus,
    context_bar_index: usize,
    dropdown_index: usize,
) -> bool {
    focus_mode == SqlQueryFocus::ContextBar && context_bar_index == dropdown_index
}

impl CodeDocument {
    // === Context dropdown creation ===

    pub(super) fn create_connection_dropdown(
        app_state: &Entity<AppStateEntity>,
        exec_ctx: &ExecutionContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<Dropdown>, Subscription) {
        let items = Self::connection_items(app_state, cx);

        let selected_index = exec_ctx.connection_id.and_then(|id| {
            let id = id.to_string();
            items.iter().position(|item| item.value.as_ref() == id)
        });

        let dropdown = cx.new(|_cx| {
            Dropdown::new("ctx-connection")
                .items(items)
                .selected_index(selected_index)
                .placeholder("No connection")
                .toolbar_style(true)
        });

        let sub = cx.subscribe_in(
            &dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                this.on_connection_changed(&event.item, window, cx);
            },
        );

        (dropdown, sub)
    }

    fn connection_items(app_state: &Entity<AppStateEntity>, cx: &App) -> Vec<DropdownItem> {
        let mut items: Vec<_> = app_state
            .read(cx)
            .connections()
            .values()
            .map(|connected| {
                DropdownItem::with_value(&connected.profile.name, connected.profile.id.to_string())
            })
            .collect();

        items.sort_by(|left, right| left.label.as_ref().cmp(right.label.as_ref()));
        items
    }

    fn default_database_for_connection(
        app_state: &Entity<AppStateEntity>,
        connection_id: Uuid,
        cx: &App,
    ) -> Option<String> {
        let connected = app_state.read(cx).connections().get(&connection_id)?;

        connected.active_database.clone().or_else(|| {
            connected
                .schema
                .as_ref()
                .and_then(|schema| schema.current_database().map(String::from))
        })
    }

    fn update_completion_provider(&mut self, cx: &mut Context<Self>) {
        let connection_id = self
            .connection_id
            .filter(|id| self.app_state.read(cx).connections().contains_key(id));

        let completion_provider: Rc<dyn CompletionProvider> =
            Rc::new(QueryCompletionProvider::new(
                self.query_language.clone(),
                self.app_state.clone(),
                connection_id,
            ));

        self.input_state.update(cx, |state, _cx| {
            state.lsp.completion_provider = Some(completion_provider);
        });
    }

    pub(super) fn sync_context_dropdowns(&mut self, cx: &mut Context<Self>) {
        let mut did_change = false;

        if self.connection_id.is_none()
            && self.exec_ctx.connection_id.is_none()
            && let Some(active_connection_id) = self.app_state.read(cx).active_connection_id()
            && self
                .app_state
                .read(cx)
                .connections()
                .contains_key(&active_connection_id)
        {
            self.connection_id = Some(active_connection_id);
            self.exec_ctx.connection_id = Some(active_connection_id);
            did_change = true;
        }

        let connection_items = Self::connection_items(&self.app_state, cx);
        let selected_connection_index = self.connection_id.and_then(|id| {
            let id = id.to_string();
            connection_items
                .iter()
                .position(|item| item.value.as_ref() == id)
        });

        let has_selected_connection = self
            .connection_id
            .is_some_and(|id| self.app_state.read(cx).connections().contains_key(&id));

        self.connection_dropdown.update(cx, |dd, cx| {
            dd.set_items(connection_items, cx);
            dd.set_selected_index(selected_connection_index, cx);
        });

        if has_selected_connection {
            if let Some(connection_id) = self.connection_id {
                self.runner.set_profile_id(connection_id);

                let database_items =
                    Self::database_items_for_connection(&self.app_state, Some(connection_id), cx);

                if self.exec_ctx.database.is_none() {
                    self.exec_ctx.database =
                        Self::default_database_for_connection(&self.app_state, connection_id, cx);
                    did_change = true;
                }

                if self.exec_ctx.database.as_ref().is_some_and(|database| {
                    !database_items
                        .iter()
                        .any(|item| item.value.as_ref() == database)
                }) {
                    self.exec_ctx.database =
                        Self::default_database_for_connection(&self.app_state, connection_id, cx);
                    did_change = true;
                }

                let selected_database_index =
                    self.exec_ctx.database.as_ref().and_then(|database| {
                        database_items
                            .iter()
                            .position(|item| item.value.as_ref() == database)
                    });

                self.database_dropdown.update(cx, |dd, cx| {
                    dd.set_items(database_items, cx);
                    dd.set_selected_index(selected_database_index, cx);
                });

                let schema_items =
                    Self::schema_items_for_connection(&self.app_state, &self.exec_ctx, cx);
                let selected_schema_index = self.exec_ctx.schema.as_ref().and_then(|schema| {
                    schema_items
                        .iter()
                        .position(|item| item.value.as_ref() == schema)
                });

                let next_schema = if selected_schema_index.is_some() {
                    self.exec_ctx.schema.clone()
                } else if schema_items
                    .iter()
                    .any(|item| item.value.as_ref() == "public")
                {
                    Some("public".to_string())
                } else {
                    None
                };

                if self.exec_ctx.schema != next_schema {
                    self.exec_ctx.schema = next_schema.clone();
                    did_change = true;
                }

                let selected_schema_index = next_schema.as_ref().and_then(|schema| {
                    schema_items
                        .iter()
                        .position(|item| item.value.as_ref() == schema)
                });

                self.schema_dropdown.update(cx, |dd, cx| {
                    dd.set_items(schema_items, cx);
                    dd.set_selected_index(selected_schema_index, cx);
                });
            }
        } else {
            self.runner.clear_profile_id();

            self.database_dropdown.update(cx, |dd, cx| {
                dd.set_items(Vec::new(), cx);
                dd.set_selected_index(None, cx);
            });

            self.schema_dropdown.update(cx, |dd, cx| {
                dd.set_items(Vec::new(), cx);
                dd.set_selected_index(None, cx);
            });
        }

        self.update_completion_provider(cx);

        if did_change {
            cx.emit(DocumentEvent::MetaChanged);
        }

        cx.notify();
    }

    pub(super) fn create_database_dropdown(
        app_state: &Entity<AppStateEntity>,
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
                .toolbar_style(true)
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
        app_state: &Entity<AppStateEntity>,
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
                .toolbar_style(true)
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

    fn on_connection_changed(
        &mut self,
        item: &DropdownItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Ok(new_conn_id) = Uuid::parse_str(item.value.as_ref()) else {
            log::warn!("Invalid connection id in dropdown: {}", item.value.as_ref());
            return;
        };

        self.exec_ctx.connection_id = Some(new_conn_id);
        self.connection_id = Some(new_conn_id);
        self.exec_ctx.database =
            Self::default_database_for_connection(&self.app_state, new_conn_id, cx);
        self.exec_ctx.schema = None;
        self.exec_ctx.container = None;

        self.sync_context_dropdowns(cx);

        // Re-validate context bar index since dropdown visibility may have changed
        if self.focus_mode == SqlQueryFocus::ContextBar {
            self.revalidate_context_bar_index(window, cx);
        }
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

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    fn on_schema_changed(&mut self, item: &DropdownItem, cx: &mut Context<Self>) {
        self.exec_ctx.schema = Some(item.value.to_string());
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Refresh the schema dropdown and pre-select the default schema ("public" for PG).
    fn refresh_schema_dropdown_with_default(&mut self, cx: &mut Context<Self>) {
        let schema_items = Self::schema_items_for_connection(&self.app_state, &self.exec_ctx, cx);

        let selected_index = self.exec_ctx.schema.as_ref().and_then(|schema| {
            schema_items
                .iter()
                .position(|item| item.value.as_ref() == schema)
        });

        let next_schema = if selected_index.is_some() {
            self.exec_ctx.schema.clone()
        } else if schema_items
            .iter()
            .any(|item| item.value.as_ref() == "public")
        {
            Some("public".to_string())
        } else {
            None
        };

        self.exec_ctx.schema = next_schema.clone();

        let selected_index = next_schema.as_ref().and_then(|schema| {
            schema_items
                .iter()
                .position(|item| item.value.as_ref() == schema)
        });

        self.schema_dropdown.update(cx, |dd, cx| {
            dd.set_items(schema_items, cx);
            dd.set_selected_index(selected_index, cx);
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
        let params = match self
            .app_state
            .read(cx)
            .prepare_database_connection(profile_id, &database)
        {
            Ok(p) => p,
            Err(e) => {
                log::warn!("Cannot connect to database {}: {}", database, e);
                self.revert_database_selection(prev_database, prev_schema, cx);
                return;
            }
        };

        let app_state = self.app_state.clone();
        let target_db = database.clone();

        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

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
                    .log_if_dropped();
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
                    .log_if_dropped();
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
        app_state: &Entity<AppStateEntity>,
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
        app_state: &Entity<AppStateEntity>,
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

    // === Context bar keyboard navigation ===

    /// Returns the list of visible dropdown indices:
    /// 0 = Connection (always), 1 = Database (if visible), 2 = Schema (if visible).
    fn visible_dropdown_indices(&self, cx: &App) -> Vec<usize> {
        if !self.query_language.supports_connection_context() {
            return Vec::new();
        }

        let mut indices = vec![0]; // Connection is always visible
        if self.should_show_database_dropdown(cx) {
            indices.push(1);
        }
        if self.should_show_schema_dropdown(cx) {
            indices.push(2);
        }
        indices
    }

    fn dropdown_for_index(&self, index: usize) -> &Entity<Dropdown> {
        match index {
            0 => &self.connection_dropdown,
            1 => &self.database_dropdown,
            _ => &self.schema_dropdown,
        }
    }

    pub(super) fn enter_context_bar(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let visible = self.visible_dropdown_indices(cx);
        if visible.is_empty() {
            return;
        }

        self.focus_mode = SqlQueryFocus::ContextBar;
        self.context_bar_index = visible[0];
        self.focus_handle.focus(window);
        self.update_context_bar_focus_rings(cx);
        cx.notify();
    }

    /// Clamp `context_bar_index` to a visible dropdown after connection changes.
    fn revalidate_context_bar_index(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let visible = self.visible_dropdown_indices(cx);

        if visible.is_empty() {
            self.exit_context_bar(window, cx);
            return;
        }

        if !visible.contains(&self.context_bar_index) {
            self.context_bar_index = visible[0];
        }

        self.update_context_bar_focus_rings(cx);
    }

    fn exit_context_bar(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.clear_context_bar_focus_rings(cx);
        self.focus_mode = SqlQueryFocus::Editor;
        self.input_state
            .update(cx, |state, cx| state.focus(window, cx));
        cx.notify();
    }

    pub(super) fn dispatch_context_bar_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let visible = self.visible_dropdown_indices(cx);
        if visible.is_empty() {
            self.exit_context_bar(window, cx);
            return true;
        }

        // If a dropdown is open, route j/k/Enter/Escape to it
        let current_dropdown = self.dropdown_for_index(self.context_bar_index).clone();
        if current_dropdown.read(cx).is_open() {
            match cmd {
                Command::SelectNext => {
                    current_dropdown.update(cx, |dd, cx| dd.select_next_item(cx));
                    return true;
                }
                Command::SelectPrev => {
                    current_dropdown.update(cx, |dd, cx| dd.select_prev_item(cx));
                    return true;
                }
                Command::Execute => {
                    current_dropdown.update(cx, |dd, cx| dd.accept_selection(cx));
                    return true;
                }
                Command::Cancel => {
                    current_dropdown.update(cx, |dd, cx| dd.close(cx));
                    return true;
                }
                _ => {}
            }
        }

        match cmd {
            Command::FocusRight => {
                if let Some(pos) = visible.iter().position(|&i| i == self.context_bar_index)
                    && pos + 1 < visible.len()
                {
                    self.context_bar_index = visible[pos + 1];
                    self.update_context_bar_focus_rings(cx);
                    cx.notify();
                }
                true
            }
            Command::FocusLeft => {
                if let Some(pos) = visible.iter().position(|&i| i == self.context_bar_index)
                    && pos > 0
                {
                    self.context_bar_index = visible[pos - 1];
                    self.update_context_bar_focus_rings(cx);
                    cx.notify();
                }
                true
            }

            Command::Execute => {
                current_dropdown.update(cx, |dd, cx| dd.toggle_open(cx));
                true
            }

            Command::FocusDown | Command::Cancel => {
                self.exit_context_bar(window, cx);
                true
            }

            Command::FocusUp => true,

            // Don't exit context bar for unrelated commands (e.g. C-b toggle sidebar)
            _ => false,
        }
    }

    fn update_context_bar_focus_rings(&self, cx: &mut Context<Self>) {
        let theme = cx.theme();
        let active_color = theme.ring;

        for idx in [0, 1, 2] {
            let dropdown = self.dropdown_for_index(idx);
            let color = if idx == self.context_bar_index {
                Some(active_color)
            } else {
                None
            };
            dropdown.update(cx, |dd, cx| dd.set_focus_ring(color, cx));
        }
    }

    fn clear_context_bar_focus_rings(&self, cx: &mut Context<Self>) {
        for idx in [0, 1, 2] {
            let dropdown = self.dropdown_for_index(idx);
            dropdown.update(cx, |dd, cx| dd.set_focus_ring(None, cx));
        }
    }

    // === Render the context bar ===

    pub(super) fn render_context_bar(&self, cx: &mut Context<Self>) -> AnyElement {
        if !self.query_language.supports_connection_context() {
            return div().id("exec-context-bar").into_any_element();
        }

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
                    .child(Icon::new(AppIcon::Database).size(px(12.0)).muted())
                    .child(Text::caption("Connection:")),
            )
            .child(
                div()
                    .min_w(context_dropdown_min_width(0))
                    .child(focus_frame(
                        context_dropdown_is_keyboard_focused(
                            self.focus_mode,
                            self.context_bar_index,
                            0,
                        ),
                        Some(theme.ring),
                        control_shell(self.connection_dropdown.clone(), cx),
                        cx,
                    )),
            )
            .when(show_db, |el| {
                el.child(Text::caption("Database:")).child(
                    div()
                        .min_w(context_dropdown_min_width(1))
                        .child(focus_frame(
                            context_dropdown_is_keyboard_focused(
                                self.focus_mode,
                                self.context_bar_index,
                                1,
                            ),
                            Some(theme.ring),
                            control_shell(self.database_dropdown.clone(), cx),
                            cx,
                        )),
                )
            })
            .when(show_schema, |el| {
                el.child(Text::caption("Schema:")).child(
                    div()
                        .min_w(context_dropdown_min_width(2))
                        .child(focus_frame(
                            context_dropdown_is_keyboard_focused(
                                self.focus_mode,
                                self.context_bar_index,
                                2,
                            ),
                            Some(theme.ring),
                            control_shell(self.schema_dropdown.clone(), cx),
                            cx,
                        )),
                )
            })
            .child(div().flex_1())
            .when_some(self.path.as_ref(), |el, path| {
                el.child(
                    div()
                        .overflow_x_hidden()
                        .child(Text::caption(path.display().to_string())),
                )
            })
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{SqlQueryFocus, context_dropdown_is_keyboard_focused, context_dropdown_min_width};
    use gpui::px;

    #[test]
    fn connection_dropdown_keeps_widest_shell() {
        assert_eq!(context_dropdown_min_width(0), px(140.0));
    }

    #[test]
    fn database_and_schema_dropdown_shells_keep_compact_widths() {
        assert_eq!(context_dropdown_min_width(1), px(120.0));
        assert_eq!(context_dropdown_min_width(2), px(100.0));
    }

    #[test]
    fn only_active_context_bar_dropdown_reports_keyboard_focus() {
        assert!(context_dropdown_is_keyboard_focused(
            SqlQueryFocus::ContextBar,
            1,
            1,
        ));
        assert!(!context_dropdown_is_keyboard_focused(
            SqlQueryFocus::ContextBar,
            1,
            0,
        ));
        assert!(!context_dropdown_is_keyboard_focused(
            SqlQueryFocus::Editor,
            1,
            1,
        ));
    }
}
