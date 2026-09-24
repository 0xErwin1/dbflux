use super::*;
use crate::ui::labels::{
    connections_disconnecting_message, connections_edit_window_title,
    connections_manager_window_title, connections_no_active_connection_message,
    connections_refresh_schema_failed_message, connections_refreshing_schema_message,
};

impl Workspace {
    pub(in crate::ui::views::workspace) fn open_connection_manager(&self, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);

        let mut options = WindowOptions {
            app_id: Some(dbflux_core::ReleaseChannel::current().app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some(connections_manager_window_title().into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        match cx.open_window(options, |window, cx| {
            dbflux_ui_base::ui_automation::install(window, cx);

            let manager = cx.new(|cx| ConnectionManagerWindow::new(app_state, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            Ok(handle) => {
                // Explicitly activate the window and force initial render (X11 fix)
                if let Err(e) = handle.update(cx, |_root, window, cx| {
                    window.activate_window();
                    cx.notify();
                }) {
                    log::warn!("Failed to activate connection manager window: {:?}", e);
                }
            }
            Err(error) => {
                log::warn!("Failed to open connection manager window: {:?}", error);
            }
        }
    }

    pub(in crate::ui::views::workspace) fn open_connection_manager_for_edit(
        &self,
        profile_id: uuid::Uuid,
        cx: &mut Context<Self>,
    ) {
        let app_state = self.app_state.clone();

        let profile = app_state
            .read(cx)
            .profiles()
            .iter()
            .find(|p| p.id == profile_id)
            .cloned();

        let Some(profile) = profile else {
            log::warn!(
                "open_connection_manager_for_edit: profile {} not found",
                profile_id
            );
            return;
        };

        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);
        let mut options = WindowOptions {
            app_id: Some(dbflux_core::ReleaseChannel::current().app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some(connections_edit_window_title().into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        if let Err(error) = cx.open_window(options, |window, cx| {
            dbflux_ui_base::ui_automation::install(window, cx);

            let manager =
                cx.new(|cx| ConnectionManagerWindow::new_for_edit(app_state, &profile, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            log::warn!("Failed to open connection editor window: {:?}", error);
        }
    }

    pub(in crate::ui::views::workspace) fn open_connection_manager_in_folder(
        &self,
        folder_id: uuid::Uuid,
        cx: &mut Context<Self>,
    ) {
        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);

        let mut options = WindowOptions {
            app_id: Some(dbflux_core::ReleaseChannel::current().app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some(connections_manager_window_title().into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        if let Err(error) = cx.open_window(options, |window, cx| {
            dbflux_ui_base::ui_automation::install(window, cx);

            let manager = cx
                .new(|cx| ConnectionManagerWindow::new_in_folder(app_state, folder_id, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            log::warn!(
                "Failed to open connection manager window for folder: {:?}",
                error
            );
        }
    }

    /// Open the in-app export modal for a single connection profile.
    pub(in crate::ui::views::workspace) fn open_export_connection_modal(
        &self,
        profile_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.export_modal.update(cx, |modal, cx| {
            modal.open(profile_id, window, cx);
        });
    }

    pub(in crate::ui::views::workspace) fn disconnect_active(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(profile_id) = self.app_state.read(cx).active_connection_id() else {
            return;
        };

        if self.prompt_active_query(ActiveQueryScope::Disconnect(profile_id), window, cx) {
            return;
        }

        self.disconnect_profile_now(profile_id, cx);
    }

    fn disconnect_profile_now(&mut self, profile_id: uuid::Uuid, cx: &mut Context<Self>) {
        let name = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.profile.name.clone());

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.disconnect_profile(profile_id, cx);
        });

        if let Some(name) = name {
            Toast::info(connections_disconnecting_message(&name))
                .meta_right(now_hms())
                .push(cx);
        }
    }

    /// Checks whether the application may quit now.
    ///
    /// Returns `true` when no query is running on any connection. Otherwise
    /// opens the active-query prompt and returns `false`; choosing "Quit
    /// anyway" then emits [`QuitConfirmed`].
    pub fn request_quit(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        !self.prompt_active_query(ActiveQueryScope::Quit, window, cx)
    }

    /// Close action for the main window's in-app (Linux CSD) title bar.
    ///
    /// Follows the window-manager close: [`Self::request_quit`] first, so a
    /// running query opens the prompt, then graceful shutdown through
    /// [`QuitConfirmed`]. Removing the window directly would skip both.
    pub(in crate::ui::views::workspace) fn title_bar_close_handler(
        &self,
        cx: &mut Context<Self>,
    ) -> platform::TitleBarHandler {
        let workspace = cx.entity().downgrade();

        Box::new(move |window, cx| {
            if let Some(workspace) = workspace.upgrade() {
                workspace.update(cx, |workspace, cx| {
                    workspace.close_from_title_bar(window, cx);
                });
            }
        })
    }

    fn close_from_title_bar(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // A shutdown already under way finishes on its own and quits.
        if self.app_state.read(cx).is_shutting_down() {
            return;
        }

        if self.request_quit(window, cx) {
            cx.emit(QuitConfirmed);
        }
    }

    /// Opens the active-query prompt when a query is running within `scope`.
    ///
    /// Returns `false`, without opening anything, when no query is running,
    /// so the caller can go ahead with the disconnect or quit.
    pub(in crate::ui::views::workspace) fn prompt_active_query(
        &mut self,
        scope: ActiveQueryScope,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        use crate::ui::overlays::modals::{ActiveQueryRequest, ActiveQueryTrigger};

        let (profile_filter, trigger) = match scope {
            ActiveQueryScope::Disconnect(profile_id) => {
                (Some(profile_id), ActiveQueryTrigger::Disconnect)
            }
            ActiveQueryScope::Quit => (None, ActiveQueryTrigger::Shutdown),
        };

        let state = self.app_state.read(cx);
        let tasks = state.running_query_tasks(profile_filter);

        let Some(longest_running) = tasks.first() else {
            return false;
        };

        let mut connection_names: Vec<String> = Vec::new();
        for task in &tasks {
            let name = task
                .profile_id
                .and_then(|profile_id| state.connections().get(&profile_id))
                .map(|connected| connected.profile.name.clone());

            if let Some(name) = name
                && !connection_names.contains(&name)
            {
                connection_names.push(name);
            }
        }

        let request = ActiveQueryRequest {
            sql: tasks
                .iter()
                .map(|task| task.description.as_str())
                .collect::<Vec<_>>()
                .join("\n"),
            trigger,
            elapsed_secs: longest_running.elapsed_secs as u64,
            connection_names,
        };

        self.pending_active_query = Some(scope);
        self.modal_active_query.update(cx, |modal, cx| {
            modal.open(request, cx);
        });

        // Take the keyboard so Enter and Escape resolve the prompt through the
        // ConfirmModal keymap instead of reaching the editor behind it.
        self.focus_handle.focus(window, cx);
        cx.notify();

        true
    }

    /// Applies the user's choice in the active-query prompt to the disconnect
    /// or quit it interrupted.
    pub(in crate::ui::views::workspace) fn resolve_active_query(
        &mut self,
        outcome: &crate::ui::overlays::modals::ActiveQueryOutcome,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::overlays::modals::ActiveQueryOutcome;

        let Some(scope) = self.pending_active_query.take() else {
            return;
        };

        let profile_filter = match scope {
            ActiveQueryScope::Disconnect(profile_id) => Some(profile_id),
            ActiveQueryScope::Quit => None,
        };

        match outcome {
            ActiveQueryOutcome::KeepWaiting => {}
            ActiveQueryOutcome::CancelQuery => {
                self.cancel_running_queries(profile_filter, cx);
            }
            ActiveQueryOutcome::ForceDisconnect => match scope {
                ActiveQueryScope::Disconnect(profile_id) => {
                    self.cancel_running_queries(profile_filter, cx);
                    self.disconnect_profile_now(profile_id, cx);
                }
                // The shutdown sequence cancels every task itself.
                ActiveQueryScope::Quit => cx.emit(QuitConfirmed),
            },
        }

        self.set_focus(self.focus_target, window, cx);
    }

    fn cancel_running_queries(
        &mut self,
        profile_filter: Option<uuid::Uuid>,
        cx: &mut Context<Self>,
    ) {
        self.app_state.update(cx, |state, cx| {
            if state.cancel_running_query_tasks(profile_filter) > 0 {
                cx.emit(AppStateChanged);
            }
        });
    }

    pub(in crate::ui::views::workspace) fn refresh_schema(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let active = self.app_state.read(cx).active_connection();

        let Some(active) = active else {
            Toast::warning(connections_no_active_connection_message())
                .meta_right(now_hms())
                .push(cx);
            return;
        };

        let conn = active.connection.clone();
        let profile_id = active.profile.id;
        let app_state = self.app_state.clone();

        let task = cx.background_executor().spawn(async move { conn.schema() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| match result {
                Ok(schema) => {
                    app_state.update(cx, |state, cx| {
                        if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                            connected.schema = Some(schema);
                        }
                        cx.emit(AppStateChanged);
                    });
                }
                Err(e) => {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Driver,
                            connections_refresh_schema_failed_message(e),
                        ),
                        cx,
                    );
                }
            });
        })
        .detach();

        Toast::info(connections_refreshing_schema_message())
            .meta_right(now_hms())
            .push(cx);
    }
}

#[cfg(test)]
mod active_query_prompt_tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::keymap::{Command, CommandDispatcher, ContextId};
    use crate::ui::views::workspace::{QuitConfirmed, Workspace};
    use dbflux_core::{
        Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbError, DbKind, DriverMetadata,
        DriverMetadataBuilder, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
        SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TaskId, TaskKind, TaskStatus,
        TaskTarget, WritePrivilege,
    };
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::{Cell, RefCell};
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread::ThreadId;
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    /// Holds driver cancels until released, the way SQLite's cancel used to
    /// wait for the lock held by the running query.
    #[derive(Default)]
    struct CancelGate {
        released: Mutex<bool>,
        condvar: Condvar,
    }

    impl CancelGate {
        /// Bounded so a regression fails the test instead of hanging it.
        fn wait(&self) {
            let Ok(released) = self.released.lock() else {
                return;
            };

            let waited =
                self.condvar
                    .wait_timeout_while(released, Duration::from_secs(10), |released| !*released);

            if waited.is_err() {
                log::warn!("cancel gate mutex poisoned");
            }
        }

        fn release(&self) {
            if let Ok(mut released) = self.released.lock() {
                *released = true;
            }
            self.condvar.notify_all();
        }
    }

    /// Connection that records driver-level cancels and runs nothing.
    struct FakeConnection {
        metadata: DriverMetadata,
        cancel_calls: Arc<AtomicUsize>,
        cancel_threads: Arc<Mutex<Vec<ThreadId>>>,
        cancel_gate: Option<Arc<CancelGate>>,
    }

    impl FakeConnection {
        fn enter_cancel(&self) {
            if let Ok(mut threads) = self.cancel_threads.lock() {
                threads.push(std::thread::current().id());
            }

            if let Some(gate) = &self.cancel_gate {
                gate.wait();
            }
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _request: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("fake connection".to_string()))
        }

        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn cancel_active(&self) -> Result<(), DbError> {
            self.enter_cancel();
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn cancel_handle(&self) -> Arc<dyn dbflux_core::QueryCancelHandle> {
            self.enter_cancel();
            Arc::new(dbflux_core::NoopCancelHandle)
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }

        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }
    }

    struct Harness<'a> {
        workspace: Entity<Workspace>,
        app_state: Entity<AppStateEntity>,
        window: &'a mut VisualTestContext,
    }

    fn new_harness(cx: &mut TestAppContext) -> Harness<'_> {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::new(RefCell::new(None));
        let workspace_ref = holder.clone();
        let app_state_for_window = app_state.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let workspace = cx.new(|cx| Workspace::new(app_state_for_window, window, cx));
            workspace_ref.replace(Some(workspace.clone()));
            gpui_component::Root::new(workspace, window, cx)
        });

        let workspace = holder
            .borrow()
            .clone()
            .expect("workspace should be created");

        Harness {
            workspace,
            app_state,
            window,
        }
    }

    /// Polls real time, because driver cancels run on their own thread.
    fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);

        while Instant::now() < deadline {
            if condition() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        condition()
    }

    impl Harness<'_> {
        /// Connects a fake profile and makes it the active connection.
        fn connect(&mut self, name: &str) -> (Uuid, Arc<AtomicUsize>) {
            let (profile_id, cancel_calls, _threads) = self.connect_with(name, None);
            (profile_id, cancel_calls)
        }

        /// Connects a fake profile whose driver cancels block until `gate`
        /// is released, and returns the threads the cancels ran on.
        fn connect_blocking(
            &mut self,
            name: &str,
            gate: Arc<CancelGate>,
        ) -> (Uuid, Arc<Mutex<Vec<ThreadId>>>) {
            let (profile_id, _calls, threads) = self.connect_with(name, Some(gate));
            (profile_id, threads)
        }

        fn connect_with(
            &mut self,
            name: &str,
            cancel_gate: Option<Arc<CancelGate>>,
        ) -> (Uuid, Arc<AtomicUsize>, Arc<Mutex<Vec<ThreadId>>>) {
            let cancel_calls = Arc::new(AtomicUsize::new(0));
            let cancel_threads = Arc::new(Mutex::new(Vec::new()));
            let connection = Arc::new(FakeConnection {
                metadata: DriverMetadataBuilder::new(
                    "fake",
                    "Fake",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .build(),
                cancel_calls: cancel_calls.clone(),
                cancel_threads: cancel_threads.clone(),
                cancel_gate,
            });
            let profile = ConnectionProfile::new(
                name,
                DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let profile_id = profile.id;

            self.window.update(|_, cx| {
                self.app_state.update(cx, |state, _| {
                    state.apply_connect_profile(
                        profile,
                        connection,
                        None,
                        None,
                        false,
                        WritePrivilege::Unknown,
                    );
                });
            });

            (profile_id, cancel_calls, cancel_threads)
        }

        fn start_query(&mut self, profile_id: Uuid) -> TaskId {
            self.window.update(|_, cx| {
                self.app_state.update(cx, |state, _| {
                    state
                        .start_task_for_target(
                            TaskKind::Query,
                            "SELECT pg_sleep(60)",
                            Some(TaskTarget {
                                profile_id,
                                database: None,
                            }),
                        )
                        .0
                })
            })
        }

        fn disconnect_active(&mut self) {
            let workspace = self.workspace.clone();
            self.window.update(|window, cx| {
                workspace.update(cx, |workspace, cx| workspace.disconnect_active(window, cx));
            });
            self.window.run_until_parked();
        }

        fn dispatch(&mut self, command: Command) {
            let workspace = self.workspace.clone();
            self.window.update(|window, cx| {
                workspace.update(cx, |workspace, cx| workspace.dispatch(command, window, cx));
            });
            self.window.run_until_parked();
        }

        fn force(&mut self) {
            let workspace = self.workspace.clone();
            self.window.update(|_, cx| {
                let modal = workspace.read(cx).modal_active_query.clone();
                modal.update(cx, |modal, cx| modal.force(cx));
            });
            self.window.run_until_parked();
        }

        fn prompt_visible(&mut self) -> bool {
            let workspace = self.workspace.clone();
            self.window
                .update(|_, cx| workspace.read(cx).modal_active_query.read(cx).is_visible())
        }

        fn is_connected(&mut self, profile_id: Uuid) -> bool {
            let app_state = self.app_state.clone();
            self.window
                .update(|_, cx| app_state.read(cx).connections().contains_key(&profile_id))
        }

        fn task_status(&mut self, task_id: TaskId) -> Option<TaskStatus> {
            let app_state = self.app_state.clone();
            self.window.update(|_, cx| {
                app_state
                    .read(cx)
                    .tasks()
                    .get(task_id)
                    .map(|task| task.status)
            })
        }

        fn request_quit(&mut self) -> bool {
            let workspace = self.workspace.clone();
            let allowed = self.window.update(|window, cx| {
                workspace.update(cx, |workspace, cx| workspace.request_quit(window, cx))
            });
            self.window.run_until_parked();
            allowed
        }

        /// Presses the main window's title-bar close button through the same
        /// handler the render path hands to the CSD title bar.
        fn close_from_title_bar(&mut self) {
            let workspace = self.workspace.clone();
            self.window.update(|window, cx| {
                let handler =
                    workspace.update(cx, |workspace, cx| workspace.title_bar_close_handler(cx));
                handler(window, cx);
            });
            self.window.run_until_parked();
        }

        fn count_quit_confirmed(&mut self) -> Rc<Cell<usize>> {
            let count = Rc::new(Cell::new(0));
            let sink = count.clone();
            let workspace = self.workspace.clone();
            self.window.update(|_, cx| {
                cx.subscribe(&workspace, move |_, _: &QuitConfirmed, _| {
                    sink.set(sink.get() + 1);
                })
                .detach();
            });
            count
        }
    }

    #[gpui::test]
    fn disconnect_without_a_running_query_disconnects_directly(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");

        harness.disconnect_active();

        assert!(!harness.prompt_visible());
        assert!(!harness.is_connected(profile_id));
    }

    #[gpui::test]
    fn a_query_on_another_connection_does_not_block_the_disconnect(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (other_profile, _) = harness.connect("analytics");
        let other_query = harness.start_query(other_profile);
        let (profile_id, _) = harness.connect("prod");

        harness.disconnect_active();

        assert!(!harness.prompt_visible());
        assert!(!harness.is_connected(profile_id));
        assert_eq!(harness.task_status(other_query), Some(TaskStatus::Running));
    }

    #[gpui::test]
    fn disconnect_with_a_running_query_opens_the_prompt_instead(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        let query = harness.start_query(profile_id);

        harness.disconnect_active();

        assert!(harness.prompt_visible());
        assert!(harness.is_connected(profile_id));
        assert_eq!(harness.task_status(query), Some(TaskStatus::Running));

        let workspace = harness.workspace.clone();
        let context = harness
            .window
            .update(|_, cx| workspace.update(cx, |workspace, cx| workspace.active_context(cx)));
        assert_eq!(
            context,
            ContextId::ConfirmModal,
            "the prompt must own Enter and Escape"
        );
    }

    #[gpui::test]
    fn sidebar_disconnect_with_a_running_query_opens_the_prompt(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        harness.start_query(profile_id);

        let workspace = harness.workspace.clone();
        harness.window.update(|_, cx| {
            let sidebar = workspace.read(cx).sidebar.clone();
            sidebar.update(cx, |sidebar, cx| sidebar.request_disconnect(profile_id, cx));
        });
        harness.window.run_until_parked();

        assert!(harness.prompt_visible());
        assert!(harness.is_connected(profile_id));
    }

    #[gpui::test]
    fn sidebar_disconnect_without_a_running_query_disconnects(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");

        let workspace = harness.workspace.clone();
        harness.window.update(|_, cx| {
            let sidebar = workspace.read(cx).sidebar.clone();
            sidebar.update(cx, |sidebar, cx| sidebar.request_disconnect(profile_id, cx));
        });
        harness.window.run_until_parked();

        assert!(!harness.prompt_visible());
        assert!(!harness.is_connected(profile_id));
    }

    #[gpui::test]
    fn escape_keeps_waiting(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, cancel_calls) = harness.connect("prod");
        let query = harness.start_query(profile_id);
        harness.disconnect_active();

        harness.dispatch(Command::Cancel);

        assert!(!harness.prompt_visible());
        assert!(harness.is_connected(profile_id));
        assert_eq!(harness.task_status(query), Some(TaskStatus::Running));
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 0);
    }

    #[gpui::test]
    fn enter_cancels_the_query_and_stays_connected(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, cancel_calls) = harness.connect("prod");
        let query = harness.start_query(profile_id);
        harness.disconnect_active();

        harness.dispatch(Command::Execute);

        assert!(!harness.prompt_visible());
        assert!(harness.is_connected(profile_id));
        assert_eq!(harness.task_status(query), Some(TaskStatus::Cancelled));
        assert!(
            wait_until(|| cancel_calls.load(Ordering::SeqCst) > 0),
            "the driver must receive the cancel"
        );
    }

    #[gpui::test]
    fn disconnect_anyway_cancels_the_query_and_disconnects(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        let query = harness.start_query(profile_id);
        harness.disconnect_active();

        harness.force();

        assert!(!harness.prompt_visible());
        assert!(!harness.is_connected(profile_id));
        assert_eq!(harness.task_status(query), Some(TaskStatus::Cancelled));
    }

    #[gpui::test]
    fn quit_without_a_running_query_proceeds(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        harness.connect("prod");

        assert!(harness.request_quit());
        assert!(!harness.prompt_visible());
    }

    #[gpui::test]
    fn quit_anyway_confirms_the_quit(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        harness.start_query(profile_id);
        let quit_confirmed = harness.count_quit_confirmed();

        assert!(
            !harness.request_quit(),
            "a running query must hold the quit"
        );
        assert!(harness.prompt_visible());
        assert_eq!(quit_confirmed.get(), 0);

        harness.force();

        assert!(!harness.prompt_visible());
        assert_eq!(quit_confirmed.get(), 1);
    }

    #[gpui::test]
    fn quit_keep_waiting_does_not_quit(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        let query = harness.start_query(profile_id);
        let quit_confirmed = harness.count_quit_confirmed();

        assert!(!harness.request_quit());
        harness.dispatch(Command::Cancel);

        assert!(!harness.prompt_visible());
        assert_eq!(quit_confirmed.get(), 0);
        assert_eq!(harness.task_status(query), Some(TaskStatus::Running));
    }

    #[gpui::test]
    fn quit_cancel_query_cancels_every_connection_without_quitting(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (first_profile, _) = harness.connect("prod");
        let (second_profile, _) = harness.connect("analytics");
        let first_query = harness.start_query(first_profile);
        let second_query = harness.start_query(second_profile);
        let quit_confirmed = harness.count_quit_confirmed();

        assert!(!harness.request_quit());
        harness.dispatch(Command::Execute);

        assert!(!harness.prompt_visible());
        assert_eq!(quit_confirmed.get(), 0);
        assert_eq!(
            harness.task_status(first_query),
            Some(TaskStatus::Cancelled)
        );
        assert_eq!(
            harness.task_status(second_query),
            Some(TaskStatus::Cancelled)
        );
        assert!(harness.is_connected(first_profile));
        assert!(harness.is_connected(second_profile));
    }

    #[gpui::test]
    fn title_bar_close_with_a_running_query_opens_the_prompt(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        let query = harness.start_query(profile_id);
        let quit_confirmed = harness.count_quit_confirmed();

        harness.close_from_title_bar();

        assert!(harness.prompt_visible());
        assert_eq!(quit_confirmed.get(), 0);
        assert_eq!(harness.task_status(query), Some(TaskStatus::Running));

        harness.force();

        assert_eq!(quit_confirmed.get(), 1);
    }

    #[gpui::test]
    fn title_bar_close_without_a_running_query_confirms_the_quit(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        harness.connect("prod");
        let quit_confirmed = harness.count_quit_confirmed();

        harness.close_from_title_bar();

        assert!(!harness.prompt_visible());
        assert_eq!(quit_confirmed.get(), 1);
    }

    #[gpui::test]
    fn title_bar_close_during_shutdown_does_nothing(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        harness.start_query(profile_id);
        let quit_confirmed = harness.count_quit_confirmed();

        let app_state = harness.app_state.clone();
        harness.window.update(|_, cx| {
            assert!(app_state.read(cx).begin_shutdown());
        });

        harness.close_from_title_bar();

        assert!(!harness.prompt_visible());
        assert_eq!(quit_confirmed.get(), 0);
    }

    /// Asserts a prompt choice returned while the driver cancel was still
    /// blocked, then that the cancel ran off the UI thread once released.
    fn assert_cancel_ran_off_the_ui_thread(
        elapsed: Duration,
        gate: &CancelGate,
        cancel_threads: &Mutex<Vec<ThreadId>>,
    ) {
        assert!(
            elapsed < Duration::from_secs(2),
            "the UI thread waited {elapsed:?} for a blocked driver cancel"
        );

        gate.release();

        let ui_thread = std::thread::current().id();
        let threads = || {
            cancel_threads
                .lock()
                .map(|threads| threads.clone())
                .unwrap_or_default()
        };

        assert!(
            wait_until(|| !threads().is_empty()),
            "the driver must still receive the cancel"
        );
        assert!(
            threads().iter().all(|thread| *thread != ui_thread),
            "driver cancels must not run on the UI thread"
        );
    }

    #[gpui::test]
    fn cancel_query_returns_while_the_driver_cancel_blocks(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let gate = Arc::new(CancelGate::default());
        let (profile_id, cancel_threads) = harness.connect_blocking("prod", gate.clone());
        let query = harness.start_query(profile_id);
        harness.disconnect_active();

        let started = Instant::now();
        harness.dispatch(Command::Execute);
        let elapsed = started.elapsed();

        assert_eq!(harness.task_status(query), Some(TaskStatus::Cancelled));
        assert!(harness.is_connected(profile_id));
        assert_cancel_ran_off_the_ui_thread(elapsed, &gate, &cancel_threads);
    }

    #[gpui::test]
    fn disconnect_anyway_returns_while_the_driver_cancel_blocks(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let gate = Arc::new(CancelGate::default());
        let (profile_id, cancel_threads) = harness.connect_blocking("prod", gate.clone());
        let query = harness.start_query(profile_id);
        harness.disconnect_active();

        let started = Instant::now();
        harness.force();
        let elapsed = started.elapsed();

        assert_eq!(harness.task_status(query), Some(TaskStatus::Cancelled));
        assert!(!harness.is_connected(profile_id));
        assert_cancel_ran_off_the_ui_thread(elapsed, &gate, &cancel_threads);
    }

    #[gpui::test]
    fn quit_cancel_query_returns_while_the_driver_cancel_blocks(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let gate = Arc::new(CancelGate::default());
        let (profile_id, cancel_threads) = harness.connect_blocking("prod", gate.clone());
        let query = harness.start_query(profile_id);
        assert!(!harness.request_quit());

        let started = Instant::now();
        harness.dispatch(Command::Execute);
        let elapsed = started.elapsed();

        assert_eq!(harness.task_status(query), Some(TaskStatus::Cancelled));
        assert_cancel_ran_off_the_ui_thread(elapsed, &gate, &cancel_threads);
    }

    #[gpui::test]
    fn pressing_escape_on_the_prompt_keeps_waiting(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        let query = harness.start_query(profile_id);
        harness.disconnect_active();

        harness.window.simulate_keystrokes("escape");
        harness.window.run_until_parked();

        assert!(!harness.prompt_visible());
        assert!(harness.is_connected(profile_id));
        assert_eq!(harness.task_status(query), Some(TaskStatus::Running));
    }

    #[gpui::test]
    fn pressing_enter_on_the_prompt_cancels_the_query(cx: &mut TestAppContext) {
        let mut harness = new_harness(cx);
        let (profile_id, _) = harness.connect("prod");
        let query = harness.start_query(profile_id);
        harness.disconnect_active();

        harness.window.simulate_keystrokes("enter");
        harness.window.run_until_parked();

        assert!(!harness.prompt_visible());
        assert!(harness.is_connected(profile_id));
        assert_eq!(harness.task_status(query), Some(TaskStatus::Cancelled));
    }
}
