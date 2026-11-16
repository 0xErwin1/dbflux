//! A scripted SQL connection and an open wizard on it, for the flow tests.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dbflux_core::{
    CancelToken, Connection, DatabaseCategory, DbConfig, DbError, DbKind, DriverCapabilities,
    DriverMetadata, FormattedError, Icon, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TransferFamily, WritePrivilege,
};
use dbflux_ui_base::app_state_entity::AppStateEntity;
use gpui::{AppContext, Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;

use crate::import_wizard::ImportWizard;
use crate::keyboard_test_support::init_keyboard_runtime;

/// What the connection supports and speaks.
#[derive(Clone, Copy)]
pub(super) struct FakeKind {
    pub(super) language_is_sql: bool,
    pub(super) transactions: bool,
    pub(super) transactional_ddl: bool,
}

impl FakeKind {
    pub(super) const SQL: Self = Self {
        language_is_sql: true,
        transactions: true,
        transactional_ddl: true,
    };
}

type StatementHook = Box<dyn Fn(&str) + Send>;

/// Records every statement it is asked to run and fails the ones a test
/// marked.
pub(super) struct ScriptFake {
    metadata: DriverMetadata,
    transactional_ddl: bool,
    pub(super) executed: Mutex<Vec<String>>,
    failures: Mutex<Vec<(String, String)>>,
    /// Called with each statement before it runs, so a test can act in the
    /// middle of a run (press Cancel Run).
    during_run: Mutex<Option<StatementHook>>,
    pub(super) interrupts: AtomicUsize,
    fail_interrupt: std::sync::atomic::AtomicBool,
}

impl ScriptFake {
    pub(super) fn new(kind: FakeKind) -> Arc<Self> {
        let capabilities = if kind.transactions {
            DriverCapabilities::TRANSACTIONS
        } else {
            DriverCapabilities::empty()
        };

        Arc::new(Self {
            metadata: metadata(kind, capabilities),
            transactional_ddl: kind.transactional_ddl,
            executed: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            during_run: Mutex::new(None),
            interrupts: AtomicUsize::new(0),
            fail_interrupt: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Fails every statement that contains `needle` with `message`.
    pub(super) fn fail_on(&self, needle: &str, message: &str) {
        if let Ok(mut failures) = self.failures.lock() {
            failures.push((needle.to_string(), message.to_string()));
        }
    }

    pub(super) fn on_statement(&self, hook: impl Fn(&str) + Send + 'static) {
        if let Ok(mut during_run) = self.during_run.lock() {
            *during_run = Some(Box::new(hook));
        }
    }

    pub(super) fn executed(&self) -> Vec<String> {
        self.executed
            .lock()
            .map(|log| log.clone())
            .unwrap_or_default()
    }

    pub(super) fn fail_interrupts(&self) {
        self.fail_interrupt.store(true, Ordering::SeqCst);
    }

    pub(super) fn interrupts(&self) -> usize {
        self.interrupts.load(Ordering::SeqCst)
    }
}

fn metadata(kind: FakeKind, capabilities: DriverCapabilities) -> DriverMetadata {
    DriverMetadata {
        id: "script-fake".to_string(),
        display_name: "ScriptFake".to_string(),
        description: "test".to_string(),
        category: DatabaseCategory::Relational,
        transfer_family: TransferFamily::Sql,
        deployment_class: None,
        query_language: if kind.language_is_sql {
            QueryLanguage::Sql
        } else {
            QueryLanguage::MongoQuery
        },
        capabilities,
        default_port: None,
        uri_scheme: "test".to_string(),
        icon: Icon::Database,
        syntax: None,
        query: None,
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
    }
}

impl Connection for ScriptFake {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        if let Ok(mut log) = self.executed.lock() {
            log.push(req.sql.clone());
        }
        if let Ok(during_run) = self.during_run.lock()
            && let Some(hook) = during_run.as_ref()
        {
            hook(&req.sql);
        }

        let message = self.failures.lock().ok().and_then(|failures| {
            failures
                .iter()
                .find(|(needle, _)| req.sql.contains(needle.as_str()))
                .map(|(_, message)| message.clone())
        });

        match message {
            Some(message) => Err(DbError::QueryFailed(FormattedError::new(message))),
            None => Ok(QueryResult::empty()),
        }
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.interrupts.fetch_add(1, Ordering::SeqCst);
        if self.fail_interrupt.load(Ordering::SeqCst) {
            return Err(DbError::QueryFailed(FormattedError::new("no such query")));
        }
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(SchemaSnapshot::default())
    }

    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn supports_transactional_ddl(&self) -> bool {
        self.transactional_ddl
    }
}

/// A scratch directory that removes itself.
pub(super) struct Scratch(PathBuf);

impl Scratch {
    pub(super) fn new(tag: &str) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "dbflux_script_flow_{tag}_{}_{}",
            std::process::id(),
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&directory).expect("create scratch dir");

        Self(directory)
    }

    pub(super) fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, bytes).expect("write scratch file");

        path
    }

    /// A sparse file of `length` bytes.
    pub(super) fn sparse(&self, name: &str, length: u64) -> PathBuf {
        let path = self.0.join(name);
        let file = std::fs::File::create(&path).expect("create sparse file");
        file.set_len(length).expect("size sparse file");

        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

fn isolated_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(|cx| {
        cx.new(|_| {
            let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                .expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    })
}

/// An open wizard on `connection`, under the app keymap.
pub(super) struct Opened<'a> {
    pub(super) wizard: Entity<ImportWizard>,
    pub(super) window: &'a mut VisualTestContext,
}

pub(super) fn open_wizard(
    cx: &mut TestAppContext,
    connection: Arc<ScriptFake>,
    read_only_profile: bool,
) -> Opened<'_> {
    init_keyboard_runtime(cx);
    let app_state = isolated_app_state(cx);
    let profile_id = Uuid::new_v4();

    let mut profile =
        dbflux_core::ConnectionProfile::new("script-profile", DbConfig::default_postgres());
    profile.id = profile_id;
    profile.set_driver_id("builtin:script-fake");
    app_state.update(cx, |state, _| {
        state.apply_connect_profile(
            profile,
            connection,
            None,
            None,
            false,
            WritePrivilege::Unknown,
        );
        if read_only_profile && let Some(connected) = state.connections_mut().get_mut(&profile_id) {
            connected.profile.read_only_flag = true;
            connected.mutation_policy = dbflux_core::MutationPolicy::ReadOnly;
            connected.read_only_reason = Some(dbflux_core::ReadOnlyReason::ProfileSetting);
        }
    });

    let slot: Rc<RefCell<Option<Entity<ImportWizard>>>> = Rc::default();
    let capture = Rc::clone(&slot);
    let state = app_state.clone();
    let (_root, window) = cx.add_window_view(move |window, cx| {
        let wizard = cx.new(|cx| ImportWizard::new(state, cx));
        wizard.update(cx, |this, cx| this.open(profile_id, None, window, cx));
        *capture.borrow_mut() = Some(wizard.clone());
        gpui_component::Root::new(wizard, window, cx)
    });
    window.run_until_parked();

    let wizard = slot.borrow().clone().expect("wizard");
    Opened { wizard, window }
}

/// Picks `path` as the import file, as "Choose File…" does, and waits for
/// the script to be read.
pub(super) fn pick(opened: &mut Opened<'_>, path: PathBuf) {
    opened.wizard.update(opened.window, |this, cx| {
        let connection = this.resolve_connection(cx).expect("connection");
        this.open_picked_file(connection, path, cx);
    });
    opened.window.run_until_parked();
}

/// The cancel token a test trips from inside the connection.
pub(super) fn cancel_token_of(opened: &mut Opened<'_>) -> Option<CancelToken> {
    opened
        .window
        .update(|_, cx| opened.wizard.read(cx).cancel_token.clone())
}

/// Reads the wizard's state.
pub(super) fn read<R>(opened: &mut Opened<'_>, read: impl FnOnce(&ImportWizard) -> R) -> R {
    opened.window.update(|_, cx| read(opened.wizard.read(cx)))
}

/// A script of five statements: a create, two writes, a read and a delete
/// without a `WHERE`.
pub(super) const SCRIPT: &str = "CREATE TABLE t (id integer);\nINSERT INTO t VALUES (1);\nUPDATE t SET id = 2 WHERE id = 1;\nSELECT 1;\nDELETE FROM t;\n";

/// A script that only reads.
pub(super) const READ_ONLY_SCRIPT: &str = "SELECT 1;\nSELECT 2;\n";

/// A script of plain writes, none destructive.
pub(super) const WRITE_SCRIPT: &str =
    "INSERT INTO t VALUES (1);\nUPDATE t SET id = 2 WHERE id = 1;\n";

/// Makes the opened wizard's profile read-only, as the user can while the
/// wizard is open.
pub(super) fn make_read_only(opened: &mut Opened<'_>) {
    opened.window.update(|_, cx| {
        let wizard = opened.wizard.read(cx);
        let Some(profile_id) = wizard.profile_id else {
            return;
        };
        let state = wizard.app_state.clone();
        state.update(cx, |state, _| {
            if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                connected.profile.read_only_flag = true;
                connected.mutation_policy = dbflux_core::MutationPolicy::ReadOnly;
                connected.read_only_reason = Some(dbflux_core::ReadOnlyReason::ProfileSetting);
            }
        });
    });
}
