//! The single asynchronous coordinator behind the shared object tree.
//!
//! One [`ObjectTreeCoordinator`] is owned by each [`AppStateEntity`]; the
//! sidebar and the wizard adapters obtain it through the same entity, so
//! identical requests across consumers share one driver execution and all
//! subscribers are notified of every settle. The coordinator owns request
//! lifetime (task handles, attempt identity, per-database slot installs),
//! not whichever consumer asked first.
//!
//! All driver work goes through the session-fenced core seams only:
//! `prepare_fetch_database_list`, `prepare_fetch_explicit_database_schema`,
//! `prepare_fetch_table_details_fenced`, and the guarded
//! `prepare_database_connection_guarded` install for missing
//! `ConnectionPerDatabase` slots. Blocking work runs on the background
//! executor; fenced application happens on the foreground. A late result
//! from a retired or invalidated attempt can never clear a newer attempt's
//! pending state or write a stale snapshot: core apply fences reject it and
//! the coordinator releases the pending state and notifies subscribers.

use std::collections::HashMap;

use dbflux_core::{
    CacheKey, ConnectionResolutionError, DbError, StaleFetchReason, StaleInstallReason,
    TableDetailsPrepareError,
};
use gpui::Task;
use uuid::Uuid;

use crate::app_state_entity::AppStateEntity;
use crate::async_ext::AsyncUpdateResultExt;
use crate::user_error::{ErrorKind, UserFacingError, report_error};

/// Identifies one unit of shared driver work. Typed — never a string codec.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ObjectTreeRequestKey {
    /// The profile's database list (primary connection).
    DatabaseList { profile_id: Uuid },
    /// One database's schemas/tables.
    DatabaseSchema { profile_id: Uuid, database: String },
    /// One table's details (and dependents).
    TableDetails {
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        table: String,
    },
}

impl ObjectTreeRequestKey {
    /// The profile this request belongs to.
    pub fn profile_id(&self) -> Uuid {
        match self {
            ObjectTreeRequestKey::DatabaseList { profile_id } => *profile_id,
            ObjectTreeRequestKey::DatabaseSchema { profile_id, .. }
            | ObjectTreeRequestKey::TableDetails { profile_id, .. } => *profile_id,
        }
    }

    /// The hierarchy node whose loading state this request drives.
    pub fn node_key(&self) -> super::ObjectTreeKey {
        match self {
            ObjectTreeRequestKey::DatabaseList { profile_id } => super::ObjectTreeKey::Profile {
                profile_id: *profile_id,
            },
            ObjectTreeRequestKey::DatabaseSchema {
                profile_id,
                database,
            } => super::ObjectTreeKey::Database {
                profile_id: *profile_id,
                database: database.clone(),
            },
            ObjectTreeRequestKey::TableDetails {
                profile_id,
                database,
                schema,
                table,
            } => super::ObjectTreeKey::Table {
                profile_id: *profile_id,
                database: database.clone(),
                schema: schema.clone(),
                table: table.clone(),
            },
        }
    }
}

/// A missing `ConnectionPerDatabase` slot being prepared. Waiters (schema
/// and table-details requests for that database) share one preparation and
/// all resume once the slot exists.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectTreeInstallKey {
    pub profile_id: Uuid,
    pub database: String,
}

/// Result of one settled request, exposed to subscribers and to
/// `object_tree_outcome` for retry UIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectTreeOutcome {
    /// The fenced apply accepted the result; core caches were updated.
    Applied,
    /// The data was already cached; no driver work was needed.
    Cached,
    /// The request was cancelled (explicitly or via profile cleanup) before
    /// completing.
    Cancelled,
    /// The result was stale (session replaced, invalidated, or target slot
    /// churned) and was NOT applied; pending state was released so the node
    /// never sticks in Loading.
    Rejected(ObjectTreeRejection),
    /// The request failed. Reported once through the centralized user-error
    /// seam by the coordinator; consumer adapters must not double-report.
    Failed(String),
}

/// Why a fenced apply rejected a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectTreeRejection {
    ProfileDisconnected,
    ConnectionReplaced,
    RequestInvalidated,
    TargetSlotReplaced,
}

impl From<StaleFetchReason> for ObjectTreeRejection {
    fn from(reason: StaleFetchReason) -> Self {
        match reason {
            StaleFetchReason::ProfileDisconnected => ObjectTreeRejection::ProfileDisconnected,
            StaleFetchReason::ConnectionReplaced => ObjectTreeRejection::ConnectionReplaced,
            StaleFetchReason::RequestInvalidated => ObjectTreeRejection::RequestInvalidated,
        }
    }
}

impl From<StaleInstallReason> for ObjectTreeRejection {
    fn from(reason: StaleInstallReason) -> Self {
        match reason {
            StaleInstallReason::ProfileDisconnected => ObjectTreeRejection::ProfileDisconnected,
            StaleInstallReason::ConnectionReplaced => ObjectTreeRejection::ConnectionReplaced,
            StaleInstallReason::RequestInvalidated => ObjectTreeRejection::RequestInvalidated,
            StaleInstallReason::TargetSlotReplaced => ObjectTreeRejection::TargetSlotReplaced,
        }
    }
}

/// Status returned when a consumer asks the coordinator for a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectTreeRequestStatus {
    /// Already cached — consumers can project immediately.
    Cached,
    /// A new attempt was started by this call.
    Dispatched,
    /// Already in flight (deduplicated; no new driver work).
    Pending,
    /// Waiting for a missing per-database connection slot to be installed;
    /// the fetch resumes automatically when the slot exists.
    WaitingForSlotInstall,
    /// The request could not even be prepared. Reported once by the
    /// coordinator; adapters must not double-report.
    Failed(String),
}

/// Emitted from the owning `AppStateEntity` whenever a request settles, so
/// every consumer (sidebar, wizard, others) reacts to the same signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTreeEvent {
    pub key: ObjectTreeRequestKey,
    pub outcome: ObjectTreeOutcome,
}

struct PendingFetch {
    attempt: u64,
    session_generation: Option<u64>,
    task: Option<Task<()>>,
    deferred_install: Option<ObjectTreeInstallKey>,
}

struct PendingInstall {
    attempt: u64,
    session_generation: Option<u64>,
    task: Option<Task<()>>,
    waiters: Vec<ObjectTreeRequestKey>,
}

/// The per-entity shared coordinator. Held as a field on `AppStateEntity`;
/// consumers never construct their own.
#[derive(Default)]
pub struct ObjectTreeCoordinator {
    next_attempt: u64,
    fetches: HashMap<ObjectTreeRequestKey, PendingFetch>,
    installs: HashMap<ObjectTreeInstallKey, PendingInstall>,
    outcomes: HashMap<ObjectTreeRequestKey, ObjectTreeOutcome>,
}

impl ObjectTreeCoordinator {
    fn next_attempt(&mut self) -> u64 {
        self.next_attempt += 1;
        self.next_attempt
    }

    fn is_fetch_pending(&self, key: &ObjectTreeRequestKey) -> bool {
        self.fetches.contains_key(key)
    }

    fn fetch_deferred_install(&self, key: &ObjectTreeRequestKey) -> Option<&ObjectTreeInstallKey> {
        self.fetches
            .get(key)
            .and_then(|fetch| fetch.deferred_install.as_ref())
    }

    fn fetch_attempt(&self, key: &ObjectTreeRequestKey) -> Option<u64> {
        self.fetches.get(key).map(|fetch| fetch.attempt)
    }

    /// Registers a new attempt for a request. Any previous entry must have
    /// been removed first; the registry never holds two attempts for one
    /// request.
    fn begin_fetch(
        &mut self,
        key: ObjectTreeRequestKey,
        deferred: Option<ObjectTreeInstallKey>,
        session_generation: Option<u64>,
    ) -> u64 {
        let attempt = self.next_attempt();
        self.fetches.insert(
            key,
            PendingFetch {
                attempt,
                session_generation,
                task: None,
                deferred_install: deferred,
            },
        );
        attempt
    }

    fn set_fetch_task(&mut self, key: &ObjectTreeRequestKey, task: Task<()>) {
        if let Some(fetch) = self.fetches.get_mut(key) {
            fetch.task = Some(task);
        }
    }

    fn set_install_task(&mut self, install_key: &ObjectTreeInstallKey, task: Task<()>) {
        if let Some(install) = self.installs.get_mut(install_key) {
            install.task = Some(task);
        }
    }

    /// Removes a fetch entry, dropping (and thereby cancelling) its task.
    fn remove_fetch(&mut self, key: &ObjectTreeRequestKey) -> Option<PendingFetch> {
        self.fetches.remove(key)
    }

    fn fetch_keys(&self) -> impl Iterator<Item = &ObjectTreeRequestKey> {
        self.fetches.keys()
    }

    fn install_pending(&self, install_key: &ObjectTreeInstallKey) -> bool {
        self.installs.contains_key(install_key)
    }

    fn install_attempt(&self, install_key: &ObjectTreeInstallKey) -> Option<u64> {
        self.installs
            .get(install_key)
            .map(|install| install.attempt)
    }

    fn begin_install(
        &mut self,
        install_key: &ObjectTreeInstallKey,
        session_generation: Option<u64>,
    ) -> u64 {
        let attempt = self.next_attempt();
        self.installs.insert(
            install_key.clone(),
            PendingInstall {
                attempt,
                session_generation,
                task: None,
                waiters: Vec::new(),
            },
        );
        attempt
    }

    fn add_waiter(&mut self, install_key: &ObjectTreeInstallKey, key: ObjectTreeRequestKey) {
        if let Some(install) = self.installs.get_mut(install_key)
            && !install.waiters.contains(&key)
        {
            install.waiters.push(key);
        }
    }

    fn remove_waiter(&mut self, install_key: &ObjectTreeInstallKey, key: &ObjectTreeRequestKey) {
        if let Some(install) = self.installs.get_mut(install_key) {
            install.waiters.retain(|waiter| waiter != key);
        }
    }

    fn install_has_waiters(&self, install_key: &ObjectTreeInstallKey) -> bool {
        self.installs
            .get(install_key)
            .is_some_and(|install| !install.waiters.is_empty())
    }

    /// Removes an install entry, dropping (and thereby cancelling) its task.
    fn take_install(&mut self, install_key: &ObjectTreeInstallKey) -> Option<PendingInstall> {
        self.installs.remove(install_key)
    }

    fn install_keys(&self) -> impl Iterator<Item = &ObjectTreeInstallKey> {
        self.installs.keys()
    }

    fn set_outcome(&mut self, key: ObjectTreeRequestKey, outcome: ObjectTreeOutcome) {
        self.outcomes.insert(key, outcome);
    }

    fn outcome(&self, key: &ObjectTreeRequestKey) -> Option<&ObjectTreeOutcome> {
        self.outcomes.get(key)
    }
}

// --- Public consumer API (orchestrated on the owning entity) ---

impl AppStateEntity {
    /// Requests one unit of shared driver work through the coordinator.
    ///
    /// Returns [`ObjectTreeRequestStatus::Cached`] when the core caches
    /// already answer the request, deduplicates identical in-flight
    /// requests, defers behind a shared slot install for missing
    /// `ConnectionPerDatabase` targets, and reports failures once.
    pub fn object_tree_request(
        &mut self,
        key: ObjectTreeRequestKey,
        cx: &mut gpui::Context<Self>,
    ) -> ObjectTreeRequestStatus {
        if self.object_tree.is_fetch_pending(&key) {
            return if self.object_tree.fetch_deferred_install(&key).is_some() {
                ObjectTreeRequestStatus::WaitingForSlotInstall
            } else {
                ObjectTreeRequestStatus::Pending
            };
        }

        if self.object_tree_cache_hit(&key) {
            self.object_tree_settle(key.clone(), ObjectTreeOutcome::Cached, cx);
            return ObjectTreeRequestStatus::Cached;
        }

        self.object_tree_dispatch(key, cx)
    }

    /// Cancels the current attempt (if any) and dispatches a fresh one.
    pub fn object_tree_retry(
        &mut self,
        key: ObjectTreeRequestKey,
        cx: &mut gpui::Context<Self>,
    ) -> ObjectTreeRequestStatus {
        self.object_tree_cancel(&key, cx);
        self.object_tree_request(key, cx)
    }

    /// Cancels one request: drops its task, releases pending state, and
    /// notifies subscribers with [`ObjectTreeOutcome::Cancelled`]. A late
    /// completion from a cancelled attempt cannot settle (attempt identity
    /// plus task cancellation).
    pub fn object_tree_cancel(&mut self, key: &ObjectTreeRequestKey, cx: &mut gpui::Context<Self>) {
        let Some(entry) = self.object_tree.remove_fetch(key) else {
            return;
        };
        drop(entry.task);
        if let Some(install_key) = entry.deferred_install {
            self.object_tree.remove_waiter(&install_key, key);
            if !self.object_tree.install_has_waiters(&install_key)
                && let Some(install) = self.object_tree.take_install(&install_key)
            {
                drop(install.task);
            }
        }
        self.object_tree_settle(key.clone(), ObjectTreeOutcome::Cancelled, cx);
    }

    /// Releases every pending request and install of one profile (call when
    /// the profile disconnects). Pending state is released and subscribers
    /// are notified; nothing is reported as an error.
    pub fn object_tree_cancel_profile(&mut self, profile_id: Uuid, cx: &mut gpui::Context<Self>) {
        let fetches: Vec<ObjectTreeRequestKey> = self
            .object_tree
            .fetch_keys()
            .filter(|key| key.profile_id() == profile_id)
            .cloned()
            .collect();
        for key in fetches {
            self.object_tree_cancel(&key, cx);
        }
        // Any surviving install for this profile has no waiters left; its
        // task is cancelled so a late install cannot land after cleanup.
        let installs: Vec<ObjectTreeInstallKey> = self
            .object_tree
            .install_keys()
            .filter(|key| key.profile_id == profile_id)
            .cloned()
            .collect();
        for install_key in installs {
            if let Some(install) = self.object_tree.take_install(&install_key) {
                drop(install.task);
            }
        }
    }

    /// Whether the request is currently in flight (including deferred
    /// behind a slot install).
    pub fn object_tree_is_pending(&self, key: &ObjectTreeRequestKey) -> bool {
        self.object_tree.is_fetch_pending(key)
    }

    /// The generation captured when a pending attempt began. The outer `None`
    /// means no attempt; `Some(None)` means an attempt without a connected session.
    /// Reading this does not cancel or mutate shared work.
    pub fn object_tree_pending_session_generation(
        &self,
        key: &ObjectTreeRequestKey,
    ) -> Option<Option<u64>> {
        self.object_tree
            .fetches
            .get(key)
            .map(|fetch| fetch.session_generation)
    }

    /// The last settled outcome for a request, if any.
    pub fn object_tree_outcome(&self, key: &ObjectTreeRequestKey) -> Option<&ObjectTreeOutcome> {
        self.object_tree.outcome(key)
    }
}

// --- Dispatch and settlement (foreground, inside the owning entity) ---

impl AppStateEntity {
    /// Whether the core caches already answer the request. Kept in sync with
    /// the prepare gates in the core seams (a cached request is rejected at
    /// prepare time, so the coordinator asks before preparing).
    fn object_tree_cache_hit(&self, key: &ObjectTreeRequestKey) -> bool {
        match key {
            ObjectTreeRequestKey::DatabaseList { profile_id } => {
                self.get_database_list(*profile_id).is_some()
            }
            ObjectTreeRequestKey::DatabaseSchema {
                profile_id,
                database,
            } => self.connections().get(profile_id).is_some_and(|connected| {
                connected.cache_contains(&CacheKey::database_schema(database.clone()))
            }),
            ObjectTreeRequestKey::TableDetails {
                profile_id,
                database,
                schema,
                table,
            } => self
                .connections()
                .get(profile_id)
                .and_then(|connected| {
                    connected
                        .table_details
                        .get(&(database.clone(), schema.clone(), table.clone()))
                })
                .is_some_and(|details| {
                    details.columns.is_some() || details.sample_fields.is_some()
                }),
        }
    }

    fn object_tree_dispatch(
        &mut self,
        key: ObjectTreeRequestKey,
        cx: &mut gpui::Context<Self>,
    ) -> ObjectTreeRequestStatus {
        let profile_id = key.profile_id();
        if !self.connections().contains_key(&profile_id) {
            let message = "Profile not connected".to_string();
            self.object_tree_settle(key, ObjectTreeOutcome::Failed(message.clone()), cx);
            return ObjectTreeRequestStatus::Failed(message);
        }

        match &key {
            ObjectTreeRequestKey::DatabaseList { .. } => {
                self.object_tree_dispatch_database_list(key, cx)
            }
            ObjectTreeRequestKey::DatabaseSchema {
                profile_id,
                database,
            } => {
                let database = database.clone();
                let key = key.clone();
                // Structured missing-target detection through the core seam:
                // a ConnectionPerDatabase target without a prepared slot is
                // installed first, then the original request is retried. No
                // error text is parsed and no active-context switch happens.
                let missing = self.connections().get(profile_id).and_then(|connected| {
                    match connected.resolve_connection_for_execution(Some(&database)) {
                        Err(ConnectionResolutionError::PendingDatabaseConnection { database }) => {
                            Some(database)
                        }
                        Ok(_) => None,
                    }
                });
                if let Some(database) = missing {
                    let install_key = ObjectTreeInstallKey {
                        profile_id: *profile_id,
                        database,
                    };
                    return self.object_tree_defer_behind_install(key, install_key, cx);
                }
                self.object_tree_dispatch_database_schema(key, database, cx)
            }
            ObjectTreeRequestKey::TableDetails {
                profile_id,
                database,
                schema,
                table,
            } => {
                let (database, schema, table) = (database.clone(), schema.clone(), table.clone());
                match self.prepare_fetch_table_details_fenced(
                    *profile_id,
                    &database,
                    schema.as_deref(),
                    &table,
                ) {
                    Err(TableDetailsPrepareError::AlreadyCached) => {
                        self.object_tree_settle(key.clone(), ObjectTreeOutcome::Cached, cx);
                        ObjectTreeRequestStatus::Cached
                    }
                    Err(TableDetailsPrepareError::PendingDatabaseConnection {
                        database: missing,
                    }) => {
                        let install_key = ObjectTreeInstallKey {
                            profile_id: *profile_id,
                            database: missing,
                        };
                        self.object_tree_defer_behind_install(key, install_key, cx)
                    }
                    Err(TableDetailsPrepareError::ProfileDisconnected) => {
                        // Not a user-triggered failure to report; the profile
                        // is simply gone. Pending is released and consumers
                        // may retry.
                        self.object_tree_settle(
                            key.clone(),
                            ObjectTreeOutcome::Failed("Profile not connected".to_string()),
                            cx,
                        );
                        ObjectTreeRequestStatus::Failed("Profile not connected".to_string())
                    }
                    Ok(params) => {
                        let task_key = key.clone();
                        let generation = self.profile_session_generation(*profile_id);
                        let attempt = self.object_tree.begin_fetch(key.clone(), None, generation);
                        let task = cx.spawn(async move |this, cx| {
                            let result = cx
                                .background_executor()
                                .spawn(async move { params.execute() })
                                .await;
                            this.update(cx, |state, cx| {
                                state
                                    .object_tree_settle_table_details(task_key, attempt, result, cx)
                            })
                            .log_if_dropped();
                        });
                        self.object_tree.set_fetch_task(&key, task);
                        ObjectTreeRequestStatus::Dispatched
                    }
                }
            }
        }
    }

    fn object_tree_dispatch_database_list(
        &mut self,
        key: ObjectTreeRequestKey,
        cx: &mut gpui::Context<Self>,
    ) -> ObjectTreeRequestStatus {
        let profile_id = key.profile_id();
        match self.prepare_fetch_database_list(profile_id) {
            Err(error) => {
                let message = format!("Failed to prepare the database list load: {error}");
                self.object_tree_report_failure("Failed to load the database list", error, cx);
                self.object_tree_settle(key, ObjectTreeOutcome::Failed(message.clone()), cx);
                ObjectTreeRequestStatus::Failed(message)
            }
            Ok(params) => {
                let task_key = key.clone();
                let generation = self.profile_session_generation(profile_id);
                let attempt = self.object_tree.begin_fetch(key.clone(), None, generation);
                let task = cx.spawn(async move |this, cx| {
                    let result = cx
                        .background_executor()
                        .spawn(async move { params.execute() })
                        .await;
                    this.update(cx, |state, cx| {
                        state.object_tree_settle_database_list(task_key, attempt, result, cx)
                    })
                    .log_if_dropped();
                });
                self.object_tree.set_fetch_task(&key, task);
                ObjectTreeRequestStatus::Dispatched
            }
        }
    }

    fn object_tree_dispatch_database_schema(
        &mut self,
        key: ObjectTreeRequestKey,
        database: String,
        cx: &mut gpui::Context<Self>,
    ) -> ObjectTreeRequestStatus {
        let profile_id = key.profile_id();
        match self.prepare_fetch_explicit_database_schema(profile_id, &database) {
            Err(error) => {
                let message = format!("Failed to prepare the schema load: {error}");
                self.object_tree_report_failure("Failed to load the database schema", error, cx);
                self.object_tree_settle(key, ObjectTreeOutcome::Failed(message.clone()), cx);
                ObjectTreeRequestStatus::Failed(message)
            }
            Ok(params) => {
                let task_key = key.clone();
                let generation = self.profile_session_generation(profile_id);
                let attempt = self.object_tree.begin_fetch(key.clone(), None, generation);
                let task = cx.spawn(async move |this, cx| {
                    let result = cx
                        .background_executor()
                        .spawn(async move { params.execute() })
                        .await;
                    this.update(cx, |state, cx| {
                        state.object_tree_settle_database_schema(task_key, attempt, result, cx)
                    })
                    .log_if_dropped();
                });
                self.object_tree.set_fetch_task(&key, task);
                ObjectTreeRequestStatus::Dispatched
            }
        }
    }

    /// Defers a fetch behind the shared per-database slot installation.
    /// The first waiting request starts the guarded install; further
    /// waiters join it and all resume once the slot exists.
    fn object_tree_defer_behind_install(
        &mut self,
        key: ObjectTreeRequestKey,
        install_key: ObjectTreeInstallKey,
        cx: &mut gpui::Context<Self>,
    ) -> ObjectTreeRequestStatus {
        let generation = self.profile_session_generation(install_key.profile_id);
        if self
            .object_tree
            .installs
            .get(&install_key)
            .is_some_and(|install| install.session_generation != generation)
            && let Some(old) = self.object_tree.take_install(&install_key)
        {
            for waiter in old.waiters {
                if let Some(rejection) = self.object_tree_stale_session(&waiter) {
                    self.object_tree_settle(waiter, ObjectTreeOutcome::Rejected(rejection), cx);
                }
            }
        }
        if !self.object_tree.install_pending(&install_key) {
            match self
                .prepare_database_connection_guarded(install_key.profile_id, &install_key.database)
            {
                Err(error) => {
                    let message = format!("Failed to prepare the database connection: {error}");
                    self.object_tree_report_failure("Failed to open the database", error, cx);
                    self.object_tree_settle(key, ObjectTreeOutcome::Failed(message.clone()), cx);
                    return ObjectTreeRequestStatus::Failed(message);
                }
                Ok(install) => {
                    let attempt = self.object_tree.begin_install(&install_key, generation);
                    let task_key = install_key.clone();
                    let task = cx.spawn(async move |this, cx| {
                        let result = cx
                            .background_executor()
                            .spawn(async move { install.execute() })
                            .await;
                        this.update(cx, |state, cx| {
                            state.object_tree_settle_install(task_key, attempt, result, cx)
                        })
                        .log_if_dropped();
                    });
                    self.object_tree.set_install_task(&install_key, task);
                }
            }
        }

        self.object_tree
            .begin_fetch(key.clone(), Some(install_key.clone()), generation);
        self.object_tree.add_waiter(&install_key, key);
        ObjectTreeRequestStatus::WaitingForSlotInstall
    }

    /// Stores the final outcome, releases the pending entry and notifies
    /// every subscriber. Never reports: reporting happens once at the first
    /// catch site.
    fn object_tree_settle(
        &mut self,
        key: ObjectTreeRequestKey,
        outcome: ObjectTreeOutcome,
        cx: &mut gpui::Context<Self>,
    ) {
        self.object_tree.remove_fetch(&key);
        self.object_tree.set_outcome(key.clone(), outcome.clone());
        cx.emit(ObjectTreeEvent { key, outcome });
    }

    fn object_tree_stale_session(&self, key: &ObjectTreeRequestKey) -> Option<ObjectTreeRejection> {
        let captured = self.object_tree.fetches.get(key)?.session_generation;
        let current = self.profile_session_generation(key.profile_id());
        if captured == current {
            None
        } else if current.is_none() {
            Some(ObjectTreeRejection::ProfileDisconnected)
        } else {
            Some(ObjectTreeRejection::ConnectionReplaced)
        }
    }

    fn object_tree_settle_database_list(
        &mut self,
        key: ObjectTreeRequestKey,
        attempt: u64,
        result: Result<dbflux_core::FetchedDatabaseList, DbError>,
        cx: &mut gpui::Context<Self>,
    ) {
        // A retired/cancelled attempt must not touch a newer attempt's
        // pending state; core apply would fence its write anyway.
        if self.object_tree.fetch_attempt(&key) != Some(attempt) {
            return;
        }
        if let Some(rejection) = self.object_tree_stale_session(&key) {
            self.object_tree_settle(key, ObjectTreeOutcome::Rejected(rejection), cx);
            return;
        }
        let outcome = match result {
            Ok(fetched) => match self.apply_fetch_database_list(fetched) {
                dbflux_core::ApplyFetchOutcome::Applied => ObjectTreeOutcome::Applied,
                dbflux_core::ApplyFetchOutcome::Rejected(reason) => {
                    ObjectTreeOutcome::Rejected(reason.into())
                }
            },
            Err(error) => {
                let message = format!("Failed to load the database list: {error}");
                self.object_tree_report_failure(
                    "Failed to load the database list",
                    format!("{error}"),
                    cx,
                );
                ObjectTreeOutcome::Failed(message)
            }
        };
        self.object_tree_settle(key, outcome, cx);
    }

    fn object_tree_settle_database_schema(
        &mut self,
        key: ObjectTreeRequestKey,
        attempt: u64,
        result: Result<dbflux_core::FetchedExplicitDatabaseSchema, DbError>,
        cx: &mut gpui::Context<Self>,
    ) {
        if self.object_tree.fetch_attempt(&key) != Some(attempt) {
            return;
        }
        if let Some(rejection) = self.object_tree_stale_session(&key) {
            self.object_tree_settle(key, ObjectTreeOutcome::Rejected(rejection), cx);
            return;
        }
        let outcome = match result {
            Ok(fetched) => match self.apply_fetch_explicit_database_schema(fetched) {
                dbflux_core::ApplyFetchOutcome::Applied => ObjectTreeOutcome::Applied,
                dbflux_core::ApplyFetchOutcome::Rejected(reason) => {
                    ObjectTreeOutcome::Rejected(reason.into())
                }
            },
            Err(error) => {
                let message = format!("Failed to load the database schema: {error}");
                self.object_tree_report_failure(
                    "Failed to load the database schema",
                    format!("{error}"),
                    cx,
                );
                ObjectTreeOutcome::Failed(message)
            }
        };
        self.object_tree_settle(key, outcome, cx);
    }

    fn object_tree_settle_table_details(
        &mut self,
        key: ObjectTreeRequestKey,
        attempt: u64,
        result: Result<dbflux_core::FetchedTableDetails, DbError>,
        cx: &mut gpui::Context<Self>,
    ) {
        if self.object_tree.fetch_attempt(&key) != Some(attempt) {
            return;
        }
        if let Some(rejection) = self.object_tree_stale_session(&key) {
            self.object_tree_settle(key, ObjectTreeOutcome::Rejected(rejection), cx);
            return;
        }
        let outcome = match result {
            Ok(fetched) => match self.apply_fetched_table_details(fetched) {
                dbflux_core::ApplyFetchOutcome::Applied => ObjectTreeOutcome::Applied,
                dbflux_core::ApplyFetchOutcome::Rejected(reason) => {
                    ObjectTreeOutcome::Rejected(reason.into())
                }
            },
            Err(error) => {
                let message = format!("Failed to load the table details: {error}");
                self.object_tree_report_failure(
                    "Failed to load the table details",
                    format!("{error}"),
                    cx,
                );
                ObjectTreeOutcome::Failed(message)
            }
        };
        self.object_tree_settle(key, outcome, cx);
    }

    /// Settles a guarded per-database connection installation and resumes
    /// (or fails) every dependent request waiting for the slot.
    fn object_tree_settle_install(
        &mut self,
        install_key: ObjectTreeInstallKey,
        attempt: u64,
        result: Result<dbflux_core::GuardedInstalledDatabaseConnection, String>,
        cx: &mut gpui::Context<Self>,
    ) {
        if self.object_tree.install_attempt(&install_key) != Some(attempt) {
            return;
        }
        let Some(install_entry) = self.object_tree.take_install(&install_key) else {
            return;
        };

        let waiters = install_entry.waiters;
        let (stale, waiters): (Vec<_>, Vec<_>) = waiters
            .into_iter()
            .partition(|waiter| self.object_tree_stale_session(waiter).is_some());
        for waiter in stale {
            if let Some(rejection) = self.object_tree_stale_session(&waiter) {
                self.object_tree_settle(waiter, ObjectTreeOutcome::Rejected(rejection), cx);
            }
        }
        if waiters.is_empty() {
            return;
        }
        match result {
            Ok(installed) => {
                // `None` means the waiters may resume against a usable
                // target; `Some` settles every waiter with that rejection.
                let rejection = match self.apply_guarded_database_connection(installed) {
                    dbflux_core::InstallDatabaseConnectionOutcome::Installed => None,
                    // The slot churned while the install was in flight. It
                    // may be genuinely present again (installed by someone
                    // else) or it may have been added and removed again
                    // (ABA). Resume the waiters only when structured
                    // resolution proves a usable target exists right now;
                    // otherwise settling them Rejected avoids silently
                    // starting yet another install without an explicit
                    // request.
                    dbflux_core::InstallDatabaseConnectionOutcome::Rejected(
                        StaleInstallReason::TargetSlotReplaced,
                    ) => {
                        let usable_now = self
                            .connections()
                            .get(&install_key.profile_id)
                            .is_some_and(|connected| {
                                connected
                                    .resolve_connection_for_execution(Some(&install_key.database))
                                    .is_ok()
                            });
                        (!usable_now).then_some(ObjectTreeRejection::TargetSlotReplaced)
                    }
                    dbflux_core::InstallDatabaseConnectionOutcome::Rejected(reason) => {
                        Some(reason.into())
                    }
                };
                match rejection {
                    None => {
                        for waiter in waiters {
                            self.object_tree.remove_fetch(&waiter);
                            self.object_tree_request(waiter, cx);
                        }
                    }
                    Some(rejection) => {
                        for waiter in &waiters {
                            self.object_tree.remove_fetch(waiter);
                            self.object_tree_settle(
                                waiter.clone(),
                                ObjectTreeOutcome::Rejected(rejection),
                                cx,
                            );
                        }
                    }
                }
            }
            Err(error) => {
                // One shared preparation failing is reported once; every
                // waiter resolves to the same failure without re-reporting.
                self.object_tree_report_failure("Failed to open the database", error.clone(), cx);
                let message = format!("Failed to open the database: {error}");
                for waiter in waiters {
                    self.object_tree.remove_fetch(&waiter);
                    self.object_tree_settle(waiter, ObjectTreeOutcome::Failed(message.clone()), cx);
                }
            }
        }
    }

    /// Reports a user-triggered failure once through the centralized seam.
    ///
    /// `report_error` resolves `AppStateGlobal` and updates the owning
    /// `AppStateEntity` synchronously. Every coordinator path runs inside an
    /// update of that same entity, so reporting directly would re-enter an
    /// active lease and panic. The report is therefore deferred through
    /// [`gpui::App::defer`], whose callback runs in a plain `&mut App`
    /// context after the owner update (and its lease) has fully ended —
    /// still exactly once, still driving toast, audit row, and error badge.
    /// Embeds/tests without a toast host degrade to a log entry; consumer
    /// adapters must not report the same failure again.
    fn object_tree_report_failure(
        &mut self,
        summary: impl Into<String>,
        cause: String,
        cx: &mut gpui::Context<Self>,
    ) {
        if !cx.has_global::<crate::toast::ToastGlobal>() {
            log::error!("object tree load failed: {}: {}", summary.into(), cause);
            return;
        }
        let error = UserFacingError::new(ErrorKind::Network, summary).with_cause(cause);
        cx.defer(move |cx| report_error(error, cx));
    }
}

#[cfg(test)]
mod tests;
