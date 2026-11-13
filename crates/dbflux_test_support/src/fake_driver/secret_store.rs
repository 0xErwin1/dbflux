use super::*;

/// The calls the fake secret stores received, as `(store_id, argument)`.
#[derive(Debug, Clone, Default)]
pub struct FakeSecretStoreCalls {
    pub list_requests: Vec<(String, SecretListRequest)>,
    pub described: Vec<(String, String)>,
    pub value_requests: Vec<(String, String)>,
    /// Every `get_value` call, as `(store_id, name, version)`: the version
    /// the caller asked for, `None` for the current value.
    pub value_version_requests: Vec<(String, String, Option<String>)>,
    /// Every `put_value` attempt, accepted or not, as `(store_id, name)`. A
    /// refused write must not reach the store, so a test watches this list
    /// and never the payload.
    pub put_attempts: Vec<(String, String)>,
    /// Only the `put_value` requests the store accepted, as
    /// `(store_id, request)`; rejected attempts are in `put_attempts`. The
    /// payload travels as the `secrecy` type `SecretPayload`, so it stays
    /// redacted in `Debug` output.
    pub put_requests: Vec<(String, SecretPutRequest)>,
    /// Every `list_versions` call, as `(store_id, name)`.
    pub version_listings: Vec<(String, String)>,
    /// Every write operation other than `put_value` that reached the store,
    /// accepted or not, as `(store_id, request)`. A refused operation must
    /// not reach the store, so a test watches this list.
    pub mutation_attempts: Vec<(String, FakeSecretMutation)>,
    /// The operations of `mutation_attempts` the modeled service accepted.
    pub mutations: Vec<(String, FakeSecretMutation)>,
}

/// One write operation the fake secret stores received, with its request.
/// Requests that carry a value hold it as a `secrecy` type, so `Debug`
/// output stays redacted.
#[derive(Debug, Clone)]
pub enum FakeSecretMutation {
    RollBack(SecretRollbackRequest),
    Delete(SecretDeleteRequest),
    Restore { name: String },
    RotateNow { name: String },
    UpdateRotation(SecretRotationUpdate),
    UpdateTags(SecretTagsUpdate),
    UpdateDescription(SecretDescriptionUpdate),
    Create(SecretCreateRequest),
}

/// The rotation function the fake flat store's rotating secret reports.
pub const FAKE_ROTATION_FUNCTION: &str =
    "arn:aws:lambda:us-east-1:123456789012:function:rotate-api-credentials";

/// Id of the fake flat store, shaped like AWS Secrets Manager.
pub const FAKE_SECRETS_STORE_ID: &str = "secrets-manager";

/// Id of the fake hierarchical store, shaped like SSM Parameter Store.
pub const FAKE_PARAMETER_STORE_ID: &str = "parameter-store";

/// Value the fake flat store returns for its key/value secret's `password`.
pub const FAKE_SECRET_PASSWORD: &str = "fake-password-7f3a";

/// Value the fake flat store returns for its plaintext secret.
pub const FAKE_SECRET_PLAINTEXT: &str = "fake plaintext token 91c2";

/// Bytes the fake flat store returns for its binary secret.
pub const FAKE_SECRET_BINARY: &[u8] = &[0xde, 0xad, 0xbe, 0xef, 0x00, 0x42];

/// Error the fake parameter store returns for its `SecureString`.
pub const FAKE_KMS_DENIED_MESSAGE: &str =
    "User is not authorized to perform kms:Decrypt on key alias/app-prod";

#[derive(Clone)]
enum FakeSecretPayload {
    Text(String),
    Binary(Vec<u8>),
}

impl FakeSecretPayload {
    /// The stored form of a payload the seam delivered. Values live only in
    /// the fake's in-memory fixtures, never in a log.
    fn for_write(payload: &SecretPayload) -> Self {
        match payload {
            SecretPayload::Text(text) => Self::Text(text.expose_secret().to_owned()),
            SecretPayload::Binary(bytes) => Self::Binary(bytes.expose_secret().to_vec()),
        }
    }
}

/// Why a read of a secret fails, as its fixture models the service state or
/// a policy: independently of the value a write stores. A decrypt denial is
/// about the caller's authorization, so an overwrite does not clear it.
#[derive(Clone)]
enum FakeSecretReadFailure {
    Denied(&'static str),
    Unavailable(&'static str),
}

#[derive(Clone)]
struct FakeSecret {
    details: SecretDetails,
    /// The payload each version carries: the fixture seeds one entry keyed
    /// by the summary's version, and every accepted write adds its own, so
    /// a written version answers by its id for `Pending` and `Current`
    /// alike.
    versions: Vec<(String, FakeSecretPayload)>,
    /// The metadata `list_versions` reports, newest first. Carries no
    /// payload, like the seam type; every id here has its payload in
    /// `versions`.
    history: Vec<SecretVersionInfo>,
    /// Why a read of this secret fails, decided at fixture time and never
    /// changed by a write: read authorization is independent of the stored
    /// value.
    read_failure: Option<FakeSecretReadFailure>,
}

/// A deterministic in-memory secret store. A flat store matches its name
/// filter as a prefix, a hierarchical one as a substring of the full name.
/// Writes mutate the connection's own copy: a fake connection rebuilt later
/// sees its fixtures again, while reads and writes through one connection
/// agree.
pub(super) struct FakeSecretStore {
    descriptor: SecretStoreDescriptor,
    secrets: RwLock<Vec<FakeSecret>>,
    default_page_size: usize,
    written_versions: AtomicUsize,
    state: Arc<FakeDriverState>,
}

impl FakeSecretStore {
    fn record_list(&self, request: &SecretListRequest) {
        mutex_lock(&self.state.secret_store_calls)
            .list_requests
            .push((self.descriptor.id.clone(), request.clone()));
    }

    fn hierarchical(&self) -> bool {
        self.descriptor
            .supports(SecretStoreCapabilities::HIERARCHICAL)
    }

    fn matches_filter(&self, name: &str, filter: Option<&str>) -> bool {
        match filter.map(str::trim).filter(|filter| !filter.is_empty()) {
            None => true,
            Some(filter) if self.hierarchical() => name.contains(filter),
            Some(filter) => name.starts_with(filter),
        }
    }

    /// Entries of one listing before paging, and the sub-paths found under
    /// the requested path.
    fn listing(&self, request: &SecretListRequest) -> (Vec<FakeSecret>, Vec<String>) {
        let filter = request.name_filter.as_deref();
        let secrets = rwlock_read(&self.secrets);

        if !self.hierarchical() {
            let entries = secrets
                .iter()
                .filter(|secret| self.matches_filter(&secret.details.summary.name, filter))
                .cloned()
                .collect();
            return (entries, Vec::new());
        }

        let path = request.path.clone().unwrap_or_else(|| "/".to_string());
        let mut entries = Vec::new();
        let mut child_paths = Vec::new();

        for secret in secrets.iter() {
            let name = &secret.details.summary.name;
            let rooted = if name.starts_with('/') {
                name.clone()
            } else {
                format!("/{name}")
            };
            let Some(rest) = rooted.strip_prefix(path.as_str()) else {
                continue;
            };

            match rest.split_once('/') {
                Some((segment, _)) if !request.recursive => {
                    let child = format!("{path}{segment}/");
                    if self.matches_filter(name, filter) && !child_paths.contains(&child) {
                        child_paths.push(child);
                    }
                }
                _ => {
                    if self.matches_filter(name, filter) {
                        entries.push(secret.clone());
                    }
                }
            }
        }

        (entries, child_paths)
    }

    /// The stored copy of one secret, cloned so no read lock escapes this
    /// method.
    fn find(&self, name: &str) -> Result<FakeSecret, DbError> {
        rwlock_read(&self.secrets)
            .iter()
            .find(|secret| secret.details.summary.name == name)
            .cloned()
            .ok_or_else(|| DbError::object_not_found(format!("Secret {name} not found")))
    }

    /// Deterministic version id of the next flat-store write.
    fn next_written_version(&self) -> String {
        let count = self
            .written_versions
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        format!("v-written-{count}")
    }
}

impl SecretStoreSource for FakeSecretStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }
    /// Pages by offset: the page token is the index of the first entry of
    /// the next page. Every page repeats the full set of sub-paths, which is
    /// what a real store may do, so callers must merge them.
    fn list(&self, request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        self.record_list(request);

        let (entries, child_paths) = self.listing(request);
        let start = match request.page_token.as_deref() {
            Some(token) => token
                .parse::<usize>()
                .map_err(|_| DbError::query_failed(format!("Invalid page token {token}")))?,
            None => 0,
        };
        // `default_page_size` stands in for the service's own maximum, so a
        // request for a larger page is clamped instead of honored: that is
        // what a real store does with `max_results`.
        let page_size = request
            .page_size
            .map_or(self.default_page_size, |size| size as usize)
            .clamp(1, self.default_page_size);

        let end = start
            .checked_add(page_size)
            .ok_or_else(|| DbError::query_failed(format!("Page token {start} is out of range")))?
            .min(entries.len());
        let page: Vec<SecretSummary> = entries
            .get(start..end)
            .unwrap_or_default()
            .iter()
            .map(|secret| secret.details.summary.clone())
            .collect();

        Ok(SecretListPage {
            entries: page,
            child_paths,
            next_page_token: (end < entries.len()).then(|| end.to_string()),
        })
    }

    fn describe(&self, name: &str) -> Result<SecretDetails, DbError> {
        mutex_lock(&self.state.secret_store_calls)
            .described
            .push((self.descriptor.id.clone(), name.to_string()));

        self.find(name).map(|secret| secret.details)
    }

    fn get_value(&self, name: &str, version: Option<&str>) -> Result<SecretValue, DbError> {
        mutex_lock(&self.state.secret_store_calls)
            .value_requests
            .push((self.descriptor.id.clone(), name.to_string()));
        mutex_lock(&self.state.secret_store_calls)
            .value_version_requests
            .push((
                self.descriptor.id.clone(),
                name.to_string(),
                version.map(ToString::to_string),
            ));

        let secret = self.find(name)?;

        if let Some(failure) = &secret.read_failure {
            return match failure {
                FakeSecretReadFailure::Denied(message) => Err(DbError::permission_denied(*message)),
                FakeSecretReadFailure::Unavailable(message) => Err(DbError::query_failed(*message)),
            };
        }

        let requested = version
            .map(ToString::to_string)
            .or_else(|| secret.details.summary.version.clone());

        secret
            .versions
            .iter()
            .find(|(id, _)| Some(id) == requested.as_ref())
            .map(|(id, payload)| SecretValue {
                version: Some(id.clone()),
                payload: payload_value(payload),
            })
            .ok_or_else(|| {
                DbError::object_not_found(format!(
                    "Secret {name} has no version {}",
                    requested.as_deref().unwrap_or("<none>")
                ))
            })
    }

    /// The fixture's history for `name`, newest first. Like the real stores,
    /// listing reads metadata only, so a secret whose value read fails still
    /// lists its versions.
    fn list_versions(&self, name: &str) -> Result<Vec<SecretVersionInfo>, DbError> {
        mutex_lock(&self.state.secret_store_calls)
            .version_listings
            .push((self.descriptor.id.clone(), name.to_string()));

        let failure = rwlock_read(&self.state.secret_version_errors)
            .iter()
            .find(|(failing, _)| failing == name)
            .map(|(_, message)| message.clone());
        if let Some(message) = failure {
            return Err(DbError::permission_denied(message));
        }

        // The real service refuses the history of a secret scheduled for
        // deletion, with the nested text every call on one carries.
        let found = self.find(name)?;
        if found.details.summary.deletion_scheduled_for.is_some() {
            return Err(DbError::query_failed(
                "InvalidRequestException: You can't perform this operation on the secret because it was marked for deletion.",
            ));
        }

        // Like the real store, a version holding the pending stage says so.
        self.find(name).map(|secret| {
            secret
                .history
                .into_iter()
                .map(|mut version| {
                    version.is_pending = version.labels.iter().any(|label| label == "AWSPENDING");
                    version
                })
                .collect()
        })
    }

    /// One overwrite, following the rules the real stores keep: secrets
    /// another service owns are refused before the write (that is a
    /// DBFlux-side refusal the AWS driver makes too), and everything else is
    /// handed to the modeled service. The service answers a put on a secret
    /// scheduled for deletion with the rejection this fake returns, the way
    /// `Unavailable` answers a read of such a secret; a `Current` write
    /// becomes the current version, and a `Pending` write moves no current
    /// value (the fake keeps one version per secret). The write-failure knob
    /// makes the delivery to the store fail without touching the value.
    ///
    /// Only a write the modeled service accepted is recorded: a refused put
    /// never reaches the store, so `put_requests` lists delivered writes.
    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        mutex_lock(&self.state.secret_store_calls)
            .put_attempts
            .push((self.descriptor.id.clone(), request.name.clone()));

        self.descriptor.ensure_can_write(&request.name)?;

        let write_error = rwlock_read(&self.state.secret_write_errors)
            .iter()
            .find(|(name, _)| name == &request.name)
            .map(|(_, message)| message.clone());
        if let Some(message) = write_error {
            return Err(DbError::permission_denied(message));
        }

        let response = if self.hierarchical() {
            self.put_parameter_value(request)?
        } else {
            self.put_secret_value(request)?
        };

        mutex_lock(&self.state.secret_store_calls)
            .put_requests
            .push((self.descriptor.id.clone(), request.clone()));

        Ok(response)
    }

    /// Moves the staging labels the way `UpdateSecretVersionStage` does for
    /// `AWSCURRENT`: the label lands on the chosen version, the version
    /// that held it becomes `AWSPREVIOUS`, and the one that held
    /// `AWSPREVIOUS` is left without it. No version is created and no
    /// payload changes.
    fn roll_back(&self, request: &SecretRollbackRequest) -> Result<SecretRollback, DbError> {
        let mutation = FakeSecretMutation::RollBack(request.clone());
        self.begin_mutation(
            mutation.clone(),
            &request.name,
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::VERSION_LABELS,
            "roll back",
        )?;

        let rollback = {
            let mut secrets = rwlock_write(&self.secrets);
            let secret = find_mut(&mut secrets, &request.name, "Secret")?;
            refuse_unwritable(secret, &request.name)?;

            let current = secret.details.summary.version.clone().unwrap_or_default();
            if request.version_id == current {
                return Err(DbError::query_failed(format!(
                    "Version {} of {} is already the current version",
                    request.version_id, request.name
                )));
            }
            if !secret
                .history
                .iter()
                .any(|entry| entry.id == request.version_id)
            {
                return Err(DbError::object_not_found(format!(
                    "Secret {} has no version {}",
                    request.name, request.version_id
                )));
            }

            if secret.history.iter().any(|entry| {
                entry.id == request.version_id
                    && entry.labels.iter().any(|label| label == "AWSPENDING")
            }) {
                return Err(DbError::query_failed(format!(
                    "Version {} of {} holds AWSPENDING, an unfinished rotation; \
                     rolling back onto it would promote an unvalidated value",
                    request.version_id, request.name
                )));
            }

            for entry in secret.history.iter_mut() {
                entry.is_current = entry.id == request.version_id;
                entry.labels.retain(|label| label != "AWSPREVIOUS");
                if entry.id == current {
                    entry.labels.retain(|label| label != "AWSCURRENT");
                    entry.labels.push("AWSPREVIOUS".to_string());
                }
                if entry.id == request.version_id {
                    entry.labels.push("AWSCURRENT".to_string());
                }
            }
            secret.details.summary.version = Some(request.version_id.clone());
            secret.details.version_labels = distinct_labels(&secret.history);

            SecretRollback {
                current_version: request.version_id.clone(),
                previous_version: Some(current),
            }
        };

        self.accept_mutation(mutation);
        Ok(rollback)
    }

    /// A `Scheduled` request on a store with a recovery window marks the
    /// secret pending deletion, the way `DeleteSecret` does: the date the
    /// deletion takes effect is the request time plus the window, and the
    /// value is unavailable until the secret is restored. An `Irreversible`
    /// request on a store without a window removes the entry. A store
    /// refuses the kind it does not offer.
    fn delete_secret(&self, request: &SecretDeleteRequest) -> Result<SecretDeletion, DbError> {
        let mutation = FakeSecretMutation::Delete(request.clone());
        let required = match request {
            SecretDeleteRequest::Scheduled { .. } => {
                SecretStoreCapabilities::WRITE
                    | SecretStoreCapabilities::DELETE
                    | SecretStoreCapabilities::SOFT_DELETE
            }
            SecretDeleteRequest::Irreversible { .. } => {
                SecretStoreCapabilities::WRITE | SecretStoreCapabilities::DELETE
            }
        };
        self.begin_mutation(mutation.clone(), request.name(), required, "delete")?;

        let deletion = match request {
            SecretDeleteRequest::Scheduled {
                name,
                recovery_window_days,
            } => {
                request.validate()?;
                self.schedule_deletion(name, *recovery_window_days)?
            }
            SecretDeleteRequest::Irreversible { name, .. } => {
                if self
                    .descriptor
                    .supports(SecretStoreCapabilities::SOFT_DELETE)
                {
                    return Err(DbError::NotSupported(format!(
                        "Secret store \"{}\" deletes with a recovery window only; refused to \
                         delete \"{name}\" without recovery.",
                        self.descriptor.id
                    )));
                }
                request.validate()?;
                self.remove(name)?
            }
        };

        self.accept_mutation(mutation);
        Ok(deletion)
    }

    /// Clears a scheduled deletion, and with it the unavailable value the
    /// scheduling caused.
    fn restore_secret(&self, name: &str) -> Result<(), DbError> {
        let mutation = FakeSecretMutation::Restore {
            name: name.to_string(),
        };
        self.begin_mutation(
            mutation.clone(),
            name,
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::SOFT_DELETE,
            "restore",
        )?;

        {
            let mut stored = rwlock_write(&self.secrets);
            let secret = find_mut(&mut stored, name, "Secret")?;
            refuse_managed(secret, name)?;

            if secret.details.summary.deletion_scheduled_for.is_none() {
                return Err(DbError::query_failed(format!(
                    "Secrets Manager secret \"{name}\" is not scheduled for deletion"
                )));
            }

            secret.details.summary.deletion_scheduled_for = None;
            secret.details.deleted_at = None;
            if matches!(
                secret.read_failure,
                Some(FakeSecretReadFailure::Unavailable(_))
            ) {
                secret.read_failure = None;
            }
        }

        self.accept_mutation(mutation);
        Ok(())
    }

    /// Starts a rotation of a secret whose rotation is enabled and has a
    /// function, the way the service runs one: a version is staged
    /// `AWSPENDING`, the function fills it, and `AWSCURRENT` moves to it
    /// with the old current becoming `AWSPREVIOUS`. The fake cannot run a
    /// function, so the new version carries a copy of the current value.
    /// The rotation is stamped and the next one scheduled.
    fn rotate_now(&self, name: &str) -> Result<SecretRotationStart, DbError> {
        let mutation = FakeSecretMutation::RotateNow {
            name: name.to_string(),
        };
        self.begin_mutation(
            mutation.clone(),
            name,
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::ROTATION,
            "rotate",
        )?;

        let version = {
            let mut stored = rwlock_write(&self.secrets);
            let secret = find_mut(&mut stored, name, "Secret")?;
            refuse_unwritable(secret, name)?;

            let rotation = secret
                .details
                .summary
                .rotation
                .clone()
                .filter(|rotation| rotation.enabled && rotation.function.is_some())
                .ok_or_else(|| {
                    DbError::query_failed(format!(
                        "Rotation of \"{name}\" is not enabled; enable it before rotating"
                    ))
                })?;

            if unfinished_rotation_version(&secret.history).is_some() {
                return Err(DbError::query_failed(format!(
                    "Rotation of \"{name}\" is already in progress"
                )));
            }
            let next_rotation = next_rotation_after(fake_now(), rotation.schedule.as_deref())?;

            let current = secret.details.summary.version.clone().unwrap_or_default();
            let payload = secret
                .versions
                .iter()
                .find(|(id, _)| *id == current)
                .map(|(_, payload)| payload.clone())
                .ok_or_else(|| DbError::query_failed("The secret has no current value"))?;

            let version = self.next_written_version();
            secret.versions.push((version.clone(), payload));
            move_current_label(&mut secret.history, &version);
            secret.details.summary.version = Some(version.clone());
            secret.details.summary.last_changed = fake_now();
            secret.details.version_count = u32::try_from(secret.history.len()).ok();
            secret.details.version_labels = distinct_labels(&secret.history);
            secret.details.summary.rotation = Some(SecretRotation {
                last_rotated: fake_now(),
                next_rotation,
                ..rotation
            });

            version
        };

        self.accept_mutation(mutation);
        Ok(SecretRotationStart {
            version: Some(version),
        })
    }

    /// Turns rotation on with the new settings, keeping the window and the
    /// function the secret already has when the update names none, or off.
    /// Nothing is rotated.
    fn update_rotation(
        &self,
        update: &SecretRotationUpdate,
    ) -> Result<SecretRotationChange, DbError> {
        let mutation = FakeSecretMutation::UpdateRotation(update.clone());
        self.begin_mutation(
            mutation.clone(),
            &update.name,
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::ROTATION,
            "edit rotation",
        )?;
        update.validate()?;

        let change = {
            let mut stored = rwlock_write(&self.secrets);
            let secret = find_mut(&mut stored, &update.name, "Secret")?;
            refuse_unwritable(secret, &update.name)?;

            let existing = secret.details.summary.rotation.clone();
            let rotation = if update.enabled {
                let existing = existing.unwrap_or_default();
                let function = update
                    .function
                    .clone()
                    .or(existing.function.clone())
                    .ok_or_else(|| {
                        DbError::query_failed(format!(
                            "Rotation of \"{}\" needs a rotation function",
                            update.name
                        ))
                    })?;

                SecretRotation {
                    enabled: true,
                    schedule: update.schedule.clone(),
                    window: update.window.clone().or(existing.window),
                    function: Some(function),
                    last_rotated: existing.last_rotated,
                    next_rotation: next_rotation_after(fake_now(), update.schedule.as_deref())?,
                }
            } else {
                let existing = existing
                    .filter(|rotation| rotation.enabled)
                    .ok_or_else(|| {
                        DbError::query_failed(format!(
                            "Rotation of \"{}\" is not enabled",
                            update.name
                        ))
                    })?;

                SecretRotation {
                    enabled: false,
                    next_rotation: None,
                    ..existing
                }
            };
            secret.details.summary.rotation = Some(rotation);

            // Cancelling does not clear a stage the cancelled rotation left.
            SecretRotationChange {
                unfinished_version: if update.enabled {
                    None
                } else {
                    unfinished_rotation_version(&secret.history)
                },
            }
        };

        self.accept_mutation(mutation);
        Ok(change)
    }

    /// Applies a tag edit: removals first, then each tag set overwrites the
    /// value of an existing key in place or is appended, and the secret may
    /// not end up with more than [`MAX_TAGS`] tags. No version is touched.
    fn update_tags(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        let mutation = FakeSecretMutation::UpdateTags(update.clone());
        self.begin_mutation(
            mutation.clone(),
            &update.name,
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::TAGS,
            "edit the tags of",
        )?;
        update.validate()?;

        {
            let mut stored = rwlock_write(&self.secrets);
            let secret = find_mut(&mut stored, &update.name, self.noun())?;
            if !self.hierarchical() {
                refuse_unwritable(secret, &update.name)?;
            }

            let mut tags = secret.details.tags.clone();
            tags.retain(|tag| !update.remove.contains(&tag.key));
            for tag in &update.set {
                match tags.iter_mut().find(|existing| existing.key == tag.key) {
                    Some(existing) => existing.value = tag.value.clone(),
                    None => tags.push(tag.clone()),
                }
            }
            if tags.len() > MAX_TAGS {
                return Err(DbError::constraint_violation(format!(
                    "{} {} would carry {} tags; at most {MAX_TAGS} are allowed",
                    self.noun(),
                    update.name,
                    tags.len()
                )));
            }

            secret.details.tags = tags;
        }

        self.accept_mutation(mutation);
        Ok(())
    }

    /// A flat store changes the description alone. A hierarchical store can
    /// only change it by writing the value again, so the fake does the same
    /// the real driver does: it reads the current value (failing closed when
    /// the read is denied or unavailable) and writes it back as the next
    /// version with the new description, leaving the type, key and data type
    /// as they are.
    fn update_description(
        &self,
        update: &SecretDescriptionUpdate,
    ) -> Result<SecretDescriptionChange, DbError> {
        let mutation = FakeSecretMutation::UpdateDescription(update.clone());
        self.begin_mutation(
            mutation.clone(),
            &update.name,
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::DESCRIPTION,
            "edit the description of",
        )?;

        let limit = if self.hierarchical() { 1024 } else { 2048 };
        if update.description.chars().count() > limit {
            return Err(DbError::constraint_violation(format!(
                "The description of {} is longer than {limit} characters",
                update.name
            )));
        }
        if self.hierarchical() && update.description.is_empty() {
            return Err(DbError::constraint_violation(format!(
                "The description of {} cannot be cleared; write a replacement text instead",
                update.name
            )));
        }
        let description = Some(update.description.clone()).filter(|text| !text.is_empty());

        let version = {
            let mut stored = rwlock_write(&self.secrets);
            let secret = find_mut(&mut stored, &update.name, self.noun())?;

            if self.hierarchical() {
                Some(rewrite_with_description(secret, &update.name, description)?)
            } else {
                refuse_unwritable(secret, &update.name)?;
                secret.details.summary.description = description;
                secret.details.summary.last_changed = fake_now();
                None
            }
        };

        self.accept_mutation(mutation);
        Ok(SecretDescriptionChange { version })
    }
    /// Creates a secret the way the service does, and never overwrites:
    /// a name that exists is refused as a constraint violation. A flat store
    /// makes the value the single `AWSCURRENT` version, with the account
    /// default key when none is named. A hierarchical store makes it
    /// version 1 of a typed parameter, with the standard tier and the `text`
    /// data type by default. Each store refuses the names and settings the
    /// real one does.
    fn create_secret(&self, request: &SecretCreateRequest) -> Result<SecretCreated, DbError> {
        let mutation = FakeSecretMutation::Create(request.clone());
        self.begin_mutation(
            mutation.clone(),
            &request.name,
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::CREATE,
            "create",
        )?;
        request.validate()?;

        let created = if self.hierarchical() {
            self.create_parameter(request)?
        } else {
            self.create_flat_secret(request)?
        };

        self.accept_mutation(mutation);
        Ok(created)
    }
}

impl FakeSecretStore {
    fn insert_new(&self, secret: FakeSecret) -> Result<(), DbError> {
        let mut stored = rwlock_write(&self.secrets);
        let name = secret.details.summary.name.clone();

        if stored
            .iter()
            .any(|existing| existing.details.summary.name == name)
        {
            return Err(DbError::constraint_violation(format!(
                "{} {name} already exists",
                self.noun()
            )));
        }

        stored.push(secret);
        Ok(())
    }

    fn create_flat_secret(&self, request: &SecretCreateRequest) -> Result<SecretCreated, DbError> {
        let name = &request.name;

        if name.chars().count() > 512
            || !name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "/_+=.@-".contains(character))
        {
            return Err(DbError::constraint_violation(format!(
                "\"{name}\" is not a valid secret name: 1 to 512 letters, digits and / _ + = . @ -"
            )));
        }
        if request.value_type.is_some() || request.tier.is_some() {
            return Err(DbError::NotSupported(
                "Secrets Manager secrets have no value type or tier".to_string(),
            ));
        }
        if request
            .description
            .as_deref()
            .is_some_and(|text| text.chars().count() > 2048)
        {
            return Err(DbError::constraint_violation(format!(
                "The description of {name} is longer than 2048 characters"
            )));
        }

        let version = self.next_written_version();
        let resource_id = format!("arn:fake:secret:{name}");
        let history = vec![written_version(&version, vec!["AWSCURRENT".to_string()])];
        let secret = FakeSecret {
            details: SecretDetails {
                summary: SecretSummary {
                    description: request.description.clone(),
                    version: Some(version.clone()),
                    last_changed: fake_now(),
                    ..SecretSummary::new(name)
                },
                resource_id: Some(resource_id.clone()),
                encryption_key: Some(
                    request
                        .encryption_key
                        .clone()
                        .unwrap_or_else(|| "aws/secretsmanager".to_string()),
                ),
                created: fake_now(),
                version_count: Some(1),
                version_labels: distinct_labels(&history),
                tags: request.tags.clone(),
                ..SecretDetails::default()
            },
            versions: vec![(
                version.clone(),
                FakeSecretPayload::for_write(&request.payload),
            )],
            history,
            read_failure: None,
        };
        self.insert_new(secret)?;

        Ok(SecretCreated {
            version: Some(version),
            resource_id: Some(resource_id),
        })
    }

    fn create_parameter(&self, request: &SecretCreateRequest) -> Result<SecretCreated, DbError> {
        let name = &request.name;
        let reserved = name.trim_start_matches('/').to_ascii_lowercase();

        if name.chars().count() > 2048
            || !name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "_.-/".contains(character))
            || reserved.is_empty()
            || reserved.starts_with("aws")
            || reserved.starts_with("ssm")
        {
            return Err(DbError::constraint_violation(format!(
                "\"{name}\" is not a valid parameter name: letters, digits and _ . - /, \
                 not starting with aws or ssm"
            )));
        }

        if name.contains('/') && !name.starts_with('/') {
            return Err(DbError::constraint_violation(format!(
                "\"{name}\" has levels but no leading slash"
            )));
        }

        let value_type = request
            .value_type
            .clone()
            .unwrap_or_else(|| "String".to_string());
        if !["String", "StringList", "SecureString"].contains(&value_type.as_str()) {
            return Err(DbError::constraint_violation(format!(
                "\"{value_type}\" is not a parameter type"
            )));
        }
        let tier = request
            .tier
            .clone()
            .unwrap_or_else(|| "Standard".to_string());
        if !["Standard", "Advanced", "Intelligent-Tiering"].contains(&tier.as_str()) {
            return Err(DbError::constraint_violation(format!(
                "\"{tier}\" is not a parameter tier"
            )));
        }
        if request.encryption_key.is_some() && value_type != "SecureString" {
            return Err(DbError::constraint_violation(
                "Only a SecureString parameter takes an encryption key".to_string(),
            ));
        }
        let value_bytes = match &request.payload {
            SecretPayload::Text(text) => text.expose_secret().len(),
            SecretPayload::Binary(bytes) => bytes.expose_secret().len(),
        };
        let tier = resolve_tier(&tier, value_bytes);
        if tier == "Standard" && value_bytes > STANDARD_TIER_VALUE_BYTES {
            return Err(DbError::constraint_violation(format!(
                "The value of {name} is longer than the {STANDARD_TIER_VALUE_BYTES} bytes the \
                 standard tier takes"
            )));
        }
        if request
            .description
            .as_deref()
            .is_some_and(|text| text.chars().count() > 1024)
        {
            return Err(DbError::constraint_violation(format!(
                "The description of {name} is longer than 1024 characters"
            )));
        }

        let encryption_key = (value_type == "SecureString").then(|| {
            request
                .encryption_key
                .clone()
                .unwrap_or_else(|| "alias/aws/ssm".to_string())
        });
        let version = "1".to_string();
        let secret = FakeSecret {
            details: SecretDetails {
                summary: SecretSummary {
                    description: request.description.clone(),
                    value_type: Some(value_type),
                    tier: Some(tier),
                    version: Some(version.clone()),
                    last_changed: fake_now(),
                    ..SecretSummary::new(name)
                },
                resource_id: Some(format!("arn:fake:secret:{name}")),
                encryption_key,
                data_type: Some("text".to_string()),
                last_changed_by: Some(FAKE_WRITER.to_string()),
                tags: request.tags.clone(),
                ..SecretDetails::default()
            },
            versions: vec![(
                version.clone(),
                FakeSecretPayload::for_write(&request.payload),
            )],
            history: vec![SecretVersionInfo {
                modified_by: Some(FAKE_WRITER.to_string()),
                ..written_version(&version, Vec::new())
            }],
            read_failure: None,
        };
        self.insert_new(secret)?;

        Ok(SecretCreated {
            version: Some(version),
            resource_id: None,
        })
    }
}

/// The hierarchical store's description change: the next numbered version
/// carries the current value again.
fn rewrite_with_description(
    secret: &mut FakeSecret,
    name: &str,
    description: Option<String>,
) -> Result<String, DbError> {
    if let Some(failure) = &secret.read_failure {
        return Err(match failure {
            FakeSecretReadFailure::Denied(message) => DbError::permission_denied(*message),
            FakeSecretReadFailure::Unavailable(message) => DbError::query_failed(*message),
        });
    }

    let current =
        secret.details.summary.version.clone().ok_or_else(|| {
            DbError::query_failed(format!("Parameter {name} has no current version"))
        })?;
    let payload = secret
        .versions
        .iter()
        .find(|(id, _)| *id == current)
        .map(|(_, payload)| payload.clone())
        .ok_or_else(|| DbError::query_failed(format!("Parameter {name} has no current value")))?;
    let next = current
        .parse::<u64>()
        .map_err(|_| DbError::query_failed(format!("Parameter {name} has no numbered version")))?
        .wrapping_add(1)
        .to_string();

    secret.versions.push((next.clone(), payload));
    secret.history.insert(
        0,
        SecretVersionInfo {
            modified_by: Some(FAKE_WRITER.to_string()),
            ..written_version(&next, Vec::new())
        },
    );
    secret.details.summary.version = Some(next.clone());
    secret.details.summary.description = description;
    secret.details.summary.last_changed = fake_now();
    secret.details.last_changed_by = Some(FAKE_WRITER.to_string());

    Ok(next)
}

/// Most bytes a standard-tier parameter value holds.
const STANDARD_TIER_VALUE_BYTES: usize = 4096;

/// The tier a parameter is stored in. `Intelligent-Tiering` is a way to
/// choose, not a tier: the service stores a parameter in the standard tier
/// while its value fits and in the advanced tier when it does not.
fn resolve_tier(requested: &str, value_bytes: usize) -> String {
    match requested {
        "Intelligent-Tiering" if value_bytes > STANDARD_TIER_VALUE_BYTES => "Advanced".to_string(),
        "Intelligent-Tiering" => "Standard".to_string(),
        other => other.to_string(),
    }
}

/// The version a rotation left holding the pending stage without ever
/// becoming current.
fn unfinished_rotation_version(history: &[SecretVersionInfo]) -> Option<String> {
    history
        .iter()
        .find(|entry| {
            entry.labels.iter().any(|label| label == "AWSPENDING")
                && !entry.labels.iter().any(|label| label == "AWSCURRENT")
        })
        .map(|entry| entry.id.clone())
}

/// When the next rotation falls after `from`, for the schedules the fake
/// understands: `rate(N days)` and `rate(N hours)` exactly, and a day for a
/// `cron(...)` expression, which the fake does not evaluate.
fn next_rotation_after(
    from: Option<chrono::DateTime<chrono::Utc>>,
    schedule: Option<&str>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, DbError> {
    let (Some(from), Some(schedule)) = (from, schedule) else {
        return Ok(None);
    };
    let out_of_range = || {
        DbError::constraint_violation(format!(
            "The next rotation of \"{schedule}\" falls outside the dates a rotation can be set for"
        ))
    };

    let Some((count, unit)) = schedule
        .strip_prefix("rate(")
        .and_then(|inner| inner.strip_suffix(')'))
        .and_then(|inner| inner.split_once(' '))
    else {
        return from
            .checked_add_signed(chrono::Duration::days(1))
            .map(Some)
            .ok_or_else(out_of_range);
    };
    let Ok(count) = count.parse::<i64>() else {
        return Ok(None);
    };

    let interval = match unit {
        "hour" | "hours" => chrono::Duration::try_hours(count),
        _ => chrono::Duration::try_days(count),
    }
    .ok_or_else(out_of_range)?;

    from.checked_add_signed(interval)
        .map(Some)
        .ok_or_else(out_of_range)
}

fn find_mut<'a>(
    secrets: &'a mut [FakeSecret],
    name: &str,
    noun: &str,
) -> Result<&'a mut FakeSecret, DbError> {
    secrets
        .iter_mut()
        .find(|secret| secret.details.summary.name == name)
        .ok_or_else(|| DbError::object_not_found(format!("{noun} {name} not found")))
}

/// Secrets another service owns are read only in DBFlux.
fn refuse_managed(secret: &FakeSecret, name: &str) -> Result<(), DbError> {
    match &secret.details.summary.owning_service {
        Some(service) => Err(DbError::permission_denied(format!(
            "Secrets Manager secret \"{name}\" is managed by {service} and is read only"
        ))),
        None => Ok(()),
    }
}

/// The refusals every flat-store change shares: a managed secret, and the
/// service's rejection of changes to a secret scheduled for deletion.
fn refuse_unwritable(secret: &FakeSecret, name: &str) -> Result<(), DbError> {
    refuse_managed(secret, name)?;

    if secret.details.summary.deletion_scheduled_for.is_some() {
        return Err(DbError::query_failed(
            "Secrets Manager secret is scheduled for deletion; it cannot be changed",
        ));
    }

    Ok(())
}

impl FakeSecretStore {
    /// What the store's entries are called in its not-found answers.
    fn noun(&self) -> &'static str {
        if self.hierarchical() {
            "Parameter"
        } else {
            "Secret"
        }
    }

    /// Marks a secret pending deletion `days` after the fake clock.
    fn schedule_deletion(&self, name: &str, days: u32) -> Result<SecretDeletion, DbError> {
        let mut stored = rwlock_write(&self.secrets);
        let secret = find_mut(&mut stored, name, "Secret")?;
        refuse_managed(secret, name)?;

        if secret.details.summary.deletion_scheduled_for.is_some() {
            return Err(DbError::query_failed(format!(
                "Secrets Manager secret \"{name}\" is already scheduled for deletion"
            )));
        }

        let requested =
            fake_now().ok_or_else(|| DbError::query_failed("The fake clock is unset"))?;
        let scheduled_for = requested + chrono::Duration::days(i64::from(days));

        secret.details.deleted_at = Some(requested);
        secret.details.summary.deletion_scheduled_for = Some(scheduled_for);
        if secret.read_failure.is_none() {
            secret.read_failure = Some(FakeSecretReadFailure::Unavailable(
                "The secret is scheduled for deletion",
            ));
        }

        Ok(SecretDeletion {
            deletion_scheduled_for: Some(scheduled_for),
        })
    }

    /// Removes an entry for good, with the service's not-found answer for
    /// one that is already gone.
    fn remove(&self, name: &str) -> Result<SecretDeletion, DbError> {
        let mut stored = rwlock_write(&self.secrets);
        let position = stored
            .iter()
            .position(|secret| secret.details.summary.name == name)
            .ok_or_else(|| DbError::object_not_found(format!("Parameter {name} not found")))?;
        stored.remove(position);

        Ok(SecretDeletion::default())
    }

    /// The start of every write operation other than `put_value`: records
    /// the attempt, applies the store's own capability gate and the
    /// write-failure knob, exactly where the real drivers begin their calls.
    fn begin_mutation(
        &self,
        mutation: FakeSecretMutation,
        name: &str,
        required: SecretStoreCapabilities,
        operation: &str,
    ) -> Result<(), DbError> {
        mutex_lock(&self.state.secret_store_calls)
            .mutation_attempts
            .push((self.descriptor.id.clone(), mutation));

        self.descriptor.ensure_can(required, operation, name)?;

        let write_error = rwlock_read(&self.state.secret_write_errors)
            .iter()
            .find(|(denied, _)| denied == name)
            .map(|(_, message)| message.clone());
        match write_error {
            Some(message) => Err(DbError::permission_denied(message)),
            None => Ok(()),
        }
    }

    fn accept_mutation(&self, mutation: FakeSecretMutation) {
        mutex_lock(&self.state.secret_store_calls)
            .mutations
            .push((self.descriptor.id.clone(), mutation));
    }

    /// The flat store's overwrite: a new version replaces the current value.
    fn put_secret_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        let mut secrets = rwlock_write(&self.secrets);
        let secret = secrets
            .iter_mut()
            .find(|secret| secret.details.summary.name == request.name)
            .ok_or_else(|| {
                DbError::object_not_found(format!("Secret {} not found", request.name))
            })?;

        if let Some(service) = &secret.details.summary.owning_service {
            return Err(DbError::permission_denied(format!(
                "Secrets Manager secret \"{}\" is managed by {service} and is read only",
                request.name
            )));
        }
        if secret.details.summary.deletion_scheduled_for.is_some() {
            // The service itself rejects a put on a secret pending deletion,
            // so this answer models the service state, not a local policy —
            // the same answer `Unavailable` gives a read of such a secret.
            return Err(DbError::query_failed(
                "Secrets Manager secret is scheduled for deletion; its value cannot be written",
            ));
        }

        let version = self.next_written_version();
        // A write keeps its payload under its own version id, whatever the
        // stage: a `Current` write moves the summary's current version to it,
        // a `Pending` write moves nothing. Earlier versions stay readable.
        secret.versions.push((
            version.clone(),
            FakeSecretPayload::for_write(&request.payload),
        ));
        if request.stage == Some(SecretVersionStage::Pending) {
            // The service moves a staging label: only the newest pending
            // version holds `AWSPENDING`.
            for entry in secret.history.iter_mut() {
                entry.labels.retain(|label| label != "AWSPENDING");
            }
            secret
                .history
                .insert(0, written_version(&version, vec!["AWSPENDING".to_string()]));
        } else {
            secret.details.summary.version = Some(version.clone());
            move_current_label(&mut secret.history, &version);
        }
        secret.details.version_count = u32::try_from(secret.history.len()).ok();
        secret.details.version_labels = distinct_labels(&secret.history);

        Ok(SecretValue {
            version: Some(version),
            payload: request.payload.clone(),
        })
    }

    /// The hierarchical store's overwrite, shaped like SSM `PutParameter`:
    /// no staging labels and text values only, and the numeric version moves.
    fn put_parameter_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        if let Some(refusal) = parameter_write_refusal(request) {
            return Err(refusal);
        }

        let mut secrets = rwlock_write(&self.secrets);
        let secret = secrets
            .iter_mut()
            .find(|secret| secret.details.summary.name == request.name)
            .ok_or_else(|| {
                DbError::object_not_found(format!("Parameter {} not found", request.name))
            })?;

        let current = secret
            .details
            .summary
            .version
            .as_deref()
            .and_then(|version| version.parse::<u64>().ok())
            .ok_or_else(|| {
                DbError::query_failed(format!(
                    "Parameter {} has no version to overwrite",
                    request.name
                ))
            })?;
        let version = (current.wrapping_add(1)).to_string();

        // The stored value changes; the read authorization does not. A
        // denial that was there before the write is there after it.
        secret.details.summary.version = Some(version.clone());
        secret.versions.push((
            version.clone(),
            FakeSecretPayload::for_write(&request.payload),
        ));
        for entry in secret.history.iter_mut() {
            entry.is_current = false;
        }
        secret.history.insert(
            0,
            SecretVersionInfo {
                modified_by: Some(FAKE_WRITER.to_string()),
                is_current: true,
                ..written_version(&version, Vec::new())
            },
        );

        Ok(SecretValue {
            version: Some(version),
            payload: request.payload.clone(),
        })
    }
}

/// The fake clock: the instant every fake write happens at.
fn fake_now() -> Option<chrono::DateTime<chrono::Utc>> {
    fake_time(11, 7, 12)
}

/// Principal the fake parameter store records for a version written through
/// it.
const FAKE_WRITER: &str = "arn:fake:user/editor";

/// The history row of a version a write just created.
fn written_version(id: &str, labels: Vec<String>) -> SecretVersionInfo {
    SecretVersionInfo {
        is_pending: false,
        id: id.to_string(),
        labels,
        created: fake_time(11, 7, 12),
        modified_by: None,
        is_current: false,
    }
}

/// A current write on a staged store: `AWSCURRENT` moves to the new version,
/// the version that held it becomes `AWSPREVIOUS`, and the one that held
/// `AWSPREVIOUS` is left with no label.
fn move_current_label(history: &mut Vec<SecretVersionInfo>, version: &str) {
    for entry in history.iter_mut() {
        entry.labels.retain(|label| label != "AWSPREVIOUS");
        entry.is_current = false;

        for label in entry.labels.iter_mut() {
            if label == "AWSCURRENT" {
                *label = "AWSPREVIOUS".to_string();
            }
        }
    }

    history.insert(
        0,
        SecretVersionInfo {
            is_current: true,
            ..written_version(version, vec!["AWSCURRENT".to_string()])
        },
    );
}

/// Every label in use, sorted, like the real store's `describe`.
fn distinct_labels(history: &[SecretVersionInfo]) -> Vec<String> {
    let mut labels: Vec<String> = history
        .iter()
        .flat_map(|entry| entry.labels.iter().cloned())
        .collect();
    labels.sort();
    labels.dedup();
    labels
}

/// The `SecretValue` payload form of a stored version.
fn payload_value(payload: &FakeSecretPayload) -> SecretPayload {
    match payload {
        FakeSecretPayload::Text(text) => SecretPayload::Text(SecretString::from(text.clone())),
        FakeSecretPayload::Binary(bytes) => SecretPayload::Binary(SecretSlice::from(bytes.clone())),
    }
}

/// Why this fake parameter store cannot take a write: no staging labels and
/// text values only, the refusals the AWS driver makes before its call,
/// worded like the driver's and naming the store and the parameter.
fn parameter_write_refusal(request: &SecretPutRequest) -> Option<DbError> {
    let name = &request.name;

    if request.stage == Some(SecretVersionStage::Pending) {
        return Some(DbError::NotSupported(format!(
            "Parameter Store has no staging labels; the write to \"{name}\" cannot be staged."
        )));
    }

    if matches!(request.payload, SecretPayload::Binary(_)) {
        return Some(DbError::NotSupported(format!(
            "Parameter Store stores values as text only; the write to \"{name}\" was refused."
        )));
    }

    None
}

fn fake_time(month: u32, day: u32, hour: u32) -> Option<chrono::DateTime<chrono::Utc>> {
    use chrono::TimeZone as _;

    chrono::Utc
        .with_ymd_and_hms(2026, month, day, hour, 0, 0)
        .single()
}

fn fake_secret(
    summary: SecretSummary,
    details: impl FnOnce(SecretDetails) -> SecretDetails,
    payload: FakeSecretPayload,
) -> FakeSecret {
    let mut summary = summary;
    let version = summary
        .version
        .clone()
        .unwrap_or_else(|| "v-current".to_string());
    // Every stored secret has a current version, so the summary and the
    // version store agree even when the fixture named no version.
    if summary.version.is_none() {
        summary.version = Some(version.clone());
    }

    let base = SecretDetails {
        resource_id: Some(format!("arn:fake:secret:{}", summary.name)),
        created: fake_time(1, 10, 8),
        summary,
        ..SecretDetails::default()
    };

    let details = details(base);
    let history = vec![SecretVersionInfo {
        is_pending: false,
        id: version.clone(),
        labels: Vec::new(),
        created: details.created,
        modified_by: details.last_changed_by.clone(),
        is_current: true,
    }];

    FakeSecret {
        details,
        versions: vec![(version, payload)],
        history,
        read_failure: None,
    }
}

/// Replaces a fixture's single version with `older` ones behind it, newest
/// first, each with its metadata and payload. The first entry stays the
/// current version.
fn with_history(
    secret: FakeSecret,
    history: Vec<(SecretVersionInfo, FakeSecretPayload)>,
) -> FakeSecret {
    FakeSecret {
        versions: history
            .iter()
            .map(|(info, payload)| (info.id.clone(), payload.clone()))
            .collect(),
        history: history
            .into_iter()
            .enumerate()
            .map(|(index, (info, _))| SecretVersionInfo {
                is_current: index == 0,
                ..info
            })
            .collect(),
        ..secret
    }
}

fn history_row(
    id: &str,
    labels: &[&str],
    created: Option<chrono::DateTime<chrono::Utc>>,
    modified_by: Option<&str>,
) -> SecretVersionInfo {
    SecretVersionInfo {
        is_pending: false,
        id: id.to_string(),
        labels: labels.iter().map(ToString::to_string).collect(),
        created,
        modified_by: modified_by.map(ToString::to_string),
        is_current: false,
    }
}

/// Marks a secret whose reads fail the way its fixture models the service
/// state or a policy — independently of any value a write later stores.
fn failing_reads(secret: FakeSecret, failure: FakeSecretReadFailure) -> FakeSecret {
    FakeSecret {
        read_failure: Some(failure),
        ..secret
    }
}

pub(super) fn fake_secret_stores(state: &Arc<FakeDriverState>) -> Vec<FakeSecretStore> {
    vec![
        FakeSecretStore {
            descriptor: SecretStoreDescriptor::new(
                FAKE_SECRETS_STORE_ID,
                "Secrets Manager",
                SecretStoreCapabilities::VERSIONS
                    | SecretStoreCapabilities::VERSION_LABELS
                    | SecretStoreCapabilities::ROTATION
                    | SecretStoreCapabilities::SOFT_DELETE
                    | SecretStoreCapabilities::WRITE
                    | SecretStoreCapabilities::DELETE
                    | SecretStoreCapabilities::TAGS
                    | SecretStoreCapabilities::DESCRIPTION
                    | SecretStoreCapabilities::CREATE,
            ),
            secrets: RwLock::new(fake_flat_secrets().into_iter().map(label_current).collect()),
            default_page_size: 4,
            written_versions: AtomicUsize::new(0),
            state: state.clone(),
        },
        FakeSecretStore {
            descriptor: SecretStoreDescriptor::new(
                FAKE_PARAMETER_STORE_ID,
                "Parameter Store",
                SecretStoreCapabilities::VERSIONS
                    | SecretStoreCapabilities::HIERARCHICAL
                    | SecretStoreCapabilities::TYPED_VALUES
                    | SecretStoreCapabilities::WRITE
                    | SecretStoreCapabilities::DELETE
                    | SecretStoreCapabilities::TAGS
                    | SecretStoreCapabilities::DESCRIPTION
                    | SecretStoreCapabilities::CREATE,
            )
            .with_create_choices(
                SecretCreateChoices::new(["String", "StringList", "SecureString"], "SecureString")
                    .with_encrypted_value_type("SecureString")
                    .with_tiers(["Standard", "Advanced", "Intelligent-Tiering"], "Standard"),
            ),
            secrets: RwLock::new(fake_parameters()),
            default_page_size: 2,
            written_versions: AtomicUsize::new(0),
            state: state.clone(),
        },
    ]
}

/// Every version of a staged store that holds the current value carries
/// `AWSCURRENT`, so a fixture with a single version lists one.
fn label_current(mut secret: FakeSecret) -> FakeSecret {
    if let Some(newest) = secret.history.first_mut()
        && newest.labels.is_empty()
    {
        newest.labels.push("AWSCURRENT".to_string());
    }

    secret
}

/// Six secrets, so the default page of four leaves a second page: a
/// key/value secret with rotation, a secret managed by another service, a
/// plaintext one, a binary one, one pending deletion and one more key/value.
fn fake_flat_secrets() -> Vec<FakeSecret> {
    let rotation = SecretRotation {
        enabled: true,
        schedule: Some("rate(30 days)".to_string()),
        window: Some("4h".to_string()),
        // Stores report the rotation function by its ARN.
        function: Some(FAKE_ROTATION_FUNCTION.to_string()),
        last_rotated: fake_time(9, 20, 3),
        next_rotation: fake_time(10, 20, 3),
    };

    let credentials = |password: &str| {
        FakeSecretPayload::Text(format!(
            r#"{{"username":"api-user","password":"{password}","port":5432}}"#
        ))
    };

    vec![
        with_history(
            fake_secret(
                SecretSummary {
                    description: Some("Payments API credentials".to_string()),
                    version: Some("v-current".to_string()),
                    last_changed: fake_time(9, 20, 3),
                    rotation: Some(rotation),
                    ..SecretSummary::new("prod/api/credentials")
                },
                |details| SecretDetails {
                    encryption_key: Some("alias/secrets".to_string()),
                    last_accessed: fake_time(10, 1, 0),
                    version_count: Some(3),
                    version_labels: vec!["AWSCURRENT".to_string(), "AWSPREVIOUS".to_string()],
                    tags: vec![
                        SecretTag::new("team", "payments"),
                        SecretTag::new("env", "prod"),
                    ],
                    ..details
                },
                FakeSecretPayload::Text(format!(
                    r#"{{"username":"api-user","password":"{FAKE_SECRET_PASSWORD}","port":5432}}"#
                )),
            ),
            vec![
                (
                    history_row("v-current", &["AWSCURRENT"], fake_time(9, 20, 3), None),
                    credentials(FAKE_SECRET_PASSWORD),
                ),
                (
                    history_row("v-previous", &["AWSPREVIOUS"], fake_time(8, 20, 3), None),
                    credentials("fake-previous-2b8d"),
                ),
                (
                    history_row("v-oldest", &[], fake_time(7, 20, 3), None),
                    credentials("fake-oldest-90e1"),
                ),
            ],
        ),
        fake_secret(
            SecretSummary {
                description: Some("Master password of the orders database".to_string()),
                last_changed: fake_time(8, 2, 12),
                owning_service: Some("rds".to_string()),
                ..SecretSummary::new("prod/db/master")
            },
            |details| SecretDetails {
                version_count: Some(1),
                version_labels: vec!["AWSCURRENT".to_string()],
                ..details
            },
            FakeSecretPayload::Text(r#"{"username":"admin","password":"managed-pass"}"#.into()),
        ),
        failing_reads(
            fake_secret(
                SecretSummary {
                    description: Some("Legacy deploy token".to_string()),
                    last_changed: fake_time(7, 15, 9),
                    deletion_scheduled_for: fake_time(11, 1, 0),
                    ..SecretSummary::new("prod/deploy/legacy-token")
                },
                |details| SecretDetails {
                    deleted_at: fake_time(10, 2, 0),
                    ..details
                },
                FakeSecretPayload::Text("legacy-token-value".to_string()),
            ),
            FakeSecretReadFailure::Unavailable("The secret is scheduled for deletion"),
        ),
        fake_secret(
            SecretSummary {
                description: Some("Webhook signing token".to_string()),
                last_changed: fake_time(6, 5, 11),
                ..SecretSummary::new("prod/notify/webhook-token")
            },
            |details| details,
            FakeSecretPayload::Text(FAKE_SECRET_PLAINTEXT.to_string()),
        ),
        fake_secret(
            SecretSummary {
                description: Some("Signing key in DER form".to_string()),
                last_changed: fake_time(5, 3, 14),
                ..SecretSummary::new("prod/tls/signing-key")
            },
            |details| details,
            FakeSecretPayload::Binary(FAKE_SECRET_BINARY.to_vec()),
        ),
        fake_secret(
            SecretSummary {
                description: Some("Staging API credentials".to_string()),
                last_changed: fake_time(4, 8, 16),
                ..SecretSummary::new("staging/api/credentials")
            },
            |details| details,
            FakeSecretPayload::Text(r#"{"username":"staging","password":"staging-pass"}"#.into()),
        ),
    ]
}

fn fake_parameter(
    name: &str,
    value_type: &str,
    tier: &str,
    payload: FakeSecretPayload,
) -> FakeSecret {
    let current = payload.clone();
    let author = "arn:fake:user/deployer";

    with_history(
        fake_secret(
            SecretSummary {
                value_type: Some(value_type.to_string()),
                tier: Some(tier.to_string()),
                version: Some("3".to_string()),
                last_changed: fake_time(9, 1, 10),
                ..SecretSummary::new(name)
            },
            |details| SecretDetails {
                data_type: Some("text".to_string()),
                last_changed_by: Some("arn:fake:user/deployer".to_string()),
                encryption_key: (value_type == "SecureString")
                    .then(|| "alias/app-prod".to_string()),
                tags: vec![SecretTag::new("app", "shop")],
                ..details
            },
            payload,
        ),
        vec![
            (
                history_row("3", &["prod"], fake_time(9, 1, 10), Some(author)),
                current,
            ),
            (
                history_row("2", &[], fake_time(8, 1, 10), Some("arn:fake:user/ana")),
                FakeSecretPayload::Text("fake-parameter-v2".to_string()),
            ),
            (
                history_row("1", &[], fake_time(7, 1, 10), Some(author)),
                FakeSecretPayload::Text("fake-parameter-v1".to_string()),
            ),
        ],
    )
}

/// A path tree `/app/prod/{db,feature-flags}/…`, `/app/staging/db/…` and
/// `/shared/…`, with a `SecureString` the caller may not decrypt.
fn fake_parameters() -> Vec<FakeSecret> {
    vec![
        fake_parameter(
            "/app/prod/db/host",
            "String",
            "Standard",
            FakeSecretPayload::Text("db.prod.internal".to_string()),
        ),
        failing_reads(
            fake_parameter(
                "/app/prod/db/password",
                "SecureString",
                "Standard",
                FakeSecretPayload::Text("unreadable-vault-value".to_string()),
            ),
            FakeSecretReadFailure::Denied(FAKE_KMS_DENIED_MESSAGE),
        ),
        fake_parameter(
            "/app/prod/db/port",
            "String",
            "Standard",
            FakeSecretPayload::Text("5432".to_string()),
        ),
        fake_parameter(
            "/app/prod/feature-flags/checkout",
            "String",
            "Advanced",
            FakeSecretPayload::Text("enabled".to_string()),
        ),
        fake_parameter(
            "/app/prod/region",
            "String",
            "Standard",
            FakeSecretPayload::Text("eu-west-1".to_string()),
        ),
        fake_parameter(
            "/app/staging/db/host",
            "String",
            "Standard",
            FakeSecretPayload::Text("db.staging.internal".to_string()),
        ),
        fake_parameter(
            "/shared/endpoint",
            "String",
            "Standard",
            FakeSecretPayload::Text("https://api.example.test".to_string()),
        ),
    ]
}
