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
}

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
            let Some(rest) = name.strip_prefix(path.as_str()) else {
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

        self.find(name).map(|secret| secret.history)
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
}

impl FakeSecretStore {
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
        secret.history.insert(
            0,
            SecretVersionInfo {
                modified_by: Some(FAKE_WRITER.to_string()),
                ..written_version(&version, Vec::new())
            },
        );

        Ok(SecretValue {
            version: Some(version),
            payload: request.payload.clone(),
        })
    }
}

/// Principal the fake parameter store records for a version written through
/// it.
const FAKE_WRITER: &str = "arn:fake:user/editor";

/// The history row of a version a write just created.
fn written_version(id: &str, labels: Vec<String>) -> SecretVersionInfo {
    SecretVersionInfo {
        id: id.to_string(),
        labels,
        created: fake_time(11, 7, 12),
        modified_by: None,
    }
}

/// A current write on a staged store: `AWSCURRENT` moves to the new version,
/// the version that held it becomes `AWSPREVIOUS`, and the one that held
/// `AWSPREVIOUS` is left with no label.
fn move_current_label(history: &mut Vec<SecretVersionInfo>, version: &str) {
    for entry in history.iter_mut() {
        entry.labels.retain(|label| label != "AWSPREVIOUS");

        for label in entry.labels.iter_mut() {
            if label == "AWSCURRENT" {
                *label = "AWSPREVIOUS".to_string();
            }
        }
    }

    history.insert(0, written_version(version, vec!["AWSCURRENT".to_string()]));
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
        id: version.clone(),
        labels: Vec::new(),
        created: details.created,
        modified_by: details.last_changed_by.clone(),
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
        history: history.into_iter().map(|(info, _)| info).collect(),
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
        id: id.to_string(),
        labels: labels.iter().map(ToString::to_string).collect(),
        created,
        modified_by: modified_by.map(ToString::to_string),
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
                    | SecretStoreCapabilities::WRITE,
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
                    | SecretStoreCapabilities::WRITE,
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
        function: Some("rotate-api-credentials".to_string()),
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
