//! The write operations of the secret store seam beyond
//! `SecretStoreSource::put_value`: rolling a version back, deleting and
//! restoring, rotating, editing tags and the description, and creating.
//!
//! Every request type here is provider-neutral and checks the limits no
//! store accepts a call past in its own `validate`, so the governed seam
//! refuses an invalid request before any call. A request that carries a
//! value (`SecretCreateRequest`) holds it as a `secrecy` type, so `Debug`
//! never renders it.

use secrecy::{ExposeSecret, SecretSlice, SecretString};

use crate::DbError;

use super::{SecretPayload, SecretTag};

/// Refuses a blank or whitespace-only secret name, which no request type can
/// act on and which would otherwise reach a store as an empty identifier.
pub fn require_name(name: &str) -> Result<(), DbError> {
    if name.trim().is_empty() {
        return Err(DbError::constraint_violation(
            "A secret operation needs the name of the secret.",
        ));
    }

    Ok(())
}

/// A request to make an earlier version of a secret the current one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRollbackRequest {
    /// Full name of the secret, including any path.
    pub name: String,

    /// The version to make current, in the selector `get_value` accepts.
    pub version_id: String,
}

impl SecretRollbackRequest {
    pub fn new(name: impl Into<String>, version_id: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version_id: version_id.into(),
        }
    }

    /// Refuses a request that names no version.
    pub fn validate(&self) -> Result<(), DbError> {
        require_name(&self.name)?;

        if self.version_id.trim().is_empty() {
            return Err(DbError::constraint_violation(format!(
                "Rolling back \"{}\" needs the version to roll back to.",
                self.name
            )));
        }

        Ok(())
    }
}

/// What a roll back moved: no value is rewritten, only which version holds
/// the current stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRollback {
    /// The version that is current after the roll back.
    pub current_version: String,

    /// The version that was current before it, which the store keeps as the
    /// previous one.
    pub previous_version: Option<String>,
}

/// Shortest recovery window a scheduled deletion accepts, in days.
pub const MIN_RECOVERY_WINDOW_DAYS: u32 = 7;

/// Longest recovery window a scheduled deletion accepts, in days.
pub const MAX_RECOVERY_WINDOW_DAYS: u32 = 30;

/// Recovery window a deletion offers unless the user picks another.
pub const DEFAULT_RECOVERY_WINDOW_DAYS: u32 = 30;

/// A request to delete a secret.
///
/// The two ways a store deletes are two variants, so an irreversible
/// deletion cannot be built without the name typed back, and a recoverable
/// one cannot be turned into an irreversible one by a flag. A store with a
/// recovery window (`SecretStoreCapabilities::SOFT_DELETE`) takes only
/// `Scheduled`, and no store is ever asked to delete without recovery when
/// it has a window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretDeleteRequest {
    /// Schedule the deletion with a recovery window, during which the secret
    /// can be restored.
    Scheduled {
        name: String,
        recovery_window_days: u32,
    },

    /// Delete with no way back. `confirmation` is the name as the user typed
    /// it, and the deletion is refused unless it equals `name` exactly.
    Irreversible { name: String, confirmation: String },
}

impl SecretDeleteRequest {
    pub fn scheduled(name: impl Into<String>, recovery_window_days: u32) -> Self {
        Self::Scheduled {
            name: name.into(),
            recovery_window_days,
        }
    }

    pub fn irreversible(name: impl Into<String>, confirmation: impl Into<String>) -> Self {
        Self::Irreversible {
            name: name.into(),
            confirmation: confirmation.into(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Scheduled { name, .. } | Self::Irreversible { name, .. } => name,
        }
    }

    /// Whether the deletion cannot be recovered.
    pub fn is_irreversible(&self) -> bool {
        matches!(self, Self::Irreversible { .. })
    }

    /// Whether the request carries the confirmation it needs: a scheduled
    /// deletion needs none, an irreversible one needs its exact name.
    pub fn confirmation_matches(&self) -> bool {
        match self {
            Self::Scheduled { .. } => true,
            Self::Irreversible { name, confirmation } => name == confirmation,
        }
    }

    /// Refuses a window outside the days the service accepts, and an
    /// irreversible deletion whose confirmation is not the exact name.
    pub fn validate(&self) -> Result<(), DbError> {
        require_name(self.name())?;

        match self {
            Self::Scheduled {
                name,
                recovery_window_days,
            } => {
                if !(MIN_RECOVERY_WINDOW_DAYS..=MAX_RECOVERY_WINDOW_DAYS)
                    .contains(recovery_window_days)
                {
                    return Err(DbError::constraint_violation(format!(
                        "The recovery window for \"{name}\" must be {MIN_RECOVERY_WINDOW_DAYS} to \
                         {MAX_RECOVERY_WINDOW_DAYS} days, not {recovery_window_days}."
                    )));
                }

                Ok(())
            }
            Self::Irreversible { name, .. } => {
                if !self.confirmation_matches() {
                    return Err(DbError::constraint_violation(format!(
                        "\"{name}\" cannot be deleted without recovery: type its exact name to confirm."
                    )));
                }

                Ok(())
            }
        }
    }
}

/// What a deletion did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretDeletion {
    /// When a scheduled deletion takes effect. `None` for a deletion that
    /// took effect at once.
    pub deletion_scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
}

/// The change a rotation edit asks for: turn automatic rotation on with a
/// schedule, or turn it off.
///
/// Enabling needs a schedule; the window and the function are optional
/// because a store keeps the ones already configured. Disabling carries no
/// settings, so a request cannot both turn rotation off and describe how it
/// runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRotationUpdate {
    /// Full name of the secret, including any path.
    pub name: String,

    /// Whether automatic rotation is on after the change.
    pub enabled: bool,

    /// Schedule expression: `rate(N days)`, `rate(N hours)` or `cron(...)`.
    pub schedule: Option<String>,

    /// Rotation window in whole hours, for example `4h`.
    pub window: Option<String>,

    /// The function that rotates the secret, as the store identifies it.
    pub function: Option<String>,
}

impl SecretRotationUpdate {
    pub fn enable(name: impl Into<String>, schedule: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enabled: true,
            schedule: Some(schedule.into()),
            window: None,
            function: None,
        }
    }

    pub fn disable(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enabled: false,
            schedule: None,
            window: None,
            function: None,
        }
    }

    pub fn with_window(mut self, window: impl Into<String>) -> Self {
        self.window = Some(window.into());
        self
    }

    pub fn with_function(mut self, function: impl Into<String>) -> Self {
        self.function = Some(function.into());
        self
    }

    /// Refuses what the service would reject, before any call: enabling
    /// without a valid schedule, a malformed window or function, and a
    /// disable that carries settings.
    pub fn validate(&self) -> Result<(), DbError> {
        let refuse = |reason: String| {
            DbError::constraint_violation(format!(
                "The rotation of \"{}\" was not changed: {reason}",
                self.name
            ))
        };
        require_name(&self.name)?;

        if !self.enabled {
            if self.schedule.is_some() || self.window.is_some() || self.function.is_some() {
                return Err(refuse(
                    "turning rotation off takes no schedule, window or function.".to_string(),
                ));
            }

            return Ok(());
        }

        let Some(schedule) = &self.schedule else {
            return Err(refuse("turning rotation on needs a schedule.".to_string()));
        };
        validate_rotation_schedule(schedule).map_err(refuse)?;

        if let Some(window) = &self.window {
            validate_rotation_window(window).map_err(refuse)?;
            validate_rotation_window_fits(schedule, window).map_err(refuse)?;
        }
        if let Some(function) = &self.function {
            validate_rotation_function(function).map_err(refuse)?;
        }

        Ok(())
    }
}

/// What a rotation edit did beyond changing the settings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretRotationChange {
    /// A version that still holds the pending stage after rotation was
    /// turned off: the cancelled rotation never finished, and the store
    /// leaves the stage where it was. Until the labels are reconciled — the
    /// credentials behind that version checked and the stage moved or
    /// removed — a later rotation can be refused. `None` when nothing was
    /// left unfinished.
    pub unfinished_version: Option<String>,
}

/// What starting a rotation reports.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretRotationStart {
    /// The version the rotation creates, when the store reports it.
    pub version: Option<String>,
}

const SCHEDULE_FORMAT: &str = "A rotation schedule is `rate(N days)`, `rate(N hours)` with N of 4 or more, \
     or a six-field `cron(...)` expression.";

/// Checks a rotation schedule the way the service parses it, so a schedule
/// it would reject is refused before the call. `rate(N unit)` takes a
/// positive whole N, the singular unit for 1 and the plural otherwise, in
/// days or in hours with N of 4 or more (the shortest period rotation
/// accepts). `cron(...)` takes six space-separated fields of cron
/// characters. The error names the accepted forms.
pub fn validate_rotation_schedule(schedule: &str) -> Result<(), String> {
    let invalid = || format!("\"{schedule}\" is not a valid schedule. {SCHEDULE_FORMAT}");

    if let Some(inner) = schedule
        .strip_prefix("rate(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        let fields: Vec<&str> = inner.split(' ').collect();
        let [count, unit] = fields.as_slice() else {
            return Err(invalid());
        };
        let Some(count) = parse_positive(count) else {
            return Err(invalid());
        };

        let (singular, plural, minimum) = match *unit {
            "day" | "days" => ("day", "days", 1),
            "hour" | "hours" => ("hour", "hours", 4),
            _ => return Err(invalid()),
        };
        let expected = if count == 1 { singular } else { plural };

        return if *unit == expected && count >= minimum {
            Ok(())
        } else {
            Err(invalid())
        };
    }

    if let Some(inner) = schedule
        .strip_prefix("cron(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        let fields: Vec<&str> = inner.split(' ').collect();
        let well_formed = fields.len() == 6
            && fields.iter().all(|field| {
                !field.is_empty()
                    && field
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || "*?,-/#".contains(c))
            });

        return if well_formed { Ok(()) } else { Err(invalid()) };
    }

    Err(invalid())
}

/// Checks a rotation window: whole hours, `1h` to `24h`.
pub fn validate_rotation_window(window: &str) -> Result<(), String> {
    let hours = window.strip_suffix('h').and_then(parse_positive);

    match hours {
        Some(1..=24) => Ok(()),
        _ => Err(format!(
            "\"{window}\" is not a valid rotation window. A window is whole hours from `1h` to `24h`."
        )),
    }
}

/// Checks that a rotation window fits the interval of a schedule: a window
/// must not reach into the next rotation, so on `rate(N hours)` it is at
/// most N hours. A `rate(N days)` interval is a day or more, which every
/// window of up to 24 hours fits, and a `cron(...)` schedule has no interval
/// to compare with.
pub fn validate_rotation_window_fits(schedule: &str, window: &str) -> Result<(), String> {
    let interval_hours = schedule
        .strip_prefix("rate(")
        .and_then(|rest| rest.strip_suffix(')'))
        .and_then(|inner| inner.split_once(' '))
        .and_then(|(count, unit)| {
            matches!(unit, "hour" | "hours")
                .then(|| parse_positive(count))
                .flatten()
        });
    let window_hours = window.strip_suffix('h').and_then(parse_positive);

    match (interval_hours, window_hours) {
        (Some(interval), Some(hours)) if hours > interval => Err(format!(
            "the {window} window is longer than the {interval}-hour interval of \"{schedule}\"; \
             a window must not reach into the next rotation. Choose a window of {interval}h or less."
        )),
        _ => Ok(()),
    }
}

/// Checks that a rotation function is a Lambda function ARN:
/// `arn:<partition>:lambda:<region>:<12-digit account>:function:<name>`,
/// with an optional version or alias after the name.
pub fn validate_rotation_function(function: &str) -> Result<(), String> {
    let parts: Vec<&str> = function.split(':').collect();
    let well_formed = match parts.as_slice() {
        [
            "arn",
            partition,
            "lambda",
            region,
            account,
            "function",
            name,
            rest @ ..,
        ] => {
            partition.starts_with("aws")
                && !region.is_empty()
                && account.len() == 12
                && account.chars().all(|c| c.is_ascii_digit())
                && !name.is_empty()
                && rest.len() <= 1
                && rest.iter().all(|qualifier| !qualifier.is_empty())
        }
        _ => false,
    };

    if well_formed {
        Ok(())
    } else {
        Err(format!(
            "\"{function}\" is not a Lambda function ARN \
             (`arn:aws:lambda:<region>:<account>:function:<name>`)."
        ))
    }
}

/// A whole number of at least 1, from digits only: no sign, no fraction.
fn parse_positive(text: &str) -> Option<u32> {
    if text.is_empty() || !text.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    text.parse::<u32>().ok().filter(|value| *value > 0)
}

/// Most tags a secret can carry.
pub const MAX_TAGS: usize = 50;

/// A change to the tags of a secret: tags to add or overwrite, and keys to
/// remove. Tags not named are left as they are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretTagsUpdate {
    /// Full name of the secret, including any path.
    pub name: String,

    /// Tags to add, or to overwrite when the key already exists.
    pub set: Vec<SecretTag>,

    /// Keys to remove.
    pub remove: Vec<String>,
}

impl SecretTagsUpdate {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            set: Vec::new(),
            remove: Vec::new(),
        }
    }

    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.set.push(SecretTag::new(key, value));
        self
    }

    pub fn remove(mut self, key: impl Into<String>) -> Self {
        self.remove.push(key.into());
        self
    }

    /// The keys the secret carries after the update, given the keys it
    /// carries now.
    fn keys_with_additions<'a>(&'a self, existing_keys: &'a [String]) -> Vec<&'a str> {
        let mut keys: Vec<&str> = existing_keys.iter().map(String::as_str).collect();
        for added in &self.set {
            if !keys.contains(&added.key.as_str()) {
                keys.push(&added.key);
            }
        }

        keys
    }

    /// How many tags the secret carries after the update, given the keys it
    /// carries now.
    pub fn tags_after(&self, existing_keys: &[String]) -> usize {
        self.keys_with_additions(existing_keys)
            .into_iter()
            .filter(|kept| !self.remove.iter().any(|removed| removed == kept))
            .count()
    }

    /// Refuses an update that would leave the secret with more than
    /// [`MAX_TAGS`] tags, before any call.
    pub fn check_quota(&self, existing_keys: &[String]) -> Result<(), DbError> {
        let after = self.tags_after(existing_keys);

        if after > MAX_TAGS {
            return Err(DbError::constraint_violation(format!(
                "The tags of \"{}\" were not changed: the update would leave {after} tags, and \
                 a secret carries at most {MAX_TAGS}.",
                self.name
            )));
        }

        Ok(())
    }

    /// Whether the removals must go first: adding before removing would take
    /// the secret past [`MAX_TAGS`] for a moment, and the service refuses
    /// that even when the final count fits.
    pub fn removes_first(&self, existing_keys: &[String]) -> bool {
        !self.remove.is_empty() && self.keys_with_additions(existing_keys).len() > MAX_TAGS
    }

    /// Refuses what the service would reject, before any call: an update
    /// that changes nothing, a key or value outside the service's limits, a
    /// reserved `aws:` key, a key that appears twice or is both set and
    /// removed, and more than [`MAX_TAGS`] tags to set.
    pub fn validate(&self) -> Result<(), DbError> {
        let refuse = |reason: String| {
            DbError::constraint_violation(format!(
                "The tags of \"{}\" were not changed: {reason}",
                self.name
            ))
        };
        require_name(&self.name)?;

        if self.set.is_empty() && self.remove.is_empty() {
            return Err(refuse(
                "the update names no tag to set or remove.".to_string(),
            ));
        }
        if self.set.len() > MAX_TAGS {
            return Err(refuse(format!("a secret carries at most {MAX_TAGS} tags.")));
        }

        let mut seen: Vec<&str> = Vec::with_capacity(self.set.len());
        for tag in &self.set {
            validate_tag(&tag.key, &tag.value).map_err(refuse)?;

            if seen.contains(&tag.key.as_str()) {
                return Err(refuse(format!("the key \"{}\" is set twice.", tag.key)));
            }
            if self.remove.contains(&tag.key) {
                return Err(refuse(format!(
                    "the key \"{}\" is both set and removed.",
                    tag.key
                )));
            }
            seen.push(&tag.key);
        }

        for key in &self.remove {
            validate_tag_key(key).map_err(refuse)?;
        }

        Ok(())
    }
}

/// Checks a tag against the limits the services share: a key of 1 to 128
/// characters that does not start with the reserved `aws:` prefix, a value
/// of 0 to 256 characters, and only letters, digits, separators (spaces,
/// including Unicode ones) and `_ . : / = + - @` in both.
pub fn validate_tag(key: &str, value: &str) -> Result<(), String> {
    validate_tag_key(key)?;

    if value.chars().count() > 256 {
        return Err(format!(
            "the value of tag \"{key}\" is longer than 256 characters."
        ));
    }
    if let Some(bad) = value.chars().find(|c| !is_tag_character(*c)) {
        return Err(format!(
            "the value of tag \"{key}\" contains {bad:?}, which tags do not allow."
        ));
    }

    Ok(())
}

fn validate_tag_key(key: &str) -> Result<(), String> {
    let length = key.chars().count();

    if length == 0 || length > 128 {
        return Err(format!(
            "the tag key \"{key}\" must be 1 to 128 characters, not {length}."
        ));
    }
    if key.to_ascii_lowercase().starts_with("aws:") {
        return Err(format!(
            "the tag key \"{key}\" uses the reserved prefix \"aws:\"."
        ));
    }
    if let Some(bad) = key.chars().find(|c| !is_tag_character(*c)) {
        return Err(format!(
            "the tag key \"{key}\" contains {bad:?}, which tags do not allow."
        ));
    }

    Ok(())
}

/// The services accept letters, numbers and separators (`\p{L}\p{N}\p{Z}`)
/// plus `_ . : / = + - @`. A tab or a line feed is whitespace but not a
/// separator, so control characters stay out.
fn is_tag_character(character: char) -> bool {
    character.is_alphanumeric()
        || (character.is_whitespace() && !character.is_control())
        || "_.:/=+-@".contains(character)
}

/// A new description for a secret. An empty description clears it on a store
/// that can clear one; a store that cannot refuses the update. The
/// `DESCRIPTION` capability does not say which kind a store is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretDescriptionUpdate {
    /// Full name of the secret, including any path.
    pub name: String,

    pub description: String,
}

impl SecretDescriptionUpdate {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
        }
    }

    /// Refuses a blank name.
    pub fn validate(&self) -> Result<(), DbError> {
        require_name(&self.name)
    }
}

/// What a description change did to the secret's versions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretDescriptionChange {
    /// The version the change created, for a store that can change a
    /// description only by writing the value again, and `None` for one that
    /// changes it alone.
    pub version: Option<String>,
}

/// A new secret to create. The value travels as a `secrecy` type, so it is
/// redacted from `Debug` output.
///
/// The type, tier and key are the store's own names, as `SecretSummary`
/// reports them: a store refuses the fields it has no use for instead of
/// ignoring them, and fills in its own default for the ones left out.
#[derive(Debug, Clone)]
pub struct SecretCreateRequest {
    /// Full name of the secret, including any path.
    pub name: String,

    /// The initial value. Creating takes text.
    pub payload: SecretPayload,

    pub description: Option<String>,

    /// The value type, for a store with `TYPED_VALUES` (for example
    /// `SecureString`). `None` takes the store's default.
    pub value_type: Option<String>,

    /// The store's tier, for a store that has them. `None` takes the
    /// store's default.
    pub tier: Option<String>,

    /// The encryption key to use, as the store names it. `None` takes the
    /// store's default key.
    pub encryption_key: Option<String>,

    pub tags: Vec<SecretTag>,
}

impl SecretCreateRequest {
    pub fn text(name: impl Into<String>, text: impl Into<String>) -> Self {
        Self::new(name, SecretPayload::Text(SecretString::from(text.into())))
    }

    /// A binary payload, which `validate` refuses: creating takes text.
    pub fn binary(name: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self::new(name, SecretPayload::Binary(SecretSlice::from(bytes)))
    }

    fn new(name: impl Into<String>, payload: SecretPayload) -> Self {
        Self {
            name: name.into(),
            payload,
            description: None,
            value_type: None,
            tier: None,
            encryption_key: None,
            tags: Vec::new(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_value_type(mut self, value_type: impl Into<String>) -> Self {
        self.value_type = Some(value_type.into());
        self
    }

    pub fn with_tier(mut self, tier: impl Into<String>) -> Self {
        self.tier = Some(tier.into());
        self
    }

    pub fn with_encryption_key(mut self, key: impl Into<String>) -> Self {
        self.encryption_key = Some(key.into());
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.push(SecretTag::new(key, value));
        self
    }

    /// Refuses what no store accepts, before any call: a blank name, a
    /// binary or empty value, and tags outside the services' limits or more
    /// than [`MAX_TAGS`]. What only one store accepts (names, types, tiers)
    /// is that store's to check.
    pub fn validate(&self) -> Result<(), DbError> {
        let refuse = |reason: String| {
            DbError::constraint_violation(format!("\"{}\" was not created: {reason}", self.name))
        };

        if self.name.trim().is_empty() {
            return Err(refuse("it needs a name.".to_string()));
        }

        match &self.payload {
            SecretPayload::Text(text) if text.expose_secret().is_empty() => {
                return Err(refuse("it needs a value.".to_string()));
            }
            SecretPayload::Text(_) => {}
            SecretPayload::Binary(_) => {
                return Err(refuse("a new secret takes a text value.".to_string()));
            }
        }

        if self.tags.len() > MAX_TAGS {
            return Err(refuse(format!("a secret carries at most {MAX_TAGS} tags.")));
        }
        let mut seen: Vec<&str> = Vec::with_capacity(self.tags.len());
        for tag in &self.tags {
            validate_tag(&tag.key, &tag.value).map_err(refuse)?;

            if seen.contains(&tag.key.as_str()) {
                return Err(refuse(format!("the tag key \"{}\" is set twice.", tag.key)));
            }
            seen.push(&tag.key);
        }

        Ok(())
    }
}

/// What creating a secret reports.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretCreated {
    /// The version the creation made, when the store reports it.
    pub version: Option<String>,

    /// Provider-wide identifier of the new secret, when the store reports it.
    pub resource_id: Option<String>,
}
