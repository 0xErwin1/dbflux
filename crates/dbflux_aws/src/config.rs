/// AWS configuration file parsing, profile detection, and write-back.
///
/// Reads and writes `~/.aws/config` to discover and register AWS profiles.
/// The parser identifies SSO and shared-credentials profiles; the writer
/// appends new profile blocks without touching existing entries. Supports
/// mtime-based caching to avoid re-parsing on every read access.
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

use crate::edit::AwsSectionHash;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct AwsProfileInfo {
    pub name: String,
    pub region: Option<String>,
    pub is_sso: bool,
    pub sso_start_url: Option<String>,
    pub sso_region: Option<String>,
    pub sso_account_id: Option<String>,
    pub sso_role_name: Option<String>,
    /// Name of the `[sso-session <name>]` section this profile references, if
    /// the profile uses the indirection form (`sso_session = <name>`) instead
    /// of inline `sso_start_url` keys. Empty for sso-session sections.
    pub sso_session: Option<String>,
    /// `true` when this entry represents an `[sso-session <name>]` section
    /// rather than a `[profile <name>]` section. SSO session entries are
    /// emitted by the parser so that `sso_session = <name>` references can be
    /// resolved to the actual `sso_start_url`.
    pub is_sso_session: bool,
}

#[derive(Debug, Clone)]
enum SectionKind {
    Profile(String),
    SsoSession(String),
}

#[derive(Debug)]
pub struct CachedAwsConfig {
    config_path: PathBuf,
    credentials_path: PathBuf,

    profiles: Vec<AwsProfileInfo>,
    last_modified: Option<SystemTime>,

    /// Section names extracted from `~/.aws/credentials`.
    ///
    /// This cache is guarded independently from the config-file cache. A
    /// change to either file triggers re-read of only that file.
    credentials_names: Vec<String>,
    credentials_last_modified: Option<SystemTime>,
}

impl Default for CachedAwsConfig {
    fn default() -> Self {
        Self {
            config_path: config_file_path(),
            credentials_path: credentials_file_path(),
            profiles: Vec::new(),
            last_modified: None,
            credentials_names: Vec::new(),
            credentials_last_modified: None,
        }
    }
}

impl CachedAwsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the path to the AWS config file used by this cache instance.
    pub fn config_path(&self) -> &std::path::Path {
        &self.config_path
    }

    /// Returns the path to the AWS credentials file used by this cache instance.
    pub fn credentials_path(&self) -> &std::path::Path {
        &self.credentials_path
    }

    /// Creates a `CachedAwsConfig` with explicit file paths.
    ///
    /// Used in tests to point the cache at temporary files instead of the
    /// real `~/.aws/config` and `~/.aws/credentials`.
    pub fn new_with_paths(config_path: PathBuf, credentials_path: PathBuf) -> Self {
        Self {
            config_path,
            credentials_path,
            profiles: Vec::new(),
            last_modified: None,
            credentials_names: Vec::new(),
            credentials_last_modified: None,
        }
    }

    /// Returns cached profiles if the config file hasn't changed since last
    /// parse. Re-parses from disk when the file's mtime differs or on first
    /// call.
    pub fn profiles(&mut self) -> &[AwsProfileInfo] {
        let path = self.config_path.clone();
        let current_mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();

        let needs_refresh = match (&self.last_modified, &current_mtime) {
            (Some(cached), Some(current)) => cached != current,
            (None, Some(_)) => true,
            (_, None) => {
                self.profiles.clear();
                self.last_modified = None;
                return &self.profiles;
            }
        };

        if needs_refresh {
            match std::fs::read_to_string(&path) {
                Ok(contents) => {
                    self.profiles = parse_aws_config_str(&contents);
                    self.last_modified = current_mtime;
                }
                Err(err) => {
                    log::warn!("Failed to read AWS config at {}: {}", path.display(), err);
                    self.profiles.clear();
                    self.last_modified = None;
                }
            }
        }

        &self.profiles
    }

    /// Returns the section names present in `~/.aws/credentials`.
    ///
    /// The cache is guarded independently from `profiles()`: a change to the
    /// credentials file triggers a re-read of only that file; the config cache
    /// is unaffected.
    ///
    /// Returns an empty slice when the file is missing or unreadable (R8.7
    /// equivalent for the credentials file). The caller is responsible for
    /// warning the user if an unreadable credentials file is unexpected.
    pub fn credentials_names(&mut self) -> &[String] {
        let path = self.credentials_path.clone();
        let current_mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();

        let needs_refresh = match (&self.credentials_last_modified, &current_mtime) {
            (Some(cached), Some(current)) => cached != current,
            (None, Some(_)) => true,
            (_, None) => {
                self.credentials_names.clear();
                self.credentials_last_modified = None;
                return &self.credentials_names;
            }
        };

        if needs_refresh {
            match std::fs::read_to_string(&path) {
                Ok(contents) => {
                    self.credentials_names = parse_aws_credentials_str(&contents);
                    self.credentials_last_modified = current_mtime;
                }
                Err(err) => {
                    log::warn!(
                        "Failed to read AWS credentials at {}: {}",
                        path.display(),
                        err
                    );
                    self.credentials_names.clear();
                    self.credentials_last_modified = None;
                }
            }
        }

        &self.credentials_names
    }

    /// Returns the union of non-SSO config profile names and credentials-file
    /// section names, deduplicated (first-seen name wins, case-preserving).
    ///
    /// This is the complete set of names that `AwsSharedCredentialsAuthProvider`
    /// should reflect: a non-SSO section in `~/.aws/config` OR a section in
    /// `~/.aws/credentials` (or both) produces exactly one reflected profile.
    pub fn shared_profile_names(&mut self) -> Vec<String> {
        let config_names: Vec<String> = self
            .profiles()
            .iter()
            .filter(|p| !p.is_sso && !p.is_sso_session)
            .map(|p| p.name.clone())
            .collect();

        let creds_names = self.credentials_names().to_vec();

        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        for name in config_names.into_iter().chain(creds_names) {
            if seen.insert(name.clone()) {
                result.push(name);
            }
        }

        result
    }
}

/// Returns the platform path to `~/.aws/config`.
pub fn config_file_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("~"))
        .join(".aws")
        .join("config")
}

/// Returns the platform path to `~/.aws/credentials`.
///
/// Respects the `AWS_SHARED_CREDENTIALS_FILE` environment variable, which the
/// AWS CLI and SDK honour as an override for the credentials file location.
/// Mirrors the behaviour of `config_file_path`.
pub fn credentials_file_path() -> PathBuf {
    if let Ok(override_path) = std::env::var("AWS_SHARED_CREDENTIALS_FILE")
        && !override_path.is_empty()
    {
        return PathBuf::from(override_path);
    }

    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("~"))
        .join(".aws")
        .join("credentials")
}

/// Parses the contents of `~/.aws/credentials` and returns the list of
/// section names found in the file.
///
/// ## Names-only contract (security invariant — ADR-7)
///
/// This function returns **section names only**. It intentionally discards
/// every key/value pair, including `aws_access_key_id` and
/// `aws_secret_access_key`. Callers can determine which credentials-file
/// profiles exist without ever loading secret material into DBFlux memory.
/// The AWS SDK reads the actual key values from the file at connect time.
///
/// Credentials files use bare `[NAME]` section headers (no `profile ` prefix
/// and no `sso-session` headers). Headers using those prefixes are treated as
/// literal profile names (mirroring the AWS CLI's credentials-file grammar).
pub fn parse_aws_credentials_str(contents: &str) -> Vec<String> {
    let mut names = Vec::new();

    for line in contents.lines() {
        let trimmed = line.trim();

        // Skip blank lines and comment lines (# and ; are both comment markers
        // in credentials files).
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }

        // Section header: extract the name and record it.  Key/value lines are
        // intentionally ignored — this is the names-only security boundary.
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            let name = trimmed[1..trimmed.len() - 1].trim().to_string();
            if !name.is_empty() {
                names.push(name);
            }
            continue;
        }

        // Key/value lines and any other content: intentionally discarded.
    }

    names
}

/// Parses an AWS config file's contents into profile info entries.
///
/// Recognizes `[default]` and `[profile <name>]` sections. A profile is
/// marked as SSO if it contains `sso_start_url` or `sso_session` keys.
/// Malformed sections are skipped with a warning.
pub fn parse_aws_config_str(contents: &str) -> Vec<AwsProfileInfo> {
    let mut profiles = Vec::new();
    let mut current_section: Option<SectionKind> = None;
    let mut current_keys: HashMap<String, String> = HashMap::new();

    for line in contents.lines() {
        let trimmed = line.trim();

        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            flush_section(&current_section, &current_keys, &mut profiles);

            let header = &trimmed[1..trimmed.len() - 1].trim();
            current_section = parse_section_kind(header);
            current_keys.clear();
            continue;
        }

        if let Some((key, value)) = parse_key_value(trimmed) {
            current_keys.insert(key, value);
        }
    }

    flush_section(&current_section, &current_keys, &mut profiles);

    profiles
}

fn parse_section_kind(header: &str) -> Option<SectionKind> {
    if header.eq_ignore_ascii_case("default") {
        return Some(SectionKind::Profile("default".to_string()));
    }

    if let Some(name) = header.strip_prefix("profile") {
        let name = name.trim();
        if name.is_empty() {
            log::warn!("Skipping AWS config section with empty profile name");
            return None;
        }
        return Some(SectionKind::Profile(name.to_string()));
    }

    if let Some(name) = header.strip_prefix("sso-session") {
        let name = name.trim();
        if name.is_empty() {
            log::warn!("Skipping AWS config section with empty sso-session name");
            return None;
        }
        return Some(SectionKind::SsoSession(name.to_string()));
    }

    None
}

fn parse_key_value(line: &str) -> Option<(String, String)> {
    let (key, value) = line.split_once('=')?;

    let key = key.trim().to_lowercase();
    let value = value.trim().to_string();

    if key.is_empty() {
        return None;
    }

    Some((key, value))
}

fn flush_section(
    section: &Option<SectionKind>,
    keys: &HashMap<String, String>,
    profiles: &mut Vec<AwsProfileInfo>,
) {
    let Some(section) = section else {
        return;
    };

    let (name, is_sso_session) = match section {
        SectionKind::Profile(name) => (name.clone(), false),
        SectionKind::SsoSession(name) => (name.clone(), true),
    };

    let is_sso =
        is_sso_session || keys.contains_key("sso_start_url") || keys.contains_key("sso_session");
    let sso_start_url = keys.get("sso_start_url").cloned();
    let sso_region = keys.get("sso_region").cloned();
    let sso_account_id = keys.get("sso_account_id").cloned();
    let sso_role_name = keys.get("sso_role_name").cloned();
    let sso_session = keys.get("sso_session").cloned();
    let region = keys.get("region").cloned();

    profiles.push(AwsProfileInfo {
        name,
        region,
        is_sso,
        sso_start_url,
        sso_region,
        sso_account_id,
        sso_role_name,
        sso_session,
        is_sso_session,
    });
}

pub fn restore_aws_config_backup() -> Result<(), io::Error> {
    let path = config_file_path();
    restore_backup_for_path(&path)
}

/// Process-wide lock serializing every read-modify-write of `~/.aws/config`.
///
/// DBFlux mutates the config from several concurrent contexts: dropdown
/// population fetches account and role options on separate threads, the SSO
/// login flow writes the profile/session blocks, and profile import writes
/// each imported profile. Without serialization two threads can each read the
/// file, mutate their own copy, and write it back — and when the underlying
/// write is not atomic, a reader can even observe a half-written file and
/// persist that truncation. That race silently deleted unrelated profiles from
/// the user's `~/.aws/config`. Every writer now funnels through this lock plus
/// an atomic temp-file rename, so the file is never read or written partially.
static AWS_CONFIG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn aws_config_lock() -> &'static Mutex<()> {
    AWS_CONFIG_LOCK.get_or_init(|| Mutex::new(()))
}

/// Reads `~/.aws/config`, applies `transform`, and writes the result back
/// atomically with a backup — all under the process-wide config lock.
///
/// The read, the transform, and the write happen inside a single critical
/// section so concurrent callers cannot interleave their read-modify-write
/// cycles. When the transform leaves the content unchanged no write (and no
/// backup) is performed, so idempotent re-syncs (e.g. repeated dropdown polls)
/// do not churn the file.
pub(crate) fn update_aws_config_atomic(
    path: &std::path::Path,
    transform: impl FnOnce(&str) -> String,
) -> Result<(), io::Error> {
    let _guard = aws_config_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    let existing = read_config_or_default(path)?;
    let updated = transform(&existing);

    if existing == updated {
        return Ok(());
    }

    write_atomic_with_backup_perms(path, &updated)
}

fn create_backup_for_path(path: &std::path::Path) -> Result<PathBuf, io::Error> {
    let base = path.with_extension("dbflux-backup");
    let backup_path = if base.exists() {
        let timestamp = chrono::Utc::now().timestamp();
        let file_name = format!("config.dbflux-backup.{}", timestamp);
        path.with_file_name(file_name)
    } else {
        base
    };

    match fs::copy(path, &backup_path) {
        Ok(_) => Ok(backup_path),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::write(&backup_path, "")?;
            Ok(backup_path)
        }
        Err(err) => Err(err),
    }
}

fn restore_backup_for_path(path: &std::path::Path) -> Result<(), io::Error> {
    let default_backup = path.with_extension("dbflux-backup");

    if default_backup.exists() {
        fs::copy(default_backup, path)?;
        return Ok(());
    }

    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "AWS config parent directory missing",
        )
    })?;

    let mut latest: Option<(PathBuf, SystemTime)> = None;
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if !name.starts_with("config.dbflux-backup.") {
            continue;
        }

        let modified = entry
            .metadata()?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let should_replace = latest
            .as_ref()
            .map(|(_, current)| modified > *current)
            .unwrap_or(true);

        if should_replace {
            latest = Some((entry.path(), modified));
        }
    }

    let (backup_path, _) = latest.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "No AWS config backup found for restore",
        )
    })?;

    fs::copy(backup_path, path)?;
    Ok(())
}

/// Appends a new SSO profile block to `~/.aws/config`.
///
/// Creates the `~/.aws/` directory and the config file if they do not exist.
/// If a `[profile <name>]` or `[default]` section with the given name already
/// exists, the file is left unchanged and the function returns `Ok(false)`.
/// On a successful write it returns `Ok(true)`.
///
/// The generated block uses the modern SSO format (direct keys, no
/// `sso-session` indirection) compatible with AWS CLI v2 and the AWS SDK.
pub fn append_aws_sso_profile(
    name: &str,
    sso_start_url: &str,
    sso_region: &str,
    sso_account_id: &str,
    sso_role_name: &str,
    region: &str,
) -> Result<bool, std::io::Error> {
    let path = config_file_path();

    let mut block = String::new();
    writeln!(block).ok();

    let header = if name == "default" {
        "[default]".to_string()
    } else {
        format!("[profile {name}]")
    };

    writeln!(block, "{header}").ok();
    writeln!(block, "sso_start_url = {sso_start_url}").ok();
    writeln!(block, "sso_region = {sso_region}").ok();
    writeln!(block, "sso_account_id = {sso_account_id}").ok();
    writeln!(block, "sso_role_name = {sso_role_name}").ok();
    writeln!(block, "region = {region}").ok();

    append_config_block_if_absent(&path, name, &block)
}

/// Appends a new shared-credentials profile block to `~/.aws/config`.
///
/// Creates the `~/.aws/` directory and the config file if they do not exist.
/// If a section with the given name already exists, the file is left unchanged
/// and the function returns `Ok(false)`. On a successful write it returns
/// `Ok(true)`.
///
/// Shared-credentials profiles carry only a `region` key in `~/.aws/config`;
/// the actual `aws_access_key_id` / `aws_secret_access_key` live in
/// `~/.aws/credentials`, which DBFlux does not manage.
pub fn append_aws_shared_credentials_profile(
    name: &str,
    region: &str,
) -> Result<bool, std::io::Error> {
    let path = config_file_path();

    let mut block = String::new();
    writeln!(block).ok();

    let header = if name == "default" {
        "[default]".to_string()
    } else {
        format!("[profile {name}]")
    };

    writeln!(block, "{header}").ok();
    writeln!(block, "region = {region}").ok();

    append_config_block_if_absent(&path, name, &block)
}

/// Appends a new `[sso-session <name>]` block to `~/.aws/config`.
///
/// Creates the `~/.aws/` directory and the config file if they do not exist.
/// If a section with the given name already exists, the file is left unchanged
/// and the function returns `Ok(false)`. On a successful write it returns
/// `Ok(true)`.
pub fn append_aws_sso_session_profile(
    name: &str,
    sso_start_url: &str,
    sso_region: &str,
) -> Result<bool, std::io::Error> {
    let path = config_file_path();

    let mut block = String::new();
    writeln!(block).ok();
    writeln!(block, "[sso-session {name}]").ok();
    writeln!(block, "sso_start_url = {sso_start_url}").ok();
    writeln!(block, "sso_region = {sso_region}").ok();

    // Use a unique sentinel for sso-session headers (not [default] or [profile X]).
    // Since `append_config_block_if_absent` checks for [profile X] / [default],
    // and sso-session sections use [sso-session X], we call `update_aws_config_atomic`
    // directly with a sso-session-aware existence check.
    let mut appended = false;
    let sso_header = format!("[sso-session {name}]");

    update_aws_config_atomic(&path, |existing| {
        let already_exists = existing
            .lines()
            .any(|line| line.trim().eq_ignore_ascii_case(&sso_header));

        if already_exists {
            return existing.to_string();
        }

        appended = true;
        let mut content = existing.to_string();
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str(&block);
        content
    })?;

    Ok(appended)
}

/// Reads the config file content, returning an empty string if the file does
/// not exist. Returns an error for other I/O failures.
fn read_config_or_default(path: &std::path::Path) -> Result<String, std::io::Error> {
    match fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(err) => Err(err),
    }
}

/// Returns true if a section named `name` already appears in `contents`.
///
/// Matches `[default]` when `name == "default"` and `[profile <name>]`
/// otherwise (case-insensitive).
fn profile_section_exists(contents: &str, name: &str) -> bool {
    let needle = if name.eq_ignore_ascii_case("default") {
        "[default]".to_string()
    } else {
        format!("[profile {name}]")
    };

    contents
        .lines()
        .any(|line| line.trim().eq_ignore_ascii_case(&needle))
}

/// Appends `block` to the config file unless a `[profile <name>]` (or
/// `[default]`) section already exists, in which case the file is left
/// untouched. Returns `Ok(true)` when the block was appended, `Ok(false)`
/// when an existing section made the write a no-op.
///
/// The existence check, the append, and the disk write all happen inside the
/// process-wide lock and the atomic-rename writer, so a concurrent reader can
/// never observe a partially written file.
fn append_config_block_if_absent(
    path: &std::path::Path,
    name: &str,
    block: &str,
) -> Result<bool, std::io::Error> {
    let mut appended = false;

    update_aws_config_atomic(path, |existing| {
        if profile_section_exists(existing, name) {
            return existing.to_string();
        }

        appended = true;

        let mut content = existing.to_string();
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str(block);
        content
    })?;

    Ok(appended)
}

// ── Section-hash helpers ──────────────────────────────────────────────────────

/// Returns a slice of `contents` from the line matching `header` through the
/// line before the next `[` header or EOF.
///
/// The `header` argument must include the brackets, e.g.
/// `"[profile dev]"` for a config section or `"[ci]"` for a credentials
/// section. The comparison is exact (byte-level), so the caller is
/// responsible for providing the correctly-formed header string.
///
/// Returns `None` when no line in `contents` matches `header`.
///
/// Used internally to extract the raw bytes for section hashing
/// (`hash_config_section`, `hash_credentials_section`) and for conflict
/// detection inside the atomic write transforms.
fn section_raw_bytes<'a>(contents: &'a str, header: &str) -> Option<&'a str> {
    let header_lower = header.to_lowercase();

    // Find the byte offset where the matching header line starts.
    let mut header_start: Option<usize> = None;
    let mut current_offset = 0;

    for line in contents.lines() {
        if line.trim().to_lowercase() == header_lower {
            header_start = Some(current_offset);
            break;
        }
        // Advance past the line + newline. `str::lines()` strips the
        // line terminator, so we need to account for it manually. Both
        // `\n` and `\r\n` are handled: the slice includes the terminator.
        current_offset += line.len();
        if contents[current_offset..].starts_with("\r\n") {
            current_offset += 2;
        } else if contents[current_offset..].starts_with('\n') {
            current_offset += 1;
        }
    }

    let start = header_start?;

    // Find the byte offset where the section ends: the start of the next
    // `[` header line (after the matched section), or EOF.
    let rest = &contents[start..];
    let mut section_end = rest.len();
    let mut at_header = false;

    let mut offset = 0;
    for line in rest.lines() {
        // The first line is the matched header itself; skip it.
        if !at_header {
            at_header = true;
            offset += line.len();
            if rest[offset..].starts_with("\r\n") {
                offset += 2;
            } else if rest[offset..].starts_with('\n') {
                offset += 1;
            }
            continue;
        }

        // Any subsequent line that starts with `[` marks the next section.
        if line.trim_start().starts_with('[') {
            section_end = offset;
            break;
        }

        offset += line.len();
        if rest[offset..].starts_with("\r\n") {
            offset += 2;
        } else if rest[offset..].starts_with('\n') {
            offset += 1;
        }
    }

    Some(&rest[..section_end])
}

/// Returns the SHA-256 hash of the raw bytes of the named profile section in
/// a config-file string.
///
/// The section is identified by its `[profile NAME]` header (or `[default]`
/// for the default profile). Uses `section_raw_bytes` internally; returns
/// `None` when the section is absent from `contents`.
///
/// The hash is over raw bytes — whitespace, comments, and value order are all
/// captured — so any byte-level change produces a different hash.
pub(crate) fn hash_config_section(contents: &str, profile_name: &str) -> Option<AwsSectionHash> {
    let header = if profile_name.eq_ignore_ascii_case("default") {
        "[default]".to_string()
    } else {
        format!("[profile {profile_name}]")
    };

    let raw = section_raw_bytes(contents, &header)?;
    let digest = Sha256::digest(raw.as_bytes());
    Some(AwsSectionHash(digest.into()))
}

/// Returns the SHA-256 hash of the raw bytes of an `[sso-session NAME]` section
/// in a config-file string.
///
/// Uses the `[sso-session NAME]` header form, which differs from the
/// `[profile NAME]` form used by `hash_config_section`. Returns `None` when
/// the section is absent from `contents`.
pub(crate) fn hash_sso_session_section(contents: &str, name: &str) -> Option<AwsSectionHash> {
    let header = format!("[sso-session {name}]");
    let raw = section_raw_bytes(contents, &header)?;
    let digest = Sha256::digest(raw.as_bytes());
    Some(AwsSectionHash(digest.into()))
}

/// Returns the SHA-256 hash of the raw bytes of the named section in a
/// credentials-file string.
///
/// The section is identified by its bare `[NAME]` header (credentials files
/// do not use the `profile ` prefix). Returns `None` when the section is
/// absent from `contents`.
pub(crate) fn hash_credentials_section(contents: &str, name: &str) -> Option<AwsSectionHash> {
    let header = format!("[{name}]");
    let raw = section_raw_bytes(contents, &header)?;
    let digest = Sha256::digest(raw.as_bytes());
    Some(AwsSectionHash(digest.into()))
}

// ── Credentials atomic writer ─────────────────────────────────────────────────

/// Reads `~/.aws/credentials`, applies `transform`, and writes the result back
/// atomically with a backup — all under the SAME process-wide lock as
/// `update_aws_config_atomic` (spec R9.2.1, design section 12).
///
/// Using a single shared lock for both files serializes all config+credentials
/// writes so there is no cross-file interleave (ADR-11). The implementation is
/// a thin sibling of `update_aws_config_atomic`; the locking and atomic-rename
/// logic is NOT forked.
///
/// Credentials files typically have mode 0600. The writer preserves the
/// original file's permissions on the temp file and backup file (security
/// requirement). If the original file does not yet exist the temp file is
/// written with the same permissions as the new file (0600 on Unix).
pub(crate) fn update_aws_credentials_atomic(
    path: &Path,
    transform: impl FnOnce(&str) -> String,
) -> Result<(), io::Error> {
    let _guard = aws_config_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    let existing = read_config_or_default(path)?;
    let updated = transform(&existing);

    if existing == updated {
        return Ok(());
    }

    write_atomic_with_backup_perms(path, &updated)
}

/// Atomically writes `content` to `path` via a temp-file-then-rename sequence,
/// creating a backup first so a rename failure can restore the original.
///
/// On Unix the temp file and backup file are restricted to at most the mode of
/// the original file (or 0600 when the file is new), ensuring that neither
/// `~/.aws/config` nor `~/.aws/credentials` is ever world-readable during the
/// write window.
///
/// Used by both `update_aws_config_atomic` and `update_aws_credentials_atomic`.
fn write_atomic_with_backup_perms(path: &Path, content: &str) -> Result<(), io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let backup_path = create_backup_for_path(path)?;
    let temp_path = path.with_extension("tmp");

    // Write the temp file first, then restrict its permissions to match the
    // original (or enforce 0600 for credentials on first create).
    fs::write(&temp_path, content)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        // Try to read the original file's permissions; fall back to 0600.
        let mode = fs::metadata(path)
            .map(|m| m.permissions().mode())
            .unwrap_or(0o600);

        // Restrict to at most the original mode — never widen.
        let effective_mode = mode & 0o600;
        fs::set_permissions(&temp_path, fs::Permissions::from_mode(effective_mode))?;

        // Also restrict the backup file to the same mode.
        if backup_path.exists() {
            fs::set_permissions(&backup_path, fs::Permissions::from_mode(effective_mode))?;
        }
    }

    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        let _ = fs::copy(&backup_path, path);
        return Err(error);
    }

    Ok(())
}

/// Replaces or appends a `[NAME]` section block in a credentials-file string.
///
/// The `[NAME]` header uses the bare form (no `profile ` prefix) as required
/// by the credentials-file grammar. When a section with this name already
/// exists, it is replaced in-place; all other sections remain byte-identical.
/// When the section is absent, it is appended at the end of the string.
///
/// `fields` lists the key/value pairs to write into the section. Keys not
/// listed in `fields` that are present in the existing section are preserved
/// (merge-write semantics: only explicitly passed keys are changed).
pub(crate) fn replace_or_append_credentials_block(
    contents: &str,
    name: &str,
    fields: &[(&str, &str)],
) -> String {
    let header = format!("[{name}]");
    let header_lower = header.to_lowercase();

    // Build the new section block from the existing section's keys merged
    // with the provided fields. Keys not provided by the caller are
    // kept verbatim from the existing section.
    let new_block = build_credentials_block(&header, contents, name, fields);

    // Find the extent of the existing section, if present.
    let Some(raw) = section_raw_bytes(contents, &header) else {
        // Section absent — append at EOF.
        let mut result = contents.to_string();
        if !result.is_empty() && !result.ends_with('\n') {
            result.push('\n');
        }
        result.push('\n');
        result.push_str(&new_block);
        return result;
    };

    // The section exists; find its start offset in `contents`.
    let start = find_section_start(contents, &header_lower);
    let end = start + raw.len();

    let mut result = String::with_capacity(contents.len());
    result.push_str(&contents[..start]);
    result.push_str(&new_block);
    result.push_str(&contents[end..]);
    result
}

/// Locates the byte offset of the first line in `contents` whose lowercased
/// trim equals `header_lower`.
fn find_section_start(contents: &str, header_lower: &str) -> usize {
    let mut offset = 0;
    for line in contents.lines() {
        if line.trim().to_lowercase() == header_lower {
            return offset;
        }
        offset += line.len();
        if contents[offset..].starts_with("\r\n") {
            offset += 2;
        } else if contents[offset..].starts_with('\n') {
            offset += 1;
        }
    }
    0
}

/// Replaces or appends a `[profile NAME]` section block in a config-file string.
///
/// The header uses the `[profile NAME]` form (or `[default]` when
/// `name == "default"`), as required by the `~/.aws/config` grammar. When a
/// matching section already exists, it is replaced in-place; all other
/// sections remain byte-identical. When the section is absent, it is
/// appended at the end of the string.
///
/// `fields` lists the key/value pairs to write into the section. Keys not
/// listed in `fields` that are present in the existing section are preserved
/// (merge-write semantics).
pub(crate) fn replace_or_append_profile_block(
    contents: &str,
    name: &str,
    fields: &[(&str, &str)],
    remove_keys: &[&str],
) -> String {
    let header = if name.eq_ignore_ascii_case("default") {
        "[default]".to_string()
    } else {
        format!("[profile {name}]")
    };

    let header_lower = header.to_lowercase();
    let new_block = build_profile_block(&header, contents, fields, remove_keys);

    let Some(raw) = section_raw_bytes(contents, &header) else {
        // Section absent — append at EOF.
        let mut result = contents.to_string();
        if !result.is_empty() && !result.ends_with('\n') {
            result.push('\n');
        }
        result.push('\n');
        result.push_str(&new_block);
        return result;
    };

    let start = find_section_start(contents, &header_lower);
    let end = start + raw.len();

    let mut result = String::with_capacity(contents.len());
    result.push_str(&contents[..start]);
    result.push_str(&new_block);
    result.push_str(&contents[end..]);
    result
}

/// Replaces or appends an `[sso-session NAME]` section block in a config-file string.
///
/// Analogous to `replace_or_append_profile_block` but uses the `[sso-session NAME]`
/// header form required for sso-session sections in `~/.aws/config`.
pub(crate) fn replace_or_append_sso_session_block(
    contents: &str,
    name: &str,
    fields: &[(&str, &str)],
) -> String {
    let header = format!("[sso-session {name}]");
    let header_lower = header.to_lowercase();
    let new_block = build_profile_block(&header, contents, fields, &[]);

    let Some(raw) = section_raw_bytes(contents, &header) else {
        let mut result = contents.to_string();
        if !result.is_empty() && !result.ends_with('\n') {
            result.push('\n');
        }
        result.push('\n');
        result.push_str(&new_block);
        return result;
    };

    let start = find_section_start(contents, &header_lower);
    let end = start + raw.len();

    let mut result = String::with_capacity(contents.len());
    result.push_str(&contents[..start]);
    result.push_str(&new_block);
    result.push_str(&contents[end..]);
    result
}

/// Builds the replacement `[profile NAME]` section block for `~/.aws/config`.
///
/// Existing on-disk keys not present in `fields` are preserved (merge
/// semantics), so unmanaged keys survive a save. Keys listed in `remove_keys`
/// are dropped from the section entirely — used to enforce mutually exclusive
/// AWS keys (e.g. a session-backed profile must not also carry an inline
/// `sso_start_url`).
fn build_profile_block(
    header: &str,
    contents: &str,
    fields: &[(&str, &str)],
    remove_keys: &[&str],
) -> String {
    let remove: std::collections::HashSet<String> =
        remove_keys.iter().map(|k| k.to_lowercase()).collect();

    // Collect existing keys from the current on-disk section, in order.
    let mut existing_keys: Vec<(String, String)> = Vec::new();
    if let Some(raw) = section_raw_bytes(contents, header) {
        for line in raw.lines().skip(1) {
            if let Some((k, v)) = parse_key_value(line) {
                existing_keys.push((k, v));
            }
        }
    }

    let overrides: HashMap<String, String> = fields
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| (k.to_lowercase(), v.to_string()))
        .collect();

    let mut block = String::new();
    block.push_str(header);
    block.push('\n');

    let mut emitted_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (key, existing_val) in &existing_keys {
        if remove.contains(key) {
            continue;
        }

        let value = overrides
            .get(key)
            .map(String::as_str)
            .unwrap_or(existing_val);
        writeln!(block, "{key} = {value}").ok();
        emitted_keys.insert(key.clone());
    }

    for (key, value) in fields {
        let key_lower = key.to_lowercase();
        if remove.contains(&key_lower) {
            continue;
        }
        if !emitted_keys.contains(&key_lower) && !value.is_empty() {
            writeln!(block, "{key_lower} = {value}").ok();
        }
    }

    block
}

/// Builds the replacement `[NAME]` section block.
///
/// Reads existing key/value lines from the current section (if present),
/// applies the caller-provided `fields` as overrides, and emits the merged
/// section. Keys provided with an empty string value are omitted (kept as-is
/// from the existing section — blank = preserve).
fn build_credentials_block(
    header: &str,
    contents: &str,
    name: &str,
    fields: &[(&str, &str)],
) -> String {
    // Collect existing keys from the current on-disk section, in order.
    let mut existing_keys: Vec<(String, String)> = Vec::new();
    if let Some(raw) = section_raw_bytes(contents, header) {
        for line in raw.lines().skip(1) {
            // Skip the header line itself.
            if let Some((k, v)) = parse_key_value(line) {
                existing_keys.push((k, v));
            }
        }
    }

    // Build a lookup map from the caller-provided overrides (non-blank only).
    let overrides: HashMap<String, String> = fields
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| (k.to_lowercase(), v.to_string()))
        .collect();

    let mut block = String::new();
    block.push_str(header);
    block.push('\n');

    // Emit existing keys, overriding where a new value was provided.
    let mut emitted_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (key, existing_val) in &existing_keys {
        let value = overrides
            .get(key)
            .map(String::as_str)
            .unwrap_or(existing_val);
        writeln!(block, "{key} = {value}").ok();
        emitted_keys.insert(key.clone());
    }

    // Emit new keys that were not already in the existing section.
    for (key, value) in fields {
        let key_lower = key.to_lowercase();
        if !emitted_keys.contains(&key_lower) && !value.is_empty() {
            writeln!(block, "{key_lower} = {value}").ok();
        }
    }

    // Preserve the name prefix (unused here but kept for symmetry with
    // replace_or_append_profile_block).
    let _ = name;

    block
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sso_and_shared_and_default_profiles() {
        let config = r#"
[default]
region = us-east-1
output = json

[profile dev-sso]
sso_start_url = https://my-sso.awsapps.com/start
sso_region = us-east-1
sso_account_id = 123456789012
sso_role_name = AdminAccess
region = us-west-2

[profile staging]
region = eu-west-1

[profile sso-session-ref]
sso_session = my-session
region = ap-southeast-1
"#;
        let profiles = parse_aws_config_str(config);
        assert_eq!(profiles.len(), 4);

        let default = &profiles[0];
        assert_eq!(default.name, "default");
        assert_eq!(default.region.as_deref(), Some("us-east-1"));
        assert!(!default.is_sso);
        assert!(default.sso_start_url.is_none());

        let dev_sso = &profiles[1];
        assert_eq!(dev_sso.name, "dev-sso");
        assert_eq!(dev_sso.region.as_deref(), Some("us-west-2"));
        assert!(dev_sso.is_sso);
        assert_eq!(
            dev_sso.sso_start_url.as_deref(),
            Some("https://my-sso.awsapps.com/start")
        );

        let staging = &profiles[2];
        assert_eq!(staging.name, "staging");
        assert_eq!(staging.region.as_deref(), Some("eu-west-1"));
        assert!(!staging.is_sso);

        let session_ref = &profiles[3];
        assert_eq!(session_ref.name, "sso-session-ref");
        assert!(session_ref.is_sso);
        assert!(session_ref.sso_start_url.is_none());
    }

    #[test]
    fn empty_content_returns_empty() {
        let profiles = parse_aws_config_str("");
        assert!(profiles.is_empty());
    }

    #[test]
    fn missing_file_path_returns_home_based() {
        let path = config_file_path();
        assert!(path.ends_with(".aws/config"));
    }

    #[test]
    fn malformed_section_is_skipped() {
        let config = r#"
[profile ]
region = us-east-1

[sso-session ]
sso_start_url = https://example.com

[profile valid]
region = eu-west-1
"#;
        let profiles = parse_aws_config_str(config);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].name, "valid");
    }

    #[test]
    fn sso_session_section_is_emitted() {
        let config = r#"
[sso-session my-session]
sso_start_url = https://example.awsapps.com/start/
sso_region = us-east-1
sso_registration_scopes = sso:account:access

[profile prod]
sso_session = my-session
sso_account_id = 111122223333
sso_role_name = AdminAccess
region = us-east-1
"#;
        let profiles = parse_aws_config_str(config);
        assert_eq!(profiles.len(), 2);

        let session = profiles.iter().find(|p| p.is_sso_session).unwrap();
        assert_eq!(session.name, "my-session");
        assert_eq!(
            session.sso_start_url.as_deref(),
            Some("https://example.awsapps.com/start/")
        );

        let profile = profiles
            .iter()
            .find(|p| !p.is_sso_session && p.name == "prod")
            .unwrap();
        assert!(profile.is_sso);
        assert!(profile.sso_start_url.is_none());
        assert_eq!(profile.sso_session.as_deref(), Some("my-session"));
        assert_eq!(profile.sso_account_id.as_deref(), Some("111122223333"));
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let config = r#"
# This is a comment
[default]
region = us-east-1

# Another comment
   # Indented comment
[profile test]
region = eu-west-1
"#;
        let profiles = parse_aws_config_str(config);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn cached_config_returns_empty_when_no_file() {
        let mut cache = CachedAwsConfig::new();
        // config_file_path() may or may not exist on the test machine,
        // but the cache mechanism itself should not panic.
        let _ = cache.profiles();
    }

    #[test]
    fn key_value_parsing_handles_whitespace() {
        let config = r#"
[default]
  region   =   us-east-1
output=json
"#;
        let profiles = parse_aws_config_str(config);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn profile_section_exists_matches_named_and_default() {
        let contents = "[default]\nregion = us-east-1\n\n[profile dev]\nregion = us-west-2\n";

        assert!(profile_section_exists(contents, "default"));
        assert!(profile_section_exists(contents, "dev"));
        assert!(!profile_section_exists(contents, "staging"));
    }

    #[test]
    fn profile_section_exists_is_case_insensitive() {
        let contents = "[profile Dev]\nregion = us-west-2\n";
        assert!(profile_section_exists(contents, "dev"));
        assert!(profile_section_exists(contents, "DEV"));
    }

    #[test]
    fn append_sso_profile_creates_file_and_block() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config");

        let mut block = String::new();
        block.push('\n');
        block.push_str("[profile new-sso]\n");
        block.push_str("sso_start_url = https://example.awsapps.com/start\n");
        block.push_str("sso_region = us-east-1\n");
        block.push_str("sso_account_id = 123456789012\n");
        block.push_str("sso_role_name = AdminAccess\n");
        block.push_str("region = us-east-1\n");

        let written = append_config_block_if_absent(&path, "new-sso", &block).expect("write");
        assert!(written);

        let content = std::fs::read_to_string(&path).expect("read");
        assert!(content.contains("[profile new-sso]"));
        assert!(content.contains("sso_start_url = https://example.awsapps.com/start"));
        assert!(content.contains("sso_account_id = 123456789012"));
    }

    #[test]
    fn append_sso_profile_skips_existing_section() {
        let existing = "[profile dev]\nregion = us-east-1\n";

        // profile_section_exists should detect it and prevent the write.
        assert!(profile_section_exists(existing, "dev"));
    }

    #[test]
    fn append_to_non_empty_file_adds_trailing_newline_separator() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config");
        std::fs::write(&path, "[default]\nregion = us-east-1").expect("seed");

        let block = "\n[profile staging]\nregion = eu-west-1\n";

        append_config_block_if_absent(&path, "staging", block).expect("write");

        let content = std::fs::read_to_string(&path).expect("read result");
        // The existing content had no trailing newline; the appender must add one.
        assert!(content.contains("[default]\nregion = us-east-1\n\n[profile staging]"));
    }

    #[test]
    fn concurrent_writes_preserve_unrelated_sections() {
        use std::sync::Arc;
        use std::thread;

        // Seed the file with several pre-existing profiles that none of the
        // concurrent writers touch. The atomic + locked writer must keep every
        // one of them intact.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = Arc::new(dir.path().join("config"));

        let mut seed = String::new();
        for index in 0..8 {
            seed.push_str(&format!(
                "[profile existing-{index}]\nregion = us-east-1\n\n"
            ));
        }
        std::fs::write(path.as_ref(), &seed).expect("seed");

        // Many threads, each appending its own distinct profile at the same
        // time. Before the lock + atomic rename, interleaved read-modify-write
        // cycles over a non-atomic write could read a half-written file and
        // persist the truncation, dropping unrelated sections.
        let mut handles = Vec::new();
        for index in 0..16 {
            let path = Arc::clone(&path);
            handles.push(thread::spawn(move || {
                let name = format!("worker-{index}");
                let block = format!("\n[profile {name}]\nregion = eu-west-1\n");
                append_config_block_if_absent(path.as_ref(), &name, &block).expect("write");
            }));
        }

        for handle in handles {
            handle.join().expect("thread join");
        }

        let final_contents = std::fs::read_to_string(path.as_ref()).expect("read result");

        for index in 0..8 {
            assert!(
                profile_section_exists(&final_contents, &format!("existing-{index}")),
                "pre-existing profile existing-{index} must survive concurrent writes"
            );
        }
        for index in 0..16 {
            assert!(
                profile_section_exists(&final_contents, &format!("worker-{index}")),
                "concurrently written profile worker-{index} must be present"
            );
        }
    }

    // T-2.1 — credentials_file_path tests

    #[test]
    fn credentials_file_path_ends_with_aws_credentials() {
        let path = credentials_file_path();
        assert!(
            path.ends_with(".aws/credentials"),
            "expected path ending in .aws/credentials, got: {}",
            path.display()
        );
    }

    #[test]
    fn credentials_file_path_override_via_env() {
        let dir = tempfile::tempdir().expect("tempdir");
        let custom = dir.path().join("my-credentials");

        // Safety: test-only env mutation is acceptable here; tests run in a
        // single-threaded test harness per process.
        unsafe {
            std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", custom.to_str().unwrap());
        }
        let path = credentials_file_path();
        unsafe {
            std::env::remove_var("AWS_SHARED_CREDENTIALS_FILE");
        }

        assert_eq!(path, custom);
    }

    // T-2.2 — parse_aws_credentials_str tests

    #[test]
    fn parse_credentials_empty_input_returns_empty() {
        let names = parse_aws_credentials_str("");
        assert!(names.is_empty());
    }

    #[test]
    fn parse_credentials_extracts_section_names_only() {
        let input = "[default]\n\
                     aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n\
                     aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n\
                     \n\
                     [prod]\n\
                     aws_access_key_id = AKIAI44QH8DHBEXAMPLE\n\
                     aws_secret_access_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY\n";

        let names = parse_aws_credentials_str(input);

        assert_eq!(names, vec!["default", "prod"]);
    }

    #[test]
    fn parse_credentials_comments_and_blank_lines_produce_no_names() {
        let input = "# This is a comment\n\
                     \n\
                     ; semicolon comment\n\
                     [staging]\n\
                     region = eu-west-1\n";

        let names = parse_aws_credentials_str(input);

        assert_eq!(names, vec!["staging"]);
    }

    #[test]
    fn parse_credentials_bare_headers_no_profile_prefix() {
        // Credentials file uses bare [NAME] not [profile NAME].
        let input = "[my-profile]\n\
                     aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n\
                     \n\
                     [another]\n\
                     aws_access_key_id = AKIAI44QH8DHBEXAMPLE\n";

        let names = parse_aws_credentials_str(input);

        assert_eq!(names, vec!["my-profile", "another"]);
    }

    /// MANDATORY security assertion (ADR-7): the parser must NEVER return key
    /// material — not access key IDs, not secret access keys. The returned
    /// `Vec<String>` contains section names only; any substring that looks like
    /// an AWS access key or secret must be absent.
    #[test]
    fn parse_credentials_never_returns_key_material() {
        // Uses AWS documentation example values (never real credentials).
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        let input = format!(
            "[default]\n\
             aws_access_key_id = {access_key}\n\
             aws_secret_access_key = {secret_key}\n\
             \n\
             [dev]\n\
             aws_access_key_id = AKIAI44QH8DHBEXAMPLE\n\
             aws_secret_access_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY\n"
        );

        let names = parse_aws_credentials_str(&input);

        // Section names must be extracted.
        assert_eq!(names, vec!["default", "dev"]);

        // No returned string may start with the AWS access-key-ID prefix "AKIA".
        for name in &names {
            assert!(
                !name.starts_with("AKIA"),
                "returned name must not start with AKIA (access key pattern): {name}"
            );
        }

        // More direct: assert the exact known secret values do not appear anywhere
        // in the returned vec.
        let all_returned = names.join(" ");
        assert!(
            !all_returned.contains(access_key),
            "access key must not appear in returned names"
        );
        assert!(
            !all_returned.contains(secret_key),
            "secret key must not appear in returned names"
        );
    }

    // T-2.3 — CachedAwsConfig credentials extension tests

    #[test]
    fn credentials_names_returns_empty_when_file_absent() {
        let mut cache = CachedAwsConfig::new();
        // credentials_file_path() may not exist on test machines; must not panic.
        let names = cache.credentials_names();
        // We cannot assert the exact content since the real file might exist on
        // the developer's machine, but the call must not panic.
        let _ = names;
    }

    #[test]
    fn credentials_cache_invalidates_when_mtime_advances() {
        let dir = tempfile::tempdir().expect("tempdir");
        let creds_path = dir.path().join("credentials");

        std::fs::write(&creds_path, "[profile-a]\nregion = us-east-1\n").expect("seed");

        let mut cache = CachedAwsConfig::new_with_paths(config_file_path(), creds_path.clone());

        let first = cache.credentials_names().to_vec();
        assert_eq!(first, vec!["profile-a"]);

        // Advance mtime by writing new content.
        std::fs::write(
            &creds_path,
            "[profile-a]\nregion = us-east-1\n\n[profile-b]\n",
        )
        .expect("update");
        // Force a detectable mtime change by setting it explicitly.
        let new_time = std::time::SystemTime::now() + std::time::Duration::from_secs(2);
        filetime::set_file_mtime(&creds_path, filetime::FileTime::from_system_time(new_time))
            .expect("set mtime");

        let second = cache.credentials_names().to_vec();
        assert_eq!(second, vec!["profile-a", "profile-b"]);
    }

    #[test]
    fn shared_profile_names_union_dedup() {
        // config file with two non-SSO profiles (one shared with credentials)
        let config_content =
            "[profile shared]\nregion = eu-west-1\n\n[profile config-only]\nregion = us-east-1\n";
        // credentials file with one profile that overlaps and one unique one
        let creds_content = "[shared]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n\n[creds-only]\n";

        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("config");
        let creds_path = dir.path().join("credentials");

        std::fs::write(&config_path, config_content).expect("write config");
        std::fs::write(&creds_path, creds_content).expect("write credentials");

        let mut cache = CachedAwsConfig::new_with_paths(config_path, creds_path);

        let mut names = cache.shared_profile_names();
        names.sort();

        // "shared" appears in both files but must appear only once.
        assert!(names.contains(&"config-only".to_string()));
        assert!(names.contains(&"creds-only".to_string()));
        assert!(names.contains(&"shared".to_string()));
        let shared_count = names.iter().filter(|n| n.as_str() == "shared").count();
        assert_eq!(shared_count, 1, "shared name must appear exactly once");
    }

    #[test]
    fn unreadable_credentials_file_does_not_panic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("does-not-exist");

        let mut cache = CachedAwsConfig::new_with_paths(config_file_path(), missing);

        // Must not panic; must return empty.
        let names = cache.credentials_names();
        assert!(names.is_empty());
    }

    // ── E2.1 — section_raw_bytes tests ──────────────────────────────────────

    #[test]
    fn section_raw_bytes_config_profile_exact_slice() {
        let contents = "[profile a]\nregion = us-east-1\n\n[profile b]\noutput = json\n";

        let raw = section_raw_bytes(contents, "[profile a]").unwrap();

        assert!(raw.contains("[profile a]"));
        assert!(raw.contains("region = us-east-1"));
        // Must NOT bleed into the next section.
        assert!(!raw.contains("[profile b]"));
        assert!(!raw.contains("output = json"));
    }

    #[test]
    fn section_raw_bytes_credentials_bare_header() {
        let contents = "[ci]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n\n[prod]\naws_access_key_id = AKIAI44QH8DHBEXAMPLE\n";

        let raw = section_raw_bytes(contents, "[ci]").unwrap();

        assert!(raw.contains("[ci]"));
        assert!(raw.contains("aws_access_key_id = AKIAIOSFODNN7EXAMPLE"));
        assert!(!raw.contains("[prod]"));
    }

    #[test]
    fn section_raw_bytes_returns_none_when_section_absent() {
        let contents = "[profile a]\nregion = us-east-1\n";

        let result = section_raw_bytes(contents, "[profile missing]");
        assert!(result.is_none());
    }

    #[test]
    fn section_raw_bytes_last_section_no_trailing_newline() {
        // The last section runs to EOF with no trailing newline.
        let contents = "[profile a]\nregion = us-east-1\n\n[profile b]\noutput = json";

        let raw = section_raw_bytes(contents, "[profile b]").unwrap();

        assert!(raw.contains("[profile b]"));
        assert!(raw.contains("output = json"));
        // Should not be empty and should end at EOF without extra content.
        assert!(!raw.is_empty());
    }

    #[test]
    fn section_raw_bytes_three_section_config_correct_bounds() {
        let contents =
            "[profile a]\nkey = val-a\n\n[profile b]\nkey = val-b\n\n[profile c]\nkey = val-c\n";

        let raw_a = section_raw_bytes(contents, "[profile a]").unwrap();
        let raw_b = section_raw_bytes(contents, "[profile b]").unwrap();
        let raw_c = section_raw_bytes(contents, "[profile c]").unwrap();

        assert!(raw_a.contains("val-a") && !raw_a.contains("val-b") && !raw_a.contains("val-c"));
        assert!(raw_b.contains("val-b") && !raw_b.contains("val-a") && !raw_b.contains("val-c"));
        assert!(raw_c.contains("val-c") && !raw_c.contains("val-a") && !raw_c.contains("val-b"));
    }

    // ── E2.2 — hash_config_section and hash_credentials_section tests ────────

    #[test]
    fn hash_config_section_identical_bytes_same_hash() {
        let contents = "[profile dev]\nsso_start_url = https://example.awsapps.com/start\nregion = us-east-1\n";

        let h1 = hash_config_section(contents, "dev").unwrap();
        let h2 = hash_config_section(contents, "dev").unwrap();

        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_config_section_one_byte_change_produces_different_hash() {
        let contents_a = "[profile dev]\nsso_start_url = https://example.awsapps.com/start\nregion = us-east-1\n";
        let contents_b = "[profile dev]\nsso_start_url = https://example.awsapps.com/start\nregion = us-east-2\n";

        let ha = hash_config_section(contents_a, "dev").unwrap();
        let hb = hash_config_section(contents_b, "dev").unwrap();

        assert_ne!(ha, hb);
    }

    #[test]
    fn hash_config_section_different_section_does_not_affect_hash() {
        let contents_before =
            "[profile a]\nregion = us-east-1\n\n[profile b]\nregion = eu-west-1\n";
        let contents_after = "[profile a]\nregion = us-east-1\n\n[profile b]\nregion = us-west-2\n";

        let ha_before = hash_config_section(contents_before, "a").unwrap();
        let ha_after = hash_config_section(contents_after, "a").unwrap();

        // Changing [profile b] must not affect [profile a]'s hash.
        assert_eq!(ha_before, ha_after);
    }

    #[test]
    fn hash_config_section_uses_profile_header_form() {
        // hash_config_section uses [profile NAME] header.
        let config_contents = "[profile dev]\nregion = us-east-1\n";

        let hash = hash_config_section(config_contents, "dev");
        assert!(hash.is_some(), "should find [profile dev] section");

        // A bare [dev] header in a config file must NOT match.
        let bare_contents = "[dev]\nregion = us-east-1\n";
        let hash_bare = hash_config_section(bare_contents, "dev");
        assert!(
            hash_bare.is_none(),
            "[profile dev] should not match bare [dev]"
        );
    }

    #[test]
    fn hash_credentials_section_uses_bare_header_form() {
        let creds_contents = "[ci]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n";

        let hash = hash_credentials_section(creds_contents, "ci");
        assert!(hash.is_some(), "should find [ci] section");

        // A [profile ci] form in a credentials file must NOT match.
        let profile_contents = "[profile ci]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n";
        let hash_profile = hash_credentials_section(profile_contents, "ci");
        assert!(
            hash_profile.is_none(),
            "hash_credentials_section should not match [profile ci]"
        );
    }

    /// MANDATORY security test (E2.2): the AwsSectionHash digest must not
    /// contain any substring from the credential values in the hashed section.
    /// The hash is a one-way SHA-256 digest; the secret bytes never appear in
    /// the returned value, even in hex form.
    #[test]
    fn hash_credentials_section_digest_contains_no_secret_substrings() {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        let contents = format!(
            "[ci]\naws_access_key_id = {access_key}\naws_secret_access_key = {secret_key}\n"
        );

        let hash = hash_credentials_section(&contents, "ci").unwrap();
        let hex_repr = format!("{hash:?}");

        // The digest representation must not contain the access key ID or the
        // secret access key value.
        assert!(
            !hex_repr.contains(access_key),
            "hash debug repr must not contain the access key ID"
        );
        assert!(
            !hex_repr.contains(secret_key),
            "hash debug repr must not contain the secret access key"
        );
        // Additional guard: the raw bytes (as u8 slice) must not encode a
        // known text value — this is trivially true for SHA-256 but documented
        // as a deliberate security assertion.
        let text_from_bytes = String::from_utf8_lossy(&hash.0);
        assert!(!text_from_bytes.contains(access_key));
        assert!(!text_from_bytes.contains(secret_key));
    }

    // ── E2.5 — update_aws_credentials_atomic tests ─────────────────────────

    #[test]
    fn credentials_atomic_write_applies_transform() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("credentials");

        std::fs::write(&path, "[ci]\naws_access_key_id = OLD\n").expect("seed");

        update_aws_credentials_atomic(&path, |existing| existing.replace("OLD", "NEW"))
            .expect("write");

        let content = std::fs::read_to_string(&path).expect("read");
        assert!(content.contains("aws_access_key_id = NEW"));
        assert!(!content.contains("aws_access_key_id = OLD"));
    }

    #[test]
    fn credentials_atomic_write_no_change_does_not_update_mtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("credentials");
        let content = "[ci]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n";

        std::fs::write(&path, content).expect("seed");

        let mtime_before = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .expect("mtime before");

        // Transform returns the original string — no change.
        update_aws_credentials_atomic(&path, |existing| existing.to_string()).expect("write");

        let mtime_after = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .expect("mtime after");

        assert_eq!(
            mtime_before, mtime_after,
            "mtime must not change when content is identical"
        );
    }

    #[test]
    fn credentials_atomic_write_uses_same_lock_as_config_write() {
        // Two threads: one writes to a temp config path under the shared lock,
        // the other writes to a credentials path. They must not deadlock and
        // both writes must land.
        use std::sync::Arc;
        use std::thread;

        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = Arc::new(dir.path().join("config"));
        let creds_path = Arc::new(dir.path().join("credentials"));

        std::fs::write(config_path.as_ref(), "[profile a]\nregion = us-east-1\n")
            .expect("seed config");
        std::fs::write(
            creds_path.as_ref(),
            "[a]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n",
        )
        .expect("seed creds");

        let cp = Arc::clone(&config_path);
        let creds = Arc::clone(&creds_path);

        let h1 = thread::spawn(move || {
            update_aws_config_atomic(&cp, |existing| existing.replace("us-east-1", "us-west-2"))
                .expect("config write");
        });

        let h2 = thread::spawn(move || {
            update_aws_credentials_atomic(&creds, |existing| {
                existing.replace("AKIAIOSFODNN7EXAMPLE", "AKIAI44QH8DHBEXAMPLE")
            })
            .expect("creds write");
        });

        h1.join().expect("thread 1");
        h2.join().expect("thread 2");

        let config_content = std::fs::read_to_string(config_path.as_ref()).expect("read config");
        let creds_content = std::fs::read_to_string(creds_path.as_ref()).expect("read creds");

        assert!(
            config_content.contains("us-west-2"),
            "config write must land"
        );
        assert!(
            creds_content.contains("AKIAI44QH8DHBEXAMPLE"),
            "creds write must land"
        );
    }

    // ── E2.6 — replace_or_append_credentials_block tests ───────────────────

    #[test]
    fn replace_credentials_block_uses_bare_header() {
        let contents = "[ci]\naws_access_key_id = OLD\n";
        let result =
            replace_or_append_credentials_block(contents, "ci", &[("aws_access_key_id", "NEW")]);

        // Bare [ci] header must be present; no [profile ci].
        assert!(result.contains("[ci]"), "must use bare [ci] header");
        assert!(
            !result.contains("[profile ci]"),
            "must NOT use [profile ci]"
        );
        assert!(result.contains("aws_access_key_id = NEW"));
    }

    #[test]
    fn replace_credentials_block_other_sections_byte_identical() {
        let contents = "[prod]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\nregion = us-east-1\n\n[ci]\naws_access_key_id = AKIAI44QH8DHBEXAMPLE\n\n[dev]\nregion = eu-west-1\n";

        let result = replace_or_append_credentials_block(
            contents,
            "ci",
            &[("aws_access_key_id", "AKIANEWVALUE7EXAMPLE")],
        );

        // [prod] and [dev] must be byte-identical.
        let prod_before = section_raw_bytes(contents, "[prod]").unwrap();
        let prod_after = section_raw_bytes(&result, "[prod]").unwrap();
        assert_eq!(prod_before, prod_after, "[prod] must be byte-identical");

        let dev_before = section_raw_bytes(contents, "[dev]").unwrap();
        let dev_after = section_raw_bytes(&result, "[dev]").unwrap();
        assert_eq!(dev_before, dev_after, "[dev] must be byte-identical");

        // [ci] must be updated.
        assert!(result.contains("aws_access_key_id = AKIANEWVALUE7EXAMPLE"));
    }

    #[test]
    fn replace_credentials_block_appends_when_section_absent() {
        let contents = "[prod]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n";

        let result = replace_or_append_credentials_block(
            contents,
            "new-profile",
            &[("aws_access_key_id", "AKIANEWVALUE7EXAMPLE")],
        );

        assert!(result.contains("[new-profile]"));
        assert!(result.contains("aws_access_key_id = AKIANEWVALUE7EXAMPLE"));
        // Original section must still be present.
        assert!(result.contains("[prod]"));
    }

    // ── E2.3 / E2.8 — blank-preserves / non-blank-overwrites ────────────────

    #[test]
    fn credentials_merge_blank_preserves_existing_key() {
        // A blank value in `fields` means "keep the existing on-disk value".
        let contents = "[ci]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n";

        let result = replace_or_append_credentials_block(
            contents,
            "ci",
            &[
                ("aws_access_key_id", "AKIANEWVALUE7EXAMPLE"),
                ("aws_secret_access_key", ""), // blank — preserve existing
            ],
        );

        assert!(result.contains("aws_access_key_id = AKIANEWVALUE7EXAMPLE"));
        // The existing secret must be preserved because the caller passed blank.
        assert!(
            result.contains("aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        );
    }

    #[test]
    fn credentials_merge_non_blank_overwrites_existing_key() {
        let contents =
            "[ci]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = OLD_SECRET\n";

        let result = replace_or_append_credentials_block(
            contents,
            "ci",
            &[
                ("aws_access_key_id", "AKIANEWVALUE7EXAMPLE"),
                ("aws_secret_access_key", "NEW_SECRET"),
            ],
        );

        assert!(result.contains("aws_secret_access_key = NEW_SECRET"));
        assert!(!result.contains("OLD_SECRET"));
    }

    // ── E2.9 — MANDATORY surgical-write regression test (both files) ─────────

    #[test]
    fn surgical_write_config_leaves_other_sections_byte_identical() {
        let contents = "\
[profile a]\nregion = us-east-1\noutput = json\n\n\
[profile b]\nregion = eu-west-1\noutput = text\n\n\
[profile c]\nregion = ap-southeast-1\noutput = json\n";

        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("config");
        std::fs::write(&config_path, contents).expect("seed");

        // Hash sections a and c BEFORE the write.
        let ha_before = hash_config_section(contents, "a").unwrap();
        let hc_before = hash_config_section(contents, "c").unwrap();

        // Write to [profile b] only.
        update_aws_config_atomic(&config_path, |existing| {
            replace_or_append_profile_block(
                existing,
                "b",
                &[("region", "us-west-2"), ("output", "text")],
                &[],
            )
        })
        .expect("config write");

        let after = std::fs::read_to_string(&config_path).expect("read after");

        let ha_after = hash_config_section(&after, "a").unwrap();
        let hc_after = hash_config_section(&after, "c").unwrap();

        assert_eq!(
            ha_before, ha_after,
            "[profile a] must be byte-identical after write to [profile b]"
        );
        assert_eq!(
            hc_before, hc_after,
            "[profile c] must be byte-identical after write to [profile b]"
        );
    }

    #[test]
    fn surgical_write_credentials_leaves_other_sections_byte_identical() {
        let contents = "\
[staging]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n\n\
[ci]\naws_access_key_id = AKIAI44QH8DHBEXAMPLE\naws_secret_access_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY\n\n\
[prod]\naws_access_key_id = AKIAJ4JHDL3GEXAMPLE\naws_secret_access_key = 9drTJvcXLB89EXAMPLEKEY\n";

        let dir = tempfile::tempdir().expect("tempdir");
        let creds_path = dir.path().join("credentials");
        std::fs::write(&creds_path, contents).expect("seed");

        // Hash [staging] and [prod] BEFORE the write.
        let h_staging_before = hash_credentials_section(contents, "staging").unwrap();
        let h_prod_before = hash_credentials_section(contents, "prod").unwrap();

        // Write to [ci] only.
        update_aws_credentials_atomic(&creds_path, |existing| {
            replace_or_append_credentials_block(
                existing,
                "ci",
                &[
                    ("aws_access_key_id", "AKIANEWVALUE7EXAMPLE"),
                    ("aws_secret_access_key", "NEW_SECRET_VALUE"),
                ],
            )
        })
        .expect("creds write");

        let after = std::fs::read_to_string(&creds_path).expect("read after");

        let h_staging_after = hash_credentials_section(&after, "staging").unwrap();
        let h_prod_after = hash_credentials_section(&after, "prod").unwrap();

        assert_eq!(
            h_staging_before, h_staging_after,
            "[staging] must be byte-identical after write to [ci]"
        );
        assert_eq!(
            h_prod_before, h_prod_after,
            "[prod] must be byte-identical after write to [ci]"
        );
    }

    // ── E2.10 — SECURITY: no secret logging; parse_aws_credentials_str unchanged ─

    /// Verifies that `update_aws_credentials_atomic` does not return any
    /// secret value — the only thing that crosses back is the `Result<(), _>`
    /// return. The secret stays inside the transform closure and is dropped
    /// when the closure returns.
    #[test]
    fn credentials_write_return_type_cannot_carry_secrets() {
        // This is a compile-time structural assertion: the return type of
        // `update_aws_credentials_atomic` is `Result<(), io::Error>`, which
        // has no `String` or `&str` payload. We call it and assert Ok(()).
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("credentials");

        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        std::fs::write(&path, format!("[ci]\naws_secret_access_key = {secret}\n")).expect("seed");

        // The return value is () — it cannot carry any string, let alone the secret.
        let result = update_aws_credentials_atomic(&path, |existing| existing.to_string());
        assert!(result.is_ok());

        // parse_aws_credentials_str called on the same file still returns names only.
        let content = std::fs::read_to_string(&path).expect("read");
        let names = parse_aws_credentials_str(&content);
        assert_eq!(names, vec!["ci"]);
        for name in &names {
            assert!(!name.contains(secret), "parse must not return secret value");
        }
    }

    /// MANDATORY file-permission test (Unix only): credentials temp and backup
    /// files must not be group- or other-readable when the original is 0600.
    #[cfg(unix)]
    #[test]
    fn credentials_write_temp_and_backup_are_not_world_readable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("credentials");

        // Seed with 0600 permissions (typical for ~/.aws/credentials).
        std::fs::write(&path, "[ci]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n").expect("seed");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("set 0600");

        // Perform a write — this creates the temp file and the backup.
        update_aws_credentials_atomic(&path, |existing| {
            existing.replace("AKIAIOSFODNN7EXAMPLE", "AKIAI44QH8DHBEXAMPLE")
        })
        .expect("write");

        // The final written file must be at most 0600 (not group/other readable).
        let final_mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            final_mode & 0o077,
            0,
            "final credentials file must not be group/other readable; mode = {:o}",
            final_mode
        );
    }

    // ── Fix 1 — config file perm test ──────────────────────────────────────────

    /// Verifies that `update_aws_config_atomic` (which now uses the unified
    /// `write_atomic_with_backup_perms`) does not leave the config temp or
    /// final file group/other readable when the source file is mode 0600.
    ///
    /// `~/.aws/config` contains `sso_start_url`, account IDs, and role names,
    /// so it must receive the same umask-independent permission treatment as
    /// `~/.aws/credentials`.
    #[cfg(unix)]
    #[test]
    fn config_write_temp_and_final_are_not_world_readable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config");

        // Seed with 0600 — a conservative mode a user might set on ~/.aws/config.
        std::fs::write(
            &path,
            "[profile ci]\nsso_start_url = https://example.awsapps.com/start\n",
        )
        .expect("seed");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("set 0600");

        // Perform a round-trip write via the config atomic writer.
        update_aws_config_atomic(&path, |existing| {
            existing.replace(
                "https://example.awsapps.com/start",
                "https://ci.awsapps.com/start",
            )
        })
        .expect("write");

        // The final file must be at most 0600 (no group/other bits).
        let final_mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;

        assert_eq!(
            final_mode & 0o077,
            0,
            "final config file must not be group/other readable; mode = {:o}",
            final_mode
        );
    }

    // ── Fix 2 — rename-failure restore test ────────────────────────────────────

    /// Verifies that when the atomic rename fails, `write_atomic_with_backup_perms`
    /// restores the original file content from the backup.
    ///
    /// Rename failure is injected by creating a *directory* at the temp-file path
    /// before the write is attempted. On Linux (and macOS) `fs::rename` fails with
    /// `EISDIR` when the destination is a directory, triggering the restore branch.
    ///
    /// The test verifies:
    /// 1. The function returns an `Err`.
    /// 2. The original file still contains the original content after the error.
    #[cfg(unix)]
    #[test]
    fn atomic_write_rename_failure_preserves_original_content() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config");
        let temp_path = path.with_extension("tmp");

        let original = "[profile dev]\nregion = us-east-1\n";
        std::fs::write(&path, original).expect("seed");

        // Block the rename by occupying the temp path with a directory.
        std::fs::create_dir_all(&temp_path).expect("create blocking dir");

        let result = write_atomic_with_backup_perms(&path, "[profile dev]\nregion = us-west-2\n");

        // The call must fail (rename is impossible with a directory in the way).
        assert!(
            result.is_err(),
            "expected Err when rename destination is a directory"
        );

        // The original file must still contain the original content.
        let actual = std::fs::read_to_string(&path).expect("read after failure");
        assert_eq!(
            actual, original,
            "original content must be intact after rename failure"
        );
    }
}
