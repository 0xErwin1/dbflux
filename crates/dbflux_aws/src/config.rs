/// AWS configuration file parsing, profile detection, and write-back.
///
/// Reads and writes `~/.aws/config` to discover and register AWS profiles.
/// The parser identifies SSO and shared-credentials profiles; the writer
/// appends new profile blocks without touching existing entries. Supports
/// mtime-based caching to avoid re-parsing on every read access.
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct AwsProfileInfo {
    pub name: String,
    pub region: Option<String>,
    pub is_sso: bool,
    pub sso_start_url: Option<String>,
}

#[derive(Debug, Default)]
pub struct CachedAwsConfig {
    profiles: Vec<AwsProfileInfo>,
    last_modified: Option<SystemTime>,
}

impl CachedAwsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns cached profiles if the config file hasn't changed since last
    /// parse. Re-parses from disk when the file's mtime differs or on first
    /// call.
    pub fn profiles(&mut self) -> &[AwsProfileInfo] {
        let path = config_file_path();
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
}

/// Returns the platform path to `~/.aws/config`.
pub fn config_file_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("~"))
        .join(".aws")
        .join("config")
}

/// Parses an AWS config file's contents into profile info entries.
///
/// Recognizes `[default]` and `[profile <name>]` sections. A profile is
/// marked as SSO if it contains `sso_start_url` or `sso_session` keys.
/// Malformed sections are skipped with a warning.
pub fn parse_aws_config_str(contents: &str) -> Vec<AwsProfileInfo> {
    let mut profiles = Vec::new();
    let mut current_section: Option<String> = None;
    let mut current_keys: HashMap<String, String> = HashMap::new();

    for line in contents.lines() {
        let trimmed = line.trim();

        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            flush_section(&current_section, &current_keys, &mut profiles);

            let header = &trimmed[1..trimmed.len() - 1].trim();
            current_section = parse_section_name(header);
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

fn parse_section_name(header: &str) -> Option<String> {
    if header.eq_ignore_ascii_case("default") {
        return Some("default".to_string());
    }

    if let Some(name) = header.strip_prefix("profile") {
        let name = name.trim();
        if name.is_empty() {
            log::warn!("Skipping AWS config section with empty profile name");
            return None;
        }
        return Some(name.to_string());
    }

    // Skip non-profile sections like [sso-session ...]
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
    section_name: &Option<String>,
    keys: &HashMap<String, String>,
    profiles: &mut Vec<AwsProfileInfo>,
) {
    let Some(name) = section_name else {
        return;
    };

    let is_sso = keys.contains_key("sso_start_url") || keys.contains_key("sso_session");
    let sso_start_url = keys.get("sso_start_url").cloned();
    let region = keys.get("region").cloned();

    profiles.push(AwsProfileInfo {
        name: name.clone(),
        region,
        is_sso,
        sso_start_url,
    });
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
    let existing = read_config_or_default(&path)?;

    if profile_section_exists(&existing, name) {
        return Ok(false);
    }

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

    write_config_block(&path, &existing, &block)
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
    let existing = read_config_or_default(&path)?;

    if profile_section_exists(&existing, name) {
        return Ok(false);
    }

    let mut block = String::new();
    writeln!(block).ok();

    let header = if name == "default" {
        "[default]".to_string()
    } else {
        format!("[profile {name}]")
    };

    writeln!(block, "{header}").ok();
    writeln!(block, "region = {region}").ok();

    write_config_block(&path, &existing, &block)
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

/// Ensures the `~/.aws/` directory exists, then appends `block` to the config
/// file (creating it if necessary). Returns `Ok(true)` on success.
fn write_config_block(
    path: &std::path::Path,
    existing: &str,
    block: &str,
) -> Result<bool, std::io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut content = existing.to_string();

    // Ensure the existing content ends with a newline before appending.
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }

    content.push_str(block);

    fs::write(path, &content)?;

    Ok(true)
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

[sso-session my-session]
sso_start_url = https://example.com

[profile valid]
region = eu-west-1
"#;
        let profiles = parse_aws_config_str(config);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].name, "valid");
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

        // Override write target via the helper (we call write_config_block directly).
        let existing = "";
        let mut block = String::new();
        block.push('\n');
        block.push_str("[profile new-sso]\n");
        block.push_str("sso_start_url = https://example.awsapps.com/start\n");
        block.push_str("sso_region = us-east-1\n");
        block.push_str("sso_account_id = 123456789012\n");
        block.push_str("sso_role_name = AdminAccess\n");
        block.push_str("region = us-east-1\n");

        let written = write_config_block(&path, existing, &block).expect("write");
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

        let existing = std::fs::read_to_string(&path).expect("read existing");
        let block = "\n[profile staging]\nregion = eu-west-1\n";

        write_config_block(&path, &existing, block).expect("write");

        let content = std::fs::read_to_string(&path).expect("read result");
        // The existing content had no trailing newline; write_config_block must add one.
        assert!(content.contains("[default]\nregion = us-east-1\n\n[profile staging]"));
    }
}
