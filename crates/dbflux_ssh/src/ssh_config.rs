//! OpenSSH config resolution for DBFlux SSH tunnels (feature #837).
//!
//! This module resolves an alias from an OpenSSH `config` file into concrete
//! connection parameters (`HostName`, `User`, `Port`, `IdentityFile`), following
//! OpenSSH's own algorithm: `Host` glob patterns with first-match-wins,
//! `Include` directives, `Match` blocks (`all` / `host` / `user`), and
//! `%h` / `%r` / `%d` tokens inside `IdentityFile`.
//!
//! Isolation rules (S7/C4):
//! - the config directory and the home directory are always caller-provided
//!   arguments; nothing here calls `dirs::home_dir()` or reads `~/.ssh`;
//! - `Match exec` never spawns a process (the block never applies);
//! - `Include` is bounded by a recursion-depth cap and a total byte cap;
//! - the SSH config is only ever read, never written.

use std::fs;
use std::path::{Path, PathBuf};

/// Maximum `Include` recursion depth before descending stops.
pub const MAX_INCLUDE_DEPTH: usize = 16;

/// Maximum total number of config bytes read (including includes) before
/// descending stops.
pub const MAX_TOTAL_BYTES: u64 = 1024 * 1024;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// A `Host` pattern the picker can offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshConfigHost {
    /// The pattern as written, e.g. `prod-bastion`.
    pub alias: String,
    /// Resolved `HostName`, or the alias when none is set.
    pub host_name: String,
    /// Effective user; `None` when only the local-user fallback would apply.
    pub user: Option<String>,
    /// Effective port, 22 by default.
    pub port: u16,
    /// Effective `IdentityFile`, tokens expanded, `~` expanded.
    pub identity_file: Option<PathBuf>,
    /// Set when the effective config depends on a directive the tunnel cannot honour.
    pub unsupported: Option<UnsupportedDirective>,
}

/// A directive the SSH tunnel cannot honour on a referenced host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedDirective {
    ProxyJump,
    ProxyCommand,
}

impl UnsupportedDirective {
    /// The directive name as OpenSSH spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProxyJump => "ProxyJump",
            Self::ProxyCommand => "ProxyCommand",
        }
    }
}

/// Connection parameters for a referenced host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedHost {
    pub host_name: String,
    pub user: Option<String>,
    pub port: u16,
    pub identity_file: Option<PathBuf>,
}

/// Errors produced when resolving a host from an SSH config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SshConfigError {
    /// The alias exists but its effective config depends on a directive the
    /// tunnel cannot honour.
    UnsupportedHost {
        alias: String,
        /// `"ProxyJump"` or `"ProxyCommand"`.
        directive: &'static str,
    },
    /// No `Host` or `Match` block matched the alias.
    UnknownHost { alias: String },
    /// The config file exists but could not be read.
    Read { path: PathBuf, message: String },
}

impl std::fmt::Display for SshConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedHost { alias, directive } => {
                write!(
                    f,
                    "host {alias:?} requires the unsupported directive {directive}"
                )
            }
            Self::UnknownHost { alias } => {
                write!(
                    f,
                    "no Host or Match block matches {alias:?} in the SSH config"
                )
            }
            Self::Read { path, message } => {
                write!(f, "cannot read {}: {message}", path.display())
            }
        }
    }
}

impl std::error::Error for SshConfigError {}

/// A parsed OpenSSH config, ready to resolve aliases.
#[derive(Debug, Clone, Default)]
pub struct SshConfigFile {
    directives: Vec<Directive>,
    diagnostics: Vec<String>,
}

impl SshConfigFile {
    /// Reads `config_dir/config`. A missing file is not an error: it yields an
    /// empty config.
    pub fn load(config_dir: &Path) -> Result<Self, SshConfigError> {
        let path = config_dir.join("config");
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => {
                return Err(SshConfigError::Read {
                    path,
                    message: error.to_string(),
                });
            }
        };
        Ok(Self::parse(&text, config_dir))
    }

    /// Parses already-read text; `Include` is still followed relative to `config_dir`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dbflux_ssh::ssh_config::SshConfigFile;
    ///
    /// let config_dir = tempfile::tempdir().unwrap();
    /// let config = SshConfigFile::parse(
    ///     "Host prod-bastion\n  HostName bastion.example.com\n  User deploy\n  Port 2222\n",
    ///     config_dir.path(),
    /// );
    /// let home = config_dir.path();
    /// let resolved = config.resolve_with_local_user("prod-bastion", "alice", home).unwrap();
    /// assert_eq!(resolved.host_name, "bastion.example.com");
    /// assert_eq!(resolved.user.as_deref(), Some("deploy"));
    /// assert_eq!(resolved.port, 2222);
    /// ```
    pub fn parse(text: &str, config_dir: &Path) -> Self {
        let mut parser = Parser {
            config_dir,
            directives: Vec::new(),
            diagnostics: Vec::new(),
            bytes_read: text.len() as u64,
            depth: 0,
        };
        parser.parse_text(text);
        Self {
            directives: parser.directives,
            diagnostics: parser.diagnostics,
        }
    }

    /// Concrete, pickable hosts in file order, deduplicated by alias (first
    /// occurrence wins). Patterns containing `*` or `?` are omitted (A5), but
    /// they still participate in resolution. Never fails.
    ///
    /// `home` is the home directory used to expand a leading `~` and `%d`
    /// inside `IdentityFile`. When no effective `User` is set, `%r` is left
    /// literal: this method has no local-user source and must not read the
    /// environment.
    pub fn hosts(&self, home: &Path) -> Vec<SshConfigHost> {
        let mut aliases: Vec<String> = Vec::new();
        for directive in &self.directives {
            if let Directive::Host { patterns } = directive {
                for pattern in patterns {
                    let pickable = !pattern.contains('*')
                        && !pattern.contains('?')
                        && !pattern.starts_with('!');
                    if pickable && !aliases.contains(pattern) {
                        aliases.push(pattern.clone());
                    }
                }
            }
        }
        aliases
            .into_iter()
            .map(|alias| {
                let state = self.resolve_state_with_local_user(&alias, "");
                let host_name = state.host_name.clone().unwrap_or_else(|| alias.clone());
                let identity_file = state
                    .identity_file
                    .as_deref()
                    .map(|raw| expand_identity_file(raw, &host_name, state.user.as_deref(), home));
                SshConfigHost {
                    alias,
                    host_name,
                    user: state.user,
                    port: state.port.unwrap_or(DEFAULT_PORT),
                    identity_file,
                    unsupported: state.unsupported,
                }
            })
            .collect()
    }

    /// Resolves `alias` for a connection attempt.
    ///
    /// The local user name (used for the `User` fallback and `%r`) is read from
    /// the environment (`USER`, then `USERNAME`); nothing else is. `home` is
    /// the home directory used to expand `~` and `%d` inside `IdentityFile`.
    pub fn resolve(&self, alias: &str, home: &Path) -> Result<ResolvedHost, SshConfigError> {
        self.resolve_with_local_user(alias, &local_user_from_env(), home)
    }

    /// Same as [`SshConfigFile::resolve`] but with the local user name injected,
    /// so it is testable and never reads the environment.
    pub fn resolve_with_local_user(
        &self,
        alias: &str,
        local_user: &str,
        home: &Path,
    ) -> Result<ResolvedHost, SshConfigError> {
        let state = self.resolve_state_with_local_user(alias, local_user);
        if !state.matched {
            return Err(SshConfigError::UnknownHost {
                alias: alias.to_string(),
            });
        }
        if let Some(directive) = state.unsupported {
            return Err(SshConfigError::UnsupportedHost {
                alias: alias.to_string(),
                directive: directive.as_str(),
            });
        }
        Ok(self.finalize(state, alias, Some(local_user), home))
    }

    /// Lines that could not be parsed, for the UI to surface. Never aborts
    /// resolution.
    pub fn diagnostics(&self) -> &[String] {
        &self.diagnostics
    }

    fn resolve_state_with_local_user(&self, alias: &str, local_user: &str) -> Resolution {
        let alias_lower = alias.to_lowercase();
        let mut state = Resolution {
            matched: false,
            host_name: None,
            user: None,
            port: None,
            identity_file: None,
            unsupported: None,
        };
        let mut active = false;
        for directive in &self.directives {
            match directive {
                Directive::Host { patterns } => {
                    active = pattern_list_matches(patterns, &alias_lower);
                    state.matched |= active;
                }
                Directive::Match { conditions } => {
                    // `Match host` sees the host name in effect so far, `Match
                    // user` the user in effect or the local-user fallback (A6).
                    let host_in_effect = state.host_name.as_deref().unwrap_or(alias).to_lowercase();
                    let user_in_effect = state.user.as_deref().unwrap_or(local_user).to_lowercase();
                    active = conditions.iter().all(|condition| match condition {
                        MatchCondition::All => true,
                        MatchCondition::Host(patterns) => {
                            pattern_list_matches(patterns, &host_in_effect)
                        }
                        MatchCondition::User(patterns) => {
                            pattern_list_matches(patterns, &user_in_effect)
                        }
                        MatchCondition::Never => false,
                    });
                    state.matched |= active;
                }
                Directive::Keyword(keyword) => {
                    if !active {
                        continue;
                    }
                    // First value obtained for a keyword wins (OpenSSH).
                    match keyword {
                        KnownKeyword::HostName(value) => {
                            if state.host_name.is_none() {
                                state.host_name = Some(value.clone());
                            }
                        }
                        KnownKeyword::User(value) => {
                            if state.user.is_none() {
                                state.user = Some(value.clone());
                            }
                        }
                        KnownKeyword::Port(value) => {
                            if state.port.is_none() {
                                state.port = Some(*value);
                            }
                        }
                        KnownKeyword::IdentityFile(value) => {
                            if state.identity_file.is_none() {
                                state.identity_file = Some(value.clone());
                            }
                        }
                        KnownKeyword::ProxyJump(value) => {
                            state.mark_unsupported(value, UnsupportedDirective::ProxyJump);
                        }
                        KnownKeyword::ProxyCommand(value) => {
                            state.mark_unsupported(value, UnsupportedDirective::ProxyCommand);
                        }
                    }
                }
            }
        }
        state
    }

    fn finalize(
        &self,
        state: Resolution,
        alias: &str,
        local_user: Option<&str>,
        home: &Path,
    ) -> ResolvedHost {
        let host_name = state.host_name.unwrap_or_else(|| alias.to_string());
        let user_for_tokens = state.user.as_deref().or(local_user);
        let identity_file = state
            .identity_file
            .as_deref()
            .map(|raw| expand_identity_file(raw, &host_name, user_for_tokens, home));
        ResolvedHost {
            host_name,
            user: state.user,
            port: state.port.unwrap_or(DEFAULT_PORT),
            identity_file,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal model
// ---------------------------------------------------------------------------

/// Effective port when no `Port` applies.
const DEFAULT_PORT: u16 = 22;

#[derive(Debug, Clone)]
enum Directive {
    Host { patterns: Vec<String> },
    Match { conditions: Vec<MatchCondition> },
    Keyword(KnownKeyword),
}

#[derive(Debug, Clone)]
enum KnownKeyword {
    HostName(String),
    User(String),
    Port(u16),
    IdentityFile(String),
    ProxyJump(String),
    ProxyCommand(String),
}

#[derive(Debug, Clone)]
enum MatchCondition {
    All,
    Host(Vec<String>),
    User(Vec<String>),
    /// A condition keyword the tunnel cannot evaluate (including `exec`): the
    /// whole block never applies and no process is ever spawned (C5).
    Never,
}

/// Effective values collected by the sequential first-match-wins pass.
struct Resolution {
    matched: bool,
    host_name: Option<String>,
    user: Option<String>,
    port: Option<u16>,
    identity_file: Option<String>,
    unsupported: Option<UnsupportedDirective>,
}

impl Resolution {
    /// `ProxyJump`/`ProxyCommand` with the value `none` (case-insensitive)
    /// count as unset; the first non-`none` value wins.
    fn mark_unsupported(&mut self, value: &str, directive: UnsupportedDirective) {
        if value.eq_ignore_ascii_case("none") || self.unsupported.is_some() {
            return;
        }
        self.unsupported = Some(directive);
    }
}

struct Parser<'a> {
    config_dir: &'a Path,
    directives: Vec<Directive>,
    diagnostics: Vec<String>,
    bytes_read: u64,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn diagnostic(&mut self, number: usize, line: &str, reason: &str) {
        self.diagnostics
            .push(format!("line {number}: {reason}: {line}"));
    }

    fn parse_text(&mut self, text: &str) {
        for (offset, line) in text.lines().enumerate() {
            self.parse_line(line, offset + 1);
        }
    }

    fn parse_line(&mut self, line: &str, number: usize) {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return;
        }
        let Some((keyword, rest)) = split_keyword_value(trimmed) else {
            self.diagnostic(number, line, "no keyword");
            return;
        };
        if rest.is_empty() {
            self.diagnostic(number, line, &format!("missing value for {keyword}"));
            return;
        }
        match keyword.to_ascii_lowercase().as_str() {
            "host" => match split_words(rest) {
                Ok(patterns) if !patterns.is_empty() => {
                    self.directives.push(Directive::Host { patterns });
                }
                Ok(_) => self.diagnostic(number, line, "Host without patterns"),
                Err(()) => self.diagnostic(number, line, "unterminated quote"),
            },
            "match" => self.parse_match(rest, number, line),
            "include" => self.parse_include(rest, number, line),
            "hostname" => self.push_value_keyword(rest, number, line, KnownKeyword::HostName),
            "user" => self.push_value_keyword(rest, number, line, KnownKeyword::User),
            "identityfile" => {
                self.push_value_keyword(rest, number, line, KnownKeyword::IdentityFile);
            }
            "proxyjump" => self.push_value_keyword(rest, number, line, KnownKeyword::ProxyJump),
            "proxycommand" => {
                self.push_value_keyword(rest, number, line, KnownKeyword::ProxyCommand);
            }
            "port" => {
                match parse_value(rest).and_then(|value| value.parse::<u16>().map_err(|_| ())) {
                    Ok(port) if port > 0 => {
                        self.directives
                            .push(Directive::Keyword(KnownKeyword::Port(port)));
                    }
                    _ => self.diagnostic(number, line, "invalid Port value"),
                }
            }
            // Unknown keywords are ignored silently: real configs are full of them.
            _ => {}
        }
    }

    fn push_value_keyword(
        &mut self,
        rest: &str,
        number: usize,
        line: &str,
        make: fn(String) -> KnownKeyword,
    ) {
        match parse_value(rest) {
            Ok(value) => self.directives.push(Directive::Keyword(make(value))),
            Err(()) => self.diagnostic(number, line, "unterminated quote"),
        }
    }

    fn parse_match(&mut self, rest: &str, number: usize, line: &str) {
        let words = match split_words(rest) {
            Ok(words) => words,
            Err(()) => {
                self.diagnostic(number, line, "unterminated quote");
                return;
            }
        };
        if words.is_empty() {
            // `Match` with no conditions matches, same as `all`.
            self.directives.push(Directive::Match {
                conditions: vec![MatchCondition::All],
            });
            return;
        }
        let mut conditions = Vec::new();
        let mut index = 0;
        let mut unsupported = false;
        while let Some(word) = words.get(index) {
            let criterion = word.to_ascii_lowercase();
            index += 1;
            match criterion.as_str() {
                "all" => conditions.push(MatchCondition::All),
                "host" | "user" => match words.get(index) {
                    Some(argument) => {
                        let patterns = pattern_list(argument);
                        conditions.push(if criterion == "host" {
                            MatchCondition::Host(patterns)
                        } else {
                            MatchCondition::User(patterns)
                        });
                        index += 1;
                    }
                    None => unsupported = true,
                },
                // `exec`, `localuser`, and every other criterion make the block
                // inactive; no process is ever spawned (C5).
                _ => unsupported = true,
            }
        }
        if unsupported {
            conditions.push(MatchCondition::Never);
        }
        self.directives.push(Directive::Match { conditions });
    }

    fn parse_include(&mut self, rest: &str, number: usize, line: &str) {
        let words = match split_words(rest) {
            Ok(words) => words,
            Err(()) => {
                self.diagnostic(number, line, "unterminated quote");
                return;
            }
        };
        for word in words {
            self.include_one(&word, number, line);
        }
    }

    fn include_one(&mut self, raw: &str, number: usize, line: &str) {
        let path = if Path::new(raw).is_absolute() {
            PathBuf::from(raw)
        } else {
            self.config_dir.join(raw)
        };
        let is_glob = raw.contains('*') || raw.contains('?');
        let candidates = if is_glob {
            match self.glob_include(&path) {
                Ok(candidates) => candidates,
                Err(reason) => {
                    self.diagnostic(number, line, &reason);
                    return;
                }
            }
        } else if path.is_file() {
            vec![path]
        } else {
            self.diagnostic(number, line, &format!("include path does not exist: {raw}"));
            return;
        };
        for candidate in candidates {
            self.read_include(candidate);
        }
    }

    fn glob_include(&self, path: &Path) -> Result<Vec<PathBuf>, String> {
        let Some(parent) = path.parent() else {
            return Err(format!(
                "include pattern has no parent directory: {}",
                path.display()
            ));
        };
        let pattern = match path.file_name() {
            Some(name) => name.to_string_lossy().to_lowercase(),
            None => {
                return Err(format!(
                    "include pattern has no file name: {}",
                    path.display()
                ));
            }
        };
        let entries = fs::read_dir(parent).map_err(|error| {
            format!(
                "cannot list directory for include pattern {}: {error}",
                path.display()
            )
        })?;
        let mut matches: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_file())
            .filter(|entry| {
                glob_matches(
                    &pattern,
                    &entry.file_name().to_string_lossy().to_lowercase(),
                )
            })
            .map(|entry| entry.path())
            .collect();
        matches.sort();
        Ok(matches)
    }

    fn read_include(&mut self, path: PathBuf) {
        if self.depth >= MAX_INCLUDE_DEPTH {
            self.diagnostics.push(format!(
                "include depth cap of {MAX_INCLUDE_DEPTH} exceeded at {}",
                path.display()
            ));
            return;
        }
        let size = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                self.diagnostics
                    .push(format!("cannot read include {}: {error}", path.display()));
                return;
            }
        };
        if self.bytes_read + size > MAX_TOTAL_BYTES {
            self.diagnostics.push(format!(
                "include byte cap of {MAX_TOTAL_BYTES} bytes (1 MiB) exceeded at {}",
                path.display()
            ));
            return;
        }
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) => {
                self.diagnostics
                    .push(format!("cannot read include {}: {error}", path.display()));
                return;
            }
        };
        self.bytes_read += text.len() as u64;
        self.depth += 1;
        self.parse_text(&text);
        self.depth -= 1;
    }
}

// ---------------------------------------------------------------------------
// Line and pattern helpers
// ---------------------------------------------------------------------------

/// Splits a trimmed config line into its keyword and the raw text after the
/// separator. `key=value`, `key value` and `key = value` are all accepted;
/// `None` when the line has no keyword.
fn split_keyword_value(line: &str) -> Option<(&str, &str)> {
    let separator = line.find(|c: char| c.is_whitespace() || c == '=')?;
    if separator == 0 {
        return None;
    }
    let keyword = line.get(..separator)?;
    let rest = strip_separator(line.get(separator..)?);
    Some((keyword, rest))
}

fn strip_separator(rest: &str) -> &str {
    let trimmed = rest.trim_start();
    match trimmed.strip_prefix('=') {
        Some(after) => after.trim_start(),
        None => trimmed,
    }
}

/// Single-value argument: quoted with `"..."` or everything up to the end of
/// the line (trailing comments are not stripped, like OpenSSH).
fn parse_value(rest: &str) -> Result<String, ()> {
    let Some(after_quote) = rest.strip_prefix('"') else {
        // A quote inside a plain token never closes (OpenSSH quotes wrap the
        // whole argument), so treat it as unterminated.
        return if rest.contains('"') {
            Err(())
        } else {
            Ok(rest.trim_end().to_string())
        };
    };
    match after_quote.find('"') {
        Some(end) => after_quote.get(..end).map(str::to_string).ok_or(()),
        None => Err(()),
    }
}

/// Shell-style word splitting with `"..."` quotes; adjacent quoted and
/// unquoted text concatenates into one word.
fn split_words(value: &str) -> Result<Vec<String>, ()> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut in_quotes = false;
    for c in value.chars() {
        match c {
            '"' if in_quotes => in_quotes = false,
            '"' => {
                in_quotes = true;
                in_word = true;
            }
            c if c.is_whitespace() && !in_quotes => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            c => {
                word.push(c);
                in_word = true;
            }
        }
    }
    if in_quotes {
        return Err(());
    }
    if in_word {
        words.push(word);
    }
    Ok(words)
}

/// Comma-separated pattern list, as `Match host` / `Match user` take them.
fn pattern_list(argument: &str) -> Vec<String> {
    argument
        .split(',')
        .filter(|pattern| !pattern.is_empty())
        .map(str::to_string)
        .collect()
}

/// A pattern list matches when no negated pattern matches and at least one
/// positive pattern matches. Matching is case-insensitive, like OpenSSH's
/// `match_pattern_list`.
fn pattern_list_matches(patterns: &[String], candidate: &str) -> bool {
    let mut has_positive = false;
    for pattern in patterns {
        let lowered = pattern.to_lowercase();
        if let Some(negated) = lowered.strip_prefix('!') {
            if glob_matches(negated, candidate) {
                return false;
            }
        } else if glob_matches(&lowered, candidate) {
            has_positive = true;
        }
    }
    has_positive
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    glob_match_chars(&pattern_chars, &text_chars)
}

/// Glob with `*` and `?`, implemented with backtracking pointers so no
/// pattern can blow up the stack.
fn glob_match_chars(pattern: &[char], text: &[char]) -> bool {
    let (mut p, mut t) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut star_text = 0usize;
    while t < text.len() {
        let pattern_char = pattern.get(p).copied();
        let text_char = text.get(t).copied();
        if pattern_char == Some('*') {
            star = Some(p);
            star_text = t;
            p += 1;
        } else if pattern_char == Some('?') || pattern_char.is_some() && pattern_char == text_char {
            p += 1;
            t += 1;
        } else if let Some(star_position) = star {
            p = star_position + 1;
            star_text += 1;
            t = star_text;
        } else {
            return false;
        }
    }
    while pattern.get(p) == Some(&'*') {
        p += 1;
    }
    p == pattern.len()
}

// ---------------------------------------------------------------------------
// IdentityFile expansion
// ---------------------------------------------------------------------------

/// Expands a leading `~` against the caller-provided home directory and the
/// `%h` (host name in effect), `%r` (user in effect), `%d` (home directory)
/// and `%%` tokens. Any other `%X` is left untouched.
fn expand_identity_file(raw: &str, host_name: &str, user: Option<&str>, home: &Path) -> PathBuf {
    let expanded = expand_tokens(raw, host_name, user, home);
    if expanded == "~" {
        home.to_path_buf()
    } else if let Some(rest) = expanded.strip_prefix("~/") {
        home.join(rest)
    } else {
        PathBuf::from(expanded)
    }
}

fn expand_tokens(value: &str, host_name: &str, user: Option<&str>, home: &Path) -> String {
    let home_text = home.to_string_lossy();
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('d') => out.push_str(&home_text),
            Some('h') => out.push_str(host_name),
            Some('r') => match user {
                Some(user) => out.push_str(user),
                // No user source here: leave the token visible instead of
                // inventing a value.
                None => out.push_str("%r"),
            },
            Some('%') => out.push('%'),
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

/// Local user name for the `User` fallback and `%r`. `USER` first, then
/// `USERNAME` (Windows); the only environment reads in this module.
fn local_user_from_env() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tests (RED first: bodies above are `todo!()`)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Fixture rooted at a `TempDir`: `dir/ssh/config` is the SSH config and
    /// `dir` doubles as the home directory handed to the resolver. Nothing
    /// outside the temp directory is ever touched (S7).
    struct Fixture {
        dir: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                dir: TempDir::new().unwrap(),
            }
        }

        /// The home directory passed to the resolver.
        fn home(&self) -> PathBuf {
            self.dir.path().to_path_buf()
        }

        /// The config directory passed to the resolver.
        fn config_dir(&self) -> PathBuf {
            self.dir.path().join("ssh")
        }

        fn write(&self, relative: &str, content: &str) {
            let path = self.dir.path().join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, content).unwrap();
        }

        fn load(&self) -> SshConfigFile {
            SshConfigFile::load(&self.config_dir()).unwrap()
        }
    }

    #[test]
    fn first_match_wins_takes_earlier_host_name_but_later_new_keywords() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host web\n  HostName first.example.com\n\nHost web\n  HostName second.example.com\n  User admin\n",
        );
        let config = fixture.load();
        let resolved = config
            .resolve_with_local_user("web", "alice", &fixture.home())
            .unwrap();
        assert_eq!(resolved.host_name, "first.example.com");
        assert_eq!(resolved.user.as_deref(), Some("admin"));
    }

    #[test]
    fn host_star_provides_defaults_for_unlisted_alias() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host *\n  User shared\n  Port 2222\n\nHost web\n  HostName web.example.com\n",
        );
        let config = fixture.load();
        let listed = config
            .resolve_with_local_user("web", "alice", &fixture.home())
            .unwrap();
        assert_eq!(listed.host_name, "web.example.com");
        assert_eq!(listed.user.as_deref(), Some("shared"));
        assert_eq!(listed.port, 2222);
        let unlisted = config
            .resolve_with_local_user("unlisted", "alice", &fixture.home())
            .unwrap();
        assert_eq!(unlisted.host_name, "unlisted");
        assert_eq!(unlisted.user.as_deref(), Some("shared"));
        assert_eq!(unlisted.port, 2222);
    }

    #[test]
    fn negated_pattern_excludes_bastion_but_wildcard_matches_others() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host !bastion *.example.com\n  User webuser\n",
        );
        let config = fixture.load();
        let matched = config
            .resolve_with_local_user("other.example.com", "alice", &fixture.home())
            .unwrap();
        assert_eq!(matched.user.as_deref(), Some("webuser"));
        assert_eq!(
            config.resolve_with_local_user("bastion", "alice", &fixture.home()),
            Err(SshConfigError::UnknownHost {
                alias: "bastion".to_string()
            })
        );
    }

    #[test]
    fn include_glob_pulls_files_in_sorted_order_and_relative_include_resolves_under_config_dir() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Include conf.d/*.conf\nInclude conf.d/app.conf\nInclude conf.d/missing.conf\n",
        );
        fixture.write(
            "ssh/conf.d/b.conf",
            "Host beta\n  HostName beta.example.com\n",
        );
        fixture.write(
            "ssh/conf.d/a.conf",
            "Host alpha\n  HostName alpha.example.com\n",
        );
        fixture.write(
            "ssh/conf.d/app.conf",
            "Host app\n  HostName app.example.com\n",
        );
        let config = fixture.load();
        // `conf.d/*.conf` expands sorted: a.conf, app.conf, b.conf; the later
        // explicit `Include conf.d/app.conf` is deduplicated by alias.
        let aliases: Vec<String> = config
            .hosts(&fixture.home())
            .into_iter()
            .map(|h| h.alias)
            .collect();
        assert_eq!(aliases, vec!["alpha", "app", "beta"]);
        for (alias, host_name) in [
            ("alpha", "alpha.example.com"),
            ("beta", "beta.example.com"),
            ("app", "app.example.com"),
        ] {
            let resolved = config
                .resolve_with_local_user(alias, "alice", &fixture.home())
                .unwrap();
            assert_eq!(resolved.host_name, host_name);
        }
        assert!(
            config
                .diagnostics()
                .iter()
                .any(|d| d.contains("missing.conf")),
            "missing include path must be reported: {:?}",
            config.diagnostics()
        );
    }

    #[test]
    fn include_recursion_stops_at_depth_cap_with_diagnostic() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host web\n  HostName web.example.com\nInclude config\n",
        );
        let config = fixture.load();
        let resolved = config
            .resolve_with_local_user("web", "alice", &fixture.home())
            .unwrap();
        assert_eq!(resolved.host_name, "web.example.com");
        assert!(
            config.diagnostics().iter().any(|d| d.contains("depth")),
            "depth cap must be reported: {:?}",
            config.diagnostics()
        );
    }

    #[test]
    fn include_stops_at_byte_cap_with_diagnostic() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Include big.conf\nHost web\n  HostName web.example.com\n",
        );
        let big = format!("{}\n", "#".repeat(MAX_TOTAL_BYTES as usize + 1));
        fixture.write("ssh/big.conf", &big);
        let config = fixture.load();
        assert!(
            config
                .diagnostics()
                .iter()
                .any(|d| d.contains("byte") || d.contains("MiB")),
            "byte cap must be reported: {:?}",
            config.diagnostics()
        );
    }

    #[test]
    fn match_host_and_user_conditions_apply_to_injected_local_user() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Match host web user alice\n  HostName matched.example.com\n\nMatch host web\n  HostName webonly.example.com\n\nMatch user bob\n  HostName bobhost.example.com\n",
        );
        let config = fixture.load();
        let alice = config
            .resolve_with_local_user("web", "alice", &fixture.home())
            .unwrap();
        assert_eq!(alice.host_name, "matched.example.com");
        let other_user = config
            .resolve_with_local_user("web", "carol", &fixture.home())
            .unwrap();
        assert_eq!(other_user.host_name, "webonly.example.com");
        let bob = config
            .resolve_with_local_user("other", "bob", &fixture.home())
            .unwrap();
        assert_eq!(bob.host_name, "bobhost.example.com");
    }

    #[test]
    fn match_exec_never_applies_and_yields_the_pre_match_value() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host web\n  HostName plain.example.com\n\nMatch exec /usr/bin/nonexistent\n  HostName hijacked.example.com\n",
        );
        let config = fixture.load();
        let resolved = config
            .resolve_with_local_user("web", "alice", &fixture.home())
            .unwrap();
        assert_eq!(resolved.host_name, "plain.example.com");
    }

    #[test]
    fn identity_file_tokens_expand_with_injected_user_and_home() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host web\n  HostName web.example.com\n  User alice\n  IdentityFile ~/keys/%h_%r_%d%%\n",
        );
        let config = fixture.load();
        let home = fixture.home();
        let resolved = config
            .resolve_with_local_user("web", "alice", &home)
            .unwrap();
        let expected = home.join(format!("keys/web.example.com_alice_{}%", home.display()));
        assert_eq!(resolved.identity_file, Some(expected));
    }

    #[test]
    fn identity_file_percent_r_uses_local_user_fallback() {
        let fixture = Fixture::new();
        fixture.write("ssh/config", "Host nouser\n  IdentityFile ~/id_%r\n");
        let config = fixture.load();
        let home = fixture.home();
        let resolved = config
            .resolve_with_local_user("nouser", "carol", &home)
            .unwrap();
        assert_eq!(resolved.identity_file, Some(home.join("id_carol")));
        // hosts() has no local-user source and must not read the environment,
        // so `%r` stays literal there (documented deviation).
        let hosts = config.hosts(&home);
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0].identity_file, Some(home.join("id_%r")));
    }

    #[test]
    fn absent_port_defaults_to_22_and_absent_identity_file_to_none() {
        let fixture = Fixture::new();
        fixture.write("ssh/config", "Host web\n  HostName web.example.com\n");
        let config = fixture.load();
        let resolved = config
            .resolve_with_local_user("web", "alice", &fixture.home())
            .unwrap();
        assert_eq!(resolved.port, 22);
        assert_eq!(resolved.identity_file, None);
        assert_eq!(resolved.user, None);
    }

    #[test]
    fn proxy_directives_fail_resolution_and_mark_hosts_but_none_resolves() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host viajump\n  HostName j.example.com\n  ProxyJump bastion\n\nHost viacmd\n  HostName c.example.com\n  ProxyCommand nc -w 1 %h %p\n\nHost clean\n  ProxyJump none\n  HostName clean.example.com\n\nHost upper\n  ProxyJump NONE\n",
        );
        let config = fixture.load();
        assert_eq!(
            config.resolve_with_local_user("viajump", "alice", &fixture.home()),
            Err(SshConfigError::UnsupportedHost {
                alias: "viajump".to_string(),
                directive: "ProxyJump",
            })
        );
        assert_eq!(
            config.resolve_with_local_user("viacmd", "alice", &fixture.home()),
            Err(SshConfigError::UnsupportedHost {
                alias: "viacmd".to_string(),
                directive: "ProxyCommand",
            })
        );
        let clean = config
            .resolve_with_local_user("clean", "alice", &fixture.home())
            .unwrap();
        assert_eq!(clean.host_name, "clean.example.com");
        let upper = config
            .resolve_with_local_user("upper", "alice", &fixture.home())
            .unwrap();
        assert_eq!(upper.host_name, "upper");
        let hosts = config.hosts(&fixture.home());
        let unsupported_of = |alias: &str| {
            hosts
                .iter()
                .find(|h| h.alias == alias)
                .map(|h| h.unsupported)
                .unwrap()
        };
        assert_eq!(
            unsupported_of("viajump"),
            Some(UnsupportedDirective::ProxyJump)
        );
        assert_eq!(
            unsupported_of("viacmd"),
            Some(UnsupportedDirective::ProxyCommand)
        );
        assert_eq!(unsupported_of("clean"), None);
        assert_eq!(unsupported_of("upper"), None);
    }

    #[test]
    fn unknown_alias_fails_and_missing_config_file_yields_empty_config() {
        let fixture = Fixture::new();
        fixture.write("ssh/config", "Host web\n  HostName web.example.com\n");
        let config = fixture.load();
        assert_eq!(
            config.resolve_with_local_user("nope", "alice", &fixture.home()),
            Err(SshConfigError::UnknownHost {
                alias: "nope".to_string()
            })
        );
        let empty_dir = Fixture::new();
        let empty = SshConfigFile::load(&empty_dir.config_dir()).unwrap();
        assert!(empty.hosts(&empty_dir.home()).is_empty());
        assert_eq!(
            empty.resolve_with_local_user("web", "alice", &empty_dir.home()),
            Err(SshConfigError::UnknownHost {
                alias: "web".to_string()
            })
        );
    }

    #[test]
    fn hosts_omit_glob_patterns_and_deduplicate_repeated_aliases() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host web\n\nHost *.example.com\n  User x\n\nHost web\n  HostName again\n\nHost db?mirror\n\nHost cache\n",
        );
        let config = fixture.load();
        let aliases: Vec<String> = config
            .hosts(&fixture.home())
            .into_iter()
            .map(|h| h.alias)
            .collect();
        assert_eq!(aliases, vec!["web", "cache"]);
    }

    #[test]
    fn unparseable_lines_are_reported_and_resolution_continues_around_them() {
        let fixture = Fixture::new();
        fixture.write(
            "ssh/config",
            "Host web\n  HostName web.example.com\n= oops\n  IdentityFile ~/k\"unterminated\nHost after\n  HostName after.example.com\n",
        );
        let config = fixture.load();
        let web = config
            .resolve_with_local_user("web", "alice", &fixture.home())
            .unwrap();
        assert_eq!(web.host_name, "web.example.com");
        let after = config
            .resolve_with_local_user("after", "alice", &fixture.home())
            .unwrap();
        assert_eq!(after.host_name, "after.example.com");
        let diagnostics = config.diagnostics().join("\n");
        assert!(diagnostics.contains("oops"), "got: {diagnostics}");
        assert!(diagnostics.contains("unterminated"), "got: {diagnostics}");
    }

    #[test]
    fn fixture_identity_file_is_returned_as_is_and_tilde_expands_to_injected_home_only() {
        let fixture = Fixture::new();
        let key_path = fixture.dir.path().join("keys/id_ed25519");
        fixture.write(
            "ssh/config",
            &format!(
                "Host web\n  IdentityFile {}\nHost tilde\n  IdentityFile ~/only_in_fixture\n",
                key_path.display()
            ),
        );
        let config = fixture.load();
        // A non-existent injected home proves `~` never reaches the real home.
        let isolated_home = fixture.dir.path().join("no-such-home");
        let resolved = config
            .resolve_with_local_user("web", "alice", &isolated_home)
            .unwrap();
        assert_eq!(resolved.identity_file, Some(key_path));
        let tilde = config
            .resolve_with_local_user("tilde", "alice", &isolated_home)
            .unwrap();
        assert_eq!(
            tilde.identity_file,
            Some(isolated_home.join("only_in_fixture"))
        );
    }
}
