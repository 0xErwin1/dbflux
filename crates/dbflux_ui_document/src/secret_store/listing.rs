//! The paged listing of one store level: its entries, its sub-paths and the
//! token of the next page.

use dbflux_core::{SecretListPage, SecretSummary};

/// How a hierarchical store lists the current path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathMode {
    /// One level: the entries directly under the path and its sub-paths.
    Tree,
    /// Every entry below the path (`SecretListRequest::recursive`).
    Flat,
}

/// The listing of the current level. Pages append to it; a reload starts it
/// over. Every load carries a generation, so the result of a load that a
/// newer one replaced is dropped instead of merged.
#[derive(Debug, Default)]
pub struct SecretListing {
    pub entries: Vec<SecretSummary>,
    pub child_paths: Vec<String>,
    pub next_page_token: Option<String>,
    pub loading: bool,
    pub error: Option<String>,
    generation: u64,
}

impl SecretListing {
    /// Starts a load and returns its generation. A first page clears what
    /// was listed; a next page keeps it.
    pub fn begin(&mut self, first_page: bool) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.loading = true;
        self.error = None;

        if first_page {
            self.entries.clear();
            self.child_paths.clear();
            self.next_page_token = None;
        }

        self.generation
    }

    /// Appends a page of the load `generation`. Returns `false` and changes
    /// nothing when a newer load replaced it. Entries already listed and
    /// sub-paths a previous page returned are not added twice.
    pub fn apply_page(&mut self, generation: u64, page: SecretListPage) -> bool {
        if generation != self.generation {
            return false;
        }

        for entry in page.entries {
            if !self.entries.iter().any(|known| known.name == entry.name) {
                self.entries.push(entry);
            }
        }

        for path in page.child_paths {
            if !self.child_paths.contains(&path) {
                self.child_paths.push(path);
            }
        }

        self.next_page_token = page.next_page_token;
        self.loading = false;
        true
    }

    /// Records the failure of the load `generation`, unless a newer load
    /// replaced it.
    pub fn apply_error(&mut self, generation: u64, message: String) -> bool {
        if generation != self.generation {
            return false;
        }

        self.loading = false;
        self.error = Some(message);
        true
    }

    pub fn has_more(&self) -> bool {
        self.next_page_token.is_some()
    }

    pub fn find(&self, name: &str) -> Option<&SecretSummary> {
        self.entries.iter().find(|entry| entry.name == name)
    }
}

/// The parent of `path` (`/app/prod/` → `/app/`), or `None` at the root.
pub(super) fn parent_path(path: &str) -> Option<String> {
    let trimmed = path.strip_suffix('/').unwrap_or(path);

    match trimmed.rfind('/') {
        Some(0) | None => None,
        Some(index) => trimmed.get(..=index).map(str::to_string),
    }
}

/// The segments of `path` with the full path each one leads to:
/// `/app/prod/` gives `[("app", "/app/"), ("prod", "/app/prod/")]`.
pub(super) fn path_segments(path: &str) -> Vec<(String, String)> {
    let mut segments = Vec::new();
    let mut prefix = String::from("/");

    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        prefix.push_str(segment);
        prefix.push('/');
        segments.push((segment.to_string(), prefix.clone()));
    }

    segments
}

/// The name of an entry relative to the listed path, which is how a tree
/// level shows it (`/app/prod/db/host` under `/app/prod/db/` reads `host`).
pub(super) fn relative_name<'a>(name: &'a str, path: Option<&str>) -> &'a str {
    path.and_then(|path| name.strip_prefix(path))
        .filter(|rest| !rest.is_empty())
        .unwrap_or(name)
}

/// The last segment of a sub-path with its separator (`/app/prod/db/` reads
/// `db/`).
pub(super) fn child_path_label(path: &str) -> String {
    let trimmed = path.strip_suffix('/').unwrap_or(path);

    match trimmed.rsplit_once('/') {
        Some((_, last)) if !last.is_empty() => format!("{last}/"),
        _ => path.to_string(),
    }
}
