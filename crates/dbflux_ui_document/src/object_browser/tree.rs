//! Lazy tree/pagination model backing `ObjectBrowserDocument`.
//!
//! This module is a pure data model: it never touches the network, GPUI, or
//! `Context`. Every transition is driven by feeding it an `ObjectListingPage`
//! (or an error) obtained from `ObjectStoreConnection::list_objects` — the
//! caller (`super::data`) owns the `cx.spawn` plumbing and simply reports
//! results back here.
//!
//! Two navigation modes are modeled:
//!
//! - **Per-level pagination** (the default, AWS-console-style): expanding a
//!   prefix loads ONE page; `has_more`/`continuation_token` drive an explicit
//!   "load more" for the next page of the same level.
//! - **Tree mode**: a bounded, cancelable walk that recursively lists every
//!   level and flattens the result into a single indented listing. Driven by
//!   a monotonically increasing `generation` counter so that toggling tree
//!   mode off invalidates any in-flight page applied after the toggle.

use dbflux_core::{ObjectListingPage, ObjectSummary};

/// Safety cap on how many `ListObjectsV2` pages a single tree-mode walk may
/// consume before it stops itself and reports `Capped`. Tree mode is a
/// non-paginated full expansion — without a cap, a bucket with a very deep or
/// wide prefix structure could turn one toggle into thousands of billed
/// S3 calls.
pub const TREE_MODE_PAGE_CAP: u32 = 500;

/// Identity of a single row in the tree — either a "folder" (common prefix)
/// or a leaf object, addressed by its full key/prefix path.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ObjectTreeNodeId {
    Prefix(String),
    Object(String),
}

/// One entry loaded into a prefix level: either a sub-prefix (folder) or an
/// object (leaf), following `ListObjectsV2` delimiter semantics.
#[derive(Clone, Debug, PartialEq)]
pub enum ObjectTreeEntry {
    Prefix(String),
    Object(ObjectSummary),
}

impl ObjectTreeEntry {
    pub fn node_id(&self) -> ObjectTreeNodeId {
        match self {
            ObjectTreeEntry::Prefix(key) => ObjectTreeNodeId::Prefix(key.clone()),
            ObjectTreeEntry::Object(summary) => ObjectTreeNodeId::Object(summary.key.clone()),
        }
    }

    pub fn full_key(&self) -> &str {
        match self {
            ObjectTreeEntry::Prefix(key) => key,
            ObjectTreeEntry::Object(summary) => &summary.key,
        }
    }

    /// Display name for this entry relative to its containing prefix — the
    /// full key with the parent prefix stripped and any trailing delimiter
    /// removed (so `"logs/2026/"` under parent `"logs/"` displays as
    /// `"2026"`).
    pub fn display_name(&self, parent_prefix: &str) -> String {
        let stripped = self
            .full_key()
            .strip_prefix(parent_prefix)
            .unwrap_or(self.full_key());
        stripped.strip_suffix('/').unwrap_or(stripped).to_string()
    }

    pub fn is_prefix(&self) -> bool {
        matches!(self, ObjectTreeEntry::Prefix(_))
    }
}

/// Loading state of a single prefix level's entry list.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum PrefixLoadState {
    #[default]
    NotLoaded,
    Loading,
    /// Entries are already loaded; a "load more" call for the next page is
    /// in flight.
    LoadingMore,
    Loaded,
    Error(String),
}

/// Loaded (or loading) state of one prefix level — the direct children of a
/// single prefix path, following per-level pagination.
#[derive(Clone, Debug, Default)]
pub struct PrefixLevel {
    pub entries: Vec<ObjectTreeEntry>,
    pub next_token: Option<String>,
    pub state: PrefixLoadState,
    pub filter: String,
}

impl PrefixLevel {
    pub fn has_more(&self) -> bool {
        self.next_token.is_some()
    }

    /// Entries under `filter` (case-insensitive substring match against the
    /// display name relative to `parent_prefix`). An empty filter returns
    /// every entry.
    pub fn filtered_entries(&self, parent_prefix: &str) -> Vec<&ObjectTreeEntry> {
        let query = self.filter.trim().to_lowercase();

        if query.is_empty() {
            return self.entries.iter().collect();
        }

        self.entries
            .iter()
            .filter(|entry| {
                entry
                    .display_name(parent_prefix)
                    .to_lowercase()
                    .contains(&query)
            })
            .collect()
    }
}

/// Status of a tree-mode walk.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum TreeModeStatus {
    #[default]
    Off,
    Running,
    Done,
    Cancelled,
    Capped,
    Error(String),
}

/// One flattened row produced by a tree-mode walk, carrying its depth for
/// indentation.
#[derive(Clone, Debug, PartialEq)]
pub struct TreeModeRow {
    pub depth: usize,
    pub parent_prefix: String,
    pub entry: ObjectTreeEntry,
}

/// State of the current (or last) tree-mode walk.
#[derive(Clone, Debug, Default)]
pub struct TreeModeState {
    pub status: TreeModeStatus,
    pub generation: u64,
    pub pages_walked: u32,
    pub rows: Vec<TreeModeRow>,
}

/// Outcome of feeding a single page into an in-progress tree-mode walk. The
/// walker (`super::data`) uses `discovered_prefixes` and `continuation_token`
/// to decide what to list next.
#[derive(Clone, Debug, PartialEq)]
pub struct TreeModeStepOutcome {
    /// `false` when the page was ignored because `generation` no longer
    /// matches the tree's current walk (the walk was cancelled or restarted).
    pub applied: bool,
    /// `true` when this step tripped the safety cap; the walker must stop.
    pub capped: bool,
    /// Sub-prefixes discovered on this page, for the walker to enqueue at
    /// `depth + 1`.
    pub discovered_prefixes: Vec<String>,
    /// `Some(token)` when this level has another page to fetch before moving
    /// on to sub-prefixes.
    pub continuation_token: Option<String>,
}

/// Breadcrumb + per-level pagination + tree-mode state for one bucket.
///
/// `""` is used as the root prefix throughout (the bucket root).
pub struct ObjectTree {
    pub bucket: String,
    levels: std::collections::HashMap<String, PrefixLevel>,
    pub current_prefix: String,
    pub selected: Option<ObjectTreeNodeId>,
    pub tree_mode: TreeModeState,
}

impl ObjectTree {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            levels: std::collections::HashMap::new(),
            current_prefix: String::new(),
            selected: None,
            tree_mode: TreeModeState::default(),
        }
    }

    pub fn level(&self, prefix: &str) -> Option<&PrefixLevel> {
        self.levels.get(prefix)
    }

    fn level_mut(&mut self, prefix: &str) -> &mut PrefixLevel {
        self.levels.entry(prefix.to_string()).or_default()
    }

    // -- Per-level pagination ---------------------------------------------

    /// Marks a prefix level as loading. `LoadingMore` when entries already
    /// exist (a "load more" continuation), `Loading` for a first fetch.
    pub fn begin_load(&mut self, prefix: &str) {
        let has_entries = self
            .levels
            .get(prefix)
            .is_some_and(|level| !level.entries.is_empty());
        let level = self.level_mut(prefix);
        level.state = if has_entries {
            PrefixLoadState::LoadingMore
        } else {
            PrefixLoadState::Loading
        };
    }

    /// The continuation token to pass on the next `list_objects` call for
    /// this level (`None` on a first fetch or once exhausted).
    pub fn continuation_token(&self, prefix: &str) -> Option<String> {
        self.levels
            .get(prefix)
            .and_then(|level| level.next_token.clone())
    }

    /// Merges a freshly loaded page into a prefix level: appended entries and
    /// a replaced continuation token.
    pub fn apply_page(&mut self, prefix: &str, page: ObjectListingPage) {
        let level = self.level_mut(prefix);

        level.entries.extend(
            page.common_prefixes
                .into_iter()
                .map(ObjectTreeEntry::Prefix)
                .chain(page.objects.into_iter().map(ObjectTreeEntry::Object)),
        );
        level.next_token = page.next_continuation_token;
        level.state = PrefixLoadState::Loaded;
    }

    /// Drops a level's cached entries and continuation token so the next load
    /// starts from the first page again. The per-level filter survives — a
    /// refresh must not silently widen what the user asked to see.
    pub fn reset_level(&mut self, prefix: &str) {
        let level = self.level_mut(prefix);

        level.entries.clear();
        level.next_token = None;
        level.state = PrefixLoadState::NotLoaded;
    }

    pub fn apply_error(&mut self, prefix: &str, message: String) {
        self.level_mut(prefix).state = PrefixLoadState::Error(message);
    }

    // -- Filter --------------------------------------------------------------

    pub fn set_filter(&mut self, prefix: &str, filter: String) {
        self.level_mut(prefix).filter = filter;
    }

    pub fn filtered_entries(&self, prefix: &str) -> Vec<&ObjectTreeEntry> {
        self.levels
            .get(prefix)
            .map(|level| level.filtered_entries(prefix))
            .unwrap_or_default()
    }

    // -- Breadcrumb / navigation ----------------------------------------------

    /// Navigates into a sub-prefix (must end in `/`, per `ListObjectsV2`
    /// delimiter semantics). Clears the row selection — the previous
    /// selection belonged to the level being left.
    pub fn navigate_into(&mut self, prefix: String) {
        self.current_prefix = prefix;
        self.selected = None;
    }

    /// Navigates one level up. No-op at the bucket root.
    pub fn navigate_up(&mut self) {
        if self.current_prefix.is_empty() {
            return;
        }

        let trimmed = self.current_prefix.trim_end_matches('/');
        self.current_prefix = match trimmed.rfind('/') {
            Some(index) => trimmed[..=index].to_string(),
            None => String::new(),
        };
        self.selected = None;
    }

    /// Breadcrumb segments from the bucket root down to `current_prefix`,
    /// e.g. `"logs/2026/07/"` -> `["logs", "2026", "07"]`.
    pub fn breadcrumb_segments(&self) -> Vec<String> {
        self.current_prefix
            .trim_end_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(str::to_string)
            .collect()
    }

    // -- Selection -------------------------------------------------------

    pub fn select(&mut self, node_id: Option<ObjectTreeNodeId>) {
        self.selected = node_id;
    }

    // -- Tree mode ---------------------------------------------------------

    /// Starts a new tree-mode walk, bumping the generation so any page
    /// applied under a stale (previous) generation is ignored. Returns the
    /// new generation for the walker to carry through its calls.
    pub fn start_tree_mode(&mut self) -> u64 {
        self.tree_mode.generation += 1;
        self.tree_mode.status = TreeModeStatus::Running;
        self.tree_mode.pages_walked = 0;
        self.tree_mode.rows.clear();
        self.tree_mode.generation
    }

    /// Cancels the current tree-mode walk (toggle-off). Bumps the generation
    /// so any page still in flight from the cancelled walk is ignored when it
    /// lands.
    pub fn cancel_tree_mode(&mut self) {
        self.tree_mode.generation += 1;
        self.tree_mode.status = TreeModeStatus::Cancelled;
    }

    pub fn is_tree_mode_current(&self, generation: u64) -> bool {
        self.tree_mode.status == TreeModeStatus::Running && self.tree_mode.generation == generation
    }

    pub fn mark_tree_mode_done(&mut self, generation: u64) {
        if self.is_tree_mode_current(generation) {
            self.tree_mode.status = TreeModeStatus::Done;
        }
    }

    pub fn mark_tree_mode_error(&mut self, generation: u64, message: String) {
        if self.tree_mode.generation == generation {
            self.tree_mode.status = TreeModeStatus::Error(message);
        }
    }

    /// Feeds one page of a tree-mode walk at `depth`/`prefix` into the
    /// accumulated flattened listing.
    ///
    /// Ignored (returns `applied: false`) if `generation` is stale. Trips
    /// `TREE_MODE_PAGE_CAP` and reports `capped: true` once reached — the
    /// walker must stop enqueueing further work in that case.
    pub fn apply_tree_mode_page(
        &mut self,
        generation: u64,
        depth: usize,
        prefix: &str,
        page: ObjectListingPage,
    ) -> TreeModeStepOutcome {
        if !self.is_tree_mode_current(generation) {
            return TreeModeStepOutcome {
                applied: false,
                capped: false,
                discovered_prefixes: Vec::new(),
                continuation_token: None,
            };
        }

        self.tree_mode.pages_walked += 1;
        let capped = self.tree_mode.pages_walked >= TREE_MODE_PAGE_CAP;

        let discovered_prefixes = page.common_prefixes.clone();

        self.tree_mode.rows.extend(
            page.common_prefixes
                .into_iter()
                .map(ObjectTreeEntry::Prefix)
                .chain(page.objects.into_iter().map(ObjectTreeEntry::Object))
                .map(|entry| TreeModeRow {
                    depth,
                    parent_prefix: prefix.to_string(),
                    entry,
                }),
        );

        if capped {
            self.tree_mode.status = TreeModeStatus::Capped;
        }

        TreeModeStepOutcome {
            applied: true,
            capped,
            discovered_prefixes,
            continuation_token: if capped {
                None
            } else {
                page.next_continuation_token
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(key: &str) -> ObjectSummary {
        ObjectSummary {
            key: key.to_string(),
            size_bytes: 10,
            storage_class: None,
            last_modified: None,
        }
    }

    fn page(prefixes: &[&str], objects: &[&str], token: Option<&str>) -> ObjectListingPage {
        ObjectListingPage {
            objects: objects.iter().map(|key| summary(key)).collect(),
            common_prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            next_continuation_token: token.map(str::to_string),
        }
    }

    // -- Per-level pagination ---------------------------------------------

    #[test]
    fn begin_load_is_loading_on_first_fetch_and_loading_more_after() {
        let mut tree = ObjectTree::new("my-bucket".to_string());

        tree.begin_load("");
        assert_eq!(tree.level("").unwrap().state, PrefixLoadState::Loading);

        tree.apply_page("", page(&["logs/"], &["readme.txt"], None));
        tree.begin_load("");
        assert_eq!(tree.level("").unwrap().state, PrefixLoadState::LoadingMore);
    }

    #[test]
    fn apply_page_appends_entries_and_replaces_token() {
        let mut tree = ObjectTree::new("my-bucket".to_string());

        tree.apply_page("", page(&["logs/"], &["a.txt"], Some("token-1")));
        assert_eq!(tree.level("").unwrap().entries.len(), 2);
        assert_eq!(tree.continuation_token(""), Some("token-1".to_string()));

        tree.apply_page("", page(&[], &["b.txt"], None));
        let level = tree.level("").unwrap();
        assert_eq!(level.entries.len(), 3);
        assert_eq!(level.state, PrefixLoadState::Loaded);
        assert_eq!(tree.continuation_token(""), None);
    }

    #[test]
    fn reset_level_clears_entries_and_token_but_keeps_the_filter() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.apply_page("", page(&["logs/"], &["a.txt"], Some("token-1")));
        tree.set_filter("", "log".to_string());

        tree.reset_level("");

        let level = tree.level("").unwrap();
        assert!(level.entries.is_empty());
        assert_eq!(level.next_token, None);
        assert_eq!(level.state, PrefixLoadState::NotLoaded);
        assert_eq!(level.filter, "log");
    }

    #[test]
    fn apply_error_records_message_on_the_level() {
        let mut tree = ObjectTree::new("my-bucket".to_string());

        tree.apply_error("logs/", "network error".to_string());

        match &tree.level("logs/").unwrap().state {
            PrefixLoadState::Error(message) => assert_eq!(message, "network error"),
            other => panic!("expected Error state, got {other:?}"),
        }
    }

    #[test]
    fn has_more_reflects_the_continuation_token() {
        let mut tree = ObjectTree::new("my-bucket".to_string());

        tree.apply_page("", page(&[], &["a.txt"], Some("token-1")));
        assert!(tree.level("").unwrap().has_more());

        tree.apply_page("", page(&[], &["b.txt"], None));
        assert!(!tree.level("").unwrap().has_more());
    }

    // -- Filter --------------------------------------------------------------

    #[test]
    fn filter_matches_display_name_case_insensitively() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.apply_page("", page(&["Logs/", "assets/"], &["README.txt"], None));

        tree.set_filter("", "log".to_string());
        let names: Vec<String> = tree
            .filtered_entries("")
            .iter()
            .map(|e| e.display_name(""))
            .collect();
        assert_eq!(names, vec!["Logs"]);
    }

    #[test]
    fn empty_filter_returns_every_entry() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.apply_page("", page(&["logs/"], &["a.txt", "b.txt"], None));

        assert_eq!(tree.filtered_entries("").len(), 3);
    }

    // -- Path navigation ---------------------------------------------------

    #[test]
    fn navigate_into_sets_prefix_and_clears_selection() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.select(Some(ObjectTreeNodeId::Object("a.txt".to_string())));

        tree.navigate_into("logs/".to_string());

        assert_eq!(tree.current_prefix, "logs/");
        assert_eq!(tree.selected, None);
    }

    #[test]
    fn navigate_up_walks_back_one_level_at_a_time() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.navigate_into("logs/2026/07/".to_string());

        tree.navigate_up();
        assert_eq!(tree.current_prefix, "logs/2026/");

        tree.navigate_up();
        assert_eq!(tree.current_prefix, "logs/");

        tree.navigate_up();
        assert_eq!(tree.current_prefix, "");

        // No-op at the root.
        tree.navigate_up();
        assert_eq!(tree.current_prefix, "");
    }

    #[test]
    fn breadcrumb_segments_split_the_current_prefix() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.navigate_into("logs/2026/07/".to_string());

        assert_eq!(tree.breadcrumb_segments(), vec!["logs", "2026", "07"]);
    }

    #[test]
    fn breadcrumb_segments_empty_at_the_bucket_root() {
        let tree = ObjectTree::new("my-bucket".to_string());

        assert!(tree.breadcrumb_segments().is_empty());
    }

    // -- Tree mode -----------------------------------------------------------

    #[test]
    fn start_tree_mode_bumps_generation_and_clears_prior_rows() {
        let mut tree = ObjectTree::new("my-bucket".to_string());

        let gen1 = tree.start_tree_mode();
        tree.apply_tree_mode_page(gen1, 0, "", page(&[], &["a.txt"], None));
        assert_eq!(tree.tree_mode.rows.len(), 1);

        let gen2 = tree.start_tree_mode();
        assert_ne!(gen1, gen2);
        assert!(tree.tree_mode.rows.is_empty());
        assert_eq!(tree.tree_mode.status, TreeModeStatus::Running);
    }

    #[test]
    fn cancel_tree_mode_invalidates_the_current_generation() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        let generation = tree.start_tree_mode();

        tree.cancel_tree_mode();

        assert!(!tree.is_tree_mode_current(generation));
        assert_eq!(tree.tree_mode.status, TreeModeStatus::Cancelled);
    }

    #[test]
    fn stale_page_after_cancel_is_ignored() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        let generation = tree.start_tree_mode();
        tree.cancel_tree_mode();

        let outcome = tree.apply_tree_mode_page(generation, 0, "", page(&[], &["a.txt"], None));

        assert!(!outcome.applied);
        assert!(tree.tree_mode.rows.is_empty());
    }

    #[test]
    fn tree_mode_accumulates_rows_across_multiple_levels() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        let generation = tree.start_tree_mode();

        let outcome =
            tree.apply_tree_mode_page(generation, 0, "", page(&["logs/"], &["readme.txt"], None));
        assert_eq!(outcome.discovered_prefixes, vec!["logs/".to_string()]);
        assert_eq!(outcome.continuation_token, None);

        tree.apply_tree_mode_page(generation, 1, "logs/", page(&[], &["2026-01-01.log"], None));

        assert_eq!(tree.tree_mode.rows.len(), 3);
        assert_eq!(tree.tree_mode.rows[0].depth, 0);
        assert_eq!(tree.tree_mode.rows[2].depth, 1);
        assert_eq!(tree.tree_mode.rows[2].parent_prefix, "logs/");
    }

    #[test]
    fn tree_mode_reports_a_continuation_token_when_a_level_has_more() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        let generation = tree.start_tree_mode();

        let outcome =
            tree.apply_tree_mode_page(generation, 0, "", page(&[], &["a.txt"], Some("tok")));

        assert_eq!(outcome.continuation_token, Some("tok".to_string()));
        assert!(!outcome.capped);
    }

    #[test]
    fn tree_mode_trips_the_safety_cap() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        let generation = tree.start_tree_mode();

        let mut last_outcome = None;
        for _ in 0..TREE_MODE_PAGE_CAP {
            last_outcome = Some(tree.apply_tree_mode_page(
                generation,
                0,
                "",
                page(&[], &["a.txt"], Some("tok")),
            ));
        }

        let outcome = last_outcome.unwrap();
        assert!(outcome.capped);
        assert_eq!(outcome.continuation_token, None);
        assert_eq!(tree.tree_mode.status, TreeModeStatus::Capped);
    }

    #[test]
    fn mark_tree_mode_done_only_applies_to_the_current_generation() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        let generation = tree.start_tree_mode();

        tree.mark_tree_mode_done(generation + 1);
        assert_eq!(tree.tree_mode.status, TreeModeStatus::Running);

        tree.mark_tree_mode_done(generation);
        assert_eq!(tree.tree_mode.status, TreeModeStatus::Done);
    }

    #[test]
    fn mark_tree_mode_error_records_the_message() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        let generation = tree.start_tree_mode();

        tree.mark_tree_mode_error(generation, "boom".to_string());

        match &tree.tree_mode.status {
            TreeModeStatus::Error(message) => assert_eq!(message, "boom"),
            other => panic!("expected Error status, got {other:?}"),
        }
    }

    #[test]
    fn display_name_strips_parent_prefix_and_trailing_delimiter() {
        let entry = ObjectTreeEntry::Prefix("logs/2026/".to_string());
        assert_eq!(entry.display_name("logs/"), "2026");

        let object = ObjectTreeEntry::Object(summary("logs/readme.txt"));
        assert_eq!(object.display_name("logs/"), "readme.txt");
    }

    #[test]
    fn node_id_distinguishes_prefixes_from_objects() {
        let prefix = ObjectTreeEntry::Prefix("logs/".to_string());
        let object = ObjectTreeEntry::Object(summary("logs/a.txt"));

        assert_eq!(
            prefix.node_id(),
            ObjectTreeNodeId::Prefix("logs/".to_string())
        );
        assert_eq!(
            object.node_id(),
            ObjectTreeNodeId::Object("logs/a.txt".to_string())
        );
        assert!(prefix.is_prefix());
        assert!(!object.is_prefix());
    }
}
