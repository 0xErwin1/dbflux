//! Lazy tree/pagination model backing `ObjectBrowserDocument`.
//!
//! This module is a pure data model: it never touches the network, GPUI, or
//! `Context`. Every transition is driven by feeding it an `ObjectListingPage`
//! (or an error) obtained from `ObjectStoreConnection::list_objects` — the
//! caller (`super::data`) owns the `cx.spawn` plumbing and simply reports
//! results back here.
//!
//! Two navigation modes are modeled, both backed by the SAME per-prefix
//! pagination cache (`levels`):
//!
//! - **Per-level pagination** (the default, AWS-console-style): only the
//!   current prefix's children are shown; expanding a prefix navigates into
//!   it, loading its first page if it has never been fetched.
//! - **Tree mode**: a nested presentation of the same cache. A prefix node
//!   is either collapsed or expanded (`tree_mode.expanded`); expanding a node
//!   loads ONLY that node's first page of children, exactly like navigating
//!   into it in per-level mode — there is no recursive walk and no eager
//!   fetch of the whole bucket. Collapsing a node only removes it from the
//!   expanded set; its already-loaded children stay cached in `levels` so
//!   re-expanding it is instant.

use dbflux_core::{ObjectListingPage, ObjectSummary};

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

/// Nested-presentation state: whether tree mode is on, and which prefix
/// nodes are currently expanded. Toggling tree mode is instant — it never
/// touches `expanded` or `levels` — and expanding/collapsing a node never
/// discards cached entries, only whether they are currently displayed.
#[derive(Clone, Debug, Default)]
pub struct TreeModeState {
    pub enabled: bool,
    pub expanded: std::collections::HashSet<String>,
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
        let Some(parent) = self.parent_prefix() else {
            return;
        };

        self.current_prefix = parent;
        self.selected = None;
    }

    /// The level above `current_prefix`, or `None` at the bucket root.
    pub fn parent_prefix(&self) -> Option<String> {
        if self.current_prefix.is_empty() {
            return None;
        }

        let trimmed = self.current_prefix.trim_end_matches('/');

        Some(match trimmed.rfind('/') {
            Some(index) => trimmed[..=index].to_string(),
            None => String::new(),
        })
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

    pub fn is_tree_mode(&self) -> bool {
        self.tree_mode.enabled
    }

    /// Flips tree mode on/off. Purely a presentation switch: it never fetches
    /// anything and never touches `expanded` or the per-prefix cache, so the
    /// toggle itself is instant.
    pub fn toggle_tree_mode(&mut self) {
        self.tree_mode.enabled = !self.tree_mode.enabled;
    }

    pub fn is_expanded(&self, prefix: &str) -> bool {
        self.tree_mode.expanded.contains(prefix)
    }

    /// Marks a prefix node expanded. Loading its first page (if it has never
    /// been fetched) is the caller's (`super::data`) responsibility.
    pub fn expand_node(&mut self, prefix: &str) {
        self.tree_mode.expanded.insert(prefix.to_string());
    }

    /// Collapses a prefix node. Its children stay cached in `levels` — only
    /// whether they are currently displayed changes.
    pub fn collapse_node(&mut self, prefix: &str) {
        self.tree_mode.expanded.remove(prefix);
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

    /// Toggling tree mode is a pure presentation flip: it never touches the
    /// per-prefix cache or the expanded set, and it never fetches anything.
    #[test]
    fn toggle_tree_mode_only_flips_the_flag() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.apply_page("", page(&["logs/"], &["a.txt"], None));

        tree.toggle_tree_mode();
        assert!(tree.is_tree_mode());
        assert!(tree.tree_mode.expanded.is_empty());
        assert_eq!(tree.level("").unwrap().entries.len(), 2);

        tree.toggle_tree_mode();
        assert!(!tree.is_tree_mode());
        // Turning tree mode off must not drop anything already loaded.
        assert_eq!(tree.level("").unwrap().entries.len(), 2);
    }

    /// Expanding a node only marks it expanded — loading its children is the
    /// caller's job (`super::data::expand_tree_node`), never `ObjectTree`'s.
    #[test]
    fn expand_node_only_marks_the_node_expanded() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.toggle_tree_mode();

        assert!(!tree.is_expanded("logs/"));
        tree.expand_node("logs/");
        assert!(tree.is_expanded("logs/"));

        // No page was applied — the node has no entries yet, matching a
        // caller that has not fetched anything for it.
        assert!(tree.level("logs/").is_none());
    }

    /// Collapsing a node removes it from the expanded set but keeps its
    /// already-loaded children cached, so re-expanding it is instant (no
    /// re-fetch).
    #[test]
    fn collapse_node_keeps_cached_children() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.toggle_tree_mode();
        tree.expand_node("logs/");
        tree.apply_page("logs/", page(&[], &["logs/2026-01-01.log"], None));

        tree.collapse_node("logs/");

        assert!(!tree.is_expanded("logs/"));
        assert_eq!(tree.level("logs/").unwrap().entries.len(), 1);

        tree.expand_node("logs/");
        assert!(tree.is_expanded("logs/"));
        assert_eq!(tree.level("logs/").unwrap().entries.len(), 1);
    }

    /// Expanding one node never touches a sibling's cache or expanded state —
    /// there is no recursive walk that would eagerly reach into it.
    #[test]
    fn expanding_one_node_never_loads_a_sibling() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.toggle_tree_mode();
        tree.apply_page("", page(&["logs/", "assets/"], &[], None));

        tree.expand_node("logs/");
        tree.apply_page("logs/", page(&[], &["logs/a.log"], None));

        assert!(tree.is_expanded("logs/"));
        assert!(!tree.is_expanded("assets/"));
        assert!(tree.level("assets/").is_none());
    }

    /// A node's own continuation token survives expand/collapse/expand,
    /// exactly like `PrefixLevel::has_more` for per-level pagination — each
    /// node paginates independently.
    #[test]
    fn expanded_node_keeps_its_own_continuation_token() {
        let mut tree = ObjectTree::new("my-bucket".to_string());
        tree.toggle_tree_mode();
        tree.expand_node("logs/");
        tree.apply_page("logs/", page(&[], &["logs/a.log"], Some("tok-1")));

        assert_eq!(tree.continuation_token("logs/"), Some("tok-1".to_string()));

        tree.collapse_node("logs/");
        tree.expand_node("logs/");

        assert_eq!(tree.continuation_token("logs/"), Some("tok-1".to_string()));
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
