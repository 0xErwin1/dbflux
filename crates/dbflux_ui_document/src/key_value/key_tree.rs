//! Rows of the key list: a flat list or a namespace tree built from the keys
//! a scan has loaded so far.
//!
//! Kept free of GPUI so grouping, counting and sorting stay unit-testable.
//! The tree only groups what the scan returned: folder counts are marked as
//! partial until the scan has read the whole keyspace.

use dbflux_core::KeyEntry;
use std::collections::{BTreeMap, HashSet};

/// How the key list lays out the loaded keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(super) enum KeyListLayout {
    /// Every key on its own row, sorted by name.
    List,
    /// Keys grouped into folders by the namespace delimiter.
    #[default]
    Tree,
}

/// One visible row of the key list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum KeyListRow {
    /// A namespace such as `user:` holding `key_count` loaded keys.
    Folder {
        /// Full prefix including the trailing delimiter (`user:1042:`); also
        /// the identity used to remember whether the folder is expanded.
        prefix: String,
        depth: usize,
        key_count: usize,
        expanded: bool,
    },
    /// A loaded key; `key_index` points into the document's key vector.
    Key { key_index: usize, depth: usize },
}

impl KeyListRow {
    pub(super) fn key_index(&self) -> Option<usize> {
        match self {
            Self::Key { key_index, .. } => Some(*key_index),
            Self::Folder { .. } => None,
        }
    }

    pub(super) fn depth(&self) -> usize {
        match self {
            Self::Key { depth, .. } | Self::Folder { depth, .. } => *depth,
        }
    }
}

/// Default namespace delimiter when the connection does not set one.
pub(super) const DEFAULT_KEY_DELIMITER: &str = ":";

/// Namespace delimiter for a connection's `key_delimiter` driver setting:
/// a blank or missing value falls back to `:`.
pub(super) fn key_delimiter_from_setting(setting: Option<&str>) -> String {
    match setting {
        Some(value) if !value.is_empty() => value.to_string(),
        _ => DEFAULT_KEY_DELIMITER.to_string(),
    }
}

/// Builds the visible rows for `keys`.
///
/// In [`KeyListLayout::List`] every key is one row, sorted by name. In
/// [`KeyListLayout::Tree`] keys are grouped into folders by `delimiter`;
/// each level lists its folders first and then its keys, both sorted by
/// name, and only the children of folders in `expanded` are included.
pub(super) fn build_key_rows(
    keys: &[KeyEntry],
    layout: KeyListLayout,
    delimiter: &str,
    expanded: &HashSet<String>,
) -> Vec<KeyListRow> {
    match layout {
        KeyListLayout::List => {
            let mut named: Vec<(&str, usize)> = keys
                .iter()
                .enumerate()
                .map(|(key_index, entry)| (entry.key.as_str(), key_index))
                .collect();
            named.sort_by(|left, right| left.0.cmp(right.0));

            named
                .into_iter()
                .map(|(_, key_index)| KeyListRow::Key {
                    key_index,
                    depth: 0,
                })
                .collect()
        }
        KeyListLayout::Tree => {
            let root = build_namespace_tree(keys, delimiter);
            let mut rows = Vec::new();
            flatten_namespace(&root, 0, expanded, &mut rows);
            rows
        }
    }
}

/// A namespace level: nested folders by segment and the keys that end here.
#[derive(Default)]
struct NamespaceNode {
    prefix: String,
    folders: BTreeMap<String, NamespaceNode>,
    keys: Vec<(String, usize)>,
    key_count: usize,
}

fn build_namespace_tree(keys: &[KeyEntry], delimiter: &str) -> NamespaceNode {
    let mut root = NamespaceNode::default();

    for (key_index, entry) in keys.iter().enumerate() {
        insert_key(&mut root, &entry.key, key_index, delimiter);
    }

    root
}

fn insert_key(root: &mut NamespaceNode, key: &str, key_index: usize, delimiter: &str) {
    let segments = namespace_segments(key, delimiter);
    let mut node = root;

    for segment in segments {
        let child_prefix = format!("{}{segment}{delimiter}", node.prefix);

        node.key_count += 1;
        node = node
            .folders
            .entry(segment.to_string())
            .or_insert_with(|| NamespaceNode {
                prefix: child_prefix,
                ..NamespaceNode::default()
            });
    }

    node.key_count += 1;
    node.keys.push((key.to_string(), key_index));
}

/// The folder segments of `key`: every delimiter-separated part except the
/// last, which names the key itself. Empty segments (from a leading or
/// doubled delimiter) are kept so distinct keys never merge.
fn namespace_segments<'a>(key: &'a str, delimiter: &str) -> Vec<&'a str> {
    if delimiter.is_empty() {
        return Vec::new();
    }

    let mut parts: Vec<&str> = key.split(delimiter).collect();
    parts.pop();
    parts
}

fn flatten_namespace(
    node: &NamespaceNode,
    depth: usize,
    expanded: &HashSet<String>,
    rows: &mut Vec<KeyListRow>,
) {
    for folder in node.folders.values() {
        let is_expanded = expanded.contains(&folder.prefix);

        rows.push(KeyListRow::Folder {
            prefix: folder.prefix.clone(),
            depth,
            key_count: folder.key_count,
            expanded: is_expanded,
        });

        if is_expanded {
            flatten_namespace(folder, depth + 1, expanded, rows);
        }
    }

    let mut keys: Vec<&(String, usize)> = node.keys.iter().collect();
    keys.sort_by(|left, right| left.0.cmp(&right.0));

    rows.extend(keys.into_iter().map(|(_, key_index)| KeyListRow::Key {
        key_index: *key_index,
        depth,
    }));
}

/// Count shown on a folder row: exact once the scan is complete, and a
/// lower bound (`≥ 612 keys`) while more keys may still arrive.
pub(super) fn folder_count_label(key_count: usize, scan_complete: bool) -> String {
    let count = group_thousands(key_count as u64);

    match (scan_complete, key_count == 1) {
        (true, true) => dbflux_i18n::t!("document.key_value.tree.folder_count.one", count = count),
        (true, false) => {
            dbflux_i18n::t!("document.key_value.tree.folder_count.many", count = count)
        }
        (false, _) => dbflux_i18n::t!(
            "document.key_value.tree.folder_count.partial",
            count = count
        ),
    }
}

/// `1474` as `1,474`.
pub(super) fn group_thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }

    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(names: &[&str]) -> Vec<KeyEntry> {
        names.iter().map(|name| KeyEntry::new(*name)).collect()
    }

    fn expanded(prefixes: &[&str]) -> HashSet<String> {
        prefixes.iter().map(|prefix| prefix.to_string()).collect()
    }

    fn describe(rows: &[KeyListRow], keys: &[KeyEntry]) -> Vec<String> {
        rows.iter()
            .map(|row| match row {
                KeyListRow::Folder {
                    prefix,
                    depth,
                    key_count,
                    expanded,
                } => format!(
                    "{}{} ({key_count}){}",
                    "  ".repeat(*depth),
                    prefix,
                    if *expanded { " open" } else { "" }
                ),
                KeyListRow::Key { key_index, depth } => {
                    format!("{}{}", "  ".repeat(*depth), keys[*key_index].key)
                }
            })
            .collect()
    }

    #[test]
    fn list_layout_sorts_every_key_by_name() {
        let keys = keys(&["session:2", "rate:api", "session:1"]);

        let rows = build_key_rows(&keys, KeyListLayout::List, ":", &HashSet::new());

        assert_eq!(
            describe(&rows, &keys),
            vec!["rate:api", "session:1", "session:2"]
        );
    }

    #[test]
    fn tree_layout_groups_keys_into_collapsed_folders() {
        let keys = keys(&[
            "user:1041",
            "events:orders",
            "user:1042",
            "queue:emails",
            "flag",
        ]);

        let rows = build_key_rows(&keys, KeyListLayout::Tree, ":", &HashSet::new());

        assert_eq!(
            describe(&rows, &keys),
            vec!["events: (1)", "queue: (1)", "user: (2)", "flag"]
        );
    }

    #[test]
    fn tree_layout_sorts_each_level_and_counts_nested_keys() {
        let keys = keys(&[
            "user:1043",
            "user:1042:scores",
            "user:1041",
            "user:1042:events",
            "config",
        ]);

        let rows = build_key_rows(
            &keys,
            KeyListLayout::Tree,
            ":",
            &expanded(&["user:", "user:1042:"]),
        );

        assert_eq!(
            describe(&rows, &keys),
            vec![
                "user: (4) open",
                "  user:1042: (2) open",
                "    user:1042:events",
                "    user:1042:scores",
                "  user:1041",
                "  user:1043",
                "config",
            ]
        );
    }

    #[test]
    fn tree_layout_honors_a_custom_delimiter() {
        let keys = keys(&["app/cache/a", "app/cache/b", "app:plain"]);

        let rows = build_key_rows(&keys, KeyListLayout::Tree, "/", &expanded(&["app/"]));

        assert_eq!(
            describe(&rows, &keys),
            vec!["app/ (2) open", "  app/cache/ (2)", "app:plain"]
        );
    }

    #[test]
    fn key_rows_point_back_into_the_loaded_keys() {
        let keys = keys(&["b:1", "a:1"]);

        let rows = build_key_rows(&keys, KeyListLayout::Tree, ":", &expanded(&["a:", "b:"]));
        let indices: Vec<usize> = rows.iter().filter_map(KeyListRow::key_index).collect();

        assert_eq!(indices, vec![1, 0]);
    }

    #[test]
    fn empty_segments_stay_separate_from_named_ones() {
        let keys = keys(&[":leading", "a::b"]);

        let rows = build_key_rows(&keys, KeyListLayout::Tree, ":", &expanded(&["a:", "a::"]));

        assert_eq!(
            describe(&rows, &keys),
            vec![": (1)", "a: (1) open", "  a:: (1) open", "    a::b"]
        );
    }

    #[test]
    fn folder_counts_are_marked_partial_until_the_scan_completes() {
        assert_eq!(folder_count_label(612, false), "≥ 612 keys");
        assert_eq!(folder_count_label(1040, false), "≥ 1,040 keys");
        assert_eq!(folder_count_label(300, true), "300 keys");
        assert_eq!(folder_count_label(1, true), "1 key");
    }

    #[test]
    fn delimiter_setting_falls_back_to_a_colon() {
        assert_eq!(key_delimiter_from_setting(None), ":");
        assert_eq!(key_delimiter_from_setting(Some("")), ":");
        assert_eq!(key_delimiter_from_setting(Some("/")), "/");
    }

    #[test]
    fn group_thousands_inserts_separators() {
        assert_eq!(group_thousands(0), "0");
        assert_eq!(group_thousands(999), "999");
        assert_eq!(group_thousands(1_474), "1,474");
        assert_eq!(group_thousands(12_480_000), "12,480,000");
    }
}
