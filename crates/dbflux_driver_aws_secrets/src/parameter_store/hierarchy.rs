//! One-level browsing of Parameter Store's `/`-separated names.
//!
//! Paths are normalized to start and end with `/`, with `/` as the root.
//! Names without a leading `/` belong to the root.

const ROOT: &str = "/";

pub(crate) fn normalize_path(path: Option<&str>) -> String {
    let trimmed = path.unwrap_or_default().trim().trim_matches('/');

    if trimmed.is_empty() {
        return ROOT.to_string();
    }

    format!("/{trimmed}/")
}

/// Value of the `Path` filter for a normalized path. The root has none,
/// because names without a leading `/` would not match a `/` path filter.
pub(crate) fn path_filter_value(path: &str) -> Option<String> {
    if path == ROOT {
        return None;
    }

    Some(path.trim_end_matches('/').to_string())
}

enum Placement {
    Entry,
    Child(String),
    Outside,
}

fn place(path: &str, name: &str) -> Placement {
    let remainder = if path == ROOT {
        match name.strip_prefix('/') {
            Some(remainder) => remainder,
            None => return Placement::Entry,
        }
    } else {
        match name.strip_prefix(path) {
            Some(remainder) => remainder,
            None => return Placement::Outside,
        }
    };

    if remainder.is_empty() {
        return Placement::Outside;
    }

    match remainder.split_once('/') {
        Some((segment, _)) if !segment.is_empty() => Placement::Child(format!("{path}{segment}/")),
        _ => Placement::Entry,
    }
}

/// Splits one page of items into the entries directly under `path` and the
/// distinct child paths below it, both in first-seen order. Items whose name
/// lies outside `path` are dropped.
pub(crate) fn group_level<T>(
    path: &str,
    items: Vec<T>,
    name: impl Fn(&T) -> &str,
) -> (Vec<T>, Vec<String>) {
    let mut entries = Vec::new();
    let mut child_paths: Vec<String> = Vec::new();

    for item in items {
        match place(path, name(&item)) {
            Placement::Entry => entries.push(item),
            Placement::Child(child_path) => {
                if !child_paths.contains(&child_path) {
                    child_paths.push(child_path);
                }
            }
            Placement::Outside => {}
        }
    }

    (entries, child_paths)
}
