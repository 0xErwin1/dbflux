use super::hierarchy::{group_level, normalize_path, path_filter_value};

fn group(path: &str, names: &[&str]) -> (Vec<String>, Vec<String>) {
    let items = names.iter().map(|name| name.to_string()).collect();
    group_level(path, items, |name| name.as_str())
}

#[test]
fn normalize_path_maps_missing_and_bare_paths_to_the_root() {
    assert_eq!(normalize_path(None), "/");
    assert_eq!(normalize_path(Some("")), "/");
    assert_eq!(normalize_path(Some("/")), "/");
}

#[test]
fn normalize_path_adds_the_leading_and_trailing_slash() {
    assert_eq!(normalize_path(Some("/app/prod/")), "/app/prod/");
    assert_eq!(normalize_path(Some("/app/prod")), "/app/prod/");
    assert_eq!(normalize_path(Some("app/prod")), "/app/prod/");
}

#[test]
fn path_filter_is_absent_at_the_root_and_drops_the_trailing_slash_elsewhere() {
    assert_eq!(path_filter_value("/"), None);
    assert_eq!(
        path_filter_value("/app/prod/"),
        Some("/app/prod".to_string())
    );
}

#[test]
fn root_level_splits_entries_from_child_paths() {
    let (entries, child_paths) = group(
        "/",
        &["plain", "/top", "/app/prod/db", "/app/dev/db", "/other/x"],
    );

    assert_eq!(entries, ["plain", "/top"]);
    assert_eq!(child_paths, ["/app/", "/other/"]);
}

#[test]
fn nested_level_returns_direct_entries_and_distinct_child_paths() {
    let (entries, child_paths) = group(
        "/app/",
        &[
            "/app/name",
            "/app/prod/db/password",
            "/app/prod/db/user",
            "/app/dev/url",
            "/app/prod/url",
        ],
    );

    assert_eq!(entries, ["/app/name"]);
    assert_eq!(child_paths, ["/app/prod/", "/app/dev/"]);
}

#[test]
fn names_outside_the_path_are_dropped() {
    let (entries, child_paths) = group("/app/", &["/application/x", "/other/app/y", "plain"]);

    assert!(entries.is_empty());
    assert!(child_paths.is_empty());
}
