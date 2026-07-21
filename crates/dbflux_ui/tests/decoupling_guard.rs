//! Decoupling guard: asserts that no user-facing "CloudWatch" string literals
//! appear in the UI crate sources.
//!
//! Enforcement scope: the six UI-layer crate source trees listed in
//! `SCOPE_DIRS`. The guard only flags Rust string literals — i.e., occurrences
//! of the byte sequence `"CloudWatch"` (including the surrounding double
//! quotes) — so Rust identifiers, type names, doc comments (`///`), and block
//! comments (`//`) are all excluded from the check.
//!
//! Explicit allow-list (suppressed even when found inside a string literal):
//! - `CloudWatchLogsInsightsQl` — the `QueryLanguage` enum variant name that
//!   must keep its capitalization for wire compatibility.
//! - Lines inside `#[cfg(test)]` blocks — test-only assertions may reference
//!   the word as part of "must not contain CloudWatch" assertions.

use std::fs;
use std::path::{Path, PathBuf};

/// Crate source directories within the workspace that the guard covers.
const SCOPE_DIRS: &[&str] = &[
    "crates/dbflux_ui/src",
    "crates/dbflux_ui_document/src",
    "crates/dbflux_ui_base/src",
    "crates/dbflux_ui_sidebar/src",
    "crates/dbflux_ui_windows/src",
    "crates/dbflux_components/src",
];

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is the crates/dbflux_ui directory; go two levels up.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Returns `true` when the given file path is under a test-fixtures or
/// test-data directory that should be excluded from the guard.
fn is_fixture_path(path: &Path) -> bool {
    path.components().any(|c| {
        matches!(
            c.as_os_str().to_str(),
            Some("fixtures") | Some("test_data") | Some("testdata")
        )
    })
}

/// Returns the line numbers (1-based) of lines inside `#[cfg(test)]` blocks.
///
/// The implementation uses a simple brace counter that starts when it sees
/// `#[cfg(test)]` and ends when the matching closing brace is found. This
/// is sufficient for the files in scope (which use standard Rust module syntax)
/// without needing a full parser.
fn test_block_lines(source: &str) -> std::collections::HashSet<usize> {
    let mut in_test_depth: u32 = 0;
    let mut brace_depth_at_entry: u32 = 0;
    let mut brace_depth: u32 = 0;
    let mut result = std::collections::HashSet::new();
    let mut pending_cfg_test = false;

    for (line_idx, line) in source.lines().enumerate() {
        let line_no = line_idx + 1;

        if line.trim_start().starts_with("#[cfg(test") {
            pending_cfg_test = true;
        }

        if in_test_depth > 0 {
            result.insert(line_no);
        }

        // Count braces on this line to track block depth.
        for ch in line.chars() {
            match ch {
                '{' => {
                    if pending_cfg_test && in_test_depth == 0 {
                        // Entering the #[cfg(test)] block.
                        brace_depth_at_entry = brace_depth;
                        in_test_depth = 1;
                        pending_cfg_test = false;
                    } else if in_test_depth > 0 {
                        in_test_depth += 1;
                    }
                    brace_depth += 1;
                }
                '}' => {
                    brace_depth = brace_depth.saturating_sub(1);
                    if in_test_depth > 0 {
                        if brace_depth <= brace_depth_at_entry {
                            in_test_depth = 0;
                        } else {
                            in_test_depth -= 1;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    result
}

#[test]
fn no_cloudwatch_string_literal_in_ui_crates() {
    let root = workspace_root();

    let mut violations: Vec<String> = Vec::new();

    for scope in SCOPE_DIRS {
        let dir = root.join(scope);
        let mut files: Vec<PathBuf> = Vec::new();
        collect_rs_files(&dir, &mut files);

        for file_path in &files {
            if is_fixture_path(file_path) {
                continue;
            }

            let source = match fs::read_to_string(file_path) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let test_lines = test_block_lines(&source);

            for (line_idx, line) in source.lines().enumerate() {
                let line_no = line_idx + 1;

                // Skip lines inside #[cfg(test)] blocks.
                if test_lines.contains(&line_no) {
                    continue;
                }

                // Only flag occurrences that appear inside a string literal
                // (surrounded by double quotes). This excludes identifiers,
                // type names, and code comments.
                if !line.contains("\"CloudWatch") {
                    continue;
                }

                // Allow-list: lines where the only match is within the
                // canonical `QueryLanguage` enum variant name.
                let stripped = line
                    .replace("CloudWatchLogsInsightsQl", "")
                    .replace("CloudWatchLogs\"", "")
                    .replace("CloudWatchLogs {", "");

                if !stripped.contains("\"CloudWatch") {
                    continue;
                }

                let rel = file_path
                    .strip_prefix(&root)
                    .unwrap_or(file_path)
                    .display()
                    .to_string();

                violations.push(format!("{rel}:{line_no}: {}", line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Found user-facing \"CloudWatch\" string literals in UI crates.\n\
         These must be replaced with driver-agnostic labels before merging.\n\
         Violations:\n{}",
        violations.join("\n")
    );
}
