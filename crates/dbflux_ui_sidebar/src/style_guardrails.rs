/// Source-scanning guardrails for `dbflux_ui_sidebar`.
///
/// These tests walk all `.rs` files under `crates/dbflux_ui_sidebar/src/` and
/// reject bare magic literals that must be replaced by design tokens.
///
/// Exemptions are opt-in at two levels:
/// - **File-level**: files whose path contains one of the exempt path fragments
///   (token/semantic/theme definition files) are skipped entirely.
/// - **Line-level**: any line containing `// guardrail-allow` is skipped, as is
///   any line that contains `px(0.)` or `px(0.0)` (zero is never a forbidden value).
///
/// The `style_guardrails.rs` file itself is always excluded from scanning so
/// that the forbidden pattern strings in this file do not self-trigger.
#[cfg(test)]
#[allow(clippy::module_inception)]
mod style_guardrails {
    use std::fs;
    use std::path::{Path, PathBuf};

    const SRC_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

    /// Fragments that, when found in a file's path, exempt it from all checks.
    ///
    /// - Token/semantic/density definition files carry legitimate raw literals.
    /// - `/chart/` covers chart canvas math and chart-only sizes.
    /// - `style_guardrails.rs` is self-excluded to prevent pattern strings here
    ///   from tripping the scan.
    const FILE_EXEMPT_FRAGMENTS: &[&str] = &[
        "tokens.rs",
        "semantic.rs",
        "density.rs",
        "/chart/",
        "style_guardrails.rs",
    ];

    /// Spacing/size literal patterns that are forbidden in sidebar code.
    ///
    /// Each pattern uses a closing-paren suffix to prevent false positives:
    /// for example, `"px(4.0)"` does NOT match `px(14.0)` or `px(24.0)`.
    const FORBIDDEN_SPACING_PATTERNS: &[&str] = &[
        "px(4.)", "px(4.0)", "px(6.)", "px(6.0)", "px(8.)", "px(8.0)", "px(12.)", "px(12.0)",
        "px(16.)", "px(16.0)", "px(24.)", "px(24.0)",
    ];

    /// Raw color constructor patterns that are forbidden in sidebar code.
    ///
    /// Sidebar files must use semantic tokens instead of constructing colors
    /// inline. Exceptions require a `// guardrail-allow` comment with a reason.
    const FORBIDDEN_COLOR_PATTERNS: &[&str] =
        &["rgb(", "rgba(", "hsla(", "gpui::rgb", "gpui::hsla"];

    fn collect_rust_files(root: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(root) else {
            return;
        };

        for entry in entries.flatten() {
            let path = entry.path();

            if path.is_dir() {
                collect_rust_files(&path, out);
                continue;
            }

            if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    fn is_file_exempt(path: &Path) -> bool {
        let path_str = path.to_string_lossy();
        FILE_EXEMPT_FRAGMENTS
            .iter()
            .any(|fragment| path_str.contains(fragment))
    }

    fn is_line_exempt(line: &str) -> bool {
        line.contains("// guardrail-allow") || line.contains("px(0.)") || line.contains("px(0.0)")
    }

    fn check_violations(forbidden_patterns: &[&str]) -> Vec<String> {
        let src_root = PathBuf::from(SRC_DIR);
        let mut files = Vec::new();
        collect_rust_files(&src_root, &mut files);

        let mut violations = Vec::new();

        for file in &files {
            if is_file_exempt(file) {
                continue;
            }

            let Ok(content) = fs::read_to_string(file) else {
                continue;
            };

            for (line_number, line) in content.lines().enumerate() {
                if is_line_exempt(line) {
                    continue;
                }

                for pattern in forbidden_patterns {
                    if line.contains(pattern) {
                        violations.push(format!(
                            "{}:{}: found forbidden pattern {:?} — use a design token or add `// guardrail-allow` with a justification comment",
                            file.display(),
                            line_number + 1,
                            pattern
                        ));
                        // Report each line once, even if multiple patterns match.
                        break;
                    }
                }
            }
        }

        violations
    }

    #[test]
    fn no_bare_spacing_literals_in_sidebar_code() {
        let violations = check_violations(FORBIDDEN_SPACING_PATTERNS);

        assert!(
            violations.is_empty(),
            "Found bare spacing literals that must use design tokens:\n{}",
            violations.join("\n")
        );
    }

    #[test]
    fn no_raw_color_constructors_in_sidebar_code() {
        let violations = check_violations(FORBIDDEN_COLOR_PATTERNS);

        assert!(
            violations.is_empty(),
            "Found raw color constructors that must use semantic tokens:\n{}",
            violations.join("\n")
        );
    }

    // ---- T19.2: Decoupling guardrail ----

    /// T19.2: Sidebar code must not branch on driver IDs or specific `DatabaseCategory`
    /// values in conditional expressions.
    ///
    /// Allowlist: file-level `FILE_EXEMPT_FRAGMENTS` still applies. Lines containing
    /// `// guardrail-allow` are permitted. Construction of `DatabaseCategory::X` values
    /// (i.e. in match arms that return them) is allowed; only comparison / conditional
    /// branching is targeted.
    ///
    /// Patterns checked (heuristic; not an AST analysis):
    /// - `driver_id ==` or `driver_id !=` — string comparison on driver ID
    /// - `match driver_id` — branching on driver ID string
    #[test]
    fn sidebar_has_no_driver_id_branching() {
        let forbidden: &[&str] = &["driver_id ==", "driver_id !=", "match driver_id"];
        let violations = check_violations(forbidden);
        assert!(
            violations.is_empty(),
            "Sidebar code must not branch on driver_id strings (use DriverCapabilities or DatabaseCategory instead):\n{}",
            violations.join("\n")
        );
    }
}
