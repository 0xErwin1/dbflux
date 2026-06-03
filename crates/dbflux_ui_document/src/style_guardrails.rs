/// Source-scanning guardrails for `dbflux_ui_document`.
///
/// These tests walk all `.rs` files under `crates/dbflux_ui_document/src/` and
/// reject bare magic literals that must be replaced by design tokens.
///
/// Exemptions are opt-in at two levels:
/// - **File-level**: files whose path contains one of the exempt path fragments
///   (token/semantic/theme definition files, chart canvas code, and the Redis
///   key-value color parser) are skipped entirely.
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
    /// - `/chart/` covers chart canvas math and chart-only sizes (`px(11.)`, etc.).
    /// - `key_value/parsing.rs` contains intentional inline color parsing for
    ///   Redis / key-value display; those colors are domain-correct by design.
    /// - `style_guardrails.rs` is self-excluded to prevent pattern strings here
    ///   from tripping the scan.
    const FILE_EXEMPT_FRAGMENTS: &[&str] = &[
        "tokens.rs",
        "semantic.rs",
        "density.rs",
        "/chart/",
        "key_value/parsing.rs",
        "style_guardrails.rs",
    ];

    /// Spacing/size literal patterns that are forbidden in document code.
    ///
    /// Each pattern uses a closing-paren suffix to prevent false positives:
    /// for example, `"px(4.0)"` does NOT match `px(14.0)` or `px(24.0)`.
    const FORBIDDEN_SPACING_PATTERNS: &[&str] = &[
        "px(4.)", "px(4.0)", "px(6.)", "px(6.0)", "px(8.)", "px(8.0)", "px(12.)", "px(12.0)",
        "px(16.)", "px(16.0)", "px(24.)", "px(24.0)",
    ];

    /// Raw color constructor patterns that are forbidden in document code.
    ///
    /// Document files must use semantic tokens instead of constructing colors
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
    fn no_bare_spacing_literals_in_document_code() {
        let violations = check_violations(FORBIDDEN_SPACING_PATTERNS);

        assert!(
            violations.is_empty(),
            "Found bare spacing literals that must use design tokens:\n{}",
            violations.join("\n")
        );
    }

    #[test]
    fn no_raw_color_constructors_in_document_code() {
        let violations = check_violations(FORBIDDEN_COLOR_PATTERNS);

        assert!(
            violations.is_empty(),
            "Found raw color constructors that must use semantic tokens:\n{}",
            violations.join("\n")
        );
    }

    /// Document code must not branch on driver IDs. Use `DriverCapabilities` or
    /// `DatabaseCategory` instead.
    ///
    /// Heuristic patterns checked:
    /// - `driver_id ==` or `driver_id !=` — string equality on driver ID
    /// - `match driver_id` — match arm switching on driver ID string
    #[test]
    fn document_has_no_driver_id_branching() {
        let forbidden: &[&str] = &["driver_id ==", "driver_id !=", "match driver_id"];
        let violations = check_violations(forbidden);
        assert!(
            violations.is_empty(),
            "Document code must not branch on driver_id strings (use DriverCapabilities or DatabaseCategory instead):\n{}",
            violations.join("\n")
        );
    }

    /// Mutation code must not hard-code driver-specific string literals.
    ///
    /// Scans files under `data_grid_panel/mutation_*` and
    /// `query_builder/mutation_state.rs` / `sections/assignments.rs` /
    /// `sections/execution.rs` for driver-id string literals like `"postgres"`,
    /// `"mysql"`, `"sqlite"`, `"mssql"`.
    ///
    /// These must never appear because mutation logic adapts through
    /// `DriverCapabilities`, `QueryLanguage`, and `MutationPolicy` only.
    /// (Spec scenario: I-3, DR-13.2)
    #[test]
    fn mutation_code_has_no_driver_id_literals() {
        let mutation_path_fragments: &[&str] = &[
            "mutation_executor.rs",
            "mutation_confirm.rs",
            "mutation_state.rs",
            "sections/assignments.rs",
            "sections/execution.rs",
        ];

        let driver_id_literals: &[&str] = &["\"postgres\"", "\"mysql\"", "\"sqlite\"", "\"mssql\""];

        let src_root = PathBuf::from(SRC_DIR);
        let mut files = Vec::new();
        collect_rust_files(&src_root, &mut files);

        let mut violations = Vec::new();

        for file in &files {
            let path_str = file.to_string_lossy();
            let is_mutation_file = mutation_path_fragments
                .iter()
                .any(|frag| path_str.contains(frag));
            if !is_mutation_file {
                continue;
            }

            let Ok(content) = fs::read_to_string(file) else {
                continue;
            };

            for (line_number, line) in content.lines().enumerate() {
                if is_line_exempt(line) {
                    continue;
                }
                for literal in driver_id_literals {
                    if line.contains(literal) {
                        violations.push(format!(
                            "{}:{}: found driver-id literal {} in mutation code — adapt via DriverCapabilities or QueryLanguage instead",
                            file.display(),
                            line_number + 1,
                            literal
                        ));
                        break;
                    }
                }
            }
        }

        assert!(
            violations.is_empty(),
            "Mutation code must not contain driver-id string literals (I-3, DR-13.2):\n{}",
            violations.join("\n")
        );
    }
}
