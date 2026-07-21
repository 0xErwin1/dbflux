/// Source-scanning guardrails for `dbflux_components`.
///
/// These tests walk all `.rs` files under `crates/dbflux_components/src/` and
/// reject bare magic literals that must be replaced by design tokens.
///
/// Exemptions are opt-in at two levels:
/// - **File-level**: files whose path contains one of the exempt path fragments
///   (token/semantic/theme definition files, and `chart/engine.rs` for canvas
///   paint geometry) are skipped entirely from both spacing and color checks.
/// - **Line-level**: any line containing `// guardrail-allow` is skipped, as is
///   any line that contains `px(0.)` or `px(0.0)` (zero is never a forbidden value).
///
/// The `style_guardrails.rs` file itself is always excluded from scanning so
/// that the forbidden pattern strings in this file do not self-trigger.
///
/// Chart factory files (`axis_bar`, `point_inspector`, `legend`) are now fully
/// under the guardrail: they use `ChartGeometry` tokens for spacing and route
/// all colour roles through `ChartColors`. Only `chart/engine.rs` stays exempt
/// (canvas paint geometry — line widths, tick lengths — are not UI spacing tokens).
#[cfg(test)]
#[allow(clippy::module_inception)]
mod style_guardrails {
    use std::fs;
    use std::path::{Path, PathBuf};

    const SRC_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

    /// Fragments that, when found in a file's path, exempt it from ALL checks
    /// (both spacing and color). These are canonical token/semantic/theme
    /// definition files where bare literals and color constructors are
    /// legitimately defined, plus `chart/engine.rs` for canvas paint geometry.
    const FILE_EXEMPT_FRAGMENTS: &[&str] = &[
        "tokens.rs",
        "semantic.rs",
        "density.rs",
        "theme.rs",
        "style_guardrails.rs",
        "/chart/engine.rs",
    ];

    /// Spacing/size literal patterns that are forbidden in component code.
    ///
    /// Each pattern uses a closing-paren suffix to prevent false positives:
    /// for example, `"px(4.0)"` does NOT match `px(14.0)` or `px(24.0)`.
    const FORBIDDEN_SPACING_PATTERNS: &[&str] = &[
        "px(4.)", "px(4.0)", "px(6.)", "px(6.0)", "px(8.)", "px(8.0)", "px(12.)", "px(12.0)",
        "px(16.)", "px(16.0)", "px(24.)", "px(24.0)",
    ];

    /// Raw color constructor patterns that are forbidden in component code.
    ///
    /// Component files must use semantic tokens or the `from_hex` helper in
    /// `semantic.rs` instead of constructing colors inline.
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

    fn is_file_exempt(path: &Path, extra_exempt: &[&str]) -> bool {
        let path_str = path.to_string_lossy();
        FILE_EXEMPT_FRAGMENTS
            .iter()
            .chain(extra_exempt.iter())
            .any(|fragment| path_str.contains(fragment))
    }

    fn is_line_exempt(line: &str) -> bool {
        line.contains("// guardrail-allow") || line.contains("px(0.)") || line.contains("px(0.0)")
    }

    fn check_violations(forbidden_patterns: &[&str], extra_exempt: &[&str]) -> Vec<String> {
        let src_root = PathBuf::from(SRC_DIR);
        let mut files = Vec::new();
        collect_rust_files(&src_root, &mut files);

        let mut violations = Vec::new();

        for file in &files {
            if is_file_exempt(file, extra_exempt) {
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
    fn no_bare_spacing_literals_in_component_code() {
        let violations = check_violations(FORBIDDEN_SPACING_PATTERNS, &[]);

        assert!(
            violations.is_empty(),
            "Found bare spacing literals that must use design tokens:\n{}",
            violations.join("\n")
        );
    }

    #[test]
    fn no_raw_color_constructors_in_component_code() {
        let violations = check_violations(FORBIDDEN_COLOR_PATTERNS, &[]);

        assert!(
            violations.is_empty(),
            "Found raw color constructors that must use semantic tokens:\n{}",
            violations.join("\n")
        );
    }
}
