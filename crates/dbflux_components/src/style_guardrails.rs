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

    /// Directory holding every workspace crate, for checks that span the UI
    /// crates.
    const CRATES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/..");

    /// UI crates whose interface text must follow the interface font size.
    const UI_CRATES: &[&str] = &[
        "dbflux_components",
        "dbflux_ui_base",
        "dbflux_ui_document",
        "dbflux_ui_sidebar",
        "dbflux_ui_windows",
        "dbflux_ui",
    ];

    /// A pixel literal text size stays fixed when the user changes the
    /// interface font size; text sizes come from `tokens::ui`, `FontSizes`,
    /// or the grid and editor accessors in `fonts`.
    const FORBIDDEN_TEXT_SIZE_PATTERNS: &[&str] = &["text_size(px("];

    /// A bundled family constant ignores the families chosen in Settings;
    /// surfaces read the active family from `fonts` (`ui_family`,
    /// `display_family`, `editor_family`, `grid_family`).
    const FORBIDDEN_FONT_FAMILY_PATTERNS: &[&str] = &[
        "font_family(AppFonts::",
        "font_family(crate::typography::AppFonts::",
        "font_family(dbflux_components::typography::AppFonts::",
        "font(AppFonts::",
    ];

    /// Files that define or resolve the bundled families.
    const FONT_FAMILY_EXEMPT: &[&str] = &["/fonts.rs", "/typography.rs"];

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
        check_violations_in(&[PathBuf::from(SRC_DIR)], forbidden_patterns, extra_exempt)
    }

    fn check_violations_in(
        roots: &[PathBuf],
        forbidden_patterns: &[&str],
        extra_exempt: &[&str],
    ) -> Vec<String> {
        let mut files = Vec::new();

        for root in roots {
            collect_rust_files(root, &mut files);
        }

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
    fn no_pixel_literal_text_sizes_in_ui_crates() {
        let roots: Vec<PathBuf> = UI_CRATES
            .iter()
            .map(|name| Path::new(CRATES_DIR).join(name).join("src"))
            .collect();

        assert!(
            roots.iter().all(|root| root.is_dir()),
            "every UI crate source directory should exist: {roots:?}"
        );

        let violations = check_violations_in(&roots, FORBIDDEN_TEXT_SIZE_PATTERNS, &[]);

        assert!(
            violations.is_empty(),
            "Found pixel literal text sizes that do not follow the interface font size:\n{}",
            violations.join("\n")
        );
    }

    #[test]
    fn no_bundled_font_families_in_ui_crates() {
        let roots: Vec<PathBuf> = UI_CRATES
            .iter()
            .map(|name| Path::new(CRATES_DIR).join(name).join("src"))
            .collect();

        let violations =
            check_violations_in(&roots, FORBIDDEN_FONT_FAMILY_PATTERNS, FONT_FAMILY_EXEMPT);

        assert!(
            violations.is_empty(),
            "Found bundled font family constants that ignore the font settings:\n{}",
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
