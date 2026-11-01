//! Source-scanning guardrails for `bolt`.
//!
//! `bolt` is the product-agnostic design system: it must stay free of product
//! knowledge so a second product can consume it as a dependency swap. The
//! style guardrails in `dbflux_components` (`src/style_guardrails.rs`) police
//! the product crates and the interface-font policies that span them; these
//! tests police what is specific to `bolt`: its independence and its token
//! discipline.
//!
//! The tests walk every `.rs` file under `crates/bolt/src` and
//! `crates/bolt/tests` (except this file) and reject:
//!
//! - a product name or product concept: DBFlux, Relay, the byzantine accent,
//!   a database driver, a design board;
//! - a raw color constructor or color literal outside the files that
//!   legitimately define colors;
//! - a bare spacing literal outside the files that legitimately define
//!   geometry;
//! - a `dbflux_core` or `dbflux_i18n` type reference.
//!
//! Token exemptions are file-level and listed explicitly with a
//! justification, so the list is reviewable and grows deliberately:
//! `tokens.rs` holds every raw color and geometry literal today and splits
//! into a `tokens/` directory later (the entries become
//! `src/tokens/<file>.rs`). Any line containing `// guardrail-allow` is
//! skipped, as is any line containing `px(0.)` or `px(0.0)` (zero is never a
//! forbidden spacing value). The name checks have no exemptions: `bolt` names
//! no product anywhere, not even in a comment.

#[test]
fn bolt_sources_name_no_product_no_driver_no_board() {
    let violations = check_files(|_file, line, line_lower| {
        let mut hits = Vec::new();

        for (pattern, case_sensitive, reason) in FORBIDDEN_NAMES {
            let matched = if *case_sensitive {
                line.contains(pattern)
            } else {
                line_lower.contains(pattern)
            };

            if matched {
                hits.push(format!("{reason} ({pattern:?})"));
            }
        }

        if let Some(kind) = design_board_span(line) {
            hits.push(format!("a design-board name ({kind})"));
        }

        hits
    });

    assert!(
        violations.is_empty(),
        "`bolt` must name no product, driver or design board:\n{}",
        violations.join("\n")
    );
}

#[test]
fn bolt_sources_reference_no_product_core_type() {
    let violations = check_files(|file, line, _| {
        let mut hits = Vec::new();

        for pattern in FORBIDDEN_CORE_TYPES {
            if line.contains(pattern) {
                hits.push(format!(
                    "{file}: a product-core type path ({pattern:?}) — the design system depends only on gpui and its own tokens"
                ));
            }
        }

        hits
    });

    assert!(
        violations.is_empty(),
        "`bolt` must not depend on a product-core crate:\n{}",
        violations.join("\n")
    );
}

#[test]
fn bolt_sources_define_colors_only_in_token_files() {
    let violations = check_files(|file, line, _| {
        if is_exempt(file, COLOR_FILES) || is_line_exempt(line) {
            return Vec::new();
        }

        raw_color_spans(line)
            .into_iter()
            .map(|literal| {
                format!(
                    "raw color literal ({literal}) — use a token or a color role; exempt files are\n{}",
                    exemption_list(COLOR_FILES)
                )
            })
            .collect()
    });

    assert!(
        violations.is_empty(),
        "raw colors outside the token files:\n{}",
        violations.join("\n")
    );
}

#[test]
fn bolt_sources_define_geometry_only_in_token_files() {
    let violations = check_files(|file, line, _| {
        if is_exempt(file, GEOMETRY_FILES) || is_line_exempt(line) {
            return Vec::new();
        }

        FORBIDDEN_SPACING_PATTERNS
            .iter()
            .filter(|pattern| line.contains(*pattern))
            .map(|pattern| {
                format!(
                    "{pattern:?} — use a spacing token; exempt files are\n{}",
                    exemption_list(GEOMETRY_FILES)
                )
            })
            .collect()
    });

    assert!(
        violations.is_empty(),
        "bare spacing literals outside the token files:\n{}",
        violations.join("\n")
    );
}

/// The bolt sources these tests scan.
const SRC_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");
const TESTS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests");

/// Product names and product concepts, forbidden everywhere in `bolt`.
///
/// Each entry is `(pattern, case_sensitive, why it is product knowledge)`.
/// Most patterns match case-insensitively so a rewording cannot sneak a name
/// back in; `Relay` matches exactly because a lowercase "relay" is ordinary
/// English (an event relay, a relay race), while the product is always
/// capitalized.
const FORBIDDEN_NAMES: &[(&str, bool, &str)] = &[
    ("dbflux", false, "the DBFlux product name"),
    ("Relay", true, "the Relay product name"),
    (
        "byzant",
        false,
        "the byzantine accent the DBFlux brand installs",
    ),
    // Database drivers are product surface; the design system serves them all
    // through generic abstractions and never names one.
    ("sqlite", false, "a database driver name"),
    ("postgres", false, "a database driver name"),
    ("mysql", false, "a database driver name"),
    ("mariadb", false, "a database driver name"),
    ("mssql", false, "a database driver name"),
    ("mongo", false, "a database driver name"),
    ("redis", false, "a database driver name"),
    ("dynamodb", false, "a database driver name"),
    ("cloudwatch", false, "a database driver name"),
    ("influxdb", false, "a database driver name"),
    ("redshift", false, "a database driver name"),
    ("clickhouse", false, "a database driver name"),
];

/// Product-core crate paths, forbidden everywhere in `bolt`.
const FORBIDDEN_CORE_TYPES: &[&str] = &["dbflux_core", "dbflux_i18n"];

/// Spacing/size literals that are forbidden in component code.
///
/// The canonical spacing values of the design system; each pattern carries a
/// closing-paren suffix so `"px(4.0)"` does not match `px(14.0)` or `px(24.0)`.
const FORBIDDEN_SPACING_PATTERNS: &[&str] = &[
    "px(4.)", "px(4.0)", "px(6.)", "px(6.0)", "px(8.)", "px(8.0)", "px(12.)", "px(12.0)",
    "px(16.)", "px(16.0)", "px(24.)", "px(24.0)",
];

/// Files that legitimately define colors, as `(path suffix, justification)`.
///
/// A path suffix exempts the whole file from the color check.
const COLOR_FILES: &[(&str, &str)] = &[
    (
        "src/tokens.rs",
        "the raw palette tables: every color literal of the design system lives here until the file splits into a tokens/ directory",
    ),
    (
        "src/semantic.rs",
        "resolves the raw palette values into the named color roles everything else reads",
    ),
    (
        "src/theme.rs",
        "the palette engine: maps tokens, brand washes and the palette mode into the theme's colors",
    ),
    (
        "src/colors/mod.rs",
        "the neutral and semantic role tables a product reads instead of theme fields",
    ),
    (
        "src/brand.rs",
        "the neutral Brand and wash math: the default accent data a product overrides with its own",
    ),
];

/// Files that legitimately define geometry, as `(path suffix, justification)`.
///
/// A path suffix exempts the whole file from the bare-spacing check.
const GEOMETRY_FILES: &[(&str, &str)] = &[
    (
        "src/tokens.rs",
        "the geometry tables: every canonical spacing and control metric literal lives here until the file splits into a tokens/ directory",
    ),
    (
        "src/density.rs",
        "the density radii are token values themselves: the file defines geometry, it does not consume it",
    ),
    (
        "src/fonts.rs",
        "the type-scale accessors: font sizes are defined here in rems and converted through the base rem",
    ),
];

/// Whether `file`'s path ends with one of the exempt path suffixes.
///
/// Suffixes are matched against the end of the path so an exemption names
/// exactly one file and cannot silently widen to a same-named file elsewhere;
/// when `tokens.rs` splits, its entries become `src/tokens/<file>.rs`.
fn is_exempt(file: &str, exemptions: &[(&str, &str)]) -> bool {
    exemptions.iter().any(|(suffix, _)| file.ends_with(suffix))
}

/// The exemption list as a printable, reviewed block.
fn exemption_list(exemptions: &[(&str, &str)]) -> String {
    exemptions
        .iter()
        .map(|(file, why)| format!("  - {file}: {why}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The span of a design-board name in `line`, as `name (kind)` text.
///
/// Boards are numbered working canvases cited as provenance
/// (`P1Modals`, `DSStates`); a product-agnostic crate does not name them.
fn design_board_span(line: &str) -> Option<String> {
    if line.contains("DSStates") {
        return Some("DSStates".to_owned());
    }

    if line.contains("canvas v4") {
        return Some("canvas v4…".to_owned());
    }

    // `P1`/`P2` followed by an uppercase letter, as in `P1Modals`.
    for prefix in ["P1", "P2"] {
        let named = line.match_indices(prefix).find_map(|(index, _)| {
            line[index + prefix.len()..]
                .chars()
                .next()
                .filter(|next| next.is_ascii_uppercase())
                .map(|_| index)
        });

        if named.is_some() {
            return Some(format!("{prefix}<Board>"));
        }
    }

    None
}

/// Every raw color constructor or color literal in `line`, as matched text.
fn raw_color_spans(line: &str) -> Vec<String> {
    let mut literals: Vec<String> = Vec::new();

    for pattern in ["rgb(", "rgba(", "hsla(", "gpui::rgb", "gpui::hsla"] {
        if line.contains(pattern) {
            literals.push(pattern.to_owned());
        }
    }

    // A hex color: `#RRGGBB`, `0xRRGGBB` or their 8-digit alpha forms.
    for marker in ["#", "0x"] {
        let mut search = 0;

        while let Some(index) = line[search..].find(marker) {
            let digits_start = search + index + marker.len();
            let digits = &line[digits_start..]
                .chars()
                .take(8)
                .take_while(|c| c.is_ascii_hexdigit())
                .collect::<String>();

            if digits.chars().count() >= 6 {
                let end = digits_start + digits.chars().map(char::len_utf8).sum::<usize>();
                literals.push(line[digits_start - marker.len()..end].to_owned());
            }

            search = digits_start;

            if search >= line.len() {
                break;
            }
        }
    }

    literals
}

/// Whether `line` carries an opt-out comment or a zero value.
fn is_line_exempt(line: &str) -> bool {
    line.contains("// guardrail-allow") || line.contains("px(0.)") || line.contains("px(0.0)")
}

/// Runs `check` over every scanned line, returning `file: line: finding`
/// strings.
fn check_files(check: impl Fn(&str, &str, &str) -> Vec<String>) -> Vec<String> {
    let mut files = Vec::new();
    collect_rust_files(SRC_DIR, &mut files);
    collect_rust_files(TESTS_DIR, &mut files);

    let mut violations = Vec::new();

    for file in files {
        // This file quotes every forbidden pattern and would self-trigger.
        let file = file.to_string_lossy().into_owned();

        if file.ends_with("guardrail.rs") {
            continue;
        }

        let Ok(content) = std::fs::read_to_string(&file) else {
            continue;
        };

        for (number, line) in content.lines().enumerate() {
            for finding in check(&file, line, &line.to_lowercase()) {
                violations.push(format!("{file}:{}: {finding}", number + 1));
            }
        }
    }

    violations
}

fn collect_rust_files(root: &str, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            collect_rust_files(&path.to_string_lossy(), out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}
