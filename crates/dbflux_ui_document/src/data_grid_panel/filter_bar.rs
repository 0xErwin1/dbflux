use dbflux_core::VisualQuerySpec;

/// Tracks the current relational-filter state for the filter-bar chip and
/// inline error affordance.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RelationalFilterState {
    /// Raw filter (or empty input) — chip hidden, no inline error.
    Inactive,
    /// FK cache is loading; a subtle spinner is shown in the chip area.
    Resolving,
    /// Relational lowering succeeded; chip visible with join count.
    Active {
        join_count: usize,
        predicate_count: usize,
    },
    /// Resolve or depth error; inline diagnostic + "Open in builder" link.
    Error {
        message: String,
        partial_spec: VisualQuerySpec,
    },
}

impl Default for RelationalFilterState {
    fn default() -> Self {
        Self::Inactive
    }
}

/// Output of the cheap pre-check before invoking the full parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterMode {
    /// No unquoted dot — take the raw-filter path without parsing.
    Raw,
    /// At least one unquoted dot found — attempt relational lowering.
    Relational,
}

/// Scan `text` for an unquoted `.` in a single pass.
///
/// String literals (single- and double-quoted) are skipped atomically so that
/// `email = 'a.b@x.com'` returns `Raw` while `user.email = 'x'` returns
/// `Relational`. Does not perform full parsing; this is intentionally cheap.
pub(crate) fn classify_filter_input(text: &str) -> FilterMode {
    if text.trim().is_empty() {
        return FilterMode::Raw;
    }

    let bytes = text.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'\'' {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'"' {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'.' => {
                return FilterMode::Relational;
            }
            _ => {
                i += 1;
            }
        }
    }

    FilterMode::Raw
}

#[cfg(test)]
mod tests {
    use super::*;

    // T21: classify_filter_input

    #[test]
    fn classify_empty_input_is_raw() {
        assert_eq!(classify_filter_input(""), FilterMode::Raw);
        assert_eq!(classify_filter_input("   "), FilterMode::Raw);
    }

    #[test]
    fn classify_dot_only_inside_string_literal_is_raw() {
        assert_eq!(classify_filter_input("email = 'a.b@x.com'"), FilterMode::Raw);
        assert_eq!(classify_filter_input("email = \"a.b@x.com\""), FilterMode::Raw);
    }

    #[test]
    fn classify_unquoted_dot_is_relational() {
        assert_eq!(classify_filter_input("user.email = 'x'"), FilterMode::Relational);
        assert_eq!(classify_filter_input("created_by.organization.name = 'Acme'"), FilterMode::Relational);
    }

    #[test]
    fn classify_bare_column_filter_is_raw() {
        assert_eq!(classify_filter_input("status = 'active'"), FilterMode::Raw);
        assert_eq!(classify_filter_input("age > 30"), FilterMode::Raw);
    }

    #[test]
    fn classify_mixed_literal_and_dotted_path_is_relational() {
        // The literal dot is inside quotes, but the path dot is outside
        assert_eq!(
            classify_filter_input("email = 'a.b@x.com' AND user.role = 'admin'"),
            FilterMode::Relational
        );
    }

    #[test]
    fn classify_single_quote_escape_does_not_terminate_early() {
        // 'it''s fine' should be treated as a single string literal
        assert_eq!(
            classify_filter_input("col = 'it''s fine'"),
            FilterMode::Raw
        );
    }

    #[test]
    fn classify_double_quote_escape_does_not_terminate_early() {
        assert_eq!(
            classify_filter_input("col = \"a\"\"b\""),
            FilterMode::Raw
        );
    }
}
