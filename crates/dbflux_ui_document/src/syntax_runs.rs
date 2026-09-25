//! Lightweight highlight runs for read-only JSON blocks (an audit event's
//! details, the payload an approval will run), colored with the palette's
//! syntax roles. These blocks are static text, so a small scanner is enough;
//! the editor keeps its tree-sitter highlighter.

use std::ops::Range;

use dbflux_components::tokens::SyntaxColors;
use gpui::{HighlightStyle, Hsla};

fn style(color: Hsla) -> HighlightStyle {
    HighlightStyle {
        color: Some(color),
        ..Default::default()
    }
}

/// Highlight runs for JSON text: object keys in the type color, string
/// values in the string color, numbers and literals in the number color.
pub(crate) fn json_highlights(
    text: &str,
    colors: &SyntaxColors,
) -> Vec<(Range<usize>, HighlightStyle)> {
    let bytes = text.as_bytes();
    let mut runs = Vec::new();
    let mut index = 0;

    while index < bytes.len() {
        let byte = bytes[index];

        if byte == b'"' {
            let start = index;
            index += 1;

            while index < bytes.len() && bytes[index] != b'"' {
                if bytes[index] == b'\\' {
                    index += 1;
                }
                index += 1;
            }

            index = (index + 1).min(bytes.len());

            let next_significant = bytes[index..]
                .iter()
                .find(|byte| !byte.is_ascii_whitespace());
            let color = if next_significant == Some(&b':') {
                colors.type_name
            } else {
                colors.string
            };

            runs.push((start..index, style(color)));
            continue;
        }

        if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            index += 1;

            while index < bytes.len()
                && (bytes[index].is_ascii_digit()
                    || matches!(bytes[index], b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                index += 1;
            }

            runs.push((start..index, style(colors.number)));
            continue;
        }

        if byte.is_ascii_alphabetic() {
            let start = index;

            while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
                index += 1;
            }

            if matches!(&text[start..index], "true" | "false" | "null") {
                runs.push((start..index, style(colors.number)));
            }
            continue;
        }

        index += 1;
    }

    runs
}

#[cfg(test)]
mod tests {
    use super::json_highlights;
    use dbflux_components::tokens::SyntaxColors;

    #[test]
    fn json_keys_and_values_take_different_colors() {
        let colors = SyntaxColors::dark();
        let text = r#"{ "table": "orders", "rows": 12, "ok": true }"#;
        let runs = json_highlights(text, &colors);

        let colored: Vec<(&str, _)> = runs
            .iter()
            .map(|(range, style)| (&text[range.clone()], style.color))
            .collect();

        assert_eq!(
            colored,
            vec![
                ("\"table\"", Some(colors.type_name)),
                ("\"orders\"", Some(colors.string)),
                ("\"rows\"", Some(colors.type_name)),
                ("12", Some(colors.number)),
                ("\"ok\"", Some(colors.type_name)),
                ("true", Some(colors.number)),
            ]
        );
    }

    #[test]
    fn json_escaped_quotes_stay_inside_their_string() {
        let colors = SyntaxColors::dark();
        let text = r#"{"a": "say \"hi\""}"#;
        let runs = json_highlights(text, &colors);

        assert_eq!(&text[runs[1].0.clone()], r#""say \"hi\"""#);
    }
}
