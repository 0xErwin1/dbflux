//! Whitespace-only JSON layout: the two layouts the value editor moves a
//! JSON value between.
//!
//! Both functions touch only the insignificant whitespace between tokens.
//! Strings are copied byte for byte and numbers are never re-parsed, so
//! nothing a re-serialization would normalize — key order, `2.50`, escapes —
//! changes. They expect text that already parses as JSON; on anything else
//! the output is unspecified, and callers check validity first.

/// The text with every run of whitespace between tokens removed.
pub(crate) fn minify(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_string = false;
    let mut escaped = false;

    for ch in text.chars() {
        if in_string {
            out.push(ch);

            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }

            continue;
        }

        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            ' ' | '\t' | '\n' | '\r' => {}
            _ => out.push(ch),
        }
    }

    out
}

/// The text indented with two spaces per level, one element per line, an
/// empty container kept on one line.
pub(crate) fn pretty(text: &str) -> String {
    let compact = minify(text);
    let mut out = String::with_capacity(compact.len() * 2);
    let mut chars = compact.chars().peekable();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if in_string {
            out.push(ch);

            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }

            continue;
        }

        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '{' | '[' => {
                out.push(ch);

                if let Some(close) = chars.next_if(|next| matches!(next, '}' | ']')) {
                    out.push(close);
                } else {
                    depth += 1;
                    break_line(&mut out, depth);
                }
            }
            '}' | ']' => {
                depth = depth.saturating_sub(1);
                break_line(&mut out, depth);
                out.push(ch);
            }
            ',' => {
                out.push(ch);
                break_line(&mut out, depth);
            }
            ':' => out.push_str(": "),
            _ => out.push(ch),
        }
    }

    out
}

fn break_line(out: &mut String, depth: usize) {
    out.push('\n');
    out.extend(std::iter::repeat_n("  ", depth));
}
