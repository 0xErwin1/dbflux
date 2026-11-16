//! Client-only commands (`DELIMITER`, `GO`) that [`QueryLanguage::split_statements`]
//! does not honor: it splits on `;` only. Run as statements they would reach
//! the server as SQL errors, or worse, join the statements around them, so a
//! script that has one is refused instead of split wrongly.
//!
//! [`QueryLanguage::split_statements`]: dbflux_core::QueryLanguage::split_statements

/// A client-only command found in a script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Directive {
    pub name: &'static str,
    /// 1-based line.
    pub line: usize,
}

/// Where the scanner is inside the text.
enum Scan {
    Code,
    SingleQuote,
    DoubleQuote,
    Backtick,
    BlockComment(usize),
    Dollar(String),
}

/// The first directive in `text`: a `DELIMITER <token>` line, or a `GO`
/// line (optionally followed by a repeat count), on a line of its own and
/// outside quotes, comments and dollar-quoted bodies. Case-insensitive.
pub(super) fn find_directive(text: &str) -> Option<Directive> {
    let mut scan = Scan::Code;

    for (position, line) in text.lines().enumerate() {
        if matches!(scan, Scan::Code)
            && let Some(name) = directive_name(line)
        {
            return Some(Directive {
                name,
                line: position + 1,
            });
        }

        scan = advance(scan, line);
    }

    None
}

fn directive_name(line: &str) -> Option<&'static str> {
    let mut words = line.split_whitespace();
    let first = words.next()?;

    if first.eq_ignore_ascii_case("go") {
        let rest: Vec<&str> = words.collect();
        let counted = match rest.as_slice() {
            [] => true,
            [count] => count.chars().all(|c| c.is_ascii_digit()),
            _ => false,
        };
        return counted.then_some("GO");
    }

    if first.eq_ignore_ascii_case("delimiter") {
        let argument = words.next()?;
        let continues_sql = argument.starts_with(['=', ',', '(', ')', '.']);
        return (!continues_sql).then_some("DELIMITER");
    }

    None
}

/// Moves the scan state across one line.
fn advance(mut scan: Scan, line: &str) -> Scan {
    let characters: Vec<char> = line.chars().collect();
    let mut position = 0;

    while let Some(&current) = characters.get(position) {
        let next = characters.get(position + 1).copied();
        position += 1;

        scan = match scan {
            Scan::Code => match (current, next) {
                ('-', Some('-')) => return Scan::Code,
                ('/', Some('*')) => {
                    position += 1;
                    Scan::BlockComment(1)
                }
                ('\'', _) => Scan::SingleQuote,
                ('"', _) => Scan::DoubleQuote,
                ('`', _) => Scan::Backtick,
                ('$', _) => match dollar_tag(&characters, position) {
                    Some((tag, consumed)) => {
                        position += consumed;
                        Scan::Dollar(tag)
                    }
                    None => Scan::Code,
                },
                _ => Scan::Code,
            },
            Scan::SingleQuote if current == '\'' => Scan::Code,
            Scan::DoubleQuote if current == '"' => Scan::Code,
            Scan::Backtick if current == '`' => Scan::Code,
            Scan::BlockComment(depth) => match (current, next) {
                ('/', Some('*')) => {
                    position += 1;
                    Scan::BlockComment(depth + 1)
                }
                ('*', Some('/')) => {
                    position += 1;
                    match depth {
                        1 => Scan::Code,
                        _ => Scan::BlockComment(depth - 1),
                    }
                }
                _ => Scan::BlockComment(depth),
            },
            Scan::Dollar(tag) => {
                if current == '$' && closes_dollar(&characters, position, &tag) {
                    position += tag.chars().count() + 1;
                    Scan::Code
                } else {
                    Scan::Dollar(tag)
                }
            }
            unchanged => unchanged,
        };
    }

    scan
}

/// The tag of a dollar-quote opening just after the `$` at `position - 1`,
/// and how many characters after that `$` it spans (the tag and the closing
/// `$`). `None` for `$1` style parameters and a lone `$`.
fn dollar_tag(characters: &[char], position: usize) -> Option<(String, usize)> {
    let mut tag = String::new();
    let mut offset = position;

    loop {
        match characters.get(offset)? {
            '$' => return Some((tag, offset + 1 - position)),
            c if c.is_alphanumeric() || *c == '_' => {
                if tag.is_empty() && c.is_ascii_digit() {
                    return None;
                }
                tag.push(*c);
                offset += 1;
            }
            _ => return None,
        }
    }
}

/// Whether `tag` followed by `$` starts at `position` (just after a `$`).
fn closes_dollar(characters: &[char], position: usize, tag: &str) -> bool {
    let mut offset = position;

    for expected in tag.chars() {
        if characters.get(offset) != Some(&expected) {
            return false;
        }
        offset += 1;
    }

    characters.get(offset) == Some(&'$')
}
