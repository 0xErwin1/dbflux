//! The editor's SQL formatter: `sqlformat` with DBFlux's rules, plus guards
//! against tokenizer defects that would change the statement.

use std::borrow::Cow;

/// The SQL dialect [`format_sql`] tokenizes for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlFormatDialect {
    /// Generic SQL, also used for PostgreSQL: `<@`, `@>`, `@-@` and `@@` are
    /// operators, so a `@` is never taken for a variable.
    Generic,
    /// MySQL and MariaDB: `@user` and `@@system` variables, `#` comments.
    MySql,
    /// T-SQL: `[bracketed identifiers]`, `@local` and `@@global` variables.
    SqlServer,
}

impl SqlFormatDialect {
    fn has_variables(self) -> bool {
        matches!(self, Self::MySql | Self::SqlServer)
    }
}

/// Reformats SQL with the editor's formatting rules: two-space indent and
/// uppercase keywords. Shared by the Format command and the "Format
/// statement" code action so both produce the same text.
pub fn format_sql(text: &str, dialect: SqlFormatDialect) -> String {
    let options = sqlformat::FormatOptions {
        indent: sqlformat::Indent::Spaces(2),
        uppercase: Some(true),
        dialect: match dialect {
            SqlFormatDialect::Generic | SqlFormatDialect::MySql => sqlformat::Dialect::Generic,
            SqlFormatDialect::SqlServer => sqlformat::Dialect::SQLServer,
        },
        ..sqlformat::FormatOptions::default()
    };

    // Text the guards cannot read safely is not formatted at all: sqlformat
    // would split its variables and parameters.
    let Some(prepared) = prepare(text, dialect) else {
        return text.to_string();
    };
    let formatted = sqlformat::format(&prepared.text, &sqlformat::QueryParams::None, &options);
    prepared.restore(formatted)
}

/// [`format_sql`] for a language service's `format`: `None` on blank text.
pub fn format_sql_text(text: &str, dialect: SqlFormatDialect) -> Option<String> {
    if text.trim().is_empty() {
        return None;
    }

    Some(format_sql(text, dialect))
}

/// Characters `sqlformat` 0.5 merges with a following `@` or `:` into one
/// operator.
const OPERATOR_CHARS: &[char] = &[
    '=', '<', '>', '!', '|', '-', '*', '/', '%', '^', '&', '~', '+',
];

/// Prefix of the identifiers that stand in for variables while `sqlformat`
/// runs. MySQL or T-SQL text that already contains it is not formatted.
const VARIABLE_STAND_IN: &str = "__dbflux_var_";

/// Text ready for `sqlformat`, with the variables it replaced.
pub(crate) struct Prepared<'a> {
    pub(crate) text: Cow<'a, str>,
    variables: Vec<String>,
}

impl Prepared<'_> {
    fn restore(&self, formatted: String) -> String {
        self.variables
            .iter()
            .enumerate()
            .fold(formatted, |formatted, (index, variable)| {
                formatted.replace(&stand_in(index), variable)
            })
    }
}

fn stand_in(index: usize) -> String {
    format!("{VARIABLE_STAND_IN}{index}__")
}

/// Works around two `sqlformat` 0.5 tokenizer defects that change the
/// statement:
///
/// - An operator glued to a following `@` or `:` is read as one operator and
///   the sigil is split from its name: `id=@id` becomes `id =@ id` and `a=:b`
///   becomes `a =: b`. A space after the operator keeps the name whole.
/// - In MySQL and T-SQL, `@@rowcount` becomes `@@ rowcount`. Those dialects
///   replace each `@name` / `@@name` variable with a plain identifier while
///   `sqlformat` runs, and [`Prepared::restore`] puts it back.
///
/// Only code is touched: quoted strings and identifiers, dollar-quoted
/// bodies and comments are copied as they are. In the generic dialect, which
/// PostgreSQL uses, `<@` and the `@` of `@-@` stay joined because they are
/// operators, and `::` is a cast, not a sigil.
///
/// Returns `None` when the text cannot be prepared safely, and the caller
/// must leave it unformatted: a backslash inside a quoted string (where that
/// string ends depends on the server's escape settings), or MySQL or T-SQL
/// text that already holds the stand-in prefix.
pub(crate) fn prepare(text: &str, dialect: SqlFormatDialect) -> Option<Prepared<'_>> {
    let use_stand_ins = dialect.has_variables();
    if use_stand_ins && text.contains(VARIABLE_STAND_IN) {
        return None;
    }

    let mut output = String::with_capacity(text.len() + 8);
    let mut variables = Vec::new();
    let mut changed = false;
    let mut previous: Option<char> = None;
    let mut index = 0;

    while let Some(current) = text[index..].chars().next() {
        let rest = &text[index..];

        if let Some(skipped) = skipped_span(rest, dialect) {
            let span = skipped?;
            output.push_str(&rest[..span]);
            index += span;
            previous = rest[..span].chars().next_back();
            continue;
        }

        if use_stand_ins && current == '@' {
            let length = variable_length(rest);
            if length > 0 {
                output.push_str(&stand_in(variables.len()));
                variables.push(rest[..length].to_string());
                index += length;
                previous = Some('_');
                changed = true;
                continue;
            }
        }

        output.push(current);
        index += current.len_utf8();

        let mut following = text[index..].chars();
        let glued_sigil = match following.next() {
            Some('@') if dialect.has_variables() => true,
            // `<@` and `@-@` are PostgreSQL operators of their own.
            Some('@') => current != '<' && previous != Some('@'),
            Some(':') => following.next() != Some(':'),
            _ => false,
        };

        if OPERATOR_CHARS.contains(&current) && glued_sigil {
            output.push(' ');
            changed = true;
        }
        previous = Some(current);
    }

    if !changed {
        return Some(Prepared {
            text: Cow::Borrowed(text),
            variables: Vec::new(),
        });
    }

    Some(Prepared {
        text: Cow::Owned(output),
        variables,
    })
}

/// Byte length of the `@name` or `@@name` variable that starts `rest`, with
/// MySQL's dotted `@@session.sql_mode` form, or `0` when the `@` names
/// nothing.
fn variable_length(rest: &str) -> usize {
    let sigil = if rest.starts_with("@@") { 2 } else { 1 };
    let name = rest[sigil..]
        .find(|character: char| {
            !(character.is_alphanumeric() || matches!(character, '_' | '$' | '.'))
        })
        .unwrap_or(rest.len() - sigil);
    let name = rest[sigil..sigil + name].trim_end_matches('.').len();

    if name == 0 { 0 } else { sigil + name }
}

/// The byte length of the quoted string, quoted identifier, dollar-quoted
/// body or comment that starts `rest`, or `None` when `rest` starts with
/// code. `Some(None)` means the text cannot be rewritten safely.
fn skipped_span(rest: &str, dialect: SqlFormatDialect) -> Option<Option<usize>> {
    let line_comment =
        rest.starts_with("--") || (dialect == SqlFormatDialect::MySql && rest.starts_with('#'));
    if line_comment {
        return Some(Some(rest.find('\n').unwrap_or(rest.len())));
    }

    if rest.starts_with("/*") {
        return Some(Some(rest.find("*/").map_or(rest.len(), |end| end + 2)));
    }

    let first = rest.chars().next()?;
    match first {
        '\'' | '"' | '`' => Some(quoted_span(rest, first)),
        // `[name]` is a T-SQL identifier; elsewhere `[` opens an array
        // subscript or constructor, which is code.
        '[' if dialect == SqlFormatDialect::SqlServer => {
            Some(Some(rest.find(']').map_or(rest.len(), |end| end + 1)))
        }
        '$' => dollar_quoted_span(rest).map(Some),
        _ => None,
    }
}

/// Length of a string quoted by `quote`, where a doubled quote is an
/// escaped one. `None` on a backslash inside a string literal, whose end
/// then depends on the server.
fn quoted_span(rest: &str, quote: char) -> Option<usize> {
    let mut chars = rest.char_indices().skip(1).peekable();

    while let Some((index, current)) = chars.next() {
        if current == '\\' && quote != '`' {
            return None;
        }

        if current == quote {
            if chars.peek().is_some_and(|(_, next)| *next == quote) {
                chars.next();
                continue;
            }
            return Some(index + 1);
        }
    }

    Some(rest.len())
}

/// Length of a PostgreSQL dollar-quoted body (`$$ … $$`, `$tag$ … $tag$`),
/// or `None` when the `$` starts a positional parameter such as `$1`.
fn dollar_quoted_span(rest: &str) -> Option<usize> {
    let tag_end = rest[1..]
        .find(|character: char| !(character.is_alphanumeric() || character == '_'))
        .map(|offset| offset + 1)?;

    if !rest[tag_end..].starts_with('$') {
        return None;
    }

    if rest[1..tag_end].starts_with(|character: char| character.is_ascii_digit()) {
        return None;
    }

    let tag = &rest[..=tag_end];
    let body_start = tag.len();
    Some(
        rest[body_start..]
            .find(tag)
            .map_or(rest.len(), |end| body_start + end + tag.len()),
    )
}

#[cfg(test)]
mod tests;
