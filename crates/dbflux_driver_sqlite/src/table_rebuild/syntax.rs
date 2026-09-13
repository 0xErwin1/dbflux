use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CreateTable {
    source: String,
    columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Column {
    name: String,
    type_span: Option<Range<usize>>,
    not_null_span: Option<Range<usize>>,
    default_span: Option<Range<usize>>,
    default_atom_span: Option<Range<usize>>,
    first_clause_start: usize,
    definition_end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DefaultChange {
    Drop,
    Set(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ColumnChange {
    pub name: String,
    pub new_type: Option<String>,
    pub nullable: Option<bool>,
    pub default: Option<DefaultChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TokenKind {
    Word,
    QuotedIdentifier,
    String,
    Symbol(char),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    span: Range<usize>,
}

impl Token {
    fn text<'a>(&self, source: &'a str) -> &'a str {
        &source[self.span.clone()]
    }

    fn is_word(&self, source: &str, keyword: &str) -> bool {
        matches!(self.kind, TokenKind::Word) && self.text(source).eq_ignore_ascii_case(keyword)
    }

    fn symbol(&self, symbol: char) -> bool {
        self.kind == TokenKind::Symbol(symbol)
    }
}

pub(super) fn parse_create_table(source: &str) -> Result<CreateTable, String> {
    let tokens = tokenize(source)?;
    let mut cursor = Cursor::new(source, &tokens);

    cursor.keyword("CREATE")?;
    if cursor.peek_keyword("TEMP")
        || cursor.peek_keyword("TEMPORARY")
        || cursor.peek_keyword("VIRTUAL")
    {
        return Err("rebuild supports ordinary CREATE TABLE only".to_string());
    }
    cursor.keyword("TABLE")?;
    if cursor.peek_keyword("IF") {
        cursor.keyword("IF")?;
        cursor.keyword("NOT")?;
        cursor.keyword("EXISTS")?;
    }

    let first_name = cursor.identifier()?;
    if cursor.symbol('.') {
        if !first_name.eq_ignore_ascii_case("main") {
            return Err("rebuild supports the main schema only".to_string());
        }
        cursor.next();
        cursor.identifier()?;
    }
    cursor.punctuation('(')?;
    let body_start = cursor.index;
    let body_end = find_matching_parenthesis(&tokens, body_start)?;
    let elements = split_elements(&tokens[body_start..body_end])?;
    if elements.is_empty() {
        return Err("CREATE TABLE must retain at least one column".to_string());
    }

    let mut columns = Vec::new();
    for element in elements {
        let element_tokens = &tokens[body_start + element.start..body_start + element.end];
        if element_tokens.is_empty() {
            return Err("CREATE TABLE contains an empty definition".to_string());
        }
        if starts_table_constraint(source, element_tokens) {
            validate_table_constraint(source, element_tokens)?;
        } else {
            columns.push(parse_column(source, element_tokens)?);
        }
    }

    if columns.is_empty() {
        return Err("CREATE TABLE must retain at least one stored column".to_string());
    }
    let mut names = columns
        .iter()
        .map(|column| column.name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    names.sort();
    if names.windows(2).any(|names| names[0] == names[1]) {
        return Err("CREATE TABLE contains duplicate column names".to_string());
    }

    cursor.index = body_end + 1;
    if cursor.symbol(';') {
        cursor.next();
    }
    if !cursor.is_finished() {
        return Err("CREATE TABLE contains an unsupported suffix".to_string());
    }

    Ok(CreateTable {
        source: source.to_string(),
        columns,
    })
}

impl CreateTable {
    pub(super) fn rewrite(&self, changes: &[ColumnChange]) -> Result<String, String> {
        if changes.is_empty() {
            return Err("rebuild requires at least one selected column change".to_string());
        }

        let mut replacements = Vec::new();
        for change in changes {
            let column = self
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&change.name))
                .ok_or_else(|| {
                    format!("selected column {:?} is not in CREATE TABLE", change.name)
                })?;

            if let Some(new_type) = &change.new_type {
                validate_type_fragment(new_type)?;
                let range = column.type_span.clone().ok_or_else(|| {
                    format!("column {:?} has no replaceable declared type", change.name)
                })?;
                replacements.push((range, new_type.clone()));
            }

            match change.nullable {
                Some(true) => {
                    if let Some(range) = &column.not_null_span {
                        replacements.push((
                            expand_leading_space(&self.source, range.clone()),
                            String::new(),
                        ));
                    }
                }
                Some(false) if column.not_null_span.is_none() => {
                    replacements.push((
                        column.first_clause_start..column.first_clause_start,
                        if column.first_clause_start == column.definition_end {
                            " NOT NULL".to_string()
                        } else {
                            "NOT NULL ".to_string()
                        },
                    ));
                }
                _ => {}
            }

            match &change.default {
                Some(DefaultChange::Drop) => {
                    let range = column.default_span.clone().ok_or_else(|| {
                        format!("column {:?} has no default clause to remove", change.name)
                    })?;
                    replacements.push((expand_leading_space(&self.source, range), String::new()));
                }
                Some(DefaultChange::Set(value)) => {
                    validate_default_atom(value)?;
                    if let Some(range) = &column.default_atom_span {
                        replacements.push((range.clone(), value.clone()));
                    } else {
                        replacements.push((
                            column.definition_end..column.definition_end,
                            format!(" DEFAULT {value}"),
                        ));
                    }
                }
                None => {}
            }
        }

        replacements.sort_by_key(|(range, _)| range.start);
        for pair in replacements.windows(2) {
            if pair[0].0.end > pair[1].0.start {
                return Err(
                    "selected changes overlap in the bounded CREATE TABLE source".to_string(),
                );
            }
        }

        let mut rewritten = self.source.clone();
        for (range, replacement) in replacements.into_iter().rev() {
            rewritten.replace_range(range, &replacement);
        }
        let reparsed = parse_create_table(&rewritten)?;
        if reparsed.columns.len() != self.columns.len() {
            return Err("selected change would alter the table column count".to_string());
        }
        Ok(rewritten)
    }
}

fn parse_column(source: &str, tokens: &[Token]) -> Result<Column, String> {
    let mut cursor = Cursor::new(source, tokens);
    let name = cursor.identifier()?;
    let name_end = cursor.previous_end();
    let mut type_span = None;
    let mut not_null_span = None;
    let mut default_span = None;
    let mut default_atom_span = None;
    let mut first_clause_start = name_end;
    let mut saw_clause = false;

    if !cursor.is_finished() && !is_clause_start(source, cursor.peek()) {
        let start = cursor
            .peek()
            .ok_or_else(|| "declared type requires a token".to_string())?
            .span
            .start;
        parse_type(&mut cursor)?;
        type_span = Some(start..cursor.previous_end());
        first_clause_start = cursor
            .peek()
            .map_or(cursor.previous_end(), |token| token.span.start);
    }

    while !cursor.is_finished() {
        let clause_start = cursor
            .peek()
            .ok_or_else(|| "column clause requires a token".to_string())?
            .span
            .start;
        let named_start = if cursor.peek_keyword("CONSTRAINT") {
            cursor.keyword("CONSTRAINT")?;
            cursor.identifier()?;
            clause_start
        } else {
            clause_start
        };
        if !saw_clause {
            first_clause_start = named_start;
            saw_clause = true;
        }

        if cursor.peek_keyword("NOT") {
            if not_null_span.is_some() {
                return Err("column contains repeated NOT NULL clauses".to_string());
            }
            cursor.keyword("NOT")?;
            cursor.keyword("NULL")?;
            not_null_span = Some(named_start..cursor.previous_end());
        } else if cursor.peek_keyword("NULL") {
            return Err(
                "explicit NULL clauses are outside the bounded rebuild grammar".to_string(),
            );
        } else if cursor.peek_keyword("DEFAULT") {
            if default_span.is_some() {
                return Err("column contains repeated DEFAULT clauses".to_string());
            }
            cursor.keyword("DEFAULT")?;
            let atom_start = cursor
                .peek()
                .ok_or_else(|| "DEFAULT requires an atom".to_string())?
                .span
                .start;
            parse_default_atom(&mut cursor)?;
            default_atom_span = Some(atom_start..cursor.previous_end());
            default_span = Some(named_start..cursor.previous_end());
        } else if cursor.peek_keyword("COLLATE") {
            cursor.keyword("COLLATE")?;
            let collation = cursor.identifier()?;
            if !matches!(
                collation.to_ascii_uppercase().as_str(),
                "BINARY" | "NOCASE" | "RTRIM"
            ) {
                return Err(
                    "rebuild supports only BINARY, NOCASE, and RTRIM collations".to_string()
                );
            }
        } else if cursor.peek_keyword("PRIMARY") {
            cursor.keyword("PRIMARY")?;
            cursor.keyword("KEY")?;
            if cursor.peek_keyword("DESC") {
                return Err("PRIMARY KEY DESC is outside the bounded rebuild grammar".to_string());
            }
            if cursor.peek_keyword("ASC") {
                cursor.next();
            }
        } else if cursor.peek_keyword("UNIQUE") {
            cursor.next();
        } else if cursor.peek_keyword("REFERENCES") {
            parse_references(&mut cursor)?;
        } else {
            return Err(format!(
                "unsupported column clause {:?}",
                cursor
                    .peek()
                    .ok_or_else(|| "column clause requires a token".to_string())?
                    .text(source)
            ));
        }
    }

    let definition_end = tokens.last().map_or(name_end, |token| token.span.end);
    Ok(Column {
        name,
        type_span,
        not_null_span,
        default_span,
        default_atom_span,
        first_clause_start: if saw_clause {
            first_clause_start
        } else {
            definition_end
        },
        definition_end,
    })
}

fn parse_type(cursor: &mut Cursor<'_>) -> Result<(), String> {
    let mut words = 0;
    while !cursor.is_finished()
        && !cursor.symbol('(')
        && !is_clause_start(cursor.source, cursor.peek())
    {
        let token = cursor
            .peek()
            .ok_or_else(|| "declared type requires identifier words".to_string())?;
        if matches!(token.kind, TokenKind::QuotedIdentifier | TokenKind::String) {
            return Err(
                "quoted declared types are outside the bounded rebuild grammar".to_string(),
            );
        }
        cursor.identifier()?;
        words += 1;
    }
    if words == 0 {
        return Err("declared type requires identifier words".to_string());
    }
    if cursor.symbol('(') {
        cursor.next();
        unsigned_integer(cursor)?;
        if cursor.symbol(',') {
            cursor.next();
            unsigned_integer(cursor)?;
        }
        cursor.punctuation(')')?;
    }
    Ok(())
}

fn validate_type_fragment(value: &str) -> Result<(), String> {
    let tokens = tokenize(value)?;
    if tokens.is_empty() {
        return Err("clearing a declared type is outside the bounded rebuild grammar".to_string());
    }
    let mut cursor = Cursor::new(value, &tokens);
    parse_type(&mut cursor)?;
    if !cursor.is_finished() {
        return Err("declared type must consume the complete fragment".to_string());
    }
    Ok(())
}

fn parse_default_atom(cursor: &mut Cursor<'_>) -> Result<(), String> {
    let wrapper_count = consume_open_wrappers(cursor);
    parse_one_default_atom(cursor)?;
    for _ in 0..wrapper_count {
        cursor.punctuation(')')?;
    }
    Ok(())
}

fn consume_open_wrappers(cursor: &mut Cursor<'_>) -> usize {
    let mut count = 0;
    while cursor.symbol('(') {
        cursor.next();
        count += 1;
    }
    count
}

fn parse_one_default_atom(cursor: &mut Cursor<'_>) -> Result<(), String> {
    let token = cursor
        .peek()
        .ok_or_else(|| "DEFAULT requires an atom".to_string())?;
    match token.kind {
        TokenKind::String => {
            cursor.next();
            Ok(())
        }
        TokenKind::Word => {
            let word = token.text(cursor.source);
            if matches!(
                word.to_ascii_uppercase().as_str(),
                "NULL" | "TRUE" | "FALSE" | "CURRENT_TIME" | "CURRENT_DATE" | "CURRENT_TIMESTAMP"
            ) {
                cursor.next();
                return Ok(());
            }
            if word.eq_ignore_ascii_case("X") {
                let next = cursor
                    .peek_n(1)
                    .filter(|next| matches!(next.kind, TokenKind::String));
                if let Some(next) = next {
                    let hex = next.text(cursor.source);
                    if hex[1..hex.len() - 1].len() % 2 != 0
                        || !hex[1..hex.len() - 1]
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit())
                    {
                        return Err("blob DEFAULT requires an even number of hexadecimal digits"
                            .to_string());
                    }
                    cursor.next();
                    cursor.next();
                    return Ok(());
                }
            }
            parse_numeric_default(cursor)
        }
        TokenKind::Symbol('+') | TokenKind::Symbol('-') | TokenKind::Symbol('.') => {
            parse_numeric_default(cursor)
        }
        _ => Err("DEFAULT must use a bounded literal atom".to_string()),
    }
}

fn parse_numeric_default(cursor: &mut Cursor<'_>) -> Result<(), String> {
    if cursor.symbol('+') || cursor.symbol('-') {
        cursor.next();
    }
    let mut has_digits_before_dot = false;
    let mut has_digits_after_dot = false;
    if cursor
        .peek()
        .is_some_and(|token| is_digits(token.text(cursor.source)))
    {
        has_digits_before_dot = true;
        cursor.next();
    }
    if cursor.symbol('.') {
        cursor.next();
        if cursor
            .peek()
            .is_some_and(|token| is_digits(token.text(cursor.source)))
        {
            has_digits_after_dot = true;
            cursor.next();
        }
    }
    if !has_digits_before_dot && !has_digits_after_dot {
        return Err("DEFAULT numeric literal requires digits".to_string());
    }
    let mantissa_end = cursor.previous_end();
    if let Some(exponent) = cursor.peek() {
        let exponent_text = exponent.text(cursor.source);
        if exponent.span.start == mantissa_end
            && (exponent_text.starts_with('E') || exponent_text.starts_with('e'))
        {
            let exponent_digits = &exponent_text[1..];
            if !exponent_digits.is_empty() {
                if !is_digits(exponent_digits) {
                    return Err("DEFAULT exponent requires digits".to_string());
                }
                cursor.next();
                return Ok(());
            }

            cursor.next();
            let mut exponent_end = cursor.previous_end();
            if cursor.symbol('+') || cursor.symbol('-') {
                let sign = cursor
                    .peek()
                    .ok_or_else(|| "DEFAULT exponent requires digits".to_string())?;
                if sign.span.start != exponent_end {
                    return Err("DEFAULT exponent requires digits".to_string());
                }
                cursor.next();
                exponent_end = cursor.previous_end();
            }
            cursor
                .peek()
                .filter(|token| {
                    token.span.start == exponent_end && is_digits(token.text(cursor.source))
                })
                .ok_or_else(|| "DEFAULT exponent requires digits".to_string())?;
            cursor.next();
        }
    }
    Ok(())
}

fn validate_default_atom(value: &str) -> Result<(), String> {
    let tokens = tokenize(value)?;
    let mut cursor = Cursor::new(value, &tokens);
    parse_default_atom(&mut cursor)?;
    if !cursor.is_finished() {
        return Err("DEFAULT must contain exactly one bounded atom".to_string());
    }
    Ok(())
}

fn parse_references(cursor: &mut Cursor<'_>) -> Result<(), String> {
    cursor.keyword("REFERENCES")?;
    cursor.identifier()?;
    if cursor.symbol('(') {
        parse_identifier_list(cursor, false)?;
    }
    parse_foreign_key_tail(cursor)
}

fn parse_foreign_key_tail(cursor: &mut Cursor<'_>) -> Result<(), String> {
    let mut saw_delete = false;
    let mut saw_update = false;
    while cursor.peek_keyword("ON") {
        cursor.keyword("ON")?;
        let is_delete = if cursor.peek_keyword("DELETE") {
            cursor.next();
            true
        } else if cursor.peek_keyword("UPDATE") {
            cursor.next();
            false
        } else {
            return Err("REFERENCES ON clause must name DELETE or UPDATE".to_string());
        };
        if (is_delete && saw_delete) || (!is_delete && saw_update) {
            return Err("FOREIGN KEY contains a repeated ON action".to_string());
        }
        if is_delete {
            saw_delete = true;
        } else {
            saw_update = true;
        }
        parse_foreign_key_action(cursor)?;
    }

    let mut saw_deferrable = false;
    if cursor.peek_keyword("DEFERRABLE") || cursor.peek_keyword("NOT") {
        if cursor.peek_keyword("NOT") {
            cursor.keyword("NOT")?;
            cursor.keyword("DEFERRABLE")?;
        } else {
            cursor.keyword("DEFERRABLE")?;
        }
        saw_deferrable = true;
    }
    if cursor.peek_keyword("INITIALLY") {
        if !saw_deferrable {
            return Err("INITIALLY requires a DEFERRABLE clause".to_string());
        }
        cursor.keyword("INITIALLY")?;
        if cursor.peek_keyword("DEFERRED") || cursor.peek_keyword("IMMEDIATE") {
            cursor.next();
        } else {
            return Err("INITIALLY must name DEFERRED or IMMEDIATE".to_string());
        }
    }
    Ok(())
}

fn parse_foreign_key_action(cursor: &mut Cursor<'_>) -> Result<(), String> {
    if cursor.peek_keyword("NO") {
        cursor.next();
        cursor.keyword("ACTION")?;
    } else if cursor.peek_keyword("RESTRICT") || cursor.peek_keyword("CASCADE") {
        cursor.next();
    } else if cursor.peek_keyword("SET") {
        cursor.next();
        if cursor.peek_keyword("NULL") || cursor.peek_keyword("DEFAULT") {
            cursor.next();
        } else {
            return Err("SET action must name NULL or DEFAULT".to_string());
        }
    } else {
        return Err("unsupported REFERENCES action".to_string());
    }
    Ok(())
}

fn parse_identifier_list(cursor: &mut Cursor<'_>, allow_key_details: bool) -> Result<(), String> {
    cursor.punctuation('(')?;
    parse_identifier_with_key_details(cursor, allow_key_details)?;
    while cursor.symbol(',') {
        cursor.next();
        parse_identifier_with_key_details(cursor, allow_key_details)?;
    }
    cursor.punctuation(')')?;
    Ok(())
}

fn parse_identifier_with_key_details(
    cursor: &mut Cursor<'_>,
    allow_key_details: bool,
) -> Result<(), String> {
    cursor.identifier()?;
    if allow_key_details && cursor.peek_keyword("COLLATE") {
        cursor.keyword("COLLATE")?;
        let collation = cursor.identifier()?;
        if !matches!(
            collation.to_ascii_uppercase().as_str(),
            "BINARY" | "NOCASE" | "RTRIM"
        ) {
            return Err("table key supports only BINARY, NOCASE, and RTRIM collations".to_string());
        }
    }
    if allow_key_details && cursor.peek_keyword("ASC") {
        cursor.next();
    }
    Ok(())
}

fn starts_table_constraint(source: &str, tokens: &[Token]) -> bool {
    tokens.first().is_some_and(|token| {
        token.is_word(source, "CONSTRAINT")
            || token.is_word(source, "PRIMARY")
            || token.is_word(source, "UNIQUE")
            || token.is_word(source, "FOREIGN")
            || token.is_word(source, "CHECK")
    })
}

fn validate_table_constraint(source: &str, tokens: &[Token]) -> Result<(), String> {
    let mut cursor = Cursor::new(source, tokens);
    if cursor.peek_keyword("CONSTRAINT") {
        cursor.next();
        cursor.identifier()?;
    }
    if cursor.peek_keyword("PRIMARY") || cursor.peek_keyword("UNIQUE") {
        cursor.next();
        if cursor.peek_keyword("KEY") {
            cursor.next();
        }
        parse_identifier_list(&mut cursor, true)?;
    } else if cursor.peek_keyword("FOREIGN") {
        cursor.keyword("FOREIGN")?;
        cursor.keyword("KEY")?;
        parse_identifier_list(&mut cursor, false)?;
        parse_references(&mut cursor)?;
    } else {
        return Err("CHECK table constraints are outside the bounded rebuild grammar".to_string());
    }
    if !cursor.is_finished() {
        return Err("table constraint contains an unsupported tail".to_string());
    }
    Ok(())
}

fn unsigned_integer(cursor: &mut Cursor<'_>) -> Result<(), String> {
    let token = cursor
        .peek()
        .ok_or_else(|| "type parameter requires an unsigned integer".to_string())?;
    if !matches!(token.kind, TokenKind::Word) || !is_digits(token.text(cursor.source)) {
        return Err("type parameter requires an unsigned integer".to_string());
    }
    cursor.next();
    Ok(())
}

fn is_clause_start(source: &str, token: Option<&Token>) -> bool {
    token.is_some_and(|token| {
        [
            "CONSTRAINT",
            "NOT",
            "NULL",
            "DEFAULT",
            "COLLATE",
            "PRIMARY",
            "UNIQUE",
            "REFERENCES",
            "CHECK",
            "GENERATED",
            "AS",
            "AUTOINCREMENT",
            "ON",
        ]
        .iter()
        .any(|keyword| token.is_word(source, keyword))
    })
}

fn find_matching_parenthesis(tokens: &[Token], start: usize) -> Result<usize, String> {
    let mut depth = 1;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        if token.symbol('(') {
            depth += 1;
        } else if token.symbol(')') {
            depth -= 1;
            if depth == 0 {
                return Ok(index);
            }
        }
    }
    Err("CREATE TABLE has an unclosed parenthesis".to_string())
}

fn split_elements(tokens: &[Token]) -> Result<Vec<Range<usize>>, String> {
    let mut elements = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    for (index, token) in tokens.iter().enumerate() {
        if token.symbol('(') {
            depth += 1;
        } else if token.symbol(')') {
            if depth == 0 {
                return Err("CREATE TABLE contains an unexpected closing parenthesis".to_string());
            }
            depth -= 1;
        } else if token.symbol(',') && depth == 0 {
            elements.push(start..index);
            start = index + 1;
        }
    }
    if depth != 0 {
        return Err("CREATE TABLE has an unclosed nested parenthesis".to_string());
    }
    elements.push(start..tokens.len());
    Ok(elements)
}

fn expand_leading_space(source: &str, range: Range<usize>) -> Range<usize> {
    let mut start = range.start;
    while start > 0 && source.as_bytes()[start - 1].is_ascii_whitespace() {
        start -= 1;
    }
    start..range.end
}

fn tokenize(source: &str) -> Result<Vec<Token>, String> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
        } else if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            return Err("comments are outside the bounded rebuild grammar".to_string());
        } else if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            return Err("comments are outside the bounded rebuild grammar".to_string());
        } else if matches!(byte, b'\'' | b'"' | b'`' | b'[') {
            let (end, kind) = quoted_token(source, index)?;
            tokens.push(Token {
                kind,
                span: index..end,
            });
            index = end;
        } else if byte.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Word,
                span: start..index,
            });
        } else if byte.is_ascii_alphabetic() || byte == b'_' {
            let start = index;
            index += 1;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
            {
                index += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Word,
                span: start..index,
            });
        } else if b"(),.;+-".contains(&byte) {
            tokens.push(Token {
                kind: TokenKind::Symbol(byte as char),
                span: index..index + 1,
            });
            index += 1;
        } else {
            return Err(format!("unsupported token starting at byte {index}"));
        }
    }
    Ok(tokens)
}

fn quoted_token(source: &str, start: usize) -> Result<(usize, TokenKind), String> {
    let bytes = source.as_bytes();
    let (quote, kind) = match bytes[start] {
        b'\'' => return read_quoted(source, start, b'\'', TokenKind::String),
        b'"' => (b'"', TokenKind::QuotedIdentifier),
        b'`' => (b'`', TokenKind::QuotedIdentifier),
        b'[' => (b']', TokenKind::QuotedIdentifier),
        _ => return Err("unsupported quoted token".to_string()),
    };
    read_quoted(source, start, quote, kind)
}

fn read_quoted(
    source: &str,
    start: usize,
    quote: u8,
    kind: TokenKind,
) -> Result<(usize, TokenKind), String> {
    let bytes = source.as_bytes();
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == quote {
            if quote != b']' && bytes.get(index + 1) == Some(&quote) {
                index += 2;
            } else {
                return Ok((index + 1, kind));
            }
        } else {
            index += 1;
        }
    }
    Err("unterminated quoted token".to_string())
}

fn unquote_identifier(raw: &str) -> Result<String, String> {
    if raw.starts_with('\'') {
        return Err(
            "single-quoted identifiers are outside the bounded rebuild grammar".to_string(),
        );
    }
    if raw.starts_with('"') {
        return Ok(raw[1..raw.len() - 1].replace("\"\"", "\""));
    }
    if raw.starts_with('`') {
        return Ok(raw[1..raw.len() - 1].replace("``", "`"));
    }
    if raw.starts_with('[') {
        return Ok(raw[1..raw.len() - 1].to_string());
    }
    Ok(raw.to_string())
}

fn is_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

struct Cursor<'a> {
    source: &'a str,
    tokens: &'a [Token],
    index: usize,
}

impl<'a> Cursor<'a> {
    fn new(source: &'a str, tokens: &'a [Token]) -> Self {
        Self {
            source,
            tokens,
            index: 0,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn peek_n(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.index + offset)
    }

    fn next(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.index);
        self.index += usize::from(token.is_some());
        token
    }

    fn previous_end(&self) -> usize {
        self.tokens[self.index - 1].span.end
    }

    fn previous_text(&self) -> &str {
        self.tokens[self.index - 1].text(self.source)
    }

    fn is_finished(&self) -> bool {
        self.index == self.tokens.len()
    }

    fn peek_keyword(&self, keyword: &str) -> bool {
        self.peek()
            .is_some_and(|token| token.is_word(self.source, keyword))
    }

    fn keyword(&mut self, keyword: &str) -> Result<(), String> {
        if self.peek_keyword(keyword) {
            self.next();
            Ok(())
        } else {
            Err(format!("expected keyword {keyword}"))
        }
    }

    fn symbol(&self, symbol: char) -> bool {
        self.peek().is_some_and(|token| token.symbol(symbol))
    }

    fn punctuation(&mut self, symbol: char) -> Result<(), String> {
        if self.symbol(symbol) {
            self.next();
            Ok(())
        } else {
            Err(format!("expected punctuation {symbol}"))
        }
    }

    fn identifier(&mut self) -> Result<String, String> {
        let token = self
            .peek()
            .ok_or_else(|| "expected identifier".to_string())?;
        match token.kind {
            TokenKind::Word | TokenKind::QuotedIdentifier => {
                let name = unquote_identifier(token.text(self.source))?;
                self.next();
                Ok(name)
            }
            TokenKind::String => {
                Err("single-quoted identifiers are outside the bounded rebuild grammar".to_string())
            }
            TokenKind::Symbol(_) => Err("expected identifier".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnChange, DefaultChange, parse_create_table};

    #[test]
    fn parses_ordinary_create_table_with_quoted_identifier() {
        let source = "CREATE TABLE \"people\" (\"id\" INTEGER PRIMARY KEY, name TEXT)";

        let parsed = parse_create_table(source).expect("ordinary table definition should parse");

        assert_eq!(parsed.source, source);
    }

    #[test]
    fn rewrites_only_selected_spans_and_reparses() {
        let source = "CREATE TABLE main.people (id INTEGER, name TEXT CONSTRAINT name_required NOT NULL DEFAULT ('x'))";
        let parsed = parse_create_table(source).expect("source should parse");
        let rewritten = parsed
            .rewrite(&[
                ColumnChange {
                    name: "name".to_string(),
                    new_type: Some("VARCHAR(32)".to_string()),
                    nullable: Some(true),
                    default: Some(DefaultChange::Set("(NULL)".to_string())),
                },
                ColumnChange {
                    name: "id".to_string(),
                    new_type: None,
                    nullable: None,
                    default: None,
                },
            ])
            .expect("selected changes should rewrite");

        assert_eq!(
            rewritten,
            "CREATE TABLE main.people (id INTEGER, name VARCHAR(32) DEFAULT (NULL))"
        );
        assert!(parse_create_table(&rewritten).is_ok());
    }

    #[test]
    fn accepts_bounded_default_atoms_and_preserves_absent_default() {
        for atom in [
            "''",
            "X''",
            "X'0A'",
            "-1.25e+3",
            ".5E-2",
            "((CURRENT_TIMESTAMP))",
            "NULL",
        ] {
            let source = format!("CREATE TABLE sample (value TEXT DEFAULT {atom})");
            assert!(parse_create_table(&source).is_ok(), "{atom} should parse");
        }

        let source = "CREATE TABLE sample (present TEXT DEFAULT NULL, absent TEXT)";
        let parsed = parse_create_table(source).expect("source should parse");
        let rewritten = parsed
            .rewrite(&[ColumnChange {
                name: "present".to_string(),
                new_type: None,
                nullable: None,
                default: Some(DefaultChange::Drop),
            }])
            .expect("explicit DEFAULT NULL should be removable");
        assert_eq!(rewritten, "CREATE TABLE sample (present TEXT, absent TEXT)");
    }

    #[test]
    fn nullable_rewrite_inserts_after_plain_types_and_executes_in_sqlite() {
        let cases = [
            (
                "CREATE TABLE sample (v TEXT)",
                "v",
                None,
                "CREATE TABLE sample (v TEXT NOT NULL)",
            ),
            (
                "CREATE TABLE sample (v TEXT   )",
                "v",
                None,
                "CREATE TABLE sample (v TEXT NOT NULL   )",
            ),
            (
                "CREATE TABLE sample (\"display name\" TEXT)",
                "display name",
                None,
                "CREATE TABLE sample (\"display name\" TEXT NOT NULL)",
            ),
            (
                "CREATE TABLE sample (`value` TEXT)",
                "value",
                Some("VARCHAR(32)"),
                "CREATE TABLE sample (`value` VARCHAR(32) NOT NULL)",
            ),
        ];

        for (source, name, new_type, expected) in cases {
            let parsed = parse_create_table(source).expect("source should parse");
            let rewritten = parsed
                .rewrite(&[ColumnChange {
                    name: name.to_string(),
                    new_type: new_type.map(str::to_string),
                    nullable: Some(false),
                    default: None,
                }])
                .expect("selected nullability change should rewrite");

            assert_eq!(rewritten, expected);
            let connection = rusqlite::Connection::open_in_memory()
                .expect("test-owned in-memory SQLite connection should open");
            connection
                .execute_batch(&rewritten)
                .expect("rewritten CREATE TABLE should execute in SQLite");
        }
    }

    #[test]
    fn accepts_unsigned_exponents_and_preserves_default_literal_spans() {
        for atom in ["1e3", "1E3", ".5e2", "1.2e3", "-1e3", "+.5E-2"] {
            let source = format!("CREATE TABLE sample (value TEXT DEFAULT {atom})");
            let parsed = parse_create_table(&source).expect("bounded exponent should parse");
            let rewritten = parsed
                .rewrite(&[ColumnChange {
                    name: "value".to_string(),
                    new_type: None,
                    nullable: Some(false),
                    default: None,
                }])
                .expect("nullable rewrite should preserve the default atom span");

            assert_eq!(
                rewritten,
                format!("CREATE TABLE sample (value TEXT NOT NULL DEFAULT {atom})")
            );
            let connection = rusqlite::Connection::open_in_memory()
                .expect("test-owned in-memory SQLite connection should open");
            connection
                .execute_batch(&rewritten)
                .expect("rewritten exponent default should execute in SQLite");
        }

        for atom in ["1e", "1e+", "1e3suffix"] {
            let source = format!("CREATE TABLE sample (value TEXT DEFAULT {atom})");
            assert!(
                parse_create_table(&source).is_err(),
                "malformed exponent {atom} must be rejected"
            );
        }
    }

    #[test]
    fn nullable_rewrite_preserves_default_spelling_and_named_default_removal() {
        let source = "CREATE TABLE sample (value DEFAULT 'keep', named TEXT CONSTRAINT source_default DEFAULT X'0A')";
        let parsed =
            parse_create_table(source).expect("omitted type and named default should parse");
        let rewritten = parsed
            .rewrite(&[
                ColumnChange {
                    name: "value".to_string(),
                    new_type: None,
                    nullable: Some(false),
                    default: None,
                },
                ColumnChange {
                    name: "named".to_string(),
                    new_type: None,
                    nullable: None,
                    default: Some(DefaultChange::Drop),
                },
            ])
            .expect("bounded changes should rewrite");

        assert_eq!(
            rewritten,
            "CREATE TABLE sample (value NOT NULL DEFAULT 'keep', named TEXT)"
        );
        assert!(parse_create_table(&rewritten).is_ok());
    }

    #[test]
    fn accepts_bounded_table_constraints_and_outbound_foreign_keys() {
        let source = "CREATE TABLE child (\
            id INTEGER,\
            parent_id INTEGER CONSTRAINT child_parent REFERENCES parent(id) \
                ON DELETE SET NULL ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED,\
            CONSTRAINT child_pk PRIMARY KEY (id ASC),\
            CONSTRAINT child_unique UNIQUE (parent_id COLLATE NOCASE),\
            CONSTRAINT child_fk FOREIGN KEY (parent_id) REFERENCES parent(id) \
                ON UPDATE SET DEFAULT ON DELETE NO ACTION NOT DEFERRABLE INITIALLY IMMEDIATE\
        )";

        let parsed = parse_create_table(source)
            .expect("bounded table constraints and outbound FK clauses should parse");
        let rewritten = parsed
            .rewrite(&[ColumnChange {
                name: "id".to_string(),
                new_type: Some("BIGINT".to_string()),
                nullable: None,
                default: None,
            }])
            .expect("selected rewrite should preserve table constraints");

        assert!(parse_create_table(&rewritten).is_ok());
        let connection = rusqlite::Connection::open_in_memory()
            .expect("test-owned in-memory SQLite connection should open");
        connection
            .execute_batch("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .expect("parent table should execute");
        connection
            .execute_batch(&rewritten)
            .expect("rewritten CREATE TABLE should execute in SQLite");
    }

    #[test]
    fn rejects_malformed_or_repeated_table_foreign_key_clauses() {
        for source in [
            "CREATE TABLE child (id INTEGER, FOREIGN KEY (id) REFERENCES parent(id) ON DELETE SET)",
            "CREATE TABLE child (id INTEGER, FOREIGN KEY (id) REFERENCES parent(id) ON UPDATE CASCADE ON UPDATE RESTRICT)",
            "CREATE TABLE child (id INTEGER, FOREIGN KEY (id) REFERENCES parent(id) DEFERRABLE DEFERRABLE)",
            "CREATE TABLE child (id INTEGER, FOREIGN KEY (id) REFERENCES parent(id) INITIALLY DEFERRED)",
            "CREATE TABLE child (id INTEGER, FOREIGN KEY () REFERENCES parent(id))",
            "CREATE TABLE child (id INTEGER, UNIQUE (id) COLLATE NOCASE)",
            "CREATE TABLE child (id INTEGER, CONSTRAINT named CHECK (id > 0))",
        ] {
            assert!(
                parse_create_table(source).is_err(),
                "{source} must remain outside the bounded table-constraint grammar"
            );
        }
    }

    #[test]
    fn rejects_unbounded_or_injected_syntax() {
        for source in [
            "CREATE TABLE sample (value TEXT /* comment */)",
            "CREATE TABLE sample ('value' TEXT)",
            "CREATE TABLE sample (value TEXT NULL)",
            "CREATE TABLE sample (value TEXT DEFAULT random())",
            "CREATE TABLE sample (value TEXT DEFAULT X'0')",
            "CREATE TABLE sample (value TEXT DEFAULT 0x10)",
            "CREATE TABLE sample (value TEXT DEFAULT 1_000)",
            "CREATE TABLE sample (value TEXT DEFAULT +NULL)",
            "CREATE TABLE sample (value TEXT DEFAULT 1e)",
            "CREATE TABLE sample (value TEXT DEFAULT \"string\")",
            "CREATE TABLE sample (value TEXT CHECK (value <> ''))",
            "CREATE TABLE sample (value TEXT GENERATED ALWAYS AS (1))",
            "CREATE TABLE sample (value TEXT) STRICT",
            "CREATE TABLE sample (value TEXT) WITHOUT ROWID",
            "CREATE TABLE sample AS SELECT 1",
        ] {
            assert!(
                parse_create_table(source).is_err(),
                "{source} must be rejected"
            );
        }

        let parsed =
            parse_create_table("CREATE TABLE sample (value TEXT)").expect("source should parse");
        for new_type in [
            "TEXT; DROP TABLE users",
            "TEXT CHECK (value)",
            "TEXT -- injected",
        ] {
            let result = parsed.rewrite(&[ColumnChange {
                name: "value".to_string(),
                new_type: Some(new_type.to_string()),
                nullable: None,
                default: None,
            }]);
            assert!(result.is_err(), "{new_type} must be rejected");
        }
    }
}
