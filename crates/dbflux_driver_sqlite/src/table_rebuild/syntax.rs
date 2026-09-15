use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CreateTable {
    source: String,
    table_name_span: Range<usize>,
    columns: Vec<Column>,
    constraints: Vec<TableConstraint>,
    key_declarations: Vec<KeyDeclaration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ColumnFacts {
    pub name: String,
    pub declared_type: Option<String>,
    pub nullable: bool,
    pub default: Option<String>,
    pub collation: String,
    pub primary_key_order: i64,
    pub primary_key_descending: bool,
    pub unique: bool,
    pub foreign_key: Option<ForeignKeyFacts>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KeyTerm {
    pub column: String,
    pub descending: bool,
    pub explicit_collation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyKind {
    PrimaryKey,
    Unique,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyDeclarationOrigin {
    Inline,
    Table,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KeyDeclaration {
    pub kind: KeyKind,
    pub origin: KeyDeclarationOrigin,
    pub terms: Vec<KeyTerm>,
    pub source_order: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TableConstraint {
    PrimaryKey(Vec<KeyTerm>),
    Unique(Vec<KeyTerm>),
    ForeignKey(ForeignKeyFacts),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CreateIndex {
    pub name: String,
    pub target_table: String,
    pub unique: bool,
    pub keys: Vec<KeyTerm>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ForeignKeyFacts {
    pub columns: Vec<String>,
    pub parent_table: String,
    pub parent_columns: Option<Vec<String>>,
    pub on_update: String,
    pub on_delete: String,
    pub deferrable: String,
    declaration_span: Range<usize>,
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
    definition_span: Range<usize>,
    removal_span: Range<usize>,
    facts: ColumnFacts,
    key_declarations: Vec<KeyDeclaration>,
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
    let mut table_name_span = cursor.previous_token()?.span.clone();
    if cursor.symbol('.') {
        if !first_name.eq_ignore_ascii_case("main") {
            return Err("rebuild supports the main schema only".to_string());
        }
        cursor.next();
        cursor.identifier()?;
        table_name_span = cursor.previous_token()?.span.clone();
    }
    cursor.punctuation('(')?;
    let body_start = cursor.index;
    let body_end = find_matching_parenthesis(&tokens, body_start)?;
    let body_tokens = tokens
        .get(body_start..body_end)
        .ok_or_else(|| "CREATE TABLE has an invalid parenthesized body".to_string())?;
    let elements = split_elements(body_tokens)?;
    if elements.is_empty() {
        return Err("CREATE TABLE must retain at least one column".to_string());
    }

    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    let mut key_declarations = Vec::new();
    for (element_index, element) in elements.iter().enumerate() {
        let element_start = body_start
            .checked_add(element.start)
            .ok_or_else(|| "CREATE TABLE element position overflowed".to_string())?;
        let element_end = body_start
            .checked_add(element.end)
            .ok_or_else(|| "CREATE TABLE element position overflowed".to_string())?;
        let element_tokens = tokens
            .get(element_start..element_end)
            .ok_or_else(|| "CREATE TABLE contains an invalid element range".to_string())?;
        if element_tokens.is_empty() {
            return Err("CREATE TABLE contains an empty definition".to_string());
        }
        if starts_table_constraint(source, element_tokens) {
            let constraint = parse_table_constraint(source, element_tokens)?;
            let source_order = element_tokens
                .first()
                .ok_or_else(|| "CREATE TABLE constraint requires a token".to_string())?
                .span
                .start;
            match &constraint {
                TableConstraint::PrimaryKey(terms) => key_declarations.push(KeyDeclaration {
                    kind: KeyKind::PrimaryKey,
                    origin: KeyDeclarationOrigin::Table,
                    terms: terms.clone(),
                    source_order,
                }),
                TableConstraint::Unique(terms) => key_declarations.push(KeyDeclaration {
                    kind: KeyKind::Unique,
                    origin: KeyDeclarationOrigin::Table,
                    terms: terms.clone(),
                    source_order,
                }),
                TableConstraint::ForeignKey(_) => {}
            }
            constraints.push(constraint);
        } else {
            let mut column = parse_column(source, element_tokens)?;
            key_declarations.append(&mut column.key_declarations);
            column.removal_span = if element_index == 0 {
                if let Some(next) = elements.get(element_index + 1) {
                    let next_start = body_start
                        .checked_add(next.start)
                        .ok_or_else(|| "CREATE TABLE element position overflowed".to_string())?;
                    let next_token = tokens
                        .get(next_start)
                        .ok_or_else(|| "CREATE TABLE has an invalid next element".to_string())?;
                    column.definition_span.start..next_token.span.start
                } else {
                    column.definition_span.clone()
                }
            } else {
                let separator = tokens
                    .get(body_start + element.start - 1)
                    .filter(|token| token.symbol(','))
                    .ok_or_else(|| "CREATE TABLE column is missing a separator".to_string())?;
                separator.span.start..column.definition_span.end
            };
            columns.push(column);
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
    if names
        .windows(2)
        .any(|names| matches!(names, [first, second] if first == second))
    {
        return Err("CREATE TABLE contains duplicate column names".to_string());
    }
    let table_primary_keys = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::PrimaryKey(columns) => Some(columns),
            _ => None,
        })
        .collect::<Vec<_>>();
    if table_primary_keys.len() > 1
        || (table_primary_keys.len() == 1
            && columns
                .iter()
                .any(|column| column.facts.primary_key_order != 0))
    {
        return Err("CREATE TABLE contains ambiguous PRIMARY KEY clauses".to_string());
    }
    if let Some(primary_key) = table_primary_keys.first() {
        for (index, term) in primary_key.iter().enumerate() {
            let column = columns
                .iter_mut()
                .find(|column| column.name.eq_ignore_ascii_case(&term.column))
                .ok_or_else(|| "PRIMARY KEY names an unknown column".to_string())?;
            column.facts.primary_key_order = (index + 1) as i64;
            column.facts.primary_key_descending = term.descending;
        }
    }

    cursor.index = body_end
        .checked_add(1)
        .ok_or_else(|| "CREATE TABLE parenthesis position overflowed".to_string())?;
    if cursor.symbol(';') {
        cursor.next();
    }
    if !cursor.is_finished() {
        return Err("CREATE TABLE contains an unsupported suffix".to_string());
    }

    Ok(CreateTable {
        source: source.to_string(),
        table_name_span,
        columns,
        constraints,
        key_declarations,
    })
}

pub(super) fn parse_create_index(source: &str) -> Result<CreateIndex, String> {
    let tokens = tokenize(source)?;
    let mut cursor = Cursor::new(source, &tokens);
    cursor.keyword("CREATE")?;
    let unique = if cursor.peek_keyword("UNIQUE") {
        cursor.next();
        true
    } else {
        false
    };
    cursor.keyword("INDEX")?;
    if cursor.peek_keyword("IF") {
        cursor.keyword("IF")?;
        cursor.keyword("NOT")?;
        cursor.keyword("EXISTS")?;
    }
    let first_index_name = cursor.identifier()?;
    let name = if cursor.symbol('.') {
        if !first_index_name.eq_ignore_ascii_case("main") {
            return Err("CREATE INDEX supports the main schema only".to_string());
        }
        cursor.next();
        cursor.identifier()?
    } else {
        first_index_name
    };
    cursor.keyword("ON")?;
    let first_target_name = cursor.identifier()?;
    let target_table = if cursor.symbol('.') {
        if !first_target_name.eq_ignore_ascii_case("main") {
            return Err("CREATE INDEX target supports the main schema only".to_string());
        }
        cursor.next();
        cursor.identifier()?
    } else {
        first_target_name
    };
    let keys = parse_key_identifier_list(&mut cursor, true)?;
    if cursor.symbol(';') {
        cursor.next();
    }
    if !cursor.is_finished() {
        return Err("CREATE INDEX contains an unsupported suffix".to_string());
    }
    Ok(CreateIndex {
        name,
        target_table,
        unique,
        keys,
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
            if let [left, right] = pair
                && left.0.end > right.0.start
            {
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

    pub(super) fn rewrite_rebuild(
        &self,
        changes: &[ColumnChange],
        drops: &[String],
        replacement_name: &str,
    ) -> Result<String, String> {
        let rewritten = if changes.is_empty() {
            self.source.clone()
        } else {
            self.rewrite(changes)?
        };
        let parsed = parse_create_table(&rewritten)?;
        if drops.is_empty() {
            return parsed.rename_table(replacement_name);
        }
        let mut selected = vec![false; parsed.columns.len()];
        let mut seen = std::collections::HashSet::new();
        for name in drops {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(format!("selected column {name:?} is repeated"));
            }
            let position = parsed
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| format!("selected column {name:?} is not in CREATE TABLE"))?;
            *selected
                .get_mut(position)
                .ok_or_else(|| "selected column position is outside CREATE TABLE".to_string())? =
                true;
        }
        if selected.iter().all(|selected| *selected) {
            return Err("rebuild must retain at least one stored column".to_string());
        }

        let mut removals = Vec::new();
        let mut start = 0;
        while let Some(is_selected) = selected.get(start) {
            if !is_selected {
                start += 1;
                continue;
            }
            let mut end = start;
            while end
                .checked_add(1)
                .and_then(|next| selected.get(next))
                .copied()
                == Some(true)
            {
                end += 1;
            }
            let start_column = parsed
                .columns
                .get(start)
                .ok_or_else(|| "selected column position is outside CREATE TABLE".to_string())?;
            let end_column = parsed
                .columns
                .get(end)
                .ok_or_else(|| "selected column position is outside CREATE TABLE".to_string())?;
            let removal = if start == 0 {
                let next_column = parsed
                    .columns
                    .get(
                        end.checked_add(1)
                            .ok_or_else(|| "selected column position overflowed".to_string())?,
                    )
                    .ok_or_else(|| "rebuild cannot remove every stored column".to_string())?;
                start_column.definition_span.start..next_column.definition_span.start
            } else {
                start_column.removal_span.start..end_column.definition_span.end
            };
            removals.push(removal);
            start = end
                .checked_add(1)
                .ok_or_else(|| "selected column position overflowed".to_string())?;
        }

        let mut without_drops = parsed.source.clone();
        for removal in removals.into_iter().rev() {
            without_drops.replace_range(removal, "");
        }
        parse_create_table(&without_drops)?.rename_table(replacement_name)
    }

    fn rename_table(&self, replacement_name: &str) -> Result<String, String> {
        if replacement_name.is_empty() {
            return Err("rebuild replacement table name is empty".to_string());
        }
        let mut rewritten = self.source.clone();
        rewritten.replace_range(
            self.table_name_span.clone(),
            &format!("main.\"{}\"", replacement_name.replace('"', "\"\"")),
        );
        parse_create_table(&rewritten)?;
        Ok(rewritten)
    }

    pub(super) fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|column| column.name.as_str())
    }

    pub(super) fn column_facts(&self) -> impl Iterator<Item = &ColumnFacts> {
        self.columns.iter().map(|column| &column.facts)
    }

    pub(super) fn constraints(&self) -> &[TableConstraint] {
        &self.constraints
    }

    pub(super) fn key_declarations(&self) -> Vec<KeyDeclaration> {
        let mut declarations = self.key_declarations.clone();
        declarations.sort_by_key(|declaration| declaration.source_order);
        declarations
    }

    pub(super) fn foreign_keys(&self) -> impl Iterator<Item = &ForeignKeyFacts> {
        self.columns
            .iter()
            .filter_map(|column| column.facts.foreign_key.as_ref())
            .chain(
                self.constraints
                    .iter()
                    .filter_map(|constraint| match constraint {
                        TableConstraint::ForeignKey(foreign_key) => Some(foreign_key),
                        _ => None,
                    }),
            )
    }

    pub(super) fn foreign_key_declaration_sources(&self) -> Vec<&str> {
        self.foreign_keys()
            .map(|foreign_key| &self.source[foreign_key.declaration_span.clone()])
            .collect()
    }
}

fn parse_column(source: &str, tokens: &[Token]) -> Result<Column, String> {
    let mut cursor = Cursor::new(source, tokens);
    let name = cursor.identifier()?;
    let name_end = cursor.previous_end()?;
    let mut type_span = None;
    let mut not_null_span = None;
    let mut default_span = None;
    let mut default_atom_span = None;
    let mut first_clause_start = name_end;
    let mut saw_clause = false;
    let mut collation = "BINARY".to_string();
    let mut primary_key = false;
    let mut primary_key_descending = false;
    let mut unique = false;
    let mut key_declarations = Vec::new();
    let mut foreign_key = None;

    if !cursor.is_finished() && !is_clause_start(source, cursor.peek()) {
        let start = cursor
            .peek()
            .ok_or_else(|| "declared type requires a token".to_string())?
            .span
            .start;
        parse_type(&mut cursor)?;
        let type_end = cursor.previous_end()?;
        type_span = Some(start..type_end);
        first_clause_start = cursor.peek().map_or(type_end, |token| token.span.start);
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
            not_null_span = Some(named_start..cursor.previous_end()?);
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
            let default_end = cursor.previous_end()?;
            default_atom_span = Some(atom_start..default_end);
            default_span = Some(named_start..default_end);
        } else if cursor.peek_keyword("COLLATE") {
            cursor.keyword("COLLATE")?;
            collation = parse_builtin_collation(&mut cursor, "rebuild supports only")?;
        } else if cursor.peek_keyword("PRIMARY") {
            cursor.keyword("PRIMARY")?;
            cursor.keyword("KEY")?;
            primary_key_descending = if cursor.peek_keyword("DESC") {
                cursor.next();
                true
            } else {
                if cursor.peek_keyword("ASC") {
                    cursor.next();
                }
                false
            };
            primary_key = true;
            key_declarations.push(KeyDeclaration {
                kind: KeyKind::PrimaryKey,
                origin: KeyDeclarationOrigin::Inline,
                terms: vec![KeyTerm {
                    column: name.clone(),
                    descending: primary_key_descending,
                    explicit_collation: None,
                }],
                source_order: named_start,
            });
        } else if cursor.peek_keyword("UNIQUE") {
            cursor.next();
            unique = true;
            key_declarations.push(KeyDeclaration {
                kind: KeyKind::Unique,
                origin: KeyDeclarationOrigin::Inline,
                terms: vec![KeyTerm {
                    column: name.clone(),
                    descending: false,
                    explicit_collation: None,
                }],
                source_order: named_start,
            });
        } else if cursor.peek_keyword("REFERENCES") {
            foreign_key = Some(parse_references(&mut cursor)?);
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
    let definition_span = tokens
        .first()
        .map(|token| token.span.start..definition_end)
        .ok_or_else(|| "column definition requires a token".to_string())?;
    let facts = ColumnFacts {
        name: name.clone(),
        declared_type: type_span
            .as_ref()
            .map(|span| source[span.clone()].to_string()),
        nullable: not_null_span.is_none(),
        default: default_atom_span
            .as_ref()
            .map(|span| source[span.clone()].to_string()),
        collation,
        primary_key_order: i64::from(primary_key),
        primary_key_descending,
        unique,
        foreign_key: foreign_key.map(|mut foreign_key| {
            foreign_key.columns.push(name.clone());
            foreign_key
        }),
    };
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
        definition_span: definition_span.clone(),
        removal_span: definition_span,
        facts,
        key_declarations,
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
    let mantissa_end = cursor.previous_end()?;
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
            let mut exponent_end = cursor.previous_end()?;
            if cursor.symbol('+') || cursor.symbol('-') {
                let sign = cursor
                    .peek()
                    .ok_or_else(|| "DEFAULT exponent requires digits".to_string())?;
                if sign.span.start != exponent_end {
                    return Err("DEFAULT exponent requires digits".to_string());
                }
                cursor.next();
                exponent_end = cursor.previous_end()?;
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

fn parse_references(cursor: &mut Cursor<'_>) -> Result<ForeignKeyFacts, String> {
    let declaration_start = cursor
        .peek()
        .ok_or_else(|| "REFERENCES requires a keyword".to_string())?
        .span
        .start;
    cursor.keyword("REFERENCES")?;
    let parent_table = cursor.identifier()?;
    let parent_columns = cursor
        .symbol('(')
        .then(|| parse_identifier_list(cursor, false))
        .transpose()?;
    let (on_update, on_delete, deferrable) = parse_foreign_key_tail(cursor)?;
    Ok(ForeignKeyFacts {
        columns: Vec::new(),
        parent_table,
        parent_columns,
        on_update,
        on_delete,
        deferrable,
        declaration_span: declaration_start..cursor.previous_end()?,
    })
}

fn parse_foreign_key_tail(cursor: &mut Cursor<'_>) -> Result<(String, String, String), String> {
    let mut on_delete = "NO ACTION".to_string();
    let mut on_update = "NO ACTION".to_string();
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
        let action = parse_foreign_key_action(cursor)?;
        if is_delete {
            saw_delete = true;
            on_delete = action;
        } else {
            saw_update = true;
            on_update = action;
        }
    }

    let mut deferrable = "NOT DEFERRABLE".to_string();
    let mut saw_deferrable = false;
    if cursor.peek_keyword("DEFERRABLE") || cursor.peek_keyword("NOT") {
        if cursor.peek_keyword("NOT") {
            cursor.keyword("NOT")?;
            cursor.keyword("DEFERRABLE")?;
        } else {
            cursor.keyword("DEFERRABLE")?;
            deferrable = "DEFERRABLE".to_string();
        }
        saw_deferrable = true;
    }
    if cursor.peek_keyword("INITIALLY") {
        if !saw_deferrable {
            return Err("INITIALLY requires a DEFERRABLE clause".to_string());
        }
        cursor.keyword("INITIALLY")?;
        let initial = if cursor.peek_keyword("DEFERRED") {
            cursor.next();
            "DEFERRED"
        } else if cursor.peek_keyword("IMMEDIATE") {
            cursor.next();
            "IMMEDIATE"
        } else {
            return Err("INITIALLY must name DEFERRED or IMMEDIATE".to_string());
        };
        deferrable.push_str(" INITIALLY ");
        deferrable.push_str(initial);
    }
    Ok((on_update, on_delete, deferrable))
}

fn parse_foreign_key_action(cursor: &mut Cursor<'_>) -> Result<String, String> {
    if cursor.peek_keyword("NO") {
        cursor.next();
        cursor.keyword("ACTION")?;
        Ok("NO ACTION".to_string())
    } else if cursor.peek_keyword("RESTRICT") {
        cursor.next();
        Ok("RESTRICT".to_string())
    } else if cursor.peek_keyword("CASCADE") {
        cursor.next();
        Ok("CASCADE".to_string())
    } else if cursor.peek_keyword("SET") {
        cursor.next();
        if cursor.peek_keyword("NULL") {
            cursor.next();
            Ok("SET NULL".to_string())
        } else if cursor.peek_keyword("DEFAULT") {
            cursor.next();
            Ok("SET DEFAULT".to_string())
        } else {
            Err("SET action must name NULL or DEFAULT".to_string())
        }
    } else {
        Err("unsupported REFERENCES action".to_string())
    }
}

fn parse_identifier_list(
    cursor: &mut Cursor<'_>,
    allow_key_details: bool,
) -> Result<Vec<String>, String> {
    parse_key_identifier_list(cursor, allow_key_details)
        .map(|terms| terms.into_iter().map(|term| term.column).collect())
}

fn parse_key_identifier_list(
    cursor: &mut Cursor<'_>,
    allow_key_details: bool,
) -> Result<Vec<KeyTerm>, String> {
    cursor.punctuation('(')?;
    let mut terms = vec![parse_identifier_with_key_details(
        cursor,
        allow_key_details,
    )?];
    while cursor.symbol(',') {
        cursor.next();
        terms.push(parse_identifier_with_key_details(
            cursor,
            allow_key_details,
        )?);
    }
    cursor.punctuation(')')?;
    Ok(terms)
}

fn parse_identifier_with_key_details(
    cursor: &mut Cursor<'_>,
    allow_key_details: bool,
) -> Result<KeyTerm, String> {
    let column = cursor.identifier()?;
    let explicit_collation = if allow_key_details && cursor.peek_keyword("COLLATE") {
        cursor.keyword("COLLATE")?;
        Some(parse_builtin_collation(cursor, "table key supports only")?)
    } else {
        None
    };
    let descending = if allow_key_details && cursor.peek_keyword("DESC") {
        cursor.next();
        true
    } else {
        if allow_key_details && cursor.peek_keyword("ASC") {
            cursor.next();
        }
        false
    };
    Ok(KeyTerm {
        column,
        descending,
        explicit_collation,
    })
}

fn parse_builtin_collation(cursor: &mut Cursor<'_>, prefix: &str) -> Result<String, String> {
    let collation = cursor.identifier()?.to_ascii_uppercase();
    if matches!(collation.as_str(), "BINARY" | "NOCASE" | "RTRIM") {
        Ok(collation)
    } else {
        Err(format!("{prefix} BINARY, NOCASE, and RTRIM collations"))
    }
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

fn parse_table_constraint(source: &str, tokens: &[Token]) -> Result<TableConstraint, String> {
    let mut cursor = Cursor::new(source, tokens);
    if cursor.peek_keyword("CONSTRAINT") {
        cursor.next();
        cursor.identifier()?;
    }
    let constraint = if cursor.peek_keyword("PRIMARY") {
        cursor.next();
        if cursor.peek_keyword("KEY") {
            cursor.next();
        }
        TableConstraint::PrimaryKey(parse_key_identifier_list(&mut cursor, true)?)
    } else if cursor.peek_keyword("UNIQUE") {
        cursor.next();
        if cursor.peek_keyword("KEY") {
            cursor.next();
        }
        TableConstraint::Unique(parse_key_identifier_list(&mut cursor, true)?)
    } else if cursor.peek_keyword("FOREIGN") {
        cursor.keyword("FOREIGN")?;
        cursor.keyword("KEY")?;
        let columns = parse_identifier_list(&mut cursor, false)?;
        let mut foreign_key = parse_references(&mut cursor)?;
        foreign_key.columns = columns;
        TableConstraint::ForeignKey(foreign_key)
    } else {
        return Err("CHECK table constraints are outside the bounded rebuild grammar".to_string());
    };
    if !cursor.is_finished() {
        return Err("table constraint contains an unsupported tail".to_string());
    }
    Ok(constraint)
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
    while start > 0
        && source
            .as_bytes()
            .get(start - 1)
            .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        start -= 1;
    }
    start..range.end
}

fn tokenize(source: &str) -> Result<Vec<Token>, String> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        if byte.is_ascii_whitespace() {
            index += 1;
        } else if (byte == b'-' && bytes.get(index + 1) == Some(&b'-'))
            || (byte == b'/' && bytes.get(index + 1) == Some(&b'*'))
        {
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
            while bytes.get(index).is_some_and(|byte| byte.is_ascii_digit()) {
                index += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Word,
                span: start..index,
            });
        } else if byte.is_ascii_alphabetic() || byte == b'_' {
            let start = index;
            index += 1;
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
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
    let (quote, kind) = match bytes.get(start) {
        Some(b'\'') => return read_quoted(source, start, b'\'', TokenKind::String),
        Some(b'"') => (b'"', TokenKind::QuotedIdentifier),
        Some(b'`') => (b'`', TokenKind::QuotedIdentifier),
        Some(b'[') => (b']', TokenKind::QuotedIdentifier),
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
    while let Some(&byte) = bytes.get(index) {
        if byte == quote {
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

    fn previous_end(&self) -> Result<usize, String> {
        Ok(self.previous_token()?.span.end)
    }

    fn previous_token(&self) -> Result<&Token, String> {
        self.index
            .checked_sub(1)
            .and_then(|index| self.tokens.get(index))
            .ok_or_else(|| "cursor has no previously consumed token".to_string())
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
    use super::{ColumnChange, DefaultChange, KeyKind, parse_create_index, parse_create_table};

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
    fn rebuild_rewrite_normalizes_adjacent_column_removals() {
        let source = "CREATE TABLE t(id INTEGER PRIMARY KEY, p TEXT DEFAULT 'keep', a TEXT, b TEXT, UNIQUE (p))";
        let parsed = parse_create_table(source).expect("source should parse");
        let rewritten = parsed
            .rewrite_rebuild(
                &[ColumnChange {
                    name: "p".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                &["a".to_string(), "b".to_string()],
                "__replacement",
            )
            .expect("adjacent selected drops should reconstruct safely");
        assert_eq!(
            rewritten,
            "CREATE TABLE main.\"__replacement\"(id INTEGER PRIMARY KEY, p VARCHAR(9) DEFAULT 'keep', UNIQUE (p))"
        );
        assert!(parse_create_table(&rewritten).is_ok());
    }

    #[test]
    fn rebuild_rewrite_removes_leading_interior_and_trailing_drop_runs_without_touching_constraints()
     {
        let cases = [
            (
                "CREATE TABLE t(id INTEGER PRIMARY KEY, p TEXT, a TEXT, b TEXT, UNIQUE (p))",
                &["id", "p"] as &[_],
                "CREATE TABLE main.\"__replacement\"(a TEXT, b TEXT, UNIQUE (p))",
            ),
            (
                "CREATE TABLE t(id INTEGER PRIMARY KEY, p TEXT, a TEXT, b TEXT, UNIQUE (p))",
                &["p", "a"] as &[_],
                "CREATE TABLE main.\"__replacement\"(id INTEGER PRIMARY KEY, b TEXT, UNIQUE (p))",
            ),
            (
                "CREATE TABLE t(id INTEGER PRIMARY KEY, p TEXT, a TEXT, b TEXT, UNIQUE (p))",
                &["a", "b"] as &[_],
                "CREATE TABLE main.\"__replacement\"(id INTEGER PRIMARY KEY, p TEXT, UNIQUE (p))",
            ),
        ];

        for (source, drops, expected) in cases {
            let parsed = parse_create_table(source).expect("source should parse");
            let rewritten = parsed
                .rewrite_rebuild(
                    &[],
                    &drops.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    "__replacement",
                )
                .expect("adjacent drop run should not overlap");
            assert_eq!(rewritten, expected);
            assert!(parse_create_table(&rewritten).is_ok());
        }
    }

    #[test]
    fn preserves_key_term_collation_and_direction_details() {
        let parsed = parse_create_table(
            "CREATE TABLE keyed (\
                id INTEGER PRIMARY KEY,\
                inline_unique TEXT COLLATE NOCASE UNIQUE,\
                a TEXT COLLATE RTRIM,\
                b TEXT,\
                PRIMARY KEY (a COLLATE RTRIM DESC, b ASC),\
                UNIQUE (a COLLATE BINARY DESC, b ASC)\
            )",
        );
        assert!(
            parsed.is_err(),
            "mixed inline and table PRIMARY KEY clauses must reject"
        );

        let parsed = parse_create_table(
            "CREATE TABLE keyed (\
                inline_unique TEXT COLLATE NOCASE UNIQUE,\
                a TEXT COLLATE RTRIM,\
                b TEXT,\
                PRIMARY KEY (a COLLATE RTRIM DESC, b ASC),\
                UNIQUE (a COLLATE BINARY DESC, b ASC)\
            )",
        )
        .expect("bounded key terms should parse");
        let declarations = parsed.key_declarations();
        assert!(declarations.iter().any(|declaration| {
            declaration.kind == KeyKind::Unique
                && declaration.terms.len() == 1
                && declaration.terms[0].column == "inline_unique"
                && declaration.terms[0].explicit_collation.is_none()
        }));
        assert!(declarations.iter().any(|declaration| {
            declaration.kind == KeyKind::PrimaryKey
                && declaration.terms[0].column == "a"
                && declaration.terms[0].explicit_collation.as_deref() == Some("RTRIM")
                && declaration.terms[0].descending
                && !declaration.terms[1].descending
        }));
        assert!(declarations.iter().any(|declaration| {
            declaration.kind == KeyKind::Unique
                && declaration.terms.len() == 2
                && declaration.terms[0].explicit_collation.as_deref() == Some("BINARY")
                && declaration.terms[0].descending
        }));

        let index = parse_create_index(
            "CREATE UNIQUE INDEX \"WHERE\" ON main.keyed(a COLLATE RTRIM DESC, b ASC)",
        )
        .expect("quoted index names must not be parsed as SQL keywords");
        assert_eq!(index.name, "WHERE");
        assert_eq!(index.target_table, "keyed");
        assert!(index.unique);
        assert!(index.keys[0].descending);
        assert_eq!(index.keys[0].explicit_collation.as_deref(), Some("RTRIM"));
        assert!(
            parse_create_index("CREATE INDEX indexed ON keyed(a) WHERE a IS NOT NULL").is_err(),
            "partial index tails must consume completely and reject"
        );
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
