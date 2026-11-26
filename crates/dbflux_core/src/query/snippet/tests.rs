#![expect(
    clippy::single_range_in_vec_init,
    reason = "a tabstop holds a Vec of ranges, and one-range vectors are what the cases assert"
)]

use super::{SnippetParseErrorKind as Kind, parse_snippet, snippet_placeholder_spans};

fn stops(body: &str) -> Vec<(u32, Vec<std::ops::Range<usize>>)> {
    parse_snippet(body)
        .expect("body parses")
        .tabstops()
        .iter()
        .map(|stop| (stop.index, stop.ranges.clone()))
        .collect()
}

fn text(body: &str) -> String {
    parse_snippet(body).expect("body parses").text().to_string()
}

fn error(body: &str) -> (Kind, usize, usize) {
    let error = parse_snippet(body).expect_err("body is rejected");
    (error.kind, error.line, error.column)
}

#[test]
fn plain_text_has_only_the_implicit_final_stop() {
    assert_eq!(text("SELECT 1"), "SELECT 1");
    assert_eq!(stops("SELECT 1"), vec![(0, vec![8..8])]);
}

#[test]
fn bare_and_braced_indexes_are_empty_stops() {
    assert_eq!(text("a $1 b ${2} c"), "a  b  c");
    assert_eq!(
        stops("a $1 b ${2} c"),
        vec![(1, vec![2..2]), (2, vec![5..5]), (0, vec![7..7])]
    );
}

#[test]
fn default_text_is_expanded_and_ranged() {
    assert_eq!(text("FROM ${1:table};"), "FROM table;");
    assert_eq!(
        stops("FROM ${1:table};"),
        vec![(1, vec![5..10]), (0, vec![11..11])]
    );
}

#[test]
fn empty_default_is_an_empty_range() {
    assert_eq!(text("x${1:}y"), "xy");
    assert_eq!(stops("x${1:}y"), vec![(1, vec![1..1]), (0, vec![2..2])]);
}

#[test]
fn explicit_final_stop_replaces_the_implicit_one() {
    assert_eq!(stops("a;$0 b"), vec![(0, vec![2..2])]);
}

#[test]
fn navigation_order_is_ascending_then_final() {
    let order: Vec<u32> = parse_snippet("$0 ${3:c} ${1:a} ${2:b}")
        .expect("parses")
        .tabstops()
        .iter()
        .map(|stop| stop.index)
        .collect();
    assert_eq!(order, vec![1, 2, 3, 0]);
}

#[test]
fn repeated_index_mirrors_and_shares_the_default() {
    assert_eq!(text("${1:t} $1 ${1}"), "t t t");
    assert_eq!(
        stops("${1:t} $1 ${1}"),
        vec![(1, vec![0..1, 2..3, 4..5]), (0, vec![5..5])]
    );
}

#[test]
fn first_default_supplies_text_even_after_an_earlier_mirror() {
    assert_eq!(text("$1-${1:x}-${1:y}"), "x-x-x");
}

#[test]
fn cte_mirrors_the_first_index() {
    let body = "WITH ${1:name} AS (\n  ${2:SELECT 1}\n)\nSELECT *\nFROM $1;$0";
    assert_eq!(
        text(body),
        "WITH name AS (\n  SELECT 1\n)\nSELECT *\nFROM name;"
    );
    let parsed = parse_snippet(body).expect("parses");
    let first = &parsed.tabstops()[0];
    assert_eq!(first.index, 1);
    assert_eq!(first.ranges.len(), 2);
    for range in &first.ranges {
        assert_eq!(&parsed.text()[range.clone()], "name");
    }
    assert_eq!(parsed.tabstops()[1].index, 2);
    assert_eq!(parsed.tabstops()[2].index, 0);
    assert_eq!(
        parsed.tabstops()[2].ranges,
        vec![parsed.text().len()..parsed.text().len()]
    );
}

#[test]
fn builtin_bodies_parse() {
    let bodies = [
        (
            "SELECT ${2:*}\nFROM ${1:table}\nWHERE ${3:condition};$0",
            vec![1, 2, 3, 0],
        ),
        (
            "INSERT INTO ${1:table} (${2:columns})\nVALUES (${3:values});$0",
            vec![1, 2, 3, 0],
        ),
        (
            "UPDATE ${1:table}\nSET ${2:column} = ${3:value}\nWHERE ${4:condition};$0",
            vec![1, 2, 3, 4, 0],
        ),
        (
            "DELETE FROM ${1:table}\nWHERE ${2:condition};$0",
            vec![1, 2, 0],
        ),
        ("SELECT COUNT(*)\nFROM ${1:table};$0", vec![1, 0]),
    ];
    for (body, expected) in bodies {
        let parsed = parse_snippet(body).expect("built-in parses");
        let order: Vec<u32> = parsed.tabstops().iter().map(|stop| stop.index).collect();
        assert_eq!(order, expected, "{body}");
        for stop in parsed.tabstops() {
            for range in &stop.ranges {
                assert!(parsed.text().is_char_boundary(range.start));
                assert!(parsed.text().is_char_boundary(range.end));
            }
        }
    }
    assert_eq!(
        text("SELECT ${2:*}\nFROM ${1:table}\nWHERE ${3:condition};$0"),
        "SELECT *\nFROM table\nWHERE condition;"
    );
}

#[test]
fn escapes_produce_literal_text() {
    assert_eq!(text(r"cost \$5 \} \\ done"), r"cost $5 } \ done");
    assert_eq!(stops(r"\$1"), vec![(0, vec![2..2])]);
    assert_eq!(text(r"${1:a\}b\$c\\d}"), r"a}b$c\d");
}

#[test]
fn unknown_backslash_sequences_stay_literal() {
    assert_eq!(text(r"a\nb"), r"a\nb");
    assert_eq!(text(r"end\"), r"end\");
}

#[test]
fn bare_dollar_is_literal() {
    assert_eq!(text("$ 5$"), "$ 5$");
    assert_eq!(text("a $- b $"), "a $- b $");
    assert_eq!(text("${1:$ x}"), "$ x");
}

#[test]
fn multibyte_text_gets_byte_ranges() {
    assert_eq!(text("é${1:ñ}"), "éñ");
    assert_eq!(stops("é${1:ñ}"), vec![(1, vec![2..4]), (0, vec![4..4])]);
}

#[test]
fn unclosed_placeholder_is_positioned_at_its_dollar() {
    assert_eq!(
        error("SELECT 1\nFROM ${1:table"),
        (Kind::UnclosedPlaceholder, 2, 6)
    );
    assert_eq!(error("${1"), (Kind::UnclosedPlaceholder, 1, 1));
    assert_eq!(error("a${"), (Kind::UnclosedPlaceholder, 1, 2));
}

#[test]
fn unclosed_error_carries_an_excerpt() {
    let failure = parse_snippet("SELECT 1\nFROM ${1:table").expect_err("rejected");
    assert_eq!(failure.excerpt, "${1:table");
    assert_eq!(
        failure.to_string(),
        "Unclosed placeholder at line 2, column 6: ${1:table"
    );
}

#[test]
fn nested_placeholder_is_rejected_at_the_inner_dollar() {
    assert_eq!(error("${1:a${2:b}}"), (Kind::NestedPlaceholder, 1, 6));
    assert_eq!(error("x\n${1:$2}"), (Kind::NestedPlaceholder, 2, 5));
}

#[test]
fn choices_are_rejected() {
    assert_eq!(error("${1|a,b|}"), (Kind::UnsupportedChoice, 1, 1));
}

#[test]
fn variables_are_rejected() {
    assert_eq!(error("a $NAME"), (Kind::UnsupportedVariable, 1, 3));
    assert_eq!(error("${NAME}"), (Kind::UnsupportedVariable, 1, 1));
    assert_eq!(error("${_x:y}"), (Kind::UnsupportedVariable, 1, 1));
    assert_eq!(error("${1:$name}"), (Kind::UnsupportedVariable, 1, 5));
}

#[test]
fn malformed_placeholders_are_rejected() {
    assert_eq!(error("${}"), (Kind::InvalidPlaceholder, 1, 1));
    assert_eq!(error("${1x}"), (Kind::InvalidPlaceholder, 1, 1));
    assert_eq!(error("${99999999999}"), (Kind::InvalidPlaceholder, 1, 1));
}

#[test]
fn crlf_counts_one_line_break() {
    assert_eq!(
        error("a\r\nb\r\n  ${1:x"),
        (Kind::UnclosedPlaceholder, 3, 3)
    );
    assert_eq!(text("a\r\n${1:x}"), "a\r\nx");
}

#[test]
fn columns_count_characters_not_bytes() {
    assert_eq!(error("éé${1"), (Kind::UnclosedPlaceholder, 1, 3));
}

#[test]
fn ten_is_one_index_and_braces_split_it() {
    assert_eq!(stops("$10"), vec![(10, vec![0..0]), (0, vec![0..0])]);
    assert_eq!(stops("${1}0"), vec![(1, vec![0..0]), (0, vec![1..1])]);
}

#[test]
fn empty_default_and_trailing_dollar() {
    assert_eq!(stops("a${1:}b"), vec![(1, vec![1..1]), (0, vec![2..2])]);
    assert_eq!(text("SELECT $"), "SELECT $");
    assert_eq!(text("$$"), "$$");
}

#[test]
fn unicode_before_a_field_and_escaped_backslash() {
    assert_eq!(stops("é ${1:x}"), vec![(1, vec![3..4]), (0, vec![4..4])]);
    assert_eq!(error("é ${1"), (Kind::UnclosedPlaceholder, 1, 3));
    assert_eq!(stops("\\\\${1:a}"), vec![(1, vec![1..2]), (0, vec![2..2])]);
    assert_eq!(text("${1:a\\}b}"), "a}b");
}

/// Each span as the raw text of its token and of its default.
fn spans(body: &str) -> Vec<(&str, Option<&str>)> {
    snippet_placeholder_spans(body)
        .into_iter()
        .map(|span| {
            (
                &body[span.range],
                span.default.map(|default| &body[default]),
            )
        })
        .collect()
}

#[test]
fn placeholder_spans_cover_every_built_in_body() {
    let sel = "SELECT ${2:*}\nFROM ${1:table}\nWHERE ${3:condition};$0";
    assert_eq!(
        spans(sel),
        vec![
            ("${2:*}", Some("*")),
            ("${1:table}", Some("table")),
            ("${3:condition}", Some("condition")),
            ("$0", None),
        ]
    );

    let ins = "INSERT INTO ${1:table} (${2:columns})\nVALUES (${3:values});$0";
    assert_eq!(spans(ins).len(), 4);

    let upd = "UPDATE ${1:table}\nSET ${2:column} = ${3:value}\nWHERE ${4:condition};$0";
    assert_eq!(spans(upd).len(), 5);

    let del = "DELETE FROM ${1:table}\nWHERE ${2:condition};$0";
    assert_eq!(spans(del).len(), 3);

    let cte = "WITH ${1:name} AS (\n  ${2:SELECT 1}\n)\nSELECT *\nFROM $1;$0";
    assert_eq!(
        spans(cte),
        vec![
            ("${1:name}", Some("name")),
            ("${2:SELECT 1}", Some("SELECT 1")),
            ("$1", None),
            ("$0", None),
        ]
    );

    let cnt = "SELECT COUNT(*)\nFROM ${1:table};$0";
    assert_eq!(
        spans(cnt),
        vec![("${1:table}", Some("table")), ("$0", None)]
    );
}

#[test]
fn placeholder_spans_are_byte_ranges_of_the_raw_body() {
    let body = "ab${1:xy}$0";
    let found = snippet_placeholder_spans(body);
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].range, 2..9);
    assert_eq!(found[0].default, Some(6..8));
    assert_eq!(found[1].range, 9..11);
    assert_eq!(found[1].default, None);
}

#[test]
fn placeholder_spans_accept_braced_indexes_without_a_default() {
    assert_eq!(spans("a ${2} b"), vec![("${2}", None)]);
    assert_eq!(spans("${1:}"), vec![("${1:}", Some(""))]);
}

#[test]
fn placeholder_spans_skip_escapes() {
    assert_eq!(spans(r"cost \$5 ${1:a}"), vec![("${1:a}", Some("a"))]);
    assert_eq!(spans(r"\${1:a}"), vec![]);
    assert_eq!(spans(r"\\$1"), vec![("$1", None)]);
    assert_eq!(spans(r"${1:a\}b}"), vec![(r"${1:a\}b}", Some(r"a\}b"))]);
    assert_eq!(spans(r"\$1 \$0"), vec![]);
}

#[test]
fn placeholder_spans_read_multi_digit_indexes() {
    assert_eq!(
        spans("$10 and ${11:x}"),
        vec![("$10", None), ("${11:x}", Some("x"))]
    );
}

#[test]
fn placeholder_spans_return_what_precedes_an_unclosed_placeholder() {
    assert_eq!(spans("${1:a} ${2:abc"), vec![("${1:a}", Some("a"))]);
    assert_eq!(spans("${1"), vec![]);
    assert_eq!(spans("$1 ${"), vec![("$1", None)]);
    assert_eq!(spans("${"), vec![]);
    assert_eq!(spans("$"), vec![]);
    assert_eq!(spans("a\\"), vec![]);
}

#[test]
fn placeholder_spans_skip_choices_and_variables_without_failing() {
    assert_eq!(spans("${1|a,b|} $NAME ${NAME} $1"), vec![("$1", None)]);
    assert_eq!(spans("$ $x $"), vec![]);
}

#[test]
fn placeholder_spans_count_bytes_after_unicode_text() {
    let body = "-- ñandú ✓ ${1:tabla}";
    let found = snippet_placeholder_spans(body);
    assert_eq!(found.len(), 1);
    assert_eq!(&body[found[0].range.clone()], "${1:tabla}");
    let default = found[0].default.clone().expect("default");
    assert_eq!(&body[default], "tabla");
    assert!(body.is_char_boundary(found[0].range.start));
}
