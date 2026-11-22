use super::{SqlFormatDialect, format_sql, format_sql_text, prepare};

fn inline(text: &str, dialect: SqlFormatDialect) -> String {
    format_sql(text, dialect)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn generic(text: &str) -> String {
    prepare(text, SqlFormatDialect::Generic).text.into_owned()
}

#[test]
fn a_variable_glued_to_an_operator_keeps_its_name() {
    for dialect in [
        SqlFormatDialect::Generic,
        SqlFormatDialect::MySql,
        SqlFormatDialect::SqlServer,
    ] {
        assert_eq!(
            inline("select x from t where x=@id", dialect),
            "SELECT x FROM t WHERE x = @id"
        );
        assert_eq!(
            inline("select x from t where x<>@id", dialect),
            "SELECT x FROM t WHERE x <> @id"
        );
        assert_eq!(inline("select a||@x", dialect), "SELECT a || @x");
    }
}

#[test]
fn a_named_parameter_glued_to_an_operator_keeps_its_name() {
    assert_eq!(
        inline("select x from t where x=:id", SqlFormatDialect::Generic),
        "SELECT x FROM t WHERE x = :id"
    );
    assert_eq!(
        inline("select x from t where x<=:id", SqlFormatDialect::Generic),
        "SELECT x FROM t WHERE x <= :id"
    );
}

#[test]
fn postgres_operators_that_contain_a_sigil_stay_joined() {
    assert_eq!(generic("a<@b"), "a<@b");
    assert_eq!(generic("a@>b"), "a@>b");
    assert_eq!(generic("a=:b::int"), "a= :b::int");
    assert_eq!(generic("a::int"), "a::int");
    assert_eq!(generic("@a:=@b"), "@a:= @b");
    assert_eq!(
        generic("select @-@ lseg '((0,0),(1,1))'"),
        "select @-@ lseg '((0,0),(1,1))'"
    );
}

#[test]
fn quoted_text_and_comments_are_left_alone() {
    let text = "select '=@x', \"a=@b\", `c=@d`, [e=@f] -- g=@h\n/* i=@j */ where k=@l";
    assert_eq!(
        generic(text),
        "select '=@x', \"a=@b\", `c=@d`, [e=@f] -- g=@h\n/* i=@j */ where k= @l"
    );
    assert_eq!(
        generic("select 'it''s =@x' where a=@b"),
        "select 'it''s =@x' where a= @b"
    );
}

#[test]
fn a_mysql_hash_comment_is_left_alone() {
    let formatted = format_sql("select 1 # a=@b", SqlFormatDialect::MySql);
    assert!(formatted.ends_with("# a=@b"), "{formatted:?}");
}

#[test]
fn dollar_quoted_bodies_are_left_alone_and_positional_parameters_are_code() {
    assert_eq!(
        generic("select $body$ a=@b $body$, $$c=@d$$ where e=$1"),
        "select $body$ a=@b $body$, $$c=@d$$ where e=$1"
    );
    assert_eq!(generic("where e=$1 and f=@g"), "where e=$1 and f= @g");
}

#[test]
fn a_backslash_in_a_string_leaves_the_text_unchanged() {
    for text in [
        "select 'a\\' , '=@x' where y=@z",
        "select \"mysql \\\"str=@x\\\"\" from t where y=@z",
    ] {
        assert_eq!(generic(text), text);
        assert_eq!(prepare(text, SqlFormatDialect::MySql).text, text);
    }
}

#[test]
fn text_without_a_glued_sigil_is_borrowed() {
    assert!(matches!(
        prepare("select a = @b", SqlFormatDialect::Generic).text,
        std::borrow::Cow::Borrowed(_)
    ));
}

#[test]
fn sql_server_keeps_bracketed_identifiers_and_variables() {
    assert_eq!(
        format_sql(
            "select [order id] from t where x=@id",
            SqlFormatDialect::SqlServer
        ),
        "SELECT\n  [order id]\nFROM\n  t\nWHERE\n  x = @id"
    );
    assert_eq!(
        format_sql("select * from t where id<@x", SqlFormatDialect::SqlServer),
        "SELECT\n  *\nFROM\n  t\nWHERE\n  id < @x"
    );
    assert_eq!(
        format_sql("select @@rowcount", SqlFormatDialect::SqlServer),
        "SELECT\n  @@rowcount"
    );
    assert_eq!(
        inline(
            "select a from t where b=@@identity",
            SqlFormatDialect::SqlServer
        ),
        "SELECT a FROM t WHERE b = @@identity"
    );
}

#[test]
fn mysql_keeps_user_and_system_variables() {
    assert_eq!(
        inline(
            "select @@session.sql_mode, @a from t where id<@x",
            SqlFormatDialect::MySql
        ),
        "SELECT @@session.sql_mode, @a FROM t WHERE id < @x"
    );
}

#[test]
fn text_that_already_holds_a_stand_in_gets_none() {
    let text = "select __dbflux_var_0__, @a";
    let prepared = prepare(text, SqlFormatDialect::MySql);
    assert_eq!(prepared.text, text);
}

#[test]
fn blank_text_is_not_formatted() {
    assert_eq!(format_sql_text(" \n\t", SqlFormatDialect::Generic), None);
}
