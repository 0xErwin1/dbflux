use super::{SqlFormatDialect, format_sql, format_sql_text, prepare};

fn inline(text: &str, dialect: SqlFormatDialect) -> String {
    format_sql(text, dialect)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn generic(text: &str) -> String {
    prepare(text, SqlFormatDialect::Generic)
        .expect("text can be prepared")
        .text
        .into_owned()
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
    let text = "select '=@x', \"a=@b\", `c=@d` -- g=@h\n/* i=@j */ where k=@l";
    assert_eq!(
        generic(text),
        "select '=@x', \"a=@b\", `c=@d` -- g=@h\n/* i=@j */ where k= @l"
    );
    let bracketed = prepare("select [e=@f] where k=@l", SqlFormatDialect::SqlServer)
        .expect("text can be prepared");
    assert_eq!(bracketed.text, "select [e=@f] where k= __dbflux_var_0__");
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
fn a_backslash_in_a_string_leaves_the_statement_unformatted() {
    for text in [
        "select 'a\\' , '=@x' where y=@z",
        "select \"mysql \\\"str=@x\\\"\" from t where y=@z",
        "select 'C:\\temp' from t where x=:id",
    ] {
        for dialect in [
            SqlFormatDialect::Generic,
            SqlFormatDialect::MySql,
            SqlFormatDialect::SqlServer,
        ] {
            assert!(prepare(text, dialect).is_none());
            assert_eq!(format_sql(text, dialect), text);
        }
    }
}

#[test]
fn a_postgres_array_subscript_is_code() {
    assert_eq!(generic("select ARRAY[1=:id]"), "select ARRAY[1= :id]");
    assert_eq!(
        inline("select ARRAY[1=:id]", SqlFormatDialect::Generic),
        "SELECT ARRAY [ 1 = :id ]"
    );
}

#[test]
fn text_without_a_glued_sigil_is_borrowed() {
    assert!(matches!(
        prepare("select a = @b", SqlFormatDialect::Generic)
            .expect("text can be prepared")
            .text,
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
fn text_that_already_holds_the_stand_in_prefix_is_not_formatted() {
    let text = "select '__dbflux_var_', @@rowcount";
    assert!(prepare(text, SqlFormatDialect::SqlServer).is_none());
    assert_eq!(format_sql(text, SqlFormatDialect::SqlServer), text);
}

#[test]
fn blank_text_is_not_formatted() {
    assert_eq!(format_sql_text(" \n\t", SqlFormatDialect::Generic), None);
}
