use super::*;

#[test]
fn an_expression_draws_in_its_own_tone_behind_the_marker() {
    let model = model_with(
        vec![text_spec("a"), text_spec("b")],
        vec![vec![
            CellValue::text("now()"),
            CellValue::expression("now()"),
        ]],
    );
    let edits = EditBuffer::new();
    let source = GridRowSource::new(&model, &edits);

    let literal = source.cell(0, 0).expect("literal");
    let expression = source.cell(0, 1).expect("expression");

    assert_eq!(literal.tone, CellTone::Normal);
    assert_eq!(expression.tone, CellTone::Expression);
    assert_eq!(
        expression.content,
        CellContent::Plain("\u{192}x now()".into())
    );
    assert_eq!(literal.content, CellContent::Plain("now()".into()));
}
