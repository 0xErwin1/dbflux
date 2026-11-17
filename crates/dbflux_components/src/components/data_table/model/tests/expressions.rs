use super::*;

#[test]
fn an_expression_is_not_the_text_it_spells() {
    let expression = CellValue::expression("now()");
    let text = CellValue::text("now()");

    assert!(!expression.holds_same_value(&text));
    assert!(!text.holds_same_value(&expression));
    assert!(expression.holds_same_value(&CellValue::expression("now()")));
    assert!(!expression.holds_same_value(&CellValue::expression("CURRENT_DATE")));
    assert!(!expression.holds_same_value(&CellValue::auto_generated("now()")));
}

#[test]
fn an_expression_cell_exposes_its_text_and_copies_it_raw() {
    let cell = CellValue::expression("gen_random_uuid()");

    assert_eq!(cell.as_expression(), Some("gen_random_uuid()"));
    assert_eq!(cell.edit_text(), "gen_random_uuid()");
    assert_eq!(
        crate::components::data_table::clipboard::format_cell(&cell),
        "gen_random_uuid()"
    );
    assert_eq!(CellValue::text("now()").as_expression(), None);
}

#[test]
fn staging_an_expression_over_the_same_text_is_a_change() {
    let model = keyed_model(&[1]);
    let mut buffer = EditBuffer::new();
    buffer.set_cell(0, 1, CellValue::expression("original"));

    assert!(buffer.is_cell_dirty(0, 1));
    assert!(
        !buffer
            .get_cell(0, 1, model.cell(0, 1).unwrap())
            .holds_same_value(model.cell(0, 1).unwrap())
    );
}

/// The base model holds what the database holds. An expression is not a
/// value yet, so a save that cannot place the returned row leaves the cell
/// as it was instead of copying the expression into the base.
#[test]
fn keeping_row_edits_does_not_copy_an_expression_into_the_base() {
    let model = keyed_model(&[1]);
    let mut buffer = EditBuffer::new();
    buffer.set_cell(0, 0, CellValue::expression("nextval('s')"));
    buffer.set_cell(0, 1, CellValue::text("edited"));

    let kept = model.with_row_edits(0, &buffer);

    assert_eq!(kept.cell(0, 0).unwrap().as_expression(), None);
    assert_eq!(
        kept.cell(0, 0).unwrap().to_value(),
        dbflux_core::Value::Int(1)
    );
    assert_eq!(kept.cell(0, 1).unwrap().display_text().as_ref(), "edited");
}
