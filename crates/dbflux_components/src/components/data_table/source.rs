//! The data grid's model and edit buffer as a [`bolt::data::RowSource`].
//!
//! Nothing here reformats a value: every cell text is the display string the
//! model's `CellValue` already computed, and every tone follows the same
//! precedence the grid's cell painter applies today. The renderer slice that
//! moves onto `bolt::data` reads through this adapter and keeps today's
//! pixels.

use bolt::data::{
    CellAlignment, CellTone, CellView, ColumnKind, ColumnView, RowSource, RowVisualState,
};
use dbflux_core::RowState;

use super::model::{CellValue, EditBuffer, TableModel, VisualRowSource};

/// The grid's rows and columns as a renderer asks for them.
///
/// Rows are addressed by their visual index: the base rows of the model with
/// the edit buffer's pending inserts interleaved where the buffer puts them,
/// which is the order the grid paints. A cell read on a base row applies the
/// buffer's staged override, exactly as the grid draws it.
pub struct GridRowSource<'a> {
    model: &'a TableModel,
    edits: &'a EditBuffer,
    visual_order: Vec<VisualRowSource>,
}

impl<'a> GridRowSource<'a> {
    pub fn new(model: &'a TableModel, edits: &'a EditBuffer) -> Self {
        // `compute_visual_order` interleaves pending inserts around the base
        // row count the buffer was rebased onto. A buffer that addresses a
        // different model would map visual rows onto unrelated rows, so such
        // a pair reads the base rows only.
        let visual_order = if edits.base_row_count() == model.row_count() {
            edits.compute_visual_order()
        } else {
            (0..model.row_count()).map(VisualRowSource::Base).collect()
        };

        Self {
            model,
            edits,
            visual_order,
        }
    }
}

/// The cell as `bolt::data` expresses it: the model's display text plus the
/// tone precedence the grid painter applies — a missing field and a nested
/// summary come first, a pending delete mutes the row's cells, then a column
/// default or a placeholder legend, then null, then a staged edit.
fn cell_view(
    cell: &CellValue,
    row_state: &RowState,
    is_dirty: bool,
    align: CellAlignment,
) -> CellView {
    let tone = if cell.is_missing() {
        CellTone::Missing
    } else if cell.is_nested() {
        CellTone::Nested
    } else if row_state.is_pending_delete() {
        CellTone::PendingDelete
    } else if cell.is_auto_generated() || cell.is_placeholder() {
        CellTone::Muted
    } else if cell.is_null() {
        CellTone::Null
    } else if is_dirty {
        CellTone::Dirty
    } else {
        CellTone::Normal
    };

    CellView::new(cell.display_text(), tone, align)
}

/// The visual state of one base row. The save-failure message stays with the
/// data owner; the visual state is what a renderer paints from.
fn visual_state(state: &RowState) -> RowVisualState {
    match state {
        RowState::Clean => RowVisualState::Clean,
        RowState::Dirty => RowVisualState::Dirty,
        RowState::Saving => RowVisualState::Saving,
        RowState::Error(_) => RowVisualState::Error,
        RowState::PendingInsert => RowVisualState::PendingInsert,
        RowState::PendingDelete => RowVisualState::PendingDelete,
    }
}

/// The grid's column classification onto Bolt's. The grid's `Bytes` and
/// `Json` kinds both draw as text, which is the only thing `bolt::data`
/// promises; nothing finer is needed to render.
fn bolt_kind(kind: super::model::ColumnKind) -> ColumnKind {
    match kind {
        super::model::ColumnKind::Text => ColumnKind::Text,
        super::model::ColumnKind::Integer => ColumnKind::Integer,
        super::model::ColumnKind::Float => ColumnKind::Float,
        super::model::ColumnKind::Bool => ColumnKind::Boolean,
        super::model::ColumnKind::Bytes | super::model::ColumnKind::Json => ColumnKind::Text,
        super::model::ColumnKind::Unknown => ColumnKind::Unknown,
    }
}

impl RowSource for GridRowSource<'_> {
    fn row_count(&self) -> usize {
        self.visual_order.len()
    }

    fn column_count(&self) -> usize {
        self.model.col_count()
    }

    fn column(&self, index: usize) -> Option<ColumnView> {
        let spec = self.model.columns.get(index)?;

        Some(
            ColumnView::new(spec.title.clone(), bolt_kind(spec.kind))
                .with_type_name(spec.type_name.clone()),
        )
    }

    fn cell(&self, row: usize, column: usize) -> Option<CellView> {
        // A column past the count has no view, like any other out-of-range
        // index; the null fallback below only covers a ragged row.
        if column >= self.column_count() {
            return None;
        }

        let align = self
            .model
            .columns
            .get(column)
            .map(|spec| CellAlignment::for_kind(bolt_kind(spec.kind)))
            .unwrap_or(CellAlignment::Left);

        match self.visual_order.get(row)? {
            VisualRowSource::Base(base_row) => {
                let null_cell = CellValue::null();
                let base = self.model.cell(*base_row, column).unwrap_or(&null_cell);
                let display = self.edits.get_cell(*base_row, column, base);

                Some(cell_view(
                    display,
                    self.edits.row_state(*base_row),
                    self.edits.is_cell_dirty(*base_row, column),
                    align,
                ))
            }
            VisualRowSource::Insert(insert_idx) => {
                let null_cell = CellValue::null();
                let cell = self
                    .edits
                    .get_pending_insert_by_idx(*insert_idx)
                    .and_then(|data| data.get(column))
                    .unwrap_or(&null_cell);

                // A pending insert has no cell-level dirty tracking; its whole
                // row is new, matching the grid's rules.
                Some(cell_view(cell, &RowState::PendingInsert, false, align))
            }
        }
    }

    fn row_state(&self, row: usize) -> RowVisualState {
        match self.visual_order.get(row) {
            Some(VisualRowSource::Base(base_row)) => visual_state(self.edits.row_state(*base_row)),
            Some(VisualRowSource::Insert(_)) => RowVisualState::PendingInsert,
            None => RowVisualState::Clean,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::{ColumnKind as GridColumnKind, ColumnSpec, RowData};
    use super::*;
    use chrono::{NaiveTime, TimeZone, Utc};
    use dbflux_core::Value;
    use gpui::TextAlign;

    fn spec(title: &str, kind: GridColumnKind) -> ColumnSpec {
        let align = match kind {
            GridColumnKind::Integer | GridColumnKind::Float => TextAlign::Right,
            _ => TextAlign::Left,
        };

        ColumnSpec {
            id: title.into(),
            title: title.into(),
            kind,
            align,
            type_name: "int4".into(),
        }
    }

    fn text_spec(title: &str) -> ColumnSpec {
        spec(title, GridColumnKind::Text)
    }

    fn model_with(columns: Vec<ColumnSpec>, rows: Vec<Vec<CellValue>>) -> TableModel {
        TableModel::new(
            columns,
            rows.into_iter().map(|cells| RowData { cells }).collect(),
        )
    }

    /// Asserts the adapter draws `value` exactly as the model's current
    /// formatting path (`CellValue::from`) produces it, with the tone and
    /// alignment the grid gives that value today.
    fn assert_value_draws_as_today(value: Value, tone: CellTone) {
        let model = model_with(vec![text_spec("v")], vec![vec![CellValue::from(&value)]]);
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        let cell = source.cell(0, 0).expect("cell exists");
        let expected = CellValue::from(&value).display_text();

        assert_eq!(
            cell.text.as_ref(),
            expected.as_ref(),
            "the adapter must draw {value:?} exactly as the model's formatting path does"
        );
        assert_eq!(cell.tone, tone, "{value:?} draws with tone {tone:?}");
        assert_eq!(
            cell.align,
            CellAlignment::Left,
            "the single text column aligns left"
        );
    }

    // One test per `Value` variant the grid can display: the adapter's text
    // must be what today's formatting path produces.
    #[test]
    fn null_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Null, CellTone::Null);
    }

    #[test]
    fn bool_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Bool(true), CellTone::Normal);
        assert_value_draws_as_today(Value::Bool(false), CellTone::Normal);
    }

    #[test]
    fn int_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Int(-42), CellTone::Normal);
    }

    #[test]
    fn float_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Float(2.5), CellTone::Normal);
    }

    #[test]
    fn text_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Text("héllo world".to_string()), CellTone::Normal);
    }

    #[test]
    fn bytes_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]), CellTone::Normal);
    }

    #[test]
    fn decimal_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Decimal("12.3400".to_string()), CellTone::Normal);
    }

    #[test]
    fn object_id_value_displays_as_the_model_does() {
        assert_value_draws_as_today(
            Value::ObjectId("507f1f77bcf86cd799439011".to_string()),
            CellTone::Normal,
        );
    }

    #[test]
    fn unsupported_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Unsupported("geometry".to_string()), CellTone::Normal);
    }

    #[test]
    fn json_value_displays_as_the_model_does() {
        assert_value_draws_as_today(Value::Json(r#"{"a":1}"#.to_string()), CellTone::Normal);
    }

    #[test]
    fn array_value_displays_as_the_model_does() {
        assert_value_draws_as_today(
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            CellTone::Normal,
        );
    }

    #[test]
    fn document_value_displays_as_the_model_does() {
        let mut doc = std::collections::BTreeMap::new();
        doc.insert("b".to_string(), Value::Int(2));
        doc.insert("a".to_string(), Value::Text("x".to_string()));
        assert_value_draws_as_today(Value::Document(doc), CellTone::Normal);
    }

    // Timestamps and floats carry the formatting rules (precision, timezone,
    // trailing fraction) a careless adapter would silently change, so they
    // pin exact literals.
    #[test]
    fn timestamp_displays_in_utc_without_subsecond_digits() {
        let dt = Utc.with_ymd_and_hms(2024, 3, 1, 14, 30, 5).unwrap();
        let model = model_with(
            vec![text_spec("v")],
            vec![vec![CellValue::from(&Value::DateTime(dt))]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        assert_eq!(
            source.cell(0, 0).expect("cell").text,
            "2024-03-01 14:30:05",
            "timestamps draw as UTC `%Y-%m-%d %H:%M:%S`, no timezone suffix, no fractional part"
        );
    }

    #[test]
    fn date_displays_as_iso_day() {
        let date = Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap();
        let model = model_with(
            vec![text_spec("v")],
            vec![vec![CellValue::from(&Value::Date(date.date_naive()))]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.cell(0, 0).expect("cell").text, "2024-03-01");
    }

    #[test]
    fn time_displays_as_wall_clock_without_subsecond_digits() {
        let model = model_with(
            vec![text_spec("v")],
            vec![vec![CellValue::from(&Value::Time(
                NaiveTime::from_hms_opt(9, 5, 3).unwrap(),
            ))]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.cell(0, 0).expect("cell").text, "09:05:03");
    }

    #[test]
    fn whole_floats_keep_one_fraction_digit() {
        let model = model_with(
            vec![text_spec("v")],
            vec![vec![CellValue::from(&Value::Float(2.0))]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        assert_eq!(
            source.cell(0, 0).expect("cell").text,
            "2.0",
            "a whole float draws with one fraction digit so it never reads as an integer"
        );
    }

    #[test]
    fn fractional_floats_draw_their_exact_value() {
        let model = model_with(
            vec![text_spec("v")],
            vec![vec![CellValue::from(&Value::Float(1234.5678))]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.cell(0, 0).expect("cell").text, "1234.5678");
    }

    #[test]
    fn auto_generated_and_placeholder_legends_read_muted() {
        let model = model_with(
            vec![text_spec("v"), text_spec("w")],
            vec![vec![
                CellValue::auto_generated("nextval('t_id_seq')"),
                CellValue::placeholder("auto"),
            ]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.cell(0, 0).expect("cell").tone, CellTone::Muted);
        assert_eq!(source.cell(0, 1).expect("cell").tone, CellTone::Muted);
    }

    #[test]
    fn missing_and_nested_cells_take_their_own_tones() {
        let model = model_with(
            vec![text_spec("v"), text_spec("w")],
            vec![vec![CellValue::missing(), CellValue::nested(false, 3)]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.cell(0, 0).expect("cell").tone, CellTone::Missing);
        assert_eq!(source.cell(0, 1).expect("cell").tone, CellTone::Nested);
    }

    #[test]
    fn staged_cell_reads_dirty_until_committed() {
        let model = model_with(
            vec![text_spec("v"), text_spec("w")],
            vec![vec![CellValue::text("a"), CellValue::text("b")]],
        );
        let mut edits = EditBuffer::new();
        edits.set_base_row_count(model.row_count());
        edits.set_cell(0, 1, CellValue::text("B"));

        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.cell(0, 0).expect("cell").tone, CellTone::Normal);
        assert_eq!(source.cell(0, 1).expect("cell").tone, CellTone::Dirty);
        assert_eq!(
            source.cell(0, 1).expect("cell").text,
            "B",
            "the staged override is what the renderer draws"
        );
    }

    #[test]
    fn a_pending_delete_row_mutes_its_cells() {
        let model = model_with(vec![text_spec("v")], vec![vec![CellValue::text("a")]]);
        let mut edits = EditBuffer::new();
        edits.set_base_row_count(model.row_count());
        edits.set_row_state(0, RowState::PendingDelete);

        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.row_state(0), RowVisualState::PendingDelete);
        assert_eq!(
            source.cell(0, 0).expect("cell").tone,
            CellTone::PendingDelete
        );
    }

    #[test]
    fn every_row_state_maps_to_its_visual_state() {
        let model = model_with(vec![text_spec("v")], vec![vec![CellValue::text("a")]; 5]);
        let mut edits = EditBuffer::new();
        edits.set_base_row_count(model.row_count());

        let cases = [
            (RowState::Clean, RowVisualState::Clean),
            (RowState::Dirty, RowVisualState::Dirty),
            (RowState::Saving, RowVisualState::Saving),
            (RowState::Error("boom".to_string()), RowVisualState::Error),
            (RowState::PendingDelete, RowVisualState::PendingDelete),
        ];

        for (state, expected) in cases {
            edits.set_row_state(0, state.clone());
            let source = GridRowSource::new(&model, &edits);
            assert_eq!(
                source.row_state(0),
                expected,
                "{state:?} must map to {expected:?}"
            );
        }
    }

    #[test]
    fn a_pending_insert_row_is_pending_and_reads_the_buffered_cells() {
        let model = model_with(vec![text_spec("v")], vec![vec![CellValue::text("a")]]);
        let mut edits = EditBuffer::new();
        edits.set_base_row_count(model.row_count());
        edits.add_pending_insert(vec![CellValue::text("new")]);

        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.row_count(), 2);
        assert_eq!(source.row_state(0), RowVisualState::Clean);
        assert_eq!(source.row_state(1), RowVisualState::PendingInsert);
        assert_eq!(source.cell(1, 0).expect("cell").text, "new");
    }

    #[test]
    fn columns_carry_name_kind_alignment_and_type_name() {
        let model = model_with(
            vec![
                spec("n", GridColumnKind::Integer),
                spec("s", GridColumnKind::Text),
                spec("j", GridColumnKind::Json),
                spec("b", GridColumnKind::Bool),
            ],
            vec![vec![CellValue::int(1); 4]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        let n = source.column(0).expect("column");
        assert_eq!(n.name, "n");
        assert_eq!(n.kind, ColumnKind::Integer);
        assert_eq!(n.align, CellAlignment::Right);
        assert_eq!(n.type_name, "int4");

        assert_eq!(source.column(1).expect("column").align, CellAlignment::Left);
        assert_eq!(
            source.column(2).expect("column").kind,
            ColumnKind::Text,
            "json draws as text"
        );
        assert_eq!(source.column(3).expect("column").kind, ColumnKind::Boolean);
    }

    #[test]
    fn out_of_range_indexes_answer_defaulted_values() {
        let model = model_with(vec![text_spec("v")], vec![vec![CellValue::text("a")]]);
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        assert_eq!(source.row_count(), 1);
        assert_eq!(source.column_count(), 1);
        assert_eq!(source.column(1), None);
        assert_eq!(source.cell(1, 0), None);
        assert_eq!(source.cell(0, 1), None);
        assert_eq!(source.row_state(1), RowVisualState::Clean);
    }
}
