//! The data grid's model and edit buffer as a [`bolt::data::RowSource`].
//!
//! Nothing here reformats a value: every cell text is the display string the
//! model's `CellValue` already computed, and every tone follows the same
//! precedence the grid's cell painter applies today. The facts a cell draws
//! beyond one text — the two-tone foreign-key label, the document grid's
//! `old → new` transition, the nested marker, the group wash on a column —
//! are filled exactly as the painter derives them, from the model's own
//! methods. The renderer slice that moves onto `bolt::data` reads through
//! this adapter and keeps today's pixels.

use bolt::data::{
    CellAlignment, CellContent, CellSpan, CellTone, CellView, ColumnKind, ColumnView, NestedMarker,
    RowSource, RowVisualState,
};
use dbflux_core::RowState;

use super::document::DocumentPresentation;
use super::model::{CellKind, CellValue, EditBuffer, TableModel, VisualRowSource};

/// The grid's rows and columns as a renderer asks for them.
///
/// Rows are addressed by their visual index: the base rows of the model with
/// the edit buffer's pending inserts interleaved where the buffer puts them,
/// which is the order the grid paints. A cell read on a base row applies the
/// buffer's staged override, exactly as the grid draws it.
pub struct GridRowSource<'a> {
    model: &'a TableModel,
    edits: &'a EditBuffer,
    /// The document extras, when the grid shows documents. The painter
    /// derives the `old → new` transition and the group wash from whether
    /// a presentation is present, so the adapter does too.
    document: Option<&'a DocumentPresentation>,
    visual_order: Vec<VisualRowSource>,
}

impl<'a> GridRowSource<'a> {
    /// The grid as a relational table draws it: no document transitions,
    /// no group wash.
    pub fn new(model: &'a TableModel, edits: &'a EditBuffer) -> Self {
        Self::with_document(model, edits, None)
    }

    /// The grid as a document presentation draws it: staged edits show as
    /// `old → new` transitions and grouped columns wash their cells.
    pub fn with_document(
        model: &'a TableModel,
        edits: &'a EditBuffer,
        document: Option<&'a DocumentPresentation>,
    ) -> Self {
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
            document,
            visual_order,
        }
    }
}

/// The cell as `bolt::data` expresses it: the model's display text plus the
/// tone precedence the grid painter applies — a missing field and a nested
/// summary come first, a pending delete mutes the row's cells, then a column
/// default or a placeholder legend, then null, then a staged edit.
///
/// `replaced` is the value a document grid draws the staged edit against;
/// `None` draws the cell's own text plain.
fn cell_view(
    cell: &CellValue,
    replaced: Option<&CellValue>,
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

    // A resolved foreign key draws its label with the raw id muted after
    // it, and that pair replaces whatever text the cell would otherwise
    // draw — including a transition's `old → new`, which the painter only
    // composes when no spans are present.
    let (content, secondary) = match cell.fk_spans() {
        Some((label, id_suffix)) => (
            CellContent::Plain(label.into()),
            Some(CellSpan {
                text: id_suffix.into(),
                tone: CellTone::Muted,
            }),
        ),
        None => (
            match replaced {
                Some(base) => CellContent::Transition {
                    from: base.display_text().into(),
                    to: cell.display_text().into(),
                },
                None => CellContent::Plain(cell.display_text().into()),
            },
            None,
        ),
    };

    // The marker the painter derives from the kind: a nested summary draws
    // a step-in affordance, an array's different from a document's.
    let nested = match cell.kind {
        CellKind::Nested { is_array, .. } => Some(NestedMarker { is_array }),
        _ => None,
    };

    CellView {
        content,
        tone,
        align,
        secondary,
        nested,
    }
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
        // Same derivation as the painter: the wash follows from a document
        // presentation being present and the column belonging to a group.
        let group_child = self
            .document
            .is_some_and(|document| document.is_group_child(index));

        Some(
            ColumnView::new(spec.title.clone(), bolt_kind(spec.kind))
                .with_type_name(spec.type_name.clone())
                .with_group_child(group_child),
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
                let is_dirty = self.edits.is_cell_dirty(*base_row, column);
                // Only a document grid shows the staged edit against the
                // value it replaced; a relational grid marks the cell dirty
                // instead (`shows_transition = is_document && is_cell_dirty`).
                let replaced = (self.document.is_some() && is_dirty).then_some(base);

                Some(cell_view(
                    display,
                    replaced,
                    self.edits.row_state(*base_row),
                    is_dirty,
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
                Some(cell_view(
                    cell,
                    None,
                    &RowState::PendingInsert,
                    false,
                    align,
                ))
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
    use super::super::document::{ColumnGroupHeader, DocumentColumnHeader, DocumentPresentation};
    use super::super::model::{ColumnKind as GridColumnKind, ColumnSpec, RowData};
    use super::*;
    use chrono::{NaiveTime, TimeZone, Utc};
    use dbflux_core::Value;
    use gpui::TextAlign;

    /// The text a plain cell draws, failing the test on any other content.
    fn drawn_text(cell: &CellView) -> &str {
        match &cell.content {
            CellContent::Plain(text) => text,
            other => panic!("expected a plain cell, got {other:?}"),
        }
    }

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
            drawn_text(&cell),
            expected.as_ref(),
            "the adapter must draw {value:?} exactly as the model's formatting path does"
        );

        assert_eq!(cell.tone, tone, "{value:?} draws with tone {tone:?}");
        assert_eq!(
            cell.align,
            CellAlignment::Left,
            "the single text column aligns left"
        );
        assert_eq!(cell.secondary, None, "{value:?} draws no second span");
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
            drawn_text(&source.cell(0, 0).expect("cell")),
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

        assert_eq!(drawn_text(&source.cell(0, 0).expect("cell")), "2024-03-01");
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

        assert_eq!(drawn_text(&source.cell(0, 0).expect("cell")), "09:05:03");
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
            drawn_text(&source.cell(0, 0).expect("cell")),
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

        assert_eq!(drawn_text(&source.cell(0, 0).expect("cell")), "1234.5678");
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
            drawn_text(&source.cell(0, 1).expect("cell")),
            "B",
            "the staged override is what the renderer draws"
        );
    }

    #[test]
    fn a_resolved_foreign_key_draws_two_tones() {
        let labeled = CellValue::int(8).with_fk_label("María López");
        let model = model_with(vec![text_spec("author")], vec![vec![labeled.clone()]]);
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        // The painter draws the pair `CellValue::fk_spans` returns: the
        // label as the cell's text, the id suffix muted after it.
        let (label, id_suffix) = labeled
            .fk_spans()
            .expect("the model carries the foreign-key label");

        let cell = source.cell(0, 0).expect("cell");
        assert_eq!(
            drawn_text(&cell),
            label.as_ref(),
            "the label is what the cell leads with, not the combined display text"
        );
        let secondary = cell.secondary.expect("the id suffix follows the label");
        assert_eq!(secondary.text.as_ref(), id_suffix.as_ref());
        assert_eq!(secondary.tone, CellTone::Muted);
    }

    #[test]
    fn a_cell_without_a_foreign_key_label_draws_one_span() {
        let model = model_with(vec![text_spec("v")], vec![vec![CellValue::int(8)]]);
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        let cell = source.cell(0, 0).expect("cell");
        assert_eq!(cell.secondary, None);
        assert_eq!(
            drawn_text(&cell),
            model.cell(0, 0).unwrap().display_text().as_ref(),
            "without spans the cell draws its own display text"
        );
    }

    #[test]
    fn a_document_grid_shows_a_staged_edit_against_the_value_it_replaced() {
        let model = model_with(vec![text_spec("v")], vec![vec![CellValue::text("old")]]);
        let mut edits = EditBuffer::new();
        edits.set_base_row_count(model.row_count());
        edits.set_cell(0, 0, CellValue::text("new"));

        let document = DocumentPresentation::default();
        let source = GridRowSource::with_document(&model, &edits, Some(&document));

        // The painter composes `old → new` from the base cell and the
        // edit-buffer override; the contract carries both ends.
        let base = model.cell(0, 0).unwrap().display_text();
        let staged = edits
            .get_cell(0, 0, model.cell(0, 0).unwrap())
            .display_text();

        let cell = source.cell(0, 0).expect("cell");
        assert_eq!(
            cell.content,
            CellContent::Transition {
                from: base.into(),
                to: staged.into(),
            }
        );
        assert_eq!(cell.tone, CellTone::Dirty);
    }

    #[test]
    fn a_relational_grid_draws_a_staged_edit_plain() {
        let model = model_with(vec![text_spec("v")], vec![vec![CellValue::text("old")]]);
        let mut edits = EditBuffer::new();
        edits.set_base_row_count(model.row_count());
        edits.set_cell(0, 0, CellValue::text("new"));

        let source = GridRowSource::new(&model, &edits);

        // Without a document presentation the painter marks the cell dirty
        // and draws the staged text alone; no transition is composed.
        let cell = source.cell(0, 0).expect("cell");
        assert_eq!(cell.tone, CellTone::Dirty);
        assert_eq!(
            drawn_text(&cell),
            CellValue::text("new").display_text().as_ref(),
        );
    }

    #[test]
    fn a_transition_reads_the_replaced_text_from_the_base_cell() {
        let model = model_with(
            vec![text_spec("v")],
            vec![vec![CellValue::nested(false, 3)]],
        );
        let mut edits = EditBuffer::new();
        edits.set_base_row_count(model.row_count());
        edits.set_cell(0, 0, CellValue::text("stepped"));

        let document = DocumentPresentation::default();
        let source = GridRowSource::with_document(&model, &edits, Some(&document));

        // The painter composes the transition from the base cell's display
        // text whatever its kind (`base_value.display_text()`), while every
        // mark — the tone included — follows the value the cell draws now.
        let cell = source.cell(0, 0).expect("cell");
        assert_eq!(cell.tone, CellTone::Dirty);
        assert_eq!(
            cell.content,
            CellContent::Transition {
                from: CellValue::nested(false, 3).display_text().into(),
                to: CellValue::text("stepped").display_text().into(),
            }
        );
    }

    #[test]
    fn nested_cells_carry_their_array_marker() {
        let model = model_with(
            vec![text_spec("doc"), text_spec("arr"), text_spec("plain")],
            vec![vec![
                CellValue::nested(false, 3),
                CellValue::nested(true, 4),
                CellValue::text("x"),
            ]],
        );
        let edits = EditBuffer::new();
        let source = GridRowSource::new(&model, &edits);

        for col in 0..3 {
            // The painter derives the marker with the same match on the
            // model's kind.
            let expected = match model.cell(0, col).unwrap().kind {
                CellKind::Nested { is_array, .. } => Some(NestedMarker { is_array }),
                _ => None,
            };
            assert_eq!(
                source.cell(0, col).expect("cell").nested,
                expected,
                "column {col} carries the marker the painter derives"
            );
        }
    }

    #[test]
    fn a_grouped_document_column_washes_its_cells() {
        let model = model_with(
            vec![text_spec("a"), text_spec("b")],
            vec![vec![CellValue::text("x"), CellValue::text("y")]],
        );
        let document = DocumentPresentation {
            headers: vec![
                DocumentColumnHeader::default(),
                DocumentColumnHeader {
                    group: Some(ColumnGroupHeader {
                        key: "price".into(),
                        label: "price".into(),
                        type_label: "obj".into(),
                    }),
                    presence: None,
                },
            ],
        };
        let edits = EditBuffer::new();

        let source = GridRowSource::with_document(&model, &edits, Some(&document));
        assert_eq!(
            source.column(1).expect("column").group_child,
            document.is_group_child(1),
        );
        assert_eq!(
            source.column(0).expect("column").group_child,
            document.is_group_child(0),
        );

        let plain = GridRowSource::new(&model, &edits);
        assert!(!plain.column(1).expect("column").group_child);
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
        assert_eq!(drawn_text(&source.cell(1, 0).expect("cell")), "new");
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
