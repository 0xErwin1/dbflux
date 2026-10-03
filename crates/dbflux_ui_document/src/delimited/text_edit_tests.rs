//! Turning an edit of the raw text into changes of the pending state: the
//! mapping rules, what is refused, and a random differential check of the
//! whole path from an edited text to the saved file.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use dbflux_components::components::data_table::model::{
    CellValue, EditBuffer, InsertAnchor, VisualRowSource,
};
use dbflux_components::components::data_table::{DataTableState, TableModel};
use dbflux_delimited::{
    ByteSource, Dialect, Encoding, MemorySource, PagedReader, ReaderOptions, Record, parse_text,
    write_edited,
};
use gpui::{AppContext as _, Entity, TestAppContext};

use super::page_model::PageModel;
use super::text::{RenderedText, SourceSpan, TEXT_LIMITS, TextRow, check_pending, render_raw};
use super::text_edit::{
    AddedInsert, Step, TextEditError, TextEdits, apply_to_pending, edit_script,
    shortest_edit_script, text_edits,
};
use super::text_view::apply_text_edits;

fn encoding(label: &str) -> &'static Encoding {
    Encoding::for_label(label.as_bytes()).expect("a known encoding label")
}

fn csv(has_header: bool) -> Dialect {
    Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header,
        encoding: encoding("utf-8"),
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

/// A row added with `values` at `anchor`, after the inserts already there.
fn added(anchor: InsertAnchor, values: &[&str]) -> AddedInsert {
    AddedInsert {
        anchor,
        fields: strings(values),
        before: None,
    }
}

fn cells(values: &[&str]) -> Vec<CellValue> {
    values.iter().map(|value| CellValue::text(value)).collect()
}

/// The loaded part of a file and the pending state over it.
struct Fixture {
    bytes: Vec<u8>,
    dialect: Dialect,
    model: PageModel,
    span: SourceSpan,
    buffer: EditBuffer,
}

impl Fixture {
    /// Reads the first `pages` pages of `bytes`, `page_size` records each,
    /// and keeps the bytes of everything read, as the document does.
    fn load(bytes: &[u8], dialect: Dialect, page_size: usize, pages: usize) -> Self {
        let options = ReaderOptions {
            page_size: NonZeroUsize::new(page_size).expect("a non-zero page size"),
            window_size: NonZeroU64::new(5).expect("a non-zero window"),
        };

        let mut reader = PagedReader::open(MemorySource::new(bytes.to_vec()), dialect, options)
            .expect("the reader opens");
        let mut model = PageModel::new(reader.header().cloned());

        for page_index in 0..pages {
            if model.is_fully_loaded() {
                break;
            }

            let page = reader.read_page(page_index).expect("the page reads");

            model
                .append_page(page, reader.record_count())
                .expect("the page follows the loaded records");
        }

        let start = reader.byte_order_mark_length();
        let end = model
            .records()
            .last()
            .or(model.header())
            .map_or(start, |record| record.byte_range.end);

        let kept = reader
            .source()
            .read_range(start..end)
            .expect("the loaded bytes read");

        let mut buffer = EditBuffer::new();
        buffer.set_base_row_count(model.records().len());

        Self {
            bytes: bytes.to_vec(),
            dialect,
            model,
            span: SourceSpan::new(start, kept),
            buffer,
        }
    }

    fn whole(bytes: &[u8], dialect: Dialect) -> Self {
        Self::load(bytes, dialect, 10_000, 1)
    }

    fn rendered(&self) -> RenderedText {
        render_raw(
            &self.model,
            &self.span,
            &self.buffer,
            &self.dialect,
            TEXT_LIMITS,
        )
        .expect("the raw text renders")
    }

    /// The changes of the raw text edited into `edited`.
    fn edits_for(&self, edited: &str) -> Result<TextEdits, TextEditError> {
        let rendered = self.rendered();

        text_edits(
            &rendered.layout,
            &rendered.text,
            edited,
            &self.dialect,
            &self.model,
            &self.buffer,
        )
    }

    /// The changes of the raw text with `from` replaced by `to`, once.
    fn replaced(&self, from: &str, to: &str) -> Result<TextEdits, TextEditError> {
        let text = self.rendered().text;
        assert!(text.contains(from), "{from:?} is in {text:?}");

        self.edits_for(&text.replacen(from, to, 1))
    }
}

const CITIES: &[u8] = b"name,city\nAna,Lima\nBo,Quito\n";

// -- Records ---------------------------------------------------------------------

#[test]
fn editing_one_field_is_one_cell_edit_on_its_row() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("Bo,Quito", "Bo,Cusco"),
        Ok(TextEdits {
            base_cells: vec![(1, 1, "Cusco".to_string())],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_line_inserted_in_the_middle_goes_after_the_row_before_it() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("Bo,", "Cy,Rome\nBo,"),
        Ok(TextEdits {
            added_inserts: vec![added(InsertAnchor::After(0), &["Cy", "Rome"])],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_line_inserted_above_the_first_record_goes_above_the_first_row() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("Ana,", "Cy,Rome\nAna,"),
        Ok(TextEdits {
            added_inserts: vec![added(InsertAnchor::BeforeFirst, &["Cy", "Rome"])],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_line_added_at_the_end_goes_after_the_last_row() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.edits_for("name,city\nAna,Lima\nBo,Quito\nCy,Rome\n"),
        Ok(TextEdits {
            added_inserts: vec![added(InsertAnchor::After(1), &["Cy", "Rome"])],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_deleted_line_deletes_its_record() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("Ana,Lima\n", ""),
        Ok(TextEdits {
            deleted_rows: vec![0],
            ..TextEdits::default()
        })
    );
}

#[test]
fn lines_replaced_by_more_lines_are_changed_rows_and_inserts() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.edits_for("name,city\nX,1\nY,2\nZ,3\n"),
        Ok(TextEdits {
            base_cells: vec![
                (0, 0, "X".to_string()),
                (0, 1, "1".to_string()),
                (1, 0, "Y".to_string()),
                (1, 1, "2".to_string()),
            ],
            added_inserts: vec![added(InsertAnchor::After(1), &["Z", "3"])],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_shorter_line_empties_its_missing_fields() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("Bo,Quito", "Bo"),
        Ok(TextEdits {
            base_cells: vec![(1, 1, String::new())],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_pending_insert_is_edited_and_removed_through_its_line() {
    let mut fixture = Fixture::whole(CITIES, csv(true));
    fixture
        .buffer
        .add_pending_insert_at(InsertAnchor::After(0), cells(&["Cy", "Rome"]));

    assert_eq!(
        fixture.rendered().text,
        "name,city\nAna,Lima\nCy,Rome\nBo,Quito\n"
    );

    assert_eq!(
        fixture.replaced("Cy,Rome", "Cy,Oslo"),
        Ok(TextEdits {
            insert_cells: vec![(0, 1, "Oslo".to_string())],
            ..TextEdits::default()
        })
    );

    assert_eq!(
        fixture.replaced("Cy,Rome\n", ""),
        Ok(TextEdits {
            removed_inserts: vec![0],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_new_line_above_a_pending_insert_of_the_same_anchor_goes_before_it_and_leaves_it_alone() {
    let mut fixture = Fixture::whole(CITIES, csv(true));
    fixture
        .buffer
        .add_pending_insert_at(InsertAnchor::After(0), cells(&["Cy", "Rome"]));

    assert_eq!(
        fixture.replaced("Cy,Rome", "Di,Oslo\nCy,Rome"),
        Ok(TextEdits {
            added_inserts: vec![AddedInsert {
                before: Some(0),
                ..added(InsertAnchor::After(0), &["Di", "Oslo"])
            }],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_record_edited_in_the_table_is_compared_as_the_text_shows_it() {
    let mut fixture = Fixture::whole(CITIES, csv(true));
    fixture.buffer.set_cell(0, 1, CellValue::text("Cusco"));

    assert_eq!(fixture.replaced("Ana,Cusco", "Ana,Lima"), {
        Ok(TextEdits {
            base_cells: vec![(0, 1, "Lima".to_string())],
            ..TextEdits::default()
        })
    });
}

// -- What changes nothing -----------------------------------------------------------

#[test]
fn the_text_as_it_was_rendered_changes_nothing() {
    let fixture = Fixture::whole(CITIES, csv(true));
    let text = fixture.rendered().text;

    assert_eq!(fixture.edits_for(&text), Ok(TextEdits::default()));
}

#[test]
fn a_line_break_or_quoting_changed_alone_changes_nothing() {
    let fixture = Fixture::whole(b"name,city\r\nAna,Lima\r\nBo,Quito\r\n", csv(true));

    assert_eq!(
        fixture.replaced("Ana,Lima\r\n", "Ana,Lima\n"),
        Ok(TextEdits::default())
    );
    assert_eq!(
        fixture.replaced("Bo,Quito", "\"Bo\",\"Quito\""),
        Ok(TextEdits::default())
    );
}

// -- The header ------------------------------------------------------------------------

#[test]
fn a_changed_header_field_renames_its_column() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("name,city", "name,town"),
        Ok(TextEdits {
            renames: vec![(1, "town".to_string())],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_header_field_after_the_last_column_appends_a_column_to_a_fully_loaded_file() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.edits_for("name,city,age\nAna,Lima,30\nBo,Quito\n"),
        Ok(TextEdits {
            appended_columns: strings(&["age"]),
            base_cells: vec![(0, 2, "30".to_string())],
            ..TextEdits::default()
        })
    );
}

#[test]
fn a_header_field_after_the_last_column_needs_the_rest_of_a_partly_loaded_file() {
    let fixture = Fixture::load(b"name,city\nAna,Lima\nBo,Quito\nCy,Rome\n", csv(true), 1, 1);

    assert_eq!(fixture.rendered().text, "name,city\nAna,Lima\n");
    assert_eq!(
        fixture.edits_for("name,city,age\nAna,Lima\n"),
        Err(TextEditError::ColumnsNeedFullLoad)
    );
}

#[test]
fn a_header_with_a_removed_field_is_refused() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("name,city", "name"),
        Err(TextEditError::HeaderFieldRemoved)
    );
    assert_eq!(
        fixture.edits_for(""),
        Err(TextEditError::HeaderFieldRemoved)
    );
}

#[test]
fn a_header_with_reordered_fields_is_refused() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("name,city", "city,name"),
        Err(TextEditError::HeaderReordered)
    );
}

#[test]
fn deleting_the_header_line_makes_the_next_line_the_header() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("name,city\n", ""),
        Ok(TextEdits {
            renames: vec![(0, "Ana".to_string()), (1, "Lima".to_string())],
            deleted_rows: vec![0],
            ..TextEdits::default()
        })
    );
}

// -- Field counts ------------------------------------------------------------------------

#[test]
fn a_data_line_with_more_fields_than_the_columns_is_refused() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("Bo,Quito", "Bo,Quito,40"),
        Err(TextEditError::TooManyFields {
            line: 3,
            fields: 3,
            columns: 2,
            has_header: true,
        })
    );

    let headerless = Fixture::whole(b"1,2\n3,4\n", csv(false));

    assert_eq!(
        headerless.replaced("3,4", "3,4,5"),
        Err(TextEditError::TooManyFields {
            line: 2,
            fields: 3,
            columns: 2,
            has_header: false,
        })
    );
}

#[test]
fn a_data_line_with_the_columns_the_header_gained_is_taken() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.edits_for("name,city,age\nAna,Lima\nBo,Quito,40\n"),
        Ok(TextEdits {
            appended_columns: strings(&["age"]),
            base_cells: vec![(1, 2, "40".to_string())],
            ..TextEdits::default()
        })
    );
}

// -- Text that does not read -----------------------------------------------------------

#[test]
fn an_unclosed_quote_is_refused_with_its_line() {
    let fixture = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        fixture.replaced("Bo,Quito", "Bo,\"Quito"),
        Err(TextEditError::UnclosedQuote { line: 3 })
    );
}

#[test]
fn a_file_that_already_ends_inside_a_quote_can_be_edited_above_it() {
    let fixture = Fixture::whole(b"name,city\nAna,Lima\nBo,\"Qui\nto", csv(true));

    assert_eq!(
        fixture.replaced("Ana,Lima", "Ana,Cusco"),
        Ok(TextEdits {
            base_cells: vec![(0, 1, "Cusco".to_string())],
            ..TextEdits::default()
        })
    );
    assert_eq!(
        fixture.replaced("Qui\nto", "Qui\nt"),
        Err(TextEditError::UnclosedQuote { line: 3 })
    );
}

#[test]
fn a_character_the_encoding_cannot_hold_is_refused_with_its_line() {
    let dialect = Dialect {
        encoding: encoding("windows-1252"),
        ..csv(true)
    };
    let fixture = Fixture::whole(b"name,city\nAna,Lima\n", dialect);

    assert_eq!(
        fixture.replaced("Lima", "\u{4e2d}"),
        Err(TextEditError::Unencodable {
            line: 2,
            character: '\u{4e2d}',
            encoding: "windows-1252",
        })
    );
}

// -- Reading only what changed ---------------------------------------------------------------

#[test]
fn a_quote_opened_above_unchanged_lines_is_read_with_them() {
    let fixture = Fixture::whole(b"id,v\n1,a\n2,b\n3,c\"\n4,d\n", csv(true));

    // The quote closes at the end of `3,c"`, so three lines are one record.
    assert_eq!(
        fixture.replaced("1,a", "1,\"a"),
        Ok(TextEdits {
            base_cells: vec![(0, 1, "a\n2,b\n3,c".to_string())],
            deleted_rows: vec![1, 2],
            ..TextEdits::default()
        })
    );

    // A quote that never closes swallows every line after it.
    let unclosed = Fixture::whole(CITIES, csv(true));

    assert_eq!(
        unclosed.replaced("Ana,Lima", "Ana,\"Lima"),
        Err(TextEditError::UnclosedQuote { line: 2 })
    );
}

#[test]
fn a_line_feed_typed_after_a_carriage_return_joins_its_terminator() {
    let fixture = Fixture::whole(b"1\r2\r3\r", csv(false));

    // `2\r` becomes `2\r\n`: the same record, with another terminator.
    assert_eq!(fixture.replaced("2\r", "2\r\n"), Ok(TextEdits::default()));

    // A line feed typed before `3` makes no empty record either.
    assert_eq!(fixture.replaced("2\r3", "2\r\n3"), Ok(TextEdits::default()));
}

// -- The diff --------------------------------------------------------------------------------

fn numbered(records: usize) -> Vec<u8> {
    let mut bytes = b"id,value\n".to_vec();

    for record in 0..records {
        bytes.extend_from_slice(format!("{record},v{record}\n").as_bytes());
    }

    bytes
}

#[test]
fn one_line_inserted_into_a_long_text_is_one_insert() {
    let fixture = Fixture::whole(&numbered(3_000), csv(true));

    assert_eq!(
        fixture.replaced("1500,v1500\n", "1500,v1500\nnew,row\n"),
        Ok(TextEdits {
            added_inserts: vec![added(InsertAnchor::After(1500), &["new", "row"])],
            ..TextEdits::default()
        })
    );
}

#[test]
fn scattered_edits_of_a_long_text_map_to_their_own_rows() {
    let fixture = Fixture::whole(&numbered(3_000), csv(true));

    let edited = fixture
        .rendered()
        .text
        .replacen("10,v10\n", "", 1)
        .replacen("700,v700", "700,changed", 1)
        .replacen("2990,v2990\n", "2990,v2990\nnew,row\n", 1);

    assert_eq!(
        fixture.edits_for(&edited),
        Ok(TextEdits {
            base_cells: vec![(700, 1, "changed".to_string())],
            deleted_rows: vec![10],
            added_inserts: vec![added(InsertAnchor::After(2990), &["new", "row"])],
            ..TextEdits::default()
        })
    );
}

/// A rewrite of every other line changes those rows only, however many
/// lines that is.
#[test]
fn a_rewrite_of_every_other_line_changes_only_those_rows() {
    let records = 3_000;
    let fixture = Fixture::whole(&numbered(records), csv(true));

    let edited: String = std::iter::once("id,value\n".to_string())
        .chain((0..records).map(|record| {
            if record % 2 == 1 {
                format!("{record},w{record}\n")
            } else {
                format!("{record},v{record}\n")
            }
        }))
        .collect();

    let edits = fixture.edits_for(&edited).expect("the text applies");

    let rows: Vec<usize> = edits.base_cells.iter().map(|(row, _, _)| *row).collect();
    let odd_rows: Vec<usize> = (0..records).filter(|row| row % 2 == 1).collect();

    assert_eq!(rows, odd_rows);
    assert!(edits.deleted_rows.is_empty() && edits.added_inserts.is_empty());
}

/// A large file whose records are quoted in different ways, so that a
/// record written again from its fields would not keep its bytes.
fn quoted_numbered(records: usize) -> Vec<u8> {
    let mut bytes = b"id,payload,flag\n".to_vec();

    for record in 0..records {
        let line = match record % 3 {
            0 => format!("{record},\"p{record}\",x\n"),
            1 => format!("\"{record}\",p{record},\"x\"\n"),
            _ => format!("{record},p{record},x\n"),
        };

        bytes.extend_from_slice(line.as_bytes());
    }

    bytes
}

/// Two long runs of deleted lines in a 46,001-line text: only their rows
/// are deleted, every other row keeps its record, and the saved file is the
/// original without those lines, byte for byte.
#[gpui::test]
fn deleting_two_long_runs_of_lines_changes_only_their_rows(cx: &mut TestAppContext) {
    let records = 46_000;
    let bytes = quoted_numbered(records);
    let mut fixture = Fixture::load(&bytes, csv(true), records, 1);
    assert!(fixture.model.is_fully_loaded());

    let deleted: Vec<usize> = (1_000..1_600).chain(30_000..30_600).collect();

    let rendered = fixture.rendered();
    let edited: String = rendered
        .layout
        .records
        .iter()
        .filter(|record| !matches!(record.row, TextRow::Base(row) if deleted.contains(&row)))
        .map(|record| &rendered.text[record.text.clone()])
        .collect();

    let edits = fixture.edits_for(&edited).expect("the text applies");

    assert!(
        edits.deleted_rows == deleted,
        "only the deleted lines' rows are deleted"
    );
    assert!(
        edits.base_cells.is_empty() && edits.added_inserts.is_empty(),
        "{} cell edits and {} inserts",
        edits.base_cells.len(),
        edits.added_inserts.len()
    );

    let (_, saved) = apply_and_save(cx, &mut fixture, &edited);

    let mut expected = Vec::new();
    expected.extend_from_slice(b"id,payload,flag\n");

    for (row, record) in fixture.model.records().iter().enumerate() {
        if !deleted.contains(&row) {
            expected.extend_from_slice(
                &bytes[record.byte_range.start as usize..record.byte_range.end as usize],
            );
        }
    }

    assert!(
        saved == expected,
        "the saved file is the original without the deleted lines"
    );
}

/// The longest common subsequence of `old` and `new`, by dynamic
/// programming.
fn longest_common_subsequence(old: &[u32], new: &[u32]) -> usize {
    let mut lengths = vec![vec![0usize; new.len() + 1]; old.len() + 1];

    for (i, old_item) in old.iter().enumerate() {
        for (j, new_item) in new.iter().enumerate() {
            lengths[i + 1][j + 1] = if old_item == new_item {
                lengths[i][j] + 1
            } else {
                lengths[i][j + 1].max(lengths[i + 1][j])
            };
        }
    }

    lengths[old.len()][new.len()]
}

/// Checks that `script` leads from `old` to `new` and keeps only equal
/// records, in order, and that no run of removed and added records between
/// two kept ones holds two equal records. Returns how many it keeps.
fn check_script(script: &[Step], old: &[u32], new: &[u32], case: usize) -> usize {
    let (mut next_old, mut next_new, mut kept) = (0, 0, 0);
    let (mut removed, mut added): (Vec<u32>, Vec<u32>) = (Vec::new(), Vec::new());

    for step in script {
        match *step {
            Step::Keep(i, j) => {
                assert_eq!((i, j), (next_old, next_new), "case {case}");
                assert_eq!(old[i], new[j], "case {case}");
                assert!(
                    !removed.iter().any(|item| added.contains(item)),
                    "case {case}: {old:?} -> {new:?}"
                );

                removed.clear();
                added.clear();
                next_old += 1;
                next_new += 1;
                kept += 1;
            }

            Step::Remove(i) => {
                assert_eq!(i, next_old, "case {case}");
                removed.push(old[i]);
                next_old += 1;
            }

            Step::Add(j) => {
                assert_eq!(j, next_new, "case {case}");
                added.push(new[j]);
                next_new += 1;
            }
        }
    }

    assert!(
        !removed.iter().any(|item| added.contains(item)),
        "case {case}: {old:?} -> {new:?}"
    );
    assert_eq!((next_old, next_new), (old.len(), new.len()), "case {case}");

    kept
}

/// Myers' search finds a shortest edit script, and the anchored diff built
/// on it leads from `old` to `new` and never leaves two equal records in a
/// run of removed and added ones.
#[test]
fn the_diff_leaves_no_equal_records_unpaired() {
    let mut random = Random(0x2545_F491_4F6C_DD1D);

    for case in 0..5_000 {
        let alphabet = 1 + random.below(6);
        let old_length = random.below(25);
        let new_length = random.below(25);

        let old: Vec<u32> = (0..old_length)
            .map(|_| random.below(alphabet) as u32)
            .collect();
        let new: Vec<u32> = (0..new_length)
            .map(|_| random.below(alphabet) as u32)
            .collect();

        let shortest = check_script(&shortest_edit_script(&old, &new), &old, &new, case);
        assert_eq!(
            shortest,
            longest_common_subsequence(&old, &new),
            "case {case}: {old:?} -> {new:?}"
        );

        check_script(&edit_script(&old, &new), &old, &new, case);
    }
}

/// A line that appears once is kept even where a shortest script would
/// keep two copies of another line instead.
#[test]
fn a_unique_line_is_kept_before_copies_of_another() {
    // `x` is kept, so the first `a` and `c` are removed and two new lines
    // are added: the shortest script would keep both `a` and drop `x`.
    let script = edit_script(&[0, 1, 0, 2], &[3, 1, 0, 0]);

    assert!(script.contains(&Step::Keep(1, 1)), "{script:?}");
}

// -- Applying, and what a save then writes ------------------------------------------------

/// Builds the table of `fixture` as the document does, applies the edit of
/// its raw text into `edited` through the document's applier, and returns
/// the table and the bytes a save of the result writes.
fn apply_and_save(
    cx: &mut TestAppContext,
    fixture: &mut Fixture,
    edited: &str,
) -> (Entity<DataTableState>, Vec<u8>) {
    let edits = fixture.edits_for(edited).expect("the text applies");

    let table_model = Arc::new(fixture.model.table_model());
    let buffer = fixture.buffer.clone();

    let table_state = cx.new(|cx| {
        let mut state = DataTableState::new(table_model, cx);
        *state.edit_buffer_mut() = buffer;
        state.set_positional_editing(true);
        state.set_insertable(true);
        state
    });

    table_state.update(cx, |state, cx| {
        apply_text_edits(&edits, &mut fixture.model, state, cx).expect("the edits apply");
    });

    fixture.buffer = table_state.read_with(cx, |state, _| state.edit_buffer().clone());

    let saved = save(fixture);

    (table_state, saved)
}

/// The bytes a save of the fixture's pending state writes.
fn save(fixture: &Fixture) -> Vec<u8> {
    let edit_set = fixture
        .model
        .edit_set(fixture.bytes.len() as u64, &fixture.buffer)
        .expect("the edit set builds");

    let mut output = Vec::new();

    write_edited(
        &MemorySource::new(fixture.bytes.clone()),
        &fixture.dialect,
        &edit_set,
        NonZeroU64::new(7).expect("a non-zero window"),
        &mut output,
    )
    .expect("the edited file writes");

    output
}

/// The cells the table shows, row by row, pending changes included and
/// deleted rows left out.
fn shown_rows(table_state: &Entity<DataTableState>, cx: &mut TestAppContext) -> Vec<Vec<String>> {
    table_state.read_with(cx, |state, _| {
        let buffer = state.edit_buffer();
        let model: &TableModel = state.model();
        let absent = CellValue::text("");

        buffer
            .compute_visual_order()
            .into_iter()
            .filter_map(|source| match source {
                VisualRowSource::Base(row) if buffer.is_pending_delete(row) => None,

                VisualRowSource::Base(row) => Some(
                    (0..model.col_count())
                        .map(|column| {
                            let base = model.cell(row, column).unwrap_or(&absent);
                            buffer.get_cell(row, column, base).edit_text()
                        })
                        .collect(),
                ),

                VisualRowSource::Insert(index) => Some(
                    buffer.pending_inserts()[index]
                        .data
                        .iter()
                        .map(CellValue::edit_text)
                        .collect(),
                ),
            })
            .collect()
    })
}

#[gpui::test]
fn an_edited_field_reaches_the_table_and_only_its_record_changes_on_save(cx: &mut TestAppContext) {
    let bytes = b"name,city\r\nAna,\"Lima\"\r\nBo,Quito\r\nCy,Rome\r\n";
    let mut fixture = Fixture::whole(bytes, csv(true));

    let edited = fixture.rendered().text.replacen("Bo,Quito", "Bo,Cusco", 1);
    let (table_state, saved) = apply_and_save(cx, &mut fixture, &edited);

    assert_eq!(
        shown_rows(&table_state, cx),
        [
            strings(&["Ana", "Lima"]),
            strings(&["Bo", "Cusco"]),
            strings(&["Cy", "Rome"])
        ]
    );
    assert_eq!(
        saved,
        b"name,city\r\nAna,\"Lima\"\r\nBo,Cusco\r\nCy,Rome\r\n".to_vec()
    );
}

#[gpui::test]
fn inserts_deletes_and_header_changes_are_saved_where_the_text_puts_them(cx: &mut TestAppContext) {
    let mut fixture = Fixture::whole(CITIES, csv(true));
    fixture
        .buffer
        .add_pending_insert_at(InsertAnchor::After(1), cells(&["Cy", "Rome"]));

    let edited = "name,town,age\nZed,Oslo,1\nBo,Quito\nDi,Bern,2\nCy,Rome,3\nEd,Kyiv\n";
    let (_, saved) = apply_and_save(cx, &mut fixture, edited);

    assert_eq!(
        String::from_utf8(saved).expect("UTF-8"),
        "name,town,age\nZed,Oslo,1\nBo,Quito,\nDi,Bern,2\nCy,Rome,3\nEd,Kyiv,\n"
    );
}

#[gpui::test]
fn a_quoted_field_with_line_breaks_is_edited_in_the_text(cx: &mut TestAppContext) {
    let bytes = b"id,note\n1,\"one\ntwo\"\n2,plain\n";
    let mut fixture = Fixture::whole(bytes, csv(true));

    let edited = fixture
        .rendered()
        .text
        .replacen("\"one\ntwo\"", "\"one\nthree, four\"", 1);
    let (table_state, saved) = apply_and_save(cx, &mut fixture, &edited);

    assert_eq!(
        shown_rows(&table_state, cx)[0],
        strings(&["1", "one\nthree, four"])
    );
    assert_eq!(
        saved,
        b"id,note\n1,\"one\nthree, four\"\n2,plain\n".to_vec()
    );
}

#[gpui::test]
fn a_utf_16_file_and_a_file_with_a_byte_order_mark_are_edited_in_the_text(cx: &mut TestAppContext) {
    let utf_16le = Dialect {
        encoding: encoding("utf-16le"),
        ..csv(true)
    };
    let text = "name,city\r\nAna,Lima\r\nBo,Quito\r\n";

    let mut bytes = b"\xFF\xFE".to_vec();
    bytes.extend(text.encode_utf16().flat_map(u16::to_le_bytes));

    let mut fixture = Fixture::whole(&bytes, utf_16le);
    assert_eq!(fixture.rendered().text, text);

    let (_, saved) = apply_and_save(cx, &mut fixture, &text.replacen("Lima", "Cusco", 1));

    let mut expected = b"\xFF\xFE".to_vec();
    expected.extend(
        text.replacen("Lima", "Cusco", 1)
            .encode_utf16()
            .flat_map(u16::to_le_bytes),
    );
    assert_eq!(saved, expected);

    let mut marked = b"\xEF\xBB\xBF".to_vec();
    marked.extend_from_slice(CITIES);

    let mut fixture = Fixture::whole(&marked, csv(true));
    assert_eq!(fixture.rendered().text, String::from_utf8_lossy(CITIES));

    let (_, saved) = apply_and_save(cx, &mut fixture, "name,city\nAna,Lima\nNew,Row\nBo,Quito\n");
    assert_eq!(
        saved,
        b"\xEF\xBB\xBFname,city\nAna,Lima\nNew,Row\nBo,Quito\n".to_vec()
    );
}

// -- The random differential check ---------------------------------------------------------

/// How many random files and edits the differential check runs.
const RANDOM_CASES: usize = 3_000;

/// A small deterministic generator, so a failing case can be run again from
/// its seed.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound.max(1) as u64) as usize
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

/// A dialect among the encodings, delimiters, quoting and header the
/// document reads.
fn random_dialect(random: &mut Random) -> Dialect {
    Dialect {
        delimiter: random.pick(b",\t;|"),
        quote: if random.chance(80) { Some(b'"') } else { None },
        has_header: random.chance(75),
        encoding: encoding(random.pick(&["utf-8", "utf-16le", "windows-1252"])),
    }
}

/// A value `dialect` can write. Without a quote character a value holds no
/// delimiter and no line break.
fn random_value(random: &mut Random, dialect: &Dialect) -> String {
    let delimiter = char::from(dialect.delimiter).to_string();

    let mut pieces = vec!["a", "b", "x y", "\u{e9}", "1", " ", "\"", "'"];

    if dialect.quote.is_some() {
        pieces.extend([delimiter.as_str(), "\n", "\r\n"]);
    }

    (0..random.below(3))
        .map(|_| random.pick(&pieces).to_string())
        .collect()
}

/// `count` values `dialect` can write as one record. Without a quote
/// character one empty field cannot be written, so it is not one.
fn random_fields(random: &mut Random, dialect: &Dialect, count: usize) -> Vec<String> {
    let mut fields: Vec<String> = (0..count).map(|_| random_value(random, dialect)).collect();

    if dialect.quote.is_none() && fields.len() == 1 && fields[0].is_empty() {
        fields[0] = "a".to_string();
    }

    fields
}

/// `fields` as one record of `dialect`, quoted only where it must be, or
/// always when `quote_all` and the dialect quotes.
fn record_text(fields: &[String], dialect: &Dialect, quote_all: bool) -> String {
    let delimiter = char::from(dialect.delimiter);

    let Some(quote) = dialect.quote.map(char::from) else {
        return fields.join(&delimiter.to_string());
    };

    let quoted = |field: &String| {
        let needs_quote = quote_all
            || field.is_empty() && fields.len() == 1
            || field.contains([delimiter, quote, '\n', '\r'])
            || field.starts_with(char::is_whitespace)
            || field.ends_with(char::is_whitespace);

        if needs_quote {
            let doubled = format!("{quote}{quote}");
            format!("{quote}{}{quote}", field.replace(quote, &doubled))
        } else {
            field.clone()
        }
    };

    fields
        .iter()
        .map(quoted)
        .collect::<Vec<_>>()
        .join(&delimiter.to_string())
}

/// `text` written in the encoding of `dialect`, after the encoding's
/// byte-order mark when `with_mark`.
fn encode(text: &str, dialect: &Dialect, with_mark: bool) -> Vec<u8> {
    match dialect.encoding.name() {
        "UTF-16LE" => {
            let mark: &[u8] = if with_mark { b"\xFF\xFE" } else { b"" };
            let mut bytes = mark.to_vec();
            bytes.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
            bytes
        }

        "UTF-8" => {
            let mark: &[u8] = if with_mark { b"\xEF\xBB\xBF" } else { b"" };
            let mut bytes = mark.to_vec();
            bytes.extend_from_slice(text.as_bytes());
            bytes
        }

        _ => dialect.encoding.encode(text).0.into_owned(),
    }
}

/// The text of a random file of `dialect`: a header when the dialect has
/// one, records of up to three fields, some shorter, some quoted where they
/// need not be, with line feeds or CRLF.
fn random_file_text(random: &mut Random, dialect: &Dialect) -> String {
    let columns = 1 + random.below(3);
    let terminator = if random.chance(50) { "\r\n" } else { "\n" };

    let mut text = String::new();

    if dialect.has_header {
        let header: Vec<String> = (0..columns).map(|column| format!("c{column}")).collect();
        text.push_str(&format!(
            "{}{terminator}",
            record_text(&header, dialect, false)
        ));
    }

    let records = if dialect.has_header {
        random.below(8)
    } else {
        1 + random.below(7)
    };

    for record in 0..records {
        let width = if random.chance(20) {
            1 + random.below(columns)
        } else {
            columns
        };

        let fields = random_fields(random, dialect, width);
        text.push_str(&record_text(&fields, dialect, random.chance(25)));

        if record + 1 < records || random.chance(80) {
            let own = if random.chance(10) {
                "\r\n"
            } else {
                terminator
            };
            text.push_str(own);
        }
    }

    text
}

/// Stages random table edits that a save of a file loaded this far can
/// write, so that the text shows pending changes too.
fn random_table_edits(random: &mut Random, fixture: &mut Fixture) {
    let rows = fixture.model.records().len();
    let columns = fixture.model.column_count();
    let fully_loaded = fixture.model.is_fully_loaded();
    let dialect = fixture.dialect;

    if columns == 0 {
        return;
    }

    for _ in 0..random.below(3) {
        match random.below(3) {
            0 if rows > 0 => {
                let mut value = random_value(random, &dialect);

                // Without a quote character an emptied record shorter than
                // the columns would be one empty field, which cannot be
                // written.
                if dialect.quote.is_none() && value.is_empty() {
                    value = "a".to_string();
                }

                fixture.buffer.set_cell(
                    random.below(rows),
                    random.below(columns),
                    CellValue::text(&value),
                );
            }

            1 if rows > 0 => fixture.buffer.mark_for_delete(random.below(rows)),

            _ => {
                // After the last loaded record of a file that is not fully
                // loaded a save refuses an insert.
                let anchor = match random.below(3) {
                    _ if rows == 0 && fully_loaded => InsertAnchor::End,
                    _ if rows == 0 => continue,
                    0 => InsertAnchor::BeforeFirst,
                    _ if fully_loaded => InsertAnchor::After(random.below(rows)),
                    _ if rows > 1 => InsertAnchor::After(random.below(rows - 1)),
                    _ => InsertAnchor::BeforeFirst,
                };

                let fields = random_fields(random, &dialect, columns);
                let values: Vec<&str> = fields.iter().map(String::as_str).collect();

                fixture.buffer.add_pending_insert_at(anchor, cells(&values));
            }
        }
    }
}

/// How a line of the edited text came to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Touch {
    /// As it was rendered.
    Untouched,

    /// With the same fields and another line break or quoting.
    Requoted,

    /// With other fields.
    Changed,

    /// Added by the user.
    New,
}

/// One line of the edited text: its text, the row it was rendered from, and
/// how the user touched it.
#[derive(Debug, Clone)]
struct Line {
    text: String,
    row: Option<TextRow>,
    touch: Touch,
}

/// Edits the lines of `rendered` at random: lines changed, inserted,
/// deleted, requoted or given another line break, and the header renamed
/// or given a new column on a fully loaded file. Keeps the text readable,
/// and adds no line after the last loaded record of a file that is not
/// fully loaded, which a save refuses.
fn random_text_edit(random: &mut Random, fixture: &Fixture, rendered: &RenderedText) -> Vec<Line> {
    let dialect = fixture.dialect;
    let fully_loaded = fixture.model.is_fully_loaded();

    let mut lines: Vec<Line> = rendered
        .layout
        .records
        .iter()
        .map(|record| Line {
            text: rendered.text[record.text.clone()].to_string(),
            row: Some(record.row),
            touch: Touch::Untouched,
        })
        .collect();

    let has_header = lines
        .first()
        .is_some_and(|line| line.row == Some(TextRow::Header));
    let mut columns = fixture.model.column_count().max(1);

    if has_header && random.chance(25) {
        let mut header = parse_text(&lines[0].text, &dialect).expect("the header reads")[0]
            .fields
            .clone();

        if fully_loaded && random.chance(50) {
            header.push(format!("n{}", random.below(9)));
            columns = fixture.model.column_count() + 1;
        } else {
            let column = random.below(header.len());
            header[column] = format!("r{}", random.below(9));
        }

        let terminator = split_terminator(&lines[0].text).1.to_string();
        lines[0].text = format!("{}{terminator}", record_text(&header, &dialect, false));
        lines[0].touch = Touch::Changed;
    }

    let data_start = usize::from(has_header);

    for _ in 0..1 + random.below(4) {
        let data_count = lines.len() - data_start;
        let at = data_start + random.below(data_count + 1);

        match random.below(5) {
            0 if at < lines.len() => {
                lines.remove(at);
            }

            1 if at < lines.len() => {
                let width = 1 + random.below(columns);
                let fields = random_fields(random, &dialect, width);
                let terminator = split_terminator(&lines[at].text).1.to_string();

                lines[at].text = format!("{}{terminator}", record_text(&fields, &dialect, false));
                lines[at].touch = Touch::Changed;
            }

            2 if at < lines.len() => {
                let fields = parse_text(&lines[at].text, &dialect).expect("a record reads")[0]
                    .fields
                    .clone();
                let terminator = match split_terminator(&lines[at].text).1 {
                    "\r\n" => "\n",
                    "" => "",
                    _ => "\r\n",
                };

                lines[at].text = format!(
                    "{}{terminator}",
                    record_text(&fields, &dialect, random.chance(50))
                );

                if lines[at].touch == Touch::Untouched {
                    lines[at].touch = Touch::Requoted;
                }
            }

            _ if !fully_loaded && at >= lines.len() => {}

            _ => {
                let width = 1 + random.below(columns);
                let fields = random_fields(random, &dialect, width);
                let terminator = if random.chance(50) { "\n" } else { "\r\n" };

                // A line added after a final record without a terminator
                // would join it.
                if let Some(before) = at.checked_sub(1).and_then(|index| lines.get_mut(index))
                    && split_terminator(&before.text).1.is_empty()
                {
                    before.text.push('\n');

                    if before.touch == Touch::Untouched {
                        before.touch = Touch::Requoted;
                    }
                }

                lines.insert(
                    at,
                    Line {
                        text: format!("{}{terminator}", record_text(&fields, &dialect, false)),
                        row: None,
                        touch: Touch::New,
                    },
                );
            }
        }
    }

    lines
}

fn split_terminator(text: &str) -> (&str, &str) {
    for terminator in ["\r\n", "\n", "\r"] {
        if let Some(content) = text.strip_suffix(terminator) {
            return (content, &text[content.len()..]);
        }
    }

    (text, "")
}

/// `fields` without the empty fields it ends with: a record written with
/// more columns than the text gave it is the same record.
fn trimmed(fields: &[String]) -> Vec<String> {
    let kept = fields.len()
        - fields
            .iter()
            .rev()
            .take_while(|field| field.is_empty())
            .count();

    fields[..kept].to_vec()
}

/// Every record of `bytes`, the header first when `dialect` has one.
fn read_all(bytes: &[u8], dialect: Dialect) -> Vec<Record> {
    let options = ReaderOptions {
        page_size: NonZeroUsize::new(100_000).expect("a non-zero page size"),
        window_size: NonZeroU64::new(64).expect("a non-zero window"),
    };

    let mut reader =
        PagedReader::open(MemorySource::new(bytes.to_vec()), dialect, options).expect("it opens");
    let records = reader.read_page(0).expect("the file reads").records;

    reader
        .header()
        .cloned()
        .into_iter()
        .chain(records)
        .collect()
}

/// The loaded records the pending state leaves alone: not deleted and
/// without an edited cell.
fn untouched_rows(fixture: &Fixture) -> Vec<usize> {
    (0..fixture.model.records().len())
        .filter(|row| {
            !fixture.buffer.is_pending_delete(*row)
                && (0..fixture.model.column_count())
                    .all(|column| !fixture.buffer.is_cell_dirty(*row, column))
        })
        .collect()
}

/// Whether `saved` is `original`, or `original` with a terminator added, which
/// the final record of a file without one gains when a record follows it.
fn keeps_bytes(saved: &[u8], original: &[u8], is_last_of_file: bool) -> bool {
    if saved == original {
        return true;
    }

    is_last_of_file
        && saved.starts_with(original)
        && matches!(
            &saved[original.len()..],
            b"\n" | b"\r\n" | b"\n\0" | b"\r\0\n\0"
        )
}

/// One random case: a file of a random dialect, read whole or in part,
/// random pending changes, and a random edit of its raw text. Returns false
/// when the edit is refused, which is checked by its own tests.
fn random_case(cx: &mut TestAppContext, case: usize) -> bool {
    let seed = 0x9E37_79B9_7F4A_7C15 ^ (case as u64 + 1);
    let mut random = Random(seed);

    let dialect = random_dialect(&mut random);
    let text = random_file_text(&mut random, &dialect);
    let bytes = encode(&text, &dialect, random.chance(30));

    let original = read_all(&bytes, dialect);
    let data_records = original.len() - usize::from(dialect.has_header && !original.is_empty());

    let mut fixture = if random.chance(70) {
        Fixture::load(&bytes, dialect, 100, 1)
    } else {
        let page_size = 1 + random.below(3);
        Fixture::load(&bytes, dialect, page_size, 1 + random.below(2))
    };

    random_table_edits(&mut random, &mut fixture);

    let rendered = fixture.rendered();
    let lines = random_text_edit(&mut random, &fixture, &rendered);
    let edited: String = lines.iter().map(|line| line.text.as_str()).collect();

    let context =
        format!("case {case} (seed {seed:#x}), {dialect:?}: {text:?} edited into {edited:?}");

    let Ok(edits) = fixture.edits_for(&edited) else {
        return false;
    };

    // What the document checks before it applies, and the state it checks.
    let mut simulated_model = std::borrow::Cow::Borrowed(&fixture.model);
    let mut simulated_buffer = fixture.buffer.clone();
    apply_to_pending(&edits, &mut simulated_model, &mut simulated_buffer)
        .expect("the column changes apply");

    if check_pending(&simulated_model, &fixture.span, &simulated_buffer, &dialect).is_err() {
        return false;
    }

    // A row after the last loaded record of a file that is not fully loaded
    // is refused by a save, as the table's own row there is.
    let Ok(simulated_edit_set) = simulated_model.edit_set(bytes.len() as u64, &simulated_buffer)
    else {
        return false;
    };

    let untouched_before = untouched_rows(&fixture);
    let had_header = fixture.model.header().cloned();

    // Whether the apply makes a step of the table's undo history: every
    // operation does, except a cell set to the value its record holds when
    // nothing was staged for it.
    let makes_an_undo_step = !edits.deleted_rows.is_empty()
        || !edits.removed_inserts.is_empty()
        || !edits.added_inserts.is_empty()
        || !edits.insert_cells.is_empty()
        || edits.base_cells.iter().any(|(row, column, value)| {
            let own = fixture.model.records()[*row]
                .fields
                .get(*column)
                .map_or("", String::as_str);

            own != value || fixture.buffer.is_cell_dirty(*row, *column)
        });

    let table_model = Arc::new(fixture.model.table_model());
    let buffer = fixture.buffer.clone();
    let table_state = cx.new(|cx| {
        let mut state = DataTableState::new(table_model, cx);
        *state.edit_buffer_mut() = buffer;
        state.set_positional_editing(true);
        state.set_insertable(true);
        state
    });

    table_state.update(cx, |state, cx| {
        apply_text_edits(&edits, &mut fixture.model, state, cx).expect("the edits apply");
    });
    fixture.buffer = table_state.read_with(cx, |state, _| state.edit_buffer().clone());

    // As for the rows below: when a line the user touched reads as a
    // rendered line, or a line appears twice, which row is the user's cannot
    // be told.
    let rendered_lines: Vec<&str> = rendered
        .layout
        .records
        .iter()
        .map(|record| split_terminator(&rendered.text[record.text.clone()]).0)
        .collect();
    let edited_lines: Vec<&str> = lines
        .iter()
        .map(|line| split_terminator(&line.text).0)
        .collect();
    let is_ambiguous = lines.iter().any(|line| {
        line.touch != Touch::Untouched && rendered_lines.contains(&split_terminator(&line.text).0)
    }) || edited_lines
        .iter()
        .enumerate()
        .any(|(index, line)| edited_lines[..index].contains(line));

    if makes_an_undo_step && !is_ambiguous {
        check_one_undo(cx, &table_state, &lines, &dialect, &context);
    }

    assert_eq!(
        Ok(simulated_edit_set),
        fixture.model.edit_set(bytes.len() as u64, &fixture.buffer),
        "{context}: the checked state is the applied one"
    );

    let saved = save(&fixture);
    let saved_records = read_all(&saved, dialect);

    // Record by record and position by position: the edited text, then the
    // records that were not loaded.
    let loaded_records = fixture.model.records().len();
    let tail = original
        .get(original.len() - (data_records - loaded_records)..)
        .unwrap_or_default();

    let edited_records = parse_text(&edited, &dialect).expect("the edited text reads");

    let expected: Vec<Vec<String>> = edited_records
        .iter()
        .map(|record| trimmed(&record.fields))
        .chain(tail.iter().map(|record| trimmed(&record.fields)))
        .collect();
    let found: Vec<Vec<String>> = saved_records
        .iter()
        .map(|record| trimmed(&record.fields))
        .collect();

    assert_eq!(found, expected, "{context}: saved {saved:?}");

    let saved_bytes = |index: usize| {
        let range = &saved_records[index].byte_range;
        &saved[range.start as usize..range.end as usize]
    };
    let original_bytes =
        |record: &Record| &bytes[record.byte_range.start as usize..record.byte_range.end as usize];

    // Which loaded record is at each place of the saved file: the table's
    // rows in order, without the deleted ones, after the header.
    let header_records = usize::from(had_header.is_some());
    let row_saved_at: std::collections::HashMap<usize, usize> = fixture
        .buffer
        .compute_visual_order()
        .into_iter()
        .filter(|source| {
            !matches!(source, VisualRowSource::Base(row) if fixture.buffer.is_pending_delete(*row))
        })
        .enumerate()
        .filter_map(|(index, source)| match source {
            VisualRowSource::Base(row) => Some((header_records + index, row)),
            VisualRowSource::Insert(_) => None,
        })
        .collect();

    let untouched_after = untouched_rows(&fixture);
    let columns_appended = !edits.appended_columns.is_empty();
    let header_changed = columns_appended || !edits.renames.is_empty();
    let records = fixture.model.records();

    // Every line the user did not touch is saved where it is, from a row
    // nobody edited that holds exactly its bytes, and unless a column was
    // added to every record, with those bytes. A requoted line is touched:
    // its fields are checked with every other record above.
    //
    // A line that appears more than once, in the rendered or in the edited
    // text, cannot be told from its copies, and several shortest edit
    // scripts keep different copies, so only lines that appear once are
    // checked. A requoted line is checked only when its fields appear once.
    let content = |text: &str| split_terminator(text).0.to_string();

    let rendered_contents: Vec<String> = rendered
        .layout
        .records
        .iter()
        .map(|record| content(&rendered.text[record.text.clone()]))
        .collect();
    let edited_contents: Vec<String> = lines.iter().map(|line| content(&line.text)).collect();

    let line_fields: Vec<Vec<String>> = lines
        .iter()
        .map(|line| {
            parse_text(&line.text, &dialect).expect("a line reads")[0]
                .fields
                .clone()
        })
        .collect();

    let has_twin = |position: usize| {
        let own = &edited_contents[position];
        let count = |contents: &[String]| contents.iter().filter(|other| *other == own).count();

        count(&edited_contents) > 1
            || count(&rendered_contents) > 1
            || (lines[position].touch == Touch::Requoted
                && line_fields
                    .iter()
                    .filter(|fields| **fields == line_fields[position])
                    .count()
                    > 1)
    };

    // A typed or changed line that reads as a line of the rendered text is
    // that line moved, as far as anyone can tell, and a moved line can be
    // kept in place of the lines it moved past. Such cases are left to the
    // record comparison.
    let reads_as_a_rendered_line = lines.iter().any(|line| {
        matches!(line.touch, Touch::New | Touch::Changed)
            && rendered_contents.contains(&content(&line.text))
    });

    for (position, line) in lines.iter().enumerate() {
        if line.touch != Touch::Untouched || reads_as_a_rendered_line {
            continue;
        }

        if has_twin(position) {
            continue;
        }

        match line.row {
            Some(TextRow::Base(row)) if untouched_before.contains(&row) => {
                let saved_row = row_saved_at.get(&position).copied();

                let Some(saved_row) = saved_row.filter(|saved_row| {
                    untouched_before.contains(saved_row) && untouched_after.contains(saved_row)
                }) else {
                    panic!(
                        "{context}: the untouched line {position} of row {row} is saved from {saved_row:?}, which was edited or is a new row"
                    );
                };

                let is_twin = match line.touch {
                    Touch::Untouched => {
                        original_bytes(&records[saved_row]) == original_bytes(&records[row])
                    }
                    _ => records[saved_row].fields == records[row].fields,
                };

                assert!(
                    is_twin,
                    "{context}: the line {position} of row {row} is saved from row {saved_row}"
                );

                if columns_appended {
                    continue;
                }

                let record = &records[saved_row];
                let is_last_of_file = record.byte_range.end as usize == bytes.len();

                assert!(
                    keeps_bytes(
                        saved_bytes(position),
                        original_bytes(record),
                        is_last_of_file
                    ),
                    "{context}: line {position}, row {saved_row}, was rewritten as {:?}",
                    saved_bytes(position)
                );
            }

            Some(TextRow::Header) if !header_changed => {
                let header = had_header.as_ref().expect("a header was rendered");

                assert_eq!(
                    saved_bytes(position),
                    original_bytes(header),
                    "{context}: the header was rewritten"
                );
            }

            _ => {}
        }
    }

    // The records that were not loaded are copied.
    let tail_start = saved_records.len() - tail.len();

    for (index, record) in tail.iter().enumerate() {
        assert_eq!(
            saved_bytes(tail_start + index),
            original_bytes(record),
            "{context}: a record that was not loaded changed"
        );
    }

    true
}

/// Every visual row of the table: whether it is marked for deletion, and
/// its cells with the pending changes.
fn table_rows(
    table_state: &Entity<DataTableState>,
    cx: &mut TestAppContext,
) -> Vec<(bool, Vec<String>)> {
    table_state.read_with(cx, |state, _| {
        let buffer = state.edit_buffer();
        let model = state.model();
        let absent = CellValue::text("");

        buffer
            .compute_visual_order()
            .into_iter()
            .map(|source| match source {
                VisualRowSource::Base(row) => (
                    buffer.is_pending_delete(row),
                    (0..model.col_count())
                        .map(|column| {
                            let base = model.cell(row, column).unwrap_or(&absent);
                            buffer.get_cell(row, column, base).edit_text()
                        })
                        .collect(),
                ),

                VisualRowSource::Insert(index) => (
                    false,
                    buffer.pending_inserts()[index]
                        .data
                        .iter()
                        .map(CellValue::edit_text)
                        .collect(),
                ),
            })
            .collect()
    })
}

/// Undoes one step of the table's history after an applied text edit and
/// checks that it changed only a row the user edited: it removes a row the
/// user added, takes back the user's change of a row, or brings back a row
/// the user removed. A row the user did not touch is never removed or
/// changed.
fn check_one_undo(
    cx: &mut TestAppContext,
    table_state: &Entity<DataTableState>,
    lines: &[Line],
    dialect: &Dialect,
    context: &str,
) {
    let user_rows: Vec<Vec<String>> = lines
        .iter()
        .filter(|line| matches!(line.touch, Touch::New | Touch::Changed))
        .map(|line| trimmed(&parse_text(&line.text, dialect).expect("a line reads")[0].fields))
        .collect();

    let applied = table_rows(table_state, cx);

    table_state.update(cx, |state, _| {
        assert!(
            state.edit_buffer_mut().undo(),
            "{context}: there is a step to undo"
        );
    });

    let undone = table_rows(table_state, cx);

    let differs = |index: usize| applied.get(index) != undone.get(index);
    let first = (0..applied.len().max(undone.len()))
        .find(|index| differs(*index))
        .unwrap_or_else(|| panic!("{context}: one undo changed nothing"));

    let made_by_the_user = |row: &(bool, Vec<String>)| user_rows.contains(&trimmed(&row.1));

    if undone.len() + 1 == applied.len() {
        assert!(
            made_by_the_user(&applied[first]),
            "{context}: one undo removed {:?}, a row the user did not add",
            applied[first]
        );
    } else if undone.len() == applied.len() {
        let changed: Vec<usize> = (0..applied.len()).filter(|index| differs(*index)).collect();

        assert_eq!(changed.len(), 1, "{context}: one undo changed {changed:?}");

        let was_deleted_by_the_user = applied[first].0 && !undone[first].0;

        assert!(
            was_deleted_by_the_user || made_by_the_user(&applied[first]),
            "{context}: one undo changed {:?} into {:?}, a row the user did not edit",
            applied[first],
            undone[first]
        );
    } else {
        assert_eq!(
            undone.len(),
            applied.len() + 1,
            "{context}: one undo brought back a row the user removed, or nothing else"
        );
    }
}

/// Random small files of every encoding, delimiter, quoting and header the
/// document reads, read whole or in part, with random pending changes, whose
/// raw text is edited at random and applied: the saved file holds the edited
/// text record by record, followed by the records that were not loaded, and
/// every line the user did not touch keeps its row and its bytes.
#[gpui::test]
fn random_text_edits_save_what_the_text_says(cx: &mut TestAppContext) {
    let applied = (0..RANDOM_CASES)
        .filter(|case| random_case(cx, *case))
        .count();

    assert!(
        applied >= RANDOM_CASES * 4 / 5,
        "only {applied} of {RANDOM_CASES} random edits applied"
    );
}
