use std::num::{NonZeroU64, NonZeroUsize};

use dbflux_components::components::data_table::model::{
    CellKind, CellValue, EditBuffer, InsertAnchor,
};
use dbflux_delimited::{
    Dialect, Encoding, InsertPosition, MemorySource, Page, PagedReader, ReaderOptions, RecordCount,
    write_edited,
};

use super::page_model::{PageModel, PageModelError};

const PEOPLE: &[u8] = b"name,city,age\r\nAna,Lima,30\r\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n";

/// A page size no fixture reaches, so one page holds the whole file.
const WHOLE_FILE: usize = 100;

fn window() -> NonZeroU64 {
    NonZeroU64::new(8).expect("a non-zero window")
}

fn dialect(has_header: bool) -> Dialect {
    Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header,
        encoding: Encoding::for_label(b"utf-8").expect("a known encoding label"),
    }
}

fn reader_over(bytes: &[u8], dialect: Dialect, page_size: usize) -> PagedReader<MemorySource> {
    let options = ReaderOptions {
        page_size: NonZeroUsize::new(page_size).expect("a non-zero page size"),
        window_size: window(),
    };

    PagedReader::open(MemorySource::new(bytes.to_vec()), dialect, options)
        .expect("the reader opens")
}

struct Fixture {
    model: PageModel,
    source: MemorySource,
    dialect: Dialect,
    source_length: u64,
}

impl Fixture {
    /// An edit buffer over the loaded rows, as the table holds it.
    fn buffer(&self) -> EditBuffer {
        let mut buffer = EditBuffer::new();
        buffer.set_base_row_count(self.model.records().len());
        buffer
    }

    /// Writes the source with `edits` applied and returns the new file.
    fn save(&self, edits: &EditBuffer) -> Vec<u8> {
        let edit_set = self
            .model
            .edit_set(self.source_length, edits)
            .expect("the edit set builds");

        let mut output = Vec::new();

        write_edited(
            &self.source,
            &self.dialect,
            &edit_set,
            window(),
            &mut output,
        )
        .expect("the edited file writes");

        output
    }
}

/// Loads the first `pages` pages of `bytes` into a page model.
fn load(bytes: &[u8], has_header: bool, page_size: usize, pages: usize) -> Fixture {
    let dialect = dialect(has_header);
    let mut reader = reader_over(bytes, dialect, page_size);
    let mut model = PageModel::new(reader.header().cloned());

    for page_index in 0..pages {
        let page = reader.read_page(page_index).expect("the page reads");

        model
            .append_page(page, reader.record_count())
            .expect("the page follows the loaded records");
    }

    let source_length = reader.source_length();

    Fixture {
        model,
        source: reader.into_source(),
        dialect,
        source_length,
    }
}

fn load_whole(bytes: &[u8], has_header: bool) -> Fixture {
    load(bytes, has_header, WHOLE_FILE, 1)
}

/// The header fields and the fields of every data record of `bytes`.
fn read_back(bytes: &[u8], has_header: bool) -> (Option<Vec<String>>, Vec<Vec<String>>) {
    let mut reader = reader_over(bytes, dialect(has_header), WHOLE_FILE);
    let header = reader.header().map(|record| record.fields.clone());
    let page = reader.read_page(0).expect("the saved file reads");

    let records = page
        .records
        .into_iter()
        .map(|record| record.fields)
        .collect();

    (header, records)
}

fn fields(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn text_cells(values: &[&str]) -> Vec<CellValue> {
    values.iter().map(|value| CellValue::text(value)).collect()
}

/// The text of every cell of the table model, row by row.
fn table_texts(model: &PageModel) -> Vec<Vec<String>> {
    model
        .table_model()
        .rows
        .iter()
        .map(|row| row.cells.iter().map(CellValue::edit_text).collect())
        .collect()
}

#[test]
fn column_names_come_from_the_header() {
    let fixture = load_whole(PEOPLE, true);

    assert_eq!(
        fixture.model.column_names(),
        fields(&["name", "city", "age"])
    );

    let table = fixture.model.table_model();
    let titles: Vec<&str> = table
        .columns
        .iter()
        .map(|column| column.title.as_ref())
        .collect();

    assert_eq!(titles, ["name", "city", "age"]);
}

#[test]
fn columns_without_a_header_get_positional_names() {
    let fixture = load_whole(b"Ana,Lima\nBo,Quito\n", false);

    assert_eq!(
        fixture.model.column_names(),
        fields(&["column_1", "column_2"])
    );
    assert_eq!(fixture.model.records().len(), 2);
}

#[test]
fn an_empty_header_field_gets_a_positional_name() {
    let fixture = load_whole(b"name,,age\nAna,Lima,30\n", true);

    assert_eq!(
        fixture.model.column_names(),
        fields(&["name", "column_2", "age"])
    );
}

#[test]
fn ragged_records_are_padded_with_empty_cells() {
    let fixture = load_whole(b"a,b\n1\n2,3,4\n", true);

    assert_eq!(fixture.model.column_count(), 3);
    assert_eq!(
        fixture.model.column_names(),
        fields(&["a", "b", "column_3"])
    );

    assert_eq!(
        table_texts(&fixture.model),
        vec![fields(&["1", "", ""]), fields(&["2", "3", "4"])]
    );
}

#[test]
fn an_empty_field_is_an_empty_text_cell_and_not_a_null() {
    let fixture = load_whole(b"a,b\n1,\n2\n", true);
    let table = fixture.model.table_model();

    for row in &table.rows {
        for cell in &row.cells {
            assert!(
                matches!(cell.kind, CellKind::Text(_)),
                "every cell is text, found {:?}",
                cell.kind
            );
        }
    }
}

#[test]
fn a_wider_later_page_widens_the_columns() {
    let bytes = b"a,b\n1,2\n3,4,5,6\n";
    let mut reader = reader_over(bytes, dialect(true), 1);
    let mut model = PageModel::new(reader.header().cloned());

    let first = reader.read_page(0).expect("the first page reads");
    let widened_by = model
        .append_page(first, reader.record_count())
        .expect("the first page appends");

    assert_eq!(widened_by, 0);
    assert_eq!(model.column_count(), 2);

    let second = reader.read_page(1).expect("the second page reads");
    let widened_by = model
        .append_page(second, reader.record_count())
        .expect("the second page appends");

    assert_eq!(widened_by, 2);
    assert_eq!(
        model.column_names(),
        fields(&["a", "b", "column_3", "column_4"])
    );
    assert_eq!(
        table_texts(&model),
        vec![fields(&["1", "2", "", ""]), fields(&["3", "4", "5", "6"])]
    );
}

#[test]
fn appending_a_page_keeps_earlier_records_and_updates_the_count() {
    let mut reader = reader_over(PEOPLE, dialect(true), 1);
    let mut model = PageModel::new(reader.header().cloned());

    assert_eq!(model.next_page(), 0);
    assert_eq!(model.record_count(), RecordCount::IndexedSoFar(0));

    let first = reader.read_page(0).expect("the first page reads");
    let count_after_first = reader.record_count();
    model
        .append_page(first, count_after_first)
        .expect("the first page appends");

    assert!(matches!(count_after_first, RecordCount::IndexedSoFar(_)));
    assert_eq!(model.record_count(), count_after_first);
    assert!(!model.is_fully_loaded());
    assert_eq!(model.next_page(), 1);

    let first_range = model
        .records()
        .first()
        .expect("one loaded record")
        .byte_range
        .clone();

    for page_index in 1..4 {
        let page = reader.read_page(page_index).expect("the page reads");

        model
            .append_page(page, reader.record_count())
            .expect("the page appends");
    }

    let names: Vec<&str> = model
        .records()
        .iter()
        .filter_map(|record| record.fields.first().map(String::as_str))
        .collect();

    assert_eq!(names, ["Ana", "Bo, Jr", "Cy"]);
    assert_eq!(
        model.records().first().map(|record| &record.byte_range),
        Some(&first_range)
    );
    assert_eq!(model.record_count(), RecordCount::Total(3));
    assert!(model.is_fully_loaded());
}

#[test]
fn a_page_that_does_not_follow_the_loaded_records_is_refused() {
    let mut reader = reader_over(PEOPLE, dialect(true), 1);
    let mut model = PageModel::new(reader.header().cloned());

    let third = reader.read_page(2).expect("the third page reads");
    let error = model
        .append_page(third, reader.record_count())
        .expect_err("a page that skips records is refused");

    assert_eq!(
        error,
        PageModelError::PageOutOfOrder {
            expected: 0,
            found: 2
        }
    );
    assert!(model.records().is_empty());
}

#[test]
fn a_replaced_malformed_sequence_in_any_loaded_record_is_reported() {
    let clean = load_whole(PEOPLE, true);
    assert!(!clean.model.had_replacements());

    let malformed = load_whole(b"name\nAna\nB\xFFo\n", true);
    assert!(malformed.model.had_replacements());
}

#[test]
fn no_pending_edits_give_an_empty_edit_set_and_the_same_bytes() {
    let fixture = load_whole(PEOPLE, true);
    let buffer = fixture.buffer();

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    assert!(edit_set.is_empty());
    assert_eq!(edit_set.source_length, PEOPLE.len() as u64);
    assert_eq!(fixture.save(&buffer), PEOPLE);
}

#[test]
fn a_cell_edit_changes_only_that_cell() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.set_cell(1, 1, CellValue::text("Cuenca"));

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\nAna,Lima,30\r\n\"Bo, Jr\",Cuenca,41\nCy,Rome,25\n"
    );
}

#[test]
fn an_edit_that_restores_the_original_value_gives_no_replacement() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.set_cell(1, 1, CellValue::text("Cuenca"));
    buffer.set_cell(1, 1, CellValue::text("Quito"));

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    assert!(edit_set.is_empty());
    assert_eq!(fixture.save(&buffer), PEOPLE);
}

#[test]
fn a_cell_edit_beyond_a_short_record_pads_it_up_to_that_column() {
    let fixture = load_whole(b"a,b,c\n1\n2,3,4\n", true);
    let mut buffer = fixture.buffer();
    buffer.set_cell(0, 2, CellValue::text("z"));

    assert_eq!(fixture.save(&buffer), b"a,b,c\n1,,z\n2,3,4\n");
}

#[test]
fn an_empty_edit_beyond_a_short_record_gives_no_replacement() {
    let source = b"a,b,c\n1\n2,3,4\n";
    let fixture = load_whole(source, true);
    let mut buffer = fixture.buffer();
    buffer.set_cell(0, 2, CellValue::text(""));

    assert_eq!(fixture.save(&buffer), source);
}

#[test]
fn set_null_becomes_an_empty_string() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.set_cell(0, 1, CellValue::null());

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\nAna,,30\r\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n"
    );
}

#[test]
fn a_deleted_row_removes_only_its_record() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.mark_for_delete(0);

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n"
    );
}

#[test]
fn a_row_inserted_in_the_middle_goes_before_the_record_that_follows_it() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.add_pending_insert_after(0, text_cells(&["Di", "Oslo", "9"]));

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\nAna,Lima,30\r\nDi,Oslo,9\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n"
    );
}

#[test]
fn a_row_inserted_before_a_deleted_record_takes_its_place() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.mark_for_delete(1);
    buffer.add_pending_insert_after(0, text_cells(&["Di", "Oslo", "9"]));

    let (_, records) = read_back(&fixture.save(&buffer), true);

    assert_eq!(
        records,
        vec![
            fields(&["Ana", "Lima", "30"]),
            fields(&["Di", "Oslo", "9"]),
            fields(&["Cy", "Rome", "25"]),
        ]
    );
}

#[test]
fn a_row_inserted_into_a_file_without_records_is_its_first_record() {
    let source = b"name,city\r\n";
    let fixture = load_whole(source, true);
    assert!(fixture.model.is_fully_loaded());

    let mut buffer = fixture.buffer();
    buffer.add_pending_insert(text_cells(&["Di", "Oslo"]));

    let saved = fixture.save(&buffer);
    let (header, records) = read_back(&saved, true);

    assert!(saved.starts_with(source));
    assert_eq!(header, Some(fields(&["name", "city"])));
    assert_eq!(records, vec![fields(&["Di", "Oslo"])]);
}

#[test]
fn a_row_inserted_above_the_first_row_is_the_first_record_of_the_saved_file() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_cells(&["Di", "Oslo", "9"]));

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    let first_record = fixture.model.records()[0].byte_range.clone();

    assert_eq!(
        edit_set
            .insertions
            .iter()
            .map(|insertion| &insertion.position)
            .collect::<Vec<_>>(),
        [&InsertPosition::Before(first_record)]
    );

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\nDi,Oslo,9\r\nAna,Lima,30\r\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n"
    );
}

#[test]
fn rows_inserted_above_and_below_the_first_row_are_saved_in_the_order_the_table_shows() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.add_pending_insert_after(0, text_cells(&["Ed", "Bern", "7"]));
    buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_cells(&["Di", "Oslo", "9"]));
    buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_cells(&["Fa", "Riga", "5"]));

    let (_, records) = read_back(&fixture.save(&buffer), true);

    assert_eq!(
        records,
        vec![
            fields(&["Di", "Oslo", "9"]),
            fields(&["Fa", "Riga", "5"]),
            fields(&["Ana", "Lima", "30"]),
            fields(&["Ed", "Bern", "7"]),
            fields(&["Bo, Jr", "Quito", "41"]),
            fields(&["Cy", "Rome", "25"]),
        ]
    );
}

#[test]
fn a_row_inserted_above_the_first_row_of_a_partly_loaded_file_is_placed() {
    let fixture = load(PEOPLE, true, 1, 1);
    assert!(!fixture.model.is_fully_loaded());

    let mut buffer = fixture.buffer();
    buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_cells(&["Di", "Oslo", "9"]));

    let (_, records) = read_back(&fixture.save(&buffer), true);

    assert_eq!(records.first(), Some(&fields(&["Di", "Oslo", "9"])));
    assert_eq!(records.len(), 4);
}

#[test]
fn a_row_inserted_above_the_first_row_of_a_file_without_records_goes_at_the_end() {
    let source = b"name,city\r\n";
    let fixture = load_whole(source, true);
    assert!(fixture.model.is_fully_loaded());

    let mut buffer = fixture.buffer();
    buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_cells(&["Di", "Oslo"]));

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    assert_eq!(
        edit_set
            .insertions
            .iter()
            .map(|insertion| &insertion.position)
            .collect::<Vec<_>>(),
        [&InsertPosition::End]
    );

    let saved = fixture.save(&buffer);
    let (header, records) = read_back(&saved, true);

    assert!(saved.starts_with(source));
    assert_eq!(header, Some(fields(&["name", "city"])));
    assert_eq!(records, vec![fields(&["Di", "Oslo"])]);
}

#[test]
fn a_row_inserted_after_the_last_record_of_a_fully_loaded_file_goes_at_the_end() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.add_pending_insert_after(2, text_cells(&["Di", "Oslo", "9"]));
    buffer.add_pending_insert(text_cells(&["Ed", "Bern", "7"]));

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    let positions: Vec<&InsertPosition> = edit_set
        .insertions
        .iter()
        .map(|insertion| &insertion.position)
        .collect();

    assert_eq!(positions, [&InsertPosition::End, &InsertPosition::End]);

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\nAna,Lima,30\r\n\"Bo, Jr\",Quito,41\nCy,Rome,25\nDi,Oslo,9\nEd,Bern,7\n"
    );
}

#[test]
fn a_row_inserted_after_the_last_loaded_record_of_a_partly_loaded_file_needs_the_next_page() {
    let fixture = load(PEOPLE, true, 1, 1);
    assert!(!fixture.model.is_fully_loaded());

    let mut after_the_last_row = fixture.buffer();
    after_the_last_row.add_pending_insert_after(0, text_cells(&["Di", "Oslo", "9"]));

    assert_eq!(
        fixture
            .model
            .edit_set(fixture.source_length, &after_the_last_row),
        Err(PageModelError::NextPageRequired)
    );

    let mut at_the_end = fixture.buffer();
    at_the_end.add_pending_insert(text_cells(&["Di", "Oslo", "9"]));

    assert_eq!(
        fixture.model.edit_set(fixture.source_length, &at_the_end),
        Err(PageModelError::NextPageRequired)
    );
}

#[test]
fn a_row_inserted_before_a_loaded_record_of_a_partly_loaded_file_is_placed() {
    let fixture = load(PEOPLE, true, 1, 2);
    assert!(!fixture.model.is_fully_loaded());

    let mut buffer = fixture.buffer();
    buffer.add_pending_insert_after(0, text_cells(&["Di", "Oslo", "9"]));

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\nAna,Lima,30\r\nDi,Oslo,9\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n"
    );
}

#[test]
fn a_short_inserted_row_is_padded_to_every_column() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.add_pending_insert(vec![CellValue::text("Di"), CellValue::null()]);

    let (_, records) = read_back(&fixture.save(&buffer), true);

    assert_eq!(records.last(), Some(&fields(&["Di", "", ""])));
}

#[test]
fn a_row_edited_and_then_deleted_is_a_deletion() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.set_cell(0, 1, CellValue::text("Cusco"));
    buffer.mark_for_delete(0);

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    assert!(edit_set.replacements.is_empty());
    assert_eq!(edit_set.deletions.len(), 1);

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n"
    );
}

#[test]
fn a_row_inserted_and_then_deleted_gives_nothing() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    let insert_index = buffer.add_pending_insert_after(0, text_cells(&["Di", "Oslo", "9"]));
    buffer.remove_pending_insert_by_idx(insert_index);

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    assert!(edit_set.is_empty());
    assert_eq!(fixture.save(&buffer), PEOPLE);
}

#[test]
fn a_header_rename_replaces_only_the_header_record() {
    let mut fixture = load_whole(PEOPLE, true);

    fixture
        .model
        .rename_column(1, "town".to_string())
        .expect("the column renames");

    assert!(fixture.model.has_column_changes());
    assert_eq!(
        fixture.model.column_names(),
        fields(&["name", "town", "age"])
    );

    let saved = fixture.save(&fixture.buffer());

    assert_eq!(
        saved,
        b"name,town,age\r\nAna,Lima,30\r\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n"
    );
    assert_eq!(
        read_back(&saved, true).0,
        Some(fields(&["name", "town", "age"]))
    );
}

#[test]
fn a_rename_back_to_the_original_name_gives_no_replacement() {
    let mut fixture = load_whole(PEOPLE, true);

    fixture
        .model
        .rename_column(1, "town".to_string())
        .expect("the column renames");
    fixture
        .model
        .rename_column(1, "city".to_string())
        .expect("the column renames back");

    assert!(!fixture.model.has_column_changes());
    assert_eq!(fixture.save(&fixture.buffer()), PEOPLE);
}

#[test]
fn a_rename_of_a_column_the_header_does_not_reach_pads_the_header() {
    let mut fixture = load_whole(b"a,b\n1,2,3\n", true);

    fixture
        .model
        .rename_column(2, "c".to_string())
        .expect("the column renames");

    assert_eq!(fixture.save(&fixture.buffer()), b"a,b,c\n1,2,3\n");
}

#[test]
fn a_rename_is_refused_without_a_header_or_for_a_column_that_does_not_exist() {
    let mut headerless = load_whole(b"Ana,Lima\n", false);

    assert_eq!(
        headerless.model.rename_column(0, "name".to_string()),
        Err(PageModelError::NoHeader)
    );

    let mut with_header = load_whole(PEOPLE, true);

    assert_eq!(
        with_header.model.rename_column(3, "country".to_string()),
        Err(PageModelError::ColumnOutOfRange { column: 3 })
    );
    assert!(!with_header.model.has_column_changes());
}

#[test]
fn an_appended_column_is_saved_with_its_name_and_values() {
    let mut fixture = load_whole(PEOPLE, true);
    fixture
        .model
        .append_column("country".to_string())
        .expect("the column appends");

    assert!(fixture.model.has_column_changes());
    assert_eq!(
        fixture.model.column_names(),
        fields(&["name", "city", "age", "country"])
    );
    assert_eq!(
        table_texts(&fixture.model).first(),
        Some(&fields(&["Ana", "Lima", "30", ""]))
    );

    let mut buffer = fixture.buffer();
    buffer.set_cell(0, 3, CellValue::text("PE"));
    buffer.set_cell(1, 1, CellValue::text("Cuenca"));
    buffer.set_cell(1, 3, CellValue::text("EC"));
    buffer.add_pending_insert(text_cells(&["Di", "Oslo", "9", "NO"]));

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    let first_range = fixture
        .model
        .records()
        .first()
        .expect("a first record")
        .byte_range
        .clone();

    assert_eq!(edit_set.replacements.len(), 1);
    assert_eq!(edit_set.appended_columns.len(), 1);
    assert_eq!(
        edit_set
            .appended_columns
            .first()
            .map(|column| column.values.clone()),
        Some(vec![(first_range, "PE".to_string())])
    );

    let saved = fixture.save(&buffer);

    assert_eq!(
        saved,
        b"name,city,age,country\r\nAna,Lima,30,PE\r\n\"Bo, Jr\",Cuenca,41,EC\nCy,Rome,25,\nDi,Oslo,9,NO\n"
    );

    let (header, records) = read_back(&saved, true);

    assert_eq!(header, Some(fields(&["name", "city", "age", "country"])));
    assert_eq!(
        records,
        vec![
            fields(&["Ana", "Lima", "30", "PE"]),
            fields(&["Bo, Jr", "Cuenca", "41", "EC"]),
            fields(&["Cy", "Rome", "25", ""]),
            fields(&["Di", "Oslo", "9", "NO"]),
        ]
    );
}

#[test]
fn an_appended_column_can_be_renamed_and_keeps_a_renamed_header() {
    let mut fixture = load_whole(PEOPLE, true);
    fixture
        .model
        .append_column("country".to_string())
        .expect("the column appends");

    fixture
        .model
        .rename_column(3, "nation".to_string())
        .expect("the appended column renames");
    fixture
        .model
        .rename_column(0, "person".to_string())
        .expect("the column renames");

    let (header, records) = read_back(&fixture.save(&fixture.buffer()), true);

    assert_eq!(header, Some(fields(&["person", "city", "age", "nation"])));
    assert_eq!(records.first(), Some(&fields(&["Ana", "Lima", "30", ""])));
}

#[test]
fn a_value_in_an_appended_column_of_a_short_record_lands_in_that_column() {
    let mut fixture = load_whole(b"a,b\n1\n2,3\n", true);
    fixture
        .model
        .append_column("c".to_string())
        .expect("the column appends");

    let mut buffer = fixture.buffer();
    buffer.set_cell(0, 2, CellValue::text("z"));

    let saved = fixture.save(&buffer);

    assert_eq!(saved, b"a,b,c\n1,,z\n2,3,\n");
}

#[test]
fn an_appended_column_of_a_file_without_a_header_gets_a_positional_name() {
    let mut fixture = load_whole(b"Ana,Lima\nBo,Quito\n", false);
    fixture
        .model
        .append_column("ignored".to_string())
        .expect("the column appends");

    assert_eq!(
        fixture.model.column_names(),
        fields(&["column_1", "column_2", "column_3"])
    );

    let mut buffer = fixture.buffer();
    buffer.set_cell(1, 2, CellValue::text("EC"));

    assert_eq!(fixture.save(&buffer), b"Ana,Lima,\nBo,Quito,EC\n");
}

#[test]
fn several_edits_of_different_kinds_land_in_one_save() {
    let source = b"id,name,city\n1,Ana,Lima\n2,Bo,Quito\n3,Cy,Rome\n4,Di,Oslo\n5,Ed,Bern\n";
    let mut fixture = load_whole(source, true);

    fixture
        .model
        .rename_column(2, "town".to_string())
        .expect("the column renames");
    fixture
        .model
        .append_column("country".to_string())
        .expect("the column appends");

    let mut buffer = fixture.buffer();
    buffer.set_cell(0, 1, CellValue::text("Anna"));
    buffer.mark_for_delete(1);
    buffer.add_pending_insert_after(1, text_cells(&["2b", "Flo", "Kyiv", "UA"]));
    buffer.set_cell(2, 2, CellValue::null());
    buffer.set_cell(3, 3, CellValue::text("NO"));
    buffer.set_cell(4, 0, CellValue::text("5"));
    buffer.add_pending_insert(text_cells(&["6", "Gil", "Rabat", "MA"]));

    let (header, records) = read_back(&fixture.save(&buffer), true);

    assert_eq!(header, Some(fields(&["id", "name", "town", "country"])));
    assert_eq!(
        records,
        vec![
            fields(&["1", "Anna", "Lima", ""]),
            fields(&["2b", "Flo", "Kyiv", "UA"]),
            fields(&["3", "Cy", "", ""]),
            fields(&["4", "Di", "Oslo", "NO"]),
            fields(&["5", "Ed", "Bern", ""]),
            fields(&["6", "Gil", "Rabat", "MA"]),
        ]
    );
}

#[test]
fn a_reset_leaves_no_stale_ranges_or_column_changes() {
    let mut fixture = load_whole(PEOPLE, true);

    fixture
        .model
        .rename_column(0, "person".to_string())
        .expect("the column renames");
    fixture
        .model
        .append_column("country".to_string())
        .expect("the column appends");

    fixture.model.reset();

    assert!(fixture.model.header().is_none());
    assert!(fixture.model.records().is_empty());
    assert_eq!(fixture.model.record_count(), RecordCount::IndexedSoFar(0));
    assert_eq!(fixture.model.next_page(), 0);
    assert_eq!(fixture.model.column_count(), 0);
    assert!(fixture.model.appended_columns().is_empty());
    assert!(!fixture.model.has_column_changes());
    assert!(!fixture.model.had_replacements());
    assert!(!fixture.model.is_fully_loaded());

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &EditBuffer::new())
        .expect("the edit set builds");

    assert!(edit_set.is_empty());
}

#[test]
fn a_column_is_not_appended_while_a_later_page_can_still_widen_the_columns() {
    let mut fixture = load(b"a,b\n1,2\n3,4,5,6\n", true, 1, 1);
    assert!(!fixture.model.is_fully_loaded());

    assert_eq!(
        fixture.model.append_column("c".to_string()),
        Err(PageModelError::FullLoadRequired)
    );
    assert!(fixture.model.appended_columns().is_empty());
    assert!(!fixture.model.has_column_changes());
    assert_eq!(fixture.model.column_names(), fields(&["a", "b"]));
}

#[test]
fn a_column_is_not_appended_while_wider_records_are_not_loaded() {
    let mut fixture = load(b"a,b\n1,2\n3,4,5,6\n7\n", true, 1, 1);
    assert_eq!(fixture.model.records().len(), 1);

    assert_eq!(
        fixture.model.append_column("c".to_string()),
        Err(PageModelError::FullLoadRequired)
    );
    assert_eq!(fixture.model.column_count(), 2);
}

#[test]
fn a_column_appended_after_the_wider_page_loaded_goes_after_every_field() {
    let mut fixture = load(b"a,b\n1,2\n3,4,5,6\n", true, 1, 3);
    assert!(fixture.model.is_fully_loaded());

    fixture
        .model
        .append_column("c".to_string())
        .expect("the column appends");

    let mut buffer = fixture.buffer();
    buffer.set_cell(0, 4, CellValue::text("z"));

    assert_eq!(fixture.save(&buffer), b"a,b,,,c\n1,2,,,z\n3,4,5,6,\n");
}

#[test]
fn an_edit_set_is_refused_when_appended_columns_meet_a_model_that_is_not_fully_loaded() {
    let mut fixture = load_whole(PEOPLE, true);

    fixture
        .model
        .append_column("country".to_string())
        .expect("the column appends");

    let no_longer_final = Page {
        first_record: 3,
        records: Vec::new(),
    };

    fixture
        .model
        .append_page(no_longer_final, RecordCount::IndexedSoFar(3))
        .expect("the empty page follows the loaded records");

    assert!(!fixture.model.is_fully_loaded());
    assert_eq!(
        fixture
            .model
            .edit_set(fixture.source_length, &fixture.buffer()),
        Err(PageModelError::FullLoadRequired)
    );
}

#[test]
fn a_row_deleted_and_then_edited_is_a_replacement() {
    let fixture = load_whole(PEOPLE, true);
    let mut buffer = fixture.buffer();
    buffer.mark_for_delete(0);
    buffer.set_cell(0, 1, CellValue::text("Cusco"));

    let edit_set = fixture
        .model
        .edit_set(fixture.source_length, &buffer)
        .expect("the edit set builds");

    assert!(edit_set.deletions.is_empty());
    assert_eq!(edit_set.replacements.len(), 1);

    assert_eq!(
        fixture.save(&buffer),
        b"name,city,age\r\nAna,Cusco,30\r\n\"Bo, Jr\",Quito,41\nCy,Rome,25\n"
    );
}

#[test]
fn a_column_appended_to_an_empty_file_has_no_name_to_write() {
    let mut fixture = load_whole(b"", true);
    assert!(fixture.model.header().is_none());
    assert!(fixture.model.is_fully_loaded());

    fixture
        .model
        .append_column("c".to_string())
        .expect("the column appends");

    assert_eq!(fixture.model.column_names(), fields(&["column_1"]));
    assert_eq!(
        fixture.model.rename_column(0, "d".to_string()),
        Err(PageModelError::NoHeader)
    );

    let mut buffer = fixture.buffer();
    buffer.add_pending_insert(text_cells(&["v"]));

    assert_eq!(fixture.save(&buffer), b"v\n");
}

#[test]
fn a_row_inserted_into_a_file_without_columns_is_one_empty_field() {
    let fixture = load_whole(b"", false);
    assert_eq!(fixture.model.column_count(), 0);

    let mut buffer = fixture.buffer();
    buffer.add_pending_insert(Vec::new());

    let saved = fixture.save(&buffer);

    assert_eq!(saved, b"\"\"\n");
    assert_eq!(read_back(&saved, false).1, vec![fields(&[""])]);
}
