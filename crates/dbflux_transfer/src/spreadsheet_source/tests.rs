use super::*;

#[test]
fn whole_numbers_inside_the_i64_range_are_integers_and_the_rest_floats() {
    assert_eq!(number_value(3.0), Value::Int(3));
    assert_eq!(number_value(-7.0), Value::Int(-7));
    assert_eq!(number_value(0.0), Value::Int(0));
    assert_eq!(number_value(2.5), Value::Float(2.5));
    assert_eq!(number_value(1e20), Value::Float(1e20));
    assert_eq!(
        number_value(9_223_372_036_854_775_808.0),
        Value::Float(9_223_372_036_854_775_808.0)
    );
    assert_eq!(
        number_value(-9_223_372_036_854_775_808.0),
        Value::Int(i64::MIN)
    );
    assert!(matches!(number_value(f64::NAN), Value::Float(_)));
    assert_eq!(number_value(f64::INFINITY), Value::Float(f64::INFINITY));
}

#[test]
fn dates_times_and_errors_map_to_values() {
    use dbflux_core::chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
    use dbflux_spreadsheet::CellErrorCode;

    let at = |year, month, day, hour, minute, second| {
        NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_opt(hour, minute, second))
            .expect("date")
    };

    assert_eq!(
        cell_value(&CellValue::Date(at(2024, 1, 31, 0, 0, 0)), true),
        Ok(Value::Date(
            NaiveDate::from_ymd_opt(2024, 1, 31).expect("date")
        ))
    );
    assert_eq!(
        cell_value(&CellValue::Date(at(2024, 3, 5, 13, 45, 30)), true),
        Ok(Value::DateTime(
            Utc.with_ymd_and_hms(2024, 3, 5, 13, 45, 30)
                .single()
                .expect("timestamp")
        ))
    );
    assert_eq!(
        cell_value(&CellValue::Date(at(1899, 12, 31, 6, 0, 0)), true),
        Ok(Value::Time(NaiveTime::from_hms_opt(6, 0, 0).expect("time")))
    );
    // Only the 1900-date-system day zero stands for a time of day; a real
    // calendar date before it (an ods date) keeps its day.
    assert_eq!(
        cell_value(&CellValue::Date(at(1890, 1, 1, 0, 0, 0)), false),
        Ok(Value::Date(
            NaiveDate::from_ymd_opt(1890, 1, 1).expect("date")
        ))
    );
    assert_eq!(
        cell_value(&CellValue::Date(at(1890, 1, 1, 0, 0, 0)), true),
        Ok(Value::Date(
            NaiveDate::from_ymd_opt(1890, 1, 1).expect("date")
        ))
    );
    assert_eq!(
        cell_value(&CellValue::Date(at(1899, 12, 31, 6, 0, 0)), false),
        Ok(Value::DateTime(
            Utc.with_ymd_and_hms(1899, 12, 31, 6, 0, 0)
                .single()
                .expect("timestamp")
        ))
    );
    assert_eq!(
        cell_value(&CellValue::Error(CellErrorCode::DivisionByZero), true),
        Err("#DIV/0!")
    );
    assert_eq!(cell_value(&CellValue::Empty, true), Ok(Value::Null));
    assert_eq!(
        cell_value(&CellValue::Bool(true), true),
        Ok(Value::Bool(true))
    );
}

#[test]
fn ods_and_xls_sheets_are_read_with_their_header_and_rows() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../dbflux_spreadsheet/tests/fixtures");

    for name in ["in.ods", "in.xls"] {
        let mut rows =
            SheetRows::open(&fixtures.join(name), None).unwrap_or_else(|e| panic!("{name}: {e:?}"));

        assert_eq!(
            &rows.headers()[..5],
            ["Name", "Amount", "Date", "Double", "Ok"],
            "{name}"
        );
        let values = rows.next_rows(100).expect("rows");
        assert_eq!(values[0][0], Value::Text("item1".to_string()), "{name}");
        assert!(values.len() >= 5, "{name}: {values:?}");
    }
}
