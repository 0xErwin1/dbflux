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
        cell_value(&CellValue::Date(at(2024, 1, 31, 0, 0, 0))),
        Ok(Value::Date(
            NaiveDate::from_ymd_opt(2024, 1, 31).expect("date")
        ))
    );
    assert_eq!(
        cell_value(&CellValue::Date(at(2024, 3, 5, 13, 45, 30))),
        Ok(Value::DateTime(
            Utc.with_ymd_and_hms(2024, 3, 5, 13, 45, 30)
                .single()
                .expect("timestamp")
        ))
    );
    assert_eq!(
        cell_value(&CellValue::Date(at(1899, 12, 31, 6, 0, 0))),
        Ok(Value::Time(NaiveTime::from_hms_opt(6, 0, 0).expect("time")))
    );
    assert_eq!(
        cell_value(&CellValue::Error(CellErrorCode::DivisionByZero)),
        Err("#DIV/0!")
    );
    assert_eq!(cell_value(&CellValue::Empty), Ok(Value::Null));
    assert_eq!(cell_value(&CellValue::Bool(true)), Ok(Value::Bool(true)));
}
