//! Lists, structs and maps shown as JSON text, cut at
//! [`NESTED_DISPLAY_CHARS`] characters.
//!
//! The walk follows the Arrow arrays; the Parquet schema rides along only to
//! recover what the Arrow types lose (UUID, INT96, unsupported logical
//! types), and every leaf goes through the scalar rules of [`crate::cells`].

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use parquet::basic::Repetition;
use parquet::schema::types::Type;

use crate::ParquetError;
use crate::cells::{Cell, CellKind, NESTED_DISPLAY_CHARS, leaf_hints, scalar_cell, type_name};

pub(crate) fn is_nested(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::Struct(_)
            | DataType::Map(..)
    )
}

/// The cell of a non-null nested value: its JSON, with dates, decimals,
/// binary values and the like as the strings the scalar rules display, cut
/// after [`NESTED_DISPLAY_CHARS`] characters and marked with an ellipsis. A
/// map whose keys are strings is an object; any other map is an array of
/// `[key, value]` pairs, so keys keep their type.
pub(crate) fn nested_cell(
    array: &dyn Array,
    row: usize,
    parquet_type: Option<&Type>,
) -> Result<Cell, ParquetError> {
    let mut json = BoundedText::new(NESTED_DISPLAY_CHARS);

    write_value(&mut json, array, row, parquet_type)?;

    Ok(Cell {
        kind: CellKind::Nested,
        display: json.finish().into(),
    })
}

/// The type of a nested column as `LIST<T>`, `STRUCT<name: T, ...>` or
/// `MAP<K, V>`, each leaf named as a top-level column of its type would be.
pub(crate) fn nested_type_name(parquet_type: Option<&Type>, data_type: &DataType) -> String {
    if let Some(name) = parquet_type.and_then(|node| leaf_hints(node).unsupported) {
        return format!("{name} (unsupported)");
    }

    match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => format!(
            "LIST<{}>",
            nested_type_name(parquet_type.and_then(list_element), element.data_type())
        ),

        DataType::Struct(fields) => {
            let children: Vec<String> = fields
                .iter()
                .map(|field| {
                    let child = parquet_type.and_then(|node| struct_child(node, field.name()));

                    format!(
                        "{}: {}",
                        field.name(),
                        nested_type_name(child, field.data_type())
                    )
                })
                .collect();

            format!("STRUCT<{}>", children.join(", "))
        }

        DataType::Map(entries, _) => {
            let (key_node, value_node) = map_entries(parquet_type);

            match entries.data_type() {
                DataType::Struct(fields) => match (fields.first(), fields.get(1)) {
                    (Some(key), Some(value)) => format!(
                        "MAP<{}, {}>",
                        nested_type_name(key_node, key.data_type()),
                        nested_type_name(value_node, value.data_type())
                    ),
                    _ => "MAP".to_string(),
                },
                _ => "MAP".to_string(),
            }
        }

        _ => match parquet_type {
            Some(node @ Type::PrimitiveType { .. }) => type_name(node, data_type),
            _ => data_type.to_string(),
        },
    }
}

fn write_value(
    json: &mut BoundedText,
    array: &dyn Array,
    row: usize,
    parquet_type: Option<&Type>,
) -> Result<(), ParquetError> {
    if json.is_full() {
        return Ok(());
    }

    if array.is_null(row) || *array.data_type() == DataType::Null {
        json.push_str("null");
        return Ok(());
    }

    let hints = parquet_type.map(leaf_hints).unwrap_or_default();

    if let Some(name) = hints.unsupported {
        json.push_string(&format!("<unsupported: {name}>"));
        return Ok(());
    }

    let data_type = array.data_type();

    match data_type {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => {
            let elements = list_elements(array, row)?;
            let element = parquet_type.and_then(list_element);

            write_elements(json, elements.as_ref(), element)
        }

        DataType::Struct(_) => {
            let structure = array.as_struct_opt().ok_or_else(|| mismatch(data_type))?;

            json.push('{');

            for (index, (field, column)) in structure
                .fields()
                .iter()
                .zip(structure.columns())
                .enumerate()
            {
                if json.is_full() {
                    break;
                }

                if index > 0 {
                    json.push(',');
                }

                let child = parquet_type.and_then(|node| struct_child(node, field.name()));

                json.push_string(field.name());
                json.push(':');
                write_value(json, column.as_ref(), row, child)?;
            }

            json.push('}');

            Ok(())
        }

        DataType::Map(..) => {
            let map = array.as_map_opt().ok_or_else(|| mismatch(data_type))?;
            let entries = map.value(row);

            let (Some(keys), Some(values)) = (entries.columns().first(), entries.columns().get(1))
            else {
                return Err(ParquetError::malformed(
                    "a map entry does not hold a key and a value",
                ));
            };

            let (key_node, value_node) = map_entries(parquet_type);

            write_map(json, keys.as_ref(), values.as_ref(), key_node, value_node)
        }

        _ => {
            let cell = scalar_cell(array, row, &hints)?;
            write_scalar(json, &cell, data_type);

            Ok(())
        }
    }
}

/// The elements of the list at `row` of `array`, a list of any offset width
/// or a fixed-size list.
fn list_elements(array: &dyn Array, row: usize) -> Result<ArrayRef, ParquetError> {
    let data_type = array.data_type();

    match data_type {
        DataType::List(_) => array.as_list_opt::<i32>().map(|list| list.value(row)),
        DataType::LargeList(_) => array.as_list_opt::<i64>().map(|list| list.value(row)),
        DataType::FixedSizeList(..) => array.as_fixed_size_list_opt().map(|list| list.value(row)),
        _ => None,
    }
    .ok_or_else(|| mismatch(data_type))
}

fn write_elements(
    json: &mut BoundedText,
    elements: &dyn Array,
    element_type: Option<&Type>,
) -> Result<(), ParquetError> {
    json.push('[');

    for index in 0..elements.len() {
        if json.is_full() {
            break;
        }

        if index > 0 {
            json.push(',');
        }

        write_value(json, elements, index, element_type)?;
    }

    json.push(']');

    Ok(())
}

fn write_map(
    json: &mut BoundedText,
    keys: &dyn Array,
    values: &dyn Array,
    key_type: Option<&Type>,
    value_type: Option<&Type>,
) -> Result<(), ParquetError> {
    let string_keys = has_string_values(keys.data_type());
    let key_hints = key_type.map(leaf_hints).unwrap_or_default();

    json.push(if string_keys { '{' } else { '[' });

    for index in 0..keys.len() {
        if json.is_full() {
            break;
        }

        if index > 0 {
            json.push(',');
        }

        if string_keys {
            let key = scalar_cell(keys, index, &key_hints)?;

            json.push_string(&key.display);
            json.push(':');
            write_value(json, values, index, value_type)?;
        } else {
            json.push('[');
            write_value(json, keys, index, key_type)?;
            json.push(',');
            write_value(json, values, index, value_type)?;
            json.push(']');
        }
    }

    json.push(if string_keys { '}' } else { ']' });

    Ok(())
}

/// Numbers and booleans stay JSON literals, a UInt64 above `i64::MAX`
/// included since its digits are exact; everything else is the scalar
/// display as a string, so a decimal never turns into a rounded number.
fn write_scalar(json: &mut BoundedText, cell: &Cell, data_type: &DataType) {
    match &cell.kind {
        CellKind::Null => json.push_str("null"),
        CellKind::Bool(_) | CellKind::Integer(_) | CellKind::Nested => json.push_str(&cell.display),
        CellKind::Float(value) if value.is_finite() => json.push_str(&cell.display),
        CellKind::Text if *data_type == DataType::UInt64 => json.push_str(&cell.display),
        CellKind::Float(_) | CellKind::Text | CellKind::Binary { .. } => {
            json.push_string(&cell.display)
        }
    }
}

fn has_string_values(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, value_type) => has_string_values(value_type),
        _ => false,
    }
}

fn mismatch(data_type: &DataType) -> ParquetError {
    ParquetError::malformed(format!(
        "a decoded column does not hold the {data_type} values its schema declares"
    ))
}

/// The element type of a Parquet list, following the backward-compatibility
/// rules of the format: a repeated field outside a LIST group is its own
/// element, and inside one the repeated field is the element unless it is a
/// one-field group not named `array` or `<list>_tuple`, whose field then is.
fn list_element(list_type: &Type) -> Option<&Type> {
    let basic_info = list_type.get_basic_info();

    if basic_info.has_repetition() && basic_info.repetition() == Repetition::REPEATED {
        return Some(list_type);
    }

    let Type::GroupType { fields, .. } = list_type else {
        return None;
    };

    let [repeated] = fields.as_slice() else {
        return None;
    };

    match repeated.as_ref() {
        Type::PrimitiveType { .. } => Some(repeated.as_ref()),

        Type::GroupType {
            fields: repeated_fields,
            ..
        } => {
            let name = repeated.name();
            let is_element = repeated_fields.len() != 1
                || name == "array"
                || name == format!("{}_tuple", list_type.name());

            if is_element {
                Some(repeated.as_ref())
            } else {
                repeated_fields.first().map(|field| field.as_ref())
            }
        }
    }
}

fn struct_child<'a>(struct_type: &'a Type, name: &str) -> Option<&'a Type> {
    let Type::GroupType { fields, .. } = struct_type else {
        return None;
    };

    fields
        .iter()
        .find(|field| field.name() == name)
        .map(|field| field.as_ref())
}

/// The key and value types of a Parquet map: its one repeated key-value
/// group holds them as its first and second fields.
fn map_entries(map_type: Option<&Type>) -> (Option<&Type>, Option<&Type>) {
    let Some(Type::GroupType { fields, .. }) = map_type else {
        return (None, None);
    };

    let Some(Type::GroupType {
        fields: entry_fields,
        ..
    }) = fields.first().map(|field| field.as_ref())
    else {
        return (None, None);
    };

    (
        entry_fields.first().map(|field| field.as_ref()),
        entry_fields.get(1).map(|field| field.as_ref()),
    )
}

/// A string that stops growing after `limit` characters. Writers keep
/// calling it once it is full; it drops the rest, so they only check
/// [`Self::is_full`] to stop walking early.
struct BoundedText {
    text: String,
    remaining: usize,
    truncated: bool,
}

impl BoundedText {
    fn new(limit: usize) -> Self {
        Self {
            text: String::new(),
            remaining: limit,
            truncated: false,
        }
    }

    fn is_full(&self) -> bool {
        self.truncated
    }

    fn push(&mut self, character: char) {
        if self.truncated {
            return;
        }

        if self.remaining == 0 {
            self.truncated = true;
            return;
        }

        self.text.push(character);
        self.remaining -= 1;
    }

    fn push_str(&mut self, text: &str) {
        for character in text.chars() {
            if self.truncated {
                return;
            }

            self.push(character);
        }
    }

    /// `text` as a JSON string literal.
    fn push_string(&mut self, text: &str) {
        self.push('"');

        for character in text.chars() {
            if self.truncated {
                return;
            }

            match character {
                '"' => self.push_str("\\\""),
                '\\' => self.push_str("\\\\"),
                '\n' => self.push_str("\\n"),
                '\r' => self.push_str("\\r"),
                '\t' => self.push_str("\\t"),
                control if u32::from(control) < 0x20 => {
                    self.push_str(&format!("\\u{:04x}", u32::from(control)))
                }
                other => self.push(other),
            }
        }

        self.push('"');
    }

    fn finish(mut self) -> String {
        if self.truncated {
            self.text.push('…');
        }

        self.text
    }
}
