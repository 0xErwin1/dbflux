use crate::controls::{Dropdown, DropdownItem, InputState};
use dbflux_core::{DriverFormDef, FormFieldDef, FormFieldKind, FormValues};
use gpui::*;
use std::collections::HashMap;

#[derive(Default)]
pub struct FormRendererState {
    pub inputs: HashMap<String, Entity<InputState>>,
    pub checkboxes: HashMap<String, bool>,
    pub dropdowns: HashMap<String, Entity<Dropdown>>,
    pub dropdown_values: HashMap<String, Vec<String>>,
}

impl FormRendererState {
    pub fn clear(&mut self) {
        self.inputs.clear();
        self.checkboxes.clear();
        self.dropdowns.clear();
        self.dropdown_values.clear();
    }
}

pub fn create_inputs<T>(
    schema: &DriverFormDef,
    values: &FormValues,
    window: &mut Window,
    cx: &mut Context<T>,
) -> FormRendererState {
    let mut state = FormRendererState::default();

    for tab in &schema.tabs {
        for section in &tab.sections {
            for field in &section.fields {
                let initial_value = values
                    .get(&field.id)
                    .filter(|value| !value.is_empty())
                    .cloned()
                    .unwrap_or_else(|| field.default_value.clone());

                match &field.kind {
                    FormFieldKind::Checkbox => {
                        state
                            .checkboxes
                            .insert(field.id.clone(), initial_value == "true");
                    }
                    FormFieldKind::Select { options } => {
                        let items: Vec<DropdownItem> = options
                            .iter()
                            .map(|option| {
                                DropdownItem::with_value(option.label.clone(), option.value.clone())
                            })
                            .collect();

                        let values_by_index: Vec<String> =
                            options.iter().map(|option| option.value.clone()).collect();

                        let selected_index = values_by_index
                            .iter()
                            .position(|value| value == &initial_value)
                            .or_else(|| {
                                values_by_index
                                    .iter()
                                    .position(|value| value == &field.default_value)
                            });

                        let dropdown = cx.new(|_cx| {
                            Dropdown::new(SharedString::from(format!("form-field-{}", field.id)))
                                .items(items)
                                .selected_index(selected_index)
                        });

                        state.dropdowns.insert(field.id.clone(), dropdown);
                        state
                            .dropdown_values
                            .insert(field.id.clone(), values_by_index);
                    }
                    _ => {
                        let placeholder = field.placeholder.clone();
                        let value = initial_value;
                        let masked = field.kind == FormFieldKind::Password
                            || field.kind == FormFieldKind::WriteOnly;

                        let input = cx.new(|cx| {
                            let mut input = InputState::new(window, cx).placeholder(placeholder);
                            if masked {
                                input = input.masked(true);
                            }

                            input.set_value(value, window, cx);
                            input
                        });

                        state.inputs.insert(field.id.clone(), input);
                    }
                }
            }
        }
    }

    state
}

pub fn collect_values(
    schema: &DriverFormDef,
    inputs: &HashMap<String, Entity<InputState>>,
    checkboxes: &HashMap<String, bool>,
    dropdowns: &HashMap<String, Entity<Dropdown>>,
    cx: &App,
) -> FormValues {
    let mut values = FormValues::new();

    for tab in &schema.tabs {
        for section in &tab.sections {
            for field in &section.fields {
                match &field.kind {
                    FormFieldKind::Checkbox => {
                        let checked = checkboxes.get(&field.id).copied().unwrap_or(false);
                        values.insert(
                            field.id.clone(),
                            if checked {
                                "true".to_string()
                            } else {
                                String::new()
                            },
                        );
                    }
                    FormFieldKind::Select { .. } => {
                        let selected = dropdowns
                            .get(&field.id)
                            .and_then(|dropdown| dropdown.read(cx).selected_value())
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| field.default_value.clone());

                        values.insert(field.id.clone(), selected);
                    }
                    _ => {
                        let value = inputs
                            .get(&field.id)
                            .map(|input| input.read(cx).value().to_string())
                            .unwrap_or_else(|| field.default_value.clone());

                        values.insert(field.id.clone(), value);
                    }
                }
            }
        }
    }

    values
}

/// Returns warnings for values that don't match the expected field type
/// (e.g. non-numeric text in a Number field). Empty values are skipped
/// because the runtime falls back to defaults.
pub fn validate_values(schema: &DriverFormDef, values: &FormValues) -> Vec<String> {
    let mut warnings = Vec::new();

    for tab in &schema.tabs {
        for section in &tab.sections {
            for field in &section.fields {
                let Some(raw) = values.get(&field.id) else {
                    continue;
                };

                if raw.is_empty() {
                    continue;
                }

                match &field.kind {
                    FormFieldKind::Number if raw.parse::<f64>().is_err() => {
                        warnings.push(format!(
                            "{}: \"{}\" is not a valid number (will use default: {})",
                            field.label, raw, field.default_value
                        ));
                    }
                    FormFieldKind::Select { options }
                        if !options.iter().any(|opt| opt.value == *raw) =>
                    {
                        warnings.push(format!(
                            "{}: \"{}\" is not a recognized option (will use default: {})",
                            field.label, raw, field.default_value
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    warnings
}

pub fn is_field_enabled(
    field: &FormFieldDef,
    checkboxes: &HashMap<String, bool>,
    field_values: &HashMap<String, String>,
) -> bool {
    if let Some(checkbox_id) = &field.enabled_when_checked {
        let is_checked = checkboxes
            .get(checkbox_id.as_str())
            .copied()
            .unwrap_or(false);
        if !is_checked {
            return false;
        }
    }

    if let Some(checkbox_id) = &field.enabled_when_unchecked {
        let is_checked = checkboxes
            .get(checkbox_id.as_str())
            .copied()
            .unwrap_or(false);
        if is_checked {
            return false;
        }
    }

    if let Some(gate) = &field.enabled_when_field_equals {
        let current_value = field_values.get(gate.field.as_str()).map(String::as_str);
        let matches = current_value
            .map(|value| gate.values.iter().any(|expected| expected == value))
            .unwrap_or(false);
        if !matches {
            return false;
        }
    }

    true
}

/// Reads the current selected value of every `Select` field owned by `state`,
/// keyed by field id. Used by `is_field_enabled` callers that render through
/// the generic `FormRendererState` (real `Dropdown` entities) to resolve
/// `enabled_when_field_equals` gates against another field's live selection.
pub fn select_values(state: &FormRendererState, cx: &App) -> HashMap<String, String> {
    state
        .dropdowns
        .iter()
        .filter_map(|(field_id, dropdown)| {
            dropdown
                .read(cx)
                .selected_value()
                .map(|value| (field_id.clone(), value.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    // NOTE: `use super::*` would also pull in `gpui::test` (re-exported via the
    // file-level `use gpui::*`), shadowing the standard `#[test]` attribute.
    // gpui's test macro expects a GPUI test-context signature and overflows
    // the compiler's stack when applied to a plain sync unit-test function, so
    // this module imports only the names it needs to keep `#[test]` bound to
    // the standard library attribute.
    use super::{FormFieldDef, FormFieldKind, HashMap, is_field_enabled};
    use dbflux_core::{field, when_field_equals};

    fn text_field(id: &str) -> FormFieldDef {
        field(id, id, FormFieldKind::Text, "")
    }

    #[test]
    fn field_equals_gate_disables_field_when_referenced_value_does_not_match() {
        let gated_field = when_field_equals(
            text_field("sentinel_master_name"),
            "topology",
            &["sentinel"],
        );
        let checkboxes = HashMap::new();
        let mut values = HashMap::new();
        values.insert("topology".to_string(), "standalone".to_string());

        assert!(!is_field_enabled(&gated_field, &checkboxes, &values));
    }

    #[test]
    fn field_equals_gate_enables_field_when_referenced_value_matches() {
        let gated_field = when_field_equals(
            text_field("additional_nodes"),
            "topology",
            &["cluster", "sentinel"],
        );
        let checkboxes = HashMap::new();
        let mut values = HashMap::new();
        values.insert("topology".to_string(), "cluster".to_string());

        assert!(is_field_enabled(&gated_field, &checkboxes, &values));
    }

    #[test]
    fn field_equals_gate_disables_field_when_referenced_value_is_missing() {
        let gated_field = when_field_equals(
            text_field("sentinel_master_name"),
            "topology",
            &["sentinel"],
        );
        let checkboxes = HashMap::new();
        let values = HashMap::new();

        assert!(!is_field_enabled(&gated_field, &checkboxes, &values));
    }

    #[test]
    fn field_without_any_gate_is_always_enabled() {
        let plain_field = text_field("host");
        let checkboxes = HashMap::new();
        let values = HashMap::new();

        assert!(is_field_enabled(&plain_field, &checkboxes, &values));
    }

    #[test]
    fn checkbox_gate_and_field_equals_gate_both_must_pass() {
        let mut gated_field = when_field_equals(
            text_field("sentinel_master_name"),
            "topology",
            &["sentinel"],
        );
        gated_field.enabled_when_checked = Some("use_advanced".to_string());

        let mut checkboxes = HashMap::new();
        checkboxes.insert("use_advanced".to_string(), true);
        let mut values = HashMap::new();
        values.insert("topology".to_string(), "sentinel".to_string());

        assert!(is_field_enabled(&gated_field, &checkboxes, &values));

        checkboxes.insert("use_advanced".to_string(), false);
        assert!(!is_field_enabled(&gated_field, &checkboxes, &values));
    }
}
