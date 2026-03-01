use crate::ui::dropdown::{Dropdown, DropdownItem};
use dbflux_core::{DriverFormDef, FormFieldDef, FormFieldKind, FormValues};
use gpui::*;
use gpui_component::input::InputState;
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
                        let masked = field.kind == FormFieldKind::Password;

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
                    FormFieldKind::Number => {
                        if raw.parse::<f64>().is_err() {
                            warnings.push(format!(
                                "{}: \"{}\" is not a valid number (will use default: {})",
                                field.label, raw, field.default_value
                            ));
                        }
                    }
                    FormFieldKind::Select { options } => {
                        if !options.iter().any(|opt| opt.value == *raw) {
                            warnings.push(format!(
                                "{}: \"{}\" is not a recognized option (will use default: {})",
                                field.label, raw, field.default_value
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    warnings
}

pub fn is_field_enabled(field: &FormFieldDef, checkboxes: &HashMap<String, bool>) -> bool {
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

    true
}
