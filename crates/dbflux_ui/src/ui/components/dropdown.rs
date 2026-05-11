pub use dbflux_components::controls::{
    Dropdown, DropdownDismissed, DropdownItem, DropdownSelectionChanged,
};

#[cfg(test)]
mod tests {
    use super::{Dropdown, DropdownDismissed, DropdownItem, DropdownSelectionChanged};

    fn accepts_shared_dropdown(_: dbflux_components::controls::Dropdown) {}
    fn accepts_shared_item(_: dbflux_components::controls::DropdownItem) {}
    fn accepts_shared_selection_changed(_: dbflux_components::controls::DropdownSelectionChanged) {}
    fn accepts_shared_dismissed(_: dbflux_components::controls::DropdownDismissed) {}

    #[test]
    fn legacy_dropdown_adapter_is_the_shared_dropdown_type() {
        let dropdown = Dropdown::new("legacy-dropdown");

        accepts_shared_dropdown(dropdown);
    }

    #[test]
    fn legacy_dropdown_symbols_match_shared_events_and_items() {
        let item = DropdownItem::with_value("Label", "value");

        accepts_shared_item(item.clone());
        accepts_shared_selection_changed(DropdownSelectionChanged { index: 0, item });
        accepts_shared_dismissed(DropdownDismissed);
    }

    #[test]
    fn legacy_dropdown_preserves_selected_label_and_value_behavior() {
        let dropdown = Dropdown::new("legacy-dropdown")
            .items(vec![
                DropdownItem::with_value("Primary", "primary-value"),
                DropdownItem::with_value("Secondary", "secondary-value"),
            ])
            .selected_index(Some(1));

        assert_eq!(
            dropdown.selected_label().map(|label| label.to_string()),
            Some("Secondary".to_string())
        );
        assert_eq!(
            dropdown.selected_value().map(|value| value.to_string()),
            Some("secondary-value".to_string())
        );
    }

    #[test]
    fn legacy_dropdown_clears_observable_selection_for_out_of_range_index() {
        let dropdown = Dropdown::new("legacy-dropdown")
            .items(vec![DropdownItem::with_value("Primary", "primary-value")])
            .selected_index(Some(9));

        assert_eq!(
            dropdown.selected_label().map(|label| label.to_string()),
            None
        );
        assert_eq!(
            dropdown.selected_value().map(|value| value.to_string()),
            None
        );
    }

    #[test]
    fn legacy_dropdown_preserves_shared_item_default_value_behavior() {
        let dropdown = Dropdown::new("legacy-dropdown")
            .items(vec![DropdownItem::new("Same Label")])
            .selected_index(Some(0));

        assert_eq!(
            dropdown.selected_label().map(|label| label.to_string()),
            Some("Same Label".to_string())
        );
        assert_eq!(
            dropdown.selected_value().map(|value| value.to_string()),
            Some("Same Label".to_string())
        );
    }
}
