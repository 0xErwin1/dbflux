use gpui::{
    AppContext as _, Context, ElementId, Entity, IntoElement, ParentElement as _, Render,
    Styled as _, TestAppContext, VisualTestContext, Window, div,
};

use super::id_selector;
use crate::composites::{
    ListRow, MenuItem, SplitButton, document_tab, inline_tab, menu_row, render_menu_overlay,
    result_tab,
};
use crate::controls::{Button, Checkbox, Dropdown, DropdownItem, Input, InputState};
use crate::icon::IconSource;
use crate::modals::Modal;
use crate::primitives::{FilePicker, SegmentedControl, SegmentedItem};

struct Components {
    dropdown: Entity<Dropdown>,
    input: Entity<InputState>,
}

impl Render for Components {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let folder = IconSource::Svg("icons/folder.svg".into());
        let clear = IconSource::Svg("icons/close.svg".into());
        let item = MenuItem::new("Open");

        div()
            .size_full()
            .flex()
            .flex_col()
            .child(Button::new("sel-button", "Save"))
            .child(Checkbox::new("sel-checkbox").label("Wrap"))
            .child(self.dropdown.clone())
            .child(Input::new(&self.input).id("sel-input"))
            .child(FilePicker::new("sel-file", "", "Choose", folder, clear))
            .child(document_tab("sel-document-tab", true, cx))
            .child(result_tab("sel-result-tab", "Result", None, false, cx))
            .child(inline_tab("sel-inline-tab", false, cx))
            .child(menu_row("sel-menu-row", &item, false, cx))
            .child(ListRow::new("sel-list-row").build(cx).child("Row"))
            .child(SplitButton::new(
                "sel-split",
                Button::new("sel-split-main", "Run"),
                div().child("v"),
            ))
            .child(
                SegmentedControl::new(
                    vec![
                        SegmentedItem::new("form", "Form"),
                        SegmentedItem::new("raw", "Raw"),
                    ],
                    "form",
                    |_, _, _| {},
                )
                .group("sel"),
            )
            .child(render_menu_overlay("sel-overlay", |_, _| {}))
            .child(Modal::new("Title").id("sel-modal").on_close(|_, _| {}))
    }
}

fn open_components(cx: &mut TestAppContext) -> &mut VisualTestContext {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });

    let (_view, window) = cx.add_window_view(|window, cx| Components {
        dropdown: cx.new(|_| Dropdown::new("sel-dropdown").items(vec![DropdownItem::new("One")])),
        input: cx.new(|cx| InputState::new(window, cx)),
    });
    window.run_until_parked();
    window
}

#[gpui::test]
fn every_id_bearing_component_is_found_by_its_id(cx: &mut TestAppContext) {
    let window = open_components(cx);

    let ids = [
        "sel-button",
        "sel-checkbox",
        "sel-dropdown",
        "sel-input",
        "sel-file",
        "sel-document-tab",
        "sel-result-tab",
        "sel-inline-tab",
        "sel-menu-row",
        "sel-list-row",
        "sel-split",
        "sel-split-main",
        "segmented-sel",
        "segmented-sel-form",
        "segmented-sel-raw",
        "sel-overlay",
        "sel-modal",
        "modal-close",
    ];
    let missing: Vec<&str> = ids
        .into_iter()
        .filter(|id| window.debug_bounds(id).is_none())
        .collect();

    assert!(missing.is_empty(), "no debug selector for {missing:?}");
}

#[test]
fn the_selector_is_the_display_form_of_the_id() {
    assert_eq!(id_selector(&ElementId::from("run-button"))(), "run-button");
    assert_eq!(
        id_selector(&ElementId::NamedInteger("row".into(), 3))(),
        "row-3"
    );
}
