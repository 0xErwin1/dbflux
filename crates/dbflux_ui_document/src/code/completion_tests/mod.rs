use gpui::{
    AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
    TestAppContext, Window, div,
};
use gpui_component::Root;
use std::cell::RefCell;
use std::rc::Rc;

const TEST_LETTER_SPACING: gpui::Pixels = gpui::px(8.); // guardrail-allow: exaggerated test typography makes under-measured completion labels observable.

#[derive(Default)]
struct MenuWidths(std::sync::Mutex<Option<gpui::Pixels>>);

impl gpui::FrameObserver for MenuWidths {
    fn accessibility_updated(&self, frame: &gpui::AccessibilityFrame) {
        *self.0.lock().expect("menu width lock") = frame
            .nodes()
            .find(|(_, node)| node.id() == "completion-menu")
            .map(|(_, node)| node.bounds().size.width);
    }
}

#[gpui::test]
fn completion_menu_keeps_table_labels_visible_while_typing(cx: &mut TestAppContext) {
    struct TableCompletions;

    impl gpui_component::input::CompletionProvider for TableCompletions {
        fn completions(
            &self,
            text: &gpui_component::input::Rope,
            _offset: usize,
            _trigger: lsp_types::CompletionContext,
            _window: &mut Window,
            _cx: &mut gpui::App,
        ) -> gpui::Task<anyhow::Result<lsp_types::CompletionResponse>> {
            let source = text.to_string();
            let prefix = source.strip_prefix("select * from ").unwrap_or_default();
            gpui::Task::ready(Ok(lsp_types::CompletionResponse::Array(
                [
                    "tbl_verification_profile",
                    "tbl_verification_data",
                    "tbl_verification_tracking",
                ]
                .into_iter()
                .map(|label| lsp_types::CompletionItem {
                    label: label.to_string(),
                    filter_text: Some(prefix.to_string()),
                    ..Default::default()
                })
                .collect(),
            )))
        }

        fn is_completion_trigger(&self, _: usize, _: &str, _: &mut gpui::App) -> bool {
            true
        }
    }

    struct SpacedEditor {
        input: Entity<gpui_component::input::EditorState>,
        expected_width: Rc<std::cell::Cell<Option<gpui::Pixels>>>,
    }

    impl Render for SpacedEditor {
        fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            use gpui_component::ActiveTheme as _;

            // Measure outside the completion menu so its width cap cannot mask clipping.
            let expected_width = div()
                .font_family(cx.theme().mono_font_family.clone())
                .text_xs()
                .letter_spacing(TEST_LETTER_SPACING)
                .p_1()
                .child(div().p_1().child("tbl_verification_tracking"))
                .into_any_element()
                .layout_as_root(
                    gpui::size(
                        gpui::AvailableSpace::MaxContent,
                        gpui::AvailableSpace::MaxContent,
                    ),
                    window,
                    cx,
                )
                .width;
            self.expected_width.set(Some(expected_width));

            gpui_component::input::Editor::new(&self.input)
                .letter_spacing(TEST_LETTER_SPACING)
                .h(gpui::px(400.))
        }
    }

    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    let menu_width = std::sync::Arc::new(MenuWidths::default());
    let expected_width = Rc::new(std::cell::Cell::new(None));
    let input_slot = Rc::new(RefCell::new(None));
    let (_, window) = cx.add_window_view(|window, cx| {
        window.observe_frames(&menu_width);
        let input = cx.new(|cx| {
            gpui_component::input::EditorState::new(window, cx).default_value("select * from ")
        });
        input.update(cx, |state, cx| {
            state.lsp_mut().completion_menu.max_width = gpui::px(640.);
            state.lsp_mut().completion_provider = Some(Rc::new(TableCompletions));
            state.set_selected_range(14..14, cx);
            state.focus(window, cx);
        });
        input_slot.replace(Some(input.clone()));
        let view = cx.new(|_| SpacedEditor {
            input,
            expected_width: expected_width.clone(),
        });
        Root::new(view, window, cx)
    });
    let input = input_slot.borrow().clone().expect("editor input");
    window.run_until_parked();
    window.update(|window, cx| input.update(cx, |state, cx| state.show_completions(window, cx)));
    let mut widths = Vec::new();

    for text in ["", "t", "bl", "_verification_"] {
        if !text.is_empty() {
            window.simulate_input(text);
        }
        window.run_until_parked();
        widths.push(
            menu_width
                .0
                .lock()
                .expect("menu width lock")
                .expect("completion menu is rendered"),
        );
    }

    let expected_width = expected_width.get().expect("reference label is measured");
    assert!(
        expected_width > gpui::px(320.),
        "the fixture must expose clipping at the old width cap: {expected_width:?}"
    );
    assert!(
        widths[0] >= expected_width - gpui::px(1.),
        "the longest label and menu padding must fit: expected {expected_width:?}, got {widths:?}"
    );
    for width in &widths {
        assert_eq!(
            *width, widths[0],
            "highlighting a prefix must not shrink unchanged labels: {widths:?}"
        );
    }
}
