use crate::app::AppState;
use crate::ui::editor::EditorPane;
use crate::ui::results::ResultsPane;
use crate::ui::sidebar::Sidebar;
use gpui::*;
use gpui_component::resizable::{h_resizable, resizable_panel, v_resizable};
use gpui_component::ActiveTheme;

pub struct Workspace {
    #[allow(dead_code)]
    app_state: Entity<AppState>,
    sidebar: Entity<Sidebar>,
    editor: Entity<EditorPane>,
    results: Entity<ResultsPane>,
}

impl Workspace {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let results = cx.new(|cx| ResultsPane::new(app_state.clone(), window, cx));
        let editor = cx.new(|cx| EditorPane::new(app_state.clone(), results.clone(), window, cx));
        let sidebar =
            cx.new(|cx| Sidebar::new(app_state.clone(), editor.clone(), results.clone(), cx));

        Self {
            app_state,
            sidebar,
            editor,
            results,
        }
    }
}

impl Render for Workspace {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let sidebar = self.sidebar.clone();
        let editor = self.editor.clone();
        let results = self.results.clone();

        div().flex().size_full().bg(cx.theme().background).child(
            h_resizable("workspace")
                .child(
                    resizable_panel()
                        .size(px(240.0))
                        .size_range(px(150.0)..px(500.0))
                        .child(sidebar),
                )
                .child(
                    resizable_panel().child(
                        div().flex().flex_col().flex_1().size_full().child(
                            v_resizable("editor-results")
                                .child(
                                    resizable_panel()
                                        .size(px(300.0))
                                        .size_range(px(100.0)..px(800.0))
                                        .child(editor),
                                )
                                .child(resizable_panel().child(results)),
                        ),
                    ),
                ),
        )
    }
}
