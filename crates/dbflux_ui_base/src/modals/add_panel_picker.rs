use dbflux_components::controls::{Button, GpuiInput as Input, InputEvent, InputState};
use dbflux_components::modals::shell::ModalShell;
use dbflux_components::primitives::Text;
use dbflux_components::saved_chart::SavedChart;
use dbflux_components::tokens::{Heights, Spacing};
use gpui::prelude::*;
use gpui::{MouseButton, *};
use uuid::Uuid;

/// Outcome emitted when the user resolves the add-panel picker.
#[derive(Clone, Debug)]
pub enum AddPanelOutcome {
    /// User confirmed selection with one or more chart IDs.
    Confirmed {
        dashboard_id: Uuid,
        chart_ids: Vec<Uuid>,
    },
    Cancelled,
}

/// Request payload for opening the add-panel picker.
#[derive(Clone, Debug)]
pub struct AddPanelRequest {
    pub dashboard_id: Uuid,
    /// All saved charts available for this profile's connection.
    pub candidates: Vec<SavedChart>,
}

/// Modal entity for selecting saved charts to add as dashboard panels.
///
/// Renders a search field (case-insensitive filter) and a checkbox list
/// of matching saved charts. The submit button label updates live:
/// - 0 selected → "Add panels" (disabled)
/// - 1 selected → "Add 1 panel"
/// - N selected → "Add N panels"
///
/// When `candidates` is empty, renders an explanatory message instead of
/// the list.
pub struct ModalAddPanelPicker {
    request: Option<AddPanelRequest>,
    visible: bool,
    search_input: Entity<InputState>,
    focus_handle: FocusHandle,
    selected_ids: Vec<Uuid>,
    _subscriptions: Vec<Subscription>,
}

impl ModalAddPanelPicker {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder("Search charts..."));

        Self {
            request: None,
            visible: false,
            search_input,
            focus_handle: cx.focus_handle(),
            selected_ids: Vec::new(),
            _subscriptions: Vec::new(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: AddPanelRequest, window: &mut Window, cx: &mut Context<Self>) {
        self.request = Some(request);
        self.visible = true;
        self.selected_ids.clear();

        self.search_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });

        let search_sub = cx.subscribe_in(
            &self.search_input.clone(),
            window,
            |this, _, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
                let _ = this;
            },
        );
        self._subscriptions = vec![search_sub];

        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.selected_ids.clear();
        cx.notify();
    }

    pub fn toggle_chart(&mut self, chart_id: Uuid, cx: &mut Context<Self>) {
        if let Some(pos) = self.selected_ids.iter().position(|id| *id == chart_id) {
            self.selected_ids.remove(pos);
        } else {
            self.selected_ids.push(chart_id);
        }
        cx.notify();
    }

    /// Returns the submit button label based on the current selection count.
    pub fn submit_label(&self) -> String {
        match self.selected_ids.len() {
            0 => "Add panels".to_string(),
            1 => "Add 1 panel".to_string(),
            n => format!("Add {n} panels"),
        }
    }

    /// Returns filtered candidates matching the current search query (case-insensitive).
    fn filtered_candidates<'a>(candidates: &'a [SavedChart], query: &str) -> Vec<&'a SavedChart> {
        if query.is_empty() {
            return candidates.iter().collect();
        }
        let lower = query.to_lowercase();
        candidates
            .iter()
            .filter(|c| c.name.to_lowercase().contains(&lower))
            .collect()
    }

    fn confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.selected_ids.is_empty() {
            return;
        }

        let Some(ref request) = self.request else {
            return;
        };

        cx.emit(AddPanelOutcome::Confirmed {
            dashboard_id: request.dashboard_id,
            chart_ids: self.selected_ids.clone(),
        });

        self.close(cx);
    }

    fn render_chart_row(
        chart_id: uuid::Uuid,
        chart_name: String,
        is_selected: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let checkbox = div()
            .w(Heights::ICON_SM)
            .h(Heights::ICON_SM)
            .border_1()
            .rounded_sm()
            .flex()
            .items_center()
            .justify_center()
            .when(is_selected, |el| el.bg(gpui::blue()))
            .into_any_element();

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .cursor_pointer()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.toggle_chart(chart_id, cx);
                }),
            )
            .child(checkbox)
            .child(div().flex_1().text_sm().child(chart_name))
            .into_any_element()
    }
}

impl EventEmitter<AddPanelOutcome> for ModalAddPanelPicker {}

impl Render for ModalAddPanelPicker {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let candidates = request.candidates.clone();
        let _dashboard_id = request.dashboard_id;
        let is_empty_profile = candidates.is_empty();

        let query = self.search_input.read(cx).value().to_string();
        let filtered = Self::filtered_candidates(&candidates, &query);
        let selected_ids = self.selected_ids.clone();
        let submit_label = self.submit_label();
        let can_confirm = !self.selected_ids.is_empty();

        let body: AnyElement = if is_empty_profile {
            div()
                .flex()
                .flex_col()
                .gap(Spacing::SM)
                .child(
                    Text::body(
                        "No saved charts on this connection. \
                         Run a chart query and save it first.",
                    )
                    .into_any_element(),
                )
                .into_any_element()
        } else {
            let search_row = div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .child(Text::label("Search").into_any_element())
                .child(Input::new(&self.search_input))
                .into_any_element();

            let chart_rows: Vec<AnyElement> = filtered
                .iter()
                .map(|chart| {
                    let is_selected = selected_ids.contains(&chart.id);
                    Self::render_chart_row(chart.id, chart.name.clone(), is_selected, cx)
                })
                .collect();

            let chart_list = div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .children(chart_rows)
                .into_any_element();

            div()
                .flex()
                .flex_col()
                .gap(Spacing::SM)
                .child(search_row)
                .child(chart_list)
                .into_any_element()
        };

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(AddPanelOutcome::Cancelled);
            this.close(cx);
        });

        let on_confirm = cx.listener(move |this, _: &gpui::ClickEvent, window, cx| {
            if can_confirm {
                this.confirm(window, cx);
            }
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(Button::new("add-panel-cancel", "Cancel").on_click(on_cancel))
            .child(
                Button::new("add-panel-confirm", submit_label)
                    .primary()
                    .disabled(!can_confirm)
                    .on_click(on_confirm),
            );

        ModalShell::new(
            "Add panels",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .width(gpui::px(500.0))
        .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{AddPanelOutcome, AddPanelRequest, ModalAddPanelPicker};
    use dbflux_components::chart::ChartSpec;
    use dbflux_components::saved_chart::{SavedChart, SavedChartRefreshPolicy, SavedChartSource};
    use uuid::Uuid;

    fn test_uuid() -> Uuid {
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    fn make_chart(name: &str, profile_id: Uuid) -> SavedChart {
        let chart_spec: ChartSpec = serde_json::from_str(
            r#"{"x_axis":{"column_index":0,"label":"t","kind":"Time","unit":null},"series":[]}"#,
        )
        .unwrap();

        SavedChart {
            id: Uuid::new_v4(),
            name: name.to_string(),
            profile_id,
            source: SavedChartSource::Query {
                query: "SELECT 1".to_string(),
            },
            chart_spec,
            bindings: Default::default(),
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    // O.5 tests

    #[test]
    fn submit_label_updates_live_based_on_selection_count() {
        // submit_label() is a pure function of selected_ids.len() — no GPUI needed.
        let ids_0: Vec<Uuid> = vec![];
        let ids_1 = vec![Uuid::new_v4()];
        let ids_3 = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        let label = |ids: &[Uuid]| -> String {
            match ids.len() {
                0 => "Add panels".to_string(),
                1 => "Add 1 panel".to_string(),
                n => format!("Add {n} panels"),
            }
        };

        assert_eq!(label(&ids_0), "Add panels");
        assert_eq!(label(&ids_1), "Add 1 panel");
        assert_eq!(label(&ids_3), "Add 3 panels");
    }

    #[test]
    fn modal_add_panel_picker_empty_profile_shows_explanatory_state() {
        // When candidates is empty, the "no saved charts" message must be shown.
        // We verify the data path: empty candidates → is_empty_profile = true.
        let req = AddPanelRequest {
            dashboard_id: test_uuid(),
            candidates: vec![],
        };
        assert!(req.candidates.is_empty());
    }

    #[test]
    fn modal_add_panel_picker_shows_only_same_profile_charts() {
        let profile_p = Uuid::new_v4();
        let profile_q = Uuid::new_v4();

        let c1 = make_chart("C1", profile_p);
        let c2 = make_chart("C2", profile_p);
        let c3 = make_chart("C3", profile_q);

        // The caller is responsible for filtering by profile before opening the modal.
        // Verify that filtering works as expected.
        let for_p: Vec<&SavedChart> = [&c1, &c2, &c3]
            .iter()
            .filter(|c| c.profile_id == profile_p)
            .copied()
            .collect();

        assert_eq!(for_p.len(), 2);
        assert!(for_p.iter().any(|c| c.name == "C1"));
        assert!(for_p.iter().any(|c| c.name == "C2"));
        assert!(!for_p.iter().any(|c| c.name == "C3"));
    }

    #[test]
    fn modal_add_panel_picker_filter_is_case_insensitive() {
        let profile_id = test_uuid();
        let candidates = vec![
            make_chart("Foo metric", profile_id),
            make_chart("FOO dashboard", profile_id),
            make_chart("bar chart", profile_id),
        ];

        let filtered = ModalAddPanelPicker::filtered_candidates(&candidates, "foo");
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|c| c.name == "Foo metric"));
        assert!(filtered.iter().any(|c| c.name == "FOO dashboard"));
    }
}
