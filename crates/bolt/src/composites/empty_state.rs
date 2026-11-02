//! `EmptyState` — what a region shows when it has nothing to show: an icon,
//! one sentence, and the two or three keys that get the user started.

use gpui::prelude::*;
use gpui::{AnyElement, App, ElementId, FontWeight, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::controls::Button;
use crate::icon::IconSource;
use crate::primitives::{Chamfer, Icon, Kbd};
use crate::tokens::{ChamferCut, ChromeColors, NavigationMetrics};

/// One way out of an empty state: an icon, a label and the keys bound to it.
pub struct EmptyStateAction {
    icon: IconSource,
    label: SharedString,
    keys: Vec<SharedString>,
}

impl EmptyStateAction {
    pub fn new(
        icon: impl Into<IconSource>,
        label: impl Into<SharedString>,
        keys: impl IntoIterator<Item = impl Into<SharedString>>,
    ) -> Self {
        Self {
            icon: icon.into(),
            label: label.into(),
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

/// Tone of the icon and sentence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum EmptyStateTone {
    #[default]
    Default,
    Danger,
}

/// An empty region's placeholder, centered in its parent.
///
/// By default it renders inline: tint icon, optional bold title, a muted
/// sentence and the actions. [`EmptyState::card`] wraps the same content in
/// a 460 px cut-14 card, for large empty regions
/// such as the workspace with no tab open. [`EmptyState::danger`] turns the
/// icon and sentence red for a failed load.
///
/// [`EmptyState::primary_action`] adds a real button under the sentence, for
/// the one thing the region asks for (Create, Retry); the key-hint actions
/// stay below it.
#[derive(IntoElement)]
pub struct EmptyState {
    icon: IconSource,
    title: Option<SharedString>,
    message: SharedString,
    actions: Vec<EmptyStateAction>,
    primary_action: Option<Button>,
    card: bool,
    tone: EmptyStateTone,
    id: Option<ElementId>,
}

impl EmptyState {
    pub fn new(icon: impl Into<IconSource>, message: impl Into<SharedString>) -> Self {
        Self {
            icon: icon.into(),
            title: None,
            message: message.into(),
            actions: Vec::new(),
            primary_action: None,
            card: false,
            tone: EmptyStateTone::Default,
            id: None,
        }
    }

    /// Sets the element id (and debug selector) of the placeholder, so tests
    /// and automation find it by name.
    pub fn id(mut self, id: impl Into<ElementId>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// A button under the sentence, drawn as the caller built it (its id,
    /// variant and click handler are the caller's).
    pub fn primary_action(mut self, button: Button) -> Self {
        self.primary_action = Some(button);
        self
    }

    pub fn title(mut self, title: impl Into<SharedString>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn action(mut self, action: EmptyStateAction) -> Self {
        self.actions.push(action);
        self
    }

    pub fn actions(mut self, actions: impl IntoIterator<Item = EmptyStateAction>) -> Self {
        self.actions.extend(actions);
        self
    }

    pub fn card(mut self) -> Self {
        self.card = true;
        self
    }

    pub fn danger(mut self) -> Self {
        self.tone = EmptyStateTone::Danger;
        self
    }

    /// `part` under `<id>-<suffix>`, named by `text`, when the state has an
    /// id; unchanged otherwise.
    fn named_part(
        part: gpui::Div,
        id: Option<&ElementId>,
        suffix: &str,
        text: SharedString,
    ) -> AnyElement {
        let id = id.map(|id| ElementId::from(SharedString::from(format!("{id}-{suffix}"))));
        crate::selector::with_optional_label(part, id, text)
    }

    /// One action row, the `index`th of the state; its label is
    /// `<id>-action-<index>`. Its keycaps are numbered from `first_key`
    /// across the whole state, so `<id>-key-<n>` stays unique.
    fn render_action(
        action: EmptyStateAction,
        muted: Hsla,
        id: Option<&ElementId>,
        index: usize,
        first_key: usize,
    ) -> gpui::Div {
        let keys = action
            .keys
            .into_iter()
            .enumerate()
            .map(move |(index, key)| {
                let keycap = Kbd::new(key);
                match id {
                    Some(id) => keycap.id(SharedString::from(format!(
                        "{id}-key-{}",
                        first_key + index
                    ))),
                    None => keycap,
                }
            })
            .collect::<Vec<_>>();

        div()
            .flex()
            .items_center()
            .gap(NavigationMetrics::EMPTY_ACTION_INNER_GAP)
            .text_size(NavigationMetrics::EMPTY_BODY_FONT)
            .child(
                Icon::new(action.icon)
                    .size(NavigationMetrics::EMPTY_ACTION_ICON)
                    .color(muted),
            )
            .child(Self::named_part(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .child(action.label.clone()),
                id,
                &format!("action-{index}"),
                action.label,
            ))
            .children(keys)
    }
}

impl RenderOnce for EmptyState {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;

        let (icon_color, message_color) = match self.tone {
            EmptyStateTone::Default => (ChromeColors::tint(theme), muted),
            EmptyStateTone::Danger => (theme.danger, theme.danger),
        };

        let id = self.id;
        let has_actions = !self.actions.is_empty();
        let mut key_count = 0;
        let actions: Vec<gpui::Div> = self
            .actions
            .into_iter()
            .enumerate()
            .map(|(index, action)| {
                let first_key = key_count;
                key_count += action.keys.len();
                Self::render_action(action, muted, id.as_ref(), index, first_key)
            })
            .collect();

        let content = div()
            .relative()
            .flex()
            .flex_col()
            .items_center()
            .gap(NavigationMetrics::EMPTY_GAP)
            .max_w(NavigationMetrics::EMPTY_WIDTH)
            .text_center()
            .when(self.card, |content| {
                content
                    .w(NavigationMetrics::EMPTY_WIDTH)
                    .p(NavigationMetrics::EMPTY_PADDING)
                    .child(
                        Chamfer::new(ChamferCut::CARD)
                            .fill(theme.popover)
                            .border(theme.border),
                    )
            })
            .child(
                Icon::new(self.icon)
                    .size(NavigationMetrics::EMPTY_ICON)
                    .color(icon_color),
            )
            .when_some(self.title, |content, title| {
                content.child(Self::named_part(
                    div()
                        .text_size(NavigationMetrics::EMPTY_TITLE_FONT)
                        .font_weight(FontWeight::BOLD)
                        .text_color(ChromeColors::strong(theme))
                        .child(title.clone()),
                    id.as_ref(),
                    "title",
                    title,
                ))
            })
            .child(Self::named_part(
                div()
                    .text_size(NavigationMetrics::EMPTY_BODY_FONT)
                    .text_color(message_color)
                    .child(self.message.clone()),
                id.as_ref(),
                "text",
                self.message,
            ))
            .when_some(self.primary_action, |content, button| content.child(button))
            .when(has_actions, |content| {
                content.child(
                    div()
                        .flex()
                        .flex_col()
                        .w_full()
                        .gap(NavigationMetrics::EMPTY_ACTION_GAP)
                        .text_color(theme.foreground)
                        .children(actions),
                )
            });

        let placeholder = div()
            .flex()
            .flex_1()
            .size_full()
            .items_center()
            .justify_center()
            .child(content);

        crate::selector::with_optional_id(placeholder, id)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use gpui::{
        Context, IntoElement, Modifiers, ParentElement as _, Render, Styled as _, TestAppContext,
        VisualTestContext, Window, div,
    };

    use super::{EmptyState, EmptyStateAction};
    use crate::controls::Button;
    use crate::icon::IconSource;

    struct Host {
        clicks: Rc<Cell<usize>>,
        with_primary: bool,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let folder = IconSource::Svg("icons/folder.svg".into());
            let clicks = self.clicks.clone();
            let state = EmptyState::new(folder.clone(), "No requests yet")
                .id("empty")
                .action(EmptyStateAction::new(folder, "New request", ["Ctrl N"]));

            let state = if self.with_primary {
                state.primary_action(Button::new("empty-create", "Create").on_click(
                    move |_, _, _| {
                        clicks.set(clicks.get() + 1);
                    },
                ))
            } else {
                state
            };

            div().size_full().child(state)
        }
    }

    fn open(
        with_primary: bool,
        cx: &mut TestAppContext,
    ) -> (Rc<Cell<usize>>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            crate::theme::install(
                crate::brand::Brand::neutral(),
                crate::palette::PaletteMode::Dark,
                crate::density::Density::Default,
                cx,
            );
        });

        let clicks = Rc::new(Cell::new(0));
        let (_view, window) = cx.add_window_view({
            let clicks = clicks.clone();
            move |_, _| Host {
                clicks,
                with_primary,
            }
        });
        window.run_until_parked();
        (clicks, window)
    }

    #[gpui::test]
    fn the_primary_action_is_a_button_that_runs_its_handler(cx: &mut TestAppContext) {
        let (clicks, window) = open(true, cx);

        let state = window.debug_bounds("empty").expect("the state is laid out");
        let button = window
            .debug_bounds("empty-create")
            .expect("the primary action is drawn");
        assert!(state.contains(&button.center()));

        window.simulate_click(button.center(), Modifiers::default());
        window.run_until_parked();
        assert_eq!(clicks.get(), 1);
    }

    fn titled_state(_cx: &mut gpui::App) -> gpui::AnyElement {
        let folder = IconSource::Svg("icons/folder.svg".into());

        EmptyState::new(folder.clone(), "No requests yet")
            .id("empty")
            .title("Nothing here")
            .action(EmptyStateAction::new(
                folder.clone(),
                "New request",
                ["Ctrl n"],
            ))
            .action(EmptyStateAction::new(folder, "Import", ["Ctrl i"]))
            .into_any_element()
    }

    #[gpui::test]
    fn the_title_text_and_keycaps_of_a_named_state_carry_their_text(cx: &mut TestAppContext) {
        use crate::accessibility::{accessible_name, render_frame};

        let frame = render_frame(cx, titled_state);

        assert_eq!(
            accessible_name(&frame, "empty-title").as_deref(),
            Some("Nothing here")
        );
        assert_eq!(
            accessible_name(&frame, "empty-text").as_deref(),
            Some("No requests yet")
        );
        assert_eq!(
            accessible_name(&frame, "empty-key-0").as_deref(),
            Some("Ctrl N")
        );
        assert_eq!(
            accessible_name(&frame, "empty-key-1").as_deref(),
            Some("Ctrl I")
        );
        assert_eq!(
            accessible_name(&frame, "empty-action-0").as_deref(),
            Some("New request")
        );
        assert_eq!(
            accessible_name(&frame, "empty-action-1").as_deref(),
            Some("Import")
        );
    }

    #[gpui::test]
    fn an_action_label_is_found_by_its_selector(cx: &mut TestAppContext) {
        let (_clicks, window) = open(false, cx);

        let state = window.debug_bounds("empty").expect("the state is laid out");
        let label = window
            .debug_bounds("empty-action-0")
            .expect("the action label is found under the state id");
        assert!(state.contains(&label.center()));
    }

    #[gpui::test]
    fn without_a_primary_action_the_state_draws_no_button(cx: &mut TestAppContext) {
        let (_clicks, window) = open(false, cx);

        assert!(window.debug_bounds("empty").is_some());
        assert!(window.debug_bounds("empty-create").is_none());
    }
}
