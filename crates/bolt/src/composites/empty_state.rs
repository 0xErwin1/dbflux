//! `EmptyState` — what a region shows when it has nothing to show: an icon,
//! one sentence, and the two or three keys that get the user started.

use gpui::prelude::*;
use gpui::{App, ElementId, FontWeight, Hsla, SharedString, Window, div};
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

    fn render_action(action: EmptyStateAction, muted: Hsla) -> gpui::Div {
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
            .child(div().flex_1().min_w_0().truncate().child(action.label))
            .children(action.keys.into_iter().map(Kbd::new))
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

        let has_actions = !self.actions.is_empty();
        let actions: Vec<gpui::Div> = self
            .actions
            .into_iter()
            .map(|action| Self::render_action(action, muted))
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
                content.child(
                    div()
                        .text_size(NavigationMetrics::EMPTY_TITLE_FONT)
                        .font_weight(FontWeight::BOLD)
                        .text_color(ChromeColors::strong(theme))
                        .child(title),
                )
            })
            .child(
                div()
                    .text_size(NavigationMetrics::EMPTY_BODY_FONT)
                    .text_color(message_color)
                    .child(self.message),
            )
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

        crate::selector::with_optional_id(placeholder, self.id)
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

    #[gpui::test]
    fn without_a_primary_action_the_state_draws_no_button(cx: &mut TestAppContext) {
        let (_clicks, window) = open(false, cx);

        assert!(window.debug_bounds("empty").is_some());
        assert!(window.debug_bounds("empty-create").is_none());
    }
}
