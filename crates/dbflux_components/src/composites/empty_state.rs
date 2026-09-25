//! `EmptyState` — what a region shows when it has nothing to show: an icon,
//! one sentence, and the two or three keys that get the user started
//! (DSAppPlan "Empty state", P1Empty).

use gpui::prelude::*;
use gpui::{App, FontWeight, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;

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
/// the 460 px cut-14 card of the DSAppPlan board, for large empty regions
/// such as the workspace with no tab open. [`EmptyState::danger`] turns the
/// icon and sentence red for a failed load.
#[derive(IntoElement)]
pub struct EmptyState {
    icon: IconSource,
    title: Option<SharedString>,
    message: SharedString,
    actions: Vec<EmptyStateAction>,
    card: bool,
    tone: EmptyStateTone,
}

impl EmptyState {
    pub fn new(icon: impl Into<IconSource>, message: impl Into<SharedString>) -> Self {
        Self {
            icon: icon.into(),
            title: None,
            message: message.into(),
            actions: Vec::new(),
            card: false,
            tone: EmptyStateTone::Default,
        }
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

        div()
            .flex()
            .flex_1()
            .size_full()
            .items_center()
            .justify_center()
            .child(content)
    }
}
