use gpui::prelude::*;
use gpui::{App, ClickEvent, ElementId, FontWeight, SharedString, Window};
use gpui_component::button::{
    Button as GpuiButton, ButtonVariant as GpuiButtonVariant, ButtonVariants,
};
use gpui_component::{Disableable, Icon, Sizable};

use crate::tokens::{FontSizes, Radii};
use crate::typography::AppFonts;

/// Visual variant of the button controlling color scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ButtonVariant {
    #[default]
    Default,
    Primary,
    Ghost,
    Danger,
    Dropdown,
}

/// Size variant of the button.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ButtonSize {
    #[default]
    Default,
    Small,
}

/// Full wrapper around `gpui_component::button::Button` that pre-applies
/// DBFlux design tokens and hides the underlying API.
#[derive(IntoElement)]
pub struct Button {
    id: ElementId,
    label: SharedString,
    variant: ButtonVariant,
    size: ButtonSize,
    icon: Option<Icon>,
    disabled: bool,
    w_full: bool,
    on_click: Option<Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync>>,
}

impl Button {
    pub fn new(id: impl Into<ElementId>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            variant: ButtonVariant::Default,
            size: ButtonSize::Default,
            icon: None,
            disabled: false,
            w_full: false,
            on_click: None,
        }
    }

    pub fn variant(mut self, variant: ButtonVariant) -> Self {
        self.variant = variant;
        self
    }

    pub fn primary(self) -> Self {
        self.variant(ButtonVariant::Primary)
    }

    pub fn ghost(self) -> Self {
        self.variant(ButtonVariant::Ghost)
    }

    pub fn danger(self) -> Self {
        self.variant(ButtonVariant::Danger)
    }

    pub fn dropdown(self) -> Self {
        self.variant(ButtonVariant::Dropdown)
    }

    pub fn small(mut self) -> Self {
        self.size = ButtonSize::Small;
        self
    }

    pub fn icon(mut self, icon: impl Into<Icon>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    pub fn w_full(mut self) -> Self {
        self.w_full = true;
        self
    }

    pub fn on_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        self.on_click = Some(Box::new(handler));
        self
    }

    fn gpui_variant(&self) -> GpuiButtonVariant {
        match self.variant {
            ButtonVariant::Default => GpuiButtonVariant::default(),
            ButtonVariant::Primary => GpuiButtonVariant::Primary,
            ButtonVariant::Ghost => GpuiButtonVariant::Ghost,
            ButtonVariant::Danger => GpuiButtonVariant::Danger,
            ButtonVariant::Dropdown => GpuiButtonVariant::default(),
        }
    }
}

impl RenderOnce for Button {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let gpui_variant = self.gpui_variant();

        let mut btn = GpuiButton::new(self.id)
            .label(self.label)
            .with_variant(gpui_variant)
            .rounded(Radii::SM)
            .font_family(AppFonts::BODY)
            .font_weight(FontWeight::MEDIUM)
            .text_size(if self.size == ButtonSize::Small {
                FontSizes::SM
            } else {
                FontSizes::BASE
            })
            .disabled(self.disabled)
            .when(self.variant == ButtonVariant::Dropdown, |b| {
                b.dropdown_caret(true)
            });

        if let Some(icon) = self.icon {
            btn = btn.icon(icon);
        }

        if self.size == ButtonSize::Small {
            btn = btn.small();
        }

        if self.w_full {
            btn = btn.w_full();
        }

        if let Some(handler) = self.on_click {
            btn = btn.on_click(handler);
        }

        btn
    }
}
