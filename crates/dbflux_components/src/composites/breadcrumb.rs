//! `Breadcrumb` — where a document sits: driver logo, path segments joined by
//! chevrons, the current segment in strong, and an optional metadata chip
//! (AppByzTable header, DSAppPlan "Breadcrumb").

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{App, ClickEvent, ElementId, FontWeight, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::icon::IconSource;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ChamferCut, ChromeColors, NavigationMetrics};

type SegmentClickHandler = Rc<dyn Fn(&ClickEvent, &mut Window, &mut App)>;

/// One step of a breadcrumb path.
pub struct BreadcrumbSegment {
    label: SharedString,
    icon: Option<(IconSource, Option<Hsla>)>,
    on_click: Option<(ElementId, SegmentClickHandler)>,
}

impl BreadcrumbSegment {
    pub fn new(label: impl Into<SharedString>) -> Self {
        Self {
            label: label.into(),
            icon: None,
            on_click: None,
        }
    }

    /// An icon before the label: a driver logo in its tone, or the object's
    /// icon (which takes the tint on the current segment when `color` is
    /// `None`).
    pub fn icon(mut self, icon: impl Into<IconSource>, color: Option<Hsla>) -> Self {
        self.icon = Some((icon.into(), color));
        self
    }

    /// Makes the segment navigable.
    pub fn on_click(
        mut self,
        id: impl Into<ElementId>,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_click = Some((id.into(), Rc::new(handler)));
        self
    }
}

/// A breadcrumb path. The last segment is the current one: strong and bold.
#[derive(IntoElement)]
pub struct Breadcrumb {
    segments: Vec<BreadcrumbSegment>,
    meta: Option<SharedString>,
    mono: bool,
}

impl Breadcrumb {
    pub fn new(segments: impl IntoIterator<Item = BreadcrumbSegment>) -> Self {
        Self {
            segments: segments.into_iter().collect(),
            meta: None,
            mono: false,
        }
    }

    /// A metadata chip after the path ("11 columns · 1,284 rows").
    pub fn meta(mut self, meta: impl Into<SharedString>) -> Self {
        self.meta = Some(meta.into());
        self
    }

    /// Sets the segment labels in JetBrains Mono, for paths made of object
    /// keys rather than names.
    pub fn mono(mut self) -> Self {
        self.mono = true;
        self
    }
}

impl RenderOnce for Breadcrumb {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let strong = ChromeColors::strong(theme);
        let tint = ChromeColors::tint(theme);
        let chevron_color = theme.input;
        let segment_count = self.segments.len();
        let mono = self.mono;

        let mut path = div()
            .flex()
            .min_w_0()
            .items_center()
            .gap(NavigationMetrics::BREADCRUMB_GAP)
            .text_size(NavigationMetrics::BREADCRUMB_FONT)
            .text_color(muted)
            .whitespace_nowrap();

        for (index, segment) in self.segments.into_iter().enumerate() {
            let is_current = index + 1 == segment_count;

            if index > 0 {
                path = path.child(
                    Icon::new(AppIcon::ChevronRight)
                        .size(NavigationMetrics::BREADCRUMB_CHEVRON)
                        .color(chevron_color),
                );
            }

            if let Some((icon, color)) = segment.icon {
                let color = color.unwrap_or(if is_current { tint } else { muted });
                path = path.child(
                    Icon::new(icon)
                        .size(NavigationMetrics::BREADCRUMB_ICON)
                        .color(color),
                );
            }

            let label = div()
                .min_w_0()
                .truncate()
                .when(mono, |label| {
                    label.font_family(crate::fonts::editor_family(cx))
                })
                .when(is_current, |label| {
                    label.text_color(strong).font_weight(FontWeight::BOLD)
                })
                .child(segment.label);

            path = match segment.on_click {
                Some((id, handler)) => path.child(
                    label
                        .id(id)
                        .cursor_pointer()
                        .hover(move |label| label.text_color(strong))
                        .on_click(move |event, window, cx| handler(event, window, cx)),
                ),
                None => path.child(label),
            };
        }

        path.when_some(self.meta, |path, meta| {
            path.child(
                div()
                    .relative()
                    .flex_shrink_0()
                    .ml(NavigationMetrics::BREADCRUMB_META_MARGIN)
                    .px(NavigationMetrics::BREADCRUMB_META_PADDING_X)
                    .py(NavigationMetrics::BREADCRUMB_META_PADDING_Y)
                    .font_family(crate::fonts::editor_family(cx))
                    .text_size(NavigationMetrics::BREADCRUMB_META_FONT)
                    .font_weight(FontWeight::NORMAL)
                    .child(Chamfer::new(ChamferCut::KEYCAP).fill(theme.secondary))
                    .child(meta),
            )
        })
    }
}
