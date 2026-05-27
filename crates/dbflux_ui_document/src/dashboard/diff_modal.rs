//! Dashboard refresh diff modal.
//!
//! Surfaces the `DashboardDiff` produced by `dbflux_core::reconcile` and
//! offers a single `Apply all` / `Cancel` choice (per the design's v1
//! decision — no per-row toggle in this iteration).
//!
//! Sections rendered (in this order):
//! - Added — upstream widgets with no matching local panel.
//! - Removed — local panels whose upstream widget has disappeared.
//! - Modified — local panels whose structural content changed upstream.
//! - Moved — local panels matched by hash at a different upstream index
//!   (small section; same conflict policy as Modified).
//!
//! Local-only panels (user-added) are listed in a compact footer note so
//! the user can confirm they will be preserved.

use dbflux_components::controls::Button;
use dbflux_components::modals::shell::ModalShell;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::DashboardDiff;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Outcome emitted when the user resolves the diff modal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DashboardDiffOutcome {
    /// User confirmed: apply every classified change atomically.
    ApplyAll,
    /// User cancelled; no state must change.
    Cancelled,
}

/// Request payload for opening the diff modal.
#[derive(Clone, Debug)]
pub struct DashboardDiffRequest {
    pub dashboard_name: String,
    pub diff: DashboardDiff,
}

/// Modal entity that renders a `DashboardDiff` and lets the user apply or
/// cancel.
///
/// Stays a plain `Entity<DashboardDiffModal>` so the host (the workspace or
/// the dashboard document) can subscribe to `DashboardDiffOutcome`.
pub struct DashboardDiffModal {
    request: Option<DashboardDiffRequest>,
    visible: bool,
}

impl DashboardDiffModal {
    pub fn new(_cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Snapshot of the current request — exposed for tests so they can
    /// assert which diff is being rendered without poking private fields.
    pub fn request(&self) -> Option<&DashboardDiffRequest> {
        self.request.as_ref()
    }

    pub fn open(&mut self, request: DashboardDiffRequest, cx: &mut Context<Self>) {
        self.request = Some(request);
        self.visible = true;
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        cx.notify();
    }
}

impl EventEmitter<DashboardDiffOutcome> for DashboardDiffModal {}

/// Build the body element for a `DashboardDiff`.
///
/// Extracted so the render impl stays short and the section layout can be
/// tested by inspecting the returned element through gpui's render path.
fn build_body(diff: &DashboardDiff, cx: &mut Context<DashboardDiffModal>) -> AnyElement {
    let theme = cx.theme();

    let header_summary = format!(
        "{} added, {} removed, {} modified, {} moved",
        diff.added.len(),
        diff.removed.len(),
        diff.modified.len(),
        diff.moved.len(),
    );

    let mut sections: Vec<AnyElement> = Vec::new();

    if !diff.added.is_empty() {
        let title = format!("Added ({})", diff.added.len());
        let rows: Vec<AnyElement> = diff
            .added
            .iter()
            .map(|w| {
                div()
                    .child(
                        Text::body(format!("+ Upstream widget #{} ({})", w.index, w.widget_kind))
                            .into_any_element(),
                    )
                    .into_any_element()
            })
            .collect();
        sections.push(section_block(&title, theme.success, rows));
    }

    if !diff.removed.is_empty() {
        let title = format!("Removed ({})", diff.removed.len());
        let rows: Vec<AnyElement> = diff
            .removed
            .iter()
            .map(|id| {
                div()
                    .child(Text::body(format!("- {id}")).into_any_element())
                    .into_any_element()
            })
            .collect();
        sections.push(section_block(&title, theme.danger, rows));
    }

    if !diff.modified.is_empty() {
        let title = format!("Modified ({})", diff.modified.len());
        let rows: Vec<AnyElement> = diff
            .modified
            .iter()
            .map(|m| {
                div()
                    .child(
                        Text::body(format!(
                            "~ {} (upstream index {})",
                            m.local_panel_id, m.upstream_index
                        ))
                        .into_any_element(),
                    )
                    .into_any_element()
            })
            .collect();
        sections.push(section_block(&title, theme.warning, rows));
    }

    if !diff.moved.is_empty() {
        let title = format!("Moved ({})", diff.moved.len());
        let rows: Vec<AnyElement> = diff
            .moved
            .iter()
            .map(|m| {
                div()
                    .child(
                        Text::body(format!(
                            "↔ {} (now at upstream index {})",
                            m.local_panel_id, m.upstream_index
                        ))
                        .into_any_element(),
                    )
                    .into_any_element()
            })
            .collect();
        sections.push(section_block(&title, theme.muted_foreground, rows));
    }

    if !diff.local_only_preserved.is_empty() {
        let note = format!(
            "{} user-added panel(s) will be preserved unchanged.",
            diff.local_only_preserved.len()
        );
        sections.push(
            div()
                .text_color(theme.muted_foreground)
                .child(Text::body(note).into_any_element())
                .into_any_element(),
        );
    }

    div()
        .flex()
        .flex_col()
        .gap(Spacing::MD)
        .child(Text::body(header_summary).into_any_element())
        .children(sections)
        .into_any_element()
}

fn section_block(title: &str, color: gpui::Hsla, rows: Vec<AnyElement>) -> AnyElement {
    div()
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .child(
            div()
                .text_color(color)
                .child(Text::body(title.to_string()).into_any_element()),
        )
        .children(rows)
        .into_any_element()
}

impl Render for DashboardDiffModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let title = format!("Sync \"{}\"", request.dashboard_name);

        // Clone the diff out of the request so the body builder doesn't
        // borrow `self.request` past the closures below.
        let diff = request.diff.clone();
        let body = build_body(&diff, cx);

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DashboardDiffOutcome::Cancelled);
            this.close(cx);
        });

        let on_apply = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DashboardDiffOutcome::ApplyAll);
            this.close(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(Button::new("dashboard-diff-cancel", "Cancel").on_click(on_cancel))
            .child(
                Button::new("dashboard-diff-apply", "Apply all")
                    .primary()
                    .on_click(on_apply),
            );

        ModalShell::new(title, body, footer.into_any_element())
            .width(px(560.0))
            .into_any_element()
    }
}


// Unit tests live in `tests/dashboard_sync.rs`; see lib.rs `recursion_limit`
// note for context.

