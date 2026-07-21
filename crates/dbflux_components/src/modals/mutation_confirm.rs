use crate::controls::{Button, ButtonVariant, Checkbox, InputEvent, InputState};
use crate::modals::shell::{ModalShell, ModalVariant};
use crate::primitives::surface_raised;
use crate::tokens::{FontSizes, Spacing};
use crate::typography::AppFonts;
use gpui::prelude::*;
use gpui::{Context, Entity, EventEmitter, SharedString, Subscription, Window, div, px};
use gpui_component::ActiveTheme;

/// Outcome emitted by both mutation confirmation modals when resolved.
#[derive(Clone, Debug)]
pub enum MutationConfirmOutcome {
    Confirmed,
    Cancelled,
}

// =============================================================================
// ModalMutationConfirm (Default / light confirmation — E-1)
// =============================================================================

/// Request payload for the light mutation confirmation modal.
#[derive(Clone, Debug)]
pub struct MutationConfirmRequest {
    /// Short description of the operation, shown in the body (e.g. "Delete 42 rows from users").
    pub summary: String,
    /// SQL preview text displayed in the code block.
    pub sql_preview: String,
    /// Optional pre-fetched sample rows shown below the SQL preview.
    ///
    /// Each item is a `Vec<String>` — one string per column value.
    pub sample_rows: Option<Vec<Vec<String>>>,
    /// Column headers for the sample-rows table.
    pub sample_columns: Vec<String>,
}

/// Light mutation confirmation modal (E-1).
///
/// Shows summary, SQL preview, and optional sample rows. No type-to-confirm
/// or opt-in checkbox. Used for UPDATE/DELETE where the row count is small
/// and the driver does not require extra confirmation.
pub struct ModalMutationConfirm {
    request: Option<MutationConfirmRequest>,
    visible: bool,
}

impl ModalMutationConfirm {
    pub fn new(_window: &mut Window, _cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: MutationConfirmRequest, cx: &mut Context<Self>) {
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

impl EventEmitter<MutationConfirmOutcome> for ModalMutationConfirm {}

impl Render for ModalMutationConfirm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let summary = request.summary.clone();
        let sql = request.sql_preview.clone();
        let sample_rows = request.sample_rows.clone();
        let sample_columns = request.sample_columns.clone();

        let mut body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .child(SharedString::from(summary)),
            )
            .child(
                surface_raised(cx)
                    .w_full()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .font_family(AppFonts::MONO)
                            .text_color(theme.foreground)
                            .child(SharedString::from(sql)),
                    ),
            );

        // Sample rows preview
        match sample_rows {
            Some(rows) if !rows.is_empty() => {
                let mut table = div().flex().flex_col().gap(Spacing::XS);

                // Header row
                let mut header_row = div().flex().flex_row().gap(Spacing::SM);
                for col in &sample_columns {
                    header_row = header_row.child(
                        div()
                            .flex_1()
                            .text_size(FontSizes::XS)
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .text_color(theme.muted_foreground)
                            .child(SharedString::from(col.clone())),
                    );
                }
                table = table.child(header_row);

                for row_vals in rows.iter().take(5) {
                    let mut data_row = div().flex().flex_row().gap(Spacing::SM);
                    for val in row_vals {
                        data_row = data_row.child(
                            div()
                                .flex_1()
                                .text_size(FontSizes::XS)
                                .font_family(AppFonts::MONO)
                                .text_color(theme.foreground)
                                .child(SharedString::from(val.clone())),
                        );
                    }
                    table = table.child(data_row);
                }

                body = body.child(surface_raised(cx).w_full().p(Spacing::SM).child(table));
            }
            Some(_) | None => {
                body = body.child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.muted_foreground)
                        .child("Sample preview unavailable"),
                );
            }
        }

        let footer = div()
            .flex()
            .flex_row()
            .gap_2()
            .justify_end()
            .child(
                Button::new("mutation-confirm-cancel", "Cancel")
                    .variant(ButtonVariant::Default)
                    .on_click(cx.listener(|this, _event, _window, cx| {
                        this.visible = false;
                        cx.emit(MutationConfirmOutcome::Cancelled);
                        cx.notify();
                    })),
            )
            .child(
                Button::new("mutation-confirm-ok", "Confirm")
                    .variant(ButtonVariant::Primary)
                    .on_click(cx.listener(|this, _event, _window, cx| {
                        this.visible = false;
                        cx.emit(MutationConfirmOutcome::Confirmed);
                        cx.notify();
                    })),
            );

        ModalShell::new(
            "Confirm mutation",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Default)
        .width(px(520.0))
        .into_any_element()
    }
}

// =============================================================================
// ModalMutationConfirmHard (Danger + TypeToConfirm + opt-in checkbox — E-2/E-3/E-4/E-6)
// =============================================================================

/// Request payload for the hard mutation confirmation modal.
#[derive(Clone, Debug)]
pub struct MutationConfirmHardRequest {
    /// Short description of the operation shown in the body.
    pub summary: String,
    /// The exact table name the user must type to enable the confirm button (E-3).
    pub type_to_confirm: String,
    /// SQL preview text.
    pub sql_preview: String,
    /// Optional pre-fetched sample rows.
    pub sample_rows: Option<Vec<Vec<String>>>,
    /// Column headers for the sample-rows table.
    pub sample_columns: Vec<String>,
    /// When `true`, the per-execution opt-in checkbox is shown (E-2/E-4).
    pub require_opt_in: bool,
}

/// Hard mutation confirmation modal (E-2, E-3, E-4, E-6).
///
/// Danger variant with a 2 px red top-border accent. The confirm button is
/// disabled until:
/// - The user types the table name exactly (TypeToConfirm gate, E-3).
/// - The opt-in checkbox is checked, if `require_opt_in` is set (E-4).
///
/// The opt-in checkbox is always unchecked on every open (E-4).
pub struct ModalMutationConfirmHard {
    request: Option<MutationConfirmHardRequest>,
    visible: bool,
    confirm_input: Entity<InputState>,
    type_matches: bool,
    opt_in_checked: bool,
    _subscription: Option<Subscription>,
}

impl ModalMutationConfirmHard {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let confirm_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Type table name to confirm"));
        Self {
            request: None,
            visible: false,
            confirm_input,
            type_matches: false,
            opt_in_checked: false,
            _subscription: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(
        &mut self,
        request: MutationConfirmHardRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Reset state on every open (E-4).
        self.confirm_input.update(cx, |input, cx| {
            input.set_value(String::new(), window, cx);
        });
        self.type_matches = false;
        self.opt_in_checked = false;

        let expected = request.type_to_confirm.clone();
        let input = self.confirm_input.clone();

        let subscription = cx.subscribe_in(
            &input,
            window,
            move |this, input_state, event: &InputEvent, _, cx| {
                if !matches!(event, InputEvent::Change) {
                    return;
                }
                let typed = input_state.read(cx).value().to_string();
                let matches = typed == expected;
                if this.type_matches != matches {
                    this.type_matches = matches;
                    cx.notify();
                }
            },
        );

        self.request = Some(request);
        self.visible = true;
        self._subscription = Some(subscription);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.type_matches = false;
        self.opt_in_checked = false;
        self._subscription = None;
        cx.notify();
    }

    fn confirm_enabled(&self) -> bool {
        let require_opt_in = self
            .request
            .as_ref()
            .map(|r| r.require_opt_in)
            .unwrap_or(false);

        self.type_matches && (!require_opt_in || self.opt_in_checked)
    }
}

impl EventEmitter<MutationConfirmOutcome> for ModalMutationConfirmHard {}

impl Render for ModalMutationConfirmHard {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let summary = request.summary.clone();
        let sql = request.sql_preview.clone();
        let sample_rows = request.sample_rows.clone();
        let sample_columns = request.sample_columns.clone();
        let require_opt_in = request.require_opt_in;
        let confirm_enabled = self.confirm_enabled();
        let opt_in_checked = self.opt_in_checked;

        let _ = window;

        let mut body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .child(SharedString::from(summary)),
            )
            .child(
                surface_raised(cx)
                    .w_full()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .font_family(AppFonts::MONO)
                            .text_color(theme.foreground)
                            .child(SharedString::from(sql)),
                    ),
            );

        // Sample rows preview
        match sample_rows {
            Some(rows) if !rows.is_empty() => {
                let mut table = div().flex().flex_col().gap(Spacing::XS);

                let mut header_row = div().flex().flex_row().gap(Spacing::SM);
                for col in &sample_columns {
                    header_row = header_row.child(
                        div()
                            .flex_1()
                            .text_size(FontSizes::XS)
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .text_color(theme.muted_foreground)
                            .child(SharedString::from(col.clone())),
                    );
                }
                table = table.child(header_row);

                for row_vals in rows.iter().take(5) {
                    let mut data_row = div().flex().flex_row().gap(Spacing::SM);
                    for val in row_vals {
                        data_row = data_row.child(
                            div()
                                .flex_1()
                                .text_size(FontSizes::XS)
                                .font_family(AppFonts::MONO)
                                .text_color(theme.foreground)
                                .child(SharedString::from(val.clone())),
                        );
                    }
                    table = table.child(data_row);
                }

                body = body.child(surface_raised(cx).w_full().p(Spacing::SM).child(table));
            }
            Some(_) | None => {
                body = body.child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.muted_foreground)
                        .child("Sample preview unavailable"),
                );
            }
        }

        // TypeToConfirm input
        body = body.child(
            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.muted_foreground)
                        .child("Type the table name to confirm:"),
                )
                .child(crate::controls::Input::new(&self.confirm_input)),
        );

        // Per-execution opt-in checkbox (E-2/E-4)
        if require_opt_in {
            body = body.child(
                Checkbox::new("mutation-hard-opt-in")
                    .checked(opt_in_checked)
                    .label("I understand this operation will modify data")
                    .on_click(cx.listener(|this, checked, _window, cx| {
                        this.opt_in_checked = *checked;
                        cx.notify();
                    })),
            );
        }

        let footer = div()
            .flex()
            .flex_row()
            .gap_2()
            .justify_end()
            .child(
                Button::new("mutation-hard-cancel", "Cancel")
                    .variant(ButtonVariant::Default)
                    .on_click(cx.listener(|this, _event, _window, cx| {
                        this.visible = false;
                        cx.emit(MutationConfirmOutcome::Cancelled);
                        cx.notify();
                    })),
            )
            .child(
                Button::new("mutation-hard-confirm", "Confirm")
                    .variant(ButtonVariant::Danger)
                    .disabled(!confirm_enabled)
                    .on_click(cx.listener(|this, _event, _window, cx| {
                        if !this.confirm_enabled() {
                            return;
                        }
                        this.visible = false;
                        cx.emit(MutationConfirmOutcome::Confirmed);
                        cx.notify();
                    })),
            );

        ModalShell::new(
            "Confirm mutation",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Danger)
        .width(px(560.0))
        .into_any_element()
    }
}
