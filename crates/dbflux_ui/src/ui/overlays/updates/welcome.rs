use crate::keymap::{Command, ContextId};
#[cfg(feature = "telemetry")]
use dbflux_app::telemetry::TelemetryLevel;
use dbflux_app::updates::{self, ChangelogRelease, SectionKind, changelog};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{Modal, ModalFocus};
use dbflux_components::primitives::{Chamfer, Icon, Text};
use dbflux_components::tokens::{ChamferCut, ChromeColors, FontSizes, Spacing, ui};
use dbflux_core::{LogErr, ReleaseChannel};
use dbflux_ui_base::AppStateEntity;
#[cfg(feature = "telemetry")]
use dbflux_ui_base::keymap::RunCommand;
use dbflux_ui_base::updates::channel_label;
#[cfg(feature = "telemetry")]
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::{
    LINK_TEXT_SIZE, UpdatePreference, full_changelog_link, labeled_heading, release_header,
    render_section, scroll_region, set_update_preference,
};
#[cfg(feature = "telemetry")]
use dbflux_components::primitives::{ChamferRing, focus_underline};

const DIALOG_WIDTH: Pixels = px(1000.0);
const WHATS_NEW_COLUMN_WIDTH: Pixels = px(330.0);
const MARK_SIZE: Pixels = px(56.0);
const TITLE_SIZE: Rems = ui(24.0);
const HEADER_GAP: Pixels = Spacing::LG;
const HEADER_PADDING_X: Pixels = px(28.0);
const HEADER_PADDING_TOP: Pixels = px(10.0);
const HEADER_PADDING_BOTTOM: Pixels = px(10.0);
const COLUMN_PADDING_X: Pixels = Spacing::XL;
const COLUMN_PADDING_Y: Pixels = px(14.0);
const WHATS_NEW_COLUMN_GAP: Pixels = Spacing::XS;
const UPDATES_COLUMN_GAP: Pixels = Spacing::XXS;
const FULL_CHANGELOG_PADDING_TOP: Pixels = px(10.0);
const FOOTER_GAP: Pixels = px(10.0);
const FOOTER_PADDING_Y: Pixels = px(14.0);
const FOOTER_ICON_SIZE: Rems = ui(14.0);
const BADGE_HEIGHT: Rems = ui(20.0);
const BADGE_PADDING_X: Pixels = px(7.0);

/// Tint share of the version badge fill.
const BADGE_FILL_OPACITY: f32 = 0.12;

/// Element id of the scrolling release highlights.
const HIGHLIGHTS_ID: &str = "welcome-whats-new-list";

/// Telemetry levels in card order; the roving index points into this list.
#[cfg(feature = "telemetry")]
const TELEMETRY_LEVELS: [TelemetryLevel; 3] = [
    TelemetryLevel::Off,
    TelemetryLevel::Minimal,
    TelemetryLevel::Basic,
];

/// The level shown as selected while the saved one is unavailable.
#[cfg(feature = "telemetry")]
const DEFAULT_TELEMETRY_LEVEL: TelemetryLevel = TelemetryLevel::Basic;

/// Padding of the telemetry choice cards.
#[cfg(feature = "telemetry")]
const TELEMETRY_CARD_PADDING: Pixels = px(8.0);
/// Gap between the telemetry choice cards.
#[cfg(feature = "telemetry")]
const TELEMETRY_CARD_GAP: Pixels = Spacing::SM;
/// Gap inside a telemetry card between label and description.
#[cfg(feature = "telemetry")]
const TELEMETRY_CARD_TEXT_GAP: Pixels = Spacing::XXS;
/// Padding of the privacy summary strip under the cards.
#[cfg(feature = "telemetry")]
const TELEMETRY_SUMMARY_PADDING_X: Pixels = px(4.0);
#[cfg(feature = "telemetry")]
const TELEMETRY_SUMMARY_PADDING_Y: Pixels = Spacing::XXS;

fn compact_preference_row(
    id: &'static str,
    label: String,
    hint: Option<String>,
    checked: bool,
    on_toggle: impl Fn(bool, &mut Window, &mut App) + 'static,
) -> Div {
    div()
        .flex()
        .items_start()
        .gap(px(6.0))
        .py(px(3.0))
        .line_height(px(14.0))
        .child(
            dbflux_components::controls::Checkbox::new(id)
                .checked(checked)
                .aria_label(label.clone())
                .on_click(move |value: &bool, window, cx| on_toggle(*value, window, cx)),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .flex()
                .flex_col()
                .child(Text::body(label).font_size(px(12.0)))
                .when_some(hint, |column, hint| {
                    column.child(Text::caption(hint).font_size(px(11.0)))
                }),
        )
}

/// Highlights listed per changelog section, as the board shows them: four
/// additions, two of anything else. The full changelog is one link away.
fn highlights_per_section(kind: &SectionKind) -> usize {
    match kind {
        SectionKind::Added => 4,
        _ => 2,
    }
}

/// First-run welcome: the running release's highlights and the two update
/// preferences. Enter or "Get started" closes it; Settings › Updates reopens
/// it.
#[cfg(feature = "telemetry")]
pub struct TelemetryDisclosureDismissed;

#[cfg(feature = "telemetry")]
impl EventEmitter<TelemetryDisclosureDismissed> for WelcomeDialog {}

pub struct WelcomeDialog {
    app_state: Entity<AppStateEntity>,
    visible: bool,
    #[cfg(feature = "telemetry")]
    disclosure_rendered: bool,
    /// Roving choice of the three telemetry level cards; Left and Right move
    /// it (and select), clicks select directly.
    #[cfg(feature = "telemetry")]
    telemetry_roving: usize,
    release: Option<&'static ChangelogRelease>,
    focus: ModalFocus,
    highlights_scroll: ScrollHandle,
}

impl WelcomeDialog {
    pub fn new(app_state: Entity<AppStateEntity>, cx: &mut Context<Self>) -> Self {
        Self {
            app_state,
            visible: false,
            #[cfg(feature = "telemetry")]
            disclosure_rendered: false,
            #[cfg(feature = "telemetry")]
            telemetry_roving: telemetry_level_index(DEFAULT_TELEMETRY_LEVEL),
            release: None,
            focus: ModalFocus::new(cx),
            highlights_scroll: ScrollHandle::new(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, cx: &mut Context<Self>) {
        self.open_for_version(updates::current_version(), cx);
    }

    /// Opens the dialog with the changelog section of `version`.
    fn open_for_version(&mut self, version: &str, cx: &mut Context<Self>) {
        self.release = updates::parse_version(version)
            .and_then(|current| changelog::release_for_version(changelog::bundled(), &current));
        self.highlights_scroll.set_offset(Point::default());
        self.visible = true;
        #[cfg(feature = "telemetry")]
        {
            self.disclosure_rendered = false;
            self.telemetry_roving = telemetry_level_index(self.saved_telemetry_level(cx));
        }
        self.focus.focus_on_next_render();
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        #[cfg(feature = "telemetry")]
        if self.visible && self.disclosure_rendered {
            cx.emit(TelemetryDisclosureDismissed);
        }
        self.hide(cx);
    }

    /// Switching dialogs is not a user acknowledgement of the disclosure.
    pub fn hide(&mut self, cx: &mut Context<Self>) {
        if !self.visible {
            return;
        }

        self.visible = false;
        #[cfg(feature = "telemetry")]
        {
            self.disclosure_rendered = false;
        }
        self.focus.restore(cx);
        cx.notify();
    }

    fn render_header(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let mark_path = match ReleaseChannel::current() {
            ReleaseChannel::Nightly => "branding/nightly/mark-256.png",
            ReleaseChannel::Stable | ReleaseChannel::Rc => "branding/stable/mark-256.png",
        };
        let badge_text = format!(
            "{} \u{b7} {}",
            updates::display_version(updates::current_version()),
            channel_label(ReleaseChannel::current())
        );

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .debug_selector(|| "welcome-header".to_string())
            .gap(HEADER_GAP)
            .line_height(px(14.0))
            .px(HEADER_PADDING_X)
            .pt(HEADER_PADDING_TOP)
            .pb(HEADER_PADDING_BOTTOM)
            .border_b_1()
            .border_color(theme.border)
            .child(img(mark_path).size(MARK_SIZE).flex_shrink_0())
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XXS)
                    .child(
                        div()
                            .font_family(dbflux_components::fonts::display_family(cx))
                            .font_weight(FontWeight::BLACK)
                            .text_size(TITLE_SIZE)
                            .line_height(px(24.0))
                            .text_color(ChromeColors::strong(theme))
                            .child(dbflux_i18n::t!("updates.welcome.title").to_uppercase()),
                    )
                    .child(
                        Text::caption(dbflux_i18n::t!("updates.welcome.tagline"))
                            .font_size(px(11.0))
                            .text_color(theme.muted_foreground),
                    ),
            )
            .child(
                div()
                    .relative()
                    .flex_shrink_0()
                    .flex()
                    .items_center()
                    .h(BADGE_HEIGHT)
                    .px(BADGE_PADDING_X)
                    .child(Chamfer::new(ChamferCut::KEYCAP).fill(tint.opacity(BADGE_FILL_OPACITY)))
                    .child(
                        div()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .text_size(FontSizes::LABEL)
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(tint)
                            .whitespace_nowrap()
                            .child(badge_text),
                    ),
            )
    }

    fn render_whats_new_column(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();

        let mut highlights = div().flex().flex_col().gap(WHATS_NEW_COLUMN_GAP);

        if let Some(release) = self.release {
            highlights = highlights.child(release_header(release, cx)).children(
                release.sections.iter().map(|section| {
                    render_section(
                        section,
                        highlights_per_section(&section.kind),
                        Spacing::XS,
                        cx,
                    )
                }),
            );
        }

        div()
            .w(WHATS_NEW_COLUMN_WIDTH)
            .flex_shrink_0()
            .min_h_0()
            .flex()
            .flex_col()
            .gap(WHATS_NEW_COLUMN_GAP)
            .px(COLUMN_PADDING_X)
            .py(COLUMN_PADDING_Y)
            .border_r_1()
            .border_color(theme.border)
            .child(
                labeled_heading(
                    AppIcon::History,
                    ChromeColors::tint(theme),
                    dbflux_i18n::t!("updates.welcome.whats_new"),
                )
                .flex_shrink_0()
                .pb(Spacing::XXS),
            )
            .child(
                scroll_region(HIGHLIGHTS_ID, &self.highlights_scroll, highlights)
                    .flex_grow(1.0)
                    .min_h_0(),
            )
            .child(
                full_changelog_link("welcome-full-changelog", cx)
                    .flex_shrink_0()
                    .pt(FULL_CHANGELOG_PADDING_TOP),
            )
    }

    fn render_updates_column(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let settings = self.app_state.read(cx).update_settings().clone();
        let check_state = self.app_state.clone();
        let whats_new_state = self.app_state.clone();

        div()
            .flex_1()
            .min_w_0()
            .flex()
            .flex_col()
            .gap(UPDATES_COLUMN_GAP)
            .px(COLUMN_PADDING_X)
            .py(COLUMN_PADDING_Y)
            .child(labeled_heading(
                AppIcon::Bell,
                ChromeColors::tint(theme),
                dbflux_i18n::t!("updates.welcome.updates"),
            ))
            .child(compact_preference_row(
                "welcome-check-for-updates",
                dbflux_i18n::t!("updates.welcome.notify"),
                Some(dbflux_i18n::t!("updates.welcome.notify_hint")),
                settings.check_for_updates_on_startup,
                move |value, _, cx| {
                    set_update_preference(&check_state, UpdatePreference::CheckOnStartup, value, cx)
                },
            ))
            .child(compact_preference_row(
                "welcome-show-whats-new",
                dbflux_i18n::t!("updates.welcome.show_whats_new"),
                Some(dbflux_i18n::t!("updates.welcome.show_whats_new_hint")),
                settings.show_whats_new_after_update,
                move |value, _, cx| {
                    set_update_preference(
                        &whats_new_state,
                        UpdatePreference::ShowWhatsNew,
                        value,
                        cx,
                    )
                },
            ))
            .children(self.render_telemetry_block(cx))
    }

    /// Heading, the three level cards and the privacy summary. `None` without
    /// the optional telemetry feature.
    #[cfg_attr(not(feature = "telemetry"), allow(unused_variables))]
    fn render_telemetry_block(&mut self, cx: &mut Context<Self>) -> Option<AnyElement> {
        #[cfg(feature = "telemetry")]
        {
            let (tint, muted) = {
                let theme = cx.theme();
                (ChromeColors::tint(theme), theme.muted_foreground)
            };
            let heading = labeled_heading(
                AppIcon::ChartNoAxesColumn,
                tint,
                dbflux_i18n::t!("updates.welcome.telemetry_heading"),
            );
            let cards = self.render_telemetry_cards(cx);
            Some(
                div()
                    .flex()
                    .flex_col()
                    .gap(UPDATES_COLUMN_GAP)
                    .child(heading)
                    .child(
                        Text::caption(dbflux_i18n::t!("updates.welcome.telemetry_helper"))
                            .font_size(px(11.0))
                            .text_color(muted),
                    )
                    .child(cards)
                    .into_any_element(),
            )
        }
        #[cfg(not(feature = "telemetry"))]
        None
    }

    /// The privacy disclosure. It belongs above the footer rather than at the
    /// end of a column: as a column child it overflowed into the footer as
    /// soon as the welcome grew a telemetry section.
    #[cfg_attr(not(feature = "telemetry"), allow(unused_variables))]
    fn render_telemetry_summary(&mut self, cx: &mut Context<Self>) -> Option<AnyElement> {
        #[cfg(feature = "telemetry")]
        {
            let muted = cx.theme().muted_foreground;
            let summary = div()
                .debug_selector(|| "welcome-privacy-box".to_string())
                .min_h(px(56.0))
                .flex_shrink_0()
                .px(TELEMETRY_SUMMARY_PADDING_X)
                .py(TELEMETRY_SUMMARY_PADDING_Y)
                .border_1()
                .border_color(cx.theme().border)
                .flex()
                .gap(px(6.0))
                .child(
                    div()
                        .debug_selector(|| "welcome-privacy-icon".to_string())
                        .size(px(12.0))
                        .flex_shrink_0()
                        .child(Icon::new(AppIcon::Lock).size(px(12.0)).color(muted)),
                )
                .child(
                    div()
                        .debug_selector(|| "welcome-privacy-text".to_string())
                        .flex_1()
                        .min_w_0()
                        .whitespace_normal()
                        .line_height(px(14.0))
                        .child(
                            Text::caption(dbflux_i18n::t!("updates.welcome.telemetry_privacy"))
                                .font_size(px(11.0))
                                .text_color(muted),
                        ),
                );
            Some(summary.into_any_element())
        }
        #[cfg(not(feature = "telemetry"))]
        None
    }

    /// The saved level, or the default one while telemetry is unavailable.
    #[cfg(feature = "telemetry")]
    fn saved_telemetry_level(&self, cx: &App) -> TelemetryLevel {
        self.app_state
            .read(cx)
            .telemetry_level()
            .unwrap_or(DEFAULT_TELEMETRY_LEVEL)
    }

    /// The Off / Minimal / Basic level cards, as three equal columns.
    /// Selecting one persists the level immediately; it never prepares,
    /// enqueues or acknowledges a delivery, and it never dismisses the
    /// disclosure.
    #[cfg(feature = "telemetry")]
    fn render_telemetry_cards(&mut self, cx: &mut Context<Self>) -> Div {
        let selected = self.saved_telemetry_level(cx);

        let cards = TELEMETRY_LEVELS.map(|level| self.telemetry_card(level, level == selected, cx));

        div()
            .key_context("FormNavigation")
            .flex()
            .gap(TELEMETRY_CARD_GAP)
            .children(cards)
    }

    #[cfg(feature = "telemetry")]
    fn telemetry_card(
        &self,
        level: TelemetryLevel,
        selected: bool,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let border = if selected { tint } else { theme.border };
        let is_roving = self.telemetry_roving == telemetry_level_index(level);

        let (id, icon, label, description) = match level {
            TelemetryLevel::Off => (
                "welcome-telemetry-off",
                AppIcon::EyeOff,
                dbflux_i18n::t!("updates.welcome.telemetry_off"),
                dbflux_i18n::t!("updates.welcome.telemetry_off_description"),
            ),
            TelemetryLevel::Minimal => (
                "welcome-telemetry-minimal",
                AppIcon::Tag,
                dbflux_i18n::t!("updates.welcome.telemetry_minimal"),
                dbflux_i18n::t!("updates.welcome.telemetry_minimal_description"),
            ),
            TelemetryLevel::Basic => (
                "welcome-telemetry-basic",
                AppIcon::ChartNoAxesColumn,
                dbflux_i18n::t!("updates.welcome.telemetry_basic"),
                dbflux_i18n::t!("updates.welcome.telemetry_basic_description"),
            ),
        };

        div()
            .id(id)
            .role(Role::RadioButton)
            .aria_toggled(if selected {
                Toggled::True
            } else {
                Toggled::False
            })
            .aria_label(label.clone())
            .debug_selector(move || id.to_string())
            .flex_1()
            .min_w_0()
            .flex()
            .flex_col()
            .gap(TELEMETRY_CARD_TEXT_GAP)
            .line_height(px(14.0))
            .p(TELEMETRY_CARD_PADDING)
            .cursor_pointer()
            .relative()
            .child(
                Chamfer::new(ChamferCut::CARD)
                    .fill(if selected {
                        tint.opacity(0.12)
                    } else {
                        theme.popover
                    })
                    .ring(ChamferRing::outline(border)),
            )
            .when(is_roving, |card| card.child(focus_underline(tint)))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.select_telemetry_level(level, cx);
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(TELEMETRY_CARD_TEXT_GAP)
                    .child(
                        div()
                            .size(px(10.0))
                            .rounded_full()
                            .border_1()
                            .border_color(border)
                            .when(selected, |dot| dot.bg(tint)),
                    )
                    .child(Icon::new(icon).size(px(12.0)).color(tint))
                    .child(Text::label(label).font_size(px(11.0))),
            )
            .child(
                Text::caption(description)
                    .font_size(px(11.0))
                    .text_color(theme.muted_foreground),
            )
    }

    /// Persists the chosen level in the foreground. The early guard in
    /// `TelemetryState::schedule` blocks every queue until the disclosure is
    /// dismissed, so this cannot send anything before the user acknowledges.
    #[cfg(feature = "telemetry")]
    fn select_telemetry_level(&mut self, level: TelemetryLevel, cx: &mut Context<Self>) {
        self.telemetry_roving = telemetry_level_index(level);

        let result = self.app_state.update(cx, |state, _| {
            state.set_telemetry_level(level, dbflux_core::chrono::Utc::now())
        });
        if let Err(_error) = result {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("updates.welcome.telemetry_save_failed"),
                ),
                cx,
            );
        }
        cx.notify();
    }

    #[cfg(feature = "telemetry")]
    fn step_telemetry_choice(&mut self, step: isize, cx: &mut Context<Self>) {
        let target = self.telemetry_roving.saturating_add_signed(step);

        if target != self.telemetry_roving
            && let Some(level) = TELEMETRY_LEVELS.get(target).copied()
        {
            self.select_telemetry_level(level, cx);
        }
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .debug_selector(|| "welcome-footer".to_string())
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(FOOTER_GAP)
            .px(COLUMN_PADDING_X)
            .py(FOOTER_PADDING_Y)
            .border_t_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Settings)
                    .size(FOOTER_ICON_SIZE)
                    .color(theme.muted_foreground),
            )
            .child(
                div().flex_1().min_w_0().child(
                    Text::caption(dbflux_i18n::t!("updates.welcome.change_later"))
                        .font_size(LINK_TEXT_SIZE),
                ),
            )
            .child(
                Button::new(
                    "welcome-get-started",
                    dbflux_i18n::t!("updates.welcome.get_started"),
                )
                .primary()
                .inline()
                .icon(AppIcon::ChevronRight)
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(ContextId::Modal, Command::Execute),
                    Button::kbd,
                )
                .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
            )
    }
}

/// Position of a telemetry level in the card row.
#[cfg(feature = "telemetry")]
fn telemetry_level_index(level: TelemetryLevel) -> usize {
    match level {
        TelemetryLevel::Off => 0,
        TelemetryLevel::Minimal => 1,
        TelemetryLevel::Basic => 2,
    }
}

impl Render for WelcomeDialog {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

        let theme = cx.theme();

        // The board draws the header and footer on the app background and
        // only the two columns on the panel fill, with no outline around the
        // card: its ring is clipped away by the cut.
        let dialog = cx.entity().downgrade();
        let columns = div()
            .debug_selector(|| "welcome-columns".to_string())
            .flex()
            .flex_1()
            .min_h_0()
            .bg(theme.popover)
            .child(self.render_whats_new_column(cx))
            .child(self.render_updates_column(cx));

        #[cfg(feature = "telemetry")]
        {
            self.disclosure_rendered = true;
        }

        let card = div()
            .flex()
            .flex_col()
            .min_h_0()
            .child(self.render_header(cx))
            .child(columns)
            .children(self.render_telemetry_summary(cx))
            .child(self.render_footer(cx));

        let card_fill = cx.theme().background;
        let dialog_for_enter = dialog.clone();

        let modal = Modal::new(dbflux_i18n::t!("updates.welcome.title"))
            .id("welcome-dialog")
            .without_header()
            // Left and Right rove the telemetry level cards in the modal.
            .when(cfg!(feature = "telemetry"), |modal| {
                #[cfg(feature = "telemetry")]
                let modal = modal.key_context("FormNavigation");
                modal
            })
            .width(DIALOG_WIDTH)
            .cut(ChamferCut::CARD)
            .fill(card_fill)
            .border(gpui::transparent_black())
            .child(card)
            .focus_handle(self.focus.handle())
            .scroll_with_keys(&self.highlights_scroll)
            // Escape only hides the dialog: the disclosure is acknowledged by
            // Get started (the confirm below), never by dismissing it.
            .on_close({
                let dialog = dialog.clone();
                move |_, cx| {
                    dialog.update(cx, |dialog, cx| dialog.hide(cx)).log_err();
                }
            })
            .on_confirm(move |_, cx| {
                dialog_for_enter
                    .update(cx, |dialog, cx| dialog.close(cx))
                    .log_err();
            });

        // The modal's focus trap sits on its backdrop, so the Left/Right
        // roving of the telemetry cards is handled on an ancestor of it.
        // Enter stays the modal confirm (Get started); Escape hides the dialog.
        #[cfg(feature = "telemetry")]
        {
            use dbflux_ui_base::keymap::run_command;
            let dialog_for_keys = dialog.clone();
            div()
                // The workspace mounts this dialog as a flow sibling after
                // the shell. The host must leave the normal flow or the
                // absolutely positioned modal lands below the window with
                // zero height; absolute inset-0 keeps it over the viewport
                // without taking flow space.
                .absolute()
                .inset_0()
                .debug_selector(|| "welcome-dialog-host".to_string())
                .on_action(move |action: &RunCommand, _window, cx| {
                    let Some(command) = run_command(action) else {
                        return;
                    };
                    let handled = match command {
                        Command::ColumnLeft | Command::FocusLeft => dialog_for_keys
                            .update(cx, |dialog, cx| dialog.step_telemetry_choice(-1, cx))
                            .log_err()
                            .is_some(),
                        Command::ColumnRight | Command::FocusRight => dialog_for_keys
                            .update(cx, |dialog, cx| dialog.step_telemetry_choice(1, cx))
                            .log_err()
                            .is_some(),
                        _ => false,
                    };
                    if !handled {
                        cx.propagate();
                    }
                })
                .child(modal)
                .into_any_element()
        }
        #[cfg(not(feature = "telemetry"))]
        {
            let _ = dialog;
            modal.into_any_element()
        }
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports: the parent's `gpui::*` glob would make `#[test]`
    // resolve to `gpui::test`, whose expansion recurses without bound.
    use super::super::test_support::{assert_scrolls_by_wheel_and_keys, test_app_state};
    use super::{SectionKind, WelcomeDialog, highlights_per_section};
    #[cfg(feature = "telemetry")]
    use crate::keymap::{Command, ContextId};
    #[cfg(feature = "telemetry")]
    use dbflux_app::telemetry::TelemetryLevel;
    #[cfg(feature = "telemetry")]
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture, MODAL_CHROME};
    use gpui::{TestAppContext, px, size};

    #[cfg(feature = "telemetry")]
    mod telemetry;
    mod telemetry_copy;

    /// Every control of the welcome dialog, including the three telemetry
    /// level cards, with the dialog chrome.

    #[gpui::test]
    fn the_highlights_scroll_when_the_window_is_short(cx: &mut TestAppContext) {
        let app_state = test_app_state(cx);
        let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(app_state, cx));
        window.simulate_resize(size(px(1200.0), px(460.0)));

        // Pinned to a release with enough highlights to overflow, so the test
        // does not depend on the running version's changelog section.
        dialog.update(window, |dialog, cx| dialog.open_for_version("0.8.0", cx));
        window.run_until_parked();

        let handle = dialog.read_with(window, |dialog, _| dialog.highlights_scroll.clone());
        assert_scrolls_by_wheel_and_keys(&handle, window);
    }

    #[test]
    fn highlights_follow_the_board_counts() {
        assert_eq!(highlights_per_section(&SectionKind::Added), 4);
        assert_eq!(highlights_per_section(&SectionKind::Fixed), 2);
        assert_eq!(highlights_per_section(&SectionKind::Changed), 2);
    }

    #[test]
    fn welcome_copy_resolves_in_every_locale() {
        for key in [
            "updates.welcome.title",
            "updates.welcome.tagline",
            "updates.welcome.whats_new",
            "updates.welcome.updates",
            "updates.welcome.notify",
            "updates.welcome.notify_hint",
            "updates.welcome.show_whats_new",
            "updates.welcome.show_whats_new_hint",
            "updates.welcome.change_later",
            "updates.welcome.get_started",
            "updates.welcome.full_changelog",
            "updates.welcome.telemetry_save_failed",
            "updates.welcome.telemetry_heading",
            "updates.welcome.telemetry_off",
            "updates.welcome.telemetry_off_description",
            "updates.welcome.telemetry_minimal",
            "updates.welcome.telemetry_minimal_description",
            "updates.welcome.telemetry_basic",
            "updates.welcome.telemetry_basic_description",
            "updates.welcome.telemetry_privacy",
            "updates.welcome.telemetry_helper",
        ] {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing in {locale}"
                );
                if locale != "en" {
                    assert_ne!(
                        value,
                        dbflux_i18n::t!(key, locale = "en"),
                        "English fallback for {key} in {locale}"
                    );
                }
            }
        }
    }
}
