//! The footer of the listing: the loaded counts and the key hints that give
//! way as it narrows.

use super::super::SecretStoreDocument;
use super::listing_shortcut;
use crate::chrome::{document_footer, footer_key_hint};
use dbflux_app::keymap::Command;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Icon;
use dbflux_components::tokens::{DocumentMetrics, ObjectStoreMetrics, ResultMetrics};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// The hints that give way as the footer narrows, first to go first. The
/// others (open, retrieve) are never dropped; each drop is still reachable
/// from the pane actions menu.
const HINT_DROP_ORDER: [&str; 3] = ["filter", "copy_arn", "up"];

/// The narrowest the footer summary gets, for the same reason.
const SUMMARY_MIN_WIDTH: Rems = dbflux_components::tokens::ui(48.0);

/// What the footer measured of itself: its own width and the natural width of
/// every key hint it has drawn. A hint that was dropped keeps its last width,
/// so it comes back when the room does.
#[derive(Default)]
pub(in crate::secret_store) struct FooterFit {
    pub(super) width: std::cell::Cell<Pixels>,
    /// The typography the cached hint widths were measured under.
    typography: std::cell::Cell<Pixels>,
    hints: std::cell::RefCell<std::collections::HashMap<&'static str, Pixels>>,
}

impl FooterFit {
    /// Forgets the measured hint widths when the typography they were
    /// measured under changes.
    pub(in crate::secret_store) fn track_typography(&self, key: Pixels) {
        if self.typography.replace(key) != key {
            self.hints.borrow_mut().clear();
        }
    }

    pub(in crate::secret_store) fn remember_hint(&self, name: &'static str, width: Pixels) {
        self.hints.borrow_mut().insert(name, width);
    }

    pub(in crate::secret_store) fn hint_width(&self, name: &str) -> Option<Pixels> {
        self.hints.borrow().get(name).copied()
    }
}

impl SecretStoreDocument {
    /// The footer's key hints in priority order: command, element name and
    /// label key, for the commands that have a shortcut.
    fn footer_hint_candidates(&self) -> Vec<(String, &'static str, &'static str)> {
        let mut hints = vec![(Command::Execute, "document.secret_store.footer.hint.open")];

        if self.hierarchical() {
            hints.push((Command::ColumnLeft, "document.secret_store.footer.hint.up"));
        }

        // Ctrl+C copies the selection's resource id, which the details pane
        // labels ARN.
        hints.extend([
            (
                Command::ToggleValuePanel,
                "document.secret_store.footer.hint.retrieve",
            ),
            (
                Command::ResultsCopyCell,
                "document.secret_store.footer.hint.copy_arn",
            ),
            (
                Command::FocusSearch,
                "document.secret_store.footer.hint.filter",
            ),
        ]);

        hints
            .into_iter()
            .filter_map(|(command, key)| {
                let name = key.rsplit('.').next().unwrap_or(key);
                let name = HINT_DROP_ORDER
                    .iter()
                    .copied()
                    .chain(["open", "retrieve"])
                    .find(|candidate| *candidate == name)?;

                listing_shortcut(command).map(|shortcut| (shortcut, name, key))
            })
            .collect()
    }

    /// The hints that do not fit beside the summary's minimum width, lowest
    /// priority first. Nothing is dropped before the first measurement.
    fn footer_dropped_hints(
        &self,
        shown: &[(String, &'static str, &'static str)],
        cx: &Context<Self>,
    ) -> Vec<&'static str> {
        let fit = &self.footer_fit;
        let summary_min = dbflux_components::fonts::ui_px(cx, SUMMARY_MIN_WIDTH);
        fit.track_typography(summary_min);

        let gap = ResultMetrics::FOOTER_GAP;
        let slack = DocumentMetrics::PADDING_X * 2.0 + px(1.0);
        let width = fit.width.get();
        let mut dropped: Vec<&'static str> = Vec::new();

        if width <= px(0.0) {
            return dropped;
        }

        let needed = |dropped: &[&str]| {
            shown
                .iter()
                .filter(|(_, name, _)| !dropped.contains(name))
                .map(|(_, name, _)| fit.hint_width(name).unwrap_or_default() + gap)
                .fold(summary_min + slack, |total, hint| total + hint)
        };

        for name in HINT_DROP_ORDER {
            if needed(&dropped) <= width {
                break;
            }

            dropped.push(name);
        }

        dropped
    }

    /// One key hint, with the canvas that measures its natural width.
    fn render_footer_hint(
        &self,
        shortcut: String,
        name: &'static str,
        key: &'static str,
    ) -> impl IntoElement {
        let fit = self.footer_fit.clone();

        div()
            .relative()
            .flex_shrink_0()
            .child(
                footer_key_hint(shortcut, dbflux_i18n::t!(key))
                    .id(format!("secret-footer-hint-{name}")),
            )
            .child(
                canvas(
                    move |bounds, window, _| {
                        let previous = fit.hint_width(name);
                        fit.remember_hint(name, bounds.size.width);

                        if previous != Some(bounds.size.width) {
                            window.refresh();
                        }
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full(),
            )
    }

    pub(super) fn render_footer(&self, cx: &Context<Self>) -> impl IntoElement {
        let fit = self.footer_fit.clone();
        let shown = self.footer_hint_candidates();
        let dropped = self.footer_dropped_hints(&shown, cx);
        let hints = shown
            .into_iter()
            .filter(|(_, name, _)| !dropped.contains(name))
            .map(|(shortcut, name, key)| self.render_footer_hint(shortcut, name, key))
            .collect::<Vec<_>>();

        let summary = self.loaded_summary();
        let tooltip = summary.clone();

        // The summary takes what the key hints leave and truncates; each hint
        // keeps its width, so the keys stay readable in a narrow listing.
        document_footer(cx)
            .relative()
            .h(ObjectStoreMetrics::FOOTER_HEIGHT)
            .child(
                canvas(
                    {
                        let fit = fit.clone();
                        move |bounds, window, _| {
                            if fit.width.replace(bounds.size.width) != bounds.size.width {
                                window.refresh();
                            }
                        }
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full(),
            )
            .child(
                div()
                    .id("secret-footer-summary")
                    .flex()
                    .flex_1()
                    .min_w(SUMMARY_MIN_WIDTH)
                    .overflow_hidden()
                    .items_center()
                    .gap(ResultMetrics::FOOTER_ITEM_GAP)
                    .role(gpui::accesskit::Role::Label)
                    .aria_label(summary.clone())
                    .tooltip(move |window, cx| {
                        gpui_component::tooltip::Tooltip::new(tooltip.clone()).build(window, cx)
                    })
                    .child(
                        Icon::new(AppIcon::KeyRound)
                            .size(ResultMetrics::FOOTER_ICON)
                            .color(cx.theme().muted_foreground),
                    )
                    .child(div().min_w_0().truncate().child(summary)),
            )
            .children(hints)
    }
}
