//! Rendering for the command palette: the card, the windowed rows, the
//! section headers and the footer.
//!
//! The engine stays in the parent module; this file is the part that turns its
//! state into elements.

use super::*;

// The product implements `EventEmitter` for its concrete palette at the old
// path, for its own selection and its own closed event (the host's
// [`PaletteHost::Closed`]): one generic impl over `T::Selection` would
// conflict with the closed event, the way two blanket impls of one event
// trait always do.

impl<T: PaletteEntry, H: PaletteHost> Render for CommandPalette<T, H>
where
    Self: EventEmitter<T::Selection> + EventEmitter<H::Closed>,
{
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();

        // Build the windowed list (scroll_offset..+VISIBLE_ITEMS) then group
        // the resulting items by section. Section headers are interleaved
        // before the first item of each section but are NOT counted in the
        // `display_idx` that compares against `selected_index`.
        let windowed: Vec<(usize, usize)> = self
            .filtered
            .iter()
            .enumerate()
            .skip(self.scroll_offset)
            .take(VISIBLE_ITEMS)
            .map(|(display_idx, filtered)| (display_idx, filtered.index))
            .collect();

        let mut rows: Vec<PaletteRow> =
            Vec::with_capacity(windowed.len() + PaletteSection::ORDER.len());

        for section in PaletteSection::ORDER {
            let mut header_pushed = false;

            for &(display_idx, palette_idx) in &windowed {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "`palette_idx` is a `FilteredItem` index; the three \
                              construction sites of `FilteredItem` (`filter_items`, \
                              `open_with_items` and `toggle`) each pair the index \
                              with `.enumerate()` over `self.items`, so \
                              `palette_idx` stays below `self.items.len()`"
                )]
                if self.items[palette_idx].section() != section {
                    continue;
                }

                if !header_pushed {
                    rows.push(PaletteRow::SectionHeader(section.label(cx)));
                    header_pushed = true;
                }

                rows.push(PaletteRow::Item {
                    display_idx,
                    palette_idx,
                });
            }
        }

        let list_rows: Vec<AnyElement> = rows
            .into_iter()
            .map(|row| match row {
                PaletteRow::SectionHeader(label) => div()
                    .pt(PaletteMetrics::SECTION_PADDING_TOP)
                    .pb(PaletteMetrics::SECTION_PADDING_BOTTOM)
                    .px(PaletteMetrics::PADDING_X)
                    .child(Text::label(label).font_size(PaletteMetrics::SECTION_FONT))
                    .into_any_element(),
                PaletteRow::Item {
                    display_idx,
                    palette_idx,
                } => {
                    let is_selected = display_idx == self.selected_index;

                    #[expect(
                        clippy::indexing_slicing,
                        reason = "`palette_idx` is a `FilteredItem` index; the three \
                                  construction sites of `FilteredItem` (`filter_items`, \
                                  `open_with_items` and `toggle`) each pair the index \
                                  with `.enumerate()` over `self.items`, so \
                                  `palette_idx` stays below `self.items.len()`"
                    )]
                    self.render_palette_item(display_idx, &self.items[palette_idx], is_selected, cx)
                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                            cx.stop_propagation();
                        })
                        .on_click(cx.listener(move |this, _, window, cx| {
                            this.selected_index = display_idx;
                            this.execute_selected(OpenPlacement::ReuseTab, window, cx);
                        }))
                        .into_any_element()
                }
            })
            .collect();

        let surface = inspect_surface_role(SurfaceRole::Modal);
        let card_shape = Chamfer::new(ChamferCut::CARD)
            .fill(surface.fill.resolve(theme))
            .border(surface.border.resolve(theme));

        let max_height = window.viewport_size().height - PaletteMetrics::TOP_OFFSET * 2.0;

        let handle = cx.entity().downgrade();
        let overlay = div()
            .id("command-palette-overlay")
            .absolute()
            .flex()
            .justify_center()
            .items_start()
            .pt(PaletteMetrics::TOP_OFFSET)
            .bg(overlay_bg(theme))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.hide(cx);
                }),
            );

        // The product's keymap binds its CommandPalette layer to its
        // run-command action; the host registers the listener, and anything
        // the palette does not answer goes on to the workspace.
        let overlay = H::attach_run_command(
            overlay,
            Arc::new(move |command_id, window, cx| {
                let Some(entity) = handle.upgrade() else {
                    return false;
                };

                entity.update(cx, |this, cx| {
                    this.handle_run_command(command_id, window, cx)
                })
            }),
        );

        overlay
            .key_context(H::key_context())
            .child(
                div()
                    .id("command-palette-container")
                    .relative()
                    .w(PaletteMetrics::WIDTH)
                    .max_w_full()
                    .max_h(max_height)
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(card_shape)
                    .child(self.render_search_row(cx))
                    .child(
                        div()
                            .id("command-palette-list")
                            .flex()
                            .flex_col()
                            .flex_1()
                            .min_h_0()
                            .overflow_y_hidden()
                            .pb(PaletteMetrics::LIST_PADDING_BOTTOM)
                            .on_scroll_wheel(cx.listener(
                                |this, event: &ScrollWheelEvent, _window, cx| {
                                    let delta = event.delta.pixel_delta(px(1.0));
                                    if delta.y < px(0.0) {
                                        this.scroll_down(cx);
                                    } else if delta.y > px(0.0) {
                                        this.scroll_up(cx);
                                    }
                                },
                            ))
                            .children(list_rows)
                            .when(self.filtered.is_empty(), |list| {
                                list.child(
                                    div()
                                        .flex()
                                        .justify_center()
                                        .h(PaletteMetrics::SEARCH_HEIGHT)
                                        .items_center()
                                        .child(
                                            Text::body(
                                                strings::text(cx, "palette.empty").to_string(),
                                            )
                                            .muted_foreground(),
                                        ),
                                )
                            }),
                    )
                    .child(Self::render_footer(cx)),
            )
            .into_any_element()
    }
}
