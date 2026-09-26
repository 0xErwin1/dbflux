//! A view that finishes background work and calls `cx.notify()` must be drawn
//! again without any input event.
//!
//! This pins the GPUI half of a report that the window only repainted after the
//! pointer moved: the notify from a background completion invalidates the
//! window and the next frame draws the new state. When a window still waits for
//! the pointer, the frame was not requested by the platform (on X11, GPUI's
//! refresh timer only runs while the X server reports the window visible),
//! not skipped by the application.

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use gpui::prelude::*;
use gpui::{Context, Entity, TestAppContext, Window, div};

const LOADED_SELECTOR: &str = "background-repaint-loaded";

struct Loader {
    loaded: bool,
    renders: Rc<Cell<usize>>,
}

impl Loader {
    /// Loads on the background executor, then records the result on the
    /// foreground and notifies, the way document loaders do.
    fn start(&mut self, cx: &mut Context<Self>) {
        let work = cx.background_executor().spawn({
            let executor = cx.background_executor().clone();
            async move {
                executor.timer(Duration::from_millis(50)).await;
                true
            }
        });

        cx.spawn(async move |this, cx| {
            let loaded = work.await;

            if let Err(error) = this.update(cx, |this, cx| {
                this.loaded = loaded;
                cx.notify();
            }) {
                panic!("the loader outlived its window: {error}");
            }
        })
        .detach();
    }
}

impl Render for Loader {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.renders.set(self.renders.get() + 1);

        div().size_full().when(self.loaded, |this| {
            this.debug_selector(|| LOADED_SELECTOR.to_string())
        })
    }
}

struct Host {
    loader: Entity<Loader>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full().child(self.loader.clone())
    }
}

#[gpui::test]
fn a_notify_from_background_work_redraws_without_input(cx: &mut TestAppContext) {
    let renders = Rc::new(Cell::new(0));

    let (host, window) = cx.add_window_view({
        let renders = renders.clone();
        move |_, cx| Host {
            loader: cx.new(|_| Loader {
                loaded: false,
                renders,
            }),
        }
    });
    window.run_until_parked();

    let renders_before_load = renders.get();
    assert!(window.debug_bounds(LOADED_SELECTOR).is_none());

    window.update(|_, cx| {
        let loader = host.read(cx).loader.clone();
        loader.update(cx, |loader, cx| loader.start(cx));
    });
    window.executor().advance_clock(Duration::from_millis(100));
    window.run_until_parked();

    assert!(
        renders.get() > renders_before_load,
        "the notify invalidated the window and the next frame rendered the loader"
    );
    assert!(
        window.debug_bounds(LOADED_SELECTOR).is_some(),
        "the loaded state is on screen with no pointer or keyboard event"
    );
}
