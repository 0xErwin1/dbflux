//! A stand-in for the workspace in document keyboard tests.
//!
//! The workspace root carries the key context of the context that owns the
//! keyboard and routes every keymap command to the active document. The host
//! here does the same for one document, with the app keymap registered, so a
//! test presses real keys and sees what the app would do.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_ui_base::keymap::{
    RunCommand, WORKSPACE_KEY_CONTEXT, init_keymap, root_key_context, run_command,
};
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext, Window, div,
};
use std::cell::RefCell;
use std::rc::Rc;

type ContextOf<D> = fn(&D, &App) -> ContextId;
type DispatchTo<D> = fn(&mut D, Command, &mut Window, &mut Context<D>) -> bool;

pub(crate) struct KeymapHost<D: 'static> {
    pub(crate) document: Entity<D>,
    context: ContextOf<D>,
    dispatch: DispatchTo<D>,
    /// Every keymap command that reached the host, in order.
    pub(crate) commands: Vec<Command>,
}

impl<D: Render> Render for KeymapHost<D> {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let context = (self.context)(self.document.read(cx), cx);

        div()
            .size_full()
            .key_context(root_key_context(WORKSPACE_KEY_CONTEXT, context, &[]))
            .on_action(cx.listener(|this, action: &RunCommand, window, cx| {
                let Some(command) = run_command(action) else {
                    return;
                };

                this.commands.push(command);

                let dispatch = this.dispatch;
                let handled = this
                    .document
                    .update(cx, |document, cx| dispatch(document, command, window, cx));

                if !handled {
                    cx.propagate();
                }
            }))
            .child(self.document.clone())
    }
}

/// Registers the component defaults, the theme, the app keymap and a toast
/// host.
pub(crate) fn init_keyboard_runtime(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(init_keymap);
    cx.update(|cx| {
        let host = cx.new(|_cx| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });
}

/// Opens a window whose root hosts the document `build` returns, under the
/// key context `context` reports and with commands routed to `dispatch`.
pub(crate) fn host_document<D: Render>(
    cx: &mut TestAppContext,
    build: impl FnOnce(&mut Window, &mut App) -> Entity<D> + 'static,
    context: ContextOf<D>,
    dispatch: DispatchTo<D>,
) -> (Entity<KeymapHost<D>>, &mut VisualTestContext) {
    let slot: Rc<RefCell<Option<Entity<KeymapHost<D>>>>> = Rc::default();

    let (_, window) = cx.add_window_view({
        let slot = slot.clone();
        move |window, cx| {
            let document = build(window, cx);
            let host = cx.new(|_| KeymapHost {
                document,
                context,
                dispatch,
                commands: Vec::new(),
            });
            slot.replace(Some(host.clone()));
            gpui_component::Root::new(host, window, cx)
        }
    });
    window.run_until_parked();

    let host = slot.borrow().clone();
    match host {
        Some(host) => (host, window),
        None => unreachable!("the window view builder always stores the host"),
    }
}
