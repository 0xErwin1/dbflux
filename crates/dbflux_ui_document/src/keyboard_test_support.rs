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
    AnyElement, App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext, Window, div,
};
use std::cell::RefCell;
use std::rc::Rc;

type ContextOf<D> = fn(&D, &App) -> ContextId;
type DispatchTo<D> = fn(&mut D, Command, &mut Window, &mut Context<D>) -> bool;
type SidePanelsOf<D> = fn(&mut D, &mut Window, &mut Context<D>) -> Vec<AnyElement>;

pub(crate) struct KeymapHost<D: 'static> {
    pub(crate) document: Entity<D>,
    context: ContextOf<D>,
    dispatch: DispatchTo<D>,
    /// The panels the workspace would draw beside the document, if any.
    side_panels: Option<SidePanelsOf<D>>,
    /// Every keymap command that reached the host, in order.
    pub(crate) commands: Vec<Command>,
}

impl<D: Render> Render for KeymapHost<D> {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let context = (self.context)(self.document.read(cx), cx);
        let side_panels = match self.side_panels {
            Some(side_panels) => self
                .document
                .update(cx, |document, cx| side_panels(document, window, cx)),
            None => Vec::new(),
        };

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
            .children(side_panels)
    }
}

/// Registers the component defaults, the theme, the app keymap and a toast
/// host.
pub(crate) fn init_keyboard_runtime(cx: &mut TestAppContext) {
    // `theme::init` runs `gpui_component::init` itself; running it twice
    // registers every input binding twice, and an input action that
    // propagates (Enter) then runs once per copy.
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
    host_document_with_side_panels(cx, build, context, dispatch, None)
}

/// [`host_document`], also drawing the panels `side_panels` returns beside
/// the document, as the workspace draws a document's side panels.
pub(crate) fn host_document_with_side_panels<D: Render>(
    cx: &mut TestAppContext,
    build: impl FnOnce(&mut Window, &mut App) -> Entity<D> + 'static,
    context: ContextOf<D>,
    dispatch: DispatchTo<D>,
    side_panels: Option<SidePanelsOf<D>>,
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
                side_panels,
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

/// An app state with one profile named `name` connected through `driver`.
/// Returns the state and the profile id.
pub(crate) fn connected_app_state(
    cx: &mut TestAppContext,
    driver: &dbflux_test_support::fake_driver::FakeDriver,
    name: &str,
) -> (Entity<dbflux_ui_base::AppStateEntity>, uuid::Uuid) {
    let profile_id = uuid::Uuid::new_v4();

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                .expect("test storage setup")
        })
    });

    let profile = dbflux_core::ConnectionProfile::new(
        name,
        dbflux_core::DbConfig::SQLite {
            path: std::path::PathBuf::from(":memory:"),
            connection_id: None,
        },
    );
    let connection = driver.connect_arc(&profile).expect("fake connection");

    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.connections_mut().insert(
                profile_id,
                dbflux_core::ConnectedProfile {
                    profile,
                    connection,
                    schema: None,
                    mutation_policy: dbflux_core::MutationPolicy::default(),
                    read_only_reason: None,
                    database_schemas: Default::default(),
                    table_details: Default::default(),
                    collection_children: Default::default(),
                    schema_types: Default::default(),
                    schema_columns: Default::default(),
                    schema_indexes: Default::default(),
                    schema_foreign_keys: Default::default(),
                    schema_routines: Default::default(),
                    dependents_cache: Default::default(),
                    active_database: None,
                    redis_key_cache: Default::default(),
                    database_connections: Default::default(),
                    proxy_tunnel: None,
                },
            );
        });
    });

    (app_state, profile_id)
}
