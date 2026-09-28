//! Keyboard cursor over a side rail of editable rows, such as the query
//! builders.
//!
//! The owner lists its rows ([`RailRow`]) from its state whenever a key
//! arrives: a stable id, the fields of the row in reading order and what the
//! row-wide keys do (toggle, add, add group, remove). [`rail_command`] moves a
//! cursor over those rows and works the field it points at: a text field
//! takes keyboard focus, a dropdown opens with the keyboard, a button runs.
//! The same function drives the rail's action menu, listing the cursor row's
//! actions and the owner's own ([`RailOwner::rail_actions`]). The owner draws
//! the cursor with the [`RailMark`] it gets from [`RailNav::mark`] and puts its
//! rows in [`rail_scroll_area`], which keeps the cursor row in view.

use std::cell::Cell;
use std::rc::Rc;

use dbflux_core::keymap_types::Command;
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Bounds, Context, Div, ElementId, Entity, FocusHandle, Hsla, Pixels,
    ScrollHandle, SharedString, Window, canvas, deferred, div, point, px,
};
use gpui_component::ActiveTheme;
use gpui_component::input::AnyInputState;
use gpui_component::scroll::Scrollbar;

use crate::composites::{MenuItem, menu_row, render_menu_container};
use crate::controls::Dropdown;
use crate::tokens::{BuilderMetrics, ChromeColors};

/// Rows one Page Up or Page Down moves the cursor.
const PAGE_ROWS: isize = 8;

/// Something a key does to the rail's owner.
pub type RailHandler<T> = Rc<dyn Fn(&mut T, &mut Window, &mut Context<T>)>;

/// What working a field (Enter) or a row action does.
pub enum RailTarget<T: 'static> {
    /// A text field: it takes keyboard focus, and Escape gives it back.
    Text(AnyInputState),
    /// A dropdown: it takes keyboard focus and opens.
    Dropdown(Entity<Dropdown>),
    /// A button, a switch or a link: runs what a click runs.
    Run(RailHandler<T>),
}

impl<T: 'static> Clone for RailTarget<T> {
    fn clone(&self) -> Self {
        match self {
            RailTarget::Text(state) => RailTarget::Text(state.clone()),
            RailTarget::Dropdown(dropdown) => RailTarget::Dropdown(dropdown.clone()),
            RailTarget::Run(handler) => RailTarget::Run(handler.clone()),
        }
    }
}

impl<T: 'static> RailTarget<T> {
    pub fn text(state: impl Into<AnyInputState>) -> Self {
        RailTarget::Text(state.into())
    }

    pub fn dropdown(dropdown: Entity<Dropdown>) -> Self {
        RailTarget::Dropdown(dropdown)
    }

    pub fn run(handler: impl Fn(&mut T, &mut Window, &mut Context<T>) + 'static) -> Self {
        RailTarget::Run(Rc::new(handler))
    }
}

/// One line of the rail the cursor stops on.
pub struct RailRow<T: 'static> {
    /// Stable across edits elsewhere in the rail, so the cursor stays on the
    /// row when rows before it come and go.
    pub id: SharedString,
    /// The row's controls in reading order, each with a stable id.
    pub fields: Vec<(SharedString, RailTarget<T>)>,
    /// Space: flips the row's switch (AND / OR, ascending / descending).
    pub toggle: Option<RailHandler<T>>,
    /// A: adds an entry to the list the row belongs to.
    pub add: Option<RailTarget<T>>,
    /// Shift+A: adds a nested group.
    pub add_group: Option<RailTarget<T>>,
    /// X or D: removes the row.
    pub remove: Option<RailHandler<T>>,
}

impl<T: 'static> RailRow<T> {
    pub fn new(id: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            fields: Vec::new(),
            toggle: None,
            add: None,
            add_group: None,
            remove: None,
        }
    }

    pub fn field(mut self, id: impl Into<SharedString>, target: RailTarget<T>) -> Self {
        self.fields.push((id.into(), target));
        self
    }

    pub fn on_toggle(
        mut self,
        handler: impl Fn(&mut T, &mut Window, &mut Context<T>) + 'static,
    ) -> Self {
        self.toggle = Some(Rc::new(handler));
        self
    }

    pub fn on_add(mut self, target: RailTarget<T>) -> Self {
        self.add = Some(target);
        self
    }

    pub fn on_add_group(mut self, target: RailTarget<T>) -> Self {
        self.add_group = Some(target);
        self
    }

    pub fn on_remove(
        mut self,
        handler: impl Fn(&mut T, &mut Window, &mut Context<T>) + 'static,
    ) -> Self {
        self.remove = Some(Rc::new(handler));
        self
    }
}

/// An entry of the rail's action menu.
pub struct RailMenuEntry<T: 'static> {
    /// Stable id, used for the menu row's element id.
    pub id: SharedString,
    pub label: SharedString,
    pub shortcut: Option<SharedString>,
    pub enabled: bool,
    pub target: RailTarget<T>,
}

impl<T: 'static> RailMenuEntry<T> {
    pub fn new(
        id: impl Into<SharedString>,
        label: impl Into<SharedString>,
        target: RailTarget<T>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            shortcut: None,
            enabled: true,
            target,
        }
    }

    pub fn shortcut(mut self, shortcut: Option<SharedString>) -> Self {
        self.shortcut = shortcut;
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

struct RailMenu<T: 'static> {
    entries: Vec<RailMenuEntry<T>>,
    selected: usize,
}

/// The cursor and the open action menu of a rail.
pub struct RailNav<T: 'static> {
    row: Option<SharedString>,
    /// Where the cursor row was, for when that row is removed.
    row_index: usize,
    field: usize,
    menu: Option<RailMenu<T>>,
    scroll: ScrollHandle,
    /// Set when the cursor moves; the cursor row scrolls into view on the
    /// next paint and clears it.
    reveal: Rc<Cell<bool>>,
}

impl<T: 'static> Default for RailNav<T> {
    fn default() -> Self {
        Self {
            row: None,
            row_index: 0,
            field: 0,
            menu: None,
            scroll: ScrollHandle::new(),
            reveal: Rc::default(),
        }
    }
}

impl<T: 'static> RailNav<T> {
    /// The scroll position of the rows, for [`rail_scroll_area`].
    pub fn scroll_handle(&self) -> &ScrollHandle {
        &self.scroll
    }

    pub fn menu_is_open(&self) -> bool {
        self.menu.is_some()
    }

    /// Labels of the open menu's entries, in order.
    pub fn menu_labels(&self) -> Vec<SharedString> {
        self.menu
            .as_ref()
            .map(|menu| {
                menu.entries
                    .iter()
                    .map(|entry| entry.label.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Id of the row the cursor is on, once a key moved it.
    pub fn cursor_row(&self) -> Option<&SharedString> {
        self.row.as_ref()
    }

    /// Puts the cursor back on the first row.
    pub fn reset(&mut self) {
        self.row = None;
        self.row_index = 0;
        self.field = 0;
        self.menu = None;
    }

    fn resolve(&self, rows: &[RailRow<T>]) -> Option<usize> {
        if rows.is_empty() {
            return None;
        }

        if let Some(id) = &self.row
            && let Some(index) = rows.iter().position(|row| &row.id == id)
        {
            return Some(index);
        }

        Some(self.row_index.min(rows.len() - 1))
    }

    fn place(&mut self, rows: &[RailRow<T>], index: usize) {
        self.row = Some(rows[index].id.clone());
        self.row_index = index;
        self.reveal.set(true);
    }

    fn field_index(&self, row: &RailRow<T>) -> Option<usize> {
        (!row.fields.is_empty()).then(|| self.field.min(row.fields.len() - 1))
    }

    /// What the owner draws: the cursor row and field while `active` (the
    /// rail holds the keyboard), nothing otherwise.
    pub fn mark(&self, rows: &[RailRow<T>], active: bool, cx: &App) -> RailMark {
        if !active {
            return RailMark::default();
        }

        let Some(index) = self.resolve(rows) else {
            return RailMark::default();
        };
        let row = &rows[index];

        RailMark {
            row: Some(row.id.clone()),
            field: self
                .field_index(row)
                .map(|field| row.fields[field].0.clone()),
            color: Some(ChromeColors::tint(cx.theme())),
            reveal: Some((self.scroll.clone(), self.reveal.clone())),
        }
    }
}

/// Where the keyboard cursor is, as the owner draws it.
#[derive(Clone, Default)]
pub struct RailMark {
    row: Option<SharedString>,
    field: Option<SharedString>,
    color: Option<Hsla>,
    reveal: Option<(ScrollHandle, Rc<Cell<bool>>)>,
}

impl RailMark {
    pub fn is_row(&self, id: &str) -> bool {
        self.row.as_ref().is_some_and(|row| row.as_ref() == id)
    }

    pub fn is_field(&self, row: &str, field: &str) -> bool {
        self.is_row(row)
            && self
                .field
                .as_ref()
                .is_some_and(|mark| mark.as_ref() == field)
    }

    /// `element` with the cursor wash when it is the cursor row. A row inside
    /// [`rail_scroll_area`] also scrolls into view when the cursor lands on it.
    pub fn row<E: ParentElement + Styled>(&self, id: &str, element: E) -> E {
        let Some(color) = self.color.filter(|_| self.is_row(id)) else {
            return element;
        };

        element
            .relative()
            .bg(color.opacity(BuilderMetrics::CURSOR_ROW_ALPHA))
            .children(self.reveal_probe())
    }

    /// [`RailMark::row`] for a row outside the scrolling area (a header).
    pub fn fixed_row<E: ParentElement + Styled>(&self, id: &str, element: E) -> E {
        match self.color.filter(|_| self.is_row(id)) {
            Some(color) => element.bg(color.opacity(BuilderMetrics::CURSOR_ROW_ALPHA)),
            None => element,
        }
    }

    /// `container` with a ring over it when it holds the cursor field.
    pub fn ring<E: ParentElement + Styled>(&self, row: &str, field: &str, container: E) -> E {
        let Some(color) = self.color.filter(|_| self.is_field(row, field)) else {
            return container;
        };

        container
            .relative()
            .child(div().absolute().inset_0().border_1().border_color(color))
    }

    /// `element` wrapped so it can carry the field ring.
    pub fn ring_element(&self, row: &str, field: &str, element: impl IntoElement) -> AnyElement {
        if !self.is_field(row, field) {
            return element.into_any_element();
        }

        self.ring(row, field, div().flex_shrink_0().child(element))
            .into_any_element()
    }

    fn reveal_probe(&self) -> Option<AnyElement> {
        let (handle, pending) = self.reveal.clone()?;

        Some(
            canvas(
                move |bounds, window, _cx| {
                    if pending.replace(false) && scroll_into_view(&handle, bounds) {
                        window.refresh();
                    }
                },
                |_, _, _, _| {},
            )
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .into_any_element(),
        )
    }
}

/// Scrolls `handle` so `bounds` sits inside its viewport. Returns whether
/// the offset changed.
fn scroll_into_view(handle: &ScrollHandle, bounds: Bounds<Pixels>) -> bool {
    let viewport = handle.bounds();
    if viewport.size.height <= px(0.) {
        return false;
    }

    let margin = BuilderMetrics::CURSOR_REVEAL_MARGIN;
    let offset = handle.offset();
    let above = viewport.top() - bounds.top();
    let below = bounds.bottom() - viewport.bottom();

    let target = if above > -margin {
        offset.y + above + margin
    } else if below > -margin {
        offset.y - below - margin
    } else {
        return false;
    };

    let max_scroll = handle.max_offset().y.max(px(0.));
    let target = target.min(px(0.)).max(-max_scroll);
    if target == offset.y {
        return false;
    }

    handle.set_offset(point(offset.x, target));
    true
}

/// The scrolling column the rail's rows sit in, tracked by the rail's own
/// scroll handle so the cursor row can scroll into view.
pub fn rail_scroll_area<T: 'static>(
    nav: &RailNav<T>,
    id: impl Into<ElementId>,
    content: Div,
) -> Div {
    div()
        .relative()
        .flex_1()
        .min_h(px(0.))
        .child(
            div()
                .id(id)
                .size_full()
                .flex()
                .flex_col()
                .overflow_y_scroll()
                .track_scroll(&nav.scroll)
                .child(content.flex_none()),
        )
        .child(
            div()
                .absolute()
                .inset_0()
                .child(Scrollbar::vertical(&nav.scroll)),
        )
}

/// What [`rail_command`] did with a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RailOutcome {
    Handled,
    /// Escape with nothing to close: the keyboard leaves the rail.
    Leave,
    /// Not a rail key.
    Unhandled,
}

/// A rail the keyboard can drive with [`rail_command`].
pub trait RailOwner: Sized + 'static {
    fn rail_nav(&mut self) -> &mut RailNav<Self>;

    /// The rows in the order they are drawn.
    fn rail_rows(&self, cx: &App) -> Vec<RailRow<Self>>;

    /// The rail-wide entries of the action menu (run, save, modes, close),
    /// listed after the cursor row's actions.
    fn rail_actions(&self, cx: &App) -> Vec<RailMenuEntry<Self>>;

    /// The element that holds the keyboard while the cursor moves.
    fn rail_focus_handle(&self, cx: &App) -> FocusHandle;

    /// The key shown beside a row action in the menu.
    fn rail_shortcut(&self, _command: Command) -> Option<SharedString> {
        None
    }
}

/// Answers a rail key. Keys the rail does not know (running, saving, modes)
/// come back [`RailOutcome::Unhandled`] for the owner.
pub fn rail_command<T: RailOwner>(
    this: &mut T,
    command: Command,
    window: &mut Window,
    cx: &mut Context<T>,
) -> RailOutcome {
    if this.rail_nav().menu.is_some() {
        menu_command(this, command, window, cx);
        return RailOutcome::Handled;
    }

    let rows = this.rail_rows(cx);

    let outcome = match command {
        Command::Cancel => {
            let handle = this.rail_focus_handle(cx);
            if handle.is_focused(window) {
                return RailOutcome::Leave;
            }
            handle.focus(window, cx);
            RailOutcome::Handled
        }
        Command::SelectNext => step_row(this, &rows, 1),
        Command::SelectPrev => step_row(this, &rows, -1),
        Command::PageDown => step_row(this, &rows, PAGE_ROWS),
        Command::PageUp => step_row(this, &rows, -PAGE_ROWS),
        Command::SelectFirst => step_row(this, &rows, isize::MIN / 2),
        Command::SelectLast => step_row(this, &rows, isize::MAX / 2),
        Command::ColumnLeft => step_field(this, &rows, -1),
        Command::ColumnRight => step_field(this, &rows, 1),
        Command::Execute => {
            let target = current_row(this, &rows).and_then(|row| {
                let nav = this.rail_nav();
                nav.field_index(row)
                    .map(|field| row.fields[field].1.clone())
                    .or_else(|| row.add.clone())
            });
            if let Some(target) = target {
                activate(this, &target, window, cx);
            }
            RailOutcome::Handled
        }
        Command::ExpandCollapse => {
            let toggle = current_row(this, &rows).and_then(|row| row.toggle.clone());
            if let Some(toggle) = toggle {
                toggle(this, window, cx);
            }
            RailOutcome::Handled
        }
        Command::AddItem | Command::AddGroup => {
            let target = current_row(this, &rows).and_then(|row| match command {
                Command::AddItem => row.add.clone(),
                _ => row.add_group.clone(),
            });
            if let Some(target) = target {
                let before: Vec<SharedString> = rows.iter().map(|row| row.id.clone()).collect();
                activate(this, &target, window, cx);
                follow_new_row(this, &before, cx);
            }
            RailOutcome::Handled
        }
        Command::Delete => {
            let remove = current_row(this, &rows).and_then(|row| row.remove.clone());
            if let Some(remove) = remove {
                remove(this, window, cx);
                this.rail_nav().field = 0;
            }
            RailOutcome::Handled
        }
        Command::OpenPaneActions => {
            open_menu(this, &rows, cx);
            RailOutcome::Handled
        }
        _ => RailOutcome::Unhandled,
    };

    if outcome == RailOutcome::Handled {
        cx.notify();
    }
    outcome
}

fn current_row<'a, T: RailOwner>(this: &mut T, rows: &'a [RailRow<T>]) -> Option<&'a RailRow<T>> {
    this.rail_nav().resolve(rows).map(|index| &rows[index])
}

fn step_row<T: RailOwner>(this: &mut T, rows: &[RailRow<T>], delta: isize) -> RailOutcome {
    let nav = this.rail_nav();
    let Some(index) = nav.resolve(rows) else {
        return RailOutcome::Handled;
    };

    let last = rows.len() as isize - 1;
    let target = (index as isize).saturating_add(delta).clamp(0, last) as usize;
    nav.place(rows, target);
    if target != index {
        nav.field = 0;
    }

    RailOutcome::Handled
}

fn step_field<T: RailOwner>(this: &mut T, rows: &[RailRow<T>], delta: isize) -> RailOutcome {
    let nav = this.rail_nav();
    let Some(index) = nav.resolve(rows) else {
        return RailOutcome::Handled;
    };
    let row = &rows[index];
    let Some(field) = nav.field_index(row) else {
        return RailOutcome::Handled;
    };

    let last = row.fields.len() as isize - 1;
    nav.field = (field as isize + delta).clamp(0, last) as usize;
    nav.place(rows, index);

    RailOutcome::Handled
}

/// After an add, puts the cursor on the last row that was not there
/// before: the entry just added (an add that also creates its group, such as
/// the first filter, lists the group first).
fn follow_new_row<T: RailOwner>(this: &mut T, before: &[SharedString], cx: &mut Context<T>) {
    let rows = this.rail_rows(cx);
    let Some(index) = rows.iter().rposition(|row| !before.contains(&row.id)) else {
        return;
    };

    let nav = this.rail_nav();
    nav.place(&rows, index);
    nav.field = 0;
}

fn activate<T: RailOwner>(
    this: &mut T,
    target: &RailTarget<T>,
    window: &mut Window,
    cx: &mut Context<T>,
) {
    match target {
        RailTarget::Text(state) => {
            let handle = state.focus_handle(cx);
            handle.focus(window, cx);
        }
        RailTarget::Dropdown(dropdown) => {
            dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
        }
        RailTarget::Run(handler) => handler(this, window, cx),
    }
    cx.notify();
}

fn open_menu<T: RailOwner>(this: &mut T, rows: &[RailRow<T>], cx: &mut Context<T>) {
    let mut entries = Vec::new();

    if let Some(row) = current_row(this, rows) {
        if let Some(target) = row.add.clone() {
            entries.push(
                RailMenuEntry::new("rail-add", dbflux_i18n::t!("composites.rail.add"), target)
                    .shortcut(this.rail_shortcut(Command::AddItem)),
            );
        }
        if let Some(target) = row.add_group.clone() {
            entries.push(
                RailMenuEntry::new(
                    "rail-add-group",
                    dbflux_i18n::t!("composites.rail.add_group"),
                    target,
                )
                .shortcut(this.rail_shortcut(Command::AddGroup)),
            );
        }
        if let Some(toggle) = row.toggle.clone() {
            entries.push(
                RailMenuEntry::new(
                    "rail-toggle",
                    dbflux_i18n::t!("composites.rail.toggle"),
                    RailTarget::Run(toggle),
                )
                .shortcut(this.rail_shortcut(Command::ExpandCollapse)),
            );
        }
        if let Some(remove) = row.remove.clone() {
            entries.push(
                RailMenuEntry::new(
                    "rail-remove",
                    dbflux_i18n::t!("composites.rail.remove"),
                    RailTarget::Run(remove),
                )
                .shortcut(this.rail_shortcut(Command::Delete)),
            );
        }
    }

    entries.extend(this.rail_actions(cx));

    let Some(selected) = entries.iter().position(|entry| entry.enabled) else {
        return;
    };

    this.rail_nav().menu = Some(RailMenu { entries, selected });
}

fn menu_command<T: RailOwner>(
    this: &mut T,
    command: Command,
    window: &mut Window,
    cx: &mut Context<T>,
) {
    match command {
        Command::MenuDown | Command::SelectNext => step_menu(this, true),
        Command::MenuUp | Command::SelectPrev => step_menu(this, false),
        Command::MenuSelect | Command::Execute => {
            let selected = this.rail_nav().menu.as_ref().map(|menu| menu.selected);
            if let Some(index) = selected {
                run_menu_entry(this, index, window, cx);
            }
        }
        Command::MenuBack
        | Command::Cancel
        | Command::OpenPaneActions
        | Command::OpenContextMenu => {
            this.rail_nav().menu = None;
        }
        _ => {}
    }
    cx.notify();
}

fn step_menu<T: RailOwner>(this: &mut T, forward: bool) {
    let Some(menu) = this.rail_nav().menu.as_mut() else {
        return;
    };

    let next = if forward {
        (menu.selected + 1..menu.entries.len()).find(|index| menu.entries[*index].enabled)
    } else {
        (0..menu.selected)
            .rev()
            .find(|index| menu.entries[*index].enabled)
    };

    if let Some(index) = next {
        menu.selected = index;
    }
}

/// Closes the menu and runs its entry at `index`, with the keyboard back on
/// the rail first so the entry can move it on (into a field or a list).
fn run_menu_entry<T: RailOwner>(
    this: &mut T,
    index: usize,
    window: &mut Window,
    cx: &mut Context<T>,
) {
    let Some(menu) = this.rail_nav().menu.take() else {
        return;
    };
    let Some(entry) = menu
        .entries
        .into_iter()
        .nth(index)
        .filter(|entry| entry.enabled)
    else {
        return;
    };

    this.rail_focus_handle(cx).focus(window, cx);
    activate(this, &entry.target, window, cx);
}

/// The open action menu, drawn over the top right of the rail's rows.
pub fn render_rail_menu<T: RailOwner>(
    nav: &RailNav<T>,
    id: impl Into<ElementId>,
    cx: &mut Context<T>,
) -> Option<AnyElement> {
    let menu = nav.menu.as_ref()?;

    let rows: Vec<AnyElement> = menu
        .entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let mut item = MenuItem::new(entry.label.clone());
            if let Some(shortcut) = entry.shortcut.clone() {
                item = item.shortcut(shortcut);
            }
            if !entry.enabled {
                item = item.disabled();
            }

            let row = menu_row(
                SharedString::from(format!("rail-action-{}", entry.id)),
                &item,
                index == menu.selected,
                cx,
            );
            if !entry.enabled {
                return row.into_any_element();
            }

            row.on_mouse_move(cx.listener(move |this: &mut T, _, _, cx| {
                if let Some(menu) = this.rail_nav().menu.as_mut()
                    && menu.selected != index
                {
                    menu.selected = index;
                    cx.notify();
                }
            }))
            .on_click(cx.listener(move |this: &mut T, _, window, cx| {
                run_menu_entry(this, index, window, cx);
                cx.notify();
            }))
            .into_any_element()
        })
        .collect();

    Some(
        deferred(
            div()
                .absolute()
                .top(BuilderMetrics::HEADER_HEIGHT)
                .right(BuilderMetrics::RAIL_PADDING_X)
                .child(
                    render_menu_container(rows, cx)
                        .id(id)
                        .occlude()
                        .on_mouse_down_out(cx.listener(|this: &mut T, _, _, cx| {
                            this.rail_nav().menu = None;
                            cx.notify();
                        })),
                ),
        )
        .with_priority(3)
        .into_any_element(),
    )
}
