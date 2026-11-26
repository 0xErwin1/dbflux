//! Element ids of the plan views, recorded as debug selectors too so tests
//! and UI automation address an element by the id it carries.

use bolt::selector::id_selector;
use gpui::{Div, ElementId, InteractiveElement as _, Stateful, div};

/// `element` under `id`, with the id as its debug selector.
pub(super) fn tag(element: Div, id: impl Into<ElementId>) -> Stateful<Div> {
    let id = id.into();
    element.debug_selector(id_selector(&id)).id(id)
}

/// A new `div` under `id`, with the id as its debug selector.
pub(super) fn tagged(id: impl Into<ElementId>) -> Stateful<Div> {
    tag(div(), id)
}
