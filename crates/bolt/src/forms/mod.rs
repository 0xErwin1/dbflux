//! The form seam: how a two-dimensional form's fields are navigated.
//!
//! [`form_navigation`] holds the `FormNavigation` contract: the edit state a
//! form host moves between, the per-field facts the navigation reads, and the
//! command-driven movement over the host's own field rows. The host supplies
//! the row layout — which field sits left, right, up or down of which — and
//! maps its keymap vocabulary onto the navigation intents through
//! [`FormCommand`].

pub mod form_navigation;

pub use form_navigation::{
    FormCommand, FormEditState, FormField, FormNav, FormNavigation, subscribe_form_input,
};
