//! The modal dialog and the body blocks dialogs share.

pub mod modal;
pub mod parts;

pub use modal::{
    AttachModalKeys, MODAL_KEY_CONTEXT, Modal, ModalFocus, ModalKey, ModalKeyHandler, ModalSeams,
    ModalVariant, ScrollKey, on_modal_key,
};
pub use parts::{
    inline_code, modal_code, modal_field, modal_form_row, modal_frame, modal_hint, modal_lead,
    modal_value_field,
};
