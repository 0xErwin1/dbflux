pub mod common;
pub mod components;
pub mod dock;
pub mod document;
pub mod icons;
pub mod overlays;
pub mod theme;
pub mod tokens;
pub mod views;
pub mod windows;

#[cfg(test)]
mod design_system_guardrails;

// AsyncUpdateResultExt now lives in dbflux_ui_base; re-export at this path so
// all ~9 existing `use crate::ui::AsyncUpdateResultExt` call-sites are unchanged.
pub(crate) use dbflux_ui_base::AsyncUpdateResultExt;
