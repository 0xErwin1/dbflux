#![cfg(any(target_os = "linux", target_os = "freebsd"))]
mod linux;

pub use linux::vulkan_driver::{
    FALLBACK_VALUE, OPT_OUT_VALUE, SELECTION_VARIABLE, restore_child_environment,
    retry_after_renderer_failure, select_vulkan_driver,
};
pub use linux::{current_platform, linux_platform};
