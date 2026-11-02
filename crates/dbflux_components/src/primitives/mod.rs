// The generic primitives moved to `bolt`; re-exported at these paths so call
// sites keep compiling. The modules that read a `dbflux_core` type or
// interface copy stay here as thin extensions over the generic primitives.

mod badge;
mod banner;
mod file_picker;

pub use badge::{
    Badge, BadgeTone, EnvTag, environment_badge_tone, environment_label, environment_short_label,
};
pub use banner::{BannerBlock, BannerVariant};
pub use bolt::primitives::{
    Chamfer, ChamferColors, ChamferCorners, ChamferEdge, ChamferFillKind, ChamferRing, DividerTone,
    FOCUS_MARKER_SELECTOR, FOCUS_RING_SELECTOR, FocusShape, Icon, Kbd, KbdTone, Label,
    LoadingState, SegmentedControl, SegmentedItem, Spinner, Status, StatusIndicator,
    SurfaceInspection, SurfaceRole, Text, TextColorSelection, TextDefaultColor, TextInspection,
    TextRoleContract, TextVariant, WhenFocusVisible, chamfer_border_polygons,
    chamfer_bottom_edge_polygon, chamfer_left_edge_polygon, chamfer_outside_polygons,
    chamfer_points, chamfer_ring_points, chamfer_top_edge_polygon, clamp_cut, divider, focus_ring,
    focus_underline, focus_visible, format_latency, hdivider, inspect_surface_role,
    is_focus_visible, is_keyboard_modality, key_label, motion_ease, new_active_id, overlay_bg,
    segmented_group_id, segmented_item_id, snap_bounds_to_device, snap_length_to_device,
    status_diamond, stepped_segment, surface, transition_color, vdivider,
};
pub use file_picker::{FilePicker, file_picker_label};
// The confirmation input moved to `bolt` with the controls it is built on
// (`controls::Input`); re-exported at its old primitives path.
pub use bolt::controls::{TypeToConfirm, TypeToConfirmEvent};
