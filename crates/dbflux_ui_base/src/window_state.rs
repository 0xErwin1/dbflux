//! Placement and persistence of the main window geometry.
//!
//! Where the main window opens is decided by two things: the geometry it had
//! when it was last closed, and, when nothing usable was saved, a preferred
//! size that fits the display. Both go through [`resolve_window_bounds`], which
//! keeps the window inside the work area of an attached display.
//!
//! That last part is not cosmetic. The bounds a window reports describe its
//! client area, while the frame is drawn outside it, so a window flush against
//! the top of a display keeps its title bar above the screen edge, and one as
//! tall as the display loses its bottom edge to the taskbar. Keeping the screen
//! margin around the window is what leaves the frame somewhere the user can
//! reach.
//!
//! The geometry lives in the `st_ui_state` key-value table under
//! [`MAIN_WINDOW_GEOMETRY_KEY`], encoded as JSON. That table is the
//! consolidated home for UI layout state, so window geometry needs neither a
//! table nor a migration of its own.

use gpui::{App, Bounds, DisplayId, Pixels, Point, Size, Window, WindowBounds, point, px, size};
use log::{error, warn};
use serde::{Deserialize, Serialize};

use dbflux_storage::{StorageRuntime, UiStateRepository};

use crate::platform::{fit_window_size, inset_by_screen_margin};

/// Key under which the main window geometry is stored in `st_ui_state`.
pub const MAIN_WINDOW_GEOMETRY_KEY: &str = "main_window_geometry";

/// Preferred size of the main window when nothing has been saved yet.
///
/// It matches the size the main window used to inherit from gpui's default
/// placement, so an existing installation does not see its window change size
/// on upgrade. Unlike that placement it is clamped to the work area rather than
/// to the full display.
const MAIN_WINDOW_DEFAULT_WIDTH: f32 = 1536.0;
const MAIN_WINDOW_DEFAULT_HEIGHT: f32 = 1095.0;

/// Geometry of a window in the logical pixels gpui reports.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WindowGeometry {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
    /// Whether the window was maximized. The origin and size still describe the
    /// window it restores down to.
    #[serde(default)]
    pub maximized: bool,
}

impl WindowGeometry {
    /// Reads the geometry of an open window.
    ///
    /// The maximized state comes from the window itself rather than from its
    /// bounds: macOS reports a maximized window as a plain one, so the bounds
    /// alone would lose the state.
    ///
    /// The size a maximized window restores down to is only reported by
    /// Windows. On X11 the bounds describe the maximized window, so
    /// unmaximizing after a restart gives a window fitted to the work area
    /// instead of the size it had before, and no public API exposes the size
    /// the window manager remembers.
    pub fn from_window(window: &Window) -> Self {
        let mut geometry = Self::from_window_bounds(window.window_bounds());
        geometry.maximized = window.is_maximized();

        geometry
    }

    /// Reads the geometry out of `bounds`, without the maximized state, which
    /// the bounds do not always carry (see [`WindowGeometry::from_window`]). A
    /// window closed while fullscreen reports the screen's bounds instead of its
    /// own, and is restored as a plain window of that size.
    pub fn from_window_bounds(bounds: WindowBounds) -> Self {
        let (rect, maximized) = match bounds {
            WindowBounds::Windowed(rect) => (rect, false),
            WindowBounds::Maximized(rect) => (rect, true),
            WindowBounds::Fullscreen(rect) => (rect, false),
        };

        Self {
            x: rect.origin.x.into(),
            y: rect.origin.y.into(),
            width: rect.size.width.into(),
            height: rect.size.height.into(),
            maximized,
        }
    }

    /// These values as window bounds, placed inside `work_area`.
    fn placed_in(self, work_area: Bounds<Pixels>) -> WindowBounds {
        let rect = self.clamped_to(work_area);

        if self.maximized {
            WindowBounds::Maximized(rect)
        } else {
            WindowBounds::Windowed(rect)
        }
    }

    /// The window these values describe, shrunk and moved so it fits inside
    /// `work_area` with the screen margin to spare.
    fn clamped_to(self, work_area: Bounds<Pixels>) -> Bounds<Pixels> {
        let fitted = fit_window_size(size(px(self.width), px(self.height)), work_area.size);
        let inset = inset_by_screen_margin(work_area);
        let origin = clamp_origin(point(px(self.x), px(self.y)), fitted, inset);

        Bounds::new(origin, fitted)
    }

    /// Whether these values describe a window at all.
    ///
    /// A value written by a different version, or by a run interrupted
    /// mid-write, can be unusable. Placement falls back to the default size
    /// rather than propagating it.
    fn is_usable(&self) -> bool {
        let finite = self.x.is_finite()
            && self.y.is_finite()
            && self.width.is_finite()
            && self.height.is_finite();

        finite && self.width > 0.0 && self.height > 0.0
    }
}

/// Where to open the main window: the display it belongs to, and the bounds
/// inside that display.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MainWindowPlacement {
    /// The display the window should open on. The windowing platform decides
    /// when this is `None`.
    pub display_id: Option<DisplayId>,
    /// `None` leaves the placement to the windowing platform.
    pub bounds: Option<WindowBounds>,
}

impl MainWindowPlacement {
    fn unplaced() -> Self {
        Self {
            display_id: None,
            bounds: None,
        }
    }
}

/// Window bounds to open the main window with, and the display they belong to.
///
/// `work_areas` is the visible bounds of every attached display, the primary
/// first and never empty. `None` means no display is known, which leaves the
/// placement to the windowing platform.
///
/// Saved geometry is restored onto the display the window was on. That display
/// is the one containing the window's center, or, when the center falls outside
/// every display, the one the window overlaps most. When neither identifies a
/// display the window was on, its placement comes from the primary display.
pub fn resolve_window_placement(
    saved: Option<WindowGeometry>,
    work_areas: &[Bounds<Pixels>],
) -> Option<(usize, WindowBounds)> {
    let primary = *work_areas.first()?;

    let Some(saved) = saved.filter(|geometry| geometry.is_usable()) else {
        let preferred = size(
            px(MAIN_WINDOW_DEFAULT_WIDTH),
            px(MAIN_WINDOW_DEFAULT_HEIGHT),
        );
        let fitted = fit_window_size(preferred, primary.size);
        let centered = Bounds::centered_at(primary.center(), fitted);

        return Some((0, WindowBounds::Windowed(centered)));
    };

    let host = host_work_area_index(saved, work_areas).unwrap_or(0);

    Some((host, saved.placed_in(work_areas[host])))
}

/// Placement for the main window, reading the attached displays from `cx`.
///
/// The display travels with the bounds because a window whose bounds are not on
/// the display the platform would have picked is moved to the platform's own
/// default placement instead. Restoring onto a second display needs both.
///
/// On Wayland the compositor owns window placement and does not report where a
/// window ended up, so there the saved origin is the last position the app was
/// told about, and the display identified from it is a request rather than a
/// guarantee.
pub fn resolve_main_window_placement(
    saved: Option<WindowGeometry>,
    cx: &App,
) -> MainWindowPlacement {
    let displays = ordered_displays(cx);
    let work_areas: Vec<Bounds<Pixels>> =
        displays.iter().map(|(_, work_area)| *work_area).collect();

    match resolve_window_placement(saved, &work_areas) {
        Some((host, bounds)) => MainWindowPlacement {
            display_id: displays.get(host).map(|(id, _)| *id),
            bounds: Some(bounds),
        },
        None => MainWindowPlacement::unplaced(),
    }
}

/// Reads the stored main window geometry, or `None` when nothing usable is
/// stored.
pub fn load(storage: &StorageRuntime) -> Option<WindowGeometry> {
    load_from(&storage.ui_state())
}

/// Stores `geometry` as the main window's last known geometry.
pub fn save(storage: &StorageRuntime, geometry: WindowGeometry) {
    save_to(&storage.ui_state(), geometry);
}

/// Attached displays, the primary first, paired with the work area of each.
fn ordered_displays(cx: &App) -> Vec<(DisplayId, Bounds<Pixels>)> {
    let mut displays: Vec<(DisplayId, Bounds<Pixels>)> = cx
        .displays()
        .iter()
        .map(|display| (display.id(), display.visible_bounds()))
        .collect();

    if let Some(primary_id) = cx.primary_display().map(|display| display.id())
        && let Some(position) = displays.iter().position(|(id, _)| *id == primary_id)
        && position != 0
    {
        displays.swap(0, position);
    }

    displays
}

/// The display `saved` was last on, as an index into `work_areas`.
fn host_work_area_index(saved: WindowGeometry, work_areas: &[Bounds<Pixels>]) -> Option<usize> {
    let rect = Bounds::new(
        point(px(saved.x), px(saved.y)),
        size(px(saved.width), px(saved.height)),
    );

    if let Some(index) = work_areas
        .iter()
        .position(|area| area.contains(&rect.center()))
    {
        return Some(index);
    }

    work_areas
        .iter()
        .enumerate()
        .map(|(index, area)| (overlap_area(rect, *area), index))
        .filter(|(overlap, _)| *overlap > 0.0)
        .max_by(|(left, _), (right, _)| left.total_cmp(right))
        .map(|(_, index)| index)
}

/// Area shared by `left` and `right`, zero when they do not overlap.
fn overlap_area(left: Bounds<Pixels>, right: Bounds<Pixels>) -> f32 {
    let width: f32 = (left.right().min(right.right()) - left.left().max(right.left())).into();
    let height: f32 = (left.bottom().min(right.bottom()) - left.top().max(right.top())).into();

    (width.max(0.0)) * (height.max(0.0))
}

/// Moves `origin` so a window of `window` fits inside `available`, keeping it
/// as close to where it was as the available space allows.
fn clamp_origin(
    origin: Point<Pixels>,
    window: Size<Pixels>,
    available: Bounds<Pixels>,
) -> Point<Pixels> {
    let left: f32 = available.left().into();
    let top: f32 = available.top().into();
    let right: f32 = available.right().into();
    let bottom: f32 = available.bottom().into();

    let x: f32 = origin.x.into();
    let y: f32 = origin.y.into();
    let width: f32 = window.width.into();
    let height: f32 = window.height.into();

    // A window wider than the available space has nothing to align to. Pinning
    // it to the leading edge keeps it visible instead of letting the clamp
    // bounds invert.
    let max_x = (right - width).max(left);
    let max_y = (bottom - height).max(top);

    point(px(x.clamp(left, max_x)), px(y.clamp(top, max_y)))
}

fn load_from(storage: &UiStateRepository) -> Option<WindowGeometry> {
    let stored = match storage.get(MAIN_WINDOW_GEOMETRY_KEY) {
        Ok(stored) => stored,
        Err(source) => {
            warn!("Could not read the saved window geometry: {source}");
            return None;
        }
    }?;

    match serde_json::from_str::<WindowGeometry>(&stored) {
        Ok(geometry) => Some(geometry),
        Err(source) => {
            warn!("Ignoring the saved window geometry, it could not be decoded: {source}");
            None
        }
    }
}

fn save_to(storage: &UiStateRepository, geometry: WindowGeometry) {
    let encoded = match serde_json::to_string(&geometry) {
        Ok(encoded) => encoded,
        Err(source) => {
            error!("Could not encode the window geometry: {source}");
            return;
        }
    };

    if let Err(source) = storage.set(MAIN_WINDOW_GEOMETRY_KEY, &encoded) {
        error!("Could not persist the window geometry: {source}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::WINDOW_SCREEN_MARGIN;

    /// A 1920x1080 display at the origin.
    fn primary_work_area() -> Bounds<Pixels> {
        Bounds::new(point(px(0.0), px(0.0)), size(px(1920.0), px(1080.0)))
    }

    /// A second 1920x1080 display to the right of the primary, with a gap.
    fn secondary_work_area() -> Bounds<Pixels> {
        Bounds::new(point(px(2500.0), px(0.0)), size(px(1920.0), px(1080.0)))
    }

    fn saved(x: f32, y: f32, width: f32, height: f32) -> WindowGeometry {
        WindowGeometry {
            x,
            y,
            width,
            height,
            maximized: false,
        }
    }

    /// The bounds of the resolved placement, dropping the display it is on.
    fn placed(
        saved: Option<WindowGeometry>,
        work_areas: &[Bounds<Pixels>],
    ) -> Option<WindowBounds> {
        resolve_window_placement(saved, work_areas).map(|(_, bounds)| bounds)
    }

    /// The display a placement resolves to, as an index into `work_areas`.
    fn placed_on_display(
        saved: Option<WindowGeometry>,
        work_areas: &[Bounds<Pixels>],
    ) -> Option<usize> {
        resolve_window_placement(saved, work_areas).map(|(host, _)| host)
    }

    #[test]
    fn a_first_run_centers_the_default_size_in_the_work_area() {
        let resolved = placed(None, &[primary_work_area()]).expect("placement");

        let expected_size = size(px(1536.0), px(1032.0));
        assert_eq!(
            resolved,
            WindowBounds::Windowed(Bounds::centered_at(
                primary_work_area().center(),
                expected_size
            ))
        );
    }

    #[test]
    fn a_first_run_on_a_small_display_keeps_the_window_inside_it() {
        let small = Bounds::new(point(px(0.0), px(0.0)), size(px(1280.0), px(720.0)));

        let resolved = placed(None, &[small]).expect("placement");
        let WindowBounds::Windowed(rect) = resolved else {
            panic!("a window without saved geometry is never maximized");
        };

        assert_eq!(rect.size, size(px(1232.0), px(672.0)));
        assert_eq!(rect.left(), px(WINDOW_SCREEN_MARGIN));
        assert_eq!(rect.top(), px(WINDOW_SCREEN_MARGIN));
    }

    #[test]
    fn a_window_that_fits_is_restored_where_it_was() {
        let resolved = placed(
            Some(saved(100.0, 100.0, 1200.0, 800.0)),
            &[primary_work_area()],
        )
        .expect("placement");

        assert_eq!(
            resolved,
            WindowBounds::Windowed(Bounds::new(
                point(px(100.0), px(100.0)),
                size(px(1200.0), px(800.0))
            ))
        );
    }

    #[test]
    fn a_window_is_restored_onto_the_display_it_was_on() {
        let work_areas = [primary_work_area(), secondary_work_area()];

        let resolved =
            placed(Some(saved(2600.0, 100.0, 1200.0, 800.0)), &work_areas).expect("placement");

        assert_eq!(
            placed_on_display(Some(saved(2600.0, 100.0, 1200.0, 800.0)), &work_areas),
            Some(1),
            "a window saved on the second display belongs to it"
        );
        assert_eq!(
            resolved,
            WindowBounds::Windowed(Bounds::new(
                point(px(2600.0), px(100.0)),
                size(px(1200.0), px(800.0))
            ))
        );
    }

    // The window straddles the gap between the two displays, so only the
    // overlap identifies the display it belongs to.
    #[test]
    fn a_window_between_displays_keeps_the_one_it_overlaps_most() {
        let work_areas = [primary_work_area(), secondary_work_area()];

        let resolved =
            placed(Some(saved(1800.0, 100.0, 1200.0, 800.0)), &work_areas).expect("placement");
        let WindowBounds::Windowed(rect) = resolved else {
            panic!("a window that is not maximized restores as a window");
        };

        assert_eq!(
            placed_on_display(Some(saved(1800.0, 100.0, 1200.0, 800.0)), &work_areas),
            Some(1)
        );
        assert_eq!(rect.left(), px(2524.0));
        assert_eq!(rect.size, size(px(1200.0), px(800.0)));
    }

    #[test]
    fn a_window_whose_display_is_gone_moves_onto_the_primary_one() {
        let resolved = placed(
            Some(saved(5000.0, 100.0, 1200.0, 800.0)),
            &[primary_work_area()],
        )
        .expect("placement");

        assert_eq!(
            placed_on_display(
                Some(saved(5000.0, 100.0, 1200.0, 800.0)),
                &[primary_work_area()]
            ),
            Some(0),
            "a window with nowhere to go lands on the primary display"
        );
        assert_eq!(
            resolved,
            WindowBounds::Windowed(Bounds::new(
                point(px(696.0), px(100.0)),
                size(px(1200.0), px(800.0))
            ))
        );
    }

    // The reported symptom: the window is as tall as the display and flush
    // against its top edge, so the title bar sits above the screen and the
    // bottom edge sits under the taskbar.
    #[test]
    fn a_window_flush_against_the_top_edge_is_brought_back_into_view() {
        let resolved = placed(
            Some(saved(512.0, 0.0, 1536.0, 1080.0)),
            &[primary_work_area()],
        )
        .expect("placement");
        let WindowBounds::Windowed(rect) = resolved else {
            panic!("a window that is not maximized restores as a window");
        };

        assert_eq!(rect.size, size(px(1536.0), px(1032.0)));
        assert_eq!(rect.left(), px(360.0));
        assert_eq!(rect.top(), px(WINDOW_SCREEN_MARGIN));
    }

    #[test]
    fn a_maximized_window_restores_maximized_with_the_size_it_restores_down_to() {
        let mut geometry = saved(100.0, 100.0, 1200.0, 800.0);
        geometry.maximized = true;

        let resolved = placed(Some(geometry), &[primary_work_area()]).expect("placement");

        assert_eq!(
            resolved,
            WindowBounds::Maximized(Bounds::new(
                point(px(100.0), px(100.0)),
                size(px(1200.0), px(800.0))
            ))
        );
    }

    #[test]
    fn unusable_geometry_falls_back_to_the_default_size() {
        let defaults = placed(None, &[primary_work_area()]);

        assert_eq!(
            placed(
                Some(saved(f32::NAN, 0.0, 100.0, 100.0)),
                &[primary_work_area()]
            ),
            defaults
        );
        assert_eq!(
            placed(Some(saved(0.0, 0.0, 0.0, 100.0)), &[primary_work_area()]),
            defaults
        );
    }

    #[test]
    fn without_a_known_display_the_placement_is_left_to_the_platform() {
        assert_eq!(resolve_window_placement(None, &[]), None);
        assert_eq!(
            resolve_window_placement(Some(saved(10.0, 10.0, 100.0, 100.0)), &[]),
            None
        );
    }

    #[test]
    fn geometry_round_trips_through_window_bounds() {
        let rect = Bounds::new(point(px(10.0), px(20.0)), size(px(300.0), px(400.0)));
        let expected = saved(10.0, 20.0, 300.0, 400.0);

        assert_eq!(
            WindowGeometry::from_window_bounds(WindowBounds::Windowed(rect)),
            expected
        );
        assert_eq!(
            WindowGeometry::from_window_bounds(WindowBounds::Maximized(rect)),
            WindowGeometry {
                maximized: true,
                ..expected
            }
        );
        assert_eq!(
            WindowGeometry::from_window_bounds(WindowBounds::Fullscreen(rect)),
            expected
        );
    }

    mod stored {
        use super::*;
        use dbflux_storage::migrations::MigrationRegistry;
        use dbflux_storage::sqlite::open_database;
        use std::sync::Arc;

        fn migrated_storage(directory: &tempfile::TempDir) -> UiStateRepository {
            let conn = open_database(&directory.path().join("dbflux.db")).expect("should open");
            MigrationRegistry::new()
                .run_all(&conn)
                .expect("migrations should run");

            #[allow(clippy::arc_with_non_send_sync)]
            UiStateRepository::new(Arc::new(conn))
        }

        #[test]
        fn nothing_saved_reads_back_as_nothing() {
            let directory = tempfile::tempdir().expect("temp dir");
            let storage = migrated_storage(&directory);

            assert_eq!(load_from(&storage), None);
        }

        #[test]
        fn saved_geometry_reads_back_unchanged() {
            let directory = tempfile::tempdir().expect("temp dir");
            let storage = migrated_storage(&directory);
            let geometry = saved(120.5, 80.25, 1440.0, 900.0);

            save_to(&storage, geometry);

            assert_eq!(load_from(&storage), Some(geometry));
        }

        #[test]
        fn saving_twice_keeps_the_last_geometry() {
            let directory = tempfile::tempdir().expect("temp dir");
            let storage = migrated_storage(&directory);

            save_to(&storage, saved(0.0, 0.0, 800.0, 600.0));
            save_to(&storage, saved(10.0, 20.0, 1024.0, 768.0));

            assert_eq!(load_from(&storage), Some(saved(10.0, 20.0, 1024.0, 768.0)));
        }

        #[test]
        fn an_undecodable_value_reads_back_as_nothing() {
            let directory = tempfile::tempdir().expect("temp dir");
            let storage = migrated_storage(&directory);

            storage
                .set(MAIN_WINDOW_GEOMETRY_KEY, "not geometry")
                .expect("store");
            assert_eq!(load_from(&storage), None);

            storage
                .set(MAIN_WINDOW_GEOMETRY_KEY, r#"{"x": 1.0, "y": 2.0}"#)
                .expect("store");
            assert_eq!(load_from(&storage), None);
        }
    }
}
