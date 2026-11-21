//! The picture that follows the pointer while files are dragged out of a window.
//!
//! The compositor draws the icon surface of a drag wherever the pointer goes, also outside the
//! window, so the picture is rendered here on the CPU: a page or folder glyph, with a count
//! badge when several entries are dragged. [`IconSpec`] decides what to draw and [`render`]
//! draws it; neither touches the display server.

use gpui::FileDragPaths;

/// Size of the icon surface in logical pixels.
pub(crate) const ICON_WIDTH: u32 = 76;
pub(crate) const ICON_HEIGHT: u32 = 84;

/// The largest count shown in the badge; more entries show `99+`.
const MAX_BADGE_COUNT: usize = 99;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Glyph {
    File,
    Folder,
}

/// What the dragged entries look like.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IconSpec {
    pub glyph: Glyph,
    pub count: usize,
}

impl IconSpec {
    /// `None` when nothing is dragged. A single entry shows its own kind; several show a folder
    /// only when every one is a folder.
    pub(crate) fn for_paths(paths: &FileDragPaths) -> Option<Self> {
        let entries = paths.entries();
        if entries.is_empty() {
            return None;
        }
        let all_folders = entries.iter().all(|(_, is_directory)| *is_directory);
        Some(Self {
            glyph: if all_folders {
                Glyph::Folder
            } else {
                Glyph::File
            },
            count: entries.len(),
        })
    }

    /// The text of the count badge, `None` for a single entry.
    pub(crate) fn badge_text(&self) -> Option<String> {
        match self.count {
            0 | 1 => None,
            count if count > MAX_BADGE_COUNT => Some(format!("{MAX_BADGE_COUNT}+")),
            count => Some(count.to_string()),
        }
    }
}

/// A rendered icon in the `wl_shm` `Argb8888` format: premultiplied, one little-endian `u32`
/// per pixel, `width * 4` bytes per row.
pub(crate) struct IconBitmap {
    pub width: u32,
    pub height: u32,
    pub scale: u32,
    pub argb: Vec<u8>,
}

/// Premultiplied RGBA.
type Color = [f32; 4];

const OUTLINE: Color = [0.17 * 0.92, 0.18 * 0.92, 0.20 * 0.92, 0.92];
const PAGE: Color = [0.98, 0.98, 0.99, 1.0];
const PAGE_FOLD: Color = [0.80, 0.82, 0.87, 1.0];
const PAGE_LINE: Color = [0.70, 0.72, 0.78, 1.0];
const FOLDER: Color = [0.96, 0.78, 0.34, 1.0];
const FOLDER_SHADE: Color = [0.86, 0.62, 0.18, 1.0];
const ACCENT: Color = [0.23, 0.51, 0.96, 1.0];
const WHITE: Color = [1.0, 1.0, 1.0, 1.0];

/// Thickness of the outline around every shape, in logical pixels.
const OUTLINE_WIDTH: f32 = 1.2;
const PAGE_WIDTH: f32 = 44.0;
const PAGE_HEIGHT: f32 = 54.0;
const PAGE_FOLD_SIZE: f32 = 12.0;
const FOLDER_WIDTH: f32 = 52.0;
const FOLDER_HEIGHT: f32 = 42.0;
/// Top-left corner of the front glyph; the pointer is at the icon's top-left corner, so the
/// glyph hangs below and to the right of it.
const FRONT_ORIGIN: (f32, f32) = (10.0, 24.0);
/// How far the second glyph of a stack peeks out behind the front one.
const STACK_OFFSET: (f32, f32) = (10.0, -10.0);
const BADGE_RIGHT: f32 = 72.0;
const BADGE_TOP: f32 = 2.0;
const BADGE_HEIGHT: f32 = 22.0;
const BADGE_PADDING: f32 = 7.0;
/// Size in logical pixels of one pixel of the badge font.
const FONT_PIXEL: f32 = 2.0;
const FONT_COLUMNS: usize = 5;
const FONT_ROWS: usize = 7;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Bounds {
    x0: f32,
    y0: f32,
    x1: f32,
    y1: f32,
}

impl Bounds {
    fn new(x: f32, y: f32, width: f32, height: f32) -> Self {
        Self {
            x0: x,
            y0: y,
            x1: x + width,
            y1: y + height,
        }
    }

    fn grown(self, amount: f32) -> Self {
        Self {
            x0: self.x0 - amount,
            y0: self.y0 - amount,
            x1: self.x1 + amount,
            y1: self.y1 + amount,
        }
    }
}

/// The pill behind a badge of `characters` characters.
fn badge_bounds(characters: usize) -> Bounds {
    let text_width = characters as f32 * (FONT_COLUMNS as f32 + 1.0) * FONT_PIXEL - FONT_PIXEL;
    let width = text_width + 2.0 * BADGE_PADDING;
    Bounds::new(BADGE_RIGHT - width, BADGE_TOP, width, BADGE_HEIGHT)
}

/// The rows of a badge character, five bits each with the leftmost column in bit 4.
fn digit_rows(character: char) -> Option<[u8; FONT_ROWS]> {
    Some(match character {
        '0' => [
            0b01110, 0b10001, 0b10011, 0b10101, 0b11001, 0b10001, 0b01110,
        ],
        '1' => [
            0b00100, 0b01100, 0b00100, 0b00100, 0b00100, 0b00100, 0b01110,
        ],
        '2' => [
            0b01110, 0b10001, 0b00001, 0b00010, 0b00100, 0b01000, 0b11111,
        ],
        '3' => [
            0b11110, 0b00001, 0b00001, 0b01110, 0b00001, 0b00001, 0b11110,
        ],
        '4' => [
            0b00010, 0b00110, 0b01010, 0b10010, 0b11111, 0b00010, 0b00010,
        ],
        '5' => [
            0b11111, 0b10000, 0b11110, 0b00001, 0b00001, 0b10001, 0b01110,
        ],
        '6' => [
            0b00110, 0b01000, 0b10000, 0b11110, 0b10001, 0b10001, 0b01110,
        ],
        '7' => [
            0b11111, 0b00001, 0b00010, 0b00100, 0b01000, 0b01000, 0b01000,
        ],
        '8' => [
            0b01110, 0b10001, 0b10001, 0b01110, 0b10001, 0b10001, 0b01110,
        ],
        '9' => [
            0b01110, 0b10001, 0b10001, 0b01111, 0b00001, 0b00010, 0b01100,
        ],
        '+' => [
            0b00000, 0b00100, 0b00100, 0b11111, 0b00100, 0b00100, 0b00000,
        ],
        _ => return None,
    })
}

struct Canvas {
    width: usize,
    height: usize,
    scale: usize,
    pixels: Vec<Color>,
}

impl Canvas {
    fn new(scale: usize) -> Self {
        let width = ICON_WIDTH as usize * scale;
        let height = ICON_HEIGHT as usize * scale;
        Self {
            width,
            height,
            scale,
            pixels: vec![[0.0; 4]; width * height],
        }
    }

    /// Paints `color` where `inside` is true within `bounds`, anti-aliased by sampling each
    /// device pixel 3 by 3 times.
    fn fill(&mut self, bounds: Bounds, color: Color, inside: impl Fn(f32, f32) -> bool) {
        const SAMPLES: usize = 3;
        let scale = self.scale as f32;
        let first_column = ((bounds.x0 * scale).floor().max(0.0)) as usize;
        let last_column = ((bounds.x1 * scale).ceil().max(0.0) as usize).min(self.width);
        let first_row = ((bounds.y0 * scale).floor().max(0.0)) as usize;
        let last_row = ((bounds.y1 * scale).ceil().max(0.0) as usize).min(self.height);
        for row in first_row..last_row {
            for column in first_column..last_column {
                let mut hits = 0;
                for sample_row in 0..SAMPLES {
                    for sample_column in 0..SAMPLES {
                        let x =
                            (column as f32 + (sample_column as f32 + 0.5) / SAMPLES as f32) / scale;
                        let y = (row as f32 + (sample_row as f32 + 0.5) / SAMPLES as f32) / scale;
                        hits += usize::from(inside(x, y));
                    }
                }
                if hits == 0 {
                    continue;
                }
                let coverage = hits as f32 / (SAMPLES * SAMPLES) as f32;
                let pixel = &mut self.pixels[row * self.width + column];
                let remaining = 1.0 - color[3] * coverage;
                for channel in 0..4 {
                    pixel[channel] = color[channel] * coverage + pixel[channel] * remaining;
                }
            }
        }
    }

    /// Paints a shape with an outline, made by painting the shape in the outline colour shifted
    /// a little in every direction and then painting it in place.
    fn fill_outlined(
        &mut self,
        bounds: Bounds,
        outline: Color,
        color: Color,
        inside: impl Fn(f32, f32) -> bool,
    ) {
        const DIRECTIONS: [(f32, f32); 8] = [
            (1.0, 0.0),
            (-1.0, 0.0),
            (0.0, 1.0),
            (0.0, -1.0),
            (0.707, 0.707),
            (-0.707, 0.707),
            (0.707, -0.707),
            (-0.707, -0.707),
        ];
        for (dx, dy) in DIRECTIONS {
            let (dx, dy) = (dx * OUTLINE_WIDTH, dy * OUTLINE_WIDTH);
            self.fill(bounds.grown(OUTLINE_WIDTH), outline, |x, y| {
                inside(x - dx, y - dy)
            });
        }
        self.fill(bounds, color, inside);
    }

    fn into_bitmap(self, scale: u32) -> IconBitmap {
        let mut argb = Vec::with_capacity(self.pixels.len() * 4);
        for [red, green, blue, alpha] in self.pixels {
            for channel in [blue, green, red, alpha] {
                argb.push((channel.clamp(0.0, 1.0) * 255.0).round() as u8);
            }
        }
        IconBitmap {
            width: self.width as u32,
            height: self.height as u32,
            scale,
            argb,
        }
    }
}

fn in_rounded_rect(bounds: Bounds, radius: f32) -> impl Fn(f32, f32) -> bool {
    move |x, y| {
        let half_width = (bounds.x1 - bounds.x0) / 2.0;
        let half_height = (bounds.y1 - bounds.y0) / 2.0;
        let dx = ((x - (bounds.x0 + half_width)).abs() - (half_width - radius)).max(0.0);
        let dy = ((y - (bounds.y0 + half_height)).abs() - (half_height - radius)).max(0.0);
        dx * dx + dy * dy <= radius * radius
    }
}

fn draw_page(canvas: &mut Canvas, (x, y): (f32, f32)) {
    let bounds = Bounds::new(x, y, PAGE_WIDTH, PAGE_HEIGHT);
    let rounded = in_rounded_rect(bounds, 4.0);
    // The top-right corner is folded over: cut along the diagonal from the fold's top to its
    // right edge.
    let (fold_x, fold_y) = (bounds.x1 - PAGE_FOLD_SIZE, bounds.y0);
    let page = move |px: f32, py: f32| rounded(px, py) && (px - fold_x) <= (py - fold_y);
    canvas.fill_outlined(bounds, OUTLINE, PAGE, page);

    let flap = Bounds::new(fold_x, fold_y, PAGE_FOLD_SIZE, PAGE_FOLD_SIZE);
    canvas.fill(flap, PAGE_FOLD, move |px, py| {
        let (u, v) = (px - fold_x, py - fold_y);
        (0.0..=PAGE_FOLD_SIZE).contains(&u) && v >= u && v <= PAGE_FOLD_SIZE
    });
    for line in 0..3 {
        let line_bounds = Bounds::new(
            x + 8.0,
            y + 24.0 + line as f32 * 8.0,
            PAGE_WIDTH - 16.0,
            3.0,
        );
        canvas.fill(line_bounds, PAGE_LINE, in_rounded_rect(line_bounds, 1.5));
    }
}

fn draw_folder(canvas: &mut Canvas, (x, y): (f32, f32)) {
    let body = Bounds::new(x, y + 9.0, FOLDER_WIDTH, FOLDER_HEIGHT - 9.0);
    let tab = Bounds::new(x, y, FOLDER_WIDTH * 0.45, 14.0);
    let in_body = in_rounded_rect(body, 5.0);
    let in_tab = in_rounded_rect(tab, 4.0);
    let whole = Bounds {
        x0: x,
        y0: y,
        x1: x + FOLDER_WIDTH,
        y1: y + FOLDER_HEIGHT,
    };
    canvas.fill_outlined(whole, OUTLINE, FOLDER, move |px, py| {
        in_body(px, py) || in_tab(px, py)
    });
    let shade = Bounds::new(x, y + 9.0, FOLDER_WIDTH, 4.0);
    canvas.fill(shade, FOLDER_SHADE, move |px, py| {
        in_rounded_rect(body, 5.0)(px, py) && py < shade.y1
    });
}

fn draw_glyph(canvas: &mut Canvas, glyph: Glyph, origin: (f32, f32)) {
    match glyph {
        Glyph::File => draw_page(canvas, origin),
        Glyph::Folder => draw_folder(canvas, origin),
    }
}

fn draw_badge(canvas: &mut Canvas, text: &str) {
    let pill = badge_bounds(text.chars().count());
    canvas.fill_outlined(
        pill,
        WHITE,
        ACCENT,
        in_rounded_rect(pill, BADGE_HEIGHT / 2.0),
    );
    let text_top = pill.y0 + (BADGE_HEIGHT - FONT_ROWS as f32 * FONT_PIXEL) / 2.0;
    let mut left = pill.x0 + BADGE_PADDING;
    for character in text.chars() {
        if let Some(rows) = digit_rows(character) {
            for (row, bits) in rows.iter().enumerate() {
                for column in 0..FONT_COLUMNS {
                    if bits & (1 << (FONT_COLUMNS - 1 - column)) != 0 {
                        let dot = Bounds::new(
                            left + column as f32 * FONT_PIXEL,
                            text_top + row as f32 * FONT_PIXEL,
                            FONT_PIXEL,
                            FONT_PIXEL,
                        );
                        canvas.fill(dot, WHITE, |_, _| true);
                    }
                }
            }
        }
        left += (FONT_COLUMNS as f32 + 1.0) * FONT_PIXEL;
    }
}

pub(crate) fn render(spec: IconSpec, scale: u32) -> IconBitmap {
    let scale = scale.max(1);
    let mut canvas = Canvas::new(scale as usize);
    if spec.count > 1 {
        draw_glyph(
            &mut canvas,
            spec.glyph,
            (
                FRONT_ORIGIN.0 + STACK_OFFSET.0,
                FRONT_ORIGIN.1 + STACK_OFFSET.1,
            ),
        );
    }
    draw_glyph(&mut canvas, spec.glyph, FRONT_ORIGIN);
    if let Some(text) = spec.badge_text() {
        draw_badge(&mut canvas, &text);
    }
    canvas.into_bitmap(scale)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn paths(entries: &[(&str, bool)]) -> FileDragPaths {
        FileDragPaths::new(
            entries
                .iter()
                .map(|(path, is_directory)| (PathBuf::from(path), *is_directory)),
        )
    }

    fn pixel(bitmap: &IconBitmap, x: u32, y: u32) -> [u8; 4] {
        let offset = ((y * bitmap.width + x) * 4) as usize;
        [
            bitmap.argb[offset],
            bitmap.argb[offset + 1],
            bitmap.argb[offset + 2],
            bitmap.argb[offset + 3],
        ]
    }

    fn file(count: usize) -> IconSpec {
        IconSpec {
            glyph: Glyph::File,
            count,
        }
    }

    #[test]
    fn nothing_dragged_has_no_icon() {
        assert_eq!(IconSpec::for_paths(&FileDragPaths::default()), None);
    }

    #[test]
    fn a_single_entry_shows_its_own_kind() {
        assert_eq!(
            IconSpec::for_paths(&paths(&[("/tmp/a", false)])),
            Some(file(1))
        );
        assert_eq!(
            IconSpec::for_paths(&paths(&[("/tmp/d", true)])),
            Some(IconSpec {
                glyph: Glyph::Folder,
                count: 1
            })
        );
    }

    #[test]
    fn several_entries_show_a_folder_only_when_all_are_folders() {
        assert_eq!(
            IconSpec::for_paths(&paths(&[("/a", true), ("/b", true)])),
            Some(IconSpec {
                glyph: Glyph::Folder,
                count: 2
            })
        );
        assert_eq!(
            IconSpec::for_paths(&paths(&[("/a", true), ("/b", false), ("/c", true)])),
            Some(file(3))
        );
    }

    #[test]
    fn the_badge_shows_the_count_and_caps_it() {
        assert_eq!(file(1).badge_text(), None);
        assert_eq!(file(2).badge_text().as_deref(), Some("2"));
        assert_eq!(file(99).badge_text().as_deref(), Some("99"));
        assert_eq!(file(100).badge_text().as_deref(), Some("99+"));
        assert_eq!(file(5000).badge_text().as_deref(), Some("99+"));
    }

    #[test]
    fn the_bitmap_has_the_icon_size_times_the_scale() {
        let bitmap = render(file(1), 1);
        assert_eq!((bitmap.width, bitmap.height, bitmap.scale), (76, 84, 1));
        assert_eq!(bitmap.argb.len(), 76 * 84 * 4);

        let scaled = render(file(1), 2);
        assert_eq!((scaled.width, scaled.height, scaled.scale), (152, 168, 2));
        assert_eq!(scaled.argb.len(), 152 * 168 * 4);
    }

    #[test]
    fn a_zero_scale_renders_at_one() {
        assert_eq!(render(file(1), 0).width, 76);
    }

    #[test]
    fn the_pointer_corner_is_transparent_and_the_glyph_is_drawn() {
        let bitmap = render(file(1), 1);
        assert_eq!(pixel(&bitmap, 0, 0), [0, 0, 0, 0]);
        let opaque = bitmap
            .argb
            .chunks_exact(4)
            .filter(|px| px[3] == 255)
            .count();
        assert!(opaque > 500, "only {opaque} opaque pixels");
    }

    #[test]
    fn every_pixel_is_premultiplied() {
        for spec in [
            file(1),
            file(7),
            IconSpec {
                glyph: Glyph::Folder,
                count: 120,
            },
        ] {
            let bitmap = render(spec, 2);
            for px in bitmap.argb.chunks_exact(4) {
                assert!(px[0] <= px[3] && px[1] <= px[3] && px[2] <= px[3], "{px:?}");
            }
        }
    }

    #[test]
    fn a_file_and_a_folder_look_different() {
        let file_icon = render(file(1), 1);
        let folder_icon = render(
            IconSpec {
                glyph: Glyph::Folder,
                count: 1,
            },
            1,
        );
        assert_ne!(file_icon.argb, folder_icon.argb);
    }

    #[test]
    fn several_entries_add_a_badge_and_stack_a_second_glyph() {
        let single = render(file(1), 1);
        let several = render(file(12), 1);
        assert_ne!(single.argb, several.argb);
        let opaque = |bitmap: &IconBitmap| {
            bitmap
                .argb
                .chunks_exact(4)
                .filter(|px| px[3] == 255)
                .count()
        };
        assert!(opaque(&several) > opaque(&single));
    }

    #[test]
    fn different_counts_draw_different_badges() {
        assert_ne!(render(file(2), 1).argb, render(file(3), 1).argb);
        assert_ne!(render(file(99), 1).argb, render(file(100), 1).argb);
    }

    #[test]
    fn the_badge_is_the_accent_colour() {
        let bitmap = render(file(2), 1);
        let badge = badge_bounds(1);
        // Left of the digits, inside the rounded end of the pill.
        let (x, y) = (
            (badge.x0 + 3.0) as u32,
            ((badge.y0 + badge.y1) / 2.0) as u32,
        );
        // Premultiplied ARGB, blue-green-red-alpha in memory: the accent is mostly blue.
        let [blue, green, red, alpha] = pixel(&bitmap, x, y);
        assert_eq!(alpha, 255);
        assert!(blue > green && green > red, "{blue} {green} {red}");
    }

    #[test]
    fn the_badge_grows_with_its_text_and_stays_inside_the_icon() {
        for length in 1..=3 {
            let badge = badge_bounds(length);
            assert!(badge.x0 >= 0.0 && badge.x1 <= ICON_WIDTH as f32);
            assert!(badge.y0 >= 0.0 && badge.y1 <= ICON_HEIGHT as f32);
        }
        assert!(badge_bounds(3).x0 < badge_bounds(1).x0);
    }

    #[test]
    fn every_badge_character_has_a_glyph() {
        for character in "0123456789+".chars() {
            let rows = digit_rows(character).expect("a glyph");
            assert!(rows.iter().any(|row| *row != 0), "{character}");
        }
        assert_eq!(digit_rows('x'), None);
        assert_ne!(digit_rows('0'), digit_rows('1'));
    }
}
