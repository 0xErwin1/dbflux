// todo("windows"): remove
#![cfg_attr(windows, allow(dead_code))]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    AtlasTextureId, AtlasTile, Background, Bounds, ContentMask, Corners, Edges, Hsla, Pixels,
    Point, Radians, ScaledPixels, Size, bounds_tree::BoundsTree, point,
};
use std::{
    fmt::Debug,
    iter::Peekable,
    ops::{Add, Range, Sub},
    slice,
};

#[allow(non_camel_case_types, unused)]
#[expect(missing_docs)]
pub type PathVertex_ScaledPixels = PathVertex<ScaledPixels>;

#[expect(missing_docs)]
pub type DrawOrder = u32;

/// A boolean stored as a `u32` so that GPU-facing structs contain no
/// compiler-inserted padding bytes, which would be undefined behavior to
/// reinterpret as `&[u8]` when writing instance buffers. Guaranteed to be
/// `0` or `1` by construction; shaders read it as a `u32`/`uint`.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct PaddedBool32(u32);

impl From<bool> for PaddedBool32 {
    fn from(value: bool) -> Self {
        PaddedBool32(value as u32)
    }
}

#[derive(Default)]
#[expect(missing_docs)]
pub struct Scene {
    pub(crate) paint_operations: Vec<PaintOperation>,
    primitive_bounds: BoundsTree<ScaledPixels>,
    layer_stack: Vec<DrawOrder>,
    transform_stack: Vec<TransformationMatrix>,
    pub shadows: Vec<Shadow>,
    pub quads: Vec<Quad>,
    pub paths: Vec<Path<ScaledPixels>>,
    pub underlines: Vec<Underline>,
    pub monochrome_sprites: Vec<MonochromeSprite>,
    pub subpixel_sprites: Vec<SubpixelSprite>,
    pub polychrome_sprites: Vec<PolychromeSprite>,
    pub surfaces: Vec<PaintSurface>,
}

#[expect(missing_docs)]
impl Scene {
    pub fn clear(&mut self) {
        self.paint_operations.clear();
        self.primitive_bounds.clear();
        self.layer_stack.clear();
        self.transform_stack.clear();
        self.paths.clear();
        self.shadows.clear();
        self.quads.clear();
        self.underlines.clear();
        self.monochrome_sprites.clear();
        self.subpixel_sprites.clear();
        self.polychrome_sprites.clear();
        self.surfaces.clear();
    }

    pub fn len(&self) -> usize {
        self.paint_operations.len()
    }

    pub fn push_layer(&mut self, bounds: Bounds<ScaledPixels>) {
        let bounds = transform_bounds(bounds, self.current_transform());
        self.push_replayed_layer(bounds);
    }

    fn push_replayed_layer(&mut self, bounds: Bounds<ScaledPixels>) {
        let order = self.primitive_bounds.insert(bounds);
        self.layer_stack.push(order);
        self.paint_operations
            .push(PaintOperation::StartLayer(bounds));
    }

    pub(crate) fn push_transformed_layer(&mut self, bounds: Bounds<ScaledPixels>) {
        self.push_replayed_layer(bounds);
    }

    pub fn pop_layer(&mut self) {
        self.layer_stack.pop();
        self.paint_operations.push(PaintOperation::EndLayer);
    }

    pub fn insert_primitive(&mut self, primitive: impl Into<Primitive>) {
        self.insert_transformed_primitive(primitive, self.current_transform());
    }

    pub(crate) fn insert_transformed_primitive(
        &mut self,
        primitive: impl Into<Primitive>,
        transform: TransformationMatrix,
    ) {
        let mut primitive = primitive.into().transform(transform);
        let clipped_bounds = primitive
            .visual_bounds()
            .intersect(&primitive.content_mask().bounds);

        if clipped_bounds.is_empty() {
            return;
        }

        let order = self
            .layer_stack
            .last()
            .copied()
            .unwrap_or_else(|| self.primitive_bounds.insert(clipped_bounds));
        match &mut primitive {
            Primitive::Shadow(shadow) => {
                shadow.order = order;
                self.shadows.push(*shadow);
            }
            Primitive::Quad(quad) => {
                quad.order = order;
                self.quads.push(*quad);
            }
            Primitive::Path(path) => {
                path.order = order;
                path.id = PathId(self.paths.len());
                self.paths.push(path.clone());
            }
            Primitive::Underline(underline) => {
                underline.order = order;
                self.underlines.push(*underline);
            }
            Primitive::MonochromeSprite(sprite) => {
                sprite.order = order;
                self.monochrome_sprites.push(*sprite);
            }
            Primitive::SubpixelSprite(sprite) => {
                sprite.order = order;
                self.subpixel_sprites.push(*sprite);
            }
            Primitive::PolychromeSprite(sprite) => {
                sprite.order = order;
                self.polychrome_sprites.push(*sprite);
            }
            Primitive::Surface(surface) => {
                surface.order = order;
                self.surfaces.push(surface.clone());
            }
        }
        self.paint_operations
            .push(PaintOperation::Primitive(primitive));
    }

    #[allow(dead_code)]
    pub(crate) fn with_transform<R>(
        &mut self,
        transform: TransformationMatrix,
        f: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let transform = self.current_transform().compose(transform);
        self.transform_stack.push(transform);
        let result = f(self);
        self.transform_stack.pop();
        result
    }

    pub(crate) fn current_transform(&self) -> TransformationMatrix {
        self.transform_stack.last().copied().unwrap_or_default()
    }

    pub fn replay(&mut self, range: Range<usize>, prev_scene: &Scene) {
        for operation in &prev_scene.paint_operations[range] {
            match operation {
                PaintOperation::Primitive(primitive) => self
                    .insert_transformed_primitive(primitive.clone(), TransformationMatrix::unit()),
                PaintOperation::StartLayer(bounds) => self.push_replayed_layer(*bounds),
                PaintOperation::EndLayer => self.pop_layer(),
            }
        }
    }

    pub fn finish(&mut self) {
        self.shadows.sort_by_key(|shadow| shadow.order);
        self.quads.sort_by_key(|quad| quad.order);
        self.paths.sort_by_key(|path| path.order);
        self.underlines.sort_by_key(|underline| underline.order);
        self.monochrome_sprites
            .sort_by_key(|sprite| (sprite.order, sprite.tile.tile_id));
        self.subpixel_sprites
            .sort_by_key(|sprite| (sprite.order, sprite.tile.tile_id));
        self.polychrome_sprites
            .sort_by_key(|sprite| (sprite.order, sprite.tile.tile_id));
        self.surfaces.sort_by_key(|surface| surface.order);
    }

    #[cfg_attr(
        all(
            any(target_os = "linux", target_os = "freebsd"),
            not(any(feature = "x11", feature = "wayland"))
        ),
        allow(dead_code)
    )]
    pub fn batches(&self) -> impl Iterator<Item = PrimitiveBatch> + '_ {
        BatchIterator {
            shadows_start: 0,
            shadows_iter: self.shadows.iter().peekable(),
            quads_start: 0,
            quads_iter: self.quads.iter().peekable(),
            paths_start: 0,
            paths_iter: self.paths.iter().peekable(),
            underlines_start: 0,
            underlines_iter: self.underlines.iter().peekable(),
            monochrome_sprites_start: 0,
            monochrome_sprites_iter: self.monochrome_sprites.iter().peekable(),
            subpixel_sprites_start: 0,
            subpixel_sprites_iter: self.subpixel_sprites.iter().peekable(),
            polychrome_sprites_start: 0,
            polychrome_sprites_iter: self.polychrome_sprites.iter().peekable(),
            surfaces_start: 0,
            surfaces_iter: self.surfaces.iter().peekable(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
#[cfg_attr(
    all(
        any(target_os = "linux", target_os = "freebsd"),
        not(any(feature = "x11", feature = "wayland"))
    ),
    allow(dead_code)
)]
pub(crate) enum PrimitiveKind {
    Shadow,
    #[default]
    Quad,
    Path,
    Underline,
    MonochromeSprite,
    SubpixelSprite,
    PolychromeSprite,
    Surface,
}

pub(crate) enum PaintOperation {
    Primitive(Primitive),
    StartLayer(Bounds<ScaledPixels>),
    EndLayer,
}

#[derive(Clone)]
#[expect(missing_docs)]
pub enum Primitive {
    Shadow(Shadow),
    Quad(Quad),
    Path(Path<ScaledPixels>),
    Underline(Underline),
    MonochromeSprite(MonochromeSprite),
    SubpixelSprite(SubpixelSprite),
    PolychromeSprite(PolychromeSprite),
    Surface(PaintSurface),
}

#[expect(missing_docs)]
impl Primitive {
    pub fn bounds(&self) -> &Bounds<ScaledPixels> {
        match self {
            Primitive::Shadow(shadow) => &shadow.bounds,
            Primitive::Quad(quad) => &quad.bounds,
            Primitive::Path(path) => &path.bounds,
            Primitive::Underline(underline) => &underline.bounds,
            Primitive::MonochromeSprite(sprite) => &sprite.bounds,
            Primitive::SubpixelSprite(sprite) => &sprite.bounds,
            Primitive::PolychromeSprite(sprite) => &sprite.bounds,
            Primitive::Surface(surface) => &surface.bounds,
        }
    }

    pub fn content_mask(&self) -> &ContentMask<ScaledPixels> {
        match self {
            Primitive::Shadow(shadow) => &shadow.content_mask,
            Primitive::Quad(quad) => &quad.content_mask,
            Primitive::Path(path) => &path.content_mask,
            Primitive::Underline(underline) => &underline.content_mask,
            Primitive::MonochromeSprite(sprite) => &sprite.content_mask,
            Primitive::SubpixelSprite(sprite) => &sprite.content_mask,
            Primitive::PolychromeSprite(sprite) => &sprite.content_mask,
            Primitive::Surface(surface) => &surface.content_mask,
        }
    }

    fn visual_bounds(&self) -> Bounds<ScaledPixels> {
        match self {
            Primitive::MonochromeSprite(sprite) => {
                transform_bounds(sprite.bounds, sprite.transformation)
            }
            Primitive::SubpixelSprite(sprite) => {
                transform_bounds(sprite.bounds, sprite.transformation)
            }
            _ => *self.bounds(),
        }
    }

    fn transform(self, transform: TransformationMatrix) -> Self {
        if transform == TransformationMatrix::unit() {
            return self;
        }

        match self {
            Primitive::Shadow(mut shadow) => {
                shadow.bounds = transform_bounds(shadow.bounds, transform);
                Primitive::Shadow(shadow)
            }
            Primitive::Quad(mut quad) => {
                quad.bounds = transform_bounds(quad.bounds, transform);
                Primitive::Quad(quad)
            }
            Primitive::Path(mut path) => {
                path.transform(transform);
                Primitive::Path(path)
            }
            Primitive::Underline(mut underline) => {
                underline.bounds = transform_bounds(underline.bounds, transform);
                Primitive::Underline(underline)
            }
            Primitive::MonochromeSprite(mut sprite) => {
                sprite.transformation = transform.compose(sprite.transformation);
                Primitive::MonochromeSprite(sprite)
            }
            Primitive::SubpixelSprite(mut sprite) => {
                sprite.transformation = transform.compose(sprite.transformation);
                Primitive::SubpixelSprite(sprite)
            }
            Primitive::PolychromeSprite(mut sprite) => {
                sprite.bounds = transform_bounds(sprite.bounds, transform);
                Primitive::PolychromeSprite(sprite)
            }
            Primitive::Surface(mut surface) => {
                surface.bounds = transform_bounds(surface.bounds, transform);
                Primitive::Surface(surface)
            }
        }
    }
}

pub(crate) fn transform_bounds(
    bounds: Bounds<ScaledPixels>,
    transform: TransformationMatrix,
) -> Bounds<ScaledPixels> {
    let top_left = transform.apply_scaled(bounds.origin);
    let top_right = transform.apply_scaled(point(bounds.right(), bounds.top()));
    let bottom_left = transform.apply_scaled(point(bounds.left(), bounds.bottom()));
    let bottom_right = transform.apply_scaled(point(bounds.right(), bounds.bottom()));

    let left = top_left
        .x
        .0
        .min(top_right.x.0)
        .min(bottom_left.x.0)
        .min(bottom_right.x.0);
    let top = top_left
        .y
        .0
        .min(top_right.y.0)
        .min(bottom_left.y.0)
        .min(bottom_right.y.0);
    let right = top_left
        .x
        .0
        .max(top_right.x.0)
        .max(bottom_left.x.0)
        .max(bottom_right.x.0);
    let bottom = top_left
        .y
        .0
        .max(top_right.y.0)
        .max(bottom_left.y.0)
        .max(bottom_right.y.0);

    Bounds::from_corners(
        point(ScaledPixels(left), ScaledPixels(top)),
        point(ScaledPixels(right), ScaledPixels(bottom)),
    )
}

#[cfg_attr(
    all(
        any(target_os = "linux", target_os = "freebsd"),
        not(any(feature = "x11", feature = "wayland"))
    ),
    allow(dead_code)
)]
struct BatchIterator<'a> {
    shadows_start: usize,
    shadows_iter: Peekable<slice::Iter<'a, Shadow>>,
    quads_start: usize,
    quads_iter: Peekable<slice::Iter<'a, Quad>>,
    paths_start: usize,
    paths_iter: Peekable<slice::Iter<'a, Path<ScaledPixels>>>,
    underlines_start: usize,
    underlines_iter: Peekable<slice::Iter<'a, Underline>>,
    monochrome_sprites_start: usize,
    monochrome_sprites_iter: Peekable<slice::Iter<'a, MonochromeSprite>>,
    subpixel_sprites_start: usize,
    subpixel_sprites_iter: Peekable<slice::Iter<'a, SubpixelSprite>>,
    polychrome_sprites_start: usize,
    polychrome_sprites_iter: Peekable<slice::Iter<'a, PolychromeSprite>>,
    surfaces_start: usize,
    surfaces_iter: Peekable<slice::Iter<'a, PaintSurface>>,
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = PrimitiveBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut orders_and_kinds = [
            (
                self.shadows_iter.peek().map(|s| s.order),
                PrimitiveKind::Shadow,
            ),
            (self.quads_iter.peek().map(|q| q.order), PrimitiveKind::Quad),
            (self.paths_iter.peek().map(|q| q.order), PrimitiveKind::Path),
            (
                self.underlines_iter.peek().map(|u| u.order),
                PrimitiveKind::Underline,
            ),
            (
                self.monochrome_sprites_iter.peek().map(|s| s.order),
                PrimitiveKind::MonochromeSprite,
            ),
            (
                self.subpixel_sprites_iter.peek().map(|s| s.order),
                PrimitiveKind::SubpixelSprite,
            ),
            (
                self.polychrome_sprites_iter.peek().map(|s| s.order),
                PrimitiveKind::PolychromeSprite,
            ),
            (
                self.surfaces_iter.peek().map(|s| s.order),
                PrimitiveKind::Surface,
            ),
        ];
        orders_and_kinds.sort_by_key(|(order, kind)| (order.unwrap_or(u32::MAX), *kind));

        let first = orders_and_kinds[0];
        let second = orders_and_kinds[1];
        let (batch_kind, max_order_and_kind) = if first.0.is_some() {
            (first.1, (second.0.unwrap_or(u32::MAX), second.1))
        } else {
            return None;
        };

        match batch_kind {
            PrimitiveKind::Shadow => {
                let shadows_start = self.shadows_start;
                let mut shadows_end = shadows_start + 1;
                self.shadows_iter.next();
                while self
                    .shadows_iter
                    .next_if(|shadow| (shadow.order, batch_kind) < max_order_and_kind)
                    .is_some()
                {
                    shadows_end += 1;
                }
                self.shadows_start = shadows_end;
                Some(PrimitiveBatch::Shadows(shadows_start..shadows_end))
            }
            PrimitiveKind::Quad => {
                let quads_start = self.quads_start;
                let mut quads_end = quads_start + 1;
                self.quads_iter.next();
                while self
                    .quads_iter
                    .next_if(|quad| (quad.order, batch_kind) < max_order_and_kind)
                    .is_some()
                {
                    quads_end += 1;
                }
                self.quads_start = quads_end;
                Some(PrimitiveBatch::Quads(quads_start..quads_end))
            }
            PrimitiveKind::Path => {
                let paths_start = self.paths_start;
                let mut paths_end = paths_start + 1;
                self.paths_iter.next();
                while self
                    .paths_iter
                    .next_if(|path| (path.order, batch_kind) < max_order_and_kind)
                    .is_some()
                {
                    paths_end += 1;
                }
                self.paths_start = paths_end;
                Some(PrimitiveBatch::Paths(paths_start..paths_end))
            }
            PrimitiveKind::Underline => {
                let underlines_start = self.underlines_start;
                let mut underlines_end = underlines_start + 1;
                self.underlines_iter.next();
                while self
                    .underlines_iter
                    .next_if(|underline| (underline.order, batch_kind) < max_order_and_kind)
                    .is_some()
                {
                    underlines_end += 1;
                }
                self.underlines_start = underlines_end;
                Some(PrimitiveBatch::Underlines(underlines_start..underlines_end))
            }
            PrimitiveKind::MonochromeSprite => {
                let texture_id = self.monochrome_sprites_iter.peek().unwrap().tile.texture_id;
                let sprites_start = self.monochrome_sprites_start;
                let mut sprites_end = sprites_start + 1;
                self.monochrome_sprites_iter.next();
                while self
                    .monochrome_sprites_iter
                    .next_if(|sprite| {
                        (sprite.order, batch_kind) < max_order_and_kind
                            && sprite.tile.texture_id == texture_id
                    })
                    .is_some()
                {
                    sprites_end += 1;
                }
                self.monochrome_sprites_start = sprites_end;
                Some(PrimitiveBatch::MonochromeSprites {
                    texture_id,
                    range: sprites_start..sprites_end,
                })
            }
            PrimitiveKind::SubpixelSprite => {
                let texture_id = self.subpixel_sprites_iter.peek().unwrap().tile.texture_id;
                let sprites_start = self.subpixel_sprites_start;
                let mut sprites_end = sprites_start + 1;
                self.subpixel_sprites_iter.next();
                while self
                    .subpixel_sprites_iter
                    .next_if(|sprite| {
                        (sprite.order, batch_kind) < max_order_and_kind
                            && sprite.tile.texture_id == texture_id
                    })
                    .is_some()
                {
                    sprites_end += 1;
                }
                self.subpixel_sprites_start = sprites_end;
                Some(PrimitiveBatch::SubpixelSprites {
                    texture_id,
                    range: sprites_start..sprites_end,
                })
            }
            PrimitiveKind::PolychromeSprite => {
                let texture_id = self.polychrome_sprites_iter.peek().unwrap().tile.texture_id;
                let sprites_start = self.polychrome_sprites_start;
                let mut sprites_end = sprites_start + 1;
                self.polychrome_sprites_iter.next();
                while self
                    .polychrome_sprites_iter
                    .next_if(|sprite| {
                        (sprite.order, batch_kind) < max_order_and_kind
                            && sprite.tile.texture_id == texture_id
                    })
                    .is_some()
                {
                    sprites_end += 1;
                }
                self.polychrome_sprites_start = sprites_end;
                Some(PrimitiveBatch::PolychromeSprites {
                    texture_id,
                    range: sprites_start..sprites_end,
                })
            }
            PrimitiveKind::Surface => {
                let surfaces_start = self.surfaces_start;
                let mut surfaces_end = surfaces_start + 1;
                self.surfaces_iter.next();
                while self
                    .surfaces_iter
                    .next_if(|surface| (surface.order, batch_kind) < max_order_and_kind)
                    .is_some()
                {
                    surfaces_end += 1;
                }
                self.surfaces_start = surfaces_end;
                Some(PrimitiveBatch::Surfaces(surfaces_start..surfaces_end))
            }
        }
    }
}

#[derive(Debug)]
#[cfg_attr(
    all(
        any(target_os = "linux", target_os = "freebsd"),
        not(any(feature = "x11", feature = "wayland"))
    ),
    allow(dead_code)
)]
#[allow(missing_docs)]
pub enum PrimitiveBatch {
    Shadows(Range<usize>),
    Quads(Range<usize>),
    Paths(Range<usize>),
    Underlines(Range<usize>),
    MonochromeSprites {
        texture_id: AtlasTextureId,
        range: Range<usize>,
    },
    #[cfg_attr(target_os = "macos", allow(dead_code))]
    SubpixelSprites {
        texture_id: AtlasTextureId,
        range: Range<usize>,
    },
    PolychromeSprites {
        texture_id: AtlasTextureId,
        range: Range<usize>,
    },
    Surfaces(Range<usize>),
}

impl PrimitiveBatch {
    #[expect(missing_docs)]
    pub fn label(&self) -> String {
        match self {
            Self::Shadows(range) => format!("shadows ({})", range.len()),
            Self::Quads(range) => format!("quads ({})", range.len()),
            Self::Paths(range) => format!("paths ({})", range.len()),
            Self::Underlines(range) => format!("underlines ({})", range.len()),
            Self::MonochromeSprites { texture_id, range } => {
                format!(
                    "monochrome sprites ({}) on atlas {}",
                    range.len(),
                    texture_id.index
                )
            }
            Self::SubpixelSprites { texture_id, range } => {
                format!(
                    "subpixel sprites ({}) on atlas {}",
                    range.len(),
                    texture_id.index
                )
            }
            Self::PolychromeSprites { texture_id, range } => {
                format!(
                    "polychrome sprites ({}) on atlas {}",
                    range.len(),
                    texture_id.index
                )
            }
            Self::Surfaces(range) => format!("surfaces ({})", range.len()),
        }
    }
}

#[derive(Default, Debug, Copy, Clone)]
#[repr(C)]
#[expect(missing_docs)]
pub struct Quad {
    pub order: DrawOrder,
    pub border_style: BorderStyle,
    pub bounds: Bounds<ScaledPixels>,
    pub content_mask: ContentMask<ScaledPixels>,
    pub background: Background,
    pub border_color: Hsla,
    pub corner_radii: Corners<ScaledPixels>,
    pub border_widths: Edges<ScaledPixels>,
}

impl From<Quad> for Primitive {
    fn from(quad: Quad) -> Self {
        Primitive::Quad(quad)
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
#[expect(missing_docs)]
pub struct Underline {
    pub order: DrawOrder,
    pub pad: u32, // align to 8 bytes
    pub bounds: Bounds<ScaledPixels>,
    pub content_mask: ContentMask<ScaledPixels>,
    pub color: Hsla,
    pub thickness: ScaledPixels,
    pub wavy: PaddedBool32,
}

impl From<Underline> for Primitive {
    fn from(underline: Underline) -> Self {
        Primitive::Underline(underline)
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
#[expect(missing_docs)]
pub struct Shadow {
    pub order: DrawOrder,
    pub blur_radius: ScaledPixels,
    pub bounds: Bounds<ScaledPixels>,
    pub corner_radii: Corners<ScaledPixels>,
    pub content_mask: ContentMask<ScaledPixels>,
    pub color: Hsla,
    pub element_bounds: Bounds<ScaledPixels>,
    pub element_corner_radii: Corners<ScaledPixels>,
    /// 0 = drop shadow (rendered outside the element), 1 = inset shadow (rendered inside).
    pub inset: u32,
    pub pad: u32, // align to 8 bytes
}

impl From<Shadow> for Primitive {
    fn from(shadow: Shadow) -> Self {
        Primitive::Shadow(shadow)
    }
}

/// The style of a border.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[repr(C)]
pub enum BorderStyle {
    /// A solid border.
    #[default]
    Solid = 0,
    /// A dashed border.
    Dashed = 1,
}

/// A data type representing a 2 dimensional transformation that can be applied to an element.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[repr(C)]
pub struct TransformationMatrix {
    /// 2x2 matrix containing rotation and scale,
    /// stored row-major
    pub rotation_scale: [[f32; 2]; 2],
    /// translation vector
    pub translation: [f32; 2],
}

impl Eq for TransformationMatrix {}

impl TransformationMatrix {
    /// The unit matrix, has no effect.
    pub fn unit() -> Self {
        Self {
            rotation_scale: [[1.0, 0.0], [0.0, 1.0]],
            translation: [0.0, 0.0],
        }
    }

    /// Move the origin by a given point
    pub fn translate(mut self, point: Point<ScaledPixels>) -> Self {
        self.compose(Self {
            rotation_scale: [[1.0, 0.0], [0.0, 1.0]],
            translation: [point.x.0, point.y.0],
        })
    }

    /// Clockwise rotation in radians around the origin
    pub fn rotate(self, angle: Radians) -> Self {
        self.compose(Self {
            rotation_scale: [
                [angle.0.cos(), -angle.0.sin()],
                [angle.0.sin(), angle.0.cos()],
            ],
            translation: [0.0, 0.0],
        })
    }

    /// Scale around the origin
    pub fn scale(self, size: Size<f32>) -> Self {
        self.compose(Self {
            rotation_scale: [[size.width, 0.0], [0.0, size.height]],
            translation: [0.0, 0.0],
        })
    }

    /// Perform matrix multiplication with another transformation
    /// to produce a new transformation that is the result of
    /// applying both transformations: first, `other`, then `self`.
    #[inline]
    pub fn compose(self, other: TransformationMatrix) -> TransformationMatrix {
        if other == Self::unit() {
            return self;
        }
        // Perform matrix multiplication
        TransformationMatrix {
            rotation_scale: [
                [
                    self.rotation_scale[0][0] * other.rotation_scale[0][0]
                        + self.rotation_scale[0][1] * other.rotation_scale[1][0],
                    self.rotation_scale[0][0] * other.rotation_scale[0][1]
                        + self.rotation_scale[0][1] * other.rotation_scale[1][1],
                ],
                [
                    self.rotation_scale[1][0] * other.rotation_scale[0][0]
                        + self.rotation_scale[1][1] * other.rotation_scale[1][0],
                    self.rotation_scale[1][0] * other.rotation_scale[0][1]
                        + self.rotation_scale[1][1] * other.rotation_scale[1][1],
                ],
            ],
            translation: [
                self.translation[0]
                    + self.rotation_scale[0][0] * other.translation[0]
                    + self.rotation_scale[0][1] * other.translation[1],
                self.translation[1]
                    + self.rotation_scale[1][0] * other.translation[0]
                    + self.rotation_scale[1][1] * other.translation[1],
            ],
        }
    }

    pub(crate) fn inverse(self) -> Option<TransformationMatrix> {
        let [[a, b], [c, d]] = self.rotation_scale;
        let determinant = a * d - b * c;
        if determinant.abs() <= f32::EPSILON {
            return None;
        }

        let inverse_determinant = determinant.recip();
        let rotation_scale = [
            [d * inverse_determinant, -b * inverse_determinant],
            [-c * inverse_determinant, a * inverse_determinant],
        ];
        let translation = [
            -(rotation_scale[0][0] * self.translation[0]
                + rotation_scale[0][1] * self.translation[1]),
            -(rotation_scale[1][0] * self.translation[0]
                + rotation_scale[1][1] * self.translation[1]),
        ];

        Some(TransformationMatrix {
            rotation_scale,
            translation,
        })
    }

    /// Apply transformation to a point, mainly useful for debugging
    pub fn apply(&self, point: Point<Pixels>) -> Point<Pixels> {
        let input = [point.x.0, point.y.0];
        let mut output = self.translation;
        for (i, output_cell) in output.iter_mut().enumerate() {
            for (k, input_cell) in input.iter().enumerate() {
                *output_cell += self.rotation_scale[i][k] * *input_cell;
            }
        }
        Point::new(output[0].into(), output[1].into())
    }

    pub(crate) fn apply_scaled(&self, point: Point<ScaledPixels>) -> Point<ScaledPixels> {
        let input = [point.x.0, point.y.0];
        let mut output = self.translation;
        for (i, output_cell) in output.iter_mut().enumerate() {
            for (k, input_cell) in input.iter().enumerate() {
                *output_cell += self.rotation_scale[i][k] * *input_cell;
            }
        }
        Point::new(ScaledPixels(output[0]), ScaledPixels(output[1]))
    }
}

impl Default for TransformationMatrix {
    fn default() -> Self {
        Self::unit()
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
#[expect(missing_docs)]
pub struct MonochromeSprite {
    pub order: DrawOrder,
    pub pad: u32,
    pub bounds: Bounds<ScaledPixels>,
    pub content_mask: ContentMask<ScaledPixels>,
    pub color: Hsla,
    pub tile: AtlasTile,
    pub transformation: TransformationMatrix,
}

impl From<MonochromeSprite> for Primitive {
    fn from(sprite: MonochromeSprite) -> Self {
        Primitive::MonochromeSprite(sprite)
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
#[expect(missing_docs)]
pub struct SubpixelSprite {
    pub order: DrawOrder,
    pub pad: u32, // align to 8 bytes
    pub bounds: Bounds<ScaledPixels>,
    pub content_mask: ContentMask<ScaledPixels>,
    pub color: Hsla,
    pub tile: AtlasTile,
    pub transformation: TransformationMatrix,
}

impl From<SubpixelSprite> for Primitive {
    fn from(sprite: SubpixelSprite) -> Self {
        Primitive::SubpixelSprite(sprite)
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
#[expect(missing_docs)]
pub struct PolychromeSprite {
    pub order: DrawOrder,
    pub pad: u32,
    pub grayscale: PaddedBool32,
    pub opacity: f32,
    pub bounds: Bounds<ScaledPixels>,
    pub content_mask: ContentMask<ScaledPixels>,
    pub corner_radii: Corners<ScaledPixels>,
    pub tile: AtlasTile,
}

impl From<PolychromeSprite> for Primitive {
    fn from(sprite: PolychromeSprite) -> Self {
        Primitive::PolychromeSprite(sprite)
    }
}

#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct PaintSurface {
    pub order: DrawOrder,
    pub bounds: Bounds<ScaledPixels>,
    pub content_mask: ContentMask<ScaledPixels>,
    #[cfg(target_os = "macos")]
    pub image_buffer: core_video::pixel_buffer::CVPixelBuffer,
}

impl From<PaintSurface> for Primitive {
    fn from(surface: PaintSurface) -> Self {
        Primitive::Surface(surface)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[expect(missing_docs)]
pub struct PathId(pub usize);

/// A line made up of a series of vertices and control points.
#[derive(Clone, Debug)]
#[expect(missing_docs)]
pub struct Path<P: Clone + Debug + Default + PartialEq> {
    pub id: PathId,
    pub order: DrawOrder,
    pub bounds: Bounds<P>,
    pub content_mask: ContentMask<P>,
    pub vertices: Vec<PathVertex<P>>,
    pub color: Background,
    start: Point<P>,
    current: Point<P>,
    contour_count: usize,
}

impl Path<Pixels> {
    /// Create a new path with the given starting point.
    pub fn new(start: Point<Pixels>) -> Self {
        Self {
            id: PathId(0),
            order: DrawOrder::default(),
            vertices: Vec::new(),
            start,
            current: start,
            bounds: Bounds {
                origin: start,
                size: Default::default(),
            },
            content_mask: Default::default(),
            color: Default::default(),
            contour_count: 0,
        }
    }

    /// Scale this path by the given factor.
    pub fn scale(&self, factor: f32) -> Path<ScaledPixels> {
        Path {
            id: self.id,
            order: self.order,
            bounds: self.bounds.scale(factor),
            content_mask: self.content_mask.scale(factor),
            vertices: self
                .vertices
                .iter()
                .map(|vertex| vertex.scale(factor))
                .collect(),
            start: self.start.map(|start| start.scale(factor)),
            current: self.current.scale(factor),
            contour_count: self.contour_count,
            color: self.color,
        }
    }

    /// Move the start, current point to the given point.
    pub fn move_to(&mut self, to: Point<Pixels>) {
        self.contour_count += 1;
        self.start = to;
        self.current = to;
    }

    /// Draw a straight line from the current point to the given point.
    pub fn line_to(&mut self, to: Point<Pixels>) {
        self.contour_count += 1;
        if self.contour_count > 1 {
            self.push_triangle(
                (self.start, self.current, to),
                (point(0., 1.), point(0., 1.), point(0., 1.)),
            );
        }
        self.current = to;
    }

    /// Draw a curve from the current point to the given point, using the given control point.
    pub fn curve_to(&mut self, to: Point<Pixels>, ctrl: Point<Pixels>) {
        self.contour_count += 1;
        if self.contour_count > 1 {
            self.push_triangle(
                (self.start, self.current, to),
                (point(0., 1.), point(0., 1.), point(0., 1.)),
            );
        }

        self.push_triangle(
            (self.current, ctrl, to),
            (point(0., 0.), point(0.5, 0.), point(1., 1.)),
        );
        self.current = to;
    }

    /// Push a triangle to the Path.
    pub fn push_triangle(
        &mut self,
        xy: (Point<Pixels>, Point<Pixels>, Point<Pixels>),
        st: (Point<f32>, Point<f32>, Point<f32>),
    ) {
        self.bounds = self
            .bounds
            .union(&Bounds {
                origin: xy.0,
                size: Default::default(),
            })
            .union(&Bounds {
                origin: xy.1,
                size: Default::default(),
            })
            .union(&Bounds {
                origin: xy.2,
                size: Default::default(),
            });

        self.vertices.push(PathVertex {
            xy_position: xy.0,
            st_position: st.0,
            content_mask: Default::default(),
        });
        self.vertices.push(PathVertex {
            xy_position: xy.1,
            st_position: st.1,
            content_mask: Default::default(),
        });
        self.vertices.push(PathVertex {
            xy_position: xy.2,
            st_position: st.2,
            content_mask: Default::default(),
        });
    }
}

impl<T> Path<T>
where
    T: Clone + Debug + Default + PartialEq + PartialOrd + Add<T, Output = T> + Sub<Output = T>,
{
    #[allow(unused)]
    #[expect(missing_docs)]
    pub fn clipped_bounds(&self) -> Bounds<T> {
        self.bounds.intersect(&self.content_mask.bounds)
    }
}

impl Path<ScaledPixels> {
    fn transform(&mut self, transform: TransformationMatrix) {
        self.bounds = transform_bounds(self.bounds, transform);
        self.start = transform.apply_scaled(self.start);
        self.current = transform.apply_scaled(self.current);

        for vertex in &mut self.vertices {
            vertex.xy_position = transform.apply_scaled(vertex.xy_position);
        }
    }
}

impl From<Path<ScaledPixels>> for Primitive {
    fn from(path: Path<ScaledPixels>) -> Self {
        Primitive::Path(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AtlasTextureKind, DevicePixels, TileId, size};

    #[test]
    fn transform_stack_composes_parent_then_child() {
        let mut scene = Scene::default();
        let parent =
            TransformationMatrix::unit().translate(point(ScaledPixels(10.), ScaledPixels(0.)));
        let child = TransformationMatrix::unit().scale(size(2., 2.));

        scene.with_transform(parent, |scene| {
            scene.with_transform(child, |scene| {
                let transformed = scene
                    .current_transform()
                    .apply(point(Pixels(3.), Pixels(4.)));

                assert_eq!(transformed, point(Pixels(16.), Pixels(8.)));
            });
        });
    }

    #[test]
    fn nested_transform_matches_direct_composed_sprite() {
        let parent =
            TransformationMatrix::unit().translate(point(ScaledPixels(10.), ScaledPixels(0.)));
        let child = TransformationMatrix::unit().scale(size(2., 2.));
        let direct_transform = parent.compose(child);

        let mut direct_scene = Scene::default();
        direct_scene.with_transform(direct_transform, |scene| {
            scene.insert_primitive(test_sprite());
        });

        let mut nested_scene = Scene::default();
        nested_scene.with_transform(parent, |scene| {
            scene.with_transform(child, |scene| {
                scene.insert_primitive(test_sprite());
            });
        });

        assert_eq!(direct_scene.monochrome_sprites.len(), 1);
        assert_eq!(nested_scene.monochrome_sprites.len(), 1);

        let Some(direct_sprite) = direct_scene.monochrome_sprites.first() else {
            panic!("direct scene should emit one monochrome sprite");
        };
        let Some(nested_sprite) = nested_scene.monochrome_sprites.first() else {
            panic!("nested scene should emit one monochrome sprite");
        };

        assert_eq!(nested_sprite.transformation, direct_sprite.transformation,);
    }

    #[test]
    fn replay_preserves_previously_transformed_primitives() {
        let transform =
            TransformationMatrix::unit().translate(point(ScaledPixels(10.), ScaledPixels(20.)));

        let mut previous_scene = Scene::default();
        previous_scene.with_transform(transform, |scene| {
            scene.insert_primitive(test_quad());
        });

        let mut replayed_scene = Scene::default();
        replayed_scene.with_transform(transform, |scene| {
            scene.replay(0..previous_scene.len(), &previous_scene);
        });

        assert_eq!(previous_scene.quads.len(), 1);
        assert_eq!(replayed_scene.quads.len(), 1);

        let Some(previous_quad) = previous_scene.quads.first() else {
            panic!("previous scene should emit one quad");
        };
        let Some(replayed_quad) = replayed_scene.quads.first() else {
            panic!("replayed scene should emit one quad");
        };

        assert_eq!(replayed_quad.bounds, previous_quad.bounds);
    }

    #[test]
    fn transformed_monochrome_sprite_is_culled_by_visual_bounds() {
        let transform =
            TransformationMatrix::unit().translate(point(ScaledPixels(100.), ScaledPixels(0.)));

        let mut sprite = test_sprite();
        sprite.content_mask = ContentMask {
            bounds: Bounds::new(
                point(ScaledPixels(100.), ScaledPixels(0.)),
                size(ScaledPixels(10.), ScaledPixels(10.)),
            ),
        };

        let mut scene = Scene::default();
        scene.insert_transformed_primitive(sprite, transform);

        assert_eq!(scene.monochrome_sprites.len(), 1);

        let Some(sprite) = scene.monochrome_sprites.first() else {
            panic!("scene should emit one transformed monochrome sprite");
        };

        assert_eq!(sprite.bounds, test_sprite().bounds);
        assert_eq!(sprite.transformation, transform);
    }

    #[test]
    fn transformed_subpixel_sprite_is_culled_by_visual_bounds() {
        let transform =
            TransformationMatrix::unit().translate(point(ScaledPixels(0.), ScaledPixels(100.)));

        let mut sprite = test_subpixel_sprite();
        sprite.content_mask = ContentMask {
            bounds: Bounds::new(
                point(ScaledPixels(0.), ScaledPixels(100.)),
                size(ScaledPixels(10.), ScaledPixels(10.)),
            ),
        };

        let mut scene = Scene::default();
        scene.insert_transformed_primitive(sprite, transform);

        assert_eq!(scene.subpixel_sprites.len(), 1);

        let Some(sprite) = scene.subpixel_sprites.first() else {
            panic!("scene should emit one transformed subpixel sprite");
        };

        assert_eq!(sprite.bounds, test_subpixel_sprite().bounds);
        assert_eq!(sprite.transformation, transform);
    }

    fn test_quad() -> Quad {
        Quad {
            order: 0,
            border_style: BorderStyle::default(),
            bounds: Bounds::new(
                point(ScaledPixels(1.), ScaledPixels(2.)),
                size(ScaledPixels(3.), ScaledPixels(4.)),
            ),
            content_mask: ContentMask {
                bounds: Bounds::new(
                    point(ScaledPixels(0.), ScaledPixels(0.)),
                    size(ScaledPixels(100.), ScaledPixels(100.)),
                ),
            },
            background: Background::default(),
            border_color: Hsla::default(),
            corner_radii: Corners::default(),
            border_widths: Edges::default(),
        }
    }

    fn test_sprite() -> MonochromeSprite {
        MonochromeSprite {
            order: 0,
            pad: 0,
            bounds: Bounds::new(
                point(ScaledPixels(0.), ScaledPixels(0.)),
                size(ScaledPixels(10.), ScaledPixels(10.)),
            ),
            content_mask: ContentMask {
                bounds: Bounds::new(
                    point(ScaledPixels(0.), ScaledPixels(0.)),
                    size(ScaledPixels(100.), ScaledPixels(100.)),
                ),
            },
            color: Hsla::default(),
            tile: AtlasTile {
                texture_id: AtlasTextureId {
                    index: 0,
                    kind: AtlasTextureKind::Monochrome,
                },
                tile_id: TileId(0),
                padding: 0,
                bounds: Bounds::new(
                    point(DevicePixels(0), DevicePixels(0)),
                    size(DevicePixels(10), DevicePixels(10)),
                ),
            },
            transformation: TransformationMatrix::unit(),
        }
    }

    fn test_subpixel_sprite() -> SubpixelSprite {
        let sprite = test_sprite();

        SubpixelSprite {
            order: sprite.order,
            pad: sprite.pad,
            bounds: sprite.bounds,
            content_mask: sprite.content_mask,
            color: sprite.color,
            tile: sprite.tile,
            transformation: sprite.transformation,
        }
    }
}

#[derive(Clone, Debug)]
#[repr(C)]
#[expect(missing_docs)]
pub struct PathVertex<P: Clone + Debug + Default + PartialEq> {
    pub xy_position: Point<P>,
    pub st_position: Point<f32>,
    pub content_mask: ContentMask<P>,
}

#[expect(missing_docs)]
impl PathVertex<Pixels> {
    pub fn scale(&self, factor: f32) -> PathVertex<ScaledPixels> {
        PathVertex {
            xy_position: self.xy_position.scale(factor),
            st_position: self.st_position,
            content_mask: self.content_mask.scale(factor),
        }
    }
}
