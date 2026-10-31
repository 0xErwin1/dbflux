# Design system

DBFlux's interface is built on **Bolt**, a design-system crate shared with
[ReqFlux](https://github.com/0xErwin1/reqflux). Bolt owns everything a product does
not: the palettes, the color and typography roles, the spacing and chrome
metrics, the cut corners, the theme mapping onto gpui-component, and the generic
GPUI components. A product supplies its accent, name and any token of its own.

The crate lives in `crates/bolt` while its API settles. It is designed to move
to its own repository without touching a screen: see
[Consuming Bolt from another repository](#consuming-bolt-from-another-repository).

## What Bolt owns

| Module | Contents |
|---|---|
| `bolt::tokens` | Spacing, heights, radii, cuts, chrome and control metrics, syntax and row colors |
| `bolt::brand` | `Brand` (accent, hover, pressed, tint, on-accent, name) and the washes derived from it |
| `bolt::palette` | `PaletteMode::{Dark, Light, System}` and the palette a product publishes |
| `bolt::theme` | `install`, `apply_theme`, the gpui-component mapping, editor chrome and syntax recoloring |
| `bolt::density` | `Density::{Default, Compact}` and the runtime font and radius accessors |
| `bolt::fonts` | `FontSettings`, `FontChoices`, the size bounds and the resolvers |
| `bolt::typography` | The bundled families and their files |
| `bolt::semantic` | Banner, row-state and chart-chrome colors per palette |
| `bolt::icon` | `IconSource`: a named icon or an SVG path |
| `bolt::primitives` | Badge, banner, chamfer, divider, file picker, focus ring, keycap, label, loading state, segmented control, status, surface, text, type-to-confirm |
| `bolt::controls` | Button, checkbox, dropdown, input, read-only text view |

Bolt depends on `gpui`, `gpui-component`, `gpui-base`, `log` and `serde_json`.
It never depends on DBFlux's domain crates and never names a product, a driver,
a database or a design board: a source-scanning guardrail in
`crates/bolt/tests/guardrail.rs` enforces that.

## What a product supplies

- **A `Brand`.** `bolt::brand::install(brand, cx)` publishes the accent roles.
  Every accent-shaped color — the primary fill, the focus ring, the selection
  washes, syntax keywords, the checked checkbox — resolves through it. DBFlux's
  byzantine accent (`#702963`) is defined in
  `crates/dbflux_components/src/theme.rs`, not in Bolt.
- **A palette mode and a density.** Dark is unconditional; `Light` and
  `Compact` are opt-in. DBFlux maps its persisted `ThemeSetting` and `AppStyle`
  onto `PaletteMode` and `Density`.
- **Its font choices.** `FontChoices` carries the families and sizes the user
  picked; Bolt clamps them and drops the families the text system cannot load.
- **Its icon catalogue.** `IconSource` is a name or a path; DBFlux's 162-variant
  `AppIcon` and ReqFlux's Lucide set both plug in.
- **Its keymap and its copy.** The dropdown takes the action ids it answers and
  a translated placeholder through globals the product installs, so Bolt needs
  neither the keymap nor the i18n crate.

## Sizes

Every interface length goes through `tokens::ui(px)`, which expresses design
pixels in rems. Windows render under `gpui_component::Root`, which sets the rem
size from the interface font size, so a size built with `ui` follows the setting
without any call site reading it. Spacing, borders, radii and panel sizes stay
fixed pixels.

## How DBFlux consumes Bolt

DBFlux keeps its product layer in `crates/dbflux_components`, and that crate
re-exports every module Bolt now owns at the path it had before:

```rust
// crates/dbflux_components/src/lib.rs
pub use bolt::{fonts, icon, semantic, tokens, typography};
```

This is deliberate. About 1,800 references across 325 files read
`dbflux_components::tokens::…` and its siblings; re-exporting keeps them
compiling and keeps a move from turning into a search-and-replace with no
behavior change to show for it. Product-coupled helpers stay behind as
extensions over the generic primitive: `EnvTag` and `environment_badge_tone`
over `Badge`, the per-variant banner icon over `BannerBlock`, the translated
placeholder over `FilePicker`, and `DropdownEnvironment` over `Dropdown`.

## Consuming Bolt from another repository

ReqFlux carries its own copy of this design system today, with the same layering
and a smaller token set. The swap is a dependency change plus a brand:

1. Add the dependency, pinned to a tag so a build is reproducible:

   ```toml
   [dependencies]
   bolt = { git = "https://github.com/0xErwin1/dbflux", tag = "bolt-v0.1.0" }
   ```

2. Delete `crates/bolt` from ReqFlux and re-export the shared crate at the old
   path, so ReqFlux's screens keep compiling:

   ```rust
   // the module ReqFlux's screens already import
   pub use bolt::*;
   ```

3. Install the brand before the first window, and drop the copied theme code:

   ```rust
   bolt::brand::install(reqflux_brand(), cx);
   bolt::theme::apply_theme(bolt::palette::PaletteMode::Dark, bolt::density::Density::Compact, None, cx);
   ```

4. Keep ReqFlux's own tokens — HTTP method colors, the draft marker — where they
   are. They are product data, not design-system roles.

Two differences to expect when moving from the earlier copy: sizes follow the
interface font size through rems instead of fixed pixels, and the light palette
derives its selection washes from the accent while the dark one derives them
from the accent tint. Both are properties of the shared palette; a product that
wants different values changes its `Brand`, not Bolt.

## Guardrails

Two source-scanning tests keep the boundary honest:

- `crates/bolt/tests/guardrail.rs` rejects a product name, a raw color, a bare
  pixel length or a domain-crate reference inside Bolt, outside the files that
  legitimately define colors and geometry.
- `crates/dbflux_components/src/style_guardrails.rs` rejects bare spacing
  literals, raw color constructors, pixel text sizes and bundled font families
  in the product crate and the UI crates.

Both run with the test suite, so a violation fails the build rather than the
review.

## What Bolt does not own yet

Domain components stay in `dbflux_components` until their seams exist:
`data_table`, `document_tree`, the modals, the chart engine, `result_panel`, the
Vim layer and the composites. They read `ColumnMeta`, `Value`, `QueryLanguage`
and `ContextId` from `dbflux_core`, so moving them means opening a generic seam
in the core first, one component at a time.
