#!/usr/bin/env bash
#
# Regenerates every derived brand asset from the per-channel sources in
# resources/branding/<channel>/:
#
#   mark.svg        full app icon, used at 48 px and up
#   mark-small.svg  glyph variant (bolt only), used at 32 px and below and as
#                   the in-app mark and the favicon
#
# Outputs:
#   resources/branding/<channel>/wordmark.svg        glyph + DBFLUX lockup, text as outlines
#   resources/branding/<channel>/mark-256.png        full icon, served in-app via img(...)
#   resources/branding/<channel>/mark-small-256.png  glyph, served in-app via img(...)
#   packaging/icons/<N>x<N>/apps/dbflux.png           stable hicolor sizes
#   packaging/icons/dbflux[-nightly].ico              Windows icon
#   packaging/icons/dbflux[-nightly].icns             macOS bundle icon
#   web/public/brand/{favicon.svg,mark-256.png,wordmark.svg}  stable copies for the site
#
# Run from anywhere: `scripts/branding/generate-icons.sh`. When the tools are
# not on PATH the script re-executes itself inside a Nix shell that provides
# them.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

required_tools=(rsvg-convert icotool png2icns python3)

missing_tool=""
for tool in "${required_tools[@]}"; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        missing_tool="$tool"
        break
    fi
done

if ! python3 -c 'import fontTools, uharfbuzz' >/dev/null 2>&1; then
    missing_tool="python3 (fonttools, uharfbuzz)"
fi

if [[ -n "$missing_tool" ]]; then
    if [[ -n "${DBFLUX_BRANDING_NIX_SHELL:-}" ]]; then
        echo "error: $missing_tool is still missing inside the Nix shell" >&2
        exit 1
    fi

    if ! command -v nix >/dev/null 2>&1; then
        echo "error: $missing_tool is not on PATH and nix is not available" >&2
        exit 1
    fi

    export DBFLUX_BRANDING_NIX_SHELL=1
    exec nix shell --impure --expr '
        with import (builtins.getFlake "nixpkgs") { };
        buildEnv {
            name = "dbflux-branding-tools";
            paths = [
                librsvg
                icoutils
                libicns
                (python3.withPackages (packages: [ packages.fonttools packages.uharfbuzz ]))
            ];
        }
    ' -c "$0" "$@"
fi

branding_dir="$repo_root/resources/branding"
packaging_dir="$repo_root/packaging/icons"
web_brand_dir="$repo_root/web/public/brand"
wordmark_font="$repo_root/crates/dbflux_components/assets/fonts/ArchivoExpanded-Black.ttf"

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

glyph_max_size=32

render_png() {
    local source="$1" size="$2" destination="$3"

    rsvg-convert --width "$size" --height "$size" --keep-aspect-ratio \
        --background-color none --output "$destination" "$source"
}

source_for_size() {
    local channel_dir="$1" size="$2"

    if (( size <= glyph_max_size )); then
        echo "$channel_dir/mark-small.svg"
    else
        echo "$channel_dir/mark.svg"
    fi
}

# Lockup from the DSBrand board: 64 px glyph, 18 px gap, "DBFLUX" in Archivo
# Expanded 900 at 52 px with 0.01em tracking, text centered on the glyph the
# way a CSS flex row with align-items: center places it.
write_wordmark() {
    local glyph_svg="$1" destination="$2"

    python3 - "$glyph_svg" "$wordmark_font" "$destination" <<'PYTHON'
import re
import sys

import uharfbuzz
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont

glyph_path, font_path, destination = sys.argv[1:4]

text = "DBFLUX"
glyph_size = 64.0
gap = 18.0
font_size = 52.0
tracking_em = 0.01
text_color = "#F7F4F7"


def number(value):
    formatted = f"{value:.2f}".rstrip("0").rstrip(".")
    return "0" if formatted == "-0" else formatted


glyph_source = open(glyph_path, encoding="utf-8").read()
glyph_body = re.search(r"</title>(.*)</svg>", glyph_source, re.DOTALL).group(1).strip()

font = TTFont(font_path)
units_per_em = font["head"].unitsPerEm
scale = font_size / units_per_em
ascent = font["hhea"].ascent
descent = -font["hhea"].descent

baseline = glyph_size / 2 + (ascent - descent) / 2 * scale

font_data = open(font_path, "rb").read()
hb_font = uharfbuzz.Font(uharfbuzz.Face(font_data))
buffer = uharfbuzz.Buffer()
buffer.add_str(text)
buffer.guess_segment_properties()
uharfbuzz.shape(hb_font, buffer, {"kern": True, "liga": False})

glyph_order = font.getGlyphOrder()
glyph_set = font.getGlyphSet()
tracking = tracking_em * font_size

path_pen = SVGPathPen(glyph_set, ntos=number)
bounds_pen = BoundsPen(glyph_set)

pen_x = glyph_size + gap
for index, (info, position) in enumerate(zip(buffer.glyph_infos, buffer.glyph_positions)):
    glyph_name = glyph_order[info.codepoint]
    transform = (scale, 0, 0, -scale, pen_x + position.x_offset * scale, baseline - position.y_offset * scale)

    glyph_set[glyph_name].draw(TransformPen(path_pen, transform))
    glyph_set[glyph_name].draw(TransformPen(bounds_pen, transform))

    pen_x += position.x_advance * scale
    if index < len(buffer.glyph_infos) - 1:
        pen_x += tracking

width = max(pen_x, bounds_pen.bounds[2])

document = (
    f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {number(width)} {number(glyph_size)}" '
    f'width="{number(width)}" height="{number(glyph_size)}" role="img" aria-label="DBFlux">'
    f"<title>DBFlux</title>"
    f"{glyph_body}"
    f'<path d="{path_pen.getCommands()}" fill="{text_color}"></path>'
    f"</svg>\n"
)

with open(destination, "w", encoding="utf-8") as output:
    output.write(document)
PYTHON
}

for channel in stable nightly; do
    channel_dir="$branding_dir/$channel"
    channel_work_dir="$work_dir/$channel"
    mkdir -p "$channel_work_dir"

    write_wordmark "$channel_dir/mark-small.svg" "$channel_dir/wordmark.svg"

    render_png "$channel_dir/mark.svg" 256 "$channel_dir/mark-256.png"
    render_png "$channel_dir/mark-small.svg" 256 "$channel_dir/mark-small-256.png"

    for size in 16 32 48 64 128 256 512 1024; do
        render_png "$(source_for_size "$channel_dir" "$size")" "$size" "$channel_work_dir/$size.png"
    done

    icon_name="dbflux"
    [[ "$channel" == "nightly" ]] && icon_name="dbflux-nightly"

    icotool --create --output "$packaging_dir/$icon_name.ico" \
        "$channel_work_dir"/{16,32,48,64,128,256}.png

    png2icns "$packaging_dir/$icon_name.icns" \
        "$channel_work_dir"/{16,32,48,128,256,512,1024}.png >/dev/null

    if [[ "$channel" == "stable" ]]; then
        for size in 16 32 48 64 128 256 512; do
            mkdir -p "$packaging_dir/${size}x${size}/apps"
            cp "$channel_work_dir/$size.png" "$packaging_dir/${size}x${size}/apps/dbflux.png"
        done

        cp "$channel_dir/mark-small.svg" "$web_brand_dir/favicon.svg"
        cp "$channel_dir/mark-256.png" "$web_brand_dir/mark-256.png"
        cp "$channel_dir/wordmark.svg" "$web_brand_dir/wordmark.svg"
    fi
done

echo "Brand assets regenerated."
