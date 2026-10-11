"""
Scatter chart from any SQL query against the Databricks lakehouse.

    python charts/scatter.py --sql "SELECT team_abbr, team_logo_espn, off_epa_per_play, def_epa_per_play
                                    FROM nfl.gold.agg_team_season WHERE season = 2026" \
        --x off_epa_per_play --y def_epa_per_play --image team_logo_espn \
        --title "Offense vs defense" --invert-y --out team_epa.png

Where the query runs:
  - Inside Databricks (a job task), on the job's own Spark. Nothing to set.
  - On your laptop, on a SQL warehouse, signed in with your Databricks CLI
    login (the same one the bundle uses). Two settings, as environment
    variables or flags:

    DATABRICKS_CONFIG_PROFILE   your CLI profile name   (databricks auth profiles)
    DATABRICKS_WAREHOUSE_ID     the SQL warehouse's ID  (warehouse page -> Overview)

Points are drawn as images when --image names a column of image URLs (team
logos), otherwise as dots. Text labels are optional; a point whose image
can't be fetched falls back to a dot with its label, so it is never anonymous.
Dashed lines mark the average of the plotted points.
"""

from __future__ import annotations

import argparse
import io
import os
import textwrap
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # render to files; no window needed

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.offsetbox import AnnotationBbox, OffsetImage
from matplotlib.ticker import FuncFormatter
from PIL import Image, ImageFilter

# Databricks sets this on every cluster and serverless environment.
IN_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# One series, light background. Marks carry the series color; all text stays
# in ink tones so labels read the same whatever color the points are.
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#8a8984"
SERIES = "#2a78d6"
GRID = "#e8e7e3"
REFERENCE = "#a8a7a2"

DPI = 150
IMAGE_PT = 28  # each image point fits in a square this many points wide
HALO_PX = 3    # background-colored outline around each image
# Square and compact so it reads on a phone without zooming: an 8-inch-wide
# chart shown at phone width keeps 9pt labels at about 5.5pt.
FIGSIZE = (8, 8)


@dataclass
class ScatterSpec:
    x: str
    y: str
    label: str | None = None
    image: str | None = None    # column of image URLs to draw as the points
    image_size: float = IMAGE_PT  # points; smaller suits charts that also label every point
    label_images: bool = True   # False: label only the points drawn as dots
    title: str = ""
    subtitle: str = ""
    x_label: str | None = None
    y_label: str | None = None
    invert_x: bool = False      # flip an axis where lower is better (defense),
    invert_y: bool = False      # so up and right always mean better
    x_format: str = "number"    # "number", "percent" (0-1 values) or "signed"
    y_format: str = "number"
    mean_lines: bool = True
    caption: str = ""
    figsize: tuple[float, float] = FIGSIZE


def run_query(sql_text: str, profile: str | None = None, warehouse_id: str | None = None) -> pd.DataFrame:
    """Run a query and return the result as a DataFrame."""
    if IN_DATABRICKS:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate().sql(sql_text).toPandas()

    from databricks import sql
    from databricks.sdk.core import Config

    profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE")
    warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        raise SystemExit("Set DATABRICKS_WAREHOUSE_ID, or pass --warehouse-id, to say which SQL warehouse to use.")

    # Reuses the login from `databricks auth login`; no token in code.
    cfg = Config(profile=profile)
    with sql.connect(
        server_hostname=cfg.hostname,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    ) as connection, connection.cursor() as cursor:
        cursor.execute(sql_text)
        return cursor.fetchall_arrow().to_pandas()


_downloads: dict[str, bytes | None] = {}
_images: dict[tuple[str, int], np.ndarray | None] = {}


def _download(url: str) -> bytes | None:
    if url not in _downloads:
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (nfl-analytics charts)"})
            with urllib.request.urlopen(request, timeout=15) as response:
                _downloads[url] = response.read()
        except Exception as exc:  # noqa: BLE001 - any failure just means "draw a dot instead"
            print(f"Couldn't load image {url}: {exc}")
            _downloads[url] = None
    return _downloads[url]


def with_halo(img: Image.Image) -> Image.Image:
    """Outline the artwork's shape in the background color, like the ring on the
    dots: overlapping logos stay separate and grid lines stop short of them."""
    padded = Image.new("RGBA", (img.width + 2 * HALO_PX, img.height + 2 * HALO_PX))
    padded.paste(img, (HALO_PX, HALO_PX))
    outline = padded.getchannel("A").filter(ImageFilter.MaxFilter(2 * HALO_PX + 1))
    halo = Image.new("RGBA", padded.size, SURFACE)
    halo.putalpha(outline)
    return Image.alpha_composite(halo, padded)


def load_image(url: str, size_px: int) -> np.ndarray | None:
    """Fetch an image URL, trimmed and sized to about size_px across. None if it can't be fetched."""
    key = (url, size_px)
    if key not in _images:
        raw = _download(url)
        if raw is None:
            _images[key] = None
            return None
        try:
            img = Image.open(io.BytesIO(raw)).convert("RGBA")
            # Logo files pad the artwork with differing amounts of transparent
            # space; trimming it makes every logo the same visual size.
            img = img.crop(img.getbbox() or (0, 0, *img.size))
            # Same area for every logo, so a wide one (the Chargers bolt) doesn't
            # look tiny beside a round one (the Steelers). Capped so very wide
            # logos stay within 1.5x the nominal size.
            target = size_px - 2 * HALO_PX
            scale = min(target / (img.width * img.height) ** 0.5, 1.5 * target / max(img.size))
            img = img.resize((max(1, round(img.width * scale)), max(1, round(img.height * scale))), Image.LANCZOS)
            _images[key] = np.asarray(with_halo(img))
        except Exception as exc:  # noqa: BLE001 - not an image we can read: draw a dot instead
            print(f"Couldn't read image {url}: {exc}")
            _images[key] = None
    return _images[key]


def _formatter(kind: str):
    if kind == "percent":  # 0.375 -> 37.5%; a fixed 0 decimals would print 38% for that tick
        return FuncFormatter(lambda v, _: f"{v * 100:.4g}%")
    if kind == "signed":
        return FuncFormatter(lambda v, _: f"{v:+g}" if v else "0")
    return FuncFormatter(lambda v, _: f"{v:g}" if v else "0")


# Where place_labels tries each label, in order: right, above, below, left
# (a name to the left of its logo reads as belonging to whatever is left of
# it), then the four corners, each at growing distances (points) from the point.
_DIRECTIONS = [(1, 0), (0, 1), (0, -1), (-1, 0), (1, 1), (1, -1), (-1, 1), (-1, -1)]
_DISTANCES = [3, 12, 22, 34, 48, 64]


def place_labels(ax, points: np.ndarray, halves: np.ndarray, labels: list) -> None:
    """Put each label beside its point without covering any point or another label.

    points: data x, y of every point. halves: each point's half width and height
    in pixels. labels: (point index, Text) pairs. Each label takes the first
    free spot, nearest first; one that lands away from its point gets a thin
    leader line. Deterministic, so the same data always lays out the same way.
    """
    renderer = ax.figure.canvas.get_renderer()
    pt = DPI / 72  # pixels per point
    plot = ax.get_window_extent(renderer)
    # Labels stay inside the plot, except that they may run into the empty
    # margin on the right, so a point at the right edge can still be labelled
    # on its right.
    area = (plot.x0, plot.y0, ax.figure.bbox.x1 - 4, plot.y1)
    centers = ax.transData.transform(points)
    boxes = [(cx - hw - 1, cy - hh - 1, cx + hw + 1, cy + hh + 1) for (cx, cy), (hw, hh) in zip(centers, halves)]

    def free(box) -> bool:
        x0, y0, x1, y1 = box
        if x0 < area[0] or y0 < area[1] or x1 > area[2] or y1 > area[3]:
            return False
        return not any(x0 < bx1 and bx0 < x1 and y0 < by1 and by0 < y1 for bx0, by0, bx1, by1 in boxes)

    def gap(a, b) -> float:
        dx = max(0, b[0] - a[2], a[0] - b[2])
        dy = max(0, b[1] - a[3], a[1] - b[3])
        return float(np.hypot(dx, dy))

    def edge_point(box, i):
        # Where a leader line leaves the label: its edge nearest the point.
        return min(max(centers[i][0], box[0]), box[2]), min(max(centers[i][1], box[1]), box[3])

    def unambiguous(box, i, ring) -> bool:
        # Closer to its own point than to any other, so it can't be misread...
        own = gap(box, boxes[i])
        points = [b for j, b in enumerate(boxes[: len(centers)]) if j != i]
        if any(gap(box, b) <= own + 6 * pt for b in points):
            return False
        if ring == 0:
            return True
        # ...and a leader line that doesn't pass over another point.
        (ex, ey), (cx, cy) = edge_point(box, i), centers[i]
        samples = [(ex + (cx - ex) * t, ey + (cy - ey) * t) for t in np.linspace(0, 1, 24)]
        return not any(b[0] <= sx <= b[2] and b[1] <= sy <= b[3] for b in points for sx, sy in samples)

    # Crowded points first, while there is still room around them.
    def neighbours(i):
        return int((np.hypot(*(centers - centers[i]).T) < 70 * pt).sum())

    for i, text in sorted(labels, key=lambda pair: -neighbours(pair[0])):
        text.set_horizontalalignment("left")
        text.set_verticalalignment("bottom")
        extent = text.get_window_extent(renderer)
        w, h = extent.width + 2, extent.height + 2
        (cx, cy), (hw, hh) = centers[i], halves[i]
        candidates = []  # (x0, y0, ring) in preference order
        for ring, distance in enumerate(_DISTANCES):
            d = distance * pt
            for dx, dy in _DIRECTIONS:
                x0 = cx + hw + d if dx > 0 else cx - hw - d - w if dx < 0 else cx - w / 2
                y0 = cy + hh + d if dy > 0 else cy - hh - d - h if dy < 0 else cy - h / 2
                candidates.append((x0, y0, ring))
        # Best: free and clearly its own. Then: just free. Last resort: right of the point.
        spot = next((c for c in candidates if free((c[0], c[1], c[0] + w, c[1] + h))
                     and unambiguous((c[0], c[1], c[0] + w, c[1] + h), i, c[2])), None) \
            or next((c for c in candidates if free((c[0], c[1], c[0] + w, c[1] + h))), None) \
            or candidates[0]
        x0, y0, ring = spot
        far = ring > 0
        boxes.append((x0, y0, x0 + w, y0 + h))
        to_data = ax.transData.inverted()
        text.set_position(to_data.transform((x0 + 1, y0 + 1)))
        if far:
            # From the label's nearest edge to the point's center, drawn under
            # the point so the image hides the end of it.
            ex, ey = edge_point((x0, y0, x0 + w, y0 + h), i)
            ax.annotate(
                "", xy=to_data.transform((cx, cy)), xytext=to_data.transform((ex, ey)),
                arrowprops=dict(arrowstyle="-", color=REFERENCE, linewidth=0.6, shrinkA=1, shrinkB=0),
                zorder=2,
            )


def scatter(df: pd.DataFrame, spec: ScatterSpec, out_path: str | Path) -> Path:
    """Draw spec's x and y from df as a scatter and save it as a PNG."""
    needed = [c for c in (spec.x, spec.y, spec.label, spec.image) if c]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Query result has no column(s) {missing}. Columns returned: {list(df.columns)}")

    data = df.dropna(subset=[spec.x, spec.y]).reset_index(drop=True)
    if data.empty:
        raise SystemExit("The query returned no rows with both x and y values.")
    if len(data) < len(df):
        print(f"Skipped {len(df) - len(data)} row(s) with no {spec.x} or {spec.y}.")
    x = data[spec.x].astype(float)
    y = data[spec.y].astype(float)

    fig, ax = plt.subplots(figsize=spec.figsize, dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    if spec.mean_lines:
        ax.axvline(x.mean(), color=REFERENCE, linestyle="--", linewidth=1, zorder=1)
        ax.axhline(y.mean(), color=REFERENCE, linestyle="--", linewidth=1, zorder=1)

    # One image per point where available; None marks a point drawn as a dot.
    size_px = round(spec.image_size * DPI / 72)
    images = [
        load_image(url, size_px) if spec.image and isinstance(url, str) and url else None
        for url in (data[spec.image] if spec.image else [None] * len(data))
    ]
    as_image = np.array([img is not None for img in images])

    # Half the width and height of each point in pixels: its image, or a dot.
    halves = np.array([(img.shape[1] / 2, img.shape[0] / 2) if img is not None else (9, 9) for img in images])
    ax.scatter(x, y, s=0)  # images don't widen the axis range on their own; this does
    for xi, yi, img in zip(x, y, images):
        if img is not None:
            ax.add_artist(AnnotationBbox(
                OffsetImage(img, zoom=1, dpi_cor=False), (xi, yi), frameon=False, pad=0, zorder=3,
            ))
    # A ring in the background color keeps overlapping dots distinguishable.
    ax.scatter(x[~as_image], y[~as_image], s=70, color=SERIES, edgecolors=SURFACE, linewidths=1.5, zorder=3)

    labels = []  # (point index, text)
    if spec.label:
        show = ~as_image | spec.label_images
        labels = [
            (i, ax.text(x[i], y[i], str(name), fontsize=9, color=INK_SECONDARY, zorder=4))
            for i, name in enumerate(data[spec.label]) if show[i]
        ]

    if spec.invert_x:
        ax.invert_xaxis()
    if spec.invert_y:
        ax.invert_yaxis()

    ax.xaxis.set_major_formatter(_formatter(spec.x_format))
    ax.yaxis.set_major_formatter(_formatter(spec.y_format))
    ax.set_xlabel(spec.x_label or spec.x, color=INK_SECONDARY, fontsize=10, labelpad=8)
    ax.set_ylabel(spec.y_label or spec.y, color=INK_SECONDARY, fontsize=10, labelpad=8)
    ax.tick_params(colors=INK_MUTED, labelsize=9, length=0)
    ax.grid(color=GRID, linewidth=0.6, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)

    # Wrap the long lines to the chart's width (about 13 characters per inch at
    # 10pt, 17 at 7.5pt) and leave room above the plot for every subtitle line.
    width_in = spec.figsize[0]
    subtitle = textwrap.wrap(spec.subtitle, int(width_in * 12))
    caption = textwrap.wrap(spec.caption, int(width_in * 16.5))
    ax.set_title(spec.title, loc="left", fontsize=14, fontweight="bold", color=INK, pad=16 + 13 * len(subtitle))
    if subtitle:
        ax.text(0, 1.02, "\n".join(subtitle), transform=ax.transAxes, fontsize=10, color=INK_SECONDARY,
                va="bottom", linespacing=1.3)
    if caption:
        fig.text(0.01, 0.01, "\n".join(caption), fontsize=7.5, color=INK_MUTED, va="bottom", linespacing=1.3)

    caption_space = (10 * len(caption) + 6) / (spec.figsize[1] * 72) if caption else 0
    fig.tight_layout(rect=(0, caption_space, 1, 1))

    # Labels go last, once the layout is final, so positions measured now hold
    # in the saved file.
    if labels:
        place_labels(ax, np.column_stack([x, y]), halves, labels)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=SURFACE)
    plt.close(fig)
    print(f"Saved {out_path} ({len(data)} points)")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Scatter chart from a SQL query.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sql", help="SQL query text")
    source.add_argument("--sql-file", help="path to a .sql file")
    parser.add_argument("--x", required=True, help="column for the x axis")
    parser.add_argument("--y", required=True, help="column for the y axis")
    parser.add_argument("--label", help="column to label each point with")
    parser.add_argument("--image", help="column of image URLs (e.g. team_logo_espn) to draw as the points")
    parser.add_argument("--no-image-labels", action="store_true",
                        help="with --image, label only points whose image couldn't load")
    parser.add_argument("--title", default="")
    parser.add_argument("--subtitle", default="")
    parser.add_argument("--x-label")
    parser.add_argument("--y-label")
    parser.add_argument("--invert-x", action="store_true", help="flip x so lower values sit right")
    parser.add_argument("--invert-y", action="store_true", help="flip y so lower values sit on top")
    parser.add_argument("--x-format", choices=["number", "percent", "signed"], default="number")
    parser.add_argument("--y-format", choices=["number", "percent", "signed"], default="number")
    parser.add_argument("--no-mean-lines", action="store_true")
    parser.add_argument("--caption", default="")
    parser.add_argument("--out", default="charts/output/scatter.png")
    parser.add_argument("--profile", help="Databricks CLI profile (default: $DATABRICKS_CONFIG_PROFILE)")
    parser.add_argument("--warehouse-id", help="SQL warehouse ID (default: $DATABRICKS_WAREHOUSE_ID)")
    args = parser.parse_args()

    sql_text = args.sql or Path(args.sql_file).read_text()
    spec = ScatterSpec(
        x=args.x, y=args.y, label=args.label, image=args.image, label_images=not args.no_image_labels,
        title=args.title, subtitle=args.subtitle, x_label=args.x_label, y_label=args.y_label,
        invert_x=args.invert_x, invert_y=args.invert_y, x_format=args.x_format, y_format=args.y_format,
        mean_lines=not args.no_mean_lines, caption=args.caption,
    )
    scatter(run_query(sql_text, args.profile, args.warehouse_id), spec, args.out)


if __name__ == "__main__":
    main()
