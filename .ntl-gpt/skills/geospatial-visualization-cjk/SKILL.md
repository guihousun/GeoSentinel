---
name: geospatial-visualization-cjk
description: Use when generating PNG/JPG charts or maps, especially when labels may contain Chinese text or mixed English/Chinese names.
---

# Geospatial Visualization With CJK Text

## Purpose
Generate publication-ready chart and map images that render Chinese labels correctly and remain readable in the Streamlit UI.

## When To Use
- Any Matplotlib, GeoPandas, Seaborn, or Rasterio visualization saved to `/outputs/`.
- Bar charts, choropleth maps, time-series plots, histograms, ranked province/city charts, and NTL raster previews.
- Any figure containing Chinese province, city, district, axis, legend, or title text.

## Required Matplotlib Setup
Put this block near the top of generated scripts before creating figures:

```python
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager


def configure_cjk_font():
    candidates = [
        "Microsoft YaHei",
        "SimHei",
        "SimSun",
        "Noto Sans CJK SC",
        "Source Han Sans SC",
        "Arial Unicode MS",
    ]
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in candidates:
        if name in available:
            plt.rcParams["font.sans-serif"] = [name, "DejaVu Sans"]
            plt.rcParams["font.family"] = "sans-serif"
            break
    else:
        # Keep plots usable even if no CJK font is installed; also print a clear runtime hint.
        plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]
        print("WARNING: No CJK font found. Install Microsoft YaHei, SimHei, or Noto Sans CJK SC for Chinese labels.")
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["savefig.dpi"] = 220
    plt.rcParams["figure.dpi"] = 140


configure_cjk_font()
```

## Figure Rules
- Prefer `fig, ax = plt.subplots(...)`; avoid pyplot-only global plotting.
- Use `fig.tight_layout()` or `fig.subplots_adjust(...)` before saving.
- For many Chinese category labels, prefer horizontal bar charts.
- For horizontal bars, allocate enough left margin: `fig.subplots_adjust(left=0.22, right=0.92)`.
- Save to `/outputs/<descriptive_name>.png` or `outputs/<descriptive_name>.png`.
- Use `bbox_inches="tight"` when saving text-heavy figures.
- Close figures after saving: `plt.close(fig)`.

## Export Pattern

```python
out_path = storage_manager.resolve_output_path("province_bar_population_density.png")
fig.savefig(out_path, dpi=220, bbox_inches="tight", facecolor="white")
plt.close(fig)
print(f"Saved figure: {out_path}")
```

## Quality Gate
Before final answer, confirm:
- The image file exists in `outputs/`.
- Chinese labels are not boxes/tofu characters.
- Axis labels, legends, and value annotations are not clipped.
- Numeric labels use sensible precision and units.
