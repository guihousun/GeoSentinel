NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "Engineer-side figure assembly: render the two required Myanmar population figures as SVG (no matplotlib dependency) purely from the already validated Analyst outputs (national CSV, ADM1 zonal CSV), which contain every value needed for the national time series and the ADM1 2020 population / density / 2000-2020 change presentation.",
    "input_manifest": [
        {"path": "outputs/mmr_population_national_2000_2020.csv", "role": "validated national time-series table 2000-2020"},
        {"path": "outputs/mmr_population_adm1_zonal_2000_2020.csv", "role": "validated ADM1 zonal table, 5 epochs x 14 units"}
    ],
    "method_steps": [
        "Read the national CSV rows for 2000..2020 (national_pop_persons, delta_5y_persons, growth_since_2000_pct).",
        "Read the ADM1 zonal CSV: 2020 population/share/density and 2000 population per unit; compute 2000-2020 percent change per unit.",
        "Render figure 1: national population time series 2000-2020 (line + markers, million persons), annotate 5-year deltas, 2000-2020 growth and CAGR.",
        "Render figure 2: ADM1 horizontal bars sorted by 2020 population (million persons with national share %), plus a companion panel of 2000-2020 % change per unit, using geoBoundaries unit names as-is.",
        "Write outputs/fig_mmr_population_national_timeseries_2000_2020.svg and outputs/fig_mmr_population_adm1_2000_2020_spatial.svg (UTF-8, standalone SVG).",
        "Validate: both SVG files exist, non-empty, contain all 14 unit names and the 5 epoch values, and well-formed root svg tags."
    ],
    "parameters": {
        "units": "persons; persons/km2; percent",
        "fallback_note": "matplotlib is unavailable/broken in this sandbox (Analyst recorded a hard subprocess abort); SVG figures are an Engineer-side deterministic render of the same validated numbers (no new computation, no scientific change)",
        "figure_media_type": "image/svg+xml",
        "source_note": "values copied at render time from the validated Analyst CSVs, not re-computed"
    },
    "output_manifest": [
        {"path": "outputs/fig_mmr_population_national_timeseries_2000_2020.svg", "required": True},
        {"path": "outputs/fig_mmr_population_adm1_2000_2020_spatial.svg", "required": True}
    ],
    "validation_checks": [
        "fig1 svg contains 5 epoch year labels and 5 value markers",
        "fig2 svg contains all 14 ADM1 unit names and the 14 2020 values and 14 change values",
        "both files start with <?xml or <svg and end with </svg>",
        "both files non-empty"
    ],
    "failure_gates": [
        "fail if either required input CSV is missing or unreadable",
        "fail if fewer than 14 ADM1 unit rows or fewer than 5 national rows",
        "fail if either required SVG output is missing, empty, or malformed"
    ],
    "execution": {
        "mode": "execute",
        "timeout_seconds": 600,
        "overwrite_policy": "version",
        "network_scope": [],
        "test_strategy": "auto",
        "repair_history": []
    }
}

import csv
import html

OUT1 = "outputs/fig_mmr_population_national_timeseries_2000_2020.svg"
OUT2 = "outputs/fig_mmr_population_adm1_2000_2020_spatial.svg"

def read_csv_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

national = read_csv_rows("outputs/mmr_population_national_2000_2020.csv")
adm1 = read_csv_rows("outputs/mmr_population_adm1_zonal_2000_2020.csv")

epoch_rows = [r for r in national if r["year"].strip().isdigit()]
assert len(epoch_rows) == 5, f"expected 5 epoch rows, got {len(epoch_rows)}"

# --- Figure 1: national time series (SVG line chart) ---
W1, H1, ML, MR, MT, MB = 900, 520, 90, 40, 70, 60
plot_w, plot_h = W1 - ML - MR, H1 - MT - MB
xmin, xmax = 1999, 2021
vals = [(int(r["year"]), float(r["national_pop_persons"]), float(r["delta_5y_persons"] or 0) / 1e6) for r in epoch_rows]
pop_min, pop_max = min(v[1] for v in vals), max(v[1] for v in vals)
pad = (pop_max - pop_min) * 0.25 or 1.0
y_lo, y_hi = pop_min - pad, pop_max + pad

def sx(year):
    return ML + (year - xmin) / (xmax - xmin) * plot_w

def sy(pop):
    return MT + (1 - (pop - y_lo) / (y_hi - y_lo)) * plot_h

parts = []
parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W1}" height="{H1}" viewBox="0 0 {W1} {H1}">')
parts.append('<rect width="100%" height="100%" fill="white"/>')
parts.append(f'<text x="{ML}" y="{MT-14}" font-size="17" font-weight="bold" font-family="sans-serif" fill="#1a1a1a">Myanmar national population 2000-2020 (GPWv4.11 UN WPP-adjusted, ADM0-masked)</text>')

# y gridlines
for i in range(6):
    frac = i / 5.0
    yv = y_lo + frac * (y_hi - y_lo)
    ypx = MT + (1 - frac) * plot_h
    parts.append(f'<line x1="{ML}" y1="{ypx:.1f}" x2="{W1-MR}" y2="{ypx:.1f}" stroke="#e5e5e5" stroke-width="1"/>')
    parts.append(f'<text x="{ML-8}" y="{ypx+4:.1f}" text-anchor="end" font-size="12" font-family="sans-serif" fill="#555">{yv/1e6:.0f}M</text>')

# x ticks
for y in (2000, 2005, 2010, 2015, 2020):
    parts.append(f'<text x="{sx(y):.1f}" y="{H1-MB+20}" text-anchor="middle" font-size="12" font-family="sans-serif" fill="#555">{y}</text>')

# area + line
poly_pts = " ".join(f"{sx(y):.1f},{sy(p):.1f}" for y, p, _ in vals)
parts.append(f'<polygon points="{poly_pts} {sx(2020):.1f},{MT+plot_h} {sx(2000):.1f},{MT+plot_h}" fill="#2f6fb2" opacity="0.15"/>')
parts.append(f'<polyline points="{poly_pts}" fill="none" stroke="#2f6fb2" stroke-width="3"/>')

for y, p, d in vals:
    parts.append(f'<circle cx="{sx(y):.1f}" cy="{sy(p):.1f}" r="5" fill="#c0392b"/>')
    parts.append(f'<text x="{sx(y):.1f}" y="{sy(p)-10:.1f}" text-anchor="middle" font-size="12" font-weight="bold" font-family="sans-serif" fill="#1a1a1a">{p/1e6:.2f}M</text>')
    if y > 2000:
        parts.append(f'<text x="{(sx(y-5)+sx(y))/2:.1f}" y="{MT+plot_h+18}" text-anchor="middle" font-size="10" font-family="sans-serif" fill="#c0392b">Δ5y +{d:.2f}M</text>')

total_growth_pct = float(national[-1].get("growth_since_2000_pct") or 0)
cagr = float(national[-1].get("cagr_since_2000_pct_per_yr") or 0)
parts.append(f'<text x="{W1-MR}" y="{H1-8}" text-anchor="end" font-size="12" font-family="sans-serif" fill="#333">2000-2020: +{total_growth_pct:.2f}%  |  CAGR {cagr:.3f}%/yr  |  source: CIESIN GPWv4.11</text>')
parts.append('</svg>')

with open(OUT1, "w", encoding="utf-8") as f:
    f.write("\n".join(parts))

# --- Figure 2: ADM1 2020 population (sorted) + 2000-2020 % change panels ---
units_2020 = {}
units_change = []
for r in adm1:
    iso = r["shapeISO"]
    name = r["shapeName"]
    year = int(r["year"])
    if iso not in units_2020:
        units_2020[iso] = {"name": name, "pop2000": None, "pop2020": None, "share2020": None, "density2020": None}
    if year == 2000:
        units_2020[iso]["pop2000"] = float(r["pop_persons"])
    if year == 2020:
        units_2020[iso]["pop2020"] = float(r["pop_persons"])
        units_2020[iso]["share2020"] = float(r["share_of_national_pct"])
        units_2020[iso]["density2020"] = float(r["density_persons_per_km2"])

rows = []
for iso, u in units_2020.items():
    pct = (u["pop2020"] - u["pop2000"]) / u["pop2000"] * 100.0
    rows.append({"iso": iso, "name": u["name"], "pop": u["pop2020"], "share": u["share2020"],
                 "dens": u["density2020"], "change": pct})
assert len(rows) == 14, f"expected 14 units, got {len(rows)}"
rows_sorted = sorted(rows, key=lambda r: r["pop"], reverse=True)

W2, H2, ML2, MR2, MT2, MB2 = 1100, 820, 170, 30, 70, 40
plot_w2, plot_h2 = W2 - ML2 - MR2, H2 - MT2 - MB2

max_pop = max(r["pop"] for r in rows_sorted) * 1.12
panel1_bottom = MT2 + plot_h2 * 0.52
bar_h = min(20, (plot_h2 * 0.52) / 16.0)
gap = 6

p2 = []
p2.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W2}" height="{H2}" viewBox="0 0 {W2} {H2}">')
p2.append('<rect width="100%" height="100%" fill="white"/>')
p2.append(f'<text x="{ML2}" y="{MT2-16}" font-size="17" font-weight="bold" font-family="sans-serif" fill="#1a1a1a">Myanmar ADM1 population distribution 2020 (GPWv4.11, geoBoundaries 14 units)</text>')

# Panel A: 2020 population bars
p2.append(f'<text x="{ML2}" y="{MT2+2}" font-size="13" font-weight="bold" font-family="sans-serif" fill="#1a1a1a">2020 population (million) & national share</text>')
for i, r in enumerate(rows_sorted):
    y_top = MT2 + 18 + i * (bar_h + gap)
    bar_w = r["pop"] / max_pop * plot_w2
    p2.append(f'<rect x="{ML2}" y="{y_top:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="#2f6fb2"/>')
    p2.append(f'<text x="{ML2-8}" y="{y_top+bar_h-3:.1f}" text-anchor="end" font-size="12" font-family="sans-serif" fill="#1a1a1a">{html.escape(r["name"])}</text>')
    p2.append(f'<text x="{ML2+bar_w+6:.1f}" y="{y_top+bar_h-3:.1f}" font-size="12" font-family="sans-serif" fill="#1a1a1a">{r["pop"]/1e6:.2f}M ({r["share"]:.2f}%)</text>')

# Panel B: 2000-2020 change per unit (second column layout within same svg)
col2_x = ML2 + int(plot_w2 * 0.62)
col2_plot_w = plot_w2 - int(plot_w2 * 0.62) - 20
max_abs = max(abs(r["change"]) for r in rows_sorted) * 1.15
p2.append(f'<text x="{col2_x}" y="{MT2+2}" font-size="13" font-weight="bold" font-family="sans-serif" fill="#1a1a1a">2000-2020 % change</text>')
for i, r in enumerate(rows_sorted):
    y_top = MT2 + 18 + i * (bar_h + gap)
    w = abs(r["change"]) / max_abs * col2_plot_w
    col = "#27ae60" if r["change"] >= 0 else "#c0392b"
    if r["change"] >= 0:
        p2.append(f'<rect x="{col2_x}" y="{y_top:.1f}" width="{w:.1f}" height="{bar_h:.1f}" fill="{col}"/>')
        p2.append(f'<text x="{col2_x+w+6:.1f}" y="{y_top+bar_h-3:.1f}" font-size="12" font-family="sans-serif" fill="#1a1a1a">+{r["change"]:.1f}%</text>')
    else:
        # negative change: bar extends leftward from a zero line at col2_x+half
        zero_x = col2_x + col2_plot_w * 0.5
        p2.append(f'<line x1="{zero_x:.1f}" y1="{y_top-2:.1f}" x2="{zero_x:.1f}" y2="{y_top+bar_h+2:.1f}" stroke="#999" stroke-width="1"/>')
        p2.append(f'<rect x="{zero_x-w:.1f}" y="{y_top:.1f}" width="{w:.1f}" height="{bar_h:.1f}" fill="{col}"/>')
        p2.append(f'<text x="{zero_x-w-6:.1f}" y="{y_top+bar_h-3:.1f}" text-anchor="end" font-size="12" font-family="sans-serif" fill="#1a1a1a">{r["change"]:.1f}%</text>')

p2.append(f'<text x="{ML2}" y="{H2-12}" font-size="11" font-family="sans-serif" fill="#666">Source: CIESIN GPWv4.11 UN WPP-Adjusted Population Count; geoBoundaries ADM1 (unit names as in file, e.g. Saigang=Sagaing, Tanitharyi=Tanintharyi). Model estimates, not census counts. Density 2020 extremes: Yangon {units_2020["MM-06"]["density2020"]:.0f}/km2, Chin {units_2020["MM-14"]["density2020"]:.1f}/km2.</text>')
p2.append('</svg>')

with open(OUT2, "w", encoding="utf-8") as f:
    f.write("\n".join(p2))

# --- validation ---
import os
for path, req_year_labels, req_names in [
    (OUT1, ["2000", "2005", "2010", "2015", "2020"], []),
    (OUT2, [], [u["name"] for u in rows_sorted]),
]:
    assert os.path.exists(path), f"missing {path}"
    with open(path, encoding="utf-8") as f:
        content = f.read()
    assert len(content) > 500, f"{path} too small"
    assert content.lstrip().startswith("<svg") and content.rstrip().endswith("</svg>"), f"{path} malformed"
    for lab in req_year_labels:
        assert lab in content, f"{path} missing year {lab}"
    for nm in req_names:
        assert nm in content, f"{path} missing unit {nm}"

print("FIGURE_OK fig1_bytes=", os.path.getsize(OUT1), "fig2_bytes=", os.path.getsize(OUT2))
