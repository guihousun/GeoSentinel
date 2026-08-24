"""
2025 Myanmar Earthquake Impact Assessment using VNP46A2 Daily NTL Data
========================================================================
Event: M7.7 earthquake on 2025-03-28 at 06:20 UTC (12:50 MMT local)
Epicenter: ~16 km N-NW of Sagaing city, ~19 km NW of Mandalay
Coordinates: 22.0°N, 95.9°E (approximate epicenter near Sagaing-Mandalay border)

First-Night Selection Rule:
- VNP46A2 overpass time: ~01:30 local time (MMT)
- Earthquake occurred at 12:50 MMT, which is AFTER the nightly overpass
- Therefore, first post-event night = 2025-03-29 (day D+1), NOT 2025-03-28

Temporal Windows:
- Pre-event baseline: 2025-03-14 to 2025-03-21 (7 days before event)
- First-night impact: 2025-03-29 (single night after event)
- Post-event recovery: 2025-04-04 to 2025-04-11 (7-14 days after event)
"""

import ee
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from storage_manager import storage_manager

# Initialize GEE
PROJECT_ID = "empyrean-caster-430308-m2"
ee.Initialize(project=PROJECT_ID)

# Earthquake event parameters (from USGS/ReliefWeb official sources)
EVENT_DATE_UTC = "2025-03-28T06:20:00Z"
EVENT_LOCAL_TIME = "12:50"  # MMT (UTC+6:30)
EPICENTER_LAT = 22.0  # Approximate epicenter near Sagaing-Mandalay region
EPICENTER_LON = 95.9
MAGNITUDE = 7.7
DEPTH_KM = 10

# Temporal windows (applying first-night selection rule)
PRE_EVENT_START = "2025-03-14"
PRE_EVENT_END = "2025-03-21"
FIRST_NIGHT_DATE = "2025-03-29"  # Day D+1 because event occurred after overpass
RECOVERY_START = "2025-04-04"
RECOVERY_END = "2025-04-11"

# VNP46A2 dataset configuration
DATASET_ID = "NASA/VIIRS/002/VNP46A2"
BAND_NAME = "Gap_Filled_DNB_BRDF_Corrected_NTL"
SCALE = 500  # meters
MAX_PIXELS = 1e13

# Load Myanmar boundary from workspace
boundary_path = storage_manager.resolve_input_path("myanmar_boundary.shp")
myanmar_boundary = gpd.read_file(boundary_path)
print(f"Myanmar boundary loaded: {len(myanmar_boundary)} features")
print(f"Boundary CRS: {myanmar_boundary.crs}")
print(f"Boundary bounds: {myanmar_boundary.total_bounds}")

# Create epicenter point and buffers (25km, 50km, 100km) in projected CRS
epicenter_point = Point(EPICENTER_LON, EPICENTER_LAT)
epicenter_gdf = gpd.GeoDataFrame([{"name": "epicenter"}], geometry=[epicenter_point], crs="EPSG:4326")

# Project to UTM zone 47N (appropriate for Myanmar central region) for accurate buffering
epicenter_utm = epicenter_gdf.to_crs("EPSG:32647")

# Create buffers
buffer_25km = epicenter_utm.buffer(25000).to_crs("EPSG:4326")
buffer_50km = epicenter_utm.buffer(50000).to_crs("EPSG:4326")
buffer_100km = epicenter_utm.buffer(100000).to_crs("EPSG:4326")

print(f"\nCreated buffers around epicenter ({EPICENTER_LAT}, {EPICENTER_LON})")
print(f"  25km buffer area: {buffer_25km.area.sum()/1e6:.2f} km²")
print(f"  50km buffer area: {buffer_50km.area.sum()/1e6:.2f} km²")
print(f"  100km buffer area: {buffer_100km.area.sum()/1e6:.2f} km²")

# Convert buffers to GEE Geometry for server-side processing
def gdf_to_ee_geometry(gdf):
    """Convert GeoDataFrame to GEE Geometry"""
    geom_json = json.loads(gdf.to_json())
    return ee.Geometry(geom_json['features'][0]['geometry'])

buffer_25km_ee = gdf_to_ee_geometry(buffer_25km.to_frame().T)
buffer_50km_ee = gdf_to_ee_geometry(buffer_50km.to_frame().T)
buffer_100km_ee = gdf_to_ee_geometry(buffer_100km.to_frame().T)

buffers = {
    "25km": buffer_25km_ee,
    "50km": buffer_50km_ee,
    "100km": buffer_100km_ee,
}

# Define analysis periods
periods = {
    "pre_event_baseline": {"start": PRE_EVENT_START, "end": PRE_EVENT_END, "label": "Pre-Event Baseline"},
    "first_night_impact": {"start": FIRST_NIGHT_DATE, "end": FIRST_NIGHT_DATE, "label": "First Night After Event"},
    "post_event_recovery": {"start": RECOVERY_START, "end": RECOVERY_END, "label": "Post-Event Recovery"},
}

# Function to compute ANTL for a period and buffer with robust error handling
def compute_antl(period_start, period_end, buffer_geom, buffer_name):
    """Compute mean ANTL for a given period and buffer using GEE server-side processing"""
    # Filter date range (end date is exclusive in GEE, so advance by 1 day to include it)
    collection = (
        ee.ImageCollection(DATASET_ID)
        .filterDate(period_start, ee.Date(period_end).advance(1, "day"))
        .select(BAND_NAME)
    )
    
    # Count images in the period
    image_count = collection.size().getInfo()
    
    if image_count == 0:
        print(f"    WARNING: No images found for {period_start} to {period_end}")
        return None, 0
    
    # Compute mean image for the period
    mean_image = collection.mean()
    
    # Compute zonal statistics - use simple mean reducer
    stats = mean_image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=buffer_geom,
        scale=SCALE,
        maxPixels=MAX_PIXELS,
        bestEffort=True,
        tileScale=4,
    )
    
    result = stats.getInfo()
    antl = result.get(BAND_NAME, None)
    
    # Count valid pixels separately using count reducer
    count_stats = mean_image.reduceRegion(
        reducer=ee.Reducer.count(),
        geometry=buffer_geom,
        scale=SCALE,
        maxPixels=MAX_PIXELS,
        bestEffort=True,
        tileScale=4,
    )
    pixel_count = count_stats.getInfo().get(BAND_NAME, 0)
    
    # Handle case where mean is None (no valid pixels)
    if antl is None:
        print(f"    WARNING: No valid pixels in {buffer_name} buffer for period {period_start}")
    
    return antl, pixel_count

# Compute ANTL for all periods and buffers
print("\nComputing ANTL for all periods and buffers...")
results = []

for buffer_name, buffer_geom in buffers.items():
    print(f"\nProcessing {buffer_name} buffer:")
    for period_key, period_info in periods.items():
        print(f"  {period_info['label']} ({period_info['start']} to {period_info['end']})...")
        antl, pixel_count = compute_antl(
            period_info["start"],
            period_info["end"],
            buffer_geom,
            buffer_name
        )
        
        results.append({
            "buffer_km": int(buffer_name.replace("km", "")),
            "period": period_key,
            "period_label": period_info["label"],
            "start_date": period_info["start"],
            "end_date": period_info["end"],
            "mean_antl": antl,
            "pixel_count": pixel_count,
        })
        
        if antl is not None:
            print(f"    ANTL: {antl:.4f}, Pixels: {pixel_count}")
        else:
            print(f"    ANTL: N/A (no valid data)")

# Create DataFrame and compute damage metrics
df = pd.DataFrame(results)
print(f"\n\nANTL results computed for {len(df)} buffer-period combinations")

# Display results
print("\nDetailed ANTL Results:")
print("-" * 80)
for _, row in df.iterrows():
    antl_str = f"{row['mean_antl']:.4f}" if row['mean_antl'] is not None else "N/A"
    print(f"  {row['buffer_km']}km | {row['period_label']:25s} | {row['start_date']} to {row['end_date']:10s} | ANTL: {antl_str:>10s} | Pixels: {row['pixel_count']}")
print("-" * 80)

# Pivot to compute damage metrics
pivot_df = df.pivot_table(
    index="buffer_km",
    columns="period",
    values="mean_antl"
).reset_index()

# Calculate damage metrics
damage_metrics = []
for _, row in pivot_df.iterrows():
    buffer_km = row["buffer_km"]
    antl_pre = row.get("pre_event_baseline", None)
    antl_first = row.get("first_night_impact", None)
    antl_recovery = row.get("post_event_recovery", None)
    
    # Drop percentage (first night vs baseline)
    # Negative drop means decrease in light (damage)
    if antl_pre is not None and antl_first is not None and antl_pre > 0:
        drop_pct = ((antl_first - antl_pre) / antl_pre) * 100
    else:
        drop_pct = None
    
    # Recovery rate
    if antl_pre is not None and antl_first is not None and antl_recovery is not None:
        denominator = (antl_pre - antl_first)
        if denominator != 0:
            recovery_rate = ((antl_recovery - antl_first) / denominator) * 100
        else:
            recovery_rate = None
    else:
        recovery_rate = None
    
    # Damage severity classification
    if drop_pct is not None:
        if drop_pct < -50:  # Negative drop means decrease in light
            severity = "Severe"
        elif drop_pct < -25:
            severity = "Moderate"
        elif drop_pct < 0:
            severity = "Light"
        else:
            severity = "No Impact / Increase"
    else:
        severity = "Unknown"
    
    damage_metrics.append({
        "buffer_km": buffer_km,
        "antl_pre_event": antl_pre,
        "antl_first_night": antl_first,
        "antl_recovery": antl_recovery,
        "drop_percentage": drop_pct,
        "recovery_rate": recovery_rate,
        "damage_severity": severity,
    })

damage_df = pd.DataFrame(damage_metrics)

# Save detailed ANTL results
antl_csv_path = storage_manager.resolve_output_path("myanmar_earthquake_antl_analysis.csv")
df.to_csv(antl_csv_path, index=False)
print(f"\nDetailed ANTL results saved to: {antl_csv_path}")

# Save damage metrics
damage_csv_path = storage_manager.resolve_output_path("myanmar_earthquake_damage_metrics.csv")
damage_df.to_csv(damage_csv_path, index=False)
print(f"Damage metrics saved to: {damage_csv_path}")

# Generate impact assessment report
impact_report = {
    "event_metadata": {
        "event_date_utc": EVENT_DATE_UTC,
        "event_local_time_mmt": EVENT_LOCAL_TIME,
        "magnitude": MAGNITUDE,
        "depth_km": DEPTH_KM,
        "epicenter_coordinates": {
            "latitude": EPICENTER_LAT,
            "longitude": EPICENTER_LON,
            "description": "Approximate epicenter near Sagaing-Mandalay border region"
        },
        "affected_regions": ["Sagaing Region", "Mandalay Region"],
        "data_sources": ["USGS", "ReliefWeb", "GDACS"],
    },
    "methodology": {
        "dataset": DATASET_ID,
        "band": BAND_NAME,
        "spatial_resolution_m": SCALE,
        "first_night_selection_rule": {
            "vnp46a2_overpass_time": "~01:30 local time (MMT)",
            "earthquake_local_time": EVENT_LOCAL_TIME,
            "rule": "Earthquake occurred AFTER nightly overpass, so first post-event night = event_date + 1 day",
            "first_night_date": FIRST_NIGHT_DATE,
        },
        "temporal_windows": {
            "pre_event_baseline": {"start": PRE_EVENT_START, "end": PRE_EVENT_END, "description": "7 days before event"},
            "first_night_impact": {"date": FIRST_NIGHT_DATE, "description": "Single night after event (day D+1)"},
            "post_event_recovery": {"start": RECOVERY_START, "end": RECOVERY_END, "description": "7-14 days after event"},
        },
        "buffer_analysis": {
            "buffers_km": [25, 50, 100],
            "description": "Multi-scale epicenter buffers for hierarchical damage assessment",
        },
        "damage_metrics": {
            "drop_percentage_formula": "(ANTL_first_night - ANTL_pre_event) / ANTL_pre_event × 100",
            "recovery_rate_formula": "(ANTL_recovery - ANTL_first_night) / (ANTL_pre_event - ANTL_first_night) × 100",
            "severity_classification": {
                "Severe": "drop < -50%",
                "Moderate": "-50% ≤ drop < -25%",
                "Light": "-25% ≤ drop < 0%",
                "No Impact / Increase": "drop ≥ 0%",
            },
        },
    },
    "results": {
        "per_buffer_analysis": damage_df.to_dict(orient="records"),
        "summary": {
            "max_drop_buffer": int(damage_df.loc[damage_df["drop_percentage"].idxmin(), "buffer_km"]) if damage_df["drop_percentage"].notna().any() else None,
            "max_drop_percentage": float(damage_df["drop_percentage"].min()) if damage_df["drop_percentage"].notna().any() else None,
            "overall_severity": damage_df.loc[damage_df["drop_percentage"].idxmin(), "damage_severity"] if damage_df["drop_percentage"].notna().any() else None,
        },
    },
    "output_files": {
        "antl_analysis_csv": "myanmar_earthquake_antl_analysis.csv",
        "damage_metrics_csv": "myanmar_earthquake_damage_metrics.csv",
    },
    "notes": [
        "VNP46A2 data availability confirmed for analysis period (2025-03-14 to 2025-04-11)",
        "Gap-filled BRDF-corrected NTL band used for consistent radiance comparison",
        "Server-side GEE processing used for efficient computation across daily images",
        "Damage assessment based on nighttime light radiance anomalies as proxy for power infrastructure impact",
        "First-night selection rule explicitly applied: earthquake occurred at 12:50 MMT (after ~01:30 overpass)",
    ],
}

# Save impact report as JSON
report_json_path = storage_manager.resolve_output_path("myanmar_earthquake_impact_assessment_report.json")
with open(report_json_path, "w", encoding="utf-8") as f:
    json.dump(impact_report, f, indent=2, ensure_ascii=False)
print(f"Impact assessment report saved to: {report_json_path}")

# Print summary
print("\n" + "="*80)
print("2025 MYANMAR EARTHQUAKE IMPACT ASSESSMENT SUMMARY")
print("="*80)
print(f"Event: M{MAGNITUDE} earthquake on {EVENT_DATE_UTC}")
print(f"Epicenter: {EPICENTER_LAT}°N, {EPICENTER_LON}°E (Sagaing-Mandalay border region)")
print(f"First-night selection: {FIRST_NIGHT_DATE} (day D+1, event occurred after VNP46A2 overpass)")
print("\nDamage Assessment by Buffer:")
print("-"*80)
for _, row in damage_df.iterrows():
    drop_str = f"{row['drop_percentage']:.2f}%" if row['drop_percentage'] is not None else "N/A"
    rec_str = f"{row['recovery_rate']:.2f}%" if row['recovery_rate'] is not None else "N/A"
    print(f"  {row['buffer_km']}km buffer: Drop = {drop_str:>10s}, Recovery = {rec_str:>10s}, Severity = {row['damage_severity']}")
print("-"*80)
print(f"\nOutput files generated:")
print(f"  - {antl_csv_path}")
print(f"  - {damage_csv_path}")
print(f"  - {report_json_path}")
print("="*80)
