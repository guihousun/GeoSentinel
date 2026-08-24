import ee
import pandas as pd
import geopandas as gpd
from storage_manager import storage_manager

# AOI_CONFIRMED_BY_USER: Boundary from Data_Searcher (wuhan_boundary.shp for 武汉市)
PROJECT_ID = "empyrean-caster-430308-m2"
ee.Initialize(project=PROJECT_ID)

# Load Wuhan boundary
boundary_path = storage_manager.resolve_input_path('wuhan_boundary.shp')
gdf = gpd.read_file(boundary_path)
region = ee.Geometry.Rectangle(gdf.total_bounds.tolist())

# Load VNP46A2 collection for the full date range (2019-01-23 to 2020-04-08)
collection = (
    ee.ImageCollection("NASA/VIIRS/002/VNP46A2")
    .filterDate("2019-01-23", "2020-04-09")
    .filterBounds(region)
    .select("Gap_Filled_DNB_BRDF_Corrected_NTL")
)

def per_image_stat(img):
    """Compute mean NTL for each image over the region."""
    value = img.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=region,
        scale=500,
        maxPixels=1e13,
        bestEffort=True,
    ).get("Gap_Filled_DNB_BRDF_Corrected_NTL")
    return ee.Feature(None, {
        "date": img.date().format("YYYY-MM-dd"),
        "value": value,
    })

# Map over the entire collection (server-side)
stats_fc = ee.FeatureCollection(collection.map(per_image_stat))

# Download results to client
rows = [f["properties"] for f in stats_fc.getInfo()["features"]]

# Create DataFrame and sort by date
df = pd.DataFrame(rows).sort_values("date")

# Define lockdown and comparison periods
lockdown_2020_start = "2020-01-23"
lockdown_2020_end = "2020-04-08"
comparison_2019_start = "2019-01-23"
comparison_2019_end = "2019-04-08"

# Filter for lockdown period (2020)
df_lockdown = df[(df["date"] >= lockdown_2020_start) & (df["date"] <= lockdown_2020_end)]
antl_lockdown = df_lockdown["value"].mean()

# Filter for comparison period (2019)
df_comparison = df[(df["date"] >= comparison_2019_start) & (df["date"] <= comparison_2019_end)]
antl_comparison = df_comparison["value"].mean()

# Calculate percentage change
pct_change = ((antl_lockdown - antl_comparison) / antl_comparison) * 100

# Add period labels
df["period"] = "other"
df.loc[(df["date"] >= lockdown_2020_start) & (df["date"] <= lockdown_2020_end), "period"] = "lockdown_2020"
df.loc[(df["date"] >= comparison_2019_start) & (df["date"] <= comparison_2019_end), "period"] = "comparison_2019"

# Save full daily data to CSV
out_csv = storage_manager.resolve_output_path("wuhan_antl_lockdown_analysis.csv")
df.to_csv(out_csv, index=False)
print(f"Full daily data saved to: {out_csv}")

# Print summary statistics
print(f"\n=== Wuhan ANTL Lockdown Analysis ===")
print(f"Lockdown period (2020-01-23 to 2020-04-08): {len(df_lockdown)} days")
print(f"  ANTL (mean): {antl_lockdown:.4f}")
print(f"\nComparison period (2019-01-23 to 2019-04-08): {len(df_comparison)} days")
print(f"  ANTL (mean): {antl_comparison:.4f}")
print(f"\nPercentage change (lockdown vs comparison): {pct_change:.2f}%")
print(f"  (Negative value indicates decrease in nighttime light during lockdown)")
