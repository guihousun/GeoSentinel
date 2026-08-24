import ee
import geopandas as gpd
import pandas as pd
from storage_manager import storage_manager

# AOI_CONFIRMED_BY_USER: Wuhan boundary loaded from Data_Searcher-confirmed wuhan_boundary.shp

# Initialize GEE
PROJECT_ID = "empyrean-caster-430308-m2"
ee.Initialize(project=PROJECT_ID)

# Load confirmed boundary file
boundary_path = storage_manager.resolve_input_path("wuhan_boundary.shp")
gdf = gpd.read_file(boundary_path)
print(f"Boundary loaded: {len(gdf)} features")
print(f"CRS: {gdf.crs}")

# Get bounds from boundary
bounds = gdf.total_bounds
region = ee.Geometry.Rectangle([bounds[0], bounds[1], bounds[2], bounds[3]])
print(f"Region bounds: {bounds}")

# Define date ranges
lockdown_2019_start = "2019-01-23"
lockdown_2019_end = "2019-04-08"
lockdown_2020_start = "2020-01-23"
lockdown_2020_end = "2020-04-08"

# Create a single feature for Wuhan region
wuhan_feature = ee.Feature(region, {'name': 'Wuhan'})
features = ee.FeatureCollection([wuhan_feature])

# Calculate zonal statistics for 2019 lockdown period using GEE reduceRegions
print("Calculating zonal statistics for 2019 lockdown period...")
image_2019 = (
    ee.ImageCollection("NASA/VIIRS/002/VNP46A2")
    .filterDate(lockdown_2019_start, lockdown_2019_end)
    .select("Gap_Filled_DNB_BRDF_Corrected_NTL")
    .mean()
)

stats_2019 = image_2019.reduceRegions(
    collection=features,
    reducer=ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True).combine(ee.Reducer.minMax(), sharedInputs=True),
    scale=500,
    crs='EPSG:4326',
    tileScale=4
)

# Get results
result_2019 = stats_2019.first().getInfo()
print("2019 stats retrieved")

# Calculate zonal statistics for 2020 lockdown period
print("Calculating zonal statistics for 2020 lockdown period...")
image_2020 = (
    ee.ImageCollection("NASA/VIIRS/002/VNP46A2")
    .filterDate(lockdown_2020_start, lockdown_2020_end)
    .select("Gap_Filled_DNB_BRDF_Corrected_NTL")
    .mean()
)

stats_2020 = image_2020.reduceRegions(
    collection=features,
    reducer=ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True).combine(ee.Reducer.minMax(), sharedInputs=True),
    scale=500,
    crs='EPSG:4326',
    tileScale=4
)

result_2020 = stats_2020.first().getInfo()
print("2020 stats retrieved")

# Extract statistics
def extract_stats(result):
    props = result['properties']
    return {
        'mean': props.get('mean'),
        'std': props.get('stdDev'),
        'min': props.get('min'),
        'max': props.get('max')
    }

stats_2019_dict = extract_stats(result_2019)
stats_2020_dict = extract_stats(result_2020)

# Create comparison DataFrame
comparison_data = {
    'period': ['2019_lockdown', '2020_lockdown'],
    'start_date': [lockdown_2019_start, lockdown_2020_start],
    'end_date': [lockdown_2019_end, lockdown_2020_end],
    'mean_ntl': [stats_2019_dict['mean'], stats_2020_dict['mean']],
    'std_ntl': [stats_2019_dict['std'], stats_2020_dict['std']],
    'min_ntl': [stats_2019_dict['min'], stats_2020_dict['min']],
    'max_ntl': [stats_2019_dict['max'], stats_2020_dict['max']]
}

df = pd.DataFrame(comparison_data)

# Calculate change percentage
if stats_2019_dict['mean'] and stats_2019_dict['mean'] != 0:
    change_pct = ((stats_2020_dict['mean'] - stats_2019_dict['mean']) / stats_2019_dict['mean']) * 100
else:
    change_pct = None

print(f"\n=== ANTL Comparison Results ===")
print(f"2019 Lockdown Period (Jan 23 - Apr 8): Mean NTL = {stats_2019_dict['mean']:.4f}")
print(f"2020 Lockdown Period (Jan 23 - Apr 8): Mean NTL = {stats_2020_dict['mean']:.4f}")
if change_pct is not None:
    print(f"Change: {change_pct:.2f}%")

# Save to CSV
out_csv = storage_manager.resolve_output_path("wuhan_antlr_lockdown_comparison.csv")
df.to_csv(out_csv, index=False)
print(f"\nComparison results saved to: {out_csv}")
