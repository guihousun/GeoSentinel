import ee
import pandas as pd
import geopandas as gpd
import json
from storage_manager import storage_manager

# Initialize GEE
PROJECT_ID = "empyrean-caster-430308-m2"
ee.Initialize(project=PROJECT_ID)

# Load Shanghai districts boundary from local workspace
boundary_path = storage_manager.resolve_input_path('shanghai_districts_boundary.shp')
gdf = gpd.read_file(boundary_path)

# Convert GeoDataFrame to GeoJSON and then to ee.FeatureCollection
geojson_str = gdf.to_json()
fc = ee.FeatureCollection(json.loads(geojson_str))

# Get NPP-VIIRS-like annual NTL for 2020
ntl_collection = ee.ImageCollection("projects/sat-io/open-datasets/npp-viirs-ntl")
image_2020 = ntl_collection.filterDate("2020-01-01", "2020-12-31").select("b1").first()

# Calculate ANTL (mean) for each district using zonal statistics
# Using tileScale for better performance on multiple regions
stats_fc = image_2020.reduceRegions(
    collection=fc,
    reducer=ee.Reducer.mean().setOutputs(['ANTL']),
    scale=500,
    maxPixelsPerRegion=1e13,
    tileScale=4,
)

# Get results and convert to DataFrame
result_list = stats_fc.getInfo()['features']
rows = []
for feat in result_list:
    props = feat['properties']
    rows.append(props)

df = pd.DataFrame(rows)

# Save to CSV
out_csv = storage_manager.resolve_output_path("shanghai_districts_antl_2020.csv")
df.to_csv(out_csv, index=False)
print(f"Output saved to: {out_csv}")
print(df)
