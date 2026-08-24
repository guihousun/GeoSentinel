import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
import os

# Files are in the debug workspace
debug_base = r'E:\NTL-GPT-Clone\user_data\debug\inputs'
raster_path = os.path.join(debug_base, 'shanghai_ntl_2022.tif')
vector_path = os.path.join(debug_base, 'shanghai_districts_boundary.shp')

# Verify files exist
if not os.path.exists(raster_path):
    raise FileNotFoundError(f"Raster not found: {raster_path}")
if not os.path.exists(vector_path):
    raise FileNotFoundError(f"Vector not found: {vector_path}")

# Output to current workspace
from storage_manager import storage_manager
out_csv = storage_manager.resolve_output_path('shanghai_district_stats_2022.csv')
summary_path = storage_manager.resolve_output_path('brightest_district_summary.csv')

# Read raster
with rasterio.open(raster_path) as src:
    arr = src.read(1)
    nodata = src.nodata
    transform = src.transform
    
    # Read vector and reproject to match raster CRS
    gdf = gpd.read_file(vector_path).to_crs(src.crs)
    
    # Calculate pixel area (for LArea if needed)
    pixel_area = abs(transform.a * transform.e)
    
    rows = []
    for _, row in gdf.iterrows():
        geom = [row.geometry]
        # Create mask: True for pixels inside geometry
        mask = geometry_mask(geom, transform=transform, invert=False, out_shape=arr.shape)
        vals = arr[mask]
        
        # Filter nodata and invalid values
        if nodata is not None:
            vals = vals[vals != nodata]
        vals = vals[np.isfinite(vals)]
        
        # Calculate statistics
        an = float(vals.mean()) if vals.size > 0 else np.nan  # ANTL
        tn = float(vals.sum()) if vals.size > 0 else np.nan  # TNL
        area = float(vals.size * pixel_area) if vals.size > 0 else 0.0
        max_val = float(vals.max()) if vals.size > 0 else np.nan
        min_val = float(vals.min()) if vals.size > 0 else np.nan
        std_val = float(vals.std()) if vals.size > 1 else np.nan
        
        rows.append({
            'Name': row.get('Name', 'unknown'),
            'AdCode': row.get('AdCode', None),
            'ANTL': an,
            'TNL': tn,
            'Area': area,
            'Max': max_val,
            'Min': min_val,
            'StdDev': std_val,
        })

# Save detailed stats
df = pd.DataFrame(rows)
df.to_csv(out_csv, index=False)
print(f"Statistics saved to: {out_csv}")

# Find brightest district
brightest = df.loc[df['ANTL'].idxmax()]
print(f"\n=== RESULT ===")
print(f"Brightest district in Shanghai (2022): {brightest['Name']}")
print(f"Maximum ANTL: {brightest['ANTL']:.2f}")

# Save summary
summary_df = pd.DataFrame([{
    'brightest_district': brightest['Name'],
    'max_ANTL': brightest['ANTL'],
    'total_districts': len(df),
    'mean_ANTL': df['ANTL'].mean(),
    'std_ANTL': df['ANTL'].std()
}])
summary_df.to_csv(summary_path, index=False)
print(f"Summary saved to: {summary_path}")

# Print top 5
print("\nTop 5 districts by ANTL:")
top5 = df.nlargest(5, 'ANTL')[['Name', 'ANTL']]
for _, row in top5.iterrows():
    print(f"  {row['Name']}: {row['ANTL']:.2f}")