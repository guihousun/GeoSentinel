"""
GEE Python Script for Myanmar Earthquake 2025 ANTL Impact Assessment
Multi-period, multi-buffer analysis using VNP46A2 daily NTL data

Event: 2025-03-28, Mw 7.7, Epicenter: 21.996 deg N, 95.926 deg E
Local first-night label: 2025-03-29 (event occurred at 12:50 MMT, after local nighttime overpass)
UTC-indexed first-night image/file date: 2025-03-28 (candidate local acquisition around 2025-03-29 00:30-02:30 MMT maps to roughly 2025-03-28 18:00-20:00 UTC)
"""

import ee
import pandas as pd
from storage_manager import storage_manager

# Initialize GEE
PROJECT_ID = "empyrean-caster-430308-m2"
ee.Initialize(project=PROJECT_ID)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Epicenter coordinates (longitude, latitude for GEE)
EPICENTER_LON = 95.926
EPICENTER_LAT = 21.996
epicenter = ee.Geometry.Point([EPICENTER_LON, EPICENTER_LAT])

# Buffer zones (in meters)
BUFFERS = {
    25: 25000,
    50: 50000,
    100: 100000
}

# Analysis periods
# Note: local first-night is 2025-03-29 because the event occurred at 12:50 MMT
# after the local nighttime overpass on day D. For UTC-indexed daily images/files,
# the corresponding UTC-indexed acquisition/file date is 2025-03-28.
# Do not claim an exact local acquisition time unless UTC_Time or official
# metadata confirms it.
PERIODS = {
    "baseline": {"start": "2025-03-14", "end": "2025-03-21"},  # event-14d to event-7d (end-exclusive in GEE)
    "first_night": {"start": "2025-03-28", "end": "2025-03-29"},  # UTC-indexed first-night image date; end-exclusive
    "recovery": {"start": "2025-04-04", "end": "2025-04-11"}  # event+7d to event+14d (end-exclusive in GEE)
}

# Dataset configuration
DATASET_ID = "NASA/VIIRS/002/VNP46A2"
BAND = "Gap_Filled_DNB_BRDF_Corrected_NTL"
SCALE = 500  # VNP46A2 native resolution
MAX_PIXELS = 1e13
TILE_SCALE = 16

# =============================================================================
# LOAD VNP46A2 COLLECTION
# =============================================================================

print("Loading VNP46A2 collection...")
ntl_collection = (
    ee.ImageCollection(DATASET_ID)
    .select(BAND)
)

# =============================================================================
# COMPUTE ANTL FOR EACH BUFFER AND PERIOD
# =============================================================================

results = []

for buffer_km, buffer_m in BUFFERS.items():
    print(f"\nProcessing {buffer_km} km buffer...")
    
    # Create buffer geometry
    buffer_geometry = epicenter.buffer(buffer_m)
    
    for period_name, period_dates in PERIODS.items():
        print(f"  Computing {period_name} ({period_dates['start']} to {period_dates['end']})...")
        
        # Filter collection by date range (GEE filterDate is end-exclusive)
        filtered_collection = ntl_collection.filterDate(
            period_dates["start"], 
            period_dates["end"]
        )
        
        # Compute mean composite for the period
        mean_image = filtered_collection.mean()
        
        # Reduce region to get mean ANTL
        antl_result = mean_image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=buffer_geometry,
            scale=SCALE,
            maxPixels=MAX_PIXELS,
            bestEffort=True,
            tileScale=TILE_SCALE
        )
        
        # Extract mean value
        mean_antl = antl_result.get(BAND)
        
        # Store result
        results.append({
            "buffer_km": buffer_km,
            "period": period_name,
            "start_date": period_dates["start"],
            "end_date": period_dates["end"] if period_name != "first_night" else period_dates["start"],  # For first_night, end_date = start_date
            "mean_antl": mean_antl
        })

# =============================================================================
# EXPORT RESULTS TO CSV
# =============================================================================

print("\nExporting results to CSV...")

# Create DataFrame
df = pd.DataFrame(results)

# Resolve output path
out_csv = storage_manager.resolve_output_path("Myanmar_Earthquake_ANTL_2025.csv")

# Save to CSV
df.to_csv(out_csv, index=False)

print(f"\nResults saved to: {out_csv}")
print("\nExpected output structure (9 rows):")
print(df.to_string())

# =============================================================================
# END OF SCRIPT
# =============================================================================
