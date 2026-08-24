"""
This script uses the Google Earth Engine Python API to:
1. Filter the Shanghai boundary from the FAO GAUL 2015 level-2 administrative boundaries.
2. Retrieve VIIRS VNP46A2 nighttime light images for the date range January 1–7, 2020.
3. Compute the pixel-wise mean to create a weekly composite and clip it to the Shanghai boundary.
4. Export the processed image directly to a specified Cloud Project Asset (empyrean-caster-430308-m2).

Notes:
- The assetId must follow the Cloud Asset format: projects/<project_id>/assets/<asset_name>.
- Specify the project in ee.Initialize() to avoid exporting to the personal 'users/' directory.
- If you need a custom nodata value (e.g., -1), mask or assign the value before export.
"""


import ee
ee.Initialize(project='empyrean-caster-430308-m2')

shanghai = ee.FeatureCollection('FAO/GAUL/2015/level2') \
    .filter(ee.Filter.eq('ADM2_NAME', 'Shanghai'))

ntl_collection = ee.ImageCollection('NASA/VIIRS/002/VNP46A2') \
    .filterDate('2020-01-01', '2020-01-07') \
    .select('DNB_BRDF_Corrected_NTL')

weekly_composite = ntl_collection.mean().clip(shanghai)

task = ee.batch.Export.image.toAsset(
    image=weekly_composite,
    description='Shanghai_VNP46A2_WeeklyComposite_20200101_07',
    assetId='projects/empyrean-caster-430308-m2/assets/Shanghai_VNP46A2_WeeklyComposite_20200101_071',
    region=shanghai.geometry().bounds().getInfo()['coordinates'],
    scale=500,
    maxPixels=1e13
)

task.start()
print('Export task started.')
