from typing import List, Optional
import os
import numpy as np
import rasterio
from pydantic.v1 import BaseModel, Field
from langchain_core.tools import StructuredTool
from storage_manager import storage_manager

class LocalNTLCompositeInput(BaseModel):
    file_paths: List[str] = Field(
        ...,
        description="A list of daily NTL GeoTIFF filenames located in 'inputs/' (e.g., ['NTL_2020_01_01.tif', 'NTL_2020_01_02.tif'])."
    )
    out_tif: str = Field(
        ...,
        description="The target filename for the mean composite to be saved in 'outputs/' (e.g., 'Monthly_Mean_Jan.tif')."
    )
    enforce_same_grid: bool = Field(True, description="Ensure all input rasters share the same CRS and resolution.")
    fallback_nodata: Optional[float] = Field(None, description="The value to use if NoData is not defined in metadata.")

def build_ntl_mean_composite_local(
    file_paths: List[str],
    out_tif: str,
    enforce_same_grid: bool = True,
    fallback_nodata: Optional[float] = None,
) -> str:
    """
    Computes a pixel-wise temporal mean composite from a list of local NTL files.
    This effectively reduces cloud noise and sensor artifacts in daily time-series data.
    """
    if not file_paths:
        return "❌ Error: file_paths list is empty."

    # Resolve absolute paths securely via storage_manager
    abs_file_paths = [storage_manager.resolve_input_path(fp) for fp in file_paths]
    abs_out_tif = storage_manager.resolve_output_path(out_tif)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(abs_out_tif), exist_ok=True)

    try:
        # Read reference metadata from the first file
        with rasterio.open(abs_file_paths[0]) as src0:
            profile = src0.profile.copy()
            height, width = src0.height, src0.width
            transform = src0.transform
            crs = src0.crs
            nodata0 = src0.nodata

        if nodata0 is None:
            nodata0 = -1.0 if fallback_nodata is None else fallback_nodata

        stack_sum = np.zeros((height, width), dtype=np.float64)
        stack_cnt = np.zeros((height, width), dtype=np.uint32)

        for fp, abs_fp in zip(file_paths, abs_file_paths):
            if not os.path.exists(abs_fp):
                return f"❌ Input file not found in 'inputs/': {fp}"
            
            with rasterio.open(abs_fp) as ds:
                if enforce_same_grid:
                    if ds.height != height or ds.width != width or ds.transform != transform or ds.crs != crs:
                        return f"❌ Grid/CRS mismatch in {fp}. All daily inputs must be spatially aligned."
                
                arr = ds.read(1).astype(np.float64)
                nd = ds.nodata if ds.nodata is not None else nodata0
                valid = (arr != nd) & np.isfinite(arr)
                stack_sum[valid] += arr[valid]
                stack_cnt[valid] += 1

        # Calculate mean safely
        with np.errstate(divide='ignore', invalid='ignore'):
            mean_arr = np.where(stack_cnt > 0, stack_sum / stack_cnt, np.nan)

        # Prepare output profile
        out_profile = profile.copy()
        out_profile.update(count=1, dtype=rasterio.float32, nodata=nodata0, compress='lzw')

        # Handle NoData values in final array
        out_data = mean_arr.copy()
        out_data[~np.isfinite(out_data)] = nodata0

        with rasterio.open(abs_out_tif, "w", **out_profile) as dst:
            dst.write(out_data.astype(np.float32), 1)

        valid_ratio = float(np.mean(stack_cnt > 0))
        return (f"✅ Success! Mean composite saved to 'outputs/{out_tif}'.\n"
                f"- Input files processed: {len(file_paths)}\n"
                f"- Effective pixel coverage: {valid_ratio:.2%}")

    except Exception as e:
        return f"❌ Error during compositing: {str(e)}"

# Registering the Tool
NTL_composite_local_tool = StructuredTool.from_function(
    func=build_ntl_mean_composite_local,
    name="NTL_Mean_Composite",
    description=(
        "Computes a pixel-wise temporal mean composite from a list of local GeoTIFFs (typically daily NTL data). "
        "It aggregates multiple rasters in the 'inputs/' folder to create a stable representative image in the 'outputs/' folder. "
        "Inputs must be spatially aligned (same CRS and resolution)."
    ),
    args_schema=LocalNTLCompositeInput
)

from typing import Optional, Literal
from pydantic.v1 import BaseModel, Field
from langchain_core.tools import StructuredTool

class NTLCompositeGEEInput(BaseModel):
    study_area: str = Field(..., description="Name of the study area. Example: '南京市'")
    scale_level: Literal['country', 'province', 'city', 'county'] = Field(..., description="Administrative scale level.")
    dataset_name: Literal['VNP46A2', 'VNP46A1'] = Field('VNP46A2', description="Daily VIIRS NTL dataset.")
    time_range_input: str = Field(..., description="Date range in 'YYYY-MM-DD to YYYY-MM-DD' format.")
    output_filename: str = Field(
        ...,
        description="Output filename in your 'outputs/' directory (e.g., 'Shanghai_NTL_Mean_2020.tif')."
    )


def NTL_composite_GEE_tool(
    study_area: str,
    scale_level: str,
    dataset_name: str,
    time_range_input: str,
    output_filename: str
) -> str:
    try:
        import ee
        import geemap
        from datetime import datetime, timedelta
    except ImportError as e:
        return f"❌ Missing dependencies: {e}"

    # Initialize Earth Engine (assumes credentials are pre-configured)
    try:
        ee.Initialize(project='empyrean-caster-430308-m2')
    except Exception as e:
        return f"❌ Failed to initialize Earth Engine: {e}"

    # Admin boundaries
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    directly_governed_cities = ['北京市', '天津市', '上海市', '重庆市']
    if scale_level == 'province' or (scale_level == 'city' and study_area in directly_governed_cities):
        admin_boundary = province_collection
        name_property = 'name'
    elif scale_level == 'country':
        admin_boundary = national_collection
        name_property = 'NAME'
    elif scale_level == 'city':
        admin_boundary = city_collection
        name_property = 'name'
    elif scale_level == 'county':
        admin_boundary = county_collection
        name_property = 'name'
    else:
        return "❌ Invalid scale_level. Use: 'country', 'province', 'city', or 'county'."

    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))
    if region.size().getInfo() == 0:
        return f"❌ No area named '{study_area}' found at '{scale_level}' level."

    # Dataset config
    daily_map = {
        'VNP46A2': {'id': 'NASA/VIIRS/002/VNP46A2', 'band': 'DNB_BRDF_Corrected_NTL'},
        'VNP46A1': {'id': 'NOAA/VIIRS/001/VNP46A1', 'band': 'DNB_At_Sensor_Radiance_500m'}
    }
    if dataset_name not in daily_map:
        return "❌ dataset_name must be 'VNP46A2' or 'VNP46A1'."

    col_id, band = daily_map[dataset_name]['id'], daily_map[dataset_name]['band']

    # Parse date
    if 'to' not in time_range_input:
        return "❌ time_range_input must be 'YYYY-MM-DD to YYYY-MM-DD'."
    start_str, end_str = [s.strip() for s in time_range_input.split('to')]
    try:
        start_date = datetime.strptime(start_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_str, '%Y-%m-%d') + timedelta(days=1)
    except ValueError as e:
        return f"❌ Invalid date format: {e}"

    # Build collection
    col = (
        ee.ImageCollection(col_id)
        .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        .select(band)
        .filterBounds(region.geometry())
        .map(lambda img: img.updateMask(img.neq(0)))  # mask 0 as nodata
    )

    if col.size().getInfo() == 0:
        return f"⚠️ No images found in GEE for {time_range_input} over {study_area}."

    composite = col.mean().clip(region)

    # Resolve output path securely
    abs_export_path = storage_manager.resolve_output_path(output_filename)
    os.makedirs(os.path.dirname(abs_export_path), exist_ok=True)

    # Export
    try:
        geemap.ee_export_image(
            ee_object=composite,
            filename=abs_export_path,
            scale=500,
            region=region.geometry(),
            crs='EPSG:4326',
            file_per_band=False
        )
    except Exception as e:
        return f"❌ Export failed: {e}"

    return f"✅ GEE NTL composite saved to 'outputs/{output_filename}' for {study_area} ({time_range_input})"


NTL_composite_GEE_tool = StructuredTool.from_function(
    func=NTL_composite_GEE_tool,
    name="composite_ntl_from_gee",
    description=(
        "Fetch and composite daily VIIRS NTL data from Google Earth Engine over a user-defined region and time period. "
        "Masks zero-value pixels (nodata), computes mean, and saves result to your 'outputs/' directory."
    ),
    args_schema=NTLCompositeGEEInput
)


# result = NTL_composite_local_tool.func(
#     file_paths=[
#         r'./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01-01.tif',
#         r'./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01-02.tif',
#         r'./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01-03.tif',
#         r'./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01-04.tif',
#         r'./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01-05.tif',
#         r'./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01-06.tif',
#         r'./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01-07.tif'
#     ],
#     out_tif=r'./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01_Mean.tif',
#     enforce_same_grid=True,
#     fallback_nodata=-1
# )

# result = NTL_composite_GEE_tool.func(
#     study_area='上海市',
#     scale_level='city',
#     dataset_name='VNP46A2',
#     time_range_input='2020-01-01 to 2020-01-07',
#     export_path='./NTL_Agent/Night_data/Shanghai/NTL_上海市_VNP46A2_DAILY_2020-01_Mean1.tif'
# )
#
# print(result)
