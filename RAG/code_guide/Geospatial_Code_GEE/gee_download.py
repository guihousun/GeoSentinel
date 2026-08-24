from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
import ee

# ee.Authenticate()
project_id = 'empyrean-caster-430308-m2'
ee.Initialize(project=project_id)

from typing import Optional, Literal
from pydantic import BaseModel, Field
from langchain.tools import StructuredTool

class NightlightDataInput(BaseModel):
    study_area: str = Field(..., description="Name of the study area of interest. Example: '南京市'")
    scale_level: Literal['country', 'province', 'city', 'county'] = Field(..., description="Administrative scale level.")
    temporal_resolution: Literal['annual', 'monthly', 'daily'] = Field(..., description="Temporal resolution: 'annual', 'monthly', or 'daily'.")
    time_range_input: str = Field(..., description="Time range. Annual: 'YYYY to YYYY' or 'YYYY'. Monthly: 'YYYY-MM to YYYY-MM'. Daily: 'YYYY-MM-DD to YYYY-MM-DD'.")
    export_folder: str = Field(..., description="Local folder path to save exports. Example: 'C:/NTL_Agent/Night_data/Nanjing'")
    dataset_name: Optional[str] = Field(None, description=(
        "Further dataset selection. "
        "For annual: 'NPP-VIIRS-Like' (default), 'NPP-VIIRS', 'DMSP-OLS'. "
        "For monthly: 'NOAA_VCMSLCFG' (fixed). "
        "For daily: 'VNP46A2' (default), 'VNP46A1'."
    ))
    collection_name: Optional[str] = Field(None, description="Optional logical name for the exported collection.")


def NTL_download_tool(
        study_area: str,
        scale_level: str,
        temporal_resolution: str,
        time_range_input: str,
        export_folder: str,
        dataset_name: Optional[str] = None,
        collection_name: Optional[str] = None,
):
    import os
    import re
    import ee
    import geemap
    import calendar
    from datetime import datetime, timedelta

    # ---------- Admin boundary sources ----------
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    def get_administrative_boundaries(scale_level: str):
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
            raise ValueError("Unknown scale level. Options are 'country', 'province', 'city', 'county'.")
        return admin_boundary, name_property

    admin_boundary, name_property = get_administrative_boundaries(scale_level)
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))

    if region.size().getInfo() == 0:
        raise ValueError(f"No area named '{study_area}' found under scale level '{scale_level}'.")

    # ---------- Temporal parsing ----------
    def parse_time_range(time_range_input: str, temporal_resolution: str):
        tr = time_range_input.replace(' ', '')
        if 'to' in tr:
            start_str, end_str = [s.strip() for s in tr.split('to')]
        else:
            start_str = end_str = tr

        if temporal_resolution == 'annual':
            if not re.fullmatch(r'\d{4}', start_str) or not re.fullmatch(r'\d{4}', end_str):
                raise ValueError("Annual format must be 'YYYY' or 'YYYY to YYYY'.")
            start_date, end_date = f"{start_str}-01-01", f"{end_str}-12-31"
        elif temporal_resolution == 'monthly':
            if not re.fullmatch(r'\d{4}-\d{2}', start_str) or not re.fullmatch(r'\d{4}-\d{2}', end_str):
                raise ValueError("Monthly format must be 'YYYY-MM' or 'YYYY-MM to YYYY-MM'.")
            sy, sm = map(int, start_str.split('-'))
            ey, em = map(int, end_str.split('-'))
            start_date = f"{sy}-{sm:02d}-01"
            end_date = f"{ey}-{em:02d}-{calendar.monthrange(ey, em)[1]}"
        elif temporal_resolution == 'daily':
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', start_str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', end_str):
                raise ValueError("Daily format must be 'YYYY-MM-DD' or 'YYYY-MM-DD to YYYY-MM-DD'.")
            start_date, end_date = start_str, end_str
        else:
            raise ValueError("temporal_resolution must be one of: 'annual', 'monthly', 'daily'.")

        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
            raise ValueError("Start date cannot be later than end date.")

        return start_date, end_date

    start_date, end_date = parse_time_range(time_range_input, temporal_resolution)

    # ---------- Dataset routing ----------
    # Defaults
    # ---------- Dataset routing: ANNUAL ----------
    if temporal_resolution == 'annual':
        dataset_name = dataset_name or 'NPP-VIIRS-Like'

        # 配置与有效年限
        annual_like_cfg = {'id': 'projects/sat-io/open-datasets/npp-viirs-ntl', 'band': 'b1', 'valid_from': 2000,
                           'valid_to': 2023}
        dmsp_cfg = {'id': 'BNU/FGS/CCNL/v1', 'band': 'b1', 'valid_from': 1992, 'valid_to': 2013}

        start_year, end_year = int(start_date[:4]), int(end_date[:4])

        if dataset_name == 'NPP-VIIRS-Like':
            if start_year < annual_like_cfg['valid_from'] or end_year > annual_like_cfg['valid_to']:
                raise ValueError(
                    f"NPP-VIIRS-Like valid year range: {annual_like_cfg['valid_from']}–{annual_like_cfg['valid_to']}.")
            col_id, band = annual_like_cfg['id'], annual_like_cfg['band']
            images = []
            for y in range(start_year, end_year + 1):
                y_start, y_end = f"{y}-01-01", f"{y + 1}-01-01"
                col = ee.ImageCollection(col_id).filterDate(y_start, y_end).select(band).filterBounds(region.geometry())
                img = col.map(lambda i: i.clip(region)).mean().set('system:time_start', ee.Date(y_start).millis())
                images.append(img)
            if not images:
                return "No images found for the specified date range and region."
            NTL_collection = ee.ImageCollection(images)
            ntl_type = "NPP-VIIRS-Like_ANNUAL"

        elif dataset_name == 'NPP-VIIRS':
            # 2012–2021: V21；2022: V22；（当前假设 2023 暂无官方年度合成）
            V21_ID, V22_ID, BAND = 'NOAA/VIIRS/DNB/ANNUAL_V21', 'NOAA/VIIRS/DNB/ANNUAL_V22', 'average'
            valid_from, valid_to = 2012, 2022
            if start_year < valid_from or end_year > valid_to:
                raise ValueError(
                    f"NPP-VIIRS valid year range: {valid_from}–{valid_to}. (2012–2021 from V21, 2022 from V22)")

            images = []
            for y in range(start_year, end_year + 1):
                y_start, y_end = f"{y}-01-01", f"{y + 1}-01-01"
                src_id = V21_ID if y <= 2021 else V22_ID
                col = ee.ImageCollection(src_id).filterDate(y_start, y_end).select(BAND).filterBounds(region.geometry())
                img = col.map(lambda i: i.clip(region)).mean().set('system:time_start', ee.Date(y_start).millis())
                images.append(img)

            if not images:
                return "No images found for the specified date range and region."
            NTL_collection = ee.ImageCollection(images)
            ntl_type = "NPP-VIIRS_ANNUAL_V21V22"

        elif dataset_name == 'DMSP-OLS':
            if start_year < dmsp_cfg['valid_from'] or end_year > dmsp_cfg['valid_to']:
                raise ValueError(f"DMSP-OLS valid year range: {dmsp_cfg['valid_from']}–{dmsp_cfg['valid_to']}.")
            col_id, band = dmsp_cfg['id'], dmsp_cfg['band']
            images = []
            for y in range(start_year, end_year + 1):
                y_start, y_end = f"{y}-01-01", f"{y + 1}-01-01"
                col = ee.ImageCollection(col_id).filterDate(y_start, y_end).select(band).filterBounds(region.geometry())
                img = col.map(lambda i: i.clip(region)).mean().set('system:time_start', ee.Date(y_start).millis())
                images.append(img)
            if not images:
                return "No images found for the specified date range and region."
            NTL_collection = ee.ImageCollection(images)
            ntl_type = "DMSP-OLS_ANNUAL"

        else:
            raise ValueError(
                "For annual, dataset_name must be one of: 'NPP-VIIRS-Like' (default), 'NPP-VIIRS', 'DMSP-OLS'.")



    elif temporal_resolution == 'monthly':
        # Fixed monthly dataset
        col_id = 'NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG'
        band = 'avg_rad'
        sy, sm = map(int, start_date[:7].split('-'))
        ey, em = map(int, end_date[:7].split('-'))

        if sy < 2014:
            raise ValueError("Monthly VIIRS is available from 2014-01 onwards.")
        images = []
        for y in range(sy, ey + 1):
            m_start = sm if y == sy else 1
            m_end = em if y == ey else 12
            for m in range(m_start, m_end + 1):
                s_day = f"{y}-{m:02d}-01"
                e_day = f"{y}-{m:02d}-{calendar.monthrange(y, m)[1]}"
                collection = ee.ImageCollection(col_id).filterDate(s_day, e_day).select(band).filterBounds(region.geometry())
                image = collection.map(lambda img: img.clip(region)).mean().set('system:time_start', ee.Date(s_day).millis())
                images.append(image)
        if not images:
            return "No images found for the specified date range and region."
        NTL_collection = ee.ImageCollection(images)
        ntl_type = "VIIRS_MONTHLY"

    elif temporal_resolution == 'daily':
        dataset_name = dataset_name or 'VNP46A2'
        daily_map = {
            'VNP46A2': {'id': 'NASA/VIIRS/002/VNP46A2', 'band': 'DNB_BRDF_Corrected_NTL'},
            'VNP46A1': {'id': 'NOAA/VIIRS/001/VNP46A1', 'band': 'DNB_At_Sensor_Radiance_500m'},
        }
        if dataset_name not in daily_map:
            raise ValueError("For daily, dataset_name must be one of: 'VNP46A2' (default), 'VNP46A1'.")
        col_id, band = daily_map[dataset_name]['id'], daily_map[dataset_name]['band']
        if int(start_date[:4]) < 2014:
            raise ValueError("Daily VIIRS is available from 2014-01 onwards.")
        NTL_collection = (
            ee.ImageCollection(col_id)
            .filterDate(start_date, (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'))
            .select(band)
            .filterBounds(region.geometry())
            .map(lambda img: img.clip(region))
        )
        ntl_type = f"{dataset_name}_DAILY"

    else:
        raise ValueError("temporal_resolution must be 'annual', 'monthly', or 'daily'.")

    # ---------- Export ----------
    os.makedirs(export_folder, exist_ok=True)
    images_list = NTL_collection.toList(NTL_collection.size())
    num_images = NTL_collection.size().getInfo()

    exported_files = []
    for i in range(num_images):
        image = ee.Image(images_list.get(i))
        if temporal_resolution == 'daily':
            image_date = image.date().format('YYYY-MM-dd').getInfo()
        elif temporal_resolution == 'monthly':
            image_date = image.date().format('YYYY-MM').getInfo()
        else:
            image_date = image.date().format('YYYY').getInfo()

        out_name = f"NTL_{study_area}_{ntl_type}_{image_date}.tif"
        export_path = os.path.join(export_folder, out_name)

        geemap.ee_export_image(
            ee_object=image,
            filename=export_path,
            scale=500,
            region=region.geometry(),
            crs='EPSG:4326',
            file_per_band=False
        )
        exported_files.append(export_path)
        print(f"Image exported to: {export_path}")

    return f"Data has been saved to: {', '.join(exported_files)}"



class NDVIDataInput(BaseModel):
    study_area: str = Field(..., description="Name of the study area of interest. Example: '南京市'")
    scale_level: str = Field(..., description="Scale level, e.g. 'country', 'province', 'city', 'county'")
    time_range_input: str = Field(..., description="Time range in the format 'YYYY to YYYY'. Example: '2016 to 2020'")
    export_folder: str = Field(..., description="The local folder path to save the exported NDVI files")


def NDVI_download_tool(
        study_area: str,
        scale_level: str,
        time_range_input: str,
        export_folder: str,
):
    import re
    import os
    import ee
    import geemap
    import calendar
    from datetime import datetime

    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    def get_administrative_boundaries(scale_level):
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
            raise ValueError("Unknown scale level.")
        return admin_boundary, name_property

    admin_boundary, name_property = get_administrative_boundaries(scale_level)
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))
    if region.size().getInfo() == 0:
        raise ValueError(f"No area named '{study_area}' found under scale level '{scale_level}'.")

    def parse_time_range(time_range_input):
        time_range_input = time_range_input.replace(' ', '')
        if 'to' in time_range_input:
            start_str, end_str = time_range_input.split('to')
            start_str, end_str = start_str.strip(), end_str.strip()
        else:
            start_str = end_str = time_range_input.strip()
        if not re.match(r'^\d{4}$', start_str) or not re.match(r'^\d{4}$', end_str):
            raise ValueError("Invalid format. Use 'YYYY' or 'YYYY to YYYY'.")
        start_date = f"{start_str}-01-01"
        end_date = f"{end_str}-12-31"
        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
            raise ValueError("Start date cannot be later than end date.")
        return int(start_str), int(end_str)

    start_year, end_year = parse_time_range(time_range_input)

    os.makedirs(export_folder, exist_ok=True)
    exported_files = []

    for year in range(start_year, end_year + 1):
        collection = (
            ee.ImageCollection("MODIS/061/MOD13Q1")
            .filterDate(f"{year}-01-01", f"{year}-12-31")
            .select("NDVI")
            .map(lambda img: img.multiply(0.0001).set(img.toDictionary(img.propertyNames())))
        )

        image = collection.filterBounds(region.geometry()).map(lambda img: img.clip(region)).mean()
        image = image.set('system:time_start', ee.Date(f'{year}-01-01').millis())

        export_path = os.path.join(export_folder, f"NDVI_{study_area}_{year}.tif")

        geemap.ee_export_image(
            ee_object=image,
            filename=export_path,
            scale=250,
            region=region.geometry(),
            crs='EPSG:4326',
            file_per_band=False
        )

        exported_files.append(export_path)
        print(f"Image exported to: {export_path}")

    return f"NDVI data has been saved to: {', '.join(exported_files)}"


class LandScanDataInput(BaseModel):
    study_area: str = Field(..., description="Name of the study area. Example: '南京市'")
    scale_level: str = Field(..., description="Scale level, one of 'country', 'province', 'city', 'county'")
    time_range_input: str = Field(..., description="Year range in format 'YYYY to YYYY'. Example: '2018 to 2020'")
    export_folder: str = Field(..., description="Local path to save the exported LandScan population rasters")

def landscan_download_tool(
        study_area: str,
        scale_level: str,
        time_range_input: str,
        export_folder: str,
):
    import os
    import re
    import ee
    import geemap
    from datetime import datetime

    # Set region boundary collections
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    def get_administrative_boundaries(scale_level):
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
            raise ValueError("Unknown scale level.")
        return admin_boundary, name_property

    admin_boundary, name_property = get_administrative_boundaries(scale_level)
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))

    if region.size().getInfo() == 0:
        raise ValueError(f"No region named '{study_area}' under scale level '{scale_level}'.")

    def parse_year_range(time_range_input):
        time_range_input = time_range_input.replace(" ", "")
        if 'to' in time_range_input:
            start_year, end_year = map(int, time_range_input.split('to'))
        else:
            start_year = end_year = int(time_range_input)
        if start_year > end_year:
            raise ValueError("Start year must not be greater than end year.")
        return start_year, end_year

    start_year, end_year = parse_year_range(time_range_input)
    os.makedirs(export_folder, exist_ok=True)
    exported_files = []

    for year in range(start_year, end_year + 1):
        image = (
            ee.ImageCollection('projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL')
            .filterDate(f'{year}-01-01', f'{year + 1}-01-01')
            .first()
        )
        image = ee.Image(image).clip(region.geometry())

        export_path = os.path.join(export_folder, f"LandScan_{study_area}_{year}.tif")
        geemap.ee_export_image(
            ee_object=image,
            filename=export_path,
            scale=1000,
            region=region.geometry(),
            crs='EPSG:4326',
            file_per_band=False
        )
        exported_files.append(export_path)
        print(f"LandScan {year} exported to: {export_path}")

    return f"LandScan data saved to: {', '.join(exported_files)}"


from pydantic import BaseModel, Field

class WorldPopDataInput(BaseModel):
    study_area: str = Field(..., description="Name of the study area. Example: '南京市'")
    scale_level: str = Field(..., description="Scale level, one of 'country', 'province', 'city', 'county'")
    time_range_input: str = Field(..., description="Year range in format 'YYYY to YYYY'. Example: '2018 to 2020'")
    export_folder: str = Field(..., description="Local path to save the exported WorldPop population rasters")

def worldpop_download_tool(
        study_area: str,
        scale_level: str,
        time_range_input: str,
        export_folder: str,
):
    import os
    import re
    import ee
    import geemap
    from datetime import datetime

    # Set region boundary collections
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    def get_administrative_boundaries(scale_level):
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
            raise ValueError("Unknown scale level.")
        return admin_boundary, name_property

    admin_boundary, name_property = get_administrative_boundaries(scale_level)
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))

    if region.size().getInfo() == 0:
        raise ValueError(f"No region named '{study_area}' under scale level '{scale_level}'.")

    def parse_year_range(time_range_input):
        time_range_input = time_range_input.replace(" ", "")
        if 'to' in time_range_input:
            start_year, end_year = map(int, time_range_input.split('to'))
        else:
            start_year = end_year = int(time_range_input)
        if start_year > end_year:
            raise ValueError("Start year must not be greater than end year.")
        return start_year, end_year

    start_year, end_year = parse_year_range(time_range_input)
    os.makedirs(export_folder, exist_ok=True)
    exported_files = []

    for year in range(start_year, end_year + 1):
        image = ee.Image(f"WorldPop/GP/100m/pop/{year}").clip(region.geometry())
        export_path = os.path.join(export_folder, f"WorldPop_{study_area}_{year}.tif")
        geemap.ee_export_image(
            ee_object=image,
            filename=export_path,
            scale=100,
            region=region.geometry(),
            crs='EPSG:4326',
            file_per_band=False
        )
        exported_files.append(export_path)
        print(f"WorldPop {year} exported to: {export_path}")

    return f"WorldPop data saved to: {', '.join(exported_files)}"



# Update the nightlight_download_tool
NTL_download_tool = StructuredTool.from_function(
    NTL_download_tool,
    name="NTL_download_tool",
    description=(
        """
        This tool downloads nighttime light data from Google Earth Engine based on specified parameters, 
        including region name, scale level ('country', 'province', 'city', 'county'), temporal resolution ('annual', 'monthly', or 'daily'), 
        optional dataset_name for product selection, and time range.

        For China, region names should be in Chinese (e.g., 江苏省, 南京市, 鼓楼区). 

        Dataset selection:
          - Annual: 'NPP-VIIRS-Like' (default), 'NPP-VIIRS', 'DMSP-OLS'
          - Monthly: fixed to 'NOAA_VCMSLCFG'
          - Daily: 'VNP46A2' (default), 'VNP46A1'

        Example Input:
        (
            study_area='南京市',
            scale_level='city',
            temporal_resolution='daily',
            dataset_name='VNP46A2',
            time_range_input='2020-01-01 to 2020-02-01',
            export_folder='C:/NTL_Agent/Night_data/Nanjing'
        )
        or
        (
            study_area='上海市',
            scale_level='country',
            temporal_resolution='annual',
            dataset_name='NPP-VIIRS-Like',
            time_range_input='2000 to 2023',
            export_folder='C:/NTL_Agent/Night_data/Shanghai'
        )
        or
        (
            study_area='黄浦区',
            scale_level='county',
            temporal_resolution='monthly',
            time_range_input='2021-05 to 2021-07',
            export_folder='C:/NTL_Agent/Night_data/Huangpu'
        )
        """
    )
    ,
    input_type=NightlightDataInput,
)

NDVI_download_tool = StructuredTool.from_function(
    NDVI_download_tool,
    name="NDVI_download_tool",
    description=(
        """
        This tool downloads annual NDVI data (MODIS MOD13Q1) from Google Earth Engine 
        based on specified region, scale level, and time range. 
        Only annual NDVI average is supported. The NDVI values are scaled (multiplied by 0.0001) as per dataset requirement.

        Supported region scale levels: 'country', 'province', 'city', 'county'. 
        Region names in China should be in Chinese (e.g., 江苏省, 南京市, 鼓楼区).
        Time range should be in format 'YYYY to YYYY'.

        Example Input:
        NDVIDataInput(
            study_area='南京市',
            scale_level='city',
            time_range_input='2016 to 2020',
            export_folder='C:/NTL_Agent/NDVI_data/Nanjing'
        )
        """
    ),
    input_type=NDVIDataInput,
)


LandScan_download_tool = StructuredTool.from_function(
    landscan_download_tool,
    name="LandScan_download_tool",
    description=(
        """
        This tool downloads LandScan Global annual population data from Google Earth Engine 
        based on region, scale level, and year range. 
        The scale can be 'country', 'province', 'city', or 'county'. 
        Region names in China should be in Chinese (e.g., 江苏省, 南京市, 鼓楼区).
        Only yearly data is supported.

        Example Input:
        LandScanDataInput(
            study_area='南京市',
            scale_level='city',
            time_range_input='2018 to 2020',
            export_folder='C:/NTL_Agent/LandScan_data/Nanjing'
        )
        """
    ),
    input_type=LandScanDataInput,
)

# 封装为 StructuredTool
WorldPop_download_tool = StructuredTool.from_function(
    worldpop_download_tool,
    name="WorldPop_download_tool",
    description=(
        """
        This tool downloads WorldPop 100m annual population data from Google Earth Engine 
        based on specified region, scale level, and year range. 
        The scale can be 'country', 'province', 'city', or 'county'. 
        Region names in China should be in Chinese (e.g., 江苏省, 南京市, 鼓楼区).
        Only yearly data is supported.

        Example Input:
        WorldPopDataInput(
            study_area='南京市',
            scale_level='city',
            time_range_input='2018 to 2020',
            export_folder='C:/NTL_Agent/WorldPop_data/Nanjing'
        )
        """
    ),
    input_type=WorldPopDataInput,
)

# result = NTL_download_tool .func(study_area='南京市',
#     scale_level='city',
#     temporal_resolution = 'annual',
#     dataset_name='DMSP-OLS',
#     time_range_input='2005 to 2010',
#     export_folder='C:/NTL_Agent/Night_data/Shanghai/DMSP_OLS')

# result2 = NDVI_download_tool.func(study_area='南京市',
#     scale_level='city',
#     time_range_input='2018 to 2020',
#     export_folder='C:/NTL_Agent/NDVI_data/Nanjing')

# result2 = NDVI_download_tool.func(study_area='南京市',
#     scale_level='city',
#     time_range_input='2018 to 2020',
#     export_folder='C:/NTL_Agent/NDVI_data/Nanjing')