from pydantic import BaseModel, Field
from storage_manager import storage_manager
class DailyANTLInput(BaseModel):
    study_area: str = Field(..., description="Name of the study area (in Chinese for China, English for others). Example: '上海市'")
    scale_level: str = Field(..., description="Scale level: 'province', 'city', or 'county'.")
    start_date: str = Field(..., description="Start date in 'YYYY-MM-DD' format. Example: '2020-01-01'")
    end_date: str = Field(..., description="End date in 'YYYY-MM-DD' format. Example: '2024-12-31'")
    out_csv_name: str = Field(None, description="Optional custom name for the output CSV file.")

import ee
import pandas as pd
import os
from datetime import datetime
from langchain_core.tools import StructuredTool

def calculate_daily_antl_tool(
    study_area: str,
    scale_level: str,
    start_date: str,
    end_date: str,
    out_csv_name: str = None
):
    """
    Calculates the daily mean Nighttime Light (ANTL) values for a specific region 
    using the NASA VNP46A2 dataset on Google Earth Engine and exports a CSV trend file.
    """
    import ee
    
    # 1. 行政边界选择逻辑 (与系统其他工具保持一致)
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    def get_region(area, level):
        directly_governed = ['北京市', '天津市', '上海市', '重庆市']
        if level == 'province' or (level == 'city' and area in directly_governed):
            return province_collection.filter(ee.Filter.eq('name', area))
        elif level == 'city':
            return city_collection.filter(ee.Filter.eq('name', area))
        elif level == 'county':
            return county_collection.filter(ee.Filter.eq('name', area))
        else:
            raise ValueError("Invalid scale_level. Use 'province', 'city', or 'county'.")

    region_fc = get_region(study_area, scale_level)
    if region_fc.size().getInfo() == 0:
        return f"Error: Region '{study_area}' not found at level '{scale_level}'."
    region_geom = region_fc.geometry()

    # 2. 加载 VNP46A2 数据集
    # 使用 NASA 最新的 V2 版本 (002)
    vnp_col = ee.ImageCollection('NASA/VIIRS/002/VNP46A2') \
                .filterDate(start_date, end_date) \
                .filterBounds(region_geom) \
                .select('DNB_BRDF_Corrected_NTL')

    count = vnp_col.size().getInfo()
    if count == 0:
        return f"Error: No VNP46A2 data found for {study_area} between {start_date} and {end_date}."

    # 3. 定义计算函数
    def calculate_mean(image):
        stats = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region_geom,
            scale=500,
            maxPixels=1e10
        )
        date = image.date().format('yyyy-MM-dd')
        return ee.Feature(None, {
            'date': date,
            'daily_mean_ntl': stats.get('DNB_BRDF_Corrected_NTL')
        })

    # 4. 执行计算并获取结果
    print(f"Processing {count} images for {study_area}...")
    stats_fc = vnp_col.map(calculate_mean)
    
    # 获取数据属性
    features = stats_fc.getInfo()['features']
    
    # 5. 整理数据
    data_list = []
    for f in features:
        props = f['properties']
        if props['daily_mean_ntl'] is not None:
            data_list.append({
                'Date': props['date'],
                'Daily_Mean_ANTL': props['daily_mean_ntl'],
                'Region': study_area
            })

    if not data_list:
        return "Error: All retrieved data points were null (possibly due to cloud cover or data gaps)."

    df = pd.DataFrame(data_list)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')

    # 6. 保存到本地 (使用 storage_manager)
    filename = out_csv_name if out_csv_name else f"{study_area}_Daily_ANTL_{start_date}_{end_date}.csv"
    # 去除文件名中的特殊字符
    filename = filename.replace(' ', '_').replace(':', '')
    
    abs_output_path = storage_manager.resolve_output_path(filename)
    df.to_csv(abs_output_path, index=False)

    return (f"✅ Daily ANTL calculation completed for {study_area}.\n"
            f"- Total valid days: {len(df)}\n"
            f"- Time range: {df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')}\n"
            f"- Mean NTL Value: {df['Daily_Mean_ANTL'].mean():.4f}\n"
            f"- Result saved to: outputs/{filename}")

# 3. 封装为 StructuredTool
NTL_Daily_ANTL_Statistics = StructuredTool.from_function(
    func=calculate_daily_antl_tool,
    name="NTL_daily_antl_statistics",
    description="""
    Calculates the average daily nighttime light (ANTL) intensity for a specified region and time period using GEE.
    This tool is ideal for long-term trend analysis, anomaly detection, or socio-economic impact studies.
    Input parameters include study_area (Chinese for China), scale_level, start_date, and end_date.
    The output is a CSV file saved in the 'outputs/' folder containing daily mean values.
    """,
    args_schema=DailyANTLInput
)