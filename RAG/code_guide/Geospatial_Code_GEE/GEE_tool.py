import datetime
import pmdarima as pm
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field, ConfigDict
from langchain_experimental.utilities import PythonREPL
# 更新函数，使用解包参数
from pydantic import BaseModel, Field
import os
import ee
import geemap

project_id = 'empyrean-caster-430308-m2'
ee.Initialize(project=project_id)

class PreprocessNightlightInput(BaseModel):
    start_date: str = Field(..., description="The start date for the VIIRS data collection in 'YYYY-MM-DD' format.")
    end_date: str = Field(..., description="The end date for the VIIRS data collection in 'YYYY-MM-DD' format.")
    study_area: str = Field(..., description="The name of the location to filter data for.")
    scale_level: str = Field(..., description="Scale level, e.g.'country', 'province', 'city', 'county'.")

def DailyNTL_preprocess(start_date: str, end_date: str, study_area: str, scale_level: str) -> str:
    
    # Set administrative boundary dataset based on scale level
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")
    # Select administrative boundaries
    def get_administrative_boundaries(scale_level):
        # Handle directly governed cities as province-level data in China
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
            raise ValueError("Unknown scale level. Options are 'country', 'province', 'city', or 'county'.")
        return admin_boundary, name_property

    admin_boundary, name_property = get_administrative_boundaries(scale_level)
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))

    dataset_id = 'NASA/VIIRS/002/VNP46A2'
    band_name = 'DNB_BRDF_Corrected_NTL'
    viirs_collection = (ee.ImageCollection(dataset_id)
                        .filterDate(start_date, end_date)
                        .select(band_name)
                        .filterBounds(region.geometry())
                        .map(lambda image: image.clip(region)))

    # Add group numbers to images
    def add_group_number(image):
        date = ee.Date(image.get('system:time_start'))
        days_diff = date.difference(ee.Date(start_date), 'day')
        group_number = days_diff.mod(16).int()
        return image.set('group_number', group_number)

    viirs_collection = viirs_collection.map(add_group_number)
    annual_mean_image = viirs_collection.mean()
    group_numbers = ee.List.sequence(0, 15)

    def compute_group_mean_image(group_number):
        group_number = ee.Number(group_number)
        group_collection = viirs_collection.filter(ee.Filter.eq('group_number', group_number))
        group_mean_image = group_collection.mean()
        return group_mean_image.set('group_number', group_number)

    group_mean_images = ee.ImageCollection(group_numbers.map(compute_group_mean_image))

    def compute_correction_image(image):
        group_number = image.get('group_number')
        group_mean_image = image
        correction_image = group_mean_image.divide(annual_mean_image).unmask(1)
        return correction_image.set('group_number', group_number)

    correction_images = group_mean_images.map(compute_correction_image)

    def correct_group_images(group_number):
        group_number = ee.Number(group_number)
        correction_image = correction_images.filter(ee.Filter.eq('group_number', group_number)).first()
        group_collection = viirs_collection.filter(ee.Filter.eq('group_number', group_number))
        corrected_group = group_collection.map(lambda image: image.divide(correction_image)
                                               .copyProperties(image, image.propertyNames()))
        return corrected_group

    corrected_groups = group_numbers.map(correct_group_images)
    all_corrected_images = ee.ImageCollection(corrected_groups.iterate(
        lambda img_col, acc: ee.ImageCollection(acc).merge(ee.ImageCollection(img_col)),
        ee.ImageCollection([])))

    corrected_collection = all_corrected_images.sort('system:time_start')

    total_pixels = ee.Number(
        ee.Image.constant(1).rename('constant').clip(region.geometry()).reduceRegion(
            reducer=ee.Reducer.count(),
            geometry=region.geometry(),
            scale=750,
            maxPixels=1e13
        ).get('constant')
    )

    def filter_and_log_invalid_images(image):
        valid_pixels = image.reduceRegion(
            reducer=ee.Reducer.count(),
            geometry=region.geometry(),
            scale=750,
            maxPixels=1e13
        ).get(band_name)

        valid_pixel_percentage = ee.Algorithms.If(
            total_pixels.gt(0),
            ee.Number(valid_pixels).divide(total_pixels).multiply(100),
            0
        )

        return image.set('valid_pixel_percentage', valid_pixel_percentage)

    collection_with_pixel_info = corrected_collection.map(filter_and_log_invalid_images)
    filtered_collection = collection_with_pixel_info.filter(ee.Filter.gte('valid_pixel_percentage', 80))

    def compute_corrected_mean(image):
        mean_dict = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region.geometry(),
            scale=750,
            maxPixels=1e13
        )
        corrected_mean_ntl = mean_dict.get(band_name)
        date = ee.Date(image.get('system:time_start')).format('YYYY-MM-dd')
        group_number = image.get('group_number')
        return ee.Feature(None, {
            'date': date,
            'corrected_mean_ntl': corrected_mean_ntl,
            'group_number': group_number
        })

    corrected_mean_features = filtered_collection.map(compute_corrected_mean).filter(ee.Filter.notNull(['corrected_mean_ntl']))
    corrected_mean_list = corrected_mean_features.reduceColumns(ee.Reducer.toList(3), ['date', 'corrected_mean_ntl', 'group_number']).get('list').getInfo()
    df = pd.DataFrame(corrected_mean_list, columns=['date', 'corrected_mean_ntl', 'group_number'])

    df['date'] = pd.to_datetime(df['date'])
    # Define the fixed holiday dates
    holiday_dates = [
    '2013-01-01','2013-02-09','2013-02-10','2013-02-11','2013-02-12','2013-02-13','2013-02-14','2013-02-15',
    '2013-04-04','2013-04-05','2013-04-06','2013-04-29','2013-04-30','2013-05-01','2013-06-10','2013-06-11','2013-06-12',
    '2013-09-19','2013-09-20','2013-09-21','2013-10-01','2013-10-02','2013-10-03','2013-10-04','2013-10-05','2013-10-06','2013-10-07',
    '2014-01-01','2014-01-31','2014-02-01','2014-02-02','2014-02-03','2014-02-04','2014-02-05','2014-02-06',
    '2014-04-05','2014-04-06','2014-04-07','2014-05-01','2014-05-02','2014-05-03','2014-06-02',
    '2014-09-08','2014-10-01','2014-10-02','2014-10-03','2014-10-04','2014-10-05','2014-10-06','2014-10-07',
    '2015-01-01','2015-01-02','2015-01-03','2015-02-18','2015-02-19','2015-02-20','2015-02-21','2015-02-22','2015-02-23','2015-02-24',
    '2015-04-05','2015-04-06','2015-05-01','2015-05-02','2015-05-03','2015-06-20','2015-06-21','2015-06-22',
    '2015-09-27','2015-10-01','2015-10-02','2015-10-03','2015-10-04','2015-10-05','2015-10-06','2015-10-07',
    '2016-01-01','2016-01-02','2016-01-03','2016-02-07','2016-02-08','2016-02-09','2016-02-10','2016-02-11','2016-02-12','2016-02-13',
    '2016-04-04','2016-04-30','2016-05-01','2016-05-02','2016-06-09','2016-06-10','2016-06-11',
    '2016-09-15','2016-09-16','2016-09-17','2016-10-01','2016-10-02','2016-10-03','2016-10-04','2016-10-05','2016-10-06','2016-10-07',
    '2017-01-01','2017-01-02','2017-01-27','2017-01-28','2017-01-29','2017-01-30','2017-01-31','2017-02-01','2017-02-02',
    '2017-04-02','2017-04-03','2017-04-04','2017-04-29','2017-04-30','2017-05-01','2017-05-28','2017-05-29','2017-05-30',
    '2017-10-01','2017-10-02','2017-10-03','2017-10-04','2017-10-05','2017-10-06','2017-10-07','2017-10-08',
    '2018-01-01','2018-02-15','2018-02-16','2018-02-17','2018-02-18','2018-02-19','2018-02-20','2018-02-21',
    '2018-04-05','2018-04-06','2018-04-07','2018-04-29','2018-04-30','2018-05-01','2018-06-16','2018-06-17','2018-06-18',
    '2018-09-22','2018-09-23','2018-09-24','2018-10-01','2018-10-02','2018-10-03','2018-10-04','2018-10-05','2018-10-06','2018-10-07',
    '2019-01-01','2019-02-04','2019-02-05','2019-02-06','2019-02-07','2019-02-08','2019-02-09','2019-02-10',
    '2019-04-05','2019-05-01','2019-05-02','2019-05-03','2019-05-04','2019-06-07',
    '2019-09-13','2019-10-01','2019-10-02','2019-10-03','2019-10-04','2019-10-05','2019-10-06','2019-10-07',
    '2020-01-01','2020-01-24','2020-01-25','2020-01-26','2020-01-27','2020-01-28','2020-01-29','2020-01-30',
    '2020-04-04','2020-04-05','2020-04-06','2020-05-01','2020-05-02','2020-05-03','2020-05-04','2020-05-05',
    '2020-06-25','2020-06-26','2020-06-27','2020-10-01','2020-10-02','2020-10-03','2020-10-04','2020-10-05','2020-10-06','2020-10-08',
    '2021-01-01','2021-01-02','2021-01-03','2021-02-11','2021-02-12','2021-02-13','2021-02-14','2021-02-15','2021-02-16','2021-02-17',
    '2021-04-03','2021-04-04','2021-04-05','2021-05-01','2021-05-02','2021-05-03','2021-05-04','2021-05-05',
    '2021-06-12','2021-06-13','2021-06-14','2021-09-19','2021-09-20','2021-09-21','2021-10-01','2021-10-02','2021-10-03','2021-10-04','2021-10-05','2021-10-06','2021-10-07',
    '2022-01-01','2022-01-02','2022-01-03','2022-01-31','2022-02-01','2022-02-02','2022-02-03','2022-02-04','2022-02-05','2022-02-06',
    '2022-04-03','2022-04-04','2022-04-05','2022-04-30','2022-05-01','2022-05-02','2022-05-03','2022-05-04',
    '2022-06-03','2022-06-04','2022-06-05','2022-09-10','2022-09-11','2022-09-12','2022-10-01','2022-10-02','2022-10-03','2022-10-04','2022-10-05','2022-10-06','2022-10-07',
    '2023-01-01','2023-01-21','2023-01-22','2023-01-23','2023-01-24','2023-01-25','2023-01-26','2023-01-27',
    '2023-04-05','2023-04-29','2023-04-30','2023-05-01','2023-05-02','2023-05-03','2023-05-04','2023-05-05',
    '2023-06-22','2023-06-23','2023-06-24','2023-09-29','2023-09-30','2023-10-01','2023-10-02','2023-10-03','2023-10-04','2023-10-05','2023-10-06',
    '2024-01-01','2024-02-09','2024-02-10','2024-02-11','2024-02-12','2024-02-13','2024-02-14','2024-02-15',
    '2024-04-04','2024-05-01','2024-05-02','2024-05-03','2024-05-04','2024-06-10','2024-06-11','2024-06-12',
    '2024-09-15','2024-10-01','2024-10-02','2024-10-03','2024-10-04','2024-10-05','2024-10-06','2024-10-07'
]
    holiday_dates_converted = [datetime.datetime.strptime(date, '%Y-%m-%d').date() for date in holiday_dates]
    df['is_holiday'] = df['date'].apply(lambda x: '节假日' if x.date() in holiday_dates_converted else '正常日期')
    df['weekday'] = df['date'].apply(lambda x: x.isoweekday())
    weekday_dict = {1: '周一', 2: '周二', 3: '周三', 4: '周四', 5: '周五', 6: '周六', 7: '周日'}
    df['weekday_name'] = df['weekday'].map(weekday_dict)

    # Define the file path and save the CSV
    file_path = 'C:/NTL_Agent/report/csv/Pre-process_DailyNTL_time_series.csv'
    df.to_csv(file_path, index=False, encoding='utf-8-sig')

    return f"Pre-process Daily NTL mean value time series has been saved to the following locations: {', '.join(file_path)}"

# 输入模型
class TimeSeriesAnalysisInput(BaseModel):
    csv_path: str = Field(..., description="包含 'date'、'corrected_mean_ntl'、'is_holiday' 列的输入 CSV 文件路径。示例：'nightlight_data.csv'")
    use_normalized_data: bool = Field(default=False, description="是否使用归一化数据。默认值为 False。示例：False")
    interpolation_method: str = Field(default="time", description="插值方法，例如 'time'、'linear'、'spline'。默认值为 'time'。示例：'time'")
    seasonal_params: dict = Field(
        default={
            'monthly': 15,
            'yearly': 23
        },
        description=(
            "包含每周期季节性参数的字典：monthly 和 yearly。\n"
            "默认值：{'weekly': 15, 'monthly': 15, 'quarterly': 15, 'yearly': 23}\n"
            "示例：\n"
            "{\n"
            "    'monthly': 15,\n"
            "    'yearly': 23\n"
            "}"
        )
    )
    outlier_handling_method: str = Field(default="include", description="处理异常值的方法：'include' 或 'remove'。默认值为 'include'。示例：'include'")

    # 允许任意类型
    model_config = ConfigDict(arbitrary_types_allowed=True)

# def time_series_analysis(params: TimeSeriesAnalysisInput):
#     # 读取 CSV 文件
#     df = pd.read_csv(params.csv_path)
#
#     # 解包参数
#     use_normalized_data = params.use_normalized_data
#     interpolation_method = params.interpolation_method
#     seasonal_params = params.seasonal_params
#     outlier_handling_method = params.outlier_handling_method
#
#     # 确保 'date' 列为日期类型
#     df['date'] = pd.to_datetime(df['date'])
#
#     # 数据预处理
#     def preprocess_data(df, use_normalized_data, interpolation_method):
#         # 创建完整的日期范围
#         full_date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
#
#         # 设置 'date' 列为索引，并重新索引以包含所有日期
#         df_ts = df.set_index('date').reindex(full_date_range)
#
#         # 确定使用的夜光值列
#         ntl_column = 'corrected_mean_ntl_norm' if use_normalized_data else 'corrected_mean_ntl'
#
#         # 插值处理缺失值
#         df_ts[ntl_column] = df_ts[ntl_column].interpolate(method=interpolation_method)
#
#         # 填充 'is_holiday' 列的缺失值
#         df_ts['is_holiday'] = df_ts['is_holiday'].fillna('正常日期')
#
#         # 可视化原始数据
#         plt.figure(figsize=(12, 6), dpi=300)
#         plt.plot(df_ts.index, df_ts[ntl_column], color='blue', label='Original Nightlight Value')
#         plt.legend()
#         plt.grid(True)
#         plt.show()
#
#         return df_ts, ntl_column
#
#     # 对数变换
#     def log_transform(df_ts, ntl_column):
#         df_ts['ntl_log'] = np.log(df_ts[ntl_column] + 1e-6)
#         plt.figure(figsize=(12, 6), dpi=300)
#         plt.plot(df_ts.index, df_ts['ntl_log'], color='green', label='Log Transformed NTL')
#         plt.legend()
#         plt.grid(True)
#         plt.show()
#         return df_ts
#
#     # 周期性调整
#     def adjust_weekly_seasonality(df_ts, seasonal_param):
#         stl_week = STL(df_ts['ntl_log'], period=7, seasonal=seasonal_param, robust=True)
#         result_week = stl_week.fit()
#         fig = result_week.plot()
#         plt.show()
#         df_ts['week_trend'] = result_week.trend
#         df_ts['week_seasonal'] = result_week.seasonal
#         df_ts['week_resid'] = result_week.resid
#         return df_ts
#
#     # 异常值校正
#     def holiday_and_outlier_correction(df_ts):
#         # 创建 'holiday_dummy' 列
#         df_ts['holiday_dummy'] = df_ts['is_holiday'].apply(lambda x: 1 if x == '节假日' else 0)
#
#         resid = df_ts['week_resid'].dropna()
#         exog = df_ts[['holiday_dummy']].loc[resid.index]
#
#         # 使用 auto_arima 自动选择最佳模型
#         arima_model = auto_arima(resid, exogenous=exog, seasonal=False, trace=False, error_action='ignore', suppress_warnings=True)
#         order = arima_model.order
#         model = sm.tsa.ARIMA(resid, exog=exog, order=order)
#         results = model.fit()
#
#         # 可视化残差分布
#         plt.figure(figsize=(8, 4), dpi=300)
#         plt.hist(resid, bins=30, edgecolor='k')
#         plt.title('Residuals Distribution')
#         plt.xlabel('Residual')
#         plt.ylabel('Frequency')
#         plt.show()
#
#         # 使用模型预测残差并计算校正后的残差
#         resid_pred = results.predict()
#         df_ts['resid_corrected'] = df_ts['week_resid'] - resid_pred
#         return df_ts
#
#     # 月度季节性调整
#     def adjust_monthly_seasonality(df_ts, seasonal_param):
#         stl_month = STL(df_ts['resid_corrected'].dropna(), period=30, seasonal=seasonal_param, robust=True)
#         result_month = stl_month.fit()
#         result_month.plot()
#         plt.show()
#         df_ts['month_trend'] = result_month.trend
#         df_ts['month_seasonal'] = result_month.seasonal
#         df_ts['month_resid'] = result_month.resid
#         return df_ts
#
#     # 季度季节性调整
#     def adjust_quarterly_seasonality(df_ts, seasonal_param):
#         stl_quarter = STL(df_ts['month_resid'].dropna(), period=91, seasonal=seasonal_param, robust=True)
#         result_quarter = stl_quarter.fit()
#         result_quarter.plot()
#         plt.show()
#         df_ts['quarter_trend'] = result_quarter.trend
#         df_ts['quarter_seasonal'] = result_quarter.seasonal
#         df_ts['quarter_resid'] = result_quarter.resid
#         return df_ts
#
#     # 执行数据预处理和季节性调整
#     df_ts, ntl_column = preprocess_data(df, use_normalized_data, interpolation_method)
#     df_ts = log_transform(df_ts, ntl_column)
#     df_ts = adjust_weekly_seasonality(df_ts, seasonal_params['weekly'])
#     df_ts = holiday_and_outlier_correction(df_ts)
#     df_ts = adjust_monthly_seasonality(df_ts, seasonal_params['monthly'])
#     df_ts = adjust_quarterly_seasonality(df_ts, seasonal_params['quarterly'])
#
#     # 重置索引以包含 'date' 列
#     df_ts.reset_index(inplace=True)
#     df_ts.rename(columns={'index': 'date'}, inplace=True)
#
#     # 确保 'date' 列为日期类型且没有时间部分
#     df_ts['date'] = pd.to_datetime(df_ts['date']).dt.date
#
#     # 可选：如果需要，可以重新设置 'date' 列为 datetime 类型但无时间部分
#     # df_ts['date'] = pd.to_datetime(df_ts['date'])
#
#     # 保存 CSV 文件，确保 'date' 列作为数据列存在
#     file_path = 'nightlight_time_series_analysis.csv'  # 根据您的环境调整路径
#     df_ts.to_csv(file_path, index=False, encoding='utf-8-sig')
#
#     # 返回保存的文件路径，以支持后续的异常检测
#     # 将前 5 行数据转换为字符串并包含在返回信息中
#     return f"Nightlight_time_series_analysis completed. Results saved at: {file_path}\n\nTop rows:\n{df_ts.head().to_string()}"

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import STL

def time_series_analysis(params: TimeSeriesAnalysisInput):
    # 读取 CSV 文件
    df = pd.read_csv(params.csv_path)

    # 解包参数
    use_normalized_data = params.use_normalized_data
    interpolation_method = params.interpolation_method
    seasonal_params = params.seasonal_params

    # 确保 'date' 列为日期类型
    df['date'] = pd.to_datetime(df['date'])

    # 数据预处理函数
    def preprocess_data(df, use_normalized_data, interpolation_method):
        # 创建完整的日期范围
        full_date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')

        # 设置 'date' 列为索引，并重新索引以包含所有日期
        df_ts = df.set_index('date').reindex(full_date_range)

        # 根据参数使用标准化或未标准化的夜光值列
        ntl_column = 'corrected_mean_ntl_norm' if use_normalized_data else 'corrected_mean_ntl'

        # 插值处理缺失值
        df_ts[ntl_column] = df_ts[ntl_column].interpolate(method=interpolation_method)

        # 填充 'is_holiday' 列的缺失值（如果没有此列，则创建）
        if 'is_holiday' not in df_ts.columns:
            df_ts['is_holiday'] = '正常日期'
        else:
            df_ts['is_holiday'] = df_ts['is_holiday'].fillna('正常日期')

        # 可视化原始数据
        plt.figure(figsize=(12, 6), dpi=300)
        plt.plot(df_ts.index, df_ts[ntl_column], color='blue', label='Original Nightlight Value')
        plt.legend()
        plt.grid(True)
        plt.show()

        return df_ts, ntl_column

    # 对数变换
    def log_transform(df_ts, ntl_column):
        df_ts['ntl_log'] = np.log(df_ts[ntl_column] + 1e-6)
        plt.figure(figsize=(12, 6), dpi=300)
        plt.plot(df_ts.index, df_ts['ntl_log'], color='green', label='Log Transformed NTL')
        plt.legend()
        plt.grid(True)
        plt.show()
        return df_ts

    # 月度季节性分解并去除
    def remove_monthly_seasonality(df_ts, seasonal_param):
        stl_month = STL(df_ts['ntl_log'].dropna(), period=30, seasonal=seasonal_param, robust=True)
        result_month = stl_month.fit()
        result_month.plot()
        plt.show()

        df_ts['month_trend'] = result_month.trend
        df_ts['month_seasonal'] = result_month.seasonal
        df_ts['month_resid'] = result_month.resid

        # 去除月度季节性项：after_month = trend + resid
        df_ts['final_adjusted'] = df_ts['month_trend'] + df_ts['month_resid']

        return df_ts

    # # 年度季节性分解并去除
    # def remove_yearly_seasonality(df_ts, seasonal_param):
    #     stl_year = STL(df_ts['after_month'].dropna(), period=90, seasonal=seasonal_param, robust=True)
    #     result_year = stl_year.fit()
    #     result_year.plot()
    #     plt.show()
    #
    #     df_ts['year_trend'] = result_year.trend
    #     df_ts['year_seasonal'] = result_year.seasonal
    #     df_ts['year_resid'] = result_year.resid
    #
    #     # 最终去除年度季节性后的数据 = trend + resid
    #     df_ts['final_adjusted'] = df_ts['year_trend'] + df_ts['year_resid']
    #
    #     return df_ts

    # 利用IQR方法识别year_resid中的异常值
    def detect_outliers(df_ts):
        resid = df_ts['month_resid'].dropna()
        Q1 = resid.quantile(0.25)
        Q3 = resid.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # 定义列is_outlier
        df_ts['is_outlier'] = False
        df_ts.loc[(df_ts['month_resid'] < lower_bound) | (df_ts['month_resid'] > upper_bound), 'is_outlier'] = True

        # 可视化箱线图
        plt.figure(figsize=(6, 4), dpi=300)
        plt.boxplot(resid, vert=False)
        plt.title('Year Residuals Boxplot')
        plt.show()

        return df_ts

    # 执行数据预处理和季节性调整
    df_ts, ntl_column = preprocess_data(df, use_normalized_data, interpolation_method)
    df_ts = log_transform(df_ts, ntl_column)
    df_ts = remove_monthly_seasonality(df_ts, seasonal_params.get('monthly'))
    # df_ts = remove_yearly_seasonality(df_ts, seasonal_params.get('yearly'))
    df_ts = detect_outliers(df_ts)

    # 重置索引以包含 'date' 列
    df_ts.reset_index(inplace=True)
    df_ts.rename(columns={'index': 'date'}, inplace=True)

    # 确保 'date' 列为日期类型且没有时间部分
    df_ts['date'] = pd.to_datetime(df_ts['date']).dt.date

    # 保存 CSV 文件
    file_path = 'nightlight_time_series_analysis.csv'
    export_folder = 'C:/NTL_Agent/report/csv'
    export_path = os.path.join(export_folder, file_path)
    df_ts.to_csv(export_path, index=False, encoding='utf-8-sig')

    # 返回保存的文件路径
    return f"NTL time series seasonal adjustment completed. Results saved at: {export_path}\n\nTop rows:\n{df_ts.head().to_string()}"


# 输入模型
class AnomalyDetectionInput(BaseModel):
    new_ntl_value: float = Field(..., description="新的夜间灯光平均值。示例：20.5")
    new_date_str: str = Field(..., description="新的日期，格式为 'YYYY-MM-DD'。示例：'2023-03-01'")
    processed_csv_path: str = Field(..., description="之前处理后的时间序列 CSV 文件路径。示例：'nightlight_time_series_analysis.csv'")
    # 允许任意类型
    model_config = ConfigDict(arbitrary_types_allowed=True)

def detect_anomaly(params: AnomalyDetectionInput):
    """
    接受用户输入的新夜间灯光值和日期，判断是否为异常值。
    """
    new_ntl_value = params.new_ntl_value
    new_date_str = params.new_date_str
    processed_csv_path = params.processed_csv_path
    ntl_column = "corrected_mean_ntl"

    # 读取处理后的时间序列数据
    df_ts = pd.read_csv(processed_csv_path, index_col=0, parse_dates=True)
    df_ts_month_extended = df_ts.copy()

    # 将输入的日期字符串转换为 datetime 对象
    new_date = pd.to_datetime(new_date_str)
    holiday_dates = [
    '2024-01-01', '2024-02-09', '2024-02-10', '2024-02-11', '2024-02-12', '2024-02-13', '2024-02-14', '2024-02-15',
    '2024-04-04', '2024-05-01', '2024-05-02', '2024-05-03', '2024-05-04', '2024-06-10', '2024-06-11', '2024-06-12',
    '2024-09-15', '2024-10-01', '2024-10-02', '2024-10-03', '2024-10-04', '2024-10-05', '2024-10-06', '2024-10-07'
    ]
    holiday_date = pd.to_datetime(holiday_dates)  # 将字符串日期列表转换为 datetime 格式
    is_holiday = '节假日' if new_date in holiday_date else '正常日期'

    # 合并趋势分量
    trend_series = df_ts['week_trend'] + df_ts_month_extended['month_trend'] + df_ts_month_extended['quarter_trend']
    trend_series.index = pd.to_datetime(trend_series.index)

    # 使用 auto_arima 自动选择模型参数并拟合模型
    model = pm.auto_arima(trend_series.dropna(), seasonal=False, stepwise=True, suppress_warnings=True)
    model_fit = model.fit(trend_series.dropna())

    # 计算需要预测的天数
    forecast_steps = (new_date - trend_series.index[-1]).days
    if forecast_steps <= 0:
        return {"error": "新日期应在已知数据之后"}

    future_dates = pd.date_range(start=trend_series.index[-1] + pd.Timedelta(days=1), periods=forecast_steps)

    # 预测趋势
    trend_forecast = model_fit.predict(n_periods=forecast_steps)
    trend_forecast = pd.Series(trend_forecast, index=future_dates)

    # 获取新日期的趋势预测值
    new_trend_value = trend_forecast.loc[new_date]

    # 提取周季节性分量并计算星期几的平均周季节性
    df_ts['weekday'] = df_ts.index.weekday
    weekday_seasonal_avg = df_ts.groupby('weekday')['week_seasonal'].mean()
    new_weekday = new_date.weekday()
    new_week_seasonal = weekday_seasonal_avg.loc[new_weekday]

    # 获取月度季节性分量
    month_seasonal = df_ts_month_extended['month_seasonal']
    month_seasonal.index = pd.to_datetime(month_seasonal.index)
    month_seasonal_avg = month_seasonal.groupby(month_seasonal.index.month).mean()
    new_month = new_date.month

    if new_month in month_seasonal_avg.index:
        new_month_seasonal = month_seasonal_avg.loc[new_month]
    else:
        # 如果缺少当前月份的数据，使用所有月份的平均值
        new_month_seasonal = month_seasonal_avg.mean()
        print(f"警告：缺少月份 {new_month} 的季节性数据，已使用所有月份的平均值代替。")

    # 获取季度季节性分量
    df_ts_month_extended['quarter'] = df_ts_month_extended.index.quarter
    quarter_seasonal = df_ts_month_extended['quarter_seasonal']
    quarter_seasonal_avg = quarter_seasonal.groupby(df_ts_month_extended['quarter']).mean()
    new_quarter = new_date.quarter

    if new_quarter in quarter_seasonal_avg.index:
        new_quarter_seasonal = quarter_seasonal_avg.loc[new_quarter]
    else:
        # 如果缺少当前季度的数据，使用所有季度的平均值
        new_quarter_seasonal = quarter_seasonal_avg.mean()
        print(f"警告：缺少季度 {new_quarter} 的季节性数据，已使用所有季度的平均值代替。")

    # 计算调整后的对数预期值并还原到原始尺度
    adjusted_log = new_trend_value + new_week_seasonal + new_month_seasonal + new_quarter_seasonal
    adjusted_ntl = np.exp(adjusted_log) - 1e-6

    # 计算残差及其 z-score
    residual = new_ntl_value - adjusted_ntl
    adjusted_ntl_series = np.exp(df_ts['week_trend'] + df_ts['week_seasonal'] + df_ts['month_trend'] + df_ts['month_seasonal'] + df_ts['quarter_trend'] + df_ts['quarter_seasonal']) - 1e-6
    residuals = (adjusted_ntl_series - df_ts[ntl_column]).dropna()
    resid_mean = residuals.mean()
    resid_std = residuals.std()
    z_score = (residual - resid_mean) / resid_std

    # 异常等级判断
    if np.abs(z_score) > 3:
        anomaly_level = '严重异常'
    elif np.abs(z_score) > 2:
        anomaly_level = '中度异常'
    elif np.abs(z_score) > 1:
        anomaly_level = '轻度异常'
    else:
        anomaly_level = '正常'

    weekday_name = new_date.strftime('%A')  # 星期几名称

    # 构建结果字典
    result = {
        '日期': new_date_str,
        '预测的夜光值': adjusted_ntl,
        '输入的夜光值': new_ntl_value,
        '残差': residual,
        'z-score': z_score,
        '异常等级': anomaly_level,
        '是否节假日': is_holiday,
        '星期几': weekday_name
    }

    return result


# Initialize Python REPL to execute code
python_repl = PythonREPL()

# Define the StructuredTool
DailyNTL_preprocess_tool = StructuredTool.from_function(
    DailyNTL_preprocess,
    name="DailyNTL_preprocess",
    description=(
        "此工具仅用于预处理指定行政区划和日期范围内的每日尺度 VIIRS 夜间灯光数据，得到预处理后该行政区划在所需日期内的夜间灯光影像校正后每日均值。"
        "它从 Google Earth Engine 获取原始每日夜光数据，对其进行校正以消除异常，"
        "并生成一个包含校正后夜光值的 CSV 文件。输出的文件还包括每个日期是否为节假日、星期几等附加信息，方便后续分析。\n\n"
        "**示例输入：**\n"
        "start_date = '2023-01-01'\n"
        "end_date = '2023-02-01'\n"
        "study_area = '黄浦区'\n\n"
        "**示例输出：**\n"
        "返回一个字符串，表示保存预处理后数据的 CSV 文件路径，例如 'C:/NTL_Agent/report/csv/nightlight_data.csv'。"
    ),
    input_type=[str, str, str],
)
# 定义 StructuredTool
time_series_tool = StructuredTool.from_function(
    time_series_analysis,
    name="time_series_analysis_tool",
    description=(
        "此工具用于对预处理后的地区每日尺度夜间灯光均值数据 CSV 文件执行时间序列分析和季节性调整。"
        "工具包括数据预处理、对数变换、周/月/季度季节性调整，以及对节假日和异常值的校正。"
        "最终生成一个包含分析结果的 CSV 文件，供异常检测工具使用。\n\n"
        "**示例输入：**\n"
        "TimeSeriesAnalysisInput(\n"
        "    csv_path='nightlight_data.csv',\n"
        "    use_normalized_data=False,\n"
        "    interpolation_method='time',\n"
        "    seasonal_params={\n"
        "        'weekly': 15,\n"
        "        'monthly': 15,\n"
        "        'quarterly': 15,\n"
        "        'yearly': 23\n"
        "    },\n"
        "    outlier_handling_method='include'\n"
        ")\n\n"
        "**示例输出：**\n"
        "返回一个字符串，表示保存分析后数据的 CSV 文件路径，例如 'C:/NTL_Agent/report/csv/nightlight_time_series_analysis.csv'。"
    ),
    input_type=TimeSeriesAnalysisInput,
)

# 定义 StructuredTool
anomaly_detection_tool = StructuredTool.from_function(
    detect_anomaly,
    name="daily_anomaly_detection_tool",
    description=(
        "此工具接受新日期的日度夜间灯光均值，基于之前分析的时间序列数据，判断该值是否为异常。"
        "工具使用时间序列模型预测给定日期的预期夜光值，计算输入值与预测值之间的残差和 z-score，"
        "并根据偏差程度确定异常等级。\n\n"
        "**示例输入：**\n"
        "AnomalyDetectionInput(\n"
        "    new_ntl_value=20.5,\n"
        "    new_date_str='2023-03-01',\n"
        "    processed_csv_path='nightlight_time_series_analysis.csv',\n"
        ")\n\n"
        "**示例输出：**\n"
        "{\n"
        "    '日期': '2023-03-01',\n"
        "    '预测的夜光值': 18.7,\n"
        "    '输入的夜光值': 20.5,\n"
        "    '残差': 1.8,\n"
        "    'z-score': 1.2,\n"
        "    '异常等级': '轻度异常',\n"
        "    '是否节假日': '正常日期',\n"
        "    '星期几': '周三'\n"
        "}"
    ),
    input_type=AnomalyDetectionInput,
)


# 定义输入参数
class RemoteSensingDataInput(BaseModel):
    study_area: str = Field(..., description="感兴趣的研究区域名称。示例：'南京市'")
    scale_level: str = Field(..., description="尺度级别，例如 'city'、'county'。示例：'city'")
    dataset_choice: str = Field(..., description="数据类型选择：'worldpop'、'MODIS_LC'、'MODIS_NDVI'、'MODIS_NPP'。示例：'worldpop'")
    export_folder: str = Field(..., description="导出文件的本地文件夹路径。示例：'C:/NTL_Agent/other_RS_data/worldpop'")
    time_range_input: str = Field(None, description="时间范围，格式为 'YYYY-MM to YYYY-MM'。示例：'2020-01 to 2020-02'，可选项")

# 修改后的下载函数
def fetch_and_download_rs_data(
    study_area: str,
    scale_level: str,
    dataset_choice: str,
    export_folder: str,
    time_range_input: str = None
):
    # 选择区域边界
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    def get_administrative_boundaries(scale_level):
        directly_governed_cities = ['北京市', '天津市', '上海市', '重庆市']
        if scale_level == 'province' or (scale_level == 'city' and study_area in directly_governed_cities):
            admin_boundary = province_collection
            name_property = 'name'
        elif scale_level == 'national':
            admin_boundary = national_collection
            name_property = 'NAME'
        elif scale_level == 'city':
            admin_boundary = city_collection
            name_property = 'name'
        elif scale_level == 'county':
            admin_boundary = county_collection
            name_property = 'name'
        else:
            raise ValueError("Unknown scale level. Options are 'national', 'province', 'city', or 'county'.")
        return admin_boundary, name_property

    admin_boundary, name_property = get_administrative_boundaries(scale_level)
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))

    # 时间范围解析（可选）
    def parse_time_range(time_range_input):
        if not time_range_input:
            return None, None
        time_range_input = time_range_input.replace(' ', '')
        if 'to' in time_range_input:
            start_str, end_str = time_range_input.split('to')
        else:
            raise ValueError("Invalid time range format. Use 'YYYY to YYYY', 'YYYY-MM to YYYY-MM', or 'YYYY-MM-DD to YYYY-MM-DD'.")
        start_str, end_str = start_str.strip(), end_str.strip()

        start_date = f"{start_str}-01-01"
        end_date = f"{end_str}-12-31"

        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
            raise ValueError("Start date cannot be later than end date.")
        return start_date, end_date

    start_date, end_date = parse_time_range(time_range_input)

    # 根据数据类型选择影像集合
    if dataset_choice.lower() == 'worldpop':
        collection = ee.ImageCollection("WorldPop/GP/100m/pop").filterBounds(region.geometry())
    elif dataset_choice.lower() == 'modis_lc':
        collection = ee.ImageCollection("MODIS/061/MCD12Q1").filterBounds(region.geometry()).select('LC_Type1')
    elif dataset_choice.lower() == 'modis_ndvi':
        collection = ee.ImageCollection("MODIS/061/MOD13A1").filterBounds(region.geometry()).select('NDVI')
        if start_date and end_date:
            collection = collection.filterDate(start_date, end_date)
    elif dataset_choice.lower() == 'modis_npp':
        collection = ee.ImageCollection("MODIS/061/MOD17A3HGF").filterBounds(region.geometry()).select('Npp')
        if start_date and end_date:
            collection = collection.filterDate(start_date, end_date)
    else:
        raise ValueError("Unknown dataset choice.")

    # 导出数据
    os.makedirs(export_folder, exist_ok=True)
    images_list = collection.toList(collection.size())
    for i in range(collection.size().getInfo()):
        image = ee.Image(images_list.get(i))
        image_date = image.date().format('YYYY-MM-dd').getInfo() if start_date and end_date else 'Annual'
        export_path = os.path.join(export_folder, f"{study_area}_{dataset_choice}_{image_date}.tif")
        geemap.ee_export_image(
            ee_object=image,
            filename=export_path,
            scale=500 if dataset_choice in ['modis_lc', 'modis_ndvi', 'modis_npp'] else 100,
            region=region.geometry(),
            crs='EPSG:4326',
            file_per_band=False
        )
        print(f"Image exported to: {export_path}")
    return f"数据已保存至指定文件夹: {export_folder}"

# 定义遥感数据下载工具
remote_sensing_download_tool = StructuredTool.from_function(
    fetch_and_download_rs_data,
    name="remote_sensing_download_tool",
    description=(
        "此工具从 Google Earth Engine 下载指定参数的遥感数据，并将其导出到指定的本地文件夹。"
        "该工具支持不同的数据集，包括世界人口数据（worldpop）、MODIS土地分类数据（MODIS_LC）、"
        "MODIS NDVI数据（MODIS_NDVI）、MODIS NPP数据（MODIS_NPP）。"
        "可以选择是否指定时间范围，MOD17A3和MCD12Q1数据可以不提供时间范围。"
        "数据将以 .tif 格式保存在本地。\n\n"
        "**示例输入：**\n"
        "RemoteSensingDataInput(\n"
        "    study_area='南京市',\n"
        "    scale_level='city',\n"
        "    dataset_choice='modis_npp',\n"
        "    time_range_input='2020-01 to 2020-12',\n"
        "    export_folder='C:/NTL_Agent/other_RS_data/modis_npp'\n"
        ")\n\n"
        "**示例输出：**\n"
        "图像已保存至指定的 `export_folder`，例如 'C:/NTL_Agent/other_RS_data/modis_npp/南京市_modis_npp_2020-01-01.tif'。"
    ),
    input_type=RemoteSensingDataInput,
)
