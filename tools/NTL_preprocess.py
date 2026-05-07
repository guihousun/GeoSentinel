# -*- coding: utf-8 -*-
import h5py
from pyresample import geometry
from typing import Optional
from skimage.transform import radon
from scipy.signal import find_peaks
from skimage import morphology
import numpy as np
from osgeo import gdal
import xarray as xr
import dask.array as da
from satpy import Scene
from pyresample import create_area_def
from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field
import gc
import os
from storage_manager import storage_manager
import rasterio

def Strip_removal(img_input_path: str, img_output_path: str, theta, threshold, method):
    gdal.AllRegister()
    input_path = storage_manager.resolve_input_path(img_input_path)
    output_path = storage_manager.resolve_output_path(img_output_path)
    img = gdal.Open(input_path)  # 璇诲彇鏂囦欢 \ read file

    if img is None:
        raise FileNotFoundError('Unable to open input image: ' + input_path)
    im_proj = (img.GetProjection())  # 璇诲彇鎶曞奖 \ Read Projection
    im_Geotrans = (img.GetGeoTransform())  # 璇诲彇浠垮皠鍙樻崲 \ Read Affine Transformation

    w = img.RasterXSize  # 鍒楁暟 \ Number of columns
    h = img.RasterYSize  # 琛屾暟 \ Number of rows

    band1 = img.GetRasterBand(1)  # 鑾峰彇鏍呮牸鍥惧儚涓変釜娉㈡ \ Acquire three bands of raster images
    band2 = img.GetRasterBand(2)
    band3 = img.GetRasterBand(3)

    # 灏嗗浘鍍忚鍙栦负鏁扮粍 \ Reading an image as an array
    band1 = band1.ReadAsArray(0, 0, w, h)
    band2 = band2.ReadAsArray(0, 0, w, h)
    band3 = band3.ReadAsArray(0, 0, w, h)

    # 鍒涘缓鍏ㄤ负1鐨勬ā鏉挎暟缁?\ Create a template array with all 1's
    I1 = np.ones((h, w), dtype=np.uint8)
    I2 = np.ones((h, w), dtype=np.uint8)
    I3 = np.ones((h, w), dtype=np.uint8)

    '''鐢变簬high-resolution nightlight澶滃厜鏁版嵁鍦ㄤ笉鍚屾椂鏈熺殑鍥惧儚鑳屾櫙鍊间笉鍚岋紝瀛樺湪宸紓锛屾棭鏈熷浘鍍忚儗鏅€间负7锛?
       涓轰簡缁熶竴鏁版嵁鐨勫€艰寖鍥达紝灏嗚儗鏅€间负1鐨勫浘鍍忎篃澶勭悊鎴愯儗鏅€间负7鐨勫浘鍍?
       Due to differences in image background values for high-resolution nightlight noctilucent data during different periods, the early image background value is 7.
       In order to unify the value range of the data, images with a background value of 1 are also processed into images with a background value of 7'''
    one_loc = np.where(band1 == 1)
    one_loc = np.array(one_loc)
    b = one_loc.size
    if b != 0:
        band1[band1 == 1] += 6
        band2[band2 == 1] += 6
        band3[band3 == 1] += 6
        for i in range(h):
            for j in range(w):
                if band1[i, j] == 6:
                    band1[i, j] = 0
                if band2[i, j] == 6:
                    band2[i, j] = 0
                if band3[i, j] == 6:
                    band3[i, j] = 0

    # 妫€娴嬫潯甯︽綔鍦ㄥ儚鍏冿紝浣嗘槸骞朵笉閮芥槸鏉″甫 \ Detect potential pixels in stripes, but not all stripes
    for i in range(h):
        for j in range(w):
            if band1[i, j] == 7 and band2[i, j] == 7 and band3[i, j] == 7:
                I1[i, j] = 0
                I2[i, j] = 0
                I3[i, j] = 0
            if band1[i, j] > 7:
                I1[i, j] = 0

            if band2[i, j] > 7:
                I2[i, j] = 0

            if band3[i, j] > 7:
                I3[i, j] = 0
    # 鎵惧嚭鏉″甫鎵€鍦ㄧ殑浣嶇疆 \ Find the location of the strip
    moban1 = RGB_Stripe_loc(I1, theta, threshold=threshold)
    moban2 = RGB_Stripe_loc(I2, theta, threshold=threshold)
    moban3 = RGB_Stripe_loc(I3, theta, threshold=threshold)

    # 鍘婚櫎娼滃湪鏉″甫鍍忓厓涓殑鏉″甫 \ Removing Stripes from Potential Striped Pixels
    m1 = moban1 * I1
    m2 = moban2 * I2
    m3 = moban3 * I3

    # 寰楀埌涓変釜閫氶亾鎵€鏈夋潯甯︾殑浣嶇疆 \ Obtain the positions of all stripes in the three channels
    m = np.logical_not(m1) * np.logical_not(m2) * np.logical_not(m3)

    # 瀵瑰師濮嬮€氶亾杩涜鏉″甫鍘婚櫎 \ Striping the original channel
    b1 = m * band1
    b2 = m * band2
    b3 = m * band3

    # 瀵瑰幓闄ょ殑鍍忓厓杩涜8鍍忓厓宸€硷紝浣跨敤涓夊垎浣嶆暟杩涜鎻掑€?\ Perform an 8-pixel difference on the removed pixels and interpolate using the third quantile
    if method == 'median':
        for i in range(h - 2):
            for j in range(w - 2):
                if b1[i, j] and b2[i, j] and b3[i, j] == 7:
                    b1[i, j] = np.median([b1[i - 1, j - 1], b1[i, j - 1], b1[i + 1, j - 1], b1[i - 1, j], b1[i + 1, j],
                                          b1[i + 1, j - 1], b1[i, j + 1], b1[i + 1, j + 1], b1[i - 1, j + 2],
                                          b1[i, j + 2], b1[i + 1, j + 2]])
                    b2[i, j] = np.median([b2[i - 1, j - 1], b2[i, j - 1], b2[i + 1, j - 1], b2[i - 1, j], b2[i + 1, j],
                                          b2[i + 1, j - 1], b2[i, j + 1], b2[i + 1, j + 1], b2[i - 1, j + 2],
                                          b2[i, j + 2], b2[i + 1, j + 2]])
                    b3[i, j] = np.median([b3[i - 1, j - 1], b3[i, j - 1], b3[i + 1, j - 1], b3[i - 1, j], b3[i + 1, j],
                                          b3[i + 1, j - 1], b3[i, j + 1], b3[i + 1, j + 1], b3[i - 1, j + 2],
                                          b3[i, j + 2], b3[i + 1, j + 2]])
    else:
        for i in range(h - 2):
            for j in range(w - 2):
                if b1[i, j] and b2[i, j] and b3[i, j] == 7:
                    b1[i, j] = np.percentile(
                        [b1[i - 1, j - 1], b1[i, j - 1], b1[i + 1, j - 1], b1[i - 1, j], b1[i + 1, j],
                         b1[i + 1, j - 1], b1[i, j + 1], b1[i + 1, j + 1], b1[i - 1, j + 2], b1[i, j + 2],
                         b1[i + 1, j + 2]], 75)
                    b2[i, j] = np.percentile(
                        [b2[i - 1, j - 1], b2[i, j - 1], b2[i + 1, j - 1], b2[i - 1, j], b2[i + 1, j],
                         b2[i + 1, j - 1], b2[i, j + 1], b2[i + 1, j + 1], b2[i - 1, j + 2], b2[i, j + 2],
                         b2[i + 1, j + 2]], 75)
                    b3[i, j] = np.percentile(
                        [b3[i - 1, j - 1], b3[i, j - 1], b3[i + 1, j - 1], b3[i - 1, j], b3[i + 1, j],
                         b3[i + 1, j - 1], b3[i, j + 1], b3[i + 1, j + 1], b3[i - 1, j + 2], b3[i, j + 2],
                         b3[i + 1, j + 2]], 75)

    # 澶勭悊寮傚父鍍忓厓 \ Handling abnormal pixels
    for i in range(h):
        for j in range(w):
            # 灏忎簬7瑙嗕负鑳屾櫙鍊?\ Less than 7 is considered a background value
            if b1[i, j] < 7 or b2[i, j] < 7 or b3[i, j] < 7:
                b1[i, j] = b2[i, j] = b3[i, j] = 7

            # 鍙?涓烘潯甯﹁涓鸿儗鏅€?\ Double 7 is set as the background value for the stripe
            if (b1[i, j] == 7 and b2[i, j] == 7) or (b1[i, j] == 7 and b3[i, j] == 7) or (
                    b2[i, j] == 7 and b3[i, j] == 7):
                b1[i, j] = b2[i, j] = b3[i, j] = 7
    # 浜屽€煎寲 \ Binarization
    b11 = np.where(b1 > 7, 1, 0)
    b22 = np.where(b2 > 7, 1, 0)
    b33 = np.where(b3 > 7, 1, 0)

    # 鍘婚櫎杩為€氬煙涓?涓儚鍏冪殑鍣０ \ Remove noise with a connected domain of 8 pixels
    b1_bw = bwareaopen(b11, 8)
    b2_bw = bwareaopen(b22, 8)
    b3_bw = bwareaopen(b33, 8)

    # 灏嗕笁涓€氶亾鏁扮粍鍚堜负涓€涓笁缁存暟缁?\ Combine three channel numbers into a three-dimensional array
    out = np.array([b1_bw * b1, b2_bw * b2, b3_bw * b3])
    # 杈撳嚭鍥惧儚 \ Output Image
    write_img(output_path, im_proj, im_Geotrans, out)


def RGB_Stripe_loc(I, theta, threshold):
    a = np.rint((max(theta) - min(theta) / 0.01))  # 璁剧疆瑙掑害鑼冨洿 \ Set Angle Range
    R = radon(I, theta=theta,
              preserve_range=True)  # 璁＄畻鍚勮搴︽柟鍚戜笅鏉″甫娼滃湪鍍忓厓妯℃澘鐨勭Н鍒嗗€?\ Calculate the integration value of the potential pixel template of the strip in each angular direction
    h, w = I.shape
    si = R.size / a
    dd = I
    moban = np.zeros((h, w))
    ij = 0

    while ij < a:
        [loc, peaks] = find_peaks(R[:, ij],
                                  threshold=threshold)  # 鏍规嵁涓€瀹氱殑闃堝€艰寖鍥村鎵剧Н鍒嗗嘲鍊?\ Find the integration peak value according to a certain threshold range
        # 鎵惧嚭婊¤冻鏉′欢宄板€肩殑鏉″甫
        for iw in range(len(loc)):
            k = np.tan((theta[ij] - 90) * np.pi / 180)
            for j in range(h):
                xx = np.int(loc[iw] + j)
                yy = np.int(k * j)
                if loc[iw] + j < w and yy + 6 < h:
                    moban[yy, xx] = 1
                    moban[yy + 1, xx] = 1
                    moban[yy + 2, xx] = 1
                    moban[yy + 3, xx] = 1
                    moban[yy + 4, xx] = 1
                    moban[yy + 5, xx] = 1
                    moban[yy + 6, xx] = 1

        ij += 1

    return moban


# 杈撳嚭鍥惧儚 \ Output Image
def write_img(filename, im_proj, im_geotrans, im_data):
    # 鍒ゆ柇鏍呮牸鏁版嵁鐨勬暟鎹被鍨?\ Determine the data type of raster data
    if 'int8' in im_data.dtype.name:
        datatype = gdal.GDT_Byte
    elif 'int16' in im_data.dtype.name:
        datatype = gdal.GDT_UInt16
    else:
        datatype = gdal.GDT_Float32

    # 鍒よ鏁扮粍缁存暟 \ Interpreting array dimensions
    if len(im_data.shape) == 3:
        im_bands, im_height, im_width = im_data.shape
    else:
        im_bands, (im_height, im_width) = 1, im_data.shape

    # 鍒涘缓鏂囦欢 \ create a file
    driver = gdal.GetDriverByName("GTiff")
    dataset = driver.Create(filename, im_width, im_height, im_bands, datatype)

    dataset.SetGeoTransform(im_geotrans)  # 鍐欏叆浠垮皠鍙樻崲鍙傛暟 \ Write affine transformation parameters
    dataset.SetProjection(im_proj)  # 鍐欏叆鎶曞奖 \ Write Projection

    if im_bands == 1:
        dataset.GetRasterBand(1).WriteArray(im_data)  # 鍐欏叆鏁扮粍鏁版嵁 \ Write array data
    else:
        for i in range(im_bands):
            dataset.GetRasterBand(i + 1).WriteArray(im_data[i])

    del dataset


def bwareaopen(image, threshold):
    # 鍘婚櫎灏忎簬闃堝€肩殑杩為€氬尯鍩? \ Remove connected areas that are smaller than the threshold
    filtered_image = morphology.remove_small_objects(image.astype(bool), min_size=threshold, connectivity=1).astype(
        np.uint8)

    return filtered_image


class StripRemovalInput(BaseModel):
    img_input_filename: str = Field(
        ..., 
        description="Filename of the high-resolution RGB nightlight image in your 'inputs/' folder (e.g., 'city_night.tif')"
    )
    img_output_filename: str = Field(
        ..., 
        description="Output filename to save in 'outputs/' (e.g., 'city_night_stripe_removed.tif')"
    )
    method: str = Field(
        ..., 
        description="Interpolation method: 'median' or 'percentile'"
    )
    start_angle: Optional[int] = Field(80, description="Start angle for Radon transform (default: 80)")
    end_angle: Optional[int] = Field(100, description="End angle for Radon transform (default: 100)")
    threshold: Optional[float] = Field(80.0, description="Peak detection threshold (default: 80.0)")


def run_strip_removal(
        img_input_filename: str,
        img_output_filename: str,
        method: str,
        start_angle: int = 80,
        end_angle: int = 100,
        threshold: float = 80
) -> str:
    method = (method or "").strip().lower()
    if method not in {"median", "percentile"}:
        return "Error: Invalid method. Choose 'median' or 'percentile'."

    theta = np.arange(start_angle, end_angle)
    img_input_path = storage_manager.resolve_input_path(img_input_filename)
    img_output_path = storage_manager.resolve_output_path(img_output_filename)

    if not os.path.exists(img_input_path):
        return f"Error: Input image not found at {img_input_path}"

    try:
        Strip_removal(img_input_path, img_output_path, theta=theta, threshold=threshold, method=method)
        return f"Striping removed. Output saved to {img_output_path}"
    except Exception as e:
        return f"Error during strip removal: {str(e)}"

class RGBRadianceCalibInput(BaseModel):
    input_filename: str = Field(..., description="high-resolution RGB nightlight image filename in 'inputs/' (e.g., 'raw_city.tif')")
    output_rgb_filename: str = Field(..., description="Calibrated RGB output filename in 'outputs/' (e.g., 'city_radiance_rgb.tif')")
    output_gray_filename: str = Field(..., description="Grayscale luminance output filename in 'outputs/' (e.g., 'city_luminance.tif')")



from osgeo import gdal
import numpy as np

def calibrate_rgb_from_calib_file(
    input_filename: str,
    output_rgb_filename: str,
    output_gray_filename: str
) -> str:
    # Based on official high-resolution nightlight calibration coefficients.
    gain = {
        "R": 0.00001354,
        "G": 0.00000507,
        "B": 0.0000099253
    }
    bias = {
        "R": 0.0000136754,
        "G": 0.000006084,
        "B": 0.0000099253
    }

    input_path = storage_manager.resolve_input_path(input_filename)
    output_rgb_path = storage_manager.resolve_output_path(output_rgb_filename)
    output_gray_path = storage_manager.resolve_output_path(output_gray_filename)

    if not os.path.exists(input_path):
        return f"Error: Input image not found at {input_path}"

    try:
        dataset = gdal.Open(input_path)
        if dataset is None:
            return f"Error: Unable to open image file: {input_path}"
        if dataset.RasterCount < 3:
            return f"Error: Input image must have at least 3 bands, found {dataset.RasterCount}."

        geo_transform = dataset.GetGeoTransform()
        projection = dataset.GetProjection()

        dn_red = dataset.GetRasterBand(1).ReadAsArray().astype(np.float32)
        dn_green = dataset.GetRasterBand(2).ReadAsArray().astype(np.float32)
        dn_blue = dataset.GetRasterBand(3).ReadAsArray().astype(np.float32)

        valid_mask = (dn_red > 0) | (dn_green > 0) | (dn_blue > 0)

        l_red = np.zeros_like(dn_red)
        l_green = np.zeros_like(dn_green)
        l_blue = np.zeros_like(dn_blue)

        l_red[valid_mask] = dn_red[valid_mask] * gain["R"] + bias["R"]
        l_green[valid_mask] = dn_green[valid_mask] * gain["G"] + bias["G"]
        l_blue[valid_mask] = dn_blue[valid_mask] * gain["B"] + bias["B"]

        calibrated = np.array([l_red, l_green, l_blue])

        gray = np.zeros_like(l_red)
        gray[valid_mask] = (
            0.2989 * l_red[valid_mask]
            + 0.5870 * l_green[valid_mask]
            + 0.1140 * l_blue[valid_mask]
        )

        driver = gdal.GetDriverByName("GTiff")
        rows, cols = l_red.shape

        out_ds = driver.Create(output_rgb_path, cols, rows, 3, gdal.GDT_Float32)
        out_ds.SetGeoTransform(geo_transform)
        out_ds.SetProjection(projection)
        for i in range(3):
            band = out_ds.GetRasterBand(i + 1)
            band.WriteArray(calibrated[i])
            band.SetNoDataValue(-9999.0)
        out_ds.FlushCache()
        del out_ds

        gray_ds = driver.Create(output_gray_path, cols, rows, 1, gdal.GDT_Float32)
        gray_ds.SetGeoTransform(geo_transform)
        gray_ds.SetProjection(projection)
        band = gray_ds.GetRasterBand(1)
        band.WriteArray(gray)
        band.SetNoDataValue(-9999.0)
        gray_ds.FlushCache()
        del gray_ds

        return f"Calibration completed. RGB: {output_rgb_path}, Gray: {output_gray_path}"
    except Exception as e:
        return f"Error during radiometric calibration: {str(e)}"

class NTL_daily_data_preprocess_Input(BaseModel):
    study_area: str = Field(..., description="Name of the study area of interest. Example:'鍗椾含甯?")
    scale_level: str = Field(..., description="Scale level, e.g.'country', 'province', 'city', 'county'.")
    time_range_input: str = Field(...,
                                  description="Time range in the format 'YYYY-MM to YYYY-MM'. Example: '2020-01 to 2020-02'")

def VNP46A2_NTL_data_preprocess(
        study_area: str,
        scale_level: str,
        time_range_input: str,
):
    import re
    import ee
    from datetime import datetime, timedelta

    # Set administrative boundary dataset based on scale level
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    # Select administrative boundaries
    def get_administrative_boundaries(scale_level):
        # Handle directly governed cities as province-level data in China
        directly_governed_cities = ['Beijing', 'Tianjin', 'Shanghai', 'Chongqing']
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
    # Validate region
    if region.size().getInfo() == 0:
        raise ValueError(f"No area named '{study_area}' found under scale level '{scale_level}'.")
    region = region.geometry()


    # if region.isEmpty().getInfo():
    #     raise ValueError(f"No area named '{study_area}' found under scale level '{scale_level}'.")

    # Parse time range
    def parse_time_range(time_range_input):
        time_range_input = time_range_input.replace(' ', '')
        if 'to' in time_range_input:
            start_str, end_str = time_range_input.split('to')
            start_str, end_str = start_str.strip(), end_str.strip()
        else:
            # Single date input
            start_str = end_str = time_range_input.strip()

        if not re.match(r'^\d{4}-\d{2}-\d{2}$', start_str) or not re.match(r'^\d{4}-\d{2}-\d{2}$', end_str):
            raise ValueError("Invalid daily format. Use 'YYYY-MM-DD' or 'YYYY-MM-DD to YYYY-MM-DD'.")
        start_date, end_date = start_str, end_str

        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
            raise ValueError("Start date cannot be later than end date.")

        return start_date, end_date

    start_date, end_date = parse_time_range(time_range_input)

    NTL_collection = (
        ee.ImageCollection('NASA/VIIRS/002/VNP46A2')
        .filterDate(start_date, (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'))
        .select('DNB_BRDF_Corrected_NTL')
        .filterBounds(region)
        .map(lambda image: image.clip(region))
    )

    # ========== 璁＄畻姣忎釜褰卞儚鐨勭粍鍙?==========

    def add_group_number(image):
        # 璁＄畻褰卞儚鏃ユ湡
        date = ee.Date(image.get('system:time_start'))
        # 璁＄畻缁勫彿锛?-15锛?
        days_diff = date.difference(ee.Date(start_date), 'day')
        group_number = days_diff.mod(16).int()
        # 灏嗙粍鍙锋坊鍔犲埌褰卞儚灞炴€т腑
        return image.set('group_number', group_number)

    # 灏嗗嚱鏁板簲鐢ㄥ埌褰卞儚闆嗗悎涓?
    viirs_collection = NTL_collection.map(add_group_number)
    # ========== 鏁版嵁棰勫鐞嗭細娑堥櫎浼犳劅鍣ㄨ搴﹀奖鍝?==========

    # ========== 瀹炵幇閫愬儚绱犺搴︽晥搴旀牎姝?==========

    # **姝ラ1锛氳绠楀勾搴﹂€愬儚鍏冨潎鍊煎奖鍍?N**

    annual_mean_image = viirs_collection.mean()

    # **姝ラ2锛氭寜缁勫彿鍒嗙粍褰卞儚闆嗗悎锛岃绠楁瘡涓粍鐨勯€愬儚鍏冨潎鍊煎奖鍍?N1, N2, ..., N16**

    group_numbers = ee.List.sequence(0, 15)

    def compute_group_mean_image(group_number):
        group_number = ee.Number(group_number)
        group_collection = viirs_collection.filter(ee.Filter.eq('group_number', group_number))
        group_mean_image = group_collection.mean()
        # 灏嗙粍鍙锋坊鍔犲埌褰卞儚灞炴€т腑
        return group_mean_image.set('group_number', group_number)

    # 璁＄畻姣忎釜缁勭殑骞冲潎褰卞儚锛屽苟鐢熸垚涓€涓?ImageCollection
    group_mean_images = ee.ImageCollection(group_numbers.map(compute_group_mean_image))

    # **姝ラ3锛氳绠楁瘡涓粍鐨勮搴︽晥搴旂郴鏁板奖鍍?Ai = Ni / N**

    def compute_correction_image(image):
        group_number = image.get('group_number')
        group_mean_image = image
        # 璁＄畻鏍℃绯绘暟褰卞儚 Ai = Ni / N
        correction_image = group_mean_image.divide(annual_mean_image).unmask(1)
        # 灏嗙粍鍙锋坊鍔犲埌鏍℃褰卞儚灞炴€т腑
        return correction_image.set('group_number', group_number)

    # 鐢熸垚鏍℃绯绘暟褰卞儚闆嗗悎
    correction_images = group_mean_images.map(compute_correction_image)

    # **姝ラ4锛氬姣忎釜缁勭殑褰卞儚闆嗗悎杩涜鏍℃**

    def correct_group_images(group_number):
        group_number = ee.Number(group_number)
        # 鑾峰彇瀵瑰簲缁勫彿鐨勬牎姝ｇ郴鏁板奖鍍?Ai
        correction_image = correction_images.filter(ee.Filter.eq('group_number', group_number)).first()
        # 鑾峰彇瀵瑰簲缁勫彿鐨勫奖鍍忛泦鍚?
        group_collection = viirs_collection.filter(ee.Filter.eq('group_number', group_number))
        # 瀵圭粍鍐呯殑姣忎釜褰卞儚杩涜鏍℃
        corrected_group = group_collection.map(lambda image: image.divide(correction_image)
                                               .copyProperties(image, image.propertyNames()))
        return corrected_group

    # 瀵规瘡涓粍杩涜鏍℃锛屽緱鍒版牎姝ｅ悗鐨勫奖鍍忛泦鍚堝垪琛?
    corrected_groups = group_numbers.map(correct_group_images)

    # 灏嗘瘡涓?ImageCollection 灞曞紑鎴愬崟涓?Image 鍒楄〃骞跺悎骞朵负涓€涓?ImageCollection
    all_corrected_images = ee.ImageCollection(corrected_groups.iterate(
        lambda img_col, acc: ee.ImageCollection(acc).merge(ee.ImageCollection(img_col)),
        ee.ImageCollection([])
    ))

    # 鐢熸垚鏈€缁堢殑 corrected_collection
    corrected_collection = all_corrected_images

    # **姝ラ5锛氭寜鏃ユ湡鎺掑簭**

    corrected_collection = corrected_collection.sort('system:time_start')
    def set_filename(image):
        date = ee.Date(image.get('system:time_start')).format('yyyy_MM_dd')
        file_name = ee.String(study_area).cat('_').cat(date)
        return image.set('file_name', file_name)

    corrected_collection = corrected_collection.map(set_filename)
    # 浠庨泦鍚堜腑鎻愬彇鎵€鏈?file_name
    filenames = corrected_collection.aggregate_array('file_name').getInfo()
    print(filenames)
    # ========= 鎵归噺瀵煎嚭 corrected_collection 鍒?Assets =========

    # 1. 鍔ㄦ€佽幏鍙栧伐浣滅┖闂寸殑 inputs 鐩綍缁濆璺緞
    # 浣跨敤 "" 瑙ｆ瀽寰楀埌 inputs 鏂囦欢澶规牴鐩綍锛屾垨鑰呮寚瀹氬瓙鏂囦欢澶?
    out_dir = str(storage_manager.resolve_input_path("")) 
    
    # 2. 纭繚鐩綍瀛樺湪 (storage_manager 閫氬父浼氬鐞嗭紝浣嗚繖閲屽仛鍙岄噸淇濋櫓)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    corrected_collection = corrected_collection.sort('system:time_start')
    def set_filename(image):
        date = ee.Date(image.get('system:time_start')).format('yyyy_MM_dd')
        # 鏂囦欢鍚嶆牸寮忥細NTL_鍗椾含甯俖2020_01_01.tif
        file_name = ee.String('NTL_').cat(study_area).cat('_').cat(date)
        return image.set('file_name', file_name)

    corrected_collection = corrected_collection.map(set_filename)
    filenames = corrected_collection.aggregate_array('file_name').getInfo()

    # 3. 鎵ц鎵归噺瀵煎嚭鍒板伐浣滅┖闂寸殑 inputs/ 鐩綍
    import geemap
    print(f"Starting batch export to: {out_dir}")
    geemap.ee_export_image_collection(
        corrected_collection, 
        out_dir=out_dir, 
        filenames=filenames
    )
    
    # 4. 杩斿洖淇℃伅浣跨敤鐩稿璺緞锛岀鍚堢敤鎴锋劅鐭?
    return f"鉁?Angular correction completed. {len(filenames)} daily NTL images have been saved to the 'inputs/' folder."


# 璇诲彇SDR鏂囦欢涓殑Radiance銆丵F1_VIIRSDNBSDR浠ュ強SDR_GEO涓殑Longitude_TC銆丩atitude_TC銆丵F2_VIIRSSDRGEO銆?
# SolarZenithAngle銆丵F1_SCAN_VIIRSSDRGEO銆丩unarZenithAngle
def read_h5(sdr_data_path, SDR_names, SDR_GEO_names):
    with h5py.File(sdr_data_path, 'r') as sdr_file:
        GROUP_DNB_SDR = dict()
        GROUP_DNB_SDR_GEO = dict()

        if len(SDR_names) != 0:
            for SDR_name in SDR_names:
                temp_subdataset = sdr_file.get(SDR_name)
                if temp_subdataset is None:
                    print("The subdataset:%s don't exist." % (SDR_name))
                    continue
                GROUP_DNB_SDR[SDR_name] = temp_subdataset[()]
                del temp_subdataset

        if len(SDR_GEO_names) != 0:
            for SDR_GEO_name in SDR_GEO_names:
                temp_subdataset = sdr_file.get(SDR_GEO_name)
                if temp_subdataset is None:
                    print("The subdataset:%s don't exist." % (SDR_GEO_name))
                    continue
                GROUP_DNB_SDR_GEO[SDR_GEO_name] = temp_subdataset[()] # temp_subdataset.value
                del temp_subdataset

    return GROUP_DNB_SDR, GROUP_DNB_SDR_GEO


# 瀵筍DR杩涜璐ㄩ噺鎺у埗锛屽墧闄ゅ彈杈圭紭鍣０銆侀槼鍏夈€佹湀鍏夌瓑褰卞搷鐨勬暟鎹紝杈撳嚭鏁版嵁杩樿繕鏈繘琛屼簯鎺╄啘
def sdr_radiance_filter(SDR_GEO_path, SDR_names, SDR_GEO_names, sdr_out_dir):
    GROUP_DNB_SDR, GROUP_DNB_SDR_GEO = read_h5(SDR_GEO_path, SDR_names, SDR_GEO_names)
    sdr_output_name = os.path.basename(SDR_GEO_path).split('.')[0]

    # 1. VIIRS Fill Values
    cloud_radiance = GROUP_DNB_SDR[SDR_names[0]]
    r_fillvalue = np.array([-999.3, -999.5, -999.8, -999.9])
    radiance_mask = np.isin(cloud_radiance, r_fillvalue)
    print(f"[VIIRS Fill Values] Masked Pixels: {np.sum(radiance_mask)}, Total: {radiance_mask.size}, "
          f"Percentage: {np.sum(radiance_mask) / radiance_mask.size * 100:.2f}%")

    # 2. Edge-of-swath pixels
    edge_of_swath_mask = np.zeros_like(cloud_radiance, dtype=bool)
    edge_of_swath_mask[:, 0:230] = True
    edge_of_swath_mask[:, 3838:] = True
    print(f"[Edge-of-Swath] Masked Pixels: {np.sum(edge_of_swath_mask)}, Total: {edge_of_swath_mask.size}, "
          f"Percentage: {np.sum(edge_of_swath_mask) / edge_of_swath_mask.size * 100:.2f}%")

    # 3. QF1_VIIRSDNBSDR_flags
    qf1_viirsdnbsdr = GROUP_DNB_SDR[SDR_names[1]]
    SDR_Quality_mask = (qf1_viirsdnbsdr & 3) > 0
    Saturated_Pixel_mask = ((qf1_viirsdnbsdr & 12) >> 2) > 0
    Missing_Data_mask = ((qf1_viirsdnbsdr & 48) >> 4) > 0
    Out_of_Range_mask = ((qf1_viirsdnbsdr & 64) >> 6) > 0
    print(f"[QF1] SDR Quality Masked Pixels: {np.sum(SDR_Quality_mask)}")
    print(f"[QF1] Saturated Pixels: {np.sum(Saturated_Pixel_mask)}")
    print(f"[QF1] Missing Data Pixels: {np.sum(Missing_Data_mask)}")
    print(f"[QF1] Out of Range Pixels: {np.sum(Out_of_Range_mask)}")

    # 4. QF2_VIIRSSDRGEO_flags
    qf2_viirssdrgeo = GROUP_DNB_SDR_GEO[SDR_GEO_names[2]]
    qf2_viirssdrgeo_do0_mask = (qf2_viirssdrgeo & 1) > 0
    qf2_viirssdrgeo_do1_mask = ((qf2_viirssdrgeo & 2) >> 1) > 0
    qf2_viirssdrgeo_do2_mask = ((qf2_viirssdrgeo & 4) >> 2) > 0
    qf2_viirssdrgeo_do3_mask = ((qf2_viirssdrgeo & 8) >> 3) > 0
    print(f"[QF2] DO0 Masked Pixels: {np.sum(qf2_viirssdrgeo_do0_mask)}")
    print(f"[QF2] DO1 Masked Pixels: {np.sum(qf2_viirssdrgeo_do1_mask)}")
    print(f"[QF2] DO2 Masked Pixels: {np.sum(qf2_viirssdrgeo_do2_mask)}")
    print(f"[QF2] DO3 Masked Pixels: {np.sum(qf2_viirssdrgeo_do3_mask)}")

    # 5. QF1_SCAN_VIIRSSDRGEO
    qf1_scan_viirssdrgeo = GROUP_DNB_SDR_GEO[SDR_GEO_names[4]]
    within_south_atlantic_anomaly = ((qf2_viirssdrgeo & 16) >> 4) > 0
    print(f"[QF1_SCAN] South Atlantic Anomaly Pixels: {np.sum(within_south_atlantic_anomaly)}")

    # 6. Solar Zenith Angle
    solarZenithAngle = GROUP_DNB_SDR_GEO[SDR_GEO_names[3]]
    solarZenithAngle_mask = (solarZenithAngle < 118.5)
    print(f"[Solar Zenith Angle] Valid Pixels (<118.5掳): {np.sum(solarZenithAngle_mask)}")

    # 7. Lunar Zenith Angle
    lunar_zenith = GROUP_DNB_SDR_GEO[SDR_GEO_names[5]]
    moon_illuminance_mask = (lunar_zenith <= 90)
    print(f"[Lunar Zenith Angle] Moon Illuminance Pixels (鈮?0掳): {np.sum(moon_illuminance_mask)}")

    # 8. Combine all masks
    viirs_sdr_geo_mask = np.logical_or.reduce((
        radiance_mask,
        edge_of_swath_mask,
        solarZenithAngle_mask,
        moon_illuminance_mask,
        SDR_Quality_mask,
        Saturated_Pixel_mask,
        Missing_Data_mask,
        Out_of_Range_mask,
        qf2_viirssdrgeo_do0_mask,
        qf2_viirssdrgeo_do1_mask,
        qf2_viirssdrgeo_do2_mask,
        qf2_viirssdrgeo_do3_mask
    ))
    print(f"[Final Combined Mask] Total Masked Pixels: {np.sum(viirs_sdr_geo_mask)}, "
          f"Percentage: {np.sum(viirs_sdr_geo_mask) / viirs_sdr_geo_mask.size * 100:.2f}%")

    viirs_sdr_geo_mask_temp = np.logical_or.reduce((
        radiance_mask,
        solarZenithAngle_mask,
        moon_illuminance_mask,
        SDR_Quality_mask,
        Saturated_Pixel_mask,
        Missing_Data_mask,
        Out_of_Range_mask,
        qf2_viirssdrgeo_do0_mask,
        qf2_viirssdrgeo_do1_mask,
        qf2_viirssdrgeo_do2_mask,
        qf2_viirssdrgeo_do3_mask
    ))

    nan_count = np.sum(viirs_sdr_geo_mask_temp == True)
    nan_count_fraction = (nan_count / np.size(viirs_sdr_geo_mask_temp)) * 100
    if nan_count_fraction > 95:  # 濡傛灉鏁版嵁鍙楁湀鍏夋垨鑰呴槼鍏夊奖鍝嶅お澶э紝瀵艰嚧鏈夋晥鏁版嵁鍗犳瘮寰堝皬锛岄偅涔堣繖閮ㄥ垎鏁版嵁琚拷鐣ワ紝涓嶄繚瀛樼粨鏋?
        print(sdr_output_name + " ignored.")
        del viirs_sdr_geo_mask, radiance_mask, edge_of_swath_mask, solarZenithAngle_mask, moon_illuminance_mask
        del SDR_Quality_mask, Saturated_Pixel_mask, Missing_Data_mask, Out_of_Range_mask, qf2_viirssdrgeo_do0_mask
        del qf2_viirssdrgeo_do1_mask, qf2_viirssdrgeo_do2_mask, qf2_viirssdrgeo_do3_mask, viirs_sdr_geo_mask_temp
        del lunar_zenith
        gc.collect()
    else:
        del viirs_sdr_geo_mask_temp, GROUP_DNB_SDR
        del radiance_mask, solarZenithAngle_mask, moon_illuminance_mask, edge_of_swath_mask
        del SDR_Quality_mask, Saturated_Pixel_mask, Missing_Data_mask, Out_of_Range_mask, qf2_viirssdrgeo_do0_mask
        del qf2_viirssdrgeo_do1_mask, qf2_viirssdrgeo_do2_mask, qf2_viirssdrgeo_do3_mask
        del lunar_zenith
        gc.collect()

        fill_value = np.nan
        scalefactor = np.float32(pow(10, 9))
        cloud_radiance = cloud_radiance * scalefactor  # convert Watts to nanoWatts
        cloud_radiance[viirs_sdr_geo_mask] = fill_value  # set fill value for masked pixels in DNB
        # del viirs_sdr_geo_mask

        sdr_lon_data = GROUP_DNB_SDR_GEO[SDR_GEO_names[0]]
        sdr_lon_data[viirs_sdr_geo_mask] = np.nan
        sdr_lat_data = GROUP_DNB_SDR_GEO[SDR_GEO_names[1]]
        sdr_lat_data[viirs_sdr_geo_mask] = np.nan
        del viirs_sdr_geo_mask
        gc.collect()
        sdr_swath_def = geometry.SwathDefinition(
            xr.DataArray(da.from_array(sdr_lon_data, chunks=4096), dims=('y', 'x')),
            xr.DataArray(da.from_array(sdr_lat_data, chunks=4096), dims=('y', 'x'))
        )
        sdr_metadata_dict = {'name': 'dnb', 'area': sdr_swath_def}

        sdr_scn = Scene()
        sdr_scn['Radiance'] = xr.DataArray(
            da.from_array(cloud_radiance, chunks=4096),
            attrs=sdr_metadata_dict,
            dims=('y', 'x')  # https://satpy.readthedocs.io/en/latest/dev_guide/xarray_migration.html#id1
        )

        sdr_scn.load(['Radiance'])
        proj_str = '+proj=aea +lat_1=27 +lat_2=45 +lat_0=35 +lon_0=105 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs'  # aea鍧愭爣
        sdr_custom_area = create_area_def('aea', proj_str, resolution=750, units='meters', area_extent=[-2641644.056319, -3051079.954397, 2222583.910354, 2174272.289243]) # China Aea Extent
        sdr_proj_scn = sdr_scn.resample(sdr_custom_area, resampler='nearest')

        # sdr_proj_shape = sdr_proj_scn.datasets['Radiance'].shape

        sdr_out_path = sdr_out_dir + "\\" + sdr_output_name + '.tif'
        # 蹇呴』灏唀nhancement_config璁句负False锛屼笉鐒惰緭鍑虹殑鍊间細鍙樼殑寰堝皬
        sdr_proj_scn.save_dataset('Radiance', sdr_out_path, writer='geotiff', dtype=np.float32, enhancement_config=False, fill_value=fill_value)
        print(sdr_output_name + ' processed.')

        # release memory
        sdr_proj_scn = None
        del r_fillvalue
        del fill_value, sdr_proj_scn, sdr_lon_data, sdr_lat_data, sdr_swath_def, sdr_metadata_dict
        gc.collect()

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
import os
from pathlib import Path
from storage_manager import storage_manager

# Note: sdr_radiance_filter is assumed to be defined in your core processing module
# from core_processing import sdr_radiance_filter

def batch_pro_noaa20(input_subfolder: str, output_subfolder: str):
    """
    Internal logic for batch processing NOAA-20 H5 files.
    """
    workspace = storage_manager.get_workspace()
    # Resolve paths relative to workspace
    sdr_input_dir = workspace / "inputs" / input_subfolder
    sdr_out_dir = workspace / "outputs" / output_subfolder
    sdr_out_dir.mkdir(parents=True, exist_ok=True)

    if not sdr_input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {sdr_input_dir}")

    h5_file_list = list(sdr_input_dir.glob("*.h5"))

    # Metadata keys for HDF5 extraction
    SDR_names = [
        "/All_Data/VIIRS-DNB-SDR_All/Radiance", 
        "/All_Data/VIIRS-DNB-SDR_All/QF1_VIIRSDNBSDR"
    ]
    SDR_GEO_names = [
        "/All_Data/VIIRS-DNB-GEO_All/Longitude_TC", 
        "/All_Data/VIIRS-DNB-GEO_All/Latitude_TC",
        "/All_Data/VIIRS-DNB-GEO_All/QF2_VIIRSSDRGEO", 
        "/All_Data/VIIRS-DNB-GEO_All/SolarZenithAngle",
        "/All_Data/VIIRS-DNB-GEO_All/QF1_SCAN_VIIRSSDRGEO", 
        "/All_Data/VIIRS-DNB-GEO_All/LunarZenithAngle"
    ]

    for h5_file in h5_file_list:
        # Calling the core filtering function
        sdr_radiance_filter(str(h5_file), SDR_names, SDR_GEO_names, str(sdr_out_dir))

class NOAA20SDRPreprocessInput(BaseModel):
    input_subfolder: str = Field(
        ..., 
        description="Name of the subfolder in 'inputs/' containing raw .h5 files."
    )
    output_subfolder: str = Field(
        "processed_noaa20", 
        description="Name of the subfolder in 'outputs/' to save the GeoTIFF results."
    )

def preprocess_noaa20_viirs(input_subfolder: str, output_subfolder: str = "processed_noaa20") -> str:
    """
    Deprecated legacy wrapper.
    """
    _ = (input_subfolder, output_subfolder)
    return (
        "Noaa20_VIIRS_Preprocess has been disabled. "
        "Use official_vj_dnb_preprocess_tool "
        "(alias: convert_vj102_vj103_precise_to_tif_tool) instead. "
        "Expected input is matched VJ102DNB/VJ103DNB NC files."
    )

# Tool Registration
noaa20_sdr_preprocess_tool = StructuredTool.from_function(
    func=preprocess_noaa20_viirs,
    name="Noaa20_VIIRS_Preprocess",
    description=(
        "DEPRECATED/DISABLED: legacy NOAA-20 SDR preprocessing entry. "
        "Use official_vj_dnb_preprocess_tool or convert_vj102_vj103_precise_to_tif_tool for the current official workflow."
    ),
    input_type=NOAA20SDRPreprocessInput
)

VNP46A2_angular_correction_tool = StructuredTool.from_function(
    VNP46A2_NTL_data_preprocess,
    name="VNP46A2_angular_correction_tool",
    description=(
        """
        Perform angular effect correction on NASA VNP46A2 daily NTL data from Google Earth Engine, 
        using 16-group mean normalization to remove sensor zenith angle effects, and output the corrected 
        image collection with pixel-wise mean values over the specified date range.

        Parameters:
        - study_area (str): Name of the target region. For China, use Chinese names (e.g., 姹熻嫃鐪? 鍗椾含甯?.
        - scale_level (str): Administrative level ('country', 'province', 'city', 'county').
        - time_range_input (str): Date range in 'YYYY-MM-DD to YYYY-MM-DD' format.

        Output:
        - Exported corrected NTL images to local folder or GEE Assets, with per-pixel angular correction applied.

        Example Input:
        (
            study_area='鍗椾含甯?,
            scale_level='city',
            time_range_input='2020-01-01 to 2020-02-01',
        )
        """
    ),
    input_type=NTL_daily_data_preprocess_Input,
)



class CrossSensorCalibrationInput(BaseModel):
    dmsp_folder: str = Field(..., description="Path to folder containing DMSP-OLS annual images (2000鈥?013)")
    viirs_folder: str = Field(..., description="Path to folder containing VIIRS-like annual images (2013鈥?018)")
    aux_data_path: str = Field(..., description="Path to GeoTIFF file containing auxiliary variables for 2013")
    output_folder: str = Field(..., description="Folder to save calibrated output images and trained model")

def dmsp_preprocess_tool(
    dmsp_folder: str,
    viirs_folder: str,
    aux_data_path: str,
    output_folder: str
) -> str:
    """
    Framework for cross-sensor calibration: DMSP to VIIRS-like brightness
    """
    # TODO: implement model training and calibration here
    return f"Calibration workflow initialized. Output will be saved to {output_folder}"

from langchain_core.tools import StructuredTool

cross_sensor_calibration_dmsp_viirs_tool = StructuredTool.from_function(
    func=dmsp_preprocess_tool,
    name="dmsp_preprocess_tool",
    description=(
        """
        This tool performs cross-sensor calibration by training a machine learning model (e.g., Random Forest) on overlapping year data (e.g., 2013)
        between DMSP-OLS and VIIRS-like NTL datasets. The model incorporates auxiliary data (e.g., population, GDP, electricity, roads),
        and is then applied to historical DMSP-OLS images (2000鈥?012) to generate calibrated VIIRS-like brightness rasters.

        ### Example Input:
        calibrate_dmsp_to_viirs(
            dmsp_folder='C:/地缘环境智能计算平台/DMSP/',
            viirs_folder='C:/地缘环境智能计算平台/VIIRS/',
            aux_data_path='C:/地缘环境智能计算平台/aux_vars_2013.tif',
            output_folder='C:/地缘环境智能计算平台/Calibrated_NTL/'
        )
        """
    ),
    input_type="CrossSensorCalibrationInput"  # You can define this dataclass separately
)

class DMSPEVIPreprocessInput(BaseModel):
    dmsp_tif: str = Field(
        ..., 
        description="Filename of the DMSP nighttime light (NTL) raster in 'inputs/' folder (e.g., 'dmsp_ntl_2020.tif')."
    )
    evi_tif: str = Field(
        ..., 
        description="Filename of the EVI raster in 'inputs/' folder (e.g., 'annual_evi_2020.tif')."
    )
    output_tif: str = Field(
        ..., 
        description="Filename for the output EANTLI raster in 'outputs/' folder (e.g., 'eantli_2020.tif')."
    )


def preprocess_dmsp_evi(dmsp_tif: str, evi_tif: str, output_tif: str) -> str:
    """
    Preprocess DMSP and EVI annual composite images using EANTLI transformation.
    Implements EANTLI = [(1 + (nNTL - EVI)) / (1 - (nNTL - EVI))] * NTL.
    """
    try:
        # Resolve paths via storage_manager
        abs_dmsp_path = storage_manager.resolve_input_path(dmsp_tif)
        abs_evi_path = storage_manager.resolve_input_path(evi_tif)
        abs_output_path = storage_manager.resolve_output_path(output_tif)

        # Ensure input files exist
        if not os.path.exists(abs_dmsp_path):
            return f"鉂?Error: DMSP file not found at 'inputs/{dmsp_tif}'"
        if not os.path.exists(abs_evi_path):
            return f"鉂?Error: EVI file not found at 'inputs/{evi_tif}'"

        # Read DMSP NTL raster
        with rasterio.open(abs_dmsp_path) as dmsp_src:
            ntl = dmsp_src.read(1).astype(np.float32)
            profile = dmsp_src.profile

        # Read EVI raster
        with rasterio.open(abs_evi_path) as evi_src:
            evi = evi_src.read(1).astype(np.float32)

        # Mask invalid pixels
        valid_mask = np.isfinite(ntl) & np.isfinite(evi) & (evi >= 0)

        # Normalize NTL (scale to [0, 1])
        max_ntl = np.nanmax(ntl)
        if max_ntl > 0:
            nntl = ntl / max_ntl
        else:
            return "鉂?Error: NTL raster contains only invalid values."

        # Compute EANTLI using provided equation
        numerator = 1 + (nntl - evi)
        denominator = 1 - (nntl - evi)
        eantli = np.full_like(ntl, -9999.0, dtype=np.float32)
        eantli[valid_mask] = (numerator[valid_mask] / denominator[valid_mask]) * ntl[valid_mask]

        # Update profile for output
        profile.update(dtype='float32')

        # Write output raster
        with rasterio.open(abs_output_path, 'w', **profile) as dst:
            dst.write(eantli, 1)

        return f"鉁?EANTLI image saved to 'outputs/{output_tif}'"

    except Exception as e:
        return f"鉂?Error during preprocessing: {str(e)}"


# Tool Registration
dmsp_evi_preprocess_tool = StructuredTool.from_function(
    func=preprocess_dmsp_evi,
    name="DMSP_Preprocess",
    description=(
        "Applies EANTLI preprocessing to DMSP nighttime light (NTL) and annual EVI raster imagery. "
        "Based on the transformation: EANTLI = [(1 + (nNTL - EVI)) / (1 - (nNTL - EVI))] * NTL. "
        "Inputs must be named rasters in the 'inputs/' workspace, and the resulting GeoTIFF is saved in 'outputs/'."
        "\n\nExample:\n"
        "dmsp_tif='dmsp_ntl_2020.tif',\n"
        "evi_tif='annual_evi_2020.tif',\n"
        "output_tif='eantli_2020.tif'."
    ),
    input_type=DMSPEVIPreprocessInput
)

if __name__ == "__main__":
    pass

    # input_sdr_dir = r"C:\璇鹃缁刓鏂數\9d_10d_IndiaData\input"  # sdr鐨勫瓨鍌ㄦ枃浠跺す
    # output_sdr_dir = r"C:\璇鹃缁刓鏂數\9d_10d_IndiaData\output"  # 杈撳嚭鏂囦欢澶癸紱杈撳嚭鏄墧闄や簡杈圭紭鍣０銆侀槼鍏夈€佹湀鍏夌瓑褰卞搷鐨凴adiance鏁版嵁锛屾牸寮忎负geotiff
    # noaa20_sdr_preprocess_tool.run({
    #     "sdr_input_dir": input_sdr_dir,
    #     "sdr_output_dir": output_sdr_dir
    # })







