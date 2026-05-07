import pandas as pd
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
import geopandas as gpd
import requests
import os
from shapely.geometry import Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon
from typing import List, Optional
from geopy.geocoders import Nominatim
import numpy as np
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from storage_manager import current_thread_id, storage_manager
from dotenv import load_dotenv

load_dotenv()


# Set up URLs for API requests
DISTRICT_URL = 'https://restapi.amap.com/v3/config/district?keywords={city}&key={api_key}'
GEO_JSON_URL = 'https://geo.datav.aliyun.com/areas/bound/{city_code}_full.json'


def _resolve_thread_id_from_config(config: Optional[RunnableConfig] = None) -> str:
    runtime_config: Optional[RunnableConfig] = None
    if isinstance(config, dict):
        runtime_config = config
    else:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited

    if isinstance(runtime_config, dict):
        try:
            tid = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if tid:
                return tid
        except Exception:
            pass
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _safe_response_json(response: requests.Response):
    try:
        return response.json(), None
    except ValueError:
        raw = (response.text or "").strip().replace("\n", " ")
        snippet = raw[:200] if raw else "<empty>"
        return None, f"Invalid JSON response (HTTP {response.status_code}). Body: {snippet}"

def gcj02_to_wgs84_logic(lng, lat):
    import math
    
    PI = math.pi
    a = 6378245.0
    ee = 0.00669342162296594323

    def out_of_china(lng, lat):
        return not (72.004 <= lng <= 137.8347 and 0.8293 <= lat <= 55.8271)

    def _transform_lat(lng, lat):
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
              0.1 * lng * lat + 0.2 * math.sqrt(abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * PI) + 20.0 * math.sin(2.0 * lng * PI)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * PI) + 40.0 * math.sin(lat / 3.0 * PI)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * PI) + 320 * math.sin(lat * PI / 30.0)) * 2.0 / 3.0
        return ret

    def _transform_lng(lng, lat):
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
              0.1 * lng * lat + 0.1 * math.sqrt(abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * PI) + 20.0 * math.sin(2.0 * lng * PI)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * PI) + 40.0 * math.sin(lng / 3.0 * PI)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * PI) + 300.0 * math.sin(lng / 30.0 * PI)) * 2.0 / 3.0
        return ret

    if out_of_china(lng, lat):
        return lng, lat
    
    dlat = _transform_lat(lng - 105.0, lat - 35.0)
    dlng = _transform_lng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * PI
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * PI)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * PI)
    
    mglat = lat + dlat
    mglng = lng + dlng
    
    # 返回逆向修正后的坐标
    return lng * 2 - mglng, lat * 2 - mglat

def convert_geom_to_wgs84(geom):
    """
    Complete version: Recursively converts any shapely geometry from GCJ-02 to WGS-84.
    """
    if geom.is_empty:
        return geom
    
    geom_type = geom.geom_type

    if geom_type == 'Point':
        return Point(gcj02_to_wgs84_logic(geom.x, geom.y))
    
    elif geom_type == 'LineString':
        return LineString([gcj02_to_wgs84_logic(x, y) for x, y in geom.coords])
    
    elif geom_type == 'Polygon':
        shell = [gcj02_to_wgs84_logic(x, y) for x, y in geom.exterior.coords]
        holes = [[gcj02_to_wgs84_logic(x, y) for x, y in hole.coords] for hole in geom.interiors]
        return Polygon(shell, holes)
    
    elif geom_type == 'MultiPoint':
        return MultiPoint([convert_geom_to_wgs84(g) for g in geom.geoms])
    
    elif geom_type == 'MultiLineString':
        return MultiLineString([convert_geom_to_wgs84(g) for g in geom.geoms])
    
    elif geom_type == 'MultiPolygon':
        return MultiPolygon([convert_geom_to_wgs84(g) for g in geom.geoms])
    
    return geom


import os
import requests
import geopandas as gpd
from shapely.geometry import shape
from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field

# Assuming these are defined elsewhere
DISTRICT_URL = "https://restapi.amap.com/v3/config/district?keywords={city}&key={api_key}&subdistrict=0"
GEO_JSON_URL = "https://geo.datav.aliyun.com/areas_v3/bound/geojson?code={city_code}_full"
# And storage_manager, convert_geom_to_wgs84 are imported

import os
import requests
import geopandas as gpd
from shapely.geometry import shape
from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field

# Assuming these are defined elsewhere
DISTRICT_URL = "https://restapi.amap.com/v3/config/district?keywords={city}&key={api_key}&subdistrict=0"
GEO_JSON_URL = "https://geo.datav.aliyun.com/areas_v3/bound/geojson?code={city_code}_full"
# And storage_manager, convert_geom_to_wgs84 are imported

class GetAdministrativeDivisionInput(BaseModel):
    city: str = Field(..., description="Name of the administrative division (e.g., 'Shanghai'). Chinese names are also accepted.")
    input_name: str = Field(..., description="input filename for the Shapefile (e.g., 'shanghai_boundary.shp').")

def get_administrative_division_data(city: str, input_name: str, config: Optional[RunnableConfig] = None) -> str:
    """
    Fetches administrative boundary, converts to WGS-84, and saves as a Shapefile.
    """
    api_key = os.environ.get("amap_api_key")
    if not api_key:
        return "Error: Amap API key is missing in environment variables."

    # 1. Resolve input path via storage_manager
    thread_id = _resolve_thread_id_from_config(config)
    abs_input_path = storage_manager.resolve_input_path(input_name, thread_id=thread_id)

    try:
        # Step 1: Get adcode
        resp = requests.get(DISTRICT_URL.format(city=city, api_key=api_key))
        data, parse_error = _safe_response_json(resp)
        if parse_error:
            return f"Error: Failed to parse district API response. {parse_error}"
        if data.get("status") != "1" or not data["districts"]:
            return f"Error: Could not find adcode for '{city}'."
        
        city_code = data["districts"][0]["adcode"]
        city_name_en = data["districts"][0]["name"]

        # Step 2: Get GeoJSON
        geo_resp = requests.get(GEO_JSON_URL.format(city_code=city_code))
        if geo_resp.status_code != 200:
            # Fallback to single boundary if full children boundary fails
            geo_resp = requests.get(GEO_JSON_URL.format(city_code=city_code).replace("_full", ""))
        
        if geo_resp.status_code != 200:
            return "Error: Failed to download GeoJSON data from source."

        geojson_data, parse_error = _safe_response_json(geo_resp)
        if parse_error:
            return f"Error: Failed to parse GeoJSON boundary response. {parse_error}"

        # Step 3: Process with GeoPandas
        gdf = gpd.GeoDataFrame.from_features(geojson_data["features"])
        
        # Convert coordinates to WGS-84
        gdf['geometry'] = gdf['geometry'].apply(convert_geom_to_wgs84)
        gdf.set_crs(epsg=4326, inplace=True)

        # Validate and fix geometries, but avoid changing types
        from shapely.validation import make_valid
        gdf['geometry'] = gdf['geometry'].apply(lambda geom: make_valid(geom) if not geom.is_valid else geom)

        # Filter to keep only polygon geometries (to avoid Shapefile type errors)
        gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]

        # If no polygons left, raise error
        if gdf.empty:
            return "Error: No valid polygon geometries found after processing."

        # Standardize column names to English for Shapefile compatibility (max 10 chars)
        column_mapping = {
            'name': 'Name',
            'adcode': 'AdCode',
            'level': 'Level',
            'parent': 'Parent'
        }
        gdf.rename(columns=column_mapping, inplace=True)
        # Keep only key columns to keep it clean
        columns_to_keep = [col for col in column_mapping.values() if col in gdf.columns] + ['geometry']
        gdf = gdf[columns_to_keep]

        # Step 4: Save
        gdf.to_file(abs_input_path, driver="ESRI Shapefile", encoding="utf-8")
        
        return (f"Success: Administrative data for '{city}' processed.\n"
                f"Coordinates: WGS-84 (EPSG:4326)\n"
                f"Saved to: {input_name} (Workspace: inputs/)")

    except Exception as e:
        return f"Error during processing: {str(e)}"

# --- Tool Definition ---

get_administrative_division_tool = StructuredTool.from_function(
    func=get_administrative_division_data,
    name="get_administrative_division_data",
    description=(
        "Retrieves the administrative boundary (Shapefile) for a city or district in China. "
        "Automatically converts coordinates from GCJ-02 to WGS-84 for standard GIS analysis. "
        "The input is saved in the workspace's 'inputs/' folder. "
        "Ideal for generating ROI masks for NTL analysis."
    ),
    args_schema=GetAdministrativeDivisionInput,
)

# result = get_administrative_division_tool.run({
#     "city": "武汉",  # 或 "Wuhan"，中文或英文均可
#     "input_name": "wuhan_boundary.shp"  # 保存的文件名，会存储到 workspace 的 'inputs/' 文件夹
# })

# print(result)  # 输出成功消息或错误


import os
import geopandas as gpd
import osmnx as ox

def get_administrative_division_osm(
    place_name: str,
    input_name: str = None,
    config: Optional[RunnableConfig] = None,
) -> str:
    """
    Fetch administrative boundary data for a given place from OSM using geocode_to_gdf
    and save as Shapefile.

    Parameters:
    - place_name (str): Name of the administrative area (e.g., 'Myanmar', 'Yangon', 'Mandalay').
    - input_name (str): input filename ONLY (e.g., 'my_boundary.shp'), no path or folder.

    Returns:
    - str: Message indicating the success or failure of the operation and the save location.
    """
    thread_id = _resolve_thread_id_from_config(config)
    abs_input_path = storage_manager.resolve_input_path(input_name, thread_id=thread_id)
    os.makedirs(os.path.dirname(abs_input_path), exist_ok=True)

    try:
        print(f"Fetching OSM boundary for '{place_name}' ...")
        gdf = ox.geocode_to_gdf(place_name, which_result=1)
        gdf = gdf.to_crs(epsg=4326)

        # 截短字段名以适配 Shapefile 格式
        gdf.columns = [col[:10] for col in gdf.columns]

        # 保存为 Shapefile
        gdf.to_file(abs_input_path, driver="ESRI Shapefile", encoding="utf-8")

        return f"{place_name} administrative boundary Shapefile generated successfully. Saved at: {abs_input_path}"

    except Exception as e:
        return f"Failed to fetch administrative division data for {place_name}: {e}"


# 输入模型
class GetAdministrativeDivisionOSMInput(BaseModel):
    place_name: str = Field(..., description="Name of the city or administrative division to retrieve data for (e.g., 'Myanmar').")
    input_name: str = Field(
        None,
        description="input filename ONLY (e.g., 'my_boundary.shp'), no path or folder."
    )


# 转换为 StructuredTool
get_administrative_division_osm_tool = StructuredTool.from_function(
    get_administrative_division_osm,
    name="get_administrative_division_osm_tool",
    description=(
        "[Deprecated] Fetches administrative boundaries from OSM/Nominatim and saves ESRI Shapefile in WGS-84. "
        "Prefer `get_administrative_division_geoboundaries_tool` for global boundaries (ADM0-ADM4) with optional "
        "GeoJSON-to-SHP conversion."
    ),
    input_type=GetAdministrativeDivisionOSMInput,
)


class ReverseGeocodeInput(BaseModel):
    latitudes: List[float] = Field(..., description="List of latitudes for the locations to reverse geocode.")
    longitudes: List[float] = Field(..., description="List of longitudes for the locations to reverse geocode.")
    input_name: str = Field(..., description="input filename ONLY (e.g., 'my_geocode.csv'), no path or folder.")
    region: str = Field("China", description="the region in 'China' or 'other_country'")

def reverse_geocode(
    latitudes: List[float],
    longitudes: List[float],
    input_name: Optional[str] = None,
    region: str = "China",
    config: Optional[RunnableConfig] = None,
) -> str:
    """
    Performs reverse geocoding for a list of latitude and longitude pairs.

    Parameters:
    - latitudes (List[float]): List of latitudes.
    - longitudes (List[float]): List of longitudes.
    - input_name (Optional[str]): input filename ONLY (e.g., 'my_geocode.csv'), no path or folder.

    Returns:
    - str: Message indicating the success of the operation and the save location.
    """
    addresses = []
    thread_id = _resolve_thread_id_from_config(config)
    abs_input_path = storage_manager.resolve_input_path(input_name, thread_id=thread_id)
    os.makedirs(os.path.dirname(abs_input_path), exist_ok=True)
    amap_api_key = os.environ.get("amap_api_key")

    if region == "China" :
        # Use Amap API for reverse geocoding
        for latitude, longitude in zip(latitudes, longitudes):
            url = f"https://restapi.amap.com/v3/geocode/regeo?location={longitude},{latitude}&key={amap_api_key}&extensions=base"
            response = requests.get(url)
            if response.status_code == 200:
                data, parse_error = _safe_response_json(response)
                if parse_error:
                    address = f"Amap API parse error: {parse_error}"
                    addresses.append(address)
                    continue
                if data.get("status") == "1":
                    address = data.get("regeocode", {}).get("formatted_address", "Address not found")
                else:
                    address = f"Amap API error: {data.get('info')}"
            else:
                address = f"Failed to request Amap API. HTTP status code: {response.status_code}"
            addresses.append(address)
    else:
        # Use Nominatim for reverse geocoding
        geolocator = Nominatim(user_agent="your_app_name")
        for latitude, longitude in zip(latitudes, longitudes):
            location = geolocator.reverse((latitude, longitude))
            address = location.address if location else "Address not found"
            addresses.append(address)
    # Save results to CSV file
    df = pd.DataFrame({"Latitude": latitudes, "Longitude": longitudes, "Address": addresses})
    df.to_csv(abs_input_path, index=False, encoding="utf-8-sig")
    # return f"Reverse geocoding completed. Results saved at: {save_path}"
    return f"Reverse geocoding completed. Results saved at: {abs_input_path}\n\nTop rows:\n{df.head().to_string()}"

reverse_geocode_tool = StructuredTool.from_function(
    reverse_geocode,
    name="reverse_geocode_tool",
    description=(
        "This tool performs reverse geocoding for a list of latitudes and longitudes, returning the corresponding full addresses."
        "### Input Example:\n"
        "- latitudes: [40.748817, 34.052235]\n"
        "- longitudes: [-73.985428, -118.243683]\n"
        "- input_name: 'reverse_geocode_results.csv'\n\n"
        "- region: 'China' or 'other_country'"
        "### input:\n"
        "Returns the addresses corresponding to the provided latitudes and longitudes, and saves the results to a CSV file."
    ),
    input_type=ReverseGeocodeInput,
)


class POISearchInput(BaseModel):
    latitude: float = Field(..., description="Central point latitude.")
    longitude: float = Field(..., description="Central point longitude.")
    radius: int = Field(500, description="Search radius in meters. Default is 500 meters.")
    types: str = Field(None, description="POI category codes, refer to Amap POI category code table.")
    input_name: str = Field(..., description="input filename ONLY (e.g., 'poi_results.csv'), will be saved to your workspace inputs/ directory.")

def search_poi_nearby(
    latitude: float,
    longitude: float,
    radius: int = 500,
    types: str = None,
    input_name: str = None,
    config: Optional[RunnableConfig] = None,
) -> str:
    """
    Search for Points of Interest around a coordinate and saves the results to user's inputs folder as CSV.
    """
    if input_name is None:
        input_name = "poi_results.csv"


    thread_id = _resolve_thread_id_from_config(config)
    abs_in_path = storage_manager.resolve_input_path(input_name, thread_id=thread_id)
    os.makedirs(os.path.dirname(abs_in_path), exist_ok=True)

    amap_api_key = os.environ.get("amap_api_key")
    if not amap_api_key:
        return "API key is not set. Please set 'amap_api_key' in environment variables."

    url = "https://restapi.amap.com/v5/place/around"
    params = {
        "key": amap_api_key,
        "location": f"{longitude},{latitude}",
        "radius": radius,
        "types": types,
        "input": "json"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data, parse_error = _safe_response_json(response)
        if parse_error:
            return f"Amap API parse error: {parse_error}"
        if data.get("status") == "1":
            pois = data.get("pois", [])
            df = pd.DataFrame(pois)
            df.to_csv(abs_in_path, index=False, encoding="utf-8-sig")
            return (f"POI search completed. Results saved at: {abs_in_path}\n\n"
                    f"Top rows:\n{df.head().to_string(index=False)}")
        else:
            return f"Amap API error: {data.get('info')}"
    else:
        return f"Failed to request Amap API. HTTP status code: {response.status_code}"



# Update the StructuredTool for POI search
poi_search_tool = StructuredTool.from_function(
    search_poi_nearby,
    name="poi_search_tool",
    description=(
        "This tool retrieves Points of Interest (POIs) within a specified radius around a given coordinate and saves the results to a CSV file. "
        "It uses Amap's POI search API. The API key is read from the environment variable 'amap_api_key'. "
        "With the retrieved latitude and longitude of the target pixel, this tool can obtain nearby POI information, "
        "helping to determine the main types of facilities in the area. This facilitates further analysis and interpretation "
        "of the context within each grid cell.\n\n"
        "### Input Example:\n"
        "- latitude: 39.984154\n"
        "- longitude: 116.307490\n"
        "- radius: 500\n"
        "- types: '050000' (Restaurant services)\n"
        "- input_name: 'poi_results.csv'\n\n"
        "### input:\n"
        "Returns a list of POIs containing name, address, type, and other information, saving the results to the specified CSV file. "
        "This information supports further analysis by providing insight into the types of nearby facilities around the selected grid cell."
    ),
    input_type=POISearchInput,
)


# --- Updated GeocodeInput ---
class GeocodeInput(BaseModel):
    address: str = Field(..., description="The address to geocode.If in China,")
    input_name: Optional[str] = Field(
        None,
        description="input filename ONLY (e.g., 'my_geocode.csv'), no path or folder."
    )

def geocode_address(
    address: str,
    input_name: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> str:
    """
    Geocodes an address using the Amap API, returning latitude and longitude.

    Parameters:
    - address (str): Address to geocode.
    - input_name (Optional[str]): input filename ONLY (e.g., 'my_geocode.csv'), no path or folder.

    Returns:
    - str: Message indicating the success of the operation and the save location.
    """
    input_name = input_name or "geocode_results.csv"
    thread_id = _resolve_thread_id_from_config(config)
    abs_input_path = storage_manager.resolve_input_path(input_name, thread_id=thread_id)
    os.makedirs(os.path.dirname(abs_input_path), exist_ok=True)
    amap_api_key = os.environ.get("amap_api_key")
    if not amap_api_key:
        return "API key is not set. Please set 'amap_api_key' in environment variables."

    url = "https://restapi.amap.com/v3/geocode/geo"
    params = {
        "key": amap_api_key,
        "address": address,
        "input": "json"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data, parse_error = _safe_response_json(response)
        if parse_error:
            return f"Amap API parse error: {parse_error}"
        if data.get("status") == "1":
            geocode_info = data.get("geocodes", [])[0]
            location = geocode_info.get("location", "").split(",")
            latitude = float(location[1])
            longitude = float(location[0])
            wgs_lng, wgs_lat = gcj02_to_wgs84_logic(longitude, latitude)
            # Save result to CSV
            df = pd.DataFrame([{"Address": address, "Latitude": wgs_lat, "Longitude": wgs_lng}])
            df.to_csv(abs_input_path, index=False, encoding="utf-8-sig")
            # 打印出前 5 行数据
            # print(df.head())  # 默认显示前 5 行
            # 将前 5 行数据转换为字符串并包含在返回信息中
            return f"Geocoding completed. Results saved at: {abs_input_path}\n\nTop rows:\n{df.head().to_string()}"
        else:
            return f"Amap API error: {data.get('info')}"
    else:
        return f"Failed to request Amap API. HTTP status code: {response.status_code}"

geocode_tool = StructuredTool.from_function(
    geocode_address,
    name="geocode_tool",
    description=(
        "This tool geocodes a given address, retrieving latitude and longitude using the Amap API, "
        "and saves the result to a CSV file. The API key is read from the environment variable 'amap_api_key'. "
        "With the obtained geographic coordinates, further analysis can be conducted, such as examining nighttime light "
        "intensity values at the specified latitude and longitude for the given indicator.\n\n"
        "Address can only in China and must be Chinese Name"
        "### Input Example:\n"
        "- address: '上海市静安区南京西路'\n"
        "- input_name: 'geocode_results.csv'\n\n"
        "### input:\n"
        "Returns latitude and longitude for the specified address, saving the result to the specified CSV file. "
    ),
    input_type=GeocodeInput,
)



# # Sample usage
# if __name__ == "__main__":
#     # Ensure the API key is set
#     # os.environ["amap_api_key"] = "your_actual_amap_api_key"
#
#     # Example usage of get_administrative_division_tool
#     result = get_administrative_division_tool.func(
#         city="上海市",
#         save_path="./test"
#     )
#     print(result)

#     # Example usage of reverse_geocode_tool
#     reverse_geocode_result = reverse_geocode_tool.func(
#         latitudes=[31.2304],
#         longitudes=[121.4737],
#         save_path="./NTL_Agent/report/geocode_results/geocode_results.csv"
#     )
#     print(reverse_geocode_result)
#
#     # Example usage of poi_search_tool
#     poi_search_result = poi_search_tool.func(
#         latitude=31.2397,
#         longitude=121.4903,
#         types='050000',  # Restaurant services
#         save_path="./NTL_Agent/report/poi_results.csv"
#     )
#     print(poi_search_result)

# Sample usage of geocode_tool
# if __name__ == "__main__":
#     # Example usage of geocode_tool
#     geocode_result = geocode_tool.func(
#         address="上海市东方明珠",
#         save_path="./NTL_Agent/report/geocode_results/geocode_results.csv"
#     )
#     print(geocode_result)

# get_administrative_division_tool.func(city="Myanmar")
# result = get_administrative_division_osm("Myanmar", admin_level=2)
# print(result)
# 省级: get_administrative_division_osm("Yangon Region, Myanmar", admin_level=4)
# 市级: get_administrative_division_osm("Yangon, Myanmar", admin_level=8)

# get_administrative_division_osm_tool.func(place_name= "Shanghai",  save_path="./NTL_Agent/report/shp/china")
