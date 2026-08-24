from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
import numpy as np
import rasterio
from rasterio.mask import mask
import geopandas as gpd
import matplotlib.pyplot as plt
from pymannkendall import original_test as mk_test
import os
from typing import List, Optional
from pathlib import Path

# 导入存储管理器
from storage_manager import storage_manager
from ntl_toolkit.adapters.langchain import trend_analysis

# ===== 1. 定义输入模型 =====
class MaskedTrendAnalysisInput(BaseModel):
    raster_files: List[str] = Field(
        ..., 
        description="List of NTL raster filenames in chronological order (e.g., 2015.tif, 2016.tif...)."
    )
    vector_file: str = Field(
        ..., 
        description="Filename of the administrative boundary (e.g., 'shanghai.shp' or 'boundary.json') in 'inputs/'."
    )
    out_prefix: str = Field(
        "NTL_Trend", 
        description="Prefix for the output files (TIFs and PNG)."
    )

# ===== 2. 定义核心逻辑函数 =====
def analyze_ntl_trend_masked_logic(
    raster_files: List[str], 
    vector_file: str, 
    out_prefix: str = "NTL_Trend"
) -> str:
    """
    Perform pixel-wise Mann-Kendall trend analysis on NTL rasters masked by a vector boundary.
    Generates Slope map, P-value map, and a visualization plot.
    """
    vector_path = storage_manager.resolve_input_path(vector_file)
    raster_paths = [storage_manager.resolve_input_path(path) for path in raster_files]
    output_prefix = storage_manager.resolve_output_path(out_prefix)
    return trend_analysis(raster_paths, vector_path, output_prefix)

# ===== 3. 工具封装与导出 =====
NTL_Trend_Analysis = StructuredTool.from_function(
    func=analyze_ntl_trend_masked_logic,
    name="Analyze_NTL_trend",
    description=(
        "Advanced tool for pixel-level trend analysis (Mann-Kendall & Sen's Slope). "
        "It uses a vector file (SHP/JSON) to mask the research area for higher accuracy and speed. "
        "Inputs: a chronological list of NTL rasters and a boundary vector file. "
        "Outputs: 1) A Slope TIF (change rate), 2) A P-value TIF (significance), 3) A PNG map preview. "
        "Best for analyzing urban expansion or economic dynamics in specific cities or regions."
    ),
    input_type=MaskedTrendAnalysisInput,
)

