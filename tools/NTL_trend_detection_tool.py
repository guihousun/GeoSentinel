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
    try:
        # 1. 路径解析
        vector_path = storage_manager.resolve_input_path(vector_file)
        if not os.path.exists(vector_path):
            return f"Error: Vector file '{vector_file}' not found in inputs/."

        # 加载矢量边界
        gdf = gpd.read_file(vector_path)
        shapes = [geom for geom in gdf.geometry]

        # 2. 准备数据堆叠
        raster_paths = []
        for f in raster_files:
            p = storage_manager.resolve_input_path(f)
            if not os.path.exists(p):
                return f"Error: Raster file '{f}' not found in inputs/."
            raster_paths.append(p)

        if len(raster_paths) < 3:
            return "Error: Trend analysis requires at least 3 time-series rasters."

        # 3. 初始读取与裁剪（获取输出元数据）
        with rasterio.open(raster_paths[0]) as src:
            # 使用掩膜裁剪并获取变换参数
            out_image, out_transform = mask(src, shapes, crop=True, nodata=np.nan)
            out_meta = src.meta.copy()
            out_meta.update({
                "driver": "GTiff",
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
                "dtype": 'float32',
                "nodata": np.nan
            })

        # 构建 3D 数据堆叠
        data_stack = np.zeros((len(raster_paths), out_meta['height'], out_meta['width']), dtype=np.float32)
        
        for idx, r_path in enumerate(raster_paths):
            with rasterio.open(r_path) as src:
                img_masked, _ = mask(src, shapes, crop=True, nodata=np.nan)
                data_stack[idx, :, :] = img_masked[0, :, :]

        # 4. 像素级趋势计算
        nrows, ncols = data_stack.shape[1], data_stack.shape[2]
        slope_map = np.full((nrows, ncols), np.nan, dtype=np.float32)
        pval_map = np.full((nrows, ncols), np.nan, dtype=np.float32)

        print(f"Starting pixel-wise MK analysis for {out_prefix}...")
        for i in range(nrows):
            for j in range(ncols):
                ts = data_stack[:, i, j]
                # 过滤有效值：至少需要3个非空观测且存在波动
                valid_mask = np.isfinite(ts)
                if np.sum(valid_mask) >= 3 and np.std(ts[valid_mask]) > 1e-4:
                    try:
                        res = mk_test(ts[valid_mask])
                        slope_map[i, j] = res.slope
                        pval_map[i, j] = res.p
                    except:
                        continue

        # 5. 保存结果
        slope_out = storage_manager.resolve_output_path(f"{out_prefix}_slope_trend.tif")
        pval_out = storage_manager.resolve_output_path(f"{out_prefix}_pvalue_map.tif")
        plot_out = storage_manager.resolve_output_path(f"{out_prefix}_trend_viz.png")

        with rasterio.open(slope_out, 'w', **out_meta) as dst:
            dst.write(slope_map, 1)
        with rasterio.open(pval_out, 'w', **out_meta) as dst:
            dst.write(pval_map, 1)

        # 6. 自动化可视化
        plt.figure(figsize=(10, 7))
        # 根据斜率分布自动调整色标范围
        v_max = np.nanpercentile(np.abs(slope_map), 98)
        im = plt.imshow(slope_map, cmap='RdYlBu_r', vmin=-v_max, vmax=v_max)
        plt.colorbar(im, label="Sen's Slope (Annual Change Rate)")
        plt.title(f"NTL Trend Analysis: {out_prefix}\n(Mann-Kendall & Sen's Slope)")
        plt.xlabel('Pixel X')
        plt.ylabel('Pixel Y')
        plt.tight_layout()
        plt.savefig(plot_out, dpi=300, bbox_inches='tight')
        plt.close()

        return (
            f"✅ Masked trend analysis for '{out_prefix}' completed.\n"
            f"- **Slope Map**: `outputs/{Path(slope_out).name}` (Rate of change)\n"
            f"- **P-Value Map**: `outputs/{Path(pval_out).name}` (Statistical significance)\n"
            f"- **Visualization**: `outputs/{Path(plot_out).name}` (Map preview)"
        )

    except Exception as e:
        return f"Error during trend analysis: {str(e)}"

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

