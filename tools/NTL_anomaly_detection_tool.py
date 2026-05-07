from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from typing import List, Optional
import numpy as np
import rasterio
import os
from pathlib import Path

# 导入你的存储管理器
from storage_manager import storage_manager

# ===== Input Schema =====
class SimpleAnomalyDetectionInput(BaseModel):
    raster_files: List[str] = Field(
        ...,
        description="Time-series NTL raster file names (e.g., ['NTL_2022.tif', 'NTL_2023.tif']). Files should be located in the workspace 'inputs/' folder."
    )
    target_index: Optional[int] = Field(
        None,
        description="Index of the specific image to be detected (0-based). Default is the latest image."
    )
    k_sigma: float = Field(
        3.0,
        description="Threshold: pixels with a Z-score > k_sigma are flagged as anomalies."
    )
    save_filename: Optional[str] = Field(
        "NTL_anomaly_mask.tif",
        description="The filename for the generated anomaly mask. Saved to the 'outputs/' folder."
    )

# ===== Tool Logic =====
def detect_ntl_anomaly(
    raster_files: List[str],
    target_index: Optional[int] = None,
    k_sigma: float = 3.0,
    save_filename: str = "NTL_anomaly_mask.tif"
) -> str:
    """
    Core function for detecting anomalies in NTL time-series using standardized workspace paths.
    """
    # 1. 获取动态工作空间路径
    workspace = storage_manager.get_workspace()
    input_dir = workspace / "inputs"
    output_dir = workspace / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 2. 补全输入文件路径
    # 支持传入纯文件名或相对路径，统一指向 inputs/
    full_raster_paths = []
    for f in raster_files:
        p = Path(f)
        full_path = input_dir / p.name if not p.is_absolute() else p
        if not full_path.exists():
            return f"Error: File not found: {full_path.name} in inputs/ folder."
        full_raster_paths.append(str(full_path))

    # 3. 确定目标检测索引
    target_index = target_index if target_index is not None else len(full_raster_paths) - 1
    if target_index < 0 or target_index >= len(full_raster_paths):
        return f"Error: target_index is out of range (0~{len(full_raster_paths)-1})"

    # 4. 读取影像并执行 Z-Score 算法
    try:
        with rasterio.open(full_raster_paths[0]) as src:
            profile = src.profile
            height, width = src.height, src.width

        stack = np.empty((len(full_raster_paths), height, width), dtype=np.float32)
        for i, f in enumerate(full_raster_paths):
            with rasterio.open(f) as src:
                stack[i] = src.read(1)

        # 统计基准计算 (排除目标期)
        baseline = np.delete(stack, target_index, axis=0)
        mean_img = np.nanmean(baseline, axis=0)
        std_img = np.nanstd(baseline, axis=0)

        # 异常检测
        target_img = stack[target_index]
        z_score = (target_img - mean_img) / (std_img + 1e-6)
        anomaly_mask = (z_score > k_sigma).astype(np.uint8)

        # 5. 保存结果到 outputs/
        output_file_path = output_dir / save_filename
        profile.update(dtype=rasterio.uint8, count=1, nodata=0)
        
        with rasterio.open(output_file_path, "w", **profile) as dst:
            dst.write(anomaly_mask, 1)

        return (
            f"✅ Anomaly Detection Task Completed.\n"
            f"- **Target Image**: {Path(full_raster_paths[target_index]).name}\n"
            f"- **Method**: Pixel-wise Z-Score Analysis (Threshold: {k_sigma}σ)\n"
            f"- **Result Saved**: `outputs/{save_filename}`"
        )
        
    except Exception as e:
        return f"Error during processing: {str(e)}"

# ===== Tool Registration =====
detect_ntl_anomaly_tool = StructuredTool.from_function(
    func=detect_ntl_anomaly,
    name="Detect_NTL_anomaly",
    description=(
        "Identifies sudden brightness spikes or significant fluctuations in nighttime light (NTL) time-series data. "
        "The tool uses a statistical Z-Score (K-Sigma) method to compare a target image against a historical baseline. "
        "It automatically reads inputs from the workspace 'inputs/' folder and saves results to 'outputs/'. "
        "Useful for detecting post-disaster recovery, large-scale construction, or unexpected economic activity."
    ),
    input_type=SimpleAnomalyDetectionInput,
)