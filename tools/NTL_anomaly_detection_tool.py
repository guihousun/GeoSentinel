from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from typing import List, Optional
import numpy as np
import rasterio
import os
from pathlib import Path

# 导入你的存储管理器
from storage_manager import storage_manager
from ntl_toolkit.adapters.langchain import anomaly_detection

# ===== Input Schema =====
class SimpleAnomalyDetectionInput(BaseModel):
    raster_files: List[str] = Field(
        ...,
        description="Time-series NTL raster file names (at least two images, e.g., ['NTL_2022.tif', 'NTL_2023.tif']). Files should be located in the workspace 'inputs/' folder."
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
    save_filename = save_filename or "NTL_anomaly_mask.tif"
    full_raster_paths = [storage_manager.resolve_input_path(path) for path in raster_files]
    output_file_path = storage_manager.resolve_output_path(save_filename)
    return anomaly_detection(
        full_raster_paths,
        output_file_path,
        target_index=target_index,
        k_sigma=k_sigma,
    )

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
