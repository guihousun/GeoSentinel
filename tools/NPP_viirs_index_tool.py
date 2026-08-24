from pydantic import BaseModel
from storage_manager import storage_manager
import rasterio
import numpy as np
import os
from langchain_core.tools import StructuredTool


class VNCIInput(BaseModel):
    ndvi_tif: str
    ntl_tif: str
    output_tif: str


def compute_vnci_index(ndvi_tif: str, ntl_tif: str, output_tif: str) -> str:
    """
    Compute Vegetation Nighttime Condition Index (VNCI) using NDVI and NTL data within a triangular feature space.
    This version incorporates workspace-aware path handling.
    """
    # Resolve paths using `storage_manager`
    abs_ndvi_path = storage_manager.resolve_input_path(ndvi_tif)
    abs_ntl_path = storage_manager.resolve_input_path(ntl_tif)
    abs_output_path = storage_manager.resolve_output_path(output_tif)

    # Check if input files exist
    if not os.path.exists(abs_ndvi_path):
        return f"❌ Error: NDVI file not found at 'inputs/{ndvi_tif}'"
    if not os.path.exists(abs_ntl_path):
        return f"❌ Error: NTL file not found at 'inputs/{ntl_tif}'"

    # Define triangle in feature space
    A = np.array([0.1, 0])  # Triangle vertex A (NDVI space)
    B = np.array([0.8, 0])  # Triangle vertex B (NDVI space)
    C = np.array([0.3, 60])  # Triangle vertex C (NTL range)
    
    def point_dist(p, a, b):
        # Compute normalized perpendicular distance from point `p` to line `ab`
        ap = np.array(p) - np.array(a)
        ab = np.array(b) - np.array(a)
        return np.abs(np.cross(ab, ap)) / np.linalg.norm(ab)

    with rasterio.open(abs_ndvi_path) as ndvi_src:
        ndvi = ndvi_src.read(1).astype(np.float32) / 10000  # Scale MODIS NDVI
        profile = ndvi_src.profile

    with rasterio.open(abs_ntl_path) as ntl_src:
        ntl = ntl_src.read(1).astype(np.float32)

    rows, cols = ndvi.shape
    vnci = np.zeros_like(ndvi, dtype=np.float32)

    for i in range(rows):
        for j in range(cols):
            x = ndvi[i, j]
            y = ntl[i, j]
            if x <= 0 or y < 0:
                vnci[i, j] = 0
                continue
            d = point_dist([x, y], A, B)
            d_max = point_dist(C, A, B)
            vnci[i, j] = d / d_max if d_max > 0 else 0

    profile.update(dtype='float32')

    with rasterio.open(abs_output_path, 'w', **profile) as dst:
        dst.write(vnci, 1)

    return f"✅ VNCI image saved to 'outputs/{output_tif}'"


# LangChain tool registration
vnci_index_tool = StructuredTool.from_function(
    func=compute_vnci_index,
    name="VNCI_Compute",
    description=(
        "This tool computes the Vegetation Nighttime Condition Index (VNCI) using NDVI and NTL imagery. VNCI evaluates "
        "relationships between vegetation health and nighttime light conditions using triangular feature space geometry. "
        "The result is saved as a GeoTIFF in your 'outputs/' folder. "
        "\n\nExample usage:\n"
        "ndvi_tif='ndvi_2021.tif',\n"
        "ntl_tif='ntl_2021.tif',\n"
        "output_tif='vnci_2021.tif'."
    ),
    input_type=VNCIInput
)