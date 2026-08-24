import os
from osgeo import gdal
import numpy as np
from pydantic.v1 import BaseModel, Field
from langchain_core.tools import StructuredTool

from storage_manager import storage_manager


# ======================
# 指数计算函数（带数值稳定性）
# ======================
def compute_rbli(b, g):
    return b / (g )

def compute_rrli(r, g):
    return r / (g )

def compute_ndibg(b, g):
    return (b - g) / (b + g )

def compute_ndigr(g, r):
    return (g - r) / (g + r )


# ======================
# 保存指数影像函数
# ======================
def save_index_tif(array, reference_tif, output_tif_path, description="Index"):
    ds = gdal.Open(reference_tif)
    if ds is None:
        raise ValueError(f"无法打开参考影像: {reference_tif}")
    
    geo_transform = ds.GetGeoTransform()
    projection = ds.GetProjection()
    rows, cols = array.shape

    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.Create(output_tif_path, cols, rows, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(geo_transform)
    out_ds.SetProjection(projection)
    
    band = out_ds.GetRasterBand(1)
    band.WriteArray(array)
    band.SetDescription(description)
    band.SetNoDataValue(-9999.0)
    
    out_ds.FlushCache()
    del out_ds
    print(f"✅ {description} image saved to: {output_tif_path}")


# ======================
# 输入参数模型（新增 min_radiance）
# ======================
class IndexInput(BaseModel):
    radiance_filename: str = Field(
        ..., 
        description="Filename of the calibrated RGB radiance GeoTIFF in your 'inputs/' folder (e.g., 'city_rgb.tif'). Must be 3-band Float32."
    )
    output_filename: str = Field(
        ..., 
        description="Output filename to save in your 'outputs/' folder (e.g., 'city_RRLI.tif')."
    )
    index_type: str = Field(
        ..., 
        description="Index type to compute. Must be one of: 'RBLI', 'RRLI', 'NDIBG', 'NDIGR'"
    )



# ======================
# 主计算函数（带最小辐射阈值）
# ======================
def compute_index_from_rgb_tif(
    radiance_filename: str,
    output_filename: str,
    index_type: str,

) -> str:
    """
    Compute spectral index with optional minimum radiance masking.
    Low-radiance pixels (R+G+B <= min_radiance) are set to NoData (-9999).
    """
    try:
        # Resolve paths securely
        abs_radiance_tif = storage_manager.resolve_input_path(radiance_filename)
        abs_output_tif = storage_manager.resolve_output_path(output_filename)

        if not os.path.exists(abs_radiance_tif):
            return f"❌ Input file not found in 'inputs/': {radiance_filename}"

        ds = gdal.Open(abs_radiance_tif)
        if ds is None or ds.RasterCount < 3:
            return f"❌ Failed to open or invalid band count (<3) in: {radiance_filename}"

        # Read bands (assume R=1, G=2, B=3)
        r = ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
        g = ds.GetRasterBand(2).ReadAsArray().astype(np.float32)
        b = ds.GetRasterBand(3).ReadAsArray().astype(np.float32)
        ds = None  # close

        # Build valid mask
        valid_mask = np.isfinite(r) & np.isfinite(g) & np.isfinite(b)

        min_total_radiance = 0

        # Apply minimum radiance threshold
        if min_total_radiance > 0:
            total_rad = r + g + b
            valid_mask &= (total_rad >= min_total_radiance)

        # Initialize output
        index_array = np.full_like(r, -9999.0, dtype=np.float32)
        idx_type = index_type.upper()

        # Compute index
        if idx_type == "RBLI":
            index_array[valid_mask] = compute_rbli(b[valid_mask], g[valid_mask])
            desc = "RBLI (Blue / Green)"
        elif idx_type == "RRLI":
            index_array[valid_mask] = compute_rrli(r[valid_mask], g[valid_mask])
            desc = "RRLI (Red / Green)"
        elif idx_type == "NDIBG":
            index_array[valid_mask] = compute_ndibg(b[valid_mask], g[valid_mask])
            desc = "NDIBG (Blue - Green) / (Blue + Green)"
        elif idx_type == "NDIGR":
            index_array[valid_mask] = compute_ndigr(g[valid_mask], r[valid_mask])
            desc = "NDIGR (Green - Red) / (Green + Red)"
        else:
            return f"❌ Unsupported index_type: '{index_type}'. Choose from: RBLI, RRLI, NDIBG, NDIGR."

        # Ensure output dir exists
        os.makedirs(os.path.dirname(abs_output_tif), exist_ok=True)

        # Save
        save_index_tif(index_array, abs_radiance_tif, abs_output_tif, desc)

        valid_ratio = np.sum(valid_mask) / valid_mask.size
        return f"✅ {idx_type} computed and saved to 'outputs/{output_filename}'. Valid pixel ratio: {valid_ratio:.2%}"

    except Exception as e:
        return f"❌ Error during index computation: {str(e)}"


# ======================
# LangChain 工具封装
# ======================
SDGSAT1_index_tool = StructuredTool.from_function(
    func=compute_index_from_rgb_tif,
    name="SDGSAT1_compute_index",
    description=(
        "Compute a spectral index (RBLI, RRLI, NDIBG, or NDIGR) from an SDGSAT-1 RGB radiance image in your 'inputs/' folder. "
        "Pixels with total radiance (R+G+B) below 'min_total_radiance' are masked as NoData (-9999). "
        "Result is saved to your 'outputs/' folder. "
        "\n\nExample:\n"
        "radiance_filename='shanghai_rgb.tif',\n"
        "output_filename='shanghai_RRLI.tif',\n"
        "index_type='RRLI',\n"
    ),
    args_schema=IndexInput,
    return_direct=True
)


# if __name__ == "__main__":
#     compute_index_from_rgb_tif(
#         radiance_tif="SDGSAT_1/SDGSAT1_GLI_shanghai_radiance_rgb.tif",
#         output_tif="SDGSAT_1/SDGSAT1_GLI_shanghai_RRLI1.tif",
#         index_type="RRLI"
#     )
#     compute_index_from_rgb_tif(
#         radiance_tif="SDGSAT_1/SDGSAT1_GLI_shanghai_radiance_rgb.tif",
#         output_tif="SDGSAT_1/SDGSAT1_GLI_shanghai_RBLI.tif",
#         index_type="RBLI"
#     )
