# import numpy as np
# import rasterio
# from scipy.spatial import Delaunay
# from shapely.geometry import LineString
# from rasterio.features import rasterize
#
# # ------------ 核心函数（与之前一致） ------------
# def build_tin(grid, transform=None):
#     # 构建简化点集，模拟 Z 容差 = 5 nW/cm²/sr
#     h, w = grid.shape
#     cols, rows = np.meshgrid(np.arange(w), np.arange(h))
#     cols_flat, rows_flat = cols.ravel(), rows.ravel()
#     if transform is not None:
#         xs, ys = transform * (cols_flat, rows_flat)
#         coords_all = np.vstack([xs, ys]).T
#     else:
#         coords_all = np.vstack([cols_flat, rows_flat]).T
#
#     # 保留与邻点亮度差 >5 的像元，减少点数
#     diffs = np.abs(grid.ravel() - np.roll(grid.ravel(), 1))
#     mask = diffs > 5.0
#     coords = coords_all[mask]
#
#     tri = Delaunay(coords)
#     return tri, coords, mask
#
#
# def detect_saddles(tri, values, coords):
#     indptr, indices = tri.vertex_neighbor_vertices
#     saddles = []
#
#     for i in range(len(values)):
#         neigh = indices[indptr[i]:indptr[i+1]]
#         # 按极角逆时针排序邻点
#         center = coords[i]
#         from math import atan2
#         angles = [atan2(coords[j][1] - center[1], coords[j][0] - center[0]) for j in neigh]
#         order = np.argsort(angles)
#         neigh = neigh[order]
#
#         diffs = values[neigh] - values[i]
#         # 计算 ΔNTL+、ΔNTL− 和符号变化 Ns
#         pos_sum = np.sum(diffs[diffs > 0])
#         neg_sum = np.sum(diffs[diffs < 0])
#         signs = np.sign(np.concatenate([diffs, diffs[:1]]))
#         Ns = np.count_nonzero(signs[:-1] != signs[1:])
#
#         # 退化鞍点分解后判断：Δ+ + Δ- >0 且 Ns == 4
#         if pos_sum + neg_sum != 0 and Ns == 4:
#             saddles.append(i)
#
#     return saddles
#
#
# def trace_skeleton(tri, coords, values, saddles, kind='ridge'):
#     indptr, indices = tri.vertex_neighbor_vertices
#     lines = []
#
#     for s in saddles:
#         nbrs = indices[indptr[s]:indptr[s+1]]
#         diffs0 = values[nbrs] - values[s]
#         # 分别取两个最陡升（ridge）或最陡降（valley）方向
#         if kind == 'ridge':
#             idx = np.argsort(-diffs0)[:2]
#         else:
#             idx = np.argsort(diffs0)[:2]
#
#         for n0 in nbrs[idx]:
#             path = [s]
#             visited = {s}
#             cur = n0
#
#             while True:
#                 if cur in visited:
#                     break
#                 path.append(cur)
#                 visited.add(cur)
#
#                 nbr = indices[indptr[cur]:indptr[cur+1]]
#                 diffs2 = values[nbr] - values[cur]
#
#                 # 到达峰点（ridge）或谷点（valley）的终止条件
#                 if kind == 'ridge':
#                     if np.all(diffs2 <= 0):
#                         break
#                     cand_idx = np.argmax(diffs2)
#                 else:
#                     if np.all(diffs2 >= 0):
#                         break
#                     cand_idx = np.argmin(diffs2)
#
#                 cur = nbr[cand_idx]
#
#             lines.append(path)
#
#     return lines
#
#
# def filter_roads(lines, values, coords, cv_thresh=0.6):
#     selected = []
#     for line in lines:
#         vals = values[line]
#         mu, sigma = vals.mean(), vals.std(ddof=1)
#         cv = sigma / mu if mu != 0 else np.inf
#         if cv <= cv_thresh:
#             selected.append(LineString(coords[i] for i in line))
#     return selected
#
#
#
#
# # ------------ 主流程 ------------
# # input_tif = r'./NTL-CHAT/SGDSAT-1/KX10_GIU_20220304_E121.82_N31.56_202200100146_L4A/' \
# #             'KX10_GIU_20220304_E121.82_N31.56_202200100146_L4A_A_LH.tif'
# input_tif = "./NTL_Agent/Night_data/SDGSAT-1/roads_binary_t.tif"
# output_mask = './NTL_Agent/Night_data/SDGSAT-1/roads_binary.tif'
#
# # 1. 读取第一个波段
# with rasterio.open(input_tif) as src:
#     grid = src.read(1).astype(float)
#     transform = src.transform
#     crs = src.crs
#     profile = src.profile
#
# # 2. 构建 TIN 并检测鞍点
# tri, coords, mask = build_tin(grid, transform)
# flat  = grid.ravel()
# values = flat[mask]
# saddles = detect_saddles(tri, values, coords)
#
# # 3. 追踪骨架线 & 按 CV 筛选为道路
# ridge_lines = trace_skeleton(tri, coords, values, saddles, kind='ridge')
# road_geoms = filter_roads(ridge_lines, values, coords, cv_thresh=0.6)
#
# # 4. 矢量栅格化：道路=1，背景=0
# mask = rasterize(
#     [(geom, 1) for geom in road_geoms],
#     out_shape=grid.shape,
#     transform=transform,
#     fill=0,
#     all_touched=False,    # 保持骨架中心线，不把相交像素全部染黑
#     dtype=rasterio.uint8
# )
#
#
# # 5. 保存二值化 TIFF
# profile.update({
#     'dtype': rasterio.uint8,
#     'count': 1,
#     'compress': 'lzw',
#     'nodata': 255
# })
# with rasterio.open(output_mask, 'w', **profile) as dst:
#     dst.write(mask, 1)
#
# print(f'道路二值掩模已保存至 {output_mask}')

# import numpy as np
# import rasterio
# from skimage import filters, morphology, util
# import warnings
#
# warnings.filterwarnings('ignore')
#
# # 用户配置
# input_tif  = r'./NTL_Agent/Night_data/SDGSAT-1/roads_binary_t.tif'   # 替换为你的灰度图路径
# output_tif = r'./NTL_Agent/Night_data/SDGSAT-1/otsu_roads_binary2.tif'              # 输出二值化道路掩模
#
# # 1. 读取单波段灰度图
# with rasterio.open(input_tif) as src:
#     gray    = src.read(1).astype(np.uint8)  # 确保 0–255
#     profile = src.profile.copy()            # 保存原图元数据
#
# # 2. 计算 Otsu 全局阈值
# #    Otsu 会自动找出一条分割亮/暗的最优阈值
# thresh = filters.threshold_otsu(gray)
# print(f"自动计算的 Otsu 阈值：{thresh}")
#
# # 3. 二值化：亮度 > 阈值 视为“道路”
# mask = gray > (thresh)
#
# # 4. 形态学后处理
# # 4.1 先去除小碎片（假阳性）：保留面积 ≥ 15 像素的连通块
# mask = morphology.remove_small_objects(mask, min_size=15)
#
# # 4.2 闭运算（膨胀后腐蚀）连接断裂的线段
# mask = morphology.binary_closing(mask)
#
# # 4.3 （可选）骨架化，提取中心线
# mask = morphology.skeletonize(mask)
#
# # 5. 保存为二值 GeoTIFF（道路=1，其余=0）
# out = mask.astype(np.uint8)
# profile.update({
#     'dtype':   rasterio.uint8,
#     'count':   1,
#     'compress':'lzw',
#     'nodata': 255
# })
# with rasterio.open(output_tif, 'w', **profile) as dst:
#     dst.write(out, 1)
#
# print(f"完成：阈值法道路掩模已保存到 {output_tif}")

# from langchain_core.tools import StructuredTool
# from pydantic.v1 import BaseModel, Field
# import numpy as np
# import rasterio
# from skimage import filters, morphology

# from storage_manager import storage_manager

# class OtsuRoadExtractionInput(BaseModel):
#     input_tif: str = Field(
#         ...,
#         description="Filename of the input grayscale GeoTIFF in your 'inputs/' directory (e.g., 'gee_nightlight.tif')."
#     )
#     output_tif: str = Field(
#         ...,
#         description="Output filename to save the road mask in your 'outputs/' directory (e.g., 'road_mask_otsu.tif')."
#     )

# def extract_road_mask_by_otsu(input_tif: str, output_tif: str) -> str:

#     input_path = storage_manager.resolve_input_path(input_tif)
#     output_path = storage_manager.resolve_output_path(output_tif)
    
#     with rasterio.open(input_path) as src:
#         gray = src.read(1).astype(np.uint16)
#         profile = src.profile.copy()

#     thresh = 40
#     print(f"Otsu computed threshold: {thresh}")

#     mask = gray > thresh
#     mask = morphology.remove_small_objects(mask, min_size=15)
#     mask = morphology.binary_closing(mask)
#     mask = morphology.skeletonize(mask)

#     out = mask.astype(np.uint8)
#     profile.update({
#         'dtype': rasterio.uint8,
#         'count': 1,
#         'compress': 'lzw',
#         'nodata': 255
#     })

#     with rasterio.open(output_path, 'w', **profile) as dst:
#         dst.write(out, 1)
#     print(f"✅ Road mask extracted using Otsu thresholding and saved to: {output_path}")
#     return f"✅ Road mask extracted using Otsu thresholding and saved to: {output_path}"

# otsu_road_extraction_tool = StructuredTool.from_function(
#     func=extract_road_mask_by_otsu,
#     name="extract_road_mask_from_grayscale_using_otsu",
#     description=(
#         "Extract a binary road centerline mask from a grayscale image using Otsu global thresholding "
#         "and morphological post-processing. Outputs a GeoTIFF binary mask."
#     ),
#     args_schema=OtsuRoadExtractionInput
# )

# otsu_road_extraction_tool.run({
#     "input_tif": "./NTL_Agent/Night_data/SDGSAT-1/SDGSAT_1_test.tif",
#     "output_tif": "./NTL_Agent/Night_data/SDGSAT-1/otsu_roads_binary.tif"
# })

from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field
import numpy as np
import rasterio
from skimage import filters, morphology
from storage_manager import storage_manager


class OtsuRoadExtractionInput(BaseModel):
    input_tif: str = Field(
        ..., description="Filename of the input grayscale GeoTIFF located in 'inputs/' (e.g., 'gee_nightlight.tif')."
    )
    output_tif: str = Field(
        ..., description="Output filename for saving the road mask in 'outputs/' (e.g., 'road_mask_otsu.tif')."
    )


def extract_road_mask_by_otsu(input_tif: str, output_tif: str) -> str:
    """
    Extract road centerline mask using Otsu-based thresholding and post-processing.
    """
    # Resolve paths using storage_manager
    input_path = storage_manager.resolve_input_path(input_tif)
    output_path = storage_manager.resolve_output_path(output_tif)

    # Load grayscale image
    with rasterio.open(input_path) as src:
        gray = src.read(1).astype(np.float32)  # Ensure compatibility for intensity-based operations
        profile = src.profile.copy()

    # Otsu Thresholding
    threshold = filters.threshold_otsu(gray)
    print(f"Otsu computed threshold: {threshold}")
    mask = gray > threshold

    # Morphological post-processing for road centerlines
    mask = morphology.remove_small_objects(mask, min_size=15)  # Remove noise
    mask = morphology.binary_closing(mask)                     # Close small gaps
    mask = morphology.skeletonize(mask)                        # Generate road centerlines

    # Prepare output
    road_mask = mask.astype(np.uint8)
    profile.update({
        'dtype': rasterio.uint8,
        'count': 1,
        'compress': 'lzw',
        'nodata': 255
    })

    # Write processed mask to output file
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(road_mask, 1)

    print(f"✅ Road mask extracted using Otsu thresholding and saved to: {output_path}")
    return f"✅ Road mask extracted successfully and saved to: 'outputs/{output_tif}'"


# Register Tool
otsu_road_extraction_tool = StructuredTool.from_function(
    func=extract_road_mask_by_otsu,
    name="Extract_Road",
    description=(
        "Extracts binary road centerline masks from grayscale images using Otsu global thresholding "
        "and morphological post-processing. The output is a GeoTIFF binary mask saved to your 'outputs/' folder."
    ),
    args_schema=OtsuRoadExtractionInput
)