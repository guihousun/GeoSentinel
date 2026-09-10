const string = (required = false) => ({ type: "string", ...(required ? { required: true } : {}) });
const paths = (required = false) => ({ type: "array", items: { type: "string" }, ...(required ? { required: true } : {}) });
const output = { output_path: string(true) };
const pointColumns = { lon_col: string(), lat_col: string() };
export const GIS_TOOLS = [
  ["inspect_vector", "vector", { path: string(true) }, "检查矢量要素数量、CRS、字段与几何类型。"],
  ["validate_geodata", "raster", { raster_paths: paths(), vector_paths: paths() }, "检查栅格和矢量的有效性、空间重叠及网格兼容性。"],
  ["clip_raster", "raster", { raster_path: string(true), vector_path: string(true), ...output, all_touched: { type: "boolean" } }, "按矢量范围裁剪栅格。"],
  ["reproject_raster", "raster", { raster_path: string(true), ...output, dst_crs: string(true), resampling: string() }, "重投影栅格；分类数据应显式采用 nearest。"],
  ["mosaic_rasters", "raster", { raster_paths: paths(true), ...output, method: string() }, "镶嵌兼容栅格，保留核心的网格检查。"],
  ["calculate_zonal_statistics", "ntl", { raster_paths: paths(true), vector_path: string(true), ...output, selected_indices: paths(), only_global: { type: "boolean" } }, "逐行政区统计夜间灯光；均值选择 ANTL，不要为已有分区统计另写脚本。"],
  ["calculate_ntl_metrics_for_raster", "ntl", { raster_path: string(true), band: { type: "integer" }, selected: paths() }, "计算单幅夜间灯光指标（如 ANTL、TNTL），保留单位和空间支持说明。"],
  ["composite_ntl_rasters", "ntl", { raster_paths: paths(true), ...output, method: string() }, "对齐网格的多期夜间灯光合成。"],
  ["analyze_ntl_trend", "ntl", { raster_paths: paths(true), vector_path: string(true), output_prefix: string(true) }, "有序时间栅格趋势分析，输出斜率及显著性；必须核实输入时序。"],
  ["detect_ntl_anomaly", "ntl", { raster_paths: paths(true), ...output, target_index: { type: "integer" }, k_sigma: { type: "number" }, minimum_baseline_observations: { type: "integer" } }, "基线夜间灯光异常检测，异常不等于事件因果证明。"],
  ["filter_points_by_polygon", "vector", { points_path: string(true), polygon_path: string(true), ...output, ...pointColumns, predicate: string() }, "按多边形空间谓词筛选点。"],
  ["buffer_points_aeqd", "vector", { points_path: string(true), ...output, ...pointColumns, radius_km: { type: "number", required: true } }, "使用局部等距投影生成点缓冲区，半径单位为公里。"],
  ["spatial_join_points_to_admin", "vector", { points_path: string(true), admin_path: string(true), ...output, ...pointColumns, admin_name_col: string(), admin_iso_col: string(), prefix: string() }, "为空间点附加行政区属性；必须指定真实存在的名称和编码字段。"],
  ["dissolve_intersections", "vector", { polygons_path: string(true), ...output }, "融合相交多边形。"],
];
export const GIS_TOOL_NAMES = GIS_TOOLS.map(([name]) => `geo_${name}`);
