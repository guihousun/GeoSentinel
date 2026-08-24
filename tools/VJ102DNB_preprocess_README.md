# VJ102DNB/VJ103DNB 数据预处理工具

## 概述

本工具用于预处理从 LAADS DAAC 下载的 Suomi NPP VIIRS Day/Night Band (DNB) 数据。

### 数据类型说明

- **VJ102DNB**: 辐射数据 (Radiance Data) - 包含 DNB 观测的辐射值
- **VJ103DNB**: 几何校正数据 (Geolocation Data) - 包含经纬度、太阳/月亮角度等

**注意**: 本预处理工具只处理 VJ102DNB 辐射数据文件。VJ103DNB 几何数据通常用于后续的大气校正或地理配准。

## 文件结构

VJ102DNB .nc 文件实际是 HDF5 格式，包含以下主要数据集：

```
observation_data/DNB_observations      - 辐射数据 (3216×4064, float32)
observation_data/DNB_quality_flags     - 质量控制标志 (3216×4064, uint16)
scan_line_attributes/                  - 扫描线属性
navigation_data/                       - 导航数据
```

## 安装依赖

```bash
conda install -c conda-forge h5py numpy xarray netCDF4
```

## 使用方法

### 单个文件预处理

```bash
python tools/VJ102DNB_preprocess.py \
    --input "e:/Download/LAADS_data/VJ102DNB.A2026058.0000.021.2026058063400.nc" \
    --output "e:/Download/LAADS_processed"
```

### 批量预处理

```bash
python tools/VJ102DNB_preprocess.py \
    --input "e:/Download/LAADS_data" \
    --output "e:/Download/LAADS_processed"
```

### 不使用质量控制

```bash
python tools/VJ102DNB_preprocess.py \
    --input "e:/Download/LAADS_data" \
    --output "e:/Download/LAADS_processed" \
    --no-qc
```

## 输出文件

对于每个输入的 .nc 文件，工具会生成：

1. **{filename}_radiance.npy**: 辐射数据数组 (numpy 格式)
   - 形状：(scans, pixels) 或 (lines, pixels)
   - 单位：W·cm⁻²·sr⁻¹
   - 已应用质量控制掩膜（无效像素设为 NaN）

2. **{filename}_metadata.json**: 元数据文件
   - 文件信息
   - 数据统计 (min, max, mean, std)
   - 有效像素数量
   - 原始 HDF5 属性

3. **batch_summary.json**: 批量处理汇总 (仅批量模式)
   - 成功/失败文件数
   - 总有效像素数

## 输出示例

```json
{
  "total_files": 13,
  "success": 13,
  "failed": 0,
  "total_valid_pixels": 170255952
}
```

## 数据说明

### VJ102DNB 辐射数据

- **时间分辨率**: 约 6 分钟/景
- **空间分辨率**: 750m
- **辐射范围**: 0 - 0.001 W·cm⁻²·sr⁻¹ (典型夜间值)
- **有效像素**: 约 13M 像素/景

### 质量控制

工具会自动应用质量控制标志：
- 读取 `DNB_quality_flags` 数据集
- 标记质量为 0 的像素为有效
- 其他质量标记的像素设为 NaN

## 后续处理建议

1. **辐射定标**: 输出的辐射值已经是物理量，可直接使用
2. **大气校正**: 可使用 VJ103DNB 中的几何数据进行大气校正
3. **云检测**: 建议结合云掩膜产品 (如 VNP03) 进行云像素过滤
4. **时间序列分析**: 多时相数据可合成月均/年均夜间灯光产品

## 常见问题

### Q: 为什么 VJ103DNB 文件处理失败？
A: VJ103DNB 是几何数据，不包含辐射观测值。工具会自动跳过 VJ103DNB 文件。

### Q: 输出辐射值非常小 (接近 0)？
A: 夜间辐射值通常在 10⁻⁹ 到 10⁻⁶ W·cm⁻²·sr⁻¹ 范围，这是正常的。

### Q: 如何处理缺失的像素？
A: 缺失像素被标记为 NaN，可使用 numpy 的 nan 函数处理，如 `np.nanmean()`.

## 数据引用

- **数据源**: LAADS DAAC (https://ladsweb.modaps.eosdis.nasa.gov/)
- **产品**: VJ102DNB - VIIRS/JPSS1 Day/Night Band L1B Swath 750m
- **版本**: v3.0.0

## 更新日志

- **2026-03-02**: 初始版本
  - 支持 VJ102DNB .nc 格式 (HDF5)
  - 自动质量控制
  - 批量处理功能
