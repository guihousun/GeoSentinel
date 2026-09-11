---
name: code-execution-validation
description: Use before running model-authored Python with geo_execute_python. Covers the bounded Docker sandbox, allowed inputs, and the validation that must accompany a script.
---

# 代码执行与验证

- 只有现成工具覆盖不了的方法才写代码；先在方案里说明缺口，不要用代码重写已有工具能算的指标。
- `geo_execute_python` 在**无网络 Docker** 中运行：不能安装包、不能用凭据、不能访问 GEE 或任何远程服务。需要远程数据时先用 `geo_download_gee` 获取，或让用户提供文件放到 `inputs/`。
- 输入来自 `inputs/`；**更早作业的产物在容器里读 `previous/<作业ID>/…`**（宿主侧读取工具与报告里写作 `outputs/<作业ID>/…`，容器内没有这个路径）；新产物写 `outputs/`。不要写工作区之外的路径。
- 代码必须显式写出：输入路径、CRS 与网格假设、NoData 与有效像元处理、单位、关键参数、输出路径。参数用任务给定的值，不要自行"调优"。
- 先验证输入：`geo_inspect_raster` / `geo_inspect_vector` / `geo_validate_geodata` 确认 CRS、范围、网格与字段；输入不合格就不要继续算。
- 运行**一次**主计算，做**一次**与任务相关的验证（有效像元、量级、网格一致性、边界对齐）。不要重复成功过的检查。
- 失败时保留日志，并区分：数据问题、方法问题、环境限制（例如依赖不可用、网络被禁）。
- 平台已注册的方法（ANTL/TNTL 等指标、分区统计、趋势、异常检测、裁剪、重投影、镶嵌、合成、点缓冲区、空间连接、融合）必须用对应工具，不要自己重写公式或阈值。
