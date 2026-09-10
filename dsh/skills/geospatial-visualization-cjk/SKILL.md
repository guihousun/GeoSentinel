---
name: geospatial-visualization-cjk
description: Use when producing charts, maps or tables from geospatial or remote-sensing data. Covers matplotlib/GeoPandas setup in the sandbox, Chinese labels, and how to make the result render inline in the conversation.
---

# 可视化与表格：画得对，也要显示出来

## 环境

`geo_execute_python` 的沙箱里已装好 `matplotlib`、`pandas`、`geopandas`、`rasterio`、`scikit-learn`，以及中文字体；Excel 导出用 `openpyxl`。无网络、不能用凭据，图和数据都在本地产出。

**中文字体的实际注册名是 `Noto Sans CJK JP`**（文件 `/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc`）。
不存在名为 `Noto Sans CJK SC` 的字体：写成 SC **不会报错**，而是静默回退到 DejaVu Sans，图里中文变成方框或空框，
日志里只留一堆 `Glyph … missing from font(s) DejaVu Sans` 警告。不确定时先查一次：

```python
import matplotlib.font_manager as fm
print(sorted({font.name for font in fm.fontManager.ttflist}))
```

## 画图（matplotlib）

```python
import matplotlib
matplotlib.use("Agg")          # 无显示环境，必须用 Agg
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["Noto Sans CJK JP", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["axes.unicode_minus"] = False

fig, ax = plt.subplots(figsize=(8, 5))
...
fig.tight_layout()
fig.savefig("outputs/district_antl_bar.png", dpi=200, bbox_inches="tight", facecolor="white")
plt.close(fig)
```

规则：

- 一定用 `fig, ax = plt.subplots(...)`，不要用全局 pyplot 状态；保存后 `plt.close(fig)`。
- 保存路径写工作区相对的 `outputs/<有意义的名字>.png`（不要写绝对路径，不要用 `storage_manager`）。
- 中文类目多时用横向条形图，并留足左边距（`fig.subplots_adjust(left=0.22, right=0.92)`）。
- 栅格预览用 `rasterio` 读入后 `ax.imshow`，并写明波段、单位与 NoData 处理；不要只画不解释。
- 图上要能读出：标题、单位、时间与数据来源；数值标注保留合理位数。

## 表格

- 小表（≤ 20 行、≤ 6 列）：直接把结果写成 **Markdown 表格**放进回答，用户能在对话里直接看。
- 大表：写 `outputs/<名字>.csv`（pandas `to_csv(index=False)`），在回答里给出路径与关键指标；需要 Excel 时用 `openpyxl` 写 `.xlsx`。
- 表里必须带区名/对象名、指标值、单位；不要只给一列数字。

## 让结果在对话里显示

`geo_execute_python` 等工具的结果里会带 `artifacts` 数组，每一项有 `path`、`kind` 和 `url`。**图片用 Markdown 图片语法内联**：

```
![2020 年上海各区 ANTL](artifacts 里对应的 url)
```

- 只内联 `kind: "image"` 的产物；`table`/`data`/`file` 给路径或下载链接。
- 图片说明写清对象、指标、时间与单位，不要只写"见图"。
- 不要把同一张图重复内联多次；也不要内联没有真实产出的图片。

## 交付前自检

- 文件确实存在（`geo_list_files` 能看到），且扩展名与内容一致。
- **运行输出里没有 `missing from font` / `Glyph … missing` 警告**；有警告就说明字体名写错，中文会变空框，必须换名重画。
- 中文不是方框/乱码；轴标签、图例、数值标注没有被裁掉。
- 图表结论与数据一致：写进回答的数值必须能在产物里找到。
- 不要把"图看起来像"当作结论证据；图是表达方式，证据仍是数据与来源。
