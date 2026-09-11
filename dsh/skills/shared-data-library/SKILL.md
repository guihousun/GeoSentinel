---
name: shared-data-library
description: Use whenever a task should read or analyse material from the shared data library (公开用例数据、全球矢量边界、影像、GDP、书籍与数据包，路径以 share/ 开头)。Tells you to read share/CATALOG.md first instead of opening files one by one, how every file type is parsed, how the container sees the same data, and what is not allowed.
---

# 共享数据库（share/）

平台管理员配置的**只读**共享数据：公开用例数据、已下载的全球矢量边界与影像、各国 GDP、参考书与数据包。
它位于账号数据目录之外，因此**不占项目配额、也不属于某个项目**，全体账号共用。

**先读目录，不要逐个文件试**：每个共享根下都有 `CATALOG.md`（以及给程序用的 `catalog.json`），
由 `dsh/tools/catalog-share.mjs` 从真实文件生成，逐条列出相对路径、类型、大小与关键信息
（矢量：几何类型/要素数/坐标系/字段；表格：工作表/列名/样本行数；影像：像元数/波段/数据类型/CRS；
地理数据库：图层与几何类型）。**一个文件读完就知道有什么、字段是什么、怎么解析**，比 `read` 试探快得多。

## 访问方式（三处路径语义一致）

| 用途 | 路径 |
| --- | --- |
| 智能体读文本/表格/目录 | `read` / `glob` / `grep` / `read_document` 用 `share/<相对路径>`（`geo_list_files` 会给出清单） |
| 用户在左栏查看 | 「文件」里的只读分组「共享数据（只读）」 |
| 容器内分析 | 同一文件只读挂载为 `/workspace/share/<相对路径>`，`geo_execute_python` 与注册 GIS 工具都能直接读 |

注册的 `geo_*` 工具只接受容器内相对路径，共享数据写成 `share/<相对路径>`
（与 `inputs/`、`previous/`、`outputs/` 同一套规则）；**输出只能写 `outputs/`**。
`geo_inspect_raster` 同样接受 `share/<根名>/<文件>`；只有不带前缀的裸文件名才由它的 `source` 参数决定根。

## 按类型怎么处理

| 类型 | 做法 | 注意 |
| --- | --- | --- |
| `.xlsx` / `.csv` | `read_document`（宿主侧）或容器内 `pandas.read_excel/read_csv` | 表注/脚注往往是口径定义，先看列名与单位 |
| `.shp` / `.geojson` / `.gpkg` | 容器内 `ogrinfo -so`、`ogr2ogr`，或 `ogr2ogr -f GPKG` 转换后用 geopandas | 平台注册工具要求数据在容器可见路径下；`.prj` 决定坐标系 |
| `.tif` / `.tiff` | 容器内 `gdalinfo` / `rasterio`；`geo_inspect_raster` 直接收 `share/<相对路径>`（`source` 只对裸文件名生效） | 书里插图常是**无地理参考的图片**（如 1677×955、LZW），只能当对照，不能当观测 |
| `.mdb`（ESRI 个人地理数据库） | **必须先转换**：`ogr2ogr -f GPKG out.gpkg "<mdb>" <图层名> [-spat 西 南 东 北]`，再用 geopandas/注册工具 | geopandas/pyogrio 自带 GDAL **不含 PGeo**，直接读会报 `not recognized as being in a supported file format`；系统 GDAL（`ogrinfo`/`ogr2ogr`）可以读 |
| `.rar` / `.zip` | 宿主侧解压后再放入 `inputs/` 或共享根目录 | 容器内没有解压工具 |
| 扫描版 `.pdf` | 需要 OCR；页号与印刷页常有固定偏移，引用数字前必须核对原页 | OCR 的数字与专名会有错，不能直接当结论 |
| `.jpg` / `.png` | 只作对照，不作计算输入 | — |

## 硬约束

- **只读**：产品侧无法修改共享数据。宿主围栏只允许把模型写入落在对话自己的 `outputs/`，容器挂载是 `readonly`。
- **引用要落到共享数据本身**：报告/证据里写清 `share/<相对路径>`（容器内路径写成 `/workspace/share/…`），
  并说明数据的年份、口径与限制；共享数据不等于平台观测，也不等于已核实结论。
  `geo_write_report` 的 `source_paths` 与 `geo_write_evidence` 的 `evidence[].source` 都接受
  `share/<根名>/<文件>`（只校验文件真实存在，只读引用，不会把共享库复制进项目）。
- **容器不挂载账号数据目录**，只挂 `inputs/ outputs/ previous/ share/`；需要长期保留的中间产物放 `outputs/`。
- 目录不会自动更新：管理员新增或调整共享数据后重新运行 `node dsh/tools/catalog-share.mjs --write`。若 `CATALOG.md`
  与目录内容不一致，以 `geo_list_files` / `ls` 的实际结果为准，并如实说明。

## 一个具体例子（缅甸地理）

`share/缅甸地理/` 下的 `CATALOG.md` 写明了 44 个文件：`城市地理/*.xlsx`（城市位序规模 77 行、人口金字塔、WDI 面板）、
`第四章 人口 图表数据/`（省邦人口表 + 33 张书插图 `.tif` + `缅甸arcgis.rar` 里的乡镇边界与人口密度 shapefile）、
`缅甸交通/…/图1-3.mdb`（读到的图层：`pic1_road` 368,131 线、`pic2_railway`、`pic3_airport`、首都/省邦首府点、国界/省邦界/区县界）、
`缅甸地理.pdf`（379 页扫描件，需 OCR）。
