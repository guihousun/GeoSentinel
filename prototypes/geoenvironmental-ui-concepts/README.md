# 地缘环境智能计算平台 UI 原型

这是独立于 NTL-GPT Streamlit 运行时的桌面端静态原型，用于展示国际地缘政治研究工作台的空间态势、智能体协作和专题分析形态。粮食物流专题采用“地图是空间证据、专题问答是任务入口”的单一界面，而不是另一套独立控制台。

## 打开方式

直接打开 `index.html`，或在本目录启动本地静态服务：

```powershell
python -m http.server 4173 --bind 127.0.0.1
```

然后访问 `http://127.0.0.1:4173/`。粮食物流专题可直接访问 `http://127.0.0.1:4173/#logistics`。

## 说明文档

[UI 展示与使用说明](SHOWCASE_GUIDE.md) 是本原型的正式说明，包含：

- 每个界面区域和智能体角色的含义；
- 缅甸粮食物流与可达性专题的专题问答、空间图层、论文案例参考与展示占位边界；
- 今晚展示的建议操作路径；
- 后续接入主智能体、地图图层和专题工具的方式。

## 文件

| 文件 | 作用 |
| --- | --- |
| `index.html` | 页面结构与展示文案。 |
| `styles.css` | 桌面端视觉样式与布局。 |
| `app.js` | 视图切换、地图图层与节点交互，以及专题问答和主线程的共享状态。 |
| `SHOWCASE_GUIDE.md` | 面向展示和后续接入的说明文档。 |
| `assets/data-layers/` | 从现有 GeoTIFF 生成的浏览器轻量预览图层与图层清单；粮食生产、居民点和可达性默认叠加显示。 |
| `assets/boundaries/` | 缅甸一级行政区划底图。`myanmar-adm1.geojson` 来自 [geoBoundaries MMR ADM1](https://www.geoboundaries.org/api/current/gbOpen/MMR/ADM1/)，数据年份 2019、CC BY 4.0。 |
| `scripts/build_food_logistics_map_previews.py` | 将粮食生产、居民点和可达性 GeoTIFF 转为前端预览图层的可复现脚本。 |

## 更新专题图层

原始 GeoTIFF 不直接打包到浏览器。需要更新展示图层时，在已安装 `rasterio` 和 `Pillow` 的 Python 环境中运行：

```powershell
python scripts/build_food_logistics_map_previews.py --source-dir <已解压的栅格目录> --output-dir assets/data-layers
```

脚本会重新生成前端使用的 PNG 预览和 `layer-manifest.js`；专题地图默认定位缅甸全境，并同时显示粮食生产、居民点和可达性三类预览。正式接入时应替换为任务工作区中的真实栅格服务或切片。
