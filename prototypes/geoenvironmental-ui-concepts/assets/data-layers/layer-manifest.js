window.FOOD_LOGISTICS_LAYER_MANIFEST = {
  "version": 1,
  "kind": "food-logistics-raster-preview",
  "layers": [
    {
      "key": "food",
      "label": "粮食生产强度",
      "resolution": "约 10 km",
      "description": "SPAM 2020 粮食生产强度。高值单元用于识别潜在供给腹地。",
      "preview_note": "真实栅格轻量预览，保持原始 SPAM 网格。",
      "url": "assets/data-layers/food-production-intensity.png",
      "coordinates": [
        [
          92.33333333,
          24.33333333
        ],
        [
          94.41666667,
          24.33333333
        ],
        [
          94.41666667,
          20.41666667
        ],
        [
          92.33333333,
          20.41666667
        ]
      ],
      "map_resampling": "nearest",
      "source_file": "spam2020_v1r0_global_P_mg_A.tif"
    },
    {
      "key": "settlement",
      "label": "居民点分布",
      "resolution": "约 30 m",
      "description": "居民点栅格，用于表征末端人口与聚落空间。",
      "preview_note": "真实已占用像元保留原位置，并作小范围可视化扩展；不表示聚落面积。",
      "url": "assets/data-layers/settlement-distribution.png",
      "coordinates": [
        [
          92.75286276,
          23.95853792
        ],
        [
          94.07772763,
          23.95853792
        ],
        [
          94.07772763,
          21.06448386
        ],
        [
          92.75286276,
          21.06448386
        ]
      ],
      "map_resampling": "nearest",
      "source_file": "settlement_raster.tif"
    },
    {
      "key": "accessibility",
      "label": "可达性成本",
      "resolution": "约 30 m",
      "description": "可达性成本表面。低成本区域更容易连接到服务与转运节点。",
      "preview_note": "真实栅格轻量预览，按相对成本着色，不替代原始成本值分析。",
      "url": "assets/data-layers/accessibility-cost.png",
      "coordinates": [
        [
          92.75286276,
          23.95853792
        ],
        [
          94.07772763,
          23.95853792
        ],
        [
          94.07772763,
          21.06448386
        ],
        [
          92.75286276,
          21.06448386
        ]
      ],
      "map_resampling": "linear",
      "source_file": "accessibility.tif"
    }
  ]
};
