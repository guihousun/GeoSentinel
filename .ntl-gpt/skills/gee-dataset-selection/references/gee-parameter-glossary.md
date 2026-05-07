# GEE Parameter Glossary for 地缘环境智能计算平台

## Dataset Fields

- `dataset_id`: Earth Engine asset ID passed to `ee.ImageCollection()`, `ee.Image()`, or `ee.FeatureCollection()`.
- `asset_type`: one of `ImageCollection`, `Image`, or `FeatureCollection`.
- `band`: raster variable name selected by `.select(band)`.
- `scale_m`: nominal reduction/export scale in meters. Match the source product unless the method explicitly aggregates.
- `temporal_resolution`: `annual`, `monthly`, `daily`, `static`, or `event`.
- `temporal_coverage`: date range where the product is expected to contain observations. Live-check before execution.

## Date Parameters

- `start_date`: inclusive start date.
- `end_date_exclusive`: exclusive end date used in `filterDate(start, end)`.
- For a single daily image on `YYYY-MM-DD`, use `filterDate("YYYY-MM-DD", "YYYY-MM-DD+1")`.
- For a full year, use `filterDate("YYYY-01-01", "YYYY+1-01-01")`.

## Reduction Parameters

- `reducer`: statistic to compute, usually `ee.Reducer.mean()`, `sum()`, `median()`, `count()`, or combined reducers.
- `scale`: nominal pixel scale used by `reduceRegion(s)`.
- `maxPixels`: maximum pixels for one `reduceRegion`.
- `maxPixelsPerRegion`: maximum pixels per feature for `reduceRegions`.
- `tileScale`: aggregation tile scaling factor. Larger values use smaller tiles and can avoid memory failures; common values are `2`, `4`, or `8`.
- `bestEffort`: allows Earth Engine to use a coarser scale to make one-geometry reductions succeed. Use carefully and report it.
- `crs`: target projection. Prefer source projection or `EPSG:4326` only when method requires it; always keep scale explicit.

## Boundary Parameters

- `shapeGroup`: geoBoundaries country code, usually ISO3-like, e.g. `CHN`, `TWN`.
- `shapeName`: geoBoundaries administrative name.
- `ADM0_NAME`, `ADM1_NAME`, `ADM2_NAME`: GAUL administrative name fields.
- `name_property`: feature property copied into output tables.

## Output Parameters

- `output_mode`: `table`, `geotiff`, `asset_export`, or `drive_export`.
- `table`: preferred for statistics/ranking/time-series outputs.
- `geotiff`: use only when the user needs imagery or a small AOI raster.
- `getInfo`: acceptable after reduction to a compact table; not acceptable for full rasters or large collections.

