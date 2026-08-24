---
name: vnp46a2-official-h5-country-mosaic
description: Use when a user explicitly needs audited, country-scale, daily non-gap-filled VNP46A2 GeoTIFFs from official NASA Earthdata HDF5 granules.
---

# Official VNP46A2 HDF5 Country Mosaic

Use this workflow only for country-scale daily raster retrieval. It is not the
default route for statistics, rankings, or long time-series summaries; those
remain GEE server-side table workflows.

## Selection Rules

Choose `official_vnp46a2_h5_country_mosaic_tool` when all of the following are
true:

1. The request explicitly needs country-scale VNP46A2 raster files.
2. The requested band is the non-gap-filled `DNB_BRDF_Corrected_NTL` band.
3. Official NASA CMR/Earthdata HDF5 provenance is required or preferable to a
   GEE GeoTIFF export.
4. The caller accepts an audited country-day package, including HDF5 manifests
   and clipped GeoTIFFs.

Do not use it for a country statistic, administrative ranking, or a request
that only needs a table. Use GEE server-side reducers for those tasks.

## Run Contract

1. Record ISO3 countries, inclusive UTC product dates, requested band, and
   output directory before executing.
2. Call the tool with `execution_mode="plan"` first. Confirm the plan when it
   entails a large country/date matrix.
3. Before `execution_mode="run"`, ensure `EARTHDATA_TOKEN` is available in the
   environment or project `.env`; never display it. GEE authentication is also
   required unless `no_gee_latest=true` is justified by authoritative product
   availability metadata.
4. The run prepares OSM admin0 boundaries simplified at `0.001` degrees,
   downloads and validates official HDF5, mosaics and clips GeoTIFFs, then audits
   each country-day.
5. For network failures, retry only failed `ISO3:YYYY-MM-DD` targets with
   `workers=4` or lower and a larger timeout. Do not restart a complete matrix.
6. After a successful audit, use `phase="organize"` with the audited run
   directory to create the ISO3 final package. The organizer uses same-volume
   hard links by default and falls back to copying when required.

## Completion Gate

- `downloaded_without_mosaic` must be zero.
- `no_granules` means source unavailability, not a failed download.
- `mosaic_all_nodata` is terminal only if valid HDF5 exists, mosaic succeeded,
  and a pixel scan confirms no valid non-gap-filled pixel in the AOI.
- Return actual artifact paths and audit results. Nighttime-light changes remain
  candidate evidence, never proof of event damage or causality.
