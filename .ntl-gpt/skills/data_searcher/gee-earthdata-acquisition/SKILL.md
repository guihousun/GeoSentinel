---
name: gee-earthdata-acquisition
description: Execute validated Google Earth Engine, Earthdata, LAADS, CMR, direct-download, or batch-export acquisition plans.
---

# GEE and Earthdata Acquisition

- Execute the accepted route: direct local export, server-side reduction, batch export, or audited official-product retrieval.
- The current run injects the Earth Engine runtime/billing project and uses one deployment credential source. Never request, guess, or override that billing project, never start interactive OAuth, and preserve classified configuration/transport/credential/access/quota failures. Full source/output asset IDs remain ordinary resource-contract fields.
- A generated server-side script blueprint is a plan unless a registered bounded host tool executes that exact workflow. Do not hand it to the Analyst child-process executor: that executor intentionally has no Earth Engine credentials or proxy context.
- For large AOIs, many features, or long series, prefer server-side reduction or batch export rather than downloading all source rasters.
- A submitted batch job is planned, not complete. Preserve job ID and manifest and verify final state before claiming completion.
- For official HDF retrieval preserve granule audit, mosaics, retry targets, and product provenance; distinguish no granules from transport failure.
- For official VNP46A1 requests that require `UTC_Time`, use the registered `official_vnp46a1_h5_tool` with a bounded WGS84 bbox or one ISO3 country and `include_utc_time=true`. Preserve the at-sensor radiance semantics; never substitute VNP46A2 or a GEE collection when the task requests the official HDF5 granule.
- Never expose credentials or claim an artifact before its actual path and non-empty content are validated.

