from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path

import pytest


def _raster_module():
    spec = importlib.util.find_spec("ntl_toolkit.core.raster")
    assert spec is not None, "ntl_toolkit.core.raster should exist"
    return importlib.import_module("ntl_toolkit.core.raster")


def test_raster_module_exports_required_public_callables() -> None:
    module = _raster_module()

    for name in [
        "inspect_raster",
        "validate_geodata",
        "clip_raster",
        "reproject_raster",
        "mosaic_rasters",
    ]:
        assert hasattr(module, name), f"ntl_toolkit.core.raster missing {name}"


def test_clip_raster_writes_reopenable_single_pixel_clip(
    sample_raster_path: Path,
    clip_polygon_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "clip.tif"

    result = _raster_module().clip_raster(
        sample_raster_path,
        clip_polygon_path,
        output_path,
    )

    assert result.status == "succeeded"
    assert result.tool == "clip_raster"
    assert len(result.outputs) == 1
    assert result.outputs[0].path == str((runtime_workspace / output_path).resolve(strict=False))
    assert result.outputs[0].media_type == "image/tiff"
    assert result.metrics["width"] == 1
    assert result.metrics["height"] == 1
    assert result.metrics["band_count"] == 1
    assert result.metrics["crs"] == "EPSG:4326"

    import rasterio

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.width == 1
        assert dataset.height == 1
        assert dataset.count == 1
        assert dataset.crs == rasterio.CRS.from_epsg(4326)
        assert dataset.read(1).tolist() == [[1.0]]


def test_clip_raster_reprojects_vector_geometries_before_masking(
    sample_raster_path: Path,
    mercator_overlap_vector_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "clip_mercator.tif"

    result = _raster_module().clip_raster(
        sample_raster_path,
        mercator_overlap_vector_path,
        output_path,
    )

    assert result.status == "succeeded"
    with __import__("rasterio").open(runtime_workspace / output_path) as dataset:
        assert dataset.width == 2
        assert dataset.height == 2
        assert dataset.read(1).tolist() == [[1.0, 2.0], [3.0, -9999.0]]


@pytest.mark.parametrize(
    ("vector_fixture", "all_touched", "error_code"),
    [
        ("far_vector_path", False, "NO_SPATIAL_OVERLAP"),
        ("invalid_vector_path", False, "INVALID_GEOMETRY"),
        ("clip_polygon_path", "yes", "INVALID_PARAMETER"),
    ],
)
def test_clip_raster_reports_validation_failures(
    sample_raster_path: Path,
    vector_fixture: str,
    all_touched: object,
    error_code: str,
    request: pytest.FixtureRequest,
) -> None:
    vector_path = request.getfixturevalue(vector_fixture)

    result = _raster_module().clip_raster(
        sample_raster_path,
        vector_path,
        Path("outputs") / "clip_failure.tif",
        all_touched=all_touched,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == error_code


@pytest.mark.parametrize(
    ("raster_path", "vector_path"),
    [
        (Path("inputs") / "missing.tif", Path("inputs") / "clip.geojson"),
        (Path("inputs") / "sample.tif", Path("inputs") / "missing.geojson"),
    ],
)
def test_clip_raster_reports_missing_inputs_as_input_not_found(
    raster_path: Path,
    vector_path: Path,
    sample_raster_path: Path,
    clip_polygon_path: Path,
) -> None:
    del sample_raster_path, clip_polygon_path

    result = _raster_module().clip_raster(
        raster_path,
        vector_path,
        Path("outputs") / "clip_missing.tif",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INPUT_NOT_FOUND"


def test_clip_raster_rejects_empty_vector_dataset(
    sample_raster_path: Path,
    empty_vector_path: Path,
) -> None:
    result = _raster_module().clip_raster(
        sample_raster_path,
        empty_vector_path,
        Path("outputs") / "clip_empty.tif",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_GEOMETRY"
    assert result.error.details["reason"] == "empty_dataset"


def test_clip_raster_requires_vector_crs(
    sample_raster_path: Path,
    vector_without_crs_path: Path,
) -> None:
    result = _raster_module().clip_raster(
        sample_raster_path,
        vector_without_crs_path,
        Path("outputs") / "clip_no_crs.tif",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "CRS_MISSING"


def test_clip_raster_preserves_mask_semantics_without_source_nodata(
    nodata_less_uint8_raster_path: Path,
    triangular_clip_polygon_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "clip_triangle_masked.tif"

    result = _raster_module().clip_raster(
        nodata_less_uint8_raster_path,
        triangular_clip_polygon_path,
        output_path,
    )

    assert result.status == "succeeded"

    import rasterio

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.nodata is None
        masked = dataset.read(1, masked=True)
        assert masked.shape == (2, 2)
        assert masked[0, 0] == 0
        assert bool(masked.mask[0, 0]) is False
        assert bool(masked.mask[0, 1]) is True
        assert bool(masked.mask[1, 0]) is True
        assert bool(masked.mask[1, 1]) is True
        assert dataset.dataset_mask().tolist() == [[255, 0], [0, 0]]


def test_clip_raster_reserves_collision_suffix_without_overwriting(
    sample_raster_path: Path,
    clip_polygon_path: Path,
    runtime_workspace: Path,
) -> None:
    requested_path = runtime_workspace / "outputs" / "clip_existing.tif"
    requested_path.write_bytes(b"sentinel")

    result = _raster_module().clip_raster(
        sample_raster_path,
        clip_polygon_path,
        Path("outputs") / "clip_existing.tif",
    )

    assert result.status == "succeeded"
    assert requested_path.read_bytes() == b"sentinel"
    assert result.outputs[0].path.endswith("clip_existing_001.tif")
    assert Path(result.outputs[0].path).exists()


@pytest.mark.parametrize("resampling", ["nearest", "bilinear", "cubic", "average"])
def test_reproject_raster_supports_public_resampling_modes_and_preserves_profile(
    sample_raster_path: Path,
    runtime_workspace: Path,
    resampling: str,
) -> None:
    output_path = Path("outputs") / f"reproject_{resampling}.tif"

    result = _raster_module().reproject_raster(
        sample_raster_path,
        output_path,
        dst_crs="EPSG:4326",
        resampling=resampling,
    )

    assert result.status == "succeeded"
    assert result.metrics["resampling"] == resampling

    import rasterio

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.crs == rasterio.CRS.from_epsg(4326)
        assert dataset.dtypes == ("float32",)
        assert dataset.nodata == pytest.approx(-9999.0)


def test_reproject_raster_writes_epsg3857_output_for_each_band_and_preserves_dtype_nodata(
    multiband_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "reprojected.tif"

    result = _raster_module().reproject_raster(
        multiband_raster_path,
        output_path,
        dst_crs="EPSG:3857",
    )

    assert result.status == "succeeded"
    assert result.tool == "reproject_raster"
    assert len(result.outputs) == 1
    assert result.outputs[0].path == str((runtime_workspace / output_path).resolve(strict=False))
    assert result.outputs[0].media_type == "image/tiff"
    assert result.metrics["band_count"] == 2
    assert result.metrics["crs"] == "EPSG:3857"
    assert result.metrics["resampling"] == "bilinear"

    import rasterio

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.count == 2
        assert dataset.crs == rasterio.CRS.from_epsg(3857)
        assert dataset.width > 0
        assert dataset.height > 0
        assert dataset.dtypes == ("float32", "float32")
        assert dataset.nodata == pytest.approx(-9999.0)
        assert dataset.read(1).shape == (dataset.height, dataset.width)
        assert dataset.read(2).shape == (dataset.height, dataset.width)


def test_reproject_raster_supports_same_crs_and_collision_suffix(
    sample_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    requested_path = runtime_workspace / "outputs" / "reproject_same.tif"
    requested_path.write_bytes(b"sentinel")

    result = _raster_module().reproject_raster(
        sample_raster_path,
        Path("outputs") / "reproject_same.tif",
        dst_crs="EPSG:4326",
        resampling="nearest",
    )

    assert result.status == "succeeded"
    assert requested_path.read_bytes() == b"sentinel"
    assert result.outputs[0].path.endswith("reproject_same_001.tif")

    import rasterio

    with rasterio.open(Path(result.outputs[0].path)) as dataset:
        assert dataset.crs == rasterio.CRS.from_epsg(4326)
        assert dataset.read(1).tolist() == [[1.0, 2.0], [3.0, -9999.0]]


@pytest.mark.parametrize(
    ("kwargs", "error_code"),
    [
        ({"dst_crs": "EPSG:3857", "resampling": "lanczos"}, "UNSUPPORTED_RESAMPLING"),
        ({"dst_crs": "not-a-crs"}, "INVALID_PARAMETER"),
    ],
)
def test_reproject_raster_rejects_invalid_parameters(
    sample_raster_path: Path,
    kwargs: dict[str, object],
    error_code: str,
) -> None:
    result = _raster_module().reproject_raster(
        sample_raster_path,
        Path("outputs") / "reproject_invalid.tif",
        **kwargs,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == error_code


def test_reproject_raster_requires_source_crs(
    raster_without_crs_path: Path,
) -> None:
    result = _raster_module().reproject_raster(
        raster_without_crs_path,
        Path("outputs") / "reproject_missing_crs.tif",
        dst_crs="EPSG:3857",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "CRS_MISSING"


@pytest.mark.parametrize(
    ("tool_name", "kwargs"),
    [
        (
            "clip_raster",
            {
                "raster_path": "C:bad-input.tif",
                "vector_path": Path("inputs") / "clip.geojson",
                "output_path": Path("outputs") / "clip_invalid_input.tif",
            },
        ),
        (
            "reproject_raster",
            {
                "raster_path": "C:bad-input.tif",
                "output_path": Path("outputs") / "reproject_invalid_input.tif",
                "dst_crs": "EPSG:3857",
            },
        ),
        (
            "mosaic_rasters",
            {
                "raster_paths": ["C:bad-input.tif"],
                "output_path": Path("outputs") / "mosaic_invalid_input.tif",
            },
        ),
    ],
)
def test_raster_transforms_reject_partially_qualified_input_paths(
    tool_name: str,
    kwargs: dict[str, object],
    sample_raster_path: Path,
    clip_polygon_path: Path,
    runtime_workspace: Path,
) -> None:
    del sample_raster_path, clip_polygon_path
    tool = getattr(_raster_module(), tool_name)

    result = tool(**kwargs)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_PARAMETER"
    assert "path" in result.error.details
    assert list((runtime_workspace / "outputs").glob("*.tif")) == []


@pytest.mark.parametrize(
    ("tool_name", "kwargs"),
    [
        (
            "clip_raster",
            {
                "raster_path": Path("inputs") / "sample.tif",
                "vector_path": Path("inputs") / "clip.geojson",
                "output_path": "C:bad-output.tif",
            },
        ),
        (
            "reproject_raster",
            {
                "raster_path": Path("inputs") / "sample.tif",
                "output_path": "C:bad-output.tif",
                "dst_crs": "EPSG:3857",
            },
        ),
        (
            "mosaic_rasters",
            {
                "raster_paths": [Path("inputs") / "sample.tif"],
                "output_path": "C:bad-output.tif",
            },
        ),
    ],
)
def test_raster_transforms_reject_partially_qualified_output_paths(
    tool_name: str,
    kwargs: dict[str, object],
    sample_raster_path: Path,
    clip_polygon_path: Path,
    runtime_workspace: Path,
) -> None:
    del sample_raster_path, clip_polygon_path
    tool = getattr(_raster_module(), tool_name)

    result = tool(**kwargs)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_PARAMETER"
    assert "path" in result.error.details
    assert list((runtime_workspace / "outputs").glob("*.tif")) == []


def test_mosaic_rasters_preserves_adjacent_union_extent(
    adjacent_left_raster_path: Path,
    adjacent_right_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "mosaic_adjacent.tif"

    result = _raster_module().mosaic_rasters(
        [adjacent_left_raster_path, adjacent_right_raster_path],
        output_path,
    )

    assert result.status == "succeeded"
    assert result.tool == "mosaic_rasters"
    assert len(result.outputs) == 1
    assert result.outputs[0].path == str((runtime_workspace / output_path).resolve(strict=False))
    assert result.outputs[0].media_type == "image/tiff"
    assert result.metrics["width"] == 4
    assert result.metrics["height"] == 2
    assert result.metrics["band_count"] == 1
    assert result.metrics["crs"] == "EPSG:4326"
    assert result.metrics["method"] == "first"

    import rasterio

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.width == 4
        assert dataset.height == 2
        assert dataset.read(1).tolist() == [[1.0, 2.0, 5.0, 6.0], [3.0, 4.0, 7.0, 8.0]]


def test_mosaic_rasters_rejects_half_pixel_grid_offset(
    sample_raster_path: Path,
    half_pixel_offset_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "mosaic_half_pixel.tif"

    result = _raster_module().mosaic_rasters(
        [sample_raster_path, half_pixel_offset_raster_path],
        output_path,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "GRID_MISMATCH"
    assert not (runtime_workspace / output_path).exists()


@pytest.mark.parametrize(
    ("paths_factory", "method", "error_code"),
    [
        (lambda request: [], "first", "INVALID_PARAMETER"),
        (
            lambda request: [
                request.getfixturevalue("sample_raster_path"),
                request.getfixturevalue("band_mismatch_raster_path"),
            ],
            "first",
            "BAND_COUNT_MISMATCH",
        ),
        (
            lambda request: [
                request.getfixturevalue("sample_raster_path"),
                request.getfixturevalue("crs_mismatch_raster_path"),
            ],
            "first",
            "CRS_MISMATCH",
        ),
        (
            lambda request: [request.getfixturevalue("sample_raster_path")],
            "meanish",
            "UNSUPPORTED_METHOD",
        ),
    ],
)
def test_mosaic_rasters_rejects_invalid_inputs(
    method: str,
    error_code: str,
    paths_factory,
    request: pytest.FixtureRequest,
) -> None:
    raster_paths = paths_factory(request)
    original_paths = list(raster_paths)

    result = _raster_module().mosaic_rasters(
        raster_paths,
        Path("outputs") / "mosaic_invalid.tif",
        method=method,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == error_code
    assert raster_paths == original_paths


def test_mosaic_rasters_mean_handles_overlap_and_nodata_exactly(
    overlapping_mean_left_raster_path: Path,
    overlapping_mean_right_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "mosaic_mean.tif"

    result = _raster_module().mosaic_rasters(
        [overlapping_mean_left_raster_path, overlapping_mean_right_raster_path],
        output_path,
        method="mean",
    )

    assert result.status == "succeeded"
    assert result.metrics["width"] == 3
    assert result.metrics["height"] == 2
    assert result.metrics["band_count"] == 2
    assert result.metrics["method"] == "mean"

    import rasterio

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.count == 2
        assert dataset.read(1).tolist() == [
            [1.0, 51.0, 200.0],
            [3.0, 300.0, -9999.0],
        ]
        assert dataset.read(2).tolist() == [
            [10.0, 510.0, 2000.0],
            [30.0, 3000.0, -9999.0],
        ]


def test_mosaic_rasters_mean_promotes_integer_inputs_to_float_for_fractional_averages(
    uint8_mean_left_raster_path: Path,
    uint8_mean_right_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "mosaic_uint8_mean.tif"

    result = _raster_module().mosaic_rasters(
        [uint8_mean_left_raster_path, uint8_mean_right_raster_path],
        output_path,
        method="mean",
    )

    assert result.status == "succeeded"

    import rasterio

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.dtypes == ("float32",)
        assert dataset.read(1).tolist() == [[1.5]]


def test_mosaic_rasters_reserves_collision_suffix_without_mutating_inputs(
    adjacent_left_raster_path: Path,
    adjacent_right_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    requested_path = runtime_workspace / "outputs" / "mosaic_existing.tif"
    requested_path.write_bytes(b"sentinel")
    raster_paths = [adjacent_left_raster_path, adjacent_right_raster_path]

    result = _raster_module().mosaic_rasters(
        raster_paths,
        Path("outputs") / "mosaic_existing.tif",
    )

    assert result.status == "succeeded"
    assert requested_path.read_bytes() == b"sentinel"
    assert raster_paths == [adjacent_left_raster_path, adjacent_right_raster_path]
    assert result.outputs[0].path.endswith("mosaic_existing_001.tif")


@pytest.mark.parametrize(
    ("raster_paths", "error_code", "output_name"),
    [
        (["sample_raster_path", "corrupt_raster_path"], "RASTER_READ_FAILED", "mosaic_corrupt.tif"),
        (["raster_without_crs_path", "sample_raster_path"], "CRS_MISSING", "mosaic_no_crs.tif"),
    ],
)
def test_mosaic_rasters_reports_member_read_failures_without_leaving_output(
    raster_paths: list[str],
    error_code: str,
    output_name: str,
    runtime_workspace: Path,
    request: pytest.FixtureRequest,
) -> None:
    resolved_paths = [request.getfixturevalue(name) for name in raster_paths]
    output_path = Path("outputs") / output_name

    result = _raster_module().mosaic_rasters(
        resolved_paths,
        output_path,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == error_code
    assert not (runtime_workspace / output_path).exists()


def test_reproject_raster_cleans_partial_output_on_write_failure(
    sample_raster_path: Path,
    runtime_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _raster_module()
    real_open = module.rasterio.open

    def failing_open(path, mode="r", *args, **kwargs):
        if mode == "w":
            Path(path).touch()
            raise module.RasterioError("simulated write failure")
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(module.rasterio, "open", failing_open)

    result = module.reproject_raster(
        sample_raster_path,
        Path("outputs") / "reproject_partial.tif",
        dst_crs="EPSG:3857",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "RASTER_READ_FAILED"
    assert not (runtime_workspace / "outputs" / "reproject_partial.tif").exists()


def test_inspect_raster_full_mode_reports_masked_stats_for_fixture(
    sample_raster_path: Path,
) -> None:
    result = _raster_module().inspect_raster(sample_raster_path)

    assert result.status == "succeeded"
    assert result.tool == "inspect_raster"
    assert result.metrics["path"] == str(sample_raster_path.resolve(strict=False))
    assert result.metrics["driver"] == "GTiff"
    assert result.metrics["crs"] == "EPSG:4326"
    assert result.metrics["width"] == 2
    assert result.metrics["height"] == 2
    assert result.metrics["band_count"] == 1
    assert result.metrics["dtype"] == "float32"
    assert result.metrics["resolution"] == [1.0, 1.0]
    assert result.metrics["nodata"] == pytest.approx(-9999.0)
    assert result.metrics["bounds"] == [0.0, 0.0, 2.0, 2.0]
    assert result.metrics["transform"] == [1.0, 0.0, 0.0, 0.0, -1.0, 2.0, 0.0, 0.0, 1.0]
    assert result.metrics["grid_signature"] == "EPSG:4326|2|2|1.0|1.0|1.0|0.0|0.0|0.0|-1.0|2.0|0.0|0.0|1.0"
    assert result.metrics["readable"] is True
    assert result.metrics["valid_count"] == 3
    assert result.metrics["min"] == pytest.approx(1.0)
    assert result.metrics["max"] == pytest.approx(3.0)
    assert result.metrics["mean"] == pytest.approx(2.0)
    assert result.metrics["std"] == pytest.approx(0.81649658)
    assert result.metrics["hints"] == []


def test_inspect_raster_basic_mode_omits_expensive_stats(
    sample_raster_path: Path,
) -> None:
    result = _raster_module().inspect_raster(sample_raster_path, mode="basic")

    assert result.status == "succeeded"
    assert "valid_count" not in result.metrics
    assert "min" not in result.metrics
    assert "max" not in result.metrics
    assert "mean" not in result.metrics
    assert "std" not in result.metrics
    assert "hints" not in result.metrics


def test_inspect_raster_sampling_is_deterministic_and_nodata_safe(
    sample_raster_path: Path,
) -> None:
    first = _raster_module().inspect_raster(sample_raster_path, sample_pixels=1)
    second = _raster_module().inspect_raster(sample_raster_path, sample_pixels=1)

    assert first.status == "succeeded"
    assert second.status == "succeeded"
    assert first.metrics["sample_pixels"] == 1
    assert first.metrics["valid_count"] == 1
    assert first.metrics["mean"] == pytest.approx(1.0)
    assert first.metrics["std"] == pytest.approx(0.0)
    assert first.metrics["valid_count"] == second.metrics["valid_count"]
    assert first.metrics["mean"] == second.metrics["mean"]
    assert first.metrics["std"] == second.metrics["std"]


@pytest.mark.parametrize(
    ("kwargs", "details"),
    [
        ({"mode": "banana"}, {"parameter": "mode", "value": "banana"}),
        ({"sample_pixels": -1}, {"parameter": "sample_pixels", "value": -1}),
        ({"sample_pixels": 1.5}, {"parameter": "sample_pixels", "value": 1.5}),
    ],
)
def test_inspect_raster_rejects_invalid_parameters(
    sample_raster_path: Path,
    kwargs: dict[str, object],
    details: dict[str, object],
) -> None:
    result = _raster_module().inspect_raster(sample_raster_path, **kwargs)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_PARAMETER"
    assert result.error.details == details


def test_inspect_raster_reports_missing_and_corrupt_inputs(
    corrupt_raster_path: Path,
) -> None:
    missing = _raster_module().inspect_raster(Path("inputs") / "missing.tif")
    corrupt = _raster_module().inspect_raster(corrupt_raster_path)

    assert missing.status == "failed"
    assert missing.error is not None
    assert missing.error.code == "INPUT_NOT_FOUND"
    assert missing.error.details["path"].endswith("inputs\\missing.tif")

    assert corrupt.status == "failed"
    assert corrupt.error is not None
    assert corrupt.error.code == "RASTER_READ_FAILED"
    assert corrupt.error.details["path"] == str(corrupt_raster_path.resolve(strict=False))


def test_validate_geodata_flags_shifted_raster_grid_mismatch(
    sample_raster_path: Path,
    shifted_raster_path: Path,
) -> None:
    result = _raster_module().validate_geodata(
        raster_paths=[sample_raster_path, shifted_raster_path]
    )

    assert result.status == "succeeded"
    assert "GRID_MISMATCH" in result.warnings
    assert result.metrics["raster_count"] == 2
    pair_report = result.metrics["raster_pair_reports"][0]
    assert pair_report["left_path"] == str(sample_raster_path.resolve(strict=False))
    assert pair_report["right_path"] == str(shifted_raster_path.resolve(strict=False))
    assert pair_report["grid_compatible"] is False
    assert "GRID_MISMATCH" in pair_report["warning_codes"]


def test_validate_geodata_does_not_flag_grid_mismatch_for_matching_rasters(
    sample_raster_path: Path,
    matching_raster_path: Path,
) -> None:
    result = _raster_module().validate_geodata(
        raster_paths=[sample_raster_path, matching_raster_path]
    )

    assert result.status == "succeeded"
    assert "GRID_MISMATCH" not in result.warnings
    assert result.metrics["raster_pair_reports"][0]["grid_compatible"] is True


def test_validate_geodata_tolerates_harmless_transform_noise(
    sample_raster_path: Path,
    noisy_transform_raster_path: Path,
) -> None:
    result = _raster_module().validate_geodata(
        raster_paths=[sample_raster_path, noisy_transform_raster_path]
    )

    assert result.status == "succeeded"
    assert "GRID_MISMATCH" not in result.warnings
    assert result.metrics["raster_pair_reports"][0]["grid_compatible"] is True


def test_validate_geodata_reports_vector_crs_mismatch_and_no_overlap(
    sample_raster_path: Path,
    mercator_overlap_vector_path: Path,
    far_vector_path: Path,
) -> None:
    result = _raster_module().validate_geodata(
        raster_paths=[sample_raster_path],
        vector_paths=[mercator_overlap_vector_path, far_vector_path],
    )

    assert result.status == "succeeded"
    assert "CRS_MISMATCH" in result.warnings
    assert "NO_BBOX_INTERSECTION" in result.warnings
    comparisons = result.metrics["raster_vector_reports"]
    mercator_report = next(
        report
        for report in comparisons
        if report["vector_path"] == str(mercator_overlap_vector_path.resolve(strict=False))
    )
    far_report = next(
        report
        for report in comparisons
        if report["vector_path"] == str(far_vector_path.resolve(strict=False))
    )
    assert mercator_report["crs_match"] is False
    assert mercator_report["bbox_intersects"] is True
    assert "CRS_MISMATCH" in mercator_report["warning_codes"]
    assert far_report["crs_match"] is True
    assert far_report["bbox_intersects"] is False
    assert "NO_BBOX_INTERSECTION" in far_report["warning_codes"]


def test_validate_geodata_propagates_invalid_geometry_without_claiming_bbox_overlap(
    sample_raster_path: Path,
    invalid_vector_path: Path,
) -> None:
    result = _raster_module().validate_geodata(
        raster_paths=[sample_raster_path],
        vector_paths=[invalid_vector_path],
    )

    assert result.status == "succeeded"
    assert "INVALID_GEOMETRY" in result.warnings
    vector_report = result.metrics["vector_reports"][0]
    pair_report = result.metrics["raster_vector_reports"][0]
    assert vector_report["invalid_geometry"] is True
    assert vector_report["bounds"] is None
    assert "INVALID_GEOMETRY" in vector_report["warning_codes"]
    assert pair_report["bbox_intersects"] is None
    assert "INVALID_GEOMETRY" in pair_report["warning_codes"]
    assert "NO_BBOX_INTERSECTION" not in pair_report["warning_codes"]


def test_validate_geodata_reports_empty_invalid_and_unreadable_inputs(
    empty_vector_path: Path,
    invalid_vector_path: Path,
    corrupt_vector_path: Path,
) -> None:
    missing_path = Path("inputs") / "missing.geojson"
    result = _raster_module().validate_geodata(
        vector_paths=[empty_vector_path, invalid_vector_path, corrupt_vector_path, missing_path]
    )

    assert result.status == "succeeded"
    assert "EMPTY_DATASET" in result.warnings
    assert "INVALID_GEOMETRY" in result.warnings
    assert "UNREADABLE" in result.warnings
    reports = {
        report["requested_path"]: report for report in result.metrics["vector_reports"]
    }
    assert reports[str(empty_vector_path)]["empty_dataset"] is True
    assert "EMPTY_DATASET" in reports[str(empty_vector_path)]["warning_codes"]
    assert reports[str(invalid_vector_path)]["invalid_geometry"] is True
    assert "INVALID_GEOMETRY" in reports[str(invalid_vector_path)]["warning_codes"]
    assert reports[str(corrupt_vector_path)]["readable"] is False
    assert "UNREADABLE" in reports[str(corrupt_vector_path)]["warning_codes"]
    assert reports[str(missing_path)]["exists"] is False
    assert "UNREADABLE" in reports[str(missing_path)]["warning_codes"]


def test_validate_geodata_does_not_mutate_caller_input_lists(
    sample_raster_path: Path,
    shifted_raster_path: Path,
    far_vector_path: Path,
) -> None:
    raster_paths = [sample_raster_path, shifted_raster_path]
    vector_paths = [far_vector_path]

    result = _raster_module().validate_geodata(
        raster_paths=raster_paths,
        vector_paths=vector_paths,
    )

    assert result.status == "succeeded"
    assert raster_paths == [sample_raster_path, shifted_raster_path]
    assert vector_paths == [far_vector_path]
