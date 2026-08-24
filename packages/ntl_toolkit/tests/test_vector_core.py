import importlib
import importlib.util
import warnings
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import MultiPoint, Polygon, box

from ntl_toolkit.core.vector import (
    buffer_points_aeqd,
    dissolve_intersections,
    filter_points_by_polygon,
    inspect_vector,
    spatial_join_points_to_admin,
)

def test_vector_module_exports_required_public_callables() -> None:
    spec = importlib.util.find_spec("ntl_toolkit.core.vector")

    assert spec is not None

    module = importlib.import_module("ntl_toolkit.core.vector")
    for name in [
        "inspect_vector",
        "filter_points_by_polygon",
        "spatial_join_points_to_admin",
        "buffer_points_aeqd",
        "dissolve_intersections",
    ]:
        assert hasattr(module, name), f"ntl_toolkit.core.vector missing {name}"


def test_inspect_vector_returns_metadata_for_polygons(
    admin_polygons_path: Path,
) -> None:
    result = inspect_vector(admin_polygons_path)

    assert result.status == "succeeded"
    assert result.metrics == {
        "path": str(admin_polygons_path.resolve(strict=False)),
        "feature_count": 2,
        "crs": "EPSG:4326",
        "geometry_types": ["Polygon"],
        "columns": ["shapeName", "iso3", "geometry"],
        "bounds": [0.0, 0.0, 2.0, 1.0],
    }


def test_filter_points_by_polygon_keeps_matching_points_and_writes_output(
    point_features_geojson_path: Path,
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "filtered.geojson"

    result = filter_points_by_polygon(
        point_features_geojson_path,
        admin_polygons_path,
        output_path,
    )

    assert result.status == "succeeded"
    assert result.metrics == {"feature_count": 2}
    written = Path(result.outputs[0].path)
    assert written == (runtime_workspace / output_path).resolve(strict=False)
    filtered = gpd.read_file(written)
    assert filtered["id"].tolist() == [1, 2]


def test_filter_points_by_polygon_accepts_supported_neighboring_predicate(
    point_features_geojson_path: Path,
    admin_polygons_path: Path,
) -> None:
    result = filter_points_by_polygon(
        point_features_geojson_path,
        admin_polygons_path,
        Path("outputs") / "intersects.geojson",
        predicate="intersects",
    )

    assert result.status == "succeeded"
    assert result.metrics == {"feature_count": 2}


def test_spatial_join_points_to_admin_retains_all_points_and_reports_match_counts(
    point_features_geojson_path: Path,
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    result = spatial_join_points_to_admin(
        point_features_geojson_path,
        admin_polygons_path,
        Path("outputs") / "joined.geojson",
    )

    assert result.status == "succeeded"
    assert result.metrics == {
        "feature_count": 3,
        "matched_count": 2,
        "unmatched_count": 1,
    }
    joined = gpd.read_file(Path(result.outputs[0].path))
    assert Path(result.outputs[0].path) == (
        runtime_workspace / "outputs" / "joined.geojson"
    ).resolve(strict=False)
    assert {"admin_name", "admin_iso3"}.issubset(joined.columns)
    assert joined["admin_name"].tolist()[:2] == ["west", "east"]
    assert joined["admin_iso3"].tolist()[:2] == ["TST", "TST"]
    assert joined["admin_name"].isna().tolist() == [False, False, True]
    assert joined["admin_iso3"].isna().tolist() == [False, False, True]


def test_buffer_points_aeqd_returns_wgs84_polygons_and_center_metrics(
    point_features_geojson_path: Path,
) -> None:
    result = buffer_points_aeqd(
        point_features_geojson_path,
        Path("outputs") / "buffers.geojson",
        radius_km=10,
    )

    assert result.status == "succeeded"
    assert result.metrics["feature_count"] == 3
    assert result.metrics["radius_km"] == pytest.approx(10.0)
    assert result.metrics["center_lon"] == pytest.approx((0.5 + 1.5 + 3.0) / 3.0)
    assert result.metrics["center_lat"] == pytest.approx((0.5 + 0.5 + 3.0) / 3.0)
    buffers = gpd.read_file(Path(result.outputs[0].path))
    assert str(buffers.crs) == "EPSG:4326"
    assert set(buffers.geometry.geom_type) == {"Polygon"}
    assert buffers.geometry.is_valid.all()


def test_dissolve_intersections_clusters_overlapping_buffers(
    point_features_geojson_path: Path,
) -> None:
    buffered = buffer_points_aeqd(
        point_features_geojson_path,
        Path("outputs") / "buffers_for_dissolve.geojson",
        radius_km=80,
    )
    assert buffered.status == "succeeded"

    result = dissolve_intersections(
        buffered.outputs[0].path,
        Path("outputs") / "dissolved.geojson",
    )

    assert result.status == "succeeded"
    assert result.metrics["cluster_count"] >= 1
    dissolved = gpd.read_file(Path(result.outputs[0].path))
    assert "cluster_id" in dissolved.columns
    assert "member_count" in dissolved.columns
    assert dissolved["member_count"].sort_values().tolist() == [1, 2]


def test_dissolve_intersections_merges_transitive_chain_into_one_cluster(
    runtime_workspace: Path,
) -> None:
    path = runtime_workspace / "inputs" / "transitive_chain.geojson"
    gpd.GeoDataFrame(
        {"name": ["a", "b", "c"]},
        geometry=[
            box(0.0, 0.0, 1.0, 1.0),
            box(0.8, 0.0, 1.8, 1.0),
            box(1.6, 0.0, 2.6, 1.0),
        ],
        crs="EPSG:4326",
    ).to_file(path, driver="GeoJSON")

    result = dissolve_intersections(
        path,
        Path("outputs") / "transitive_dissolved.geojson",
    )

    assert result.status == "succeeded"
    assert result.metrics["cluster_count"] == 1
    dissolved = gpd.read_file(Path(result.outputs[0].path))
    assert dissolved["cluster_id"].tolist() == [0]
    assert dissolved["member_count"].tolist() == [3]


def test_filter_points_by_polygon_accepts_csv_longitude_latitude_inputs(
    point_features_csv_path: Path,
    admin_polygons_path: Path,
) -> None:
    result = filter_points_by_polygon(
        point_features_csv_path,
        admin_polygons_path,
        Path("outputs") / "filtered_from_csv.geojson",
    )

    assert result.status == "succeeded"
    filtered = gpd.read_file(Path(result.outputs[0].path))
    assert filtered["id"].tolist() == [1, 2]


def test_output_collision_uses_001_without_overwriting_existing_output(
    point_features_geojson_path: Path,
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    existing = runtime_workspace / "outputs" / "collision.geojson"
    existing.write_text("taken", encoding="utf-8")

    result = filter_points_by_polygon(
        point_features_geojson_path,
        admin_polygons_path,
        Path("outputs") / "collision.geojson",
    )

    assert result.status == "succeeded"
    assert Path(result.outputs[0].path) == (
        runtime_workspace / "outputs" / "collision_001.geojson"
    ).resolve(strict=False)
    assert existing.read_text(encoding="utf-8") == "taken"


def test_missing_input_returns_input_not_found_and_leaves_no_partial_output(
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    requested = runtime_workspace / "outputs" / "missing.geojson"

    result = filter_points_by_polygon(
        Path("inputs") / "missing.geojson",
        admin_polygons_path,
        requested,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INPUT_NOT_FOUND"
    assert result.error.details["path"].endswith("inputs\\missing.geojson")
    assert not requested.exists()


def test_missing_crs_returns_crs_missing(runtime_workspace: Path) -> None:
    path = runtime_workspace / "inputs" / "missing_crs.gpkg"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        gpd.GeoDataFrame(
            {"name": ["no-crs"]},
            geometry=[box(0.0, 0.0, 1.0, 1.0)],
        ).to_file(path, driver="GPKG")

    result = inspect_vector(path)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "CRS_MISSING"


def test_invalid_or_empty_geometry_returns_invalid_geometry(
    runtime_workspace: Path,
) -> None:
    path = runtime_workspace / "inputs" / "invalid.geojson"
    gpd.GeoDataFrame(
        {"name": ["invalid"]},
        geometry=[Polygon()],
        crs="EPSG:4326",
    ).to_file(path, driver="GeoJSON")

    result = inspect_vector(path)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_GEOMETRY"


def test_unsupported_predicate_returns_stable_error(
    point_features_geojson_path: Path,
    admin_polygons_path: Path,
) -> None:
    result = filter_points_by_polygon(
        point_features_geojson_path,
        admin_polygons_path,
        Path("outputs") / "bad_predicate.geojson",
        predicate="banana",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "UNSUPPORTED_PREDICATE"
    assert result.error.details == {"predicate": "banana"}


def test_missing_required_columns_returns_column_not_found(
    point_features_csv_path: Path,
    admin_polygons_path: Path,
) -> None:
    result = filter_points_by_polygon(
        point_features_csv_path,
        admin_polygons_path,
        Path("outputs") / "missing_column.geojson",
        lon_col="lon",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "COLUMN_NOT_FOUND"
    assert result.error.details == {"column": "lon"}


def test_invalid_radius_returns_invalid_parameter_and_no_partial_output(
    point_features_geojson_path: Path,
    runtime_workspace: Path,
) -> None:
    requested = runtime_workspace / "outputs" / "bad_buffer.geojson"

    result = buffer_points_aeqd(
        point_features_geojson_path,
        requested,
        radius_km=0,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_PARAMETER"
    assert result.error.details == {"parameter": "radius_km", "value": 0.0}
    assert not requested.exists()


@pytest.mark.parametrize("radius_value", ["abc", None])
def test_invalid_radius_coercion_returns_structured_failure_without_output(
    point_features_geojson_path: Path,
    runtime_workspace: Path,
    radius_value: object,
) -> None:
    requested = runtime_workspace / "outputs" / f"bad_buffer_{radius_value}.geojson"

    result = buffer_points_aeqd(
        point_features_geojson_path,
        requested,
        radius_km=radius_value,  # type: ignore[arg-type]
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_PARAMETER"
    assert result.error.details == {"parameter": "radius_km", "value": radius_value}
    assert result.error.suggestion is not None
    assert not requested.exists()


def test_buffer_points_aeqd_rejects_multipoint_inputs_without_creating_output(
    runtime_workspace: Path,
) -> None:
    points_path = runtime_workspace / "inputs" / "multipoint.geojson"
    requested = runtime_workspace / "outputs" / "multipoint_buffers.geojson"
    gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[MultiPoint([(0.5, 0.5), (0.6, 0.6)])],
        crs="EPSG:4326",
    ).to_file(points_path, driver="GeoJSON")

    result = buffer_points_aeqd(
        points_path,
        requested,
        radius_km=10,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_GEOMETRY"
    assert not requested.exists()
