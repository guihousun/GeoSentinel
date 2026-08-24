from __future__ import annotations

import json
from unittest.mock import Mock, patch

from shapely.geometry import GeometryCollection, LineString, MultiPolygon, Polygon

from tools.GaoDe_tool import (
    _extract_polygonal_geometry,
    _resolve_amap_admin_code,
    get_administrative_division_data,
)


def _response(payload: dict) -> Mock:
    response = Mock()
    response.json.return_value = payload
    response.text = ""
    response.status_code = 200
    return response


def test_english_admin_name_falls_back_to_amap_geocoding() -> None:
    district_response = _response({"status": "1", "districts": []})
    geocode_response = _response(
        {
            "status": "1",
            "geocodes": [
                {
                    "formatted_address": "上海市",
                    "province": "上海市",
                    "city": "上海市",
                    "district": [],
                    "adcode": "310000",
                }
            ],
        }
    )

    with patch("tools.GaoDe_tool.requests.get", side_effect=[district_response, geocode_response]) as mocked_get:
        adcode, name, error = _resolve_amap_admin_code("Shanghai", "test-key")

    assert (adcode, name, error) == ("310000", "上海市", "")
    assert mocked_get.call_count == 2


def test_chinese_admin_name_uses_district_result_without_fallback() -> None:
    district_response = _response(
        {"status": "1", "districts": [{"name": "上海市", "adcode": "310000"}]}
    )

    with patch("tools.GaoDe_tool.requests.get", return_value=district_response) as mocked_get:
        adcode, name, error = _resolve_amap_admin_code("上海市", "test-key")

    assert (adcode, name, error) == ("310000", "上海市", "")
    assert mocked_get.call_count == 1


def test_geometry_repair_keeps_polygon_parts_from_geometry_collection() -> None:
    first = Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
    second = Polygon([(2, 0), (3, 0), (3, 1), (2, 0)])
    line = LineString([(0, 0), (3, 1)])

    result = _extract_polygonal_geometry(
        GeometryCollection([first, MultiPolygon([second]), line])
    )

    assert result is not None
    assert result.geom_type == "MultiPolygon"
    assert len(result.geoms) == 2
    assert result.is_valid


def test_boundary_result_reports_child_feature_evidence(tmp_path) -> None:
    district_response = _response(
        {"status": "1", "districts": [{"name": "测试市", "adcode": "990100"}]}
    )
    boundary_response = _response(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "name": "甲区",
                        "adcode": 990101,
                        "level": "district",
                        "parent": {"adcode": 990100},
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[110, 30], [111, 30], [111, 31], [110, 30]]],
                    },
                },
                {
                    "type": "Feature",
                    "properties": {
                        "name": "乙区",
                        "adcode": 990102,
                        "level": "district",
                        "parent": {"adcode": 990100},
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[112, 30], [113, 30], [113, 31], [112, 30]]],
                    },
                },
            ],
        }
    )
    output_path = tmp_path / "test_city.shp"

    with (
        patch.dict("os.environ", {"amap_api_key": "test-key"}),
        patch(
            "tools.GaoDe_tool.requests.get",
            side_effect=[district_response, boundary_response],
        ),
        patch(
            "tools.GaoDe_tool.storage_manager.resolve_input_path",
            return_value=str(output_path),
        ),
    ):
        payload = json.loads(
            get_administrative_division_data("测试市", "test_city.shp")
        )

    assert payload["status"] == "success"
    assert payload["boundary_scope"] == "children"
    assert payload["feature_count"] == 2
    assert payload["name_field"] == "Name"
    assert payload["adcode_field"] == "AdCode"
    assert payload["attribute_fields"] == ["Name", "AdCode", "Level", "Parent"]
    assert payload["feature_levels"] == {"district": 2}
    assert payload["feature_names"] == ["甲区", "乙区"]
    assert payload["geometry_valid"] is True
    assert payload["primary_file"] == "inputs/test_city.shp"
