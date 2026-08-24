from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from ntl_toolkit.core import gee_download
from ntl_toolkit.core.gee_download import (
    GeeDownloadRequest,
    download_gee_raster,
    validate_gee_request,
)


class _FakeImage:
    def __init__(self) -> None:
        self.selected: object = None
        self.renamed: str | None = None

    def select(self, band: object) -> "_FakeImage":
        self.selected = band
        return self

    def normalizedDifference(self, bands: list[str]) -> "_FakeImage":
        self.selected = bands
        return self

    def rename(self, name: str) -> "_FakeImage":
        self.renamed = name
        return self

    def mean(self) -> "_FakeImage":
        return self

    def median(self) -> "_FakeImage":
        return self

    def mosaic(self) -> "_FakeImage":
        return self


class _FakeCollection:
    def filterDate(self, _start: str, _end: str) -> "_FakeCollection":
        return self

    def filterBounds(self, _geometry: object) -> "_FakeCollection":
        return self

    def select(self, _band: str) -> "_FakeCollection":
        return self

    def mean(self) -> _FakeImage:
        return _FakeImage()

    def median(self) -> _FakeImage:
        return _FakeImage()

    def mosaic(self) -> _FakeImage:
        return _FakeImage()

    def first(self) -> _FakeImage:
        return _FakeImage()

    def size(self) -> "_FakeCollection":
        return self

    def getInfo(self) -> int:
        return 1


class _FakeGeometry:
    @staticmethod
    def Rectangle(bounds: list[float]) -> tuple[str, tuple[float, ...]]:
        return "rectangle", tuple(bounds)


class _FakeEe:
    Geometry = _FakeGeometry

    @staticmethod
    def ImageCollection(_dataset_id: str) -> _FakeCollection:
        return _FakeCollection()

    @staticmethod
    def Image(_dataset_id: str) -> _FakeImage:
        return _FakeImage()


def _valid_request(tmp_path: Path) -> GeeDownloadRequest:
    return GeeDownloadRequest(
        dataset_id="NASA/VIIRS/002/VNP46A2",
        band="Gap_Filled_DNB_BRDF_Corrected_NTL",
        start_date=date(2026, 4, 20),
        end_date=date(2026, 4, 21),
        bbox=(34.0, 29.0, 35.0, 30.0),
        output=str(tmp_path / "export.tif"),
        reducer="mean",
    )


def _write_valid_tif(_image: object, _request: GeeDownloadRequest, output: Path) -> Path:
    with rasterio.open(
        output,
        "w",
        driver="GTiff",
        height=1,
        width=1,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(34.0, 30.0, 1.0, 1.0),
    ) as dataset:
        dataset.write(np.array([[1.0]], dtype=np.float32), 1)
    return output


def test_gee_request_rejects_reverse_dates(tmp_path: Path) -> None:
    request = GeeDownloadRequest(
        dataset_id="NASA/VIIRS/002/VNP46A2",
        band="Gap_Filled_DNB_BRDF_Corrected_NTL",
        start_date=date(2026, 4, 21),
        end_date=date(2026, 4, 20),
        bbox=(34.0, 29.0, 35.0, 30.0),
        output=str(tmp_path / "out.tif"),
    )

    with pytest.raises(ValueError, match="end_date"):
        validate_gee_request(request)


def test_gee_request_rejects_non_wgs84_bbox(tmp_path: Path) -> None:
    request = _valid_request(tmp_path).model_copy(update={"bbox": (34.0, 91.0, 35.0, 92.0)})

    with pytest.raises(ValueError, match="latitude"):
        validate_gee_request(request)


def test_gee_export_reports_all_phases_and_reserves_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[tuple[float, float | None, str]] = []
    request = _valid_request(tmp_path)
    Path(request.output).write_bytes(b"already-exists")
    monkeypatch.setattr(gee_download, "_initialize_ee", lambda _project: _FakeEe())
    monkeypatch.setattr(gee_download, "_export_image", _write_valid_tif)

    result = download_gee_raster(
        request,
        progress=lambda current, total, message: events.append((current, total, message)),
    )

    assert result.status == "succeeded"
    assert Path(result.outputs[0].path).name == "export_001.tif"
    assert [event[2] for event in events] == [
        "initializing Earth Engine",
        "selecting imagery",
        "exporting GeoTIFF",
        "validating output",
        "completed",
    ]


def test_gee_initialization_failure_is_structured(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        gee_download,
        "_initialize_ee",
        lambda _project: (_ for _ in ()).throw(RuntimeError("credentials missing")),
    )

    result = download_gee_raster(_valid_request(tmp_path))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "GEE_NOT_INITIALIZED"
    assert "EasyGEE" in result.error.suggestion


def test_legacy_band_is_normalized_into_multiband_request(tmp_path: Path) -> None:
    request = _valid_request(tmp_path)

    assert request.bands == ["Gap_Filled_DNB_BRDF_Corrected_NTL"]


def test_static_image_download_does_not_require_dates(tmp_path: Path) -> None:
    request = GeeDownloadRequest(
        dataset_id="USGS/SRTMGL1_003",
        bands=["elevation"],
        bbox=(120.0, 30.0, 120.1, 30.1),
        output=str(tmp_path / "srtm.tif"),
        asset_type="Image",
        scale=30,
    )

    validate_gee_request(request)
    image = gee_download._materialize_image(_FakeEe(), request)

    assert isinstance(image, _FakeImage)
    assert image.selected == ["elevation"]


def test_normalized_difference_is_declarative(tmp_path: Path) -> None:
    request = GeeDownloadRequest(
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B8", "B4"],
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 5),
        bbox=(120.0, 30.0, 120.1, 30.1),
        output=str(tmp_path / "ndvi.tif"),
        asset_type="ImageCollection",
        reducer="median",
        processing_preset="normalized_difference",
        output_band_name="NDVI",
        scale=10,
    )

    image = gee_download._materialize_image(_FakeEe(), request)

    assert isinstance(image, _FakeImage)
    assert image.selected == ["B8", "B4"]
    assert image.renamed == "NDVI"


def test_combined_quality_and_index_preset_keeps_index_contract(tmp_path: Path) -> None:
    request = GeeDownloadRequest(
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B8", "B4"],
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 5),
        bbox=(120.0, 30.0, 120.1, 30.1),
        output=str(tmp_path / "ndvi_masked.tif"),
        asset_type="ImageCollection",
        reducer="median",
        processing_preset="sentinel2_cloud_score_plus_normalized_difference",
        output_band_name="NDVI",
        scale=10,
    )

    image = gee_download._apply_post_reduction_processing(_FakeImage(), request)

    assert request.index_bands == ("B8", "B4")
    assert image.selected == ["B8", "B4"]
    assert image.renamed == "NDVI"
