def build_gis_core_mcp():
    from .gis_core import build_gis_core_mcp as _build_gis_core_mcp

    return _build_gis_core_mcp()


def validate_environment():
    from .gis_core import validate_environment as _validate_environment

    return _validate_environment()


__all__ = ["build_gis_core_mcp", "validate_environment"]
