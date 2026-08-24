from importlib.metadata import version


def test_package_exposes_version() -> None:
    import ntl_toolkit

    assert version("ntl-toolkit") == "0.1.0"
    assert ntl_toolkit.__version__ == "0.1.0"
    assert version("ntl-toolkit") == ntl_toolkit.__version__
