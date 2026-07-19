"""Package smoke tests."""

from horus_engine import __version__


def test_package_version() -> None:
    """Expose the configured package version."""
    assert __version__ == "0.1.0"
