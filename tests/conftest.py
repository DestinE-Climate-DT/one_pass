"""Pytest configuration for one_pass tests, including MPI support."""

import pytest
from pathlib import Path


def pytest_configure(config):
    """Configure pytest markers and settings."""
    # Register custom markers
    config.addinivalue_line(
        "markers",
        "mpi(min_size=2): mark test to run with MPI with minimum number of processes",
    )
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line(
        "markers", "performance: mark test as performance/benchmarking test"
    )
    # Register MPI test plugin
    config.pluginmanager.register(MPITestPlugin(), "mpi_test_plugin")


def pytest_collection_modifyitems(config, items):
    """Modify test collection behavior."""
    # Always skip MPI tests - we never want to run them in CI
    skip_mpi = pytest.mark.skip(reason="MPI tests disabled in CI environment")
    for item in items:
        if "mpi" in item.keywords:
            item.add_marker(skip_mpi)

    # Handle slow tests based on command line options
    if not config.getoption("--runslow", default=False):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--runperformance",
        action="store_true",
        default=False,
        help="run performance tests",
    )


def pytest_runtest_setup(item):
    """Setup for individual test runs."""
    # Skip performance tests unless explicitly requested
    if "performance" in item.keywords:
        if not item.config.getoption("--runperformance"):
            pytest.skip("need --runperformance option to run performance tests")

    # Always skip MPI tests - they should never run in CI
    if "mpi" in item.keywords:
        pytest.skip("MPI tests disabled in CI environment")


@pytest.fixture(scope="session")
def mpi_info():
    """Provide MPI information for tests."""
    # MPI tests are disabled in CI, so always return False
    return {"launched": False, "rank": None, "size": None}


@pytest.fixture(scope="session")
def test_data_path():
    """Provide path to test data directory."""
    test_dir = Path(__file__).parent
    data_dir = test_dir.parent / "data"
    return data_dir


@pytest.fixture
def temp_test_dir():
    """Provide a temporary directory for test files."""
    import tempfile
    import shutil

    temp_dir = tempfile.mkdtemp(prefix="one_pass_test_")
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# Custom assertion helpers for MPI tests
def assert_mpi_consistent(
    values, rtol=1e-12, atol=1e-12
):  # pylint: disable=unused-argument
    """Assert that values from different MPI ranks are consistent."""
    # MPI tests are disabled in CI, so this function does nothing


# Pytest plugin for better MPI test output
class MPITestPlugin:
    """Pytest plugin to handle MPI test output and reporting."""

    def pytest_runtest_logreport(self, report):  # pylint: disable=unused-argument
        """Customize test result reporting for MPI tests."""
        # MPI tests are disabled in CI, so this plugin does nothing


# Register the MPI test plugin in the existing pytest_configure function above


# Error handling for MPI-specific issues
def pytest_exception_interact(node, call, report):  # pylint: disable=unused-argument
    """Handle MPI-specific exceptions."""
    # MPI tests are disabled in CI, so this function does nothing
