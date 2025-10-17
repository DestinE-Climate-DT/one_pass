"""Pytest configuration for one_pass tests, including MPI support."""

import pytest
import os
import sys
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
    # Check if we're in an MPI environment
    mpi_env_vars = ["OMPI_COMM_WORLD_SIZE", "PMI_SIZE", "MPI_LOCALNRANKS"]
    in_mpi_env = any(var in os.environ for var in mpi_env_vars)

    # Skip MPI tests if not in MPI environment
    if not in_mpi_env:
        skip_mpi = pytest.mark.skip(reason="Not running in MPI environment")
        for item in items:
            if "mpi" in item.keywords:
                item.add_marker(skip_mpi)
    else:
        # If we are in MPI environment, skip non-MPI tests to avoid conflicts
        skip_non_mpi = pytest.mark.skip(
            reason="Skipping non-MPI tests in MPI environment"
        )
        for item in items:
            if "mpi" not in item.keywords:
                item.add_marker(skip_non_mpi)

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

    # Additional MPI-specific setup
    if "mpi" in item.keywords:
        # Verify MPI environment is properly set up
        try:
            from one_pass.mpi import MPI_LAUNCHED, MPI_SIZE

            if MPI_LAUNCHED:
                # Check minimum size requirement
                min_size = 2  # default
                mpi_marker = item.get_closest_marker("mpi")
                if mpi_marker and mpi_marker.kwargs.get("min_size"):
                    min_size = mpi_marker.kwargs["min_size"]

                if MPI_SIZE < min_size:
                    pytest.skip(
                        f"MPI test requires at least {min_size} processes, got {MPI_SIZE}"
                    )
        except ImportError:
            pytest.skip("MPI support not available")


@pytest.fixture(scope="session")
def mpi_info():
    """Provide MPI information for tests."""
    try:
        from one_pass.mpi import MPI_LAUNCHED, MPI_RANK, MPI_SIZE

        return {"launched": MPI_LAUNCHED, "rank": MPI_RANK, "size": MPI_SIZE}
    except ImportError:
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
def assert_mpi_consistent(values, rtol=1e-12, atol=1e-12):
    """Assert that values from different MPI ranks are consistent."""
    try:
        from one_pass.mpi import MPI_COMM, MPI_RANK

        if MPI_COMM is not None:
            all_values = MPI_COMM.allgather(values)
            if MPI_RANK == 0:
                import numpy as np

                for i, other_values in enumerate(all_values[1:], 1):
                    np.testing.assert_allclose(
                        all_values[0],
                        other_values,
                        rtol=rtol,
                        atol=atol,
                        err_msg=f"Values differ between rank 0 and rank {i}",
                    )
    except ImportError:
        # Not in MPI environment, skip check
        pass


# Pytest plugin for better MPI test output
class MPITestPlugin:
    """Pytest plugin to handle MPI test output and reporting."""

    def pytest_runtest_logreport(self, report):
        """Customize test result reporting for MPI tests."""
        if hasattr(report, "keywords") and "mpi" in report.keywords:
            try:
                from one_pass.mpi import MPI_RANK

                if MPI_RANK is not None and MPI_RANK != 0:
                    # Suppress output from non-root ranks to avoid clutter
                    report.sections = []
            except ImportError:
                pass


# Register the MPI test plugin in the existing pytest_configure function above


# Error handling for MPI-specific issues
def pytest_exception_interact(node, call, report):
    """Handle MPI-specific exceptions."""
    if hasattr(call, "excinfo") and call.excinfo:
        exc_info = call.excinfo
        exc_value = exc_info.value

        # Handle common MPI errors gracefully
        if "mpi" in str(exc_value).lower():
            # Add helpful information about MPI environment
            try:
                from one_pass.mpi import MPI_LAUNCHED, MPI_RANK, MPI_SIZE

                mpi_info = f"MPI Environment: launched={MPI_LAUNCHED}, rank={MPI_RANK}, size={MPI_SIZE}"
                if hasattr(report, "longrepr"):
                    report.longrepr = f"{report.longrepr}\n\n{mpi_info}"
            except ImportError:
                pass
