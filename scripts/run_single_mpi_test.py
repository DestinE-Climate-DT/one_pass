#!/usr/bin/env python
"""
Single MPI test runner for one_pass package.

This script runs a single test file in an MPI environment without the
complications of running pytest itself under MPI.
"""

import sys
import os
import subprocess
import tempfile


def run_mpi_test(test_file, num_procs=2, verbose=False):
    """Run a single MPI test file."""

    # Create a temporary Python script that imports and runs the test
    test_script = f"""
import sys
import os
import pytest

# Ensure the one_pass package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Set environment to indicate MPI environment for our tests
os.environ["PYTEST_MPI_TEST"] = "1"

if __name__ == "__main__":
    # Run pytest on the specific test file
    exit_code = pytest.main([
        "{test_file}",
        "-v" if {verbose} else "-q",
        "--tb=short",
        "-x"  # Stop on first failure
    ])
    sys.exit(exit_code)
"""

    # Write the test script to a temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(test_script)
        temp_script = f.name

    try:
        # Run the test script under MPI
        cmd = ["mpirun", "-n", str(num_procs), "python", temp_script]

        if verbose:
            print(f"Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, capture_output=not verbose, text=True)

        if not verbose and result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        return result.returncode == 0

    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_script)
        except OSError:
            pass


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print(
            "Usage: python run_single_mpi_test.py <test_file> [num_procs] [--verbose]"
        )
        sys.exit(1)

    test_file = sys.argv[1]
    num_procs = 2
    verbose = False

    # Parse arguments
    for arg in sys.argv[2:]:
        if arg == "--verbose":
            verbose = True
        elif arg.isdigit():
            num_procs = int(arg)

    # Check if test file exists
    if not os.path.exists(test_file):
        print(f"Error: Test file {test_file} not found")
        sys.exit(1)

    # Run the test
    success = run_mpi_test(test_file, num_procs, verbose)

    if success:
        print(f"✓ Test {test_file} passed with {num_procs} processes")
        sys.exit(0)
    else:
        print(f"✗ Test {test_file} failed with {num_procs} processes")
        sys.exit(1)


if __name__ == "__main__":
    main()
