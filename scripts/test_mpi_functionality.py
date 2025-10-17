#!/usr/bin/env python
"""
MPI functionality test runner.

This script tests MPI functionality without using pytest under MPI,
which avoids the segmentation fault issues.
"""

import sys
import os
import subprocess
import tempfile
from pathlib import Path


def create_mpi_test_script():
    """Create a test script that can be run under MPI."""
    script_content = '''
import sys
import os

def test_mpi_infrastructure():
    """Test MPI infrastructure."""
    try:
        from one_pass.mpi import MPI_LAUNCHED, MPI_RANK, MPI_SIZE, MPI_get_indices

        if not MPI_LAUNCHED:
            print("ERROR: MPI not detected")
            return False

        print(f"Rank {MPI_RANK}/{MPI_SIZE}: MPI infrastructure OK")

        # Test MPI_get_indices
        start, end = MPI_get_indices(100)
        if start is None or end is None:
            print(f"Rank {MPI_RANK}: ERROR: MPI_get_indices returned None")
            return False

        print(f"Rank {MPI_RANK}: MPI_get_indices({100}) = {start}:{end}")

        # Verify no overlap across ranks
        from one_pass.mpi import MPI_COMM
        all_ranges = MPI_COMM.allgather((start, end))

        if MPI_RANK == 0:
            # Check for overlaps
            all_indices = set()
            for s, e in all_ranges:
                if s is not None and e is not None:
                    for i in range(s, e):
                        if i in all_indices:
                            print(f"ERROR: Index {i} assigned to multiple ranks")
                            return False
                        all_indices.add(i)
            print(f"Rank 0: Index distribution verified - {len(all_indices)} indices distributed")

        return True

    except Exception as e:
        print(f"Rank {MPI_RANK if 'MPI_RANK' in locals() else '?'}: ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_mpi_tdigest():
    """Test MPI TDigest functionality."""
    try:
        from one_pass.mpi import MPI_RANK, MPI_SIZE
        from one_pass.opa import Opa
        from one_pass.statistics.library.tdigest import initialise

        # Create OPA instance
        pass_dic = {
            "stat": "histogram",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": False,
            "bins": 10
        }

        opa = Opa(pass_dic)
        opa.data_set_info.size_data_source_tail = 20
        opa.data_set_info.shape_data_source_tail = (1, 4, 5)
        opa.request.compression = 1

        # Initialize TDigests
        tdigests = initialise(opa)

        if len(tdigests) != 20:
            print(f"Rank {MPI_RANK}: ERROR: Expected 20 TDigests, got {len(tdigests)}")
            return False

        # Count local TDigests (non-None)
        local_count = sum(1 for t in tdigests if t is not None)

        print(f"Rank {MPI_RANK}: TDigest initialization OK - {local_count}/20 local TDigests")

        return True

    except Exception as e:
        print(f"Rank {MPI_RANK if 'MPI_RANK' in locals() else '?'}: ERROR in TDigest test: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function."""
    print("=== MPI Functionality Tests ===")

    # Test 1: Infrastructure
    print("\\n--- Test 1: MPI Infrastructure ---")
    success1 = test_mpi_infrastructure()

    # Test 2: TDigest
    print("\\n--- Test 2: MPI TDigest ---")
    success2 = test_mpi_tdigest()

    # Summary
    print("\\n=== Test Summary ===")
    from one_pass.mpi import MPI_RANK
    if MPI_RANK == 0:
        if success1 and success2:
            print("✓ All MPI functionality tests PASSED")
            sys.exit(0)
        else:
            print("✗ Some MPI functionality tests FAILED")
            sys.exit(1)

if __name__ == "__main__":
    main()
'''
    return script_content


def run_mpi_tests_direct(verbose=False):
    """Run MPI functionality tests directly (assumes already under MPI)."""
    try:
        # Import MPI here to test if we're in MPI environment
        from one_pass.mpi import MPI_LAUNCHED, MPI_RANK, MPI_SIZE

        if verbose:
            print(f"Rank {MPI_RANK} of {MPI_SIZE}: MPI_LAUNCHED = {MPI_LAUNCHED}")

        if not MPI_LAUNCHED:
            print("ERROR: Not running in MPI environment")
            return False

        if MPI_SIZE < 2:
            print("ERROR: Need at least 2 MPI processes")
            return False

        # Simple test: each rank reports
        if MPI_RANK == 0:
            print(f"✓ MPI environment active with {MPI_SIZE} processes")

        # Test basic MPI functionality
        from one_pass.mpi import MPI_get_indices

        start, end = MPI_get_indices(100)

        if verbose:
            print(f"Rank {MPI_RANK}: indices {start} to {end}")

        # Only rank 0 reports success
        if MPI_RANK == 0:
            print("✓ Basic MPI functionality working")

        return True

    except ImportError as e:
        print(f"ERROR: Failed to import MPI modules: {e}")
        return False
    except Exception as e:
        print(f"ERROR: MPI test failed: {e}")
        return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Test MPI functionality")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    print("Testing MPI functionality (direct execution under MPI)...")

    success = run_mpi_tests_direct(args.verbose)

    if success:
        print("\\n✓ MPI functionality tests PASSED")
        sys.exit(0)
    else:
        print("\\n✗ MPI functionality tests FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
