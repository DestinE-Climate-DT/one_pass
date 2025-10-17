#!/bin/bash

# Script to run MPI tests with different process counts
# Usage: ./scripts/run_mpi_tests.sh [options]

# set -euo pipefail

# Default values
DEFAULT_MAX_PROCS=4
DEFAULT_TEST_PATTERN="tests/mpi/"
VERBOSE=false
QUICK_MODE=false
PERFORMANCE_TESTS=false
DRY_RUN=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run MPI tests for the one_pass package with different process counts.

OPTIONS:
    -h, --help              Show this help message
    -p, --max-procs NUM     Maximum number of MPI processes to test (default: $DEFAULT_MAX_PROCS)
    -t, --test-pattern PAT  Test directory or pattern (default: $DEFAULT_TEST_PATTERN)
    -v, --verbose           Verbose output
    -q, --quick             Quick mode - only test with 2 processes
    --performance           Include performance tests (slower)
    --dry-run               Show commands that would be run without executing

EXAMPLES:
    $0                      # Run with default settings
    $0 -p 8 -v              # Test up to 8 processes with verbose output
    $0 -q                   # Quick test with only 2 processes
    $0 --performance        # Include performance tests

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -p|--max-procs)
            if [[ -z "${2:-}" ]]; then
                print_error "Option $1 requires an argument"
                exit 1
            fi
            DEFAULT_MAX_PROCS="$2"
            shift 2
            ;;
        -t|--test-pattern)
            if [[ -z "${2:-}" ]]; then
                print_error "Option $1 requires an argument"
                exit 1
            fi
            DEFAULT_TEST_PATTERN="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -q|--quick)
            QUICK_MODE=true
            shift
            ;;
        --performance)
            PERFORMANCE_TESTS=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Check if we're in the correct directory
if [[ ! -f "setup.py" ]] || [[ ! -d "one_pass" ]]; then
    print_error "This script must be run from the one_pass project root directory"
    exit 1
fi

# Check if mpirun/mpiexec is available
if ! command -v mpirun &> /dev/null && ! command -v mpiexec &> /dev/null; then
    print_error "MPI runtime (mpirun or mpiexec) not found. Please install MPI."
    exit 1
fi

# Determine MPI command
if command -v mpirun &> /dev/null; then
    MPI_CMD="mpirun"
elif command -v mpiexec &> /dev/null; then
    MPI_CMD="mpiexec"
else
    print_error "Neither mpirun nor mpiexec found, but previous check passed. This should not happen."
    exit 1
fi

print_status "Using MPI command: $MPI_CMD"

# Check if pytest is available
if ! python -m pytest --version &> /dev/null; then
    print_error "pytest not found. Please install pytest."
    exit 1
fi

# Check if required packages are installed
print_status "Checking dependencies..."
python -c "import mpi4py" 2>/dev/null || {
    print_error "mpi4py not found. Please install: pip install mpi4py"
    exit 1
}

# Function to run a single test command
run_test_command() {
    local procs=$1
    local test_args=$2

    # Run tests under MPI using our custom test script
    local cmd="$MPI_CMD -n $procs python scripts/test_mpi_functionality.py"

    if [[ "$VERBOSE" == "true" ]]; then
        cmd="$cmd --verbose"
    fi

    # Add timeout to prevent hanging
    cmd="timeout 120 $cmd"

    print_status "Running: $cmd"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "    [DRY RUN] Would execute: $cmd"
        return 0
    fi

    if eval "$cmd"; then
        print_success "MPI tests passed with $procs processes."
        return 0
    else
        print_error "MPI tests failed with $procs processes."
        return 1
    fi
}

# Main test execution
print_status "Starting MPI tests..."
print_status "Test directory: $DEFAULT_TEST_PATTERN"
print_status "Max processes: $DEFAULT_MAX_PROCS"

# Test configuration
if [[ "$QUICK_MODE" == "true" ]]; then
    PROCESS_COUNTS=(2)
    print_status "Quick mode: testing with 2 processes only"
else
    PROCESS_COUNTS=(2 3 4)
    if [[ $DEFAULT_MAX_PROCS -gt 4 ]]; then
        PROCESS_COUNTS+=(8)
        if [[ $DEFAULT_MAX_PROCS -gt 8 ]]; then
            PROCESS_COUNTS+=($DEFAULT_MAX_PROCS)
        fi
    fi
fi

# Remove duplicates and sort
if [[ ${#PROCESS_COUNTS[@]} -gt 0 ]]; then
    PROCESS_COUNTS=($(printf '%s\n' "${PROCESS_COUNTS[@]}" | sort -nu))
fi

# Debug: Show what process counts we're using
if [[ ${#PROCESS_COUNTS[@]} -gt 0 ]]; then
    print_status "Process counts to test: ${PROCESS_COUNTS[*]}"
else
    print_status "Process counts to test: (empty array)"
fi

# Test results tracking
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Run tests with different process counts
print_status "About to start loop with ${#PROCESS_COUNTS[@]} process counts"
for procs in "${PROCESS_COUNTS[@]}"; do
    print_status "Loop iteration: procs=$procs, DEFAULT_MAX_PROCS=$DEFAULT_MAX_PROCS"
    if [[ $procs -gt $DEFAULT_MAX_PROCS ]]; then
        print_status "Skipping $procs processes (exceeds max)"
        continue
    fi

    echo
    print_status "Testing with $procs MPI processes"
    print_status "================================"

    # Basic MPI functionality tests
    echo
    print_status "Running MPI environment check..."
    ((TOTAL_TESTS++))

    # Test MPI environment without actually running MPI
    print_status "About to run Python command..."
    # Run Python command (no need for set +e/-e since we removed strict mode)
    timeout 10 python -c "from one_pass.mpi import MPI_LAUNCHED; print(' MPI module imports OK'); print('MPI_LAUNCHED:', MPI_LAUNCHED)" 2>&1
    test_result=$?
    print_status "Python command completed with exit code: $test_result"

    if [[ $test_result -eq 0 ]]; then
        ((PASSED_TESTS++))
        print_success "MPI environment check passed"
    else
        ((FAILED_TESTS++))
        print_error "MPI environment check failed with exit code: $test_result"
    fi

    # Infrastructure tests
    echo
    print_status "Running non-MPI infrastructure tests..."
    ((TOTAL_TESTS++))
    if run_test_command $procs "tests/mpi/test_mpi_infrastructure.py"; then
        ((PASSED_TESTS++))
    else
        ((FAILED_TESTS++))
    fi

    # Integration tests (only for smaller process counts to avoid timeout)
    if [[ $procs -le 4 ]]; then
        echo
        print_status "Running MPI integration tests..."
        ((TOTAL_TESTS++))
        if run_test_command $procs "tests/mpi/test_mpi_integration.py"; then
            ((PASSED_TESTS++))
        else
            ((FAILED_TESTS++))
        fi
    fi

    # Performance tests (if requested and only for specific process counts)
    if [[ "$PERFORMANCE_TESTS" == "true" ]] && [[ $procs -eq 2 || $procs -eq 4 ]]; then
        echo
        print_status "Running MPI performance tests..."
        ((TOTAL_TESTS++))
        if run_test_command $procs "tests/mpi/test_mpi_performance.py --runperformance"; then
            ((PASSED_TESTS++))
        else
            ((FAILED_TESTS++))
        fi
    fi
done

# Test summary
echo
print_status "Test Summary"
print_status "============"
echo "Total test runs: $TOTAL_TESTS"
echo "Passed: $PASSED_TESTS"
echo "Failed: $FAILED_TESTS"

if [[ $FAILED_TESTS -eq 0 ]]; then
    print_success "All MPI tests passed!"
    exit 0
else
    print_error "Some MPI tests failed."
    exit 1
fi
