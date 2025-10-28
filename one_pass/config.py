"""OnePass config module. Provides paths PATH_ROOT and PATH_DATA (for testing).

Note: Data files are fetched via tools/get_test_data.sh and stored in the
repository's data/ directory, not in the installed package.
"""

from importlib.resources import files
from pathlib import Path

# Try current working directory first (for non-editable installs when running from repo)
_cwd_data = Path.cwd() / "data"
if _cwd_data.is_dir():
    PATH_ROOT = Path.cwd()
    PATH_DATA = _cwd_data
else:
    # Try to use importlib to find package location
    try:
        _package_root = files("one_pass")
        PATH_ROOT = Path(str(_package_root)).parent
    except (TypeError, FileNotFoundError, ModuleNotFoundError, AttributeError):
        # Fall back to file location
        PATH_ROOT = Path(__file__).absolute().parent.parent

    PATH_DATA = PATH_ROOT / "data"
