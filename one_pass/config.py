"""OnePass config module. Provides paths PATH_ROOT and PATH_DATA (for testing)."""

from pathlib import Path

PATH_ROOT = Path(__file__).absolute().parent.parent
PATH_DATA = PATH_ROOT / "data"
