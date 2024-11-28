""" OPA module"""

import importlib.metadata

try:
    # Try to get the version using importlib.metadata
    __version__ = importlib.metadata.version("one_pass")
except importlib.metadata.PackageNotFoundError:
    # If the package is not installed, set a default version
    __version__ = "unknown"

__all_ = [
        "__version__",
        "util",
        "check_request",
        "convert_time",
        "update_statistics",
        "Opa"
]
