"""This module provides logging functionality through the function `get_logger`."""

from copy import copy
import logging
from typing import Optional, Union
import socket
from one_pass.mpi import MPI_LAUNCHED, MPI_RANK

LOGGING_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

_FORMAT = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
_MPI_FORMAT = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s [MPI rank: {rank}]"


def get_logger(
    name: Optional[str] = None,
    logging_level: str = "INFO",
    filename: Optional[str] = None,
    hostname: bool = True,
    **kwargs,
):
    """Create and configure a logger instance for OPA.

    Sets up a logger with specified level and output destination (file or console).
    Includes hostname information when requested.

    Parameters
    ----------
    name : str, optional
        Name for the logger instance. If None, uses the default logger.
    logging_level : str, default "INFO"
        Minimum severity level for log messages. Options are 'DEBUG', 'INFO',
        'WARNING', 'ERROR', and 'CRITICAL'.
    filename : str, optional
        Path to log file. If None, logs to console (stderr).
    hostname : bool, default True
        Whether to include hostname information in log records.
    **kwargs
        Additional keyword arguments passed to LoggerAdapter.

    Returns
    -------
    logging.Logger or logging.LoggerAdapter
        Configured logger instance for OPA logging.

    Raises
    ------
    ValueError
        If logging_level is not one of the valid options.
    """
    if logging_level.upper() not in LOGGING_LEVELS:
        raise ValueError(
            f"Logging level must be one of {LOGGING_LEVELS}, but was '{logging_level}"
        )
    logger = logging.getLogger(name)
    if not logger.handlers:
        logging_level = logging_level.upper()
        logger.setLevel(logging_level)
        if MPI_LAUNCHED:
            formatter = logging.Formatter(_MPI_FORMAT.format(rank=MPI_RANK))
        else:
            formatter = logging.Formatter(_FORMAT)
        if filename:
            file_handler = logging.FileHandler(filename)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        else:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
    if hostname:
        kwargs.update({"hostname": socket.gethostname()})
    if kwargs:
        logger = logging.LoggerAdapter(logger)
    return logger


def clone_logger(
    logger: Union[logging.Logger, logging.LoggerAdapter], name: str
) -> Union[logging.Logger, logging.LoggerAdapter]:
    """
    Clone a Logger or LoggerAdapter under a new name, preserving its configuration.

    Args:
        logger: The original logger or logger adapter.
        name: The name of the cloned logger.

    Returns:
        A new Logger or LoggerAdapter with the same configuration.
    """
    # If it's a LoggerAdapter, unwrap and clone the underlying logger first
    if isinstance(logger, logging.LoggerAdapter):
        base_logger = logger.logger
        extra = copy(logger.extra)
        cloned_base = clone_logger(base_logger, name)
        return logging.LoggerAdapter(cloned_base, extra)

    # --- Regular Logger ---
    cloned = logging.getLogger(name)

    # Copy level
    cloned.setLevel(logger.level)

    # Clear existing handlers (to avoid duplicates if re-cloned)
    cloned.handlers.clear()

    # Copy handlers by reference (typical logging pattern)
    for h in logger.handlers:
        cloned.addHandler(h)

    # Copy filters
    cloned.filters.clear()
    for f in logger.filters:
        cloned.addFilter(f)

    # Copy propagate
    cloned.propagate = logger.propagate

    return cloned
