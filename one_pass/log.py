"""This module provides logging functionality through the function `get_logger`."""

import logging
from typing import Optional
import socket

LOGGING_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


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
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
        )
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
