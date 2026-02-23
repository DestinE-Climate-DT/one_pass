""" OPA utility library"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from logging import Logger
from pathlib import Path
from typing import Any, Dict, Literal, Mapping, Optional, Sequence, Tuple
import os
import yaml

from numpy import inf
from numpy.typing import NDArray

from one_pass.options import (
    StatKey,
    FrequencyKey,
)
from one_pass.validation.request import check_request
from one_pass.mpi import MPI_LAUNCHED, MPI_RANK, MPI_SIZE


class RequestError(KeyError):
    """Request error. Raised during OPA request validation."""


def load_yaml(infile: str | Path) -> Dict:
    """Load configuration from a YAML file.

    Parameters
    ----------
    infile : str or Path
        Path to the YAML file to load.

    Returns
    -------
    dict
        Dictionary containing the loaded YAML configuration.

    Raises
    ------
    FileNotFoundError
        If the specified YAML file does not exist.
    """
    try:
        with open(infile, "r", encoding="utf-8") as file:
            cfg = yaml.load(file, Loader=yaml.FullLoader)
            if len(cfg) == 0:
                cfg = cfg[0]
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"{infile} not found: you need to have this configuration file!"
        ) from exc
    return cfg


@dataclass
class Request:  # pylint: disable=R0902
    """Configuration for OPA computation requests.

    Initializes all variables from the user request and validates them
    through check_request to ensure the request is valid with all required
    variables passed. This becomes a nested dataclass in the Opa class.

    Attributes
    ----------
    stat : StatKey
        The statistic to compute (e.g., 'mean', 'std', 'percentile').
    time_step : int
        Time step between incoming data points in minutes (typically 60).
    variable : str
        Variable of interest. If incoming data is an xr.Dataset, this
        specifies which variable to extract. OPA can only process one variable.
    checkpoint : bool
        Whether to save statistics to pickle files for resuming computation.
    save : bool
        Whether to save the final statistic to disk.
    stat_freq : FrequencyKey, optional
        Frequency of the requested statistic (e.g., 'daily'). Required for
        non-raw statistics.
    output_freq : FrequencyKey, optional
        Frequency for saving final statistics to NetCDF (e.g., 'monthly').
        Defaults to stat_freq if not specified.
    checkpoint_filepath : str, optional
        Directory where checkpoint files are stored. Required if checkpoint=True.
    save_filepath : str, optional
        Directory where final statistics are saved. Required if save=True.
    compression : int, optional
        Compression parameter for TDigest objects. Used for "histogram" or
        "percentile" statistics. Defaults to 1.
    bias_adjust : bool, default False
        Whether to apply bias adjustment to input data.
    ba_reference_dir : str, optional
        Directory containing reference TDigest pickle files for bias adjustment.
        Required if bias_adjust=True.
    ba_lower_threshold : float, default -inf
        Lower threshold for bias adjustment application.
    ba_non_negative : bool, default False
        Whether to ensure bias-adjusted variable is non-negative.
    ba_agg_method : {"sum", "mean"}, default "sum"
        Method for daily aggregation in bias adjustment.
    ba_future_method : {"additive", "multiplicative"}, default "additive"
        Method for future bias adjustment.
    ba_future_weight : float, default 1.0
        Weight for future values in TDigests to wash out historical data.
    ba_future_start_date : str, default "9999-12-31"
        Start date for future period in YYYY-MM-DD format.
    ba_detrend : bool, default False
        Whether to detrend the variable.
    ba_detrend_skip_years : int, default 10
        Number of years to skip at the beginning for detrending.
    bins : int or array-like, optional
        Number of bins or bin edges for "histogram" statistic.
    range : tuple of float, optional
        Value range for histogram bins.
    percentile_list : list of float, optional
        List of percentiles to compute for "percentile" statistic.
    thresh_exceed : list of float, optional
        Threshold values for "thresh_exceed" statistic.
    ba_transform_method : str or None, default None
        Transformation method to apply to data. Options: None, "log"
    ba_ref_trend_year : int, optional
        If reference used de-trending, the year to evaluate the reference trend
        needs to be given. By default will use the start year of the simulation.
    """

    # TODO: Check what variables from the following shouldn't be optional
    stat: StatKey
    time_step: int
    variable: str
    checkpoint: bool
    save: bool
    stat_freq: Optional[FrequencyKey] = None  # NOTE: Required if stat != "raw"
    # NOTE: Combinations of stat_freq and output_freq need to be validated
    output_freq: Optional[FrequencyKey] = (
        None  # NOTE: Defaults to stat_freq if stat != "raw"
    )
    # IDEA: Change name to checkpoint_directory for clarity? (Involved change)
    checkpoint_filepath: Optional[str] = None  # NOTE: Required if checkpoint is True
    save_filepath: Optional[str] = None  # NOTE: Required if save is True
    compression: Optional[int] = None
    bias_adjust: bool = False
    # Optional variables, when bias_adjust is True
    ba_reference_dir: str = None
    ba_lower_threshold: float = -inf
    ba_non_negative: bool = False
    ba_agg_method: Literal["sum", "mean"] = "sum"
    ba_future_method: Literal["additive", "multiplicative"] = "additive"
    ba_future_weight: float = 1.0
    ba_future_start_date: str = "9999-12-31"
    ba_detrend: bool = False
    ba_detrend_skip_years: int = 10
    ba_transform_method: Optional[str] = None
    ba_ref_trend_year: Optional[int] = None
    # Optional variables, for specific stats
    bins: Optional[int | NDArray] = None  # NOTE: required if stat == "histogram"
    range: Optional[Tuple[float, float]] = None
    percentile_list: Optional[Sequence[float]] = None
    thresh_exceed: Optional[Sequence[float]] = None
    # Deprecated variables
    bias_adjustment: bool = False
    bias_adjustment_method: Optional[str] = None
    # TODO: log becomes a separate module
    _logger: Optional[Logger] = None

    def __post_init__(self):
        logger = self._logger
        self.stat = StatKey(self.stat)
        _validate_freqs(self, logger=logger)
        _validate_filepaths(self)
        _normalize_directories(self, logger=logger)
        _validate_types(self)
        _validate_mpi(self)
        check_request(self, logger=logger)

    def __repr__(self):
        skip_keys = {
            "bias_adjustment",
            "checkpoint_filepath",
            "save_filepath",
            "_logger",
        }
        valid_value_types = (StatKey, FrequencyKey, bool, int, float, str)
        elements = []
        for key, value in asdict(self).items():
            if key in skip_keys:
                continue
            if value is None:
                continue
            if self.bias_adjust is False and key.startswith("ba_"):
                continue
            if not isinstance(value, valid_value_types):
                continue
            if isinstance(value, (StatKey, FrequencyKey)):
                elements.append(f"{key}={value.value}")
            else:
                elements.append(f"{key}={value}")
        return f"Request({', '.join(elements)})"

    @classmethod
    def parse(
        cls,
        user_request: str | Path | Mapping[str, Any],
        logger: Optional[Logger] = None,
    ):
        """Parse user request from various input formats.

        Interprets user request from YAML file path or dictionary and converts
        it to a validated Request instance.

        Parameters
        ----------
        user_request : str, Path, or dict
            Request configuration. If str or Path, interpreted as path to YAML
            file which is loaded and processed. If dict, processed directly.
        logger : Logger, optional
            Logger instance for validation warnings and errors.

        Returns
        -------
        Request
            Validated Request instance containing the parsed configuration.

        Raises
        ------
        TypeError
            If user_request is not a supported type.
        RequestError
            If required fields are missing or unrecognized fields are present.
        """
        if isinstance(user_request, dict):
            pass
        elif isinstance(user_request, (Path, str)):
            user_request = load_yaml(user_request)
        else:
            raise TypeError(
                f"Could not extract request from type {type(user_request)}. "
                "Request must be parsed as Python Dict or from a YAML file."
            )
        cls_fields = [field.name for field in fields(cls)]
        cls_required_fields = [
            field.name
            for field in fields(cls)
            if (field.default is field.default_factory and field.default is not None)
        ]
        missing_fields = [
            field for field in cls_required_fields if field not in user_request
        ]
        unrecognized_fields = [
            field for field in user_request if field not in cls_fields
        ]
        if missing_fields:
            raise RequestError(f"Fields missing: {', '.join(missing_fields)}.")
        if unrecognized_fields:
            raise RequestError(f"Unrecognized fields: {', '.join(missing_fields)}.")
        return cls(**user_request, _logger=logger)

    def _checkpoint_name_base(self) -> str:
        """Generate base checkpoint filename without parallel suffix.

        Returns
        -------
        str
            Base checkpoint filename incorporating variable, frequencies, timestep,
            and statistic type, without .pkl extension or parallel suffix.
        """
        return (
            f"checkpoint_{self.variable}_"
            f"{self.stat_freq.value}_"
            f"timestep_{self.time_step}_"
            f"{self.output_freq.value}_"
            f"{self.stat.value}"
        )

    def checkpoint_name_for_worker(self, worker_id: int, n_workers: int) -> str:
        """Generate checkpoint filename for a specific worker/rank in parallel mode.

        Works for both MPI ranks and MP workers, as they use the same naming pattern.

        Parameters
        ----------
        worker_id : int
            Worker/rank ID (0-indexed)
        n_workers : int
            Total number of workers/ranks

        Returns
        -------
        str
            Checkpoint filename for the specified worker/rank, incorporating variable,
            frequencies, timestep, and statistic type, with parallel suffix.
        """
        name_stem = self._checkpoint_name_base()
        name_stem += f".{worker_id:06d}-{n_workers:06d}"
        return name_stem + ".pkl"

    # IDEA: Change name to checkpoint_name for clarity? (Involved change)
    @property
    def checkpoint_name(self) -> str:
        """Generate checkpoint filename based on request properties.

        Automatically handles MPI and serial/MP modes:
        - MPI: Appends .RANK-SIZE (e.g., .000000-000004)
        - Serial / MP: No suffix (MP workers use checkpoint_file_for_worker directly)

        Returns
        -------
        str
            Checkpoint filename incorporating variable, frequencies, timestep,
            and statistic type, with appropriate parallel suffix.
        """
        name_stem = self._checkpoint_name_base()
        if MPI_LAUNCHED:
            return self.checkpoint_name_for_worker(MPI_RANK, MPI_SIZE)
        # In MP mode (workers > 1), the main process uses the regular name.
        # Worker processes use checkpoint_file_for_worker() directly and never
        # go through this property, so there is no name collision.
        return name_stem + ".pkl"

    def checkpoint_file_for_worker(self, worker_id: int, n_workers: int) -> str:
        """Full checkpoint file path for a specific worker/rank in parallel mode.

        Works for both MPI ranks and MP workers. Primarily used when iterating
        over workers/ranks (e.g., in MP mode to gather checkpoints from all workers).

        Parameters
        ----------
        worker_id : int
            Worker/rank ID (0-indexed)
        n_workers : int
            Total number of workers/ranks

        Returns
        -------
        str
            Complete path to checkpoint file for the specified worker/rank.

        Raises
        ------
        ValueError
            If checkpoint_filepath was not set in the request.
        """
        checkpoint_filepath = self.checkpoint_filepath
        if checkpoint_filepath is None:
            raise ValueError("Checkpoint filepath wasn't set.")
        return os.path.join(
            checkpoint_filepath, self.checkpoint_name_for_worker(worker_id, n_workers)
        )

    # TODO: Rename to checkpoint_dir
    @property
    def checkpoint_file(self) -> str:
        """Full checkpoint file path.

        Returns
        -------
        str
            Complete path to checkpoint file (directory + filename).

        Raises
        ------
        ValueError
            If checkpoint_filepath was not set in the request.
        """
        checkpoint_filepath = self.checkpoint_filepath
        if checkpoint_filepath is None:
            raise ValueError("Checkpoint filepath wasn't set.")
        return os.path.join(checkpoint_filepath, self.checkpoint_name)

    @property
    def time_word(self):
        """Get the appropriate time frequency for computation.

        Returns
        -------
        FrequencyKey
            stat_freq for non-continuous statistics, output_freq for continuous
            statistics.
        """
        stat_freq = self.stat_freq
        return stat_freq if stat_freq != FrequencyKey.CONTINUOUS else self.output_freq


def _validate_freqs(request: Request, logger: Logger):
    if request.stat != StatKey.RAW:
        # IDEA: Make it actually non-optional (not even for stat RAW, to avoid too much hardcoding).
        if request.stat_freq is None:
            raise RequestError(
                "Data request (python dictionary or config.yml) must include "
                "the key 'stat_freq' with an appropriate value pair."
            )
        request.stat_freq = FrequencyKey(request.stat_freq)
        if request.output_freq is None:
            request.output_freq = FrequencyKey(request.stat_freq.value)
            logger.warning(
                "Data request (python dictionary or config.yml) did not include 'output_freq'. "
                f"It has been set to '{request.output_freq.value}', to match with 'stat_freq'."
            )
            return
        request.output_freq = FrequencyKey(request.output_freq)


def _normalize_directory(path, key, logger):
    dirname = os.path.dirname(path)
    if os.path.isdir(path):
        # Path was a directory, and it exists
        return path
    if os.path.isdir(dirname):
        # Path is a file (which exists or not), whose directory exists
        logger.warning(
            f"Removed file name from {key}, as this "
            f"is created dynamically. Filepath is now {dirname}"
        )
        return dirname
    try:
        # Directory does not exist, try and create. Does not attempt to create parents
        os.mkdir(dirname)
        return dirname
    except Exception as exc:
        raise RequestError(
            f"Path {path} does not exist and could not create pass a valid directory for {key}"
        ) from exc


def _normalize_directories(request: Request, logger):
    if request.save_filepath is not None:
        request.save_filepath = _normalize_directory(
            request.save_filepath, "save_filepath", logger
        )
    if request.checkpoint_filepath is not None:
        request.checkpoint_filepath = _normalize_directory(
            request.checkpoint_filepath, "checkpoint_filepath", logger
        )


def _validate_filepaths(request: Request):
    if request.save and request.save_filepath is None:
        raise RequestError("If save is True, save_filepath must be passed.")
    if request.checkpoint and request.checkpoint_filepath is None:
        raise RequestError("If checkpoint is True, checkpoint_filepath must be passed.")


def _validate_types(request: Request):
    if not isinstance(request.checkpoint, bool):
        raise TypeError(
            f"checkpoint must be of type bool, but was '{str(type(request.checkpoint))}'."
        )
    if not isinstance(request.save, bool):
        raise TypeError(
            f"save must be of type bool, but was '{str(type(request.save))}'."
        )
    # time_step must be a positive integer number of minutes
    if not isinstance(request.time_step, int):
        raise TypeError(
            f"time_step must be of type int, but was '{type(request.time_step).__name__}'."
        )
    if request.time_step <= 0:
        raise ValueError("time_step must be a positive integer number of minutes.")


def _validate_mpi(request: Request):
    if MPI_LAUNCHED:
        if request.stat not in (StatKey.HISTOGRAM, StatKey.PERCENTILE):
            raise RuntimeError(
                f"OPA run with MPI only allowed for stats {StatKey.HISTOGRAM.value} and"
                f"{StatKey.PERCENTILE.value}, but {request.stat.value()} was requested."
            )
