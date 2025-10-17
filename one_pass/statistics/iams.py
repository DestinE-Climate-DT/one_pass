"""IAMS state class."""

from typing import List, Literal, Union

import numpy as np
from numpy.typing import ArrayLike, NDArray


class Iams:  # pylint: disable=R0903
    """
    Incremental Annual Maximum Statistic (IAMS) state container.

    Attributes
    ----------
    durations : List[int]
        Valid duration windows (in minutes).
    n_data_durations : np.ndarray
        Duration windows in units of opa.request.time_step.
    count_durations : np.ndarray
        Rolling counter for each duration.
    count_durations_full : np.ndarray
        Full counter for each duration (never reset).
    buffer : np.ndarray
        Circular buffer storing most recent data.
    cumulative : np.ndarray
        Cumulative maximum sums across durations.
    """

    # all possible durations in minutes
    _DURATIONS = (
        5,
        10,
        15,
        20,
        30,
        45,
        60,
        90,
        120,
        180,
        240,
        360,
        540,
        720,
        1080,
        1440,
        2880,
        4320,
        5760,
        7200,
        8640,
        10080,
    )

    def __init__(self, time_step: int, shape: ArrayLike) -> NDArray[np.float64]:
        """
        Initialize IAMS buffers for a given time step and data shape.

        Parameters
        ----------
        time_step : int
            Time step in minutes.
        shape : tuple[int]
            Shape of incoming data (with time as the first dim).

        Returns
        -------
        np.ndarray
            Empty cumulative maxima array with shape
            (num_durations, *shape[1:])
        """
        shape = np.asarray(shape)[1:]  # drop time dim

        durations = Iams.duration_pick(time_step)
        n_data_durations = np.array([d // time_step for d in durations], dtype=np.int64)

        self.durations = durations
        self.n_data_durations = n_data_durations
        self.count_durations = np.zeros_like(n_data_durations)
        self.count_durations_full = np.zeros_like(n_data_durations)
        self.buffer = np.zeros((n_data_durations[-1], *shape))
        self.cumulative = np.zeros((len(durations), *shape))

    @staticmethod
    def _get_filter_fun(time_step: Union[float, int]):
        return lambda duration: (duration >= time_step) and (duration % time_step == 0)

    @classmethod
    def duration_pick(
        cls,
        time_step: Union[float, int],
        unit: Union[Literal["hours"], Literal["minutes"]] = "minutes",
    ) -> List[int]:
        """Select valid durations given the time step."""
        if unit == "hours":
            time_step *= 60
        filter_fun = cls._get_filter_fun(time_step)
        durations = list(filter(filter_fun, cls._DURATIONS))
        if unit == "hours":
            return sorted(set(int(d // 60) for d in durations))
        return durations
