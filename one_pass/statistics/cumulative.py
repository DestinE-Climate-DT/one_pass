"""Cumulative stats object for the Opa.OpaStatistics class."""

from collections import defaultdict
from typing import Sequence, TYPE_CHECKING, List

from crick import TDigest
import numpy as np
from numpy.typing import NDArray

from one_pass.options import StatKey
from .iams import Iams
from .library.tdigest import initialise as initialise_tdigest

if TYPE_CHECKING:
    from one_pass.opa import Opa


class CumulativeStats(defaultdict):
    """Container for cumulative statistics data.

    This class manages the storage and creation of cumulative statistics.
    All data in this class will be large in memory as it stores the
    rolling statistics arrays and intermediate computation results.

    Parameters
    ----------
    opa : Opa
        Reference to the main OPA instance.
    shape : sequence of int
        Shape of the data arrays for statistics storage.

    Attributes
    ----------
    opa : Opa
        Reference to the main OPA instance.
    shape : tuple
        Shape of the statistics arrays.
    """

    def __init__(self, opa: "Opa", shape: Sequence[int]):
        self.opa = opa
        self.shape = shape
        super().__init__(None)

    def __reduce__(self):
        # Save opa, shape, factory, and the actual dict items
        return (
            self.__class__._reconstruct,
            (self.opa, self.shape, self.default_factory, dict(self)),
            self.__dict__,
        )

    @classmethod
    def _reconstruct(cls, opa, shape, factory, items):
        obj = cls(opa, shape)
        obj.default_factory = factory
        obj.update(items)
        return obj

    def __repr__(self):
        return (
            f"CumulativeStats for {self.opa.request.stat} (shape: {self.shape}). "
            f"Contains: {sorted(self.keys())}"
        )

    def __missing__(self, key):
        if isinstance(key, StatKey):
            key = key.value
        if key not in self._VALID_KEYS:
            raise ValueError(f"Invalid stat key: {key}")
        if key in self._FACTORIES:
            self[key] = self._FACTORIES[key](self)
        else:
            self[key] = np.zeros(self.shape, dtype=np.float64)
        return self[key]

    def _init_iams(self) -> Iams:
        return Iams(self.opa.request.time_step, self.shape)

    def _init_tdigest(self) -> List[TDigest]:
        """Initialize a flat array of empty TDigest objects.

        Creates TDigest objects for percentile and histogram computations.
        The number of TDigests matches the flattened size of the input data.

        Returns
        -------
        list of TDigest
            List of empty TDigest objects with specified compression,
            one for each spatial point in the input data.
        """

        return initialise_tdigest(self.opa)

    def _init_thresh_exceed(self) -> NDArray[np.float64]:
        thresh_exceed = self.opa.request.thresh_exceed
        shape = self.shape
        assert thresh_exceed is not None
        # INFO: Removed here some old logic for dask
        return np.zeros((shape[0], len(thresh_exceed), *shape[1:]))

    def _init_timings(self) -> NDArray[np.datetime64]:
        unit = "ns"
        shape = self.shape
        return np.full(shape, np.datetime64("NaT", unit), dtype=f"datetime64[{unit}]")

    _FACTORIES = {
        StatKey.THRESH_EXCEED.value: _init_thresh_exceed,
        StatKey.IAMS.value: _init_iams,
        "tdigest": _init_tdigest,
        "timings": _init_timings,
    }
    _ADDITIONAL_KEYS = ("histogram_counts", "histogram_edges")
    _VALID_KEYS = tuple(
        sorted(
            [key.value for key in StatKey]
            + list(_FACTORIES.keys())
            + list(_ADDITIONAL_KEYS)
        )
    )
