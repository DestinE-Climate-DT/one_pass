""" Initalise statistic attributes for the OPA class"""

from collections import defaultdict
from typing import Sequence, Union, Tuple, TYPE_CHECKING, List

from crick import TDigest
import numpy as np
from numpy.typing import NDArray
import xarray as xr

from one_pass.mpi import MPI_LAUNCHED, MPI_RANK
from one_pass.options import FrequencyKey, StatKey
from one_pass.saving.create_file_names import create_file_name
from .cumulative import CumulativeStats
from .iams import Iams
from .library.tdigest import initialise as initialise_tdigest

if TYPE_CHECKING:
    from one_pass.opa import Opa


class OpaStatistics:
    """Manager for OPA cumulative statistics computation and storage.

    This class handles the initialization, computation, and management of
    rolling statistics. All data stored in this class will consume significant
    memory as it maintains cumulative arrays and intermediate results.
    """

    def __init__(self, opa: "Opa"):
        """Initialize the OpaStatistics instance.

        Sets up the statistics manager with references to the parent OPA instance
        and initializes state tracking variables.

        Parameters
        ----------
        opa : Opa
            Reference to the parent OPA instance containing request configuration.

        Attributes
        ----------
        opa : Opa
            Reference to the parent OPA instance.
        initialized : bool
            Flag indicating whether statistics have been initialized.
        _cumulative : CumulativeStats or None
            Container for rolling statistics arrays.
        _final : xr.Dataset or None
            Final computed statistics ready for output.
        _final2 : xr.Dataset or None
            Second final dataset (used for histogram bin edges).
        """

        self.opa = opa
        self._final = None
        self._final2 = None
        self._cumulative = None
        self.initialized = False

    @property
    def cumulative(self) -> CumulativeStats:
        """Access the cumulative statistics container.

        Returns
        -------
        CumulativeStats
            Container with rolling statistics arrays and intermediate results.

        Raises
        ------
        RuntimeError
            If OpaStatistics has not been initialized yet.
        """
        if self._cumulative is None:
            raise RuntimeError("OpaStatistics not initialized.")
        return self._cumulative

    def get_final(self) -> Union[xr.Dataset, Tuple[xr.Dataset, xr.Dataset]]:
        """Get the finalized dataset(s) from the statistics computation.

        Returns
        -------
        Union[xr.Dataset, Tuple[xr.Dataset, xr.Dataset]]
            The finalized dataset(s). Returns a single dataset for most statistics,
            or a tuple of two datasets for histogram statistics.

        Raises
        ------
        ValueError
            If no statistic has been finalized yet
        """
        if self._final is None:
            if MPI_LAUNCHED and MPI_RANK != 0:
                return None
            raise ValueError("No statistic has yet been finalised.")
        if self._final2 is None:
            return self._final
        return (self._final, self._final2)

    def reset(self, keep_final: bool = False):
        """Resets all of the attributes in the class opa.statistics
        to None, as they are no longer needed.

        Attributes
        ------------
        removal_final : bool. If this is true, as default it will also
                remove the final xr.Dataset "final_cum". If false, it
                will leave "final_cum" (and "final2_cum") as these are
                the appended xr.Datasets and are needed in the next
                calls
        """
        self._cumulative = None
        self.initialized = False
        if not keep_final:
            self._final = None
            self._final2 = None

    def initialize(self, data_source: xr.DataArray):
        """Initialize data structures for cumulative statistics.

        Sets up the statistics arrays and metadata based on the input data shape.
        For continuous statistics, also initializes continuous counting.

        Parameters
        ----------
        data_source : xr.DataArray
            Input data used to determine array shapes and initialize metadata.

        Notes
        -----
        This method:
        - Creates zero-filled arrays with input data shape (time dim = 1)
        - Stores data shape and size information in opa.data_set_info
        - Initializes continuous counting for continuous statistics
        - Sets the filename for output based on time frequency
        """
        opa = self.opa
        data_source_tail = data_source.tail(time=1)
        zeros_shape = np.shape(data_source_tail)
        opa.data_set_info.shape_data_source_tail = zeros_shape
        opa.data_set_info.size_data_source_tail = np.size(data_source_tail)
        self.initialized = True
        self._cumulative = CumulativeStats(opa, zeros_shape)

        time_word = (
            opa.request.stat_freq
            if opa.request.stat_freq != FrequencyKey.CONTINUOUS
            else opa.request.output_freq
        )
        opa.data_set_info.filename = create_file_name(opa, time_word=time_word)

        if opa.request.stat_freq == FrequencyKey.CONTINUOUS:
            opa.time.count_continuous = 0
            opa.time.init_count_timestamp = opa.time.last_timestamp

    def add_final(self, dataset: Union[xr.Dataset, Tuple[xr.Dataset, xr.Dataset]]):
        """Add a finalized dataset to the statistics collection.

        Parameters
        ----------
        dataset : Union[xr.Dataset, Tuple[xr.Dataset, xr.Dataset]]
            The finalized dataset(s) to add. Can be a single dataset or a tuple
            of two datasets (for histogram statistics).
        """
        opa = self.opa

        if isinstance(dataset, tuple):
            # This occurs for histogram
            assert len(dataset) == 2
            self._final = _concat_ds(self._final, dataset[0])
            self._final2 = _concat_ds(self._final2, dataset[1])

        else:
            self._final = _concat_ds(self._final, dataset)

        opa.time.cur_statistics += 1
        self.opa.logger.debug(
            f"Appended {opa.time.cur_statistics} out of {opa.time.num_statistics} "
            f"completed {opa.request.stat_freq.value} statistics "
            f"to final dataset"
        )

    def load(self):
        """Dask load in place all elements into memory."""

        if self._cumulative is not None:
            for value in self._cumulative.values():
                if isinstance(value, xr.DataArray):
                    value.load()

        if self._final is not None:
            self._final.load()

        if self._final2 is not None:
            self._final2.load()


def _concat_ds(ds1: Union[xr.Dataset, None], ds2: Union[xr.Dataset, None]):
    if ds1 is None:
        # ds2 may be None only in MPI case for RANK > 0.
        return ds2
    # Attributes will be those of ds1.
    concat_ds = xr.concat([ds1, ds2], "time", combine_attrs="override")
    # TODO: This should be the case by design. Simple assert? Remove?
    concat_ds = concat_ds.sortby("time")
    return concat_ds
