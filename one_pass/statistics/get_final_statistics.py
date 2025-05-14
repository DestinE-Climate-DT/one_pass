""" Module to get the final statistics. Called when statistic
is full at ndata = count"""

import logging
import numpy as np
import tqdm

def get_histogram(opa_self):
    """Converts tdigests into histograms. It first creates the
    two attributes, bin_edges and bin_counts which it then fills
    with the output of the data from the t-digest histogram func.
    It then re-shapes these attributes back into the original grid.
    (https://github.com/dask/crick/blob/main/crick/tdigest.pyx)

    Arguments
    ----------
    opa_self : Opa class
    opa_self.request.bins : int or array_like, optional
            If ``bins`` is an int, it defines the number of equal
            width bins in the given range. If ``bins`` is an array_like,
            the values define the edges of the bins (rightmost edge
            inclusive), allowing for non-uniform bin widths. The default
            is 10.

    Returns
    ---------
    opa_self.statistics.histogram_cum: numpy array with the same shape as the
            original grid (+ one extra dimension of length opa_self.request.bins)
            that contains the cumulative count for every bin.
    opa_self.statistics.histogram_bin_edges_cum: numpy array with the same shape
            as theoriginal grid (+ one extra dimension of length opa_self.request
            .bins + 1)that contains the edges for all the bins. This needs to be
            one longer in the extra dimension.
    """
    if hasattr(opa_self.request, "bins") is False:
        # if bins not set, setting to default
        opa_self.request.bins = 10

    opa_self.statistics.histogram_cum = np.empty(
        np.shape(
            [opa_self.statistics.digests_cum]*opa_self.request.bins
        ),dtype=np.int32
        )
    opa_self.statistics.histogram_bin_edges_cum = np.empty(
        np.shape(
            [opa_self.statistics.digests_cum]*(opa_self.request.bins+1)
        ),dtype=np.int32
        )

    if hasattr(opa_self.request,"range") is False:
        if opa_self.logger.isEnabledFor(logging.DEBUG):
            for j in tqdm.tqdm(
                    range(opa_self.data_set_info.size_data_source_tail),
                    desc="extracting histogram from digests"
                ):
                opa_self.statistics.histogram_cum[:,j], \
                opa_self.statistics.histogram_bin_edges_cum[:,j] = \
                opa_self.statistics.digests_cum[j].histogram(
                bins = opa_self.request.bins
            )

        else:
        # this is looping through every grid cell
            for j in range(opa_self.data_set_info.size_data_source_tail):
                opa_self.statistics.histogram_cum[:,j], \
                opa_self.statistics.histogram_bin_edges_cum[:,j] = \
                    opa_self.statistics.digests_cum[j].histogram(
                    bins = opa_self.request.bins
                )

    else: #tqdm.tqdm(
        for j in range(opa_self.data_set_info.size_data_source_tail):
            opa_self.statistics.histogram_cum[:,j], \
            opa_self.statistics.histogram_bin_edges_cum[:,j] = \
                opa_self.statistics.digests_cum[j].histogram(
                bins = opa_self.request.bins, range = opa_self.request.range
            )

    # # adding axis for time
    value = opa_self.data_set_info.shape_data_source_tail
    final_size_edges = [opa_self.request.bins+1, *value[1:]]
    final_size_counts = [opa_self.request.bins, *value[1:]]

    opa_self.statistics.histogram_cum = np.reshape(
        opa_self.statistics.histogram_cum, final_size_counts
    )
    opa_self.statistics.histogram_bin_edges_cum = np.reshape(
        opa_self.statistics.histogram_bin_edges_cum, final_size_edges
    )

    opa_self.statistics.histogram_cum = np.expand_dims(
        opa_self.statistics.histogram_cum, axis=0
    )
    opa_self.statistics.histogram_bin_edges_cum = np.expand_dims(
        opa_self.statistics.histogram_bin_edges_cum, axis=0
    )

def get_percentile(opa_self):
    """Converts digest functions into percentiles and reshapes
    the attribute percentile_cum back into the shape of the original
    grid. It will use the attribute percentile_list if specified,
    to return the requested percentiles, if not it will default to
    percentiles 0-99

    Arguments
    ---------
    opa_self : Opa class

    Returns
    ---------
    opa_self.statistics.percentile_cum : numpy array with the same shape as the
            original data but with an extra dimension corresponding
            to the number of percentiles requested.
    """
    if len(opa_self.request.percentile_list) == 0:
        opa_self.request.percentile_list = (np.linspace(0, 99, 100)) / 100

    opa_self.statistics.percentile_cum = np.zeros(
        np.shape([opa_self.statistics.digests_cum]*
                 np.shape(opa_self.request.percentile_list)[0])
        )

    for j in range(opa_self.data_set_info.size_data_source_tail):
        # for crick
        opa_self.statistics.percentile_cum[:,j] = \
            opa_self.statistics.digests_cum[j].quantile(
            opa_self.request.percentile_list
        )

    value = opa_self.data_set_info.shape_data_source_tail
    final_size = [np.size(opa_self.request.percentile_list), *value[1:]]
    # with the percentiles we add another dimension for the percentiles
    opa_self.statistics.percentile_cum = np.reshape(
        opa_self.statistics.percentile_cum, final_size
    )
    # adding axis for time
    opa_self.statistics.percentile_cum = np.expand_dims(
        opa_self.statistics.percentile_cum, axis=0
    )

def get_std(opa_self):
    """Computes one pass standard deviation and create the attribute
    opa_self.std_cum by squaring the variance. This is only done when
    n_data = count to stop the opa checkpoint std_cum along with
    the continuous variance.

    Arguments
    ---------
    opa_self : Opa class

    Returns
    ---------
    opa_self.std_cum : np.ndarray updated cumulative standard deviation by
            square rooting the value of the cumulative variance. 
    """
    opa_self.statistics.std_cum = np.sqrt(opa_self.statistics.var_cum)

def get_final_statistics(opa_self):
    """Called when n_data == count and we need to create the final
    statistics from rolling summaries. These create new attributes
    which are then removed after returning in memory and potentially
    saved.

    Attributes
    ---------
    opa_self : Opa class

    Returns
    --------
    Potentially new attributes depending on the statistic.
    """
    if opa_self.request.stat == "percentile":
        get_percentile(opa_self)

    if opa_self.request.stat == "histogram":
        get_histogram(opa_self)

    if opa_self.request.stat == "std":
        get_std(opa_self)
