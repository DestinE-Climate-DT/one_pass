"""Module for all the functions releated to the bias correction
digest files. The bias-correction requires 3 outputs as opposed to
1 for the other statistics. It requires raw data, daily aggregates
and t-digest objects that include the daily aggregations for each month.
This module includes all the functions for the monthly t-digest objects.
There will be one file for each month, where each grid cell contains a
t-digest. These digests are updated with the daily aggregation, either
a mean or sum depending on the variable."""

import os
from datetime import datetime
import numpy as np


try:
    from bias_corr import bias_adjust as run
except ImportError:
    run = None


def bias_adjust(data, request):
    """
    Bias adjust data.

    Parameters
    ----------
    data : xr.DataArray
        data to be adjusted
    request : Request
        OPA request with the bias adjustment configuration

    Returns
    -------
    xr.DataArray
        adjusted data

    Raises
    ------
    ImportError
        If bias_corr package is not installed.
    """
    if not request.bias_adjust:
        return data
    if run is None:
        raise ImportError("OPA: 'bias_corr' package not found.")
    # If ba_reference_dir contains a 'mask.npy' file, it is assumed to be the mask
    # that the reference files was computed with. Otherwise, assume that the reference
    # files were computed with no mask.
    mask_path = os.path.join(request.ba_reference_dir, "mask.npy")
    if os.path.isfile(mask_path):
        mask = np.load(mask_path)
    else:
        mask = None
    return run(
        data=data.load(),
        tdigest_ref=request.ba_reference_dir,
        tdigest_model=os.path.join(request.save_filepath, "ba"),
        lower_threshold=request.ba_lower_threshold,
        non_negative=request.ba_non_negative,
        agg_meth=request.ba_agg_method,
        evaluate=False,
        proceed=False,
        stream=True,
        in_mask=mask,
        future_meth=request.ba_future_method,
        future_start_date=datetime.fromisoformat(request.ba_future_start_date),
        future_weight=request.ba_future_weight,
        detrend=request.ba_detrend,
        detrend_skip_years=request.ba_detrend_skip_years,
        transform_method=request.ba_transform_method,
        ref_trend_year=request.ba_ref_trend_year,
    )
