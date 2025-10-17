"""One-Pass std library."""

# pylint: disable=W0613

from typing import TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray

if TYPE_CHECKING:
    from one_pass.opa import Opa


def finalize_std(opa: "Opa") -> NDArray:
    """Computes one pass standard deviation and create the attribute
    opa.std_cum by squaring the variance. This is only done when
    n_data = count to stop the opa checkpoint std_cum along with
    the continuous variance.

    Arguments
    ---------
    opa : Opa class

    Returns
    -------
    opa.std_cum : np.ndarray updated cumulative standard deviation by
            square rooting the value of the cumulative variance.
    """

    return np.sqrt(opa.statistics.cumulative["var"])
