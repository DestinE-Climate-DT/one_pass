"""Functions to remove unnecessary attributes during continuous statistics.

These utilities clean up intermediate computation results that don't need to be
maintained between output periods for continuous statistics.
"""

from typing import TYPE_CHECKING

from one_pass.options import StatKey

if TYPE_CHECKING:
    from one_pass.opa import Opa


def find_items_with_substr(pass_dict, target_substring):
    """Find all class attributes containing a specific substring.

    Searches through the attributes of an object (typically an OPA instance)
    and returns those whose names contain the target substring.

    Parameters
    ----------
    pass_dict : object
        Object (typically Opa instance) whose attributes to search.
    target_substring : str
        Substring to match in attribute names.

    Returns
    -------
    list of str
        List of attribute names containing the target substring.
    """
    matching_items = []

    for key in pass_dict.__dict__.keys():

        if target_substring in key:
            matching_items.append((key))

    return matching_items


def remove_attributes_continuous(opa: "Opa"):
    """Clean up intermediate attributes for continuous statistics.

    For continuous statistics, removes attributes that are no longer needed after
    output but keeps essential cumulative data for the next computation cycle.

    Parameters
    ----------
    opa : Opa
        OPA instance with continuous statistics.

    Notes
    -----
    This function selectively removes:
    - Final percentile/histogram arrays (keeping underlying TDigests)
    - Standard deviation (keeping variance for recomputation)
    - Finalized datasets after output

    These removals reduce memory usage while preserving the state needed for
    continued rolling computation.
    """
    if opa.request.stat in (StatKey.PERCENTILE, StatKey.HISTOGRAM):
        matching_items = find_items_with_substr(opa.statistics, "cum")
        for key in matching_items:
            # Keep the cumulative object which contains tdigest
            if key != "_cumulative":
                setattr(opa.statistics, key, None)

    if opa.request.stat == StatKey.STD:
        setattr(opa.statistics, "std_cum", None)

    if opa.statistics.final_cum is not None:
        setattr(opa.statistics, "final_cum", None)

    if opa.request.stat == StatKey.HISTOGRAM:
        if opa.statistics.final2_cum is not None:
            setattr(opa.statistics, "final2_cum", None)
