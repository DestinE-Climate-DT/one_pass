"""Functions to remove unnecessary attributes that do
not need to be carried through"""

from one_pass.checkpointing.write_checkpoint import find_items_with_substr

def remove_attributes_continuous(opa_self):
    """Resets attributes before the output of the statistic
    when the stat_freq is continuous. We can not remove all
    the attributes STAT.cum because they are needed in the
    next calculation but we can remove some uncessary ones.
    For example, it will remove the final attributes created
    to output the percentiles and histograms, but will leave
    the underlying digests_cum. It will remove the std_cum
    as we only need to carry through the variance and it will
    remove the final_cum which is the dataset used for returning
    """
    if opa_self.request.stat in ("percentile", "histogram"):
        matching_items = find_items_with_substr(opa_self.statistics, "cum")
        for key in matching_items:
            if key != "digests_cum":
                setattr(opa_self.statistics, key, None)

    if opa_self.request.stat == "std":
        setattr(opa_self.statistics, "std_cum", None)

    if opa_self.statistics.final_cum is not None:
        setattr(opa_self.statistics, "final_cum", None)

    if opa_self.request.stat == "histogram":
        if opa_self.statistics.final2_cum is not None:
            setattr(opa_self.statistics, "final2_cum", None)
