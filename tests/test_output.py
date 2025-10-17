"""
Testing the output structure of the statistics that include extra
dimensions
"""

from init_tests import get_opa_stat, temp_directory


def hist_calc(directory):
    n_start = 4 * 24
    n_data = n_start + 7 * 24
    step = 7
    pass_dic = {
        "stat": "histogram",
        "stat_freq": "12hourly",
        "bins": 10,
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "compression": 20,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": directory,
        "save_filepath": directory,
    }

    one_pass = get_opa_stat(n_start, n_data, step, pass_dic)

    return one_pass


def thresh_exceed_calc(directory):

    n_start = 4 * 24
    n_data = n_start + 7 * 24
    step = 7

    pass_dic = {
        "stat": "thresh_exceed",
        "thresh_exceed": [0.6, 0.02, 0.01, 0.008],
        "stat_freq": "12hourly",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": directory,
        "save_filepath": directory,
    }

    one_pass = get_opa_stat(n_start, n_data, step, pass_dic)

    return one_pass


def percentile_calc(directory):

    n_start = 24
    n_data = n_start + 24
    step = 1

    pass_dic = {
        "stat": "percentile",
        "percentile_list": [],
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "compression": 20,
        "checkpoint_filepath": directory,
        "save_filepath": directory,
    }

    one_pass = get_opa_stat(n_start, n_data, step, pass_dic)

    return one_pass


####################### py tests ##############################


def test_histogram_count_length(temp_directory):
    # the two pass will just be over the last stat_freq
    bin_counts = hist_calc(temp_directory)
    # print(bin_counts)
    len_bin_counts = len(bin_counts[0].pr.bin_count)
    assert len_bin_counts == 10


def test_threshold_length(temp_directory):
    # the two pass will just be over the last stat_freq
    output = thresh_exceed_calc(temp_directory)
    len_threshold = len(output.thresholds)
    assert len_threshold == 4


def test_percentile_length(temp_directory):
    # the two pass will just be over the last stat_freq
    output = percentile_calc(temp_directory)
    len_percentile = len(output.percentile)
    assert len_percentile == 100
