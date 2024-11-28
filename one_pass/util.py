""" OPA utility library"""
import sys
from pathlib import Path

import yaml

# from typing import Dict


def load_yaml(infile):
    """Load generic yaml file"""
    try:
        with open(infile, "r", encoding="utf-8") as file:
            cfg = yaml.load(file, Loader=yaml.FullLoader)
            if len(cfg) == 0:
                cfg = cfg[0]
    except IOError:
        sys.exit(
            f"ERROR: {infile} not found: you need to have this configuration file!"
        )
    return cfg

def parse_request(user_request):
    """
    Interpret user request and convert it to pyfdb-readable dict.

    Arguments:
    ----------
    user_request : str or dict
        If str, a path to YAML file is interpreted. This file is
        opened to obtain a dict and then processed
        If dict, request in dict form is interpreted, and is
        directly processed.

    Returns:
    --------
    dict
        Dictionary contian MARS request in pyfdb-readable format.
    """
    if isinstance(user_request, (Path, str)):
        user_request = load_yaml(user_request)
    elif not isinstance(user_request, dict):
        raise RuntimeError(
            f"Could not extract request from type {type(user_request)}. \
                Request must be parsed as Python Dict or from a YAML file."
        )

    return user_request
