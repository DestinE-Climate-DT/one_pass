"""Module that contains functions to write a checkpoint file.
The nature of the checkpoint file will be decided based on the size of the
attributes. If any data is passed as a dask array, it will load it into
memory before checkpointing.
"""
import random
import sys
from one_pass.checkpointing.write_pickle import write_pickle
from one_pass.checkpointing.write_zarr import write_zarr

def find_items_with_substr(pass_dict, target_substring):
    """Finds all attributes of dict (which is the whole Opa opa_self class)
    that have a specific ending 'target_substring'.

    Attributes
    -----------
    dict : the Opa class
    target_substring : string specifying all the attributes
            from dict you want to select.

    Returns
    ---------
    matching_items : A list of a class attributes with a
            name containing the target_substring.
    """
    matching_items = []

    for key in pass_dict.__dict__.keys():

        if target_substring in key:
            matching_items.append((key))

    return matching_items

def get_digest_total_size(opa_self, key : str, total_size : float):
    """Finds the size of the attribute digests_cum by taking the
    size of a random digest and multiplying across the size of the
    grid. Doing this to save looping through every grid cell which
    is time consuming.

    Attributes
    -----------
    opa_self : the Opa class
    key : str. the class attribute to consider, in this case "digests_cum"
    total_size : float. current cumulative size of attributes of interest

    Returns
    ---------
    total_size : Updated to include the GB size of the specified
            attribute class
    """

    random_element = random.choice(getattr(opa_self.statistics, key).flat)
    total_size += (sys.getsizeof(
        random_element.centroids())*
        opa_self.data_set_info.size_data_source_tail/(10**9))

    # if opa_self.stat == "bias_correction":
    #     # needs .flat as it has been reshaped into a numpy array
    #     for element in getattr(opa_self, key).flat:
    #         #total_size += (np.size(element.centroids())*8*2)/(10**9)
    #         total_size += (sys.getsizeof(element.centroids()))/(10**9)

    return total_size

def get_total_size(opa_self, just_digests : bool = False):
    """ Function to calculate the size of all the 'heavy' attributes
    in the Opa class. All the statistics are named with "cum" for
    cumulative.

    Attributes
    -----------
    opa_self : the Opa class
    just_digests : blooean flag to specify if we just want to find the total
            size of t-digests or if we want to find the total size of all the
            heavy attributes.

    Returns
    ---------
    total_size : total size in GB of all the 'heavy' Opa class attributes
            corresponding to the statistics.
    """
    total_size = 0

    if just_digests:
        key = "digests_cum"
        total_size = get_digest_total_size(opa_self, key, total_size)

    else:
        # this contains all the attributes will rolling stats
        for element in opa_self.statistics.__dict__.items():
            if getattr(opa_self.statistics, element[0]) is not None:
                if element[0] == "digests_cum":
                    total_size = get_digest_total_size(opa_self, element[0], total_size)
                # both final_cum and final2_cum are xr.Datasets so size is checked
                # differently
                elif element[0] == "final_cum" or element[0] == "final2_cum":
                    total_size += (
                            opa_self.statistics.final_cum[
                            opa_self.request.variable].values.size
                        *
                            opa_self.statistics.final_cum[
                            opa_self.request.variable].values.data.itemsize
                        )/(10**9)
                else:
                    # final cum will be a xr.DataArray so will have values
                    if hasattr(getattr(opa_self.statistics, element[0]), 'values'):
                        total_size += (
                            getattr(opa_self.statistics, element[0]).size *
                            getattr(opa_self.statistics, element[0]).values.itemsize
                            )/(10**9)
                    else:
                        total_size += (
                            getattr(opa_self.statistics, element[0]).size *
                            getattr(opa_self.statistics, element[0]).itemsize
                            )/(10**9)

    return total_size

def load_dask(opa_self, key):
    """Computing dask lazy operations and calling data into memory
    First, finds all attributes that actually contain data by
    searching for the string cum
    
    Attributes
    -----------
    key : the attribute of the opa class that needs to be
            loaded. Most probably STAT.cum
    
    """
    if hasattr(getattr(opa_self, key), "compute"):
        setattr(opa_self, key,
                        getattr(opa_self, key).compute()
        )

def write_checkpoint(opa_self):
    """Write checkpoint file. First, any array that is dask, must
    beloaded into memory otherwise pickle files get too large. 
    
    Then checks the size of the class and if it can fit in memory.
    If it's larger than pickle limit (1.6) GB (pickle limit is 2GB)
    it will save the main climate data to zarr with only meta data
    stored as pickle
    """
    for element in opa_self.statistics.__dict__.items():
        load_dask(opa_self.statistics, element[0])

    total_size = get_total_size(opa_self)
    # limit on a pickle file is 2GB
    if total_size < opa_self.fixed.pickle_limit:
        # have to include opa_self here as the second input
        write_pickle(opa_self)
        opa_self.logger.debug(
                f'Writing pickle checkpoint with size {total_size} GB'
            )

    else:
        # this will pickle metaData inside as well
        write_zarr(opa_self)
        opa_self.logger.debug(
                f'Writing zarr checkpoint with size {total_size} GB'
            )
