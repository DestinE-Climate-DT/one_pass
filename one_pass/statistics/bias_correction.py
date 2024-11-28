"""Module for all the functions releated to the bias correction
digest files. The bias-correction requires 3 outputs as opposed to
1 for the other statistics. It requires raw data, daily aggregates
and t-digest objects that include the daily aggregations for each month.
This module includes all the functions for the monthly t-digest objects.
There will be one file for each month, where each grid cell contains a
t-digest. These digests are updated with the daily aggregation, either
a mean or sum depending on the variable."""

import os
import pickle
import zarr
import numpy as np
import xarray as xr

from one_pass.checkpointing.write_pickle import write_pickle
from one_pass.checkpointing.write_zarr import write_zarr_for_bc
from one_pass.checkpointing.write_checkpoint import get_total_size
from one_pass.statistics.update_statistics import update_tdigest
from one_pass.saving.modify_attributes import update_metadata_for_bc_digests
from one_pass.saving.create_data_sets import create_data_set_for_bc
from one_pass.checkpointing.write_checkpoint import load_dask

class BiasCorrection:
    """Class for pickling meta data from a list of attributes that you want.
    This class will be used when getting the metadata required to complement
    the digest .zarr files for the bias correction.
    """
    def __init__(self):
        """Initalise attributes of the class.

        monthly_digest_file_name : str. Name of file that saves the tdigest
                objects. This could have .pkl ending or .zarr
        monthly_digets_file_name : str. If digest files get saved to zarr
                this attribute will be the string containing the name of the
                pickle file containing the rest of the metadata.
        """
        self.monthly_digest_file_name : str = None
        self.monthly_digest_file_bc_att : str = None

    def load_or_init_digests(self, opa_self : object):
        """This function checks to see if a monthly digest file for the bias
        correction t-digests exists. It will take the current time stamp and
        check if a file corresponding that month exists. If the files does exist,
        it will load that file and set the atribute opa_self.statistics.digests_cum
        with the flattened shape. If the file doesn't exist, it will initalise the
        digests.

        Returns
        --------
        opa_self.monthly_digest_file_name = The file name for the stored tDigest
                objects, corresponding to the month of the time stamp of the data.
        """
        # extracts the month based on the current time stamp
        bc_month = self.get_month_str_bc(opa_self)
        # sets the variable opa_self.monthly_digest_file_name
        name = self.get_monthly_digest_filename_bc(opa_self, bc_month)
        pkl_name = os.path.join(f"{name}.pkl")
        zarr_name = os.path.join(f"{name}.zarr")
        # this means it's a pickle file
        if os.path.exists(pkl_name):
            temp_opa_self = self.load_pickle_for_bc(
                pkl_name
            )
            # extracting the underlying list out of the xr.Dataset
            opa_self.statistics.digests_cum = temp_opa_self[
                opa_self.request.variable
            ].values
            del temp_opa_self
            # reshaping so that it's flat
            opa_self.statistics.digests_cum = np.reshape(
                opa_self.statistics.digests_cum,
                opa_self.data_set_info.size_data_source_tail
            )

        elif os.path.exists(zarr_name):
            # this means it has a zarr file
            opa_self.statistics.digests_cum = zarr.load(
                zarr_name
            )
        else:
            # this will only need to initalise for the first month after that,
            # you're reading from the pickle files on disk. This will initalise
            # an attribute called 'digests_cum'
            opa_self.statistics.init_digests(opa_self)

    def load_pickle_for_bc(self, file_path : str):
        """Function that will load pickled data for the bias correction as the
        bias correction will not output .nc files, rather pickle or zarr files
        containing the t-digests

        Arguments
        ----------
        file_path : str. path, including name, to digests file

        Returns
        ---------
        temp_opa_self : class. Temporary class data, not passed as attributes to
        Opa class
        """
        with open(file_path, 'rb') as f:
            temp_opa_self = pickle.load(f)
        f.close()

        return temp_opa_self

    def reshape_bc_tdigest(self, opa_self : object):
        """Converts list of t-digests back into numpy original grid shape

        Arguments
        -----------
        opa_self.statistics.digests_cum : List[objects]. type will be list

        Returns
        ---------
        opa_self.digets_cum : type will be numpy array with original
                shape grid
        """
        # reshaping digest cum into the correct shape
        # this will still have 1 for time dimension
        final_size = opa_self.data_set_info.shape_data_source_tail
        setattr(opa_self.statistics, "digests_cum", np.reshape(
            opa_self.statistics.digests_cum, final_size
        ))

    def get_monthly_digest_filename_bc(self,
                opa_self : object,
                bc_month : str,
                total_size : float = None
            ):
        """Function to create the file name for the monthly t-digest files
        required for the bias-correction.

        Arguments
        ---------
        opa_self : the Opa class
        bc_month : the month of the current time step so you know which file
                to access
        total_size (opt) : if provided will write the file name based on 
                whether the total size is more than the pickle limit

        Returns
        -------
        opa_self.monthly_digest_file_name : file name for the digests 
        opa_self.monthly_digest_file_bc_att : pickle file name that will store
                metdata if digests are going into a zarr
        extension : the extension (pkl or zarr) depending on the size of
                the file
        """
        extension = ""
        path = opa_self.request.save_filepath
        name = (f"month_{bc_month}_{opa_self.request.variable}_"
            f"{opa_self.request.stat}")

        if total_size is None:
            name = os.path.join(path, f"{name}")
            return name

        # this says if it's got zarr already, keep with zarr
        # regardless of size
        # as there is a tiny chance size can decrease
        if (total_size > opa_self.fixed.pickle_limit or
                self.monthly_digest_file_bc_att is not None
            ):
            extension = ".zarr"
            extension_pkl = ".pkl"
            # creating a file name for the meta data
            self.monthly_digest_file_bc_att = os.path.join(
            path, f"{name}_attributes{extension_pkl}",
            )
        else:
            extension = ".pkl"

        self.monthly_digest_file_name = os.path.join(
            path, f"{name}{extension}",
        )

        return extension

    def write_monthly_digest_files_bc(self, opa_self : object, dm : xr.Dataset):
        """Writes to the monthly t-digest pickle file with the updated
        t-digests including the new daily summary. If the file size is
        over the pickle limit it will write to zarr. It will then reset
        opa_self.statistics.digests_cum to None, as this doesn't need to
        be carried through.

        Arguments
        ---------
        opa_self : the Opa class
        dm : xr.Dataset. Final xrarray data set containing the t-digest
                objects in the shape of the original grid.

        Returns
        -------
        opa_self : removes the attribute 'digests_cum' as
                this is a heavy attribute and doesn't need to be carried
                around in the class. Only accessed at the end of the day.
        """
        # determine the total size of the 'digests_cum' attribute
        total_size = get_total_size(opa_self, just_digests=True)
        # extracts the month based on the current time stamp
        bc_month = self.get_month_str_bc(opa_self)
        # sets the variable opa_self.monthly_digest_file_name
        extension = self.get_monthly_digest_filename_bc(opa_self, bc_month, total_size)

        if extension == ".pkl":
            write_pickle(dm, self.monthly_digest_file_name)
        else:
            write_zarr_for_bc(
                opa_self, dm = dm[f'{opa_self.request.variable}'].values
            )
            # now just pickle the rest of the meta data
            write_pickle(opa_self.data_set_info, self.monthly_digest_file_bc_att)

        # resets digests_cum here so that we don't carry it through for
        # the next day if the time step is sub daily, we would be checkpointing
        # and reading this data uncessariliy, because at the end of each day
        # we re-read it and add the lastest daily aggregation
        setattr(opa_self.statistics, "digests_cum", None)

    def get_month_str_bc(self, opa_self : object):
        """Function to extract the month of the time stamp. This month
        is part of the bias correction file name and will dictate which
        which tDigests the daily aggregations are added to

        Returns
        ---------
        bc_month : str. the month of the incoming daily agregation
        """
        bc_month = opa_self.time.init_time_stamp.strftime("%m")

        return bc_month

    def create_and_save_digests_for_bc(
                        self,
                        opa_self : object,
                        data_source : xr.DataArray
                    ):
        """Called when the opa_self.request.stat is complete (daily). Creates
        the monthly digest files required for bias-correction. Will load or initalise
        the digests, update them with the daily means or sums and set the attr
        digests_cum = updated digests. Will put them back onto original grid and make
        them picklabe. Finally it save them as a pickle (or zarr depending on size) and
        reset the attribute "digests_cum".

        Arguments
        ----------
        data_source : xr.DataArray. incoming data chunk
        """
        # load or initalise the monthly digest file
        self.load_or_init_digests(opa_self)

        # including a loading dask function as the daily aggregates
        # won't have been checkpointed so they may be in dask form
        # looping through all the attributes with 'cum' - the big ones
        for element in opa_self.statistics.__dict__.items():
            # first load data into memory
            if element[1] is not None:
                if element[0] != "final_cum":
                    load_dask(opa_self.statistics, element[0])

        # update digests with daily aggregation
        # we know the weight = 1 as it's the aggregation over that day
        if opa_self.request.variable not in opa_self.fixed.precip_options:
            update_tdigest(opa_self, opa_self.statistics.mean_cum, 1)
        else:
            update_tdigest(opa_self, opa_self.statistics.sum_cum, 1)

        # this will give the original grid back for digests_cum
        self.reshape_bc_tdigest(opa_self)

        # output mean or sum as Dataset
        data_set_attrs, data_var_attrs = update_metadata_for_bc_digests(opa_self)

        #create dataset to save the monthly pickle files
        dm = create_data_set_for_bc(
                opa_self, data_source, data_set_attrs, data_var_attrs
            )

        # want to save the final TDigest files for the bias correction as
        # pickle files or if they're too big, as zarr files
        self.write_monthly_digest_files_bc(opa_self, dm)
