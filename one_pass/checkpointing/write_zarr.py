"""Module to write zarr checkpoint files. Called when opa_self.statistics.
STAT_cum exceeds pickle size limit"""

import os
import zarr
import numpy as np
from numcodecs import Blosc
from numcodecs import Pickle

from one_pass.checkpointing.write_pickle import write_pickle

def write_zarr(opa_self : object):
    """Write checkpoint file as to zarr. This will be used when
    size of the checkpoint file is over 2GB. The only thing written
    to zarr will be the summary statstic (opa_self.STAT_cum). All the meta
    Data will be pickled (included in this function)

    Returns
    ---------
    self_opa.time.using_zarr : bool. Set to true. Flag to indicate that
            checkpoints are being done with zarr.
    all elements of opa_self.statistics saved as zarr, other than the
    final_cum and final2_cum.
    pickle checkpoint file written with the rest of the Opa class.
    """
    compressor = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)

    # looping through all the attributes with 'cum' - the big ones
    for element in opa_self.statistics.__dict__.items():
        if (element[0] != "final_cum" and
                element[0] != "final2_cum" and
                element[1] is not None
            ):
            opa_self.time.using_zarr = True
            checkpoint_file_zarr = os.path.join(
                opa_self.request.checkpoint_filepath,
                f"checkpoint_{opa_self.request.variable}_"
                f"{opa_self.request.stat_freq}_"
                f"{opa_self.request.output_freq}_"
                f"timestep_{opa_self.request.time_step}_"
                f"{element[0]}.zarr",
            )
            if opa_self.request.stat in ("percentile", "histogram"):
                zarr.array(
                    getattr(opa_self.statistics, element[0]),
                    store=checkpoint_file_zarr,
                    object_codec=Pickle(),
                    compressor=compressor,
                    overwrite=True,
                )
            else:
                try:
                    zarr.array(
                        getattr(opa_self.statistics, element[0]),
                        store=checkpoint_file_zarr,
                        compressor=compressor,
                        overwrite=True,
                    )

                except TypeError:
                    zarr.array(
                        getattr(opa_self.statistics, element[0]).values,
                        store=checkpoint_file_zarr,
                        compressor=compressor,
                        overwrite=True,
                    )
            # this will set everything that's need checkpointed to zarr to None
            setattr(opa_self.statistics, element[0], None)

    # now pickling the other key info that needs to be carried
    # through
    write_pickle(opa_self)

def write_zarr_for_bc(opa_self : object, dm : np.array):
    """Write monthly digest files file as to zarr. This will be used when
    size of the monthly digest file is over 2GB. 
    Arguments
    ----------
    opa_self : Opa class
    dm :  numpy.array. Underlying numpy array from the final xr.DataSet
            created to contain the t-digest objects used in the bias-
            correction for the monthly climatologies.

    Returns
    ---------
    zarr file containing the t-digest objects with file name
    opa_self.bc.monthly_digest_file_name
    """
    compressor = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)

    opa_self.time.using_zarr = True
    zarr.array(
        dm,
        store= opa_self.bc.monthly_digest_file_name,
        dtype=object,
        object_codec=Pickle(),
        compressor=compressor,
        overwrite=True,
    )
