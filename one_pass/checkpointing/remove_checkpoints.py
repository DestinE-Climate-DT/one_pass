""""Module for removing checkpoint"""
import os
import numpy as np

def remove_zarr_checkpoints(opa_self):
    """Will first check if the attribute matching_items
    is present which indicates that checkpointing has
    been done with zarr. If it is it will go through and
    remove all the zarr checkpoint files
    """
    if opa_self.time.using_zarr:
        # looping through all the data that is something_cum
        for element in opa_self.statistics.__dict__.items():
            if element[0] != "final_cum":
                checkpoint_file_zarr = os.path.join(
                    opa_self.request.checkpoint_filepath,
                    f"checkpoint_{opa_self.request.variable}_"
                    f"{opa_self.request.stat_freq}_"
                    f"{opa_self.request.output_freq}_"
                    f"timestep_{opa_self.request.time_step}_"
                    f"{element[0]}.zarr",
                )
                if os.path.exists(checkpoint_file_zarr):
                    file_list = os.listdir(checkpoint_file_zarr)
                    num_of_files = np.size(file_list)
                    for files in range(num_of_files):
                        os.remove(
                            os.path.join(checkpoint_file_zarr,
                                        file_list[files])
                            )
                    os.rmdir(checkpoint_file_zarr)

def remove_checkpoints(opa_self):
    """Will first check if checkpointing is turned on.
    If yes, it will find the path for the checkpoint
    file and remove it. It will also call remove
    zarr checkpoints
    """
    if opa_self.request.checkpoint:
        if os.path.isfile(opa_self.request.checkpoint_file):
            opa_self.logger.debug('removing checkpoint')
            os.remove(opa_self.request.checkpoint_file)
        remove_zarr_checkpoints(opa_self)
