"""Functions to read and write pickle files"""

import pickle

def write_pickle(what_to_dump, file_name = None):
    """ Writes pickle file

    Arguments
    ----------
    what_to_dump : the contents of what to pickle
    file_name : optional file name if different from
            self.checkpoint_file. Used for bias_correction
    """
    if file_name:
        with open(file_name, 'wb') as file:
            pickle.dump(what_to_dump, file)
        file.close()
    else:
        with open(what_to_dump.request.checkpoint_file, 'wb') as file:
            pickle.dump(what_to_dump, file)
        file.close()
