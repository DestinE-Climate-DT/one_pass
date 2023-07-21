from typing import Dict
import os 
import sys
import pickle 
import numpy as np
import xarray as xr
import pandas as pd
import dask 
import dask.array as da
import time 
from pytdigest import TDigest
from numcodecs import Blosc
import zarr 
import time 
import tqdm
import math 

from one_pass.convert_time import convert_time
from one_pass.check_request import check_request 
from one_pass import util

class PicklableTDigest:
    
    """
    Class to manage pickling (checkpointing) of TDigest data
    
    Attributes:
    -----------
    tdigest : wrapped C object containing the tDigest data 
    """

    def __init__(self, tdigest: TDigest) -> None:
        self.tdigest = tdigest

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return getattr(self, attr)
        return getattr(self.tdigest, attr)

    def __getstate__(self):
        """
        Here we select what we want to serializable from TDigest.
        We will pick only the necessary to re-create a new TDigest.
        """
        state = {
            'centroids': self.tdigest.get_centroids(),
            'compression': self.tdigest.compression
        }
        return state

    def __setstate__(self, state):
        """
        Then here we use the data we serialized to deserialize it.

        We use that data to create a new instance of TDigest. It has
        some static functions to re-create or combine TDigest's.

        Here ``of_centroids`` is used to demonstrate how it works.
        """
        self.tdigest = TDigest.of_centroids(
            centroids=state['centroids'],
            compression=state['compression']
        )

    def __repr__(self):
        return repr(self.tdigest)
    
class OpaMeta:

    """
    Class for pickling all the meta data. 
    
    This class will only be used if checkpointing files 
    larger than 2GB. In this case, the numpy data containing 
    all the rolling statistics (larger data, e.g. mean_cum) 
    will be checkpointed as zarr while the remaining attributes 
    will be pickled.

    """
    def __init__(self, Opa): 
        
        blacklist = str(Opa.stat + "_cum")
        for key, value in Opa.__dict__.items():
            if key not in blacklist:
                self.__setattr__(key, value)

class Opa:

    """ 
    The class that will contain the requested statistic and 
    required attributes. 
    
    Attributes:
    ------------
    Config file: The class will contain all the attributes 
        speficified in the config file: ['stat', 'percentile_list',
        'threshold_exceed', 'stat_freq', 'output_freq', 'time_step',
        'variable', 'save', 'checkpoint', 'checkpoint_filepath',
        'out_filepath',]. These are set in __init__ 
    n_data: Integer value specifiing the number of data points 
        (time steps) required to complete the requested statistic. 
    count: Integer value giving the current number of data points 
        that have been passed to the statisitc. Statistic is complete
        when n_data == count.
    time_stamp: Pandas time stamp of the last piece of data given to the 
        statistic. 
    init_time_stamp: Time stamp of the first piece of data belonging to 
        the requested statistic. This is the time stamp that will be given
        to the final dataSet containing the completed statistic.
    data_set_attr: Attributes of the original dataSet to pass back to
        the final dataSet when the statistic is complete.
    time_append: Integer value specifiying the number of completed statistics 
        that need to be appended together in the final dataSet. If the 
        user sets stat_freq = output_freq this will be 1. 
    stat_freq_min: Integer number of minutes in the duration of the 
        requested statistic 
    checkpoint_file: the name of the checkpoint file, set if checkpoint 
        is True. 
    statistic_cum: Numpy or dask array containing the cumulative state of the 
        requested statistc. 
    """

    def __init__(self, user_request: Dict): 

        """ 
        Initalisation
        
        Arguments 
        ---------
        user_request : pass either a config file or dictionary 
        
        Returns
        ---------
        Initalised Opa class containing attributes from the 
        user_request and, if there was a checkpoint file, the 
        attributes from the checkpoint file
        
        """
                
        request = util.parse_request(user_request)

        self._process_request(request)

        # will check for errors in specified request
        check_request(request = self) 

        # if checkpointing is True
        if(request.get("checkpoint")): 
            # Will check for checkpoint file and load if one exists
            self._check_checkpoint(request)

    def _load_pickle(self, file_path):
        
        """
        Function that will load pickled data 
        
        Arguments
        ----------
        file_path: path, including name, to pickle file 
        
        Returns
        ---------
        Opa class attributes loaded from the pickle file
        
        """
        f = open(file_path, 'rb')
        temp_self = pickle.load(f)
        f.close()

        for key in vars(temp_self):
            self.__setattr__(key, vars(temp_self)[key])

        del(temp_self)
                
    def _load_pickle_for_bc(self, file_path): 
        
        """
        Function that will load pickled data 
        
        Arguments
        ----------
        file_path: path, including name, to pickle file 
        
        Returns
        ---------
        Temporary class data, not passed as attributes to
        Opa class 
        
        """
        
        f = open(file_path, 'rb')
        temp_self = pickle.load(f)
        f.close()

        return temp_self
        
    def _check_checkpoint(self, request):
        
        """
        Takes user user request and creates the file name 
        of the checkpoint file. If the checkpoint file is there
        it will update the class with data from the checkpoint

        Arguments:
        ----------
        user_request : python dictionary containing the file path 
        for the checkpoint file

        Returns:
        --------
        if checkpoint file is present: 
            Opa (self) class object with old attributes
        """
        
        # already checked if path valid if check request
        file_path = request.get("checkpoint_filepath")

        # if it doesn't have zarr, all data in the pickle
        if (hasattr(self, 'use_zarr')):  
            self.checkpoint_file_zarr = os.path.join(
                file_path, 
                (f'checkpoint_{self.variable}_{self.stat_freq}_'
                f'{self.output_freq}_{self.stat}.zarr')
            )

        # if stat is bias correction, the checkpointed statistic will 
        # be the daily mean
        if (self.stat == "bias_correction"):
            self.checkpoint_file = os.path.join(
                file_path, 
                (f'checkpoint_{self.variable}_{self.stat_freq}_'
                f'{self.output_freq}_{self.stat}.pkl')
            )
        
        else: 
            self.checkpoint_file = os.path.join(
                file_path, 
                f'checkpoint_{self.variable}_{self.stat_freq}_'
                f'{self.output_freq}_{self.stat}.pkl'
            )

        # see if the checkpoint file exists
        if os.path.exists(self.checkpoint_file): 
            self._load_pickle(self.checkpoint_file)
            
            # if using a zarr file
            if (hasattr(self, 'use_zarr')):
                if os.path.exists(self.checkpoint_file_zarr):  
                    self.__setattr__(str(self.stat + "_cum"), 
                                     zarr.load(store=self.checkpoint_file_zarr)
                    ) 

        return 
                
    def _process_request(self, request):

        """
        Assigns all class attributes from the given 
        user request  

        Arguments:
        ----------
        user_request : python dictionary from config file 
            or dictionary 

        Returns:
        --------
        class object with initalised attributes 
        """

        for key in request: 
            self.__setattr__(key, request[key])
    
    ############### end if __init__ #####################

    def _initialise_time(self, time_stamp_min, time_stamp_tot_append):

        """
        Called when total time in minutes of the incoming time stamp is 
        less than the timestep of the data. 
        Will initalise the time attributes related to the statistic call 

        Arguments:
        ----------
        time_stamp_min: total time in minutes of the incoming time stamp
            into the requested statistic duration. i.e. 40 minutes into
            a day if stat_freq = "daily"
        time_stamp_tot_append = the number of units (given by time word) 
        of the current time stamp - used for appending data in 
        time_append

        Returns:
        --------
        self.n_data = number of required pieces of data for the statistic 
            to complete 
        self.time_append = how many completed statistics do we need to 
            append in one file 
        self.init_time_stamp = the timestap of the first incoming piece of 
            data for that statistic. Will become the timestamp of the 
            final data array.
        """

        self.init_time_stamp = self.time_stamp 

        # calculated by MIN freq of stat / timestep min of data
        if ((self.stat_freq_min/self.time_step).is_integer()):
            if(self.stat_freq != "continuous"):
                if(time_stamp_min == 0 or 
                   (self.time_step/time_stamp_min).is_integer()): 
                    self.n_data = int(self.stat_freq_min/self.time_step) 
                else:
                    print('WARNING: timings of input data span over new statistic')
                    self.n_data = int(self.stat_freq_min/self.time_step) 
            
            else:
                # if continuous, can start from any point in time so 
                # n_data might not span the full output_freq (hence why 
                # we minus time_stamp_min)
                self.n_data = math.ceil(
                              (self.stat_freq_min - time_stamp_min)/self.time_step
                              )
        
        else:
            raise Exception(
                f'Frequency of the requested statistic (e.g. daily) must'
                f' be wholly divisible by the timestep (dt) of the input data'
            )

        try:
            # if time_append already exisits it won't overwrite it
            getattr(self, "time_append") 
        
        except AttributeError:

            # looking at output freq - how many cum stats you want to 
            # save in one netcdf 
            if (self.stat_freq == self.output_freq and self.stat_freq != "continuous"):
                self.time_append = 1 

            elif(self.stat_freq != self.output_freq and self.stat_freq != "continuous"):
                # converting output freq into a number
                output_freq_min = convert_time(
                    time_word = self.output_freq, time_stamp_input = self.time_stamp
                    )[0]
                
                # self.time_append is  how many days requested: 
                # e.g. 7 days of saving with daily data
                self.time_append = (
                    (output_freq_min - time_stamp_tot_append)/ self.stat_freq_min
                    ) 
                
                #output_freq_min < self.stat_freq_min
                if(self.time_append < 1 and self.stat_freq != "continuous"): 
                    raise ValueError(
                        'Output frequency can not be less than frequency of statistic'
                    )

        return 
    
    def _init_digests(self, ds_size):
    
        """ 
        Function to initalise a flat array full of empty 
        tDigest objects. 
        
        Arguments 
        ----------
        ds_size = size of incoming data 
        
        Returns
        ---------
        a call attribute corresponding to a flat array of 
        empty digests matching the size of the incoming data 
        
        """
        
        self.array_length = np.size(ds_size)
        # list of dictionaries for each grid cell, preserves order 
        digest_list = [dict() for x in range(self.array_length)] 

        for j in (range(self.array_length)):
            # initalising digests and adding to list
            digest_list[j] = TDigest(compression = 25) 

        self.__setattr__(str(self.stat + "_cum"), digest_list)  
            
    def _initialise_attrs(self, ds):

        """
        Initialises data structuresfor cumulative stats 
        
        Arguments
        ---------
        ds = incoming data 

        Returns:
        --------
        self.stat_cum = zero filled data array with the shape of 
            the data compressed in the time dimension 
            For some statistics it may intialise more than one zero filled 
            array as some stats require the storage of more than one stat
        
        Maybe Returns:
        self.count_continuous = like self.count, counts the number of 
            pieces of data seen but never gets reset
        self.raw_data_for_bias_corr = bias_correction requires three 
            different data outputs, this initalises the array to save 
            the raw daily raw data 
        """
        ds_size = ds.tail(time = 1)

        if ds.chunks is None:
            # only using dask if incoming data is in dask
            # forcing computation in float64
            value = np.zeros_like(ds_size, dtype=np.float64) 
        else:
            value = da.zeros_like(ds_size, dtype=np.float64) 

        if(self.stat_freq == "continuous"):
            self.count_continuous = 0

        if(self.stat != "bias_correction" and self.stat != "percentile"): 
            
            ds_size = ds.tail(time = 1)

            self.__setattr__(str(self.stat + "_cum"), value)

            if(self.stat == "var"):
                # then also need to calculate the mean
                self.__setattr__("mean_cum", value)

            # for the standard deviation need both the mean and 
            # variance throughout
            # TODO: can reduce storage here by not saving both 
            # the cumulative variance and std
            elif(self.stat == "std"): 
                self.__setattr__("mean_cum", value)
                self.__setattr__("var_cum", value)

            elif(self.stat == "min" or self.stat == "max"):
                self.__setattr__("timings", value)

        elif(self.stat == "percentile"):
            self._init_digests(ds_size)
            
        elif (self.stat == "bias_correction"): 
            # not ititalising the digests here, as only need to update
            # them with daily means, would create unncessary I/O at 
            # checkpointing 
            
            # also need to get raw data to pass 
            self.__setattr__("raw_data_for_bc", ds)
            # also going to need the daily means 
            self.__setattr__("mean_cum", value)

        return 
            
    def _initialise(self, ds, time_stamp, time_stamp_min, 
                    time_stamp_tot_append):

        """ 
        initalises both time attributes and attributes relating 
        to the statistic as well starting the count and fixing 
        the class time stamp to the current time stamp 
        
        Arguments
        ----------
        ds = incoming raw data 
        time_stamp = time stamp of incoming data 
        time_stamp_min = number of minutes of the incoming timestamp 
            into the current requested freq  
        time_stamp_tot_append = 
        
        Returns
        ---------
        Initalised time and statistic attributes 
        Sets self.count = 0 
        
        """
        self.count = 0
        self.time_stamp = time_stamp

        self._initialise_time(time_stamp_min, time_stamp_tot_append)
        self._initialise_attrs(ds)

        return 
    
    def _check_n_data(self):

        """ checks if the attribute n_data is already there 
        
        Returns
        ---------
        n_data_att_exist = flag to say if the attr exists 
        
        """
        try:
            getattr(self, "n_data")
            n_data_att_exist = True 
        except AttributeError:
            n_data_att_exist = False
        
        return n_data_att_exist
    
    def _check_digests_exist(self):
        
        """ checks if the attribute bias_correction_cum 
        is already there 
        
        Returns
        ---------
        digests_exist = flag to say if the attr exists 
        
        """
        try:
            getattr(self, "bias_correction_cum")
            digests_exist = True 
        except AttributeError:
            digests_exist = False
        
        return digests_exist
            
    def _should_initalise(self, time_stamp_min, proceed):

        """
        Checks to see if in the timestamp of the data is the 'first' 
        in the requested statistic. If so, should_init and proceed are 
        set to true. 
        
        Arguments
        ----------
        time_stamp_min = number of minutes of the incoming timestamp 
            into the current requested freq  
        proceed = Flag to say if the code should proceed
        
        Returns
        should_init = flag to say time should be initalised 
        proceed = flag to say code should proceed 
        ----------
        """ 
        
        should_init = False

        # this indicates that it's the first data otherwise time_stamp 
        # will be larger 
        if(time_stamp_min < self.time_step):
            should_init = True
            proceed = True 

        return proceed, should_init

    def _should_initalise_contin(self, time_stamp_min, proceed):
            
        """ Calls should initalise and then checks if the rolling 
        statistic should also be initalised. When stat_freq = 
        continuous, the time and count is intalised whenver the 
        output_freq is complete (i.e. 1 month) but the rolling 
        statistic is never re-initalised 
        
        Returns:
        ---------
        proceed = flag to say proceed 
        should_init_time = from should_initalise 
        should init_value = should you start rolling stat (will
            only be true the first time the function is called)
        """
        proceed, should_init_time = self._should_initalise(time_stamp_min, proceed)

        should_init_value = False
        n_data_att_exist = self._check_n_data()

        if(n_data_att_exist):
            pass 
        else: 
            should_init_value = True 
            proceed = True

        return proceed, should_init_time, should_init_value
    
    # def _should_initalise_bc(self, time_stamp_min_bc):
            
    #     should_init_digests = False
        
    #     digests_exist = self._check_digests_exist()
                
    #     if(digests_exist):
    #         pass
        
    #     elif(time_stamp_min_bc < self.time_step): 
    #         should_init_digests = True

    #     self.should_init_digests = should_init_digests
        
    #     return 
    
    def _remove_time_append(self):

        """removes 3 attributes relating to time_append 
        (when output_freq > stat_freq) and removes checkpoint file
        """

        for attr in ('dm_append','count_append', 'time_append'):
            self.__dict__.pop(attr, None)
    
        if(self.checkpoint):# delete checkpoint file 
            if os.path.isfile(self.checkpoint_file):
                os.remove(self.checkpoint_file)
            if os.path.isfile(self.checkpoint_file_zarr):
                os.remove(self.checkpoint_file_zarr)
        
        return 

    def _option_one(self,time_stamp, proceed):
        
        """ 
        Called from compare_old_time_steps
        
        Difference in time is equal to the time step. 
        proceed = true 
        
        """    
                
        # re-set self.time_stamp with the new time stamp 
        self.time_stamp = time_stamp 
        # we're happy as it's simply the next step 
        proceed = True 
        
        return proceed 
    
    def _option_two(self, min_diff, time_stamp, proceed):
        
        """ 
        Called from compare_old_time_steps
        
        Time stamp too far in the future. If this is only slighly 
            into the future (2*timestep), just throw a warning if 
            it's more into the future, throw error
        
        """
        
        if abs(min_diff) < 2*self.time_step:
            print(
                f'Time gap at ' + str(time_stamp) + ' too large,'
                f' there seems to be data missing, small enough to carry on'
            )
            self.time_stamp = time_stamp
            proceed = True

            return proceed 
        
        else: 
            raise ValueError(
                'Time gap too large, there seems to be some data missing'
            )
        
    def _option_four(self, min_diff, time_stamp_min, time_stamp,
                     time_stamp_tot_append, proceed):
        
        """ 
        Called from compare_old_time_steps
        
        This function is called if the incoming time stamp is in the 
        past. What to do will depend on if there is a time_append option. 
        If no, it will simply delete the checkpoint fileand carry on. 
        If there is a time_append, it will check if it needs to 'roll back' 
        this appended data set.  """
        
        if(self.stat_freq != "continuous"):
            time_stamp_min_old = convert_time(
                time_word = self.stat_freq, 
                time_stamp_input = self.time_stamp
            )[1]

            # here it's gone back to before the stat it was previously 
            # calculating so delete attributes 
            if(abs(min_diff) > time_stamp_min_old):
                for attr in ('n_data', 'count'):
                    self.__dict__.pop(attr, None)
                    
            # else: if it just goes backwards slightly in the stat you're 
            # already computing, it will either re-int later
            # or it will be caught by 'already seen' later on

        else:
             # always delete these for continuous 
            for attr in ('n_data', 'count'):
                self.__dict__.pop(attr, None)

        try:
            getattr(self, "time_append")
            # if time_append == 1 then this isn't a problem
            if(self.time_append > 1): 
                # first check if you've gone even further back than 
                # the original time append 
                if (time_stamp < self.init_time_stamp):
                    print(
                        f"removing checkpoint as going back to before"
                        f" original time append"
                    )
                    self._remove_time_append()

                else: # you've got back sometime within the time_append window
                    output_freq_min = convert_time(
                        time_word = self.output_freq, 
                        time_stamp_input = self.time_stamp
                    )[0]
                    
                    # time (units of stat_append) for how 
                    # long time_append should be
                    full_append = (output_freq_min/self.stat_freq_min) 
                    # if starting mid way through a output_freq, 
                    # gives you the start of where you are
                    start_int = full_append - self.time_append 
                    # time (units of stat_append) 
                    # for for where time_append should start with new timestamp 
                    start_new_int = (time_stamp_tot_append / self.stat_freq_min) 

                    if ((start_int == start_new_int) or 
                        (start_new_int < start_int)): 
                        # new time step == beginning of time append 
                        
                        print(
                            f"removing checkpoint as starting from beginning"
                            f" of time append"
                        )
                        self._remove_time_append()

                    else: 
                        # rolling back the time append calculation 
                        # now you want to check if it's gone back an exact 
                        # amount i.e. it's gone back to the beginning of a 
                        # new stat or mid way through 
                        
                        should_init = self._should_initalise(
                            time_stamp_min, proceed
                            )[1]

                        # if it's initalising it's fine, roll back
                        if(should_init): 
                            
                            gap = start_new_int - start_int
                            # how many do you need to append
                            roll_back = int(abs(np.size(self.dm_append.time) - gap)) 
                            print('rolling back time_append by ..', roll_back)
                            self.dm_append.isel(time=slice(0,roll_back))
                            self.count_append -= int(roll_back) 
                        
                        # can't roll back if this new time_stamp isn't the 
                        # start of a stat, so deleting
                        else: 
                            print(
                                f"removing previous data in time_append as can't"
                                f" initalise from this point")
                            self._remove_time_append()

        except AttributeError: 
            pass 
        
        return 
                    
    def _compare_old_timestamp(self, time_stamp, time_stamp_min, 
                               time_stamp_tot_append, proceed): 

        """
        This function compares the incoming time_stamp against one 
        that may already been there from a checkpoint.If no previous 
        time stamp it will simply assign the current time stamp to 
        self.timestamp. If there is an old one (self.time_stamp), 
        found from check_n_data, it will compare the difference in time 
        between the two time stamps. 4 options (given as functions above)
           1. Difference in time is equal to the time step. 
                proceed = true 
           2. Time stamp in the future. If this is only slighly into the 
                future (2*timestep), just throw a warning if it's more into 
                the future, throw error 
           3. The time stamp is the same, this will just pass through and 
                will be caught in a later check
           4. Time stamp is in the past. This then depends on if there is 
                a time_append option. If no, it will simply delete the 
                checkpoint fileand carry on. If there is a time_append, 
                it will check if it needs to 'roll back' this appended data set.       
        """

        n_data_att_exist = self._check_n_data()

        if(n_data_att_exist): 
            
            # calculates the difference in the old and new time stamps in minutes 
            min_diff = time_stamp - self.time_stamp
            min_diff = min_diff.total_seconds() / 60 
            
            # option 1, it's the time step directly before, carry on 
            if (min_diff == self.time_step): 
                proceed = self._option_one(time_stamp, proceed)

            # option 2, it's a time step into the future - data mising 
            elif(time_stamp > self.time_stamp): 
                proceed = self._option_two(min_diff, time_stamp, proceed)

            # option 3, time stamps are the same 
            elif(time_stamp == self.time_stamp):
                pass # this will get caught in 'already seen'
            
            # option 4, it's a time stamp from way before, do you need to roll back?
            elif(time_stamp < self.time_stamp):  
                self._option_four(min_diff, time_stamp_min, time_stamp,
                                  time_stamp_tot_append, proceed)
    
        else: 
            self.time_stamp = time_stamp # very first in the series 

        return proceed
    
    def _check_have_seen(self, time_stamp_min):

        """
        check if the OPA has already 'seen' the data done by comparing 
        what the count of the current time stamp is against the actual 
        count.
        
        Returns
        --------
        already_seem = biary True or False flag """

        try: 
            getattr(self, "count")
            if(time_stamp_min/self.time_step) < self.count:
                already_seen = True
            else:
                already_seen = False
        
        except AttributeError:
            already_seen = False
            
        return already_seen

    def _check_time_stamp(self, ds, weight):

        """
        Function to check the incoming timestamps of the data and check if it
        is the first one of the required statistic. If there are multiple incoming 
        timestamps, it will loop through them. First time stamps will pass 
        through 'convert_time' which will convert the time stamp into minutes into
        the requested statistic length and how length of the requested statistic 
        in minutes. This data will then pass through 'compare old timestamps' which 
        will compare the new timestamps against any previous timestamps stored in a 
        checkpoint file. For details see function. 
        
        If this passes, the timestamp will be checked to see if it's 
        the first in the required statstic. If so, initalise the function with the 
        required variables. If not, the function will also check if it's either 
        'seen' the data before or if it should simply be skipped as it's halfway 
        through a required statistic. 

        Arguments
        ----------
        ds: Incoming xarray dataArray with associated timestamp(s)
        weight: the length along the time-dimension of the incoming array 

        Returns
        ---------
        If function is initalised it will assign the attributes: 
        n_data: number of pieces of information required to make up the 
            requested statistic 
        count: how far through the statistic you are, when initalised, this will 
            be set to 1
        stat_cum: will set an empty array of the correct dimensions, ready to be 
            filled with the statistic ds may change length if the time stamp 
            corresponding to the start of the statistic corresponds to half 
            way through ds.
            
        Optional return 
        ----------------
        If statistic is bias correction 
        self.stat_freq_min_bc = same as self.stat_freq_min but with the time word 
            always equal to monthly. Bias correction needs to keep track of both 
            daily stats (for the means) and monthly stats for the tDigests

        Checks
        --------
        If it is not the first timestamp of the statistic, it will check: 
        - that the attribute n_data has already been assigned, otherwise will 
            realise that this data doesn't correspond to this statistic
        If the first piece of incoming data doesn't correspond to the initial data, 
            and weight is greater than 1, it will check the other incoming pieces 
            of data to see if they correspond to the initial statistic
        
        """

        # assuming that incoming data has a time dimension
        time_stamp_sorted = sorted(ds.time.data) 
        time_stamp_list = [pd.to_datetime(x) for x in time_stamp_sorted]
        index = 0 
        proceed = False 
            
        while(proceed == False) and (index < weight): 
            
            time_stamp = time_stamp_list[index]
            
            """
            Outputs of convert time: 
            self.stat_freq_min = the number of minutes in the statistic 
                requested, e.g. daily = 60*24 
            time_stamp_min = the number of minutes in the given timestamp 
                based on your requested stat_freq. so if you're timestamp 
                is 15 minutes into a new day and you requested stat_freq = 
                daily, then this = 15
            time_stamp_tot_append = the number of 'stat_freq' in minutes 
                of the time stamp already completed (only used for appending
                data)
            """
            
            if(self.stat_freq != "continuous"):
                self.stat_freq_min, time_stamp_min, time_stamp_tot_append = convert_time(
                    time_word = self.stat_freq, 
                    time_stamp_input = time_stamp
                )

            elif(self.stat_freq == "continuous"): 
                self.stat_freq_min, time_stamp_min, time_stamp_tot_append = convert_time(
                    time_word = self.output_freq, 
                    time_stamp_input = time_stamp
                )
            
            if(self.stat == "bias_correction"): 
                self.stat_freq_min_bc = convert_time(
                    time_word = "monthly", time_stamp_input = time_stamp
                )[0]

            if(self.stat_freq_min < self.time_step):
                raise ValueError('time_step too large for requested statistic')
            
            proceed = self._compare_old_timestamp(time_stamp, time_stamp_min, 
                                                  time_stamp_tot_append, proceed)

            if(self.stat_freq != "continuous"):
                proceed, should_init = self._should_initalise(
                    time_stamp_min, proceed) 

                if(should_init):   
                    self._initialise(
                        ds, time_stamp, time_stamp_min, time_stamp_tot_append
                    )
                
            else: # for continuous - difference between intialising time and stats
                proceed, should_init_time, should_init = self._should_initalise_contin(
                    time_stamp_min, proceed
                    )
                
                if(should_init):
                    self._initialise(
                        ds, time_stamp, time_stamp_min, time_stamp_tot_append
                        )
                    print('initialising continuous statistic')

                elif(should_init_time):
                    self.count = 0
                    self.time_stamp = time_stamp
                    self._initialise_time(time_stamp_min, time_stamp_tot_append)

            already_seen = self._check_have_seen(time_stamp_min) # checks count 
            
            if(already_seen): 
                print('pass on this data as already seen this data')

            # this will change from False to True if it's just been initalised
            n_data_att_exist = self._check_n_data()  
            if (n_data_att_exist == False):
                print(
                    f'passing on this data as its not the initial data for the'
                    f' requested statistic'
                )
            
            index = index + 1 

        # it will enter this loop it's skipping the first few time steps that come 
        # through but starts from mid way through the incoming data 
        if (index > 1) and (proceed): 
            index = index - 1 
            
            print(index, 'index')
            # chops data from where it needs to start 
            ds = ds.isel(time=slice(index, weight))
            weight = weight - index 
            
        return ds, weight, already_seen, n_data_att_exist, time_stamp_list

    def _check_variable(self, ds):

        """ 
        Checks if the incoming data is an xarray dataArray. 
        If it's a dataSet, will convert to dataArray and keep 
        metaData attributes.
        
        Returns
        --------
        self.data_set_attr = the metadata to be given to the final dataSet
        """

        try:
            # this means it a data_set
            getattr(ds, "data_vars") 
            #keeping the attributes of the full dataSet to give to the final dataSet
            self.data_set_attr = ds.attrs 
            try:
                ds = getattr(ds, self.variable) # converts to a dataArray

            except AttributeError:
                raise Exception(
                    f'If passing dataSet need to provide the correct variable,'
                    f' opa can only use one variable at the moment'
                )
        
        except AttributeError:
            # still extracting attributes from dataArray here 
            self.data_set_attr = ds.attrs 

        return ds
    
    def _check_raw(self, ds, weight):
        
        """
        This function is called if the user has requested stat: 'raw'. 
        This means that they do not want to compute any statstic 
        over any frequency, we will simply save the incoming data. 
        """
        
        final_time_file_str = self._create_raw_file_name(ds, weight)
                                 
        # this will convert the dataArray back into a dataSet with the metadata 
        # of the dataSet and include a new attribute saying that it's saving 
        # raw data for the OPA 
        dm = self._create_raw_data_set(ds)

        if(self.save == True):            
            self._save_output(dm, final_time_file_str)

        return dm 
    
    def _check_num_time_stamps(self, ds):

        """
        Check how many time stamps are in the incoming data. 
        It's possible that the GSV interface will have multiple messages
        """

        time_num = np.size(ds.time.data)

        return time_num

    def _two_pass_mean(self, ds): 
        
        """ computes normal mean using numpy two pass """ 
        
        ax_num = ds.get_axis_num('time')
        temp = np.mean(ds, axis = ax_num, dtype = np.float64, keepdims=True)  
        
        return temp

    def _two_pass_var(self, ds): 
        
        """ computes normal variance using numpy two pass, setting ddof = 1 """ 

        ax_num = ds.get_axis_num('time')
        temp = np.var(ds, axis = ax_num, dtype = np.float64, keepdims=True, ddof = 1)  

        return temp

    def _update_mean(self, ds, weight):

        """ 
        computes one pass mean with weight corresponding to the number 
        of timesteps being added. Also updates count. 
        """ 

        self.count = self.count + weight
        
        if (weight == 1):
            mean_cum = self.mean_cum + weight*(ds - self.mean_cum) / (self.count) 
        else:
             # compute two pass mean first
            temp_mean = self._two_pass_mean(ds)
            mean_cum = self.mean_cum + weight*(temp_mean - self.mean_cum) / (self.count) 

        self.mean_cum = mean_cum.data
        
        return 

    def _update_var(self, ds, weight):

        """ 
        Computes one pass variance with weight corresponding to the number 
        of timesteps being added don't need to update count as that is done 
        in mean
        """ 
        # storing 'old' mean temporarily
        old_mean = self.mean_cum  

        if(weight == 1):
            self._update_mean(ds, weight)
            var_cum = self.var_cum + weight*(ds - old_mean)*(ds - self.mean_cum)
        
        else:
            # two-pass mean
            temp_mean = self._two_pass_mean(ds) 
            # two pass variance
            temp_var = (self._two_pass_var(ds))*(weight-1) 
            # see paper Mastelini. S
            var_cum = self.var_cum + temp_var + np.square(old_mean - temp_mean)*(
                (self.count*weight)/(self.count+weight)
            )
            
            self._update_mean(ds, weight)
            
        if (self.count == self.n_data):
            # using sample variance NOT population variance
            var_cum = var_cum/(self.count - 1) 

        self.var_cum = var_cum.data
        
        return 

    def _update_std(self, ds, weight):
        
        """ 
        Computes one pass standard deviation with weight corresponding 
        to the number of timesteps being added. Uses one pass variance 
        then square root at the end of the statistic. Does not update 
        count as that is done in mean (inside update var)
        
        """ 

        self._update_var(ds, weight)
        self.std_cum = np.sqrt(self.var_cum)
        
        return 

    def _update_min(self, ds, weight):

        """ 
        Finds the cumulative minimum values of the data along with an 
        array of timesteps corresponding to the minimum values. Updates 
        count with weight.   
        """ 
                
        if(weight == 1):
            timestamp = np.datetime_as_string((ds.time.values[0]))
            ds_time = xr.zeros_like(ds)
            ds_time = ds_time.where(ds_time != 0, timestamp)

        else:
            ax_num = ds.get_axis_num('time')
            timings = ds.time
            min_index = ds.argmin(axis = ax_num, keep_attrs = False)
            ds = np.amin(ds, axis = ax_num, keepdims = True)
            ds_time = xr.zeros_like(ds) # now this will have dimensions 1,lat,lon

            for i in range(0, weight):
                timestamp = np.datetime_as_string((timings.values[i]))
                ds_time = ds_time.where(min_index != i, timestamp)

        if(self.count > 0):
            self.min_cum['time'] = ds.time
            self.timings['time'] = ds.time
            ds_time = ds_time.where(ds < self.min_cum, self.timings)
            # this gives the new self.min_cum number when the  condition is FALSE 
            # (location at which to preserve the objects values)
            ds = ds.where(ds < self.min_cum, self.min_cum)

        # convert to datetime64 for saving
        ds_time = ds_time.astype('datetime64[ns]') 

        self.count = self.count + weight
        #running this way around as Array type does not have the function .where,
        # this only works for data_array
        self.min_cum = ds 
        self.timings = ds_time

        return

    def _update_max(self, ds, weight):

        """
        Finds the cumulative maximum values of the data along with an array of 
        timesteps corresponding to the maximum values  
        
        """ 

        if(weight == 1):
            timestamp = np.datetime_as_string((ds.time.values[0]))
            ds_time = xr.zeros_like(ds)
            ds_time = ds_time.where(ds_time != 0, timestamp)
        else:
            ax_num = ds.get_axis_num('time')
            timings = ds.time
            max_index = ds.argmax(axis = ax_num, keep_attrs = False)
            self.max_index = max_index
            ds = np.amax(ds, axis = ax_num, keepdims = True)
            # now this will have dimensions 1,incoming grid
            ds_time = xr.zeros_like(ds) 

            for i in range(0, weight):
                timestamp = np.datetime_as_string((timings.values[i]))
                ds_time = ds_time.where(max_index != i, timestamp)

        if(self.count > 0):
            self.max_cum['time'] = ds.time
            self.timings['time'] = ds.time
            ds_time = ds_time.where(ds > self.max_cum, self.timings)
            # this gives the new self.max_cum number when the  condition is 
            # FALSE (location at which to preserve the objects values)
            ds = ds.where(ds > self.max_cum, self.max_cum)

        # convert to datetime64 for saving
        ds_time = ds_time.astype('datetime64[ns]') 

        self.count = self.count + weight
        self.max_cum = ds 
        self.timings = ds_time

        return

    def _update_threshold(self, ds, weight):

        """ 
        Creates an array with the frequency that a threshold has 
        been exceeded. Updates count with weight.
        """ 
        
        if(weight > 1):

            ds = xr.where(ds < abs(self.thresh_exceed), 0, 1)
             #try slower np version that preserves dimensions
            ds = np.sum(ds, axis = 0, keepdims = True)
            ds = self.thresh_exceed_cum + ds

        else:
            if(self.count > 0):
                self.thresh_exceed_cum['time'] = ds.time

            # need seperate else statment for dimensions
            ds = xr.where(
                ds < abs(self.thresh_exceed), self.thresh_exceed_cum, 
                self.thresh_exceed_cum + 1
            )

        self.count = self.count + weight
        self.thresh_exceed_cum = ds 

        return

    def _update_tdigest(self, ds, weight=1):

        """
        Sequential loop that updates the digest for each grid point. 
        If the statistic is not bias correction, it will also update the 
        count with the weight. For bias correction, this is done in the 
        daily means calculation. 
        
        """
        
        if(weight == 1):
            
            #TODO: check you need the differnence with bias_correction 
            if(self.stat == "bias_correction"):
                if hasattr(self.mean_cum, "chunks"):
                    ds = ds.compute()
                    # here we have extracted the underlying np array, /
                    ds_values = np.reshape(ds, self.array_length) 
                else: 
                    ds_values = np.reshape(ds, self.array_length) 
            else: 
                ds_values = np.reshape(ds.values, self.array_length) 
                    
            # this is looping through every grid cell using crick or pytdigest
            for j in (range(self.array_length)): 
                self.__getattribute__(str(self.stat + "_cum"))[j].update(ds_values[j])

        else: 
            ds_values = ds.values.reshape((weight, -1))

            for j in (range(self.array_length)):
                # using crick or pytdigest
                self.__getattribute__(str(self.stat + "_cum"))[j].update(ds_values[:, j])

        if (self.stat != "bias_correction"):
            self.count = self.count + weight
        
        return 
    
    def _update_raw_data(self, ds):

        """Concantes all the raw data required for the bias-correction"""
        
        self.raw_data_for_bc = xr.concat([self.raw_data_for_bc, ds], dim = 'time') 
        
        return 
    
    def _get_percentile(self, ds):

        """
        Converts digest functions into percentiles and reshapes 
        the attribute percentile_cum back into the shape of the original 
        grid 
        """
        
        if self.percentile_list[0] == 'all':
            self.percentile_list = (np.linspace(0, 100, 101))/100
        
        for j in range(self.array_length):
            # for crick 
            # self.percentile_cum[j] = self.percentile_cum[j].quantile(
                # self.percentile_list
            #)             
            self.percentile_cum[j] = self.percentile_cum[j].inverse_cdf(
                self.percentile_list
            ) 

        self.percentile_cum = np.transpose(self.percentile_cum)

        # reshaping percentile cum into the correct shape 
        ds_size = ds.tail(time = 1) # will still have 1 for time dimension 
        
        # forcing computation in float64
        value = da.zeros_like(ds_size, dtype=np.float64) 
        final_size = da.concatenate([value] * np.size(self.percentile_list), axis=0)

        # with the percentiles we add another dimension for the percentiles
        self.percentile_cum = np.reshape(self.percentile_cum, np.shape(final_size))
        # adding axis for time
        self.percentile_cum = np.expand_dims(self.percentile_cum, axis = 0) 
        
        return 

    def _get_bias_correction_tdigest(self, ds):

        """Converts list of t-digests back into original grid shape and makes 
        them picklable """

        self.bias_correction_cum = np.transpose(self.bias_correction_cum)
        for j in (range(self.array_length)):
            self.bias_correction_cum[j] = PicklableTDigest(
                self.__getattribute__(str(self.stat + "_cum"))[j]
            )
                
        # reshaping percentile cum into the correct shape 
        # this will still have 1 for time dimension 
        ds_size = ds.tail(time = 1) 
        
        value = da.zeros_like(ds_size, dtype=np.float64) 
        final_size = da.concatenate([value], axis=0)
        
        self.bias_correction_cum = np.reshape(
            self.bias_correction_cum, np.shape(final_size)
        )
        #self.bias_correction_cum = np.expand_dims(self.bias_correction_cum, axis = 0) 


        return 
    
    def _load_or_init_digests(self, ds_size):
  
        """ 
        This function checks to see if a checkpoint file for the bias 
        correction t-digests exists. It will take the current time stamp 
        and check if a file corresponding that month exists. If the files 
        does exist, it will load that file and set the atribute self.bias_
        correction_cum with the flattened shape. If the file doesn't exist, 
        it will initalise the digests.
        
        Returns
        --------
        self.checkpoint_file_bc = The file name for the stored tDigest 
        objects, corresponding to the month of the time stamp of the data. 
        
        """
        
        self.array_length = np.size(ds_size)
        final_time_file_str = self._create_file_name_bc() 
        self.checkpoint_file_bc = os.path.join(
            self.out_filepath, 
            f'month_{final_time_file_str}_{self.variable}_{self.stat}.pkl'
        )
              
        # this is loading the t-digest class   
        if (os.path.exists(self.checkpoint_file_bc)): 
            temp_self = self._load_pickle_for_bc(self.checkpoint_file_bc)
            # extracting the underlying list out of the xarray dataSet 
            self.bias_correction_cum = temp_self[self.variable].values
            self.bias_correction_cum = np.reshape(self.bias_correction_cum, 
                                                  self.array_length
                                        )
            del(temp_self)
        
        else:
            # this will only need to initalise for the first month after that, 
            # you're reading from the checkpoints 
            # TODO: do you need if (self.should_init_digests): 
            print('initalising digests')
            self._init_digests(ds_size)

        return 
        
    def _update(self, ds, weight=1):

        """ Depending on the requested statistic will send data to the correct
        function """
        
        if (self.stat == "mean"):
            self._update_mean(ds, weight) 

        elif(self.stat == "var"):
            self._update_var(ds, weight)

        elif(self.stat == "std"):
            self._update_std(ds, weight)

        elif(self.stat == "min"):
            self._update_min(ds, weight)

        elif(self.stat == "max"):
            self._update_max(ds, weight)

        elif(self.stat == "thresh_exceed"):
            self._update_threshold(ds, weight)

        elif(self.stat == "percentile"):
            self._update_tdigest(ds, weight)
            
        elif(self.stat == "bias_correction"): 
        # bias correction requires raw data and daily 
        # means for each call 
        
            if(self.count > 0): 
                # first raw data saved during _init_attrs 
                self._update_raw_data(ds)
            
             # want daily means
            self._update_mean(ds, weight) 
            
        return 

    def _write_pickle(self, what_to_dump, file_name = None): 
        
        """ 
        Writes pickle file
        
        Arguments
        ----------
        what_to_dump = the contents of what to pickle 
        file_name = optional file name if different from 
            self.checkpoint_file. Used for bias_correction
        
        """ 
        
        if (file_name): 
            with open(file_name, 'wb') as file: 
                pickle.dump(what_to_dump, file)
        else: 
            with open(self.checkpoint_file, 'wb') as file: 
                pickle.dump(what_to_dump, file)
                
        return 
            
    def _write_zarr(self): 
        
        """
        Write checkpoint file as to zarr. This will be used when 
        size of the checkpoint file is over 2GB. The only thing written 
        to zarr will be the summary statstic (self.stat_cum). All the meta
        Data will be pickled (included in this function)
        
        """
        
        self.use_zarr = True
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)

        if(hasattr(self, 'checkpoint_file_zarr')):
            zarr.array(
                self.__getattribute__(str(self.stat + "_cum")), 
                store = self.checkpoint_file_zarr, 
                compressor=compressor, overwrite = True
            )
        else: 
            self.checkpoint_file_zarr = os.path.join(
                self.checkpoint_filepath, f'checkpoint_{self.variable}_'
                f'{self.stat_freq}_{self.output_freq}_{self.stat}.zarr'
            )

            zarr.array(
                self.__getattribute__(str(self.stat + "_cum")), 
                store = self.checkpoint_file_zarr, 
                compressor=compressor, overwrite = True
            )

        # now just pickle the rest of the meta data 
        # creates seperate class for the meta data 
        opa_meta = OpaMeta(self) 
        self._write_pickle(opa_meta)
        
        return 
                
    def _load_dask(self):

        """ Computing dask lazy operations and calling data into memory """

        start_time = time.time()
    
        if(self.stat == "bias_correction"):
            self.__setattr__("mean_cum", 
                             self.__getattribute__("mean_cum").compute()
            )
        else: 
            self.__setattr__(str(self.stat + "_cum"), 
                             self.__getattribute__(str(self.stat + "_cum")).compute()
            )
        
        end_time = time.time() - start_time
        #print(np.round(end_time,4), 's to load dask')
        
        return 
    
    def _write_checkpoint(self):

        """
        Write checkpoint file. First checks the size of the class and 
        if it can fit in memory. If it's larger than 1.6 GB (pickle 
        limit is 2GB) it will save the main climate data to zarr with 
        only meta data stored as pickle """
            
        if(self.stat == "bias_correction"):
            if hasattr(self.__getattribute__("mean_cum"), 'compute'):
                # first load data into memory  
                self._load_dask()            
                
            # total size in GB
            total_size = sys.getsizeof(self.__getattribute__("mean_cum"))/(10**9)
            total_size += sys.getsizeof(self.__getattribute__("raw_data_for_bc"))/(10**9)
                 
        else:
            if hasattr(self.__getattribute__(str(self.stat + "_cum")), 'compute'):
                self._load_dask() 
        
            total_size = sys.getsizeof(self.__getattribute__(str(self.stat + "_cum")))/(10**9) 
             
        # they have the arrays repeated         
        if (self.stat == "var" or self.stat == "min" or self.stat == "max"): 
            total_size *= 2
        if(self.stat == "std"): 
            total_size *= 3
               
        if(self.stat == "percentile"):
    
            for j in (range(self.array_length)): #tqdm.tqdm
                self.percentile_cum[j] = PicklableTDigest(
                    self.__getattribute__(str(self.stat + "_cum"))[j]
                )

        # limit on a pickle file is 2GB 
        if(total_size < 1.6): 
            # have to include self here as the second input 
            self._write_pickle(self) 

        else: 
            # this will pickle metaData inside as welll 
            self._write_zarr() 
            
        return 
    
    def _create_raw_data_set(self, ds):
        
        """
        Creates an xarray dataSet for the option of stat: "none". 
        Here the dataSet will be exactly the same as the 
        original, but only containing the requested variable / 
        variable from the config.yml
        
        """
        try:
            ds = getattr(ds, self.variable) 
            dm = ds.to_dataset(dim = None, name = self.variable)
        except AttributeError: 
            dm = ds.to_dataset(dim = None, name = ds.name)    
        
        raw_data_attrs = self.data_set_attr
        raw_data_attrs["OPA"] = str(
            f"raw data at native temporal"
            f" resolution saved by one-pass algorithm"
        )
        dm = dm.assign_attrs(raw_data_attrs)
        
        return dm 
    
    def _create_data_set(self, final_stat, final_time_stamp, ds):

        """ Creates xarray dataSet object with final data 
        and original metadata  
        
        Arguments
        ----------
        final_stat = actual numpy array with cumulative stat 
        final_time_stamp = this is the time stamp for the final array
            this will be equal to the time stamp of the first piece of 
            incoming data that went into the array 
        ds = original data used for sizing 
        
        """

         # compress the dataset down to 1 dimension in time 
        ds = ds.tail(time = 1)
        # re-label the time coordinate 
        ds = ds.assign_coords(time = (["time"], [final_time_stamp], ds.time.attrs)) 
            
        if (self.stat == "percentile"):

            ds = ds.expand_dims(
                dim={"percentile": np.size(self.percentile_list)}, axis=1
            )
            # re-label the time coordinate 
            ds = ds.assign_coords(
                percentile = ("percentile", np.array(self.percentile_list))
            ) 
            
        dm = xr.Dataset(
        data_vars = dict(
                # need to add variable attributes 
                [(str(ds.name), (ds.dims, final_stat, self.data_set_attr))],   
            ),
        coords = dict(
            ds.coords
        ),
        attrs = self.data_set_attr
        )

        if(hasattr(self, 'timings')):
            timing_attrs = {'OPA':'time stamp of ' + str(self.stat_freq + " " + self.stat)}
            dm = dm.assign(timings = (ds.dims, self.timings.data, timing_attrs))

        return dm

    def _data_output(self, ds, time_word = None, bc_mean = False):

        """ Gathers final data and meta data for the final xarray dataSet 
        Writes data attributes of the dataSet depending on the statistic 
        requested. 
        
        Arguments
        ----------
        ds = original data 
        time_word = Word required to create final file name 
        bc_mean = Flag to state that the final data output is for daily means 
            created for the bias correction 
            
        Returns
        ---------
        dm = final dataSet created from the function create_data_set
        final_time_file_str = this is the name of the final saved netCDF file
        
        """
        # final stat will be the cummulative statistic array 
        final_stat = None
              
        # this is the flag to extract the mean value for the bias_correction
        if(bc_mean): 
            final_stat = self.__getattribute__(str("mean_cum"))
            self.data_set_attr["OPA"] = str(
                self.stat_freq + " mean calculated using one-pass algorithm"
            )
        elif(self.stat == "bias_correction"): 
            final_stat = self.__getattribute__(str(self.stat + "_cum"))
            self.data_set_attr["OPA"] = str(
                f"daily means added to the monthly digest for" 
                + self.stat + f" calculated using one-pass algorithm"
            )    
        else: 
            final_stat = self.__getattribute__(str(self.stat + "_cum"))
            self.data_set_attr["OPA"] = str(
                self.stat_freq + " " + self.stat + 
                f" calculated using one-pass algorithm"
            )
            
        if(self.stat == "min" or self.stat == "max" or self.stat == "thresh_exceed"):
            final_stat = final_stat.data
        self.final_stat = final_stat 
        
        # creating the final file name 
        if(self.stat == "bias_correction" and bc_mean == False):
            final_time_file_str = self._create_file_name_bc()
        else:
            final_time_file_str = self._create_file_name(time_word = time_word)
        
        # gets the final time stamp for the data array 
        final_time_stamp = self._create_final_timestamp()

        dm = self._create_data_set(final_stat, final_time_stamp, ds)

        return dm, final_time_file_str

    def _data_output_append(self, dm):

        """  
        Appeneds final dataSet along the time dimension if stat_output 
        is larger than the requested stat_freq. It also sorts along the 
        time dimension to ensure data is also increasing in time 
        """

        dm_append = xr.concat([self.dm_append, dm], "time") 
        self.dm_append = dm_append.sortby("time")
        self.count_append = self.count_append + 1 

        return 

    def _create_final_timestamp(self):

        """
        Creates the final time stamp for each accumulated statistic. 
        For now, simply using the time stamp of the first incoming data 
        of that statistic 
        """

        final_time_stamp = self.init_time_stamp

        return final_time_stamp 
    
    def _create_raw_file_name(self, ds, weight):

        """ 
        Creates the final file name for the netCDF file corresponding to
        the raw data."""

        final_time_file_str = None
        time_stamp_sorted = sorted(ds.time.data) 
        time_stamp_list = [pd.to_datetime(x) for x in time_stamp_sorted]
        
        if (weight > 1):
            final_time_file_str = (
                time_stamp_list[0].strftime("%Y_%m_%d_T%H_%M") 
                + "_to_" 
                + time_stamp_list[-1].strftime("%Y_%m_%d_T%H_%M")
            )
        else:
            final_time_file_str = time_stamp_list[0].strftime("%Y_%m_%d_T%H_%M")

        return final_time_file_str
    

    def _create_file_name(self, append = False, time_word = None):

        """ 
        Creates the final file name for the netCDF file. If append is True 
        then the file name will span from the first requested statistic to the 
        last. time_word corresponds to the continous option which outputs checks 
        every month
        """

        final_time_file_str = None

        if (time_word != None):
            self.stat_freq_old = self.stat_freq
            self.stat_freq = time_word

        if (append):

            if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or 
                self.stat_freq == "6hourly" or self.stat_freq == "12hourly"):
                self.final_time_file_str = (
                    self.final_time_file_str 
                    + "_to_" 
                    + self.time_stamp.strftime("%Y_%m_%d_T%H")
                )

            elif (self.stat_freq == "daily" or self.stat_freq == "weekly"):
                self.final_time_file_str = (
                    self.final_time_file_str 
                    + "_to_" 
                    + self.time_stamp.strftime("%Y_%m_%d")
                )

            elif (self.stat_freq == "monthly" or self.stat_freq == "3monthly"):
                self.final_time_file_str = (
                    self.final_time_file_str 
                    + "_to_" 
                    + self.time_stamp.strftime("%Y_%m")
                )

            elif (self.stat_freq == "annually"):
                self.final_time_file_str = (
                    self.final_time_file_str 
                    + "_to_" 
                    + self.time_stamp.strftime("%Y")
                )
        else:
            if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or 
                self.stat_freq == "6hourly" or self.stat_freq == "12hourly"):
                final_time_file_str = self.init_time_stamp.strftime("%Y_%m_%d_T%H")

            elif (self.stat_freq == "daily"):
                final_time_file_str = self.init_time_stamp.strftime("%Y_%m_%d")

            elif (self.stat_freq == "weekly"):
                final_time_file_str = self.init_time_stamp.strftime("%Y_%m_%d")

            elif (self.stat_freq == "monthly" or self.stat_freq == "3monthly"):
                final_time_file_str = self.init_time_stamp.strftime("%Y_%m")

            elif (self.stat_freq == "annually"):
                final_time_file_str = self.init_time_stamp.strftime("%Y")

        if (time_word != None):
            self.stat_freq = self.stat_freq_old
            delattr(self, "stat_freq_old")

        return final_time_file_str
        
    def _create_file_name_bc(self): 
        
        """
        Function to extract the month of the time stamp. This month 
        is part of the bias correction file name and will dictate which 
        which tDigests the daily means are added to
        
        """
        final_time_file_str = self.init_time_stamp.strftime("%m")

        return final_time_file_str
        
    def _create_and_save_outputs_for_bc(self, ds):
        
        """ 
        Called when the self.stat is complete (daily). Creates the 3 
        files required for bias-correction. Will load or initalise the digests, 
        update them with the daily means and set the attr bias_correction_cum = 
        updated digests. Will put them back onto original grid and make them 
        picklabe. Will also create dm_raw, the daily raw data and dm_mean the 
        daily mean. Then saves this data 
        """
                
        ds_size = ds.tail(time = 1)
        # now want to pass daily mean into bias_corr 
        # load or initalise the monthly digest file 
        self._load_or_init_digests(ds_size) 
        # update digests with daily mean 
        # we know the weight = 1 as it's the mean over that day.
        self._update_tdigest(self.mean_cum, 1) 
        # this will give the original grid back with meta data and picklable digests 
        self._get_bias_correction_tdigest(ds) 
        
        # output raw data 
        dm_raw = self._create_raw_data_set(self.raw_data_for_bc) 
        # output as dataSet 
        dm_mean, final_time_file_str_bc = self._data_output(ds, bc_mean = True) 

        # this should never not be true as will be caught in check_request
        if(self.time_append == 1): 
            if(self.save == True):
                # saving raw data for bc as well 
                self._save_output(dm_raw, final_time_file_str_bc, bc_raw = True) 
                # saving daily means 
                self._save_output(dm_mean, final_time_file_str_bc, bc_mean = True) 
        else: 
            raise AttributeError(
                f'Cannot have stat_freq and output_freq not equal to daily'
                f' for bias_correction'
            )
                        
        return dm_raw, dm_mean
        
    def _save_output(self, dm, final_time_file_str, bc_raw = False, bc_mean = False):

        """
        Function that creates final file name and path and saves final dataSet """
            
        if(self.stat == 'raw'):
            file_name = os.path.join(
                self.out_filepath, f'{final_time_file_str}_{self.variable}_raw_data.nc'
            )
  
        elif(self.stat == 'bias_correction'): 
            
            if(bc_mean):
                file_name = os.path.join(
                    self.out_filepath, 
                    f'{final_time_file_str}_{self.variable}_{self.stat_freq}_mean.nc'
                )                    
            elif(bc_raw):
                file_name = os.path.join(
                    self.out_filepath, 
                    f'{final_time_file_str}_{self.variable}_raw_data_for_bc.nc'
                )

        else: # normal other stats 
            file_name = os.path.join(
                self.out_filepath, 
                f'{final_time_file_str}_{self.variable}_{self.stat_freq}_{self.stat}.nc'
            )
         
        start_time = time.time()
        
        if(self.stat != "bias_correction" or bc_raw == True or bc_mean == True):
            # will re-write the file if it is already there
            dm.to_netcdf(path = file_name, mode = 'w') 
            dm.close()
        
        # want to save the final TDigest files for the bias correction as pickle files
        else:
            #TODO: check total size and if too big, save as zarr 
            self._write_pickle(dm, self.checkpoint_file_bc)
    
        end_time = time.time() - start_time
        #print('finished saving tdigest files in', np.round(end_time,4) ,'s')

        return 

    def _call_recursive(self, how_much_left, weight, ds):

        """if there is more data given than what is required for the statistic,
          it will make a recursive call to itself with the remaining data """
        
        ds = ds.isel(time=slice(how_much_left, weight))
        Opa.compute(self, ds) 

        return 
    
    def _update_statistics(self, weight, time_stamp_list, ds):
        
        """
        A function to see how many more data points needed to fill the statistic.
        If it's under the weight it will pass all the new data to the statistc
        that needs updating and write a checkpoint. If there's more data than
        required to fill the statistic, it will pass only what's required then 
        then return how much is left.
        
        """
        # how much is let of your statistic to fill
        how_much_left = (self.n_data - self.count) 
        
        # will not span over new statistic
        if (how_much_left >= weight): 
            # moving the time stamp to the last of the set
            self.time_stamp = time_stamp_list[-1]  
            
            # update rolling statistic with weight
            self._update(ds, weight)  

            if(self.stat_freq != "continuous"): 
                if(self.checkpoint == True and self.count < self.n_data):
                    # this will not be written when count == ndata 
                    self._write_checkpoint() 

            else: 
                if(self.checkpoint == True):
                    # this will be written when count == ndata because 
                    # still need the checkpoitn for continuous
                    self._write_checkpoint() 

        # this will span over the new statistic 
        elif(how_much_left < weight): 

            # extracting time until the end of the statistic
            ds_left = ds.isel(time=slice(0, how_much_left)) 
            # CHECK moving the time stamp to the last of the set
            self.time_stamp = time_stamp_list[how_much_left] 
            # update rolling statistic with weight of the last few days 
            # still need to finish the statistic (see below)
            self._update(ds_left, how_much_left)

        return how_much_left
    
    def _full_continuous_data(self, ds, how_much_left, weight):
        
        """
        Called when n_data = count but the stat_freq = continuous. So saving 
        intermediate files at the frequency of output_freq. 
        
        Here time append will never be an option. Will create final data, 
        save the output if rquired and called the recursive function if required
        """
            
        dm, final_time_file_str = self._data_output(ds, time_word = self.output_freq)
        if(self.save == True):
            self._save_output(dm, final_time_file_str)
        self.count_continuous = self.count_continuous + self.count
        
        # if there's more to compute - call before return 
        if (how_much_left < weight): 
            self._call_recursive(how_much_left, weight, ds)

        return dm 

    ############## defining class methods ####################
    
    def compute(self, ds):
    
        """  Actual function call  """
        
        # convert from a data_set to a data_array if required
        ds = self._check_variable(ds) 
    
        # this checks if there are multiple time stamps 
        # in a file and will do two pass statistic
        weight = self._check_num_time_stamps(ds) 

        if (self.stat == "raw"): 
            dm = self._check_raw(ds, weight)
            return dm 
        
        # check the time stamp and if the data needs to be initalised 
        (ds, weight, already_seen, n_data_att_exist, 
        time_stamp_list) = self._check_time_stamp(ds, weight)

        # check if data has been 'seen', will only skip if data doesn't 
        # get re-initalised
        if(already_seen): 
            return    
        
        if (n_data_att_exist == False):
            return
                    
        how_much_left = self._update_statistics(weight, time_stamp_list, ds)

        if (self.count == self.n_data and self.stat_freq == "continuous"):

            dm = self._full_continuous_data(ds, how_much_left, weight)

            return dm 

        # when the statistic is full
        if (self.count == self.n_data and self.stat_freq != "continuous"):   
            
            if(self.stat == "percentile"):
                 # this will give self.tdigest but it's full of percentiles 
                self._get_percentile(ds)

            if(self.stat == "bias_correction"): 
                # loads, updates and makes picklabe tdigests, 
                # saves daily raw and daily mean
                dm_raw, dm_mean = self._create_and_save_outputs_for_bc(ds) 
            
            # output as a dataset
            dm, final_time_file_str = self._data_output(ds)  

            #output_freq_min == self.stat_freq_min
            if(self.time_append == 1): 
                if(self.save == True):
                    self._save_output(dm, final_time_file_str)
                        
                # delete checkpoint file 
                if(self.checkpoint == True): 
                    if os.path.isfile(self.checkpoint_file):
                        os.remove(self.checkpoint_file)
                    
                    if (hasattr(self, 'use_zarr')):
                        if os.path.isfile(self.checkpoint_file_zarr):
                            os.remove(self.checkpoint_file_zarr)

                # if there's more to compute - call before return 
                if (how_much_left < weight): 
                    self._call_recursive(how_much_left, weight, ds)
                
                if(self.stat != "bias_correction"):
                    return dm
                else: 
                    return dm, dm_raw, dm_mean

            #output_freq_min > self.stat_freq_min
            elif(self.time_append > 1): 

                # first time it appends
                if (hasattr(self, 'count_append') == False or 
                    self.count_append == 0): 
                    self.count_append = 1
                    # storing the data_set ready for appending
                    self.dm_append = dm 
                    self.final_time_file_str = final_time_file_str
                    
                    if(self.checkpoint):

                        self._write_checkpoint()

                    # if there's more to compute - call before return 
                    if (how_much_left < weight): 
                        self._call_recursive(how_much_left, weight, ds)
                    
                    return self.dm_append
                
                elif(self.count_append < self.time_append):
                    
                    # append data array with new time outputs and update count_append 
                    self._data_output_append(dm)
                    
                    # if this is still true 
                    if(self.count_append < self.time_append): 

                        if(self.checkpoint):

                            self._write_checkpoint()

                        # if there's more to compute - call before return 
                        if (how_much_left < weight): 
                            self._call_recursive(how_much_left, weight, ds)

                        return self.dm_append 

                    elif(self.count_append == self.time_append):

                        # change file name
                        if(self.save):  

                            self._create_file_name(append = True)
                            self._save_output(self.dm_append, self.final_time_file_str)
                            
                        # delete checkpoint file 
                        if(self.checkpoint):
                            if os.path.isfile(self.checkpoint_file):
                                os.remove(self.checkpoint_file)
                                
                            if (hasattr(self, 'use_zarr')):
                                if os.path.isfile(self.checkpoint_file_zarr):
                                    os.remove(self.checkpoint_file_zarr)

                        self.count_append = 0

                        for attr in ('init_time_stamp','final_time_file_str', 'time_append'):
                            self.__dict__.pop(attr, None)

                        # if there's more to compute - call before return 
                        if (how_much_left < weight): 
                            self._call_recursive(how_much_left, weight, ds)

                        return self.dm_append

   
