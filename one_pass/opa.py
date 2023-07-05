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
#from crick import TDigest as TDigestCrick
from pytdigest import TDigest
#from dask.distributed import client, LocalCluster
from numcodecs import Blosc
import zarr 
import time 
import tqdm
import math 

from one_pass.convert_time import convert_time
from one_pass.check_stat import check_stat
from one_pass import util

class OpaMeta:

    """Meta data class. This class will only be used if checkpointing files larger than 2GB. 
    In this case, the numpy data will be checkpointed as zarr while only this metadata class will be pickled"""

    def __init__(self, Opa): 
        
        blacklist = str(Opa.stat + "_cum")
        for key, value in Opa.__dict__.items():
            if key not in blacklist:
                self.__setattr__(key, value)


class Opa:

    """ One pass algorithm class that will contain the rolling statistic """

    def __init__(self, user_request: Dict): 

        #start_time = time.time() 

        """ Initalisation
        
        Arguments 
        ---------
        : pass either a config file or dictionary 
        
        """
                
        request = util.parse_request(user_request)

        self._process_request(request)

        if(request.get("checkpoint")): # are we reading from checkpoint files each time? 
            self._check_checkpoint(request)
          
        check_stat(statistic = self.stat) # first check it's a valid statistic 

        self._check_attrs() # checks unique config attributes required for some statistics 

    def _check_checkpoint(self, request):
        """
        Takes user user request and checks if a checkpoint file exists from which to initalise the statistic from

        Arguments:
        ----------
        user_request : python dictionary containing the file path for the checkpoint file

        Returns:
        --------
        if checkpoint file is present: 
            self object is returned with old attributes
        """

        if (request.get("checkpoint_filepath")): 

            file_path = request.get("checkpoint_filepath")

            if (hasattr(self, 'use_zarr')): # if it doesn't have zarr, all data in the pickle 
                if(hasattr(self, 'variable')):
                    self.checkpoint_file_zarr = os.path.join(file_path, 'checkpoint_'f'{self.variable}_{self.stat_freq}_{self.output_freq}_{self.stat}.zarr')
                else: 
                    self.checkpoint_file_zarr = os.path.join(file_path, 'checkpoint_'f'{self.stat_freq}_{self.output_freq}_{self.stat}.zarr')

            if (hasattr(self, 'variable')): # if there are multiple variables in the file
                self.checkpoint_file = os.path.join(file_path, 'checkpoint_'f'{self.variable}_{self.stat_freq}_{self.output_freq}_{self.stat}.pkl')
            else:
                self.checkpoint_file = os.path.join(file_path, 'checkpoint_'f'{self.stat_freq}_{self.output_freq}_{self.stat}.pkl')

            if os.path.exists(self.checkpoint_file): # see if the checkpoint file exists

                f = open(self.checkpoint_file, 'rb')
                temp_self = pickle.load(f)
                f.close()

                for key in vars(temp_self):
                    self.__setattr__(key, vars(temp_self)[key])

                del(temp_self)

                #self._compare_request()
                
                if (hasattr(self, 'use_zarr')):
                    if os.path.exists(self.checkpoint_file_zarr): # if using a zarr file 

                        self.__setattr__(str(self.stat + "_cum"), zarr.load(store=self.checkpoint_file_zarr)) 
            else: 
                # using checkpoints but theres is no file 
                pass
        else:
            raise KeyError("need to pass a file path for the checkpoint file")

    def _process_request(self, request):

        """
        Assigns all self attributes from given dictionary or config request 

        Arguments:
        ----------
        user_request : python dictionary from config file or dictionary 

        Returns:
        --------
        self object with initalised attributes 
        """

        for key in request: 
            self.__setattr__(key, request[key])
    
    def _check_attrs(self):

        """ check for threshold """

        if(self.stat == "thresh_exceed"):
            if (hasattr(self, "threshold") == False):
                raise AttributeError('need to provide threshold of exceedance value')
            
        if(self.stat == "percentile"):
            if (hasattr(self, "percentile_list") == False):
                raise AttributeError('For the percentile statistic you need to provide a list of required percentiles,'
                                     'e.g. "percentile_list" : [0.01, 0.5, 0.99] for the 1st, 50th and 99th percentile,'
                                     'if you want the whole distribution, "percentile_list" : ["all"]')
            
            if (self.percentile_list[0] != "all"):
                for j in range(np.size(self.percentile_list)):
                    if(self.percentile_list[j] > 1): 
                        raise AttributeError('Percentiles must be between 0 and 1 or ["all"] for the whole distribution')
            
    def _compare_request(self):
        """checking that the request in the checkpoint file matches the incoming request, if not, take the incoming request"""
        pass # TODO: do you want this function? It shouldn't be needed if the checkpoint file path is changed but 
             # it's a good check

    ############### end if __init__ #####################

    def _initialise_time(self, time_stamp_min, time_stamp_tot_append):

        """
        Called when total time in minutes of the incoming time stamp is less than the timestep of the data. 
        Will initalise the time attributes related to the statistic call 

        Arguments:
        ----------
        time_stamp_min: total time in minutes of the incoming time stamp

        Returns:
        --------
        self.n_data : number of required pieces of data for the statistic to complete 
        """

        # the timestamp for the final dataArray = the first timestamp of that statistic 
        self.init_time_stamp = self.time_stamp 

        # calculated by MIN freq of stat / timestep min of data
        if ((self.stat_freq_min/self.time_step).is_integer()):
            if(self.stat_freq != "continuous"):
                if(time_stamp_min == 0 or (self.time_step/time_stamp_min).is_integer()): 
                    self.n_data = int(self.stat_freq_min/self.time_step) 
                else:
                    print('WARNING: timings of input data span over new statistic')
                    self.n_data = int(self.stat_freq_min/self.time_step) 
            else:
                self.n_data = math.ceil((self.stat_freq_min - time_stamp_min)/self.time_step)
        
        else:
            raise Exception('Frequency of the requested statistic (e.g. daily) must be wholly divisible by the timestep (dt) of the input data')

        try:
            getattr(self, "time_append") # if time_append already exisits it won't overwrite it 
        except AttributeError:

            ## looking at output freq - how many cum stats you want to save in one netcdf ##
            if (self.stat_freq == self.output_freq and self.stat_freq != "continuous"):
                self.time_append = 1 
                self.time_append_time_stamp = self.time_stamp

            elif(self.stat_freq != self.output_freq and self.stat_freq != "continuous"):
                # converting output freq into a number
                output_freq_min = convert_time(time_word = self.output_freq, time_stamp_input = self.time_stamp)[0]
                # eg. how many days requested: 7 days of saving with daily data
                
                self.time_append = ((output_freq_min - time_stamp_tot_append)/ self.stat_freq_min) # how many do you need to append
                self.time_append_time_stamp = self.time_stamp
                
                if(self.time_append < 1 and self.stat_freq != "continuous"): #output_freq_min < self.stat_freq_min
                    raise ValueError('Output frequency can not be less than frequency of statistic')

    def _initialise_attrs(self, ds):

        """
        Initialises data structure for cumulative stats 

        Returns:
        --------
        self.stat_cum : zero filled data array with the shape of the data compressed in the time dimension 
        """
        ds_size = ds.tail(time = 1)

        if ds.chunks is None:
            value = np.zeros_like(ds_size, dtype=np.float64) # forcing computation in float64
        else:
            value = da.zeros_like(ds_size, dtype=np.float64) # forcing computation in float64

        if(self.stat_freq == "continuous"):
            self.count_continuous = 0

        if(self.stat_freq == "continuous"):
            self.count_continuous = 0

        if(self.stat != "hist" and self.stat != "percentile"):
            
            ds_size = ds.tail(time = 1)

            self.__setattr__(str(self.stat + "_cum"), value)

            if(self.stat == "var"):
                # then also need to calculate the mean
                self.__setattr__("mean_cum", value)

            # for the standard deviation need both the mean and variance throughout
            elif(self.stat == "std"): # can reduce storage here by not saving both the cumulative variance and std
                self.__setattr__("mean_cum", value)
                self.__setattr__("var_cum", value)

            elif(self.stat == "min" or self.stat == "max"):
                self.__setattr__("timings", value)

            # else loop for histograms of percentile calculations that may require a different initial grid
        else: # TODO: complete for hist and percetiles 
            # if ds.chunks is None:
            #     pass 
            #     #value = np.zeros((1, np.size(ds.lat), np.size(ds.lon)))
            # else:
            #     pass 
            #     # value = da.zeros((1, np.size(ds.lat), np.size(ds.lon))) # keeping everything as a 3D array

            self.array_length = np.size(ds_size)
            digest_list = [dict() for x in range(self.array_length)] # list of dictionaries for each grid cell, lists preserve order 

            for j in tqdm.tqdm(range(self.array_length)):
                digest_list[j] = TDigest(compression = 15) # initalising digests and adding to list

            self.__setattr__(str(self.stat + "_cum"), digest_list)
            
            # first set percentile cum as flattened array with on axis length of number of percentiles
            #self.__setattr__(str(self.stat + "_cum"), da.tile(da.empty(self.array_length), (np.size(self.percentile_list), 1)))

    def _initialise(self, ds, time_stamp, time_stamp_min, time_stamp_tot_append):

        """ initalises both time attributes and attributes relating to the statistic """
        self.count = 0
        self.time_stamp = time_stamp

        self._initialise_time(time_stamp_min, time_stamp_tot_append)
        self._initialise_attrs(ds)


    def _check_n_data(self):

        """ checks if the attribute n_data is already there """
        try:
            getattr(self, "n_data")
            n_data_att_exist = True 
        except AttributeError:
            n_data_att_exist = False
        
        return n_data_att_exist
            
    def _should_initalise(self, time_stamp_min, proceed):

        """
        Checks to see if in the timestamp of the data is the 'first' in the requested statistic. If so, 
        should_init and proceed are set to true.  
        """ 

        should_init = False

        if(time_stamp_min < self.time_step): # this indicates that it's the first data  otherwise time_stamp will be larger
            should_init = True
            proceed = True 

        return proceed, should_init

    def _should_initalise_contin(self, time_stamp_min, proceed):
            
        proceed, should_init_time = self._should_initalise(time_stamp_min, proceed)

        should_init_value = False
        n_data_att_exist = self._check_n_data()

        if(n_data_att_exist):
            pass 
        else: 
            should_init_value = True 
            proceed = True

        return proceed, should_init_time, should_init_value

    def _remove_time_append(self):

        """removes 4 attributes relating to time_append (when output_freq > stat_freq) and removes checkpoint file"""

        for attr in ('dm_append','count_append', 'time_append', 'time_append_time_stamp'):
            self.__dict__.pop(attr, None)
    
        if(self.checkpoint):# delete checkpoint file 
            if os.path.isfile(self.checkpoint_file):
                os.remove(self.checkpoint_file)
            if os.path.isfile(self.checkpoint_file_zarr):
                os.remove(self.checkpoint_file_zarr)

    def _compare_old_timestamp(self, time_stamp, time_stamp_min, time_stamp_tot_append, proceed): 

        """This function compares the incoming time_stamp against one that may already been there from a checkpoint.
        If no previous time stamp it will simply assign the current time stamp to self.timestamp.
        If there is an old one (self.time_stamp), found from check_n_data, it will compare the difference in time between 
        the two time stamps. 4 options 
           1. Difference in time is equal to the time step. proceed = true 
           2. Time stamp in the future. If this is only slighly into the future (2*timestep), just throw a warning 
            if it's more into the future, throw error 
           3. The time stamp is the same, this will just pass through and will be caught in a later check
           4. Time stamp is in the past. This then depends on if there is a time_append option. If no, it will simply 
            delete the checkpoint file and carry on. If there is a time_append, it will check if it needs to 'roll back' 
            this appended data set. """

        n_data_att_exist = self._check_n_data()

        if(n_data_att_exist): 
            
            min_diff = time_stamp - self.time_stamp
            min_diff = min_diff.total_seconds() / 60 # calculates the difference in the old and new time stamps in minutes 

            if (min_diff == self.time_step): # option 1, it's the time step directly before, carry on 
                self.time_stamp = time_stamp # re-set self.time_stamp with the new time stamp 
                proceed = True # we're happy as it's simply the next step 

            elif(time_stamp > self.time_stamp): # option 2, it's a time step into the future - data mising 
                
                if abs(min_diff) < 2*self.time_step:
                    
                    print('Time gap at ' + str(time_stamp) + ' too large, there seems to be data missing, small enough to carry on')
                    self.time_stamp = time_stamp
                    proceed = True

                else: 
                    raise ValueError('Time gap too large, there seems to be some data missing')

            elif(time_stamp == self.time_stamp):
                pass # this should get caught in 'already seen'
            
            elif(time_stamp < self.time_stamp): # option 4, it's a time stamp from way before, back 
                
                if(self.stat_freq != "continuous"):
                    time_stamp_min_old = convert_time(time_word = self.stat_freq, time_stamp_input = self.time_stamp)[1]

                    if(abs(min_diff) > time_stamp_min_old):
                        # here it's gone back to before the stat it was previously calculating so delete attributes 
                        for attr in ('n_data', 'count'):
                            self.__dict__.pop(attr, None)

                else:
                    for attr in ('n_data', 'count'): # always delete these for continuous 
                        self.__dict__.pop(attr, None)

                    # else: if it just goes backwards slightly in the stat you're already computing, it will either re-int later
                    # or it will be caught by 'already seen' later on
                try:
                    getattr(self, "time_append")
                    if(self.time_append > 1): # if time_append == 1 then this isn't a problem

                        # first check if you've gone even further back than the original time append 
                        if (time_stamp < self.time_append_time_stamp):
                            print("removing checkpoint as going back to before original time append")
                            self._remove_time_append()

                        else: # you've got back sometime within the time_append window
                            output_freq_min = convert_time(time_word = self.output_freq, time_stamp_input = self.time_stamp)[0]
                            
                            full_append = (output_freq_min/self.stat_freq_min) # time (units of stat_append) for how long time_append should be
                            start_int = full_append - self.time_append # if starting mid way through a output_freq, gives you the start of where you are
                            start_new_int = (time_stamp_tot_append / self.stat_freq_min) # time (units of stat_append) for for where time_append should start with new timestamp 

                            if ((start_int == start_new_int) or (start_new_int < start_int)): # new time step == beginning of time append 
                                
                                print("removing checkpoint as starting from beginning of time append")
                                self._remove_time_append()

                            else: # rolling back the time append calculation 
                                # now you want to check if it's gone back an exact amount 
                                # i.e. it's gone back to the beginning of a new stat or mid way through 
                                
                                should_init = self._should_initalise(time_stamp_min, proceed)[1]

                                if(should_init): # if it's initalising it's fine, roll back
                                    
                                    gap = start_new_int - start_int
                                    roll_back = int(abs(np.size(self.dm_append.time) - gap)) # how many do you need to append
                                    print('rolling back time_append by ..', roll_back)
                                    self.dm_append.isel(time=slice(0,roll_back))
                                    self.count_append -= int(roll_back) 
                                
                                else: # can't roll back if this new time_stamp isn't the start of a stat, so deleting
                                    print("removing previous data in time_append as can't initalise from this point")
                                    self._remove_time_append()

                except AttributeError: 
                    pass 
            
        else: 
            self.time_stamp = time_stamp # very first in the series 

        return proceed
    
    def _check_have_seen(self, time_stamp_min):

        """check if the OPA has already 'seen' the data 
            done by comparing what the count of the current time stamp is against the actual count 
            returns, True or False"""

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
        Function to check the incoming timestamps of the data and check if it is the first one of the required statistic.
        First time stamps will pass through 'compare old timestamps' which will compare the new timestamps against 
        any previous timestamps stored in a checkpoint file. For details see function. If this passes, the timestamp will 
        be checked to see if it's the first in the required statstic. If so, initalise the function with the required variables.
        If not, the function will also check if it's either 'seen' the data before or if it should simply be skipped as it's half
        way through a required statistic. 

        Args:
        ds: Incoming xarray dataArray with associated timestamp(s)
        weight: the length along the time-dimension of the incoming array 

        Output:
        If function is initalised it will assign the attributes: 
        n_data: number of pieces of information required to make up the requested statistic 
        count: how far through the statistic you are, when initalised, this will be set to 1
        stat_cum: will set an empty array of the correct dimensions, ready to be filled with the statistic 
        ds may change length if the time stamp corresponding to the start of the statistic corresponds to half way through ds.

        Checks:
        If it is not the first timestamp of the statistic, it will check: 
        - that the attribute n_data has already been assigned, otherwise will realise that this data doesn't correspond to this statistic
        If the first piece of incoming data doesn't correspond to the initial data, and weight is greater than 1, 
        it will check the other incoming pieces of data to see if they correspond to the initial statistic
        """

        time_stamp_sorted = sorted(ds.time.data) # assuming that incoming data has a time dimension
        time_stamp_list = [pd.to_datetime(x) for x in time_stamp_sorted]
        index = 0 
        proceed = False 
            
        while(proceed == False) and (index < weight): 
            
            time_stamp = time_stamp_list[index]
            
            """Outputs of convert time: 
            self.stat_freq_min = the number of minutes in the statistic requested, e.g. daily = 60*24 
            time_stamp_min = the number of minutes in the given timestamp based on your requested stat_freq. 
                                so if you're timestamp is 15 minutes into a new day and you requested stat_freq = daily, then this = 15
            time_stamp_tot_append = the number of 'stat_freq' in minutes of the time stamp already completed (only used for appending data)
            """
            if(self.stat_freq != "continuous"):
                self.stat_freq_min, time_stamp_min, time_stamp_tot_append = convert_time(time_word = self.stat_freq, time_stamp_input = time_stamp)
            else: 
                self.stat_freq_min, time_stamp_min, time_stamp_tot_append = convert_time(time_word = self.output_freq, time_stamp_input = time_stamp)
            
            proceed = self._compare_old_timestamp(time_stamp, time_stamp_min, time_stamp_tot_append, proceed)

            if(self.stat_freq_min < self.time_step):
                raise ValueError('time_step too large for requested statistic')
            
            if(self.stat_freq != "continuous"):
                proceed, should_init = self._should_initalise(time_stamp_min, proceed) 
                if(should_init):
                    self._initialise(ds, time_stamp, time_stamp_min, time_stamp_tot_append)
            else: 
                proceed, should_init_time, should_init = self._should_initalise_contin(time_stamp_min, proceed)
                
                if(should_init):
                    self._initialise(ds, time_stamp, time_stamp_min, time_stamp_tot_append)
                    print('initialising continuous statistic')

                elif(should_init_time):
                    self.count = 0
                    self.time_stamp = time_stamp
                    self._initialise_time(time_stamp_min, time_stamp_tot_append)

            already_seen = self._check_have_seen(time_stamp_min) # checks count 
            
            if(already_seen): 
                print('pass on this data as already seen this data')

            n_data_att_exist = self._check_n_data() # this will change from False to True if it's just been initalised 
            if (n_data_att_exist == False):
                print('passing on this data as its not the initial data for the requested statistic')
            
            index = index + 1 

        if (index > 1) and (proceed): # it will enter this loop it's skipping the first few time steps that come through
            index = index - 1 
            ds = ds.isel(time=slice(index, weight))
            weight = weight - index 
            
        return ds, weight, already_seen, n_data_att_exist, time_stamp_list

    def _check_variable(self, ds):

        """ Checks if the incoming data is an xarray dataArray. If it's a dataSet, will convert """

        try:
            getattr(ds, "data_vars") # this means it a data_set
            self.data_set_attr = ds.attrs #keeping the attributes of the full data_set to append to the final data_set
            try:
                ds = getattr(ds, self.variable) # converts to a dataArray

            except AttributeError:
                raise Exception('If passing dataSet need to provide the correct variable/variableeter, opa can only use one variable at the moment')
        
        except AttributeError:
            self.data_set_attr = ds.attrs # still extracting attributes from dataArray here 
            pass # data already at data_array

        return ds
    
    def _check_raw(self, ds, weight):
        
        """This function is called if the user has requested stat: 'raw'. 
        This means that they do not want to compute any statstic 
        over any frequency, we will simply save the incoming data. """
        
        final_time_file_str = self._create_none_file_name(ds, weight)
                                
        dm = self._create_none_data_set(ds) # this will convert the dataArray back into a dataSet with the metadata of the dataSet

        if(self.save == True):            
            self._save_output(dm, ds, final_time_file_str)

        return dm 
    
    def _check_num_time_stamps(self, ds):

        """Check how many time stamps are in the incoming data. 
        It's possible that the GSV interface will have multiple messages"""

        #time_stamp_list = sorted(ds.time.data) # this method could be very different for a GRIB file, need to understand time stamps
        time_num = np.size(ds.time.data)

        return time_num


    def _two_pass_mean(self, ds): 
        
        """ computes normal mean using two pass """ 
        
        ax_num = ds.get_axis_num('time')
        temp = np.mean(ds, axis = ax_num, dtype = np.float64, keepdims=True)  
        
        return temp

    def _two_pass_var(self, ds): 
        
        """ computes normal variance using two pass, setting ddof = 1 """ 

        ax_num = ds.get_axis_num('time')
        temp = np.var(ds, axis = ax_num, dtype = np.float64, keepdims=True, ddof = 1)  

        return temp

    def _update_mean(self, ds, weight):

        """ computes one pass mean with weight corresponding to the number of timesteps being added  """ 

        self.count = self.count + weight
        
        if (weight == 1):
            mean_cum = self.mean_cum + weight*(ds - self.mean_cum) / (self.count) 
        else:
            temp_mean = self._two_pass_mean(ds) # compute two pass mean first
            mean_cum = self.mean_cum + weight*(temp_mean - self.mean_cum) / (self.count) 

        self.mean_cum = mean_cum.data

    def _update_var(self, ds, weight):

        """ computes one pass variance with weight corresponding to the number of timesteps being added
        don't need to update count as that is done in mean""" 

        old_mean = self.mean_cum  # storing 'old' mean temporarily

        if(weight == 1):
            self._update_mean(ds, weight)
            var_cum = self.var_cum + weight*(ds - old_mean)*(ds - self.mean_cum)
        
        else:
            temp_mean = self._two_pass_mean(ds) # two-pass mean
            temp_var = (self._two_pass_var(ds))*(weight-1) # two pass variance
            # see paper Mastelini. S
            var_cum = self.var_cum + temp_var + np.square(old_mean - temp_mean)*((self.count*weight)/(self.count+weight))
            
            self._update_mean(ds, weight)
            
        if (self.count == self.n_data):
            var_cum = var_cum/(self.count - 1) # using sample variance NOT population variance

        self.var_cum = var_cum.data

    def _update_std(self, ds, weight):
        
        """ computes one pass standard deviation with weight corresponding to the number of timesteps being added
            Uses one pass variance then square root at the end of the statistic  """ 

        self._update_var(ds, weight)
        self.std_cum = np.sqrt(self.var_cum)

    def _update_min(self, ds, weight):

        """ finds the cumulative minimum values of the data along with an array of timesteps
            corresponding to the minimum values  """ 
                
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
            # this gives the new self.min_cum number when the  condition is FALSE (location at which to preserve the objects values)
            ds = ds.where(ds < self.min_cum, self.min_cum)

        ds_time = ds_time.astype('datetime64[ns]') # convert to datetime64 for saving

        self.count = self.count + weight
        self.min_cum = ds #running this way around as Array type does not have the function .where, this only works for data_array
        self.timings = ds_time

        return

    def _update_max(self, ds, weight):

        """ finds the cumulative maximum values of the data along with an array of timesteps
            corresponding to the maximum values  """ 

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
            ds_time = xr.zeros_like(ds) # now this will have dimensions 1,lat,lon

            for i in range(0, weight):
                timestamp = np.datetime_as_string((timings.values[i]))
                ds_time = ds_time.where(max_index != i, timestamp)

        if(self.count > 0):
            self.max_cum['time'] = ds.time
            self.timings['time'] = ds.time
            ds_time = ds_time.where(ds > self.max_cum, self.timings)
            # this gives the new self.max_cum number when the  condition is FALSE (location at which to preserve the objects values)
            ds = ds.where(ds > self.max_cum, self.max_cum)

        ds_time = ds_time.astype('datetime64[ns]') # convert to datetime64 for saving

        self.count = self.count + weight
        self.max_cum = ds 
        self.timings = ds_time

        return

    def _update_threshold(self, ds, weight):

        """ creates an array with the frequency that a threshold has been exceeded  """ 
        
        if(weight > 1):

            ds = ds.where(ds < self.threshold, 1)
            ds = ds.where(ds >= self.threshold, 0)
            ds = np.sum(ds, axis = 0, keepdims = True) #try slower np version that preserves dimensions
            ds = self.thresh_exceed_cum + ds

        else:
            if(self.count > 0):
                self.thresh_exceed_cum['time'] = ds.time

            ds = ds.where(ds < self.threshold, self.thresh_exceed_cum + 1)
            ds = ds.where(ds >= self.threshold, self.thresh_exceed_cum)

        self.count = self.count + weight
        self.thresh_exceed_cum = ds 

        return

    def _update_percentile(self, ds, weight=1):

        """currently sequential loop that updates the digest for each grid point"""
        
        if(weight == 1):

            ds_values = np.reshape(ds.values, self.array_length) # here we have extracted the underlying np array, this might be a problem with dask? 
        
            for j in tqdm.tqdm(range(self.array_length)): # this is looping through every grid cell 
                # using crick or pytdigest
                self.percentile_cum[j].update(ds_values[j])

        else: 

            ds_values = ds.values.reshape((weight, -1))

            for j in tqdm.tqdm(range(self.array_length)):
                # using crick or pytdigest
                self.percentile_cum[j].update(ds_values[:,j]) # update multiple values 

        self.count = self.count + weight
        
        return 

    def _get_percentile(self, ds):

        """converts digest functions into percentiles """
        
        if self.percentile_list[0] == 'all':
            self.percentile_list = np.linspace(0, 100, 101)
        
        for j in range(self.array_length):
            # for crick 
            # self.percentile_cum[j] = self.percentile_cum[j].quantile(self.percentile_list) # if there are 4 percentiles, : should be 4 
            self.percentile_cum[j] = self.percentile_cum[j].inverse_cdf(self.percentile_list) # if there are 4 percentiles, : should be 4 

        self.percentile_cum = np.transpose(self.percentile_cum)

        # reshaping percentile cum into the correct shape 
        ds_size = ds.tail(time = 1)
        value = da.zeros_like(ds_size, dtype=np.float64) # forcing computation in float64
        final_size = da.concatenate([value] * np.size(self.percentile_list), axis=0)
        
        self.percentile_cum = np.reshape(self.percentile_cum, np.shape(final_size))

        self.percentile_cum = np.expand_dims(self.percentile_cum, axis = 0) # adding axis for time 

        return 

    def _update(self, ds, weight=1):

        """ depending on the requested statistic will send data to the correct function """ 

        #TODO: can probably make this cleaner / auto? 

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
            self._update_percentile(ds, weight)
            

    def _load_dask(self):

        """ computing dask lazy operations and calling data into memory """

        start_time = time.time()
        
        #print('loading dask')
        self.__setattr__(str(self.stat + "_cum"), self.__getattribute__(str(self.stat + "_cum")).compute())
        
        end_time = time.time() - start_time
        print(np.round(end_time,4), 's to load dask')
        
    def _write_checkpoint(self):

        """write checkpoint file. First checks the size of the class and if it can fit in memory. 
        If it's larger than 1.6 GB (pickle limit is 2GB) it will save the main climate data to zarr
        with only meta data stored as pickle """
        
        if hasattr(self.__getattribute__(str(self.stat + "_cum")), 'compute'):
            self._load_dask() # first load data into memory 

            # can maybe just use the line below: 
        total_size = sys.getsizeof(self.__getattribute__(str(self.stat + "_cum")))/(10**9) # total size in GB

        if (self.stat == "var" or self.stat == "min" or self.stat == "max"): # they have the arrays repeated 
            total_size *= 2
        if(self.stat == "std"): 
            total_size *= 3

        if(total_size < 1.6): # limit on a pickle file is 2GB 

            with open(self.checkpoint_file, 'wb') as file: 
                #start_time = time.time()
                pickle.dump(self, file)
                
                #end_time =  time.time() - start_time
                #print(end_time, 's to save pickle checkpoint')

        else: 
            self.use_zarr = True
            compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)

            if(hasattr(self, 'checkpoint_file_zarr')):
                zarr.array(self.__getattribute__(str(self.stat + "_cum")), store = self.checkpoint_file_zarr, compressor=compressor, overwrite = True)
            else: 
                if(hasattr(self, 'variable')):
                    self.checkpoint_file_zarr = os.path.join(self.checkpoint_filepath, 'checkpoint_'f'{self.variable}_{self.stat_freq}_{self.output_freq}_{self.stat}.zarr')
                else: 
                    self.checkpoint_file_zarr = os.path.join(self.checkpoint_filepath, 'checkpoint_'f'{self.stat_freq}_{self.output_freq}_{self.stat}.zarr')

                #start_time = time.time() 
                zarr.array(self.__getattribute__(str(self.stat + "_cum")), store = self.checkpoint_file_zarr, compressor=compressor, overwrite = True)
                #end_time = time.time() - start_time
                #print(end_time, 's to save zarr checkpoint')
            
            # now just pickle the rest of the meta data 
            opa_meta = OpaMeta(self) # creates seperate class for the meta data 
            
            with open(self.checkpoint_file, 'wb') as file: 
                pickle.dump(opa_meta, file)
    
    def _create_none_data_set(self, ds):
        
        """creates an xarray dataSet for the option of stat: "none". Here the dataSet will be exactly the same as the 
        original, but only containing the requested variable / variable from the config.yml"""
        
        try:
            ds = getattr(ds, self.variable) 
            dm = ds.to_dataset(dim = None, name = self.variable)
        except AttributeError: 
            dm = ds.to_dataset(dim = None, name = ds.name)    
        
        dm = dm.assign_attrs(self.data_set_attr)

        return dm 
    
    def _create_data_set(self, final_stat, final_time_stamp, ds):

        """ creates xarray dataSet object with final data and original metadata  """

        ds = ds.tail(time = 1) # compress the dataset down to 1 dimension in time 
        ds = ds.assign_coords(time = (["time"], [final_time_stamp], ds.time.attrs)) # re-label the time coordinate 
            
        if (self.stat == "percentile"):

            ds = ds.expand_dims(dim={"percentile": np.size(self.percentile_list)}, axis=1)
            ds = ds.assign_coords(percentile = ("percentile", np.array(self.percentile_list))) # re-label the time coordinate 
            
        dm = xr.Dataset(
        data_vars = dict(
                [(str(ds.name), (ds.dims, final_stat, ds.attrs))],   # need to add variable/variable attributes CHANGED
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

    def _data_output(self, ds, time_word = None):

        """ gathers final data and meta data for the final xarray dataSet  """

        final_stat = None
        
        final_stat = self.__getattribute__(str(self.stat + "_cum"))

        if(self.stat == "min" or self.stat == "max" or self.stat == "thresh_exceed"):
            final_stat = final_stat.data

        final_time_file_str = self._create_file_name(time_word = time_word)
        final_time_stamp = self._create_final_timestamp(time_word = time_word)

        #try: # THIS IS NOW INCLUDED IN CHECK+DATASET FUCNTION 
        #    getattr(self, "data_set_attr") # if it was originally a data set
        #except AttributeError: # only looking at a data_array
        #    self.data_set_attr = ds.attrs #both data_set and data_array will have matching attribs

        self.data_set_attr["OPA"] = str(self.stat_freq + " " + self.stat + " " + "calculated using one-pass algorithm")
        ds.attrs["OPA"] = str(self.stat_freq + " " + self.stat + " " + "calculated using one-pass algorithm")

        dm = self._create_data_set(final_stat, final_time_stamp, ds)

        return dm, final_time_file_str

    def _data_output_append(self, dm):

        """  appeneds final dataSet along the time dimension if stat_output is larger than the 
        requested stat_freq. It also sorts along the time dimension
        to ensure data is also increasing in time """

        dm_append = xr.concat([self.dm_append, dm], "time") 
        self.dm_append = dm_append.sortby("time")
        self.count_append = self.count_append + 1 


    def _create_final_timestamp(self, time_word = None):

        """Creates the final time stamp for each accumulated statistic. For now, simply 
        using the time stamp of the first incoming data of that statistic"""

        final_time_stamp = self.init_time_stamp

        return final_time_stamp 
    
    def _create_none_file_name(self, ds, weight):

        """ creates the final file name for the netCDF file. If append is True 
        then the file name will span from the first requested statistic to the last. 
        time_word corresponds to the continous option which outputs checks every month"""

        final_time_file_str = None
        time_stamp_sorted = sorted(ds.time.data) # assuming that incoming data has a time dimension
        time_stamp_list = [pd.to_datetime(x) for x in time_stamp_sorted]
        
        if (weight > 1):
            final_time_file_str = time_stamp_list[0].strftime("%Y_%m_%d_T%H_%M") + "_to_" + time_stamp_list[-1].strftime("%Y_%m_%d_T%H_%M")
        else:
            final_time_file_str = time_stamp_list[0].strftime("%Y_%m_%d_T%H_%M")

        return final_time_file_str
    

    def _create_file_name(self, append = False, time_word = None):

        """ creates the final file name for the netCDF file. If append is True 
        then the file name will span from the first requested statistic to the last. 
        time_word corresponds to the continous option which outputs checks every month"""

        final_time_file_str = None

        if (time_word != None):
            self.stat_freq_old = self.stat_freq
            self.stat_freq = time_word

        if (append):

            if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or self.stat_freq == "6hourly" or self.stat_freq == "12hourly"):
                self.final_time_file_str = self.final_time_file_str + "_to_" + self.time_stamp.strftime("%Y_%m_%d_T%H")

            elif (self.stat_freq == "daily" or self.stat_freq == "weekly"):
                self.final_time_file_str = self.final_time_file_str + "_to_" + self.time_stamp.strftime("%Y_%m_%d")

            elif (self.stat_freq == "monthly" or self.stat_freq == "3monthly"):
                self.final_time_file_str = self.final_time_file_str + "_to_" + self.time_stamp.strftime("%Y_%m")

            elif (self.stat_freq == "annually"):
                self.final_time_file_str = self.final_time_file_str + "_to_" + self.time_stamp.strftime("%Y")
        else:
            if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or self.stat_freq == "6hourly" or self.stat_freq == "12hourly"):
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

    def _save_output(self, dm, ds, final_time_file_str):

        """  Creates final file name and path and saves final dataSet """
        if(self.stat == 'raw'):
            if (hasattr(self, 'variable')): # if there are multiple variables/variables in the file
                file_name = os.path.join(self.out_filepath, f'{final_time_file_str}_{self.variable}_raw_data.nc')
            else:
                file_name = os.path.join(self.out_filepath, f'{final_time_file_str}_{ds.name}_raw_data.nc')

        else: 
            if (hasattr(self, 'variable')): # if there are multiple variables/ variable in the file

                file_name = os.path.join(self.out_filepath, f'{final_time_file_str}_{self.variable}_{self.stat_freq}_{self.stat}.nc')
            else:
                file_name = os.path.join(self.out_filepath, f'{final_time_file_str}_{ds.name}_{self.stat_freq}_{self.stat}.nc')

        start_time = time.time()

        dm.to_netcdf(path = file_name, mode = 'w') # will re-write the file if it is already there
        dm.close()
        
        end_time = time.time() - start_time
        print('finished saving in', np.round(end_time,4) ,'s')


    def _call_recursive(self, how_much_left, weight, ds):

        """if there is more data given than what is required for the statistic,
          it will make a recursive call to itself """
        
        ds = ds.isel(time=slice(how_much_left, weight))
        Opa.compute(self, ds) # calling recursive function

    ############## defining class methods ####################
    
    def compute(self, ds):
    
        """  Actual function call  """
        
        ds = self._check_variable(ds) # convert from a data_set to a data_array if required
 
        weight = self._check_num_time_stamps(ds) # this checks if there are multiple time stamps in a file and will do two pass statistic

        if (self.stat == "raw"):

            dm = self._check_raw(ds, weight)
            return dm 
        
        ds, weight, already_seen, n_data_att_exist, time_stamp_list = self._check_time_stamp(ds, weight) # check the time stamp and if the data needs to be initalised 

        if(already_seen): # check if data has been 'seen', will only skip if data doesn't get re-initalised
            return    
        
        if (n_data_att_exist == False):
            return
                    
        how_much_left = (self.n_data - self.count) # how much is let of your statistic to fill
        

        if (how_much_left >= weight): # will not span over new statistic

            self.time_stamp = time_stamp_list[-1] # moving the time stamp to the last of the set 
            
            self._update(ds, weight)  # update rolling statistic with weight

            if(self.checkpoint == True and self.count < self.n_data):
                
                self._write_checkpoint() # this will not be written when count == ndata 

        elif(how_much_left < weight): # this will span over the new statistic 

            ds_left = ds.isel(time=slice(0, how_much_left)) # extracting time until the end of the statistic
            self.time_stamp = time_stamp_list[how_much_left] # CHECK moving the time stamp to the last of the set
            # update rolling statistic with weight of the last few days  - # still need to finish the statistic (see below)
            self._update(ds_left, how_much_left)

        if (self.count == self.n_data and self.stat_freq == "continuous"):

            dm, final_time_file_str = self._data_output(ds, time_word = self.output_freq)
            self._save_output(dm, ds, final_time_file_str)
            self.count_continuous = self.count_continuous + self.count

            if (how_much_left < weight): # if there's more to compute - call before return 
                self._call_recursive(how_much_left, weight, ds)

            return dm 

        if (self.count == self.n_data and self.stat_freq != "continuous"):   # when the statistic is full

            if(self.stat == "percentile"): 
                self._get_percentile(ds)

            dm, final_time_file_str = self._data_output(ds) # output as a dataset 

            if(self.time_append == 1): #output_freq_min == self.stat_freq_min
                if(self.save == True):
                    self._save_output(dm, ds, final_time_file_str)

                if(self.checkpoint == True): # and write_check == True):# delete checkpoint file 
                    if os.path.isfile(self.checkpoint_file):
                        os.remove(self.checkpoint_file)
                    
                    if (hasattr(self, 'use_zarr')):
                        if os.path.isfile(self.checkpoint_file_zarr):
                            os.remove(self.checkpoint_file_zarr)
                    #else:
                    #    print("Error: %s file not found" % self.checkpoint_file)

                if (how_much_left < weight): # if there's more to compute - call before return 
                    self._call_recursive(how_much_left, weight, ds)
                
                return dm

            elif(self.time_append > 1): #output_freq_min > self.stat_freq_min

                if (hasattr(self, 'count_append') == False or self.count_append == 0): # first time it appends
                    self.count_append = 1
                    self.dm_append = dm # storing the data_set ready for appending
                    self.final_time_file_str = final_time_file_str
                    
                    if(self.checkpoint):

                        self._write_checkpoint()

                    if (how_much_left < weight): # if there's more to compute - call before return 
                        self._call_recursive(how_much_left, weight, ds)
                    
                    return self.dm_append
                
                elif(self.count_append < self.time_append):
                    
                    # append data array with new time outputs and update count_append 
                    self._data_output_append(dm)
                    
                    if(self.count_append < self.time_append): # if this is still true 

                        if(self.checkpoint):

                            self._write_checkpoint()

                        if (how_much_left < weight): # if there's more to compute - call before return 
                            self._call_recursive(how_much_left, weight, ds)

                        return self.dm_append 

                    elif(self.count_append == self.time_append):

                        if(self.save): # change file name 

                            self._create_file_name(append = True)
                            self._save_output(self.dm_append, ds, self.final_time_file_str)

                        if(self.checkpoint):# delete checkpoint file 
                            if os.path.isfile(self.checkpoint_file):
                                os.remove(self.checkpoint_file)
                                
                            if (hasattr(self, 'use_zarr')):
                                if os.path.isfile(self.checkpoint_file_zarr):
                                    os.remove(self.checkpoint_file_zarr)
                            #else:
                            #    print("Error: %s file not found" % self.checkpoint_file)

                        self.count_append = 0

                        for attr in ('init_time_stamp','final_time_file_str', 'time_append'):
                            self.__dict__.pop(attr, None)

                        if (how_much_left < weight): # if there's more to compute - call before return 
                            self._call_recursive(how_much_left, weight, ds)

                        return self.dm_append

   
