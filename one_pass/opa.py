from typing import Dict
import os 
from sys import exit
import pickle 
import numpy as np
import xarray as xr
import pandas as pd
import dask 
import dask.array as da
import time 
from crick import TDigest as TDigestCrick

#from dask.distributed import client, LocalCluster

from one_pass.convert_time import convert_time
from one_pass.check_stat import check_stat
from one_pass import util

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

                self._compare_request()
                self._check_thresh()
            else: 
                # using checkpoints but theres is no file 
                pass
        else:
            raise KeyError("need to pass a file path for the checkpoint file")


    def _process_request(self, request):
        """
        If no checkpoint file exists or checkpoint = False, will assign attributes of self from the given dict 

        Arguments:
        ----------
        user_request : python dictionary from config file or dictionary 

        Returns:
        --------
        self object with initalised attributes 
        """

        for key in request: 
            self.__setattr__(key, request[key])
    
        self._check_thresh()

    def _check_thresh(self):
        if(self.stat == "thresh_exceed"):
            if (hasattr(self, "threshold") == False):
                raise AttributeError('need to provide threshold of exceedance value')
            
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
                self.n_data = int((self.stat_freq_min - time_stamp_min)/self.time_step)
        
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
                output_freq_min = convert_time(time_word = self.output_freq, time_stamp_input = self.time_stamp, time_step_input = self.time_step)[0]
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

        if(self.stat_freq == "continuous"):
            self.count_continuous = 0

        if(self.stat != "hist" or self.stat != "percentile"):
            
            #value = np.zeros_like(ds_size, dtype=np.float64) # forcing computation in float64
            value = da.zeros_like(ds_size, dtype=np.float64) # forcing computation in float64

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
        elif(self.stat == "percentile"): 

            # do you even need to check dimensions here? 
            if np.size(ds.dims) > 3: 
                # TODO: # panic, what other dimensions do you have? 
                raise ValueError("dimensions greater than 3, need to fix this")
            elif np.size(ds.dims) == 1: 
                raise ValueError("well you have a massive problem if you only have 1 dimension")  

            self.array_length = np.size(ds_size)
            digest_list = [dict() for x in range(self.array_length)] # list of dictionaries for each grid cell, lists preserve order 
            percen_list = [dict() for x in range(self.array_length)]

            for j in range(self.array_length):
                digest = TDigestCrick() # initalising digests and adding to list
                digest_list[j] = digest

            self.__setattr__("digest_cum", digest_list)
            self.__setattr__(str(self.stat + "_cum"), percen_list)

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
                    print('Time gap too large, there seems to be data missing, small enough to carry on')
                    self.time_stamp = time_stamp
                    proceed = True

                else: 
                    raise ValueError('Time gap too large, there seems to be some data missing')

            elif(time_stamp == self.time_stamp):
                pass # this should get caught in 'already seen'
            
            elif(time_stamp < self.time_stamp): # option 4, it's a time stamp from way before, back 
                
                if(self.stat_freq != "continuous"):
                    time_stamp_min_old = convert_time(time_word = self.stat_freq, time_stamp_input = self.time_stamp, time_step_input = self.time_step)[1]

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
                            output_freq_min = convert_time(time_word = self.output_freq, time_stamp_input = self.time_stamp, time_step_input = self.time_step)[0]
                            
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
                self.stat_freq_min, time_stamp_min, time_stamp_tot_append = convert_time(time_word = self.stat_freq, time_stamp_input = time_stamp, time_step_input = self.time_step)
            else: 
                self.stat_freq_min, time_stamp_min, time_stamp_tot_append = convert_time(time_word = self.output_freq, time_stamp_input = time_stamp, time_step_input = self.time_step)
            
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
        
        check_stat(statistic = self.stat) # first check it's a valid statistic 

        try:
            getattr(ds, "data_vars") # this means it a data_set
            self.data_set_attr = ds.attrs #keeping the attributes of the full data_set to append to the final data_set
            try:
                ds = getattr(ds, self.variable) # converts to a dataArray

            except AttributeError:
                raise Exception('If passing dataSet need to provide the correct variable, opa can only use one variable at the moment')
        
        except AttributeError:
            pass # data already at data_array

        return ds


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

        """ computes one pass variance with weight corresponding to the number of timesteps being added  """ 

        old_mean = self.mean_cum  # storing 'old' mean temporarily

        if(weight == 1):
            self._update_mean(ds, weight)
            var_cum = self.var_cum + weight*(ds - old_mean)*(ds - self.mean_cum)
        else:
            temp_mean = self._two_pass_mean(ds) # two-pass mean
            self._update_mean(temp_mean, weight) # update self.mean_cum
            temp_var = self._two_pass_var(ds) # two pass variance
            # see paper Mastelini. S
            var_cum = self.var_cum + temp_var + np.square(old_mean - temp_mean)*((self.count - weight)*weight/self.count)

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
        
        if(weight == 1):
            ds_values = np.reshape(ds.values, self.array_length) # here we have extracted the underlying np array, this might be a problem with dask? 
        else: 
            pass 

        for j in range(self.array_length):
            digest = self.digest_cum[j]
            digest.update(ds_values[j])
            self.digest_cum[j] = digest

        return 

    def get_percentile(self, ds):
        
        if(np.size(self.percentiles) == 1):
            
            for j in range(self.array_length):
                digest = self.digest_cum[j] # extract digest 
                self.percentile_cum[j] = digest.quantile(self.percentiles)

        ds_size = ds.tail(time = 1)
        self.percentile_cum = np.reshape(self.percentile_cum, np.shape(ds_size))

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

        """Function to load the dask array and call it into memory """
        
        print('computing dask')
        self.__getattribute__(str(self.stat + "_cum")).compute() # maybe check if it's dask to begin with 
        print('finished computing dask')

    def _write_checkpoint(self):

        """write checkpoint file """
        
        self._load_dask()

        with open(self.checkpoint_file, 'wb') as file: 
            pickle.dump(self, file)

    def _create_data_set(self, final_stat, final_time_stamp, ds):

        """ creates xarray dataSet object with final data and original metadata  """

        ds = ds.tail(time = 1) # compress the dataset down to 1 dimension in time 
        ds = ds.assign_coords(time = (["time"], [final_time_stamp], ds.time.attrs)) # re-label the time coordinate 

        dm = xr.Dataset(
        data_vars = dict(
                [(ds.name, (ds.dims, final_stat, ds.attrs))],   # need to add variable attributes CHANGED
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

        try:
            getattr(self, "data_set_attr") # if it was originally a data set
        except AttributeError: # only looking at a data_array
            self.data_set_attr = ds.attrs #both data_set and data_array will have matching attribs

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


    def _create_file_name(self, append = False, time_word = None):

        """ creates the final file name for the netCDF file. If append is True 
        then the file name will span from the first requested statistic to the last. 
        time_word corresponds to the continous option which outputs checks every month"""

        final_time_file_str = None

        if (time_word != None):
            self.stat_freq_old = self.stat_freq
            self.stat_freq = time_word

        if (append):

            if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or self.stat_freq == "6hourly"):
                self.final_time_file_str = self.final_time_file_str + "_to_" + self.time_stamp.strftime("%Y_%m_%d_T%H")

            elif (self.stat_freq == "daily" or self.stat_freq == "weekly"):
                self.final_time_file_str = self.final_time_file_str + "_to_" + self.time_stamp.strftime("%Y_%m_%d")

            elif (self.stat_freq == "monthly" or self.stat_freq == "3monthly"):
                self.final_time_file_str = self.final_time_file_str + "_to_" + self.time_stamp.strftime("%Y_%m")

            elif (self.stat_freq == "annually"):
                self.final_time_file_str = self.final_time_file_str + "_to_" + self.time_stamp.strftime("%Y")
        else:
            if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or self.stat_freq == "6hourly"):
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

        if (hasattr(self, 'variable')): # if there are multiple variables in the file

            file_name = os.path.join(self.out_filepath, f'{final_time_file_str}_{self.variable}_{self.stat_freq}_{self.stat}.nc')
        else:
            file_name = os.path.join(self.out_filepath, f'{final_time_file_str}_{ds.name}_{self.stat_freq}_{self.stat}.nc')

        dm.to_netcdf(path = file_name, mode = 'w') # will re-write the file if it is already there
        dm.close()
        print('finished saving')


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
                self._get_percentile()

            dm, final_time_file_str = self._data_output(ds) # output as a dataset 

            if(self.time_append == 1): #output_freq_min == self.stat_freq_min
                if(self.save == True):
                    self._save_output(dm, ds, final_time_file_str)

                if(self.checkpoint == True): # and write_check == True):# delete checkpoint file 
                    if os.path.isfile(self.checkpoint_file):
                        os.remove(self.checkpoint_file)
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
                            #else:
                            #    print("Error: %s file not found" % self.checkpoint_file)

                        self.count_append = 0

                        for attr in ('init_time_stamp','final_time_file_str', 'time_append'):
                            self.__dict__.pop(attr, None)

                        if (how_much_left < weight): # if there's more to compute - call before return 
                            self._call_recursive(how_much_left, weight, ds)

                        return self.dm_append

   