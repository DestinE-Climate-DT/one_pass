from typing import Dict
import os 
from sys import exit
import pickle 
import numpy as np
import xarray as xr
import pandas as pd
import dask.array as da
#from dask.distributed import client, LocalCluster

from one_pass.convert_time import convert_time
from one_pass.check_stat import check_stat
from one_pass import util

class Opa:
    """ One pass algorithm class that will contain the rolling statistic """

    def __init__(self, user_request: Dict): 

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

    def _initialise_time(self, time_stamp_tot):

        """
        Called when total time in minutes of the incoming time stamp is less than the timestep of the data. 
        Will initalise the time attributes related to the statistic call 

        Arguments:
        ----------
        time_stamp_tot : total time in minutes of the incoming time stamp

        Returns:
        --------
        self.n_data : number of required pieces of data for the statistic to complete 
        """

        # TODO: decide how the time stamps should look for 3hourly and 6 hourly (begininng or end)
        self.init_time_stamp = self.time_stamp # this will be re-written for larger frequencies 

        # calculated by MIN freq of stat / timestep min of data
        if ((self.stat_freq_min/self.time_step).is_integer()):
            if(time_stamp_tot == 0 or (self.time_step/time_stamp_tot).is_integer()): # this should be greater than 1?
                self.n_data = int(self.stat_freq_min/self.time_step) 
            else:
                print('WARNING: timings of input data span over new statistic')
                self.n_data = int(self.stat_freq_min/self.time_step) 
        else:
            raise Exception('Frequency of the requested statistic (e.g. daily) must be wholly divisible by the timestep (dt) of the input data')

   
        try:
            getattr(self, "time_append") # if time_append already exisits it won't overwrite it 
        except AttributeError:
            ## looking at output freq - how many cum stats you want to save in one netcdf ##
            # converting output freq into a number
            output_freq_min, time_stamp_min_tot = convert_time(time_word = self.output_freq, time_stamp_input = self.time_stamp, time_step_input = self.time_step)
            # eg. how many days requested: 7 days of saving with daily data
            self.time_append = (output_freq_min - time_stamp_min_tot)/ self.stat_freq_min # how many do you need to append

            if(self.time_append < 1 and self.stat_freq != "continuous"): #output_freq_min < self.stat_freq_min
                raise ValueError('Output frequency can not be less than frequency of statistic')

        # for continous setting these values 
        if (self.stat_freq == "continuous"):
            self.mon_freq_min, self.time_stamp_min_tot = convert_time(time_word = "monthly", time_stamp_input = self.time_stamp, time_step_input = self.time_step)



    def _initialise_attrs(self, ds):
        """
        Initialises data structure for cumulative stats 

        Returns:
        --------
        self.stat_cum : zero filled data array with the shape of the data compressed in the time dimension 
        """

        if(self.stat != "hist" or self.stat != "percentile"):
            
            ds_size = ds.tail(time = 1)

            value = np.zeros_like(ds_size, dtype=np.float64) # forcing computation in float64
            #value = da.zeros_like(ds_size, dtype=np.float64, chunks=[1,450,600]) # forcing computation in float64

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
            if ds.chunks is None:
                value = np.zeros((1, np.size(ds.lat), np.size(ds.lon)))
            else:
                value = da.zeros((1, np.size(ds.lat), np.size(ds.lon))) # keeping everything as a 3D array


    def _initialise(self, ds, time_stamp_tot):

        self.count = 0
        self._initialise_time(time_stamp_tot)
        self._initialise_attrs(ds)


    def _check_n_data(self, error_flag):
        try:
            getattr(self, "n_data")
        except AttributeError:
            error_flag = True
            #raise AttributeError('cannot start required statistic without the initial data')
        
        return error_flag
            
        

    def _compare_time(self, ds, time_stamp_tot):

        """
        Checks to see if in the timestamp of the data is the 'first' in the requested statistic. If so, 
        will initalise the class using above functions. Will also check if statistic has previously been initalised  
        """

        error_flag = False

        if(time_stamp_tot < self.time_step): # this indicates that it's the first data  otherwise time_stamp will be larger
            
            self._initialise(ds, time_stamp_tot)
        
        elif(self.stat_freq == "continuous"):
            
            if(hasattr(self, 'n_data')):
                pass
                # if(time_stamp_tot > self.time_stamp_tot): # is new one greater than old one? 
                #     pass # yes? carry on 
                # else: 
                #     self._initialise(ds, time_stamp_tot) # re-initalise 
            else: 
                self._initialise(ds, time_stamp_tot)

            #self.time_stamp_tot = time_stamp_tot

        else:
            # check were we are in the sequence 
            error_flag = self._check_n_data(error_flag)
            
        return error_flag


    def _check_time_stamp(self, ds):

        """
        Function to check the incoming timestamp of the data and check if it is the first one of the required statistic.
        If so, initalise the function with the required variables 

        Args:
        ds: Incoming xarray dataArray with associated timestamp 

        Output:
        If function is initalised it will assign the attributes: 
        n_data: number of pieces of information required to make up the requested statistic 
        count: how far through the statistic you are, when initalised, this will be set to 1
        stat_cum: will set an empty array of the correct dimensions, ready to be filled with the statistic 

        Checks:
        If it is not the first timestamp of the statistic, it will check: 
        - that the attribute n_data has already been assigned, otherwise will through an error 
        """

        time_stamp_list = sorted(ds.time.data) # assuming that incoming data has a time dimension
        time_stamp_pandas = [pd.to_datetime(x) for x in time_stamp_list]
        time_stamp = time_stamp_pandas[0] # converting to a pandas datetime to calculate if it's the first
        self.time_stamp = time_stamp

        # converting statistic freq into minutes 'other code -  convert_time'
        self.stat_freq_min, time_stamp_tot = convert_time(time_word = self.stat_freq, time_stamp_input = time_stamp, time_step_input = self.time_step)
        error_flag = self._compare_time(ds, time_stamp_tot)

        # stat_freq < time_step
        if(self.stat_freq_min < self.time_step):
            # we have a problem
            raise ValueError('time_step too large for requested statistic')
        
        return time_stamp_tot, error_flag
    

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

        #da.rechunk(ds, chunks=[1,450,600])
        return ds


    def _check_num_time_stamps(self, ds):

        """Check how many time stamps are in the incoming data. 
        It's possible that the GSV interface will have multiple messages"""

        time_stamp_list = sorted(ds.time.data) # this method could be very different for a GRIB file, need to understand time stamps
        time_num = np.size(time_stamp_list)

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

        self.count += weight
        if (weight == 1):
            mean_cum = self.mean_cum + weight*(ds - self.mean_cum) / (self.count) 
        else:
            temp_mean = self._two_pass_mean(ds) # compute two pass mean first
            mean_cum = self.mean_cum + weight*(temp_mean - self.mean_cum) / (self.count) 

        self.mean_cum = mean_cum.data
        #print('update mean')

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

        self.count += weight
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

        self.count += weight
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

        self.count += weight
        self.thresh_exceed_cum = ds 

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

    def _write_checkpoint(self):

        """write checkpoint file """

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
        self.count_append += 1 


    def _create_final_timestamp(self, time_word = None):

        """Creates the final time stamp for each accumulated statistic. For now, simply 
        using the time stamp of the first incoming data of that statistic"""
        # final_time_stamp = None

        # if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or self.stat_freq == "6hourly"):
        #     final_time_stamp = self.init_time_stamp.to_datetime64().astype('datetime64[h]') # see initalise for final_time_stamp

        # elif (self.stat_freq == "daily"):
        #     final_time_stamp = self.init_time_stamp #.to_datetime64().astype('datetime64[D]')

        # elif (self.stat_freq == "weekly"):
        #     final_time_stamp = self.init_time_stamp.to_datetime64().astype('datetime64[W]')

        # elif (self.stat_freq == "monthly" or self.stat_freq == "3monthly" or time_word == "monthly"):
        #     final_time_stamp = self.init_time_stamp.to_datetime64().astype('datetime64[M]')

        # elif (self.stat_freq == "annually"):
        #     final_time_stamp = self.init_time_stamp.to_datetime64().astype('datetime64[Y]')

        final_time_stamp = self.init_time_stamp

        return final_time_stamp 


    def _create_file_name(self, append = False, time_word = None):

        """ creates the final file name for the netCDF file. If append is True 
        then the file name will span from the first requested statistic to the last. 
        time_word corresponds to the continous option which outputs checks every month"""

        final_time_file_str = None

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

            elif (self.stat_freq == "monthly" or self.stat_freq == "3monthly" or time_word == "monthly"):
                final_time_file_str = self.init_time_stamp.strftime("%Y_%m")

            elif (self.stat_freq == "annually"):
                final_time_file_str = self.init_time_stamp.strftime("%Y")

        return final_time_file_str

    def _save_output(self, dm, ds, final_time_file_str):

        """  Creates final file name and path and saves final dataSet """

        if (hasattr(self, 'variable')): # if there are multiple variables in the file

            file_name = os.path.join(self.out_filepath, f'{final_time_file_str}_{self.variable}_{self.stat_freq}_{self.stat}.nc')
        else:
            file_name = os.path.join(self.out_filepath, f'{final_time_file_str}_{ds.name}_{self.stat_freq}_{self.stat}.nc')

        dm.to_netcdf(path = file_name, mode ='w') # will re-write the file if it is already there
        dm.close()
        print('finished saving')


    def _call_recursive(self, how_much_left, weight, ds):

        """if there is more data given than what is required for the statistic,
          it will make a recursive call to itself """
        
        ds = ds.isel(time=slice(how_much_left, weight))
        Opa.compute(self, ds) # calling recursive function

    def compute(self, ds):
    
        """  Actual function call  """

        ds = self._check_variable(ds) # convert from a data_set to a data_array if required

        time_stamp_tot, error_flag = self._check_time_stamp(ds) # check the time stamp and if the data needs to be initalised 
        if error_flag:
            print('passing on this data as its not the initial data for the requested statistic')
            return

        if((time_stamp_tot/self.time_step) < self.count): # check if data has been 'seen', will only skip if data doesn't get re-initalised
            print('already seen this data')
            return 
             
        weight = self._check_num_time_stamps(ds) # this checks if there are multiple time stamps in a file and will do two pass statistic
        
        how_much_left = (self.n_data - self.count) # how much is let of your statistic to fill

        if (how_much_left >= weight): # will not span over new statistic

            self._update(ds, weight)  # update rolling statistic with weight

            if(self.checkpoint == True and self.count < self.n_data):
                self._write_checkpoint() # this will not be written when count == ndata 

        elif(how_much_left < weight): # this will span over the new statistic 

            ds_left = ds.isel(time=slice(0, how_much_left)) # extracting time until the end of the statistic

            # update rolling statistic with weight of the last few days  - # still need to finish the statistic (see below)
            self._update(ds_left, how_much_left)

        if (self.stat_freq == "continuous"):

            if(self.count == int((self.mon_freq_min - self.time_stamp_min_tot)/self.time_step)):
                dm, final_time_file_str = self._data_output(ds, time_word = "monthly")
                self._save_output(dm, ds, final_time_file_str)
                self.count = 0 
            
            if (self.count == 1):
                self.mon_freq_min, self.time_stamp_min_tot = convert_time(time_word = "monthly", time_stamp_input = self.time_stamp, time_step_input = self.time_step)

        if (self.count == self.n_data):   # when the statistic is full
            
            #self.mean_cum.compute() 
            #print('finished compute')
            
            dm, final_time_file_str = self._data_output(ds) # output as a dataset 

            if(self.time_append == 1): #output_freq_min == self.stat_freq_min
                if(self.save == True):
                    self._save_output(dm, ds, final_time_file_str)

                if(self.checkpoint):# delete checkpoint file 
                    if os.path.isfile(self.checkpoint_file):
                        os.remove(self.checkpoint_file)
                    else:
                        print("Error: %s file not found" % self.checkpoint_file)

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
                            else:
                                print("Error: %s file not found" % self.checkpoint_file)

                        self.count_append = 0
                        delattr(self, "init_time_stamp")
                        delattr(self, "final_time_file_str")

                        if (how_much_left < weight): # if there's more to compute - call before return 
                            self._call_recursive(how_much_left, weight, ds)

                        return self.dm_append

   