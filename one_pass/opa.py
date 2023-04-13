from typing import Dict
import os 
import pickle 
import numpy as np
import xarray as xr
import pandas as pd
import dask.array as da

from one_pass.convert_time import convert_time
from one_pass import util

class Opa:
    """Individual clusters."""

    def __init__(self, user_request: Dict): 

        # Check and process request
        request = util.parse_request(user_request)
        self.request = request
        self._process_request(request)

        if(self.checkpoint): # have the option of not using checkpointing files and keeping everything in memory
            self._check_checkpoint(request)

    def _process_request(self, request):
        self.statistic = request.get("stat")
        self.stat_freq = request.get("stat_freq")
        self.output_freq = request.get("output_freq")
        self.save = request.get("save")
        self.time_step = request.get("time_step") # this is int value in minutes
        self.checkpoint_in_file = request.get("checkpoint_in_file")
        self.checkpoint_out_file = request.get("checkpoint_out_file")
        self.file_path_save = request.get("out_file")
        self.checkpoint = request.get("checkpoint")

        if(self.statistic == "thresh_exceed"):
            try:
                self.threshold = request.get("threshold")
            except:
                raise Exception('need to provide threshold of exceedance value')

        if (request.get("variable") is not None):
            self.variable = request.get("variable")

    def _check_checkpoint(self, request):
        """
        Takes user user request and checks if a checkpoint file exists from which to initalise the statistic from

        Arguments:
        ----------
        user_request : python dictionary containing the file path for the checkpoint file

        Returns:
        --------
        if checkpoint file is present: 
            xarray object containing summary statistics
        """
        if (request.get("checkpoint_in_file")): 
            path = request.get("checkpoint_in_file")
            if os.path.exists(path): 
                
                file_name = self.checkpoint_in_file
                
                with open(file_name, 'rb') as file: 
                    self = pickle.load(file)

            else: 
                self.mean_cum = None
                self.min_cum = None
                self.max_cum = None
                self.var_cum = None
                self.thresh_exceed_cum = None
                self.count_append = 0 
        else:
            raise KeyError("need to pass a file path for the checkpoint file")


    def _initialise(self, ds, time_stamp_tot):
        # only initialise cumulative mean if you know this is the first input
        self.count = 0
        self.final_time_stamp = self.time_stamp # this will be re-written for larger frequencies 
        # calculated by MIN freq of stat / timestep min of data
        if ((self.stat_freq_min/self.time_step).is_integer()):

            if(time_stamp_tot == 0):
                self.n_data = int(self.stat_freq_min/self.time_step) # number of elements of data that need to be added to the cumulative mean
            else:
                if((self.time_step/time_stamp_tot).is_integer()):# THIS SHOULD BE GREATER THAN 1
                    self.n_data = int(self.stat_freq_min/self.time_step) # number of elements of data that need to be added to the cumulative mean
                else:

                    print('WARNING: timings of input data span over new statistic')
                    self.n_data = int(self.stat_freq_min/self.time_step) # number of elements of data that need to be added to the cumulative mean

        else:
            # we have a problem
            raise Exception('Frequency of the requested statistic (e.g. daily) must be wholly divisible by the timestep (dt) of the input data')


        if(self.statistic != "hist" or self.statistic != "percentile"):
        # potentially don't want zeros for threshold exceedance as well?
            if(np.size(ds.dims) <=3):
                if ds.chunks is None:
                    value = np.zeros((1, np.size(ds.lat), np.size(ds.lon)))
                else:

                    value = da.zeros((1, np.size(ds.lat), np.size(ds.lon))) # keeping everything as a 3D array
                    #, coords = ds.coords, dims= ds.dims
            else:
                ds_size = ds.tail(time = 1)
                value = np.zeros_like(ds_size)

            self.__setattr__(str(self.statistic+"_cum"), value)

            if(self.statistic == "var"):
                # then also need to calculate the mean
                self.__setattr__("mean_cum", value)

            # for the standard deviation need both the mean and variance throughout
            elif(self.statistic == "std"): # can reduce storage here by not saving both the cumulative variance and std
                self.__setattr__("mean_cum", value)
                self.__setattr__("var_cum", value)

            elif(self.statistic == "min" or self.statistic == "max"):
                self.__setattr__("timings", value)

            # else loop for histograms of percentile calculations that may require a different initial grid
        else: # TODO: NEED TO CHANGE THIS!
            if ds.chunks is None:
                value = np.zeros((1, np.size(ds.lat), np.size(ds.lon)))
            else:
                value = da.zeros((1, np.size(ds.lat), np.size(ds.lon))) # keeping everything as a 3D array

    def _check_n_data(self):
        try:
            getattr(self, "n_data")
        except AttributeError:
            raise Exception('cannot start required statistic without the initial data')

    def _check_time_stamp(self, ds):
    # function to calculate:
    # - n_data (number of pieces of information (GRIB messages) required to make up the required statistic)
    # - count (how far through the statistic you are, i.e. count = 5 if you've read 5 of the required GRIB messages)
    # - initialises empty array for the statistic
    # all based on time_stamp and time_step

        time_stamp_list = sorted(ds.time.data) # assuming that incoming data has a time dimension
        time_stamp_pandas = [pd.to_datetime(x) for x in time_stamp_list]
        time_stamp = time_stamp_pandas[0] # converting to a pandas datetime to calculate if it's the first
        self.time_stamp = time_stamp

        # converting statistic freq into a number 'other code -  convert_time'
        self.stat_freq_min = convert_time(time_word = self.stat_freq, time_stamp_input = time_stamp)

        # stat_freq < time_step
        if(self.stat_freq_min < self.time_step):
            # we have a problem
            raise Exception('time_step too large for requested statistic')


        if(self.stat_freq == "hourly"):
            # first thing to check is if this is the first in the series, time stamp must be less than 60
            time_stamp_min = self.time_stamp.minute

            # stat_freq >= time_step
            if(time_stamp_min <= self.time_step): # this indicates that it's the first data of the hour otherwise time_stamp will be larger
                # initialise cumulative statistic array
                self._initialise(ds, time_stamp_min)
            else:
                self._check_n_data()

        if(self.stat_freq == "3hourly"):
            # first thing to check is if this is the first in the series, time stamp must be less than 3*60
            time_stamp_min = time_stamp.minute
            time_stamp_hour = time_stamp.hour # converting to minutes -
            if(np.mod(time_stamp_hour, 3) != 0):
                time_stamp_tot = time_stamp_min + time_stamp_hour*60
            else:
                time_stamp_tot = time_stamp_min

            # stat_freq >= time_step
            # just less than otherwise timestep 00:00 and 01:00 would both initialise
            if(time_stamp_tot < self.time_step):
                # initialise cumulative statistic array
                self._initialise(ds, time_stamp_tot)
            else:
                self._check_n_data()

        if(self.stat_freq == "6hourly"):
            # first thing to check is if this is the first in the series, time stamp must be less than 6*60
            time_stamp_min = time_stamp.minute
            time_stamp_hour = time_stamp.hour # converting to minutes -
            if(np.mod(time_stamp_hour, 6) != 0):
                time_stamp_tot = time_stamp_min + time_stamp_hour*60
            else:
                time_stamp_tot = time_stamp_min

            # stat_freq >= time_step
            # just less than otherwise timestep 00:00 and 01:00 would both initialise
            if(time_stamp_tot < self.time_step):
                # initialise cumulative statistic array
                self._initialise(ds, time_stamp_tot)
            else:
                self._check_n_data()

        if(self.stat_freq == "daily"):
            time_stamp_min = time_stamp.minute
            time_stamp_hour = time_stamp.hour*60 # converting to minutes -
            time_stamp_tot = time_stamp_min + time_stamp_hour

            # this will only work if the one hour is wholly divisible by the timestep
            if(time_stamp_tot < self.time_step): # this indicates that it's the first, works when comparing float64 to int
                # initialise cumulative statistic array
                self._initialise(ds, time_stamp_tot)
            else:
                self._check_n_data()


        elif(self.stat_freq == "weekly"):

            time_stamp_min = time_stamp.minute
            time_stamp_hour = time_stamp.hour*60 # converting to minutes -
            time_stamp_day = (time_stamp.day_of_week)*24*60
            time_stamp_tot = time_stamp_min + time_stamp_hour + time_stamp_day

            # this will only work if the one hour is wholly divisible by the timestep
            if(time_stamp_tot < self.time_step): # this indicates that it's the first, works when comparing float64 to int
                # initialise cumulative statistic array
                self._initialise(ds, time_stamp_tot)

            else:
                self._check_n_data()


        if(self.stat_freq == "monthly"):

            time_stamp_min = time_stamp.minute
            time_stamp_hour = time_stamp.hour*60 # converting to minutes -
            time_stamp_day = (time_stamp.day-1)*24*60
            time_stamp_month = time_stamp.month * time_stamp.days_in_month
            time_stamp_tot = time_stamp_min + time_stamp_hour + time_stamp_day + time_stamp_month

            # this will only work if the one hour is wholly divisible by the timestep
            if(time_stamp_tot < self.time_step): # this indicates that it's the first, works when comparing float64 to int
                # initialise cumulative statistic array
                self._initialise(ds, time_stamp_tot)

            else:
                self._check_n_data()

        if(self.stat_freq == "annually"):

            time_stamp_min = time_stamp.minute
            time_stamp_hour = time_stamp.hour*60 # converting to minutes -
            time_stamp_day = (time_stamp.day-1)*24*60
            time_stamp_tot = time_stamp_min + time_stamp_hour + time_stamp_day

            # this will only work if the one hour is wholly divisible by the timestep
            if(time_stamp_tot < self.time_step): # this indicates that it's the first, works when comparing float64 to int
                # initialise cumulative statistic array
                self._initialise(ds, time_stamp_tot)

            else:
                self._check_n_data()


    def _check_variable(self, ds):

        try:
            getattr(ds, "data_vars") # this means it a data_set
            self.data_set_attr = ds.attrs #keeping the attributes of the full data_set to append to the final data_set
            try:
                ds = getattr(ds, self.variable) # converts to a dataArray
            except AttributeError:
                raise Exception('If passing data_set need to provide variable, opa can only use one variable at the moment')
        except AttributeError:
            pass # data already at data_array

        return ds


    def _check_num_time_stamps(self, ds):
        # checking to see how many time stamps are actually in the file, it's possible that the GSV interface will have multiple messages
        time_stamp_list = sorted(ds.time.data) # this method could be very different for a GRIB file, need to understand time stamps
        time_num = np.size(time_stamp_list)

        return time_num


    def _two_pass_mean(self, ds): # computes normal mean using two pass
        ax_num = ds.get_axis_num('time')
        temp = np.mean(ds, axis = ax_num, dtype = np.float64, keepdims=True)  # updating the mean with np.mean over the timesteps available
        #temp = ds.resample(time ='1D').mean() # keeps the format (1, lat, lon)
        return temp

    def _two_pass_var(self, ds): # computes sample (ddof = 1) variance using two pass
        #temp = ds.resample(time ='1D').var(ddof = 1) # keeps the format (1, lat, lon)
        # TODO: where is ds_np coming from?
        ax_num = ds.get_axis_num('time')
        temp = np.var(ds, axis = ax_num, dtype = np.float64, keepdims=True, ddof = 1)  # updating the mean with np.mean over the timesteps available

        return temp


    # actual mean function
    def _update_mean(self, ds_np, weight):# where x is the new value and weight is the new weight
        self.count += weight
        if (weight == 1):
            mean_cum = self.mean_cum + weight*(ds_np - self.mean_cum) / (self.count) # updating mean with one-pass algorithm
        else:
            temp_mean = self._two_pass_mean(ds_np) # compute two pass mean first
            mean_cum = self.mean_cum + weight*(temp_mean - self.mean_cum) / (self.count) # updating mean with one-pass algorithm

        self.mean_cum = mean_cum.data


    # variance one-pass
    def _update_var(self, ds_np, weight):

        # storing 'old' mean temporarily
        old_mean = self.mean_cum

        if(weight == 1):
            self._update_mean(ds_np, weight)
            var_cum = self.var_cum + weight*(ds_np - old_mean)*(ds_np - self.mean_cum)
        else:
            temp_mean = self._two_pass_mean(ds_np) # two-pass mean
            self._update_mean(temp_mean, weight) # update self.mean_cum
            temp_var = self._two_pass_var(ds_np) # two pass variance
            # see paper Mastelini. S
            var_cum = self.var_cum + temp_var + np.square(old_mean - temp_mean)*((self.count - weight)*weight/self.count)

        if (self.count == self.n_data):
            var_cum = var_cum/(self.count - 1) # using sample variance NOT population variance

        self.var_cum = var_cum.data



    def _update_std(self, ds_np, weight):
        # can reduce storage here if you choose not to have specific var_cum and std_cum names
        self._update_var(ds_np, weight)
        if (self.count == self.n_data):
            self.std_cum = np.sqrt(self.var_cum)


    def _update_min(self, ds_np, weight):
        # creating array of timestamps that corresponds to the min value
        if(weight == 1):
            timestamp = np.datetime_as_string((ds_np.time.values[0]))
            ds_time = xr.zeros_like(ds_np)
            ds_time = ds_time.where(ds_time != 0, timestamp)

        else:
            ax_num = ds_np.get_axis_num('time')
            timings = ds_np.time
            min_index = ds_np.argmin(axis = ax_num, keep_attrs = False)
            #self.min_index = min_index
            ds_np = np.amin(ds_np, axis = ax_num, keepdims = True)
            ds_time = xr.zeros_like(ds_np) # now this will have dimensions 1,lat,lon

            for i in range(0, weight):
                #self.i = i
                #self.timestamp = ds_np.time.values[i]
                timestamp = np.datetime_as_string((timings.values[i]))
                ds_time = ds_time.where(min_index != i, timestamp)

        if(self.count > 0):
            self.min_cum['time'] = ds_np.time
            self.timings['time'] = ds_np.time
            ds_time = ds_time.where(ds_np < self.min_cum, self.timings)
            # this gives the new self.min_cum number when the  condition is FALSE (location at which to preserve the objects values)
            ds_np = ds_np.where(ds_np < self.min_cum, self.min_cum)

        ds_time = ds_time.astype('datetime64[ns]') # convert to datetime64 for saving

        #self.ds_np = ds_np
        self.count += weight
        self.min_cum = ds_np #running this way around as Array type does not have the function .where, this only works for data_array
        self.timings = ds_time

        return


    def _update_max(self, ds_np,weight):

        if(weight == 1):
            timestamp = np.datetime_as_string((ds_np.time.values[0]))
            ds_time = xr.zeros_like(ds_np)
            ds_time = ds_time.where(ds_time != 0, timestamp)
        else:
            ax_num = ds_np.get_axis_num('time')
            timings = ds_np.time
            max_index = ds_np.argmax(axis = ax_num, keep_attrs = False)
            self.max_index = max_index
            ds_np = np.amax(ds_np, axis = ax_num, keepdims = True)
            ds_time = xr.zeros_like(ds_np) # now this will have dimensions 1,lat,lon

            for i in range(0, weight):
                timestamp = np.datetime_as_string((timings.values[i]))
                ds_time = ds_time.where(max_index != i, timestamp)

        if(self.count > 0):
            self.max_cum['time'] = ds_np.time
            self.timings['time'] = ds_np.time
            ds_time = ds_time.where(ds_np > self.max_cum, self.timings)
            # this gives the new self.max_cum number when the  condition is FALSE (location at which to preserve the objects values)
            ds_np = ds_np.where(ds_np > self.max_cum, self.max_cum)

        ds_time = ds_time.astype('datetime64[ns]') # convert to datetime64 for saving

        self.count += weight
        self.max_cum = ds_np #running this way around as Array type does not have the function .where, this only works for data_array
        self.timings = ds_time

        return


    def _update_threshold(self, ds_np, weight):

        if(weight > 1):

            ds_np = ds_np.where(ds_np < self.threshold, 1)
            ds_np = ds_np.where(ds_np >= self.threshold, 0)
            #ds_np = ds_np.sum(dim = "time")
            ds_np = np.sum(ds_np, axis = 0, keepdims = True) #try slower np version that preserves dimensions
            ds_np = self.thresh_exceed_cum + ds_np

        else:
            if(self.count > 0):
                self.thresh_exceed_cum['time'] = ds_np.time

            ds_np = ds_np.where(ds_np < self.threshold, self.thresh_exceed_cum + 1)
            ds_np = ds_np.where(ds_np >= self.threshold, self.thresh_exceed_cum)

        self.count += weight
        self.thresh_exceed_cum = ds_np #running this way around as Array type does not have the function .where, this only works for data_array

        return


    def _update(self, ds, weight=1):

        if (self.statistic == "mean"):
            self._update_mean(ds, weight) # sometimes called with weights and sometimes not

        elif(self.statistic == "var"):
            self._update_var(ds, weight)

        elif(self.statistic == "std"):
            self._update_std(ds, weight)

        elif(self.statistic == "min"):
            self._update_min(ds, weight)

        elif(self.statistic == "max"):
            self._update_max(ds, weight)

        elif(self.statistic == "thresh_exceed"):
            self._update_threshold(ds, weight)

        #elif(self.statistic == "percentile"):
        # run tdigest

    def _write_update(self):
        """write checkpoint file 
        """
        file_name = self.checkpoint_in_file
        with open(file_name, 'wb') as file: 
            pickle.dump(self, file)


    def _create_data_set(self, final_stat, final_time_stamp, ds):

        if(np.size(np.shape(final_stat)) == 3):

            dm = xr.Dataset(
            data_vars = dict(
                    [(ds.name, (["time","lat","lon"], final_stat, ds.attrs))],    # need to add variable attributes
                ),
            coords = dict(
                #time = (["time"], [pd.to_datetime(final_time_stamp)]),
                time = (["time"], [final_time_stamp], ds.time.attrs),
                lon = (["lon"], ds.lon.data, ds.lon.attrs),
                lat = (["lat"], ds.lat.data, ds.lat.attrs),
            ),
            attrs = self.data_set_attr
            )
        
        else:

            ds = ds.tail(time = 1) # I DON'T THINK THIS WORKS FOR GIVING 1 AT A TIME 
            ds = ds.assign_coords(time = (["time"], [final_time_stamp], ds.time.attrs))

            dm = xr.Dataset(
            data_vars = dict(
                    [(ds.name, (ds.dims, final_stat, ds.attrs))],    # need to add variable attributes
                ),
            coords = dict(
                ds.coords
            ),
            attrs = self.data_set_attr
            )

            #dm.assign_coords(time = (["time"], [final_time_stamp], ds.time.attrs))

        if(hasattr(self, 'timings')):
            timing_attrs = {'OPA':'time stamp of ' + str(self.stat_freq + " " + self.statistic)}
            dm = dm.assign(timings = (["time","lat","lon"], self.timings.data, timing_attrs))

        return dm


    def _save_output(self, dm, ds, time_stamp_string):

        if (hasattr(self, 'var')): # if there are multiple variables in the file
            file_name = self.file_path_save + time_stamp_string + "_" + self.var + "_" + self.stat_freq + "_" + self.statistic + ".nc"
        else:
            file_name = self.file_path_save + time_stamp_string + "_" + ds.name + "_" + self.stat_freq  + "_" + self.statistic + ".nc"

        dm.to_netcdf(path = file_name, mode ='w') # will re-write the file if it is already there
        dm.close()
        print('finished saving')



    def _data_output(self, ds):

        final_stat = None
        if (self.statistic == "mean"):
            final_stat = self.mean_cum

        elif(self.statistic == "var"):
            final_stat = self.var_cum

        elif(self.statistic == "std"):
            final_stat = self.std_cum

        elif(self.statistic == "min"):
            final_stat = self.min_cum.data

        elif(self.statistic == "max"):
            final_stat = self.max_cum.data

        elif(self.statistic == "thresh_exceed"):
            final_stat = self.thresh_exceed_cum.data
            ds.name = 'exceedance_freq'

        final_time_stamp = None
        time_stamp_string = None
        if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or self.stat_freq == "6hourly"):
            final_time_stamp = self.final_time_stamp #.to_datetime64().astype('datetime64[h]')
            #self.final_time_stamp = final_time_stamp
            time_stamp_string = self.time_stamp.strftime("%Y_%m_%d_T%H")

        elif (self.stat_freq == "daily"):
            final_time_stamp = self.time_stamp.to_datetime64().astype('datetime64[D]')
            self.final_time_stamp = final_time_stamp
            time_stamp_string = self.time_stamp.strftime("%Y_%m_%d")

        elif (self.stat_freq == "weekly"):
            final_time_stamp = self.time_stamp.to_datetime64().astype('datetime64[W]')
            time_stamp_string = self.time_stamp.strftime("%Y_%m_%d")

        elif (self.stat_freq == "monthly"):
            final_time_stamp = self.time_stamp.to_datetime64().astype('datetime64[M]')
            time_stamp_string = self.time_stamp.strftime("%Y_%m")

        elif (self.stat_freq == "annual"):
            final_time_stamp = self.time_stamp.to_datetime64().astype('datetime64[Y]')
            time_stamp_string = self.time_stamp.strftime("%Y")


        try:
            getattr(self, "data_set_attr") # if it was originally a data set
        except AttributeError: # only looking at a data_array
            self.data_set_attr = ds.attrs #both data_set and data_array will have matching attribs

        self.data_set_attr["OPA"] = str(self.stat_freq + " " + self.statistic + " " + "calculated using one-pass algorithm")
        ds.attrs["OPA"] = str(self.stat_freq + " " + self.statistic + " " + "calculated using one-pass algorithm")

        dm = self._create_data_set(final_stat, final_time_stamp, ds)

        return dm, time_stamp_string, final_time_stamp


    def _data_output_append(self, dm, ds, time_append):

        self.dm_output = xr.concat([self.dm_output, dm], "time") # which way around should this be! 
        self.count_append += 1 # updating count

        if(self.count_append == time_append and self.save == True):

            if (self.stat_freq == "hourly" or self.stat_freq == "3hourly" or self.stat_freq == "6hourly"):
                self.time_stamp_string = self.time_stamp_string + "_to_" + self.time_stamp.strftime("%Y_%m_%d_T%H")

            elif (self.stat_freq == "daily"):
                self.time_stamp_string = self.time_stamp_string + "_to_" + self.time_stamp.strftime("%Y_%m_%d")

            elif (self.stat_freq == "weekly"):
                self.time_stamp_string = self.time_stamp_string + "_to_" + self.time_stamp.strftime("%Y_%m_%d")

            elif (self.stat_freq == "monthly"):
                self.time_stamp_string = self.time_stamp_string + "_to_" + self.time_stamp.strftime("%Y_%m")

            elif (self.stat_freq == "annually"):
                self.time_stamp_string = self.time_stamp_string + "_to_" + self.time_stamp.strftime("%Y")

            self._save_output(self.dm_output, ds, self.time_stamp_string)

        return self.dm_output


    def compute(self, ds):
    
        ds = self._check_variable(ds) # convert from a data_set to a data_array if required

        self._check_time_stamp(ds) # check the time stamp and if the data needs to be reset

        weight = self._check_num_time_stamps(ds) # this checks if there are multiple time stamps in a file and will do two pass statistic
        
        how_much_left = (self.n_data - self.count) # how much is let of your statistic to fill

        if (how_much_left >= weight): # will not span over new statistic

            self._update(ds, weight)  # update rolling statistic with weight

            if(self.checkpoint == True and self.count < self.n_data):
                self._write_checkpoint()

        elif(how_much_left < weight): # this will span over the new statistic 

            ds_left = ds.isel(time=slice(0, how_much_left)) # extracting time until the end of the statistic

            # update rolling statistic with weight of the last few days  -this will finish the statistic 
            self._update(ds_left, how_much_left)
            # still need to finish the statistic (see below)

        if (self.count == self.n_data):  # when the statistic is full

        # how to output the data as a data_set
            dm, time_stamp_string, final_time_stamp = self._data_output(ds)

            # if there's more to compute
            if (how_much_left < weight):
                ds = ds.isel(time=slice(how_much_left, weight))
                Opa.compute(self, ds) # calling recursive function

            # converting output freq into a number
            output_freq_min = convert_time(time_word = self.output_freq, time_stamp_input = self.time_stamp)
            # eg. how many days requested: 7 days of saving with daily data
            time_append = output_freq_min / self.stat_freq_min # how many do you need to append
            self.time_append = time_append

            if(time_append < 1): #output_freq_min < self.stat_freq_min
                print('Output frequency can not be less than frequency of statistic!')

            elif(time_append == 1): #output_freq_min == self.stat_freq_min
                if(self.save == True):
                    self._save_output(dm, ds, time_stamp_string)
                return dm

            elif(time_append > 1): #output_freq_min > self.stat_freq_min

                if (hasattr(self, 'count_append') == False or self.count_append == 0):
                    self.count_append = 1
                    self.dm_output = dm # storing the data_set ready for appending
                    self.final_time_stamp = final_time_stamp
                    self.time_stamp_string = time_stamp_string
                else:
                    # append data array with new time outputs
                    dm = self._data_output_append(dm, ds, time_append)


            if(self.count_append == time_append):
                self.count_append = 0
                delattr(self, "final_time_stamp")
                delattr(self, "time_stamp_string")
                return dm
