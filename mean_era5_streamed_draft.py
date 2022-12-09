# timing to check preformance 
start = time.perf_counter()

# if you want to run this script, update the location of output netCDF file
pathSave = "/esarchive/scratch/kgrayson/git/onepass_development/daily_means_era5_u100m.nc"

# creating list of era5 data files (stored in files containing 1 month of data)
filePath = "/esarchive/recon/ecmwf/era5/1hourly/u100m/"
fileList = sorted(os.listdir(filePath)) # sorted to get them into the correct order 
nFiles = np.size(fileList) # finding number of files (also number of months)

# mean frequency required, this should be able to change 
meanFreq = "daily"

# initalising variables 
countHours = 0 # stores the number of TOTAL data points or windows examined (in this case hourly data), this needs to be cumlative for each file
countDays = 0 # this stores total days counted used for placing daily means into final array

# going to run though all montly files so the loop opens each file 
for k in range(0, 1):

    fileName = filePath + fileList[k]
    ds = xr.open_dataset(fileName, engine = "netcdf4") # open dataset 

    if (k == 0):
        # initalising daily mean array, starting initial dimension as 0 as don't know how many days (simulating real GSV)
        # only need to initalise once (hence if loop) and nee to start index as 0 not 1, otherwise end up with empty index 
        # might be faster to take this outside of the main loop and just open the first dataset outside main loop
        meanDaily = np.zeros((0, np.size(ds.lat), np.size(ds.lon))) # better to use empty or zeros? 
        timeDaily = pd.DatetimeIndex([]) # initalising time loop 
        # can initialise the array with known number of days but probably won't know that while streaming
        #meanDaily = np.zeros((nDays, np.size(ds.lat), np.size(ds.lon))) # initalising daily mean array 


    # extracting time array from file, calling timeData not time as time is name of python module 
    timeData = sorted(ds.time.data)

    # checking that the spacing of the data is indeed hours
    timeDiff = timeData[1] - timeData[0]
    #hours = timeDiff.astype('timedelta64[h]')
    hours = timeDiff / np.timedelta64(1, 'h') # this should be 1 

    # knowning time difference is hours, want to know how many hours 
    nHours = np.size(timeData)

    # want to check how many days in the file 
    # this is an easy way but should find a more robust way, maybe the commented out method below?
    nDays = int(nHours / 24)

    #daysDiff = time[-1] - time[0] # whole time diff of data file 
    #days = daysDiff.astype('timedelta64[D]')
    #nDays = int(days / np.timedelta64(1, 'D')) # converts from timedelta64 into integer value

    # this exracts all the timestamps of each data point in hours 
    timeStamp = [pd.to_datetime(x) for x in ds.time.values]
    # converts to daily timestamp 
    timeStampDaily = pd.date_range(timeStamp[0], freq = "D", periods = nDays)


    # for now asking for daily mean with hourly data (will need to create a loop of options here)
    if (meanFreq == "daily" and hours == 1):
        
        for i in range(0, nDays): # the way python indexs having range(0, 20) goes 0 to 19. Last number not included

            mean = 0.0 # initalising mean for that day 
            
            for j in range(0, 24): # always 24 hours in a day, again range will go 0 to 23, what about day light savings? find more robust way
                
                timeSlice = ds.u100m.isel(time=slice(countHours,(countHours+1))) # slice(start, stop, step), extract 'moving window' which is hourly data
                timeSlice = np.squeeze(timeSlice) # removing the redundant 1 dimension 
                
                countHours = countHours + 1 # update frequency count (this HAS TO BE the loop + 1 because python index starts at 0)
                
                # now to update the acutal mean 

                # this could be written on two lines (as shown below) but need to store both mean and meanOld:            
                #mean = meanOld + (timeSlice-meanOld)/(j+1)
                #meanOld = mean

                mean += (timeSlice-mean)/(j+1) # as looking at daily mean the denominator must range between 1 to 24 (hence j+1)

            # now in daily array, adding daily mean to data. Can only use top line if array size predefined.
            #meanDaily[countDays,:,:] = mean
            meanDaily = np.insert(meanDaily, countDays,  mean, axis = 0)
            timeDaily = pd.DatetimeIndex.insert(timeDaily, countDays, timeStampDaily[i])
            countDays = countDays + 1


# checking preformance time 
end = time.perf_counter()
elapsed = end-start
elapsed

# Now creating new dataset for the updated files
# Copying the latitude and longitude data as this hasn't changed, with new time dimension based on daily time

dm = xr.Dataset(
    data_vars = dict(
        dailyMean_u100 = (["time","lat","lon"], meanDaily),                            
    ),
    coords = dict(
        time = timeDaily,
        lon = (["lon"], ds.lon),
        lat = (["lat"], ds.lat),
    ),
    attrs = dict(
        description = "daily means of u100 m wind calculated using one-pass algorithm",
        #attrs = ds.attrs,
    ),
)
   
# saving the new dataArray to netCDF 
dm.to_netcdf(pathSave, mode = 'w', format = "NETCDF4")