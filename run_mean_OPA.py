# run mean OPA 
import xarray as xr 
import meanOPA 

# filePath = '/esarchive/scratch/kgrayson/git/one_pass/config.yml'

# # then run the meanOPA with the config file 

# meanOpa = meanOPA(filePath)

# # simulating streaming 
# filePath = "/esarchive/scratch/alacima/python/destination_earth/icon/tas-uas-vas-ngc2009_atm_2d_1h_inst_20200501T000000Z_5km.nc" 
# ds = xr.open_dataset(filePath) # open dataset 

# for i in range(24): 

#     ds = ds.isel(time=slice(i,(i+1))) # slice(start, stop, step), extract 'moving window' which is hourly data
#     meanOpa.mean(ds)

