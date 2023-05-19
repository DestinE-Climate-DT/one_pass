import glob 
import os 
import sys 

# making sure we're in the correct folder  
path = os.path.realpath(os.path.join(os.path.dirname(__file__)))
sys.path.append(path)
os.chdir(path)

from one_pass.opa import *
from one_pass.opa import Opa

def main():
###### initalising OPA ################# 

    pass_dic = {"stat": "mean",
    "stat_freq": "daily",
    "output_freq": "daily",
    "time_step": 60,
    "variable": "uas",
    "save": True,
    "checkpoint": True,
    "checkpoint_filepath": "/home/bsc32/bsc32263/git/data/",
    "out_filepath": "/home/bsc32/bsc32263/git/data/"}

    ########### GSV Interface ################  

    #### reading some data from disk - replace with call to GSV interface  #### 
    file_path_data = os.path.realpath(os.path.join(os.path.dirname(__file__), 'tests', 'uas_10_months.nc'))

    fileList = glob.glob(file_path_data) 
    fileList.sort() 
    data = xr.open_dataset(fileList[0])  
    data = data.astype(np.float64)


    ############ computing OPA ############# 

    for i in range(0, 24, 1): 

        ds = data.isel(time=slice(i,i+1)) # extract moving window 'simulating streaming'
        # can pass either a dictionary as above or data from the config file 
        #daily_mean = Opa("config.yml")
        daily_mean = Opa(pass_dic)
        dm = daily_mean.compute(ds) # computing algorithm with new data 


if __name__ == '__main__':
    main()
