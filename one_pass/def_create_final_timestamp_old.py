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
