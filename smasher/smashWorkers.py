#import smashControls
import pymssql
import math
import datetime
import csv
import itertools
import numpy as np
import yaml
import form_connection as fc

""" Smash Workers is the working classes for doing daily aggregation. Use the smashworkers individually to quickly process a data set or two :) """


class DateRange(object):
    """ Compresses startdate and enddate into a range- you must give both a start date and an end date to do processing with smasher"""
    
    def __init__(self, startdate, enddate):
    
        self.dr = [datetime.datetime.strptime(startdate, '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(enddate, '%Y-%m-%d %H:%M:%S')]

    def human_readable(self):
        """makes date ranges even databases can read"""
        hr = [datetime.datetime.strftime(x, '%Y-%m-%d %H:%M:%S') for x in self.dr]
        
        return hr

class MethodTableReader(object):
    """
    Reads height, method, etc. from method history table
    """
    def __init__(self, cursor_sheldon):

        self.cursor_sheldon = cursor_sheldon

    def height_and_method_getter(self, probe_code, daterange):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        
        self.cursor_sheldon.execute(query)
        
        for row in cursor_sheldon:
            
            try:  
                this_height = int(row[0])
            except Exception:
                this_height = 100

            try:
                this_method = str(row[1])
            except Exception:
                this_method = probe_code[0:3] + '999'

            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = 'ANYMET'

        return this_height, this_method, this_sitecode

class LogIssues(object):
    """defines a dictionary that can be written to a logfile"""

    def __init__(self, filename):

        self.filename = filename + ".csv"
        self.dictionary = {}

    def write(self, errorid, errortext):
        """ writes identified errors to the log. same id appends."""
        if errorid not in self.dictionary:
            self.dictionary[errorid] = [errortext]

        elif errorid in self.dictionary:
            self.dictionary[errorid].append(errortext)

    def dump(self):
        """ Dumps out an error log as a csv file"""
        import csv

        with open(self.filename, 'wb') as writefile:
            writer = csv.writer(writefile, delimiter=',',quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(["ERROR", "DESCRIPTION"])
            
            for each_error in sorted(self.dictionary.keys()):
                list_of_points = self.dictionary[each_error]
                
                for each_item in list_of_points:
                    new_row = [each_error, each_item]
                    writer.writerow(new_row)

class AirTemperature(object):
    """ 
    Generates air temperature daily data, consolidates or adds flags, and does methods
    """

    def __init__(self, startdate, enddate, server):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)
        self.daterange = DateRange(startdate,enddate)
        # must be integer
        self.entity = 1
        # server is SHELDON or STEWARTIA
        self.server = server    
        # query against the database
        self.querydb()
        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        # attack data is a method for condensing the data into a structure for processing
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population. """

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        # Queries for SHELDON and STEWARTIA
        if self.server == "SHELDON":
                dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
                dbname = "FSDBDATA.dbo."
        
        try:    
            query = "SELECT DATE_TIME, PROBE_CODE, AIRTEMP_MEAN, AIRTEMP_MEAN_FLAG, AIRTEMP_MIN, AIRTEMP_MIN_FLAG, AIRTEMP_MAX, AIRTEMP_MAX_FLAG from " + dbname + "MS04311 WHERE DATE_TIME >= \'"  + humanrange[0] +  "\' AND DATE_TIME < \'" + humanrange[1]+  "\' ORDER BY DATE_TIME ASC"

            # execute the query
            self.cursor.execute(query)
        
        except Exception:
            
            try:
                query = "SELECT DATE_TIME, PROBE_CODE, AIRTEMP_MEAN, AIRTEMP_MEAN_FLAG from " + dbname + "MS04311 WHERE DATE_TIME >= \'"  + humanrange[0] +  "\' AND DATE_TIME < \'" + humanrange[1]+  "\' ORDER BY DATE_TIME ASC"
                print "...We need to use the old syntax for airtemperature on %s because %s contains not 5 minute maxes" %(self.server, self.server)
        
                # execute the query
                self.cursor.execute(query)
            
            except ValueError:
                print("Invalid Range Chosen, please select different range")


    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' order by date_bgn and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        
        # this method will get the first method code from where the probe is in range. for longer probes this makes sense since writing them to the updater will run a method checker later.
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc"

        cursor_sheldon.execute(query)
        
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100
            
            try:    
                this_method = str(row[1])
            except Exception:
                this_method = "AIR999"

            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"

        return this_height, this_method, this_sitecode

    def attack_data(self):
        """ gather the daily air temperature data from your chosen DB. With this arguement we have already populated the cursor with query_db() """

        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:
            
            # get only the day from the incoming result row  - this is the original result   
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
            
            # resolve the midnight point to the prior day - ie 1/1/2010 00:00:00 is actually 12/31/2014 24:00:00

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)
            else:
                pass

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)

            probe_code = str(row[1])

            if probe_code not in od:

                try:
                    # if the probe code hasn't been extracted yet, assign it and all associated values
                    od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'minval':[str(row[4])], 'minflag': [str(row[5])], 'maxval':[str(row[6])], 'maxflag':[str(row[7])], 'timekeep':[dt_old]}}


                except Exception:
                    od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'minval':[str(row[2])], 'minflag': [str(row[3])], 'maxval':[str(row[2])], 'maxflag':[str(row[3])], 'timekeep':[dt_old]}}

            elif probe_code in od:

                # fail-over to the old method-->
                if dt not in od[probe_code]:
                    try:
                        od[probe_code][dt] = {'val': [str(row[2])], 'fval': [str(row[3])], 'minval':[str(row[4])], 'minflag': [str(row[5])], 'maxval':[str(row[6])], 'maxflag':[str(row[7])], 'timekeep':[dt_old]}
                    
                    except Exception:
                        od[probe_code][dt] = {'val': [str(row[2])], 'fval': [str(row[3])], 'minval':[str(row[2])], 'minflag': [str(row[3])], 'maxval':[str(row[2])], 'maxflag':[str(row[3])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new values and f values, with the appropriate columns listed.
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))

                    try:
                        od[probe_code][dt]['minval'].append(str(row[4]))
                        od[probe_code][dt]['minflag'].append(str(row[5]))
                        od[probe_code][dt]['maxval'].append(str(row[6]))
                        od[probe_code][dt]['maxflag'].append(str(row[7]))
                    
                    except Exception:
                        od[probe_code][dt]['minval'].append(str(row[2]))
                        od[probe_code][dt]['minflag'].append(str(row[3]))
                        od[probe_code][dt]['maxval'].append(str(row[2]))
                        od[probe_code][dt]['maxflag'].append(str(row[3]))

                    # the timekeep attribute was put in place to mark the max five minute or min five minute interval. It's pretty likely this is not needed in most current uses, but in the case of fail over it is important. 
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        # write an output log
        mylog = LogIssues('mylog_airtemp')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a SHELDON cursor if you do not have one to get the LTERLogger_new.dbo.method_history_daily table.
        cursor_sheldon = fc.form_connection("SHELDON")
            
        # iterate over each probe-code that was collected -- had to hard code a bunch of this for now
        for probe_code in self.od.keys():

            if "AIRR" not in probe_code and probe_code != "AIRCEN08":
                try:  
                    height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)
                
                except Exception:
                    height, method_code, site_code = 350, 'AIR243','CENMET'

            elif "AIRR" in probe_code:
                # height is 150m?, method is AIR999, site is REFS plus last two digits of probe_code
                height, method_code, site_code = 150, "AIR999", "REFS"+probe_code[-4:-2]

            elif probe_code == "AIRCEN08":
                height, method_code, site_code = 350, 'AIR243','CENMET'

            else:
                height, method_code, site_code = 100, 'AIR999', 'ANYMET'

            # valid_dates are the dates we will iterate over to do the computation of the daily airtemperature - recall that the first date, if started on midnight, will flip into the prior day, and be erroneous
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
                    
            for each_date in valid_dates:

                # number of observations that aren't "none"
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                num_valid_obs_min = len([x for x in self.od[probe_code][each_date]['minval'] if x != 'None'])
                num_valid_obs_max = len([x for x in self.od[probe_code][each_date]['maxval'] if x != 'None'])

                # notify if there are no observations
                if num_valid_obs == 0:
                    error_string = ("there are only null values on %s for %s") %(each_date, probe_code)
                    # print(error_string)
                    mylog.write('nullday', error_string)
                
                # get the TOTAL number of obs, should be 288, 96, or 24 - includes "missing"- 
                # we only need to count the value-- if it's missing from the mean we aren't going to see a min and max of course
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 287, 96, 95, 24, 23, 1] and each_date not in self.daterange.dr:
                    # notify the number of observations is incorrect
                    error_string2 = "Incomplete or overfilled day, %s, probe %s, total number of observations: %s" %(each_date, probe_code, num_total_obs)
                    # print error_string2
                    mylog.write('incompleteday', error_string2)
                    my_new_rows.append(['MS043', 1, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None, "M", "None", "NA", source + "_incomplete_day"])
                    continue
                else:
                    pass

                # Daily flags based simply on the total number of values in the day.

                # default condition
                df = 'A'
                
                # now if we have that last day which is different then we can just test it explicitly - it would have a baseline of H or F, not "A":
                
                # print("total observations is: " + str(num_total_obs))
                # print("day is:" + datetime.datetime.strftime(each_date,'%Y-%m-%d'))
                if num_total_obs == 24 or num_total_obs == 23:
                    df = 'H'
                elif num_total_obs == 96 or num_total_obs == 95:
                    df = 'F'
                
                # if it's some other value we're going to probably have a questionable flag in the end  
                else:
                    df = 'A'

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_missing_obs_min = len([x for x in self.od[probe_code][each_date]['minflag'] if x == 'M' or x == 'I'])
                num_missing_obs_max = len([x for x in self.od[probe_code][each_date]['maxflag'] if x == 'M' or x == 'I'])

                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_questionable_obs_min = len([x for x in self.od[probe_code][each_date]['minflag'] if x == 'Q' or x == 'O'])
                num_questionable_obs_max = len([x for x in self.od[probe_code][each_date]['maxflag'] if x == 'Q' or x == 'O'])
                
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
                
                num_estimated_obs_min = len([x for x in self.od[probe_code][each_date]['minflag'] if x == 'E'])
                num_estimated_obs_max = len([x for x in self.od[probe_code][each_date]['maxflag'] if x == 'E'])
                

                # daily flag on the mean: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                
                elif (num_missing_obs + num_questionable_obs)/num_total_obs > 0.05:
                    daily_flag = 'Q'
                
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F" or "H" here
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = df
                else:
                    daily_flag = 'Q'


                # DAILY FLAG ON MAXIMUMS
                if num_missing_obs_max/num_total_obs >= 0.2:
                    max_flag = 'M'
                
                elif (num_missing_obs_max + num_questionable_obs_max)/num_total_obs > 0.05:
                    max_flag = 'Q'
                
                elif (num_estimated_obs_max)/num_total_obs >= 0.05:
                    max_flag = 'E'

                # default to "A"
                elif (num_estimated_obs_max + num_missing_obs_max + num_questionable_obs_max)/num_total_obs <= 0.05:
                    max_flag = df
                
                else:
                    max_flag = 'Q'

                
                # DAILY FLAG ON MINIMUMS
                if num_missing_obs_min/num_total_obs >= 0.2:
                    min_flag = 'M'
                
                elif (num_missing_obs_min + num_questionable_obs_min)/num_total_obs > 0.05:
                    min_flag = 'Q'
                
                elif (num_estimated_obs_min)/num_total_obs >= 0.05:
                    min_flag = 'E'

                # default to "A"
                elif (num_estimated_obs_min + num_missing_obs_min + num_questionable_obs_min)/num_total_obs <= 0.05:
                    min_flag = df
                
                else:
                    min_flag = 'Q'

                # DAILY MEAN AIR TEMPERATURE
                
                # the mean is the mean of the day where the values are not none
                try:
                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)
                    
                except ZeroDivisionError:
                    # if the whole day is missing, then the mean_valid_obs is None
                    mean_valid_obs = None

                # DAILY MAX AIR TEMPERATURE

                # the max of the day is the max of the max column
                try:
                    max_valid_obs = round(np.max([float(x) for x in self.od[probe_code][each_date]['maxval'] if x != 'None']),3)
                except Exception:
                    # the max of the day is the max of the mean column
                    try:
                        max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                    except ValueError:
                        # check to see if the whole day was missing, if so, set it to none
                        if mean_valid_obs == None:
                            max_valid_obs = None

                        else:
                            error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                            mylog.write('max_value_error', error_string3)

                # DAILY MAX TIME OF AIR TEMPERATURE
                # the max time is stolen from the corresponding five minutes 
                try:
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['maxval']) if j != "None" and round(float(j),3) == max_valid_obs]
                    max_valid_time = max_valid_time[0]
                except Exception:
                    # the max time is stolen from the mean corresponding five minutes
                    try:
                        max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]
                        max_valid_time = max_valid_time[0]
                    except Exception:
                        # check to see if the the whole day was missing, if so, set it to none
                        if mean_valid_obs == None:
                            max_valid_time = "None"

                        else: 
                            error_string4 = "error in max_valid_time for %s on %s" %(probe_code, each_date)
                            my_log.write('max_time_error', error_string4)
                

                # DAILY MINIMUM AIR TEMPERATURE
                try:
                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['minval'] if x != 'None']),3)
                
                except Exception:
                    try:
                        min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                    except Exception:
                        if mean_valid_obs == None:
                            min_valid_obs = None
                        else:
                            error_string5 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)
                            mylog.write('min_value_error',error_string5)

                # MINIMUM TIME AIR TEMPERATURE
                try:
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['minval']) if j != "None" and round(float(j),3) == min_valid_obs]
                    min_valid_time = min_valid_time[0]
                
                except Exception:
                    try:
                        min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                        min_valid_time = min_valid_time[0]
                    except Exception:
                        if mean_valid_obs == None:
                            min_valid_time = "None"
                        
                        else:
                            error_string6 = "error in min_valid_time for %s on %s" %(probe_code, each_date)
                            mylog.write('mintimeerror', error_string6)

                # final check on missing days
                if mean_valid_obs == None:
                    daily_flag = "M"
                    max_flag = "M"
                    min_flag = "M"
                else:
                    pass


                # set the sources for the output based on the input server
                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04311"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_Pro_MS04311"
                else:
                    print("no server given")

                # in the best possible case, we print it out just as it is here: 
                try:
                #print each_date
                    newrow = ['MS043', 1, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag, datetime.datetime.strftime(max_valid_time, '%H%M'), min_valid_obs, min_flag, datetime.datetime.strftime(min_valid_time, '%H%M'), "NA", source]

                # in the missing day case, we print out a version with Nones filled in for missing values
                except Exception:
                    newrow = ['MS043', 1, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None, "M", "None", "NA", source]

                my_new_rows.append(newrow)
    
            mylog.dump()
        return my_new_rows

class RelHum(object):
    """ 
    Generates relative humidity from 5 or 15 or hourly data; 
    no weird methods that I know of -- i.e. relhum "max" or "min"
    """

    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)
        self.daterange = DateRange(startdate,enddate)
        self.entity = 2
        self.server = server

        # query the database
        self.querydb()
        self.od = {}

        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database - now can go to either sheldon or stewartia"""
        
        # human-readable date range for the database
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."
        
        query = "SELECT DATE_TIME, PROBE_CODE, RELHUM_MEAN, RELHUM_MEAN_FLAG from " + dbname + "MS04312 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end <= \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        
        # this method will get the first method code from where the probe is in range. for longer probes this makes sense since writing them to the updater will run a method checker later.
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "


        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100

            try:
                this_method = str(row[1])
            except Exception:
                this_method = "REL999"

            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"

        return this_height, this_method, this_sitecode
    
    def attack_data(self):
        """ gather the daily relative humidity data """
        
        od = {}

        for row in self.cursor:

            # get only the day, do the resolution of the day onto the 2400 hour
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)
            else:
                pass

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)

            probe_code = str(row[1])

            if probe_code not in od:
                od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    od[probe_code][dt] = {'val': [str(row[2])], 'fval':[str(row[3])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_relhum')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # each probe code is addressed here
        for probe_code in self.od.keys():
            
            if "RELR" not in probe_code:
                # get the height, method_code, and sitecode from the height_and_method_getter function  
                height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            elif "RELR" in probe_code:
                # some default settings in case you add a new probe that doesn't work
                height, method_code, site_code = 150, "REL999", "REFS"+probe_code[-4:-2]

            else:
                height, method_code, site_code = 100, "REL999", "ANYMET"

            # valid_dates are the dates we will iterate over to do the computation of daily relhum
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])

                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write('nullday',error_string)
                
                # get the number of obs total 
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # get the TOTAL number of obs, should be 288, 96, or 24 - includes "missing"- 
                # we only need to count the value-- if it's missing from the mean we aren't going to see a min and max of course
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 287, 96, 95, 24, 23, 1] and each_date not in self.daterange.dr:
                    # notify the number of observations is incorrect
                    error_string2 = "Incomplete or overfilled day, %s, probe %s, total number of observations: %s" %(each_date, probe_code, num_total_obs)
                    # print error_string2
                    mylog.write('incompleteday', error_string2)

                    my_new_rows.append(['MS043',2, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None,"M", "None", "NA", self.server])
                    #newrow = ['MS043',2, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None,"M", "None", "NA", self.server]

                    continue
                else:
                    pass

                # Daily flag naming for accetable-- if the number of obs is 24, 'H', if it's 96, 'F'
                df = 'A'
                if num_total_obs == 24 or num_total_obs == 23:
                    df = 'H'
                elif num_total_obs == 96 or num_total_obs==95:
                    df = 'F'
                else:
                    df = 'A'

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
            
                # daily flag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F".
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = df
                else:
                    daily_flag = 'Q'

                # DAILY MEAN RELATIVE HUMIDITY
                try:
                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)
                
                except ZeroDivisionError:
                    mean_valid_obs = None

                # DAILY MAX RELATIVE HUMIDITY
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                        mylog.write("maxvalueerror", error_string3)

                # DAILY MAX TIME RELATIVE HUMIDITY
                try:
                    # get the time of that maximum - the two arrays of values, flags, and times are in line so we enumerate to it.
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set max valid obs and max valid time to none
                    if mean_valid_obs == None:
                        max_valid_time = "None"
                    else: 
                        error_string4 = "error in max_valid_time for %s on %s" %(probe_code, each_date)
                        mylog.write("max_time_error", error_string4)

                # DAILY MAX FLAG RELATIVE HUMIDITY
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs][0]
                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = "M"
                    
                    else:
                        error_string5 = "error in max_valid_flag for %s on %s" %(probe_code, each_date)
                        mylog.write("max_flag_error", error_string5)

                # DAILY MINIMUM RELATIVE HUMIDITY
                try:
                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:
                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        error_string6 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)
                        mylog.write("min_value_error", error_string6)

                # DAILY MINIMUM TIME RELATIVE HUMIDITY 
                try:
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = "None"
                    else:
                        error_string7 = "error in min_valid_time for %s on %s" %(probe_code, each_date)
                        mylog.write("mintimeerror", error_string7)

                # DAILY MINIMUM FLAG RELATIVE HUMIDITY
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs][0]
                
                else:
                    error_string8= "min flag is none on %s, %s" %(probe_code, each_date)
                    mylog.write("minflagerror",error_string8)
                    
                    if mean_valid_obs == None:
                        min_flag = "M"
                    
                    else:
                        error_string9 = "error in minimum flagging for %s on %s" %(probe_code, each_date)
                        mylog.wirte("minflagerror2", error_string9)

                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = [df]
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

                try: 
                    if max_flag[0].strip() =="":
                        max_flag = df
                    else:
                        pass
                
                except IndexError:
                    # the maximum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        max_flag = ["M"]

                if mean_valid_obs == None:
                    daily_flag == "M"
                else:
                    pass

                # names for source
                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04312"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04312"
                else:
                    print("no server given")

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043',2, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag[0], datetime.datetime.strftime(min_valid_time[0], '%H%M'), "NA", self.server]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043',2, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None,"M", "None", "NA", self.server]

                
                my_new_rows.append(newrow)
        
            mylog.dump()
        return my_new_rows

class DewPoint(object):
    """ 
    Generates dewpoint daily data, consolidates or adds flags, and does methods
    """

    def __init__(self, startdate, enddate, server):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        # the date range contains the start date and the end date, 
        # and a method for making it into a human readable
        self.daterange = DateRange(startdate,enddate)

        # entity is integer
        self.entity = 7

        # server is SHELDON or STEWARTIA
        self.server = server
        
        # query against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        # attack data is a method for condensing the data into a structure for processing
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population. THIS MAY CAUSE A NATURAL SWITCH BETWEEN LOGGER_PRO and LOGGER_NEW BECAUSE PRO DOESN'T HAVE THE SAME COLUMNS BUT THAT WILL ONLY WORK IN AIRTEMP ATTRIBUTE"""

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        # Queries for SHELDON and STEWARTIA
        if self.server == "SHELDON":
                dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
                dbname = "FSDBDATA.dbo."
    
        try:
            query = "SELECT DATE_TIME, PROBE_CODE, DEWPT_MEAN, DEWPT_MEAN_FLAG, DEWPT_MIN, DEWPT_MIN_FLAG, DEWPT_MAX, DEWPT_MAX_FLAG from " + dbname + "MS04317 WHERE DATE_TIME >= \'"  + humanrange[0] +  "\' AND DATE_TIME < \'" + humanrange[1]+  "\' ORDER BY DATE_TIME ASC"

            # execute the query
            self.cursor.execute(query)
        
        except Exception:
            query = "SELECT DATE_TIME, PROBE_CODE, DEWPT_MEAN, DEWPT_MEAN_FLAG from " + dbname + "MS04317 WHERE DATE_TIME >= \'"  + humanrange[0] +  "\' AND DATE_TIME < \'" + humanrange[1]+  "\' ORDER BY DATE_TIME ASC"
            print "... we had to use the old dewpoint method!- new method could not be applied ..."
            # execute the query
            self.cursor.execute(query)
        

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        # query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100

            try:
                this_method = str(row[1])
            except Exception:
                this_method = "DEW999"
            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"

        return this_height, this_method, this_sitecode


    def attack_data(self):
        """ gather the daily dewpoint data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:
            
            # get only the day from the incoming result row    
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old -datetime.timedelta(days =1)

            # extract day info
            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)

            # extract the probe code
            probe_code = str(row[1])

            if probe_code not in od:

                try:
                    od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'minval':[str(row[4])], 'minflag': [str(row[5])], 'maxval':[str(row[6])], 'maxflag':[str(row[7])], 'timekeep':[dt_old]}}
                
                except Exception:
                    od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'minval':[str(row[2])], 'minflag': [str(row[3])], 'maxval':[str(row[2])], 'maxflag':[str(row[3])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                # if the date isn't there and we dont have one fo the new methods
                if dt not in od[probe_code]:
                    try:
                        
                        od[probe_code][dt] = {'val': [str(row[2])], 'fval': [str(row[3])], 'minval':[str(row[4])], 'minflag': [str(row[5])], 'maxval':[str(row[6])], 'maxflag':[str(row[7])], 'timekeep':[dt_old]}
                    except Exception:
                        # old method
                        od[probe_code][dt] = {'val': [str(row[2])], 'fval': [str(row[3])], 'minval':[str(row[2])], 'minflag': [str(row[3])], 'maxval':[str(row[2])], 'maxflag':[str(row[3])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    
                    # in all these cases, the "try" block is for the "new method" and the exception fails over to the old method
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))

                    try:
                        od[probe_code][dt]['minval'].append(str(row[4]))
                        od[probe_code][dt]['minflag'].append(str(row[5]))
                        od[probe_code][dt]['maxval'].append(str(row[6]))
                        od[probe_code][dt]['maxflag'].append(str(row[7]))
                    except Exception:
                        od[probe_code][dt]['minval'].append(str(row[2]))
                        od[probe_code][dt]['minflag'].append(str(row[3]))
                        od[probe_code][dt]['maxval'].append(str(row[2]))
                        od[probe_code][dt]['maxflag'].append(str(row[3]))

                    # the timekeep attribute holds onto the 5 minute time, so that if it happens to be the max or the min of the day we have it handy
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        # return the output dictionary keyed on probe and day
        return od


    def condense_data(self):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_dewpt')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a SHELDON cursor if you do not have one to get the LTERLogger_new.dbo.method_history_daily table.
        cursor_sheldon = fc.form_connection("SHELDON")
            
        for probe_code in self.od.keys():

            if "DEWR" not in probe_code:
                # get the height, method_code, and sitecode from the height_and_method_getter function  
                height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            elif "DEWR" in probe_code:
                # default data
                height, method_code, site_code = 150, "DEW999", "REFS"+probe_code[-4:-2]

            # valid_dates are the dates we will iterate over to do the computation of the daily dew points
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:

                # number of observations that aren't "none"
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                num_valid_obs_min = len([x for x in self.od[probe_code][each_date]['minval'] if x != 'None'])
                num_valid_obs_max = len([x for x in self.od[probe_code][each_date]['maxval'] if x != 'None'])

                # notify if there are no observations
                if num_valid_obs == 0:
                    error_string = ("there are only null values on %s for %s") %(each_date, probe_code)
                    mylog.write('nullday', error_string)
                
                # get the TOTAL number of obs, should be 288, 96, or 24 - includes "missing"- 
                # we only need to count the value-- if it's missing from the mean we aren't going to see a min and max of course
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not 288, 96, or 24
                if num_total_obs not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]:

                    # notify the number of observations is incorrect
                    error_string2 = "Incomplete or overfilled day, %s, probe %s, total number of observations: %s" %(each_date, probe_code, num_total_obs)
                    # print error_string2
                    mylog.write('incompleteday', error_string2)

                    newrow = ['MS043', 7, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None, "M", "None", "NA", source]

                    continue

                else:
                    pass

                # Daily flag naming for acceptable-- if the number of obs is 24, 'H' (hourly), if it's 96, 'F'

                # default condition
                df = 'A'
                
                if num_total_obs == 24:
                    df = 'H'
                elif num_total_obs == 96:
                    df = 'F'
                
                # if it's some other value we're not going to write it anyway, so be sure the df is dynamically set to A   
                else:
                    df = 'A'

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_missing_obs_min = len([x for x in self.od[probe_code][each_date]['minflag'] if x == 'M' or x == 'I'])
                num_missing_obs_max = len([x for x in self.od[probe_code][each_date]['maxflag'] if x == 'M' or x == 'I'])

                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_questionable_obs_min = len([x for x in self.od[probe_code][each_date]['minflag'] if x == 'Q' or x == 'O'])
                num_questionable_obs_max = len([x for x in self.od[probe_code][each_date]['maxflag'] if x == 'Q' or x == 'O'])
                
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
                
                num_estimated_obs_min = len([x for x in self.od[probe_code][each_date]['minflag'] if x == 'E'])
                num_estimated_obs_max = len([x for x in self.od[probe_code][each_date]['maxflag'] if x == 'E'])
                

                # daily flag on the mean: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                
                elif (num_missing_obs + num_questionable_obs)/num_total_obs > 0.05:
                    daily_flag = 'Q'
                
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F" or "H" here
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = df
                else:
                    daily_flag = 'Q'


                # DAILY FLAG ON MAXIMUMS
                if num_missing_obs_max/num_total_obs >= 0.2:
                    max_flag = 'M'
                
                elif (num_missing_obs_max + num_questionable_obs_max)/num_total_obs > 0.05:
                    max_flag = 'Q'
                
                elif (num_estimated_obs_max)/num_total_obs >= 0.05:
                    max_flag = 'E'

                # default to "A"
                elif (num_estimated_obs_max + num_missing_obs_max + num_questionable_obs_max)/num_total_obs <= 0.05:
                    max_flag = df
                
                else:
                    max_flag = 'Q'


                # DAILY FLAG ON MINIMUMS
                if num_missing_obs_min/num_total_obs >= 0.2:
                    min_flag = 'M'
                
                elif (num_missing_obs_min + num_questionable_obs_min)/num_total_obs > 0.05:
                    min_flag = 'Q'
                
                elif (num_estimated_obs_min)/num_total_obs >= 0.05:
                    min_flag = 'E'

                # default to "A"
                elif (num_estimated_obs_min + num_missing_obs_min + num_questionable_obs_min)/num_total_obs <= 0.05:
                    min_flag = df
                
                else:
                    min_flag = 'Q'

                # DAILY MEAN DEWPOINT
                
                # the mean is the mean of the day where the values are not none
                try:
                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)
                    
                except ZeroDivisionError:
                    # if the whole day is missing, then the mean_valid_obs is None
                    mean_valid_obs = None

                # DAILY MAX DEWPOINT

                # the max of the day is the max of the max column
                try:
                    max_valid_obs = round(np.max([float(x) for x in self.od[probe_code][each_date]['maxval'] if x != 'None']),3)

                except Exception:
                    
                    # the max of the day is the max of the mean column
                    try:
                        max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                    except ValueError:
                        # check to see if the whole day was missing, if so, set it to none
                        if mean_valid_obs == None:
                            max_valid_obs = None
                        else:
                            error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                            #print "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                            mylog.write('max_value_error', error_string3)

                # DAILY MAX TIME OF DEWPOINT
                # the max time is stolen from the corresponding five minutes 
                try:
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['maxval']) if j != "None" and round(float(j),3) == max_valid_obs]
                
                except Exception:
                    # the max time is stolen from the mean corresponding five minutes
                    try:
                        max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                    except ValueError:
                        # check to see if the the whole day was missing, if so, set it to none
                        if mean_valid_obs == None:
                            max_valid_time = None
                        
                        else: 
                            error_string4 = "error in max_valid_time for %s on %s" %(probe_code, each_date)
                            my_log.write('max_time_error', error_string4)
                

                # DAILY MINIMUM DEWPOINT
                try:
                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['minval'] if x != 'None']),3)
                
                except Exception:
                    try:
                        min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                    except Exception:
                        if mean_valid_obs == None:
                            min_valid_obs = None
                        else:
                            error_string5 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)
                            mylog.write('min_value_error',error_string5)

                # MINIMUM TIME DEWPOINT
                try:
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['minval']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except Exception:
                    try:
                        min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                    except ValueError:
                        if mean_valid_obs == None:
                            min_valid_time = None
                        else:
                            error_string6 = "error in min_valid_time for %s on %s" %(probe_code, each_date)
                            mylog.write('mintimeerror', error_string6)

                # final check on missing days
                if mean_valid_obs == None:
                    daily_flag == "M"
                    max_flag = "M"
                    min_flag = "M"
                else:
                    pass


                # set the sources for the output based on the input server
                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04317"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_Pro_MS04317"
                else:
                    print("no server given")

                try:
                    maxt = datetime.datetime.strftime(max_valid_time[0], '%H%M')
                except IndexError:
                    maxt = "None"

                try:
                    mint = datetime.datetime.strftime(min_valid_time[0], '%H%M')
                except IndexError:
                    mint = "None"

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043', 7, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag, maxt, min_valid_obs, min_flag, mint, "NA", source]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043', 7, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None, "M", "None", "NA", source]

                my_new_rows.append(newrow)
    
        mylog.dump()
        return my_new_rows


class VPD(object):
    """ 
    Generates VPD daily data, consolidates or adds flags, and does methods -- depricated! Works on the old data sets!
    """

    def __init__(self, startdate, enddate, server):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """

        import form_connection as fc

        self.cursor = fc.form_connection(server)

        self.daterange = DateRange(startdate,enddate)

        self.entity = 8

        self.server = server
        
        self.querydb()

        self.od = {}

        self.od = self.attack_data()

    def querydb(self):
        """ warning that this may become flaky in the case where there is airtemp max but not vpd max"""

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
                dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
                dbname = "FSDBDATA.dbo."
             
        query = "SELECT DATE_TIME, PROBE_CODE, VPD_MEAN, VPD_MEAN_FLAG from " + dbname + "MS04318 WHERE DATE_TIME >= \'"  + humanrange[0] +  "\' AND DATE_TIME < \'" + humanrange[1]+  "\' ORDER BY DATE_TIME ASC"
        
        # execute the query
        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100
            try:
                this_method = str(row[1])
            except Exception:
                this_method = "VPD999"
            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"
        
        return this_height, this_method, this_sitecode

    def attack_data(self):
        """ gather the daily vpd data """
        
        od = {}

        for row in self.cursor:
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    od[probe_code][dt] = {'val': [str(row[2])], 'fval':[str(row[3])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        Gathers VPD
        """
        
        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
            
            if "VPDR" not in probe_code:

                # get the height, method_code, and sitecode from the height_and_method_getter function  
                height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            elif "VPDR" in probe_code:
                # height is 150m?, method is AIR999, site is REFS plus last two digits of probe_code
                height, method_code, site_code = 150, "VPD999", "REFS"+probe_code[-4:-2]

            # valid_dates are the dates we will iterate over to do the computation of the daily airtemperature
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])

                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                
                # get the number of obs total 
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.daterange.dr[0]:

                    # break on missing dates and continue to the next

                    error_string2 = "Incomplete or overfilled day:  %s, probe %s, total number of observations: %s" %(each_date, probe_code, num_total_obs)
                    continue
                else:
                    pass

                # Daily flag naming for accetable-- if the number of obs is 24, 'H', if it's 96, 'F'
                df = 'A'
                if num_total_obs == 24:
                    df = 'H'
                elif num_total_obs == 96:
                    df = 'F'
                else:
                    df = 'A'

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
            
                # daily flag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F".
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = df
                else:
                    daily_flag = 'Q'

                # DAILY MEAN VPD
                try:
                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)
                
                except ZeroDivisionError:
                    # if the whole day is missing, then the mean_valid_obs is None
                    mean_valid_obs = None

                # DAILY MAX VPD
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)

                # DAILY MAX TIME VPD
                try:
                    # get the time of that maximum - the two arrays of values, flags, and times are in line so we enumerate to it.
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set max valid obs and max valid time to none
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        error_string4 = "error in max_valid_time for %s on %s" %(probe_code, each_date)

                # DAILY MAX FLAG VPD
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]
                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        error_string5 = "error in max_valid_flag for %s on %s" %(probe_code, each_date)

                # DAILY MINIMUM VPD
                try:
                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:
                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        error_string6 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)

                # DAILY MINIMUM TIME VPD 
                try:
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        error_string7 = "error in min_valid_time for %s on %s" %(probe_code, each_date)

                # DAILY MINIMUM FLAG VPD
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                else:
                    error_string8= "min flag is none on %s, %s" %(probe_code, each_date)
                    
                    if mean_valid_obs == None:
                        min_flag = ["M"]
                    
                    else:
                        error_string9 = "error in minimum flagging for %s on %s" %(probe_code, each_date)
                        mylog.write("missing_flags", error_string9)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = [df]
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

                try: 
                    if max_flag[0].strip() =="":
                        max_flag = [df]
                    else:
                        pass
                
                except IndexError:
                    # the maximum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        max_flag = ["M"]

                if mean_valid_obs == None:
                    daily_flag == "M"
                else:
                    pass

                # names for source
                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04318"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04318"
                else:
                    print("no server given")

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag[0], datetime.datetime.strftime(min_valid_time[0], '%H%M'), "NA", self.server]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None,"M", "None", "NA", self.server]

                
                my_new_rows.append(newrow)
        
        return my_new_rows

class VPD2(object):
    """ 
    Generates VPD daily data, consolidates or adds flags, and does methods - from the combination approach
    """

    def __init__(self, startdate, enddate, server):
        """ uses form_connection to communicate with the database; 
        queries for a start and end date and possibly a probe code, 
        generates a date-mapped dictionary. """

        import form_connection as fc

        self.cursor = fc.form_connection(server)

        self.daterange = DateRange(startdate,enddate)

        self.entity = 8

        self.server = server
        
        self.querydb()

        self.od = {}

        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population.
        for this VPD we must specify explicitly which probe_codes we want to use
        """

        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
                dbname = "LTERLogger_pro.dbo."
        
        elif self.server == "STEWARTIA":
                dbname = "FSDBDATA.dbo."
             
        query = "SELECT " + dbname + "MS04311.DATE_TIME, " + dbname +"MS04311.PROBE_CODE, " + dbname + "MS04311.AIRTEMP_MEAN, " + dbname + "MS04311.AIRTEMP_MEAN_FLAG, RH.RELHUM_MEAN, RH.RELHUM_MEAN_FLAG from " + dbname + "MS04311 inner join " + dbname + "MS04312 as RH on " + dbname + "MS04311.date_time = RH.date_time AND " + dbname + "MS04311.HEIGHT = RH.HEIGHT AND " + dbname + "MS04311.SITECODE = RH.SITECODE WHERE " + dbname + "MS04311.DATE_TIME >= \'"  + humanrange[0] +  "\' AND "+ dbname +"MS04311.DATE_TIME < \'" + humanrange[1]+  "\' ORDER BY " + dbname + "MS04311.DATE_TIME ASC"
        
        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        # query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100
            try:
                this_method = str(row[1])
            except Exception:
                this_method = "VPD999"

            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"

        return this_height, this_method, this_sitecode

    def attack_data(self):
        """ gather the daily vpd data """
        
        od = {}

        for row in self.cursor:
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            
            probe_code = 'VPD'+str(row[1])[3:]

            # skip values which are from PRIMET aspirated and other aspirated
            if probe_code[-2:] in ['05','06','07','08','09','10']:
                continue

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'airval': [str(row[2])], 'airfval': [str(row[3])], 'relval':[str(row[4])], 'relfval': [str(row[5])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'airval': [str(row[2])], 'airfval': [str(row[3])], 'relval':[str(row[4])], 'relfval': [str(row[5])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['airval'].append(str(row[2]))
                    od[probe_code][dt]['airfval'].append(str(row[3]))
                    od[probe_code][dt]['relval'].append(str(row[4]))
                    od[probe_code][dt]['relfval'].append(str(row[5]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        Calculates VPD from raw data!
        """
        mylog = LogIssues('calculatedVPDlog')
        
        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
            #print probe_code
            
            if "VPDR" not in probe_code:

                # get the height, method_code, and sitecode from the height_and_method_getter function  
                height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            elif "VPDR" in probe_code:
                # height is 150m?, method is AIR999, site is REFS plus last two digits of probe_code
                height, method_code, site_code = 150, "VPD999", "REFS"+probe_code[-4:-2]

            # valid_dates are the dates we will iterate over to do the computation of the daily airtemperature
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs_air = len([x for x in self.od[probe_code][each_date]['airval'] if x != 'None'])
                num_valid_obs_rel = len([x for x in self.od[probe_code][each_date]['relval'] if x != 'None'])

                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs_air == 0 or num_valid_obs_rel == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write('nullday',error_string)

                # get the number of obs total 
                num_total_obs_air = len(self.od[probe_code][each_date]['airval'])
                num_total_obs_rel = len(self.od[probe_code][each_date]['relval'])

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs_air not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]:

                    # break on missing dates and continue to the next
                    error_string2 = "Incomplete or overfilled day-AIRTEMP (called first):  %s, probe %s, total number of observations: %s" %(each_date, probe_code, num_total_obs_air)

                    mylog.write('incompleteday', error_string2)

                    continue

                elif num_total_obs_rel not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]:

                    # break on missing dates and continue to the next
                    error_string2 = "Incomplete or overfilled day- RELHUM (AIRTEMP OK):  %s, probe %s, total number of observations: %s" %(each_date, probe_code, num_total_obs_air)

                    mylog.write('incompleteday', error_string2)
                    continue

                else:
                    pass

                # Daily flag naming for accetable-- if the number of obs is 24, 'H', if it's 96, 'F'
                df = 'A'
                
                if num_total_obs_air == 24:
                    df = 'H'
                elif num_total_obs_air == 96:
                    df = 'F'
                else:
                    df = 'A'
                
                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs_air = len([x for x in self.od[probe_code][each_date]['airfval'] if x == 'M' or x == 'I'])
                
                num_questionable_obs_air = len([x for x in self.od[probe_code][each_date]['airfval'] if x == 'Q' or x == 'O'])
                
                num_estimated_obs_air = len([x for x in self.od[probe_code][each_date]['airfval'] if x == 'E'])
                
                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs_rel = len([x for x in self.od[probe_code][each_date]['relfval'] if x == 'M' or x == 'I'])
                
                num_questionable_obs_rel = len([x for x in self.od[probe_code][each_date]['relfval'] if x == 'Q' or x == 'O'])
                
                num_estimated_obs_rel = len([x for x in self.od[probe_code][each_date]['relfval'] if x == 'E'])
                
            
                # daily flag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_air/num_total_obs_air >= 0.2 or num_missing_obs_rel/num_total_obs_air >= 0.2:
                    daily_flag = 'M'

                elif (num_missing_obs_air + num_questionable_obs_air)/num_total_obs_air >= 0.05:
                    daily_flag = 'Q'

                elif (num_missing_obs_rel + num_questionable_obs_rel)/num_total_obs_rel >= 0.05:
                    daily_flag = 'Q'

                elif (num_estimated_obs_air)/num_total_obs_air >= 0.05:
                    daily_flag = 'E'

                elif (num_estimated_obs_rel)/num_total_obs_rel >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F".
                elif (num_estimated_obs_air + num_missing_obs_air + num_questionable_obs_air)/num_total_obs_rel <= 0.05 and (num_estimated_obs_rel + num_missing_obs_rel + num_questionable_obs_rel)/num_total_obs_rel <= 0.05:
                    daily_flag = df

                else:
                    daily_flag = 'Q'

                # ZIP DAILY VALUES TOGETHER

                # get the daily airtemp values, daily relhum values, and daily times
                pre_sample = zip(self.od[probe_code][each_date]['airval'], self.od[probe_code][each_date]['relval'], self.od[probe_code][each_date]['timekeep'])

                # zip em together in a tuple
                good_sample = [tup for tup in pre_sample if 'None' not in tup]
                
                # break out each part to make teh vpd calculations easier to understand 
                sample_at = [float(val) for (val,_,_) in good_sample]
                sample_rh = [float(val) for (_,val,_) in good_sample]
                sample_dates = [val for (_,_,val) in good_sample]
                

                # the days satvp - a function of air temp
                sample_SatVP = [6.1094*math.exp(17.625*(float(AT))/(243.04+float(AT))) for AT in sample_at]

                try:
                    # the days dewpt - a function of RH and satvp (Ta)
                    sample_Td = [237.3*math.log(SatVP*float(RH)/611.)/(7.5*math.log(10)-math.log(SatVP*float(RH)/611.)) for SatVP, RH in itertools.izip(sample_SatVP, sample_rh)]
                except Exception:
                    error_string = 'one or more component of VPD was Null, could not compute daily dewpoint'
                    mylog.write('null_component', error_string)
                    sample_Td = []
                # the days vpd - a function of rel and satvp
                try:
                    sample_vpd = [((100-float(RH))*0.01)*SatVP for SatVP, RH in itertools.izip(sample_SatVP, sample_rh)]
                except Exception:
                    error_string = 'one or more component of VPD was Null, could not compute daily dewpoint'
                    mylog.write('null_component', error_string)
                    sample_vpd = []
                # the daily vapor pressure, a function of dewpoint
                try:
                    sample_regvap = [6.1094*math.exp((17.625*float(Td))/(243.04+float(Td))) for Td in sample_Td]
                except Exception:
                    mylog.write('one or more component of VPD was Null, could not compute a daily vapor pressure, %s')
                    sample_regvap=[]

                # if no values exist, then the daily flag is missing
                if sample_vpd == []:
                    mean_valid_obs_vpd = None
                    daily_flag = 'M'

                # sat vp could potentially exist without RH because is from air temp, so has to be flagged alone
                if sample_SatVP == []:
                    mean_valid_obs_satvp = None
                    daily_flag_satvp = 'M'

                # vapor pressure needs humidity and temp, but we might as well test its emptiness to give the none
                if sample_regvap == []:
                    mean_valid_obs_regvap = None
                    daily_flag_regvap = 'M'


                # DAILY MEANS FOR  VPD, SATVP, REGULAR VP -- we've gotten rid of the None values 
                try:
                    mean_valid_obs_vpd = round(sum(sample_vpd)/len(sample_vpd),3)
                except Exception:
                    mean_valid_obs_vpd = None
                    daily_flag = 'M'
                    error_string = "error in computing the VPD on %s, probe %s" %(each_date, probe_code)
                    mylog.write('computedvpdfail', error_string)

                # mean satvp
                try:
                    mean_valid_obs_satvp = round(sum(sample_SatVP)/len(sample_SatVP),3)
                    daily_flag_satvp = df
                except Exception:
                    mean_valid_obs_satvp = None
                    daily_flag_satvp = "M"
                    error_string = "error in computing the satvp on %s, probe %s" %(each_date, probe_code)
                    mylog.write('computedsatvpfail', error_string)

                # mean vap
                try:
                    mean_valid_obs_regvap = round(sum(sample_regvap)/len(sample_regvap),3)
                    daily_flag_regvap = daily_flag
                except Exception:
                    mean_valid_obs_regvap = None
                    daily_flag_regvap="M"
                    error_string = "error in computing the vapor pressure on %s, probe %s" %(each_date, probe_code)
                    mylog.write('computedvaporpressure', error_string)

                # DAILY MAX VPD
                try:
                    max_valid_obs_vpd = round(max(sample_vpd),3)
                except Exception:
                    max_valid_obs_vpd = None
                    max_flag_vpd = 'M'
                    error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                    mylog.write("maxvalueerror", error_string3)

                # DAILY MAX SATVP
                try:
                    max_valid_obs_satvp = round(max(sample_SatVP),3)
                    max_flag_satvp = daily_flag_satvp
                except Exception:
                    max_valid_obs_satvp = None
                    max_flag_satvp = 'M'
                    error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                    mylog.write("maxvalueerror_satvp", error_string3)

                # DAILY MAX REGVAP
                try:
                    max_valid_obs_regvap = round(max(sample_regvap),3)
                    max_flag_regvap = daily_flag_regvap
                except Exception:
                    max_valid_obs_regvap = None
                    max_flag_regvap = 'M'
                    error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                    mylog.write("maxvalueerror_regvap", error_string3)

                # DAILY MAX TIME VPD
                try:
                    index_of_max = sample_vpd.index(max(sample_vpd))
                    max_valid_time_vpd = datetime.datetime.strftime(sample_dates[index_of_max], '%H%M')
                    max_flag_vpd = daily_flag
                
                except Exception:
                    max_valid_time_vpd = None
                    error_string4 = "error in max_valid_time for %s on %s" %(probe_code, each_date)
                    mylog.write("max_time_error", error_string4)

                # DAILY MINIMUM VPD
                try:
                    min_valid_obs_vpd = round(min(sample_vpd),3)
                    min_flag_vpd = daily_flag
                except Exception:
                    min_valid_obs_vpd = None
                    min_flag_vpd = 'M'
                    error_string3 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)
                    mylog.write("min_value_error", error_string3)

                # DAILY MINIMUM SATVP
                try:
                    min_valid_obs_satvp = round(min(sample_SatVP),3)
                    min_flag_satvp = daily_flag_satvp
                except Exception:
                    min_valid_obs_satvp = None
                    min_flag_satvp = 'M'
                    error_string3 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)
                    mylog.write("min_value_error_satvp", error_string3)

                # DAILY MINIMUM REGVAP
                try:
                    min_valid_obs_regvap = round(min(sample_regvap),3)
                    min_flag_regvap = daily_flag_regvap
                except Exception:
                    min_valid_obs_regvap = None
                    min_flag_regvap = 'M'
                    error_string3 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)
                    mylog.write("min_value_error_satvp", error_string3)

                # DAILY MINTIME VPD
                try:
                    index_of_min = sample_vpd.index(min(sample_vpd))
                    min_valid_time_vpd = datetime.datetime.strftime(sample_dates[index_of_min], '%H%M')
                    min_flag_vpd = daily_flag
                except Exception:
                    min_valid_time_vpd = None
                    min_flag_vpd = 'M'
                    error_string4 = "error in min_valid_time for %s on %s" %(probe_code, each_date)
                    mylog.write("mintimeerror", error_string4)


                # names for source
                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS043-CALCULATED"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS043-CALCULATED"
                else:
                    print("no server given")

                # in the best possible case, we print it out just as it is here: 
                # right now I am giving the max flags on satvp and regularvp the same as their mean flags
                try:
                   # newrow = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs_vpd, daily_flag, max_valid_obs_vpd, max_flag_vpd, max_valid_time_vpd, min_valid_obs_vpd, min_flag_vpd, min_valid_time_vpd, mean_valid_obs_regvap, daily_flag_regvap, max_valid_obs_regvap, max_flag_regvap, min_valid_obs_regvap, min_flag_regvap, mean_valid_obs_satvp, daily_flag_satvp, max_valid_obs_satvp, max_flag_satvp, min_valid_obs_satvp, min_flag_satvp, "NA", self.server]

                    newrow = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs_vpd, daily_flag, max_valid_obs_vpd, max_flag_vpd, max_valid_time_vpd, min_valid_obs_vpd, min_flag_vpd, min_valid_time_vpd, mean_valid_obs_regvap, daily_flag_regvap, max_valid_obs_regvap, max_flag_regvap, min_valid_obs_regvap, min_flag_regvap, "NA", self.server]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    #newrow = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None,"M", "None", None, "M", None, "M", None, "M", None, "M", None, "M", None, "M" "NA", self.server]
                    newrow = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None,"M", "None", None, "M", None, "M", None, "M", "NA", self.server]

                
                my_new_rows.append(newrow)
        
            mylog.dump()
        return my_new_rows


class PhotosyntheticRad(object):
    """ 
    PAR radiations- right now only one that measures this
    """
    def __init__(self, startdate, enddate, server):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """
        import form_connection as fc

        self.cursor = fc.form_connection(server)
        self.daterange = DateRange(startdate,enddate)

        self.entity = 22

        self.server = server
        
        self.querydb()

        self.od = {}

        # attack data is a method for condensing the data into a structure for processing
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database - now can go to either sheldon or stewartia"""

        # human-readable date range for the database
        humanrange = self.daterange.human_readable()

        # Queries for SHELDON and STEWARTIA
        if self.server == "SHELDON":
                dbname = "LTERLogger_pro.dbo."

        elif self.server == "STEWARTIA":
                dbname = "FSDBDATA.dbo."
        else:
            pass

        try:
            query = "SELECT DATE_TIME, PROBE_CODE, PAR_MEAN, PAR_MEAN_FLAG, PAR_MAX from " + dbname + "MS04332 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

            self.cursor.execute(query)
        
        except Exception:
            query = "SELECT DATE_TIME, PROBE_CODE, PAR_MEAN, PAR_MEAN_FLAG from " + dbname + "MS04332 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"
        
            self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        #query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100

            try:
                this_method = str(row[1])
            except Exception:
                this_method = "PAR999"

            try:
                this_sitecode = str(row[2])
            except Exception:
                this_method = "ANYMET"
        
        return this_height, this_method, this_sitecode

    def attack_data(self):
        """ gather the daily PAR data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day from the incoming result row  - this is the original result   
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
            
            # resolve the midnight point to the prior day - ie 1/1/2010 00:00:00 is actually 12/31/2014 24:00:00

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)
            else:
                pass

            
            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            
            probe_code = str(row[1])
            
            if probe_code not in od:
                
                try:
                    # if there is par max
                    od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'mval': [str(row[4])], 'timekeep':[dt_old]}}
                except Exception:
                    # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                    od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:

                    try:
                        # if there is PAR MAX
                        od[probe_code][dt] = {'val': [str(row[2])], 'fval':[str(row[3])], 'mval': [str(row[4])], 'timekeep':[dt_old]}
                    except Exception:
                        # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                        od[probe_code][dt] = {'val': [str(row[2])], 'fval':[str(row[3])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:

                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                    # if there is a max
                    try:
                        od[probe_code][dt]['mval'].append(str(row[4]))
                    except Exception:
                        pass

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        PAR does not need any special methods to compress it.

        """

        mylog = LogIssues('parlog')
        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the height, method_code, and sitecode from the height_and_method_getter function  
            height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            # valid_dates are the dates we will iterate over to do the computation of the daily airtemperature
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write('nullday', error_string)
                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Fully missed days are ok
                    error_string = "the total number of observations on %s is %s for probe %s" %(each_date, num_total_obs, probe_code)
                    mylog.write("missing_observations", error_string)
                    continue

                else:
                    pass

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
            
                # daily flag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F". 
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of the daily observations - not including the missing, questionable, or estimated ones
                try:
                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)
                
                except ZeroDivisionError:
                    # if the whole day is missing, then the mean_valid_obs is None
                    mean_valid_obs = None

                try:
                    # if Max is given in the data
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['mval'] if x != 'None']),3)
                # MAX PAR from the data
                except Exception:
                    try:
                        max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                    except ValueError:
                        # check to see if the whole day was missing, if so, set MAX to None
                        if mean_valid_obs == None:
                            max_valid_obs = None
                        else:
                            error_string2 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                            mylog.write('max_obs_error', error_string2)

                try:
                # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['mval']) if j != "None" and round(float(j),3) == max_valid_obs]
                except Exception:
                    try:
                        # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                        max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]
                    except ValueError:
                        # check to see if the the whole day was missing, if so, set it to none
                        if mean_valid_obs == None:
                            max_valid_time = None
                        else: 
                            error_string2 = "error in max_valid_time for %s on %s" %(probe_code, each_date)
                            mylog.write('max_time_error', error_string2)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        error_string3 = "error in max_valid_flag for %s on %s" %(probe_code, each_date)
                        mylog.write('max_flag_error', error_string3)


                try: 
                    if max_flag[0].strip() =="":
                        max_flag = ["A"]
                    else:
                        pass
                
                except IndexError:
                    # the maximum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        max_flag = ["M"]

                if mean_valid_obs == None:
                    daily_flag == "M"
                else:
                    pass

                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04332"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04332"
                else:
                    print("no server given")

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043',22, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), "NA", source]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043', 22, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None, "NA", source]
                my_new_rows.append(newrow)
        return my_new_rows

class SoilTemperature(object):
    """ 
    Soil Temperature - no method for five minute maxes
    """
    def __init__(self, startdate, enddate, server):

        # same stucture as the old air temperature and relhum. We are not updating this for every 5 minutes max!
        self.cursor = fc.form_connection(server)
        self.daterange = DateRange(startdate,enddate)
        self.entity = 21
        self.server = server

        # query the database and write to od
        self.querydb() 
        self.od = {}

        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database - now can go to either sheldon or stewartia"""
        
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."

        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."

        query = "SELECT DATE_TIME, PROBE_CODE, SOILTEMP_MEAN, SOILTEMP_MEAN_FLAG from " + dbname + "MS04331 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it.
            Sheldon cursor is passed in if the original cursor is Stewartia"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT depth, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 depth, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 0
            try:
                this_method = str(row[1])
            except Exception:
                this_method = 'SOI999'
            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = 'ANYMET'
        
        return this_height, this_method, this_sitecode
    
    def attack_data(self):
        """ gather the daily soil temperaturedata """
        od = {}

        for row in self.cursor:

            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            # offset to midnight
            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days = 1)
            
            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)

            probe_code = str(row[1])

            if probe_code not in od:
                od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                if dt not in od[probe_code]:
                    od[probe_code][dt] = {'val': [str(row[2])], 'fval':[str(row[3])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_soiltemp')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
            # soil probes from VANMET do not appear to work?
            if "SOIR" not in probe_code and probe_code != "SOIVAN02" and probe_code != "SOIVAN03" and probe_code != "SOIVAN04" and probe_code != "SOIVAN01":
                height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            elif "SOIR" in probe_code:
                height, method_code, site_code = 0, "SOI999", "REFS"+probe_code[-4:-2]

            elif probe_code == "SOIVAN01" or probe_code == "SOIVAN02" or probe_code == "SOIVAN03" or probe_code == "SOIVAN04":
                height, method_code, site_code = 20, "SOI000", "VANMET"

            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:

                # get the number of valid observations - all that are not "None"
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])

                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write('nullday',error_string)
                
                # get the number of obs total 
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]:

                    # break on missing dates and continue to the next

                    error_string2 = "Incomplete or overfilled day:  %s, probe %s, total number of observations: %s" %(each_date, probe_code, num_total_obs)
                    mylog.write('incompleteday', error_string2)
                    continue
                else:
                    pass

                # Daily flag naming for accetable-- if the number of obs is 24, 'H', if it's 96, 'F'
                df = 'A'
                if num_total_obs == 24:
                    df = 'H'
                elif num_total_obs == 96:
                    df = 'F'
                else:
                    df = 'A'

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
            
                # daily flag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F".
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = df
                else:
                    daily_flag = 'Q'

                # DAILY MEAN SOIL TEMPERATURE
                try:
                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)
                
                except ZeroDivisionError:
                    # if the whole day is missing, then the mean_valid_obs is None
                    mean_valid_obs = None

                # DAILY MAX SOIL TEMPERATURE
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                        mylog.write("maxvalueerror", error_string3)

                # DAILY MAX TIME SOIL TEMPERATURE
                try:
                    # get the time of that maximum - the two arrays of values, flags, and times are in line so we enumerate to it.
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set max valid obs and max valid time to none
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        error_string4 = "error in max_valid_time for %s on %s" %(probe_code, each_date)
                        mylog.write("max_time_error", error_string4)

                # DAILY MAX FLAG SOIL TEMPERATURE
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs][0]
                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = "M"
                    
                    else:
                        error_string5 = "error in max_valid_flag for %s on %s" %(probe_code, each_date)
                        mylog.write("max_flag_error", error_string5)

                # DAILY MINIMUM SOIL TEMPERATURE
                try:
                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:
                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        error_string6 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)
                        mylog.write("min_value_error", error_string6)

                # DAILY MINIMUM TIME SOIL TEMPERATURE 
                try:
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        error_string7 = "error in min_valid_time for %s on %s" %(probe_code, each_date)
                        mylog.write("mintimeerror", error_string7)

                # DAILY MINIMUM FLAG SOIL TEMPERATURE
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs][0]
                
                else:
                    error_string8= "min flag is none on %s, %s" %(probe_code, each_date)
                    mylog.write("minflagerror",error_string8)
                    
                    if mean_valid_obs == None:
                        min_flag = "M"
                    
                    else:
                        print("error in minimum flagging for %s on %s") %(probe_code, each_date)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = df
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

                try: 
                    if max_flag[0].strip() =="":
                        max_flag = df
                    else:
                        pass
                
                except IndexError:
                    # the maximum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        max_flag = "M"

                if mean_valid_obs == None:
                    daily_flag == "M"
                else:
                    pass

                # names for source
                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04331"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04331"
                else:
                    print("no server given")

                if max_flag == None or max_flag == "None":
                    max_flag = daily_flag
                else:
                    pass

                if min_flag == None or min_flag == "None":
                    min_flag = daily_flag
                else:
                    pass

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043',21, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag, datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag, datetime.datetime.strftime(min_valid_time[0], '%H%M'), "NA", self.server]
                
                   
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043',21, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None,"M", "None", "NA", self.server]

                
                my_new_rows.append(newrow)
        
            mylog.dump()
        return my_new_rows
    

class SoilWaterContent(object):

    """
    Computes Soil Water Content from 5 minute data
    """


    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        # the date range contains the start date and the end date, 
        # and a method for making it into a human readable
        self.daterange = DateRange(startdate,enddate)
        
        # entity is integer 23
        self.entity = 23

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database - now can go to either sheldon or stewartia"""
        
        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."

        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."
        
        query = "SELECT DATE_TIME, PROBE_CODE, SOILWC_MEAN, SOILWC_MEAN_FLAG from " + dbname + "MS04333 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        # query = "SELECT depth, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100
            try:
                this_method = str(row[1])
            except Exception:
                this_method = "SWC999"
            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"
        return this_height, this_method, this_sitecode
    
    def attack_data(self):
        """ gather the daily SWC data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)

            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'val': [str(row[2])], 'fval':[str(row[3])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od


    def condense_data(self):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_soilwc')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
            
            if "SWCR" not in probe_code:
                # get the height, method_code, and sitecode from the height_and_method_getter function  
                height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            elif "SWCR" in probe_code:
                # height is 150m?, method is AIR999, site is REFS plus last two digits of probe_code
                height, method_code, site_code = 0, "SWC999", "REFS"+probe_code[-4:-2]

            # valid_dates are the dates we will iterate over to do the computation of the daily soil water content
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass


            for each_date in valid_dates:

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])

                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write('nullday',error_string)
                
                # get the number of obs total 
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.daterange.dr[0]:

                    # break on missing dates and continue to the next

                    error_string2 = "Incomplete or overfilled day:  %s, probe %s, total number of observations: %s" %(each_date, probe_code, num_total_obs)
                    mylog.write('incompleteday', error_string2)
                    continue
                else:
                    pass

                # Daily flag naming for accetable-- if the number of obs is 24, 'H', if it's 96, 'F'
                df = 'A'

                if num_total_obs == 24:
                    df = 'H'
                elif num_total_obs == 96:
                    df = 'F'
                else:
                    df = 'A'

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
            
                # daily flag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F".
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = df
                else:
                    daily_flag = 'Q'

                # DAILY MEAN SOIL WATER CONTENT
                try:
                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)
                
                except ZeroDivisionError:
                    # if the whole day is missing, then the mean_valid_obs is None
                    mean_valid_obs = None

                # DAILY MAX SOIL WATER CONTENT
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                        mylog.write("max_value_error", error_string3)

                # DAILY MAX TIME SOIL WC
                try:
                    # get the time of that maximum - the two arrays of values, flags, and times are in line so we enumerate to it.
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set max valid obs and max valid time to none
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        error_string4 = "error in max valid time for %s on %s" %(probe_code, each_date)
                        mylog.write("max_time_error", error_string4)

                # DAILY MAX FLAG SOIL WC
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs][0]
                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = "M"
                    
                    else:
                        error_string5 = "error in max_valid_flag for %s on %s" %(probe_code, each_date)
                        mylog.write("max_flag_error", error_string5)

                # DAILY MINIMUM SOIL WC
                try:
                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:
                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        error_string6 = "error in min_valid_obs for %s on %s" %(probe_code, each_date)
                        mylog.write("min_value_error", error_string6)

                # DAILY MINIMUM TIME SOILWC 
                try:
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        error_string7 = "error in min_valid_time for %s on %s" %(probe_code, each_date)
                        mylog.write("min_time_error", error_string7)

                # DAILY MINIMUM FLAG SOIL WC
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs][0]
                
                else:
                    error_string8= "min flag is none on %s, %s" %(probe_code, each_date)
                    mylog.write("min_flag_error",error_string8)
                    
                    if mean_valid_obs == None:
                        min_flag = "M"
                    
                    else:
                        error_string9 = "minimum flagging throws an unknown error for %s on %s" %(probe_code, each_date)
                        mylog.write("min_flag_error", error_string9)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = df
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

                try: 
                    if max_flag[0].strip() =="":
                        max_flag = df
                    else:
                        pass
                
                except IndexError:
                    # the maximum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        max_flag = "M"

                if mean_valid_obs == None:
                    daily_flag == "M"
                else:
                    pass

                # names for source
                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04333"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04333"
                else:
                    print("no server given")

                if max_flag == None or max_flag == "None":
                    max_flag = daily_flag
                else:
                    pass

                if min_flag == None or min_flag == "None":
                    min_flag = daily_flag
                else:
                    pass

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043',23, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag, datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag, datetime.datetime.strftime(min_valid_time[0], '%H%M'), "NA", self.server]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043',23, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None,"M", "None", "NA", self.server]

                
                my_new_rows.append(newrow)
        
            mylog.dump()
        return my_new_rows

class Precipitation(object):

    """
    Daily Precipitation. 
    """

    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        # the date range contains the start date and the end date, 
        # and a method for making it into a human readable
        self.daterange = DateRange(startdate,enddate)
        
        # entity is integer 3
        self.entity = 3

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        self.od = self.attack_data()


    def querydb(self):
        """ 
        queries the data base, returning a cursor to the requested data
        """

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."

        query = "SELECT DATE_TIME, PROBE_CODE, PRECIP_TOT, PRECIP_TOT_FLAG from " + dbname + "MS04313 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    
    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        #if probe_code == 'PPTVAR02':
        #    probe_code = 'PPTVAN02'

        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
        #print query
            
        try:
            for row in cursor_sheldon:
                try:
                    this_height = int(row[0])
                except Exception:
                    this_height = 100
                try:
                    this_method = str(row[1])
                except Exception:
                    this_method = "PPT999"
                try:
                    this_sitecode = str(row[2])
                except Exception:
                    this_sitecode = "ANYMET"

            #print row
        except Exception:
            print "...exception thrown on precipitation methods..."
        
        return this_height, this_method, this_sitecode

    def attack_data(self):
        """ gather the daily precipitation data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day      
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
          
            # fix the day for the zeroth hour; if the hour is 0 and the minute is 0 then the day is the previous day
            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old- datetime.timedelta(days = 1)
            # otherwise, that is not the case! -> THIS CHANGE MUST PERSIST IN ALL THE THINGS
            else:
                pass

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            #dt = datetime.datetime(dt_fixed.year, dt_fixed.month, dt_fixed.day)
            
            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min

                od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])]}}


            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'val': [str(row[2])], 'fval':[str(row[3])]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_precip')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor - used for getting the daily methods
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            # valid_dates are the dates we will iterate over to do the computation of the daily precip
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:
                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write("nullday", error_string)
                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                
                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!

                    error_string3 = "incomplete or overfilled day: the total number of observations on %s is %s for %s" %(each_date, num_total_obs, probe_code)
                    mylog.write("incomplete_day",error_string3)
                    continue


                else:
                    pass

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
            
                # daily flag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F"
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                try:
                    # sum up the observations - not including the missing, questionable, or estimated ones
                    total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                    # hacky summary - if the month is between May and September, if the precip is < 0, don't count it.
                    if each_date.month>5 or each_date.month <= 9:
                        total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None' and x >=0]),3)

                except Exception:
                    total_valid_obs = None
                    daily_flag = "M"


                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04313"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04313"
                else:
                    print("no server given")
                
                if daily_flag == 'None' or daily_flag == None:
                    daily_flag = 'Q'
                    source = self.server + "_flag_is_NULL_in_DB"
                else:
                    pass

                newrow = ['MS043',3, site_code, method_code, int(height), "2D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), total_valid_obs, daily_flag, "NA", source]


                my_new_rows.append(newrow)
    
        return my_new_rows

class SnowLysimeter(object):

    """
    Snow Lysimeter data does not include a "depth" but assign a 0 regardless 
    """

    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        # the date range contains the start date and the end date, 
        # and a method for making it into a human readable
        self.daterange = DateRange(startdate,enddate)
        
        # entity is integer 9
        self.entity = 9

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        self.od = self.attack_data()


    def querydb(self):
        """ 
        queries the data base, returning a cursor to the requested data
        """

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."

        query = "SELECT DATE_TIME, PROBE_CODE, SNOWMELT_TOT, SNOWMELT_TOT_FLAG from " + dbname + "MS04319 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_method = str(row[0])
            except Exception:
                this_method = "SNO999"
            try:
                this_sitecode = str(row[1])
            except Exception:
                this_sitecode = "ANYMET"
        return this_method, this_sitecode

    def attack_data(self):
        """ gather the daily precipitation data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old -datetime.timedelta(days=1)
            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'val': [str(row[2])], 'fval': [str(row[3])]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'val': [str(row[2])], 'fval':[str(row[3])]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['val'].append(str(row[2]))
                    od[probe_code][dt]['fval'].append(str(row[3]))

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_lys')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
       
            # get the height, method_code, and sitecode from the height_and_method_getter function  
            # doesn't look like we'll need any exceptions here right now
            method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            # valid_dates are the dates we will iterate over to do the computation of the daily precip
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:
                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write("nullday", error_string)
                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                
                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.daterange.dr[0]:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!

                    error_string3 = "incomplete or overfilled day: the total number of observations on %s is %s for %s" %(each_date, num_total_obs, probe_code)
                    mylog.write("incomplete_day",error_string3)
                    continue


                else:
                    pass

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc.
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])
            
                # daily flag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.05:
                    daily_flag = 'E'

                # because we are counting things which are not A, we don't need to deal with the case of "F"
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs)/num_total_obs <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                try:
                    # sum up the observations - not including the missing, questionable, or estimated ones
                    total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except Exception:
                    total_valid_obs = None
                    daily_flag = "M"


                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04319"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04319"
                else:
                    print("no server given")
                
                newrow = ['MS043',9, site_code, method_code, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), total_valid_obs, daily_flag, "NA", source]

                
                my_new_rows.append(newrow)
        mylog.dump()
        return my_new_rows

class Solar(object):
    """ computes Solar Radiation from Sheldon or Stewartia, NOT the net radiometer version!"""

    
    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        # the date range contains the start date and the end date, 
        # and a method for making it into a human readable
        self.daterange = DateRange(startdate,enddate)
        
        # entity is integer 5
        self.entity = 5

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        self.od = self.attack_data()


    def querydb(self):
        """ 
        queries the data base, returning a cursor to the requested data
        """

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."

        try:
            # if a solar max is given
            query = "SELECT DATE_TIME, PROBE_CODE, SOLAR_TOT, SOLAR_TOT_FLAG, SOLAR_MEAN, SOLAR_MEAN_FLAG, SOLAR_MAX from " + dbname + "MS04315 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

            self.cursor.execute(query)
        
        except Exception:
            # otherwise default to no solar max
            query = "SELECT DATE_TIME, PROBE_CODE, SOLAR_TOT, SOLAR_TOT_FLAG, SOLAR_MEAN, SOLAR_MEAN_FLAG from " + dbname + "MS04315 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

            self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100
            try:
                this_method = str(row[1])
            except Exception:
                this_method = "SOL999"
            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"

        return this_height, this_method, this_sitecode
    
    def attack_data(self):
        """ gather the daily solar data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            # if the hour is the midnight hour we move it back to the prior day
            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old-datetime.timedelta(days=1)
            else:
                pass

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                try:
                    # if the probe code isn't there, get the day, total val, total value flag, mean value, mean value flag,  max value, and the the time of the max value
                    od[probe_code] = {dt:{'tot_val': [str(row[2])], 'tot_fval': [str(row[3])], 'mean_val': [str(row[4])], 'mean_fval': [str(row[5])], 'mval':[str(row[6])],'timekeep':[dt_old]}}
                except Exception:
                    # if the probe code isn't there and max is not there, get the day, total val, total value flag, mean value, mean value flag, and store the time to match to the max and min
                    od[probe_code] = {dt:{'tot_val': [str(row[2])], 'tot_fval': [str(row[3])], 'mean_val': [str(row[4])], 'mean_fval': [str(row[5])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    try:
                        # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, etc. 
                        od[probe_code][dt] = {'tot_val': [str(row[2])], 'tot_fval':[str(row[3])], 'mean_val': [str(row[4])], 'mean_fval': [str(row[5])], 'mval':[str(row[6])], 'timekeep':[dt_old]}

                    except Exception:
                        # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                        od[probe_code][dt] = {'tot_val': [str(row[2])], 'tot_fval':[str(row[3])], 'mean_val': [str(row[4])], 'mean_fval': [str(row[5])], 'timekeep':[dt_old]}


                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['tot_val'].append(str(row[2]))
                    od[probe_code][dt]['tot_fval'].append(str(row[3]))
                    od[probe_code][dt]['mean_val'].append(str(row[4]))
                    od[probe_code][dt]['mean_fval'].append(str(row[5]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                    # if a five minute max is given for solar
                    try:
                        od[probe_code][dt]['mval'].append(str(row[6]))
                    except Exception:
                        pass

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self, *args):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_solar')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
       
            height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:
                # get the number of valid observations
                num_valid_obs_tot = len([x for x in self.od[probe_code][each_date]['tot_val'] if x != 'None'])
                num_valid_obs_mean = len([x for x in self.od[probe_code][each_date]['mean_val'] if x != 'None'])
                
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs_tot == 0 or num_valid_obs_mean == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write("nullday", error_string)
                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['tot_val'])
                
                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.daterange.dr[0]:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!

                    error_string3 = "incomplete or overfilled day: the total number of observations on %s is %s for %s" %(each_date, num_total_obs, probe_code)
                    mylog.write("incomplete_day",error_string3)
                    continue


                else:
                    pass


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily "total"
                num_missing_obs_tot = len([x for x in self.od[probe_code][each_date]['tot_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_tot = len([x for x in self.od[probe_code][each_date]['tot_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_tot = len([x for x in self.od[probe_code][each_date]['tot_fval'] if x == 'E'])


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily "mean"
                num_missing_obs_mean = len([x for x in self.od[probe_code][each_date]['mean_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_mean = len([x for x in self.od[probe_code][each_date]['mean_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_mean = len([x for x in self.od[probe_code][each_date]['mean_fval'] if x == 'E'])

                # daily flag, total: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_tot/num_total_obs >= 0.2:
                    daily_flag_tot = 'M'
                elif (num_missing_obs_tot + num_questionable_obs_tot)/num_total_obs >= 0.05:
                    daily_flag_tot = 'Q'
                elif (num_estimated_obs_tot)/num_total_obs >= 0.05:
                    daily_flag_tot = 'E'
                elif (num_estimated_obs_tot + num_missing_obs_tot + num_questionable_obs_tot)/num_total_obs <= 0.05:
                    daily_flag_tot = 'A'
                else:
                    daily_flag_tot = 'Q'

                # daily flag, mean: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_mean/num_total_obs >= 0.2:
                    daily_flag_mean = 'M'
                elif (num_missing_obs_mean + num_questionable_obs_mean)/num_total_obs >= 0.05:
                    daily_flag_mean = 'Q'
                elif (num_estimated_obs_mean)/num_total_obs >= 0.05:
                    daily_flag_mean = 'E'
                elif (num_estimated_obs_mean + num_missing_obs_mean + num_questionable_obs_mean)/num_total_obs <= 0.05:
                    daily_flag_mean = 'A'
                else:
                    daily_flag_mean = 'Q'

                # compute the mean of the daily observations of mean - not including the missing, questionable, or estimated ones -- do the mean first so you can do it to tag the total if it's bad
                
                try:

                    #mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['mean_val'] if x != 'None'])/num_valid_obs_mean),3)
                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['mean_val'] if x != 'None'])/num_valid_obs_mean))

                except ZeroDivisionError:
                    # if the whole day is missing, then the mean_valid_obs is None
                    mean_valid_obs = None

                # compute the total of the daily observations of total - not including the missing, questionable, or estimated ones - use the mean to tag if it's bad

                if mean_valid_obs != None:
                
                    #total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['tot_val'] if x != 'None']),3)
                    total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['tot_val'] if x != 'None']))
                else:
                    # if there's no mean there's no total, either
                    total_valid_obs = None
                
                    
                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['mval'] if x != 'None']),3)
                except Exception:
                    try:
                        max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['mean_val'] if x != 'None']),3)
                    except ValueError:
                        # check to see if the whole day was missing, if so, set it to none
                        if mean_valid_obs == None:
                            max_valid_obs = None
                        else:

                            error_string3 = "error in max_valid_obs for %s on %s" %(probe_code, each_date)
                            mylog.write("max_valid_obs_error",error_string3)
                try:
                    # get the time of that maximum, if you have the maximum column, it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['mval']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:
                    try:
                        # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                        max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['mean_val']) if j != "None" and round(float(j),3) == max_valid_obs]

                    except ValueError:
                        # check to see if the the whole day was missing, if so, set it to none
                        # *** something I was testing, : for index,j in enumerate(self.od[probe_code][each_date]['val']):
                        #    print index, j ****
                        if mean_valid_obs == None:
                            max_valid_time = None
                        else: 
                            error_string4 = "error in max_valid_time for %s on %s" %(probe_code, each_date)
                            mylog.write("max_valid_time_error",error_string4)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['mean_fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['mean_val']) if j != "None" and round(float(j),3) == max_valid_obs][0]
                   
                    # if there is definitely a max observation, the max flag is ok
                    if max_flag == "None":
                        max_flag = "A"
                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = "M"
                    
                    else:

                        error_string5 = "error in max_valid_flag for %s on %s" %(probe_code, each_date)
                        mylog.write("max_valid_flag_error",error_string5)


                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04305"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04305"
                else:
                    print("no server given")

                try:
                    newrow = ['MS043',5, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), total_valid_obs, daily_flag_tot, mean_valid_obs, daily_flag_mean, max_valid_obs, max_flag, datetime.datetime.strftime(max_valid_time[0], '%H%M'), "NA", self.server]
                
                except Exception:

                    newrow = ['MS043',5, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None, "M", "None", "NA", self.server]

                my_new_rows.append(newrow)
        mylog.dump()
        return my_new_rows

class SnowDepth(object):
    """ combines MS04320 an MS04340 to make MS04310"""
    
    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        # the date range contains the start date and the end date, 
        # and a method for making it into a human readable
        self.daterange = DateRange(startdate,enddate)
        
        # entity is integer 5
        self.entity = 10

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        self.od = self.attack_data()


    def querydb(self):
        """ 
        queries the data base, returning a cursor to the requested data
        """

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."

        query = "SELECT " + dbname + "MS04340.DATE_TIME, " + dbname + "MS04340.PROBE_CODE, " + dbname + "MS04340.SWE_MED, " + dbname + "MS04340.SWE_MED_FLAG, DEEP.SNODEP_MED, DEEP.SNODEP_MED_FLAG from " + dbname + "MS04340 inner join " + dbname + "MS04320 as DEEP on " + dbname + "MS04340.date_time = DEEP.date_time AND " + dbname + "MS04340.PROBE_CODE = DEEP.PROBE_CODE WHERE " + dbname + "MS04340.DATE_TIME >= \'"  + humanrange[0] +  "\' AND "+ dbname +"MS04340.DATE_TIME < \'" + humanrange[1]+  "\' ORDER BY " + dbname + "MS04340.DATE_TIME ASC"

        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_method = str(row[0])
            except Exception:
                this_method = "SNO999"
            try:
                this_sitecode = str(row[1])
            except Exception:
                this_sitecode = "ANYMET"

        return this_method, this_sitecode
    
    def attack_data(self):
        """ gather the daily solar data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old -datetime.timedelta(days =1)

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'swe_val': [str(row[2])], 'swe_fval': [str(row[3])], 'sno_val': [str(row[4])], 'sno_fval': [str(row[5])], 'hour':[dt_old.hour]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'swe_val': [str(row[2])], 'swe_fval': [str(row[3])], 'sno_val': [str(row[4])], 'sno_fval': [str(row[5])], 'hour':[dt_old.hour]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['swe_val'].append(str(row[2]))
                    od[probe_code][dt]['swe_fval'].append(str(row[3]))
                    od[probe_code][dt]['sno_val'].append(str(row[4]))
                    od[probe_code][dt]['sno_fval'].append(str(row[5]))
                    od[probe_code][dt]['hour'].append(dt_old.hour)
                
                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self, *args):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_snowdepth')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
       
            # get the height, method_code, and sitecode from the height_and_method_getter function  
            # doesn't look like we'll need any exceptions here right now
            method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            # valid_dates are the dates we will iterate over to do the computation of the daily precip
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:
                # get the number of valid observations
                num_valid_obs_swe = len([x for x in self.od[probe_code][each_date]['swe_val'] if x != 'None'])
                num_valid_obs_sno = len([x for x in self.od[probe_code][each_date]['sno_val'] if x != 'None'])
                
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs_swe == 0 or num_valid_obs_sno == 0:
                    error_string = "there are only null values on %s for %s" %(each_date, probe_code)
                    mylog.write("nullday", error_string)
                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['tot_val'])
                
                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.daterange.dr[0]:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!

                    error_string3 = "incomplete or overfilled day: the total number of observations on %s is %s for %s" %(each_date, num_total_obs, probe_code)
                    mylog.write("incomplete_day",error_string3)
                    continue


                else:
                    pass


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily "total"
                num_missing_obs_swe = len([x for x in self.od[probe_code][each_date]['swe_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_swe = len([x for x in self.od[probe_code][each_date]['swe_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_swe = len([x for x in self.od[probe_code][each_date]['swe_fval'] if x == 'E'])


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily "mean"
                num_missing_obs_sno = len([x for x in self.od[probe_code][each_date]['sno_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_sno = len([x for x in self.od[probe_code][each_date]['sno_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_sno = len([x for x in self.od[probe_code][each_date]['sno_fval'] if x == 'E'])

                # daily flag, total: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_sno/num_total_obs >= 0.2:
                    daily_flag_sno = 'M'
                elif (num_missing_obs_tot + num_questionable_obs_sno)/num_total_obs >= 0.05:
                    daily_flag_sno = 'Q'
                elif (num_estimated_obs_sno)/num_total_obs >= 0.05:
                    daily_flag_sno = 'E'
                elif (num_estimated_obs_sno + num_missing_obs_sno + num_questionable_obs_sno)/num_total_obs <= 0.05:
                    daily_flag_sno = 'A'
                else:
                    daily_flag_sno = 'Q'

                # daily flag, mean: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_swe/num_total_obs >= 0.2:
                    daily_flag_swe = 'M'
                elif (num_missing_obs_swe + num_questionable_obs_swe)/num_total_obs >= 0.05:
                    daily_flag_swe = 'Q'
                elif (num_estimated_obs_swe)/num_total_obs >= 0.05:
                    daily_flag_swe = 'E'
                elif (num_estimated_obs_swe + num_missing_obs_swe + num_questionable_obs_swe)/num_total_obs <= 0.05:
                    daily_flag_swe = 'A'
                else:
                    daily_flag_swe = 'Q'

                # compute the median on swe
                
                try:
                    swe_valid_obs = round(np.median([float(x)for index,x in enumerate(self.od[probe_code][each_date]['swe_val']) if x != 'None' and self.od['probe_code'][each_date]['hour']==0]),3)

                except Exception:
                    # if the whole day is missing, then the mean_valid_obs is None
                    swe_valid_obs = None


                # compute the total of the daily observations of median snow not including the missing, questionable, or estimated ones - use the mean to tag if it's bad

                if swe_valid_obs != None:
                    median_valid_obs = round(np.median([float(x) for index,x in enumerate(self.od[probe_code][each_date]['sno_val']) if x != 'None' and self.od['probe_code'][each_date]['hour']==0]),3)

                else:
                    # if there's no mean there's no total, either
                    median_valid_obs = None
                    daily_flag_sno = "M"

                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA-CONDENSED"
                
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO-CONDENSED"
                
                else:
                    print("no server given")

                try:
                    newrow = ['MS043', 10, site_code, method_code, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), swe_valid_obs, daily_flag_swe, median_valid_obs, daily_flag_sno, "NA", self.server]
                
                except Exception:

                    newrow = ['MS043', 10, site_code, method_code, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "NA", self.server]
                
                my_new_rows.append(newrow)
        mylog.dump()
        return my_new_rows

class NetRadiometer(object):

    """ computes Solar Radiation from Sheldon or Stewartia, THIS IS THE NET RADIOMETER!!"""

    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)
        self.daterange = DateRange(startdate,enddate)
        
        # entity is integer 25
        self.entity = 25

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        self.od = self.attack_data()


    def querydb(self):
        """ 
        queries the data base, returning a cursor to the requested data
        """
        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."

        query = "SELECT DATE_TIME, PROBE_CODE, SW_IN_MEAN, SW_IN_MEAN_FLAG, SW_OUT_MEAN, SW_OUT_MEAN_FLAG, LW_IN_MEAN, LW_IN_MEAN_FLAG, LW_OUT_MEAN, LW_OUT_MEAN_FLAG, NR_TOT_MEAN, NR_TOT_MEAN_FLAG, SENSOR_TEMP, SENSOR_TEMP_FLAG from " + dbname + "MS04335 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc" 
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:

            try:
                this_height = int(row[0])
            except Exception:
                this_height = 100

            try:
                this_method = str(row[1])
            except Exception:
                this_method = "RAD999"

            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"
        
            return this_height, this_method, this_sitecode
    

    def attack_data(self):
        """ gather the daily net radiometer data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour==0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'swin_val': [str(row[2])], 'swin_fval': [str(row[3])], 'swout_val': [str(row[4])], 'swout_fval': [str(row[5])], 'lwin_val': [str(row[6])], 'lwin_fval': [str(row[7])], 'lwout_val': [str(row[8])], 'lwout_fval': [str(row[9])], 'nr_val': [str(row[10])], 'nr_fval': [str(row[11])], 'temp_val': [str(row[12])], 'temp_fval': [str(row[13])],'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'swin_val': [str(row[2])], 'swin_fval': [str(row[3])], 'swout_val': [str(row[4])], 'swout_fval': [str(row[5])], 'lwin_val': [str(row[6])], 'lwin_fval': [str(row[7])], 'lwout_val': [str(row[8])], 'lwout_fval': [str(row[9])], 'nr_val': [str(row[10])], 'nr_fval': [str(row[11])], 'temp_val': [str(row[12])], 'temp_fval': [str(row[13])],'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['swin_val'].append(str(row[2]))
                    od[probe_code][dt]['swin_fval'].append(str(row[3]))
                    od[probe_code][dt]['swout_val'].append(str(row[4]))
                    od[probe_code][dt]['swout_fval'].append(str(row[5]))
                    od[probe_code][dt]['lwin_val'].append(str(row[6]))
                    od[probe_code][dt]['lwin_fval'].append(str(row[7]))
                    od[probe_code][dt]['lwout_val'].append(str(row[8]))
                    od[probe_code][dt]['lwout_fval'].append(str(row[9]))
                    od[probe_code][dt]['nr_val'].append(str(row[10]))
                    od[probe_code][dt]['nr_fval'].append(str(row[11]))
                    od[probe_code][dt]['temp_val'].append(str(row[12]))
                    od[probe_code][dt]['temp_fval'].append(str(row[13]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self, *args):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_solar')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
       
            # get the height, method_code, and sitecode from the height_and_method_getter function  
            # doesn't look like we'll need any exceptions here right now
            height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            # valid_dates are the dates we will iterate over to do the computation of the daily precip
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass

            for each_date in valid_dates:

                # get the number of valid observations
                num_valid_obs_swin = len([x for x in self.od[probe_code][each_date]['swin_val'] if x != 'None'])
                num_valid_obs_swout = len([x for x in self.od[probe_code][each_date]['swout_val'] if x != 'None'])
                num_valid_obs_lwin = len([x for x in self.od[probe_code][each_date]['lwin_val'] if x != 'None'])
                num_valid_obs_lwout = len([x for x in self.od[probe_code][each_date]['lwout_val'] if x != 'None'])
                num_valid_obs_nr = len([x for x in self.od[probe_code][each_date]['nr_val'] if x != 'None'])
                num_valid_obs_temp = len([x for x in self.od[probe_code][each_date]['temp_val'] if x != 'None'])

                if num_valid_obs_swin == 0:
                    error_string = "There are only null values on %s for %s on the SWIN" %(each_date, probe_code)
                    mylog.write('nullday', error_string)

                if num_valid_obs_swout == 0:
                    error_string = "There are only null values on %s for %s on the SWOUT" %(each_date, probe_code)
                    mylog.write('nullday', error_string)

                if num_valid_obs_lwin == 0:
                    error_string = "There are only null values on %s for %s on the LWIN" %(each_date, probe_code)
                    mylog.write('nullday', error_string)

                if num_valid_obs_lwout == 0:
                    error_string = "There are only null values on %s for %s on the LWOUT" %(each_date, probe_code)
                    mylog.write('nullday', error_string)

                if num_valid_obs_nr == 0:
                    error_string = "There are only null values on %s for %s on the NR" %(each_date, probe_code)
                    mylog.write('nullday', error_string)

                if num_valid_obs_temp == 0:
                    error_string = "There are only null values on %s for %s on the TEMP" %(each_date, probe_code)
                    mylog.write('nullday', error_string)

                # get the number of obs - not sure if maybe it won't be the same for all? i think it will but just in case - this is just counting the day, actually, so it doesn't really matter which one we pull
                num_total_obs = len(self.od[probe_code][each_date]['swin_val'])
                

                # if it's not a total of observations on that day that we would expect, then print this-- we expect that since this is counting up the rows, it shouldn't matter which it gets!
                if num_total_obs not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]: 
                    
                    error_string2 = "incomplete day: the total number of observations on %s is %s on probe %s" %(each_date, num_total_obs, probe_code)
                    mylog.write('incomplete_day', error_string2)
                    continue


                else:
                    pass

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily sw in
                num_missing_obs_swin = len([x for x in self.od[probe_code][each_date]['swin_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_swin = len([x for x in self.od[probe_code][each_date]['swin_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_swin = len([x for x in self.od[probe_code][each_date]['swin_fval'] if x == 'E'])


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily sw out
                num_missing_obs_swout = len([x for x in self.od[probe_code][each_date]['swout_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_swout = len([x for x in self.od[probe_code][each_date]['swout_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_swout = len([x for x in self.od[probe_code][each_date]['swout_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily lw in
                num_missing_obs_lwin= len([x for x in self.od[probe_code][each_date]['lwin_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_lwin = len([x for x in self.od[probe_code][each_date]['lwin_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_lwin = len([x for x in self.od[probe_code][each_date]['lwin_fval'] if x == 'E'])


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily lw out
                num_missing_obs_lwout = len([x for x in self.od[probe_code][each_date]['lwout_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_lwout = len([x for x in self.od[probe_code][each_date]['lwout_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_lwout = len([x for x in self.od[probe_code][each_date]['lwout_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily net radiometer
                num_missing_obs_nr = len([x for x in self.od[probe_code][each_date]['nr_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_nr = len([x for x in self.od[probe_code][each_date]['nr_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_nr = len([x for x in self.od[probe_code][each_date]['nr_fval'] if x == 'E'])


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily temperature
                num_missing_obs_temp = len([x for x in self.od[probe_code][each_date]['temp_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_temp = len([x for x in self.od[probe_code][each_date]['temp_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_temp = len([x for x in self.od[probe_code][each_date]['temp_fval'] if x == 'E'])

                # daily flag, total: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_swin/num_total_obs >= 0.2:
                    daily_flag_swin = 'M'
                elif (num_missing_obs_swin + num_questionable_obs_swin)/num_total_obs >= 0.05:
                    daily_flag_swin = 'Q'
                elif (num_estimated_obs_swin)/num_total_obs >= 0.05:
                    daily_flag_swin = 'E'
                elif (num_estimated_obs_swin + num_missing_obs_swin + num_questionable_obs_swin)/num_total_obs <= 0.05:
                    daily_flag_swin = 'A'
                else:
                    daily_flag_swin = 'Q'

                # daily flag, mean: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_swout/num_total_obs >= 0.2:
                    daily_flag_swout = 'M'
                elif (num_missing_obs_swout + num_questionable_obs_swout)/num_total_obs > 0.05:
                    daily_flag_swout = 'Q'
                elif (num_estimated_obs_swout)/num_total_obs > 0.05:
                    daily_flag_swout = 'E'
                elif (num_estimated_obs_swout + num_missing_obs_swout + num_questionable_obs_swout)/num_total_obs <= 0.05:
                    daily_flag_swout = 'A'
                else:
                    daily_flag_swout = 'Q'

                # daily flag, total: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_lwin/num_total_obs >= 0.2:
                    daily_flag_lwin = 'M'
                elif (num_missing_obs_lwin + num_questionable_obs_lwin)/num_total_obs > 0.05:
                    daily_flag_lwin = 'Q'
                elif (num_estimated_obs_lwin)/num_total_obs > 0.05:
                    daily_flag_lwin = 'E'
                elif (num_estimated_obs_lwin + num_missing_obs_lwin + num_questionable_obs_lwin)/num_total_obs <= 0.05:
                    daily_flag_lwin = 'A'
                else:
                    daily_flag_lwin = 'Q'

                # daily flag, mean: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_lwout/num_total_obs >= 0.2:
                    daily_flag_lwout = 'M'
                elif (num_missing_obs_lwout + num_questionable_obs_lwout)/num_total_obs > 0.05:
                    daily_flag_lwout = 'Q'
                elif (num_estimated_obs_lwout)/num_total_obs > 0.05:
                    daily_flag_lwout = 'E'
                elif (num_estimated_obs_lwout + num_missing_obs_lwout + num_questionable_obs_lwout)/num_total_obs <= 0.05:
                    daily_flag_lwout = 'A'
                else:
                    daily_flag_lwout = 'Q'


                # daily flag, total: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_nr/num_total_obs >= 0.2:
                    daily_flag_nr = 'M'
                elif (num_missing_obs_nr + num_questionable_obs_nr)/num_total_obs > 0.05:
                    daily_flag_nr = 'Q'
                elif (num_estimated_obs_nr)/num_total_obs > 0.05:
                    daily_flag_nr = 'E'
                elif (num_estimated_obs_nr + num_missing_obs_nr + num_questionable_obs_nr)/num_total_obs <= 0.05:
                    daily_flag_nr = 'A'
                else:
                    daily_flag_nr = 'Q'

                # daily flag, mean: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_temp/num_total_obs >= 0.2:
                    daily_flag_temp = 'M'
                elif (num_missing_obs_temp + num_questionable_obs_temp)/num_total_obs > 0.05:
                    daily_flag_temp = 'Q'
                elif (num_estimated_obs_temp)/num_total_obs > 0.05:
                    daily_flag_temp = 'E'
                elif (num_estimated_obs_temp + num_missing_obs_temp + num_questionable_obs_temp)/num_total_obs <= 0.05:
                    daily_flag_temp = 'A'
                else:
                    daily_flag_temp = 'Q'
                

                # compute the mean of the daily observations of mean - not including the missing, questionable, or estimated ones -- do the mean first so you can do it to tag the total if it's bad
                
                try:
                    mean_swin = round(float(sum([float(x) for x in self.od[probe_code][each_date]['swin_val'] if x != 'None'])/num_valid_obs_swin),3)

                except ZeroDivisionError:
                    # if the whole day is missing, then the mean is None
                    mean_swin = None

                try:
                    mean_swout = round(float(sum([float(x) for x in self.od[probe_code][each_date]['swout_val'] if x != 'None'])/num_valid_obs_swout),3)

                except ZeroDivisionError:
                    # if the whole day is missing, then the mean is None
                    mean_swout = None

                try:
                    mean_lwin = round(float(sum([float(x) for x in self.od[probe_code][each_date]['lwin_val'] if x != 'None'])/num_valid_obs_lwin),3)

                except ZeroDivisionError:
                    # if the whole day is missing, then the mean is None
                    mean_lwin = None

                try:
                    mean_lwout = round(float(sum([float(x) for x in self.od[probe_code][each_date]['lwout_val'] if x != 'None'])/num_valid_obs_lwout),3)

                except ZeroDivisionError:
                    # if the whole day is missing, then the mean  is None
                    mean_lwout = None


                # we may need to strip some of these numerics, as they are coming in as empties!
                try:
                    mean_nr = round(float(sum([float(x) for x in self.od[probe_code][each_date]['nr_val'] if x != 'None' and x.strip() != ''])/num_valid_obs_nr),3)

                except ZeroDivisionError:
                    # if the whole day is missing, then the mean  is None
                    mean_nr = None

                # we may need to strip some of these numerics, as they are coming in as empties!
                try:
                    mean_temp = round(float(sum([float(x) for x in self.od[probe_code][each_date]['temp_val'] if x != 'None' and x.strip() != ''])/num_valid_obs_temp),3)

                except ZeroDivisionError:
                    # if the whole day is missing, then the mean  is None
                    mean_temp = None


                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04335"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04335"
                else:
                    print("no server given")



                try:
                    newrow = ['MS043', 25, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_swin, daily_flag_swin, mean_swout, daily_flag_swout, mean_lwin, daily_flag_lwin, mean_lwout, daily_flag_lwout, mean_nr, daily_flag_nr, mean_temp, daily_flag_temp,"NA"]
                
                except Exception:
                    # which might happen if a day is just missing

                    newrow = ['MS043', 25, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None, "M", None, "M", None, "M", None, "M", "NA"]

                my_new_rows.append(newrow)
        mylog.dump()
        return my_new_rows

class Wind(object):

    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)
        self.daterange = DateRange(startdate,enddate)
        
        # entity is integer 4
        self.entity = 4

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        self.od = self.attack_data()

    def querydb(self):
        """ 
        queries the data base, returning a cursor to the requested data
        """

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."
        else:
            print "no server found"

       
        query = "SELECT DATE_TIME, PROBE_CODE, WSPD_PRO_MEAN, WSPD_PRO_MEAN_FLAG, WMAG_PRO_MEAN, WMAG_PRO_MEAN_FLAG, WDIR_PRO_MEAN, WDIR_PRO_MEAN_FLAG, WDIR_PRO_STDDEV, WDIR_PRO_STDDEV_FLAG from " + dbname + "MS04314 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 1000

            try:
                this_method = str(row[1])
            except Exception:
                this_method = "WND999"

            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"

        return this_height, this_method, this_sitecode

    def attack_data(self):
        """ gather the daily wind (propellor) data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)

            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'spd_val': [str(row[2])], 'spd_fval': [str(row[3])], 'mag_val': [str(row[4])], 'mag_fval': [str(row[5])], 'dir_val': [str(row[6])], 'dir_fval': [str(row[7])], 'dirstd_val': [str(row[8])], 'dirstd_fval': [str(row[9])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'spd_val': [str(row[2])], 'spd_fval': [str(row[3])], 'mag_val': [str(row[4])], 'mag_fval': [str(row[5])], 'dir_val': [str(row[6])], 'dir_fval': [str(row[7])], 'dirstd_val': [str(row[8])], 'dirstd_fval': [str(row[9])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['spd_val'].append(str(row[2]))
                    od[probe_code][dt]['spd_fval'].append(str(row[3]))
                    od[probe_code][dt]['mag_val'].append(str(row[4]))
                    od[probe_code][dt]['mag_fval'].append(str(row[5]))
                    od[probe_code][dt]['dir_val'].append(str(row[6]))
                    od[probe_code][dt]['dir_fval'].append(str(row[7]))
                    od[probe_code][dt]['dirstd_val'].append(str(row[8]))
                    od[probe_code][dt]['dirstd_fval'].append(str(row[9]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od


    def condense_data(self, *args):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above - wind prop data
        """
        mylog = LogIssues('mylog_wind')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
       
            # get the height, method_code, and sitecode from the height_and_method_getter function  
            # doesn't look like we'll need any exceptions here right now
            height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            # valid_dates are the dates we will iterate over to do the computation of the daily precip
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass

            for each_date in valid_dates:
                # get the number of valid observations
                num_valid_obs_spd = len([x for x in self.od[probe_code][each_date]['spd_val'] if x != 'None'])
                num_valid_obs_mag = len([x for x in self.od[probe_code][each_date]['mag_val'] if x != 'None'])
                num_valid_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])
                num_valid_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_val'] if x != 'None'])

                # get the number of obs
                num_total_obs_spd = len(self.od[probe_code][each_date]['spd_val'])
                num_total_obs_mag = len(self.od[probe_code][each_date]['mag_val'])
                num_total_obs_dir = len(self.od[probe_code][each_date]['dir_val'])
                num_total_obs_mag = len(self.od[probe_code][each_date]['dirstd_val'])

                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs_spd not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]: 
                    error_string = "the total number of observations on %s is %s on probe %s" %(each_date, num_total_obs_spd, probe_code)
                    mylog.write('incomplete_day', error_string)
                    continue
                
                else:
                    pass

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_spd = len([x for x in self.od[probe_code][each_date]['spd_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_spd = len([x for x in self.od[probe_code][each_date]['spd_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_spd = len([x for x in self.od[probe_code][each_date]['spd_fval'] if x == 'E'])
                

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean mag
                num_missing_obs_mag = len([x for x in self.od[probe_code][each_date]['mag_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_mag = len([x for x in self.od[probe_code][each_date]['mag_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_mag = len([x for x in self.od[probe_code][each_date]['mag_fval'] if x == 'E'])


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean dir
                num_missing_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily dirstd
                num_missing_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'E'])

                # daily flag, wind speed-- if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_spd/num_total_obs_spd >= 0.2:
                    daily_flag_spd = 'M'
                elif (num_missing_obs_spd + num_questionable_obs_spd)/num_total_obs_spd > 0.05:
                    daily_flag_spd = 'Q'
                elif (num_estimated_obs_spd)/num_total_obs_spd > 0.05:
                    daily_flag_spd = 'E'
                elif (num_estimated_obs_spd + num_missing_obs_spd + num_questionable_obs_spd)/num_total_obs_spd<= 0.05:
                    daily_flag_spd = 'A'
                else:
                    daily_flag_spd = 'Q'

                # daily flag, wind mag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_mag/num_total_obs_mag >= 0.2:
                    daily_flag_mag = 'M'
                elif (num_missing_obs_mag + num_questionable_obs_mag)/num_total_obs_mag > 0.05:
                    daily_flag_mag = 'Q'
                elif (num_estimated_obs_mag)/num_total_obs_mag > 0.05:
                    daily_flag_mag = 'E'
                elif (num_estimated_obs_mag + num_missing_obs_mag + num_questionable_obs_mag)/num_total_obs_mag <= 0.05:
                    daily_flag_mag = 'A'
                else:
                    daily_flag_mag = 'Q'

                # daily flag, wind dir: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_dir/num_total_obs_dir >= 0.2:
                    daily_flag_dir = 'M'
                elif (num_missing_obs_dir + num_questionable_obs_dir)/num_total_obs_dir > 0.05:
                    daily_flag_dir = 'Q'
                elif (num_estimated_obs_dir)/num_total_obs_dir > 0.05:
                    daily_flag_dir = 'E'
                elif (num_estimated_obs_dir + num_missing_obs_dir + num_questionable_obs_dir)/num_total_obs_dir <= 0.05:
                    daily_flag_dir = 'A'
                else:
                    daily_flag_dir = 'Q'

                # daily flag, wind dir std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_dirstd/num_total_obs_dir >= 0.2:
                    daily_flag_dirstd = 'M'
                elif (num_missing_obs_dirstd + num_questionable_obs_dirstd)/num_total_obs_dir > 0.05:
                    daily_flag_dirstd = 'Q'
                elif (num_estimated_obs_dirstd)/num_total_obs_dir > 0.05:
                    daily_flag_dirstd = 'E'
                elif (num_estimated_obs_dirstd + num_missing_obs_dirstd + num_questionable_obs_dirstd)/num_total_obs_dir <= 0.05:
                    daily_flag_dirstd = 'A'
                else:
                    daily_flag_dirstd = 'Q'

                try: 
                    # compute the mean daily wind speed -- just the mean of what is given...
                    daily_spd_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['spd_val'] if x != 'None'])/num_valid_obs_spd),3)

                except ZeroDivisionError:
                    # if the case is that there are no valid observations
                    daily_spd_valid_obs = None
                    
                    # magnitude and speed are therefore also M
                    daily_flag_spd = "M"
                    
                if num_valid_obs_spd != 0:
                # compute the daily resultant--- this one is a true joy.
                
                ## This is a working example using [2, 2, 2, 4, 4] for speed and [10, 10, 10, 350, 10] for direction:
                # daily_mag_x_part = (sum([speed * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(speedlist,dirlist) if speed != 'None' and x != 'None'])/5.)**2
                # daily_mag_y_part = (sum([speed * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(speedlist,dirlist) if speed != 'None' and x != 'None'])/5.)**2
                # math.sqrt(daily_mag_y_part + daily_mag_x_part)
                # >> Returns: 2.7653239207215683

                    daily_mag_y_part = (sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'], self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)**2  

                    daily_mag_x_part = (sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'],self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)**2 

                    daily_mag_results = math.sqrt(daily_mag_y_part + daily_mag_x_part)

                    if daily_mag_results != None:
                        daily_mag_results = round(daily_mag_results,3)
                    else:
                        pass

                    
                    # compute the mean of the daily observations of degrees-- must be done with RADIANS - not including the missing, questionable, or estimated ones

                    # campbell uses the frickin weighted resultant aaaaahgh

                    theta_u = math.atan2(sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'], self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd, sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'],self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)

                    daily_dir_valid_obs = round(math.degrees(theta_u),3)

                    # roll over the zero
                    if daily_dir_valid_obs < 0.:
                        daily_dir_valid_obs +=360
                    else:
                        pass

                   # print "daily dir valid sin: %s, daily dir valid cos: %s, daily dir valid obs: %s" %(sum(daily_dir_valid_sins), sum(daily_dir_valid_cos), daily_dir_valid_obs)
                   # daily_dir_valid_obs = round(math.degrees(math.atan((float(sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/float(sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])))))),3)


                    # compute the standard deviation of the daily wind directions -- yamartino method:
                    # see this: http://en.wikipedia.org/wiki/Yamartino_method for details

                    daily_epsilon = math.sqrt(1-((sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/num_valid_obs_dir)**2 + (sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/num_valid_obs_dir)**2))

                    daily_sigma_theta = math.degrees(math.asin(daily_epsilon)*(1+(2./math.sqrt(3))-1)*daily_epsilon)

                    # if it gives you back a less than 0 value due to the conversion, abs it. 
                    if daily_sigma_theta < 0.:
                        daily_sigma_theta = round(abs(daily_sigma_theta),3)
                    else:
                        daily_sigma_theta = round(daily_sigma_theta,3)

                    # daily_dirstd_valid_obs = round(math.degrees(math.atan((float(sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/float(sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])))))),3)

                    # get the max of those observations (mean speed)
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['spd_val'] if x != 'None']),3)

                elif num_valid_obs_spd == 0:
                    max_valid_obs = None
                    daily_sigma_theta = None
                    daily_dir_valid_obs = None
                    daily_mag_results = None

                else:
                    pass

                if num_valid_obs_spd != 0:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['spd_val']) if j != "None" and round(float(j),3) == max_valid_obs]
                    
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['spd_fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['spd_val']) if j != "None" and round(float(j),3) == max_valid_obs]


                else: 
                    max_valid_time = None
                    max_flag = ["M"]

                # If no flag has been assigned
                if max_flag[0] =="":
                    max_flag = ["A"]
                else:
                    pass


                # throw b or n flag if speed or mag is less than detection limits
                
                if daily_spd_valid_obs < 1.0 and daily_spd_valid_obs > 0.3:
                    daily_flag_spd = "B"
                elif daily_spd_valid_obs <= 0.3:
                    daily_flag_spd = "N"
                else:
                    pass
                
                if daily_mag_results < 1.0 and daily_mag_results > 0.3:
                    daily_flag_mag = "B"
                elif daily_spd_valid_obs <= 0.3:
                    daily_flag_mag = "N"
                else:
                    pass


                if self.server == "STEWARTIA":
                    source = self.server + "_FSDBDATA_MS04314"
                elif self.server == "SHELDON":
                    source = self.server + "_LTERLogger_Pro_MS04314"
                else:
                    pass

                try:
                    newrow = ['MS043',4, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), daily_spd_valid_obs, daily_flag_spd, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), round(daily_mag_results,3) ,daily_flag_mag, round(daily_dir_valid_obs,3), daily_flag_dir, round(daily_sigma_theta,3), daily_flag_dirstd, None,  None, None, None, None, None, None, None, None, None,None, None, None, None, None, None, "NA", source]
                
                except TypeError:
                    newrow = ['MS043', 4, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None,  None,"M", None, "M", None, "M", None,  None, None, None, None, None, None, None, None, None,None, None, None, None, None, None, "NA", source]

                my_new_rows.append(newrow)
        mylog.dump()
        return my_new_rows

class Wind2(object):
    """ Wind 2 should be used on the new prop wind data 
    where we will have a filed for max at five minues"""

    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        # the date range contains the start date and the end date, 
        # and a method for making it into a human readable
        self.daterange = DateRange(startdate,enddate)
        
        # entity is integer 4
        self.entity = 4

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}

        self.od = self.attack_data()

    def querydb(self):
        """ 
        queries the data base, returning a cursor to the requested data
        """

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."

        # max are at the end of query!
        query = "SELECT DATE_TIME, PROBE_CODE, WSPD_PRO_MEAN, WSPD_PRO_MEAN_FLAG, WMAG_PRO_MEAN, WMAG_PRO_MEAN_FLAG, WDIR_PRO_MEAN, WDIR_PRO_MEAN_FLAG, WDIR_PRO_STDDEV, WDIR_PRO_STDDEV_FLAG, WSPD_PRO_MAX, WSPD_PRO_MAX_FLAG from " + dbname + "MS04314 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:
            try:
                this_height = int(row[0])
            except Exception:
                this_height = 1000

            try:
                this_method = str(row[1])
            except Exception:
                this_method = "WND999"
            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = "ANYMET"

        return this_height, this_method, this_sitecode

    def attack_data(self):
        """ gather the daily wind (propellor) data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute ==0:
                dt_old = dt_old -datetime.timedelta(days=1)
            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'spd_val': [str(row[2])], 'spd_fval': [str(row[3])], 'mag_val': [str(row[4])], 'mag_fval': [str(row[5])], 'dir_val': [str(row[6])], 'dir_fval': [str(row[7])], 'dirstd_val': [str(row[8])], 'dirstd_fval': [str(row[9])], 'maxgust_val':[str(row[10])], 'maxgust_fval':[str(row[11])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'spd_val': [str(row[2])], 'spd_fval': [str(row[3])], 'mag_val': [str(row[4])], 'mag_fval': [str(row[5])], 'dir_val': [str(row[6])], 'dir_fval': [str(row[7])], 'dirstd_val': [str(row[8])], 'dirstd_fval': [str(row[9])], 'maxgust_val':[str(row[10])], 'maxgust_fval':[str(row[11])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['spd_val'].append(str(row[2]))
                    od[probe_code][dt]['spd_fval'].append(str(row[3]))
                    od[probe_code][dt]['mag_val'].append(str(row[4]))
                    od[probe_code][dt]['mag_fval'].append(str(row[5]))
                    od[probe_code][dt]['dir_val'].append(str(row[6]))
                    od[probe_code][dt]['dir_fval'].append(str(row[7]))
                    od[probe_code][dt]['dirstd_val'].append(str(row[8]))
                    od[probe_code][dt]['dirstd_fval'].append(str(row[9]))
                    od[probe_code][dt]['maxgust_val'].append(str(row[10]))
                    od[probe_code][dt]['maxgust_fval'].append(str(row[11]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od


    def condense_data(self, *args):
    
        
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above - wind prop data
        """
        mylog = LogIssues('mylog_wind2')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a sheldon cursor
        cursor_sheldon = fc.form_connection("SHELDON")
        
        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():
       
            # get the height, method_code, and sitecode from the height_and_method_getter function  
            # doesn't look like we'll need any exceptions here right now
            height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            # valid_dates are the dates we will iterate over to do the computation of the daily precip
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass

            for each_date in valid_dates:
                # get the number of valid observations
                num_valid_obs_spd = len([x for x in self.od[probe_code][each_date]['spd_val'] if x != 'None'])
                num_valid_obs_mag = len([x for x in self.od[probe_code][each_date]['mag_val'] if x != 'None'])
                num_valid_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])
                num_valid_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_val'] if x != 'None'])
                num_valid_obs_maxgust = len([x for x in self.od[probe_code][each_date]['maxgust_val'] if x != 'None'])
               

                # get the number of obs
                num_total_obs_spd = len(self.od[probe_code][each_date]['spd_val'])
                num_total_obs_mag = len(self.od[probe_code][each_date]['mag_val'])
                num_total_obs_dir = len(self.od[probe_code][each_date]['dir_val'])
                num_total_obs_mag = len(self.od[probe_code][each_date]['dirstd_val'])
                num_total_obs_maxgust = len(self.od[probe_code][each_date]['maxgust_val'])

                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs_spd not in [288, 96, 24] and each_date != self.daterange.dr[0]: 
                    error_string = "the total number of observations on %s is %s on probe %s" %(each_date, num_total_obs_spd, probe_code)
                    mylog.write('incomplete_day', error_string)
                    continue

                
                else:
                    pass

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_spd = len([x for x in self.od[probe_code][each_date]['spd_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_spd = len([x for x in self.od[probe_code][each_date]['spd_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_spd = len([x for x in self.od[probe_code][each_date]['spd_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean mag
                num_missing_obs_mag = len([x for x in self.od[probe_code][each_date]['mag_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_mag = len([x for x in self.od[probe_code][each_date]['mag_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_mag = len([x for x in self.od[probe_code][each_date]['mag_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean dir
                num_missing_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily dirstd
                num_missing_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'E'])

                # daily flag, wind speed-- if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_spd/num_total_obs_spd >= 0.2:
                    daily_flag_spd = 'M'
                elif (num_missing_obs_spd + num_questionable_obs_spd)/num_total_obs_spd > 0.05:
                    daily_flag_spd = 'Q'
                elif (num_estimated_obs_spd)/num_total_obs_spd > 0.05:
                    daily_flag_spd = 'E'
                elif (num_estimated_obs_spd + num_missing_obs_spd + num_questionable_obs_spd)/num_total_obs_spd<= 0.05:
                    daily_flag_spd = 'A'
                else:
                    daily_flag_spd = 'Q'

                # daily flag, wind mag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_mag/num_total_obs_mag >= 0.2:
                    daily_flag_mag = 'M'
                elif (num_missing_obs_mag + num_questionable_obs_mag)/num_total_obs_mag > 0.05:
                    daily_flag_mag = 'Q'
                elif (num_estimated_obs_mag)/num_total_obs_mag > 0.05:
                    daily_flag_mag = 'E'
                elif (num_estimated_obs_mag + num_missing_obs_mag + num_questionable_obs_mag)/num_total_obs_mag <= 0.05:
                    daily_flag_mag = 'A'
                else:
                    daily_flag_mag = 'Q'


                # daily flag, wind dir: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_dir/num_total_obs_dir >= 0.2:
                    daily_flag_dir = 'M'
                elif (num_missing_obs_dir + num_questionable_obs_dir)/num_total_obs_dir > 0.05:
                    daily_flag_dir = 'Q'
                elif (num_estimated_obs_dir)/num_total_obs_dir > 0.05:
                    daily_flag_dir = 'E'
                elif (num_estimated_obs_dir + num_missing_obs_dir + num_questionable_obs_dir)/num_total_obs_dir <= 0.05:
                    daily_flag_dir = 'A'
                else:
                    daily_flag_dir = 'Q'

                # daily flag, wind dir std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_dirstd/num_total_obs_dir >= 0.2:
                    daily_flag_dirstd = 'M'
                elif (num_missing_obs_dirstd + num_questionable_obs_dirstd)/num_total_obs_dir > 0.05:
                    daily_flag_dirstd = 'Q'
                elif (num_estimated_obs_dirstd)/num_total_obs_dir > 0.05:
                    daily_flag_dirstd = 'E'
                elif (num_estimated_obs_dirstd + num_missing_obs_dirstd + num_questionable_obs_dirstd)/num_total_obs_dir <= 0.05:
                    daily_flag_dirstd = 'A'
                else:
                    daily_flag_dirstd = 'Q'

                try: 
                    # compute the mean daily wind speed -- just the mean of what is given...
                    daily_spd_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['spd_val'] if x != 'None'])/num_valid_obs_spd),3)

                except ZeroDivisionError:
                    # if the case is that there are no valid observations
                    daily_spd_valid_obs = None
                    
                    # magnitude and speed are therefore also M
                    daily_flag_spd = "M"
                    

                if num_valid_obs_spd != 0:
                # compute the daily resultant--- this one is a true joy.
                
                ## This is a working example using [2, 2, 2, 4, 4] for speed and [10, 10, 10, 350, 10] for direction:
                # daily_mag_x_part = (sum([speed * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(speedlist,dirlist) if speed != 'None' and x != 'None'])/5.)**2
                # daily_mag_y_part = (sum([speed * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(speedlist,dirlist) if speed != 'None' and x != 'None'])/5.)**2
                # math.sqrt(daily_mag_y_part + daily_mag_x_part)
                # >> Returns: 2.7653239207215683

                    daily_mag_y_part = (sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'], self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)**2  

                    daily_mag_x_part = (sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'],self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)**2 

                    daily_mag_results = math.sqrt(daily_mag_y_part + daily_mag_x_part)

                    if daily_mag_results != None:
                        daily_mag_results = round(daily_mag_results,3)
                    else:
                        pass

                    
                    # compute the mean of the daily observations of degrees-- must be done with RADIANS - not including the missing, questionable, or estimated ones

                    # campbell uses the frickin weighted resultant aaaaahgh

                    theta_u = math.atan2(sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'], self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd, sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'],self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)

                    daily_dir_valid_obs = round(math.degrees(theta_u),3)

                    # roll over the zero
                    if daily_dir_valid_obs < 0.:
                        daily_dir_valid_obs +=360
                    else:
                        pass

                    # compute the standard deviation of the daily wind directions -- yamartino method:
                    # see this: http://en.wikipedia.org/wiki/Yamartino_method for details

                    daily_epsilon = math.sqrt(1-((sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/num_valid_obs_dir)**2 + (sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/num_valid_obs_dir)**2))

                    daily_sigma_theta = math.degrees(math.asin(daily_epsilon)*(1+(2./math.sqrt(3))-1)*daily_epsilon)

                    # if it gives you back a less than 0 value due to the conversion, abs it. 
                    if daily_sigma_theta < 0.:
                        daily_sigma_theta = round(abs(daily_sigma_theta),3)
                    else:
                        daily_sigma_theta = round(daily_sigma_theta,3)

                    # daily_dirstd_valid_obs = round(math.degrees(math.atan((float(sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/float(sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])))))),3)

                    # get the max of those observations (mean speed)
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['maxgust_val'] if x != 'None']),3)

                elif num_valid_obs_spd == 0:
                    max_valid_obs = None
                    daily_sigma_theta = None
                    daily_dir_valid_obs = None
                    daily_mag_results = None

                else:
                    pass

                if num_valid_obs_spd != 0:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['maxgust_val']) if j != "None" and round(float(j),3) == max_valid_obs]
                    
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['spd_fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['maxgust_val']) if j != "None" and round(float(j),3) == max_valid_obs]


                else: 
                    max_valid_time = None
                    max_flag = ["M"]

                # If no flag has been assigned
                if max_flag[0] =="":
                    max_flag = ["A"]
                else:
                    pass

                # throw b or n flag if speed or mag is less than detection limits
                
                if daily_spd_valid_obs < 1.0 and daily_spd_valid_obs > 0.3:
                    daily_flag_spd = "B"
                elif daily_spd_valid_obs <= 0.3:
                    daily_flag_spd = "N"
                else:
                    pass
                
                if daily_mag_results < 1.0 and daily_mag_results > 0.3:
                    daily_flag_mag = "B"
                elif daily_spd_valid_obs <= 0.3:
                    daily_flag_mag = "N"
                else:
                    pass


                if self.server == "STEWARTIA":
                    source = self.server + "_FSDBDATA_MS04314"
                elif self.server == "SHELDON":
                    source = self.server + "_LTERLogger_Pro_MS04314"
                else:
                    pass

                try:
                    newrow = ['MS043',4, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), daily_spd_valid_obs, daily_flag_spd, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), round(daily_mag_results,3) ,daily_flag_mag, round(daily_dir_valid_obs,3), daily_flag_dir, round(daily_sigma_theta,3), daily_flag_dirstd, None,  None, None, None, None, None, None, None, None, None,None, None, None, None, None, None, "NA", source]
                
                except TypeError:
                    newrow = ['MS043', 4, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None,  None,"M", None, "M", None, "M", None,  None, None, None, None, None, None, None, None, None,None, None, None, None, None, None, "NA", source]

                my_new_rows.append(newrow)
        mylog.dump()
        return my_new_rows


class Sonic(object):

    """ Sonic anemometer daily aggregation. Has Wux, Wuy, Temp Sensor, etc.  """
   
    def __init__(self, startdate, enddate, server):

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        # the date range contains the start date and the end date, 
        # and a method for making it into a human readable
        self.daterange = DateRange(startdate,enddate)
        self.entity = 24

        # server is STEWARTIA OR SHELDON
        self.server = server

        # query the database - use method provided
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. self.od gets condensed into new rows for writing table or csv
        self.od = {}

        self.od = self.attack_data()

    def querydb(self):
        """ 
        queries the data base, returning a cursor to the requested data
        """

        # human-readable date range for the database
        # dr = self.human_readable()
        humanrange = self.daterange.human_readable()

        if self.server == "SHELDON":
            dbname = "LTERLogger_pro.dbo."
        
        elif self.server == "STEWARTIA":
            dbname = "FSDBDATA.dbo."

        query = "SELECT DATE_TIME, PROBE_CODE, WSPD_SNC_MEAN, WSPD_SNC_MEAN_FLAG, WSPD_SNC_MAX, WSPD_SNC_MAX_FLAG, WDIR_SNC_MEAN, WDIR_SNC_MEAN_FLAG, WDIR_SNC_STDDEV, WDIR_SNC_STDDEV_FLAG,  WUX_SNC_MEAN,  WUX_SNC_MEAN_FLAG,  WUX_SNC_STDDEV,  WUX_SNC_STDDEV_FLAG, WUY_SNC_MEAN,  WUY_SNC_MEAN_FLAG,  WUY_SNC_STDDEV,  WUY_SNC_STDDEV_FLAG, WAIR_SNC_MEAN,  WAIR_SNC_MEAN_FLAG,  WAIR_SNC_STDDEV,  WAIR_SNC_STDDEV_FLAG from " + dbname + "MS04334 WHERE DATE_TIME >= \'" + humanrange[0] + "\' AND DATE_TIME < \'" + humanrange[1] + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    
    def height_and_method_getter(self, probe_code, cursor_sheldon):
        """ determines the height and method based on the method_history_daily table in LTERLogger_new. If a method is not found, we'll need to pass over it. sheldon cursor is passed in"""
        
        # use the human readable date
        humanrange = self.daterange.human_readable()

        # query the DB for the right height and method - this query doesn't work over long data series because a series may cross the methods boundary
        #query = "SELECT height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] + "\' and date_end > \'" + humanrange[1] + "\' and probe_code like \'" + probe_code + "\'"
        
        # this query is better over long series. it's slower but it won't break.
        query = "SELECT top 1 height, method_code, sitecode FROM LTERLogger_new.dbo.method_history_daily where date_bgn <= \'" + humanrange[0] +"\' and probe_code like \'" + probe_code + "\' order by date_bgn desc "
        
        cursor_sheldon.execute(query)
            
        for row in cursor_sheldon:

            try:
                this_height = int(row[0])
            except Exception:
                this_height = 1000
            try:
                this_method = str(row[1])
            except Exception:
                this_method = "WND999"

            try:
                this_sitecode = str(row[2])
            except Exception:
                this_sitecode = 'ANYMET'
        
        return this_height, this_method, this_sitecode

    def attack_data(self):
        """ gather the daily wind (sonic) data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

            if dt_old.hour == 0 and dt_old.minute == 0:
                dt_old = dt_old - datetime.timedelta(days=1)
            
            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'snc_mean_val': [str(row[2])], 'snc_mean_fval': [str(row[3])], 'snc_max_val': [str(row[4])], 'snc_max_fval': [str(row[5])], 'dir_val': [str(row[6])], 'dir_fval': [str(row[7])], 'dirstd_val': [str(row[8])], 'dirstd_fval': [str(row[9])], 'wux_val': [str(row[10])], 'wux_fval': [str(row[11])], 'wux_std_val': [str(row[12])], 'wux_std_fval': [str(row[13])], 'wuy_val': [str(row[14])], 'wuy_fval': [str(row[15])], 'wuy_std_val': [str(row[16])], 'wuy_std_fval': [str(row[17])], 'wair_val': [str(row[18])], 'wair_fval': [str(row[19])], 'wair_std_val': [str(row[20])], 'wair_std_fval': [str(row[21])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'snc_mean_val': [str(row[2])], 'snc_mean_fval': [str(row[3])], 'snc_max_val': [str(row[4])], 'snc_max_fval': [str(row[5])], 'dir_val': [str(row[6])], 'dir_fval': [str(row[7])], 'dirstd_val': [str(row[8])], 'dirstd_fval': [str(row[9])], 'wux_val': [str(row[10])], 'wux_fval': [str(row[11])], 'wux_std_val': [str(row[12])], 'wux_std_fval': [str(row[13])], 'wuy_val': [str(row[14])], 'wuy_fval': [str(row[15])], 'wuy_std_val': [str(row[16])], 'wuy_std_fval': [str(row[17])], 'wair_val': [str(row[18])], 'wair_fval': [str(row[19])], 'wair_std_val': [str(row[20])], 'wair_std_fval': [str(row[21])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['snc_mean_val'].append(str(row[2]))
                    od[probe_code][dt]['snc_mean_fval'].append(str(row[3]))
                    od[probe_code][dt]['snc_max_val'].append(str(row[4]))
                    od[probe_code][dt]['snc_max_fval'].append(str(row[5]))
                    od[probe_code][dt]['dir_val'].append(str(row[6]))
                    od[probe_code][dt]['dir_fval'].append(str(row[7]))
                    od[probe_code][dt]['dirstd_val'].append(str(row[8]))
                    od[probe_code][dt]['dirstd_fval'].append(str(row[9]))
                    od[probe_code][dt]['wux_val'].append(str(row[10]))
                    od[probe_code][dt]['wux_fval'].append(str(row[11]))
                    od[probe_code][dt]['wux_std_val'].append(str(row[12]))
                    od[probe_code][dt]['wux_std_fval'].append(str(row[13]))
                    od[probe_code][dt]['wuy_val'].append(str(row[14]))
                    od[probe_code][dt]['wuy_fval'].append(str(row[15]))
                    od[probe_code][dt]['wuy_std_val'].append(str(row[16]))
                    od[probe_code][dt]['wuy_std_fval'].append(str(row[17]))
                    od[probe_code][dt]['wair_val'].append(str(row[18]))
                    od[probe_code][dt]['wair_fval'].append(str(row[19]))
                    od[probe_code][dt]['wair_std_val'].append(str(row[20]))
                    od[probe_code][dt]['wair_std_fval'].append(str(row[21]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self):
        """ 
        Computes the daily aggregates, assigns the flags and methods selected above
        """
        mylog = LogIssues('mylog_sonic')

        # my new rows is the output rows that can be read as csv or into the database
        my_new_rows = []

        # make a SHELDON cursor if you do not have one to get the LTERLogger_new.dbo.method_history_daily table.
        cursor_sheldon = fc.form_connection("SHELDON")
            
        # iterate over each probe-code that was collected
        for probe_code in self.od.keys():
            
            if probe_code != "WNDCEN02":
                # get height, method, and site from table  
                height, method_code, site_code = self.height_and_method_getter(probe_code, cursor_sheldon)

            elif probe_code == "WNDCEN02":
                # missing from db at the moment
                height, method_code, site_code = 1000, "WND011","CENMET"
           
            # valid_dates for sonic wind -- dates from the desired range we can get data from 
            valid_dates = sorted(self.od[probe_code].keys())

            ## THIS CODE WAS ADDED ON 08/26/2015 -- it appears we could end up over writing one value each time we run this if we don't skip it due to dealing with the 2400 convention!
            if valid_dates[0] == self.daterange.dr[0] - datetime.timedelta(days=1):
                valid_dates = sorted(self.od[probe_code].keys())[1:]
            else:
                pass
            
            for each_date in valid_dates:

                # NUMBER OF VALID (not null) OBSERVATIONS FOR EACH DAY
                # mean spd
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['snc_mean_val'] if x != 'None'])
                # max speed
                num_valid_obs_max = len([x for x in self.od[probe_code][each_date]['snc_max_val'] if x != 'None'])
                # dir
                num_valid_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])
                # dirstd
                num_valid_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_val'] if x != 'None'])
                # wux
                num_valid_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_val'] if x != 'None'])
                # wux std
                num_valid_obs_wuxstd = len([x for x in self.od[probe_code][each_date]['wux_std_val'] if x != 'None'])
                # wuy
                num_valid_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_val'] if x != 'None'])
                # wuy std
                num_valid_obs_wuystd = len([x for x in self.od[probe_code][each_date]['wuy_std_val'] if x != 'None'])
                # wair
                num_valid_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_val'] if x != 'None'])
                # wairstd
                num_valid_obs_wairstd = len([x for x in self.od[probe_code][each_date]['wair_std_val'] if x != 'None'])

                # TOTAL NUMBER OF OBSERVATIONS (includes null)
                num_total_obs = len(self.od[probe_code][each_date]['snc_mean_val'])
                
                # if it's not a total of observations on that day that we would expect, then log error, continue to a day with the right number
                if num_total_obs not in [288, 96, 24, 1] and each_date != self.daterange.dr[0]: 
                    error_string = "the total number of observations on %s is %s and probe %s" %(each_date, num_total_obs, probe_code)
                    mylog.write('nullday',error_string)
                    continue

                else:
                    pass

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_snc_mean = len([x for x in self.od[probe_code][each_date]['snc_mean_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_snc_mean = len([x for x in self.od[probe_code][each_date]['snc_mean_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_snc_mean = len([x for x in self.od[probe_code][each_date]['snc_mean_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily max speed
                num_missing_obs_snc_max = len([x for x in self.od[probe_code][each_date]['snc_max_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_snc_max = len([x for x in self.od[probe_code][each_date]['snc_max_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_snc_max = len([x for x in self.od[probe_code][each_date]['snc_max_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily direction
                num_missing_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily std on direction
                num_missing_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily x vector
                num_missing_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily y vector
                num_missing_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily air temperature
                num_missing_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'E'])


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily x vector std
                num_missing_obs_wuxstd = len([x for x in self.od[probe_code][each_date]['wux_std_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wuxstd = len([x for x in self.od[probe_code][each_date]['wux_std_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wuxstd = len([x for x in self.od[probe_code][each_date]['wux_std_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily y vector std
                num_missing_obs_wuystd = len([x for x in self.od[probe_code][each_date]['wuy_std_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wuystd = len([x for x in self.od[probe_code][each_date]['wuy_std_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wuystd = len([x for x in self.od[probe_code][each_date]['wuy_std_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily airtemp std
                num_missing_obs_wairstd = len([x for x in self.od[probe_code][each_date]['wair_std_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wairstd = len([x for x in self.od[probe_code][each_date]['wair_std_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wairstd = len([x for x in self.od[probe_code][each_date]['wair_std_fval'] if x == 'E'])

                # daily flag, sonic speed-- if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_snc_mean/num_total_obs >= 0.2:
                    daily_flag_snc_mean = 'M'
                elif (num_missing_obs_snc_mean + num_questionable_obs_snc_mean)/num_total_obs > 0.05:
                    daily_flag_snc_mean = 'Q'
                elif (num_estimated_obs_snc_mean)/num_total_obs > 0.05:
                    daily_flag_snc_mean = 'E'
                elif (num_estimated_obs_snc_mean + num_missing_obs_snc_mean + num_questionable_obs_snc_mean)/num_total_obs<= 0.05:
                    daily_flag_snc_mean = 'A'
                else:
                    daily_flag_snc_mean = 'Q'

                # daily flag, sonic max: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_snc_max/num_total_obs >= 0.2:
                    daily_flag_snc_max = 'M'
                elif (num_missing_obs_snc_max + num_questionable_obs_snc_max)/num_total_obs > 0.05:
                    daily_flag_snc_max = 'Q'
                elif (num_estimated_obs_snc_max)/num_total_obs > 0.05:
                    daily_flag_snc_max = 'E'
                elif (num_estimated_obs_snc_max + num_missing_obs_snc_max + num_questionable_obs_snc_max)/num_total_obs <= 0.05:
                    daily_flag_snc_max = 'A'
                else:
                    daily_flag_snc_max = 'Q'

                # daily flag, wind dir: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_dir/num_total_obs >= 0.2:
                    daily_flag_dir = 'M'
                elif (num_missing_obs_dir + num_questionable_obs_dir)/num_total_obs > 0.05:
                    daily_flag_dir = 'Q'
                elif (num_estimated_obs_dir)/num_total_obs > 0.05:
                    daily_flag_dir = 'E'
                elif (num_estimated_obs_dir + num_missing_obs_dir + num_questionable_obs_dir)/num_total_obs <= 0.05:
                    daily_flag_dir = 'A'
                else:
                    daily_flag_dir = 'Q'

                # daily flag, wind dir std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_dirstd/num_total_obs >= 0.2:
                    daily_flag_dirstd = 'M'
                elif (num_missing_obs_dirstd + num_questionable_obs_dirstd)/num_total_obs > 0.05:
                    daily_flag_dirstd = 'Q'
                elif (num_estimated_obs_dirstd)/num_total_obs > 0.05:
                    daily_flag_dirstd = 'E'
                elif (num_estimated_obs_dirstd + num_missing_obs_dirstd + num_questionable_obs_dirstd)/num_total_obs <= 0.05:
                    daily_flag_dirstd = 'A'
                else:
                    daily_flag_dirstd = 'Q'

                # daily flag, x vector: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_wux/num_total_obs >= 0.2:
                    daily_flag_wux = 'M'
                elif (num_missing_obs_wux + num_questionable_obs_wux)/num_total_obs > 0.05:
                    daily_flag_wux = 'Q'
                elif (num_estimated_obs_wux)/num_total_obs > 0.05:
                    daily_flag_wux = 'E'
                elif (num_estimated_obs_wux + num_missing_obs_wux + num_questionable_obs_wux)/num_total_obs <= 0.05:
                    daily_flag_wux = 'A'
                else:
                    daily_flag_wux = 'Q'

                # daily flag, x vector std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_wuxstd/num_total_obs >= 0.2:
                    daily_flag_wuxstd = 'M'
                elif (num_missing_obs_wuxstd + num_questionable_obs_wuxstd)/num_total_obs > 0.05:
                    daily_flag_wuxstd = 'Q'
                elif (num_estimated_obs_wuxstd)/num_total_obs > 0.05:
                    daily_flag_wuxstd = 'E'
                elif (num_estimated_obs_wuxstd + num_missing_obs_wuxstd + num_questionable_obs_wuxstd)/num_total_obs <= 0.05:
                    daily_flag_wuxstd = 'A'
                else:
                    daily_flag_wuxstd = 'Q'

                # daily flag, y vector: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_wuy/num_total_obs >= 0.2:
                    daily_flag_wuy = 'M'
                elif (num_missing_obs_wuy + num_questionable_obs_wuy)/num_total_obs > 0.05:
                    daily_flag_wuy = 'Q'
                elif (num_estimated_obs_wuy)/num_total_obs > 0.05:
                    daily_flag_wuy = 'E'
                elif (num_estimated_obs_wuy + num_missing_obs_wuy + num_questionable_obs_wuy)/num_total_obs <= 0.05:
                    daily_flag_wuy = 'A'
                else:
                    daily_flag_wuy = 'Q'

                # daily flag, y vector std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_wuystd/num_total_obs >= 0.2:
                    daily_flag_wuystd = 'M'
                elif (num_missing_obs_wuystd + num_questionable_obs_wuystd)/num_total_obs > 0.05:
                    daily_flag_wuystd = 'Q'
                elif (num_estimated_obs_wuystd)/num_total_obs > 0.05:
                    daily_flag_wuystd = 'E'
                elif (num_estimated_obs_wuystd + num_missing_obs_wuystd + num_questionable_obs_wuystd)/num_total_obs <= 0.05:
                    daily_flag_wuystd = 'A'
                else:
                    daily_flag_wuystd = 'Q'

                # daily flag, air temp: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_wair/num_total_obs >= 0.2:
                    daily_flag_wair = 'M'
                elif (num_missing_obs_wair + num_questionable_obs_wair)/num_total_obs > 0.05:
                    daily_flag_wair = 'Q'
                elif (num_estimated_obs_wair)/num_total_obs > 0.05:
                    daily_flag_wair = 'E'
                elif (num_estimated_obs_wair + num_missing_obs_wair + num_questionable_obs_wair)/num_total_obs <= 0.05:
                    daily_flag_wair = 'A'
                else:
                    daily_flag_wair = 'Q'

                # daily flag, airtemp std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_wairstd/num_total_obs >= 0.2:
                    daily_flag_wairstd = 'M'
                elif (num_missing_obs_wairstd + num_questionable_obs_wairstd)/num_total_obs > 0.05:
                    daily_flag_wairstd = 'Q'
                elif (num_estimated_obs_wairstd)/num_total_obs > 0.05:
                    daily_flag_wairstd = 'E'
                elif (num_estimated_obs_wairstd + num_missing_obs_wairstd + num_questionable_obs_wairstd)/num_total_obs <= 0.05:
                    daily_flag_wairstd = 'A'
                else:
                    daily_flag_wairstd = 'Q'

                try: 
                    # compute the mean daily wind speed -- just the mean of what is given...
                    daily_snc_mean = round(float(sum([float(x) for x in self.od[probe_code][each_date]['snc_mean_val'] if x != 'None'])/num_valid_obs),3)

                # if all the day is missing there are 0 valid obs, and we get a 0 div error
                except ZeroDivisionError:
                    daily_snc_mean = None
                    daily_flag_snc_mean = "M"

                try: 
                    # compute the  daily max sonic speed? -- max of the max!...
                    daily_snc_max = round(max([float(x) for x in self.od[probe_code][each_date]['snc_max_val'] if x != 'None']),3)

                except ValueError:
                    daily_snc_max = None
                    daily_flag_snc_max = "M"

                try: 
                    # compute the mean wux
                    daily_wux = round(float(sum([float(x) for x in self.od[probe_code][each_date]['wux_val'] if x != 'None'])/num_valid_obs),3)

                except ZeroDivisionError:
                    daily_wux = None
                    daily_flag_wux = "M"

                try: 
                    # compute the daily wuy
                    daily_wuy = round(float(sum([float(x) for x in self.od[probe_code][each_date]['wuy_val'] if x != 'None'])/num_valid_obs),3)

                except ZeroDivisionError:
                    daily_wuy = None
                    daily_flag_wuy = "M"

                try: 
                    # compute daily air temp
                    daily_wair = round(float(sum([float(x) for x in self.od[probe_code][each_date]['wair_val'] if x != 'None'])/num_valid_obs),3)

                except ZeroDivisionError:
                    daily_wair = None
                    daily_flag_wair = "M"
                    

                if num_valid_obs != 0:
                # compute the daily resultant magnitude--- this one is a true joy.

                    daily_mag_from_wux = round(float(sum([float(x) for x in self.od[probe_code][each_date]['wux_val'] if x != 'None']))**2,3)
                    daily_mag_from_wuy = round(float(sum([float(x) for x in self.od[probe_code][each_date]['wuy_val'] if x != 'None']))**2,3)
                    daily_snc_mag = round(math.sqrt(daily_mag_from_wux + daily_mag_from_wuy),3)
                    
                    # flag the mag's... if it's from 2 a's, it's A. If a Q is part or an E is part, it's Q. If an "M" is part, it's M, otherwise it's A.
                    if daily_flag_wux == "A" and daily_flag_wuy == "A":
                        daily_flag_snc_mag = "A"
                    
                    elif daily_flag_wux == "Q" or daily_flag_wuy == "Q":
                        daily_flag_snc_mag = "Q"
                    
                    elif daily_flag_wux == "E" or daily_flag_wuy == "E":
                        daily_flag_snc_mag = "Q"
                    
                    elif daily_flag_wux == "M" or daily_flag_wuy == "M":
                        daily_flag_snc_mag = "M"
                    
                    else:
                        daily_flag_snc_mag = "A"

                    # compute the direction for the day as based on the individual x and y vectors
                    theta_u = math.atan2(sum([float(x) for x in self.od[probe_code][each_date]['wuy_val'] if x != 'None'])/num_valid_obs, sum([float(x) for x in self.od[probe_code][each_date]['wux_val'] if x != 'None'])/num_valid_obs)

                    daily_dir_valid_obs = round(math.degrees(theta_u),3)

                    if daily_dir_valid_obs < 0.:
                        daily_dir_valid_obs +=360.
                    elif daily_dir_valid_obs > 360.:
                        daily_dir_valid_obs -=360.
                    else:
                        pass


                    # compute the mean of the daily observations of degrees-- must be done with RADIANS - not including the missing, questionable, or estimated ones
                    # daily_dir_valid_obs = round(math.degrees(math.atan((float(sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/float(sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])))))),3)

                    # compute the standard deviation of the daily wind directions -- yamartino method:
                    # see this: http://en.wikipedia.org/wiki/Yamartino_method for details

                    daily_epsilon = round(math.sqrt(1-((sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/num_valid_obs_dir)**2 + (sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/num_valid_obs_dir)**2)),3)

                    daily_sigma_theta = round(math.degrees(math.asin(daily_epsilon)*(1+(2./math.sqrt(3))-1)*daily_epsilon),3)

                    # daily_dirstd_valid_obs = round(math.degrees(math.atan((float(sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/float(sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])))))),3)

                    # the daily standard deviation is the standard deviation of the day's values by component
                    if daily_wux != None:
                        # compute the std of the day
                        daily_wux_std = round(np.std([float(x) for x in self.od[probe_code][each_date]['wux_val'] if x != "None"]),3)
                        
                        # added a method to check if it's a nan because all values are missing
                        if np.isnan(daily_wux_std):
                            daily_wux_std = None
                        else:
                            pass

                    else: 
                        daily_wux_std = None

                    if daily_wuy != None:
                        # compute the std of the day
                        daily_wuy_std = round(np.std([float(x) for x in self.od[probe_code][each_date]['wuy_val'] if x != "None"]),3)

                        # added a method to check if it's a nan because all values are missing
                        if np.isnan(daily_wuy_std):
                            daily_wuy_std = None
                        else:
                            pass

                    else: 
                        daily_wuy_std = None

                    if daily_wair != None:
                        # compute the std of the day
                        daily_wair_std = round(np.std([float(x) for x in self.od[probe_code][each_date]['wair_val'] if x != "None"]),3)
                    
                        # added a method to check if it's a nan because all values are missing
                        if np.isnan(daily_wair_std):
                            daily_wair_std = None
                        else:
                            pass

                    else: 
                        daily_wair_std = None


                elif num_valid_obs == 0:
                    daily_snc_mag = None
                    daily_sigma_theta = None
                    daily_dir_valid_obs = None
                    daily_wair_std = None
                    daily_wuy_std = None
                    daily_wux_std = None

                else:
                    pass

                if self.server == "STEWARTIA":
                    source = self.server + "_FSDBDATA_MS04334"
                elif self.server == "SHELDON":
                    source = self.server + "_LTERLogger_Pro_MS04334"
                else:
                    pass

                newrow = ['MS043',24, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), daily_snc_mean, daily_flag_snc_mean, daily_snc_max, daily_flag_snc_max, daily_snc_mag, daily_flag_snc_mag, daily_dir_valid_obs, daily_flag_dir, daily_sigma_theta, daily_flag_dirstd, daily_wux, daily_flag_wux, daily_wux_std, daily_flag_wuxstd, daily_wuy_std, daily_flag_wuystd, daily_wair_std, daily_flag_wairstd, "NA", source]

                my_new_rows.append(newrow)
        mylog.dump()
        return my_new_rows