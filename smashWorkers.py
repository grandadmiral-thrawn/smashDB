#import smashControls
import pymssql
import math
import datetime
import csv
import itertools
import numpy as np
import yaml

class HeaderWriter(object):
    """ Writes a header given a certain attribute"""


    def __init__(self, attribute):
    
        daily_attr = {'LYS': '09',
                'NR': '25',
                'WSPD_SNC': '24',
                'SOILWC':'23',
                'PAR':'22',
                'SOILTEMP':'21',
                'VPD':'08',
                'DEWPT':'07',
                'SOLAR': '05',
                'WSPD_PRO': '04',
                'PRECIP': '03',
                'RELHUM':'02',
                'AIRTEMP':'01',
                'WSPD_SNC': '24',}

        dbcode = 'MS043'
        
        # The following properties create the daily components needed in any headers
        self.attribute = attribute
        self.dbcode = dbcode
        self.entity = daily_attr[attribute]
        self.method = attribute + '_METHOD'
        self.height = self.isdirty()
        self.mean_method = attribute + "_MEAN_DAY"
        self.mean_flag_method = attribute + "_MEAN_FLAG"
        self.max_method = attribute + "_MAX_DAY"
        self.max_flag_method = attribute + "_MAX_FLAG"
        self.maxtime_method = attribute + "_MAXTIME"
        self.min_method = attribute + "_MIN_DAY"
        self.mintime_method = attribute + "_MINTIME"
        self.min_flag_method = attribute + "_MIN_FLAG"
    
        # writes a header line for a csv
        self.header = self.write_header_template()

        # name of the csvfile
        self.filename = dbcode + daily_attr[attribute] + '_test.csv'

        # lists the name of codes you can use
        self.help = """The following codes can be used for attribute access:
        'ATM'' for atmospheric pressure, 'NR' for net radiation, 'PAR' for photosynthetic
        active radiation, 'SOILTEMP' for soil temperature, 'LYS' for snowmelt, 
        'VPD' for vapor pressure deficit, 'DEWPT' for dewpoint, 'SOLAR' for pyranometer,
        'WSPD_PRO' for propellor anemometer, 'WSPD_SNC' for sonic anemometer,
        'PRECIP' for precipitation (as rain), 'RELHUM' for relative
        humidity, and 'AIRTEMP' for airtemp"""

    def isdirty(self):
        """ If an attribute has to do with the soil, it will take depth rather than height in the header"""

        if self.attribute == "SOILWC" or self.attribute == "SOILTEMP":
            height_word = "DEPTH"
        else:
            height_word = "HEIGHT"

        return height_word

    def write_header_template(self):
        """ The following headers are generated based on given attributes"""

        # if the attribute is given in lowercase, make it into an upper case.
        if self.attribute in ["airtemp", "relhum", "dewpt", "soilwc", "vpd", "soiltemp", "par", "nr", "precip", "lys", "wspd_snc", "wspd_pro"]:

            self.attribute = self.attribute.upper()
        else:
            pass

        # the "big six" simplest case
        if self.attribute == "AIRTEMP" or self.attribute == "RELHUM" or self.attribute == "SOILWC" or self.attribute == "DEWPT" or self.attribute == "SOILTEMP":

            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, self.min_method, self.min_flag_method, self.mintime_method, "EVENT_CODE", "SOURCE"]

        elif self.attribute == "VPD":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, self.min_method, self.mintime_method, self.min_flag_method, "VAP_MEAN_DAY", "VAP_MEAN_FLAG", "VAP_MAX_DAY", "VAP_MAX_FLAG", "VAP_MIN_DAY", "VAP_MIN_FLAG", "SATVP_MEAN_DAY", "SATVP_MEAN_FLAG", "SATVP_MAX_DAY", "SATVP_MAX_FLAG", "SATVP_MIN_DAY",   "SATVP_MIN_FLAG", "EVENT_CODE", "SOURCE"]

        # attributes which need a "total"
        elif self.attribute == "PRECIP":

            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "PRECIP_TOT_DAY", "PRECIP_TOT_FLAG", "EVENT_CODE", "SOURCE"]

        # propellor anemometer
        elif self.attribute == "WSPD_PRO":

            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "WMAG_PRO_MEAN_DAY", "WMAG_PRO_MEAN_FLAG", "WDIR_PRO_MEAN_DAY", "WDIR_PRO_MEAN_FLAG", "WDIR_PRO_STDDEV_DAY", "WDIR_PRO_STDDEV_FLAG", "EVENT_CODE", "SOURCE"]

        # the sonic anemometer
        elif self.attribute == "WSPD_SNC":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "WMAG_SNC_MEAN_DAY", "WMAG_SNC_MEAN_FLAG", "WDIR_SNC_MEAN_DAY", "WDIR_SNC_MEAN_FLAG", "WDIR_SNC_STDDEV_DAY", "WDIR_SNC_STDDEV_FLAG", "WUX_SNC_MEAN_DAY", "WUX_SNC_MEAN_FLAG", "WUX_SNC_STDDEV_DAY", "WUX_SNC_STDDEV_DAY_FLAG","WUY_SNC_MEAN_DAY", "WUY_SNC_MEAN_FLAG", "WUY_SNC_STDDEV_DAY", "WUY_SNC_STDDEV_DAY_FLAG", "WAIR_SNC_MEAN_DAY", "WAIR_SNC_MEAN_FLAG", "WAIR_SNC_STDDEV_DAY", "WAIR_SNC_STDDEV_FLAG",  "EVENT_CODE", "SOURCE"]

        # net radiometer
        elif self.attribute == "NR":

            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "SW_IN_MEAN_DAY", "SW_IN_MEAN_FLAG", "SW_OUT_MEAN_DAY", "SW_OUT_MEAN_FLAG", "LW_IN_MEAN_DAY", "LW_IN_MEAN_FLAG", "LW_OUT_MEAN_DAY", "LW_OUT_MEAN_FLAG", "NR_TOT_MEAN_DAY", "NR_TOT_MEAN_FLAG", "SENSOR_TEMP_DAY", "SENSOR_TEMP_FLAG", "EVENT_CODE"]

        # pyranometer (similar method to precip but takes a max and min time as well)
        elif self.attribute == "SOLAR":
            
            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "SOLAR_TOT_DAY", "SOLAR_TOT_FLAG", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "EVENT_CODE", "SOURCE"]

        # very simple, like the "big six", but no minimums
        elif self.attribute == "PAR":
            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "EVENT_CODE", "SOURCE"]

        # still not sure if this is right
        elif self.attribute == "LYS":

            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "SNOWMELT_TOT_DAY", "SNOWMELT_TOT_FLAG", "EVENT_CODE", "SOURCE"]

        return header


class AirTemperature(object):
    """ For generating MS04301 from LTERLoggers_new or from STEWARTIA
    Takes start date and end date as date strings, server is either SHELDON or STEWARTIA
    If a final argument is passed it is a probe-code which can be used to limit how many data
    are run through the tool. 
    No matter what inputs are given, call condense_data on the results to generate row-by-row output. If you pass condense_data an argument of a yaml file, it will use that yaml file to generate the method/probe mapping, otherwise, it will default to the mapping that I give it.
    To have certain probes on certain dates, use the ProbeBoss to call a LIMITED.yaml file which will map each probe to a special start and end date, and generate a header independently.
    """


    def __init__(self, startdate, enddate, server, *limited):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '01'
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day. Can take time as either a python date object or as a date string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, AIRTEMP_MEAN, AIRTEMP_MEAN_FLAG from LTERLogger_pro.dbo.MS04311 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, AIRTEMP_MEAN, AIRTEMP_MEAN_FLAG from FSDBDATA.dbo.MS04311 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, AIRTEMP_MEAN, AIRTEMP_MEAN_FLAG from LTERLogger_pro.dbo.MS04311 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, AIRTEMP_MEAN, AIRTEMP_MEAN_FLAG from FSDBDATA.dbo.MS04311 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)


    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for all that is not H15202 or CS201"""

        if probe_code == "AIRCS202":
            height = "150"
            return height

        elif probe_code == "AIRH1502":
            height = "150"
            return height

        elif probe_code == "AIRVAR10":
            height = "450"
            return height
        
        else:

            value = probe_code[-1:]
            stat = probe_code[3:6]

            # if the site isn't high 15 or cs2met, then the height value is 5-number concatenated with the string 50. Absolute value flips the ones that are aspirated. BOOM!.
            height = str(abs(5-int(value))) + "50"

            return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code 
        an optional arguement can be passed in which is a different YAML file containing other mappings of probe_codes onto method codes. The appropriate form is

        sitename(lowercase): 
            PROBE_CODE: METHOD_CODE
            PROBE_CODE: METHOD_CODE

        this is a yaml standard. Sorry! :(
        """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass


        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "AIR999"
            return site_code, method_code
            

    def attack_data(self):
        """ gather the daily air temperature data from your chosen DB"""
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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
                    # the timekeep attribute holds onto the 5 minute time, so that if it happens to be the max or the min of the day we have it handy
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        return od

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                # print "the number of total obs is %s" %(num_total_obs)

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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
                elif (num_missing_obs + num_questionable_obs)/num_total_obs > 0.05:
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

                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        print "error in max_valid_obs for %s on %s" %(probe_code, each_date)

                try:
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set it to none
                    # *** something I was testing, : for index,j in enumerate(self.od[probe_code][each_date]['val']):
                    #    print index, j ****
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        print "error in max_valid_time for %s on %s" %(probe_code, each_date)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        print "error in max_valid_flag for %s on %s" %(probe_code, each_date)


                # get the min of those observations - not including missing, questionable, or estimated ones
                try:

                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:

                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        print("error in min_valid_obs for %s on %s") %(probe_code, each_date)

                # get the time of that minimum - conrolled by the minimum value for flags
                try:
                
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        print("error in min_valid_time for %s on %s") %(probe_code, each_date)

                # get the flag on the minimum
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                    
                
                else:
                    print "mean valid obs is none"
                    
                    if mean_valid_obs == None:
                        min_flag = ["M"]
                    
                    else:
                        print("error in minimum flagging for %s on %s") %(probe_code, each_date)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = ["A"]
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

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
                    source = "STEWARTIA_FSDBDATA_MS04311"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04311"
                else:
                    print("no server given")


                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043', 1, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag[0], datetime.datetime.strftime(min_valid_time[0], '%H%M'), "NA", source]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043', 1, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", "None", None, "M", "None", "NA", source]

                #print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class RelHum(object):
    """ For generating MS04302 from LTERLoggers_new or from STEWARTIA
    Takes start date and end date as date strings, server is either SHELDON or STEWARTIA
    
    If a final argument is passed it is a probe-code which can be used to limit how many data
    are run through the tool. 
    No matter what inputs are given, call condense_data on the results to generate row-by-row output. If you pass condense_data an argument of a yaml file, it will use that yaml file to generate the method/probe mapping, otherwise, it will default to the mapping that I give it.
    To have certain probes on certain dates, use the ProbeBoss to call a LIMITED.yaml file which will map each probe to a special start and end date, and generate a header independently.
    """


    def __init__(self, startdate, enddate, server, *limited):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '02'
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database - now can go to either sheldon or stewartia"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, RELHUM_MEAN, RELHUM_MEAN_FLAG from LTERLogger_pro.dbo.MS04312 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, RELHUM_MEAN, RELHUM_MEAN_FLAG from FSDBDATA.dbo.MS04312 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)


    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, RELHUM_MEAN, RELHUM_MEAN_FLAG from LTERLogger_pro.dbo.MS04312 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, RELHUM_MEAN, RELHUM_MEAN_FLAG from FSDBDATA.dbo.MS04312 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)


    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for all that is not H15202 or CS201"""

        if probe_code == "RELCS202":
            height = "150"
            return height

        elif probe_code == "RELH1502":
            height = "150"
            return height

        elif probe_code == "RELVAR10":
            height = "450"
            return height
        
        else:
            value = probe_code[-1:]
            stat = probe_code[3:6]

            # if the site isn't high 15 or cs2met, then the height value is 5-number concatenated with the string 50. Absolute value flips the ones that are aspirated. BOOM!.
            height = str(abs(5-int(value))) + "50"

            return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """
        
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "REL999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily relative humidity data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                print "the number of total obs is %s" %(num_total_obs)

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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

                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        print "error in max_valid_obs for %s on %s" %(probe_code, each_date)

                try:
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set it to none
                    # *** something I was testing, : for index,j in enumerate(self.od[probe_code][each_date]['val']):
                    #    print index, j ****
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        print "error in max_valid_time for %s on %s" %(probe_code, each_date)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        print "error in max_valid_flag for %s on %s" %(probe_code, each_date)


                # get the min of those observations - not including missing, questionable, or estimated ones
                try:

                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:

                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        print("error in min_valid_obs for %s on %s") %(probe_code, each_date)

                # get the time of that minimum - conrolled by the minimum value for flags
                try:
                
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        print("error in min_valid_time for %s on %s") %(probe_code, each_date)

                # get the flag on the minimum
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                    
                
                else:
                    print "mean valid obs is none"
                    
                    if mean_valid_obs == None:
                        min_flag = ["M"]
                    
                    else:
                        print("error in minimum flagging for %s on %s") %(probe_code, each_date)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = ["A"]
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

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
    
        return my_new_rows

class DewPoint(object):
    """ For generating MS04307 from LTERLoggers_New, LTERLoggers_Pro, or FSDBDATA. Takes either a start datae and end date or a server of SHELDON or STEWARTIA.
    If a final argument is passed it is a probe-code which can be used to limit how many data
    are run through the tool. 
    No matter what inputs are given, call condense_data on the results to generate row-by-row output. If you pass condense_data an argument of a yaml file, it will use that yaml file to generate the method/probe mapping, otherwise, it will default to the mapping that I give it.
    To have certain probes on certain dates, use the ProbeBoss to call a LIMITED.yaml file which will map each probe to a special start and end date, and generate a header independently.
    """

    def __init__(self, startdate, enddate, server, *limited):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '07'
        self.server = server

        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, DEWPT_MEAN, DEWPT_MEAN_FLAG from LTERLogger_pro.dbo.MS04317 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, DEWPT_MEAN, DEWPT_MEAN_FLAG from FSDBDATA.dbo.MS04317 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, DEWPT_MEAN, DEWPT_MEAN_FLAG from LTERLogger_pro.dbo.MS04317 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, DEWPT_MEAN, DEWPT_MEAN_FLAG from FSDBDATA.dbo.MS04317 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for all that is not H15202 or CS201"""

        if probe_code == "DEWCS202":
            height = "150"
            return height

        elif probe_code == "DEWH1502":
            height = "150"
            return height

        elif probe_code == "DEWVAR10":
            height = "450"
            return height
        else:

            value = probe_code[-1:]
            stat = probe_code[3:6]

            # if the site isn't high 15 or cs2met, then the height value is 5-number concatenated with the string 50. Absolute value flips the ones that are aspirated. BOOM!.
            height = str(abs(5-int(value))) + "50"

            return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code 
        an optional arguement can be passed in which is a different YAML file containing other mappings of probe_codes onto method codes. The appropriate form is

        sitename(lowercase): 
            PROBE_CODE: METHOD_CODE
            PROBE_CODE: METHOD_CODE

        this is a yaml standard. Sorry! :(
        """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "DEW999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily dewpoint data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                print "the number of total obs is %s" %(num_total_obs)

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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

                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        print "error in max_valid_obs for %s on %s" %(probe_code, each_date)

                try:
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set it to none
                    # *** something I was testing, : for index,j in enumerate(self.od[probe_code][each_date]['val']):
                    #    print index, j ****
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        print "error in max_valid_time for %s on %s" %(probe_code, each_date)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        print "error in max_valid_flag for %s on %s" %(probe_code, each_date)


                # get the min of those observations - not including missing, questionable, or estimated ones
                try:

                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:

                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        print("error in min_valid_obs for %s on %s") %(probe_code, each_date)

                # get the time of that minimum - conrolled by the minimum value for flags
                try:
                
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        print("error in min_valid_time for %s on %s") %(probe_code, each_date)

                # get the flag on the minimum
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                    
                
                else:
                    print "mean valid obs is none"
                    
                    if mean_valid_obs == None:
                        min_flag = ["M"]
                    
                    else:
                        print("error in minimum flagging for %s on %s") %(probe_code, each_date)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = ["A"]
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

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
                    source = "STEWARTIA_FSDBDATA_MS04317"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04317"
                else:
                    print("no server given")

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043',7, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag[0], datetime.datetime.strftime(min_valid_time[0],'%H%M'), "NA", source]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043',7, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, None, "M", None, None, "M", None, "NA", source]

               
                my_new_rows.append(newrow)
    
        return my_new_rows


class VPD(object):
    """ For generating MS04308 from LTERLoggers_new, LTERLoggers_Pro or from STEWARTIA
    Takes start date and end date as date strings, server is either SHELDON or STEWARTIA
    
    If a final argument is passed it is a probe-code which can be used to limit how many data
    are run through the tool. 
    No matter what inputs are given, call condense_data on the results to generate row-by-row output. If you pass condense_data an argument of a yaml file, it will use that yaml file to generate the method/probe mapping, otherwise, it will default to the mapping that I give it.
    To have certain probes on certain dates, use the ProbeBoss to call a LIMITED.yaml file which will map each probe to a special start and end date, and generate a header independently.
    """
    def __init__(self, startdate, enddate, server, *limited):

        import form_connection as fc

                # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 8
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()


    def querydb(self):
        """ queries against the database - now can go to either sheldon or stewartia"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, VPD_MEAN, VPD_MEAN_FLAG from LTERLogger_pro.dbo.MS04318 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, VPD_MEAN, VPD_MEAN_FLAG from FSDBDATA.dbo.MS04318 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, VPD_MEAN, VPD_MEAN_FLAG from LTERLogger_pro.dbo.MS04318 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, VPD_MEAN, VPD_MEAN_FLAG from FSDBDATA.dbo.MS04318 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for all that is not H15202 or CS201"""

        if probe_code == "VPDCS202":
            height = "150"
            return height

        elif probe_code == "VPDH1502":
            height = "150"
            return height

        elif probe_code == "VPDVAR10":
            height = "450"
            return height
        else:

            value = probe_code[-1:]
            stat = probe_code[3:6]

            # if the site isn't high 15 or cs2met, then the height value is 5-number concatenated with the string 50. Absolute value flips the ones that are aspirated. BOOM!.
            height = str(abs(5-int(value))) + "50"

            return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code 
        an optional arguement can be passed in which is a different YAML file containing other mappings of probe_codes onto method codes. The appropriate form is

        sitename(lowercase): 
            PROBE_CODE: METHOD_CODE
            PROBE_CODE: METHOD_CODE

        this is a yaml standard. Sorry! :(
        """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code,method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "VPD999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily dewpoint data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                print "the number of total obs is %s" %(num_total_obs)

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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

                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        print "error in max_valid_obs for %s on %s" %(probe_code, each_date)

                try:
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set it to none
                    # *** something I was testing, : for index,j in enumerate(self.od[probe_code][each_date]['val']):
                    #    print index, j ****
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        print "error in max_valid_time for %s on %s" %(probe_code, each_date)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        print "error in max_valid_flag for %s on %s" %(probe_code, each_date)


                # get the min of those observations - not including missing, questionable, or estimated ones
                try:

                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:

                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        print("error in min_valid_obs for %s on %s") %(probe_code, each_date)

                # get the time of that minimum - conrolled by the minimum value for flags
                try:
                
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        print("error in min_valid_time for %s on %s") %(probe_code, each_date)

                # get the flag on the minimum
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                    
                
                else:
                    print "mean valid obs is none"
                    
                    if mean_valid_obs == None:
                        min_flag = ["M"]
                    
                    else:
                        print("error in minimum flagging for %s on %s") %(probe_code, each_date)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = ["A"]
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

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
                    source = "STEWARTIA_FSDBDATA_MS04318"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04318"
                else:
                    print("no server given")

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag[0], datetime.datetime.strftime(min_valid_time[0],'%H%M'), None, None, None, None, None, None, None, None, None, None, None, None, "NA", source]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043',self.entity, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None, None, "M", None, None, None, None, None, None, None, None, None, None, None, None, None, "NA", source]

               
                my_new_rows.append(newrow)
    
        return my_new_rows

class PhotosyntheticRad(object):
    """ For generating MS04322 from LTERLoggers_new, LTERLoggers_Pro or from STEWARTIA
        Takes start date and end date as date strings, server is either SHELDON or STEWARTIA
    
    If a final argument is passed it is a probe-code which can be used to limit how many data
    are run through the tool. 
    No matter what inputs are given, call condense_data on the results to generate row-by-row output. If you pass condense_data an argument of a yaml file, it will use that yaml file to generate the method/probe mapping, otherwise, it will default to the mapping that I give it.
    To have certain probes on certain dates, use the ProbeBoss to call a LIMITED.yaml file which will map each probe to a special start and end date, and generate a header independently.
    """
    def __init__(self, startdate, enddate, server, *limited):

        import form_connection as fc

                # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 22
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()


    def querydb(self):
        """ queries against the database - now can go to either sheldon or stewartia"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, PAR_MEAN, PAR_MEAN_FLAG from LTERLogger_pro.dbo.MS04332 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, PAR_MEAN, PAR_MEAN_FLAG from FSDBDATA.dbo.MS04332 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, PAR_MEAN, PAR_MEAN_FLAG from LTERLogger_pro.dbo.MS04332 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, PAR_MEAN, PAR_MEAN_FLAG from FSDBDATA.dbo.MS04332 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    @staticmethod
    def heightcalc(probe_code):
        """ determines the height PAR- we only have 1!"""

        if probe_code == "PARCEN01":
            height = "627"
            return height

        else:
            height = "627"
            return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code 
        an optional arguement can be passed in which is a different YAML file containing other mappings of probe_codes onto method codes. The appropriate form is

        sitename(lowercase): 
            PROBE_CODE: METHOD_CODE
            PROBE_CODE: METHOD_CODE

        this is a yaml standard. Sorry! :(
        """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "PAR999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily dewpoint data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                print "the number of total obs is %s" %(num_total_obs)

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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

                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        print "error in max_valid_obs for %s on %s" %(probe_code, each_date)

                try:
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set it to none
                    # *** something I was testing, : for index,j in enumerate(self.od[probe_code][each_date]['val']):
                    #    print index, j ****
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        print "error in max_valid_time for %s on %s" %(probe_code, each_date)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        print "error in max_valid_flag for %s on %s" %(probe_code, each_date)


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

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class SoilTemperature(object):
    """ For generating MS04306 from LTERLoggers_new or from STEWARTIA
    Takes start date and end date as date strings, server is either SHELDON or STEWARTIA
    If a final argument is passed it is a probe-code which can be used to limit how many data
    are run through the tool. 
    No matter what inputs are given, call condense_data on the results to generate row-by-row output. If you pass condense_data an argument of a yaml file, it will use that yaml file to generate the method/probe mapping, otherwise, it will default to the mapping that I give it.
    To have certain probes on certain dates, use the ProbeBoss to call a LIMITED.yaml file which will map each probe to a special start and end date, and generate a header independently.
    """
    def __init__(self, startdate, enddate, server, *limited):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 21
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":

            query = "SELECT DATE_TIME, PROBE_CODE, SOILTEMP_MEAN, SOILTEMP_MEAN_FLAG from LTERLogger_pro.dbo.MS04331 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        elif self.server == "STEWARTIA":

            query = "SELECT DATE_TIME, PROBE_CODE, SOILTEMP_MEAN, SOILTEMP_MEAN_FLAG from FSDBDATA.dbo.MS04331 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, SOILTEMP_MEAN, SOILTEMP_MEAN_FLAG from LTERLogger_pro.dbo.MS04331 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, SOILTEMP_MEAN, SOILTEMP_MEAN_FLAG from FSDBDATA.dbo.MS04331 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    @staticmethod
    def heightcalc(probe_code):
        """ determines the depth for soil probes!"""

        value = probe_code[-1:]
        stat = probe_code[3:6]

        if value == '1':
            height = '10'
        elif value == '2':
            height = '20'
        elif value == '3':
            height = '50'
        elif value == '4':
            height = '100'
        elif value == '6':
            height = '10'
        elif value == '7':
            height = '20'
        elif value == '8':
            height = '50'
        elif value == '9':
            height = '100'
        else:
            height = '10'


        return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "SOI999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily soiltemp data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                print "the number of total obs is %s" %(num_total_obs)

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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

                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        print "error in max_valid_obs for %s on %s" %(probe_code, each_date)

                try:
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set it to none
                    # *** something I was testing, : for index,j in enumerate(self.od[probe_code][each_date]['val']):
                    #    print index, j ****
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        print "error in max_valid_time for %s on %s" %(probe_code, each_date)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        print "error in max_valid_flag for %s on %s" %(probe_code, each_date)


                # get the min of those observations - not including missing, questionable, or estimated ones
                try:

                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:

                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        print("error in min_valid_obs for %s on %s") %(probe_code, each_date)

                # get the time of that minimum - conrolled by the minimum value for flags
                try:
                
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        print("error in min_valid_time for %s on %s") %(probe_code, each_date)

                # get the flag on the minimum
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                    
                
                else:
                    print "mean valid obs is none"
                    
                    if mean_valid_obs == None:
                        min_flag = ["M"]
                    
                    else:
                        print("error in minimum flagging for %s on %s") %(probe_code, each_date)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = ["A"]
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

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
                    source = "STEWARTIA_FSDBDATA_MS04331"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04331"
                else:
                    print("no server given")

                # in the best possible case, we print it out just as it is here: 
                try:
                    newrow = ['MS043', 21, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs,  min_flag[0], datetime.datetime.strftime(min_valid_time[0], '%H%M'), "NA", source]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043', 21, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None, None, "M", None, "NA", source]

                my_new_rows.append(newrow)
            
        return my_new_rows
    

class SoilWaterContent(object):

    """ For generating MS04323 from LTERLoggers_new or from STEWARTIA
    Takes start date and end date as date strings, server is either SHELDON or STEWARTIA
    If a final argument is passed it is a probe-code which can be used to limit how many data
    are run through the tool. 
    No matter what inputs are given, call condense_data on the results to generate row-by-row output. If you pass condense_data an argument of a yaml file, it will use that yaml file to generate the method/probe mapping, otherwise, it will default to the mapping that I give it.
    To have certain probes on certain dates, use the ProbeBoss to call a LIMITED.yaml file which will map each probe to a special start and end date, and generate a header independently.
    """


    def __init__(self, startdate, enddate, server, *limited):
        """ uses form_connection to communicate with the database; queries for a start and end date and possibly a probe code, generates a date-mapped dictionary. """

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 23
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day. Can take time as either a python date object or as a date string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, SOILWC_MEAN, SOILWC_MEAN_FLAG from LTERLogger_pro.dbo.MS04333 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, SOILWC_MEAN, SOILWC_MEAN_FLAG from FSDBDATA.dbo.MS04333 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, SOILWC_MEAN, SOILWC_MEAN_FLAG from LTERLogger_pro.dbo.MS04333 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, SOILWC_MEAN, SOILWC_MEAN_FLAG from FSDBDATA.dbo.MS04333 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    @staticmethod
    def heightcalc(probe_code):
        """ determines the depth for soil probes!"""

        value = probe_code[-1:]
        stat = probe_code[3:6]

        if value == '1':
            height = '10'
        elif value == '2':
            height = '20'
        elif value == '3':
            height = '50'
        elif value == '4':
            height = '100'
        elif value == '6':
            height = '10'
        elif value == '7':
            height = '20'
        elif value == '8':
            height = '50'
        elif value == '9':
            height = '100'
        else:
            height = '10'


        return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code,method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "SWC999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily soil-moisture data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                print "the number of total obs is %s" %(num_total_obs)

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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

                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        print "error in max_valid_obs for %s on %s" %(probe_code, each_date)

                try:
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except ValueError:
                    # check to see if the the whole day was missing, if so, set it to none
                    # *** something I was testing, : for index,j in enumerate(self.od[probe_code][each_date]['val']):
                    #    print index, j ****
                    if mean_valid_obs == None:
                        max_valid_time = None
                    else: 
                        print "error in max_valid_time for %s on %s" %(probe_code, each_date)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]
                    max_flag = max_flag[0].rstrip()

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        print "error in max_valid_flag for %s on %s" %(probe_code, each_date)


                # get the min of those observations - not including missing, questionable, or estimated ones
                try:

                    min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                
                except Exception:

                    if mean_valid_obs == None:
                        min_valid_obs = None
                    else:
                        print("error in min_valid_obs for %s on %s") %(probe_code, each_date)

                # get the time of that minimum - conrolled by the minimum value for flags
                try:
                
                    min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                
                except ValueError:
                    if mean_valid_obs == None:
                        min_valid_time = None
                    else:
                        print("error in min_valid_time for %s on %s") %(probe_code, each_date)

                # get the flag on the minimum
                if mean_valid_obs is not None:

                    min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]
                    min_flag = min_flag[0].rstrip()
                
                else:
                    print "mean valid obs is none"
                    
                    if mean_valid_obs == None:
                        min_flag = ["M"]
                    
                    else:
                        print("error in minimum flagging for %s on %s") %(probe_code, each_date)


                # final exception handles for flags-- take care of the "" for mins and maxes, and for the whole missing days. 
                
                try:
                    if min_flag[0].strip() == "": 
                        min_flag = ["A"]
                    else:
                        pass

                except IndexError:
                    # the minimum flag may not come out if all the values are missing... 
                    if mean_valid_obs == None:
                        min_flag = "M"

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

                # in the best possible case, we print it out just as it is here: 

                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04333"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04333"
                else:
                    print("no server given")

                try:
                    newrow = ['MS043',23, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag[0], datetime.datetime.strftime(min_valid_time[0], '%H%M'), "NA", source]
                
                # in the missing day case, we print out a version with Nones filled in for missing values
                except IndexError:
                    newrow = ['MS043',23, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None, None, "M", None, "NA", source]

                
                my_new_rows.append(newrow)
    
        return my_new_rows

class Precipitation(object):

    def __init__(self, startdate, enddate, server, *limited):

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 3
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()


    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day. Can take time as either a python date object or as a date string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":

            query = "SELECT DATE_TIME, PROBE_CODE, PRECIP_TOT, PRECIP_TOT_FLAG from LTERLogger_pro.dbo.MS04313 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":

            query = "SELECT DATE_TIME, PROBE_CODE, PRECIP_TOT, PRECIP_TOT_FLAG from FSDBDATA.dbo.MS04313 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, PRECIP_TOT, PRECIP_TOT_FLAG from LTERLogger_pro.dbo.MS04313 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, PRECIP_TOT, PRECIP_TOT_FLAG from FSDBDATA.dbo.MS04313 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for the precip gauges!"""

        value = probe_code[-1:]
        stat = probe_code[3:6]

        if stat == "CEN" and value == "1":
            height = "455"
        elif stat == "CEN" and value == "2":
            height = "625"
        elif stat == "PRI":
            height = "100"
        elif stat == "UPL" and value == "1":
            height = "457"
        elif stat == "UPL" and value == "2":
            height = "627"
        elif stat == "VAN" or stat == "VAR":
            height = "425"
        elif stat == "H15":
            height = "410"
        else:
            height = "425"

        return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "PRE999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily precipitation data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                print "the number of total obs is %s" %(num_total_obs)
                

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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


                try:
                    # sum up the observations - not including the missing, questionable, or estimated ones
                    total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)
                except Exception:
                    total_valid_obs = None
                    daily_flag = "M"


                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04333"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04333"
                else:
                    print("no server given")
                
                newrow = ['MS043',3, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), total_valid_obs, daily_flag, "NA", source]

                
                my_new_rows.append(newrow)
    
        return my_new_rows

class SnowLysimeter(object):

    def __init__(self, startdate, enddate, server, *limited):

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 9
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()


    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day. Can take time as either a python date object or as a date string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":

            query = "SELECT DATE_TIME, PROBE_CODE, SNOWMELT_TOT, SNOWMELT_TOT_FLAG from LTERLogger_pro.dbo.MS04319 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":

            query = "SELECT DATE_TIME, PROBE_CODE, SNOWMELT_TOT, SNOWMELT_TOT_FLAG from FSDBDATA.dbo.MS04319 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, SNOWMELT_TOT, SNOWMELT_TOT_FLAG from LTERLogger_pro.dbo.MS04319 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, SNOWMELT_TOT, SNOWMELT_TOT_FLAG from FSDBDATA.dbo.MS04319 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)


    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "SNO999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily precipitation data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging
        the bulk of the action happens here. If an additional arguement is given, it is a new configuration file which is designed to map other method codes onto the probe codes. We pass it into the static function "which stand" from here, as this is actually the iterator which runs over the date-stamped dictionary. output is stored in a list of lists, each sub-list is a row that can be written to a csv or hopefully into a sql statement later!
        """
        
        my_new_rows = []

        # iterate over the returns, getting each probe code - if args are passed, include them also!
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)


            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations - these are observations which are numbers that aren't none
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # there may be the case that all the numbers are none, and in this case, we want to know about it, but keep on going through that day
                if num_valid_obs == 0:
                    print("there are only null values on %s for %s") %(each_date, probe_code)

                
                # get the number of obs - will print every day as is running so that you can be sure it is behaving as expected.
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                print "the number of total obs is %s" %(num_total_obs)
                

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate:

                    # it will break and go on to the next probe if needed when the number of total observations is not 288, 96, or 24. Note that on fully missing days we don't have a problem because we have 288 missing observations!
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class Solar(object):
    """ computes Solar Radiation from Sheldon or Stewartia, NOT the net radiometer version!"""

    def __init__(self, startdate, enddate, server, *limited):

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 5
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day. Can take time as either a python date object or as a date string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, SOLAR_TOT, SOLAR_TOT_FLAG, SOLAR_MEAN, SOLAR_MEAN_FLAG from LTERLogger_pro.dbo.MS04315 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, SOLAR_TOT, SOLAR_TOT_FLAG, SOLAR_MEAN, SOLAR_MEAN_FLAG from FSDBDATA.dbo.MS04315 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, SOLAR_TOT, SOLAR_TOT_FLAG, SOLAR_MEAN, SOLAR_MEAN_FLAG from LTERLogger_pro.dbo.MS04315 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT ATE_TIME, PROBE_CODE, SOLAR_TOT, SOLAR_TOT_FLAG, SOLAR_MEAN, SOLAR_MEAN_FLAG from FSDBDATA.dbo.MS04315 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)


    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for the pyranometers!"""

        stat = probe_code[3:6]

        if stat == "PRI":
            height = "100"
        elif stat == "CEN":
            height = "450"
        elif stat == "UPL":
            height = "617"
        elif stat == "VAN":
            height = "860"
        else:
            height = "450"

        return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """

        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "RAD999"
            return site_code, method_code
    
    def attack_data(self):
        """ gather the daily solar data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
            dt = datetime.datetime(dt_old.year, dt_old.month, dt_old.day)
            probe_code = str(row[1])

            if probe_code not in od:
                # if the probe code isn't there, get the day, val, fval, and store the time to match to the max and min
                od[probe_code] = {dt:{'tot_val': [str(row[2])], 'tot_fval': [str(row[3])], 'mean_val': [str(row[4])], 'mean_fval': [str(row[5])], 'timekeep':[dt_old]}}

            elif probe_code in od:
                
                if dt not in od[probe_code]:
                    # if the probe code is there, but not that day, then add the day as well as the corresponding val, fval, and method
                    od[probe_code][dt] = {'tot_val': [str(row[2])], 'tot_fval':[str(row[3])], 'mean_val': [str(row[4])], 'mean_fval': [str(row[5])], 'timekeep':[dt_old]}

                elif dt in od[probe_code]:
                    # if the date time is in the probecode day, then append the new vals and fvals, and flip to the new method
                    od[probe_code][dt]['tot_val'].append(str(row[2]))
                    od[probe_code][dt]['tot_fval'].append(str(row[3]))
                    od[probe_code][dt]['mean_val'].append(str(row[4]))
                    od[probe_code][dt]['mean_fval'].append(str(row[5]))
                    od[probe_code][dt]['timekeep'].append(dt_old)

                else:
                    pass
            else:
                pass
        
        return od

    def condense_data(self, *args):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs_tot = len([x for x in self.od[probe_code][each_date]['tot_val'] if x != 'None'])
                num_valid_obs_mean = len([x for x in self.od[probe_code][each_date]['mean_val'] if x != 'None'])

                if num_valid_obs_tot == 0:
                    print("There are only null values on %s for %s on the SOLAR TOTAL") %(each_date, probe_code)

                if num_valid_obs_mean == 0:
                    print("There are only null values on %s for %s on the SOLAR MEAN") %(each_date, probe_code)

                # get the number of obs
                num_total_obs_tot = len(self.od[probe_code][each_date]['tot_val'])
                num_total_obs_mean = len(self.od[probe_code][each_date]['mean_val'])
                

                # if it's not a total of observations on that day that we would expect, then print this-- we expect that since this is counting up the rows, it shouldn't matter which it gets!
                if num_total_obs_tot not in [288, 96, 24] and each_date != self.startdate: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs_tot)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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
                if num_missing_obs_tot/num_total_obs_tot >= 0.2:
                    daily_flag_tot = 'M'
                elif (num_missing_obs_tot + num_questionable_obs_tot)/num_total_obs_tot >= 0.05:
                    daily_flag_tot = 'Q'
                elif (num_estimated_obs_tot)/num_total_obs_tot >= 0.05:
                    daily_flag_tot = 'E'
                elif (num_estimated_obs_tot + num_missing_obs_tot + num_questionable_obs_tot)/num_total_obs_tot <= 0.05:
                    daily_flag_tot = 'A'
                else:
                    daily_flag_tot = 'Q'

                # daily flag, mean: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_mean/num_total_obs_mean >= 0.2:
                    daily_flag_mean = 'M'
                elif (num_missing_obs_mean + num_questionable_obs_mean)/num_total_obs_mean >= 0.05:
                    daily_flag_mean = 'Q'
                elif (num_estimated_obs_mean)/num_total_obs_mean >= 0.05:
                    daily_flag_mean = 'E'
                elif (num_estimated_obs_mean + num_missing_obs_mean + num_questionable_obs_mean)/num_total_obs_mean <= 0.05:
                    daily_flag_mean = 'A'
                else:
                    daily_flag_mean = 'Q'

                # compute the mean of the daily observations of mean - not including the missing, questionable, or estimated ones -- do the mean first so you can do it to tag the total if it's bad
                
                try:

                    mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['mean_val'] if x != 'None'])/num_valid_obs_mean),3)

                except ZeroDivisionError:
                    # if the whole day is missing, then the mean_valid_obs is None
                    mean_valid_obs = None


                # compute the total of the daily observations of total - not including the missing, questionable, or estimated ones - use the mean to tag if it's bad

                if mean_valid_obs != None:
                
                    total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['tot_val'] if x != 'None']),3)

                else:
                    # if there's no mean there's no total, either
                    total_valid_obs = None
                
                    
                # get the max of those observations
                try:
                    max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['mean_val'] if x != 'None']),3)

                except ValueError:
                    # check to see if the whole day was missing, if so, set it to none
                    if mean_valid_obs == None:
                        max_valid_obs = None
                    else:
                        print "error in max_valid_obs for %s on %s" %(probe_code, each_date)

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
                        print "error in max_valid_time for %s on %s" %(probe_code, each_date)
                
                if mean_valid_obs is not None:
                    # get the flag of that maximum - which again, is controlled via the max_valid_obs
                    max_flag = [self.od[probe_code][each_date]['mean_fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['mean_val']) if j != "None" and round(float(j),3) == max_valid_obs]

                else:
                    # check to see if the whole day was missing, if so, set to "M"
                    if mean_valid_obs is None:
                        max_flag = ["M"]
                    
                    else:
                        print "error in max_valid_flag for %s on %s" %(probe_code, each_date)


                if self.server == "STEWARTIA":
                    source = "STEWARTIA_FSDBDATA_MS04305"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04305"
                else:
                    print("no server given")

                try:
                    newrow = ['MS043', 5, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), total_valid_obs, daily_flag_tot, mean_valid_obs, daily_flag_mean, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), "NA", self.server]
                
                except Exception:

                    newrow = ['MS043',5, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None, "M", "None", "NA", self.server]


                
                my_new_rows.append(newrow)
    
        return my_new_rows

class NetRadiometer(object):

    """ computes Solar Radiation from Sheldon or Stewartia, THIS IS THE NET RADIOMETER!!"""

    def __init__(self, startdate, enddate, server, *limited):

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 25
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day. Can take time as either a python date object or as a date string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, SW_IN_MEAN, SW_IN_MEAN_FLAG, SW_OUT_MEAN, SW_OUT_MEAN_FLAG, LW_IN_MEAN, LW_IN_MEAN_FLAG, LW_OUT_MEAN, LW_OUT_MEAN_FLAG, NR_TOT_MEAN, NR_TOT_MEAN_FLAG, SENSOR_TEMP, SENSOR_TEMP_FLAG from LTERLogger_pro.dbo.MS04335 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, SW_IN_MEAN, SW_IN_MEAN_FLAG, SW_OUT_MEAN, SW_OUT_MEAN_FLAG, LW_IN_MEAN, LW_IN_MEAN_FLAG, LW_OUT_MEAN, LW_OUT_MEAN_FLAG, NR_TOT_MEAN, NR_TOT_MEAN_FLAG, SENSOR_TEMP, SENSOR_TEMP_FLAG from FSDBDATA.dbo.MS04335 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)

    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, SW_IN_MEAN, SW_IN_MEAN_FLAG, SW_OUT_MEAN, SW_OUT_MEAN_FLAG, LW_IN_MEAN, LW_IN_MEAN_FLAG, LW_OUT_MEAN, LW_OUT_MEAN_FLAG, NR_TOT_MEAN, NR_TOT_MEAN_FLAG, SENSOR_TEMP, SENSOR_TEMP_FLAG from LTERLogger_pro.dbo.MS04335 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, SW_IN_MEAN, SW_IN_MEAN_FLAG, SW_OUT_MEAN, SW_OUT_MEAN_FLAG, LW_IN_MEAN, LW_IN_MEAN_FLAG, LW_OUT_MEAN, LW_OUT_MEAN_FLAG, NR_TOT_MEAN, NR_TOT_MEAN_FLAG, SENSOR_TEMP, SENSOR_TEMP_FLAG from FSDBDATA.dbo.MS04335 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)

    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for the pyranometers!"""

        height = "600"

        return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """

        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code
    
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "RAD999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily net radiometer data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code

            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)

            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs_swin = len([x for x in self.od[probe_code][each_date]['swin_val'] if x != 'None'])
                num_valid_obs_swout = len([x for x in self.od[probe_code][each_date]['swout_val'] if x != 'None'])
                num_valid_obs_lwin = len([x for x in self.od[probe_code][each_date]['lwin_val'] if x != 'None'])
                num_valid_obs_lwout = len([x for x in self.od[probe_code][each_date]['lwout_val'] if x != 'None'])
                num_valid_obs_nr = len([x for x in self.od[probe_code][each_date]['nr_val'] if x != 'None'])
                num_valid_obs_temp = len([x for x in self.od[probe_code][each_date]['temp_val'] if x != 'None'])

                if num_valid_obs_swin == 0:
                    print("There are only null values on %s for %s on the SWIN") %(each_date, probe_code)

                if num_valid_obs_swout == 0:
                    print("There are only null values on %s for %s on the SWOUT") %(each_date, probe_code)

                if num_valid_obs_lwin == 0:
                    print("There are only null values on %s for %s on the LWIN") %(each_date, probe_code)

                if num_valid_obs_lwout == 0:
                    print("There are only null values on %s for %s on the LWOUT") %(each_date, probe_code)

                if num_valid_obs_nr == 0:
                    print("There are only null values on %s for %s on the NR") %(each_date, probe_code)

                if num_valid_obs_temp == 0:
                    print("There are only null values on %s for %s on the TEMP") %(each_date, probe_code)

                # get the number of obs - not sure if maybe it won't be the same for all? i think it will but just in case - this is just counting the day, actually, so it doesn't really matter which one we pull
                num_total_obs = len(self.od[probe_code][each_date]['swin_val'])
                

                # if it's not a total of observations on that day that we would expect, then print this-- we expect that since this is counting up the rows, it shouldn't matter which it gets!
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate: 
                    
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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
                    source = "STEWARTIA_FSDBDATA_MS04305"
                elif self.server == "SHELDON":
                    source = "SHELDON_LTERLogger_PRO_MS04305"
                else:
                    print("no server given")



                try:
                    newrow = ['MS043', 25, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_swin, daily_flag_swin, mean_swout, daily_flag_swout, mean_lwin, daily_flag_lwin, mean_lwout, daily_flag_lwout, mean_nr, daily_flag_nr, mean_temp, daily_flag_temp,"NA"]
                
                except Exception:
                    # which might happen if a day is just missing

                    newrow = ['MS043', 25, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), None, "M", None, "M", None, "M", None, "M", None, "M", None, "M", "NA"]


                
                my_new_rows.append(newrow)
    
        return my_new_rows

class Wind(object):

    def __init__(self, startdate, enddate, server, *limited):

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 4
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day. Can take time as either a python date object or as a date string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, WSPD_PRO_MEAN, WSPD_PRO_MEAN_FLAG, WMAG_PRO_MEAN, WMAG_PRO_MEAN_FLAG, WDIR_PRO_MEAN, WDIR_PRO_MEAN_FLAG, WDIR_PRO_STDDEV, WDIR_PRO_STDDEV_FLAG from LTERLogger_pro.dbo.MS04314 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, WSPD_PRO_MEAN, WSPD_PRO_MEAN_FLAG, WMAG_PRO_MEAN, WMAG_PRO_MEAN_FLAG, WDIR_PRO_MEAN, WDIR_PRO_MEAN_FLAG, WDIR_PRO_STDDEV, WDIR_PRO_STDDEV_FLAG from FSDBDATA.dbo.MS04314 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)


    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, WSPD_PRO_MEAN, WSPD_PRO_MEAN_FLAG, WMAG_PRO_MEAN, WMAG_PRO_MEAN_FLAG, WDIR_PRO_MEAN, WDIR_PRO_MEAN_FLAG, WDIR_PRO_STDDEV, WDIR_PRO_STDDEV_FLAG  from LTERLogger_pro.dbo.MS04314 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, WSPD_PRO_MEAN, WSPD_PRO_MEAN_FLAG, WMAG_PRO_MEAN, WMAG_PRO_MEAN_FLAG, WDIR_PRO_MEAN, WDIR_PRO_MEAN_FLAG, WDIR_PRO_STDDEV, WDIR_PRO_STDDEV_FLAG  from FSDBDATA.dbo.MS04314 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)


    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for the prop anemometers!"""

        stat = probe_code[3:6]

        if stat == "H15":
            height = "500"
        else:
            height = "1000"

        return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
        
        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code
        
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "WND999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily wind (propellor) data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)
            
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

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
                if num_total_obs_spd not in [288, 96, 24] and each_date != self.startdate: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs_spd)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
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

                    daily_dir_valid_obs = math.degrees(theta_u)

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


                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class Sonic(object):

    def __init__(self, startdate, enddate, server, *limited):

        import form_connection as fc

        # the server is either "SHELDON" or "STEWARTIA"
        self.cursor = fc.form_connection(server)

        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = 24
        self.server = server
        
        if not limited:
            # query against the database the normal way
            self.querydb()

        elif limited:
            # query against the database, but only for that one probe code
            probe_code = limited[0]
            self.querydb_limited(probe_code)

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day. Can take time as either a python date object or as a date string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, WSPD_SNC_MEAN, WSPD_SNC_MEAN_FLAG, WSPD_SNC_MAX, WSPD_SNC_MAX_FLAG, WDIR_SNC_MEAN, WDIR_SNC_MEAN_FLAG, WDIR_SNC_STDDEV, WDIR_SNC_STDDEV_FLAG,  WUX_SNC_MEAN,  WUX_SNC_MEAN_FLAG,  WUX_SNC_STDDEV,  WUX_SNC_STDDEV_FLAG, WUY_SNC_MEAN,  WUY_SNC_MEAN_FLAG,  WUY_SNC_STDDEV,  WUY_SNC_STDDEV_FLAG, WAIR_SNC_MEAN,  WAIR_SNC_MEAN_FLAG,  WAIR_SNC_STDDEV,  WAIR_SNC_STDDEV_FLAG from LTERLogger_pro.dbo.MS04334 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, WSPD_SNC_MEAN, WSPD_SNC_MEAN_FLAG, WSPD_SNC_MAX, WSPD_SNC_MAX_FLAG, WDIR_SNC_MEAN, WDIR_SNC_MEAN_FLAG, WDIR_SNC_STDDEV, WDIR_SNC_STDDEV_FLAG,  WUX_SNC_MEAN,  WUX_SNC_MEAN_FLAG,  WUX_SNC_STDDEV,  WUX_SNC_STDDEV_FLAG, WUY_SNC_MEAN,  WUY_SNC_MEAN_FLAG,  WUY_SNC_STDDEV,  WUY_SNC_STDDEV_FLAG, WAIR_SNC_MEAN,  WAIR_SNC_MEAN_FLAG,  WAIR_SNC_STDDEV,  WAIR_SNC_STDDEV_FLAG from FSDBDATA.dbo.MS04334 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)


    def querydb_limited(self, probe_code):
        """ queries the data base, limited to certain probes, use in special cases. can take date as either a python date object or as a date-string"""

        if type(self.startdate) is datetime.datetime:
            startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        else:
            pass

        if type(self.enddate) is datetime.datetime:
            enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')
        else:
            pass
            
        if self.server == "SHELDON":
            query = "SELECT DATE_TIME, PROBE_CODE, WSPD_SNC_MEAN, WSPD_SNC_MEAN_FLAG, WSPD_SNC_MAX, WSPD_SNC_MAX_FLAG, WDIR_SNC_MEAN, WDIR_SNC_MEAN_FLAG, WDIR_SNC_STDDEV, WDIR_SNC_STDDEV_FLAG,  WUX_SNC_MEAN,  WUX_SNC_MEAN_FLAG,  WUX_SNC_STDDEV,  WUX_SNC_STDDEV_FLAG, WUY_SNC_MEAN,  WUY_SNC_MEAN_FLAG,  WUY_SNC_STDDEV,  WUY_SNC_STDDEV_FLAG, WAIR_SNC_MEAN,  WAIR_SNC_MEAN_FLAG,  WAIR_SNC_STDDEV,  WAIR_SNC_STDDEV_FLAG  from LTERLogger_pro.dbo.MS04334 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code + "\' ORDER BY DATE_TIME ASC"
        
        elif self.server == "STEWARTIA":
            query = "SELECT DATE_TIME, PROBE_CODE, WSPD_SNC_MEAN, WSPD_SNC_MEAN_FLAG, WSPD_SNC_MAX, WSPD_SNC_MAX_FLAG, WDIR_SNC_MEAN, WDIR_SNC_MEAN_FLAG, WDIR_SNC_STDDEV, WDIR_SNC_STDDEV_FLAG,  WUX_SNC_MEAN,  WUX_SNC_MEAN_FLAG,  WUX_SNC_STDDEV,  WUX_SNC_STDDEV_FLAG, WUY_SNC_MEAN,  WUY_SNC_MEAN_FLAG,  WUY_SNC_STDDEV,  WUY_SNC_STDDEV_FLAG, WAIR_SNC_MEAN,  WAIR_SNC_MEAN_FLAG,  WAIR_SNC_STDDEV,  WAIR_SNC_STDDEV_FLAG  from FSDBDATA.dbo.MS04334 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' AND PROBE_CODE LIKE \'" + probe_code +"\' ORDER BY DATE_TIME ASC"
        
        self.cursor.execute(query)


    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for the prop anemometers!"""

        height = "1000"

        return height

    @staticmethod
    def whichsite(probe_code, *args):
        """ match the probe code to the site and method code """
        import yaml

        if not args: 
            with open('CONFIG.yaml','rb') as readfile:
                cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                cfg = yaml.load(readfile)

        else:
            pass

        if probe_code in cfg['cenmet'].keys():
            site_code = 'CENMET'
            method_code = cfg['cenmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['primet'].keys():
            site_code = "PRIMET"
            method_code = cfg['primet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['vanmet'].keys():
            site_code = 'VANMET'
            method_code = cfg['vanmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['varmet'].keys():
            site_code = 'VARMET'
            method_code = cfg['varmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['uplmet'].keys():
            site_code = 'UPLMET'
            method_code = cfg['uplmet'][probe_code]
            return site_code, method_code

        elif probe_code in cfg['cs2met'].keys():
            site_code = 'CS2MET'
            method_code = cfg['cs2met'][probe_code]
            return site_code, method_code
        
        elif probe_code in cfg['h15met'].keys():
            site_code = 'H15MET'
            method_code = cfg['h15met'][probe_code]
            return site_code, method_code
        
        else:
            # reference stand id
            site_code = "RS" + probe_code[3:5]
            method_code = "WND999"
            return site_code, method_code

    def attack_data(self):
        """ gather the daily wind (propellor) data """
        
        # obtained dictionary dictionary
        od = {}

        for row in self.cursor:

            # get only the day
            
            dt_old = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
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

    def condense_data(self, *args):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            if args:
                site_code, method_code = self.whichsite(probe_code, args[0])
            else:
                site_code, method_code = self.whichsite(probe_code)
            
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['snc_mean_val'] if x != 'None'])
                num_valid_obs_max = len([x for x in self.od[probe_code][each_date]['snc_max_val'] if x != 'None'])
                num_valid_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])
                num_valid_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_val'] if x != 'None'])

                # wux
                num_valid_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_val'] if x != 'None'])
                num_valid_obs_wuxstd = len([x for x in self.od[probe_code][each_date]['wux_std_val'] if x != 'None'])
                # wuy
                num_valid_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_val'] if x != 'None'])
                num_valid_obs_wuystd = len([x for x in self.od[probe_code][each_date]['wuy_std_val'] if x != 'None'])
                # wair
                num_valid_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_val'] if x != 'None'])
                num_valid_obs_wairstd = len([x for x in self.od[probe_code][each_date]['wair_std_val'] if x != 'None'])

                # get the number of obs - only need one of these
                num_total_obs = len(self.od[probe_code][each_date]['snc_mean_val'])
                
                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs not in [288, 96, 24] and each_date != self.startdate: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                    print("I will not process the day %s for probe %s as it has not been gap filled") %(each_date, probe_code)
                    continue

                
                else:
                    pass

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_snc_mean = len([x for x in self.od[probe_code][each_date]['snc_mean_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_snc_mean = len([x for x in self.od[probe_code][each_date]['snc_mean_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_snc_mean = len([x for x in self.od[probe_code][each_date]['snc_mean_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_snc_max = len([x for x in self.od[probe_code][each_date]['snc_max_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_snc_max = len([x for x in self.od[probe_code][each_date]['snc_max_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_snc_max = len([x for x in self.od[probe_code][each_date]['snc_max_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dir = len([x for x in self.od[probe_code][each_date]['dir_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily dirstd
                num_missing_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_dirstd = len([x for x in self.od[probe_code][each_date]['dirstd_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'E'])


                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wux = len([x for x in self.od[probe_code][each_date]['wux_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wuy = len([x for x in self.od[probe_code][each_date]['wuy_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wair = len([x for x in self.od[probe_code][each_date]['wair_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wuxstd = len([x for x in self.od[probe_code][each_date]['wux_std_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wuxstd = len([x for x in self.od[probe_code][each_date]['wux_std_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wuxstd = len([x for x in self.od[probe_code][each_date]['wux_std_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wuystd = len([x for x in self.od[probe_code][each_date]['wuy_std_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wuystd = len([x for x in self.od[probe_code][each_date]['wuy_std_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wuystd = len([x for x in self.od[probe_code][each_date]['wuy_std_fval'] if x == 'E'])

                # get the number of each flag present- i.e. count M's, I's, Q's, O's, E's, etc. - for the daily mean speed
                num_missing_obs_wairstd = len([x for x in self.od[probe_code][each_date]['wair_std_fval'] if x == 'M' or x == 'I'])
                num_questionable_obs_wairstd = len([x for x in self.od[probe_code][each_date]['wair_std_fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs_wairstd = len([x for x in self.od[probe_code][each_date]['wair_std_fval'] if x == 'E'])

                # daily flag, wind speed-- if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
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

                # daily flag, wind mag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
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

                # daily flag, wind dir: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
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

                # daily flag, wind wux std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
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

                # daily flag, wind dir: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
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

                # daily flag, wind wuy std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
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

                # daily flag, wind dir: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
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

                # daily flag, wind wair std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
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

                except ZeroDivisionError:
                    # if the case is that there are no valid observations
                    daily_snc_mean = None
                    # magnitude and speed are therefore also M
                    daily_flag_snc_mean = "M"

                try: 
                    # compute the  daily max sonic speed? -- max of the max!...
                    daily_snc_max = round(max([float(x) for x in self.od[probe_code][each_date]['snc_max_val'] if x != 'None']),3)

                except ValueError:
                    # if the case is that there are no valid observations
                    daily_snc_max = None
                    # magnitude and speed are therefore also M
                    daily_flag_snc_max = "M"


                try: 
                    # compute the mean wux
                    daily_wux = round(float(sum([float(x) for x in self.od[probe_code][each_date]['wux_val'] if x != 'None'])/num_valid_obs),3)

                except ZeroDivisionError:
                    # if the case is that there are no valid observations
                    daily_wux = None
                    # magnitude and speed are therefore also M
                    daily_flag_wux = "M"

                try: 
                    # compute the daily wuy
                    daily_wuy = round(float(sum([float(x) for x in self.od[probe_code][each_date]['wuy_val'] if x != 'None'])/num_valid_obs),3)

                except ZeroDivisionError:
                    # if the case is that there are no valid observations
                    daily_wuy = None
                    # magnitude and speed are therefore also M
                    daily_flag_wuy = "M"


                try: 
                    # compute daily air temp
                    daily_wair = round(float(sum([float(x) for x in self.od[probe_code][each_date]['wair_val'] if x != 'None'])/num_valid_obs),3)

                except ZeroDivisionError:
                    # if the case is that there are no valid observations
                    daily_wair = None
                    # magnitude and speed are therefore also M
                    daily_flag_wair = "M"
                    

                if num_valid_obs != 0:
                # compute the daily resultant--- this one is a true joy.
                
                ## This is a working example using [2, 2, 2, 4, 4] for speed and [10, 10, 10, 350, 10] for direction:
                # daily_mag_x_part = (sum([speed * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(speedlist,dirlist) if speed != 'None' and x != 'None'])/5.)**2
                # daily_mag_y_part = (sum([speed * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(speedlist,dirlist) if speed != 'None' and x != 'None'])/5.)**2
                # math.sqrt(daily_mag_y_part + daily_mag_x_part)
                # >> Returns: 2.7653239207215683

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



                    theta_u = math.atan2(sum([float(x) for x in self.od[probe_code][each_date]['wuy_val'] if x != 'None'])/num_valid_obs, sum([float(x) for x in self.od[probe_code][each_date]['wux_val'] if x != 'None'])/num_valid_obs)

                    daily_dir_valid_obs = math.degrees(theta_u)

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

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows