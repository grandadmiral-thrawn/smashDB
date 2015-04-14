#import smashControls
import pymssql
import math
import datetime
import csv
import yaml

class HeaderWriter(object):
    """ Writes a header given a certain attribute"""


    def __init__(self, attribute):
    
        daily_attr = {'ATM': '26',
                'NR': '25',
                'WSPD_SNC': '24',
                'SOILWC':'23',
                'PAR':'22',
                'SOILTEMP':'21',
                'SNOWMELT':'09',
                'VPD':'08',
                'DEWPT':'07',
                'SOLAR': '05',
                'WSPD_PRO': '04',
                'PRECIP': '03',
                'RELHUM':'02',
                'AIRTEMP':'01',}

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
    
        self.header = self.write_header_template()

        # the filename is a generated filename
        self.filename = dbcode + daily_attr[attribute] + '_test.csv'

        self.help = """The following codes can be used for attribute access:
        'ATM'' for atmospheric pressure, 'NR' for net radiation, 'PAR' for photosynthetic
        active radiation, 'SOILTEMP' for soil temperature, 'SNOWMELT' for snowmelt, 
        'VPD' for vapor pressure deficit, 'DEWPT' for dewpoint, 'SOLAR' for pyranometer,
        'WSPD_PRO' for propellor anemometer, 'WSPD_SNC' for sonic anemometer,
        'PRECIP' for precipitation (as rain), 'RELHUM' for relative
        humidity, and 'AIRTEMP' for airtemp"""

    def isdirty(self):
        """ If an attribute has to do with the soil, it will take depth rather than height in the header"""

        if self.attribute == "SOILMP" or self.attribute == "SOILTEMP":
            height_word = "DEPTH"
        else:
            height_word = "HEIGHT"

        return height_word

    def write_header_template(self):
        """ The following headers are generated based on given attributes"""

        # the "big six" simplest case
        if self.attribute == "AIRTEMP" or self.attribute == "RELHUM" or self.attribute == "SOILWC" or self.attribute == "DEWPT" or self.attribute == "VPD" or self.attribute == "SOILTEMP":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, self.min_method, self.mintime_method, self.min_flag_method, "EVENT_CODE"]

        # attributes which need a "total"
        elif self.attribute == "PRECIP":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "PRECIP_TOT_DAY", "PRECIP_TOT_FLAG", "EVENT_CODE"]

        # propellor anemometer
        elif self.attribute == "WSPD_PRO":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "WMAG_PRO_MEAN_DAY", "WMAG_PRO_MEAN_FLAG", "WDIR_PRO_MEAN_DAY", "WDIR_PRO_MEAN_FLAG", "WDIR_PRO_STDDEV_DAY", "WDIR_PRO_STDDEV_FLAG", "EVENT_CODE"]

        # the sonic anemometer
        elif self.attribute == "WSPD_SNC":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "WMAG_SNC_MEAN_DAY", "WMAG_SNC_MEAN_FLAG", "WDIR_SNC_MEAN_DAY", "WDIR_SNC_MEAN_FLAG", "WDIR_SNC_STDDEV_DAY", "WDIR_SNC_STDDEV_FLAG", "WUX_SNC_MEAN_DAY", "WUX_SNC_MEAN_FLAG", "WUX_SNC_STDDEV_DAY", "WUX_SNC_STDDEV_DAY_FLAG","WUY_SNC_MEAN_DAY", "WUY_SNC_MEAN_FLAG", "WUY_SNC_STDDEV_DAY", "WUY_SNC_STDDEV_DAY_FLAG", "WAIR_SNC_MEAN_DAY", "WAIR_SNC_MEAN_FLAG", "WAIR_SNC_STDDEV_DAY", "WAIR_SNC_STDDEV_FLAG",  "EVENT_CODE"]

        # net radiometer
        elif self.attribute == "NR":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "SW_IN_MEAN_DAY", "SW_IN_MEAN_FLAG", "SW_OUT_MEAN_DAY", "SW_OUT_MEAN_FLAG", "LW_IN_MEAN_DAY", "LW_IN_MEAN_FLAG", "LW_OUT_MEAN_DAY", "LW_OUT_MEAN_FLAG", "NR_TOT_MEAN_DAY", "NR_TOT_MEAN_FLAG", "SENSOR_TEMP_DAY", "SENSOR_TEMP_FLAG", "EVENT_CODE"]

        # pyranometer (similar method to precip but takes a max and min time as well)
        elif self.attribute == "SOLAR":
            
            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "SOLAR_TOT_DAY", "SOLAR_TOT_FLAG", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "EVENT_CODE"]

        # very simple, like the "big six", but no minimums
        elif self.attribute == "PAR":
            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "EVENT_CODE"]

        # still not sure if this is right
        elif self.attribute == "SNOWMELT":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "SNOWMELT_TOT_DAY", "SNOWMELT_TOT_FLAG", "EVENT_CODE"]

        return header

class AirTemperature(object):
    """ For generating MS04301 from LTERLoggers_new"""


    def __init__(self, startdate, enddate):
        """ uses form_connection to communicate with the database- in this case the back end to gather the data """
        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '01'
        
        # query against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries the data base and returns the cursor after population- the date start will include all the day, the date end will NOT include that final day"""

        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')

        query = "SELECT DATE_TIME, PROBE_CODE, AIRTEMP_MEAN, AIRTEMP_MEAN_FLAG from LTERLogger_new.dbo.MS04311 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
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
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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
        """ gather the daily air temperature data """
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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            site_code, method_code = self.whichsite(probe_code)
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not a total of observations on that day that we would expect, and it's not the first day, then do this:
                if each_date != self.startdate:
                    if num_total_obs != 288 or num_total_obs != 96: 
                        print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                        break
                    else:
                        pass
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
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of the daily observations - not including the missing, questionable, or estimated ones
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)

                # get the max of those observations
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['val']):
                        print index, j

                # get the flag of that maximum - which again, is controlled via the max_valid_obs
                max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]


                # get the min of those observations - not including missing, questionable, or estimated ones
                min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                # get the time of that minimum - conrolled by the minimum value for flags
                min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                # get the time of that minimum
                min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                if min_flag[0] == "": 
                    min_flag[0] = "A"
                else:
                    pass

                if max_flag[0] =="":
                    max_flag[0] = "A"
                else:
                    pass

                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%Y-%m-%d %H:%M:%S'), min_valid_obs, min_valid_time[0], min_flag[0], "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class RelHum(object):

    def __init__(self, startdate, enddate):

        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '02'

        # queries against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')


        query = "SELECT DATE_TIME, PROBE_CODE, RELHUM_MEAN, RELHUM_MEAN_FLAG from LTERLogger_new.dbo.MS04312 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

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
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            site_code, method_code = self.whichsite(probe_code)
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                
                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs != 288 or num_total_obs != 24 or num_total_obs != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
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
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of the daily observations - not including the missing, questionable, or estimated ones
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)

                # get the max of those observations
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['val']):
                        print index, j

                # get the flag of that maximum - which again, is controlled via the max_valid_obs
                max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]


                # get the min of those observations - not including missing, questionable, or estimated ones
                min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                # get the time of that minimum - conrolled by the minimum value for flags
                min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                # get the time of that minimum
                min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                if min_flag[0] == "": 
                    min_flag[0] = "A"
                else:
                    pass

                if max_flag[0] =="":
                    max_flag[0] = "A"
                else:
                    pass

                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%Y-%m-%d %H:%M:%S'), min_valid_obs, min_valid_time[0], min_flag[0], "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class DewPoint(object):

    def __init__(self, startdate, enddate):

        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '07'

        # queries against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')


        query = "SELECT DATE_TIME, PROBE_CODE, DEWPT_MEAN, DEWPT_MEAN_FLAG from LTERLogger_new.dbo.MS04317 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

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
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            site_code, method_code = self.whichsite(probe_code)
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                

                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs != 288 or num_total_obs != 24 or num_total_obs != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
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
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of the daily observations - not including the missing, questionable, or estimated ones
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)

                # get the max of those observations
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['val']):
                        print index, j

                # get the flag of that maximum - which again, is controlled via the max_valid_obs
                max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]


                # get the min of those observations - not including missing, questionable, or estimated ones
                min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                # get the time of that minimum - conrolled by the minimum value for flags
                min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                # get the time of that minimum
                min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                if min_flag[0] == "": 
                    min_flag[0] = "A"
                else:
                    pass

                if max_flag[0] =="":
                    max_flag[0] = "A"
                else:
                    pass

                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%Y-%m-%d %H:%M:%S'), min_valid_obs, min_valid_time[0], min_flag[0], "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class VPD(object):

    def __init__(self, startdate, enddate):

        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '02'

        # queries against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')


        query = "SELECT DATE_TIME, PROBE_CODE, VPD_MEAN, VPD_MEAN_FLAG from LTERLogger_new.dbo.MS04318 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME <= \'" + enddate + "\' ORDER BY DATE_TIME ASC"

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
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            site_code, method_code = self.whichsite(probe_code)
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['val'])
               

                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs != 288 or num_total_obs != 24 or num_total_obs != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
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
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of the daily observations - not including the missing, questionable, or estimated ones
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)

                # get the max of those observations
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['val']):
                        print index, j

                # get the flag of that maximum - which again, is controlled via the max_valid_obs
                max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]


                # get the min of those observations - not including missing, questionable, or estimated ones
                min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                # get the time of that minimum - conrolled by the minimum value for flags
                min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                # get the time of that minimum
                min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                if min_flag[0] == "": 
                    min_flag[0] = "A"
                else:
                    pass

                if max_flag[0] =="":
                    max_flag[0] = "A"
                else:
                    pass

                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%Y-%m-%d %H:%M:%S'), min_valid_obs, min_valid_time[0], min_flag[0], "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class SoilTemperature(object):

    def __init__(self, startdate, enddate):

        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '21'

        # queries against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')


        query = "SELECT DATE_TIME, PROBE_CODE, SOILTEMP_MEAN, SOILTEMP_MEAN_FLAG from LTERLogger_new.dbo.MS04331 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

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
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            site_code, method_code = self.whichsite(probe_code)
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['val'])

                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs != 288 or num_total_obs != 24 or num_total_obs != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
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
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of the daily observations - not including the missing, questionable, or estimated ones
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)

                # get the max of those observations
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['val']):
                        print index, j

                # get the flag of that maximum - which again, is controlled via the max_valid_obs
                max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]


                # get the min of those observations - not including missing, questionable, or estimated ones
                min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                # get the time of that minimum - conrolled by the minimum value for flags
                min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                # get the time of that minimum
                min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                if min_flag[0] == "": 
                    min_flag[0] = "A"
                else:
                    pass

                if max_flag[0] =="":
                    max_flag[0] = "A"
                else:
                    pass

                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%Y-%m-%d %H:%M:%S'), min_valid_obs, min_valid_time[0], min_flag[0], "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows
    

class SoilWaterContent(object):

    def __init__(self, startdate, enddate):

        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '23'

        # queries against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')


        query = "SELECT DATE_TIME, PROBE_CODE, SOILWC_MEAN, SOILWC_MEAN_FLAG from LTERLogger_new.dbo.MS04333 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

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
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            site_code, method_code = self.whichsite(probe_code)
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                

                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs != 288 or num_total_obs != 24 or num_total_obs != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
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
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of the daily observations - not including the missing, questionable, or estimated ones
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)

                # get the max of those observations
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['val']):
                        print index, j

                # get the flag of that maximum - which again, is controlled via the max_valid_obs
                max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                # get the min of those observations - not including missing, questionable, or estimated ones
                min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                # get the time of that minimum - conrolled by the minimum value for flags
                min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                # get the time of that minimum
                min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == min_valid_obs]

                if min_flag[0] == "": 
                    min_flag[0] = "A"
                else:
                    if min_flag[0] not in ["","E","M","Q","A"]:
                        min_flag[0] = "space"
                    else:
                        pass

                if max_flag[0] =="":
                    max_flag[0] = "A"
                else:
                    if max_flag[0] not in ["","E","M","Q","A"]:
                        max_flag[0] = "spaces"
                    else:
                        pass

                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%Y-%m-%d %H:%M:%S'), min_valid_obs, min_valid_time[0], min_flag[0], "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class Precipitation(object):

    def __init__(self, startdate, enddate):

        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '13'

        # queries against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')


        query = "SELECT DATE_TIME, PROBE_CODE, PRECIP_INST, PRECIP_INST_FLAG from LTERLogger_new.dbo.MS04313 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

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
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            site_code, method_code = self.whichsite(probe_code)
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs = len([x for x in self.od[probe_code][each_date]['val'] if x != 'None'])
                # get the number of obs
                num_total_obs = len(self.od[probe_code][each_date]['val'])
                

                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs != 288 or num_total_obs != 24 or num_total_obs != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
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
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # sum up the observations - not including the missing, questionable, or estimated ones
                total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                
                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), total_valid_obs, daily_flag, "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class Solar(object):

    def __init__(self, startdate, enddate):

        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '05'

        # queries against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')


        query = "SELECT DATE_TIME, PROBE_CODE, SOLAR_TOT, SOLAR_TOT_FLAG, SOLAR_MEAN, SOLAR_MEAN_FLAG from LTERLogger_new.dbo.MS04315 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

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
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
            site_code, method_code = self.whichsite(probe_code)
            height = self.heightcalc(probe_code)

            # iterate over each of the dates
            for each_date in sorted(self.od[probe_code].keys()):

                # get the number of valid observations
                num_valid_obs_tot = len([x for x in self.od[probe_code][each_date]['tot_val'] if x != 'None'])
                num_valid_obs_mean = len([x for x in self.od[probe_code][each_date]['mean_val'] if x != 'None'])
                # get the number of obs
                num_total_obs_tot = len(self.od[probe_code][each_date]['tot_val'])
                num_total_obs_mean = len(self.od[probe_code][each_date]['mean_val'])
                

                # if it's not a total of observations on that day that we would expect, then print this
                if num_total_obs_tot != 288 or num_total_obs_tot != 24 or num_total_obs_tot != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs_tot)
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
                elif (num_estimated_obs_tot + num_missing_obs_tot + num_questionable_obs_tot) <= 0.05:
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
                elif (num_estimated_obs_mean + num_missing_obs_mean + num_questionable_obs_mean) <= 0.05:
                    daily_flag_mean = 'A'
                else:
                    daily_flag_mean = 'Q'

                # compute the total of the daily observations of total - not including the missing, questionable, or estimated ones
                total_valid_obs = round(sum([float(x) for x in self.od[probe_code][each_date]['tot_val'] if x != 'None']),3)


                # compute the mean of the daily observations of mean - not including the missing, questionable, or estimated ones
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['mean_val'] if x != 'None'])/num_valid_obs_mean),3)


                # get the max of those observations (means)
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['mean_val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['mean_val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['mean_val']):
                        print index, j

                # get the flag of that maximum - which again, is controlled via the max_valid_obs
                max_flag = [self.od[probe_code][each_date]['mean_fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['mean_val']) if j != "None" and round(float(j),3) == max_valid_obs]

                # no minimum is computed
                if max_flag[0] =="":
                    max_flag[0] = "A"
                else:
                    pass

                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), total_valid_obs, daily_flag_tot, mean_valid_obs, daily_flag_mean, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%Y-%m-%d %H:%M:%S'), "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

class Wind(object):

    def __init__(self, startdate, enddate):

        import form_connection as fc

        self.cursor = fc.form_connection("SHELDON")
        self.startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S')
        self.entity = '04'

        # queries against the database
        self.querydb()

        # od is the 'obtained dictionary'. it is blank before the query. 
        self.od = {}
        self.od = self.attack_data()

    def querydb(self):
        """ queries against the database"""
            
        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')


        query = "SELECT DATE_TIME, PROBE_CODE, WSPD_PRO_MEAN, WSPD_PRO_MEAN_FLAG, WMAG_PRO_MEAN, WMAG_PRO_MEAN_FLAG, WDIR_PRO_MEAN, WDIR_PRO_MEAN_FLAG, WDIR_PRO_STDDEV, WDIR_PRO_STDDEV_FLAG from LTERLogger_new.dbo.MS04314 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME < \'" + enddate + "\' ORDER BY DATE_TIME ASC"

        self.cursor.execute(query)


    @staticmethod
    def heightcalc(probe_code):
        """ determines the height for the pyranometers!"""

        stat = probe_code[3:6]

        if stat == "H15":
            height = "500"
        else:
            height = "1000"

        return height

    @staticmethod
    def whichsite(probe_code):
        """ match the probe code to the site and method code """
        import yaml

        with open('CONFIG.yaml','rb') as readfile:
            cfg = yaml.load(readfile)

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
                return method_code

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

    def condense_data(self):
        """ check the date range, do stats and flagging"""
        
        my_new_rows = []

        # iterate over the returns, getting each probe code
        for probe_code in self.od.keys():

            # get the site code and the method code from that probe code
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
                    break
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
                if num_missing_obs_spd/num_spdal_obs_spd >= 0.2:
                    daily_flag_spd = 'M'
                elif (num_missing_obs_spd + num_questionable_obs_spd)/num_spdal_obs_spd >= 0.05:
                    daily_flag_spd = 'Q'
                elif (num_estimated_obs_spd)/num_spdal_obs_spd >= 0.05:
                    daily_flag_spd = 'E'
                elif (num_estimated_obs_spd + num_missing_obs_spd + num_questionable_obs_spd) <= 0.05:
                    daily_flag_spd = 'A'
                else:
                    daily_flag_spd = 'Q'

                # daily flag, wind mag: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_mag/num_total_obs_mag >= 0.2:
                    daily_flag_mag = 'M'
                elif (num_missing_obs_mag + num_questionable_obs_mag)/num_total_obs_mag >= 0.05:
                    daily_flag_mag = 'Q'
                elif (num_estimated_obs_mag)/num_total_obs_mag >= 0.05:
                    daily_flag_mag = 'E'
                elif (num_estimated_obs_mag + num_missing_obs_mag + num_questionable_obs_mag) <= 0.05:
                    daily_flag_mag = 'A'
                else:
                    daily_flag_mag = 'Q'


                # daily flag, wind dir: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_dir/num_total_obs_dir >= 0.2:
                    daily_flag_dir = 'M'
                elif (num_missing_obs_dir + num_questionable_obs_dir)/num_total_obs_dir >= 0.05:
                    daily_flag_dir = 'Q'
                elif (num_estimated_obs_dir)/num_total_obs_dir >= 0.05:
                    daily_flag_dir = 'E'
                elif (num_estimated_obs_dir + num_missing_obs_dir + num_questionable_obs_dir) <= 0.05:
                    daily_flag_dir = 'A'
                else:
                    daily_flag_dir = 'Q'

                # daily flag, wind dir std: if missing relative to total > 20 % missing, if missing + questionable relative to total > 5%, questionable, if estimated relative to total > 5%, estimated, if estimated + missing + questionable < 5 %, accepted, otherwise, questionable.
                if num_missing_obs_dirstd/num_total_obs_dirstd >= 0.2:
                    daily_flag_dirstd = 'M'
                elif (num_missing_obs_dirstd + num_questionable_obs_dirstd)/num_total_obs_dirstd >= 0.05:
                    daily_flag_dirstd = 'Q'
                elif (num_estimated_obs_dirstd)/num_total_obs_dirstd >= 0.05:
                    daily_flag_dirstd = 'E'
                elif (num_estimated_obs_dirstd + num_missing_obs_dirstd + num_questionable_obs_dirstd) <= 0.05:
                    daily_flag_dirstd = 'A'
                else:
                    daily_flag_dirstd = 'Q'

                # compute the mean daily wind speed -- just the mean of what is given...
                daily_spd_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['spd_val'] if x != 'None'])/num_valid_obs_spd),3)

                # compute the daily resultant--- this one is a true joy.
                
                ## This is a working example using [2, 2, 2, 4, 4] for speed and [10, 10, 10, 350, 10] for direction:
                # daily_mag_x_part = (sum([speed * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(speedlist,dirlist) if speed != 'None' and x != 'None'])/5.)**2
                # daily_mag_y_part = (sum([speed * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(speedlist,dirlist) if speed != 'None' and x != 'None'])/5.)**2
                # math.sqrt(daily_mag_y_part + daily_mag_x_part)
                # >> Returns: 2.7653239207215683

                daily_mag_y_part = (sum([speed * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'], self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)**2  

                daily_mag_x_part = (sum([speed * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'],self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)**2 

                daily_mag_results = sqrt(daily_mag_y_part + daily_mag_x_part)

                # compute the mean of the daily observations of degrees-- must be done with RADIANS - not including the missing, questionable, or estimated ones
                daily_dir_valid_obs = round(math.degrees(math.atan((float(sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/float(sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])))))),3)

                # compute the standard deviation of the daily wind directions -- yamartino method:
                # see this: http://en.wikipedia.org/wiki/Yamartino_method for details

                daily_epsilon = math.sqrt(1-((sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/num_valid_obs_dir)**2 + (sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/num_valid_obs_dir)**2))

                daily_sigma_theta = math.asin((1+(2./math.sqrt(3))-1)*daily_epsilon)

                # daily_dirstd_valid_obs = round(math.degrees(math.atan((float(sum([math.sin(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])/float(sum([math.cos(math.radians(float(x))) for x in self.od[probe_code][each_date]['dir_val'] if x != 'None'])))))),3)

                # get the max of those observations (mean speed)
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['spd_val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum - it will be controlled re. flags by the control on max_valid_obs
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['spd_val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['mean_val']):
                        print index, j

                # get the flag of that maximum - which again, is controlled via the max_valid_obs
                max_flag = [self.od[probe_code][each_date]['spd_fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['spd_val']) if j != "None" and round(float(j),3) == max_valid_obs]

                # no minimum is computed
                if max_flag[0] =="":
                    max_flag[0] = "A"
                else:
                    pass

                newrow = ['MS043',self.entity, site_code, method_code, height, "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), daily_spd_valid_obs, daily_flag_spd, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_mag_results, daily_flag_mag, daily_dir_valid_obs, daily_flag_dir, daily_sigma_theta, daily_flag_dirstd, "EVENT_CODE"]

                print newrow
                my_new_rows.append(newrow)
    
        return my_new_rows

if __name__ == "__main__":

    print "you are running a test loop"

    # let's get some air temperature

    ##### HERE IS HOW WE DO AIRTEMP #######

    A = HeaderWriter('AIRTEMP')
    AT = AirTemperature('2015-04-01 00:00:00', '2015-04-05 00:00:00')
    my_new_rows = AT.condense_data()

    # open a test file for writing
    with open(A.filename,'wb') as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")

        myHeader = A.write_header_template()
        print myHeader
        writer.writerow(myHeader)

        for row in my_new_rows:
            writer.writerow(row)

    del A, my_new_rows, AT

    #### HERE IS HOW WE DO RELHUM #####

    R = HeaderWriter('RELHUM')
    REL = RelHum('2015-03-01 00:00:00', '2015-03-15 00:00:00')

    my_new_rows2 = REL.condense_data()

    with open(R.filename,'wb') as writefile2:
        writer2 = csv.writer(writefile2, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")


        myHeader = R.write_header_template()
        print myHeader
        writer2.writerow(myHeader)

        for row in my_new_rows2:
            writer2.writerow(row)

    del R, my_new_rows2, REL

    #### HERE IS HOW WE DO DEWPOINT #####

    D = HeaderWriter('DEWPT')
    DW = DewPoint('2015-03-01 00:00:00', '2015-03-15 00:00:00')

    my_new_rows3 = DW.condense_data()

    with open(D.filename,'wb') as writefile3:
        writer3 = csv.writer(writefile3, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")


        myHeader = D.write_header_template()
        print myHeader
        writer3.writerow(myHeader)

        for row in my_new_rows3:
            writer3.writerow(row)

    del D, my_new_rows3, DW

    #### HERE IS HOW WE DO VPD #####

    V = HeaderWriter('VPD')
    VAP = VPD('2015-03-01 00:00:00', '2015-03-15 00:00:00')

    my_new_rows4 = VAP.condense_data()

    with open(V.filename,'wb') as writefile4:
        writer4 = csv.writer(writefile4, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")


        myHeader = V.write_header_template()
        print myHeader
        writer4.writerow(myHeader)

        for row in my_new_rows4:
            writer4.writerow(row)

    del V, my_new_rows4, VAP

    #### HERE IS HOW WE DO SOILTEMP #####

    ST = HeaderWriter('SOILTEMP')
    STEMP = SoilTemperature('2015-03-01 00:00:00', '2015-03-15 00:00:00')

    my_new_rows5 = STEMP.condense_data()

    with open(ST.filename,'wb') as writefile5:
        writer5 = csv.writer(writefile5, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")


        myHeader = ST.write_header_template()
        print myHeader
        writer5.writerow(myHeader)

        for row in my_new_rows5:
            writer5.writerow(row)

    del ST, my_new_rows5, STEMP

    #### HERE IS HOW WE DO SOILWC #####

    SW = HeaderWriter('SOILWC')
    SWC = SoilWaterContent('2015-03-01 00:00:00', '2015-03-15 00:00:00')

    my_new_rows6 = SWC.condense_data()

    with open(SW.filename,'wb') as writefile6:
        writer6 = csv.writer(writefile6, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")


        myHeader = SW.write_header_template()
        print myHeader
        writer6.writerow(myHeader)

        for row in my_new_rows6:
            writer6.writerow(row)

    del SW, my_new_rows6, SWC


    #### HERE IS HOW WE DO PRECIP #####

    P = HeaderWriter('PRECIP')
    PRE = Precipitation('2015-03-01 00:00:00', '2015-03-15 00:00:00')

    my_new_rows7 = PRE.condense_data()

    with open(P.filename,'wb') as writefile7:
        writer7 = csv.writer(writefile7, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")


        myHeader = P.write_header_template()
        print myHeader
        writer7.writerow(myHeader)

        for row in my_new_rows7:
            writer7.writerow(row)

    del P, my_new_rows7, PRE

    #### HERE IS HOW WE DO SOLAR #####

    SL = HeaderWriter('SOLAR')
    SOL = Solar('2015-03-01 00:00:00', '2015-03-15 00:00:00')

    my_new_rows8 = SOL.condense_data()

    with open(SL.filename,'wb') as writefile8:
        writer8 = csv.writer(writefile8, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")


        myHeader = SL.write_header_template()
        print myHeader
        writer8.writerow(myHeader)

        for row in my_new_rows8:
            writer8.writerow(row)

    del SL, my_new_rows8, SOL

    with open('README.md','wb') as otherwritefile:
        otherwritefile.write(" THIS IS OUR NEW TOOL FOR UPDATING FSDBDATA \r \
    ===========================\n\
    This tool will ultimately serve to bridge the gap between Hans' program and our database.\r \
    Current methods are up for Air Temperature, Relative Humidity, Dew Point, VPD, Soil Temp, SoilMoisture, and Precip.\n")