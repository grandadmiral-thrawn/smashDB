#import smashControls
import pymssql
import math
import datetime
import csv
import yaml

class HeaderWriter(object):

    def __init__(self, attribute):
    
        daily_attr = {'ATM': '26',
                'NR': '25',
                'WSPD_SNC': '24',
                'SOILWC':'23',
                'PAR':'22',
                'SOILTEMP':'21',
                'SNOWMELT':'MSO4309',
                'VPD':'08',
                'DEWPT':'07',
                'SOLAR': '05',
                'WSPD_PRO': '04',
                'PRECIP': '03',
                'RELHUM':'02',
                'AIRTEMP':'01',}

        dbcode = 'MS043'
        
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

        self.filename = dbcode + daily_attr[attribute] + '_test.csv'


    def isdirty(self):

        if self.attribute == "SOILMP" or self.attribute == "SOILTEMP":
            height_word = "DEPTH"
        else:
            height_word = "HEIGHT"
        return height_word

    def write_header_template(self):

        if self.attribute == "AIRTEMP" or self.attribute == "RELHUM" or self.attribute == "SOILWC" or self.attribute == "DEWPT" or self.attribute == "VPD" or self.attribute == "SOILTEMP":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, self.min_method, self.mintime_method, self.min_flag_method, "EVENT_CODE"]

        elif self.attribute == "PRECIP":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "PRECIP_TOT_DAY", "PRECIP_TOT_FLAG", "EVENT_CODE"]

        elif self.attribute == "WSPD_PRO":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "WMAG_PRO_MEAN_DAY", "WMAG_PRO_MEAN_FLAG", "WDIR_PRO_MEAN_DAY", "WDIR_PRO_MEAN_FLAG", "WDIR_PRO_STDDEV_DAY", "WDIR_PRO_STDDEV_FLAG", "EVENT_CODE"]

        elif self.attribute == "WSPD_SNC":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "SNCBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "WMAG_SNC_MEAN_DAY", "WMAG_SNC_MEAN_FLAG", "WDIR_SNC_MEAN_DAY", "WDIR_SNC_MEAN_FLAG", "WDIR_SNC_STDDEV_DAY", "WDIR_SNC_STDDEV_FLAG", "WUX_SNC_MEAN_DAY", "WUX_SNC_MEAN_FLAG", "WUX_SNC_STDDEV_DAY", "WUX_SNC_STDDEV_DAY_FLAG","WUY_SNC_MEAN_DAY", "WUY_SNC_MEAN_FLAG", "WUY_SNC_STDDEV_DAY", "WUY_SNC_STDDEV_DAY_FLAG", "WAIR_SNC_MEAN_DAY", "WAIR_SNC_MEAN_FLAG", "WAIR_SNC_STDDEV_DAY", "WAIR_SNC_STDDEV_FLAG",  "EVENT_CODE"]

        elif self.attribute == "NR":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "SNCBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "SW_IN_MEAN_DAY", "SW_IN_MEAN_FLAG", "SW_OUT_MEAN_DAY", "SW_OUT_MEAN_FLAG", "LW_IN_MEAN_DAY", "LW_IN_MEAN_FLAG", "LW_OUT_MEAN_DAY", "LW_OUT_MEAN_FLAG", "NR_TOT_MEAN_DAY", "NR_TOT_MEAN_FLAG", "SENSOR_TEMP_DAY", "SENSOR_TEMP_FLAG", "EVENT_CODE"]

        elif self.attribute == "SOLAR":
            
            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "SOLAR_TOT_DAY", "SOLAR_TOT_FLAG", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "EVENT_CODE"]

        elif self.attribute == "PAR":
            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "EVENT_CODE"]

        elif self.attribute == "SNOWMELT":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "SNOWMELT_TOT_DAY", "SNOWMELT_TOT_FLAG", "EVENT_CODE"]

        return header

class AirTemperature(object):

    def __init__(self, startdate, enddate):

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
        
        """ queries the data base and returns the cursor after population"""

        startdate = datetime.datetime.strftime(self.startdate,'%Y-%m-%d %H:%M:%S')
        enddate = datetime.datetime.strftime(self.enddate,'%Y-%m-%d %H:%M:%S')

        query = "SELECT DATE_TIME, PROBE_CODE, AIRTEMP_MEAN, AIRTEMP_MEAN_FLAG from LTERLogger_new.dbo.MS04311 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME <= \'" + enddate + "\' ORDER BY DATE_TIME ASC"
        
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

                if num_total_obs != 288 or num_total_obs != 24 or num_total_obs != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                else:
                    pass

                # get the number of each flag
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])

                # daily flag: 
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.5:
                    daily_flag = 'E'
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of those observations
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)

                # get the max of those observations
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                
                # get the time of that maximum
                max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if round(float(j),3) == max_valid_obs]

                # get the flag of that maximum
                max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if round(float(j),3) == max_valid_obs]


                # get the min of those observations
                min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                # get the time of that minimum
                min_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if round(float(j),3) == min_valid_obs]

                # get the time of that minimum
                min_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if round(float(j),3) == min_valid_obs]

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


        query = "SELECT DATE_TIME, PROBE_CODE, RELHUM_MEAN, RELHUM_MEAN_FLAG from LTERLogger_new.dbo.MS04312 WHERE DATE_TIME >= \'" + startdate + "\' AND DATE_TIME <= \'" + enddate + "\' ORDER BY DATE_TIME ASC"

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

                if num_total_obs != 288 or num_total_obs != 24 or num_total_obs != 96: 
                    print("the total number of observations on %s is %s") %(each_date, num_total_obs)
                else:
                    pass

                # get the number of each flag
                num_missing_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'M' or x == 'I'])
                num_questionable_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'Q' or x == 'O'])
                num_estimated_obs = len([x for x in self.od[probe_code][each_date]['fval'] if x == 'E'])

                # daily flag: 
                if num_missing_obs/num_total_obs >= 0.2:
                    daily_flag = 'M'
                elif (num_missing_obs + num_questionable_obs)/num_total_obs >= 0.05:
                    daily_flag = 'Q'
                elif (num_estimated_obs)/num_total_obs >= 0.5:
                    daily_flag = 'E'
                elif (num_estimated_obs + num_missing_obs + num_questionable_obs) <= 0.05:
                    daily_flag = 'A'
                else:
                    daily_flag = 'Q'

                # take the mean of those observations
                mean_valid_obs = round(float(sum([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None'])/num_valid_obs),3)

                # get the max of those observations
                max_valid_obs = round(max([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                try:
                
                    # get the time of that maximum
                    max_valid_time = [self.od[probe_code][each_date]['timekeep'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]

                except Exception:

                    for index,j in enumerate(self.od[probe_code][each_date]['val']):
                        print index, j

                # get the flag of that maximum
                max_flag = [self.od[probe_code][each_date]['fval'][index] for index, j in enumerate(self.od[probe_code][each_date]['val']) if j != "None" and round(float(j),3) == max_valid_obs]


                # get the min of those observations
                min_valid_obs = round(min([float(x) for x in self.od[probe_code][each_date]['val'] if x != 'None']),3)

                # get the time of that minimum
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


if __name__ == "__main__":

    print "you are running a test loop"

    # let's get some air temperature

    A = HeaderWriter('AIRTEMP')
    AT = AirTemperature('2015-04-01 00:00:00', '2015-04-05 00:00:00')

    #od1 = AT.attack_data()
    #AT.od = od1
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

    R = HeaderWriter('RELHUM')
    REL = RelHum('2015-03-01 00:00:00', '2015-03-15 00:00:00')

    my_new_rows2 = REL.condense_data()

    with open('wootwootcsv.csv','wb') as writefile2:
        writer2 = csv.writer(writefile2, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")


        myHeader = R.write_header_template()
        print myHeader
        writer2.writerow(myHeader)

        for row in my_new_rows2:
            writer2.writerow(row)

    with open('README.md','wb') as otherwritefile:
        otherwritefile.write(
            """ 
            THIS IS OUR NEW TOOL FOR UPDATING FSDBDATA
            ===========================

            This tool will ultimately serve to bridge the gap between Hans' program and our database. 
                """)