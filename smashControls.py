import form_connection as fc
import pymssql
import csv
import datetime
import math
import yaml
import numpy as np
import itertools
import smashWorkers

             
class Worker(object):
    ''' The smash controls "Workers" are responsible for using the attribute arguement to get the correct data from the servers. They can also generate a header, if desired. '''

    def __init__(self, attribute, startdate, enddate, server, *args):

        self.HeaderWriter = smashWorkers.HeaderWriter(attribute)
        self.attribute = attribute

        if args:
            self.probe_code = args[0]
        else:
            pass

        if self.attribute == "AIRTEMP":
            self.Worker = smashWorkers.AirTemperature(startdate, enddate, server, *args)

        elif self.attribute == "LYS":
            self.Worker = smashWorkers.SnowLysimeter(startdate, enddate, server, *args)

        elif self.attribute == "RELHUM":
            self.Worker = smashWorkers.RelHum(startdate, enddate, server, *args)

        elif self.attribute == "DEWPT":
            self.Worker = smashWorkers.DewPoint(startdate, enddate, server, *args)

            self.backupAir = smashWorkers.AirTemperature(startdate, enddate, server, *args)
            air_rows = self.backupAir.condense_data()
            self.backupRel = smashWorkers.RelHum(startdate, enddate, server, *args)
            rel_rows = self.backupRel.condense_data()

            self.bad_dates = []
            for index, details in air_rows:
                if details[9] == "M":
                    bad_dates.append(details[7])

            for index2, details2 in rel_rows:
                if details[9] == "M" and details[7] not in bad_dates:
                    bad_dates.append(details[7])

        elif self.attribute == "VPD":
            self.Worker = smashWorkers.VPD(startdate, enddate, server, *args)

        elif self.attribute == "PAR":
            self.Worker = smashWorkers.PhotosyntheticRad(startdate, enddate, server, *args)

        elif self.attribute == "SOILTEMP":
            self.Worker = smashWorkers.SoilTemperature(startdate, enddate, server, *args)

        elif self.attribute == "SOILWC":
            self.Worker = smashWorkers.SoilWaterContent(startdate, enddate, server, *args)

        elif self.attribute == "WSPD_PRO":
            self.Worker = smashWorkers.Wind(startdate, enddate, server, *args)

        elif self.attribute == "PRECIP":
            self.Worker = smashWorkers.Precipitation(startdate, enddate, server, *args)

        elif self.attribute == "SOLAR":
            self.Worker = smashWorkers.Solar(startdate, enddate, server, *args)

        elif self.attribute == "NR":
            self.Worker = smashWorkers.NetRadiometer(startdate, enddate, server, *args)
        
        else:
            pass

class VaporControl(object):

    def __init__(self, attribute, startdate, enddate, server, *args):

        # create air temperature and relative humidity
        self.A = smashWorkers.AirTemperature(startdate, enddate, server, *args)
        self.R = smashWorkers.RelHum(startdate, enddate, server, *args)

        air_rows = self.A.condense_data()
        rel_rows = self.R.condense_data()

        # create a list of "bad dates"
        self.bad_dates = []
        for index, details in air_rows:
            if details[9] == "M":
                bad_dates.append(details[7])

        for index2, details2 in rel_rows:
            if details[9] == "M" and details[7] not in bad_dates:
                bad_dates.append(details[7])

        # load a lookup table
        if not args:
            with open('CONFIG.yaml','rb') as readfile:
                self.cfg = yaml.load(readfile)

        elif args:
            with open(args[0], 'rb') as readfile:
                self.cfg = yaml.load(readfile)

    def compute_shared_probes(self):
        """ determine if two of the probes share names/times so we can compute a vapor pressure deficit or dewpoint"""
        # happy data is our output!
        happy_data = []

        # get the 3rd to end of each, which is the shared probe code sans the front part of name
        overlaps = [x[3:] for x in self.R.od.keys() if x[3:] in [y[3:] for y in self.A.od.keys()]]
        
        # these will come back in the same order so we can index on them
        Over_Rel = ['REL'+ x for x in overlaps]
        Over_Air = ['AIR'+ x for x in overlaps]

        # zip together the probes you want to look at together
        iProbes = itertools.izip(Over_Rel, Over_Air)

        # iterate over the zipped list of matching probes
        for x,y in iProbes:

            # calculate the height
            height = self.R.heightcalc(x)
            # write the probe code
            probe_code = 'VPD' + x[3:]
            # write the site code
            site_code = x[3:6].lower() +'met'
            
            # write the method code
            try:
                method_code = self.cfg[site_code][probe_code]
            except KeyError:
                method_code = "VPD999"

            # iterate over the shared days
            for each_key in sorted(B.od[x].keys()):

                # if either of the days is a "bad day", write a null row, append it to the happy_data, and continue on
                if each_key in self.bad_dates:
                    new_row = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_key,'%Y-%m-%d %H:%M:%S'),None, "M", None, "M", "None", None, "M", "None", "NA", "MS04318"]
                    happy_data.append(new_row)
                    continue

                # get all the values which are not none 
                check_flags1 =  [val for val in A.od[y][each_key]['val'] if val != "None"]
                check_flags2 =  [val for val in B.od[x][each_key]['val'] if val != "None"]

                # get all the values who have impossible flags
                check_flags_impossible=  len([val for val in A.od[y][each_key]['fval'] if val == "M" or val == "I"])/len(A.od[y][each_key]['fval'])
                check_flags_impossible2=  len([val for val in B.od[x][each_key]['fval'] if val == "M" or val == "I"])/len(B.od[x][each_key]['fval'])

                # get all the values who have questionable flags
                check_flags_questionable=  len([val for val in A.od[y][each_key]['fval'] if val == "Q" or val == "O"])/len(A.od[y][each_key]['fval'])
                check_flags_questionable2=  len([val for val in B.od[x][each_key]['fval'] if val == "Q" or val == "O"])/len(B.od[x][each_key]['fval'])

                # get all the values who have estimated flags
                check_flags_estimated=  len([val for val in A.od[y][each_key]['fval'] if val == "E"])/len(A.od[y][each_key]['fval'])
                check_flags_estimated2=  len([val for val in B.od[x][each_key]['fval'] if val == "E"])/len(B.od[x][each_key]['fval'])
                
                # if the number of impossible flags/missing flags / total number of flags > 20 % then the day is missing and all the attributes are nones
                if len(check_flags1)/len(A.od[y][each_key]['val']) > 0.2 or len(check_flags2)/len(B.od[x][each_key]['val']) > 0.2:
                    
                    # new outputs
                    mean_vpd = None
                    vpd_flag = "M"
                    max_vpd = None
                    max_flag = "M"
                    max_time = "None"
                    min_time = "None"
                    min_flag = "M"
                    min_vpd = None
                

                elif check_flags_impossible > 0.2 or check_flags_impossible2 > 0.2:

                    # new outputs
                    mean_vpd = None
                    vpd_flag = "M"
                    max_vpd = None
                    max_flag = "M"
                    max_time = "None"
                    min_time = "None"
                    min_flag = "M"
                    min_vpd = None

                # if the proportion of days that are queationable is > 5 % then the day is questionable, but still do the analysis
                elif check_flags_questionable > 0.05 or check_flags_questionable2 > 0.05:

                    # new outputs
                    vpd_flag = "Q"
                    max_flag = "Q"
                    min_flag = "Q"

                # if the day's estimated values > 5 % then its estimated, but stll do the analysis
                elif check_flags_estimated > 0.05 or check_flags_estimated2 > 0.05:

                    # new outputs
                    vpd_flag = "E"
                    max_flag = "E"
                    min_flag = "E"

                # if the Q + E + M > 5 % then it's questionable, but still do the analysis
                elif check_flags_estimated + check_flags_questionable + check_flags_impossible > 0.05 or check_flags_estimated2 + check_flags_questionable2 + check_flags_impossible2 > 0.05:

                    vpd_flag = "Q"
                    max_flag = "Q"
                    min_flag = "Q"

                # in all other cases it's ok!
                else:
                    # get the daily airtemp values, daily relhum values, and daily times
                    pre_sample = zip(A.od[y][each_key]['val'], B.od[x][each_key]['val'], A.od[y][each_key]['timekeep'])
                    
                    # zip em together in a tuple
                    good_sample = [tup for tup in pre_sample if 'None' not in tup]
                    
                    # break out each part to make teh vpd calculations easier to understand 
                    sample_at = [float(val) for (val,_,_) in good_sample]
                    sample_rh = [float(val) for (_,val,_) in good_sample]
                    sample_dates = [val for (_,_,val) in good_sample]
                    
                    # the days satvp - a function of air temp
                    sample_SatVP = [6.1094*math.exp(17.625*(float(AT))/(243.04+float(AT))) for AT in sample_at]

                    # the days dewpt - a function of rel adn satvp
                    sample_Td = [237.3*math.log(SatVP*float(RH)/611.)/(7.5*math.log(10)-math.log(SatVP*float(RH)/611.)) for SatVP, RH in itertools.izip(sample_SatVP, sample_rh)]

                    # the days vpd - a function of rel and satvp
                    sample_vpd = [((100-float(RH))*0.01)*SatVP for SatVP, RH in itertools.izip(sample_SatVP, sample_rh)]

                    # determine the daily vpd -- we've gotten rid of the crappy values 
                    mean_vpd = sum(sample_vpd)/len(sample_vpd)

                    # determine the max vpd - no crappy values
                    max_vpd = max(sample_vpd)
                    index_of_max = sample_vpd.index(max(sample_vpd))
                    max_time = datetime.datetime.strftime(sample_times[index_of_max], '%H:%M')
                   
                    
                    # determine the min vpd - no crappy values
                    min_vpd = min(sample_vpd)
                    index_of_min = sample_vpd.index(min(sample_vpd))
                    min_time = datetime.datetime.strftime(sample_times[index_of_min], '%H:%M')
                    
                    vpdflag = "A"
                    maxflag = "A"
                    minflag = "A"

                # the new row should be clean
                new_row = ['MS043',8, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_key,'%Y-%m-%d %H:%M:%S'),mean_vpd, vpdflag, max_vpd, max_flag, max_time, min_vpd, min_flag, min_time, "NA", "MS04318"]

                happy_data.append(new_row)
                
        return happy_data
                

class DBControl(object):
    """ used to generate a list of attributes that needs updating and the date of last update """
    def __init__(self, server):
        
        self.server = server
        self.cursor = fc.form_connection(server)
        
        # until we switch to LTERLogger_pro use this one:

        self.daily_table_list = ['MS04301','MS04302','MS04303','MS04304','MS04305','MS04307','MS04308','MS04309','MS04321','MS04322','MS04323','MS04324']

        self.hr_table_list = ['MS04311','MS04312','MS04313','MS04314','MS04315','MS04317','MS04318','MS04319','MS04331','MS04332','MS04333','MS04334']

        #self.daily_table_list = ['MS04301','MS04302','MS04303','MS04304','MS04305','MS04307','MS04308','MS04309','MS04321','MS04322','MS04323','MS04324','MS04325']
        
        #self.hr_table_list = ['MS04311','MS04312','MS04313','MS04314','MS04315','MS04317','MS04318','MS04319','MS04331','MS04332','MS04333','MS04334','MS04335']

        self.lookup = {}

    def build_queries(self):
        """ check the existing dates and output last values for each table"""
        

        # zip together the two tables
        iTables = itertools.izip(self.daily_table_list, self.hr_table_list)

        if self.server == "SHELDON":

            # for the two tables, check the last input value
            for daily_table, hr_table in iTables:

                last_daily = self.cursor.execute("select top 1 date from LTERLogger_new.dbo." + daily_table + " order by date desc")

                # get the day of that value
                for row in self.cursor:
                    daily = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
                    
                    converted_d = datetime.datetime(daily.year, daily.month, daily.day)

                # checking the last on high res, in theory it should be after the daily
                last_hr = self.cursor.execute("select top 1 date_time from LTERLogger_new.dbo." + hr_table + " order by date_time desc")


                for row in self.cursor:
                    high_res = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

                    converted_hr = datetime.datetime(high_res.year, high_res.month, high_res.day)

                # no matter which database, we want to compare to be sure that the converted hr comes after the daily so we can see how much to grab

                if converted_hr > converted_d:

                    # add a day to the last daily record for where we will start from 
                    daily_data_plus_one = converted_d + datetime.timedelta(days = 1)

                    # the start date is the first day we have still in our daily data plus one
                    startdate = datetime.datetime.strftime(daily_data_plus_one, '%Y-%m-%d %H:%M:%S')

                    # the end date is the last complete day in our high res data - we stop at the zeroeth hour- because remember on the last go-round we ended with < the zeroth hour, so it will be ok.
                    enddate = datetime.datetime.strftime(converted_hr, '%Y-%m-%d %H:%M:%S')

                    if daily_table not in self.lookup:
                        self.lookup[daily_table] = {'startdate': startdate, 'enddate': enddate}
                    elif daily_table in self.lookup:
                        print 'this table is already in the lookup'

                elif converted_hr <= converted_d:

                    print(daily_table + " is already up to date")

        elif self.server == "STEWARTIA":

            # for the two tables, check the last input value
            for daily_table, hr_table in iTables:


                last_daily = self.cursor.execute("select top 1 date from FSDBDATA.dbo." + daily_table + " order by date desc")

                # get the day of that value
                for row in self.cursor:

                    try:
                        daily = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        daily = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d')
                    
                    converted_d = datetime.datetime(daily.year, daily.month, daily.day)

                # checking the last on high res, in theory it should be after the daily
                last_hr = self.cursor.execute("select top 1 date_time from FSDBDATA.dbo." + hr_table + " order by date_time desc")


                for row in self.cursor:
                    high_res = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S')

                    converted_hr = datetime.datetime(high_res.year, high_res.month, high_res.day)


                # no matter which database, we want to compare to be sure that the converted hr comes after the daily so we can see how much to grab

                if converted_hr > converted_d:

                    # add a day to the last daily record for where we will start from 
                    daily_data_plus_one = converted_d + datetime.timedelta(days = 1)

                    # the start date is the first day we have still in our daily data plus one
                    startdate = datetime.datetime.strftime(daily_data_plus_one, '%Y-%m-%d %H:%M:%S')

                    # the end date is the last complete day in our high res data - we stop at the zeroeth hour- because remember on the last go-round we ended with < the zeroth hour, so it will be ok.
                    enddate = datetime.datetime.strftime(converted_hr, '%Y-%m-%d %H:%M:%S')

                    if daily_table not in self.lookup:
                        self.lookup[daily_table] = {'startdate': startdate, 'enddate': enddate}
                    elif daily_table in self.lookup:
                        print 'this table is already in the lookup'

                elif converted_hr <= converted_d:

                    pass

    def check_out_one_attribute(self, attribute):

        if attribute == "AIRTEMP":
            startdate_out = self.lookup['MS04301']['startdate']
            enddate_out = self.lookup['MS04301']['enddate']
        elif attribute == "RELHUM":
            startdate_out = self.lookup['MS04302']['startdate']
            enddate_out = self.lookup['MS04302']['enddate']
        elif attribute == "PRECIP":
            startdate_out = self.lookup['MS04303']['startdate']
            enddate_out = self.lookup['MS04303']['enddate']
        elif attribute == "WSPD_PRO":
            startdate_out = self.lookup['MS04304']['startdate']
            enddate_out = self.lookup['MS04304']['enddate']
        elif attribute == "SOLAR":
            startdate_out = self.lookup['MS04305']['startdate']
            enddate_out = self.lookup['MS04305']['enddate']
        elif attribute == "LYS":
            startdate_out = self.lookup['MS04309']['startdate']
            enddate_out = self.lookup['MS04309']['enddate']
        elif attribute == "NR":
            startdate_out = self.lookup['MS04325']['startdate']
            enddate_out = self.lookup['MS04325']['enddate']
        elif attribute == "VPD":
            startdate_out = self.lookup['MS04307']['startdate']
            enddate_out = self.lookup['MS04307']['enddate']
        elif attribute == "DEWPT":
            startdate_out = self.lookup['MS04308']['startdate']
            enddate_out = self.lookup['MS04308']['enddate']
        elif attribute == "WSPD_SNC":
            startdate_out = self.lookup['MS04324']['startdate']
            enddate_out = self.lookup['MS04324']['enddate']
        elif attribute == "PAR":
            startdate_out = self.lookup['MS04322']['startdate']
            enddate_out = self.lookup['MS04322']['enddate']
        elif attribute == "SOILWC":
            startdate_out = self.lookup['MS04323']['startdate']
            enddate_out = self.lookup['MS04323']['enddate']
        elif attribute == "SOILTEMP":
            startdate_out = self.lookup['MS04321']['startdate']
            enddate_out = self.lookup['MS04321']['enddate']

        return startdate_out, enddate_out