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

    def compute_shared_probes(self):
        """ determine if two of the probes share names/times so we can compute a vapor pressure deficit or dewpoint"""

        # get the 3rd to end of each, which is the shared probe code sans the front part of name
        overlaps = [x[3:] for x in self.R.od.keys() if x[3:] in [y[3:] for y in self.A.od.keys()]]

        # these will come back in the same order so we can index on them
        Over_Rel = ['REL'+ x for x in overlaps]
        Over_Air = ['AIR'+ x for x in overlaps]


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