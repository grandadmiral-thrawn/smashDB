import form_connection as fc
import pymssql
import csv
import datetime
import math
import numpy as np
import itertools
import smashWorkers


class MethodControl(object):
    """ used to generate a date range from the method table and then check the database that every item it it agrees"""
    def __init__(self, server):

        # connect to the right server
        self.cursor = fc.form_connection('SHELDON')
        self.cursor2 = fc.form_connection('STEWARTIA')
        
        self.server = server
        self.stations = ['CENMET','PRIMET','VANMET','UPLMET','VARMET','CS2MET','H15MET']

        ## COMMENT THIS IN FOR MS043: look up  table for the daily method
        self.lu = {'AIR':'MS04301', 'REL': 'MS04302', 'DEW': 'MS04307', 'VPD': 'MS04308','RAD': 'MS04305', 'SOI': 'MS04321', 'PAR': 'MS04322', 'WND': 'MS04304', 'PPT': 'MS04303', 'SWC': 'MS04323', 'LYS':'MS04309', 'SNO':'MS04310'}

        # look up  table for the daily method
        #self.lu = {'AIR':'MS00101', 'REL': 'MS00102', 'DEW': 'MS00107', 'VPD': 'MS00108','RAD': 'MS00105', 'SOI': 'MS00121', 'PAR': 'MS00122', 'WND': 'MS00104', 'PPT': 'MS00103', 'SWC': 'MS00123', 'LYS':'MS00109', 'SNO':'MS00110'}

        if self.server == "SHELDON":
            self.lu = {'AIR':'MS00101', 'REL': 'MS00102', 'DEW': 'MS00107', 'VPD': 'MS00108','RAD': 'MS00105', 'SOI': 'MS04321', 'PAR': 'MS00122', 'WND': 'MS00104', 'PPT': 'MS00103', 'SWC': 'MS00123', 'LYS':'MS00109', 'SNO':'MS00110'}
        else:
            pass

        ## COMMENT THIS IN FOR MS001:  name of the method column
        self.special = {'MS04301': 'AIRTEMP_METHOD', 'MS04302': 'RELHUM_METHOD', 'MS04307':'DEWPT_METHOD', 'MS04308': 'VPD_METHOD','MS04305': 'SOLAR_METHOD', 'MS04325': 'SOLAR_METHOD', 'MS04321': 'SOILTEMP_METHOD', 'MS04322':'PAR_METHOD', 'MS04304':'WIND_METHOD', 'MS04303': 'PRECIP_METHOD', 'MS04323': 'SOILWC_METHOD', 'MS04309':'SNOWMELT_METHOD','MS04324': 'WIND_METHOD', 'MS04310':'SNOW_METHOD'}

        #self.special = {'MS00101': 'AIRTEMP_METHOD', 'MS00102': 'RELHUM_METHOD', 'MS00107':'DEWPT_METHOD', 'MS00108': 'VPD_METHOD','MS00105': 'SOLAR_METHOD', 'MS00125': 'SOLAR_METHOD', 'MS00121': 'SOILTEMP_METHOD', 'MS00122':'PAR_METHOD', 'MS00104':'WIND_METHOD', 'MS00103': 'PRECIP_METHOD', 'MS00123': 'SOILWC_METHOD', 'MS00109':'SNOWMELT_METHOD','MS00124': 'WIND_METHOD', 'MS00110':'SNOW_METHOD'}

        #if self.server == "SHELDON":
        #    self.special = {'MS00101': 'AIRTEMP_METHOD', 'MS00102': 'RELHUM_METHOD', 'MS00107':'DEWPT_METHOD', 'MS00108': 'VPD_METHOD','MS00105': 'SOLAR_METHOD', 'MS00125': 'SOLAR_METHOD', 'MS04321': 'SOILTEMP_METHOD', 'MS00122':'PAR_METHOD', 'MS00104':'WIND_METHOD', 'MS00103': 'PRECIP_METHOD', 'MS00123': 'SOILWC_METHOD', 'MS00109':'SNOWMELT_METHOD','MS00124': 'WIND_METHOD', 'MS00110':'SNOW_METHOD'}



    # query database
    def process_db(self):

        # write to the error log if the method is not consistent
        with open('errorlog_ms001.csv', 'wb') as writefile, open('eventlog_ms001.csv','wb') as writefile2, open('heightlog_ms001.csv','wb') as writefile3:

            writer = csv.writer(writefile)
            writer2 = csv.writer(writefile2)
            writer3 = csv.writer(writefile3)
            writer.writerow(['date_in_db','method_in_db', 'method_in_table', 'probe_code', 'date_start_table','date_end_table'])
            writer2.writerow(['probe_code', 'date_start_table','date_end_table', 'current_event_code'])
            writer3.writerow(['height_in_db', 'height_in_table', 'depth_in_table', 'probe_code', 'date_in_db'])


            query = "SELECT probe_code, date_bgn, date_end, method_code, height, depth from LTERLogger_new.dbo.method_history_daily" 
            self.cursor.execute(query)
             
            od = {}

            for row in self.cursor:
                
                # probe code in row 0, beginning of type of attribute in probe code, position (tokened from) 0-3
                probe_code = str(row[0])
                site_code = probe_code[3:6]
                qual = probe_code[0:3]
                method_code = str(row[3])
                height = str(row[4])
                depth = str(row[5])

                # dt1 = startdate, dt2 = enddate
                dt1 = datetime.datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S')   
                dt2 = datetime.datetime.strptime(str(row[2]), '%Y-%m-%d %H:%M:%S')

                # if the probe code is for the sonic wind go to table 24
                if probe_code in ['WNDPRI02', 'WNDVAN02', 'WNDCEN02']:
                    table = 'MS04324'
                    #table = 'MS00124'

                # if the probe code is for net radiometer, go to table 25
                elif probe_code in ['RADPRI02', 'RADVAN02']:
                    table = 'MS04325'
                    #table = 'MS00125'

                # if the qualifier is soil moisture potential or snow depth we don't know how to do it righ tnow
                elif qual == 'SMP' or qual == "SNO":
                    continue

                else:
                    table = self.lu[qual]

                
                # if not listed, list in the output 
                if table not in od:
                    od[table] =[(probe_code, dt1, dt2, method_code, height, depth)]
                elif table in od:
                    od[table].append((probe_code, dt1, dt2, method_code, height, depth))


            for each_key in od.keys():

                # special is the reference method to go to the high resolution method. 
                special = self.special[each_key]

                # for each of the tables, walk over the daily table that corresponds to it, getting the probe codes etc. between the start and end dates. 
                # added, also get event code
                for each_item in od[each_key]:
                    print each_key
                    if self.server == 'STEWARTIA':

                        newquery = "select probe_code, date, " + special + " from fsdbdata.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"

                        self.cursor2.execute(newquery)

                        for row in self.cursor2:

                            # if the method is what is listed, we're probably ok
                            if str(row[2]) == each_item[3]:
                                continue

                            elif str(row[2]) != each_item[3]:
                                nr = [str(row[1]), str(row[2]), each_item[3], each_item[0], datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S'), datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S')]
                                writer.writerow(nr)

                            else: 
                                print "this should not be called ever"
                    
                        # select the event code from the database on the begin date
                        newquery2 = "select event_code from fsdbdata.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date = \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\'"

                        self.cursor2.execute(newquery2)

                        for row in self.cursor2:

                            if str(row[0]) == "METHOD":
                                continue
                            elif str(row[0]) != "METHOD":
                                nr2 = [each_item[0], datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S'), datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S'), str(row[0])]
                                writer2.writerow(nr2)

                        # if each_key == "MS04309":
                        
                        ## COMMENT IN FOR MS00109
                        if each_key == "MS00109" or each_key == "MS04309":
                            continue

                        # if the key word is "height"
                        # if each_key not in ['MS04321', 'MS04323']:

                        ## COMMENT IN FOR MS001:
                        if each_key not in ['MS04321', 'MS00121','MS00123','MS04323']:
                        
                            newquery3 = "select date, height from fsdbdata.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"
                        
                        # if the key word is depth
                        # elif each_key in ['MS04321', 'MS04323']:

                        # COMMENT IN FOR MS001: 
                        elif each_key in ['MS00121', 'MS04321', 'MS00123','MS04323']:
                         
                            newquery3 = "select date, depth from fsdbdata.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"

                        self.cursor2.execute(newquery3)

                        for row in self.cursor2:

                            if str(row[1]) == each_item[4]:
                                continue
                            elif str(row[1]) == each_item[5]:
                                continue

                            elif str(row[1]) != each_item[4] or str(row[1]) != each_item[5]:

                                #nr3 = 'height_in_db', 'height_in_table', 'depth_in_table' 'probe_code', 'date_in_db'
                                nr3 = [str(row[1]), each_item[4], each_item[5], each_item[0], str(row[0])]
                                writer3.writerow(nr3)

                    elif self.server == "SHELDON":

                        newquery = "select probe_code, date, " + special + " from LTERLogger_pro.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') + "\'"
                        
                        print newquery
                        self.cursor.execute(newquery)

                        for row in self.cursor:
                            if str(row[2]) == each_item[3]:
                                continue

                            elif str(row[2]) != each_item[3]:
                                print "method_code of %s is not %s from table for %s" %(str(row[2]), each_item[3], each_item[0])

                            else: 
                                print "this should not be called ever"

                        # select the event code from the database on the begin date
                        newquery2 = "select event_code from LTERLogger_pro.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date = \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\'"

                        print newquery

                        self.cursor.execute(newquery2)

                        for row in self.cursor:

                            if str(row[0]) == "METHOD":
                                continue
                            elif str(row[0]) != "METHOD":
                                nr2 = [each_item[0], datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S'), datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S'), str(row[0])]
                                writer2.writerow(nr2)

                        if each_key == "MS04309":
                            continue

                        # if the key word is "height"
                        if each_key not in ['MS04321', 'MS04323','MS00121', 'MS00123']:

                        
                            newquery3 = "select date, height from LTERLogger_pro.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"
                        
                        # if the key word is depth
                        elif each_key in ['MS04321', 'MS04323','MS00121','MS00123']:
                         
                            newquery3 = "select date, depth from LTERLogger_pro.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"

                        self.cursor2.execute(newquery3)

                        for row in self.cursor2:

                            if str(row[1]) == each_item[4]:
                                continue
                            elif str(row[1]) == each_item[5]:
                                continue

                            elif str(row[1]) != each_item[4] or str(row[1]) != each_item[5]:

                                #nr3 = 'height_in_db', 'height_in_table', 'depth_in_table' 'probe_code', 'date_in_db'
                                nr3 = [str(row[1]), each_item[4], each_item[5], each_item[0], str(row[0])]
                                writer3.writerow(nr3)


class HRMethodControl(object):
    """ used to generate a date range from the method table and then check the database that every item it it agrees"""
    def __init__(self, server):

        # connect to the right server
        self.cursor = fc.form_connection('SHELDON')
        self.cursor2 = fc.form_connection('STEWARTIA')
        self.server = server
        
        ## look up  table for the hr method
        self.lu = {'AIR':'MS04311', 'REL': 'MS04312', 'DEW': 'MS04317', 'VPD': 'MS04318', 'RAD': 'MS04315', 'SOI': 'MS04331', 'PAR': 'MS04332', 'WND': 'MS04314', 'PPT': 'MS04313', 'SWC': 'MS04333', 'LYS':'MS04319'}
        ## COMMENT IN FOR MS001: 
        #self.lu = {'AIR':'MS00111', 'REL': 'MS00112', 'DEW': 'MS00117', 'VPD': 'MS00118', 'RAD': 'MS00115', 'SOI': 'MS00131', 'PAR': 'MS00132', 'WND': 'MS00114', 'PPT': 'MS00113', 'SWC': 'MS00133', 'LYS':'MS00119','ATM': 'MS00136'}

        ##  name of the method column
        self.special = {'MS04311': 'AIRTEMP_METHOD', 'MS04312': 'RELHUM_METHOD', 'MS04317':'DEWPT_METHOD', 'MS04318': 'VPD_METHOD','MS04335': 'SOLAR_METHOD', 'MS04315': 'SOLAR_METHOD', 'MS04331': 'SOILTEMP_METHOD', 'MS04332':'PAR_METHOD', 'MS04314':'WIND_METHOD', 'MS04334':'WIND_METHOD', 'MS04313': 'PRECIP_METHOD', 'MS04333': 'SOILWC_METHOD', 'MS04319':'SNOWMELT_METHOD','MS04334': 'WIND_METHOD'}
        # name of the method column
        #self.special = {'MS00111': 'AIRTEMP_METHOD', 'MS00112': 'RELHUM_METHOD', 'MS00117':'DEWPT_METHOD', 'MS00118': 'VPD_METHOD','MS00135': 'SOLAR_METHOD', 'MS00115': 'SOLAR_METHOD', 'MS00131': 'SOILTEMP_METHOD', 'MS00132':'PAR_METHOD', 'MS00114':'WIND_METHOD', 'MS00134':'WIND_METHOD', 'MS00113': 'PRECIP_METHOD', 'MS00133': 'SOILWC_METHOD', 'MS00119':'SNOWMELT_METHOD','MS00134': 'WIND_METHOD','MS00136': 'ATMPRESS_METHOD'}



    # query database
    def process_db(self):

        # write to the error log if the method is not consistent
        with open('errorlog_hr.csv', 'wb') as writefile, open('eventlog_hr.csv','wb') as writefile2, open('heightlog_hr.csv','wb') as writefile3:

            writer = csv.writer(writefile)
            writer2 = csv.writer(writefile2)
            writer.writerow(['first_date_in_db','method_in_db', 'method_in_table', 'probe_code', 'date_start_table','date_end_table'])
            writer2.writerow(['probe_code', 'date_start_table','date_end_table', 'current_event_code'])
            writer3 = csv.writer(writefile3)
            writer3.writerow(['height_in_db', 'height_in_table', 'probe_code', 'date_in_db'])

            # get the probe code, date surroundings, etc. from the method history

            # FIRST TEST IS FOR THE METHOD CODES BEING THE SAME!
            query = "SELECT probe_code, date_time_bgn, date_time_end, method_code, height, depth from LTERLogger_new.dbo.method_history" 
            print query
            self.cursor.execute(query)

            od = {}

            for row in self.cursor:
                print row
                # probe code in row 0, beginning of type of attribute in probe code, position (tokened from) 0-3
                probe_code = str(row[0])
                qual = probe_code[0:3]
                site_code = probe_code[3:6]
                method_code = str(row[3])
                height = str(row[4])
                depth = str(row[5])

                # dt1 = startdate, dt2 = enddate
                dt1 = datetime.datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S')   
                dt2 = datetime.datetime.strptime(str(row[2]), '%Y-%m-%d %H:%M:%S')

                # if the probe code is for the sonic wind go to table 24
                if probe_code in ['WNDPRI02', 'WNDVAN02', 'WNDCEN02']:
                    # table = 'MS04334'
                    table = 'MS00134'


                # if the probe code is for net radiometer, go to table 25
                elif probe_code in ['RADPRI02', 'RADVAN02']:
                    # table = 'MS04335'
                    table = 'MS00135'

                # if the qualifier is soil moisture potential or snow depth we don't know how to do it righ tnow
                elif qual == 'SMP' or qual == "SNO" or qual == "LYS" or qual=="ATM":
                    continue

                else:
                    table = self.lu[qual]

                # if not listed, list in th eoutput 
                if table not in od:
                    od[table] =[(probe_code, dt1, dt2, method_code, height, depth)]
                elif table in od:
                    od[table].append((probe_code, dt1, dt2, method_code, height, depth))


            for each_key in od.keys():

                # special is the reference method to go to the high resolution method. 
                special = self.special[each_key]

                # for each of the tables, walk over the hr table that corresponds to it, getting the probe codes etc. between the start and end dates. 
                # added, also get event code

                # for each table in the database
                for each_item in od[each_key]:
                    print each_item
                    if self.server == 'STEWARTIA':
                        print special
                        # find the date times in that range -- and the methods, are the the same as what you want?
                        newquery = "select probe_code, date_time, " + special + " from fsdbdata.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date_time >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date_time < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"

                        # execute!
                        self.cursor2.execute(newquery)

                        for row in self.cursor2:

                            # if the method is what is listed, we're probably ok
                            if str(row[2]) == each_item[3]:
                                continue

                            # if the method is not what is listed
                            elif str(row[2]) != each_item[3]:

                                nr = [str(row[1]), str(row[2]), each_item[3], each_item[0], datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S'), datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S')]
                                writer.writerow(nr)

                            else: 
                                print "this should not be called ever"
                    
                        # select the event code from the database on the begin date = we want "method" to be this row
                        newquery2 = "select event_code from fsdbdata.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date_time = \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\'"

                        self.cursor2.execute(newquery2)

                        for row in self.cursor2:

                            if str(row[0]) == "METHOD":
                                continue

                            # if that entry is not "method, write it out to the file"
                            elif str(row[0]) != "METHOD":
                                nr2 = [each_item[0], datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S'), datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S'), str(row[0])]
                                writer2.writerow(nr2)

                        ##  third test for height and depth-- first, skip lysimeter
                        # if each_key == "MS04319":
                        ## COMMENT IN FOR MS001
                        if each_key == "MS00119":
                            continue

                        ## if the key word is "height"
                        # if each_key not in ['MS04331', 'MS04333']:
                        ## COMMENT IN FOR MS001
                        try:
                            if each_key not in ['MS00131', 'MS00133','MS04331','MS04333']:
                                newquery3 = "select date_time, height from fsdbdata.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date_time >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date_time < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"
                            
                            ## if the key word is depth
                            # elif each_key in ['MS04331', 'MS04333']:
                            ## COMMENT IN FOR MS001
                            elif each_key in ['MS00131', 'MS00133','MS04331','MS04333']:

                                newquery3 = "select date_time, depth from fsdbdata.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date_time >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date_time < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"

                            self.cursor2.execute(newquery3)

                            for row in self.cursor2:

                                if str(row[1]) == each_item[4]:
                                    continue
                                elif str(row[1]) == each_item[5]:
                                    continue

                                elif str(row[1]) != each_item[4] or str(row[1]) != each_item[5]:

                                    #nr3 = 'height_in_db', 'height_in_table', 'depth_in_table' 'probe_code', 'date_in_db'
                                    nr3 = [str(row[1]), each_item[4], each_item[5], each_item[0], str(row[0])]
                                    writer3.writerow(nr3)

                        except Exception:

                            continue 

                    elif self.server == "SHELDON":

                        # find the date times in that range -- and the methods, are the the same as what you want?
                        newquery = "select probe_code, date_time, " + special + " from LTERLogger_pro.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date_time >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date_time < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"

                        # execute!
                        self.cursor.execute(newquery)

                        for row in self.cursor:

                            # if the method is what is listed, we're probably ok
                            if str(row[2]) == each_item[3]:
                                continue

                            # if the method is not what is listed
                            elif str(row[2]) != each_item[3]:

                                nr = [str(row[1]), str(row[2]), each_item[3], each_item[0], datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S'), datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S')]
                                writer.writerow(nr)

                            else: 
                                print "this should not be called ever"
                    
                        # select the event code from the database on the begin date = we want "method" to be this row
                        newquery2 = "select event_code from LTERLogger_pro.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date_time = \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\'"

                        self.cursor.execute(newquery)

                        for row in self.cursor2:

                            if str(row[0]) == "METHOD":
                                continue

                            # if that entry is not "method, write it out to the file"
                            elif str(row[0]) != "METHOD":
                                nr2 = [each_item[0], datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S'), datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S'), str(row[0])]
                                writer2.writerow(nr2)

                        # third test for height and depth-- first, skip lysimeter
                        if each_key == "MS04319":
                            continue

                        # if the key word is "height"
                        if each_key not in ['MS04331', 'MS04333']:
                        
                            newquery3 = "select date_time, height from LTERLogger_pro.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date_time >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date_time < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"
                        
                        # if the key word is depth
                        elif each_key in ['MS04331', 'MS04333']:
                         
                            newquery3 = "select date_time, depth from LTERLogger_pro.dbo." + each_key + " where probe_code like \'" + each_item[0] + "\' and date_time >= \'" + datetime.datetime.strftime(each_item[1], '%Y-%m-%d %H:%M:%S') + "\' and date_time < \'" + datetime.datetime.strftime(each_item[2], '%Y-%m-%d %H:%M:%S') +  "\'"

                        self.cursor.execute(newquery3)

                        for row in self.cursor:

                            if str(row[1]) == each_item[4] or str(row[1]) == each_item[5]:
                                continue

                            elif str(row[1]) != each_item[4] or str(row[1]) != each_item[5]:

                                #nr3 = 'height_in_db', 'height_in_table', 'depth_in_table' 'probe_code', 'date_in_db'
                                nr3 = [str(row[1]), each_item[4], each_item[5], each_item[0], str(row[0])]
                                writer3.writerow(nr3)


class DBControl(object):
    """ used to generate a list of attributes that needs updating and the date of last update """
    def __init__(self, server, *args):
        
        self.server = server
        self.cursor = fc.form_connection(server)

        if args and args != []:
            self.station = args[0]
        
        try: 
            ## These lists have MS04335; fits Stewartia and other SHELDON server 
            self.daily_table_list = ['MS04301','MS04302','MS04303','MS04304','MS04305','MS04307','MS04308','MS04309','MS04321','MS04322','MS04323','MS04324','MS04325']
            self.hr_table_list = ['MS04311','MS04312','MS04313','MS04314','MS04315','MS04317','MS04318','MS04319','MS04331','MS04332','MS04333','MS04334','MS04335']

        except Exception:
            try:
                ## These lists don't have MS04335; one of the SHELDON servers doesnt have it on it (new?)
                self.daily_table_list = ['MS04301','MS04302','MS04303','MS04304','MS04305','MS04307','MS04308','MS04309','MS04321','MS04322','MS04323','MS04324']
                self.hr_table_list = ['MS04311','MS04312','MS04313','MS04314','MS04315','MS04317','MS04318','MS04319','MS04331','MS04332','MS04333','MS04334']
            except Exception:
                print "Please check which tables are available using a query into the database.information_schema.tables structure"

        ## Holds a reference of the valid start and end dates based on the most recent update in the db
        self.lookup = {}

    def build_queries(self):
        """ check the existing dates and outputs values for each table. Handles errors like this: (1) checks to be sure that the table isn't already up to date, which would mean an error; skips it if this is the case, (2) tries a few date formats that we've encountered to be sure it's not misreading. Uses a new format if this is the case. (3) if the daily table is blank, it finds the first day in the HR table and uses that to start the daily (rolling forward a day to get the first FULL day). """
        
        # zip together the two tables
        iTables = itertools.izip(self.daily_table_list, self.hr_table_list)
        if self.server == "SHELDON":
            
            # for the two tables, check the most recent input value on both the daily and high resolution tables
            for daily_table, hr_table in iTables:

                self.cursor.execute("select top 1 date from LTERLogger_pro.dbo." + daily_table + " order by date desc")
                returned_value = self.cursor.fetchone()

                # if there's nothing in the daily table, it will return a None to the fetch one query; however, it doesn't return at all using the row iteration method!
                if returned_value != None:

                    try:
                        daily = returned_value[0]
                        
                    except Exception:
                        # This has never been called, but just in case, I am putting this here so we can debug
                        print "some error has been found on :"
                        print returned_value
                        print "entering debug mode : "
                        import pdb; pdb.set_trace()

                    try:
                        converted_d = datetime.datetime(daily.year, daily.month, daily.day)
                    
                    except AttributeError:
                        # ms04325 has only the date without the same seconds format
                        cd1 = datetime.datetime.strptime(daily, '%Y-%m-%d')
                        converted_d = datetime.datetime(cd1.year, cd1.month, cd1.day)

                    # print datetime.datetime.strftime(converted_d, '%Y-%m-%d') + " was found as the last update in " + daily_table 

                elif returned_value == None:

                    # if the daily table is totally blank we'll need to rebuild the whole thing from the hr table
                    self.cursor.execute("select top 1 date_time from LTERLogger_pro.dbo." + hr_table + " order by date_time asc")
                        
                    returned_value = self.cursor.fetchone()

                    try:
                        daily = returned_value[0]

                    except ValueError:
                        print "an error! see: "
                        print returned_value[0]
                        print " entering debug mode : "
                        import pdb; pdb.set_trace()

                    converted_d = datetime.datetime(daily.year, daily.month, daily.day)
                    
                    # print datetime.datetime.strftime(converted_d, '%Y-%m-%d') + " was taken from the HR table to start " + daily_table 
                    
                # because we haven't updated the daily table as recently as the hr table has been updated we can assume that the last_hr value follows after the daily value
                self.cursor.execute("select top 1 date_time from LTERLogger_pro.dbo." + hr_table + " order by date_time desc")

                new_returned_value = self.cursor.fetchone()
                
                high_res = new_returned_value[0]
                
                converted_hr = datetime.datetime(high_res.year, high_res.month, high_res.day)

                if converted_hr > converted_d:

                    # add a day to the last daily record for where we will start from 
                    daily_data_plus_one = converted_d + datetime.timedelta(days = 1)

                    # the new start date is the first day we have in our daily data (plus one)
                    startdate = datetime.datetime.strftime(daily_data_plus_one, '%Y-%m-%d %H:%M:%S')

                    # the new end date is the last complete day in our high res data - we stop at the zeroeth hour- because remember on the last go-round we ended with < the zeroth hour, so it will be ok.
                    enddate = datetime.datetime.strftime(converted_hr, '%Y-%m-%d %H:%M:%S')

                    if daily_table not in self.lookup:

                        self.lookup[daily_table] = {'startdate': startdate, 'enddate': enddate}
                    
                    elif daily_table in self.lookup:

                        print 'Warning : ' + daily_table + ' has already been processed in the lookup'

                elif converted_hr <= converted_d:
                    print(daily_table + " is already up to date with its HR counterpart")
                
                del converted_hr
                del converted_d
                del daily
                del high_res
                del returned_value
                del new_returned_value

        elif self.server == "STEWARTIA":

            # for the two tables, check the most recent input value on both the daily and high resolution tables
            for daily_table, hr_table in iTables:

                self.cursor.execute("select top 1 date from FSDBDATA.dbo." + daily_table + " order by date desc")
                returned_value = self.cursor.fetchone()

                if returned_value != None:

                    try:
                        daily = returned_value[0]
                        
                    except Exception:
                        print "some error has been found on :"
                        print returned_value
                        print "entering debug mode :"
                        import pdb; pdb.set_trace()

                    try:
                        converted_d = datetime.datetime(daily.year, daily.month, daily.day)
                    except AttributeError:
                        # ms04325 has only the date without the same seconds format
                        cd1 = datetime.datetime.strptime(daily, '%Y-%m-%d')
                        converted_d = datetime.datetime(cd1.year, cd1.month, cd1.day)

                    # print datetime.datetime.strftime(converted_d, '%Y-%m-%d') + " was found successfully from " + daily_table

                elif returned_value == None:

                    # if the daily table is totally blank we'll need to rebuild the whole thing from the hr table
                    self.cursor.execute("select top 1 date_time from FSDBDATA.dbo." + hr_table + " order by date_time asc")
                        
                    returned_value = self.cursor.fetchone()

                    try:
                        daily = returned_value[0]

                    except ValueError:
                        print "an error! see: "
                        print returned_value[0]
                        
                    
                    converted_d = datetime.datetime(daily.year, daily.month, daily.day)
                    
                    # print datetime.datetime.strftime(converted_d, '%Y-%m-%d') + " was taken to start " + daily_table + "from the HR table"
                    
                # because we haven't updated the daily table as recently as the hr table has been updated we can assume that the last_hr value follows after the daily value
                self.cursor.execute("select top 1 date_time from FSDBDATA.dbo." + hr_table + " order by date_time desc")

                new_returned_value = self.cursor.fetchone()
                
                high_res = new_returned_value[0]
                converted_hr = datetime.datetime(high_res.year, high_res.month, high_res.day)

                if converted_hr > converted_d:

                    # add a day to the last daily record for where we will start from 
                    daily_data_plus_one = converted_d + datetime.timedelta(days = 1)

                    # the new start date is the first day we have in our daily data (plus one)
                    startdate = datetime.datetime.strftime(daily_data_plus_one, '%Y-%m-%d %H:%M:%S')

                    # the new end date is the last complete day in our high res data - we stop at the zeroeth hour- because remember on the last go-round we ended with < the zeroth hour, so it will be ok.
                    enddate = datetime.datetime.strftime(converted_hr, '%Y-%m-%d %H:%M:%S')

                    if daily_table not in self.lookup:

                        self.lookup[daily_table] = {'startdate': startdate, 'enddate': enddate}
                    
                    elif daily_table in self.lookup:

                        print 'Warning: ' + daily_table + ' has already been processed in the lookup'

                elif converted_hr <= converted_d:
                    print(daily_table + " is already up to date with its high resolution counterpart")
                
                del converted_hr
                del converted_d
                del daily
                del high_res
                del returned_value
                del new_returned_value

    def build_queries_station(self):
        """ check the existing dates and output last values for each table"""
        
         # zip together the two tables
        iTables = itertools.izip(self.daily_table_list, self.hr_table_list)
        if self.server == "SHELDON":
            
            # for the two tables, check the most recent input value on both the daily and high resolution tables
            for daily_table, hr_table in iTables:

                self.cursor.execute("select top 1 date from LTERLogger_pro.dbo." + daily_table + " where sitecode like \'" + self.station + "\' order by date desc")
                returned_value = self.cursor.fetchone()

                if returned_value != None:
                    try:
                        print " processing table : " + daily_table + " and station : " + self.station
                        daily = returned_value[0]
                        
                    except Exception:
                        print "some error has been found on :"
                        print returned_value
                        print "entering debug mode : "
                        import pdb; pdb.set_trace()
                    
                    try:
                        converted_d = datetime.datetime(daily.year, daily.month, daily.day)
                    
                    except AttributeError:
                        # ms04325 has only the date without the same seconds format
                        cd1 = datetime.datetime.strptime(daily, '%Y-%m-%d')
                        converted_d = datetime.datetime(cd1.year, cd1.month, cd1.day)

                    # print datetime.datetime.strftime(converted_d, '%Y-%m-%d') + " was found from " + daily_table

                elif returned_value == None:

                    # if the daily table is totally blank we'll need to rebuild the whole thing from the hr table
                    self.cursor.execute("select top 1 date_time from LTERLogger_pro.dbo." + hr_table + " where sitecode like \'" + self.station + "\' order by date_time asc")
                        
                    returned_value = self.cursor.fetchone()

                    try:
                        print "we have to get the daily from the hr table which is " + hr_table + " and the returned value is "
                        print returned_value
                        daily = returned_value[0]

                    except ValueError:
                        print "a value error! see: "
                        print returned_value[0]

                    except TypeError: 
                        print "it's likely we dont have that attribute for the site you want, let's check..."

                        self.cursor.execute("select distinct sitecode from LTERLogger_pro.dbo." + hr_table)

                        list_of_sites = []
                        for row in self.cursor:
                            list_of_sites.append(str(row[0]))

                        if self.station in list_of_sites:
                            print "no, actually we have that site"
                            stop_processing = raw_input("type DEBUG to enter debug mode or CTRL + C to stop the process otherwise we will continue")

                            if stop_processing == "DEBUG":
                                import pdb; pdb.set_trace()

                            else:
                                print "continuing the processing, but skipping this table of : " + hr_table
                                continue

                        elif self.station not in list_of_sites:
                            print "... nope, we don't have the attribute in table " + hr_table + " on " + self.station + ", sorry!"
                            continue
                        
                    print daily
                    converted_d = datetime.datetime(daily.year, daily.month, daily.day)
                    
                    # print datetime.datetime.strftime(converted_d, '%Y-%m-%d') + " as found from the hr table"
                    
                # because we haven't updated the daily table as recently as the hr table has been updated we can assume that the last_hr value follows after the daily value
                self.cursor.execute("select top 1 date_time from LTERLogger_pro.dbo." + hr_table + " where sitecode like \'" + self.station + "\' order by date_time desc")

                new_returned_value = self.cursor.fetchone()
                
                if new_returned_value != None:
                    high_res = new_returned_value[0]
                elif new_returned_value == None:
                    print "there is an error pulling the high res data in station-specific " + hr_table

                converted_hr = datetime.datetime(high_res.year, high_res.month, high_res.day)

                if converted_hr > converted_d:

                    # add a day to the last daily record for where we will start from 
                    daily_data_plus_one = converted_d + datetime.timedelta(days = 1)

                    # the new start date is the first day we have in our daily data (plus one)
                    startdate = datetime.datetime.strftime(daily_data_plus_one, '%Y-%m-%d %H:%M:%S')

                    # the new end date is the last complete day in our high res data - we stop at the zeroeth hour- because remember on the last go-round we ended with < the zeroth hour, so it will be ok.
                    enddate = datetime.datetime.strftime(converted_hr, '%Y-%m-%d %H:%M:%S')

                    if daily_table not in self.lookup:

                        self.lookup[daily_table] = {'startdate': startdate, 'enddate': enddate}
                    
                    elif daily_table in self.lookup:

                        print 'this ' + daily_table + ' has already been processed in the lookup'

                elif converted_hr <= converted_d:
                    print(daily_table + " is already up to date with its high resolution counterpart")
                
                del converted_hr
                del converted_d
                del daily
                del high_res
                del returned_value
                del new_returned_value

        elif self.server == "STEWARTIA":
            
            # for the two tables, check the most recent input value on both the daily and high resolution tables
            for daily_table, hr_table in iTables:

                self.cursor.execute("select top 1 date from FSDBDATA.dbo." + daily_table + " where sitecode like \'" + self.station + "\' order by date desc")
                returned_value = self.cursor.fetchone()

                if returned_value != None:
                    try:
                        # print "the table in question is " + daily_table + " and the returned value is "
                        # print returned_value
                        daily = returned_value[0]
                        
                    except Exception:
                        print "some error has been found on :"
                        print returned_value
                        print "entering debug mode :"
                        import pdb; pdb.set_trace()
                    try:
                        converted_d = datetime.datetime(daily.year, daily.month, daily.day)
                    
                    except AttributeError:
                        # ms04325 has only the date without the same seconds format
                        cd1 = datetime.datetime.strptime(daily, '%Y-%m-%d')
                        converted_d = datetime.datetime(cd1.year, cd1.month, cd1.day)

                    # print datetime.datetime.strftime(converted_d, '%Y-%m-%d') + " as found from the daily table"

                elif returned_value == None:

                    # if the daily table is totally blank we'll need to rebuild the whole thing from the hr table
                    self.cursor.execute("select top 1 date_time from FSDBDATA.dbo." + hr_table + " where sitecode like \'" + self.station + "\' order by date_time asc")
                        
                    returned_value = self.cursor.fetchone()

                    try:
                        # print "In this case, we have to get the daily lower date bound from the HR table which is " + hr_table + "; the returned value is "
                        # print returned_value
                        daily = returned_value[0]

                    except ValueError:
                        print "a value error! see: "
                        print returned_value[0]
                        print "entering debug mode: "
                        import pdb; pdb.set_trace()

                    except TypeError: 
                        print "it's likely we dont have that attribute for the site you want, let's check..."

                        self.cursor.execute("select distinct sitecode from FSDBDATA.dbo." + hr_table)

                        list_of_sites = []
                        for row in self.cursor:
                            list_of_sites.append(str(row[0]))

                        if self.station in list_of_sites:
                            print "no, actually we have that site"
                            stop_processing = raw_input("type DEBUG to enter debug mode or CTRL + C to stop the process otherwise we will continue")

                            if stop_processing == "DEBUG":
                                import pdb; pdb.set_trace()

                            else:
                                print "continuing the processing, but skipping this table of : " + hr_table
                                continue

                        elif self.station not in list_of_sites:
                            print "... nope, we don't have the attribute in table " + hr_table + " on " + self.station + ", sorry!"
                            continue
                        
                    print daily
                    converted_d = datetime.datetime(daily.year, daily.month, daily.day)
                    
                    #print datetime.datetime.strftime(converted_d, '%Y-%m-%d') + " as found from the hr table"
                    
                # because we haven't updated the daily table as recently as the hr table has been updated we can assume that the last_hr value follows after the daily value
                self.cursor.execute("select top 1 date_time from FSDBDATA.dbo." + hr_table + " where sitecode like \'" + self.station + "\' order by date_time desc")

                new_returned_value = self.cursor.fetchone()
                
                if new_returned_value != None:
                    high_res = new_returned_value[0]
                elif new_returned_value == None:
                    print "there is an error pulling the high-res data in station-specific " + hr_table

                converted_hr = datetime.datetime(high_res.year, high_res.month, high_res.day)

                if converted_hr > converted_d:

                    # add a day to the last daily record for where we will start from 
                    daily_data_plus_one = converted_d + datetime.timedelta(days = 1)

                    # the new start date is the first day we have in our daily data (plus one)
                    startdate = datetime.datetime.strftime(daily_data_plus_one, '%Y-%m-%d %H:%M:%S')

                    # the new end date is the last complete day in our high res data - we stop at the zeroeth hour- because remember on the last go-round we ended with < the zeroth hour, so it will be ok.
                    enddate = datetime.datetime.strftime(converted_hr, '%Y-%m-%d %H:%M:%S')

                    if daily_table not in self.lookup:

                        self.lookup[daily_table] = {'startdate': startdate, 'enddate': enddate}
                    
                    elif daily_table in self.lookup:

                        print 'this ' + daily_table + ' has already been processed in the lookup'

                elif converted_hr <= converted_d:
                    print(daily_table + " is already up to date with its high resolution counterpart")
                
                del converted_hr
                del converted_d
                del daily
                del high_res
                del returned_value
                del new_returned_value

    def check_out_one_attribute(self, attribute):
        """ For any given attribute check to see if it has been included in the lookup yet"""
        
        if attribute == "AIRTEMP" or attribute == "MS04301":
            startdate_out = self.lookup['MS04301']['startdate']
            enddate_out = self.lookup['MS04301']['enddate']

        elif attribute == "RELHUM" or attribute=="MS04302":
            startdate_out = self.lookup['MS04302']['startdate']
            enddate_out = self.lookup['MS04302']['enddate']

        elif attribute == "PRECIP" or attribute=="MS04303":
            startdate_out = self.lookup['MS04303']['startdate']
            enddate_out = self.lookup['MS04303']['enddate']

        elif attribute == "WSPD_PRO" or attribute=="MS04304" or attribute =="WSPD_PRO2":
            startdate_out = self.lookup['MS04304']['startdate']
            enddate_out = self.lookup['MS04304']['enddate']

        elif attribute == "SOLAR" or attribute=="MS04305":
            startdate_out = self.lookup['MS04305']['startdate']
            enddate_out = self.lookup['MS04305']['enddate']

        elif attribute == "LYS" or attribute== "MS04309":
            startdate_out = self.lookup['MS04309']['startdate']
            enddate_out = self.lookup['MS04309']['enddate']

        elif attribute == "SNOWDEPTH" or attribute == "MS04310":
            startdate_out = self.lookup['MS04310']['startdate']
            enddate_out = self.lookup['MS04310']['enddate']

        elif attribute == "NR" or attribute == "MS04325":
            startdate_out = self.lookup['MS04325']['startdate']
            enddate_out = self.lookup['MS04325']['enddate']

        elif attribute == "VPD" or attribute == "VPD2" or attribute=="MS04308":
            startdate_out = self.lookup['MS04308']['startdate']
            enddate_out = self.lookup['MS04308']['enddate']

        elif attribute == "DEWPT" or attribute == "MS04307":
            startdate_out = self.lookup['MS04307']['startdate']
            enddate_out = self.lookup['MS04307']['enddate']

        elif attribute == "WSPD_SNC" or attribute=="MS04324" or attribute=="SONIC":
            startdate_out = self.lookup['MS04324']['startdate']
            enddate_out = self.lookup['MS04324']['enddate']

        elif attribute == "PAR" or attribute == "MS04322":
            startdate_out = self.lookup['MS04322']['startdate']
            enddate_out = self.lookup['MS04322']['enddate']

        elif attribute == "SOILWC" or attribute=="MS04323":
            startdate_out = self.lookup['MS04323']['startdate']
            enddate_out = self.lookup['MS04323']['enddate']

        elif attribute == "SOILTEMP" or attribute=="MS04321" or attribute == "MS00121":
            try:
                startdate_out = self.lookup['MS04321']['startdate']
            except Exception:
                startdate_out = self.lookup['MS00121']['startdate']
            try:
                enddate_out = self.lookup['MS04321']['enddate']
            except Exception:
                enddate_out = self.lookup['MS00121']['enddate']

        return startdate_out, enddate_out

class HeaderWriter(object):
    """ 
    Writes a header given a certain attribute, should be used to generate CSVs

    """

    def __init__(self, attribute):
    
        # the reference entity and call to the worker 
        daily_attr = {'LYS': '09',
                'NR': '25',
                'WSPD_SNC': '24',
                'SOILWC':'23',
                'PAR':'22',
                'SOILTEMP':'21',
                'VPD':'08',
                'VPD2':'08',
                'DEWPT':'07',
                'SOLAR': '05',
                'WSPD_PRO': '04',
                'WSPD_PRO2': '04',
                'PRECIP': '03',
                'RELHUM':'02',
                'AIRTEMP':'01',
                'WSPD_SNC': '24',
                'SNODEP': '20'}

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
        self.filename = dbcode + daily_attr[attribute] + '_copy.csv'

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

        elif self.attribute == "VPD" or self.attribute == "VPD2":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, self.min_method, self.min_flag_method, self.mintime_method, "VAP_MEAN_DAY", "VAP_MEAN_FLAG", "VAP_MAX_DAY", "VAP_MAX_FLAG", "VAP_MIN_DAY", "VAP_MIN_FLAG", "SATVP_MEAN_DAY", "SATVP_MEAN_FLAG", "SATVP_MAX_DAY", "SATVP_MAX_FLAG", "SATVP_MIN_DAY", "SATVP_MIN_FLAG", "EVENT_CODE", "SOURCE"]

        # attributes which need a "total"
        elif self.attribute == "PRECIP":

            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "PRECIP_TOT_DAY", "PRECIP_TOT_FLAG", "EVENT_CODE", "SOURCE"]

        # propellor anemometer
        elif self.attribute == "WSPD_PRO" or self.attribute == "WSPD_PRO2":

            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "WMAG_PRO_MEAN_DAY", "WMAG_PRO_MEAN_FLAG", "WDIR_PRO_MEAN_DAY", "WDIR_PRO_MEAN_FLAG", "WDIR_PRO_STDDEV_DAY", "WDIR_PRO_STDDEV_FLAG", "WSPD_ROSE1_MEAN_DAY", "WSPD_ROSE1_MEAN_FLAG", "WSPD_ROSE2_MEAN_DAY", "WSPD_ROSE2_MEAN_FLAG", "WSPD_ROSE3_MEAN_DAY", "WSPD_ROSE1_MEAN_FLAG", "WSPD_ROSE4_MEAN_DAY", "WSPD_ROSE4_MEAN_FLAG", "WSPD_ROSE5_MEAN_DAY",  "WSPD_ROSE5_MEAN_FLAG", "WSPD_ROSE6_MEAN_DAY", "WSPD_ROSE6_MEAN_FLAG", "WSPD_ROSE7_MEAN_DAY", "WSPD_ROSE7_MEAN_FLAG", "WSPD_ROSE8_MEAN_DAY", "WSPD_ROSE8_MEAN_FLAG", "EVENT_CODE", "SOURCE"]

        # the sonic anemometer
        elif self.attribute == "WSPD_SNC":

            header = ['DBCODE','ENTITY','SITECODE', self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, "WDIR_SNC_MEAN_DAY", "WDIR_SNC_MEAN_FLAG", "WDIR_SNC_STDDEV_DAY", "WDIR_SNC_STDDEV_FLAG", "WUX_SNC_MEAN_DAY", "WUX_SNC_MEAN_FLAG", "WUX_SNC_STDDEV_DAY", "WUX_SNC_STDDEV_FLAG","WUY_SNC_MEAN_DAY", "WUY_SNC_MEAN_FLAG", "WUY_SNC_STDDEV_DAY", "WUY_SNC_STDDEV_FLAG", "WAIR_SNC_MEAN_DAY", "WAIR_SNC_MEAN_FLAG", "WAIR_SNC_STDDEV_DAY", "WAIR_SNC_STDDEV_FLAG",  "EVENT_CODE", "SOURCE"]

        # net radiometer
        elif self.attribute == "NR":

            # NO SOURCE TABLE HERE!!
            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "SW_IN_MEAN_DAY", "SW_IN_MEAN_FLAG", "SW_OUT_MEAN_DAY", "SW_OUT_MEAN_FLAG", "LW_IN_MEAN_DAY", "LW_IN_MEAN_FLAG", "LW_OUT_MEAN_DAY", "LW_OUT_MEAN_FLAG", "NR_TOT_MEAN_DAY", "NR_TOT_MEAN_FLAG", "SENSOR_TEMP_DAY", "SENSOR_TEMP_FLAG", "EVENT_CODE"]

        # pyranometer (similar method to precip but takes a max and min time as well)
        elif self.attribute == "SOLAR":
            
            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", "SOLAR_TOT_DAY", "SOLAR_TOT_FLAG", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "EVENT_CODE", "SOURCE"]

        # very simple, like the "big six", but no minimums
        elif self.attribute == "PAR":
            header = ["DBCODE","ENTITY","SITECODE", self.method, self.height, "QC_LEVEL", "PROBE_CODE", "DATE", self.mean_method, self.mean_flag_method, self.max_method, self.max_flag_method, self.maxtime_method, "EVENT_CODE", "SOURCE"]

        # still not sure if this is right
        elif self.attribute == "LYS":

            header = ["DBCODE","ENTITY","SITECODE", "SNOWMELT_METHOD", "QC_LEVEL", "PROBE_CODE", "DATE", "SNOWMELT_TOT_DAY", "SNOWMELT_TOT_FLAG", "EVENT_CODE", "SOURCE"]

        elif self.attribute == "SNODEP":

            header = ["DBCODE","ENTITY","SITECODE", "SNOW_METHOD", "QC_LEVEL", "PROBE_CODE", "DATE", "SWE_DAY", "SWE_DAY_FLAG", "SNODEP_DAY","SNODEP_DAY_FLAG", "EVENT_CODE", "DB_TABLE", "SOURCE"]

        return header