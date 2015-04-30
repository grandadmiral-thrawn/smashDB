import form_connection as fc
import pymssql
import csv
import datetime
import math
import yaml
import numpy as np
import itertools
import smashControls
import argparse

class ProbeBoss(object):
    """ The smashBosses are responsible for conducting specific data queries, such as generating probe-specific outputs """

    def __init__(self, attribute, server, vpd="off"):
        """ initialize the boss with a configuration file and an attribute"""

        # limits is the config
        self.limits = yaml.load(open('LIMITED.yaml','rb'))
        # attribute you give
        self.attribute = attribute
        # server is either sheldon or stewartia
        self.server = server
        # startdate is the day it begins
        self.startdate = ''
        # end date is the day it ends
        self.enddate = ''
        # vpd is a kwarg- if it's "off" then nothing happens. if it's "on" we use the special computation. we change the default when we change method
        self.vpd = vpd

    def get_one_config(self, chosen_probe):
        """ get one configuration - that means 1 probe over 1 set of time stamps, i.e. AIRPRI01 between say january 2 2015 and february 22, 2015. or VPDCEN04 between april 5 2015 and april 10 2015. This doesn't get all the probes at once! But note that by default in the controller dew and vpd are checked for bad dates. """

        # compute the startdate and end date as strings if they are not already strings!
        self.startdate = datetime.datetime.strftime(self.limits[chosen_probe]['startdate'], '%Y-%m-%d %H:%M:%S')
        self.enddate = datetime.datetime.strftime(self.limits[chosen_probe]['enddate'], '%Y-%m-%d %H:%M:%S')
        
        # if it is not VPD, then go ahead and make a worker
        if self.attribute != "VPD":
            myWorker = smashControls.Worker(self.attribute, self.startdate, self.enddate, self.server, chosen_probe)

        # if it is VPD but we are using the old method, then go ahead and make a worker
        elif self.attribute == "VPD" and self.vpd == "off":
            myWorker = smashControls.Worker(self.attribute, self.startdate, self.enddate, self.server, chosen_probe)

        # if it is VPD and we are using the new method, then we assign that to the worker
        elif self.attribute == "VPD" and self.vpd == "on":
            myWorker = smashControls.VaporControl(self.startdate, self.enddate, self.server, chosen_probe)

        # my worker is one worker object
        return myWorker

    @staticmethod
    def write_one_worker_to_csv(myWorker, *args):
        """ open a csvfile and write out one data - this is in a sense just doing one probe to one csv, for the purpose of maybe patching something"""
        
        with open(myWorker.HeaderWriter.filename, 'wb') as writefile:
            writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")

            myHeader = myWorker.HeaderWriter.write_header_template()
            
            writer.writerow(myHeader)

            if args:
                try:
                    my_new_rows = myWorker.Worker.condense_data(args[0])
                except Exception:
                    my_new_rows = myWorker.condense_data(args[0])
            
            else:
                try:
                    my_new_rows = myWorker.Worker.condense_data()
                except Exception:
                    my_new_rows = myWorker.condense_data()
            
            for row in my_new_rows:

                writer.writerow(row)


    def iterate_over_many_config(self, *args):
        """ walks over the limits file and creates for each a worker, who then goes and gets the data for each and writes it to a single csv output"""
        
        import smashWorkers

        # writes the csv-headers
        templateWorker = smashWorkers.HeaderWriter(self.attribute)

        # writing to the csv
        with open(templateWorker.filename, 'wb') as writefile:
                
            writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")

            myHeader = templateWorker.write_header_template()
            
            writer.writerow(myHeader)

            # for the prose you choose
            for chosen_probe in self.limits.keys():

                # get that worker
                myWorker = self.get_one_config(chosen_probe)

                # if you need the custom configuration, use it
                if args:
                    my_new_rows = myWorker.Worker.condense_data(args[0])
                else:
                    my_new_rows = myWorker.Worker.condense_data()

                # write the rows, then delete the worker
                for row in my_new_rows:
                    writer.writerow(row)

                del myWorker

        print("Finished writing data to " + templateWorker.filename)


class UpdateBoss(object):
    """ The UpdateBoss updates an attribute based on the times you specify"""

    def __init__(self, attribute, startdate, enddate, server, vpd="off"):
        
        self.attribute = attribute
        self.startdate = startdate
        self.enddate = enddate
        self.server = server
        self.vpd = vpd

        # if it is not VPD, then go ahead and make a worker
        if self.attribute != "VPD":
            self.myWorker = smashControls.Worker(self.attribute, self.startdate, self.enddate, self.server)
            
            self.new_rows = self.myWorker.Worker.condense_data()

            if self.myWorker.Worker.entity < 10:
                new_string = "0"+str(self.myWorker.Worker.entity)
            else:
                new_string = str(self.myWorker.Worker.entity)

            # name of the table
            self.table = 'MS043' + new_string

        # if it is VPD but we are using the old method, then go ahead and make a worker
        elif self.attribute == "VPD" and self.vpd == "off":
            self.myWorker = smashControls.Worker(self.attribute, self.startdate, self.enddate, self.server)
            self.new_rows = self.myWorker.Worker.condense_data()

            if self.myWorker.Worker.entity < 10:
                new_string = "0"+str(self.myWorker.Worker.entity)
            else:
                new_string = str(self.myWorker.Worker.entity)


            # name of the table
            self.table = 'MS043' + new_string

        # if it is VPD and we are using the new method, then we assign that to the worker
        elif self.attribute == "VPD" and self.vpd == "on":
            self.myWorker = smashControls.VaporControl(self.startdate, self.enddate, self.server)
            self.new_rows = self.myWorker.condense_data()

            # name of the table
            self.table = 'MS04308'
        
        else: 
            print "this will never get called"

    def only_one_station(self, station):

        h = [self.new_rows[index] for index, row in enumerate(self.new_rows) if self.new_rows[index][2] == station]

        return h

    def update_the_db(self):
        """ Updates LTER Logger Pro-- NOT LTER LOGGER NEW! -- currently as of 04-20-3015 its empty so I can't check it for pre-existing values without an error! """
        print("This is gonna update the LTERLogger_Pro database")

        # form a new connection (we need this because we need the conn object to commit)
        import form_connection as fc
        conn = fc.micro_conn('SHELDON')

        # keep the tuples from the previous analysis
        new_tuples = [tuple(x) for x in self.new_rows]

        
        cursor = conn.cursor()
        # get_the_column_names
        cursor.execute("select column_name from LTERLogger_Pro.information_schema.columns where table_name like \'" + self.table + "\' and table_schema like 'dbo'")

        print( " your execution query was: \n SELECT column_name from LTERLogger_Pro.information_schema.columns where table_name like \'" + self.table + "\' and table_schema like 'dbo'")


        # make 'em ' into a list
        nr = []
        for row in cursor:
            nr.append(str(row[0]))


        if self.attribute != "NR":
            column_string = " ,".join(nr[:-1])
        elif self.attribute == "NR":
            column_string = " ,".join(nr)

                
        # for some reason it likes to use "d" which is not the usual "f" formatter for doing floats. Yes, a total mystery. Example of working prototype        
        # cursor.executemany("insert into LTERLogger_Pro.dbo.Test (CreateTime, DValue) VALUES (%s, %d)", [('2015-01-02 00:00:00', 17.343823), ('2015-01-03 00:00:00', 18.238123), ('2015-01-04 00:00:00', 23.328)])

        if self.attribute in ["AIRTEMP", "RELHUM", "DEWPT", "SOILTEMP", "SOILWC"]:
        
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table +" (" + column_string + ")  VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %s, %d, %s, %s, %s, %s)", new_tuples)

            conn.commit()

        elif self.attribute in "VPD":

            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ") VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", new_tuples)

            conn.commit()

        elif self.attribute in "SOLAR":

            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table +" (" + column_string + ")  VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %d, %s, %s, %s, %s)", new_tuples)
            conn.commit()

        elif self.attribute in "PRECIP":

            cursor.executemany("insert into LTERLogger_Pro.dbo" + self.table + " (" + column_string + ") VALUES( %s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d , %s,  %s, %s)", new_tuples)
            conn.commit()

        elif self.attribute in "LYS":

           cursor.executemany("insert into LTERLogger_Pro.dbo" + self.table + " (" + column_string + ") VALUES( %s, %d, %s, %s, %s, %s, %s, %d, %s, %d , %s, %s, %s)", new_tuples)

           conn.commit()

        elif self.attribute in "PAR":
            
            cursor.executemany("insert into LTERLogger_Pro.dbo" + self.table + " (" + column_string + ") VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %d, %s, %s)", new_tuples)
            conn.commit()

        elif self.attribute in "NR":
            
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ")  VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", new_tuples)

            conn.commit()


        elif self.attribute in "WSPD_PRO":
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ")  VALUES(%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %d,%s ,%s, %d, %s, %d, %s, %d,  %s, %d, %s, %d, %s, %d, %s, %d, %s, %d, %s, %d, %s, %d, %s, %s, %s)", new_tuples)

            conn.commit()
        
        elif self.attribute in "WSPD_SNC":
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ")  VALUES( %s,  %d,  %s, %s,  %d,  %s, %s,  %s,    %d,  %s,  %d,  %s,  %d, %s,  %d, %s,    %d, %s,   %d, %s, %d,   %s,   %d,  %s, %d   %s,  %d, %s,  %s,  %s)",new_tuples)

            conn.commit()
    
    def write_a_csv(self):
        """ writes the rows in self.new_rows to a csv"""
        import smashWorkers
        templateWorker = smashWorkers.HeaderWriter(self.attribute)
        
        filename = self.table + "_" + self.server + "_temp.csv"
        with open(filename,'wb') as writefile:

            writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")

            myHeader = templateWorker.write_header_template()
            
            writer.writerow(myHeader)

            for row in self.new_rows:
                writer.writerow(row)

class MethodBoss(object):
    """ uses the file from don for updating the LterLogger_Pro"""
    def __init__(self, attribute):

        self.filename = "method_current_daily.CSV"
        self.attribute = attribute
        self.od = self.csv_to_dict()

    def csv_to_dict(self):
        """ update method list is imported. """
        od = {}

        with open(self.filename, 'rb') as readfile:
            reader = csv.reader(readfile)

            for row in reader:

                if str(row[1]) not in od:
                    # make start, end, and method update-able
                    od[str(row[1])] = {'startdate': [str(row[2])], 'enddate': [str(row[3])], 'height': str(row[4]), 'method_code':[str(row[6])], 'site_code': str(row[0])}
                elif str(row[1]) in od:
                    od[str(row[1])]['startdate'].append(str(row[2]))
                    od[str(row[1])]['enddate'].append(str(row[3]))
                    od[str(row[1])]['method_code'].append(str(row[6]))
                else: 
                    pass

        return od

    def update_methods(self):

        import form_connection as fc
        conn = fc.micro_conn('SHELDON')

        cursor = conn.cursor()

        query_d = {'AIRTEMP': 'MS04301',
                    'RELHUM': 'MS04302',
                    'PRECIP': 'MS04303',
                    'WSPD_PRO': 'MS04304',
                    'SOLAR': 'MS04305',
                    'DEWPT': 'MS04307',
                    'VPD': 'MS04308',
                    'LYS': 'MS04309',
                    'NR': 'MS04325',
                    'WSPD_SNC': 'MS04324',
                    'SOILTEMP': 'MS04321',
                    'SOILWC': 'MS04323',
                    'PAR': 'MS04322'}


        valid_keys = []
        if self.attribute == "AIRTEMP":

            for key in self.od.keys():

                if "AIR" in key:
                    valid_keys.append(key)

        elif self.attribute == "RELHUM":

            for key in self.od.keys():

                if "REL" in key:
                    valid_keys.append(key)


        elif self.attribute == "PRECIP":
            for key in self.od.keys():

                    if "PPT" in key:
                        valid_keys.append(key)

        elif self.attribute == "WSPD_SNC":


            valid_keys = ["WNDPRI02", "WNDVAN02"]


        elif self.attribute == "WSPD_PRO":

            valid_keys = ["WNDPRI01", "WNDVAN01", "WNDUPL01", "WNDCEN01", "WNDH1501"]


        elif self.attribute == "DEWPT":
            for key in self.od.keys():

                if "DEW" in key:
                    valid_keys.append(key)

        elif self.attribute == "VPD":
            for key in self.od.keys():

                if "VPD" in key:
                    valid_keys.append(key)

        elif self.attribute in "SOLAR":

            valid_keys = ["RADPRI01","RADVAN01","RADUPL01","RADCEN01"]

        elif self.attribute in "NR":

            valid_keys = ["RADPRI02","RADVAN02"]

        elif self.attribute in "SOILWC":
            for key in self.od.keys():

                if "SWC" in key:
                    valid_keys.append(key)

        elif self.attribute in "SOILTEMP":
            for key in self.od.keys():

                if "SOI" in key:
                    valid_keys.append(key)


        elif self.attribute in "LYS":

            for key in self.od.keys():
                if "LYS" in key:
                    valid_keys.append(key)

        else:
            pass

        if valid_keys != []:

            for each_key in valid_keys:

                startdate = self.od[each_key]['startdate'][0]
                enddate = self.od[each_key]['enddate'][0]
                new_method_code = self.od[each_key]['method_code'][0]
            
                new_query = "update LTERLogger_Pro.dbo." + query_d[self.attribute] + " set " + self.attribute + "_METHOD = \'" +  str(new_method_code) + "\' where probe_code like \'" + each_key + "\' and Date >= \'" + startdate + "\' and Date < \'" + enddate + "\'"

                print new_query

                cursor.execute(new_query)   

            conn.commit() 
        else:
            print "nothing to commit!"


# class MethodBoss2(object):

#     self.filename = "method_current_hires.csv"
#     self.d = self.get_highres()

#     def get_highres(self):
#         d = {}
#         with open('method_current_hires.csv','rb') as readfile:
#             reader = csv.reader(readfile)
#             for row in reader:
#                 if str(row[1]) not in d:
#                     d[str(row[1])] = {'startdate': str(row[2]), 'enddate':str(row[3]), 'res': str(row[6]), 'hrmethod': str(row[7])}
#                 elif str(row[1]) in d:
#                     print "huh"
#         return d

#     def update_methods(self):

#         import form_connection as fc
#         conn = fc.micro_conn('SHELDON')

#         cursor = conn.cursor()

#         query_d = {'AIRTEMP': 'MS04301',
#                     'RELHUM': 'MS04302',
#                     'PRECIP': 'MS04303',
#                     'WSPD_PRO': 'MS04304',
#                     'SOLAR': 'MS04305',
#                     'DEWPT': 'MS04307',
#                     'VPD': 'MS04308',
#                     'LYS': 'MS04309',
#                     'NR': 'MS04325',
#                     'WSPD_SNC': 'MS04324',
#                     'SOILTEMP': 'MS04321',
#                     'SOILWC': 'MS04323',
#                     'PAR': 'MS04322'}

#         valid_keys = []
        
#         if self.attribute == "AIRTEMP":

#             for key in self.d.keys():

#                 if "AIR" in key:
#                     valid_keys.append(key)

#         elif self.attribute == "RELHUM":

#             for key in self.d.keys():

#                 if "REL" in key:
#                     valid_keys.append(key)


#         elif self.attribute == "PRECIP":
#             for key in self.d.keys():

#                 if "PPT" in key:
#                     valid_keys.append(key)

#         elif self.attribute == "WSPD_SNC":


#             valid_keys = ["WNDPRI02", "WNDVAN02"]


#         elif self.attribute == "WSPD_PRO":

#             valid_keys = ["WNDPRI01", "WNDVAN01", "WNDUPL01", "WNDCEN01", "WNDH1501"]


#         elif self.attribute == "DEWPT":
#             for key in self.d.keys():

#                 if "DEW" in key:
#                     valid_keys.append(key)

#         elif self.attribute == "VPD":
#             for key in self.d.keys():

#                 if "VPD" in key:
#                     valid_keys.append(key)

#         elif self.attribute in "SOLAR":

#             valid_keys = ["RADPRI01","RADVAN01","RADUPL01","RADCEN01"]

#         elif self.attribute in "NR":

#             valid_keys = ["RADPRI02","RADVAN02"]

#         elif self.attribute in "SOILWC":
#             for key in self.d.keys():

#                 if "SWC" in key:
#                     valid_keys.append(key)

#         elif self.attribute in "SOILTEMP":
#             for key in self.d.keys():

#                 if "SOI" in key:
#                     valid_keys.append(key)


#         elif self.attribute in "LYS":

#             for key in self.d.keys():
#                 if "LYS" in key:
#                     valid_keys.append(key)

#         else:
#             pass

#         if valid_keys != []:

#             for each_key in valid_keys:

#                 startdate = self.d[each_key]['startdate']
#                 enddate = self.d[each_key]['enddate']
#                 res = self.d[each_key]['res']

#                 if res == "15 minutes":
#                     replace_flag = "F"
#                 elif res == "60 minutes":
#                     replace_flag = "H"
#                 else:
#                     continue
                
#                 # check for max and min sets
#                 if self.attribute in ["AIRTEMP", "RELHUM", "WSPD_PRO", "SOLAR", "DEWPT", "VPD", "SOILTEMP", "SOILWC", "PAR", "WSPD_SNC"]:
                    
#                     ### Update max flag to new replacement
#                     new_query = "update LTERLogger_Pro.dbo." + query_d[self.attribute] + " set " + self.attribute + "_MAX_FLAG = \'" +  replace_flag + "\' where probe_code like \'" + each_key + "\' and Date >= \'" + startdate + "\' and Date < \'" + enddate + "\'"

#                     cursor.execute(new_query) 


#                 if self.attribute in ["AIRTEMP", "DEWPT", "VPD", "SOILTEMP", "SOILWC"]:
                    
#                     # update min flag to new replacement
#                     new_query = "update LTERLogger_Pro.dbo." + query_d[self.attribute] + " set " + self.attribute + "_MIN_FLAG = \'" +  replace_flag + "\' where probe_code like \'" + each_key + "\' and Date >= \'" + startdate + "\' and Date < \'" + enddate + "\'"
 
#                     cursor.execute(new_query)

            
#             # commit the changes
#             conn.commit() 
        
#         else:
#             print "nothing to commit!"