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

    @staticmethod
    def f5(seq, idfun=None): 
        """ a quick, order-preserving way to search a list like the one we will have of the probes"""
        # order preserving
        if idfun is None:
           def idfun(x): return x
        seen = {}
        result = []
        for item in seq:
            marker = idfun(item)
            # in old Python versions:
            # if seen.has_key(marker)
            # but in new ones:
            if marker in seen: continue
            seen[marker] = 1
            result.append(item)
        return result

    def update_the_db_methods(self):
        """ updates the daily db based on the methods in method_history_daily"""
        
        print("This is gonna update the LTERLogger_Pro database based on the methods in methods_history_daily")

        shortened_lookup = {}

        # form a new connection (we need this because we need the conn object to commit)
        import form_connection as fc
        conn = fc.micro_conn('SHELDON')

        # get all the probecodes we have and dates we have
        probe_codes_ordered = [x[6] for x in self.new_rows]
        dates_ordered = [x[7] for x in self.new_rows]

        # get the unique entries of probe codes
        probe_string = self.f5(probe_codes_ordered)

        # make it into a string that the query can injest
        query_string = "\', \'".join(probe_string)

        # get the limited info from the lookup table
        query = "select probe_code, date_bgn, date_end, method_code, height, depth from LTERLogger_new.dbo.method_history_daily where probe_code in (\'" + query_string +"\') and date_end >= \'" + self.startdate + "\' order by date_bgn asc"

        cur = conn.cursor()

        cur.execute(query)

        # gather the methods by populating a dictionary called "shortened lookup"
        for row in cur:
            probe_code = str(row[0])
            date_bgn = str(row[1])
            date_end = str(row[2])
            method_code = str(row[3])
            height = int(row[4])
            depth = str(row[5])


            if probe_code not in shortened_lookup:
                shortened_lookup[probe_code] = {method_code: {'begin': date_bgn, 'end': date_end, 'height': height, 'depth': depth}}
            
            elif probe_code in shortened_lookup:
                if method_code not in shortened_lookup[probe_code]:
                    shortened_lookup[probe_code][method_code] = {'begin': date_bgn, 'end': date_end, 'height': height, 'depth': depth}
                elif method_code in shortened_lookup[probe_code]:
                    print "the method code %s is already collected for %s between the dates of %s and %s" %(method_code, probe_code, date_bgn, date_end)
                    pass
        
        # iterate over the rows you plan to insert
        for row in self.new_rows:
            
            # extract the date as a dt obj
            thisdate = datetime.datetime.strptime(row[7],'%Y-%m-%d %H:%M:%S')

            # check the length of the rows in the lookup table that share a key with the probe you are on
            get_length_of_rows = len(shortened_lookup[row[6]].keys())

            # if there's only one of those, than that's the only method, so accept it as correct
            if get_length_of_rows == 1:
                correct_method = shortened_lookup[row[6]].keys()[0]
                height_valid = shortened_lookup[row[6]][correct_method]['height']

                if row[3] == correct_method and row[4] == height_valid:
                    continue
                
                else:
                    print "correcting the method to %s from %s" %(correct_method, row[3])
                    print "correcting the height to %s from %s" %(height_valid, row[4])

                    row[3] = correct_method
                    row[4] = height_valid
            
            elif get_length_of_rows > 1:
                correct_method = [x for x in shortened_lookup[row[6]].keys() if thisdate >= datetime.datetime.strptime(shortened_lookup[row[6]][x]['begin'], '%Y-%m-%d %H:%M:%S') and thisdate < datetime.datetime.strptime(shortened_lookup[row[6]][x]['end'], '%Y-%m-%d %H:%M:%S')][0]
                
                height_valid = shortened_lookup[row[6]][correct_method]['height']

                if row[3] == correct_method and row[4] == height_valid:
                    continue
                
                else:
                    print "correcting the method to %s from %s" %(correct_method, row[3])
                    print "correcting the height to %s from %s" %(height_valid, row[4])

                    row[3] = correct_method
                    row[4] = height_valid

            elif get_length_of_rows < 1:
                print "the needed probe is not listed: %s" %(row[6])
                pass


            import pdb; pdb.set_trace()
            print shortened_lookup
            #print self.new_rows

        return self.new_rows


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
    def __init__(self, attribute, startdate, enddate, server):
        self.server = server # depends on where you want to pull the probe codes from 
        self.attribute = attribute
        self.startdate = startdate
        self.enddate = enddate

    def csv_to_dict(self):
        """ update method list is imported. """

        print "this method has been depricated, please use the method table"
        pass

    def gather_methods_from_table(self):

        import form_connection as fc
        conn = fc.micro_conn('SHELDON')
        old_conn = fc.micro_conn('STEWARTIA')

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


        cursor = conn.cursor()
        old_cursor = old_conn.cursor()
        
        # collect the distinct probes from your server
        if self.server == "STEWARTIA":
            query = "select distinct probe_code from fsdbdata.dbo." + query_d[self.attribute] 
            old_cursor.execute(query)

            distinct_probes = []
            for row in old_cursor:
                distinct_probes.append(str(row[0]))
                query_string = query_string = "\', \'".join(distinct_probes)

        elif self.server == "SHELDON":
            #query = "select distinct probe_code from lterlogger_new.dbo" + query_d[self.attribute]
            query = "select distinct probe_code from lterlogger_pro.dbo" + query_d[self.attribute]
            cursor.execute(query)


            # append distinct probes to a list
            distinct_probes = []
            for row in cursor:
                distinct_probes.append(str(row[0]))
                query_string = query_string = "\', \'".join(distinct_probes)
        else:
            pass

        
        # get the limited info from the lookup table
        query = "select probe_code, date_bgn, date_end, method_code, height, depth from LTERLogger_new.dbo.method_history_daily where probe_code in (\'" + query_string +"\') and date_end >= \'" + self.startdate + "\' order by date_bgn asc"

        cursor.execute(query)


        shortened_lookup = {}
        # gather the methods by populating a dictionary called "shortened lookup"
        for row in cursor:
            probe_code = str(row[0])
            date_bgn = str(row[1])
            date_end = str(row[2])
            method_code = str(row[3])
            height = int(row[4])
            depth = str(row[5])


            if probe_code not in shortened_lookup:
                shortened_lookup[probe_code] = {method_code: {'begin': date_bgn, 'end': date_end, 'height': height, 'depth': depth}}
            
            elif probe_code in shortened_lookup:
                if method_code not in shortened_lookup[probe_code]:
                    shortened_lookup[probe_code][method_code] = {'begin': date_bgn, 'end': date_end, 'height': height, 'depth': depth}
                elif method_code in shortened_lookup[probe_code]:
                    print "the method code %s is already collected for %s between the dates of %s and %s" %(method_code, probe_code, date_bgn, date_end)
                    pass


        # iterate over the distinct probes in our set
        for each_probe in distinct_probes:

            # and over each of the methods for that probes
            for each_method in shortened_lookup[each_probe].keys():

                # get that probe method start
                import pdb; pdb.set_trace()
                method_startdate = shortened_lookup[each_probe][each_method]['begin']
                print method_startdate

                # get that method's end
                method_enddate = shortened_lookup[each_probe][each_method]['end']
                
                # new query to update the lter logger pro
                new_query = "update LTERLogger_Pro.dbo." + query_d[self.attribute] + " set " + self.attribute + "_METHOD = \'" +  str(each_method) + "\' where probe_code like \'" + each_probe + "\' and Date >= \'" + method_startdate + "\' and Date < \'" + method_enddate + "\'"
                print new_query

                cursor.execute(new_query) 

            conn.commit() 
            return shortened_lookup()
        else:
            print "nothing to commit!"

            return shortened_lookup
