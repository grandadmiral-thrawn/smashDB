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


class UpdateBoss(object):
    """ The UpdateBoss updates an attribute based on the times you specify"""

    def __init__(self, Worker):
        """ Pass in a worker (contains start date, end date, and attribute) to update"""
        self.Worker = Worker
        self.new_rows = Worker.condense_data()

        # gets the id name of the row based on the first row
        sample_row = self.new_rows[0]

        # if the entity number is less than 10 put a 0 in front of it
        if sample_row[1] < 10:
            entity_string = "0" + str(sample_row[1])

        # if the entity number is > than 10 then just run as is
        elif sample_row[1] >= 10:
            entity_string = str(sample_row[1])

        # the table name is these two things put together
        self.table = "MS043" + entity_string

    def only_one_station(self, station):
        """ If we want to parse incoming data to just one station, use this -- we generated all the rows but only return 1 particular station"""
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
        
        print("Updating your heights, depths, and methods prior to insertion in the db!")

        shortened_lookup = {}

        # form a new connection (we need this because we need the conn object to commit)
        import form_connection as fc
        conn = fc.micro_conn('SHELDON')

        # get all the probe_codes we have and dates we have
        probe_codes_ordered = [x[6] for x in self.new_rows]
        dates_ordered = [x[7] for x in self.new_rows]

        # get the unique entries of probe codes
        probe_string = self.f5(probe_codes_ordered)

        # make it into a string that the query can injest
        query_string = "\', \'".join(probe_string)

        # get the limited info from the lookup table -- we only need to look up the relevant probes and dates
        query = "select probe_code, date_bgn, date_end, method_code, height, depth from LTERLogger_new.dbo.method_history_daily where probe_code in (\'" + query_string +"\') and date_end >= \'" + datetime.datetime.strftime(self.Worker.daterange.dr[0], '%Y-%m-%d %H:%M:%S') + "\' order by date_bgn asc"

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
            try:
                get_length_of_rows = len(shortened_lookup[row[6]].keys())
            except KeyError:
                print "the probe %s is not listed" %(row[6])
                continue

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

        return self.new_rows


    def update_the_db(self):
        """ Updates LTER Logger Pro-- NOT LTER LOGGER NEW! -- currently as of 04-20-3015 its empty so I can't check it for pre-existing values without an error! """
        print("This will update the LTERLogger_Pro database")

        # form a new connection (we need this because we need the conn object to commit)
        import form_connection as fc
        conn = fc.micro_conn('SHELDON')

        # keep the tuples from each object. I.e. self might be an AirTemperature, a Sonic, etc.
        new_tuples = [tuple(x) for x in self.new_rows]
        
        cursor = conn.cursor()
        # get_the_column_names
        cursor.execute("select column_name from LTERLogger_Pro.information_schema.columns where table_name like \'" + self.table + "\' and table_schema like 'dbo'")

        # create a list from those columns, which is used to generate the sql later. For net radiation we have one strange additional column in LTERLogger_pro that is manually removed here.
        nr = []
        for row in cursor:
            nr.append(str(row[0]))

        if self.table != "MS04325":
            column_string = " ,".join(nr[:-1])
        elif self.table == "MS04325":
            column_string = " ,".join(nr)


        # the simplest case of data such as airtemp, relhum, dewpoint, soil temp, and soil wc
        if self.table in ["MS04301", "MS04302", "MS04307", "MS04321", "MS04323"]:
        
            # 'MS043',21, site_code, method_code, int(height), "1D", probe_code, datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S'), mean_valid_obs, daily_flag, max_valid_obs, max_flag[0], datetime.datetime.strftime(max_valid_time[0], '%H%M'), min_valid_obs, min_flag[0], datetime.datetime.strftime(min_valid_time[0], '%H%M'), "NA", self.server
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table +" (" + column_string + ")  VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %s, %d, %s, %s, %s, %s)", new_tuples)

            conn.commit()

        # vpd is complicated, there is a long string for some older data and a short string for some newer data. 
        elif self.table in "MS04308":
            print "processing vpd!"
            print "structure in LTERLogger_pro is: DBCODE, ENTITY, SITECODE, VPD_METHOD, HEIGHT, QC_LEVEL, PROBE_CODE, DATE, VPD_MEAN_DAY, VPD_MEAN_FLAG, VPD_MAX_DAY, VPD_MAX_FLAG, VPD_MAXTIME, VPD_MIN_DAY, VPD_MIN_FLAG, VPD_MINTIME, VAP_MEAN_DAY, VAP_MEAN_FLAG, VAP_MAX_DAY, VAP_MAX_FLAG,    VAP_MIN_DAY, VAP_MIN_FLAG, EVENT_CODE, DB_TABLE"
            try: 
                cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ") VALUES (%s,%d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %s, %d, %s, %s, %d, %s, %d, %s, %d, %s, %s, %s)", new_tuples)
            except Exception:
                print "First insertion attempt failed, trying a longer insertion for VPD et al."
                try:
                    cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ") VALUES (%s,%d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %s, %d, %s, %s, %d, %s, %d, %s, %d, %s, %d, %s, %d, %s,%d, %s, %s, %s)", new_tuples)
                except Exception:
                    print "Second insertion attempt failed, trying an even longer insertion for VPD et al."
            
                    try:
                        cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ") VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", new_tuples)
                    except Exception:
                        print "Please edit the database insertion string for VPD in smashBosses. Check information schema.columns and the first row in MS04308"

            conn.commit()

        elif self.table in "MS04305":

            # DBCODE, ENTITY, SITECODE, SOLAR_METHOD, HEIGHT, QC_LEVEL, PROBE_CODE, DATE , SOLAR_TOT_DAY, SOLAR_TOT_FLAG, SOLAR_MEAN_DAY, SOLAR_MEAN_FLAG, SOLAR_MAX_DAY  SOLAR_MAX_FLAG, SOLAR_MAXTIME, EVENT_CODE, DB_TABLE, ID
            for each_tuple in new_tuples:

                print each_tuple

            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table +" (" + column_string + ")  VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %d, %s, %s, %s, %s)", new_tuples)
            conn.commit()
            
        elif self.table in "MS04303":
            # fixed to not have two outputs.
            # 'MS043', 3, 'PRIMET', 'PPT108', 100, '2D', 'PPTPRI01', '2015-01-01 00:00:00', 0.0, 'A', 'NA', 'SHELDON_LTERLogger_PRO_MS04313'
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ") VALUES( %s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %s, %s)", new_tuples)
            conn.commit()

        elif self.table in "MS04309":

           cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ") VALUES( %s, %d, %s, %s, %s, %s, %s, %d, %s, %d , %s, %s, %s)", new_tuples)

           conn.commit()

        elif self.table in "MS04322":
            
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ") VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %d, %s, %s)", new_tuples)
            conn.commit()

        elif self.table in "MS04325":
            
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ")  VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", new_tuples)

            conn.commit()

        elif self.table in "MS04304":
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ")  VALUES(%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %d,%s ,%s, %d, %s, %d, %s, %d,  %s, %d, %s, %d, %s, %d, %s, %d, %s, %d, %s, %d, %s, %d, %s, %s, %s)", new_tuples)

            conn.commit()
        
        elif self.table in "MS04334":
            cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table + " (" + column_string + ")  VALUES( %s,  %d,  %s, %s,  %d,  %s, %s,  %s,    %d,  %s,  %d,  %s,  %d, %s,  %d, %s,    %d, %s,   %d, %s, %d,   %s,   %d,  %s, %d   %s,  %d, %s,  %s,  %s)",new_tuples)

            conn.commit()


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

        query_m = {'AIRTEMP': 'MS00101',
                    'RELHUM': 'MS00102',
                    'PRECIP': 'MS00103',
                    'WSPD_PRO': 'MS00104',
                    'SOLAR': 'MS00105',
                    'DEWPT': 'MS00107',
                    'VPD': 'MS00108',
                    'LYS': 'MS00109',
                    'NR': 'MS00125',
                    'WSPD_SNC': 'MS00124',
                    'SOILTEMP': 'MS00121',
                    'SOILWC': 'MS00123',
                    'PAR': 'MS00122'}

        cursor = conn.cursor()
        old_cursor = old_conn.cursor()
        
        # collect the distinct probes from your server
        if self.server == "STEWARTIA":
            
            ## COMMENT THIS LINE IN FOR MS001
            # query = "select distinct probe_code from fsdbdata.dbo." + query_m[self.attribute] 

            ## COMMENT THIS LINE IN FOR MS043
            query = "select distinct probe_code from fsdbdata.dbo." + query_d[self.attribute] 
            old_cursor.execute(query)

            distinct_probes = []
            
            for row in old_cursor:
                distinct_probes.append(str(row[0]))
                query_string = query_string = "\', \'".join(distinct_probes)

        elif self.server == "SHELDON":

            ## COMMENT THIS LINE IN FOR LTERLogger_new
            #query = "select distinct probe_code from lterlogger_new.dbo" + query_d[self.attribute]

            ## COMMENT THIS LINE IN FOR LTERLogger_pro
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
