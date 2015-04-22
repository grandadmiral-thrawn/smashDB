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
                my_new_rows = myWorker.Worker.condense_data(args[0])
            
            else:
                my_new_rows = myWorker.Worker.condense_data()

            
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

        # if it is VPD but we are using the old method, then go ahead and make a worker
        elif self.attribute == "VPD" and self.vpd == "off":
            self.myWorker = smashControls.Worker(self.attribute, self.startdate, self.enddate, self.server)
            self.new_rows = self.myWorker.Worker.condense_data()

        # if it is VPD and we are using the new method, then we assign that to the worker
        elif self.attribute == "VPD" and self.vpd == "on":
            self.myWorker = smashControls.VaporControl(self.startdate, self.enddate, self.server)
            self.new_rows = self.myWorker.Worker.compute_shared_probes()
        
        else: 
            pass
        
        # name of the table
        self.table = 'MS043' + self.myWorker.Worker.entity

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

        # make 'em ' into a list
        nr = []
        for row in cursor:
            nr.append(str(row[0]))

        column_string = " ,".join(nr[:-1])


                
        # for some reason it likes to use "d" which is not the usual "f" formatter for doing floats. Yes, a total mystery. Example of working prototype        
        # cursor.executemany("insert into LTERLogger_Pro.dbo.Test (CreateTime, DValue) VALUES (%s, %d)", [('2015-01-02 00:00:00', 17.343823), ('2015-01-03 00:00:00', 18.238123), ('2015-01-04 00:00:00', 23.328)])


        cursor.executemany("insert into LTERLogger_Pro.dbo." + self.table +" (" + column_string + ")  VALUES (%s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %s, %d, %s, %s, %s, %s)", new_tuples)

        conn.commit()
