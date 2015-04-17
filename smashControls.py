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
            # args is the probe code

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



