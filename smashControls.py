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

        elif self.attribute == "RELHUM":
            self.Worker = smashWorkers.RelHum(startdate, enddate, server, *args)

        elif self.attribute == "DEWPT":
            self.Worker = smashWorkers.DewPoint(startdate, enddate, server, *args)

        elif self.attribute == "VPD":
            self.Worker = smashWorkers.VPD(startdate, enddate, server, *args)

        elif self.attribute == "SOILTEMP":
            self.Worker = smashWorkers.SoilTemperature(startdate, enddate, server, *args)

        elif self.attribute == "SOILWC":
            self.Worker = smashWorkers.SoilWaterContent(startdate, enddate, server, *args)

        elif self.attribute == "WSPD_PRO":
            self.Worker = smashWorkers.Wind(startdate, enddate, server, *args)

        elif self.attribute == "PRECIP":
            self.Worker = smashWorkers.Precip(startdate, enddate, server, *args)

        elif self.attribute == "SOLAR":
            self.Worker = smashWorkers.Solar(startdate, enddate, server, *args)

        else:
            pass
