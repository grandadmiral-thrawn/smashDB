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

    ''' Workers do the methods specific to each attribute'''

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


        
class Boss(Worker):
    ''' Boss controls which attributes should be computed'''

    def __init__(self):
        # limits file contains
        limits = yaml.load(open('LIMITED.yaml','rb'))

        super(Boss, self).__init__(attribute)

class Subscriber(object):

    ''' Subscriber controls how outputs should be processed'''
