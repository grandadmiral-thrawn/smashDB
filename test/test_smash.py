#!/usr/bin/env python
"""
Smasher unit tests

"""
import os
import platform
import csv
import time
import datetime
import itertools
import sys
import math
import types
from pprint import pprint

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(HERE, os.pardir))

from smasher import form_connection, smashWorkers

def test_conn():
  c = form_connection.micro_conn("STEWARTIA")
  assert(type(c) != types.StringType)

def test_cur():
  c = form_connection.form_connection("SHELDON")
  sql = "select top 1 airtemp_mean from LTERLogger_pro.dbo.MS04311"
  c.execute(sql)
  res = c.fetchone()
  string_res = str(res)
  assert(type(string_res) == types.StringType)

def test_daterange():
  startdate = '2015-01-01 00:00:00'
  enddate = '2015-01-02 00:00:00'

  compare_s = datetime.datetime(2015,1,1,0,0)
  compare_e = datetime.datetime(2015,1,2,0,0)
  A = smashWorkers.DateRange(startdate,enddate)
  a_range = A.dr

  assert(type(a_range) == types.ListType)
  assert(2 == len(a_range))

def test_Air():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.AirTemperature(sd, ed, "SHELDON")
  assert(A.entity == 1)
  assert(type(A.od.keys())==types.ListType) 
  x_1 = 'AIRCEN04'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd

  assert(datetime.datetime.strftime(x, '%Y-%m-%d %H:%M:%S')==sd)

def test_Rel():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.RelHum(sd, ed, "SHELDON")
  assert(A.entity == 2)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'RELCEN04'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x, '%Y-%m-%d %H:%M:%S')==sd)

def test_Dew():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.DewPoint(sd, ed, "SHELDON")
  assert(A.entity == 7)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'DEWCEN04'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)

def test_VPD():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.VPD2(sd, ed, "SHELDON")
  assert(A.entity == 8)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'VPDCEN04'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)

def test_PAR():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.PhotosyntheticRad(sd, ed, "STEWARTIA")
  assert(A.entity == 22)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'PARCEN01'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)

def test_ST():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.SoilTemperature(sd, ed, "SHELDON")
  assert(A.entity == 21)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'SOICEN01'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)


def test_SW():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.SoilWaterContent(sd, ed, "SHELDON")
  assert(A.entity == 23)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'SWCPRI01'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)


def test_pre():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.Precipitation(sd, ed, "SHELDON")
  assert(A.entity == 3)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'PPTCEN01'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)

def test_sno():
  sd = '2015-04-21 00:00:00'
  ed = '2015-04-22 00:00:00'
  A = smashWorkers.SnowLysimeter(sd, ed, "SHELDON")
  assert(A.entity == 9)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'LYSCEN01'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)

def test_solar():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.Solar(sd, ed, "SHELDON")
  assert(A.entity == 5)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'RADCEN01'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)

def test_net():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.NetRadiometer(sd, ed, "SHELDON")
  assert(A.entity == 25)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'RADVAN02'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)


def test_wind():
  sd = '2015-02-01 00:00:00'
  ed = '2015-02-02 00:00:00'
  A = smashWorkers.NetRadiometer(sd, ed, "SHELDON")
  assert(A.entity == 4)
  assert(type(A.od.keys())==types.ListType)
  x_1 = 'WNDPRI01'
  x = sorted(A.od[x_1].keys())[1]
  print x
  print sd
  assert(datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')==sd)

if __name__ == "__main__":
  test_conn()
  test_cur()
  test_daterange()
  test_Air()
  test_Rel()
  test_ST()
  test_Dew()
  test_PAR()
  test_SW()
  test_pre()
  test_solar()
  test_net()
