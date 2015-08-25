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

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(HERE, os.pardir))

import smashWorkers 

def test_daterange():
    startdate = '2015-01-01 00:00:00'
    enddate = '2015-01-02 00:00:00'

    compare_s = datetime.datetime(2015,1,1,0,0)
    compare_e = datetime.datetime(2015,1,2,0,0)
    A = smashWorkers.DateRange(startdate,enddate)
    a_range = A.dr

    assert(type(a_range) == types.ListType)
    assert(2 == len(a_range))

# def test_drange():
#     expected = [
#         "2012-10-10 01:01:00",
#         "2012-10-20 01:01:00",
#         "2012-10-30 01:01:00",
#         "2012-11-09 01:01:00",
#         "2012-11-19 01:01:00",
#         "2012-11-29 01:01:00",
#         "2012-12-09 01:01:00",
#         "2012-12-19 01:01:00",
#         "2012-12-29 01:01:00",
#         "2013-01-08 01:01:00",
#         "2013-01-18 01:01:00",
#         "2013-01-28 01:01:00",
#         "2013-02-07 01:01:00",
#         "2013-02-17 01:01:00",
#         "2013-02-27 01:01:00",
#         "2013-03-09 01:01:00",
#         "2013-03-19 01:01:00",
#         "2013-03-29 01:01:00",
#         "2013-04-08 01:01:00",
#         "2013-04-18 01:01:00",
#         "2013-04-28 01:01:00",
#         "2013-05-08 01:01:00",
#         "2013-05-18 01:01:00",
#         "2013-05-28 01:01:00",
#         "2013-06-07 01:01:00",
#         "2013-06-17 01:01:00",
#         "2013-06-27 01:01:00",
#         "2013-07-07 01:01:00",
#         "2013-07-17 01:01:00",
#         "2013-07-27 01:01:00",
#         "2013-08-06 01:01:00",
#         "2013-08-16 01:01:00",
#         "2013-08-26 01:01:00",
#         "2013-09-05 01:01:00",
#         "2013-09-15 01:01:00",
#         "2013-09-25 01:01:00",
#         "2013-10-05 01:01:00",
#     ]

#     start = datetime.datetime(2012, 10, 10, 1, 1)
#     stop = datetime.datetime(2013, 10, 10, 1, 1)
#     step = datetime.timedelta(days=10)
#     drange_generator = smashWorkers.DateRangedrange(start, stop, step)
# #     assert(type(drange_generator) == types.GeneratorType)
#     for i, d in enumerate(drange_generator):
#         assert(str(d) == expected[i])
#     assert(i+1 == len(expected))

if __name__ == "__main__":
    test_daterange()
