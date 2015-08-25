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
import form_connection

def test_conn():
    c = form_connection.micro_conn("STEWARTIA")
    assert(type(c) != types.StringType)

def test_cur():
    c = form_connection.form_connection("SHELDON")
    assert(type(c) != types.StringType)

def test_daterange():
    startdate = '2015-01-01 00:00:00'
    enddate = '2015-01-02 00:00:00'

    compare_s = datetime.datetime(2015,1,1,0,0)
    compare_e = datetime.datetime(2015,1,2,0,0)
    A = smashWorkers.DateRange(startdate,enddate)
    a_range = A.dr

    assert(type(a_range) == types.ListType)
    assert(2 == len(a_range))


# def test_method_reader():
#     smashWorkers.MethodTableReader
if __name__ == "__main__":
    test_conn()
    test_cur()
    test_daterange()
