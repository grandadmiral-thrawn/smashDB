#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymssql
import yaml

def micro_conn(server):

    if server == "STEWARTIA":

        # Connect to MSSQL Server
        conn = pymssql.connect(server="stewartia.forestry.oregonstate.edu:1433",
                               user="petersonf",
                               password="D0ntd1sATLGA!!",
                               )


        return conn

    elif server == "SHELDON":

        # Connect to MSSQL Server
        conn = pymssql.connect(server="sheldon.forestry.oregonstate.edu:1433",
                               user="petersonf",
                               password="D0ntd1sATLGA!!",
                               )


        return conn


def form_connection(server):
    """
    connects to the METDAT, FSDBDATA,
    or SHELDON database to gather the data
    returns a cursor (pymssql object)
    """
    if server == "STEWARTIA":

        # Connect to MSSQL Server
        conn = pymssql.connect(server="stewartia.forestry.oregonstate.edu:1433",
                               user="petersonf",
                               password="D0ntd1sATLGA!!",
                               )

        # Create a database cursor
        cursor = conn.cursor()

        return cursor

    elif server == "SHELDON":

        # Connect to MSSQL Server
        conn = pymssql.connect(server="sheldon.forestry.oregonstate.edu:1433",
                               user="petersonf",
                               password="D0ntd1sATLGA!!",
                               )

        # Create a database cursor
        cursor = conn.cursor()

        return cursor

    elif server == "FSDBDATA" or server == "METDAT":

        print("Actually, try \'STEWARTIA\' :) ")

    else:

        print("Cannot connect to server: %s") %(server)
