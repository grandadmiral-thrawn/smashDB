import argparse
import smashBosses
import smashControls
import smashWorkers
import datetime
import pymssql

"""SMASHER is the executable for the other parts of the data 'smashing'. The smasher bash program will run all of the update bosses in a row, for one day.

The SMASHER API is designed to ease the updating of FSDBDATA and LTERLogger_Pro, as well as the import of high resolution data into MS043.

FUNCTIONS:

CREATE - daily data from 5/15 minute data. Default to consolidate or add flags. Default to get methods from the method table.

READ  - Methods that exist in the historical database 
      - Diagnose missing days/values in LTERLogger_pro or LTERLogger_new

UPDATE - Update daily data or flags in the database
        - in LTERLogger_pro --> LTERLogger_pro
        - in LTERLogger_new --> LTERLogger_new
        - in FSDBDATA --> FSDBDATA
        - in LTERLogger_new --> FSDBDATA
      - Update flags and methods based on the table reference
        - In LTERLogger_pro --> FSDBDATA
        - IN FSDBDATA --> FSDBDATA
        - IN LTERLogger_new --> FSDBDATA
      - Update a single attribute between certain times (spot cleaning )

DELETE - Duplicate rows
       - A date range that needs to be re-done
       - All records of a certain method

"""

parser = argparse.ArgumentParser(description="SMASHER tool for FSDB summaries. Use the SMASHER bosses within a bash file or on the API to conduct controlled summaries. SMASHER bosses are superclasses with controlled operations and vocabularies. SMASHER controls are switch statements designed to drive SMASHER bosses. SMASHER workers are microprocesses designed to work with high-resolution data in the FSDB structure.")

# which function you are running -- REQUIRED!
parser.add_argument('crud')

# which server you are using -- REQUIRED!
parser.add_argument('server')

# which attribute you are using -- REQUIRED, can be "all"
parser.add_argument('attribute')

# startdate 
parser.add_argument('--startdate', '-sd', nargs = 1, required = False, help = " the first date, as a date-string in form YYYY-MM-DD HH:MM:SS, that you want to process ")

# enddate
parser.add_argument('--enddate', '-ed', nargs = 1, required = False, help = " the last date, as a date-string in form YYYY-MM-DD HH:MM:SS, that you want to process ")

# station
parser.add_arguement('--station', nargs = 1, required=False, help = "used with the update method to only do one station")

# # go!
args = parser.parse_args()

# Printing an intro 
print(" You are processing using the SMASHER Python toolkit. (c) MIT 2015. You have given the following information: \n")

print("~ Method: {}".format(args.crud))
print("~ Attribute: {}".format(args.attribute))
print("~ Server: {}".format(args.server))

print("~ Start Date: {}".format(args.startdate))
print("~ End Date: {}".format(args.enddate))

### CREATION METHODS ###

server = args.server

if args.crud == "CREATE" and args.attribute == "ALL" and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("AIRTEMP")
  C = smashWorkers.AirTemperature(sd, ed, server)
  nr = C.condense_data()
  print nr
  print "finished creating AIRTEMP from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("RELHUM")
  C = smashWorkers.RelHum(sd, ed, server)
  nr = C.condense_data()
  print "finished creating RELHUM from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("VPD2")
  C = smashWorkers.VPD2(sd, ed, server)
  nr = C.condense_data()
  print "finished creating VPD2 from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("DEWPT")
  C = smashWorkers.DewPoint(sd, ed, server)
  nr = C.condense_data()
  print "finished creating DEWPT from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("NR")
  C = smashWorkers.NetRadiometer(sd, ed, server)
  nr = C.condense_data()
  print "finished creating NR from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("SOLAR")
  C = smashWorkers.Solar(sd, ed, server)
  nr = C.condense_data()
  print "finished creating SOLAR from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("WSPD_SNC")
  C = smashWorkers.Sonic(sd, ed, server)
  nr = C.condense_data()
  print "finished creating WSPD_SNC from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("WSPD_PRO")
  C = smashWorkers.Wind(sd, ed, server)
  nr = C.condense_data()
  print "finished creating WSPD_PRO from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("SOILTEMP")
  C = smashWorkers.SoilTemperature(sd, ed, server)
  nr = C.condense_data()
  print "finished creating SOILTEMP from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("SOILWC")
  C = smashWorkers.SoilWaterContent(sd, ed, server)
  nr = C.condense_data()
  print "finished creating SOILWC from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("PRECIP")
  C = smashWorkers.Precipitation(sd, ed, server)
  nr = C.condense_data()
  print "finished creating PRECIP from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("SOLAR")
  C = smashWorkers.Solar(sd, ed, server)
  nr = C.condense_data()
  print "finished creating SOLAR from %s to %s" %(sd, ed)
  del C

  sd, ed = B.check_out_one_attribute("LYS")
  C = smashWorkers.SnowLysimeter(sd, ed, server)
  nr = C.condense_data()
  print "finished creating SNOW LYSIMETER from %s to %s" %(sd, ed)
  del C

elif args.crud == "CREATE" and args.attribute == "BIG4" and args.startdate == None and args.enddate == None:
  
  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("AIRTEMP")
  print "airtemp needs from %s to %s" %(sd, ed)
  C = smashWorkers.AirTemperature(sd, ed, server)
  nr = C.condense_data()
  del C

  sd, ed = B.check_out_one_attribute("RELHUM")
  print "relhum needs from %s to %s" %(sd, ed)
  C = smashWorkers.RelHum(sd, ed, server)
  nr = C.condense_data()
  del C

  sd, ed = B.check_out_one_attribute("VPD2")
  print "vapd needs from %s to %s" %(sd, ed)
  C = smashWorkers.VPD2(sd, ed, server)
  nr = C.condense_data()
  del C

  sd, ed = B.check_out_one_attribute("DEWPT")
  print "dewpoint needs from %s to %s" %(sd, ed)
  C = smashWorkers.DewPoint(sd, ed, server)
  nr = C.condense_data()
  del C


elif args.crud == "CREATE" and args.attribute in ["AIRTEMP", "airtemp", "MS04301"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("AIRTEMP")
  print sd, ed
  C = smashWorkers.AirTemperature(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["RELHUM", "relhum", "MS04302"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("RELHUM")
  print sd, ed
  C = smashWorkers.RelHum(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["PRECIP", "precip", "MS04303"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()


  sd, ed = B.check_out_one_attribute("PRECIP")
  print sd, ed
  C = smashWorkers.Precipitation(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["VPD", "vpd"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("VPD")
  print sd, ed
  C = smashWorkers.VPD(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["VPD2", "vpd2", "MS04308"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("VPD2")
  print sd, ed
  C = smashWorkers.VPD2(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["DEWPT", "dewpt", "MS04307"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("DEWPT")
  print sd, ed
  C = smashWorkers.DewPoint(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["SOLAR", "solar", "MS04305"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("SOLAR")
  print sd, ed
  C = smashWorkers.Solar(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["WSPD_SNC", "SONIC", "sonic", "wspd_snc" "MS04334"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("WSPD_SNC")
  print sd, ed
  C = smashWorkers.Sonic(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["NR", "net", "radiation", "netrad", "NETRAD", "MS04325"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("NR")
  print sd, ed
  C = smashWorkers.NetRadiometer(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["WSPD_PRO", "wspd_pro", "WIND","wind", "PROP", "prop", "MS04304"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("WSPD_PRO")
  print sd, ed
  C = smashWorkers.Wind(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["SOILTEMP", "soiltemp", "MS04321"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("SOILTEMP")
  print sd, ed
  C = smashWorkers.SoilTemperature(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["SOILWC", "SWC", "soiltemp","swc","MS04323"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("SOILWC")
  print sd, ed
  C = smashWorkers.SoilWaterContent(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["LYS", "SNOWMELT", "lys", "snowmelt", "MS04309"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("LYS")
  print sd, ed
  C = smashWorkers.SnowLysimeter(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["SNOWDEPTH", "SNOW", "snowdepth","snow","MS04310"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("SNOWDEPTH")
  print sd, ed
  C = smashWorkers.SnowDepth(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

else: 
  pass

### CREATION METHODS FOR A SPECIFIC RANGE ###

if args.crud == "CREATE" and args.startdate != None and args.enddate != None:

  sd = args.startdate[0]
  ed = args.enddate[0]

  if args.attribute in ["SNOWDEPTH", "SNOW", "snowdepth","snow","MS04310"]:

    C = smashWorkers.SnowDepth(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["LYS", "SNOWMELT", "lys", "snowmelt", "MS04309"]:

    C = smashWorkers.SnowLysimeter(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["AIRTEMP", "airtemp", "MS04301"]:

    C = smashWorkers.AirTemperature(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["SOILWC", "SWC", "soiltemp","swc","MS04323"]:

    C = smashWorkers.SoilWaterContent(sd, ed, server)
    nr = C.condense_data()
    del C

  elif args.attribute in ["SOILTEMP", "soiltemp", "MS04321"]:

    C = smashWorkers.SoilTemperature(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["WSPD_PRO", "wspd_pro", "WIND","wind", "PROP", "prop", "MS04304"]:

    C = smashWorkers.Wind(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["NR", "net", "radiation", "netrad", "NETRAD", "MS04325"]:

    C = smashWorkers.NetRadiometer(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["VPD2", "vpd2", "MS04308"]:

    C = smashWorkers.VPD2(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["WSPD_SNC", "SONIC", "sonic", "wspd_snc" "MS04334"]:

    C = smashWorkers.Sonic(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["SOLAR", "solar", "MS04305"]:

    C = smashWorkers.Solar(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["DEWPT", "dewpt", "MS04307"]:

    C = smashWorkers.DewPoint(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["PRECIP", "precip", "MS04303"]:

    C = smashWorkers.Precipitation(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

  elif args.attribute in ["RELHUM", "relhum", "MS04302"]:

    C = smashWorkers.RelHum(sd, ed, server)
    nr = C.condense_data()
    for row in nr:
      print row
    del C

### READ METHODS ###

if args.crud == "READ" and args.attribute == "ALL":
  U = smashControls.MethodControl(args.server)
  U.process_db()
  print "see error.csv for a list of non-agreeing method codes"

if args.crud == "READ" and args.attribute == "ALLHR":
  U = smashControls.HRMethodControl(args.server)
  U.process_db()
  print "see error.csv for a list of non-agreeing method codes"



### DELETION METHODS ###
if args.crud == 'DELETE' and args.station == None:

  print(" Deleting all data from LTERLogger_Pro for your attribute! ")

  if args.attribute == None:
    print("I cannot process this command without an attribute to delete. Try again :)")

  else:

    deleteable = args.attribute[0]

    if deleteable == "AIRTEMP" or deleteable == "MS04301":
        full_name = "LTERLogger_Pro.dbo.MS04301"
    elif deleteable == "RELHUM" or deleteable == "MS04302":
        full_name = "LTERLogger_Pro.dbo.MS04302"
    elif deleteable == "WSPD_PRO" or deleteable == "MS04304":
        full_name = "LTERLogger_Pro.dbo.MS04304"
    elif deleteable == "SOLAR" or deleteable == "MS04305":
        full_name = "LTERLogger_Pro.dbo.MS04305"
    elif deleteable == "PRECIP" or deleteable == "MS04303":
        full_name = "LTERLogger_Pro.dbo.MS04303"
    elif deleteable == "NR" or deleteable == "MS04325":
        full_name = "LTERLogger_Pro.dbo.MS04325"
    elif deleteable == "WSPD_SNC" or deleteable == "MS04324":
        full_name = "LTERLogger_Pro.dbo.MS04324"
    elif deleteable == "SOILWC" or deleteable == "MS04323":
        full_name = "LTERLogger_Pro.dbo.MS04323"
    elif deleteable == "SOILTEMP" or deleteable == "MS04321":
        full_name = "LTERLogger_Pro.dbo.MS04321"
    elif deleteable == "PAR" or deleteable == "MS04322":
        full_name = "LTERLogger_Pro.dbo.MS04322"
    elif deletable == "LYS" or deleteable == "MS04309":
        full_name = "LTERLogger_Pro.dbo.MS04309"
    else:
        print("need to create a method to delete {}".format(deleteable))

    query = "delete from " + full_name + " where date >= \'" + args.startdate[0] + "\'"

    conn = fc.micro_conn("SHELDON")

    cur = conn.cursor()

    cur.execute(query)

    conn.commit()

    print "you have deleted from %s following %s" %(full_name, args.startdate[0])

elif args.crud == "DELETE" and args.station != None:
  station = args.station[0]

  if args.attribute == None:
    print("I cannot process this command without an attribute to delete. Try again :)")

  else:

    print "DELETING FOR STATION: %s" %(station)
    deleteable = args.attribute[0]

    if deleteable == "AIRTEMP" or deletable == "MS04301":
        full_name = "LTERLogger_Pro.dbo.MS04301"
    elif deleteable == "RELHUM" or deleteable == "MS04302":
        full_name = "LTERLogger_Pro.dbo.MS04302"
    elif deleteable == "WSPD_PRO" or deleteable == "MS04304":
        full_name = "LTERLogger_Pro.dbo.MS04304"
    elif deleteable == "SOLAR" or deleteable == "MS04305":
        full_name = "LTERLogger_Pro.dbo.MS04305"
    elif deleteable == "PRECIP" or deleteable == "MS04303":
        full_name = "LTERLogger_Pro.dbo.MS04303"
    elif deleteable == "NR" or deleteable == "MS04325":
        full_name = "LTERLogger_Pro.dbo.MS04325"
    elif deleteable == "WSPD_SNC" or deleteable == "MS04324":
        full_name = "LTERLogger_Pro.dbo.MS04324"
    elif deleteable == "SOILWC" or deleteable == "MS04323":
        full_name = "LTERLogger_Pro.dbo.MS04323"
    elif deleteable == "SOILTEMP" or deleteable == "MS04321":
        full_name = "LTERLogger_Pro.dbo.MS04321"
    elif deleteable == "PAR" or deleteable == "MS04322":
        full_name = "LTERLogger_Pro.dbo.MS04322"
    elif deletable == "LYS" or deleteable == "MS04309":
        full_name = "LTERLogger_Pro.dbo.MS04309"
    else:
        print("need to create a method to delete {}".format(deleteable))

    query = "delete from " + full_name + " where date >= \'" + args.startdate[0] + "\' and sitecode like \'" + station + "\'"

    conn = fc.micro_conn("SHELDON")

    cur = conn.cursor()

    cur.execute(query)

    conn.commit()

    print "you have deleted from %s following %s on the station %s" %(full_name, args.startdate[0], station)


### UPDATE METHODS ####

if args.crud == "UPDATE" and args.attribute == "ALL" and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()


  # AIR TEMPERATURE
  sd, ed = B.check_out_one_attribute("AIRTEMP")
  C = smashWorkers.AirTemperature(sd, ed, server)
  nr = C.condense_data()
  print "finished creating AIRTEMP from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  print "checking that the methods are updated"
  D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for AIRTEMP" %(sd, ed)
  del C
  del D

  # RELHUM
  sd, ed = B.check_out_one_attribute("RELHUM")
  C = smashWorkers.RelHum(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating RELHUM from %s to %s" %(sd, ed)
  del C
  del D


  # VPD
  sd, ed = B.check_out_one_attribute("VPD2")
  C = smashWorkers.VPD2(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating VPD2 from %s to %s" %(sd, ed)
  del C

  # Dew point
  sd, ed = B.check_out_one_attribute("DEWPT")
  C = smashWorkers.DewPoint(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating DEWPT from %s to %s" %(sd, ed)
  del C


  # Net Radiometer
  sd, ed = B.check_out_one_attribute("NR")
  C = smashWorkers.NetRadiometer(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating NR from %s to %s" %(sd, ed)
  del C
  del D

  # SOLAR
  sd, ed = B.check_out_one_attribute("SOLAR")
  C = smashWorkers.Solar(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating SOLAR from %s to %s" %(sd, ed)
  del C
  del D

  # SONIC
  sd, ed = B.check_out_one_attribute("WSPD_SNC")
  C = smashWorkers.Sonic(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating WSPD_SNC from %s to %s" %(sd, ed)
  del C

  # PROP
  sd, ed = B.check_out_one_attribute("WSPD_PRO")
  C = smashWorkers.Wind(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating WSPD_PRO from %s to %s" %(sd, ed)
  del C
  del D

  # SOIL TEMP
  sd, ed = B.check_out_one_attribute("SOILTEMP")
  C = smashWorkers.SoilTemperature(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  #D.update_the_db_methods()
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db()
  print "finished creating SOILTEMP from %s to %s" %(sd, ed)
  del C


  # SOIL WC
  sd, ed = B.check_out_one_attribute("SOILWC")
  C = smashWorkers.SoilWaterContent(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  #D.update_the_db_methods()
  D.update_the_db()
  print "finished creating SOILWC from %s to %s" %(sd, ed)
  del C
  del D


  # PRECIP
  sd, ed = B.check_out_one_attribute("PRECIP")
  C = smashWorkers.Precipitation(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating PRECIP from %s to %s" %(sd, ed)
  del C


  # SOLAR
  sd, ed = B.check_out_one_attribute("SOLAR")
  C = smashWorkers.Solar(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating SOLAR from %s to %s" %(sd, ed)
  del C
  del D

  # SNOW LYSIMETER
  sd, ed = B.check_out_one_attribute("LYS")
  C = smashWorkers.SnowLysimeter(sd, ed, server)
  nr = C.condense_data()
  print "checking that the methods are updated"
  D = smashBosses.UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "finished creating SNOW LYSIMETER from %s to %s" %(sd, ed)
  del C
    
  print("Updates come from the source of {}".format(args.server))


if args.crud == "UPDATE" and args.attribute in ["AIRTEMP","MS04301"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("AIRTEMP")
  C = smashWorkers.AirTemperature(sd, ed, server)
  nr = C.condense_data()
  print "finished creating AIRTEMP from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  D = UpdateBoss(C, nr)
  if args.station != None:
    D.only_one_station(args.station[0])
  else:
    pass
  D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for AIRTEMP" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["RELHUM","MS04302"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("RELHUM")
  C = smashWorkers.RelHum(sd, ed, server)
  nr = C.condense_data()
  print "finished creating RELHUM from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for RELHUM" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["PRECIP","MS04303"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("PRECIP")
  C = smashWorkers.Precipitation(sd, ed, server)
  nr = C.condense_data()
  print "finished creating PRECIP from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for PRECIP" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["DEWPT","MS04307"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("DEWPT")
  C = smashWorkers.DewPoint(sd, ed, server)
  nr = C.condense_data()
  print "finished creating Dewpoint from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Dewpoint" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["VPD2", "MS04308"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("VPD2")
  C = smashWorkers.VPD2(sd, ed, server)
  nr = C.condense_data()
  print "finished creating calculated Vapor Pressure Defecit from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Vapor Pressure Defecit" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["SOLAR", "MS04305"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("SOLAR")
  C = smashWorkers.Solar(sd, ed, server)
  nr = C.condense_data()
  print "finished creating Solar Radiation from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Solar Radiation" %(sd, ed)
  del C
  del D


elif args.crud == "UPDATE" and args.attribute in ["SOILTEMP", "MS04321"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("SOILTEMP")
  C = smashWorkers.SoilTemperature(sd, ed, server)
  nr = C.condense_data()
  print "finished creating Soil Temperature from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  #D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Soil Temperature" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["SOILWC", "MS04323"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("SOILWC")
  C = smashWorkers.SoilTemperature(sd, ed, server)
  nr = C.condense_data()
  print "finished creating Soil Water Content from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  #D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Soil Water Content" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["NR", "MS04325"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("NR")
  C = smashWorkers.NetRadiometer(sd, ed, server)
  nr = C.condense_data()
  print "finished creating Net Radiometer from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  #D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Net Radiometer" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["WSPD_PRO", "MS04304"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("WSPD_PRO")
  C = smashWorkers.Wind(sd, ed, server)
  nr = C.condense_data()
  print "finished creating Net Radiometer from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  #D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Net Radiometer" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["WSPD_SNC","MS04334"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("WSPD_SNC")
  C = smashWorkers.Sonic(sd, ed, server)
  nr = C.condense_data()
  print "finished creating Sonic from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  #D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Sonic" %(sd, ed)
  del C
  del D

elif args.crud == "UPDATE" and args.attribute in ["LYS","MS04309"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("LYS")
  C = smashWorkers.Sonic(sd, ed, server)
  nr = C.condense_data()
  print "finished creating Snowmelt from %s to %s" %(sd, ed)
  D = smashBosses.UpdateBoss(C, nr)
  print "checking that the methods are updated"
  #D.update_the_db_methods()
  D.update_the_db()
  print "database updated from %s to %s for Snowmelt" %(sd, ed)
  del C
  del D