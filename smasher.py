import argparse
import smashBosses
import smashControls
import smashWorkers
import datetime
import pymssql
import csv

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
parser.add_argument('--station', nargs = 1, required=False, help = "used with the update method to only do one station")

# csv
parser.add_argument('--csv', nargs = 1, required = False, help = "used with the create or update methods to generate a csv output")

# # go!
args = parser.parse_args()

# Printing an intro 
print(" You are processing using the SMASHER Python toolkit. (c) MIT 2015. You have given the following information: \n")

print("~ Method: {}".format(args.crud))
print("~ Attribute: {}".format(args.attribute))
print("~ Server: {}".format(args.server))

print("~ Start Date: {}".format(args.startdate))
print("~ End Date: {}".format(args.enddate))
print("~ Station: {}".format(args.station))
print("~ Creating CSV?: {}".format(args.csv))


# station and server names
server = args.server

if args.station != None or args.station != []:
  station = args.station[0]
else:
  station = None

# csv name, need to separate the arg here
if args.csv == None or args.csv == []:
  mycsv = None
elif args.csv != None:
  mycsv = args.csv[0]

### CSV NAMING ###

if mycsv == "TRUE" and station != None:
  csv_filename = 'daily_smash_for_' + args.attribute + station + '_updated_on_' + datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d') + '.csv' 
elif mycsv == "TRUE" and station == None:
  csv_filename = 'daily_smash_for_' + args.attribute + '_updated_on_' + datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d') + '.csv' 
else:
  pass


### CREATION METHODS ###

if args.crud == "CREATE" and args.attribute == "ALL" and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    # if no station is specified, do this for all data
    B.build_queries()
  
  # otherwise, just do it for the specific station
  else:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  print "creating all data for all sites for your range. To spare memory, please generate csvs by attribute... "

  ## AIR TEMPERATURE
  try:
    sd, ed = B.check_out_one_attribute("AIRTEMP")
    C = smashWorkers.AirTemperature(sd, ed, server)
    nr = C.condense_data()

    # prints only the station of interest
    if station != None:
      print "this is the case"
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass

    else:
      print "no this is the case"
      print nr

    print "finished creating AIRTEMP from %s to %s" %(sd, ed)
    del C

  except Exception:
    print "could not generate AIRTEMP from %s to %s" %(sd, ed)

  ## RELHUM
  try: 
    sd, ed = B.check_out_one_attribute("RELHUM")
    C = smashWorkers.RelHum(sd, ed, server)
    nr = C.condense_data()

    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr

    print "finished creating RELHUM from %s to %s" %(sd, ed)
    del C

  except Exception:
    print "could not generate RELHUM from %s to %s" %(sd, ed)

  ## VPD 2 - the CALCULATED ONE
  try:
    sd, ed = B.check_out_one_attribute("VPD2")
    C = smashWorkers.VPD2(sd, ed, server)
    nr = C.condense_data()

    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr

    print "finished creating VPD2 from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not generate VPD2 from %s to %s" %(sd, ed)

  ## DEWPOINT
  try:
    sd, ed = B.check_out_one_attribute("DEWPT")
    C = smashWorkers.DewPoint(sd, ed, server)
    nr = C.condense_data()

    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr

    print "finished creating DEWPT from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not generate DEWPT from %s to %s" %(sd, ed)


  ## NET RADIOMETER
  try:
    sd, ed = B.check_out_one_attribute("NR")
    C = smashWorkers.NetRadiometer(sd, ed, server)
    nr = C.condense_data()

    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr

    print "finished creating NR from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not generate NR from %s to %s" %(sd, ed)

  ## SOLAR THE OLD ONE
  try:
    sd, ed = B.check_out_one_attribute("SOLAR")
    C = smashWorkers.Solar(sd, ed, server)
    nr = C.condense_data()

    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr

    print "finished creating SOLAR from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not generate SOLAR from %s to %s" %(sd, ed)
  
  ## SONIC ANEMOMETER
  try:
    sd, ed = B.check_out_one_attribute("WSPD_SNC")
    C = smashWorkers.Sonic(sd, ed, server)
    nr = C.condense_data()

    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr

    print "finished creating WSPD_SNC from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not generate WSPD_SNC from %s to %s" %(sd, ed)


  ## PROP ANEMOMETER
  try:
    sd, ed = B.check_out_one_attribute("WSPD_PRO")
    C = smashWorkers.Wind(sd, ed, server)
    nr = C.condense_data()
    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr
    print "finished creating WSPD_PRO from %s to %s" %(sd, ed)
    del C

  except Exception:
    print "could not generate WSPD_PRO from %s to %s" %(sd, ed)
    print "trying the new method for wind speed" 
    
    try: 
      sd, ed = B.check_out_one_attribute("WSPD_PRO2")
      C = smashWorkers.Wind2(sd, ed, server)
      nr = C.condense_data()

      # prints only the station of interest
      if station != None:
        for each_list in nr:
          if each_list[2] == station:
            print each_list
          else:
            pass
      else:
        print nr
        print "finished creating WSPD_PRO2 from %s to %s" %(sd, ed)
        del C
    except Exception:
      print "Could not make windspeed at all from %s to %s" %(sd, ed)

  ## SOIL TEMPERATURE
  try:
    sd, ed = B.check_out_one_attribute("SOILTEMP")
    C = smashWorkers.SoilTemperature(sd, ed, server)
    nr = C.condense_data()
    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr
    print "finished creating SOILTEMP from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not make SOILTEMP from %s to %s" %(sd, ed)

  ## SOIL WC
  try:
    sd, ed = B.check_out_one_attribute("SOILWC")
    C = smashWorkers.SoilWaterContent(sd, ed, server)
    nr = C.condense_data()
    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr
    print "finished creating SOILWC from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not make soil WC from %s to %s" %(sd, ed)

  ## PRECIP
  try:
    sd, ed = B.check_out_one_attribute("PRECIP")
    C = smashWorkers.Precipitation(sd, ed, server)
    nr = C.condense_data()
    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr
    print "finished creating PRECIP from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not make PRECIP from %s to %s" %(sd, ed)

  ## SOLAR
  try:
    sd, ed = B.check_out_one_attribute("SOLAR")
    C = smashWorkers.Solar(sd, ed, server)
    nr = C.condense_data()
    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr
    print "finished creating SOLAR from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not make SOLAR from %s to %s" %(sd, ed)

  ## LYSIMETER
  try:
    sd, ed = B.check_out_one_attribute("LYS")
    C = smashWorkers.SnowLysimeter(sd, ed, server)
    nr = C.condense_data()
    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr
    print "finished creating SNOW LYSIMETER from %s to %s" %(sd, ed)
    del C
  except Exception:
    print "could not make SNOW LYS from %s to %s" %(sd, ed)



#---- in this case, we are doing only one attribute-- 

elif args.crud == "CREATE" and args.attribute in ["AIRTEMP", "airtemp", "MS04301"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("AIRTEMP")
  print "Starting on : %s and ending on %s" %(sd, ed)
  
  C = smashWorkers.AirTemperature(sd, ed, server)
  nr = C.condense_data()
  new_rows = []

  # prints only the station of interest
  if station != None:
   
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      writer.writerow(['DBCODE', 'ENTITY', 'SITECODE', 'HEIGHT', 'QC_LEVEL','DATE','AIRTEMP_MEAN_DAY', 'AIRTEMP_MEAN_FLAG','AIRTEMP_MAX_DAY','AIRTEMP_MAX_FLAG','AIRTEMP_MAXTIME','AIRTEMP_MIN_DAY','AIRTEMP_MIN_FLAG','AIRTEMP_MINTIME','EVENT_CODE','SOURCE'])
      for row in new_rows:
        writer.writerow(row)
  del C
  del nr
  del new_rows

elif args.crud == "CREATE" and args.attribute in ["RELHUM", "relhum", "MS04302"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("RELHUM")
  print "starting on %s and ending on %s" %(sd, ed)
  C = smashWorkers.RelHum(sd, ed, server)
  nr = C.condense_data()
  new_rows = []

  # prints only the station of interest
  if station != None:
  
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      writer.writerow(['DBCODE', 'ENTITY', 'SITECODE', 'HEIGHT', 'QC_LEVEL','DATE','RELHUM_MEAN_DAY', 'RELHUM_MEAN_FLAG','RELHUM_MAX_DAY','RELHUM_MAX_FLAG','RELHUM_MAXTIME','RELHUM_MIN_DAY','RELHUM_MIN_FLAG','RELHUM_MINTIME','EVENT_CODE','SOURCE'])
      for row in new_rows:
        writer.writerow(row)
  del C

elif args.crud == "CREATE" and args.attribute in ["PRECIP", "precip", "MS04303"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("PRECIP")
  print sd, ed
  C = smashWorkers.Precipitation(sd, ed, server)
  nr = C.condense_data()
  new_rows = []

  # prints only the station of interest
  if station != None:
  
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      writer.writerow(['DBCODE', 'ENTITY', 'SITECODE', 'HEIGHT', 'QC_LEVEL','DATE','PRECIP_TOT_DAY', 'PRECIP_TOT_FLAG','EVENT_CODE','SOURCE'])
      for row in new_rows:
        writer.writerow(row)
  del C

elif args.crud == "CREATE" and args.attribute in ["VPD", "vpd"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("VPD")
  print sd, ed
  C = smashWorkers.VPD(sd, ed, server)
  nr = C.condense_data()
  new_rows = []

  # prints only the station of interest
  if station != None:
  
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      output = smashControls.HeaderWriter('VPD').write_header_template()
      writer.writerow(output)
      for row in new_rows:
        writer.writerow(row)
  del C

elif args.crud == "CREATE" and args.attribute in ["VPD2", "vpd2", "MS04308"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("VPD2")
  print sd, ed
  C = smashWorkers.VPD2(sd, ed, server)
  nr = C.condense_data()
  new_rows = []

  # prints only the station of interest
  if station != None:
  
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      output = smashControls.HeaderWriter('VPD').write_header_template()
      writer.writerow(output)
      for row in new_rows:
        writer.writerow(row)
  del C

elif args.crud == "CREATE" and args.attribute in ["DEWPT", "dewpt", "MS04307"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()


  sd, ed = B.check_out_one_attribute("DEWPT")
  print sd, ed
  C = smashWorkers.DewPoint(sd, ed, server)
  nr = C.condense_data()
  new_rows = []

  # prints only the station of interest
  if station != None:
  
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      output = smashControls.HeaderWriter('DEWPT').write_header_template()
      writer.writerow(output)
      for row in new_rows:
        writer.writerow(row)
  del C

elif args.crud == "CREATE" and args.attribute in ["SOLAR", "solar", "MS04305"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("SOLAR")
  print sd, ed
  C = smashWorkers.Solar(sd, ed, server)
  nr = C.condense_data()
  new_rows = []

  # prints only the station of interest
  if station != None:
  
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      output = smashControls.HeaderWriter('SOLAR').write_header_template()
      writer.writerow(output)
      for row in new_rows:
        writer.writerow(row)
  del C



elif args.crud == "CREATE" and args.attribute in ["WSPD_SNC", "SONIC", "sonic", "wspd_snc" "MS04334"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("WSPD_SNC")
  print sd, ed
  C = smashWorkers.Sonic(sd, ed, server)
  nr = C.condense_data()
  new_rows = []

  # prints only the station of interest
  if station != None:
  
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      output = smashControls.HeaderWriter('SOILTEMP').write_header_template()
      writer.writerow(output)
      for row in new_rows:
        writer.writerow(row)
  del C

elif args.crud == "CREATE" and args.attribute in ["NR", "net", "radiation", "netrad", "NETRAD", "MS04325"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("NR")
  print sd, ed
  C = smashWorkers.NetRadiometer(sd, ed, server)
  nr = C.condense_data()
  new_rows = []
  # prints only the station of interest
  if station != None:
  
    for each_list in nr:
      if each_list[2] == station:
        print each_list
        new_rows.append(each_list)
      else:
        pass
  else:
    for row in nr:
      print row
      new_rows.append(row)

  # if the csv is not none
  if mycsv != None:
    with open(csv_filename, 'wb') as writefile:
      writer = csv.writer(writefile)
      output = smashControls.HeaderWriter('NR').write_header_template()
      writer.writerow(output)
      for row in new_rows:
        writer.writerow(row)
  del C

elif args.crud == "CREATE" and args.attribute in ["WSPD_PRO", "wspd_pro", "WIND","wind", "PROP", "prop", "MS04304"] and args.startdate == None and args.enddate == None:

  if station == None:
    # create a list of last updates
    B = smashControls.DBControl(server)
    B.build_queries()

  elif station != None:
    B = smashControls.DBControl(server, station)
    B.build_queries_station()

  sd, ed = B.check_out_one_attribute("WSPD_PRO")
  print sd, ed
  C = smashWorkers.Wind(sd, ed, server)
  nr = C.condense_data()
  for row in nr:
      print row
  del C

elif args.crud == "CREATE" and args.attribute in ["WSPD_PRO2", "wspd_pro2", "WIND2","wind2", "PROP2", "prop2", "MS04304"] and args.startdate == None and args.enddate == None:

  # create a list of last updates
  B = smashControls.DBControl(server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("WSPD_PRO2")
  print sd, ed
  C = smashWorkers.Wind2(sd, ed, server)
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
    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
        else:
          pass
    else:
      print nr
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

    new_rows = []
    # prints only the station of interest
    if station != None:
      for each_list in nr:
        if each_list[2] == station:
          print each_list
          new_rows.append(each_list)
        else:
          pass
    else:
      for row in nr:
        new_rows.append(each_list)
        print nr

    # if csv is not none, write csv
    if args.csv[0] == "TRUE":
      with open(csv_filename, 'wb') as writefile:
        writer = csv.writer(writefile)
        writer.writerow(['DBCODE', 'ENTITY', 'STATION', 'AIRTEMP_METHOD', 'HEIGHT', 'QC_LEVEL', 'DATE', 'AIRTEMP_MEAN_DAY', 'AIRTEMP_MEAN_FLAG', 'AIRTEMP_MAX_DAY', 'AIRTEMP_MAX_FLAG','AIRTEMP_MAXTIME', 'AIRTEMP_MIN_DAY', 'AIRTEMP_MIN_FLAG', 'AIRTEMP_MINTIME'])
        for row in new_rows:
          writer.writerow(row)
    del C
    del nr
    del new_rows

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

  elif args.attribute in ["WSPD_PRO2", "wspd_pro2", "WIND2","wind2", "PROP2", "prop2", "MS04304"]:

    C = smashWorkers.Wind2(sd, ed, server)
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
    elif deleteable == "WSPD_PRO2" or deleteable == "MS04304":
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

  if sd != ed:
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

  else:
    print "database for AIRTEMP is already up to date"

  # RELHUM


  sd, ed = B.check_out_one_attribute("RELHUM")

  if sd != ed:
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

  else:
    print "database for RELHUM is already up to date!"

  # VPD
  sd, ed = B.check_out_one_attribute("VPD2")

  if sd != ed:
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
    del D
  else:
    print "database for VPD is already up to date!"

  # Dew point
  sd, ed = B.check_out_one_attribute("DEWPT")

  if sd != ed:
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
    del D
  else:
    print "database for Dewpoint is already up to date!"


  # Net Radiometer

  
  sd, ed = B.check_out_one_attribute("NR")
  if sd != ed:
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
  else:
    print "database for Net Radiometer is already up to date!"

  # SOLAR
  sd, ed = B.check_out_one_attribute("SOLAR")

  if sd != ed:
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
  else: 
    print "database for solar is already up to date!"

  # SONIC
  if sd != ed: 
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
    del D
  else:
    print "database for SOLAR is already up to date!"

  # PROP
  sd, ed = B.check_out_one_attribute("WSPD_PRO")
  if sd != ed:
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
  else:
    print "database for PROP is already up to date!"

  # # PROP2
  # sd, ed = B.check_out_one_attribute("WSPD_PRO2")
  # if sd != ed:
  #   C = smashWorkers.Wind2(sd, ed, server)
  #   nr = C.condense_data()
  #   print "checking that the methods are updated"
  #   D = smashBosses.UpdateBoss(C, nr)
  #   if args.station != None:
  #     D.only_one_station(args.station[0])
  #   else:
  #     pass
  #   D.update_the_db_methods()
  #   D.update_the_db()
  #   print "finished creating WSPD_PRO2 from %s to %s" %(sd, ed)
  #   del C
  #   del D
  # else:
  #   print "database for PROP is already up to date!"

  # SOIL TEMP
  sd, ed = B.check_out_one_attribute("SOILTEMP")
  if sd != ed:
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
    del D
  else:
    print "database for Soil Temp is already up to date!"


  # SOIL WC
  sd, ed = B.check_out_one_attribute("SOILWC")
  if sd != ed:
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
  else:
    print "database for soil WC is already up to date!"

  # PRECIP
  sd, ed = B.check_out_one_attribute("PRECIP")

  if sd != ed:
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
    del D
  else:
    print "database for PRECIP is already up to date!"

  # # SOLAR - PAR
  sd, ed = B.check_out_one_attribute("PAR")
  if sd != ed:
    C = smashWorkers.PhotosyntheticRad(sd, ed, server)
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
  else: 
    print "PAR is already up to date!"

  # SNOW LYSIMETER

  sd, ed = B.check_out_one_attribute("LYS")
  if sd != ed:
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
  else:
    print "database for lysimeter is already up to date!"
    
  print("Updates come from the source of {}".format(args.server))


if args.crud == "UPDATE" and args.attribute in ["AIRTEMP","MS04301"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("AIRTEMP")

  if sd != ed:

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

  else:
    print "database is already up to date!"   

elif args.crud == "UPDATE" and args.attribute in ["RELHUM","MS04302"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("RELHUM")

  if sd != ed:

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

  else:
    pass

elif args.crud == "UPDATE" and args.attribute in ["PRECIP","MS04303"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("PRECIP")

  if sd != ed:

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

  else:
    pass

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

### CSV

  def csvwrite(args,nr):
    filename = args.attribute + '_daily_updated_on_' + datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
   
    with open(filename, 'wb') as csvfile:
      writer = csv.writer(csvfile)

      for row in nr:
        writer.writerow(row)

    print "finished! with %s" %(args.attribute)

### UPDATE METHODS IF WE HAVE START AND END DATES:

### UPDATE METHODS ####

if args.crud == "UPDATE" and args.attribute == "ALL" and args.startdate != None and args.enddate != None:

  sd = args.startdate
  ed = args.enddate

  # AIR TEMPERATURE
  

  if sd != ed:
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

  else:
    print "database for AIRTEMP is already up to date"

  # RELHUM


  sd, ed = B.check_out_one_attribute("RELHUM")

  if sd != ed:
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

  else:
    print "database for RELHUM is already up to date!"

  # VPD
  sd, ed = B.check_out_one_attribute("VPD2")

  if sd != ed:
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
    del D
  else:
    print "database for VPD is already up to date!"

  # Dew point
  sd, ed = B.check_out_one_attribute("DEWPT")

  if sd != ed:
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
    del D
  else:
    print "database for Dewpoint is already up to date!"


  # Net Radiometer

  
  sd, ed = B.check_out_one_attribute("NR")
  if sd != ed:
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
  else:
    print "database for Net Radiometer is already up to date!"

  # SOLAR
  sd, ed = B.check_out_one_attribute("SOLAR")

  if sd != ed:
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
  else: 
    print "database for solar is already up to date!"

  # SONIC
  if sd != ed: 
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
    del D
  else:
    print "database for SOLAR is already up to date!"

  # PROP
  sd, ed = B.check_out_one_attribute("WSPD_PRO")
  if sd != ed:
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
  else:
    print "database for PROP is already up to date!"

  # # PROP2
  # sd, ed = B.check_out_one_attribute("WSPD_PRO2")
  # if sd != ed:
  #   C = smashWorkers.Wind2(sd, ed, server)
  #   nr = C.condense_data()
  #   print "checking that the methods are updated"
  #   D = smashBosses.UpdateBoss(C, nr)
  #   if args.station != None:
  #     D.only_one_station(args.station[0])
  #   else:
  #     pass
  #   D.update_the_db_methods()
  #   D.update_the_db()
  #   print "finished creating WSPD_PRO2 from %s to %s" %(sd, ed)
  #   del C
  #   del D
  # else:
  #   print "database for PROP is already up to date!"

  # SOIL TEMP
  sd, ed = B.check_out_one_attribute("SOILTEMP")
  if sd != ed:
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
    del D
  else:
    print "database for Soil Temp is already up to date!"


  # SOIL WC
  sd, ed = B.check_out_one_attribute("SOILWC")
  if sd != ed:
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
  else:
    print "database for soil WC is already up to date!"

  # PRECIP
  sd, ed = B.check_out_one_attribute("PRECIP")

  if sd != ed:
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
    del D
  else:
    print "database for PRECIP is already up to date!"

  # # SOLAR - PAR
  sd, ed = B.check_out_one_attribute("PAR")
  if sd != ed:
    C = smashWorkers.PhotosyntheticRad(sd, ed, server)
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
  else: 
    print "PAR is already up to date!"

  # SNOW LYSIMETER

  sd, ed = B.check_out_one_attribute("LYS")
  if sd != ed:
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
  else:
    print "database for lysimeter is already up to date!"
    
  print("Updates come from the source of {}".format(args.server))


if args.crud == "UPDATE" and args.attribute in ["AIRTEMP","MS04301"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("AIRTEMP")

  if sd != ed:

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

  else:
    print "database is already up to date!"   

elif args.crud == "UPDATE" and args.attribute in ["RELHUM","MS04302"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("RELHUM")

  if sd != ed:

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

  else:
    pass

elif args.crud == "UPDATE" and args.attribute in ["PRECIP","MS04303"] and args.startdate == None and args.enddate == None:

  B = smashControls.DBControl(args.server)
  B.build_queries()

  sd, ed = B.check_out_one_attribute("PRECIP")

  if sd != ed:

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

  else:
    pass

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