import argparse
import smashBosses
import smashControls
import datetime

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
parser.add_argument('boss')

# which server you are using -- REQUIRED!
parser.add_argument('server')

# which attribute you are using 
parser.add_argument('--attribute', '-a', nargs = 1, required = False, help = " the official name of an attribute to be processed in isolation ")

# startdate 
parser.add_argument('--startdate', '-sd', nargs = 1, required = False, help = " the first date, as a date-string in form YYYY-MM-DD HH:MM:SS, that you want to process ")

# enddate
parser.add_argument('--enddate', '-ed', nargs = 1, required = False, help = " the last date, as a date-string in form YYYY-MM-DD HH:MM:SS, that you want to process ")

# specific probe
parser.add_argument('--probe', '-p', nargs = 1, required = False, help = " a single probe, which can be run in isolation ")

# specific station
parser.add_arguement('--station', nargs=1, required=False, help="one station, such as PRIMET, for updates, deletes, and management")

# new configuration file for mapping specific methods differently
parser.add_argument('--newcfg', nargs=1, required=False, help = "follow --newcfg with a .yaml file to be used for configuration rather than the default yaml file")

# use this arguement to store a csv files
parser.add_argument('--csv', action='store_true', help = '--csvs will also output csv files', required=False)

# use this arguement to compute VPD using the VPD special control 
parser.add_argument('--vpd', action='store_true', help = "If --vpd is on, vpd will be calculated with math rather than taken from MS04318. Default is off", required = False)

# use this arguement to update to the method yaml
parser.add_arguement('--method', action='store_true', help = "If --method is on, the methods will be updated based on the input table")

# use this arguement to run a daily batch file
parser.add_arguement('--batch', action="store-true", required=False, help="if --batch is on, the whole data will be run for 1 day")

# go!
args = parser.parse_args()

# Printing an intro 
print(" You are processing using the SMASHER Python toolkit. (c) MIT LICENSCE. 2015. You have given the following information: \n")
 
print("~ Attribute: {}".format(args.attribute))
print("~ Server: {}".format(args.server))

print("~ Start Date: {}".format(args.startdate))
print("~ End Date: {}".format(args.enddate))

print("~ Probe Code: {}".format(args.probe))
print("~ New Configuration File: {}".format(args.newcfg))


if args.boss == 'XXDEL'

    print(" Deleting all data from LTERLogger_Pro for your attribute! ")

    if args.attribute == None:
        print("I cannot process this command without an attribute to delete. Try again :)")

    else:

        deleteable = args.attribute[0]

        if deleteable == "AIRTEMP":
            full_name = "LTERLogger_Pro.dbo.MS04301"
        elif deleteable == "RELHUM":
            full_name = "LTERLogger_Pro.dbo.MS04302"
        elif deleteable == "WSPD_PRO":
            full_name = "LTERLogger_Pro.dbo.MS04304"
        elif deleteable == "SOLAR"
            full_name = "LTERLogger_Pro.dbo.MS04305"
        elif deleteable == "PRECIP":
            full_name = "LTERLogger_Pro.dbo.MS04303"
        elif deleteable == "NR":
            full_name = "LTERLogger_Pro.dbo.MS04325"
        elif deleteable == "WSPD_SNC":
            full_name = "LTERLogger_Pro.dbo.MS04324"
        elif deleteable == "SOILWC":
            full_name = "LTERLogger_Pro.dbo.MS04323"
        elif deleteable == "SOILTEMP":
            full_name = "LTERLogger_Pro.dbo.MS04321"
        elif deleteable == "PAR":
            full_name = "LTERLogger_Pro.dbo.MS04322"
        elif deletable == "LYS":
            full_name = "LTERLogger_Pro.dbo.MS04309"
        else:
            print("need to create a method to delete {}".format(deleteable))


        # choose the range over which to delete.
        if args.startdate == None:

            new_query = ("Delete from " + full_name + " where date >= '2014-01-01'")

        elif args.startdate[0] != None and args.enddate == None:
            sd = args.startdate[0]
            new_query = ("Delete from " + full_name + " where date >= \'" + sd + "\'")

        elif args.startdate[0] != None and args.enddate[0] != None:
            sd = args.startdate
            ed = args.enddate

            new_query = ("Delete from " + full_name + " where date >= \'" + sd + "\' and date <= \'" + ed "\'")

        else:
            print( "You have not made a valid combination of start date, end date, and attribute, please try again")

        import form_connection as fc
        conn = fc.micro_conn(args.server)

        cursor = conn.cursor()

        cursor.execute(new_query)

        print("Your rows have been deleted. You may now try to update again!")


if args.boss == "LIMITED":

    if args.attribute == None:
        print(" To used the LIMITED function, you need to explicitly provide the single attribute you want to process.\r The appropriate syntax is: \n python smasher.py 'LIMITED' 'SHELDON' --attribute 'PRECIP'. This function will only do certin probes from that attribute which are in your limited.yaml file. Start dates and end dates do not need to be input, as they are in the file.")
    else:
        # processes the data only based on the specifics in the LIMITED.yaml file
        print(" The smasher will process your LIMITED.yaml file ... Start and Ending dates will be taken from the file, and only your selected probes will be processed... You are doing attribute {} ".format(args.attribute))

        if args.newcfg != None:
            print(" SMASHER assumes you want to use the method codes configuration provided in {}".format(args.newcfg[0]))
            
            IterBoss = smashBosses.ProbeBoss(args.attribute[0], args.server).iterate_over_many_config(args.newcfg[0])

            print( " SMASHER has processed your LIMITED.yaml file ")
        
        elif args.newcfg == None:
            
            print(" You did not provide a custom configuration, smasher assumes you want to use CONFIG.yaml ")
            IterBoss = smashBosses.ProbeBoss(args.attribute[0], args.server).iterate_over_many_config()
            
            print(" smasher has processed your LIMITED.yaml file based on the standard configuration file ")

if args.boss == 'TO-DO':
    """ TO-DO is a function to print what needs to be processed. It can take an arguement or not"""
    
    print("First, getting the reasonable date ranges for your server, {}".format(args.server))
    
    # get the controller object
    DBController = smashControls.DBControl(args.server)

    # build queries based on this object
    DBController.build_queries()
    
    # if there is not an attribute specified, then try all the attributes
    if args.attribute == None:
        for attribute in ["AIRTEMP", "RELHUM", "PRECIP", "WSPD_PRO", "SOLAR", "DEWPT", "VPD", "LYS", "SOILTEMP", "PAR", "SOILWC", "WSPD_SNC", "NR"]:

            # get start and ending dates, if the attribute is up to date, tell us
            try:
                sd, ed = DBController.check_out_one_attribute(attribute)
                print(" the attribute : " + attribute + " needs updates between " + sd + " and " + ed)
            except AttributeError:
                print(" the attribute " + attribute + " may already be up to date...")
            except KeyError:
                print(" the attribute " + attribute + " may already be up to date... ")
    
    # if an attribute IS specified, then just get that attribute
    else:

        # get start and ending dates, if the attribute is up to date, tell us
        # if all lower-case is given, it will be accepted, but the rest may break later.
        try:
            sd, ed = DBController.check_out_one_attribute(args.attribute[0].upper())
            print(" the attribute : " + args.attribute[0] + " needs updates between " + sd + " and " + ed)
        except AttributeError:
            print(" the attribute " + args.attribute[0] + " may already be up to date, did you see a message? ")
        except KeyError:
            print(" the attribute " + args.attribute[0] + " may already be up to date, did you see a message? ")


if args.boss == 'XXDEL1':

    print(" Deleting all data from LTERLogger_Pro for your attribute! ")

    if args.attribute == None:
        print("I cannot process this command without an attribute to delete. Try again :)")

    else:

        deleteable = args.attribute[0]

        if deleteable == "AIRTEMP":
            full_name = "LTERLogger_Pro.dbo.MS04301"
        elif deleteable == "RELHUM":
            full_name = "LTERLogger_Pro.dbo.MS04302"
        elif deleteable == "WSPD_PRO":
            full_name = "LTERLogger_Pro.dbo.MS04304"
        elif deleteable == "SOLAR"
            full_name = "LTERLogger_Pro.dbo.MS04305"
        elif deleteable == "PRECIP":
            full_name = "LTERLogger_Pro.dbo.MS04303"
        elif deleteable == "NR":
            full_name = "LTERLogger_Pro.dbo.MS04325"
        elif deleteable == "WSPD_SNC":
            full_name = "LTERLogger_Pro.dbo.MS04324"
        elif deleteable == "SOILWC":
            full_name = "LTERLogger_Pro.dbo.MS04323"
        elif deleteable == "SOILTEMP":
            full_name = "LTERLogger_Pro.dbo.MS04321"
        elif deleteable == "PAR":
            full_name = "LTERLogger_Pro.dbo.MS04322"
        elif deletable == "LYS":
            full_name = "LTERLogger_Pro.dbo.MS04309"
        else:
            print("need to create a method to delete {}".format(deleteable))

        # choose the range over which to delete.
        if args.startdate == None:

            new_query = ("Delete from " + full_name + " where sitecode like " + station + " and date >= '2014-01-01'")

        elif args.startdate[0] != None and args.enddate == None:
            
            sd = args.startdate[0]
            new_query = ("Delete from " + full_name + " where sitecode like " + station + " and date >= \'" + sd + "\'")

        elif args.startdate[0] != None and args.enddate[0] != None:
            sd = args.startdate
            ed = args.enddate

            new_query = ("Delete from " + full_name + " where sitecode like " + station + " and date >= \'" + sd + "\' and date <= \'" + ed "\'")

        else:
            print( "You have not made a valid combination of start date, end date, and attribute, please try again")

        import form_connection as fc
        conn = fc.micro_conn(args.server)

        cursor = conn.cursor()

        cursor.execute(new_query)

        print("Your rows have been deleted. You may now try to update again!")


# if the provisional arguement is provided, you will update the LTERLogger_Pro
if args.boss == 'PROVO':

    # update LTERLogger_Pro.dbo.attribute
    print("Updating the provisional database at LTERLogger_Pro")
    
    print("Updates come from the source of {}".format(args.server))
    my_server = args.server


    # if there is one attribute given, you will use it
    if args.attribute != None:

        # check to be sure the attribute is accepted in our list:
        if args.attribute[0].upper() not in ["AIRTEMP", "LYS", "NR", "WSPD_SNC", "SOILWC", "PAR", "SOILTEMP", "VPD", "DEWPT", "SOLAR", "WSPD_PRO", "PRECIP", "RELHUM"]:
    
            print("Please use an acceptable attribute such as (upper-case) AIRTEMP, LYS, NR, WSPD_SNC, SOILWC, PAR, SOILTEMP, VPD, DEWPT, SOLAR, WSPD_PRO, PRECIP, or RELHUM")
        else:

            pass
        
    # if start dates and end dates are given
    if args.startdate != None and args.enddate != None:
        
        # take the start and end dates
        sd_in = args.startdate[0]
        ed_in = args.enddate[0]

        try:
            # create the queries of the most recent updates
            DBController = smashControls.DBControl(args.server)
            DBController.build_queries()
            sd, ed = DBController.check_out_one_attribute(args.attribute[0])

            # check that the input start date does not precede the earliest start date which is already in the DB, otherwise you will write some duplicates
            if datetime.datetime.strptime(sd_in, '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(sd,'%Y-%m-%d %H:%M:%S'):
                print(" You cannot perform an insert starting at this time, or you will make duplicate values! I am changing to the newest time you can use, which is {}".format(sd))
                ed = ed_in
            else:
                sd = sd_in # set the start date value to the inputs
                ed = ed_in

        except Exception:
            print(" The start date check could not be performed. Is there already values in this table?")


        # create an update-boss
        # if it is VPD, check for the vpd flag indicating we want the "slow way"
        if args.attribute[0] == 'VPD' and args.vpd ==True:
            U = smashBosses.UpdateBoss(args.attribute[0], sd, ed, args.server, vpd="on")
        else:
            U = smashBosses.UpdateBoss(args.attribute[0], sd, ed, args.server)
        
        # update the methods
        print "updating the methods based on method table"
        U.update_the_db_methods()

        # inserts rows into the database- see the UpdateBoss
        print "now we are switching to the SHELDON connection to update LTERLogger_pro"
        U.update_the_db()

        # write a csv if it is chosen to do so
        if args.csv != None:
            U.write_a_csv()
        else:
            pass

        # update the methods based on daily table
        if args.method != None:
            U.update_methods
        else:
            pass

        # update the flags based on flags table
        if u.reflags != None;

if args.boss == "HANS":

    # take out a connection thread
    import form_connection as fc
    conn = fc.micro_conn('SHELDON')
    
    # establish a cursor
    cur = conn.cursor()

    # current day
    now_day_1 = datetime.datetime.now()

    # the end date is right now, but at midnight of the beginning of today
    now_day = datetime.datetime(now_day_1.year, now_day_1.month, now_day_1.day, 0, 0)

    ed = datetime.datetime.strftime(now_day,'%Y-%m-%d %H:%M:%S')

    
    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04301 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('AIRTEMP', sd, ed, args.server)
    print(" Updating heights and methods ")
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed AIRTEMP for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04302 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted
    
    Z = smashBosses.UpdateBoss('RELHUM', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed RELHUM for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04308 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('VPD', sd, ed, args.server, vpd='on')
    print("Processing VPD-- sorry this one is slow")
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed VPD for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04307 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted


    Z = smashBosses.UpdateBoss('DEWPT', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed DEWPT for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04305 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('SOLAR', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed SOLAR for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04321 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('SOILTEMP', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed SOILTEMP for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04304 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('WSPD_PRO', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed WINDSPEED PROP for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04324 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('WSPD_SNC', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed WINDSPEED SONIC for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04303 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('PRECIP', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed PRECIP for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04309 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('LYS', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed SNOW LYSIMETER for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04325 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('NR', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed NET RADIATION for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04322 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('PAR', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed PHOTOSYNTHETIC RADIATION for the day!... cleaning myself.")
    del Z

    # the start date is the last thing in hte db + 1 day, since we use > = method
    query = "select top 1 date from LTERLogger_Pro.dbo.MS04323 order by date desc"
    cur.execute(query)

    for row in cur:
        most_recent_formatted = datetime.datetime.strptime(str(row[0]),'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days = 1)

    sd = most_recent_formatted

    Z = smashBosses.UpdateBoss('SOILWC', sd, ed, args.server)
    Z.update_the_db_methods()
    Z.update_the_db()
    print("I have processed SOIL WATER CONTENT for the day!... cleaning myself.")
    del Z

    print "all tables processed"