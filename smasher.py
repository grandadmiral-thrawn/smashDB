import argparse
import smashBosses
import smashControls
import datetime

"""SMASHER is the executable for the other parts of the data 'smashing'"""

parser = argparse.ArgumentParser()

# which function you are running
parser.add_arguement('boss')

# which server you are using
parser.add_argument('server')

# these are all optional arguements
# which attribute you are using 
parser.add_argument('--attribute', '-a', nargs = 1, required = False, help = " the official name of an attribute to be processed in isolation ")

# startdate and enddate
parser.add_argument('--startdate', '-sd', nargs = 1, required = False, help = " the first date, as a date-string in form YYYY-MM-DD HH:MM:SS, that you want to process ")

parser.add_argument('--enddate', '-ed', nargs = 1, required = False, help = " the last date, as a date-string in form YYYY-MM-DD HH:MM:SS, that you want to process ")

# specific probe
parser.add_argument('--probe', '-p', nargs = 1, required = False, help = " a single probe, which can be run in isolation ")

# new configuration file for mapping specific methods differently
parser.add_argument('--newcfg', nargs=1, required=False, help = "follow --newcfg with a .yaml file to be used for configuration rather than the default yaml file")

parser.add_argument('--csvs', action='store_true', help = '--csvs will also output csv files', required=False)

parser.add_argument('--fsdb', action='store_true', help = 'If --fsdb is called, sql updates to FSDBDATA will be generated ', required=False)

parser.add_argument('--vpd', action='store_true', help = "If --vpd is on, vpd will be calculated with math rather than taken from MS04318. Default is off", required = False)

parser.add_argument('--flow', action='store_true', help = "If --flow is called, a daily workflow to do all sites and attribtues will be started.", required = False)


args = parser.parse_args()


if args.boss = 'LIMITED':

    # processes the data only based on the specifics in the LIMITED.yaml file
    print(" The smasher will process your LIMITED.yaml file ... Start and Ending dates will be taken from the file, and only your selected probes will be processed... ")

    if args.newcfg != None:
        print(" smasher assumes you want to use the configuration provided in {}".format(args.newcfg[0]))
        
        IterBoss = smashBosses.ProbeBoss(args.attribute, args.server).iterate_over_many_config(args.newcfg[0])

        print( " smasher has processed your LIMITED.yaml file ")
    
    elif args.newcfg == None:
        
        print(" You did not provide a custom configuration, smasher assumes you want to use CONFIG.yaml ")
        IterBoss = smashBosses.ProbeBoss(args.attribute, args.server).iterate_over_many_config()
        
        print(" smasher has processed your LIMITED.yaml file based on the standard configuration file ")


if args.boss = 'TO-DO':
    # prints to the screen what needs to be processed
    print("First, getting the reasonable date ranges for your server, {}".format(args.server))
    
    DBController = smashControls.DBControl(args.server).build_queries()

    for attribute in ["AIRTEMP", "RELHUM", "PRECIP", "WSPD_PRO", "SOLAR", "DEWPT", "VPD", "LYS", "SOILTEMP", "PAR", "SOILWC", "WSPD_SNC", "NR"]:

        try:
            sd, ed = DBController.check_out_one_attribute(attribute)
            print(" the attribute : " + attribute + " needs updates between " + sd + " and " + ed)
        except KeyError:
            print(" the attribute " + attribute + " may already be up to date, did you see a message? ")
            pass

if args.boss = 'PROVO':
    # update LTERLogger_Pro.dbo.attribute
    print("updating the provisional database at LTERLogger_Pro on SHELDON")

    if args.server != "SHELDON":
        args.server == "SHELDON"
        print(" You need to be on Sheldon; smasher is switching your connection." )

        DBController = smashControls.DBControl(args.server).build_queries()

        if args.attribute != None:

            # check to be sure the attribute is accepted in our list:
            if args.attribute not in ["AIRTEMP", "LYS", "NR", "WSPD_SNC", "SOILWC", "PAR", "SOILTEMP", "SWE", "VPD", "DEWPT", "SOLAR", "WSPD_PRO", "PRECIP", "RELHUM"]:

                args.attribute = args.attribute.upper()
    
            if args.attribute not in ["AIRTEMP", "LYS", "NR", "WSPD_SNC", "SOILWC", "PAR", "SOILTEMP", "SWE", "VPD", "DEWPT", "SOLAR", "WSPD_PRO", "PRECIP", "RELHUM"]:
        
                print(" please use an acceptable attribute such as (upper-case) AIRTEMP, LYS, NR, WSPD_SNC, SOILWC, PAR, SOILTEMP, SWE, VPD, DEWPT, SOLAR, WSPD_PRO, PRECIP, or RELHUM")
            else:

                pass

            try:
                sd, ed = DBController.check_out_one_attribute(args.attribute)
            except AttributeError:
                print( " that attribute is already up to date!")

        elif args.attribute == "None":

            print( "You must choose an attribute, type --attribute AIRTEMP for example")
            break

    if args.startdate != None and args.enddate != None:
        sd_in = args.startdate
        ed_in = args.enddate

        if datetime.datetime.strptime(sd_in, '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(sd,'%Y-%m-%d %H:%M:%S'):
            print(" You cannot perform an insert starting at this time, or you will make duplicate values! ")
            break
        
        elif datetime.datetime.strptime(ed_in, '%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(ed, '%Y-%m-%d %H:%M:%S'):
            print(" Your end date is after the end of the data, we can only process until the data has completed ")

        else:
            sd = sd_in # set the values to the inputs
            ed = ed_in # set the values to the inputs
            
            # create an update-boss
            U = smashBosses.UpdateBoss(args.attribute, sd, ed,'SHELDON')
            
            # inserts rows into the database
            U.update_the_db()

    elif args.startdate != None and args.enddate == None:

        sd_in = args.startdate

        if datetime.datetime.strptime(sd_in, '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(sd,'%Y-%m-%d %H:%M:%S'):
            print(" You cannot perform an insert starting at this time, or you will make duplicate values! ")
            break
        else: 
        
        print(" You didn\'t enter an end-date, so I am using the last one I can process")
        
        sd = sd_in
        # create an update-boss
        U = smashBosses.UpdateBoss(args.attribute, sd, ed,'SHELDON')
        
        # inserts rows into the database
        U.update_the_db()


    elif args.startdate == None and args.enddate == None:


        # create an update-boss
        U = smashBosses.UpdateBoss(args.attribute, sd, ed,'SHELDON')
        # inserts rows into the database
        U.update_the_db()
        print(" Rows have been inserted into the database for the entire missing range. Please check for duplicates if you feel you have made an error. ")

# Printing an intro 
print(" You are processing using the smasher Python toolkit. You have given the following information: \n")
 
print("~ Attribute: {}".format(args.attribute))
print("~ Server: {}".format(args.server))

print("~ Start Date: {}".format(args.startdate))
print("~ End Date: {}".format(args.enddate))

print("~ Probe Code: {}".format(args.probe_code))
print("~ New Configuration File: {}".format(args.newcfg))



if args.sheldon:
    print (" Updates to LTERLoggers_Pro are being created. Processing {}".format(args.attribute))
else:
    pass

if args.fsdb:
    print (" Updates to FSDB are being created. Processing {}".format(args.attribute))
else:
    pass

if args.csvs:
    print(" New CSVS are being generated. Processing {}".format(args.attribute))
else:
    pass

if args.vpd:
    print(" VPD will be calculated with the maths, rather than trying to pull from entity 18")
else:
    pass

if args.flow:
    print(" You're in it for the long haul, huh? Processing ALL THE THINGS! ")

    print("First, getting the reasonable date ranges for your server, {}".format(args.server))
    
    DBController = smashControls.DBControl(args.server).build_queries()

    for attribute in ["AIRTEMP", "RELHUM", "PRECIP", "WSPD_PRO", "SOLAR", "DEWPT", "VPD", "LYS", "SOILTEMP", "PAR", "SOILWC", "WSPD_SNC", "NR"]:

        sd, ed = DBController.check_out_one_attribute(attribute)
        

else:
    pass
