import argparse
import smashBosses
import smashControls
import datetime

"""SMASHER is the executable for the other parts of the data 'smashing'"""

parser = argparse.ArgumentParser()
parser.add_argument('attribute')
parser.add_argument('server')
parser.add_argument('startdate')
parser.add_argument('enddate')
parser.add_argument('probe_code', nargs="*", required = "FALSE")

parser.add_argument('--newcfg', nargs=1, required=False, help = "follow --newcfg with a .yaml file to be used for configuration rather than the default yaml file")

parser.add_argument('--iterboss', action='store_true', help = "if --iterboss is called, uses LIMITED.yaml to process only the probes listed", required=False,)

parser.add_argument('--sheldon', action='store_true', help = 'If --sql is called, sql updates to LTERLogger_Pro will occur', required=False)

parser.add_argument('--csvs', action='store_true', help = '--go or -g will launch the application to get all dates and values between the start and end, and generate csvs in our standard format.', required=False)

parser.add_argument('--fsdb', action='store_true', help = 'If --fsdb is called, sql updates to FSDBDATA will be generated ', required=False)

parser.add_argument('--vpd', action='store_true', help = "If --vpd is on, vpd will be calculated with math rather than taken from MS04318. Default is off", required = False)

parser.add_argument('--flow', action='store_true', help = "If --flow is called, a daily workflow to do all sites and attribtues will be started.", required = False)


args = parser.parse_args()

# check to be sure that the start date is before the end dates!

dt1 = datetime.datetime.strptime(args.startdate,'%Y-%m-%d %H:%M:%S')
dt2 = datetime.datetime.strptime(args.enddate,'%Y-%m-%d %H:%M:%S')

if dt2 < dt1:
    print(" Please try again, entering a start date that happens before your end date")

# check to be sure the attribute is accepted in our list:

if args.attribute not in ["AIRTEMP", "LYS", "NR", "WSPD_SNC", "SOILWC", "PAR", "SOILTEMP", "SWE", "VPD", "DEWPT", "SOLAR", "WSPD_PRO", "PRECIP", "RELHUM"]:

    args.attribute = args.attribute.upper()
    
    if args.attribute not in ["AIRTEMP", "LYS", "NR", "WSPD_SNC", "SOILWC", "PAR", "SOILTEMP", "SWE", "VPD", "DEWPT", "SOLAR", "WSPD_PRO", "PRECIP", "RELHUM"]:
        
        print(" please use an acceptable attribute such as (upper-case) AIRTEMP, LYS, NR, WSPD_SNC, SOILWC, PAR, SOILTEMP, SWE, VPD, DEWPT, SOLAR, WSPD_PRO, PRECIP, or RELHUM")
    else:

        pass

# Printing an intro 
print(" You are processing using the smasher Python toolkit. You have given the following information: \n")
 
print("~ Attribute: {}".format(args.attribute))
print("~ Server: {}".format(args.server))

print("~ Start Date: {}".format(args.startdate))
print("~ End Date: {}".format(args.enddate))

print("~ Probe Code: {}".format(args.probe_code))
print("~ New Configuration File: {}".format(args.newcfg))

if args.iterboss:
    print(" The ProbeBoss will process your LIMITED.yaml file ... The start-date and end-date arguments will not be needed! ")

    if args.newcfg != None:
        IterBoss = smashBosses.ProbeBoss(args.attribute, args.server).iterate_over_many_config(args.newcfg[0])

        try:
            IterBoss.keys()
        
        except AttributeError:
            print (" Your LIMITED.yaml file may not contain the appropriate attribute. Please check. ")

        print(" The ProbeBoss has processed your LIMITED.yaml file based on the inputs in {}".format(args.newcfg))
    
    elif args.newcfg == None:
        
        IterBoss = smashBosses.ProbeBoss(args.attribute, args.server).iterate_over_many_config()
        
        try:
            IterBoss.keys()
        
        except AttributeError:
            print (" Your LIMITED.yaml file may not contain the appropriate attribute. Please check. ")

        print(" The ProbeBoss has processed your LIMITED.yaml file based on the standard configuration file ")
else:
    pass

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
        print sd, ed

else:
    pass
