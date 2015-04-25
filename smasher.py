import argparse
import smashBosses
import smashControls
import datetime

"""SMASHER is the executable for the other parts of the data 'smashing'"""

parser = argparse.ArgumentParser()

# which function you are running
parser.add_argument('boss')

# which server you are using
parser.add_argument('server')

# these are all optional arguements
# which attribute you are using 
parser.add_argument('--attribute', '-a', nargs = 1, required = False, help = " the official name of an attribute to be processed in isolation ")

# startdate 
parser.add_argument('--startdate', '-sd', nargs = 1, required = False, help = " the first date, as a date-string in form YYYY-MM-DD HH:MM:SS, that you want to process ")

# enddate
parser.add_argument('--enddate', '-ed', nargs = 1, required = False, help = " the last date, as a date-string in form YYYY-MM-DD HH:MM:SS, that you want to process ")

# specific probe
parser.add_argument('--probe', '-p', nargs = 1, required = False, help = " a single probe, which can be run in isolation ")

# new configuration file for mapping specific methods differently
parser.add_argument('--newcfg', nargs=1, required=False, help = "follow --newcfg with a .yaml file to be used for configuration rather than the default yaml file")

# use this arguement to store a csv files
parser.add_argument('--csv', action='store_true', help = '--csvs will also output csv files', required=False)

# use this arguement to compute VPD using the VPD special control 
parser.add_argument('--vpd', action='store_true', help = "If --vpd is on, vpd will be calculated with math rather than taken from MS04318. Default is off", required = False)

# go!
args = parser.parse_args()

# Printing an intro 
print(" You are processing using the smasher Python toolkit. You have given the following information: \n")
 
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

        new_query = ("Delete from " + full_name + " where date > '2014-01-01'")

        import form_connection as fc
        conn = fc.micro_conn(args.server)

        cursor = conn.cursor()

        cursor.execute(new_query)

        print("Your rows have been deleted. Please try your updates again!")


if args.boss == 'LIMITED':

    if args.attribute == None:
        print(" To used the LIMITED function, you need to explicitly provide the single attribute you want to process.\r The appropriate syntax is: \n python smasher.py 'LIMITED' 'SHELDON' --attribute 'PRECIP'")
    else:
        # processes the data only based on the specifics in the LIMITED.yaml file
        print(" The smasher will process your LIMITED.yaml file ... Start and Ending dates will be taken from the file, and only your selected probes will be processed... You are doing attribute {} ".format(args.attribute))

        if args.newcfg != None:
            print(" smasher assumes you want to use the configuration provided in {}".format(args.newcfg[0]))
            
            IterBoss = smashBosses.ProbeBoss(args.attribute[0], args.server).iterate_over_many_config(args.newcfg[0])

            print( " smasher has processed your LIMITED.yaml file ")
        
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


# if the BACK-CHECK arguement is provided, go back in and check the database for bad methods based on a yaml file



# if the provisional arguement is provided, you will update the LTERLogger_Pro
if args.boss == 'PROVO':

    # update LTERLogger_Pro.dbo.attribute
    print("updating the provisional database at LTERLogger_Pro")
    
    print("updates come from the source of {}".format(args.server))
    my_server = args.server

    # create the queries of the most recent updates
    DBController = smashControls.DBControl(args.server)
    DBController.build_queries()

    # if there is one attribute given, you will use it
    if args.attribute != None:

        # check to be sure the attribute is accepted in our list:
        if args.attribute[0].upper() not in ["AIRTEMP", "LYS", "NR", "WSPD_SNC", "SOILWC", "PAR", "SOILTEMP", "VPD", "DEWPT", "SOLAR", "WSPD_PRO", "PRECIP", "RELHUM"]:
    
            print("Please use an acceptable attribute such as (upper-case) AIRTEMP, LYS, NR, WSPD_SNC, SOILWC, PAR, SOILTEMP, VPD, DEWPT, SOLAR, WSPD_PRO, PRECIP, or RELHUM")
        else:

            pass

        # COMMENTED OUT DATE CHECK BECAUSE THIS IS THE FIRST UPDATE!
        # check out the date range on the attribute
        #try:
        #    sd, ed = DBController.check_out_one_attribute(args.attribute[0])
        
        #except AttributeError:
        #    print( "That attribute is already as up to date as it can be!")

    #elif args.attribute == "None":

    #    print( "You must choose an attribute, type --attribute AIRTEMP for example")
        
    # if start dates and end dates are given
    if args.startdate != None and args.enddate != None:
        
        # take the start and end dates
        sd_in = args.startdate[0]
        ed_in = args.enddate[0]

        # check that the input start date does not precede the earliest start date which is already in the DB, otherwise you will write some duplicates
        # if datetime.datetime.strptime(sd_in, '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(sd,'%Y-%m-%d %H:%M:%S'):
        #     print(" You cannot perform an insert starting at this time, or you will make duplicate values! I am changing to the newest time you can use, which is {}")
        
        # else:
        #     sd = sd_in # set the start date value to the inputs
    
        # if datetime.datetime.strptime(ed_in, '%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(ed, '%Y-%m-%d %H:%M:%S'):
        #     print(" Your end date is after the end of the data, we can only process until the data has completed ")
        # else:
        #     ed = ed_in # set the end date value to the input

        # create an update-boss

        # if args.attribute[0] == 'VPD' and args.vpd != None:
        #     U = smashBosses.UpdateBoss(args.attribute[0], sd, ed, args.server, vpd="on")
        # else:


        sd = sd_in
        ed = ed_in

        U = smashBosses.UpdateBoss(args.attribute[0], sd, ed, args.server)
        
        # inserts rows into the database
        print "now we are switching to the SHELDON connection to update LTERLogger_pro"
        U.update_the_db()

        # write a csv if it is chosen to do so
        if args.csv != None:
            U.write_a_csv()
        else:
            pass

    """FROM HERE ON IS  THE AUTOMATED WAY """

    # elif args.startdate == None and args.enddate != None:

    #     ed_in = args.enddate[0]

    #     if datetime.datetime.strptime(ed_in, '%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(ed, '%Y-%m-%d %H:%M:%S'):
    #         print(" Your end date is after the end of the data, we can only process until the data has completed ")
    #     else:
    #         ed = ed_in # set the end date value to the input

    #     # create an update-boss

    #     if args.attribute[0] == 'VPD':
    #         U = smashBosses.UpdateBoss(args.attribute[0], sd, ed, 'SHELDON', vpd="off")
    #     else:
    #         U = smashBosses.UpdateBoss(args.attribute[0], sd, ed, 'SHELDON')
        
    #     # inserts rows into the database
    #     U.update_the_db()

    #     # write a csv if it is chosen to do so
    #     if args.csv != None:
    #         U.write_a_csv()
    #     else:
    #         pass

    # elif args.startdate != None and args.enddate == None:

    #     sd_in = args.startdate[0]

    #     if datetime.datetime.strptime(sd_in, '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(sd,'%Y-%m-%d %H:%M:%S'):
    #         print(" You cannot perform an insert starting at this time, or you will make duplicate values! ")
            
    #     else: 
    #         sd = sd_in

    #         print(" You didnt enter an end-date, so I am using the last one I can process")

    #     # create an update-boss
    #     U = smashBosses.UpdateBoss(args.attribute, sd, ed,'SHELDON')
        
    #     # inserts rows into the database
    #     U.update_the_db()

    #     # write a csv if it is chosen to do so
    #     if args.csv != None:
    #         U.write_a_csv()
    #     else:
    #         pass

    # # if no inputs are given, take care of it yourself
    # elif args.startdate == None and args.enddate == None:

    #     # create an update-boss
    #     U = smashBosses.UpdateBoss(args.attribute[0], sd, ed,'STEWARTIA')
    #     # inserts rows into the database
    #     U.update_the_db()
    #     print(" Rows have been inserted into the database for the entire missing range. Please check for duplicates if you feel you have made an error. ")

    #     # write a csv if it is chosen to do so
    #     if args.csv != None:
    #         U.write_a_csv()
    #     else:
    #         pass





# if args.flow:
#     print(" You're in it for the long haul, huh? Processing ALL THE THINGS! ")

#     print("First, getting the reasonable date ranges for your server, {}".format(args.server))
    
#     DBController = smashControls.DBControl(args.server).build_queries()


#     for attribute in ["AIRTEMP", "RELHUM", "PRECIP", "WSPD_PRO", "SOLAR", "DEWPT", "VPD", "LYS", "SOILTEMP", "PAR", "SOILWC", "WSPD_SNC", "NR"]:


#         sd, ed = DBController.check_out_one_attribute(attribute)
        

# else:
#     pass
