SMASHER
========

v 0.0.7

SMASHER is a command line tool for updating LTERLogger_pro, our MSSQL 1st QC level provisional server. It can also get updates from MS043, the production level annual server. 

- SMASHER will work for attributes of AIRTEMP, RELHUM, DEWPT, VPD, SOLAR, WSPD_SNC, WSPD_PRO, NR, LYS, PRECIP, PAR, SOILTEMP, SNOWDEPTH (pending), LIGHT (pending) and SOILWC. It has methods for doing VPD from a calculation. These are technically called in the smashWorker.VPD2 class, not the smashWorker.VPD class. It has methods for the two forms of WSPD_PRO, one with the 5 minute maxes and one without.

- for wind, SMASHER uses daily methods which respect the standard deviations and means of the day directionally; i.e. It decomposes and computes a mean resultant wind for the propellor anemometer, and uses the Yamartino method to get a coordinate-appropriate standard deviation.

- SMASHER does not perform flagging on the high resolution data. SMASHER simply "smashes" high resolution flags into the daily aggregate.

- SMASHER can compute the daily VPD from the daily AIRTEMP and RELHUM, or it can take the high resolution values. when it computes VPD, it also computes SATVP and VAPDEF. 

- SMASHER knows when to assign net radiometer to the net radiometer table and sonic wind to the sonic wind table. 

- SMASHER gets unhappy when you call VANMET VARMET or VARMET VANMET. It will grumble at you. 

- SMASHER will try to handle as many exceptions as it can by substituting null objects. However, if the exceptions get out of hand, it will stop and give you an error message. The batch versions (CREATE ALL, UPDATE ALL, DELETE ALL) are designed to run through every table and handle the errors that I could forsee, but there is of course the possibility of errors I did not forsee.

- SMASHER when processing 5 minute maxes and mins in air temp and dew point, will first look for the daily max or min from a measured max or min for the five minute interval. if that column is null all day, it will then look to the mean to find the max or min for the day based on the extreme values about the mean. 

SMASHER is a product of Fox Peterson, Hans Luh, and Don Henshaw. It prefers Python 2.7.

Using SMASHER.
===============

Using smasher is super easy.

First, clone this repo!

[super awesome smasher repo](https://github.com/dataRonin/smashDB.git)

okay, you're ready to smash

--------

Use the CREATE method to print to screen the rows and ranges you may potentially create before doing an update. 

For example:

        python smasher.py CREATE SHELDON SOLAR

Will give you back:

        You are processing using the SMASHER Python toolkit. (c) MIT 2015. You have given the following information: 

        ~ Method: CREATE
        ~ Attribute: SOLAR
        ~ Server: SHELDON
        ~ Start Date: None
        ~ End Date: None
        2015-04-15 00:00:00 2015-04-22 00:00:00
        Finished writing LogFile
        ['MS043', 5, 'CENMET', 'RAD112', 607, '1P', 'RADCEN01', '2015-04-15 00:00:00', 16.369, 'A', 189.5, 'A', 1027.0, 'A', '1215', 'NA', 'SHELDON']
        ['MS043', 5, 'CENMET', 'RAD112', 607, '1P', 'RADCEN01', '2015-04-16 00:00:00', 21.228, 'A', 245.667, 'A', 873.0, 'A', '1215', 'NA', 'SHELDON']
        ['MS043', 5, 'CENMET', 'RAD112', 607, '1P', 'RADCEN01', '2015-04-17 00:00:00', 21.463, 'A', 248.406, 'A', 870.0, 'A', '1200', 'NA', 'SHELDON']
        ['MS043', 5, 'CENMET', 'RAD112', 607, '1P', 'RADCEN01', '2015-04-18 00:00:00', 22.14, 'A', 256.25, 'A', 885.0, 'A', '1200', 'NA', 'SHELDON']


You can pass to CREATE either an attribute name AIRTEMP, RELHUM, DEWPT, VPD, SOLAR, WSPD_SNC, WSPD_PRO, NR, LYS, PRECIP, PAR, SOILTEMP, SNOWDEPTH, LIGHT and SOILWC -- preferable to be in CAPS but lower case should be acceptable -- or the table number (MS04302, MS04305, etc.)

You can pass an extra arguement of a date range if you want to constrain the creation.

For example:

        python smasher.py CREATE SHELDON SOLAR --startdate '2015-01-01 00:00:00' --enddate '2015-01-10 00:00:00'

or 

        python smasher.py CREATE SHELDON MS04305 --startdate '2015-01-01 00:00:00' --enddate '2015-01-10 00:00:00'


You can also call CREATE on ALL.

ALL will run all the attributes, telling you when each is done

If you call VPD, it will try to get the VPD high-resolution data which we are no longer collecting. VPD2 will compute VPD and the other attributes from the raw AIRTEMP and RELHUM.

If you just want to create one station, call

        python smasher.py CREATE SHELDON SOLAR  --startdate '2015-01-01 00:00:00' --enddate '2015-01-10 00:00:00' --station CENMET

You can use the start/end date with the station, or just call it by itself

        python smasher.py CREATE SHELDON SOLAR --station CENMET

If that station doesn't have the attribute, no problem! You just won't get anything back.

If you want to print an approved structure for CSV, add on the flag --csv TRUE to the end of your call

    python smasher.py CREATE SHELDON SOLAR  --startdate '2015-01-01 00:00:00' --enddate '2015-01-10 00:00:00' --station CENMET --csv TRUE

You can call --csv for any CREATE except "ALL". That would be a monster.


------

use the DELETE method to delete an attribute from LTERLogger_pro following a certain startdate

Because the update methods use bulk insert, if you are concerned you have messed up the data, it is easier to DELETE and rebuild it than it is to risk double entry.


For example:

        python smasher.py DELETE SHELDON SOLAR --startdate '2015-04-15 00:00:00' will delete daily SOLAR INCLUDING AND AFTER 2015-04-15. repeat: INCLUDING and AFTER!


You can also delete for only one station.

For example:

        python smasher.py DELETE SHELDON MS04305 --station CENMET --startdate '2015-04-15 00:00:00'


Be sure to use the double-dashed flags for station and startdate to indicate their placement in the command.

You can also delete for a range. THIS IS NOT RECOMMENDED. You are not protected in this program against creating duplicates, or leaving gaps. It is not hard to write to the end. You have been warned!

        python smasher.py DELETE SHELDON SOLAR --startdate '2015-04-15 00:00:00' --enddate '2015-04-20 00:00:00'

------

use the UPDATE method to update the database LTERLogger_pro 

Note that you cannot both UPDATE and make CSVs. CSVs are in the CREATE method. This is to keep you from accidentally updating the database when you just want to get some daily data from it that you may already have. However, in general, you:

Call UPDATE the same way you called CREATE, for example: 

        Foxs-MacBook-Pro:smash dataRonin$ python smasher.py UPDATE SHELDON SOLAR
         You are processing using the SMASHER Python toolkit. (c) MIT 2015. You have given the following information: 

        ~ Method: UPDATE
        ~ Attribute: SOLAR
        ~ Server: SHELDON
        ~ Start Date: None
        ~ End Date: None
        Finished writing LogFile
        finished creating Solar Radiation from 2015-04-15 00:00:00 to 2015-04-22 00:00:00
        Finished writing LogFile
        checking that the methods are updated
        Updating your methods prior to insertion in the db!
        This is gonna update the LTERLogger_Pro database
        database updated from 2015-04-15 00:00:00 to 2015-04-22 00:00:00 for Solar Radiation

Your methods will be checked against METHOD_HISTORY_DAILY, just in case.

For every attribute, an instance of mylog is generated to print errors. Although the names of the mylog files vary, they are all created in the home directory.

For example, mylog-dewpoint.csv contains:

        "ERROR","DESCRIPTION"
        "incompleteday","Incomplete or overfilled day, 2015-04-16 00:00:00, probe DEWCEN04, total number of observations: 21"
        "incompleteday","Incomplete or overfilled day, 2015-04-16 00:00:00, probe DEWCEN01, total number of observations: 21"


mylog-soilwc.csv contains:

        "ERROR","DESCRIPTION"
        "incompleteday","Incomplete or overfilled day:  2015-04-16 00:00:00, probe SWCCEN01, total number of observations: 21"
        "incompleteday","Incomplete or overfilled day:  2015-04-16 00:00:00, probe SWCCEN03, total number of observations: 21"
        "incompleteday","Incomplete or overfilled day:  2015-04-16 00:00:00, probe SWCCEN02, total number of observations: 21"
        "incompleteday","Incomplete or overfilled day:  2015-04-16 00:00:00, probe SWCCEN04, total number of observations: 21"

mylog-sonic.csv contains

        "ERROR","DESCRIPTION"
        "nullday","the total number of observations on 2015-04-20 00:00:00 is 287 and probe WNDPRI02"
        "nullday","the total number of observations on 2015-04-16 00:00:00 is 21 and probe WNDCEN02"


These files don't do anything special, and you can call them whatever you want. You can find them in the top of the condense_data() method in each Worker class.

You can also just update one station, just add the double-dashed station flag to your call.

For example:

        python smasher.py UPDATE SHELDON MS04305 --station CENMET --startdate '2015-04-10 00:00:00' --enddate '2015-05-01 00:00:00'

You do not need to use the startdate and enddate calls if you do not want to. But, if you supply a start date, you must also supply an end date. 

--------

use the READ method to read the methods in the database and output logs of discrepancies

For example: 

    python smasher.py READ STEWARTIA ALL

This reads all the methods in Stewartia for the daily

An attribute-by-attribute read is pending.

Outputs for errors in the method_code are in errorlog.csv and outputs for the errors in eventcode are in eventlog.csv. eventlog.csv is looking for when the event_code should read "METHOD" but it says something instead of "METHOD"

In the daily data, errorlog.csv looks like this:

        date_in_db,method_in_db,method_in_table,date_start_table,date_end_table
        2013-04-17,RAD014,RAD114,RADPRI02,2013-04-17 00:00:00,2050-12-31 00:00:00
        2013-04-18,RAD014,RAD114,RADPRI02,2013-04-17 00:00:00,2050-12-31 00:00:00


In the daily data, eventlog.csv looks like this:

        probe_code,date_start_table,date_end_table,current_event_code
        VPDCEN01,2014-08-26 00:00:00,2050-12-31 00:00:00,NA    
        VPDCEN01,2014-08-26 00:00:00,2050-12-31 00:00:00,NA    


If you want to read the high resolution data, just call READ on ALLHR:

    python smasher.py READ STEWARTIA ALLHR

In the HR data, errorlog_hr.csv looks like this:

        VPDCEN04,2014-08-26 00:00:00,2050-12-31 00:00:00,NA    


------


how stuff works in SMASHER.
-----------

The fundamental unit of smasher is the smashWorker classes. The Worker classes are named for what attribute they compute and flag, such as AirTemperature, DewPoint, or VPD. They are all in CamelCase and are generally the full word for the attribute they represent. You can actually call them as individuals, also, with something like:

        B = smashWorkers.AirTemperature(startdate, enddate, server)

All worker classes are in the file smashWorkers.py.  Also in smashWorkers are a DateRange class (to make sure that dates are in a range structure) and a class to encapsulate how to get methods from the method_history and method_history_daily (MethodReader). You should not run SMASHER without both a start and end date. 

The condense_data() method in each Worker class contains the physical functions used to condense the data. If you need to change the algorithms for aggregation, they are all in the condense_data() functions. Each Worker has its own condense_data() function, even if that is a lot of repeated code, so that it can be flexible in the future as loggers change. 

Some workers, like AirTemperature, are smart to handle the "new style" data with five-minute maxes. WSPD_PRO and VPD are not so smart, and have VPD2 and WSPD_PRO2 "Friends" that handle the new style. They will usually be called by default. Since most of VPD has changed, VPD2 is the default, but since not most of WSPD_PRO has changed, WSPD_PRO is the default.

The Workers are controlled by the Controls classes, found in smashControls.py. The Controls dictate date ranges, method ranges, etc., that can be used in the API. DBControl, for example, is used to find the recentest end date in the database in order to perform the minimum update.MethodControl is used to find the methods for the READ function. HRMethodControl finds the High-Resolution methods for the READ function. The controls are used to reduce the amount of data we need to process on each operation.

The Bosses class right now only contains the update boss. Originally this was a larger structure. UpdateBoss is used to write values back to the database. UpdateBoss does some final checks on methods and writes the insert statements using the information schema. It makes sure everyone behaves.

The smasher API is how you work with SMASHER in the minimum typing way. Call it from the command line and you should be up and running. It might be kind of hard to install python at first, but we can help you get through that. If you call python on the windows you need to make sure you know where your .exe is, and please DO NOT use the system python. :)

----

Recent updates to SMASHER!
---------

- totals/means now correctly assign the midnight value to the prior day.

- VPD calculations have been clarified with better names

- things like airtemp that may use older loggers pull in various date stamps. This may need to be expanded later.

- default values for height and method were added to deal with inconsistencies or added sites. They are taken in the smashWorkers function.

        height_and_method_getter(probe_code, daterange)

* gets the right probe code for daily from the table
* if it can't find it, it gives a warning and a height of 100, a method of prefix + 999 and a sitecode of "ANYMET"

- "F" and "H" flags are given for 15-minute and Hourly values that are acceptable

- DBControl can now take an argument of station to smartly get the date ranges for each station (useful for updates or creates)

- The CREATE method now allows you to use the --csv TRUE flag to create a csv with an obvious name. You can make a --csv TRUE csv with the --station WHATEVER or with all stations. You can make a CSV only if you are doing one attribute. If you are doing more than one you need to do each CSV separately. This is for your own good. You can write a loop if you want to have more csvs. Something like "for attribute in list_of_attributes; do_things(); condense_data(); write_rows_to_csv;" etc.

- All workers write errors in gathering or condensing data to myWhateverTheNameIs.log. These are overwritten each time you run the program on that attribute so if you need them, you will want to get them before re-running. They will show up where you download this program (directory-wise)