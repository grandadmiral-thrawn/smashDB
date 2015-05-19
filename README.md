smasher
========

v 0.0.4

smasher is a command line tool for updating LTERLogger_pro, our MSSQL 1st level provisional server. It can also update MS043, the production level annual server. 

- smasher will work for attributes of AIRTEMP, RELHUM, DEWPT, VPD, SOLAR, WSPD_SNC, WSPD_PRO, NR, LYS, PRECIP, PAR, SOILTEMP, SNOWDEPTH (pending), LIGHT (pending) and SOILWC.

- for wind, smasher uses daily methods which respect the standard deviations and means of the day directionally; i.e. it decomposes and computes a mean resultant wind for the propellor anemometer, and uses the Yamartino method to get a coordinate-appropriate standard deviation.

- smasher does not perform flagging on the high resolution data. smasher simply "smashes" high resolution flags into the daily aggregate.

- smasher can compute the daily VPD from the daily AIRTEMP and RELHUM, or it can take the high resolution values. when it computes VPD, it also computes SATVP and VAPDEF. 

- smasher knows when to assign net radiometer to the net radiometer table and sonic wind to the sonic wind table. 

- smasher when processing 5 minute maxes and mins in air temp and dew point, will first look for the daily max or min from a measured max or min for the five minute interval. if that column is null all day, it will then look to the mean to find the max or min for the day based on the extreme values about the mean. 

smasher is a product of Fox Peterson, Hans Luh, and Don Henshaw.

Using smasher.
===============

Using smasher is super easy.

First, download this repo!

[super awesome smasher repo](https://github.com/dataRonin/smashDB.git)

okay you're ready.

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


You can also call CREATE on ALL or BIG4.

ALL will run all the attributes, telling you when each is done

BIG4 will run AIRTEMP, RELHUM, DEWPT, and VPD2. If you call VPD, it will try to get the VPD high-resolution data which we are no longer collecting. VPD2 will compute VPD and the other attributes from the raw AIRTEMP and RELHUM.



------

use the DELETE method to delete an attribute from LTERLogger_pro following a certain startdate

Because the update methods use bulk insert, if you are concerned you have messed up the data, it is easier to DELETE and rebuild it than it is to risk double entry.


For example:

        python smasher.py DELETE SHELDON SOLAR --startdate '2015-04-15 00:00:00' will delete daily SOLAR INCLUDING AND AFTER 2015-04-15. repeat: INCLUDING and AFTER!


You can also delete for only one station.

For example:

        python smasher.py DELETE SHELDON MS04305 --station CENMET --startdate '2015-04-15 00:00:00'


Be sure to use the double-dashed flags for station and startdate to indicate their placement in the command.


------

use the UPDATE method to update the database LTERLogger_pro 


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


how stuff works in smasher.
-----------

The fundamental unit of smasher is the Worker classes. The Worker classes are named for what attribute they compute and flag, such as AirTemperature, DewPoint, or VPD. All worker classes are in the file smashWorkers.py.  Also in smashWorkers are a DateRange class (to make sure that dates are in a range structure) and a class to encapsulate how to get methods from the method_history.

The condense_data() method in each Worker class contains the physical functions used to condense the data. If you need to change the math fundamentally, this is where to do it.


The Workers are controlled by the Controls classes, found in smashControls.py. The Controls dictate date ranges, method ranges, etc., that can be used in the API. DBControl, for example, is used to find the recentest end date in the database in order to perform the minimum update. MethodControl is used to find the methods for the READ function. HRMethodControl finds the High-Resolution methods for the READ function. The controls are used to reduce the amount of data we need to process on each operation.

The Bosses class right now only contains the update boss. Originally this was a larger structure. UpdateBoss is used to write values back to the database. UpdateBoss does some final checks on methods and writes the insert statements using the information schema. It makes sure everyone behaves.


The smasher API is how you work with smasher in the minimum way. Call it from the command line and you should be up and running. It might be kind of hard to install python at first, but we can help you get through that.



