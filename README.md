smasher
========

v 0.0.3

smasher is a command line tool for updating LTERLogger_pro, our MSSQL 1st level provisional server. 

smasher will work for attributes of AIRTEMP, RELHUM, DEWPT, VPD, SOLAR, WSPD_SNC, WSPD_PRO, NR, LYS, PRECIP, PAR, SOILTEMP, and SOILWC.

for wind, smasher uses daily methods which respect the standard deviations and means of the day directionally; i.e. it decomposes and computes a mean resultant wind for the propellor anemometer, and uses the Yamartino method to get a coordinate-appropriate standard deviation.

smasher does not perform flagging. flagging can be done with the (not published) xtempx toolkit. 

smasher can compute the daily VPD from the daily AIRTEMP and RELHUM, or it can take the high resolution values.

smasher is a product of Fox Peterson, Hans Luh, and Don Henshaw.
