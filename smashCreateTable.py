import pymssql


def form_connection():
    """
    connects to the METDAT, FSDBDATA,
    or SHELDON database to gather the data
    returns a cursor (pymssql object)
    """
    

    # Connect to MSSQL Server
    conn = pymssql.connect(server="SHELDON.forestry.oregonstate.edu:1433",
                           user="petersonf",
                           password="D0ntd1sATLGA!!")
    

    return conn


def create_query(conn, one_or_many, *list_reference):
    
    # Create a database cursor
    cursor = conn.cursor()


    #cursor.execute("select column_name, table_name from LTERLogger_Pro.information_schema.columns where table_name like 'Test' and table_schema like 'dbo'")

    if one_or_many == "one":

        cursor.execute("insert into LTERLogger_Pro.dbo.Test (CreateTime, DValue) VALUES ('2015-01-01 00:00:00', 14.5678766)")

        conn.commit()

    elif one_or_many == "many":

        if list_reference and list_reference != []:

            # the list is actually the first item in the args
            insertion_list = list_reference[0]
            
            # for some reason it likes to use "d" which is not the usual "f" formatter for doing floats. Yes, a total mystery. Example of working prototype        
            # cursor.executemany("insert into LTERLogger_Pro.dbo.Test (CreateTime, DValue) VALUES (%s, %d)", [('2015-01-02 00:00:00', 17.343823), ('2015-01-03 00:00:00', 18.238123), ('2015-01-04 00:00:00', 23.328)])

            cursor.executemany("insert into LTERLogger_Pro.dbo.Test (CreateTime, DValue) VALUES (%s, %d)", insertion_list)

            conn.commit()

        elif list_reference == [] or not list_reference:

            print "Need to provide a list of values"


def delete_query(conn, operation, startdate):
    
    # Create a database cursor
    cursor = conn.cursor()

    # sample of a working query:
    # cursor.execute("delete from LTERLogger_Pro.dbo.Test where CreateTime > '2015-01-06 00:00:00'")

    if operation = ">":

        cursor.execute("delete from LTERLogger_Pro.dbo.Test where CreateTime > \'" + startdate +"\'")

        conn.commit()

    elif operation = "=":

        cursor.execute("delete from LTERLogger_Pro.dbo.Test where CreateTime = \'" + startdate + "\'")
        conn.commit()
    
    else:

        print "you have not selected a valid operation, try \'>\' or \'=\'"


def update_query(conn, newvalue, match_date):

    # Create a database cursor:
    cursor = conn.cursor()

    # sample of a working query:
    # cursor.execute("update LTERLogger_Pro.dbo.Test set DValue = 77.7777  where CreateTime = '2015-01-04 00:00:00'")
    cursor.execute("update LTERLogger_Pro.dbo.Test set Dvalue = " + str(newvalue) + "where CreateTime = \'" + match_date + "\'")

    cursor.commit()

def update_most_recent(conn, last_good_date, better_data):

    # Create a database cursor

    cursor = conn.cursor()
    # get the date and value following the last good date you trust, as well as subsequent dates, and their values

    cursor.execute("select CreateTime, Dvalue from LTERLogger_Pro.dbo.Test where CreateTime > \'" + last_good_date + "\'")

    od = {}

    for row in cursor:
        dt = str(row[0]).rstrip('0').rstrip('.')
        val = str(row[1])

        if dt not in od:
            od[dt] = val
        elif dt in od:
            print "the last insert contained duplicated values, you will need to run a delete method instead"
            break

    for dt in od.keys():

        nv = better_data[dt]
        cursor.execute("update LTERLogger_Pro.dbo.Test set Dvalue = " + str(nv) + " where CreateTime = \'" + dt + "\'" )

        cursor.commit()

    # update these data with new values, without having replace the date stamps

