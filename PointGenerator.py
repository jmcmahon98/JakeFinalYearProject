import datetime 
import json
import string
import psycopg2
import random
import tkinter as tk
from random import randint
from random import randrange
from shapely.geometry import Polygon, Point, shape, GeometryCollection
from configparser import ConfigParser
from tkinter import filedialog

# Function used to create a root window to allow user to select a .ini file.
def filePicker():

    # Lets User know what file is necessary
    print("Please select a .ini file with sections: [postgresql], [numPoints], [geojson], [TableName], [SQLFile], [addColumn] and [colVals].")

    # To initialize tkinter, I created a Tk root widget, which is a window with a title bar and other decoration provided by the window manager.
    # The root widget has to be created before any other widgets and there can only be one root widget.
    root = tk.Tk()

    # Use .withdraw to hide root window after it is used. 
    root.withdraw()

    # Opens the file explorer to allow the user to store selected file in fileName.
    fileName = filedialog.askopenfilename()
    
    # Asks the user which service they would like from the program.
    check = input("Would you like to connect to the database (A) or print to an SQL file (B)? (A/B): ")
    # Checks A or B
    if check == "A" or check == "a":
        # Sends the fileName to the database connection program.
        toDatabase(fileName)

    elif check == "B" or check == "b":
        # Sends the fileName to the SQL file print program.
        createSql(fileName)
    else:
        print("Invalid Entry")
        exit()

# Commits the points to the databse if requested by the user.
def toDatabase(fileName):

    # Acquire number of rows from file.
    NUMBER_ROWS = getNumPoints(fileName)
    # Acquire polygon from file.
    poly = getPolygon(fileName)
    # Acquire the name of the table from file.
    TABLE_NAME = getTableName(fileName)
    # Use random_points_within function to generate all of the necessary points with the submitted polygon and number of points .
    generated_points = random_points_within(poly, NUMBER_ROWS)
    # Receive Database parameters from config file.
    params = config(fileName)

    # Connect to database using config file parameters, throw exception and close if wrong credentials.
    try:
        # Make connection with given parameters.
        con = psycopg2.connect(**params)
        print("Connected to Database Successfully.")
    except Exception:
        print("Incorrect database credentials.")
        exit()

    # Create a cursor for the database.
    cur = con.cursor()

    # Ask the user questions about what functionality they would like.
    check = input("Would you like to create a new table? (Y/N): ")

    if check == "Y" or check == "y":
        # If "Y", create a table with the user's inputted table name 
        # Try to see the table doesn't exist
        try:
            # Create table if it doesn't exist
            cur.execute(
                "CREATE TABLE {} (pkid SERIAL PRIMARY KEY NOT NULL, randStr TEXT, randInt INT, randTime TIMESTAMP, thegeom GEOMETRY DEFAULT ST_GeomFromText('POINT(-6.7 54)',4326)); \n".format(TABLE_NAME))
            # Commit to database.
            con.commit()
            print("Table {} created. ".format(TABLE_NAME))
        
        # If the table exists throw exception.
        except Exception:
            # Commit to database to clear cursor.
            con.commit()
            # Drop the table if it exits before creating the table.
            cur.execute("DROP TABLE IF EXISTS {}".format(TABLE_NAME))
            # Create table with all possible columns.
            cur.execute(
                "CREATE TABLE {} (pkid SERIAL PRIMARY KEY NOT NULL, randStr TEXT, randInt INT, randTime TIMESTAMP, thegeom GEOMETRY DEFAULT ST_GeomFromText('POINT(-6.7 54)',4326)); \n".format(TABLE_NAME))
            # Create spatial index with table name in index name.
            cur.execute("CREATE INDEX {}_spatial_index ON {} USING gist (thegeom); \n".format(TABLE_NAME, TABLE_NAME))
            # Commit all to database
            con.commit()
            print("Table {} created. ".format(TABLE_NAME))
    
    # Asks the user if they'd like to clear the table if they're not creating a new one.
    elif check == "N" or check == "n":
        # If "N", use table name submitted at the start.
        clear = input("Would you like to clear the table first? (Y/N): ")
        
        # Ask user if they would like the table cleared, not dropped. If "Y", then delete all rows.
        if clear == "Y" or check == "y":
            # Execute delete statement to clear current table.
            cur.execute("DELETE from {}".format(TABLE_NAME))
            # Commit changes to database
            con.commit()
            print("Successfully cleared table: {}.".format(TABLE_NAME))
        elif clear =="N" or clear == "n":
            print("{} rows successfully added to table: {}".format(NUMBER_ROWS, TABLE_NAME))
        else:
            # Close program if wrong entry.
            print("Invalid entry.")
            exit()

    else:
        # Close program if wrong entry.
        print("Invalid entry.")
        exit()
    # Commit changes to the database.
    con.commit()  

    # Use iterator function to execute insert statements to database
    pointIterDb(generated_points, cur, con, TABLE_NAME, fileName)
    print("Successfully committed {} rows to table: {}.".format(NUMBER_ROWS, TABLE_NAME))

# Writes the points to an SQL file if requested by the user for manual commits.
def createSql(fileName):

    # Acquire number of rows from file.
    NUMBER_ROWS = getNumPoints(fileName)
    # Acquire polygon from file.
    poly = getPolygon(fileName)
    # Use random_points_within function to generate all of the necessary points with the submitted polygon and number of points.
    generated_points = random_points_within(poly, NUMBER_ROWS)
    # Acquire the name of the table from file.
    TABLE_NAME = getTableName(fileName)
    try:
        # Opens up the sql filename given in the .ini file and writes to it.
        sqlFile = open(getSqlFile(fileName), "w")
    except Exception:
        print("No SQL file name found.\nClosed.")
        exit()

    # Writes nothing to clear file.
    sqlFile.write("")
    # Closes sql file.
    sqlFile.close()

    # Opens up the sql filename given in the .ini file and appends to it.
    sqlFile = open(getSqlFile(fileName), "a")
    # Write to the file a DROP statement in case the table exists.
    sqlFile.write("DROP TABLE IF EXISTS {}; \n".format(TABLE_NAME))
    # Write CREATE statement to file to make table with optional columns.
    sqlFile.write(
        "CREATE TABLE {} (pkid SERIAL PRIMARY KEY NOT NULL, randStr TEXT, randInt INT, randTime TIMESTAMP, thegeom GEOMETRY DEFAULT ST_GeomFromText('POINT(-6.7 54)',4326)); \n".format(TABLE_NAME))
    # Write a CREATE statement for a spatial index for the table.
    sqlFile.write("CREATE INDEX {}_spatial_index ON {} USING gist (thegeom); \n".format(TABLE_NAME, TABLE_NAME))
    # Use iterator function to iterate through all points and append them to the file.
    pointIterSql(generated_points, sqlFile, TABLE_NAME, fileName)
    print("Successfully printed {} rows to {} with table name: {}.".format(NUMBER_ROWS, getSqlFile(fileName), TABLE_NAME))

# Config function takes in a .ini file and acquires the database connect information from it
def config(fileName):

    # create a parser.
    parser = ConfigParser()
    # read config file.
    parser.read(fileName)

    # Check parser for section of name 'postgresql'.
    db = {}     # Create empty list to store config information.
    if parser.has_section('postgresql'):
        params = parser.items('postgresql')

        # Store section in params list.
        for param in params:
            db[param[0]] = param[1]
            # Move everything from params list to db list.
            
    else:
        # If no 'postgresql' section found, skip.
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, fileName))
    
    # Return the db list.
    return db

# Takes in the name of the .ini file, finds the TableName section and finds the table name.
def getTableName(fileName):

    # create a parser.
    parser = ConfigParser()
    # read config file.
    parser.read(fileName)

    # Check parser for section of name 'TableName'.
    if parser.has_section('TableName'):
        nameOfTable = parser.items('TableName')
        # Store items in section 'TableName' in nameOfTable list of tuples.
    else:
        print("No [TableName] in .ini file.\nClosed.")
        exit()
    TABLE_NAME = (nameOfTable[0][1])

    # Stores the name of the table.
    return TABLE_NAME

# Takes in the name of the .ini file, finds the numPoints section and finds out how many points are wanted.
def getNumPoints(fileName):

    # create a parser.
    parser = ConfigParser()
    # read config file.
    parser.read(fileName)

    # Check parser for section of name 'numPoints'.
    if parser.has_section('numPoints'):
        pointsNum = parser.items('numPoints')
        # Store items in section 'numPoints' in pointsNum list of tuples.
    else:
        print("No [numPoints] in .ini file.\nClosed.")
        exit()
    # Store value for number of points in NUMBER_ROWS.
    try:
        NUMBER_ROWS = int(pointsNum[0][1])
        # If the number of rows submitted is not an integer throw exception.
    except ValueError:
        print("numPoints must be an integer.")
        exit()

    if NUMBER_ROWS >= 0:
        return NUMBER_ROWS
        # Make sure NUMBER_ROWS is positive.
    else:
        print("numPoints must be a positive integer.")
        exit()    

# Takes in the name of the .ini file and finds out which file has the geojson polygon.
def getPolygon(fileName):

    # create a parser.
    parser = ConfigParser()
    # read config file.
    parser.read(fileName)

    try:
        # Check parser for section of name 'geojson'.
        if parser.has_section('geojson'):
            shapeBounds = parser.items('geojson')
            # Store items in section 'geojson' in shapeBounds list of tuples.
        else:
            print("No [geojson] in .ini file.\nClosed.")
            exit()
        # Store value of geojson name in geojsonName
        geojsonName = (shapeBounds[0][1])
    
        # Load in GeoJSON file from .ini file as f and obtain "features" object.
        with open(geojsonName) as f:
            features = json.load(f)["features"]
        
        # Define shape as "geometry" from GeoJSON file and buffer(0) removes overlapping coordinates.
        poly = (GeometryCollection([shape(feature["geometry"]).buffer(0) for feature in features]))
        return poly

    except FileNotFoundError:
        # If file not found, end program.
        print("GeoJSON file not found.")
        exit()
    # Stores the name of the geojson file with the bounds of the polygon.

# Takes in the name of the .ini file and finds out which file will be wrote to to create an SQL file.
def getSqlFile(fileName):

    # create a parser.
    parser = ConfigParser()
    # read config file.
    parser.read(fileName)
    # Check parser for section of name 'geojson'.
    if parser.has_section('SQLFile'):
        sqlFile = parser.items('SQLFile')
        # Store items in section 'geojson' in shapeBounds list of tuples.
    else:
        print("No [SQLFile] in .ini file.\nClosed.")
        exit()
    # Store the sql file name in fileSql.
    try:
        fileSql = (sqlFile[0][1])
    except Exception:
        print("No SQL file name found.\nClosed.")
        exit()
    return fileSql

# Iterator takes in points to be commited, database cursor, database and the name of the table.
def pointIterDb(generated_points, cur, con, TABLE_NAME, fileName):
    
    # Acquire the lists 'check' and 'values' from addColumnsDb.
    check = addColumnsDb(fileName, cur, con)
    # Store first tuple in check_bool and second in check_bounds.
    check_bool = check[0]
    check_bounds = check[1]
    
    # Acquire inputted string length from .ini file
    try:
        strLen = int(check_bounds[0])
    except ValueError:
        print("strLen must be a positive integer.\nClosed.")
        exit()

    if strLen <= 0:
        print("strLen must be a positive integer.\nClosed.")
        exit()
    # Acquire inputted int bounds from .ini file
    try:
        intStart = int(check_bounds[1])
    except ValueError:
        print("intStart must be an integer.\nClosed.")
        exit()
    try:
        intEnd = int(check_bounds[2])
    except ValueError:
        print("intEnd must be an integer.\nClosed.")
        exit()

    if intStart >= intEnd:
        print("intStart must be smaller than intEnd.\nClosed.")
        exit()

    # Acquire inputted time stamp start date from .ini file and change the format to suit datetime.date
    startDate = list(check_bounds[3])
    # Change inputted bounds into desired int value.
    try:
        startYear = int(startDate[0]+startDate[1]+startDate[2]+startDate[3])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    try:
        startMonth = int(startDate[5]+startDate[6])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    try:
        startDay = int(startDate[8]+startDate[9])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    
    # Acquire inputted time stamp end date from .ini file and change the format to suit datetime.date
    endDate = list(check_bounds[4])
    # Change inputted bounds into desired int value.
    try:
        endYear = int(endDate[0]+endDate[1]+endDate[2]+endDate[3])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    try:
        endMonth = int(endDate[5]+endDate[6])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    try:
        endDay = int(endDate[8]+endDate[9])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()

    if startYear > endYear:
        print("timeStart must be before timeEnd.\nClosed.")
        exit()
    elif startYear == endYear and startMonth > endMonth:
        print("timeStart must be before timeEnd.\nClosed.")
        exit()
    elif startYear == endYear and startMonth == endMonth and startDay >= endDay:
        print("timeStart must be before timeEnd.\nClosed.")
        exit()

    # Iterate through every point generated. 
    for aPoint in generated_points:
        # Create random string with size inputted in .ini file.
        random_string = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(strLen))
        # Create random int with bounds inputted from .ini file.
        random_int = randint(intStart, intEnd)
        # Take a random number, translate that to seconds and develops that into HH:MM:SS format.
        random_timestamp = datetime.timedelta(seconds=randrange(86400))
        # Set a range based off of the inputted dates.
        start_date = datetime.date(startYear, startMonth, startDay)
        end_date = datetime.date(endYear, endMonth, endDay)
        # Calculate range.
        time_between_dates = end_date - start_date
        # Reformat into days.
        days_between_dates = time_between_dates.days
        # Randomise the days.
        random_number_of_days = random.randrange(days_between_dates)
        # Format into YY:MM:DD.
        random_date = start_date + datetime.timedelta(days=random_number_of_days)

        # Set 'y' value to Latitude and 'x' value to Longitude.
        pointLatitude = aPoint.y
        pointLongitude = aPoint.x

        if check_bool[0] == True and check_bool[1] == True and check_bool[2] == True:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randStr, randInt, randTime, theGeom) VALUES ('{}', {}, '{} {}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, random_int, random_date, random_timestamp, pointLongitude, pointLatitude)
            # Execute query string to database
            try:
                cur.execute(queryStr)
            except Exception:
                print("Table columns have changed.\nClosed.")
                exit()
            # Commit changes to database
            con.commit()

        elif check_bool[0] == False and check_bool[1] == True and check_bool[2] == True:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randInt, randTime, theGeom) VALUES ({}, '{} {}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_int, random_date, random_timestamp, pointLongitude, pointLatitude)
            # Execute query string to database
            try:
                cur.execute(queryStr)
            except Exception:
                print("Table columns have changed.\nClosed.")
                exit()
            # Commit changes to database
            con.commit()

        elif check_bool[0] == True and check_bool[1] == False and check_bool[2] == True:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randStr, randTime, theGeom) VALUES ('{}','{} {}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, random_date, random_timestamp, pointLongitude, pointLatitude)
            # Execute query string to database
            try:
                cur.execute(queryStr)
            except Exception:
                print("Table columns have changed.\nClosed.")
                exit()
            # Commit changes to database
            con.commit()

        elif check_bool[0] == True and check_bool[1] == True and check_bool[2] == False:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randStr, randInt, theGeom) VALUES ('{}', {}, ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, random_int, pointLongitude, pointLatitude)
            # Execute query string to database
            try:
                cur.execute(queryStr)
            except Exception:
                print("Table columns have changed.\nClosed.")
                exit()
            # Commit changes to database
            con.commit()

        elif check_bool[0] == True and check_bool[1] == False and check_bool[2] == False:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randStr, theGeom) VALUES ('{}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, pointLongitude, pointLatitude)
            # Execute query string to database
            try:
                cur.execute(queryStr)
            except Exception:
                print("Table columns have changed.\nClosed.")
                exit()
            # Commit changes to database
            con.commit()

        elif check_bool[0] == False and check_bool[1] == True and check_bool[2] == False:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randInt, theGeom) VALUES ({}, ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_int, pointLongitude, pointLatitude)
            # Execute query string to database
            try:
                cur.execute(queryStr)
            except Exception:
                print("Table columns have changed.\nClosed.")
                exit()
            # Commit changes to database
            con.commit()

        elif check_bool[0] == False and check_bool[1] == False and check_bool[2] == True:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randTime, theGeom) VALUES ('{} {}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_date, random_timestamp, pointLongitude, pointLatitude)
            # Execute query string to database
            try:
                cur.execute(queryStr)
            except Exception:
                print("Table columns have changed.\nClosed.")
                exit()
            # Commit changes to database
            con.commit()

        elif check_bool[0] == False and check_bool[1] == False and check_bool[2] == False:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (theGeom) VALUES (ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, pointLongitude, pointLatitude)
            # Execute query string to database
            try:
                cur.execute(queryStr)
            except Exception:
                print("Table columns have changed.\nClosed.")
                exit()
            # Commit changes to database
            con.commit()

# Iterates through generated points and gives them a random string for the txtfield.
def pointIterSql(generated_points, sqlFile, TABLE_NAME, fileName):

    # Acquire the lists 'check' and 'values' from addColumnsSql
    check = addColumnsSql(fileName, sqlFile)
    # Store first tuple in check_bool and second in check_bounds.
    check_bool = check[0]
    check_bounds = check[1]
    
    # Acquire inputted string length from .ini file
    try:
        strLen = int(check_bounds[0])
    except ValueError:
        print("strLen must be a positive integer.\nClosed.")
        exit()

    if strLen <= 0:
        print("strLen must be a positive integer.\nClosed.")
        exit()
    # Acquire inputted int bounds from .ini file
    try:
        intStart = int(check_bounds[1])
    except ValueError:
        print("intStart must be an integer.\nClosed.")
        exit()
    try:
        intEnd = int(check_bounds[2])
    except ValueError:
        print("intEnd must be an integer.\nClosed.")
        exit()

    if intStart >= intEnd:
        print("intStart must be smaller than intEnd.\nClosed.")
        exit()

    # Acquire inputted time stamp start date from .ini file and change the format to suit datetime.date
    startDate = list(check_bounds[3])
    # Change inputted bounds into desired int value.
    try:
        startYear = int(startDate[0]+startDate[1]+startDate[2]+startDate[3])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    try:
        startMonth = int(startDate[5]+startDate[6])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    try:
        startDay = int(startDate[8]+startDate[9])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    
    # Acquire inputted time stamp end date from .ini file and change the format to suit datetime.date
    endDate = list(check_bounds[4])
    # Change inputted bounds into desired int value.
    try:
        endYear = int(endDate[0]+endDate[1]+endDate[2]+endDate[3])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    try:
        endMonth = int(endDate[5]+endDate[6])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()
    try:
        endDay = int(endDate[8]+endDate[9])
    except ValueError:
        print("timeStart must be of the form YYYY,MM,DD.\nClosed.")
        exit()

    if startYear > endYear:
        print("timeStart must be before timeEnd.\nClosed.")
        exit()
    elif startYear == endYear and startMonth > endMonth:
        print("timeStart must be before timeEnd.\nClosed.")
        exit()
    elif startYear == endYear and startMonth == endMonth and startDay >= endDay:
        print("timeStart must be before timeEnd.\nClosed.")
        exit()

    # Iterate through every point generated. 
    for aPoint in generated_points:
        # Create random string with size inputted in .ini file.
        random_string = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(strLen))
        # Create random int with bounds inputted from .ini file.
        random_int = randint(intStart, intEnd)
        # Take a random number, translate that to seconds and develops that into HH:MM:SS format.
        random_timestamp = datetime.timedelta(seconds=randrange(86400))
        # Set a range based off of the inputted dates.
        start_date = datetime.date(startYear, startMonth, startDay)
        end_date = datetime.date(endYear, endMonth, endDay)
        # Calculate range.
        time_between_dates = end_date - start_date
        # Reformat into days.
        days_between_dates = time_between_dates.days
        # Randomise the days.
        random_number_of_days = random.randrange(days_between_dates)
        # Format into YY:MM:DD.
        random_date = start_date + datetime.timedelta(days=random_number_of_days)
        
        # Set 'y' value to Latitude and 'x' value to Longitude.
        pointLatitude = aPoint.y
        pointLongitude = aPoint.x

        if check_bool[0] == True and check_bool[1] == True and check_bool[2] == True:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randStr, randInt, randTime, theGeom) VALUES ('{}', {}, '{} {}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, random_int, random_date, random_timestamp, pointLongitude, pointLatitude)
            # Write query string to SQL file.
            sqlFile.write(queryStr)

        elif check_bool[0] == False and check_bool[1] == True and check_bool[2] == True:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randInt, randTime, theGeom) VALUES ({}, '{} {}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_int, random_date, random_timestamp, pointLongitude, pointLatitude)
            # Write query string to SQL file.
            sqlFile.write(queryStr)

        elif check_bool[0] == True and check_bool[1] == False and check_bool[2] == True:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randStr, randTime, theGeom) VALUES ('{}','{} {}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, random_date, random_timestamp, pointLongitude, pointLatitude)
            # Write query string to SQL file.
            sqlFile.write(queryStr)

        elif check_bool[0] == True and check_bool[1] == True and check_bool[2] == False:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randStr, randInt, theGeom) VALUES ('{}', {}, ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, random_int, pointLongitude, pointLatitude)
            # Write query string to SQL file.
            sqlFile.write(queryStr)

        elif check_bool[0] == True and check_bool[1] == False and check_bool[2] == False:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randStr, theGeom) VALUES ('{}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, pointLongitude, pointLatitude)
            # Write query string to SQL file.
            sqlFile.write(queryStr)

        elif check_bool[0] == False and check_bool[1] == True and check_bool[2] == False:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randInt, theGeom) VALUES ({}, ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_int, pointLongitude, pointLatitude)
            # Write query string to SQL file.
            sqlFile.write(queryStr)

        elif check_bool[0] == False and check_bool[1] == False and check_bool[2] == True:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (randTime, theGeom) VALUES ('{} {}', ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_date, random_timestamp, pointLongitude, pointLatitude)
            # Write query string to SQL file.
            sqlFile.write(queryStr)

        elif check_bool[0] == False and check_bool[1] == False and check_bool[2] == False:
            # Make INSERT statement for the database using the table name, the extra columns and the latitude and longitude of the point.
            queryStr = "INSERT into {} (theGeom) VALUES (ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, pointLongitude, pointLatitude)
            # Write query string to SQL file.
            sqlFile.write(queryStr)

# Generates random points throughout the submitted polygon and returns them.
def random_points_within(poly, num_points):
    # Acquire shape bounds from poly.
    min_x, min_y, max_x, max_y = poly.bounds

    # Make list to hold all points.
    points = []
    
    # While theres more points than in the points list, generate a random point and append to list.
    while len(points) < num_points:

        # Generate random point using random.uniform.
        random_point = Point(
            [random.uniform(min_x, max_x), random.uniform(min_y, max_y)])

        # Append the points if within polygon.
        if (random_point.within(poly)):  
            points.append(random_point)

    return points

# Takes in the name of the .ini file and finds out which columns are to be added.
def addColumnsSql(fileName, sqlFile):

    # create a parser.
    parser = ConfigParser()
    # read config file.
    parser.read(fileName)
    tableName = getTableName(fileName)

    # Create lists to store .ini file values.
    check = {}
    values = {}
    # Search for section 'addColumn' in .ini file.
    if parser.has_section('addColumn'):
        # Store elements of 'addColumn' in columns.
        columns = parser.items('addColumn')
    else:
        print("No [addColumn] section in .ini file.\nClosed.")
        exit()
    
    # Search .ini file for section colVals. 
    if parser.has_section('colVals'):
        # Store elements of 'colVals' in bounds.
        bounds = parser.items('colVals')
    else:
        print("No [colVals] section in .ini file.\nClosed.")
        exit()
    
    values[0] = bounds[0][1]
    values[1] = bounds[1][1]
    values[2] = bounds[2][1]
    values[3] = bounds[3][1]
    values[4] = bounds[4][1]

    # If randStr=yes, make the corresponding check value True else False. Then store the values for the bounds to values list.
    if columns[0][1] == "yes":
        check[0] = True
    else:
        # Drop column randStr if 'no' in .ini file.
        sqlFile.write("ALTER TABLE {} DROP COLUMN randStr;\r".format(tableName))
        check[0] = False

    # If randInt=yes, make the corresponding check value True else False. Then store the values for the bounds to values list.
    if columns[1][1] == "yes":
        check[1] = True
    else:
        # Drop column randInt if 'no' in .ini file.
        sqlFile.write("ALTER TABLE {} DROP COLUMN randInt;\r".format(tableName))
        check[1] = False

    # If randTime=yes, make the corresponding check value True else False. Then store the values for the bounds to values list.
    if columns[2][1] == "yes":
        check[2] = True
    else:
        # Drop column randTime if 'no' in .ini file.
        sqlFile.write("ALTER TABLE {} DROP COLUMN randTime;\r".format(tableName))
        check[2] = False
    
    # Return back lists: check and values, with the information on whether the columns are wanted and what the bounds of the columns are. 
    return check, values

# Takes in the name of the .ini file, the database sursor and the active database connection then finds out which columns are to be added.
def addColumnsDb(fileName, cur, con):

    # create a parser.
    parser = ConfigParser()
    # read config file.
    parser.read(fileName)
    tableName = getTableName(fileName)
    # Create lists to store .ini file values.
    check = {}
    values = {}

    # Search for section 'addColumn' in .ini file.
    if parser.has_section('addColumn'):
        # Store elements of 'addColumn' in columns.
        columns = parser.items('addColumn')
    else:
        print("No [addColumn] section in .ini file.\nClosed.")
        exit()

    # Search for section 'colVals' in .ini file.
    if parser.has_section('colVals'):
        bounds = parser.items('colVals')
    else:
        print("No [colVals] section in .ini file.\nClosed.")
        exit()

    # Store all of the bounds information in values list.
    values[0] = bounds[0][1]
    values[1] = bounds[1][1]
    values[2] = bounds[2][1]
    values[3] = bounds[3][1]
    values[4] = bounds[4][1]

    # If randStr=yes, make the corresponding check value True else False. Then store the values for the bounds to values list.
    if columns[0][1] == "yes":
        check[0] = True
    else:
        check[0] = False
        try:
            # Drop column randStr if 'no' in .ini file.
            cur.execute("ALTER TABLE {} DROP COLUMN randStr;\r".format(tableName))
        except Exception:
            print("Column randStr kept.")
        # Commit changes to database if there are any.
        con.commit()

    # If randInt=yes, make the corresponding check value True else False. Then store the values for the bounds to values list.
    if columns[1][1] == "yes":
        check[1] = True
    else:
        check[1] = False
        try:
            # Drop column randStr if 'no' in .ini file.
            cur.execute("ALTER TABLE {} DROP COLUMN randInt;\r".format(tableName))
        except Exception:
            print("Column randInt kept.")
        # Commit changes to database if there are any.
        con.commit()

    # If randTime=yes, make the corresponding check value True else False. Then store the values for the bounds to values list.
    if columns[2][1] == "yes":
        check[2] = True
    else:
        check[2] = False
        try:
            # Drop column randStr if 'no' in .ini file.
            cur.execute("ALTER TABLE {} DROP COLUMN randTime;\r".format(tableName))
        except Exception:
            print("Column randTime kept.")
            # Commit changes to database if there are any.
        con.commit()
    
    # Return back lists: check and values, with the information on whether the columns are wanted and what the bounds of the columns are. 
    return check, values

# Begins program. 
if __name__ == '__main__':
    # Calls the first function to allow the user to pick a file.
    filePicker()
