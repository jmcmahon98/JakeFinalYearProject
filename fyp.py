import psycopg2
import random
from random import randint
from random import randrange
import datetime 
import json
import string
from shapely.geometry import Polygon, Point, shape, GeometryCollection
from configparser import ConfigParser
import tkinter as tk
from tkinter import filedialog

# Function used to create a root window to allow user to select a .ini file.
def filePicker():

    # Lets User know what file is necessary
    print("Please select a .ini file with sections: [postgresql], [numPoints], [geojson], [TableName], [SQLFile] and [addColumn].")

    # To initialize tkinter, I created a Tk root widget, 
    # which is a window with a title bar and other decoration provided by the window manager.
    # The root widget has to be created before any other widgets and there can only be one root widget.
    root = tk.Tk()

    # Use .withdraw to hide root window after it is used. 
    root.withdraw()


    # Opens the file explorer to allow the user to 
    # Store selected file in fileName.
    fileName = filedialog.askopenfilename()
    
    # Asks the user which service they would like from the program.
    check = input("Would you like to connect to the database (A) or print to an SQL file (B)? (A/B): ")
    if check == "A" or check == "a":
        toDatabase(fileName)
        # Sends the fileName to the database connection program.
    elif check == "B" or check == "b":
        createSql(fileName)
        # Sends the fileName to the SQL file print program.

# Commits the points to the databse if requested by the user.
def toDatabase(fileName):

    # Acquire number of rows from file
    NUMBER_ROWS = getNumPoints(fileName)
    
    # Acquire polygon from file
    poly = getPolygon(fileName)
    
    # Acquire the name of the table from file
    TABLE_NAME = getTableName(fileName)

    # Use random_points_within function to generate all of the necessary points with the submitted polygon and number of points 
    generated_points = random_points_within(poly, NUMBER_ROWS)

    # Receive Database parameters from config file.
    params = config(fileName)

    # Connect to database using config file parameters.
    try:
        con = psycopg2.connect(**params)
    except Exception:
        print("Incorrect database credentials.")
        exit()

    print("Connected to Database Successfully.")

    # Create a cursor for the database.
    cur = con.cursor()

    # Ask the user questions about what functionality they would like.
    check = input("Would you like to create a new table? (Y/N): ")

    if check == "Y" or check == "y":
        # If "Y", create a table with the user's inputted table name 
        # Try to see the table doesn't exist
        try:
            # Create table if it doesn't exist
            cur.execute("CREATE TABLE {} (pkid SERIAL PRIMARY KEY NOT NULL, txtfield TEXT NOT NULL, thegeom GEOMETRY DEFAULT ST_GeomFromText('POINT(-6.7 54)',4326)); \n".format(TABLE_NAME))
            # Commit to database
            con.commit()
            print("Table {} created with {} points inserted.".format(TABLE_NAME, NUMBER_ROWS))
        
        # If the table exists throw exception
        except Exception:
            # Commit to database to clear cursor
            con.commit()
            # Drop the table if it exits before creating the table
            cur.execute("DROP TABLE IF EXISTS {}".format(TABLE_NAME))
            cur.execute("CREATE TABLE {} (pkid SERIAL PRIMARY KEY NOT NULL, txtfield TEXT NOT NULL, thegeom GEOMETRY DEFAULT ST_GeomFromText('POINT(-6.7 54)',4326)); \n".format(TABLE_NAME))
            con.commit()
            

    elif check == "N" or check == "n":
        # If "N", use table name submitted at the start.
        clear = input("Would you like to clear the table first? (Y/N): ")
        
        # Ask user if they would like the table cleared, not dropped. If "Y", then delete all rows.
        if clear == "Y" or check == "y":
            # Execute delete statement to clear current table.
            cur.execute("DELETE from {}".format(TABLE_NAME))
            # Execute ALTER statements to remove extra columns.
            cur.execute("ALTER TABLE {} DROP COLUMN randStr;\r".format(TABLE_NAME))
            cur.execute("ALTER TABLE {} DROP COLUMN randInt;\r".format(TABLE_NAME))
            cur.execute("ALTER TABLE {} DROP COLUMN randTime;\r".format(TABLE_NAME))

            # Commit changes to database
            con.commit()
            print("Successfully cleared table: {}.".format(TABLE_NAME))
            
        elif clear == "N" or check == "n":
            # Execute ALTER statements to remove extra columns.
            cur.execute("ALTER TABLE {} DROP COLUMN randStr;\r".format(TABLE_NAME))
            cur.execute("ALTER TABLE {} DROP COLUMN randInt;\r".format(TABLE_NAME))
            cur.execute("ALTER TABLE {} DROP COLUMN randTime;\r".format(TABLE_NAME))
        
    else:
        print("Invalid entry.")
        exit()
    # Commit changes to the database.
    con.commit()  

    # Use iterator function to iterate through all points and commit them to the database.
    pointIterDb(generated_points, cur, con, TABLE_NAME)
    print("Successfully committed {} rows to table: {}.".format(NUMBER_ROWS, TABLE_NAME))

    # Call the addColumnsDb function which determines which columns are to be added. 
    addColumnsDb(fileName, cur, con)

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

    # Opens up the sql filename given in the .ini file and writes to it.
    sqlFile = open(getSqlFile(fileName), "w")
    # Writes nothing to clear file.
    sqlFile.write("")
    # Closes sql file.
    sqlFile.close()

    # Opens up the sql filename given in the .ini file and appends to it.
    sqlFile = open(getSqlFile(fileName), "a")
    # Write to the file a DROP statement in case the table exists.
    sqlFile.write("DROP TABLE IF EXISTS {}; \n".format(TABLE_NAME))
    # Write CREATE statement to file to make table.
    sqlFile.write("CREATE TABLE {} (pkid SERIAL PRIMARY KEY NOT NULL, txtfield TEXT NOT NULL, thegeom GEOMETRY DEFAULT ST_GeomFromText('POINT(-6.7 54)',4326)); \n".format(TABLE_NAME))
    # Write a CREATE statement for a spatial index for the table.
    sqlFile.write("CREATE INDEX x_spatial_index ON {} USING gist (thegeom); \n".format(TABLE_NAME))

    # Use iterator function to iterate through all points and append them to the file.
    pointIterSql(generated_points, sqlFile, TABLE_NAME)
    print("Successfully printed {} rows to {} with table name: {}.".format(NUMBER_ROWS, getSqlFile(fileName), TABLE_NAME))

    # Call the addColumnSql function which determines which columns are to be added to the SQL file. 
    addColumnsSql(fileName, sqlFile)    

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
    fileSql = (sqlFile[0][1])
    # Stores the name of the geojson file with the bounds of the polygon.

    return fileSql

# Iterator takes in points to be commited, database cursor, database and the name of the table.
def pointIterDb(generated_points, cur, con, TABLE_NAME):
    for aPoint in generated_points:
        # for each point in all points, create a random string of size 10.
        random_string = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(10))

        # Set 'y' value to Latitude and 'x' value to Longitude.
        pointLatitude = aPoint.y
        pointLongitude = aPoint.x

        # Make INSERT statement for the database using the table name, the random string and the latitude and longitude of the point.
        queryStr = "INSERT into {} (txtField,theGeom) VALUES ('{}',ST_SetSRID(ST_MakePoint({},{}),4326)); \n".format(
            TABLE_NAME, random_string, pointLongitude, pointLatitude)

        # Execute INSERT statement into database.
        cur.execute(queryStr)

        # Commit statement to database.
        con.commit()

# Iterates through generated points and gives them a random string for the txtfield.
def pointIterSql(generated_points, sqlFile, TABLE_NAME):
    for aPoint in generated_points:
        # for each point in all points, create a random string of size 10.
        random_string = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(10))
        
        # Set 'y' value to Latitude and 'x' value to Longitude.
        pointLatitude = aPoint.y
        pointLongitude = aPoint.x

        # Make INSERT statement for the database using the table name, the random string and the latitude and longitude of the point.
        queryStr = "INSERT into {} (txtField,theGeom) VALUES ('{}',ST_SetSRID(ST_MakePoint({},{}),4326)); \r".format(
                TABLE_NAME, random_string, pointLongitude, pointLatitude)
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

    # Search for section 'addColumn' in .ini file.
    if parser.has_section('addColumn'):
        # Store elements of 'addColumn' in columns.
        columns = parser.items('addColumn')
        
    # If randStr=yes then call addColumnStrSql.
    if columns[0][1] == "yes":
        addColumnStrSql(fileName, sqlFile)

    # If randInt=yes then call addColumnIntSql.
    if columns[1][1] == "yes":
        addColumnIntSql(fileName, sqlFile)

    # If randTime=yes then call addColumnTimeSql.
    if columns[2][1] == "yes":
        addColumnTimeSql(fileName, sqlFile)

# Takes in the name of the .ini file, the database sursor and the active database connection then finds out which columns are to be added.
def addColumnsDb(fileName, cur, con):

    # create a parser.
    parser = ConfigParser()
    # read config file.
    parser.read(fileName)

    # Search for section 'addColumn' in .ini file.
    if parser.has_section('addColumn'):
        # Store elements of 'addColumn' in columns.
        columns = parser.items('addColumn')

    # If randStr=yes then call addColumnStrDb.
    if columns[0][1] == "yes":
        addColumnStrDb(fileName, cur, con)

    # If randInt=yes then call addColumnIntDb.
    if columns[1][1] == "yes":
        addColumnIntDb(fileName, cur, con)

    # If randTime=yes then call addColumnTimeDb.
    if columns[2][1] == "yes":
        addColumnTimeDb(fileName, cur, con)

# Takes in the .ini fileName and sql file name and updates the SQL file with random strings.
def addColumnStrSql(fileName, sqlFile):

    # Acquire the name of the table and the number of rows in it.
    tableName = getTableName(fileName)
    numPoints = getNumPoints(fileName)

    # Write ALTER statement used to add new column to table.
    sqlFile.write("ALTER TABLE {} ADD randStr VARCHAR(50);\r".format(tableName))
    
    # Iterate through the rows adding in a random string.
    for x in range(numPoints):
        # Generates random string.
        random_string = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(10))
        # Generate query string using the table, the generated string and the pkid.
        queryStr = "UPDATE {} SET randStr = '{}' WHERE pkid = {}; \r".format(tableName, random_string, x+1)
        # Append query to SQL file.
        sqlFile.write(queryStr)

    print("Successfully added {} random strings into {} for column randStr in table: {}".format(numPoints, getSqlFile(fileName), tableName))

# Takes in the .ini fileName and sql file name and updates the SQL file with random integers.
def addColumnIntSql(fileName, sqlFile):

    # Acquire the name of the table and the number of rows in it.
    tableName = getTableName(fileName)
    numPoints = getNumPoints(fileName)
    
    # Write ALTER statement used to add new column to table.
    sqlFile.write("ALTER TABLE {} ADD randInt INT;\r".format(tableName))

    # Iterate through the rows adding in a random integer.
    for x in range(numPoints):
        # Generates random integer between 0 and 1000.
        random_int = randint(0, 1000)
        # Generate query string using the table, the generated integer and the pkid.
        queryStr = "UPDATE {} SET randInt = '{}' WHERE pkid = {}; \r".format(tableName, random_int, x+1)
        # Append query to SQL file.
        sqlFile.write(queryStr)

    print("Successfully added {} random integers into {} for column randInt in table: {}".format(numPoints, getSqlFile(fileName), tableName))

# Takes in the .ini fileName and sql file name and updates the SQL file with random timestamps.
def addColumnTimeSql(fileName, sqlFile):
        
    # Acquire the name of the table and the number of rows in it.
    tableName = getTableName(fileName)
    numPoints = getNumPoints(fileName)

    # Write ALTER statement used to add new column to table.
    sqlFile.write("ALTER TABLE {} ADD randTime TIMESTAMP;\r".format(tableName))

    # Iterate through the rows adding in a random integer.
    for x in range(numPoints):
        # Take a random number, translate that to seconds and develops that into HH:MM:SS format.
        random_timestamp = datetime.timedelta(seconds=randrange(86400))

        # Set a range for the random dates.
        start_date = datetime.date(2020, 1, 1)
        end_date = datetime.date(2020, 12, 31)
        # Calculate range.
        time_between_dates = end_date - start_date
        # Reformat into days.
        days_between_dates = time_between_dates.days
        # Randomise the days.
        random_number_of_days = random.randrange(days_between_dates)
        # Format into YY:MM:DD.
        random_date = start_date + datetime.timedelta(days=random_number_of_days)

        # Generate query string using the table, the generated timestamp and the pkid.
        queryStr = "UPDATE {} SET randTime = '{} {}' WHERE pkid = {}; \r".format(tableName, random_date, random_timestamp, x+1)
        # Append query to SQL file.
        sqlFile.write(queryStr)

    print("Successfully added {} random timestamps into {} for column randTime in table: {}".format(numPoints, getSqlFile(fileName), tableName))

# Takes in the .ini fileName and sql file name and updates the SQL file with random strings.
def addColumnStrDb(fileName, cur, con):

    # Acquire the name of the table and the number of rows in it.
    tableName = getTableName(fileName)
    numPoints = getNumPoints(fileName)

    # Execute ALTER statement used to add new column to table in database.
    cur.execute("ALTER TABLE {} ADD randStr VARCHAR(50);\r".format(tableName))
    # Commit changes to database.
    con.commit()

    # Iterate through the rows adding in a random string.
    for x in range(numPoints):
        # Generate random string in range(10).
        random_string = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(10))
        # Create query to be committed.
        queryStr = "UPDATE {} SET randStr = '{}' WHERE pkid = {}; \r".format(tableName, random_string, x+1)
        # Execute query to table.
        cur.execute(queryStr)
        # Commit changes to database.
        con.commit()
    print("Successfully committed {} random strings into new column randStr in table: {}".format(numPoints, tableName))

# Takes in the .ini fileName and sql file name and updates the SQL file with random integers.
def addColumnIntDb(fileName, cur, con):

    # Acquire the name of the table and the number of rows in it.
    tableName = getTableName(fileName)
    numPoints = getNumPoints(fileName)
    
    # Execute ALTER statement used to add new column to table in database.
    cur.execute("ALTER TABLE {} ADD randInt INT;\r".format(tableName))
    # Commit changes to database.
    con.commit()

    # Iterate through the rows adding in a random string.
    for x in range(numPoints):
        # Generates random integer between 0 and 1000.
        random_int = randint(0, 1000)
        # Generate query string using the table, the generated integer and the pkid.
        queryStr = "UPDATE {} SET randInt = '{}' WHERE pkid = {}; \r".format(tableName, random_int, x+1)
        # Execute query to table.
        cur.execute(queryStr)
        # Commit changes to database.
        con.commit()
    print("Successfully committed {} random integers into new column randInt in table: {}".format(numPoints, tableName))

# Takes in the .ini fileName and sql file name and updates the SQL file with random timestamps.
def addColumnTimeDb(fileName, cur, con):

    # Acquire the name of the table and the number of rows in it.
    tableName = getTableName(fileName)
    numPoints = getNumPoints(fileName)

    # Execute ALTER statement used to add new column to table in database.
    cur.execute("ALTER TABLE {} ADD randTime TIMESTAMP;\r".format(tableName))
    # Commit changes to database.
    con.commit()

    # Iterate through the rows adding in a random string.
    for x in range(numPoints):
        # Take a random number, translate that to seconds and develops that into HH:MM:SS format.
        random_timestamp = datetime.timedelta(seconds=randrange(86400))

        # Set a range for the random dates.
        start_date = datetime.date(2020, 1, 1)
        end_date = datetime.date(2020, 12, 31)
        
        # Calculate range.
        time_between_dates = end_date - start_date
        # Reformat into days.
        days_between_dates = time_between_dates.days
        # Randomise the days.
        random_number_of_days = random.randrange(days_between_dates)
        # Format into YY:MM:DD.
        random_date = start_date + datetime.timedelta(days=random_number_of_days)

        # Generate query string using the table, the generated timestamp and the pkid.
        queryStr = "UPDATE {} SET randTime = '{} {}' WHERE pkid = {}; \r".format(tableName, random_date, random_timestamp, x+1)
        # Execute query to table.
        cur.execute(queryStr)
        # Commit changes to database.
        con.commit()
    print("Successfully committed {} random timestamps into new column randTime in table: {}".format(numPoints, tableName))

# Calls the first function to allow the user to pick a file.
filePicker()
