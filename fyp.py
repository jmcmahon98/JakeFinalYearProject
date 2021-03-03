import psycopg2
import random
import json
import string
from shapely.geometry import Polygon, Point, shape, GeometryCollection
from configparser import ConfigParser
import tkinter as tk
from tkinter import filedialog

# Function used to create a root window to allow user to select a .ini file.
def filePicker():

    print("Please select a .ini file with sections: [postgresql], [numPoints], [geojson], [TableName] and [SQLFile].")
    # To initialize tkinter, I created a Tk root widget, 
    # which is a window with a title bar and other decoration provided by the window manager.
    # The root widget has to be created before any other widgets and there can only be one root widget.
    root = tk.Tk()

    # Use .withdraw to hide root window after it is used. 
    root.withdraw()


    # Store selected file in fileName.
    fileName = filedialog.askopenfilename()
    
    check = input("Would you like to connect to the database or print to an SQL file? (A/B): ")
    if check == "A":
        toDatabase(fileName)
    elif check == "B":
        createSql(fileName)

    return 0

# Commits the points to the databse if requested by the user.
def toDatabase(fileName):

    # Acquire number of rows from file
    NUMBER_ROWS = getNumPoints(fileName)
    
    # Acquire polygon from file
    poly = getPolygon(fileName)
    
    # Acquire the name of the table from file
    TABLE_NAME = getTableName(fileName)

    # Receive Database parameters from config file.
    params = config(fileName)

    # Connect to database using config file parameters.
    con = psycopg2.connect(**params)
    print("Connected to Database Successfully")

    # Create a cursor for the database.
    cur = con.cursor()

    # Ask the user questions about what functionality they would like.
    check = input("Would you like to create a new table? (Y/N): ")

    if check == "Y":
        # If "Y", create a table with the user's inputted table name 
        # TABLE_NAME = input("What would you like to name the table? ")
        cur.execute("CREATE TABLE {} (pkid SERIAL PRIMARY KEY NOT NULL, txtfield TEXT NOT NULL, thegeom GEOMETRY DEFAULT ST_GeomFromText('POINT(-6.7 54)',4326)); \n".format(TABLE_NAME))
        print("Table {} created with {} points inserted".format(TABLE_NAME, NUMBER_ROWS))
    elif check == "N":
        # If "N", use table name submitted at the start
        clear = input("Would you like to clear the table first? (Y/N): ")
        
        # Ask user if they would like the table cleared, not dropped. If "Y", then delete all rows
        if clear == "Y":
            cur.execute("DELETE from {}".format(TABLE_NAME))
            print("Successfully cleared table and committed {} rows to table: {}".format(NUMBER_ROWS, TABLE_NAME))
        else:
            print("Successfully committed {} rows to table: {}".format(NUMBER_ROWS, TABLE_NAME))

        return 0

    
    # committ changes to the database.
    con.commit()  

    # Use random_points_within function to generate all of the necessary points with the submitted polygon and number of points 
    generated_points = random_points_within(poly, NUMBER_ROWS)

    # Use iterator function to iterate through all points and commit them to the database
    pointIterDb(generated_points, cur, con, TABLE_NAME)

    return 0

# Writes the points to an SQL file if requested by the user for manual commits.
def createSql(fileName):

    # Acquire number of rows from file
    NUMBER_ROWS = getNumPoints(fileName)
    
    # Acquire polygon from file
    poly = getPolygon(fileName)
    
    # Acquire the name of the table from file
    TABLE_NAME = getTableName(fileName)

    sqlFile = open(getSqlFile(fileName), "w")
    sqlFile.write("")
    sqlFile.close()

    sqlFile = open(getSqlFile(fileName), "a")
    sqlFile.write("CREATE TABLE {} (pkid SERIAL PRIMARY KEY NOT NULL, txtfield TEXT NOT NULL, thegeom GEOMETRY DEFAULT ST_GeomFromText('POINT(-6.7 54)',4326)); \n".format(TABLE_NAME))

    generated_points = random_points_within(poly, NUMBER_ROWS)
    pointIterSql(generated_points, sqlFile, TABLE_NAME)
    print("Successfully printed to {}".format(getSqlFile(fileName)))

    return 0

# Config function takes in a .ini file and acquires the database connect information from it
def config(fileName):

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(fileName)

    # get section, default to postgresql
    db = {}     # Create empty list to store config information
    if parser.has_section('postgresql'):
        params = parser.items('postgresql')

        # Check if .ini file has section 'postgresql' and if so, store section in params list
        for param in params:
            db[param[0]] = param[1]
            # Move everything from params list to db list
            
    else:
        # If no 'postgresql' section found, skip.
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, fileName))
    
    # Return the db list
    return db

# Takes in the name of the .ini file, finds the TableName section and finds the table name.
def getTableName(fileName):

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(fileName)

    # Check parser for section of name 'TableName'
    if parser.has_section('TableName'):
        nameOfTable = parser.items('TableName')
        # Store items in section 'TableName' in nameOfTable list of tuples
    TABLE_NAME = (nameOfTable[0][1])
    # Stores the name of the table
    return TABLE_NAME

# Takes in the name of the .ini file, finds the numPoints section and finds out how many points are wanted.
def getNumPoints(fileName):

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(fileName)

    # Check parser for section of name 'numPoints'
    if parser.has_section('numPoints'):
        pointsNum = parser.items('numPoints')
        # Store items in section 'numPoints' in pointsNum list of tuples
    NUMBER_ROWS = int(pointsNum[0][1])
    # Store value for number of points in NUMBER_ROWS
    return NUMBER_ROWS

# Takes in the name of the .ini file and finds out whihc file has the geojson polygon.
def getPolygon(fileName):

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(fileName)

    # Check parser for section of name 'geojson'
    if parser.has_section('geojson'):
        shapeBounds = parser.items('geojson')
        # Store items in section 'geojson' in shapeBounds list of tuples
    geojsonName = (shapeBounds[0][1])
    # Stores the name of the geojson file with the bounds of the polygon

        # Load in GeoJSON file from .ini file as f and obtain "features" object
    with open(geojsonName) as f:
        features = json.load(f)["features"]

    # Define shape as "geometry" from GeoJSON file and buffer(0) removes overlapping coordinates
    poly = (GeometryCollection(
        [shape(feature["geometry"]).buffer(0) for feature in features]))

    return poly

# Takes in the name of the .ini file and finds out which file will be wrote to to create an SQL file.
def getSqlFile(fileName):

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(fileName)

    # Check parser for section of name 'geojson'
    if parser.has_section('SQLFile'):
        sqlFile = parser.items('SQLFile')
        # Store items in section 'geojson' in shapeBounds list of tuples
    fileSql = (sqlFile[0][1])
    # Stores the name of the geojson file with the bounds of the polygon

    return fileSql

# Iterator takes in points to be commited, database cursor, database and the name of the table.
def pointIterDb(generated_points, cur, con, TABLE_NAME):
    for aPoint in generated_points:
        # for each point in all points, create a random string of size 10
        random_string = ''.join(random.SystemRandom().choice(
            string.ascii_letters + string.digits) for _ in range(10))
        

        pointLatitude = aPoint.y
        pointLongitude = aPoint.x

        # Make INSERT statement for the database using the table name, the random string and the latitude and longitude of the point.
        queryStr = "INSERT into {} (txtField,theGeom) VALUES ('{}',ST_SetSRID(ST_MakePoint({},{}),4326)); \n".format(
            TABLE_NAME, random_string, pointLongitude, pointLatitude)
        
        # Execute INSERT statement into database
        cur.execute(queryStr)
        # Commit statement
        con.commit()

        return 0

# Iterates through generated points and gives them a random string for the txtfield.
def pointIterSql(generated_points, sqlFile, TABLE_NAME):
        for aPoint in generated_points:
            random_string = ''.join(random.SystemRandom().choice(
                string.ascii_letters + string.digits) for _ in range(10))

            pointLatitude = aPoint.y
            pointLongitude = aPoint.x

            queryStr = "INSERT into {} (txtField,theGeom) VALUES ('{}',ST_SetSRID(ST_MakePoint({},{}),4326)); \n".format(
                TABLE_NAME, random_string, pointLongitude, pointLatitude)

            sqlFile.write(queryStr)

# Generates random points throughout the submitted polygon and returns them.
def random_points_within(poly, num_points):
    
    min_x, min_y, max_x, max_y = poly.bounds # Acquire shape bounds from poly

    points = []  # this a list to hold all points.
    
    # While theres more points than in the points list, generate a random point and append to list
    while len(points) < num_points:

        random_point = Point(
            [random.uniform(min_x, max_x), random.uniform(min_y, max_y)])

        if (random_point.within(poly)):  
            points.append(random_point)  # add to the list.

    return points

# Calls the first function to allow the user to pick a file.
filePicker()
