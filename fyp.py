import psycopg2
import random
import json
import string
from shapely.geometry import Polygon, Point, shape, GeometryCollection
from configparser import ConfigParser
import datetime


def config(filename='data.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename))

    return db


def main():

    x = datetime.datetime.now()
    print(x)
    print(x.year)
    print(x.strftime("%B"))
    # Receive Database parameters from config file.
    params = config()
    print('Connecting to the PostgreSQL database...')

    # Connect to database using config file parameters.
    con = psycopg2.connect(**params)
    print("Connected Successfully")

    # Load in GeoJSON file as f and obtain "features" object
    with open("map.geojson") as f:
        features = json.load(f)["features"]

    # Define shape as "geometry" from GeoJSON file and buffer(0) removes overlapping coordinates
    poly = (GeometryCollection(
        [shape(feature["geometry"]).buffer(0) for feature in features]))

    # User inputs required number of rows
    try:
        val = int(input("Enter number of points desired: "))
    except ValueError:
        val = int(input("Enter a positive integer value: "))
    if val < 0 :
        val = val*(-1)

    NUMBER_ROWS = val

    # Call up desired table name within database
    TABLE_NAME = "FourthYearProject"

    # Create a database cursor
    cur = con.cursor()

    # Start off with a new table (empty)
    cur.execute("DELETE from {}".format(TABLE_NAME))
    con.commit()  # committ changes to the database.

    generated_points = random_points_within(poly, NUMBER_ROWS)

    # Iterate across all of the generated points.
    for aPoint in generated_points:
        random_string = ''.join(random.SystemRandom().choice(
            string.ascii_letters + string.digits) for _ in range(10))

        pointLatitude = aPoint.y
        pointLongitude = aPoint.x

        queryStr = "INSERT into {} (txtField,theGeom) VALUES ('{}',ST_SetSRID(ST_MakePoint({},{}),4326))".format(
            TABLE_NAME, random_string, pointLongitude, pointLatitude)

        print(queryStr)

        # Do an insert of a random point from python
        cur.execute(queryStr)
        # Committ changes to the database.
        con.commit()


def random_points_within(poly, num_points):
    min_x, min_y, max_x, max_y = poly.bounds  # shapely

    points = []  # this a list to hold all points.

    while len(points) < num_points:
        random_point = Point(
            [random.uniform(min_x, max_x), random.uniform(min_y, max_y)])
        if (random_point.within(poly)):  # this is from shapely
            points.append(random_point)  # add to the list.

    return points


main()
