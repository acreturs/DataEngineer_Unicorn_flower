import pymongo
from pymongo import MongoClient
import psycopg2
import numpy as np


class Pickups:
    """
    Each pickup object stands for one object or one row in the big table
    """
    location_id = 0
    flower_id = 0
    # unicorn_id
    time_stamp = ''

    def __init__(self, uni_id):
        self.unicorn_id = uni_id


class Read:

    """
    read and add directly connects to the mongodb base and filters out all rows. I then itterate over each row. Each row consts of 3 types of dictionaries.
    Relevant information for us:
    The first one only contains the unicorn name
    THe second one contains all the pickups and is located within the first dictionary
    Each pickup from the second dictionary is a dictionary itself which contains the flower name, the location and the time.
    We save the unicorn name and add it to the dict object which we create at the beginning of going into the third dictionary
    This way we have one pickup object per dictionary which we can store all the information per pickup in. THis object can then be passed on to add_to_table
    """
    def read_and_add_psql(self):

        # everything till found is just to connect to the docker
        client = MongoClient()
        client = MongoClient('mongodb://localhost:27017/')
        db = client['unicorn_db_docker']
        collection = db["unicorns_big_table"]
        # Get the entire mongodb base
        found = collection.find()
        # Found = pymongo datei aber row ist dann jeweils eine Zeile

        for row in found:
            unicorn = 0
            for x in row:
                if x.__eq__("unicorn"):
                    unicorn = self.find_id_unicorn(row[x])
                if x.__eq__("pickups"):
                    list_of_pickups = row[x]
                    for y in list_of_pickups:
                        addedObject = Pickups(unicorn)
                        # Itteriert durch den individuellen pickup
                        for z in y:
                            if str(z).__eq__("flower"):
                                addedObject.flower_id = self.find_id_flower(y[z])
                            if str(z).__eq__("datetime"):
                                addedObject.time_stamp = y[z]
                            if str(z).__eq__("location"):
                                addedObject.location_id = self.find_id_location(y[z])
                        self.add_to_table(addedObject)
    """
    Add to table receives an object which ccntains all the information necessary for the main table (locationid, flowerid, unicornid and timestamp). It then simply inserts these into the table
    """
    def add_to_table(self, dictobject):
        connection = psycopg2.connect(
            host="acreturs-VivoBook-ASUSLaptop-X415UA-D415UA",
            database="postgres",
            user="postgres2",  # Kann irgendwie nur mit 2. account verbinden
            password="postgres")
        cursor = connection.cursor()

        postgres_read_unicorn_id = f"""INSERT INTO pickups (location_id, flower_id, unicorn_id, timestamp) VALUES ({dictobject.location_id}, {dictobject.flower_id},{dictobject.unicorn_id},'{dictobject.time_stamp}')"""
        cursor.execute(postgres_read_unicorn_id)
        connection.commit()


    """
    All find_id functions work the same. They receive the name of the unicorn in order to later give it an id. Then they query the given name to see if a result comes back (so to check
    if the name already has an id). If the returned array with the results is empty then I enter the name with its id into the table. Once that is done or if that step
    was skipped because the name already existed I query the id which belongs to the unicorn (here I could improve by not quering twice if I already checked it the unicorn exists. 
    After receiving the Id of the unicorn in the form of a tuple I use numpy to filter out the idea and I return it
    """
    def find_id_unicorn(self, name) -> int:
        connection = psycopg2.connect(
            host="acreturs-VivoBook-ASUSLaptop-X415UA-D415UA",
            database="postgres",
            user="postgres2",  # Kann irgendwie nur mit 2. account verbinden
            password="postgres")
        cursor = connection.cursor()
        postgres_read_unicorn_id = f"""SELECT unicorn_id FROM unicorn_id WHERE unicorn = '{name}'"""
        cursor.execute(postgres_read_unicorn_id)
        connection.commit()
        if len(cursor.fetchall()) == 0:
            postgres_insert_unicorn_id = f"""INSERT INTO unicorn_id (unicorn) VALUES ('{name}')"""
            cursor.execute(postgres_insert_unicorn_id)
            connection.commit()
        postgres_read_unicorn_id = f"""SELECT unicorn_id FROM unicorn_id WHERE unicorn = '{name}'"""
        cursor.execute(postgres_read_unicorn_id)
        connection.commit()
        werte = cursor.fetchall()
        arr = np.array(werte)
        fla_arr = arr.flatten()
        return fla_arr[0]


    def find_id_location(self, name) -> int:
        connection = psycopg2.connect(
            host="acreturs-VivoBook-ASUSLaptop-X415UA-D415UA",
            database="postgres",
            user="postgres2",  # Kann irgendwie nur mit 2. account verbinden
            password="postgres")
        cursor = connection.cursor()
        postgres_read_unicorn_id = f"""SELECT location_id FROM location_id WHERE location= '{name}'"""
        cursor.execute(postgres_read_unicorn_id)
        connection.commit()
        if len(cursor.fetchall()) == 0:
            postgres_insert_unicorn_id = f"""INSERT INTO location_id (location) VALUES ('{name}')"""
            cursor.execute(postgres_insert_unicorn_id)
            connection.commit()
        postgres_read_unicorn_id = f"""SELECT location_id FROM location_id WHERE location = '{name}'"""
        cursor.execute(postgres_read_unicorn_id)
        connection.commit()
        werte = cursor.fetchall()
        arr = np.array(werte)
        fla_arr = arr.flatten()
        return fla_arr[0]


    def find_id_flower(self, name) -> int:
        connection = psycopg2.connect(
            host="acreturs-VivoBook-ASUSLaptop-X415UA-D415UA",
            database="postgres",
            user="postgres2",  # Kann irgendwie nur mit 2. account verbinden
            password="postgres")
        cursor = connection.cursor()
        postgres_read_unicorn_id = f"""SELECT flower_id FROM flower_id WHERE flower = '{name}'"""
        cursor.execute(postgres_read_unicorn_id)
        connection.commit()
        if len(cursor.fetchall()) == 0:
            postgres_insert_unicorn_id = f"""INSERT INTO flower_id (flower) VALUES ('{name}')"""
            cursor.execute(postgres_insert_unicorn_id)
            connection.commit()
        postgres_read_unicorn_id = f"""SELECT flower_id FROM flower_id WHERE flower = '{name}'"""
        cursor.execute(postgres_read_unicorn_id)
        connection.commit()
        werte = cursor.fetchall()
        arr = np.array(werte)
        fla_arr = arr.flatten()
        return fla_arr[0]


"""
ADD is used to add code to the mongodb database. I primarly used this to test the rest of the code
"""


def add(self):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["unicorn_db_docker"]
    mycol = mydb["unicorns_big_table"]

    mylist = {"unicorn": "Bubble Gum",
              "pickups": [{"flower": "afrai", "location": "meadow", "datetime": "2022-08-19T15:20:00"},
                          {"flower": "ambrosia", "location": "forest", "datetime": "2022-08-19T14:05:36”"},
                          {"flower": "dewberry", "location": "meadow", "datetime": "2022-08-19T16:21:07"},

                          ]
              }

    x = mycol.insert_one(mylist)  # nicht insert many
    print("worked")


"""
This method is used to delete all information from all tables (pickups and all the ids) -> Used to go back to zero when testing the application
"""


def clear_everything():
    connection = psycopg2.connect(
        host="acreturs-VivoBook-ASUSLaptop-X415UA-D415UA",
        database="postgres",
        user="postgres2",  # Kann irgendwie nur mit 2. account verbinden
        password="postgres")
    cursor = connection.cursor()
    postgres_read_unicorn_id = """DELETE FROM pickups"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()
    cursor = connection.cursor()
    postgres_read_unicorn_id = """DELETE FROM unicorn_id"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()
    cursor = connection.cursor()
    postgres_read_unicorn_id = """DELETE FROM location_id"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()
    cursor = connection.cursor()
    postgres_read_unicorn_id = """DELETE FROM flower_id"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()

    cursor = connection.cursor()
    postgres_read_unicorn_id = """ALTER SEQUENCE location_id_location_id_seq RESTART WITH 1"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()
    postgres_read_unicorn_id = """ALTER SEQUENCE flower_id_flower_id_seq RESTART WITH 1"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()
    postgres_read_unicorn_id = """ALTER SEQUENCE unicorn_id_unicorn_id_seq RESTART WITH 1"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()
    postgres_read_unicorn_id = """ALTER SEQUENCE pickups_object_id_seq RESTART WITH 1"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()


if __name__ == '__main__':
    clear_everything()
    # read = Read()
    # read.read_and_add_psql()
