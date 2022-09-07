import psycopg2
import numpy as np




def find_id_unicorn(self,name)-> int:
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


def find_id_location(self,name)-> int:
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
        postgres_insert_unicorn_id = f"""INSERT INTO location_ud (location) VALUES ('{name}')"""
        cursor.execute(postgres_insert_unicorn_id)
        connection.commit()
    postgres_read_unicorn_id = f"""SELECT location_id FROM location_id WHERE location = '{name}'"""
    cursor.execute(postgres_read_unicorn_id)
    connection.commit()
    werte = cursor.fetchall()
    arr = np.array(werte)
    fla_arr = arr.flatten()
    return fla_arr[0]

def find_id_flower(self,name):
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
    print(fla_arr[0])



