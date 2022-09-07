import psycopg2

# Connect to db.
print("test")
connection = psycopg2.connect(
    host="acreturs-VivoBook-ASUSLaptop-X415UA-D415UA",
    database="postgres",
    user="postgres2",  # Kann irgendwie nur mit 2. account verbinden
    password="postgres")
#  port = "5432", nicht nötig weil stanni port

postgres_insert_query = """INSERT INTO location_id (location) VALUES (%s)"""

record_to_insert = ('t2estse')
cursor = connection.cursor()
cursor.execute("INSERT INTO location_id (location) VALUES ('lol')")

connection.commit()
count = cursor.rowcount
print(count, "Record inserted successfully into mobile table")

# immer allesschließen auch curor

connection.close()  # immer closen
