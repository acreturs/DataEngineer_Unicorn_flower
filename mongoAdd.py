import pymongo
from pymongo import MongoClient


def add():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["unicorn_db"]
    mycol = mydb["unicorns_test_alex"]

    mylist = {"unicorn": "Bubble Gum",
              "pickups": [{"flower": "afrai", "location": "meadow", "datetime": "2022-08-19T15:20:00"},
                          {"flower": "ambrosia", "location": "forest", "datetime": "2022-08-19T14:05:36”"},
                          {"flower": "dewberry", "location": "meadow", "datetime": "2022-08-19T16:21:07"},

                          ]
              }

    x = mycol.insert_one(mylist)#nicht insert many

    # print list of the _id values of the inserted documents:
    #print(x.inserted_ids)


def read():
    client = MongoClient()
    client = MongoClient('mongodb://localhost:27017/')
    db = client['unicorn_db']
    collection = db['customers']

    for row in collection.find():
        print(row)


if __name__ == '__main__':
    add()
