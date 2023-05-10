import sys
import pymongo


uri = "mongodb+srv://kirbel:kirfi@pokrovskii.n78o7gq.mongodb.net/?retryWrites=true&w=majority"
try:
    client = pymongo.MongoClient(uri)
    print("success")

except pymongo.errors.ConfigurationError:
    print("An Invalid URI host error was received. Is your Atlas host name correct in your connection string?")
    sys.exit(1)


db = client.myDatabase
collection_name = "products"
if collection_name not in db.list_collection_names():
    db.create_collection(collection_name)
my_collection = db[collection_name]
