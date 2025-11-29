from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://root:password@localhost:27017/")

# Select the database
db = client["test_database"]

# Access the collection
items_collection = db["collection1"]

# Insert some items
items_to_insert = [
    {"name": "Item 1", "price": 10},
    {"name": "Item 2", "price": 20},
    {"name": "Item 3", "price": 30}
]

# Insert many at once
items_collection.insert_many(items_to_insert)

# Print all items in the collection
print("Items in collection1:")
for item in items_collection.find():
    print(item)
