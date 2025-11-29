# loads from mongo db
from pymongo import MongoClient
import csv
import pandas as pd

class Data:

    # mongo db document passed in here

    def __init__(self, document_id) -> None:
        client = MongoClient("mongodb://root:password@localhost:27017/?authSource=admin")
        self.db = client["test_database_1"]
        self.items_collection = self.db["collection1"]
        doc = self.items_collection.find_one({"_id": document_id})
        self.records = pd.DataFrame([doc])

    # export back to mongo db
    def load_csv(self, csv_file: csv) -> None:
        with open(csv_file, newline='') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        self.items_collection.insert_many(data)

    def export_csv(self, collection_name: str) -> None:
        collection = self.db[collection_name]
        data = list(collection.find())
        df = pd.DataFrame(data)
        df.to_csv(f"{collection_name}.csv")


