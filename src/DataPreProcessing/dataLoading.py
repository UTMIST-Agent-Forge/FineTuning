# loads from mongo db
import io
import os
import csv
import pandas as pd
import supabase

SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]

TRAINING_DATA_BUCKET = "lora_training_data"
RAW_DATA_FOLDER = "raw_data"
FINAL_PROCESSED_DATA_FOLDER = "processed_data"

sb_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)

class Data:

    def __init__(self, document_name) -> None:
        self.document_name = document_name
        res = sb_client.storage.from_(TRAINING_DATA_BUCKET).download(f"{RAW_DATA_FOLDER}/{document_name}")
        self.records = pd.read_csv(io.BytesIO(res))
        print(self.records)
    
    def export_back(self, folder=FINAL_PROCESSED_DATA_FOLDER):
        csv_buffer = io.StringIO()
        self.records.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        file_path = f"{folder}/{self.document_name}"

        response = sb_client.storage.from_(TRAINING_DATA_BUCKET).upload(
            file_path,
            csv_bytes,
            file_options={"content-type": "text/csv", "upsert": "true"}
        )

        return response