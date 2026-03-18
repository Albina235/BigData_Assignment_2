import pandas as pd
from pymongo import MongoClient
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data')

client = MongoClient('mongodb://user:password@localhost:27017/')
db = client['e_commerce_db']

files = {
    'campaigns': 'campaigns_cleaned.csv',
    'messages': 'messages_cleaned.csv',
    'events': 'events_cleaned.csv',
    'friends': 'friends_cleaned.csv',
    'client_first_purchase': 'client_first_purchase_date_cleaned.csv'
}

def load_data():
    for collection_name, filename in files.items():
        file_path = os.path.join(DATA_DIR, filename)
        
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} not found.")
            continue
            
        print(f"Loading {filename} into collection '{collection_name}'...")
        
        df = pd.read_csv(file_path)
        
        data = df.to_dict(orient='records')
        
        collection = db[collection_name]
        collection.drop() 
        collection.insert_many(data)
        
        print(f"Successfully loaded {len(data)} documents into {collection_name}.")

if __name__ == "__main__":
    load_data()
    print("MongoDB data import finished successfully!")