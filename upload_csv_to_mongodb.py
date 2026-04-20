import pandas as pd
import os
import sys
from churn_src.Configuration.Mongo_db_connection import MongoDBClient

# File paths
TRAIN_FILE = "Notebook/Data/customer_churn_dataset-training-master.csv"
TEST_FILE = "Notebook/Data/customer_churn_dataset-testing-master.csv"
COLLECTION_NAME = "churn_data"

# Load train and test data
print("Loading train and test data...")
train_df = pd.read_csv(TRAIN_FILE)
test_df = pd.read_csv(TEST_FILE)

print(f"Train data shape: {train_df.shape}")
print(f"Test data shape: {test_df.shape}")

# Merge train and test data
print("Merging train and test data...")
merged_df = pd.concat([train_df, test_df], ignore_index=True)

merged_df=merged_df.iloc[:500000,:]

print(f"Merged data shape: {merged_df.shape}")

# Convert dataframe to list of dictionaries for MongoDB
data_to_insert = merged_df.to_dict(orient='records')


# Upload to MongoDB
print("Uploading to MongoDB...")
try:
   
    mongo_client = MongoDBClient()  # Create MongoDB client and connect
    # Insert data into collection
    collection = mongo_client.database[COLLECTION_NAME]

    for doc in data_to_insert:
     doc.pop("_id", None)
        
    result = collection.insert_many(data_to_insert)
    
    print(f"Successfully uploaded {len(result.inserted_ids)} records to MongoDB")
    print(f"Collection: {COLLECTION_NAME}")
    print(f"Database: {mongo_client.database_name}")
    
except Exception as e:
    print(f"Error uploading to MongoDB: {e}")



    