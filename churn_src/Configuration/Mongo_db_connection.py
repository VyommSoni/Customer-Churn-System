import os
import sys
from churn_src.logger import logging
from churn_src.exception import ChurnException
import pymongo
from dotenv import load_dotenv
import os
from churn_src.Constant import *
load_dotenv()
import certifi
ca = certifi.where()

print("mongodburl",os.getenv("MongoDB_URL"))

class MongoDBClient:
    client = None

    def __init__(self, database_name=os.getenv('DATABASE_NAME')):
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv("MongoDB_URL")
                if mongo_db_url is None:
                    raise Exception(f"Environment key: {"MongoDB_URL"} is not set.")
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            print("successfully connected to mongodb")
        except Exception as e:
            raise ChurnException(e, sys)