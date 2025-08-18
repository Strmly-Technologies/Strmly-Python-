# db_client.py
import os
from pymongo import MongoClient, errors



 


def connect_database():
    MONGODB_URI = os.getenv("MONGODB_URI")   
    DB_NAME = os.getenv("MONGODB_DB_NAME", "test")
    if not MONGODB_URI:
        raise ValueError("❌ MONGODB_URI not set in environment variables")

    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)  # 5s timeout
        # Force connection check
        client.admin.command("ping")
        db = client[DB_NAME]
        long_video_collection = db["longvideos"]
        auto_copyright_collection=db["autocopyrights"]
        auto_nsfw_collection=db["autonsfws"]
        print(f"✅ Connected to MongoDB: {DB_NAME}")
        return client,long_video_collection,auto_copyright_collection,auto_nsfw_collection
    except errors.ServerSelectionTimeoutError as e:
        raise ConnectionError(f"❌ Could not connect to MongoDB: {e}")
    except Exception as e:
        raise ConnectionError(f"❌ MongoDB connection failed: {e}")


 

