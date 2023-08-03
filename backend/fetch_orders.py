import time
import requests
import json
from pymongo import MongoClient
import os

DATABASE_NAME = "uniswapX"
COLLECTION_NAME = "orders"
UNI_X_ORDER_ENDPOINT = "https://api.uniswap.org/v2/orders?orderStatus=open&chainId=1"


def create_database_collection():
    # Connect to MongoDB
    client = MongoClient(mongo_endpoint)
    db_list = client.list_database_names()
    print("Db list", db_list)

    # Check if the database exists
    if DATABASE_NAME not in db_list:
        # Create the database if it doesn't exist
        db = client[DATABASE_NAME]
        print(f"Database '{DATABASE_NAME}' created.")
        db.list_collection_names()  # This is to trigger the creation of the database

    # Check if the collection exists
    db = client[DATABASE_NAME]
    collection_list = db.list_collection_names()
    if COLLECTION_NAME not in collection_list:
        # Create the collection if it doesn't exist
        db.create_collection(COLLECTION_NAME)
        print(f"Collection '{COLLECTION_NAME}' created.")

    client.close()


def poll_api_and_save_to_db():
    client = MongoClient(mongo_endpoint)
    while True:
        try:
            # Poll the API endpoint
            response = requests.get(UNI_X_ORDER_ENDPOINT)
            response.raise_for_status()

            # Process the response data
            orders = response.json()["orders"]

            # Connect to MongoDB
            db = client[DATABASE_NAME]
            collection = db[COLLECTION_NAME]

            # Insert or replace the orders into the collection based on 'orderHash'
            for order in orders:
                order_hash = order.get("orderHash")
                processed_order = json.dumps(order)
                collection.update_one({"orderHash": order_hash}, {
                                      "$set": {"data": processed_order}}, upsert=True)

            print("Successfully polled and saved data to MongoDB.")

        except requests.exceptions.RequestException as e:
            print(f"Error occurred while polling the API: {e}")

        except json.JSONDecodeError as e:
            print(f"Error occurred while processing JSON: {e}")

        except Exception as e:
            print(f"Unexpected error occurred: {e}")

        time.sleep(30)  # Wait for 30 seconds before polling again

    client.close()


if __name__ == "__main__":
    mongo_endpoint = os.environ.get('MONGO_ENDPOINT')
    if mongo_endpoint is None:
        mongo_endpoint = "mongodb://localhost:27017/"
    create_database_collection()
    poll_api_and_save_to_db()
