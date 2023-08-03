from pymongo import MongoClient
import os

# Replace with your MongoDB connection string
DATABASE_NAME = "uniswapX"
COLLECTION_NAME = "orders"


def query_orders_collection():
    # Connect to MongoDB
    client = MongoClient(mongo_endpoint)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    # Find all documents in the 'orders' collection
    result = collection.find({})

    # Print the documents
    for document in result:
        print(document)

    client.close()


if __name__ == "__main__":
    mongo_endpoint = os.environ.get('MONGO_ENDPOINT')
    if mongo_endpoint is None:
        mongo_endpoint = "mongodb://localhost:27017/"

    query_orders_collection()
