from kafka import KafkaConsumer
import json
import pymongo
from datetime import datetime
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fraud_detection_system"]

    collection = db["raw_transactions"]

    collection.create_index(
        [("transaction_id", pymongo.ASCENDING)]
    )

    collection.create_index(
        [("timestamp", pymongo.DESCENDING)]
    )

    print("Connected to MongoDB & Indexes Verified!")

except Exception as e:
    print(f"Database Error: {e}")
    exit()


consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    group_id="data-consumer",
    auto_offset_reset="latest",
    value_deserializer=lambda x:
        json.loads(x.decode("utf-8"))
)

print("Data Consumer Active.")
print("Waiting for transactions...")

for message in consumer:

    txn = message.value

    db_record = {
        "transaction_id": txn["id"],
        "user_name": txn["name"],
        "amount": txn["amount"],
        "currency": "INR",
        "city": txn["city"],
        "timestamp": datetime.fromtimestamp(
            txn["timestamp"]
        )
    }

    try:

        collection.insert_one(db_record)

        print(
            f"DATA SAVED: "
            f"₹{txn['amount']} "
            f"in {txn['city']} "
            f"-> raw_transactions"
        )

    except Exception as e:

        print(f"Error saving transaction: {e}")