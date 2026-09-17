from kafka import KafkaConsumer
import json
import pymongo
import numpy as np
import joblib
from datetime import datetime
from tensorflow.keras.models import load_model



try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    db = client["fraud_detection_system"]

    collection = db["fraud_predictions"]

    collection.create_index(
        [("transaction_id", pymongo.ASCENDING)]
    )

    collection.create_index(
        [("prediction_timestamp", pymongo.DESCENDING)]
    )

    print("Connected to MongoDB & Indexes Verified!")

except Exception as e:
    print(f"Database Error: {e}")
    exit()


print("Loading Deep Learning Model...")

try:

    model = load_model("fraud_dl_model.keras")

    scaler = joblib.load("scaler.pkl")

    print("Neural Network Loaded Successfully!")

except Exception as e:

    print(f"Error loading model files: {e}")
    print("Run 'train_deep_model.py' first.")
    exit()

consumer = KafkaConsumer(
    "transactions",

    bootstrap_servers="localhost:9092",

    group_id="fraud-scorer",

    auto_offset_reset="latest",

    value_deserializer=lambda x:
        json.loads(x.decode("utf-8"))
)


print("Fraud Detection Consumer Active.")
print("Waiting for transactions...")


for message in consumer:

    txn = message.value

    print(
        f"\nReceived Transaction: {txn['id']}"
    )

    features = np.array([
        [txn["amount"]]
    ])

    features_scaled = scaler.transform(features)


    risk_score = float(
        model.predict(
            features_scaled,
            verbose=0
        )[0][0]
    )

    is_fraud = risk_score > 0.50


    prediction_record = {

        "transaction_id": txn["id"],

        "risk_score": risk_score,

        "is_fraud": is_fraud,

        "model_version": "v1.0",

        "prediction_timestamp": datetime.now()
    }


    try:

        collection.insert_one(
            prediction_record
        )



        if is_fraud:

            print(
                f"BLOCKED: "
                f"₹{txn['amount']} "
                f"in {txn['city']} "
                f"(Risk: {risk_score:.2f}) "
                f"-> Saved to fraud_predictions"
            )
        else:

            print(
                f"APPROVED: "
                f"₹{txn['amount']} "
                f"in {txn['city']} "
                f"(Risk: {risk_score:.2f}) "
                f"-> Saved to fraud_predictions"
            )

    except Exception as e:

        print(
            f"Error saving prediction to DB: {e}"
        )