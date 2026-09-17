from kafka import KafkaConsumer
import json
import numpy as np
import joblib
import pymongo
from datetime import datetime
from tensorflow.keras.models import load_model

try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fraud_detection_system"]
    collection = db["live_transactions"]
    collection.create_index([("user_id", pymongo.ASCENDING)])
    collection.create_index([("timestamp", pymongo.DESCENDING)])
    print(" Connected to MongoDB & Indexes Verified!")
except Exception as e:
    print(f" Database Error: {e}")
    exit()

print(" Loading Deep Learning Model...")
try:
    model = load_model('fraud_dl_model.keras')
    scaler = joblib.load('scaler.pkl')
    print(" Neural Network Loaded Successfully!")
except Exception as e:
    print(" Error: Model files missing! Run 'python train_deep_model.py' first.")
    exit()

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    group_id='fraud-scorer',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(" Fraud Detection System Active. Waiting for live stream...")

for message in consumer:
    txn = message.value
    
    features = np.array([[txn['amount']]])
    features_scaled = scaler.transform(features)
    
    risk_score = float(model.predict(features_scaled, verbose=0)[0][0])
    is_fraud = risk_score > 0.50

    db_record = {
        "transaction_id": txn['id'],
        "user_name": txn['name'],
        "amount": txn['amount'],
        "currency": "INR",
        "city": txn['city'],
        "timestamp": datetime.fromtimestamp(txn['timestamp']), 
        "ai_analysis": {
            "risk_score": risk_score,
            "is_blocked": is_fraud,
            "model_version": "v1.0"
        },
    }

    try:
        collection.insert_one(db_record)
        
        if is_fraud:
            print(f"BLOCKED: ₹{txn['amount']} in {txn['city']} (Risk: {risk_score:.2f}) -> 💾 Saved to DB")
        else:
            print(f" APPROVED: ₹{txn['amount']} in {txn['city']} (Risk: {risk_score:.2f}) -> 💾 Saved to DB")
            
    except Exception as e:
        print(f"Error saving to DB: {e}")