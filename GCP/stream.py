# streaming.py

import csv
import time
import json
from datetime import datetime
from google.cloud import pubsub_v1

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

# Replace with your GCP project ID and Pub/Sub topic name
project_id = "tst-dev-02"
topic_id = "dfdemo1"

# Path to CSV file (example)
csv_file_path = "employees.csv"

# --------------------------------------------------
# PUB/SUB CLIENT SETUP
# --------------------------------------------------

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# --------------------------------------------------
# FUNCTION TO PUBLISH MESSAGE
# --------------------------------------------------

def publish_message(empid, firstname, salary):
    timestamp = datetime.utcnow().isoformat() + "Z"

    message = {
        "empid": empid,
        "name": firstname,
        "salary": salary,
        "timestamp": timestamp
    }

    # Convert to JSON and encode to bytes
    message_json = json.dumps(message).encode("utf-8")

    # Publish message
    publish_future = publisher.publish(topic_path, message_json)

    print(f"Published message ID: {publish_future.result()} | Data: {message}")

# --------------------------------------------------
# STREAM DATA FROM CSV (SIMULATED STREAMING)
# --------------------------------------------------

def stream_csv_data():
    with open(csv_file_path, mode="r") as file:
        reader = csv.DictReader(file)

        for row in reader:
            empid = row["empid"]
            firstname = row["firstname"]
            salary = row["salary"]

            publish_message(empid, firstname, salary)

            # Simulate streaming delay
            time.sleep(2)

# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":
    print("Starting Pub/Sub streaming...")
    stream_csv_data()
