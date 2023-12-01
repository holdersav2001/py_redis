import boto3
import json
import random
import time
REGION = 'us-east-1'
STREAM_NAME = 'test-stream'
PARTITION_KEY = 'sensorId'
RETRY_ATTEMPTS = 3
RETRY_DELAY = 0.1
BATCH_SIZE = 500
session = boto3.Session(region_name=REGION)
kinesis = session.client('kinesis')
def generate_data():
    try:
        stream_description = kinesis.describe_stream(StreamName=STREAM_NAME)
        if stream_description['StreamDescription']['StreamStatus'] != 'ACTIVE':
            print(f"Stream {STREAM_NAME} is not active.")
            return
    except kinesis.exceptions.ResourceNotFoundException:
        print(f"Stream {STREAM_NAME} not found.")
        return
    records = []
    for i in range(1, 1000):
        record = {
            'sensor_id': random.randint(1, 100),
            'temperature': random.uniform(20.0, 30.0),
            'humidity': random.uniform(40.0, 60.0),
            'timestamp': int(time.time())
        }
        records.append({
            'Data': json.dumps(record),
            'PartitionKey': PARTITION_KEY
        })
        if len(records) == BATCH_SIZE:
            put_records(records)
            records = []
    if records:
        put_records(records)
def put_records(records):
    for i in range(RETRY_ATTEMPTS):
        try:
            response = kinesis.put_records(StreamName=STREAM_NAME, Records=records)
            failed_records = response.get('FailedRecordCount', 0)
            if failed_records == 0:
                return
            print(f"Failed to put {failed_records} records.")
        except Exception as e:
            print(f"Exception encountered: {e}")
        time.sleep(RETRY_DELAY)
def main(event, context):
    generate_data()
if __name__ == '__main__':
    generate_data()