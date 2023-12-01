import boto3
import json
import random
import time

def lambda_handler(event, context):
    session = boto3.Session(region_name='us-east-1')
    kinesis = session.client('kinesis')

session = boto3.Session(region_name='us-east-1')
kinesis = session.client('kinesis')

def generate_data():
    try:
        stream_description = kinesis.describe_stream(StreamName='test-stream')
        if stream_description['StreamDescription']['StreamStatus'] != 'ACTIVE':
            print(f"Stream {stream_description['StreamDescription']['StreamName']} is not active.")
            return
    except kinesis.exceptions.ResourceNotFoundException:
        print(f"Stream 'test-stream' not found.")
        return

    for i in range(1, 1000):
        try:
            kinesis.put_record(
                StreamName='test-stream',
                Data=json.dumps({
                                    'sensor_id': random.randint(1, 100),
                                    'temperature': random.uniform(20.0, 30.0),
                                    'humidity': random.uniform(40.0, 60.0),
                                    'timestamp': int(time.time())
                                }),
                PartitionKey='sensorId'
            )
 #           time.sleep(0.1)
        except Exception as e:
            print(f"Exception encountered: {e}")

