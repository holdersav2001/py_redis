import boto3
import json
import random
import time

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

    for i in range(1, 10):
        try:
            kinesis.put_record(
                StreamName='test-stream',
                Data=json.dumps({'eventID': i, 'value': random.randint(1, 100)}),
                PartitionKey='test'
            )
            time.sleep(0.1)
        except Exception as e:
            print(f"Exception encountered: {e}")

def main(event, context):
    generate_data()

if __name__ == '__main__':
    generate_data()
