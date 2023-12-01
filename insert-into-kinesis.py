import boto3
import json
import random
import time

def lambda_handler(event, context):
    kinesis = boto3.client('kinesis')
    stream_name = 'test-stream'
    for i in range(1000):
        data = {
            'sensor_id': random.randint(1, 100),
            'temperature': random.uniform(20.0, 30.0),
            'humidity': random.uniform(40.0, 60.0),
            'timestamp': int(time.time())
        }
        partition_key = str(data['sensor_id'])
        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=partition_key
        )
    return {
        'statusCode': 200,
        'body': 'Successfully generated 1000 Kinesis records'
    }