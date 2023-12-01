import logging
import boto3
import json
import zlib
import base64
import random
import time
import string
 # Set up the Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-1')
 # Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 # Define the Kinesis stream name and batch size
stream_name = 'test-stream'
batch_size = 1000000  # 1MB in bytes
 # Define the function to compress the payload
def compress_payload(payload):
    compressed_payload = zlib.compress(payload.encode('utf-8'))
    encoded_payload = base64.b64encode(compressed_payload).decode('utf-8')
    return encoded_payload
 # Define the function to send records to Kinesis
def send_to_kinesis(records):
    # Compress the records and calculate the total size
    compressed_records = []
    total_size = 0
    for record in records:
        compressed_record = compress_payload(json.dumps(record))
        compressed_records.append({'Data': compressed_record})
        total_size += len(compressed_record)
     # Write the compressed records to Kinesis
    response = kinesis_client.put_records(StreamName=stream_name, Records=compressed_records)
     # Check for failed records
    if response['FailedRecordCount'] > 0:
        logger.error(f"Failed to send {response['FailedRecordCount']} records to Kinesis")
        print(f"Failed to send {response['FailedRecordCount']} records to Kinesis")
    else:
        logger.info(f"Successfully sent {len(records)} records to Kinesis")
        print(f"Successfully sent {len(records)} records to Kinesis")
 # Define the sample record generation function
def generate_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))
 # Define the Lambda handler function
def lambda_handler(event, context):
    # Generate 1000 records
    records = []
    for _ in range(1000):
        record = {
            'sensor_id': random.randint(1, 100),
            'temperature': random.uniform(20.0, 30.0),
            'humidity': random.uniform(40.0, 60.0),
            'timestamp': int(time.time()),
            'region': generate_random_string(random.randint(20, 40)),
            'comments': generate_random_string(random.randint(20, 40))
        }
        records.append(record)
        
     print(f"Created 1000 records")
     # Process the records synchronously
    send_to_kinesis(records)
 # Define the function to process records synchronously
def process_records(records):
    # Send records to Kinesis synchronously
    send_to_kinesis(records)