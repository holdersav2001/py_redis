import boto3
import json
import zlib
import base64
from concurrent.futures import ThreadPoolExecutor
 # Set up the Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-1')
 # Define the Kinesis stream name and batch size
stream_name = 'my_kinesis_stream'
batch_size = 1000000  # 1MB in bytes
 # Define a function to compress the payload
def compress_payload(payload):
    compressed_payload = zlib.compress(payload.encode('utf-8'))
    encoded_payload = base64.b64encode(compressed_payload).decode('utf-8')
    return encoded_payload
 # Define a function to send records to Kinesis
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
        print(f"Failed to send {response['FailedRecordCount']} records to Kinesis")
    else:
        print(f"Successfully sent {len(records)} records to Kinesis")
 # Define a function to process records asynchronously
def process_records(records):
    # Use a thread pool to send records to Kinesis asynchronously
    with ThreadPoolExecutor(max_workers=5) as executor:
        batch = []
        total_size = 0
        for record in records:
            compressed_record = compress_payload(json.dumps(record))
            record_size = len(compressed_record)
            # Check if adding this record will exceed the batch size
            if total_size + record_size > batch_size:
                executor.submit(send_to_kinesis, batch)
                batch = []
                total_size = 0
            # Check if this record alone exceeds the batch size
            elif record_size > batch_size:
                executor.submit(send_to_kinesis, [record])
            else:
                batch.append(record)
                total_size += record_size
        # Send any remaining records
        if batch:
            executor.submit(send_to_kinesis, batch)
 # Define some sample records to send to Kinesis
records = [
    {'id': 1, 'name': 'John'},
    {'id': 2, 'name': 'Jane'},
    {'id': 3, 'name': 'Bob'},
    {'id': 4, 'name': 'Alice'},
    {'id': 5, 'name': 'Charlie'},
    {'id': 6, 'name': 'David'},
    {'id': 7, 'name': 'Emily'},
    {'id': 8, 'name': 'Frank'},
    {'id': 9, 'name': 'Grace'},
    {'id': 10, 'name': 'Henry'}
]
 # Process the records asynchronously
process_records(records)