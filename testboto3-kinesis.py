import boto3 
import json 
import os 
# Importing boto3 library 
import boto3 
 # Function to read records from a shard 
def read_shard_records(kinesis, shard_id): 
    # Getting shard iterator for the given shard 
    shard_iterator = kinesis.get_shard_iterator( 
        StreamName='test-stream', 
        ShardId=shard_id, 
        ShardIteratorType='TRIM_HORIZON' 
    ) 
    # Assigning iterator to a variable 
    iterator = shard_iterator['ShardIterator'] 
    # Initializing empty list to store records 
    records = [] 
    # Loop to get records from the shard 
    while True: 
        # Getting records from the shard 
        response = kinesis.get_records(ShardIterator=iterator, Limit=1000) 
        # Breaking the loop if there are no more records 
        if not response['Records']: 
            break 
        # Appending records to the list 
        records.extend(response['Records']) 
        # Updating iterator 
        iterator = response['NextShardIterator'] 
    # Returning the list of records 
    return records 
 # Function to read records from all shards of a stream 
def read_records(stream_name='test-stream'): 
    # Creating Kinesis client 
    kinesis = boto3.client('kinesis') 
    # Describing the stream 
    response = kinesis.describe_stream(StreamName=stream_name) 
    # Extracting shards from the response 
    shards = response['StreamDescription']['Shards'] 
    # Initializing empty list to store records 
    records = [] 
    # Looping through all shards 
    for shard in shards: 
        # Getting records from the shard 
        shard_records = read_shard_records(kinesis, shard['ShardId']) 
        # Appending records to the list 
        records.extend(shard_records) 
    # Returning the list of records from all shards 
    return records 
 # Main handler function for AWS Lambda 
def lambda_handler(event, context): 
    # TODO: Add implementation logic here 
    pass 
    stream_name = event.get('stream_name', 'test-stream') 
    records = read_records(stream_name) 
    for record in records: 
        print(record)
            
            
                







