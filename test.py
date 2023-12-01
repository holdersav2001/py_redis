import boto3

def lambda_handler(event, context):
    # Set up Kinesis client
    client = boto3.client('kinesis')
    # Set up data lineage information
    stream_name = 'test-stream'
    # Get shard iterator for each shard in the stream
    response = client.describe_stream(StreamName=stream_name)
    shard_ids = [shard['ShardId'] for shard in response['StreamDescription']['Shards']]
    shard_iterators = [client.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator'] for shard_id in shard_ids]
    # Read records from each shard and count the number of records
    num_records_per_shard = {}
    for shard_iterator, shard_id in zip(shard_iterators, shard_ids):
        num_records = 0
        while shard_iterator:
            try:
                response = client.get_records(ShardIterator=shard_iterator, Limit=100)
                records = response['Records']
                num_records += len(records)
                shard_iterator = response['NextShardIterator']
            except Exception as e:
                logger.error(f"Error reading records from shard {shard_id}: {e}")
                break
        num_records_per_shard[shard_id] = num_records
    # Return the number of records per shard
    return num_records_per_shard