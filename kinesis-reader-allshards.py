import boto3
import json
import time
from jsonschema import Draft7Validator, validators
 # Set up Kinesis client
client = boto3.client('kinesis')
 # Set up data lineage information
lineage = {
    "name": "reading records",
    "stream": "test-stream",
    "start_time": int(time.time()),
    "end_time": 0,
    "num_records": 0,
    "schema": {}
}
 # Get stream information
stream_info = client.describe_stream(StreamName=lineage['stream'])
shards = stream_info['StreamDescription']['Shards']
 # Read records from each shard and log data lineage
for shard in shards:
    shard_id = shard['ShardId']
    shard_iterator = client.get_shard_iterator(
        StreamName=lineage['stream'],
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )['ShardIterator']
    while True:
        response = client.get_records(
            ShardIterator=shard_iterator,
            Limit=100
        )
        records = response['Records']
        num_records = len(records)
        if num_records == 0:
            break
        for record in records:
            print(record['Data'])
            # Infer schema from record
            record_data = json.loads(record['Data'])
            for key in record_data.keys():
                if key not in lineage['schema']:
                    lineage['schema'][key] = str(type(record_data[key]))
        lineage['num_records'] += num_records
        shard_iterator = response['NextShardIterator']
 # Update end time and print data lineage and schema information
lineage['end_time'] = int(time.time())
print(lineage['num_records'])
print(json.dumps(lineage, indent=4))