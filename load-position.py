import psycopg2
import redis

# Connect to Postgres
conn = psycopg2.connect(
    dbname="ods",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Query the position table
query = "SELECT as_of_date, account_no, security_no, quantity FROM ods_transaction.position"
cur.execute(query)
result = cur.fetchall()

# Connect to Redis and use pipeline
r = redis.Redis(host='127.0.0.1', port=6379)
pipeline = r.pipeline()

# Load the result into Redis using hashes
for row in result:
    key = f"transaction:{row[0]}:{row[1]}:{row[2]}"
    values = {
        "as_of_date": str(row[0]),
        "account_no": row[1],
        "security_no": row[2],
        "quantity": row[3]
    }
    pipeline.hset(key, mapping=values)

# Execute the pipeline and close connections
pipeline.execute()
cur.close()
conn.close()