import psycopg2
import redis

# Define PostgreSQL connection parameters
pg_connection_params = {
    "dbname": "ods",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Connect to PostgreSQL
conn = psycopg2.connect(**pg_connection_params)
cursor = conn.cursor()

# Define Redis connection parameters
redis_host = 'localhost'
redis_port = 6379
redis_db = 0

# Connect to Redis
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Specify the batch_id you want to process
batch_id = 10

# Fetch data from the transaction table for the specified batch_id
cursor.execute(
    "SELECT as_of_date, account_no, security_no,  sum(quantity) FROM transaction_ods.transaction WHERE batch_id = %s group by \
      as_of_date, account_no, security_no  ",
    (batch_id,))
transaction_rows = cursor.fetchall()

# Use a Redis pipeline for efficient data loading
with r.pipeline() as pipe:
    # Load transaction data into Redis as hash tables
    for row in transaction_rows:
        key = f"transaction:{row[0]}:{row[1]}:{row[2]}"
        data = {
            "as_of_date": str(row[0]),  # Convert date to string
            "account_no": row[1],
            "security_no": row[2],
            "quantity": row[3]
        }
        pipe.hset(key, mapping=data)

    # Execute all pipeline commands
    pipe.execute()



    # Execute all pipeline commands
    pipe.execute()


# Close the PostgreSQL connection
cursor.close()
conn.close()