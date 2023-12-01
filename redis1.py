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
    "SELECT as_of_date, account_no, security_no, quantity FROM transaction_ods.transaction WHERE batch_id = %s",
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

# Fetch data from the positin table for the specified batch_id
cursor.execute(
    "select p.as_of_date,p.account_no, p.security_no, p.quantity from transaction_ods.position p, transaction_ods.transaction t where \
     p.account_no=t.account_no and p.as_of_date = t.as_of_date and p.security_no = t.security_no and t.batch_id = %s",(batch_id,))
position_rows = cursor.fetchall()

# Use a Redis pipeline for efficient data loading
with r.pipeline() as pipe:
    # Load transaction data into Redis as hash tables
    for row in position_rows:
        key = f"position:{row[0]}:{row[1]}:{row[2]}"
        data = {
            "as_of_date": str(row[0]),  # Convert date to string
            "account_no": row[1],
            "security_no": row[2],
            "quantity": row[3]
        }
        pipe.hset(key, mapping=data)

    # Execute all pipeline commands
    pipe.execute()

# Fetch data from the price table for the specified batch_id
cursor.execute(
    "select distinct p.as_of_date, p.security_no, p.price from transaction_ods.security_price p, transaction_ods.transaction t where \
      p.as_of_date = t.as_of_date and p.security_no = t.security_no and t.batch_id = %s",(batch_id,))
price_rows = cursor.fetchall()

# Use a Redis pipeline for efficient data loading
with r.pipeline() as pipe:
    # Load transaction data into Redis as hash tables
    for row in price_rows:
        key = f"price:{row[0]}:{row[1]}:{row[2]}"
        data = {
            "as_of_date": str(row[0]),  # Convert date to string
            "security_no": row[1],
            "price": row[2]
        }
        pipe.hset(key, mapping=data)

    # Execute all pipeline commands
    pipe.execute()


# Close the PostgreSQL connection
cursor.close()
conn.close()