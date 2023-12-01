import psycopg2
import redis

# Define your PostgreSQL connection parameters
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

# Fetch data from the position table for a specific batch_id (e.g., batch_id = 100)
batch_id = 100
cursor.execute("SELECT as_of_date, account_no, security_no, quantity FROM position WHERE batch_id = %s", (batch_id,))
position_rows = cursor.fetchall()

# Fetch data from the price table
cursor.execute("SELECT as_of_date, security_no, price FROM price")
price_rows = cursor.fetchall()

# Close the PostgreSQL connection
cursor.close()
conn.close()

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Use pipeline for data loading into Redis
with r.pipeline() as pipe:
    # Load data into Redis for the position table
    for row in position_rows:
        key = f"position:{row[0]}:{row[1]}:{row[2]}"
        data = {
            "as_of_date": row[0],
            "account_no": row[1],
            "security_no": row[2],
            "quantity": row[3]
        }
        pipe.hmset(key, data)

    # Load data into Redis for the price table
    for row in price_rows:
        key = f"price:{row[0]}:{row[1]}"
        data = {
            "as_of_date": row[0],
            "security_no": row[1],
            "price": row[2]
        }
        pipe.hmset(key, data)

    # Calculate and store the value of a position for the specified batch_id in a new Redis hash table
    for row in position_rows:
        key = f"position_value:{row[0]}:{row[1]}:{row[2]}"
        data = {
            "as_of_date": row[0],
            "account_no": row[1],
            "security_no": row[2],
            "value": row[3] * float(pipe.hget(f"price:{row[0]}:{row[2]}", "price").decode("utf-8"))
        }
        pipe.hmset(key, data)

    # Execute all pipeline commands
    pipe.execute()

print("Data loaded into Redis successfully using pipeline.")
