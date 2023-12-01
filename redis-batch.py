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
cursor.execute("SELECT as_of_date, account_no, security_no, quantity FROM transaction_ods.transaction WHERE batch_id = %s", (batch_id,))
transaction_rows = cursor.fetchall()

# Use a Redis pipeline for efficient data loading
with r.pipeline() as pipe:
    # Load transaction data into Redis as hash tables
# Load transaction data into Redis as hash tables
    for row in transaction_rows:
        key = f"transaction:{row[0]}:{row[1]}:{row[2]}"
        data = {
            "as_of_date": row[0],
            "account_no": row[1],
            "security_no": row[2],
            "quantity": row[3]
        }
        pipe.hset(key, mapping=data)


    # Fetch unique account_no and security_no values from the transaction data
    unique_accounts = set(row[1] for row in transaction_rows)
    unique_security = set(row[2] for row in transaction_rows)

    # Fetch position data for unique account_no and security_no values
    for account in unique_accounts:
        for security in unique_security:
            key = f"position:{account}:{security}"
            cursor.execute("SELECT as_of_date, quantity FROM transaction_ods.position WHERE account_no = %s AND security_no = %s", (account, security))
            position_rows = cursor.fetchall()
            
            # Calculate total quantity from position data and transaction data
            total_quantity = sum(row[1] for row in position_rows)
            
            # Store the total quantity in a new Redis hash table
            pipe.hmset(key, {"quantity": total_quantity})

    # Fetch unique security_no values from the transaction data
    unique_security = set(row[2] for row in transaction_rows)

    # Fetch security_price data for unique security_no values
    for security in unique_security:
        key = f"security_price:{security}"
        cursor.execute("SELECT price FROM transaction_ods.security_price WHERE security_no = %s", (security,))
        price_rows = cursor.fetchall()
        
        # Calculate total price from security_price data
        total_price = sum(row[0] for row in price_rows)
        
        # Store the total price in a new Redis hash table
        pipe.hmset(key, {"price": total_price})

    # Create a new Redis hash table for the final result by combining transaction, position, and security_price data
    for row in transaction_rows:
        transaction_key = f"transaction:{row[0]}:{row[1]}:{row[2]}"
        position_key = f"position:{row[1]}:{row[2]}"
        security_price_key = f"security_price:{row[2]}"
        result_key = f"result:{row[0]}:{row[1]}:{row[2]}"
        
        # Get quantity from transaction data
        quantity = int(pipe.hget(transaction_key, "quantity"))
        
        # Get total quantity from position data
        total_quantity = int(pipe.hget(position_key, "quantity"))
        
        # Get total price from security_price data
        total_price = int(pipe.hget(security_price_key, "price"))
        
        # Calculate the result
        result = quantity * total_price
        
        # Store the result in the final Redis hash table
        pipe.hmset(result_key, {"as_of_date": row[0], "account_no": row[1], "security_no": row[2], "result": result})

    # Execute all pipeline commands
    pipe.execute()

# Close the PostgreSQL connection
cursor.close()
conn.close()

print("Data loaded into Redis and calculated successfully using pipeline.")
