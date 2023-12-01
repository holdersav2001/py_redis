import redis
import psycopg2
import json

# Define PostgreSQL connection parameters
pg_connection_params = {
    "dbname": "ods",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Define Redis connection parameters
redis_host = 'localhost'
redis_port = 6379
redis_db = 0

def get_redis_connection():
    return redis.Redis(host=redis_host, port=redis_port, db=redis_db)

def fetch_data(as_of_date, account_no):
    redis_key_pattern = f"{as_of_date}-{account_no}-*"
    r = get_redis_connection()

    # Initialize scan
    cursor = 0
    keys_found = False

    while True:
        cursor, keys = r.scan(cursor, match=redis_key_pattern, count=10)
        if keys:
            keys_found = True
            break
        if cursor == 0:
            break

    if keys_found:
        print("Found matching keys in Redis, exiting.")
        return

    else:
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(**pg_connection_params)
            cursor = conn.cursor()

            # Execute PostgreSQL function
            cursor.callproc("get_position_details", [as_of_date, account_no])
            result = cursor.fetchall()

            if result:
                # Populate Redis with individual records
                pipeline = r.pipeline()
                for record in result:
                    record_dict = {
                        'as_of_date': record[0].strftime('%Y-%m-%d'),
                        'account_no': record[1],
                        'security_no': record[2],
                        'quantity': record[3]
                    }
                    record_key = f"{as_of_date}-{account_no}-{record[2]}"
                    pipeline.hmset(record_key, record_dict)
                    pipeline.expire(record_key, 60)  # Set TTL to 60 seconds
                pipeline.execute()

            return result

        except (redis.exceptions.RedisError, psycopg2.Error) as e:
            print(f"Error fetching data: {e}")
        finally:
            # Close database connections
            cursor.close()
            conn.close()


def main():
    # Example usage
    as_of_date = '2023-06-01'
    account_no = '00000023'
    data = fetch_data(as_of_date, account_no)
    print(data)

if __name__ == "__main__":
    main()