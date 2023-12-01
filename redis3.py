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
    redis_key = f"{as_of_date}_{account_no}"
    r = get_redis_connection()

    # Use Redis pipeline
    pipeline = r.pipeline()
    pipeline.hgetall(redis_key)
    cached_data = pipeline.execute()[0]

    if cached_data:
        # Decode cached data
        decoded_data = {k.decode(): json.loads(v.decode()) for k, v in cached_data.items()}
        return decoded_data
    else:
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(**pg_connection_params)
            cursor = conn.cursor()

            # Execute PostgreSQL function
            cursor.callproc("my_flexible_function_json_columns", ['2023-06-01', '00000023', None, ['account_no','security_no','quantity']])
            result = cursor.fetchall()[0][0]

            # Populate Redis as hash with configurable TTL
            r.hmset(redis_key, {'data': json.dumps(result)})
            r.expire(redis_key, 60) # Set TTL to 60 seconds

            return result
        except (redis.exceptions.RedisError, psycopg2.Error) as e:
            print(f"Error fetching data: {e}")
        finally:
            # Close database connections
            cursor.close()
            conn.close()

def main():
    # Example usage
    data = fetch_data('2023-06-01', '00000023')
    print(data)

if __name__ == "__main__":
    main()