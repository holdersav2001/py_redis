import concurrent.futures
import psycopg2
import math

# Database connection parameters
db_params = {
    'dbname': 'ods',
    'user': 'posgres',
    'password': 'postgres',
    'host': 'localhost'
}

def get_tcid_ranges(connection, num_executors):
    cursor = connection.cursor()
    cursor.execute("SELECT MIN(tcid), MAX(tcid) FROM transaction_ods.\"position\"")
    min_tcid, max_tcid = cursor.fetchone()
    cursor.close()

    tcid_range = max_tcid - min_tcid
    range_size = math.ceil(tcid_range / num_executors)
    return [(min_tcid + i * range_size, min(min_tcid + (i + 1) * range_size - 1, max_tcid)) for i in range(num_executors)]

def process_range(db_params, range_start, range_end):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM transaction_ods.\"position\" WHERE tcid BETWEEN %s AND %s", (range_start, range_end))
        data = cursor.fetchall()
        cursor.close()
        # Process data...
        print(f"Processed TCID range {range_start} to {range_end}")
    except Exception as e:
        print(f"Error processing range {range_start} to {range_end}: {str(e)}")
    finally:
        connection.close()

def main():
    num_executors = 5  # Adjust based on your needs
    connection = psycopg2.connect(**db_params)
    ranges = get_tcid_ranges(connection, num_executors)
    connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_executors) as executor:
        futures = [executor.submit(process_range, db_params, start, end) for start, end in ranges]
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    main()
