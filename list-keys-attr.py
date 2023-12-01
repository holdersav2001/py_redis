import redis

# Establish Redis connection
r = redis.Redis(host='localhost', port=6379, db=0)

# Use `keys` to get all keys that match the pattern
matching_keys = r.keys("2023-06-01-00000023*")

# Initialize an empty list to store the data
data = []

# Loop through the keys and fetch the data
for key in matching_keys:
    key = key.decode()  # Decode byte string to string
    data.append(r.hgetall(key))

# At this point, `data` will hold all the field-value pairs for each key as a list
print(data)