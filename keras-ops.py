import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

# Define the dataset
dataset = np.array([
    [1, 1000, 1100],
    [2, 1100, 1200],
    [3, 1200, 1300],
    [4, 1200, 1300],
    [5, 1300, 1400],
    [6, 1400, 1500],
    [7, 1400, 1500]
])

# Create a dictionary to map jobs to integer labels
job_to_label = {job: i for i, job in enumerate(np.unique(dataset[:, 0]))}

# Convert job labels to integers
encoded_dataset = np.copy(dataset)
for i in range(encoded_dataset.shape[0]):
    encoded_dataset[i, 0] = job_to_label[encoded_dataset[i, 0]]

# Convert time strings to numerical values
encoded_dataset[:, 1:] = encoded_dataset[:, 1:].astype(int)

# Sort the dataset based on start times
sorted_dataset = encoded_dataset[encoded_dataset[:, 1].argsort()]

# Create input and target sequences for LSTM
X = np.array([sorted_dataset[i:i + 2, 1:3] for i in range(len(sorted_dataset) - 1)])
y = np.array(sorted_dataset[1:, 0]).astype(int)

# Define the LSTM model
model = Sequential()
model.add(LSTM(10, input_shape=(2, 2)))
model.add(Dense(len(job_to_label), activation='softmax'))
model.compile(loss='sparse_categorical_crossentropy', optimizer='adam', metrics=['accuracy'])

# Train the LSTM model
model.fit(X, y, epochs=100, verbose=0)

# Predict the job dependencies
predicted_dependencies = model.predict(X)
predicted_dependencies = np.argmax(predicted_dependencies, axis=1)

# Print the dependencies
for i in range(len(predicted_dependencies)):
    job = dataset[i + 1, 0]
    dependency = dataset[predicted_dependencies[i] + 1, 0]
    print(f"If job {dependency} is late, job {job} will be affected.")
