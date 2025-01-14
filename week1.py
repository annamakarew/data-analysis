import sys
import csv
from collections import Counter
from datetime import datetime
import time

# Check for valid args
if len(sys.argv) < 5:
    print("Intended use: python3 week1.py YYYY-MM-DD HH YYYY-MM-DD HH")
    sys.exit()

start_date = sys.argv[1]
start_hour = sys.argv[2]
end_date = sys.argv[3]
end_hour = sys.argv[4]

# Convert to datetime objects for comparison
start = datetime.strptime(start_date + " " + start_hour, '%Y-%m-%d %H')
end = datetime.strptime(end_date + " " + end_hour, '%Y-%m-%d %H')

# Validate that end date is not before start date
if start > end:
    print("Error: start date is after end date")
    sys.exit()

# Start the timer
start_time = time.perf_counter_ns()

# From the data, find the most placed color during that timeframe 
# and the most placed pixel location during that timeframe.
color_counter = Counter()
coord_counter = Counter()

with open("../../../../Downloads/2022_place_canvas_history.csv", 'r') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)

    min_timestamp = None
    max_timestamp = None

    for row in reader:
        date_pieces = row[0].split()
        try:
            # Try parsing with fractional seconds
            timestamp = datetime.strptime(date_pieces[0] + " " + date_pieces[1], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            # If it fails, parse without fractional seconds
            timestamp = datetime.strptime(date_pieces[0] + " " + date_pieces[1], '%Y-%m-%d %H:%M:%S')

        # Update min and max timestamps
        if min_timestamp is None or timestamp < min_timestamp:
            min_timestamp = timestamp
        if max_timestamp is None or timestamp > max_timestamp:
            max_timestamp = timestamp

        # If timestamp is within the range, count color and coordinates
        if start <= timestamp <= end:
            color_counter[row[2]] += 1
            coord_counter[row[3]] += 1

if min_timestamp is None or max_timestamp is None:
    print("Error: No valid timestamps found in data.")
    sys.exit()

if end < min_timestamp or start > max_timestamp:
    print(f"Error: Given time input is not represented in data, which ranges from {min_timestamp} to {max_timestamp}.")
    sys.exit()

# Find the most common color and coordinates
most_common_color = color_counter.most_common(1)[0][0]
most_common_coord = coord_counter.most_common(1)[0][0]

# Stop the timer
end_time = time.perf_counter_ns()

# Calculate execution time in milliseconds
execution_time = (end_time - start_time) / 1_000_000

# Output the results
print(f"Most common color: {most_common_color}")
print(f"Most common coord: {most_common_coord}")
print(f"Execution time: {execution_time:.3f} ms")
