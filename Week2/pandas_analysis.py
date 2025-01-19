import pandas as pd
import sys
import time
from collections import Counter

# Check for valid args
if len(sys.argv) < 5:
    print("Intended use: python3 week1.py YYYY-MM-DD HH YYYY-MM-DD HH")
    sys.exit()

start_date = sys.argv[1]
start_hour = sys.argv[2]
end_date = sys.argv[3]
end_hour = sys.argv[4]

# Combine the date and hour into strings
start_str = start_date + " " + start_hour + ":00:00"  # Format it as YYYY-MM-DD HH:00:00
end_str = end_date + " " + end_hour + ":00:00"  # Format it as YYYY-MM-DD HH:00:00

# Validate that end date is not before start date
if start_str > end_str:
    print("Error: start date is after end date")
    sys.exit()

# Start the timer
start_time = time.perf_counter_ns()

# Efficient CSV reading with datetime parsing and chunking
chunksize = 100000  # Adjust based on memory available
color_counter = Counter()  # To count occurrences of each color
coord_counter = Counter()  # To count occurrences of each coordinate

for chunk in pd.read_csv('../../../../../Downloads/2022_place_canvas_history.csv', 
                         chunksize=chunksize, 
                         parse_dates=['timestamp']):
    # Filter the data based on the timestamp range
    chunk_filtered = chunk[(chunk['timestamp'] >= start_str) & (chunk['timestamp'] <= end_str)]
    
    # Update the frequency counts for color and coordinate
    color_counter.update(chunk_filtered['pixel_color'])
    coord_counter.update(chunk_filtered['coordinate'])

# After processing all chunks, get the most common color and coordinate
most_common_color = color_counter.most_common(1)[0][0] if color_counter else None
most_common_coord = coord_counter.most_common(1)[0][0] if coord_counter else None

# Stop the timer
end_time = time.perf_counter_ns()

# Calculate execution time in milliseconds
execution_time = (end_time - start_time) / 1_000_000

# Output the results
print(f"Execution time: {execution_time:.3f} ms")
print(f"Most common color: {most_common_color}")
print(f"Most common coord: {most_common_coord}")
