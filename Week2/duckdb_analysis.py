import duckdb
import sys 
import time

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

# From the data, find the most placed color during that timeframe 
# and the most placed pixel location during that timeframe.
con = duckdb.connect()
con.execute("CREATE TABLE my_table AS SELECT * FROM read_csv('../../../../../Downloads/2022_place_canvas_history.csv')")
most_common_color = con.execute(f"""SELECT pixel_color
                  FROM my_table 
                  WHERE timestamp BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}' 
                  GROUP BY pixel_color 
                  ORDER BY COUNT(pixel_color) DESC 
                  LIMIT 1""").fetchall()

most_common_coord = con.execute(f"""SELECT coordinate 
                  FROM my_table 
                  WHERE timestamp BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}' 
                  GROUP BY coordinate
                  ORDER BY COUNT(coordinate) DESC 
                  LIMIT 1""").fetchall()

# Stop the timer
end_time = time.perf_counter_ns()

# Calculate execution time in milliseconds
execution_time = (end_time - start_time) / 1_000_000

# Output the results
print(f"Execution time: {execution_time:.3f} ms")
print(f"Most common color: {most_common_color}")
print(f"Most common coord: {most_common_coord}")