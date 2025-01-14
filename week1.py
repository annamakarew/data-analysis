import sys

# Check for valid args

if len(sys.argv) < 4:
    print("Intended use: python3 week1.py YYYY-MM-DD HH YYYY-MM-DD HH")
    sys.exit()

start_date = sys.argv[1]
start_hour = sys.argv[2]
end_date = sys.argv[3]
end_hour = sys.argv[4]

# Validate that end date is not before start date
if start_date > end_date:
    print("Error: start date is after end date")
    sys.exit()

# Validate that end hour is after start hour
if start_date == end_date & start_hour > end_hour:
    print("Error: start hour is after end hour")
    sys.exit()

# From the data, find the most placed color during that timeframe.

# From the data, find the most placed pixel location during that timeframe.