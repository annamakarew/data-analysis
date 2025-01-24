import polars as pl
import sys
import time
from datetime import datetime
from colory.color import Color

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

print('Scanning CSV')
# Filter by timestamp here
df = pl.scan_csv('../../../../../Downloads/2022_place_canvas_history.csv').filter(pl.col("timestamp") <= pl.lit(end_str))

print("Converting to parquet")
df.sink_parquet('output.parquet')

# Read the parquet file
df = pl.read_parquet('output.parquet')

# Sort data by user_id and timestamp
df_sorted = df.sort(['user_id', 'timestamp'])

# Remove UTC from the timestamp values
df_sorted = df_sorted.with_columns(
    pl.col("timestamp").str.replace(r" UTC$", "")  # Remove the ' UTC' part from the timestamp string
    .alias("timestamp")
)

# Add missing milliseconds to the timestamp if it's missing
df_sorted = df_sorted.with_columns(
    pl.when(~pl.col("timestamp").str.contains(r"\."))
    .then(pl.concat_str([pl.col("timestamp"), pl.lit(".000")]))
    .otherwise(pl.col("timestamp"))
    .alias("timestamp")
)

# Convert timestamp column to datetime format
df_sorted = df_sorted.with_columns(
    pl.col("timestamp").str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f").alias("timestamp")
)

# Filter the DataFrame to include only records within the specified date and time range
df_time_filtered = df_sorted.filter(pl.col("timestamp") >= datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S"))

# Start measuring execution time
start_time = time.perf_counter_ns()

# Ranking of colors by distinct users
color_rank = df_time_filtered.group_by("pixel_color").agg(
    [
       pl.n_unique("user_id").alias("distinct_user_count") 
    ]
).sort("distinct_user_count", descending=True)

print("Ranking of Colors by Distinct Users")
for idx, item in enumerate(color_rank.iter_rows(), start=1):
    try:
        color_name = Color(item[0], 'xkcd').name
    except ValueError:
        color_name = item[0]
    print(f"{idx}. {color_name}: {item[1]} users")

# Calculate average session length
# Calculate the time difference (in minutes) between consecutive timestamps within each user
df_session_length = df_time_filtered.with_columns(
    pl.col("timestamp")
    .diff()
    .over("user_id")
    .alias("time_diff")
)

# Create a session column based on time difference
df_session_length = df_session_length.with_columns(
    (pl.col("time_diff").is_null() | (pl.col("time_diff") > pl.duration(minutes=15)))
    .cum_sum()
    .over("user_id")
    .alias("session")
)

# Per user and session, count the number of pixels placed during that session
df_grouped= df_session_length.group_by(["user_id", "session"]).agg([
    pl.col("timestamp").count().alias("session_count"),
    pl.col("timestamp").first().alias("session_start"),
    pl.col("timestamp").last().alias("session_end"),
])

# Filter out sessions with only one pixel placed
valid_sessions = df_grouped.filter(pl.col("session_count") > 1)

# Calculate the session duration
valid_sessions = valid_sessions.with_columns(
    (pl.col("session_end") - pl.col("session_start")).alias("session_duration")
)

# Calculate the average session duration
print(f"\nAverage session length: {valid_sessions["session_duration"].mean().total_seconds()} seconds")

# Pixel placement percentiles
df_percentiles= df_time_filtered.group_by("user_id").agg([
    pl.col("timestamp").count().alias("total_pixels_placed")
])

# Calculate the 50th, 75th, 90th, and 99th percentiles of total pixels placed per user
percentiles = df_percentiles.select([
    pl.col("total_pixels_placed").quantile(0.50).alias("50th_percentile"),
    pl.col("total_pixels_placed").quantile(0.75).alias("75th_percentile"),
    pl.col("total_pixels_placed").quantile(0.90).alias("90th_percentile"),
    pl.col("total_pixels_placed").quantile(0.99).alias("99th_percentile"),
])

print(percentiles)

# Count first time users
df_first_time = df_sorted.with_columns(
    pl.col("user_id").is_first_distinct().alias("first")
)

df_first_time_filtered = df_first_time.filter(
    pl.col("timestamp") >= datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
)

# Filter only first-time users
df_first_time_filtered = df_first_time_filtered.filter(
    pl.col("first") == "true"
)

print(f"Count of first time users: {df_first_time_filtered.select(pl.count("first"))}")

# Stop runtime timer
end_time = time.perf_counter_ns()

execution_time = (end_time - start_time) / 1_000_000

print(f"Runtime: {execution_time}")