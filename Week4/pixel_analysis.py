import polars as pl
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas
from colory.color import Color

print("Reading parquet file")
df = pl.read_parquet('output.parquet')

# Remove UTC from the timestamp values
print("Normalizing timestamp column")
df = df.with_columns(
    pl.col("timestamp").str.replace(r" UTC$", "")  # Remove the ' UTC' part from the timestamp string
    .alias("timestamp")
)

# Add missing milliseconds to the timestamp if it's missing
df = df.with_columns(
    pl.when(~pl.col("timestamp").str.contains(r"\."))
    .then(pl.concat_str([pl.col("timestamp"), pl.lit(".000")]))
    .otherwise(pl.col("timestamp"))
    .alias("timestamp")
)

# Convert timestamp column to datetime format
df = df.with_columns(
    pl.col("timestamp").str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f").alias("timestamp")
)

# Find the top 3 coordinates
most_common_coord = df.select(pl.col('coordinate').value_counts(sort=True)).unnest('coordinate').head(3)
print(most_common_coord)

# Results: (0,0) with 98,807 placements, (359,564) with 69,198 placements, and (349, 564) with 55,230

# 1. WHY are these pixels getting so many hits? Who is responsible for painting these pixels?
df_top = df.filter(pl.col("coordinate").is_in(most_common_coord['coordinate'].to_list()))

most_common_users = df_top.select(pl.col('user_id').value_counts(sort=True)).unnest('user_id')
print(most_common_users)

# Group by coordinate and count unique users per coordinate
unique_users_per_coord = (
    df_top.group_by("coordinate")
    .agg(pl.col("user_id").n_unique().alias("unique_users"))
)

# Convert to a Pandas DataFrame for plotting
unique_users_pd = unique_users_per_coord.to_pandas()

# Plot the bar chart
plt.figure(figsize=(10, 5))
ax = unique_users_pd.plot.bar(x="coordinate", y="unique_users", legend=False)
ax.set_xlabel("Coordinate")
ax.set_ylabel("Number of Unique Users")
ax.set_title("Unique Users per Top Coordinate")
plt.tight_layout()
plt.show()

# Results: 162,547 unique users between those 3 coordinates.
# Top user 10272996276027584121 placed in one of those 3 coordinates 102 times.

# 2. WHY do both casual users and superusers contribute to the pixel placements at these coordinates?

# Group by coordinate and user_id to count how many times each user painted that coordinate.
user_counts = (df_top
            .group_by(['coordinate', 'user_id'])
            .agg(
                    pl.count('coordinate').alias('paint_count')
            )
            .sort(['paint_count', 'coordinate'], descending=[True, False])
)

print(user_counts)

dist_sum = (
    user_counts
    .group_by('coordinate')
    .agg([
        pl.col('paint_count').mean().alias('avg_paints'),
        pl.col('paint_count').median().alias('median_paints'),
        pl.count('user_id').alias('total_users')
    ])
    .sort('coordinate')
)

print(dist_sum)

# Results: (359, 564) had user 10272996276027584121 paint 95 times. (359, 564) had user 6279457773120745798 paint 65 times.
# (0,0) had user 17773545971454702240 paint 62 times. (0, 0) had user 18346040412207687616 paint 52 times. 
# (0,0) had user 9117946461022715619 paint 50 times.

# 3. WHY were these pixels specifically targeted by these users, and what motivations influenced this choice?
result = (
    df_top
    .with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("hour_interval")
    )
    .group_by(["coordinate", "hour_interval"])
    .agg(pl.len().alias("paints_in_interval"))
    .sort(["paints_in_interval"], descending=True)
)
print(result)

# Convert the Polars DataFrame to a Pandas DataFrame
result_pd = result.to_pandas()

# Create a pivot table with coordinates as rows and hour intervals as columns
pivot_table = result_pd.pivot(index="coordinate", columns="hour_interval", values="paints_in_interval")
# Replace NaN with 0 for plotting purposes
pivot_table = pivot_table.fillna(0)

# Plotting the heatmap
plt.figure(figsize=(12, 8))
heatmap = plt.imshow(pivot_table, cmap='viridis', aspect='auto')

# Create a colorbar with label
plt.colorbar(heatmap, label='Number of Paints')

# Set x-tick labels using the hour intervals
# Convert the hour intervals to strings for clarity (assuming they are datetime objects)
x_labels = [col.strftime('%Y-%m-%d %H:%M') if hasattr(col, 'strftime') else str(col) for col in pivot_table.columns]
plt.xticks(ticks=np.arange(len(pivot_table.columns)), labels=x_labels, rotation=45)

# Set y-tick labels using the coordinates
plt.yticks(ticks=np.arange(len(pivot_table.index)), labels=pivot_table.index)

plt.xlabel('Hour Interval')
plt.ylabel('Coordinate')
plt.title('Heatmap of Placements Over Hourly Intervals')
plt.tight_layout()
plt.show()

# Result: Pixels are being placed the most the evening of April 4th and the early morning of April 5th at the coordinate (0,0).

# 4. WHY is is the evening of April 4th such a popular time to be placing pixels? Was the event ending?
print(df.select(pl.col('timestamp')).min())
print(df.select(pl.col('timestamp')).max())

# Result: The event was ending then. The first pixel was placed at 12:44 pm on 2022-04-01. The last pixel was placed at 12:14 am on 2022-04-05.
# Due to the event ending, there was a rush of activity.


# 5. WHY does (0,0) get painted so much at the end of the event? Was there a color battle?
threshold_datetime = datetime(2022, 4, 4, 23, 0, 0)

df_filtered = df_top.select(
    pl.col("pixel_color")
    .filter(
        (pl.col("timestamp") >= pl.lit(threshold_datetime)) & 
        (pl.col("coordinate") == "0,0"))
    .value_counts(sort=True)
      
)

print(df_filtered)

# Use a list comprehension to map hex color codes to their English names using colory
color_names = [Color(hex_val['pixel_color'], 'xkcd').name for hex_val in df_filtered["pixel_color"]]

# Add the color names as a new column
df_filtered = df_filtered.with_columns(pl.Series("color_name", color_names))

print(df_filtered)

# Result: White was placed 27,830 times at (0,0) and was the only color placed during that timeframe.