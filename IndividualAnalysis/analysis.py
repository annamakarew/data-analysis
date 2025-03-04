import polars as pl
import matplotlib.pyplot as plt

# Define function filter out outliers
def filter_outliers(df, column_name):
    # Calculate the mean and standard deviation of column age
    mean = df.select(pl.col(column_name)).mean().collect()
    mean_val = mean[0, column_name]
    std = df.select(pl.col(column_name)).std().collect()
    std_val = std[0, column_name]
    print(mean_val)
    print(std_val)

    # Remove outliers through filtering
    df_filtered = df.filter(
        (pl.col(column_name) < mean_val + 3*std_val) & (pl.col(column_name) > mean_val - 3*std_val)
    )
    
    return df_filtered

# Use lazy scan
df_lazy = pl.scan_parquet("../../../../../Downloads/archive/parquet/*.parquet")

# Apply filters
df_lazy_filtered = df_lazy.filter(
    (pl.col('base_passenger_fare') > 0) & (pl.col('driver_pay') > 0)
)

# Calculate trip duration
df_lazy_filtered = df_lazy_filtered.with_columns(
    # Calculate trip duration in seconds
    ((pl.col('dropoff_datetime') - pl.col('pickup_datetime')) / 1e9).cast(pl.Int64).alias('calc_duration')    
)

# Reduce precision lazily (only after performing necessary operations)
df_lazy_filtered = df_lazy_filtered.select(
    [pl.col(col).cast(pl.Float32) if df_lazy_filtered.schema[col] == pl.Float64 else pl.col(col) 
     for col in df_lazy_filtered.columns]
)

# Select the relevant columns
df_lazy_filtered = df_lazy_filtered.select(['driver_pay', 'tips', 'calc_duration', 'request_datetime', 'pickup_datetime', 'dropoff_datetime', 'trip_miles', 'PULocationID', 'DOLocationID'])

# Filter outliers based on tips and base pay
df_lazy_filtered = filter_outliers(df_lazy_filtered, 'tips')
df_lazy_filtered = filter_outliers(df_lazy_filtered, 'driver_pay')
df_lazy_filtered = filter_outliers(df_lazy_filtered, 'calc_duration')

# Apply filters
df_lazy_filtered = df_lazy_filtered.filter(
    pl.col('calc_duration') > 0
)

# Assuming pickup_datetime is a datetime column, extract date components
df_time = df_lazy_filtered.with_columns(
    pl.col('pickup_datetime').dt.month().alias('month'),
    pl.col('pickup_datetime').dt.year().alias('year')
)

# Create new columns to calculate key metrics (total earnings and tip proportion)
df_lazy_filtered = df_lazy_filtered.with_columns(
    (pl.col('driver_pay') + pl.col('tips')).alias('total_earnings')
)
df_lazy_filtered = df_lazy_filtered.with_columns(
    (pl.col('total_earnings') / pl.col('tips')).alias('tip_proportion')
)

# Calculate average base pay and average tip per trip
df_stats = df_lazy_filtered.select([
    pl.col('driver_pay').mean().alias('avg_pay'),
    pl.col('tips').mean().alias('avg_tip'),
    pl.col('total_earnings').mean().alias('avg_total_earnings')
])

# Calculate the average proportion of earnings from tips
df_stats = df_stats.with_columns(
    (pl.col('avg_tip') / pl.col('avg_total_earnings')).alias('avg_tip_proportion')
)

print(df_stats.collect())

# Convert from LazyFrame to DataFrame for plotting
df_plotting_df = df_lazy_filtered.collect()

# Plot histograms for distribution of base pay and tips
plt.figure(figsize=(12,6))
'''
# Base pay histogram
plt.subplot(1, 2, 1)
plt.hist(df_plotting_df['driver_pay'], bins=30, color='blue', alpha=.7)
plt.title('Distribution of Base Pay')
plt.xlabel('Base Pay')
plt.ylabel('Frequency')

# Tips histogram
plt.subplot(1, 2, 2)
plt.hist(df_plotting_df['tips'], bins=30, color='green', alpha=.7)
plt.title('Distribution of Tips')
plt.xlabel('Tips')
plt.ylabel('Frequency')

plt.tight_layout()
plt.show()
'''
# Boxplot for base pay vs. tips
plt.figure(figsize=(8, 6))
plt.boxplot([df_plotting_df['driver_pay'], df_plotting_df['tips']], tick_labels=['Base Pay', 'Tips'])
plt.title('Boxplot of Base Fares vs. Tips')
plt.xlabel('Type')
plt.ylabel('Amount')
plt.show()

# Assuming pickup_datetime is a datetime column, extract date components
df_time = df_lazy_filtered.with_columns(
    pl.col('pickup_datetime').dt.month().alias('month')
)

# Calculate the average tip per trip per month
df_time_avg_tips = df_time.group_by(['month']).agg(
    pl.col('tips').mean().alias('avg_tip')
)

# Collect the result and convert to a DataFrame for plotting
df_time_avg_tips_df = df_time_avg_tips.sort('month').collect()

# Plotting the average tip over time (by month)
plt.figure(figsize=(10, 6))
plt.plot(df_time_avg_tips_df['month'], df_time_avg_tips_df['avg_tip'], marker='o', color='orange')
plt.title('Average Tip Per Trip Over Time')
plt.xlabel('Month')
plt.ylabel('Average Tip')
plt.xticks(df_time_avg_tips_df['month'], rotation=45)
plt.tight_layout()
plt.show()

# Extract hour of the day from pickup_datetime
df_time = df_lazy_filtered.with_columns(
    pl.col('pickup_datetime').dt.hour().alias('hour')
)

# Calculate the average tip per hour
df_time_avg_tips_hour = df_time.group_by('hour').agg(
    pl.col('tips').mean().alias('avg_tip')
)

# Collect the result and plot
df_time_avg_tips_hour_df = df_time_avg_tips_hour.sort('hour').collect()

plt.figure(figsize=(10, 6))
plt.plot(df_time_avg_tips_hour_df['hour'], df_time_avg_tips_hour_df['avg_tip'], marker='o', color='purple')
plt.title('Average Tip Per Hour of the Day')
plt.xlabel('Hour of the Day')
plt.ylabel('Average Tip')
plt.tight_layout()
plt.show()

# Assuming 'calc_duration' is the trip duration in seconds
plt.figure(figsize=(8, 6))
plt.scatter(df_plotting_df['calc_duration'], df_plotting_df['tips'], alpha=0.5, color='red')
plt.title('Relationship Between Trip Duration and Tips')
plt.xlabel('Trip Duration (seconds)')
plt.ylabel('Tips')
plt.tight_layout()
plt.show()

plt.figure(figsize=(8, 6))
plt.scatter(df_plotting_df['trip_miles'], df_plotting_df['tips'], alpha=0.5, color='red')
plt.title('Relationship Between Trip Miles and Tips')
plt.xlabel('Trip Miles')
plt.ylabel('Tips')
plt.tight_layout()
plt.show()

plt.figure(figsize=(8,6))
plt.scatter(df_plotting_df['driver_pay'],df_plotting_df['tips'], alpha=0.5, color='red')
plt.title('Relationship between Base Pay and Tips')
plt.xlabel('Base Pay')
plt.ylabel('Tips')
plt.tight_layout()
plt.show()