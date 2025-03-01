import polars as pl
import matplotlib.pyplot as plt

# Read the Parquet files from the directory using Dask
df = pl.scan_parquet("../../../../../Downloads/archive/parquet/*.parquet").collect(streaming=True)

# Show the resulting DataFrame
print(df)

# Check for null values in specific columns
null_base = df['base_passenger_fare'].is_null()
null_tips = df['tips'].is_null()
null_pay = df['driver_pay'].is_null()
null_pickup = df['pickup_datetime'].is_null()
null_dropoff = df['dropoff_datetime'].is_null()

print(null_base.sum())
print(null_tips.sum())
print(null_pay.sum())
print(null_pickup.sum())
print(null_dropoff.sum())

# Calculate trip duration in seconds
df = df.with_columns(
    ((pl.col('dropoff_datetime') - pl.col('pickup_datetime'))/ 1e9).cast(pl.Int64).alias('calc_duration')
)

# Create the 'duration_equals_trip_time' column, set to False if difference > 1 second, else True
df = df.with_columns(
    ((pl.col('calc_duration') - pl.col('trip_time')).abs() <= 1).alias('duration_equals_trip_time')
)

# Reduce precision to save memory
for col in df.columns:
    if df[col].dtype == pl.Float64:
        print(f"casting {col}")
        df = df.with_columns(
            pl.col(col).cast(pl.Float32)
        )

df_filtered = df.filter(pl.col('duration_equals_trip_time') == False).select(['pickup_datetime', 'dropoff_datetime', 'trip_time', 'calc_duration'])

# Display the result
print(df_filtered)

df_filtered = df.drop(['trip_time'])
'''
# Downsampling to reduce the number of points being plotted
downsample_rate = 1000  # Adjust the rate depending on the desired resolution
df_sampled = df_filtered[::downsample_rate]  # Sample every nth row (adjust n as needed)

# Plot the relationship between base_passenger_fare, tips, and driver_pay
plt.figure(figsize=(10, 6))

# Plot tips vs. driver_pay
plt.scatter(df_sampled['tips'], df_sampled['driver_pay'], label='Tips vs Driver Pay', color='green', alpha=0.7)

# Add labels and title
plt.title('Visualizing Wages: Tips and Driver Pay', fontsize=14)
plt.xlabel('Tip Amount ($)', fontsize=12)
plt.ylabel('Driver Pay ($)', fontsize=12)

# Add a legend
plt.legend()

# Show the plot
plt.show()
plt.close()

'''
'''
# Tips distribution
plt.figure(figsize=(10,6))
plt.hist(df['tips'], bins='auto', color='blue', alpha=0.7)
plt.xlim(0, 25)
plt.title('Distribution of Tips')
plt.xlabel('Tips')
plt.ylabel('Frequency')
plt.show()

# Check for outliers in the pay columns
print(df_filtered[['base_passenger_fare', 'tips', 'driver_pay']].describe())
'''

df_lazy = df.lazy()

# Remove rows where base_passenger_fare or driver_pay are negative
# First, apply a filter on base_passenger_fare
df_filtered = df_lazy.filter(pl.col('base_passenger_fare') > 0).filter(pl.col('driver_pay') > 0)

print(df_filtered.select('base_passenger_fare', 'tips', 'driver_pay').collect().describe())