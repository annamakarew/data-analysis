from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import FloatType
import math
import polars as pl
import pyarrow as pa

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("../Week4/output.parquet")

df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss.SSS 'UTC'"))

# See if any official Cal Poly colors are in the data
filter_list = ["#154734", "#BD8B13", "#3A913F", "#A4D65E", "#F2C75C", "#F8E08E", "#5CB8B2", "#B5E3D8", "#ABCAE9", "#D5E4F4", 
               "#CAC7A7", "#E4E3D3", "#B7CDC2", "#789F90", "#54585A", "#8E9089",
               "#D0DF00", "#FF6A39"]
df_filtered = df.filter(col("pixel_color").isin(filter_list))

df_filtered.show()

# Let's find the closest green color to Cal Poly green
def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')  # Remove '#' if present
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

# Example usage:
cal_poly_hex = "#154734"  # Cal Poly green in hex
cal_poly_rgb_color = hex_to_rgb(cal_poly_hex)

# Define color distance function with green bias, and check for significantly higher green component
def refined_color_distance(hex_color, target_hex, min_green_value=50, green_threshold=6):
    r1, g1, b1 = hex_to_rgb(hex_color)
    r2, g2, b2 = hex_to_rgb(target_hex)

    # Ensure that green is significantly higher than red and blue
    if g1 < min_green_value or (g1 <= r1 + green_threshold) or (g1 <= b1 + green_threshold):
        return float('inf')  # Assign a very high distance to non-green pixels

    # Calculate Euclidean distance for color matching
    return math.sqrt((r1 - r2)**2 + (g1 - g2)**2 + (b1 - b2)**2)


# Create UDF for refined color distance
refined_color_distance_udf = udf(lambda hex_color: refined_color_distance(hex_color, cal_poly_hex), FloatType())

# Apply the UDF to the DataFrame
df_with_refined_distance = df.withColumn("color_distance", refined_color_distance_udf("pixel_color"))

# Sort by color distance to find the closest pixels
df_sorted = df_with_refined_distance.orderBy("color_distance")

# Show the closest green pixels
df_sorted.show(20)

df_grouped = df_sorted.groupBy("pixel_color").agg(
    F.count("*").alias("color_count"),  # Count of occurrences for each color
    F.min("color_distance").alias("min_color_distance")  # Get the minimum color_distance for each color
)

# Sort by color_distance and color_count
df_sorted = df_grouped.orderBy(
    ["min_color_distance", "color_count"], ascending=[True, False]
)

# Show the sorted DataFrame
df_sorted.show()

spark.stop()