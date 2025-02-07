import polars as pl

print('Reading CSV')
df = pl.read_csv('../../../../../Downloads/2022_place_canvas_history.csv', columns=['user_id', 'timestamp']).filter(pl.col("timestamp") <= pl.lit("2022-04-03 06:00:00"))

print("Counting unique user IDs")
result = df.select(pl.n_unique("user_id"))
print(result)

print("Manipulating user id")
df = df.with_columns(
    pl.col("user_id").hash())

result = df.select(pl.n_unique("user_id"))
print(result)