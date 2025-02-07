import polars as pl

print('Scanning CSV')
df = pl.scan_csv('../../../../../Downloads/2022_place_canvas_history.csv')

print("Manipulating user id")
df = df.with_columns(
    pl.col("user_id").hash())

print("Converting to parquet")
df.sink_parquet('output.parquet')