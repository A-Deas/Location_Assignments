import polars as pl
pl.Config.set_tbl_rows(-1)   # show all rows
pl.Config.set_tbl_cols(-1)   # show all columns

# df = pl.read_parquet("../Location_Assignments/Synthetic Population/acts_cohort_1__1_fmt.parquet")
# print(df.head(1000))

df = pl.read_parquet("../Wildfire_Smoke/utah_final_50m_v2.parquet")
print(df.head(100))

# Count number of rows
print(f"Number of rows: {df.height:,}")

# Example: count unique entries in specific columns
cols = ["source_h3", "dest_h3"]

unique_counts = df.select([pl.col(c).n_unique().alias(c) for c in cols])
print(unique_counts)