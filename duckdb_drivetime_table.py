import duckdb
from pathlib import Path

# === Paths ===
PARQUET_PATH = Path("utah_final_50m_v2.parquet")   # your parquet file
DUCKDB_PATH = Path("Data/utah_drivetimes.duckdb")     # output database file

# === Step 1: Connect (will create file if not exist) ===
con = duckdb.connect(str(DUCKDB_PATH))

# === Step 2: Create a persistent table from the parquet file ===
# This copies the data physically into DuckDB (so you can delete the parquet later if you wish)
con.execute(f"""
    CREATE OR REPLACE TABLE utah_drivetimes AS
    SELECT * FROM read_parquet('{PARQUET_PATH.as_posix()}');
""")

# === Step 3: Optional — create an index for faster queries ===
con.execute("CREATE INDEX IF NOT EXISTS idx_source ON utah_drivetimes(source_h3);")
con.execute("CREATE INDEX IF NOT EXISTS idx_dest ON utah_drivetimes(dest_h3);")

# === Step 4: Optional — quick verification ===
print("\nTable successfully created!")
print(con.execute("SELECT COUNT(*) AS n_rows FROM utah_drivetimes").df())
print(con.execute("SELECT * FROM utah_drivetimes LIMIT 100").df())

# === Step 5: Close connection ===
con.close()
