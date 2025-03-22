import duckdb
from pathlib import Path
import pandas as pd

db_dir = Path("db/dwh-perbankan.duckdb")
# print(db_dir)
con = duckdb.connect(db_dir)

# # Set agar semua kolom ditampilkan
# pd.set_option('display.max_columns', None)

# # (Opsional) Set agar semua baris ditampilkan juga
# pd.set_option('display.max_rows', None)

# # (Opsional) Set lebar kolom agar tidak dipotong
# pd.set_option('display.max_colwidth', None)  # atau gunakan pd.set_option('display.max_colwidth', -1) di versi lama

result = con.execute(
    "SELECT * FROM fact_transaction"
).fetchdf()

# Tampilkan hasil
print(result.head(10))
