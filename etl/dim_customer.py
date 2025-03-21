import duckdb
import pandas as pd

def etl_dim_customer(df, con):
  print("Memproses Dim_Customer...")

  # Ambil data unik customer berdasarkan AccountID
  dim_customer_df = df[["AccountID"]].drop_duplicates()

  # Add data CustomerAge dengan di validasi, jika NAN akan di isi default 30
  dim_customer_df["CustomerAge"] = pd.to_numeric(df["CustomerAge"], errors="coerce")
  dim_customer_df["CustomerAge"].fillna(30) 

  # Add data CustomerOccupation dengan di validasi, jika kosong akan di isi default Unknown
  dim_customer_df["CustomerOccupation"] = df["CustomerOccupation"]
  dim_customer_df["CustomerOccupation"].fillna("Unknown")
  # print(dim_customer_df.columns)

  # Insert ke DuckDB
  for _, row in dim_customer_df.iterrows():
        lastKey = con.execute("SELECT COALESCE(MAX(CustomerKey), 0) FROM Dim_Customer").fetchone()[0]
        CustomerKey = lastKey + 1

        con.execute(f"""
            INSERT INTO Dim_Customer (CustomerKey, CustomerID, CustomerAge, CustomerOccupation)
            VALUES ('{CustomerKey}', '{row.AccountID}', {row.CustomerAge}, '{row.CustomerOccupation}')
            ON CONFLICT (CustomerKey) DO NOTHING;
        """)

  df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Customer").fetchone()[0]
  print(f"✅ Dim_Customer berhasil diproses! Jumlah baris di Dim_Customer: {df_count}")
