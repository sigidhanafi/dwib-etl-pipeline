import duckdb
import pandas as pd

def etl_dim_device(df, con):
  # ETL UNTUK DIM_DEVICE
  print("Memproses Dim_Device...")

  # Ambil data unik DeviceID dan IP Address
  dim_device_df = df[["DeviceID"]].drop_duplicates()

  # Tambahkan IP Address default jika tidak ada
  dim_device_df["IPAddress"] = "0.0.0.0" 

  # Insert
  for _, row in dim_device_df.iterrows():
      con.execute(f"""
          INSERT INTO Dim_Device (DeviceID, IPAddress)
          VALUES ('{row.DeviceID}', '{row.IPAddress}')
          ON CONFLICT (DeviceID) DO NOTHING;
      """)

  df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Device").fetchone()[0]
  print(f"✅ Dim_Device berhasil diproses! Jumlah baris di Dim_Device: {df_count}")