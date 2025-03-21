import duckdb
import pandas as pd

def etl_dim_channel(df, con):
  # ETL UNTUK DIM_CHANNEL
  print("Memproses Dim_Channel...")

  # Ambil data unik Channel
  dim_channel_df = df[["Channel"]].drop_duplicates()

  # Tambahkan nama channel jika tidak ada di CSV
  id_mapping = {"ATM": 1, "Online": 2, "Branch": 3}
  dim_channel_df["ChannelID"] = df["Channel"].map(id_mapping)
  dim_channel_df["ChannelName"] = df["Channel"].fillna("Unknown")
  dim_channel_df = dim_channel_df.drop(columns=["Channel"])

  # Insert ke DuckDB
  for _, row in dim_channel_df.iterrows():
      con.execute(f"""
          INSERT INTO Dim_Channel (ChannelID, ChannelName)
          VALUES ({row.ChannelID}, '{row.ChannelName}')
          ON CONFLICT (ChannelID) DO NOTHING;
      """)

  df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Channel").fetchone()[0]
  print(f"✅ Dim_Channel berhasil diproses! Jumlah baris di Dim_Channel: {df_count}")
