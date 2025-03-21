import duckdb
import pandas as pd

def etl_dim_time(df, con):
    print("Memproses Dim_Time...")
    
    # Ambil data waktu unik
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])
    dim_time_df = df[["TransactionDate"]].drop_duplicates()
    
    # Buat kolom tambahan untuk analisis waktu
    dim_time_df["TimeID"] = range(1, len(dim_time_df) + 1)
    dim_time_df["Year"] = dim_time_df["TransactionDate"].dt.year
    dim_time_df["Month"] = dim_time_df["TransactionDate"].dt.month
    dim_time_df["Day"] = dim_time_df["TransactionDate"].dt.day
    dim_time_df["Weekday"] = dim_time_df["TransactionDate"].dt.day_name()
    
    # Insert ke DuckDB
    for _, row in dim_time_df.iterrows():
        con.execute(f"""
            INSERT INTO Dim_Time (TimeID, TransactionDate, Year, Month, Day, Weekday)
            VALUES ({row.TimeID}, '{row.TransactionDate}', {row.Year}, {row.Month}, {row.Day}, '{row.Weekday}')
            ON CONFLICT (TimeID) DO NOTHING;
        """)
    
    df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Time").fetchone()[0]
    print(f"✅ Dim_Time berhasil diproses! Jumlah baris di Dim_Time: {df_count}")

