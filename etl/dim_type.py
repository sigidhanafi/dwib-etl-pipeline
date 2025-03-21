import duckdb
import pandas as pd

def etl_dim_type(df, con):
    print("Memproses Dim_Type...")
    
    # Ambil data unik Type
    dim_type_df = df[["TransactionType"]].drop_duplicates().reset_index(drop=True)
    dim_type_df["TypeID"] = range(1, len(dim_type_df) + 1)
    
    # Insert ke DuckDB
    for _, row in dim_type_df.iterrows():
        con.execute(f"""
            INSERT INTO Dim_Type (TypeID, TransactionType)
            VALUES ({row.TypeID}, '{row.TransactionType}')
            ON CONFLICT (TypeID) DO NOTHING;
        """)
    
    df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Type").fetchone()[0]
    print(f"✅ Dim_Type berhasil diproses! Jumlah baris di Dim_Type: {df_count}")
