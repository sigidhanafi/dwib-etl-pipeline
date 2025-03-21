import duckdb
import pandas as pd

def etl_dim_type(df, con):
    print("Memproses Dim_Transaction_Type...")

    # Ambil data unik TransactionType
    dim_type_df = df[["TransactionType"]].drop_duplicates().reset_index(drop=True)
    dim_type_df["TransactionTypeID"] = range(1, len(dim_type_df) + 1)

    # Insert ke DuckDB
    con.executemany(
        """
        INSERT INTO Dim_Transaction_Type (TransactionTypeID, TransactionType)
        VALUES (?, ?)
        ON CONFLICT (TransactionTypeID) DO NOTHING;
        """,
        dim_type_df.values.tolist()
    )

    df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Transaction_Type").fetchone()[0]
    print(f"✅ Dim_Transaction_Type berhasil diproses! Jumlah baris di Dim_Transaction_Type: {df_count}")
