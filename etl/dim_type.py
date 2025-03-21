import duckdb
import pandas as pd

def etl_dim_type(df, con):
    print("\t\tETL Dim_Transaction_Type")
    try:
        # hanya proses data baru 
        existing_ids_df = con.execute("SELECT TransactionType FROM Dim_Transaction_Type").fetchdf()
        existing_ids = set(existing_ids_df["TransactionType"])
        new_data = df[~df["TransactionType"].isin(existing_ids)]

        if new_data.empty:
            print("\t\tℹ️  Tidak ada baru yang perlu dimuat.")
            return True
        
        # Ambil data unik TransactionType
        dim_type_df = df[["TransactionType"]].drop_duplicates()

        # Tambahkan nama TransactionTypeID jika tidak ada di CSV
        id_mapping = {"Debit": 1, "Credit": 2}
        dim_type_df["TransactionTypeID"] = df["TransactionType"].map(id_mapping)

        # Insert ke DuckDB
        for _, row in dim_type_df.iterrows():
            con.execute(f"""
                INSERT INTO Dim_Transaction_Type (TransactionTypeID, TransactionType)
                VALUES ({row.TransactionTypeID}, '{row.TransactionType}')
                ON CONFLICT (TransactionTypeID) DO NOTHING;
            """)

        df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Transaction_Type").fetchone()[0]
        print(f"\t\t✅ Dim_Transaction_Type berhasil diproses! Jumlah baris di Dim_Transaction_Type: {df_count}")

    except duckdb.Error as e:
        raise ValueError(f"{e}")

    except Exception as e:
        raise ValueError(f"{e}")

    finally:
        print("\t\tProses Dim_Time Selesai!")
        print("\n")