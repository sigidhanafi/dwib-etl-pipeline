import duckdb
import pandas as pd

def etl_dim_time(df, con):
    print("\t\tETL Dim_Time")

    try:
        # Ambil tanggal transaksi unik
        dim_time_df = df[["TransactionDate"]].drop_duplicates().reset_index(drop=True)
        # Konversi ke format datetime
        dim_time_df["TransactionDate"] = pd.to_datetime(dim_time_df["TransactionDate"])
        # Buat kolom TimeID dalam format YYMMDD
        dim_time_df["TimeID"] = dim_time_df["TransactionDate"].dt.strftime('%y%m%d').astype(int)

        # hanya proses data baru 
        existing_ids_df = con.execute("SELECT TimeID FROM Dim_Time").fetchdf()
        existing_ids = set(existing_ids_df["TimeID"])
        new_data = df[~dim_time_df["TimeID"].isin(existing_ids)]

        if new_data.empty:
            print("\t\tℹ️  Tidak ada baru yang perlu dimuat.")
            return True

        # Tambahkan informasi waktu lainnya
        dim_time_df["Day"] = dim_time_df["TransactionDate"].dt.day
        dim_time_df["Week"] = dim_time_df["TransactionDate"].dt.isocalendar().week
        dim_time_df["Quartile"] = dim_time_df["TransactionDate"].dt.quarter
        dim_time_df["Month"] = dim_time_df["TransactionDate"].dt.month
        dim_time_df["Year"] = dim_time_df["TransactionDate"].dt.year

        # Hapus kolom yang tidak dibutuhkan
        dim_time_df = dim_time_df.drop(columns=["TransactionDate"])

        print("\t\t✅ Transform date format berhasil!")

        # Insert ke DuckDB
        con.executemany(
            """
            INSERT INTO Dim_Time (TimeID, Day, Week, Quartile, Month, Year)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (TimeID) DO NOTHING;
            """,
            dim_time_df.values.tolist()
        )

        df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Time").fetchone()[0]
        print(f"\t\t✅ Dim_Time berhasil diproses! Jumlah baris di Dim_Time: {df_count}")
    except duckdb.Error as e:
        raise ValueError(f"{e}")

    except Exception as e:
        raise ValueError(f"{e}")

    finally:
        print("\t\tProses Dim_Time Selesai!")
        print("\n")

