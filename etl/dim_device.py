import duckdb
import pandas as pd

def etl_dim_device(df, con):
    print("\t\tETL Dim_Device")

    try:
        # hanya proses data baru 
        existing_ids_df = con.execute("SELECT DeviceID FROM Dim_Device").fetchdf()
        existing_ids = set(existing_ids_df["DeviceID"])
        new_data = df[~df["DeviceID"].isin(existing_ids)]

        if new_data.empty:
            print("\t\tℹ️  Tidak ada baru yang perlu dimuat.")
            return True
        
        # Ambil data unik DeviceID dan IP Address
        dim_device_df = df[["DeviceID"]].drop_duplicates()

        # Tambahkan IP Address default jika tidak ada
        dim_device_df["IPAddress"] = "0.0.0.0" 

        # Insert ke DuckDB
        for _, row in dim_device_df.iterrows():
            con.execute(f"""
                INSERT INTO Dim_Device (DeviceID, IPAddress)
                VALUES ('{row.DeviceID}', '{row.IPAddress}')
                ON CONFLICT (DeviceID) DO NOTHING;
            """)

        df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Device").fetchone()[0]
        print(f"\t\t✅ Dim_Device berhasil diproses! Jumlah baris di Dim_Device: {df_count}")

    except duckdb.Error as e:
        raise ValueError(f"{e}")

    except Exception as e:
        raise ValueError(f"{e}")

    finally:
        print("\t\tProses Dim_Device Selesai!")
        print("\n")