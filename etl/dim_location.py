import duckdb
import pandas as pd

def etl_dim_location(df, con):
    print("\t\tETL Dim_Location")

    try:
        # Ambil data lokasi unik
        dim_location_df = df[["Location"]].drop_duplicates()

        # Menambahkan ID lokasi secara manual
        dim_location_df["LocationID"] = range(1, len(dim_location_df) + 1)
        print("\t\t✅ Transform LocationID berhasil!")

        # Insert ke DuckDB
        for _, row in dim_location_df.iterrows():
            con.execute(f"""
                INSERT INTO Dim_Location (LocationID, Location)
                VALUES ({row.LocationID}, '{row.Location}')
                ON CONFLICT (LocationID) DO NOTHING;
            """)

        df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Location").fetchone()[0]
        print(f"\t\t✅ Dim_Location berhasil diproses! Jumlah baris di Dim_Location: {df_count}")
        
    except duckdb.Error as e:
        raise ValueError(f"{e}")

    except Exception as e:
        raise ValueError(f"{e}")

    finally:
        print("\t\tProses Dim_Location Selesai!")
        print("\n")