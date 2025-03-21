import duckdb
import pandas as pd

def etl_dim_channel(df, con):
    print("\t\tETL Dim_Channel")

    try:
        # Ambil data unik Channel
        dim_channel_df = df[["Channel"]].drop_duplicates()

        # Tambahkan nama channel jika tidak ada di dataset
        id_mapping = {"ATM": 1, "Online": 2, "Branch": 3}
        dim_channel_df["ChannelID"] = df["Channel"].map(id_mapping)
        dim_channel_df["ChannelName"] = df["Channel"].fillna("Unknown")
        dim_channel_df = dim_channel_df.drop(columns=["Channel"])
        print("\t\t✅ Transform ChannelID berhasil!")

        # Insert ke DuckDB
        for _, row in dim_channel_df.iterrows():
            con.execute(f"""
                INSERT INTO Dim_Channel (ChannelID, ChannelName)
                VALUES ({row.ChannelID}, '{row.ChannelName}')
                ON CONFLICT (ChannelID) DO NOTHING;
            """)

        df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Channel").fetchone()[0]
        print(f"\t\t✅ Dim_Channel berhasil diproses! Jumlah baris di Dim_Channel: {df_count}")
     
    except duckdb.Error as e:
        raise ValueError(f"{e}")

    except Exception as e:
        raise ValueError(f"{e}")

    finally:
        print("\t\tProses Dim_Channel Selesai!")
        print("\n")
