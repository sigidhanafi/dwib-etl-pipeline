import duckdb
import pandas as pd

def etl_fact_transaction(df, con):
    print("\t\tETL Fact_Transactions")

    try:
        # Transform: Konversi tanggal transaksi
        df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])
        df["TimeID"] = df["TransactionDate"].dt.strftime("%Y%m%d").astype(int)
        print("\t\t✅ Mapping TimeID berhasil!")

        # Ambil data dari Dim_Location (Mapping LocationID)
        location_mapping = con.execute("SELECT Location, LocationID FROM Dim_Location").fetchdf()
        df = df.merge(location_mapping, on="Location", how="left")
        print("\t\t✅ Mapping Location berhasil!")

        # Ambil data dari Dim_Device (Mapping DeviceID)
        device_mapping = con.execute("SELECT DeviceID FROM Dim_Device").fetchdf()
        df = df.merge(device_mapping, on="DeviceID", how="left")
        print("\t\t✅ Mapping DeviceID berhasil!")

        # Ambil data dari Dim_Channel (Mapping ChannelID)
        channel_mapping = con.execute("SELECT ChannelID, ChannelName FROM Dim_Channel").fetchdf()
        df = df.merge(channel_mapping, left_on="Channel", right_on="ChannelName", how="left")
        print("\t\t✅ Mapping ChannleID berhasil!")

        type_mapping = con.execute("SELECT TransactionTypeID, TransactionType FROM Dim_Transaction_Type").fetchdf()
        df = df.merge(type_mapping, on="TransactionType", how="left")
        print("\t\t✅ Mapping TransactionTypeID berhasil!")

        # Insert data ke Fact_Transaction
        for _, row in df.iterrows():
            con.execute(f"""
                INSERT INTO Fact_Transaction (
                    TransactionID, CustomerID, DeviceID, TransactionAmount, 
                    TransactionTypeID, TimeID, LocationID, MerchantID, ChannelID, TransactionDuration, 
                    LoginAttempts, AccountBalance, PreviousTransactionDate
                ) VALUES (
                    '{row.TransactionID}', '{row.AccountID}', '{row.DeviceID}', 
                    {row.TransactionAmount}, {row.TransactionTypeID}, {row.TimeID}, {row.LocationID}, 
                    '{row.MerchantID}', '{row.ChannelID}', {row.TransactionDuration}, {row.LoginAttempts}, 
                    {row.AccountBalance}, '{row.PreviousTransactionDate}'
                )
                ON CONFLICT (TransactionID) DO NOTHING;
            """)

        
        df_count = con.execute("SELECT COUNT(*) AS row_count FROM Fact_Transaction").fetchone()[0]
        print(f"\t\t✅ Fact_Transaction berhasil diproses! Jumlah baris di Fact_Transaction: {df_count}")
    except duckdb.Error as e:
        raise ValueError(f"{e}")

    except Exception as e:
        raise ValueError(f"{e}")

    finally:
        print("\t\tProses Fact_Transaction Selesai!")
        print("\n")
