import duckdb
import pandas as pd
from datetime import datetime
from decimal import Decimal
import time

def etl_dim_customer(df, con):
    time.sleep(10)
    print("\t\tETL Dim_Customer")

    thereiIsNewDataAdded = False

    try:
        # Ambil data unik customer berdasarkan AccountID
        # dim_customer_df = df[["AccountID", "CustomerAge", "CustomerOccupation", "AccountBalance"]].drop_duplicates()
        dim_customer_df = df.sort_values("TransactionDate").drop_duplicates(
            subset=["AccountID", "CustomerAge", "CustomerOccupation", "AccountBalance"]
        )

        # Validasi data
        dim_customer_df["CustomerAge"] = pd.to_numeric(dim_customer_df["CustomerAge"], errors="coerce").fillna(30).astype(int)
        dim_customer_df["CustomerOccupation"] = dim_customer_df["CustomerOccupation"].fillna("Unknown")
        dim_customer_df["AccountBalance"] = pd.to_numeric(dim_customer_df["AccountBalance"], errors="coerce").fillna(0)

        # Iterasi setiap customer
        for _, row in dim_customer_df.iterrows():
            account_id = row.AccountID
            customer_age = row.CustomerAge
            customer_occupation = row.CustomerOccupation
            account_balance = row.AccountBalance

            # Cek apakah customer sudah ada di Dim_Customer
            # berdasarkan on customerID
            existing_customer = con.execute("""
                SELECT CustomerKey, CustomerID, CustomerAge, CustomerOccupation, PreviousCustomerOccupation, 
                        AccountBalance, EffectiveDate, IsCurrent
                FROM Dim_Customer 
                WHERE CustomerID = ?
                ORDER BY CustomerKey DESC
            """, (account_id,)).fetchone()

            # jika data customer sudah ada
            if existing_customer:
                # existing_customer_key, existing_occupation = existing_customer
                existing_customer_key, existing_customer_id, existing_customer_age, existing_customer_occupation, existing_prev_occupation, existing_customer_balance, existing_customer_eff_date, existing_customer_is_current = existing_customer
                
                # Cek apakah ada data customer dengan data yang benar-benar sama di Dim_Customer
                # based on CustomerID, CustomerAge, CustomerOccupation, AccountBalance
                existing_samedata_customer = con.execute("""
                    SELECT CustomerKey, CustomerID, CustomerAge, CustomerOccupation, PreviousCustomerOccupation, 
                        AccountBalance, EffectiveDate, IsCurrent 
                    FROM Dim_Customer 
                    WHERE CustomerID = ? AND CustomerAge = ? AND CustomerOccupation = ? AND AccountBalance = ?
                    ORDER BY CustomerKey DESC
                """, (account_id, customer_age, customer_occupation, account_balance)).fetchone()

                # jika tidak ada, maka update data yang lama ke IsCurrent = false
                if not existing_samedata_customer:
                    # Update CustomerAge Jika Ada SCD Type 1
                    con.execute("""
                        UPDATE Dim_Customer 
                        SET CustomerAge = ?
                        WHERE CustomerKey = ?
                    """, (existing_customer_age, existing_customer_key))

                    # **SCD Type 2: Jika AccountBalance berubah, simpan histori sebagai baris baru**
                    if round(existing_customer_balance) != round(account_balance):
                        # Update EndDate pada data lama
                        con.execute("""
                            UPDATE Dim_Customer 
                            SET EndDate = ?, IsCurrent = FALSE
                            WHERE CustomerKey = ?
                        """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), existing_customer_key))

                        # Dapatkan CustomerKey baru
                        last_key = con.execute("SELECT COALESCE(MAX(CustomerKey), 0) FROM Dim_Customer").fetchone()[0]
                        new_customer_key = last_key + 1

                        # Insert baris baru dengan saldo terbaru
                        con.execute("""
                            INSERT INTO Dim_Customer (CustomerKey, CustomerID, CustomerAge, CustomerOccupation, PreviousCustomerOccupation,
                                                    AccountBalance, EffectiveDate, EndDate, IsCurrent)
                            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, TRUE)
                        """, (new_customer_key, account_id, customer_age, customer_occupation, existing_customer_occupation,
                            account_balance, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                        
                        # print("ADDEDD on round(existing_customer_balance) != round(account_balance)")
                        thereiIsNewDataAdded = True
            else:
                # jika data CustomerID belum ada di table, maka insert data baru
                # Dapatkan CustomerKey baru
                last_key = con.execute("SELECT COALESCE(MAX(CustomerKey), 0) FROM Dim_Customer").fetchone()[0]
                new_customer_key = last_key + 1

                # Insert data baru jika customer belum ada
                con.execute("""
                    INSERT INTO Dim_Customer (CustomerKey, CustomerID, CustomerAge, CustomerOccupation, PreviousCustomerOccupation,
                                            AccountBalance, EffectiveDate, EndDate, IsCurrent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, NULL, TRUE)
                """, (new_customer_key, account_id, customer_age, customer_occupation, None,
                    account_balance, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                
                # print("ADDEDD on NON existing_customer")
                thereiIsNewDataAdded = True

    except duckdb.Error as e:
        raise ValueError(f"{e}")

    except Exception as e:
        raise ValueError(f"{e}")

    finally:
        if thereiIsNewDataAdded == True:
            df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Customer").fetchone()[0]
            print(f"\t\t✅ Dim_Customer berhasil diproses! Jumlah baris di Dim_Customer: {df_count}")
        else:
            print("\t\tℹ️  Tidak ada baru yang perlu dimuat.")

        print("\t\tProses Dim_Customer Selesai!")
        print("\n")