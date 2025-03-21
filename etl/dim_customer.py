import duckdb
import pandas as pd
from datetime import datetime

def etl_dim_customer(df, con):
    print("Memproses Dim_Customer dengan SCD Type 1, 2, dan 3...")

    # Ambil data unik customer berdasarkan AccountID
    dim_customer_df = df[["AccountID", "CustomerAge", "CustomerOccupation", "AccountBalance"]].drop_duplicates()

    # Validasi CustomerAge, jika NAN akan diisi default 30 (SCD Type 1)
    dim_customer_df["CustomerAge"] = pd.to_numeric(dim_customer_df["CustomerAge"], errors="coerce").fillna(30)

    # Validasi CustomerOccupation, jika kosong akan diisi default "Unknown" (SCD Type 3)
    dim_customer_df["CustomerOccupation"] = dim_customer_df["CustomerOccupation"].fillna("Unknown")

    # Validasi AccountBalance, jika kosong akan diisi default 0 (SCD Type 2)
    dim_customer_df["AccountBalance"] = pd.to_numeric(dim_customer_df["AccountBalance"], errors="coerce").fillna(0)

    # Insert atau update dengan SCD Type 1, 2, dan 3
    for _, row in dim_customer_df.iterrows():
        account_id = row.AccountID
        customer_age = row.CustomerAge
        customer_occupation = row.CustomerOccupation
        account_balance = row.AccountBalance

        # Cek apakah customer sudah ada di Dim_Customer
        existing_customer = con.execute(f"""
            SELECT CustomerKey, CustomerAge, CustomerOccupation, Previous_CustomerOccupation, 
                   AccountBalance, EffectiveDate, IsCurrent 
            FROM Dim_Customer 
            WHERE AccountID = '{account_id}' AND IsCurrent = TRUE
        """).fetchone()

        if existing_customer:
            customer_key, old_age, old_occupation, prev_occupation, old_balance, eff_date, is_current = existing_customer

            # ✅ **SCD Type 1: Update CustomerAge langsung**
            con.execute(f"""
                UPDATE Dim_Customer 
                SET CustomerAge = {customer_age} 
                WHERE CustomerKey = {customer_key}
            """)

            # ✅ **SCD Type 3: Update CustomerOccupation dengan menyimpan histori**
            if old_occupation != customer_occupation:
                con.execute(f"""
                    UPDATE Dim_Customer 
                    SET Previous_CustomerOccupation = '{old_occupation}', 
                        CustomerOccupation = '{customer_occupation}'
                    WHERE CustomerKey = {customer_key}
                """)

            # ✅ **SCD Type 2: Jika AccountBalance berubah, simpan histori sebagai baris baru**
            if old_balance != account_balance:
                # Update EndDate pada data lama
                con.execute(f"""
                    UPDATE Dim_Customer 
                    SET EndDate = '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', IsCurrent = FALSE
                    WHERE CustomerKey = {customer_key}
                """)

                # Dapatkan CustomerKey baru
                last_key = con.execute("SELECT COALESCE(MAX(CustomerKey), 0) FROM Dim_Customer").fetchone()[0]
                new_customer_key = last_key + 1

                # Insert baris baru dengan saldo terbaru
                con.execute(f"""
                    INSERT INTO Dim_Customer (CustomerKey, AccountID, CustomerAge, CustomerOccupation, Previous_CustomerOccupation,
                                              AccountBalance, EffectiveDate, EndDate, IsCurrent)
                    VALUES ({new_customer_key}, '{account_id}', {customer_age}, '{customer_occupation}', '{old_occupation}',
                            {account_balance}, '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', NULL, TRUE)
                """)

        else:
            # Dapatkan CustomerKey baru
            last_key = con.execute("SELECT COALESCE(MAX(CustomerKey), 0) FROM Dim_Customer").fetchone()[0]
            new_customer_key = last_key + 1

            # Insert data baru jika customer belum ada
            con.execute(f"""
                INSERT INTO Dim_Customer (CustomerKey, AccountID, CustomerAge, CustomerOccupation, Previous_CustomerOccupation,
                                          AccountBalance, EffectiveDate, EndDate, IsCurrent)
                VALUES ({new_customer_key}, '{account_id}', {customer_age}, '{customer_occupation}', NULL,
                        {account_balance}, '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', NULL, TRUE)
            """)

    df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Customer").fetchone()[0]
    print(f"✅ Dim_Customer berhasil diproses! Jumlah baris di Dim_Customer: {df_count}")