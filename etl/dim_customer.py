import duckdb
from datetime import datetime

# Buat koneksi DuckDB dalam memori
con = duckdb.connect(database=':memory:')

# Buat tabel Dim_Customer jika belum ada
con.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Customer (
        CustomerKey INTEGER PRIMARY KEY,
        AccountID TEXT,
        CustomerAge INTEGER,
        CustomerOccupation TEXT,
        Previous_CustomerOccupation TEXT,
        AccountBalance FLOAT,
        EffectiveDate TIMESTAMP,
        EndDate TIMESTAMP NULL,
        IsCurrent BOOLEAN
    )
""")

def etl_dim_customer(df, con):
    print("Memproses Dim_Customer dengan SCD Type 1, 2, dan 3...")

    # Ambil data unik customer berdasarkan AccountID
    dim_customer_df = df[["AccountID", "CustomerAge", "CustomerOccupation", "AccountBalance"]].drop_duplicates()

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
        existing_customer = con.execute("""
            SELECT CustomerKey, CustomerAge, CustomerOccupation, Previous_CustomerOccupation, 
                   AccountBalance, EffectiveDate, IsCurrent 
            FROM Dim_Customer 
            WHERE AccountID = ? AND IsCurrent = TRUE
        """, (account_id,)).fetchone()

        if existing_customer:
            customer_key, old_age, old_occupation, prev_occupation, old_balance, eff_date, is_current = existing_customer

            # ✅ **SCD Type 1: Update CustomerAge langsung**
            con.execute("""
                UPDATE Dim_Customer 
                SET CustomerAge = ? 
                WHERE CustomerKey = ?
            """, (customer_age, customer_key))

            # ✅ **SCD Type 3: Update CustomerOccupation dengan menyimpan histori**
            if old_occupation != customer_occupation:
                con.execute("""
                    UPDATE Dim_Customer 
                    SET Previous_CustomerOccupation = ?, 
                        CustomerOccupation = ?
                    WHERE CustomerKey = ?
                """, (old_occupation, customer_occupation, customer_key))

            # ✅ **SCD Type 2: Jika AccountBalance berubah, simpan histori sebagai baris baru**
            if old_balance != account_balance:
                # Update EndDate pada data lama
                con.execute("""
                    UPDATE Dim_Customer 
                    SET EndDate = ?, IsCurrent = FALSE
                    WHERE CustomerKey = ? AND IsCurrent = TRUE
                """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), customer_key))

                # Dapatkan CustomerKey baru
                last_key = con.execute("SELECT COALESCE(MAX(CustomerKey), 0) FROM Dim_Customer").fetchone()[0]
                new_customer_key = last_key + 1

                # Insert baris baru dengan saldo terbaru
                con.execute("""
                    INSERT INTO Dim_Customer (CustomerKey, AccountID, CustomerAge, CustomerOccupation, Previous_CustomerOccupation,
                                              AccountBalance, EffectiveDate, EndDate, IsCurrent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, NULL, TRUE)
                """, (new_customer_key, account_id, customer_age, customer_occupation, old_occupation,
                      account_balance, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        else:
            # Dapatkan CustomerKey baru
            last_key = con.execute("SELECT COALESCE(MAX(CustomerKey), 0) FROM Dim_Customer").fetchone()[0]
            new_customer_key = last_key + 1

            # Insert data baru jika customer belum ada
            con.execute("""
                INSERT INTO Dim_Customer (CustomerKey, AccountID, CustomerAge, CustomerOccupation, Previous_CustomerOccupation,
                                          AccountBalance, EffectiveDate, EndDate, IsCurrent)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, TRUE)
            """, (new_customer_key, account_id, customer_age, customer_occupation, None,
                  account_balance, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    df_count = con.execute("SELECT COUNT(*) AS row_count FROM Dim_Customer").fetchone()[0]
    print(f"✅ Dim_Customer berhasil diproses! Jumlah baris di Dim_Customer: {df_count}")

# Jalankan ETL
etl_dim_customer(df, con)

# Cek hasil akhir dari Dim_Customer
con.execute("SELECT * FROM Dim_Customer").df()
