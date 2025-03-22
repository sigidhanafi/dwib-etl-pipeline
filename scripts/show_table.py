import duckdb
from pathlib import Path
import pandas as pd

db_dir = Path("db/dwh-perbankan.duckdb")
# print(db_dir)
con = duckdb.connect(db_dir)

# # Set agar semua kolom ditampilkan
# pd.set_option('display.max_columns', None)

# # (Opsional) Set agar semua baris ditampilkan juga
# pd.set_option('display.max_rows', None)

# # (Opsional) Set lebar kolom agar tidak dipotong
# pd.set_option('display.max_colwidth', None)  # atau gunakan pd.set_option('display.max_colwidth', -1) di versi lama

result = con.execute(
    "SELECT * FROM fact_transaction"
).fetchdf()

result = con.execute(
"""
    SELECT
        l.Location AS CustomerLocation,
        -- Segmentasi umur dalam rentang
        CASE 
            WHEN c.CustomerAge < 30 THEN 'Under 30'
            WHEN c.CustomerAge BETWEEN 30 AND 39 THEN '30-39'
            WHEN c.CustomerAge BETWEEN 40 AND 49 THEN '40-49'
            WHEN c.CustomerAge BETWEEN 50 AND 59 THEN '50-59'
            WHEN c.CustomerAge >= 60 THEN '60+'
            ELSE 'Unknown'
        END AS AgeSegment,
        t.Year AS TransactionYear,
        t.Month AS TransactionMonth,
        COUNT(DISTINCT f.CustomerID) AS TotalCustomers,
        COUNT(f.TransactionID) AS TotalTransactions,
        SUM(f.TransactionAmount) AS TotalTransactionAmount
    FROM Fact_Transaction f
    JOIN Dim_Customer c ON f.CustomerID = c.CustomerID
    JOIN Dim_Location l ON f.LocationID = l.LocationID
    JOIN Dim_Time t ON f.TimeID = t.TimeID
    WHERE c.IsCurrent = TRUE
    GROUP BY 
        l.Location,
        AgeSegment,
        t.Year,
        t.Month
    ORDER BY 
        l.Location,
        t.Year,
        t.Month,
        AgeSegment;

"""
).fetchdf()

# Tampilkan hasil
print(result)
