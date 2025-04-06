import duckdb
from pathlib import Path
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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

df = con.execute(
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
# print(result)


# # Buat bar chart dengan Seaborn
# plt.figure(figsize=(12, 6))
# sns.barplot(data=df, x="AgeSegment", y="TotalCustomers", hue="CustomerLocation")

# plt.title("Segmentasi Customer per Umur dan Lokasi")
# plt.xlabel("Segment Umur")
# plt.ylabel("Jumlah Customer")
# plt.legend(title="Lokasi")
# plt.tight_layout()
# plt.show()


# Query sebaran umur pelanggan
query = """
SELECT
    CASE 
        WHEN CustomerAge < 20 THEN 'Under 20'
        WHEN CustomerAge BETWEEN 20 AND 29 THEN '20-29'
        WHEN CustomerAge BETWEEN 30 AND 39 THEN '30-39'
        WHEN CustomerAge BETWEEN 40 AND 49 THEN '40-49'
        WHEN CustomerAge BETWEEN 50 AND 59 THEN '50-59'
        WHEN CustomerAge >= 60 THEN '60+'
        ELSE 'Unknown'
    END AS AgeSegment,
    COUNT(DISTINCT CustomerID) AS TotalCustomers
FROM Dim_Customer
WHERE IsCurrent = TRUE
GROUP BY AgeSegment
ORDER BY AgeSegment;
"""

# Eksekusi query dan ambil hasil ke dataframe
df = con.execute(query).df()

# Plot pie chart
plt.figure(figsize=(8, 8))
plt.pie(df["TotalCustomers"], labels=df["AgeSegment"], autopct='%1.1f%%', startangle=140)
plt.title("Sebaran Umur Pelanggan (Pie Chart)")
plt.axis('equal')  # Membuat pie chart jadi lingkaran sempurna
plt.show()
