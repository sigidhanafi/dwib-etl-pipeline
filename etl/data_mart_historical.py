import duckdb
import pandas as pd

def data_mart_historical(df, con):
    print("\t\tData Mart History")
    try:
        delete_data_mart = con.execute("DROP TABLE IF EXISTS datamart_trend_transaksi;")
        
        create_data_mart = con.execute("""
CREATE TABLE datamart_trend_transaksi AS
SELECT
    CAST(dt.Year AS TEXT) AS Year,
    dt.Month AS MonthNumber,
    CASE dt.Month
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Feb'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Apr'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul'
        WHEN 8 THEN 'Aug'
        WHEN 9 THEN 'Sep'
        WHEN 10 THEN 'Oct'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Dec'
        ELSE NULL
    END AS MonthName,
    dl.Location,
    dc.ChannelName,
    dtt.TransactionType,
    COUNT(ft.TransactionID) AS total_transaksi,
    SUM(ft.TransactionAmount) AS total_nominal,
    AVG(ft.TransactionDuration) AS rata2_durasi
FROM Fact_Transaction ft
JOIN Dim_Time dt ON ft.TimeID = dt.TimeID
JOIN Dim_Location dl ON ft.LocationID = dl.LocationID
JOIN Dim_Channel dc ON ft.ChannelID = dc.ChannelID
JOIN Dim_Transaction_Type dtt ON ft.TransactionTypeID = dtt.TransactionTypeID
GROUP BY CAST(dt.Year AS TEXT), MonthNumber, MonthName, dl.Location, dc.ChannelName, dtt.TransactionType
ORDER BY CAST(dt.Year AS TEXT),
    CASE MonthName
        WHEN 'Jan' THEN 1
        WHEN 'Feb' THEN 2
        WHEN 'Mar' THEN 3
        WHEN 'Apr' THEN 4
        WHEN 'May' THEN 5
        WHEN 'Jun' THEN 6
        WHEN 'Jul' THEN 7
        WHEN 'Aug' THEN 8
        WHEN 'Sep' THEN 9
        WHEN 'Oct' THEN 10
        WHEN 'Nov' THEN 11
        WHEN 'Dec' THEN 12
    END,
    dl.Location;
""")
        print("\t\t✅ Data Mart History berhasil!")
        
    except duckdb.Error as e:
        raise ValueError(f"{e}")

    except Exception as e:
        raise ValueError(f"{e}")

    finally:
        print("\t\tProses Data Mart Historical Selesai!")
        print("\n")
