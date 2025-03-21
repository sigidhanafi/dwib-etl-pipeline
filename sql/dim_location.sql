-- mapping id dengan ambil all location, generate ID, nanti waktu mau isi fact musti merge location dan ID
CREATE TABLE IF NOT EXISTS Dim_Location (
    LocationID INT PRIMARY KEY,
    Location VARCHAR(100)
);