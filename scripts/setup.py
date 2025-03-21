import duckdb


def setup():
    # Koneksi ke database DuckDB (file-based)
    con = duckdb.connect("db/dwh-perbankan.duckdb")

    # Buat skema database (DDL)
    ddl_script = """
    -- Buat Dim_Customer dengan SCD Type 2
    -- mapping id dengan auto increment
    CREATE TABLE IF NOT EXISTS Dim_Customer (
        CustomerKey BIGINT PRIMARY KEY NOT NULL,
        CustomerID VARCHAR NOT NULL,
        CustomerAge INT,
        CustomerOccupation VARCHAR(50),
        EffectiveFrom TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        EffectiveTo TIMESTAMP,
        IsCurrent BOOLEAN DEFAULT TRUE
    );

    -- mapping id dengan if else, karena hanya ada 2 type (debit/credit)
    CREATE TABLE IF NOT EXISTS Dim_Transaction_Type (
        TransactionTypeID INT PRIMARY KEY,
        TransactionType VARCHAR(50)
    );

    -- mapping id dengan YYMMDD
    CREATE TABLE IF NOT EXISTS Dim_Time (
        TimeID INT PRIMARY KEY,
        Day INT,
        Week INT,
        Quartile INT,
        Month INT,
        Year INT
    );

    -- mapping id dengan ambil all location, generate ID, nanti waktu mau isi fact musti merge location dan ID
    CREATE TABLE IF NOT EXISTS Dim_Location (
        LocationID INT PRIMARY KEY,
        Location VARCHAR(100)
    );

    -- mapping id dengan device id and IP address
    CREATE TABLE IF NOT EXISTS Dim_Device (
        DeviceID VARCHAR PRIMARY KEY,
        IPAddress VARCHAR(50)
    );

    -- mapping id dengan if else, karena hanya ada ATM, online, branch
    CREATE TABLE IF NOT EXISTS Dim_Channel (
        ChannelID INT PRIMARY KEY,
        ChannelName VARCHAR(50)
    );

    -- Buat Fact Table (Tabel Transaksi)
    CREATE TABLE IF NOT EXISTS Fact_Transaction (
        TransactionID VARCHAR PRIMARY KEY,
        TransactionAmount DECIMAL(15,2),
        TransactionDuration INT,
        LoginAttempts INT,
        CustomerID VARCHAR,
        DeviceID VARCHAR,
        TimeID INT,
        LocationID INT,
        TransactionTypeID INT,
        ChannelID INT,
        MerchantID VARCHAR,
        AccountBalance DECIMAL(15,2),
        PreviousTransactionDate TIMESTAMP,

        -- Foreign Keys
        FOREIGN KEY (DeviceID) REFERENCES Dim_Device(DeviceID),
        FOREIGN KEY (TimeID) REFERENCES Dim_Time(TimeID),
        FOREIGN KEY (LocationID) REFERENCES Dim_Location(LocationID),
        FOREIGN KEY (LocationID) REFERENCES Dim_Location(LocationID),
        FOREIGN KEY (TransactionTypeID) REFERENCES Dim_Transaction_Type(TransactionTypeID),
        FOREIGN KEY (ChannelID) REFERENCES Dim_Channel(ChannelID),
    );

    CREATE INDEX idx_fact_transaction_customer ON Fact_Transaction(CustomerID);
    CREATE INDEX idx_fact_transaction_device ON Fact_Transaction(DeviceID);
    CREATE INDEX idx_fact_transaction_time ON Fact_Transaction(TimeID);
    CREATE INDEX idx_fact_transaction_location ON Fact_Transaction(LocationID);
    CREATE INDEX idx_fact_transaction_type ON Fact_Transaction(TransactionTypeID);
    CREATE INDEX idx_fact_transaction_channel ON Fact_Transaction(ChannelID);

    """

    con.execute(ddl_script)
    print(f"\n DB Setup & Table successfully created in DuckDB!")

    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchdf()

    # Looping untuk menampilkan struktur semua tabel
    for table in tables["table_name"]:
        print(f"\n ==== Tabel: {table} ====")
        desc = con.execute(f"DESCRIBE {table}").fetchdf()
        print(desc)


    # Tampilkan daftar tabel
    print(tables)

    # Tutup koneksi
    con.close()
