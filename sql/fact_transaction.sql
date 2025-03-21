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