-- mapping id dengan if else, karena hanya ada 2 type (debit/credit)
CREATE TABLE IF NOT EXISTS Dim_Transaction_Type (
    TransactionTypeID INT PRIMARY KEY,
    TransactionType VARCHAR(50)
);