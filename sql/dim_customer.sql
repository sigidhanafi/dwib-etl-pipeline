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