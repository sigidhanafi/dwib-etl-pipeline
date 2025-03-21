-- mapping id dengan device id and IP address
CREATE TABLE IF NOT EXISTS Dim_Device (
    DeviceID VARCHAR PRIMARY KEY,
    IPAddress VARCHAR(50)
);