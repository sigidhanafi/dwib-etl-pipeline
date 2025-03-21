-- mapping id dengan YYMMDD
CREATE TABLE IF NOT EXISTS Dim_Time (
    TimeID INT PRIMARY KEY,
    Day INT,
    Week INT,
    Quartile INT,
    Month INT,
    Year INT
);