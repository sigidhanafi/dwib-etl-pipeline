-- mapping id dengan if else, karena hanya ada ATM, online, branch
CREATE TABLE IF NOT EXISTS Dim_Channel (
    ChannelID INT PRIMARY KEY,
    ChannelName VARCHAR(50)
);