
-- Create an external data source for ADLS Gen 2
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'AdlsGen2DataSource_hot')
BEGIN
    CREATE EXTERNAL DATA SOURCE AdlsGen2DataSource_hot
    WITH (
        LOCATION = 'https://icccricketadls.dfs.core.windows.net/gold/', -- Updated to point to the 'bronze' container
        CREDENTIAL = credential_cricket 
    );
END;


-- Create External Table Directly Over the Existing CSV File
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_hot_match_data' AND SCHEMA_NAME(schema_id) = 'Warm_gold')
BEGIN
    DROP EXTERNAL TABLE Warm_gold.ext_hot_match_data;
END;

CREATE EXTERNAL TABLE Warm_gold.ext_hot_match_data
(
   match_id VARCHAR(50),
    inning TINYINT,
    ball_over TINYINT,
    ball TINYINT,
    batsman VARCHAR(50),
    bowler VARCHAR(50),
    runs TINYINT,
    event_type VARCHAR(20),
    timestamp DATETIME2
)
WITH
(
    LOCATION = 'hot_data/*.csv', -- point to the correct file path relative to the data source
    DATA_SOURCE = AdlsGen2DataSource_hot,    
    FILE_FORMAT = CsvFileFormat
);

select * from Warm_gold.ext_hot_match_data;