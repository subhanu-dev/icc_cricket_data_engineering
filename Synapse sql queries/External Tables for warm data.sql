--- Create Master Key 
CREATE MASTER KEY ENCRYPTION BY PASSWORD ='subhanu23*'; -- to access the external location 


--- Create Database Scoped Credentials 
CREATE DATABASE SCOPED CREDENTIAL credential_cricket
WITH 
    IDENTITY = 'Managed Identity';

-- Creating an external file format for CSV files
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'CsvFileFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT CsvFileFormat
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS (
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2, -- Skip the header row (row 1)
            USE_TYPE_DEFAULT = FALSE
        )
    );
END;


-- Create an external data source for ADLS Gen 2
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'AdlsGen2DataSource')
BEGIN
    CREATE EXTERNAL DATA SOURCE AdlsGen2DataSource
    WITH (
        LOCATION = 'https://icccricketadls.dfs.core.windows.net/bronze/', -- Updated to point to the 'bronze' container
        CREDENTIAL = credential_cricket 
    );
END;

-- Create the external table for the T20 batsmen rankings data

-- Create External Table Directly Over the Existing CSV File
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_api_top_batsmen' AND SCHEMA_NAME(schema_id) = 'Warm_gold')
BEGIN
    DROP EXTERNAL TABLE Warm_gold.ext_api_top_batsmen;
END;

CREATE EXTERNAL TABLE Warm_gold.ext_api_top_batsmen
(
    id INT,
    rank INT,
    name VARCHAR(100),
    country VARCHAR(50),
    rating INT,
    points INT,
    lastUpdatedOn DATE,
    trend VARCHAR(20),
    faceImageId INT,
    countryId INT
)
WITH
(
    LOCATION = 'api_data/top_players.csv', -- Updated to point to the correct file path relative to the data source
    DATA_SOURCE = AdlsGen2DataSource,     -- Corrected to match the data source name
    FILE_FORMAT = CsvFileFormat
);


--------------------------------Using the 2nd way  here ------------------------------------------------

---  Create the external table by materializing data from the view 

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_api_top_batsmen2' AND SCHEMA_NAME(schema_id) = 'Warm_gold')
BEGIN
    DROP EXTERNAL TABLE Warm_gold.ext_api_top_batsmen2;
END;

CREATE EXTERNAL TABLE Warm_gold.ext_api_top_batsmen2
WITH
(
    LOCATION = 'api_data/ext_api_top_batsmen2', -- New file path for the materialized data
    DATA_SOURCE = AdlsGen2DataSource,          
    FILE_FORMAT = CsvFileFormat                
)
AS
SELECT * FROM Warm_gold.api_top_batsmen;

--- what this does ?
-- Creates an external table (Warm_gold.ext_api_top_batsmen2).
-- Materializes data from the view Warm_gold.api_top_batsmen into a new CSV file (ext_api_top_batsmen2).

--  Query the external table to verify the data
SELECT * FROM Warm_gold.ext_api_top_batsmen2;


------------------------------------------ Top Bowlers T20 --------------------------------------------

-- Create External Table Directly Over the Existing CSV File
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_api_top_bowlers' AND SCHEMA_NAME(schema_id) = 'Warm_gold')
BEGIN
    DROP EXTERNAL TABLE Warm_gold.ext_api_top_bowlers;
END;

CREATE EXTERNAL TABLE Warm_gold.ext_api_top_bowlers
(
    id INT,
    rank INT,
    name VARCHAR(100),
    country VARCHAR(50),
    rating INT,
    points INT,
    lastUpdatedOn DATE,
    trend VARCHAR(20),
    faceImageId INT,
    countryId INT
)
WITH
(
    LOCATION = 'api_data/top_bowlers.csv', -- Updated to point to the correct file path relative to the data source
    DATA_SOURCE = AdlsGen2DataSource,     -- the data source name
    FILE_FORMAT = CsvFileFormat
);



----------------------------------- Trending Searches of Players -------------------------------------
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_api_trending_players' AND SCHEMA_NAME(schema_id) = 'Warm_gold')
BEGIN
    DROP EXTERNAL TABLE Warm_gold.ext_api_trending_players;
END;

CREATE EXTERNAL TABLE Warm_gold.ext_api_trending_players
(
    id INT,
    name VARCHAR(100),
    teamName VARCHAR(100),
    faceImageId INT,
    category VARCHAR(50)
)
WITH
(
    LOCATION = 'api_data/trending_players.csv',
    DATA_SOURCE = AdlsGen2DataSource,     
    FILE_FORMAT = CsvFileFormat
);


IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_api_top_bowlers' AND SCHEMA_NAME(schema_id) = 'Warm_gold')
BEGIN
    DROP EXTERNAL TABLE Warm_gold.ext_api_top_bowlers;
END;

CREATE EXTERNAL TABLE Warm_gold.ext_api_top_bowlers
(
    id INT,
    rank INT,
    name VARCHAR(100),
    country VARCHAR(50),
    rating INT,
    points INT,
    lastUpdatedOn DATE,
    trend VARCHAR(20),
    faceImageId INT,
    countryId INT
)
WITH
(
    LOCATION = 'api_data/top_bowlers.csv', -- Updated to point to the correct file path relative to the data source
    DATA_SOURCE = AdlsGen2DataSource,     -- the data source name
    FILE_FORMAT = CsvFileFormat
);



----------------------------------- Recent and Upcoming matches  -------------------------------------

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_api_recentmatches' AND SCHEMA_NAME(schema_id) = 'Warm_gold')
BEGIN
    DROP EXTERNAL TABLE Warm_gold.ext_api_recentmatches;
END;

CREATE EXTERNAL TABLE Warm_gold.ext_api_recentmatches
(
id VARCHAR(50),
    name VARCHAR(100),
    matchType VARCHAR(10),
    status VARCHAR(100),
    venue VARCHAR(100),
    date DATE,
    dateTimeGMT VARCHAR(20),
    series_id VARCHAR(50),
    fantasyEnabled BIT,
    bbbEnabled BIT,
    hasSquad BIT,
    matchStarted BIT,
    matchEnded BIT,
    api_status VARCHAR(20)
)
WITH
(
    LOCATION = 'api_data/recentmatches.csv',
    DATA_SOURCE = AdlsGen2DataSource,     
    FILE_FORMAT = CsvFileFormat
);

select * from Warm_gold.ext_api_recentmatches;

