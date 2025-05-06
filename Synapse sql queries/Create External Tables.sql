--- Create Master Key 
CREATE MASTER KEY ENCRYPTION BY PASSWORD ='subhanu23*'; -- to access the external location 


--- Create Database Scoped Credentials 
CREATE DATABASE SCOPED CREDENTIAL credential_cricket
WITH 
    IDENTITY = 'Managed Identity';


--- Create External Data Source for Gold 

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://icccricketadls.blob.core.windows.net/gold', 
    CREDENTIAL = credential_cricket 
);


--- Create an external file format for Parquet files.
CREATE EXTERNAL FILE FORMAT format_parquet
WITH 
(
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


---------------------------------------------  External Table Creation  ----------------------------------------------------------

--- Create external table for sl_head_t20
CREATE EXTERNAL TABLE GOLD.ext_sl_head_t20
WITH
(
    LOCATION = 'ext_sl_head_t20',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * from GOLD.sl_head_t20; -- this is the view that's being fetched which we created previously


-- for the t20 batsmen data

CREATE EXTERNAL TABLE GOLD.ext_t20_batsmen
WITH
(
    LOCATION = 'ext_t20_batsmen',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * from GOLD.t20_batsmen; 

select * from GOLD.ext_t20_batsmen;



---- for t20 team Stats

CREATE EXTERNAL TABLE GOLD.ext_t20_team_stats
WITH
(
    LOCATION = 'ext_t20_team_stats',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * from GOLD.t20_team_stats; 

select * from GOLD.ext_t20_team_stats;


----  external table for the latest sl team matches T20

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_sl_latest_t20' AND SCHEMA_NAME(schema_id) = 'GOLD')
BEGIN
    DROP EXTERNAL TABLE GOLD.ext_sl_latest_t20;
END;

--- Create external table for sl_head_t20, pointing directly to the original data location

CREATE EXTERNAL TABLE GOLD.ext_sl_latest_t20
(
    Team VARCHAR(50),
    Result VARCHAR(20),
    Margin VARCHAR(20),
    Balls_Remaining INT,
    Toss VARCHAR(10),
    Bat VARCHAR(10),
    Opposition VARCHAR(50),
    Ground VARCHAR(50),
    Date DATE
)
WITH
(
    LOCATION = 'df_sl_latest_t20.parquet', -- Point to the original location of the data
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
);

select * from GOLD.ext_sl_latest_t20


-- the above did not work for some reason, rolling back to the previous way.

CREATE EXTERNAL TABLE GOLD.ext_sl_latest_t20
WITH
(
    LOCATION = 'ext_sl_latest_t20',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * from GOLD.sl_latest_t20;

select * from GOLD.ext_sl_latest_t20