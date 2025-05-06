Create Schema Warm_gold

-- DROP VIEW IF EXISTS Warm_gold.api_top_batsmen;

CREATE VIEW Warm_gold.api_top_batsmen
AS
SELECT 
    id,
    rank,
    name,
    country,
    rating,
    points,
    lastUpdatedOn,
    trend,
    faceImageId,
    countryId
FROM 
OPENROWSET
(
    BULK 'https://icccricketadls.blob.core.windows.net/bronze/api_data/top_players.csv',  
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2, -- Skip the header row (row 1)
    FIELDQUOTE = '"'
) 
WITH (
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
) AS api_top_batsmen;

select * from Warm_gold.api_top_batsmen ;
