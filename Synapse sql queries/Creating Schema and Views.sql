-- using the raw T-SQL function
select *
from 
    openrowset(
        bulk 'https://icccricketadls.dfs.core.windows.net/gold/df_sl_head_t20.parquet/',
        format='parquet'
    ) as sl_head_t20


 -- Uses OPENROWSET to directly query a Parquet file in ADLS Gen2 without creating a table.
 -- Returns all columns from the Parquet file as a result set.


-- creating this gold schema
create schema GOLD;

-- creating a view under the schema
CREATE VIEW GOLD.sl_head_t20
AS
SELECT * FROM 
OPENROWSET
(
    BULK 'abfss://gold@icccricketadls.dfs.core.windows.net/df_sl_head_t20.parquet/', -- used abfss here instead of https. 
    FORMAT ='PARQUET'
) as sl_head;


-- Creates a view that acts as a "virtual table" pointing to the Parquet file.
-- Allows querying the Parquet file via SELECT * FROM GOLD.sl_head_t20.

select * from GOLD.sl_head_t20;




-- creating views for other data files

-- for t20 batsmen --
CREATE VIEW GOLD.t20_batsmen
AS
SELECT * FROM 
OPENROWSET
(
    BULK 'https://icccricketadls.blob.core.windows.net/gold/t20_batting_data.parquet/',  
    FORMAT ='PARQUET'
) as t20_batsmen;


select * from GOLD.t20_batsmen ;

-- for all time stats t20 by country
CREATE VIEW GOLD.t20_team_stats 
AS
SELECT * FROM 
OPENROWSET
(
    BULK 'https://icccricketadls.blob.core.windows.net/gold/t20_team_stats.parquet/',  
    FORMAT ='PARQUET'
) as t20_team_stats;

select * from GOLD.t20_team_stats ;


-- creating a view for latest t20 matches by SL team
CREATE VIEW GOLD.sl_latest_t20
AS
SELECT * FROM 
OPENROWSET
(
    BULK 'https://icccricketadls.blob.core.windows.net/gold/df_sl_latest_t20.parquet/',  
    FORMAT ='PARQUET'
) as t20_team_stats;

select * from GOLD.sl_latest_t20 ;




