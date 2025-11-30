
CREATE DATABASE IF NOT EXISTS freight_group;  -- creating database
USE freight_group;



CREATE EXTERNAL TABLE traffic_raw_onecsv (    --This table points to the 60 minute csv spanning 01/01/2019-01/03/2019
  ts_raw            STRING,
  street_id_raw     DOUBLE,
  traffic_count_raw INT,
  traffic_speed_raw DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/tmp/freight_onecsv';



SELECT * FROM traffic_raw_onecsv LIMIT 5;      --validating the data
