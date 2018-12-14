-- Create database.
CREATE DATABASE IF NOT EXISTS hotels;

-- Select database.
USE hotels;

-- We can create external table for sample submission dataset, Hive will not copy any data to its warehouse.
-- When table will be dropped data in file will remain untouched. This type of tables are convenient when
-- data in table used not only by Hive or when you want to associate multiple schemas with single dataset.
-- Such table supports inserts, updates, deletes, but does not support ACID.
CREATE EXTERNAL TABLE IF NOT EXISTS sample_submission_ext (id INT, hotel_cluster STRING)
COMMENT 'Hotels search sample submission dataset csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/root/hive/submission'
TBLPROPERTIES ('skip.header.line.count'='1');

-- We create external table for train dataset. We treat BOOLEAN as TINYINT, because Hive does not have
-- good support for BOOLEAN and converting/inserting/updating results are strange.
CREATE EXTERNAL TABLE IF NOT EXISTS train_dataset_ext (
 date_time STRING, site_name INT, posa_continent INT, user_location_country INT, user_location_region INT,
 user_location_city INT, orig_destination_distance DOUBLE, user_id INT, is_mobile TINYINT, is_package TINYINT,
 channel INT, srch_ci STRING, srch_co STRING, srch_adults_cnt INT, srch_children_cnt INT, srch_rm_cnt INT,
 srch_destination_id INT, srch_destination_type_id INT, is_booking TINYINT, cnt BIGINT, hotel_continent INT,
 hotel_country INT, hotel_market INT,  hotel_cluster INT)
COMMENT 'Hotels search train dataset csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/root/hive/train'
TBLPROPERTIES ('skip.header.line.count'='1');

-- We create managed table for train dataset with same schema as in train_dataset_ext table, but using
-- partitions, buckets and ORC format. This table will execute most of the queries faster than external
-- one, it supports ACID, provides table stats, ORC uses less memory.
CREATE TABLE IF NOT EXISTS train_dataset_orc (
 date_time STRING, site_name INT, posa_continent INT, user_location_country INT, user_location_region INT,
 user_location_city INT, orig_destination_distance DOUBLE, user_id INT, is_mobile TINYINT, is_package TINYINT,
 channel INT, srch_ci STRING, srch_co STRING, srch_adults_cnt INT, srch_children_cnt INT, srch_rm_cnt INT,
 srch_destination_id INT, srch_destination_type_id INT, cnt BIGINT, hotel_continent INT, hotel_country INT,
 hotel_market INT, hotel_cluster INT)
COMMENT 'Hotels search train dataset ORC'
PARTITIONED BY (is_booking TINYINT)
CLUSTERED BY (hotel_continent)
SORTED BY (hotel_country, hotel_market, hotel_cluster)
INTO 5 BUCKETS
STORED AS ORC;

-- Inserting data into partitioned and bucketed table for more fast querying and compact storage.
INSERT OVERWRITE TABLE train_dataset_orc PARTITION(is_booking=1)
SELECT date_time, site_name, posa_continent, user_location_country, user_location_region, user_location_city, orig_destination_distance,
 user_id, is_mobile, is_package, channel, srch_ci, srch_co, srch_adults_cnt, srch_children_cnt, srch_rm_cnt, srch_destination_id,
 srch_destination_type_id, cnt, hotel_continent, hotel_country, hotel_market, hotel_cluster FROM train_dataset_ext
WHERE is_booking=1
ORDER BY hotel_continent, hotel_country, hotel_market, hotel_cluster;

INSERT OVERWRITE TABLE train_dataset_orc PARTITION(is_booking=0)
SELECT date_time, site_name, posa_continent, user_location_country, user_location_region, user_location_city, orig_destination_distance,
 user_id, is_mobile, is_package, channel, srch_ci, srch_co, srch_adults_cnt, srch_children_cnt, srch_rm_cnt, srch_destination_id,
 srch_destination_type_id, cnt, hotel_continent, hotel_country, hotel_market, hotel_cluster FROM train_dataset_ext
WHERE is_booking=0
ORDER BY hotel_continent, hotel_country, hotel_market, hotel_cluster;

-- Creating external table for test dataset csv file.
CREATE EXTERNAL TABLE IF NOT EXISTS test_dataset_ext (
 id INT, date_time STRING, site_name INT, posa_continent INT, user_location_country INT, user_location_region INT,
 user_location_city INT, orig_destination_distance DOUBLE, user_id INT, is_mobile TINYINT, is_package TINYINT,
 channel INT, srch_ci STRING, srch_co STRING, srch_adults_cnt INT, srch_children_cnt INT, srch_rm_cnt INT,
 srch_destination_id INT, srch_destination_type_id INT, hotel_continent INT, hotel_country INT, hotel_market INT)
COMMENT 'Hotels search test dataset csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/root/hive/test'
TBLPROPERTIES ('skip.header.line.count'='1');

-- CREATE TABLE ... AS SELECT (or CTAS) is used to quickly create new table from the other one. This way we can
-- create tables of different format, schema or just make new managed table from external one. We cannot specify
-- partitions or buckets in such statement, but we can do it further with ALTER TABLE statement.
CREATE TABLE test_dataset_orc
STORED AS ORC
AS SELECT * FROM test_dataset_ext
SORT BY id;

-- Creating managed table for destinations csv file.
CREATE TABLE IF NOT EXISTS destinations (
 srch_destination_id INT,d1 DOUBLE, d2 DOUBLE, d3 DOUBLE, d4 DOUBLE, d5 DOUBLE, d6 DOUBLE, d7 DOUBLE, d8 DOUBLE,
 d9 DOUBLE, d10 DOUBLE, d11 DOUBLE, d12 DOUBLE, d13 DOUBLE, d14 DOUBLE, d15 DOUBLE, d16 DOUBLE, d17 DOUBLE,
 d18 DOUBLE, d19 DOUBLE, d20 DOUBLE, d21 DOUBLE, d22 DOUBLE, d23 DOUBLE, d24 DOUBLE, d25 DOUBLE, d26 DOUBLE,
 d27 DOUBLE, d28 DOUBLE, d29 DOUBLE, d30 DOUBLE, d31 DOUBLE, d32 DOUBLE, d33 DOUBLE, d34 DOUBLE, d35 DOUBLE,
 d36 DOUBLE, d37 DOUBLE, d38 DOUBLE, d39 DOUBLE, d40 DOUBLE, d41 DOUBLE, d42 DOUBLE, d43 DOUBLE, d44 DOUBLE,
 d45 DOUBLE, d46 DOUBLE, d47 DOUBLE, d48 DOUBLE, d49 DOUBLE, d50 DOUBLE, d51 DOUBLE, d52 DOUBLE, d53 DOUBLE,
 d54 DOUBLE, d55 DOUBLE, d56 DOUBLE, d57 DOUBLE, d58 DOUBLE, d59 DOUBLE, d60 DOUBLE, d61 DOUBLE, d62 DOUBLE,
 d63 DOUBLE, d64 DOUBLE, d65 DOUBLE, d66 DOUBLE, d67 DOUBLE, d68 DOUBLE, d69 DOUBLE, d70 DOUBLE, d71 DOUBLE,
 d72 DOUBLE, d73 DOUBLE, d74 DOUBLE, d75 DOUBLE, d76 DOUBLE, d77 DOUBLE, d78 DOUBLE, d79 DOUBLE, d80 DOUBLE,
 d81 DOUBLE, d82 DOUBLE, d83 DOUBLE, d84 DOUBLE, d85 DOUBLE, d86 DOUBLE, d87 DOUBLE, d88 DOUBLE, d89 DOUBLE,
 d90 DOUBLE, d91 DOUBLE, d92 DOUBLE, d93 DOUBLE, d94 DOUBLE, d95 DOUBLE, d96 DOUBLE, d97 DOUBLE, d98 DOUBLE,
 d99 DOUBLE, d100 DOUBLE, d101 DOUBLE, d102 DOUBLE, d103 DOUBLE, d104 DOUBLE, d105 DOUBLE, d106 DOUBLE, d107 DOUBLE,
 d108 DOUBLE, d109 DOUBLE, d110 DOUBLE, d111 DOUBLE, d112 DOUBLE, d113 DOUBLE, d114 DOUBLE, d115 DOUBLE, d116 DOUBLE,
 d117 DOUBLE, d118 DOUBLE, d119 DOUBLE, d120 DOUBLE, d121 DOUBLE, d122 DOUBLE, d123 DOUBLE, d124 DOUBLE, d125 DOUBLE,
 d126 DOUBLE, d127 DOUBLE, d128 DOUBLE, d129 DOUBLE, d130 DOUBLE, d131 DOUBLE, d132 DOUBLE, d133 DOUBLE, d134 DOUBLE,
 d135 DOUBLE, d136 DOUBLE, d137 DOUBLE, d138 DOUBLE, d139 DOUBLE, d140 DOUBLE, d141 DOUBLE, d142 DOUBLE, d143 DOUBLE,
 d144 DOUBLE, d145 DOUBLE, d146 DOUBLE, d147 DOUBLE, d148 DOUBLE, d149 DOUBLE)
COMMENT 'Hotels destinations dataset csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- Loading data from csv file to destinations table. This way original csv file will be moved to Hive
-- warehouse and when table will be dropped, data will be deleted as well.
LOAD DATA INPATH "/user/root/hive/destinations/destinations.csv" INTO TABLE destinations;

-- Queries

-- Task 1
-- We select countries with the biggest number of search requests with successful booking result.
SELECT hotel_country, COUNT(*) as search_req_count FROM train_dataset_orc
WHERE is_booking=1
GROUP BY hotel_country
ORDER BY search_req_count DESC
LIMIT 3;

-- Task 2
-- We use Hive DATEDIFF function to get period of stay specified in search request. We selecting MAX
-- period of stay from those couples (2 adults) with at least one child.
-- This query is executed over external table as there is some bug in hive or Ambari that skips the
-- real longest period of stay from results when executing over ORC managed hive table. The situation
-- was investigated, table contains such row, but somehow does not take it to account. Screenshots of
-- this situation are attached to the archive with screenshots.
SELECT MAX(DATEDIFF(srch_co, srch_ci)) as period_of_stay_days FROM train_dataset_ext
WHERE srch_adults_cnt = 2 AND srch_children_cnt > 0;

-- Task 3 (exclusive)
-- In subquery we select all hotels that have at least one successful booking (this subquery is pretty fast,
-- as we have partitioning by is_booking field and there is much less successful bookings than not successful).
-- Then we subtract all this hotels with successful bookings from all other by doing LEFT JOIN and , and
-- calculating search requests count, choosing three most popular hotels.
SELECT all_hotels.hotel_continent, all_hotels.hotel_country, all_hotels.hotel_market, COUNT(*) as searches_count
FROM train_dataset_orc all_hotels
LEFT JOIN (
 SELECT hotel_continent, hotel_country, hotel_market
 FROM train_dataset_orc
 WHERE is_booking=1
 GROUP BY hotel_continent, hotel_country, hotel_market) booked
ON all_hotels.hotel_continent = booked.hotel_continent
AND all_hotels.hotel_country = booked.hotel_country
AND all_hotels.hotel_market = booked.hotel_market
WHERE booked.hotel_continent IS NULL
AND booked.hotel_country IS NULL
AND booked.hotel_market IS NULL
GROUP BY all_hotels.hotel_continent, all_hotels.hotel_country, all_hotels.hotel_market
ORDER BY searches_count DESC
LIMIT 3;

-- Task 3 (inclusive)
-- We filter all request which bookings were not successful and sort by number of requests descending,
-- limiting the output to top three hotels with largest number of such requests.
SELECT hotel_continent, hotel_country, hotel_market, COUNT(*) as searches_count
FROM train_dataset_orc
WHERE is_booking=0
GROUP BY hotel_continent, hotel_country, hotel_market
ORDER BY searches_count DESC
LIMIT 3;
