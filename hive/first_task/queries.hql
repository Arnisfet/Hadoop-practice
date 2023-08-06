ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

USE sberSm20232020_test;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=116;
SET hive.exec.max.dynamic.partitions.pernode=116;
SET hive.auto.convert.join = false;


drop table if exists IPRegions;
drop table if exists user_logs;
drop table if exists Users;
drop table if exists subnets;
drop table if exists Logs;

CREATE EXTERNAL TABLE IPRegions (
ip STRING,
region STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\d+.\\d+.\\d+.\\d+)\\s(.+)$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';

CREATE EXTERNAL TABLE Users (
ip STRING,
browser STRING,
sex STRING,
age INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\d+.\\d+.\\d+.\\d+)\\s(\\S+)\\s(male|female)\\s(\\d+)'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M';


CREATE EXTERNAL TABLE user_logs (
ip STRING,
time_request INT,
http_request STRING,
page_size INT,
http_code INT,
app_info STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\d+.\\d+.\\d+.\\d+)\\s+(\\d{8})\\d+\\s+(http:\\/\\/\\\S+)\\s+(\\d+)\\s+(\\d+)\\s+(\\S+\\s.+)'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';

CREATE EXTERNAL TABLE subnets (
ip STRING,
mask STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\d+.\\d+.\\d+.\\d+)\\s+(\\d+.\\d+.\\d+.\\d+)'
)
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';

create external table Logs(
ip STRING,
http_request STRING,
page_size INT,
http_code INT,
app_info STRING
)
partitioned by (time_request INT)
stored as textfile;

insert overwrite table Logs partition (time_request)
select ip,
http_request,
page_size,
http_code,
app_info,
time_request
from user_logs;

select * from Logs limit 10;
select * from Users limit 10;
select * from IPRegions limit 10;
SELECT * FROM Subnets limit 10;
