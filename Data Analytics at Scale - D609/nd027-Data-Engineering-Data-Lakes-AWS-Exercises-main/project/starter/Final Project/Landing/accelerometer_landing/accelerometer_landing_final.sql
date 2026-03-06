CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_landing` (
    `user` string,
    `timeStamp` bigint,
    `x` float,
    `y` float,
    `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES(
    'serialization.format' = '1'
) LOCATION 's3://datalake-stedi-bucket/accelerometer/landing'
TBLPROPERTIES ('has_encrypted_data'='false');