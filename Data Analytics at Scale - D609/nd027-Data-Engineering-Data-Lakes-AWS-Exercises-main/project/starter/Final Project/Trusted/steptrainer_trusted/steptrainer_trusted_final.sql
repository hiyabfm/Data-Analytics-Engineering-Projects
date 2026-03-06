CREATE EXTERNAL TABLE IF NOT EXISTS stedi.steptrainer_trusted (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://datalake-stedi-bucket/step_trainer/trusted'
TBLPROPERTIES ('has_encrypted_data'='false');