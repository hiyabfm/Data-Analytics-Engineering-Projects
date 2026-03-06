CREATE EXTERNAL TABLE IF NOT EXISTS stedi.machine_learning_curated (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject INT,
    user STRING,
    timeStamp BIGINT,
    x FLOAT,
    y FLOAT,
    z FLOAT,
    customerName STRING,
    email STRING,
    birthDay STRING,
    registrationDate BIGINT,
    shareWithResearchAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://datalake-stedi-bucket/machine_learning/curated/'
TBLPROPERTIES (
    'classification'='json',
    'has_encrypted_data'='false'
);
