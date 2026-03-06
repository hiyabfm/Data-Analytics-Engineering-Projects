CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_trusted (
  customerName string,
  email string,
  phone string,
  birthDay string,
  serialNumber string,
  registrationDate bigint,
  lastUpdateDate bigint,
  shareWithResearchAsOfDate bigint,
  shareWithPublicAsOfDate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://datalake-stedi-bucket/customer/trusted/';
