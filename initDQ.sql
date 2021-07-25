use epam;
CREATE SCHEMA ingestion;

-- stage
CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STAGE bikes
    FILE_FORMAT = json_format;

CREATE STAGE bikes
    URL='s3://b-i-k-e-s/'
    CREDENTIALS=(AWS_KEY_ID='xxxx' AWS_SECRET_KEY='xxxxx')
    FILE_FORMAT = json_format;

-- create epam.ingestion.stage table
CREATE OR REPLACE TABLE stage (
  raw variant not null,
  filename varchar not null,
  copied_at datetime not null
);

-- create epam.datamodel.dim_bike table
CREATE TABLE dim_bike(
  id integer autoincrement primary key,
  desription    VARCHAR,
  frame_model VARCHAR,
  manufacturer_name VARCHAR,
  serial VARCHAR
);

-- populate epam.datamodel.dim_bike with data from epam.ingestion.stage
insert into datamodel
.dim_bike
select
    raw:id,
    raw:description,
    raw:frame_model,
    raw:manufacturer_name,
    raw:serial
from ingestion.stage;
