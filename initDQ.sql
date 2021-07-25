use epam;
CREATE SCHEMA myschema;

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

-- stage
CREATE OR REPLACE TABLE stage (
  raw variant not null,
  filename varchar not null,
  copied_at datetime not null
);
