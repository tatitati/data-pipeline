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
CREATE or REPLACE TABLE dim_bike(
  surrogateId integer autoincrement primary key,
  id varchar unique,
  description    VARCHAR,
  frame_model VARCHAR,
  manufacturer_name VARCHAR,
  serial VARCHAR,
  -- slow changed dimension type 2
  valid_from datetime not null,
  valid_to datetime not null,
  valid boolean not null
);

-- create epam.datamodel.dim_datetime table
CREATE OR REPLACE TABLE dim_date (
       MY_DATE          DATE        NOT NULL
      ,YEAR             SMALLINT    NOT NULL
      ,MONTH            SMALLINT    NOT NULL
      ,MONTH_NAME       CHAR(3)     NOT NULL
      ,DAY_OF_MON       SMALLINT    NOT NULL
      ,DAY_OF_WEEK      VARCHAR(9)  NOT NULL
      ,WEEK_OF_YEAR     SMALLINT    NOT NULL
      ,DAY_OF_YEAR      SMALLINT    NOT NULL
    )
    AS
      WITH CTE_MY_DATE AS (
        SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS MY_DATE
          FROM TABLE(GENERATOR(ROWCOUNT=>10000))  -- Number of days after reference date in previous line
      )
      SELECT MY_DATE
            ,YEAR(MY_DATE)
            ,MONTH(MY_DATE)
            ,MONTHNAME(MY_DATE)
            ,DAY(MY_DATE)
            ,DAYOFWEEK(MY_DATE)
            ,WEEKOFYEAR(MY_DATE)
            ,DAYOFYEAR(MY_DATE)
        FROM CTE_MY_DATE;



CREATE or REPLACE TABLE factless_bikes_stolen(
  surrogateId int autoincrement primary key,
  bikeid int references dim_bike(surrogateId),
  date date references dim_date(date) -- this might allow me to create partitioned tables in the fact table
);

