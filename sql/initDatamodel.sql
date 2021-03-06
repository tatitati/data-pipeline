use epam;

drop table if exists epam.datamodel.dim_bike;
drop table if exists epam.datamodel.dim_date;
drop table if exists epam.datamodel.factless_bikes_stolen;

CREATE or REPLACE TABLE epam.datamodel.dim_bike(
  surrogateId integer autoincrement primary key,
  id varchar unique,
  description    VARCHAR,
  frame_model VARCHAR,
  manufacturer_name VARCHAR,
  serial VARCHAR,
  -- slow changing dimension (SCD) type 2
  valid_from datetime not null,
  valid_to datetime not null,
  isActive boolean not null,
  entryHash varchar not null
);

CREATE OR REPLACE TABLE epam.datamodel.dim_date (
       MY_DATE          DATE        NOT NULL primary key
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

CREATE or REPLACE TABLE epam.datamodel.factless_bikes_stolen(
  surrogateId int autoincrement primary key,
  bikeid int references epam.datamodel.dim_bike(surrogateId),
  date date references epam.datamodel.dim_date(my_date), -- this might allow me to create partitioned tables in the fact table
  fact varchar,
  location varchar
);
