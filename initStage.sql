use epam;
drop table if exists epam.ingestion.stage;


CREATE OR REPLACE TABLE epam.ingestion.stage (
  raw variant not null,
  filename varchar not null,
  amount_records int not null,
  copied_at datetime not null
);
