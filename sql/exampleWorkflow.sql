create or replace table user_ingestion(
    sugid integer autoincrement primary key,
    id varchar not null,
    name varchar not null,
    postcode varchar not null
);

create or replace table user_dim(
    sugId integer autoincrement primary key,
    userId varchar,
    name varchar,
    postcode varchar,
    valid_from datetime not null,
    valid_to datetime not null,
    isActive boolean not null,
    userHash varchar not null
);

insert into user_ingestion(id, name, postcode) values
    ('idA', 'nameA', 'pA'),
    ('idB', 'nameB', 'pB'),
    ('idC', 'nameC', 'pC'),
    ('idD', 'nameD', 'pD');
    
insert into user_dim(userId, name, postcode, valid_from, valid_to, isActive, userHash)
select 
    id, 
    name, 
    postcode,
    current_timestamp(),
    '9999-02-20 00:00:00.000' as datetime,
    true,
    md5(to_varchar(array_construct(id, name, postcode)))
from user_ingestion;

select * from user_dim;


-- AUTOINCREMENT START

DELETE FROM USER_INGESTION;
insert into user_ingestion(id, name, postcode) values
    ('idA', 'nameA', 'pA'),
    ('idB', 'nameB', 'pB'),
    ('idC', 'nameC', 'pC2'),
    ('idD', 'nameD', 'pD2'); --<<<< UDPATE
SELECT * FROM user_ingestion;


SELECT user_ingestion.*
from user_ingestion
left join user_dim  on user_dim.userId = user_ingestion.id
where   md5(to_varchar(array_construct(user_ingestion.id, user_ingestion.name, user_ingestion.postcode))) <> userHash;


-- wer receive a full dataset daily. This update (set active=false) the ones in user_dim without changes from the whole dataset
update user_dim set 
    isActive = false,
    valid_to = current_timestamp()
where userId in (
  SELECT id
  from user_ingestion
  left join user_dim  on user_dim.userId = user_ingestion.id
  where   md5(to_varchar(array_construct(user_ingestion.id, user_ingestion.name, user_ingestion.postcode))) <> userHash
);

select * from user_dim;

insert into user_dim(userId, name, postcode, valid_from, valid_to, isActive, userHash)
  SELECT 
    user_ingestion.id, 
    user_ingestion.name,
    user_ingestion.postcode,
    current_timestamp(),
    '9999-02-20 00:00:00.000' as datetime,
    true,
    md5(to_varchar(array_construct(user_ingestion.id, user_ingestion.name, user_ingestion.postcode)))
  from user_ingestion user_ingestion
  left join user_dim  on user_dim.userId = user_ingestion.id
  where   md5(to_varchar(array_construct(user_ingestion.id, user_ingestion.name, user_ingestion.postcode))) <> userHash;

select * from user_dim;




