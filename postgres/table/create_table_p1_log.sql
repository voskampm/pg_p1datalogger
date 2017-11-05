drop table if exists p1_log;

create table if not exists p1_log (
   p1_id serial primary key
,  p1_timestamp timestamp(0) not null 
,  p1_data jsonb not null
);