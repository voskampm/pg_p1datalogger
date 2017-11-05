\set ON_ERROR_STOP on
set client_min_messages='warning';

begin transaction; 

-- Drop alle P1 database objects
DROP SCHEMA if exists p1 cascade;

CREATE DATABASE oad;
CREATE USER oadprd;
GRANT ALL PRIVILEGES ON DATABASE oad to oadprd;

alter USER oadprd WITH password 'XXXXX';

create schema oadprd authorization oadprd;
   set search_path=oadprd,public; -- default schema instellen om te voorkomen dat alles in public komt te staan.
   alter user oadprd set search_path=oadprd,public;
   alter database oad set search_path=oadprd,public;
   revoke all on schema public from oadprd;
   show search_path;


-- \i create_schema.sql

-- \i create_tables.sql
-- \i create_views.sql
-- \i create_functions.sql