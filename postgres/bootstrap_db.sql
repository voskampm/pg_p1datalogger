\set ON_ERROR_STOP on
set client_min_messages='warning';

begin transaction; 

-- Drop alle P1 database objects
DROP SCHEMA if exists p1 cascade;

\i create_schema.sql

-- \i create_tables.sql
-- \i create_views.sql
-- \i create_functions.sql