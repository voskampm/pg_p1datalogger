--
--
CREATE SCHEMA p1
  AUTHORIZATION p1;

GRANT ALL ON SCHEMA p1 TO p1;
GRANT USAGE ON SCHEMA p1 TO p1;

set search_path='p1';
