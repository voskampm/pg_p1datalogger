--
--

create or replace function insert_p1_datagram(
   p_p1_data jsonb
,  p_p1_timestamp timestamp(0) default now()       
)

returns table (
    exitcode integer
 ,  exitinfo character varying ( 65536 )
 ,  p1_id integer
  )
LANGUAGE plpgsql
security definer
-- set search_path=p1   -- Weer aanzetten als we eigen p1 schema hebben!
-- set search_path=oadprd 
AS
$function$
declare
  p_exitcode          integer;
  p_exitinfo          character varying ( 65536 );
  p_query             text;
begin
  exitcode := 255;
  exitinfo := 'Developer forgot to fill out the exitinfo';
  select current_query() into p_query;

  begin
    insert into p1_log ( 
       p1_timestamp
    ,  p1_data
    )
    values (
       p_p1_timestamp
    ,  p_p1_data
    )
    returning p1_log.p1_id into p1_id;
  exception
    when Others then
      exitinfo := 'Unhandled exception: '||SQLERRM||' ['||SQLSTATE||']';
      exitcode := 3;
      return next;
      return;
  end;
  
  exitinfo := 'P1 datagram added';
  exitcode := 0;

  return next;
  return;
end;
$function$;
