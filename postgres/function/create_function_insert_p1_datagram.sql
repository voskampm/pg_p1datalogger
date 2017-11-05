--
--

create or replace function insert_p1_datagram(
    p_p1_timestamp timestamp(0) default now()
,   p_p1_meter_supplier character varying(4) default null
,   p_p1_header character varying(80)  default null
,   p_p1_dsmr_version character varying(2) default null
,   p_p1_equipment_id character varying(50)  default null
,   p_p1_meterreading_in_1 decimal(103)  default null
,   p_p1_unitmeterreading_in_1 character varying(3)  default null
,   p_p1_meterreading_in_2 decimal(103)  default null
,   p_p1_unitmeterreading_in_2 character varying(3) default null
,   p_p1_meterreading_out_1 decimal(103) default null
,   p_p1_unitmeterreading_out_1 character varying(3) default null
,   p_p1_meterreading_out_2 decimal(103) default null
,   p_p1_unitmeterreading_out_2 character varying(3) default null
,   p_p1_meterreading_prd decimal(103) default null
,   p_p1_unitmeterreading_prd character varying(3) default null
,   p_p1_current_tariff integer default null
,   p_p1_current_power_in decimal(103) default null
,   p_p1_unit_current_power_in character varying(3) default null
,   p_p1_current_power_out decimal(103) default null
,   p_p1_unit_current_power_out character varying(3) default null
,   p_p1_current_power_prd decimal(103) default null
,   p_p1_unit_current_power_prd character varying(3) default null
,   p_p1_current_threshold decimal(103) default null
,   p_p1_unit_current_threshold character varying(3) default null
,   p_p1_current_switch_position integer default null
,   p_p1_powerfailures integer default null
,   p_p1_long_powerfailures integer default null
,   p_p1_long_powerfailures_log character varying(1024) default null
,   p_p1_voltage_sags_l1 integer default null
,   p_p1_voltage_sags_l2 integer default null
,   p_p1_voltage_sags_l3 integer default null
,   p_p1_voltage_swells_l1 integer default null
,   p_p1_voltage_swells_l2 integer default null
,   p_p1_voltage_swells_l3 integer default null
,   p_p1_instantaneous_current_l1 decimal(103) default null
,   p_p1_unit_instantaneous_current_l1 character varying(3) default null
,   p_p1_instantaneous_current_l2 decimal(103) default null
,   p_p1_unit_instantaneous_current_l2 character varying(3) default null
,   p_p1_instantaneous_current_l3 decimal(103) default null
,   p_p1_unit_instantaneous_current_l3 character varying(3) default null
,   p_p1_voltage_l1 decimal(103) default null
,   p_p1_unit_voltage_l1 character varying(3) default null
,   p_p1_voltage_l2 decimal(103) default null
,   p_p1_unit_voltage_l2 character varying(3) default null
,   p_p1_voltage_l3 decimal(103) default null
,   p_p1_unit_voltage_l3 character varying(3) default null
,   p_p1_instantaneous_active_power_in_l1 decimal(103) default null
,   p_p1_unit_instantaneous_active_power_in_l1 character varying(3) default null
,   p_p1_instantaneous_active_power_in_l2 decimal(103) default null
,   p_p1_unit_instantaneous_active_power_in_l2 character varying(3) default null
,   p_p1_instantaneous_active_power_in_l3 decimal(103) default null
,   p_p1_unit_instantaneous_active_power_in_l3 character varying(3) default null
,   p_p1_instantaneous_active_power_out_l1 decimal(103) default null
,   p_p1_unit_instantaneous_active_power_out_l1 character varying(3) default null
,   p_p1_instantaneous_active_power_out_l2 decimal(103) default null
,   p_p1_unit_instantaneous_active_power_out_l2 character varying(3) default null
,   p_p1_instantaneous_active_power_out_l3 decimal(103) default null
,   p_p1_unit_instantaneous_active_power_out_l3 character varying(3) default null
,   p_p1_message_code character varying(8) default null
,   p_p1_message_text character varying(1024) default null
,   p_p1_channel_1_id integer default null
,   p_p1_channel_1_type_id integer  default null
,   p_p1_channel_1_type_desc character varying(20) default null
,   p_p1_channel_1_equipment_id character varying(50) default null
,   p_p1_channel_1_timestamp timestamp(0) default null
,   p_p1_channel_1_meterreading decimal(103)  default null
,   p_p1_channel_1_unit character varying(3) default null
,   p_p1_channel_1_valveposition integer default null
,   p_p1_channel_2_id integer default null
,   p_p1_channel_2_type_id integer  default null
,   p_p1_channel_2_type_desc character varying(20) default null
,   p_p1_channel_2_equipment_id character varying(50) default null
,   p_p1_channel_2_timestamp timestamp(0) default null
,   p_p1_channel_2_meterreading decimal(103)  default null
,   p_p1_channel_2_unit character varying(3) default null
,   p_p1_channel_2_valveposition integer default null
,   p_p1_channel_3_id integer default null
,   p_p1_channel_3_type_id integer  default null
,   p_p1_channel_3_type_desc character varying(20) default null
,   p_p1_channel_3_equipment_id character varying(50) default null
,   p_p1_channel_3_timestamp timestamp(0) default null
,   p_p1_channel_3_meterreading decimal(103)  default null
,   p_p1_channel_3_unit character varying(3) default null
,   p_p1_channel_3_valveposition integer default null
,   p_p1_channel_4_id integer default null
,   p_p1_channel_4_type_id integer  default null
,   p_p1_channel_4_type_desc character varying(20) default null
,   p_p1_channel_4_equipment_id character varying(50) default null
,   p_p1_channel_4_timestamp timestamp(0) default null
,   p_p1_channel_4_meterreading decimal(103)  default null
,   p_p1_channel_4_unit character varying(3) default null
,   p_p1_channel_4_valveposition integer default null
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
   ,  p1_meter_supplier
   ,  p1_header
   ,  p1_dsmr_version
   ,  p1_equipment_id
   ,  p1_meterreading_in_1
   ,  p1_unitmeterreading_in_1
   ,  p1_meterreading_in_2
   ,  p1_unitmeterreading_in_2
   ,  p1_meterreading_out_1
   ,  p1_unitmeterreading_out_1
   ,  p1_meterreading_out_2
   ,  p1_unitmeterreading_out_2
   ,  p1_meterreading_prd
   ,  p1_unitmeterreading_prd
   ,  p1_current_tariff
   ,  p1_current_power_in
   ,  p1_unit_current_power_in
   ,  p1_current_power_out
   ,  p1_unit_current_power_out
   ,  p1_current_power_prd
   ,  p1_unit_current_power_prd
   ,  p1_current_threshold
   ,  p1_unit_current_threshold
   ,  p1_current_switch_position
   ,  p1_powerfailures
   ,  p1_long_powerfailures
   ,  p1_long_powerfailures_log
   ,  p1_voltage_sags_l1
   ,  p1_voltage_sags_l2
   ,  p1_voltage_sags_l3
   ,  p1_voltage_swells_l1
   ,  p1_voltage_swells_l2
   ,  p1_voltage_swells_l3
   ,  p1_instantaneous_current_l1
   ,  p1_unit_instantaneous_current_l1
   ,  p1_instantaneous_current_l2
   ,  p1_unit_instantaneous_current_l2
   ,  p1_instantaneous_current_l3
   ,  p1_unit_instantaneous_current_l3
   ,  p1_voltage_l1
   ,  p1_unit_voltage_l1
   ,  p1_voltage_l2
   ,  p1_unit_voltage_l2
   ,  p1_voltage_l3
   ,  p1_unit_voltage_l3
   ,  p1_instantaneous_active_power_in_l1
   ,  p1_unit_instantaneous_active_power_in_l1
   ,  p1_instantaneous_active_power_in_l2
   ,  p1_unit_instantaneous_active_power_in_l2
   ,  p1_instantaneous_active_power_in_l3
   ,  p1_unit_instantaneous_active_power_in_l3
   ,  p1_instantaneous_active_power_out_l1
   ,  p1_unit_instantaneous_active_power_out_l1
   ,  p1_instantaneous_active_power_out_l2
   ,  p1_unit_instantaneous_active_power_out_l2
   ,  p1_instantaneous_active_power_out_l3
   ,  p1_unit_instantaneous_active_power_out_l3
   ,  p1_message_code
   ,  p1_message_text
   ,  p1_channel_1_id
   ,  p1_channel_1_type_id
   ,  p1_channel_1_type_desc
   ,  p1_channel_1_equipment_id
   ,  p1_channel_1_timestamp
   ,  p1_channel_1_meterreading
   ,  p1_channel_1_unit
   ,  p1_channel_1_valveposition
   ,  p1_channel_2_id
   ,  p1_channel_2_type_id
   ,  p1_channel_2_type_desc
   ,  p1_channel_2_equipment_id
   ,  p1_channel_2_timestamp
   ,  p1_channel_2_meterreading
   ,  p1_channel_2_unit
   ,  p1_channel_2_valveposition
   ,  p1_channel_3_id
   ,  p1_channel_3_type_id
   ,  p1_channel_3_type_desc
   ,  p1_channel_3_equipment_id
   ,  p1_channel_3_timestamp
   ,  p1_channel_3_meterreading
   ,  p1_channel_3_unit
   ,  p1_channel_3_valveposition
   ,  p1_channel_4_id
   ,  p1_channel_4_type_id
   ,  p1_channel_4_type_desc
   ,  p1_channel_4_equipment_id
   ,  p1_channel_4_timestamp
   ,  p1_channel_4_meterreading
   ,  p1_channel_4_unit
   ,  p1_channel_4_valveposition
   )
   values (
      p_p1_timestamp
   ,  p_p1_meter_supplier
   ,  p_p1_header
   ,  p_p1_dsmr_version
   ,  p_p1_equipment_id
   ,  p_p1_meterreading_in_1
   ,  p_p1_unitmeterreading_in_1
   ,  p_p1_meterreading_in_2
   ,  p_p1_unitmeterreading_in_2
   ,  p_p1_meterreading_out_1
   ,  p_p1_unitmeterreading_out_1
   ,  p_p1_meterreading_out_2
   ,  p_p1_unitmeterreading_out_2
   ,  p_p1_meterreading_prd
   ,  p_p1_unitmeterreading_prd
   ,  p_p1_current_tariff
   ,  p_p1_current_power_in
   ,  p_p1_unit_current_power_in
   ,  p_p1_current_power_out
   ,  p_p1_unit_current_power_out
   ,  p_p1_current_power_prd
   ,  p_p1_unit_current_power_prd
   ,  p_p1_current_threshold
   ,  p_p1_unit_current_threshold
   ,  p_p1_current_switch_position
   ,  p_p1_powerfailures
   ,  p_p1_long_powerfailures
   ,  p_p1_long_powerfailures_log
   ,  p_p1_voltage_sags_l1
   ,  p_p1_voltage_sags_l2
   ,  p_p1_voltage_sags_l3
   ,  p_p1_voltage_swells_l1
   ,  p_p1_voltage_swells_l2
   ,  p_p1_voltage_swells_l3
   ,  p_p1_instantaneous_current_l1
   ,  p_p1_unit_instantaneous_current_l1
   ,  p_p1_instantaneous_current_l2
   ,  p_p1_unit_instantaneous_current_l2
   ,  p_p1_instantaneous_current_l3
   ,  p_p1_unit_instantaneous_current_l3
   ,  p_p1_voltage_l1
   ,  p_p1_unit_voltage_l1
   ,  p_p1_voltage_l2
   ,  p_p1_unit_voltage_l2
   ,  p_p1_voltage_l3
   ,  p_p1_unit_voltage_l3
   ,  p_p1_instantaneous_active_power_in_l1
   ,  p_p1_unit_instantaneous_active_power_in_l1
   ,  p_p1_instantaneous_active_power_in_l2
   ,  p_p1_unit_instantaneous_active_power_in_l2
   ,  p_p1_instantaneous_active_power_in_l3
   ,  p_p1_unit_instantaneous_active_power_in_l3
   ,  p_p1_instantaneous_active_power_out_l1
   ,  p_p1_unit_instantaneous_active_power_out_l1
   ,  p_p1_instantaneous_active_power_out_l2
   ,  p_p1_unit_instantaneous_active_power_out_l2
   ,  p_p1_instantaneous_active_power_out_l3
   ,  p_p1_unit_instantaneous_active_power_out_l3
   ,  p_p1_message_code
   ,  p_p1_message_text
   ,  p_p1_channel_1_id
   ,  p_p1_channel_1_type_id
   ,  p_p1_channel_1_type_desc
   ,  p_p1_channel_1_equipment_id
   ,  p_p1_channel_1_timestamp
   ,  p_p1_channel_1_meterreading
   ,  p_p1_channel_1_unit
   ,  p_p1_channel_1_valveposition
   ,  p_p1_channel_2_id
   ,  p_p1_channel_2_type_id
   ,  p_p1_channel_2_type_desc
   ,  p_p1_channel_2_equipment_id
   ,  p_p1_channel_2_timestamp
   ,  p_p1_channel_2_meterreading
   ,  p_p1_channel_2_unit
   ,  p_p1_channel_2_valveposition
   ,  p_p1_channel_3_id
   ,  p_p1_channel_3_type_id
   ,  p_p1_channel_3_type_desc
   ,  p_p1_channel_3_equipment_id
   ,  p_p1_channel_3_timestamp
   ,  p_p1_channel_3_meterreading
   ,  p_p1_channel_3_unit
   ,  p_p1_channel_3_valveposition
   ,  p_p1_channel_4_id
   ,  p_p1_channel_4_type_id
   ,  p_p1_channel_4_type_desc
   ,  p_p1_channel_4_equipment_id
   ,  p_p1_channel_4_timestamp
   ,  p_p1_channel_4_meterreading
   ,  p_p1_channel_4_unit
   ,  p_p1_channel_4_valveposition
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
