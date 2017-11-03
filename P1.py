#!python3
# P1 Datalogger
# Release 0.8 / M401 / Iskra / S0 / S0prd / prd / pv / pvo / emoncmso
# Author J. van der Linde
# Copyright (c) 2011-2016 J. van der Linde
#
# Although there is a explicit copyright on this sourcecode, anyone may use it freely under a 
# "Creative Commons Naamsvermelding-NietCommercieel-GeenAfgeleideWerken 3.0 Nederland" licentie.
# Please check http://creativecommons.org/licenses/by-nc-nd/3.0/nl/ for details
#
# This software is provided as is and comes with absolutely no warranty.
# The author is not responsible or liable (direct or indirect) to anyone for the use or misuse of this software.
# Any person using this software does so entirely at his/her own risk. 
# That person bears sole responsibility and liability for any claims or actions, legal or civil, arising from such use.
# If you believe this software is in breach of anyone's copyright you will inform the author immediately so the offending material 
# can be removed upon receipt of proof of copyright for that material.
#
# Dependend on Python 3.2+ and Python 3.x packages: PySerial 2.5+
#

progname='P1.py'
version = "v0.86"
# Set to True if P1 telegram need to be enriched
import_db = False
#if set to 'true', check #Start of functionality to add other meterdata to p1-telegram  # section
import sys
import serial
import datetime
import time
import csv
import os
import locale
import socket
socket.setdefaulttimeout(30)
import http.client
import urllib.parse
import urllib.request
from urllib.error import URLError, HTTPError
MySQL_loaded = True
try:
    import mysql.connector
except ImportError:
    MySQL_loaded=False
SQLite_loaded = True
try:
    import sqlite3
except ImportError:
    SQLite_loaded=False

from time import sleep
import time as _time
from datetime import timezone, tzinfo, timedelta, datetime
import argparse
import serial.tools.list_ports
#####################################################################
# pvoutput.org system parameters
#####################################################################
pvo_url = 'http://pvoutput.org/service/r2/addstatus.jsp'
#####################################################################

class P1_ChannelData:
    def __init__(self, id=0, type_id=0, type_desc='', equipment_id='', timestamp='0000-00-00 00:00:00', meterreading=0.0, unit='', valveposition=0):
        self.id = id
        self.type_id = type_id
        self.type_desc = type_desc
        self.equipment_id = equipment_id
        self.timestamp = timestamp
        self.meterreading = meterreading
        self.unit = unit
        self.valveposition = valveposition

def scan_serial():
#  scan for available ports. return a list of tuples (name, description)
    available = []
    for i in serial.tools.list_ports.comports():
        available.append((i[0], i[1]))
    return available

##################
#Time conversion #
##################
ZERO = timedelta(0)
HOUR = timedelta(hours=1)
# A UTC class.
class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO
utc = UTC()
# A class building tzinfo objects for fixed-offset time zones.
# Note that FixedOffset(0, "UTC") is a different way to build a
# UTC tzinfo object.
class FixedOffset(tzinfo):
    """Fixed offset in minutes east from UTC."""
    def __init__(self, offset, name):
        self.__offset = timedelta(minutes = offset)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO
# A class capturing the platform's idea of local time.

STDOFFSET = timedelta(seconds = -_time.timezone)
if _time.daylight:
    DSTOFFSET = timedelta(seconds = -_time.altzone)
else:
    DSTOFFSET = STDOFFSET
DSTDIFF = DSTOFFSET - STDOFFSET

class LocalTimezone(tzinfo):

    def utcoffset(self, dt):
        if self._isdst(dt):
            return DSTOFFSET
        else:
            return STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return DSTDIFF
        else:
            return ZERO

    def tzname(self, dt):
        return _time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0

Local = LocalTimezone()

def utc_to_local(utc_dt):
#    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
#def utc_to_cet(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(Local) 
#    local_t = t.astimezone(Local)
#    str(local_t)
    
################
#Error display #
################
def show_error():
    ft = sys.exc_info()[0]
    fv = sys.exc_info()[1]
    print("Fout type: %s" % ft )
    print("Fout waarde: %s" % fv )
    return
    
################
#Scherm output #
################
def print_p1_telegram():
    print ("---------------------------------------------------------------------------------------")
    if use_systemtime == True:
       timestamp_qualifier = "Systeem datum-tijd"
    else:
       timestamp_qualifier = "P1 datum-tijd"
    print ("P1 telegram ontvangen op: %s (UTC) / %s (Local) - %s" % (p1_timestamp_utc, p1_timestamp, timestamp_qualifier))
    if p1_meter_supplier == "KMP":
        print ("Meter fabrikant: Kamstrup")
    elif p1_meter_supplier == "ISk":
        print ("Meter fabrikant: IskraEmeco")
    elif p1_meter_supplier == "XMX":
        print ("Meter fabrikant: Xemex")
    elif p1_meter_supplier == "KFM":
        print ("Meter fabrikant: Kaifa")
    else:
        print ("Meter fabrikant: Niet herkend")
    print ("Meter informatie: %s" % p1_header )
    print (" 0. 2. 8 - DSMR versie: %s" % p1_dsmr_version )
    print ("96. 1. 1 - Meternummer Elektriciteit: %s" % p1_equipment_id )
    print (" 1. 8. 1 - Meterstand Elektriciteit levering (T1/Laagtarief): %0.3f %s" % (p1_meterreading_in_1,p1_unitmeterreading_in_1) )
    print (" 1. 8. 2 - Meterstand Elektriciteit levering (T2/Normaaltarief): %0.3f %s" % (p1_meterreading_in_2,p1_unitmeterreading_in_2) )
    print (" 2. 8. 1 - Meterstand Elektriciteit teruglevering (T1/Laagtarief): %0.3f %s" % (p1_meterreading_out_1,p1_unitmeterreading_out_1) )
    print (" 2. 8. 2 - Meterstand Elektriciteit teruglevering (T2/Normaaltarief): %0.3f %s" % (p1_meterreading_out_2,p1_unitmeterreading_out_2) )
    print ("           Meterstand Elektriciteit productie: %0.3f %s" % (p1_meterreading_prd,p1_unitmeterreading_prd) )
    print ("96.14. 0 - Actueel tarief Elektriciteit: %d" % p1_current_tariff )
    print (" 1. 7. 0 - Actueel vermogen Electriciteit levering (+P): %0.3f %s" % (p1_current_power_in,p1_unit_current_power_in) )
    print (" 2. 7. 0 - Actueel vermogen Electriciteit teruglevering (-P): %0.3f %s" % (p1_current_power_out,p1_unit_current_power_out) )
    print ("           Actueel vermogen Electriciteit productie: %0.3f %s" % (p1_current_power_prd,p1_unit_current_power_prd) )    
    print ("17. 0. 0 - Actuele doorlaatwaarde Elektriciteit: %0.3f %s" % (p1_current_threshold,p1_unit_current_threshold) )
    print ("96. 3.10 - Actuele schakelaarpositie Elektriciteit: %s" % p1_current_switch_position )
    print ("96. 7.21 - Aantal onderbrekingen Elektriciteit: %s" % p1_powerfailures )
    print ("96. 7. 9 - Aantal lange onderbrekingen Elektriciteit: %s" % p1_long_powerfailures )
    print ("99.97. 0 - Lange onderbrekingen Elektriciteit logboek: %s" % p1_long_powerfailures_log )
    print ("32.32. 0 - Aantal korte spanningsdalingen Elektriciteit in fase 1: %s" % p1_voltage_sags_l1 )
    print ("52.32. 0 - Aantal korte spanningsdalingen Elektriciteit in fase 2: %s" % p1_voltage_sags_l2 )
    print ("72.32. 0 - Aantal korte spanningsdalingen Elektriciteit in fase 3: %s" % p1_voltage_sags_l3 )
    print ("32.36. 0 - Aantal korte spanningsstijgingen Elektriciteit in fase 1: %s" % p1_voltage_swells_l1 )
    print ("52.36. 0 - Aantal korte spanningsstijgingen Elektriciteit in fase 2: %s" % p1_voltage_swells_l2 )
    print ("72.36. 0 - Aantal korte spanningsstijgingen Elektriciteit in fase 3: %s" % p1_voltage_swells_l3 )       
    print ("31. 7. 0 - Instantane stroom Elektriciteit in fase 1: %0.3f %s" % (p1_instantaneous_current_l1,p1_unit_instantaneous_current_l1) )  
    print ("51. 7. 0 - Instantane stroom Elektriciteit in fase 2: %0.3f %s" % (p1_instantaneous_current_l2,p1_unit_instantaneous_current_l2) )  
    print ("71. 7. 0 - Instantane stroom Elektriciteit in fase 3: %0.3f %s" % (p1_instantaneous_current_l3,p1_unit_instantaneous_current_l3) )     
    print ("32. 7. 0 - Spanning Elektriciteit in fase 1: %0.3f %s" % (p1_voltage_l1,p1_unit_voltage_l1) )  
    print ("52. 7. 0 - Spanning Elektriciteit in fase 2: %0.3f %s" % (p1_voltage_l2,p1_unit_voltage_l2) )  
    print ("72. 7. 0 - Spanning Elektriciteit in fase 3: %0.3f %s" % (p1_voltage_l3,p1_unit_voltage_l3) )  
    print ("21. 7. 0 - Instantaan vermogen Elektriciteit levering (+P) in fase 1: %0.3f %s" % (p1_instantaneous_active_power_in_l1,p1_unit_instantaneous_active_power_in_l1) )  
    print ("41. 7. 0 - Instantaan vermogen Elektriciteit levering (+P) in fase 2: %0.3f %s" % (p1_instantaneous_active_power_in_l2,p1_unit_instantaneous_active_power_in_l2) )  
    print ("61. 7. 0 - Instantaan vermogen Elektriciteit levering (+P) in fase 3: %0.3f %s" % (p1_instantaneous_active_power_in_l3,p1_unit_instantaneous_active_power_in_l3) )   
    print ("22. 7. 0 - Instantaan vermogen Elektriciteit teruglevering (-P) in fase 1: %0.3f %s" % (p1_instantaneous_active_power_out_l1,p1_unit_instantaneous_active_power_out_l1) )  
    print ("42. 7. 0 - Instantaan vermogen Elektriciteit teruglevering (-P) in fase 2: %0.3f %s" % (p1_instantaneous_active_power_out_l2,p1_unit_instantaneous_active_power_out_l2) )  
    print ("62. 7. 0 - Instantaan vermogen Elektriciteit teruglevering (-P) in fase 3: %0.3f %s" % (p1_instantaneous_active_power_out_l3,p1_unit_instantaneous_active_power_out_l3) )   
    print ("96.13. 1 - Bericht code: %s" % p1_message_code )
    print ("96.13. 0 - Bericht tekst: %s" % p1_message_text )
    channellist = [p1_channel_1, p1_channel_2, p1_channel_3, p1_channel_4]
    for channel in channellist:
        if channel.id != 0:
            print ("MBus Meterkanaal: %s" % channel.id )
            print ("24. 1. 0 - Productsoort: %s (%s)" % (channel.type_id, channel.type_desc) )
            print ("91. 1. 0 - Meternummer %s: %s" % (channel.type_desc, channel.equipment_id) )
            if p1_dsmr_version != "40":
                print ("24. 3. 0 - Tijdstip meterstand %s levering: %s (Local)" % (channel.type_desc, channel.timestamp) )
                print ("24. 3. 0 - Meterstand %s levering: %0.3f %s" % (channel.type_desc, channel.meterreading, channel.unit) )
            else:
                print ("24. 2. 1 - Tijdstip meterstand %s levering: %s (UTC) / %s (Local)" % (channel.type_desc, channel.timestamp, utc_to_local(datetime.strptime(channel.timestamp, "%Y-%m-%d %H:%M:%S")) ) )            
                print ("24. 2. 1 - Meterstand %s levering: %0.3f %s" % (channel.type_desc, channel.meterreading, channel.unit) )            
            print ("24. 4. 0 - Actuele kleppositie %s: %s" % (channel.type_desc,channel.valveposition) )
    print ("Einde P1 telegram" )
    return
################
#Csv output #
################
def csv_p1_telegram():
#New filename every day
    csv_filename=datetime.strftime(datetime.utcnow(), "P1_"+"%Y-%m-%d_"+str(log_interval)+"s.csv" )
    try:
#If csv_file exists: open it
        csv_file=open(csv_filename, 'rt')
        csv_file.close()
        csv_file=open(csv_filename, 'at', newline='', encoding="utf-8")
        writer = csv.writer(csv_file, dialect='excel', delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
    except IOError:
#Otherwise: create it
        csv_file=open(csv_filename, 'wt', newline='', encoding="utf-8")
        writer = csv.writer(csv_file, dialect='excel', delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
#Write csv-header
        writer.writerow([
         'p1_timestamp', 
         'p1_meter_supplier', 
         'p1_header',
         'p1_dsmr_version',
         'p1_equipment_id', 
         'p1_meterreading_in_1', 
         'p1_unitmeterreading_in_1', 
         'p1_meterreading_in_2', 
         'p1_unitmeterreading_in_2',
         'p1_meterreading_out_1',
         'p1_unitmeterreading_out_1',
         'p1_meterreading_out_2',
         'p1_unitmeterreading_out_2',
         'p1_meterreading_prd',
         'p1_unitmeterreading_prd',
         'p1_current_tariff',
         'p1_current_power_in',
         'p1_unit_current_power_in',
         'p1_current_power_out',
         'p1_unit_current_power_out',
         'p1_current_power_prd',
         'p1_unit_current_power_prd',
         'p1_current_threshold',
         'p1_unit_current_threshold',
         'p1_current_switch_position',
         'p1_powerfailures',
         'p1_long_powerfailures',
         'p1_long_powerfailures_log',
         'p1_voltage_sags_l1',
         'p1_voltage_sags_l2',
         'p1_voltage_sags_l3',
         'p1_voltage_swells_l1',
         'p1_voltage_swells_l2',
         'p1_voltage_swells_l3',
         'p1_instantaneous_current_l1',
         'p1_unit_instantaneous_current_l1',
         'p1_instantaneous_current_l2',
         'p1_unit_instantaneous_current_l2',
         'p1_instantaneous_current_l3',
         'p1_unit_instantaneous_current_l3',
         'p1_voltage_l1',
         'p1_unit_voltage_l1',
         'p1_voltage_l2',
         'p1_unit_voltage_l2',
         'p1_voltage_l3',
         'p1_unit_voltage_l3',             
         'p1_instantaneous_active_power_in_l1',
         'p1_unit_instantaneous_active_power_in_l1',
         'p1_instantaneous_active_power_in_l2',
         'p1_unit_instantaneous_active_power_in_l2',
         'p1_instantaneous_active_power_in_l3',
         'p1_unit_instantaneous_active_power_in_l3',
         'p1_instantaneous_active_power_out_l1',
         'p1_unit_instantaneous_active_power_out_l1',
         'p1_instantaneous_active_power_out_l2',
         'p1_unit_instantaneous_active_power_out_l2',
         'p1_instantaneous_active_power_out_l3',
         'p1_unit_instantaneous_active_power_out_l3',
         'p1_message_code',
         'p1_message_text',
         'p1_channel_1_id',
         'p1_channel_1_type_id', 
         'p1_channel_1_type_desc',
         'p1_channel_1_equipment_id',
         'p1_channel_1_timestamp',
         'p1_channel_1_meterreading', 
         'p1_channel_1_unit',
         'p1_channel_1_valveposition',
         'p1_channel_2_id',
         'p1_channel_2_type_id', 
         'p1_channel_2_type_desc',
         'p1_channel_2_equipment_id',
         'p1_channel_2_timestamp',
         'p1_channel_2_meterreading', 
         'p1_channel_2_unit',
         'p1_channel_2_valveposition',
         'p1_channel_3_id',
         'p1_channel_3_type_id', 
         'p1_channel_3_type_desc',
         'p1_channel_3_equipment_id',
         'p1_channel_3_timestamp',
         'p1_channel_3_meterreading', 
         'p1_channel_3_unit',
         'p1_channel_3_valveposition',
         'p1_channel_4_id',
         'p1_channel_4_type_id', 
         'p1_channel_4_type_desc',
         'p1_channel_4_equipment_id',
         'p1_channel_4_timestamp',
         'p1_channel_4_meterreading', 
         'p1_channel_4_unit',
         'p1_channel_4_valveposition' ])

    print ("P1 telegram in %s gelogd op: %s (UTC) / %s (Local)" % (csv_filename, p1_timestamp_utc, p1_timestamp))
    writer.writerow([
         p1_timestamp_utc, 
         p1_meter_supplier, 
         p1_header, 
         p1_dsmr_version,    
         p1_equipment_id,
         p1_meterreading_in_1, p1_unitmeterreading_in_1, 
         p1_meterreading_in_2, p1_unitmeterreading_in_2,
         p1_meterreading_out_1,p1_unitmeterreading_out_1,
         p1_meterreading_out_2,p1_unitmeterreading_out_2,
         p1_meterreading_prd,p1_unitmeterreading_prd,
         p1_current_tariff,
         p1_current_power_in,p1_unit_current_power_in,
         p1_current_power_out,p1_unit_current_power_out,
         p1_current_power_prd,p1_unit_current_power_prd,
         p1_current_threshold,p1_unit_current_threshold,
         p1_current_switch_position,
         p1_powerfailures,
         p1_long_powerfailures,
         p1_long_powerfailures_log,
         p1_voltage_sags_l1,
         p1_voltage_sags_l2,
         p1_voltage_sags_l3,
         p1_voltage_swells_l1,
         p1_voltage_swells_l2,
         p1_voltage_swells_l3,
         p1_instantaneous_current_l1, p1_unit_instantaneous_current_l1,
         p1_instantaneous_current_l2, p1_unit_instantaneous_current_l2,
         p1_instantaneous_current_l3, p1_unit_instantaneous_current_l3,
         p1_voltage_l1, p1_unit_voltage_l1,
         p1_voltage_l2, p1_unit_voltage_l2,
         p1_voltage_l3, p1_unit_voltage_l3,   
         p1_instantaneous_active_power_in_l1, p1_unit_instantaneous_active_power_in_l1,
         p1_instantaneous_active_power_in_l2, p1_unit_instantaneous_active_power_in_l2,
         p1_instantaneous_active_power_in_l3, p1_unit_instantaneous_active_power_in_l3,
         p1_instantaneous_active_power_out_l1, p1_unit_instantaneous_active_power_out_l1,
         p1_instantaneous_active_power_out_l2, p1_unit_instantaneous_active_power_out_l2,
         p1_instantaneous_active_power_out_l3, p1_unit_instantaneous_active_power_out_l3,
         p1_message_code,
         p1_message_text,
         p1_channel_1.id,
         p1_channel_1.type_id, 
         p1_channel_1.type_desc,
         p1_channel_1.equipment_id,
         p1_channel_1.timestamp,
         p1_channel_1.meterreading, p1_channel_1.unit,
         p1_channel_1.valveposition,
         p1_channel_2.id,
         p1_channel_2.type_id, 
         p1_channel_2.type_desc,
         p1_channel_2.equipment_id,
         p1_channel_2.timestamp,
         p1_channel_2.meterreading, p1_channel_2.unit,
         p1_channel_2.valveposition,
         p1_channel_3.id,
         p1_channel_3.type_id, 
         p1_channel_3.type_desc,
         p1_channel_3.equipment_id,
         p1_channel_3.timestamp,
         p1_channel_3.meterreading, p1_channel_3.unit,
         p1_channel_3.valveposition,
         p1_channel_4.id,
         p1_channel_4.type_id, 
         p1_channel_4.type_desc,
         p1_channel_4.equipment_id,
         p1_channel_4.timestamp,
         p1_channel_4.meterreading, p1_channel_4.unit,
         p1_channel_4.valveposition ])
    csv_file.close()
    
    return        

################
#DB output     #
################
def mysql_p1_telegram():
    query = "insert into p1_log values (\'" + \
         p1_timestamp_utc + "\',\'" + \
         p1_meter_supplier + "\',\'" + \
         p1_header + "\',\'" + \
         p1_dsmr_version + "\',\'" + \
         p1_equipment_id + "\',\'" + \
         str(p1_meterreading_in_1) + "\',\'" + \
         p1_unitmeterreading_in_1 + "\',\'" + \
         str(p1_meterreading_in_2) + "\',\'" + \
         p1_unitmeterreading_in_2 + "\',\'" + \
         str(p1_meterreading_out_1) + "\',\'" +\
         p1_unitmeterreading_out_1 + "\',\'" + \
         str(p1_meterreading_out_2) + "\',\'" + \
         p1_unitmeterreading_out_2 + "\',\'" + \
         str(p1_meterreading_prd) + "\',\'" + \
         p1_unitmeterreading_prd + "\',\'" + \
         str(p1_current_tariff) + "\',\'" + \
         str(p1_current_power_in) + "\',\'" + \
         p1_unit_current_power_in + "\',\'" + \
         str(p1_current_power_out) + "\',\'" + \
         p1_unit_current_power_out + "\',\'" + \
         str(p1_current_power_prd) + "\',\'" + \
         p1_unit_current_power_prd + "\',\'" + \
         str(p1_current_threshold) + "\',\'" + \
         p1_unit_current_threshold + "\',\'" + \
         str(p1_current_switch_position) + "\',\'" + \
         str(p1_powerfailures) + "\',\'" + \
         str(p1_long_powerfailures) + "\',\'" + \
         p1_long_powerfailures_log + "\',\'" + \
         str(p1_voltage_sags_l1)  + "\',\'" + \
         str(p1_voltage_sags_l2) + "\',\'" + \
         str(p1_voltage_sags_l3) + "\',\'" + \
         str(p1_voltage_swells_l1) + "\',\'" + \
         str(p1_voltage_swells_l2) + "\',\'" + \
         str(p1_voltage_swells_l3) + "\',\'" + \
         str(p1_instantaneous_current_l1)  + "\',\'" + \
         p1_unit_instantaneous_current_l1 + "\',\'" + \
         str(p1_instantaneous_current_l2)  + "\',\'" + \
         p1_unit_instantaneous_current_l2 + "\',\'" + \
         str(p1_instantaneous_current_l3)  + "\',\'" + \
         p1_unit_instantaneous_current_l3 + "\',\'" + \
         str(p1_voltage_l1) + "\',\'" + \
         p1_unit_voltage_l1 + "\',\'" + \
         str(p1_voltage_l2) + "\',\'" + \
         p1_unit_voltage_l2 + "\',\'" + \
         str(p1_voltage_l3) + "\',\'" + \
         p1_unit_voltage_l3 + "\',\'" + \
         str(p1_instantaneous_active_power_in_l1)  + "\',\'" + \
         p1_unit_instantaneous_active_power_in_l1 + "\',\'" + \
         str(p1_instantaneous_active_power_in_l2)  + "\',\'" + \
         p1_unit_instantaneous_active_power_in_l2 + "\',\'" + \
         str(p1_instantaneous_active_power_in_l3)  + "\',\'" + \
         p1_unit_instantaneous_active_power_in_l3 + "\',\'" + \
         str(p1_instantaneous_active_power_out_l1)  + "\',\'" + \
         p1_unit_instantaneous_active_power_out_l1 + "\',\'" + \
         str(p1_instantaneous_active_power_out_l2)  + "\',\'" + \
         p1_unit_instantaneous_active_power_out_l2 + "\',\'" + \
         str(p1_instantaneous_active_power_out_l3)  + "\',\'" + \
         p1_unit_instantaneous_active_power_out_l3 + "\',\'" + \
         p1_message_code + "\',\'" + \
         p1_message_text + "\',\'" + \
         str(p1_channel_1.id) + "\',\'" + \
         str(p1_channel_1.type_id) + "\',\'" +  \
         p1_channel_1.type_desc + "\',\'" + \
         str(p1_channel_1.equipment_id) + "\',\'" + \
         p1_channel_1.timestamp + "\',\'" + \
         str(p1_channel_1.meterreading) + "\',\'" + \
         p1_channel_1.unit + "\',\'" + \
         str(p1_channel_1.valveposition) + "\',\'" + \
         str(p1_channel_2.id) + "\',\'" + \
         str(p1_channel_2.type_id) + "\',\'" +  \
         p1_channel_2.type_desc + "\',\'" + \
         str(p1_channel_2.equipment_id) + "\',\'" + \
         p1_channel_2.timestamp + "\',\'" + \
         str(p1_channel_2.meterreading) + "\',\'" + \
         p1_channel_2.unit + "\',\'" + \
         str(p1_channel_2.valveposition) + "\',\'" + \
         str(p1_channel_3.id) + "\',\'" + \
         str(p1_channel_3.type_id) + "\',\'" + \
         p1_channel_3.type_desc + "\',\'" + \
         str(p1_channel_3.equipment_id) + "\',\'" + \
         p1_channel_3.timestamp + "\',\'" + \
         str(p1_channel_3.meterreading) + "\',\'" + \
         p1_channel_3.unit + "\',\'" + \
         str(p1_channel_3.valveposition) + "\',\'" + \
         str(p1_channel_4.id) + "\',\'" + \
         str(p1_channel_4.type_id) + "\',\'" + \
         p1_channel_4.type_desc + "\',\'" + \
         str(p1_channel_4.equipment_id) + "\',\'" + \
         p1_channel_4.timestamp + "\',\'" + \
         str(p1_channel_4.meterreading) + "\',\'" + \
         p1_channel_4.unit + "\',\'" + \
         str(p1_channel_4.valveposition)  + "\')"
#    print(query)
    try:
        db = mysql.connector.connect(user=p1_mysql_user, password=p1_mysql_passwd, host=p1_mysql_host, database=p1_mysql_db)
        c = db.cursor()
        c.execute (query)
        db.commit()
        print ("P1 telegram in database %s / %s gelogd op: %s (UTC) / %s (Local)" % (p1_mysql_host, p1_mysql_db, p1_timestamp_utc, p1_timestamp))
        c.close()
        db.close()
    except:
        show_error()
        print ("Fout bij het openen van / schrijven naar database %s / %s. P1 Telegram wordt gelogd in csv-bestand."  % (p1_mysql_host, p1_mysql_db))      
        csv_p1_telegram()
    return    

def sqlite_p1_telegram():
    query = "insert into p1_log values (\'" + \
         p1_timestamp_utc + "\',\'" + \
         p1_meter_supplier + "\',\'" + \
         p1_header + "\',\'" + \
         p1_dsmr_version + "\',\'" + \
         p1_equipment_id + "\',\'" + \
         str(p1_meterreading_in_1) + "\',\'" + \
         p1_unitmeterreading_in_1 + "\',\'" + \
         str(p1_meterreading_in_2) + "\',\'" + \
         p1_unitmeterreading_in_2 + "\',\'" + \
         str(p1_meterreading_out_1) + "\',\'" +\
         p1_unitmeterreading_out_1 + "\',\'" + \
         str(p1_meterreading_out_2) + "\',\'" + \
         p1_unitmeterreading_out_2 + "\',\'" + \
         str(p1_meterreading_prd) + "\',\'" + \
         p1_unitmeterreading_prd + "\',\'" + \
         str(p1_current_tariff) + "\',\'" + \
         str(p1_current_power_in) + "\',\'" + \
         p1_unit_current_power_in + "\',\'" + \
         str(p1_current_power_out) + "\',\'" + \
         p1_unit_current_power_out + "\',\'" + \
         str(p1_current_power_prd) + "\',\'" + \
         p1_unit_current_power_prd + "\',\'" + \
         str(p1_current_threshold) + "\',\'" + \
         p1_unit_current_threshold + "\',\'" + \
         str(p1_current_switch_position) + "\',\'" + \
         str(p1_powerfailures) + "\',\'" + \
         str(p1_long_powerfailures) + "\',\'" + \
         p1_long_powerfailures_log + "\',\'" + \
         str(p1_voltage_sags_l1)  + "\',\'" + \
         str(p1_voltage_sags_l2) + "\',\'" + \
         str(p1_voltage_sags_l3) + "\',\'" + \
         str(p1_voltage_swells_l1) + "\',\'" + \
         str(p1_voltage_swells_l2) + "\',\'" + \
         str(p1_voltage_swells_l3) + "\',\'" + \
         str(p1_instantaneous_current_l1)  + "\',\'" + \
         p1_unit_instantaneous_current_l1 + "\',\'" + \
         str(p1_instantaneous_current_l2)  + "\',\'" + \
         p1_unit_instantaneous_current_l2 + "\',\'" + \
         str(p1_instantaneous_current_l3)  + "\',\'" + \
         p1_unit_instantaneous_current_l3 + "\',\'" + \
         str(p1_voltage_l1) + "\',\'" + \
         p1_unit_voltage_l1 + "\',\'" + \
         str(p1_voltage_l2) + "\',\'" + \
         p1_unit_voltage_l2 + "\',\'" + \
         str(p1_voltage_l3) + "\',\'" + \
         p1_unit_voltage_l3 + "\',\'" + \
         str(p1_instantaneous_active_power_in_l1)  + "\',\'" + \
         p1_unit_instantaneous_active_power_in_l1 + "\',\'" + \
         str(p1_instantaneous_active_power_in_l2)  + "\',\'" + \
         p1_unit_instantaneous_active_power_in_l2 + "\',\'" + \
         str(p1_instantaneous_active_power_in_l3)  + "\',\'" + \
         p1_unit_instantaneous_active_power_in_l3 + "\',\'" + \
         str(p1_instantaneous_active_power_out_l1)  + "\',\'" + \
         p1_unit_instantaneous_active_power_out_l1 + "\',\'" + \
         str(p1_instantaneous_active_power_out_l2)  + "\',\'" + \
         p1_unit_instantaneous_active_power_out_l2 + "\',\'" + \
         str(p1_instantaneous_active_power_out_l3)  + "\',\'" + \
         p1_unit_instantaneous_active_power_out_l3 + "\',\'" + \
         p1_message_code + "\',\'" + \
         p1_message_text + "\',\'" + \
         str(p1_channel_1.id) + "\',\'" + \
         str(p1_channel_1.type_id) + "\',\'" +  \
         p1_channel_1.type_desc + "\',\'" + \
         str(p1_channel_1.equipment_id) + "\',\'" + \
         p1_channel_1.timestamp + "\',\'" + \
         str(p1_channel_1.meterreading) + "\',\'" + \
         p1_channel_1.unit + "\',\'" + \
         str(p1_channel_1.valveposition) + "\',\'" + \
         str(p1_channel_2.id) + "\',\'" + \
         str(p1_channel_2.type_id) + "\',\'" +  \
         p1_channel_2.type_desc + "\',\'" + \
         str(p1_channel_2.equipment_id) + "\',\'" + \
         p1_channel_2.timestamp + "\',\'" + \
         str(p1_channel_2.meterreading) + "\',\'" + \
         p1_channel_2.unit + "\',\'" + \
         str(p1_channel_2.valveposition) + "\',\'" + \
         str(p1_channel_3.id) + "\',\'" + \
         str(p1_channel_3.type_id) + "\',\'" + \
         p1_channel_3.type_desc + "\',\'" + \
         str(p1_channel_3.equipment_id) + "\',\'" + \
         p1_channel_3.timestamp + "\',\'" + \
         str(p1_channel_3.meterreading) + "\',\'" + \
         p1_channel_3.unit + "\',\'" + \
         str(p1_channel_3.valveposition) + "\',\'" + \
         str(p1_channel_4.id) + "\',\'" + \
         str(p1_channel_4.type_id) + "\',\'" + \
         p1_channel_4.type_desc + "\',\'" + \
         str(p1_channel_4.equipment_id) + "\',\'" + \
         p1_channel_4.timestamp + "\',\'" + \
         str(p1_channel_4.meterreading) + "\',\'" + \
         p1_channel_4.unit + "\',\'" + \
         str(p1_channel_4.valveposition)  + "\')"
#    print(query)
#    print (p1_meterreading_prd)
#    print (p1_current_power_prd)

    try:
        db = sqlite3.connect('p1_log.db')   
        c = db.cursor()
        c.execute (query)
        db.commit()
        print ("P1 telegram in database 'p1_log.db' gelogd op: %s (UTC) / %s (Local)" % (p1_timestamp_utc, p1_timestamp))
        c.close()
        db.close()
    except:
        show_error()
        print ("Fout bij het openen van / schrijven naar database 'p1_log.db' . P1 Telegram wordt gelogd in csv-bestand.")      
        csv_p1_telegram()
    return    

######################
#PVOutput.org output #
######################
def pvo_p1_telegram():
    global pvo_prev_date
    global p1_prev_meterreading_out_1, p1_prev_meterreading_out_2
    global p1_prev_meterreading_in_1, p1_prev_meterreading_in_2    
    global p1_prev_meterreading_channel_1, p1_prev_meterreading_channel_2, p1_prev_meterreading_channel_3, p1_prev_meterreading_channel_4
    if pvo_url[0:7] != "http://":
        print("Invalid PVOutput.org URL to post to, must be of form http://host/service: %s" % pvo_url)
        sys.exit(1)
    url = pvo_url[7:].split('/')
    pvo_host = url[0]
    pvo_service = '/' + '/'.join(url[1:])
#
# d   Date
# t   Time
# v1  energy generation (Wh) => P1 Export
# v2  power generation (W) => P1 Export
# v3  energy consumption (Wh) => P1 Import
# v4  power consumption (W) => P1 Import
# v5  temperature (c)
# v6  voltage (V)
# v7  volume_channel_1, numeric with 3 decimals
# v8  volume_channel_2, numeric with 3 decimals
# v9  volume_channel_3, numeric with 3 decimals
# v10 volume_channel_4, numeric with 3 decimals
# c1  cumulative flag: if set to '1' lifetime values are to be passed
# n   net flag: if set to '1' net import/export values are to be passed
#
# PVOutput gets local time (can't handle UTC)
    pvo_date=datetime.strftime(datetime.strptime(p1_timestamp, "%Y-%m-%d %H:%M:%S" ), "%Y%m%d" )
    pvo_time=datetime.strftime(datetime.strptime(p1_timestamp, "%Y-%m-%d %H:%M:%S" ), "%H:%M" )
    
# Initialize pvo volumes when a new day has started
    if pvo_prev_date != pvo_date:
        print ("PVOutput volumes are reset, previous date: %s, current date: %s" % (pvo_prev_date, pvo_date) )
        p1_prev_meterreading_out_1 = p1_meterreading_out_1
        p1_prev_meterreading_out_2 = p1_meterreading_out_2
        p1_prev_meterreading_in_1 = p1_meterreading_in_1
        p1_prev_meterreading_in_2 = p1_meterreading_in_2
        p1_prev_meterreading_channel_1 = p1_channel_1.meterreading
        p1_prev_meterreading_channel_2 = p1_channel_2.meterreading
        p1_prev_meterreading_channel_3 = p1_channel_3.meterreading
        p1_prev_meterreading_channel_4 = p1_channel_4.meterreading        
        pvo_prev_date = pvo_date
        
    pvo_volume_out=round((p1_meterreading_out_1+p1_meterreading_out_2-p1_prev_meterreading_out_1-p1_prev_meterreading_out_2) * 1000)
    pvo_volume_in=round((p1_meterreading_in_1+p1_meterreading_in_2-p1_prev_meterreading_in_1-p1_prev_meterreading_in_2) *1000)
    pvo_volume_channel_1=round(p1_channel_1.meterreading-p1_prev_meterreading_channel_1,3)
    pvo_volume_channel_2=round(p1_channel_2.meterreading-p1_prev_meterreading_channel_2,3)
    pvo_volume_channel_3=round(p1_channel_3.meterreading-p1_prev_meterreading_channel_3,3)
    pvo_volume_channel_4=round(p1_channel_4.meterreading-p1_prev_meterreading_channel_4,3)    
    
    pvo_power_out=round(p1_current_power_out * 1000)
    pvo_power_in=round(p1_current_power_in * 1000)
    
    print("PVOutput volume out        (v1): %s"% pvo_volume_out)
#    print("MR1 out: %s"% p1_meterreading_out_1)
#    print("MR2 out: %s"% p1_meterreading_out_2)
#    print("Prev MR1 out: %s"% p1_prev_meterreading_out_1)
#    print("Prev MR2 out: %s"% p1_prev_meterreading_out_2)
    print("PVOutput power out         (v2): %s"% pvo_power_out)
    
    print("PVOutput volume in         (v3): %s"% pvo_volume_in)
#    print("MR1 in: %s"% p1_meterreading_in_1)
#    print("MR2 in: %s"% p1_meterreading_in_2)
#    print("Prev MR1 in: %s"% p1_prev_meterreading_in_1)
#    print("Prev MR2 in: %s"% p1_prev_meterreading_in_2)
    print("PVOutput power in          (v4): %s"% pvo_power_in)
    
    print("PVOutput volume channel 1  (v7): %s"% pvo_volume_channel_1)    
    print("PVOutput volume channel 2  (v8): %s"% pvo_volume_channel_2)  
    print("PVOutput volume channel 3  (v9): %s"% pvo_volume_channel_3)  
    print("PVOutput volume channel 4 (v10): %s"% pvo_volume_channel_4)      
    
    pvo_cumulative = 0 # volumes are reset once a day
    pvo_net = 1 #Drawback: net=1 discards the volume data
    params = urllib.parse.urlencode({ 'd' : pvo_date,
                         't'  : pvo_time,
                         'v1' : pvo_volume_out,
                         'v2' : pvo_power_out,
                         'v3' : pvo_volume_in,
                         'v4' : pvo_power_in,                         
                         'c1' : pvo_cumulative,
                         'n'  : pvo_net,
                         'v7' : pvo_volume_channel_1,
                         'v8' : pvo_volume_channel_2,
                         'v9' : pvo_volume_channel_3,
                         'v10' : pvo_volume_channel_4
                         })
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain",
               "X-Pvoutput-SystemId" : pvo_systemid,
               "X-Pvoutput-Apikey" : pvo_apikey}
    print("Verbinden met %s" % pvo_host)
    try:
        conn = http.client.HTTPConnection(pvo_host)
#        print("Sending data: %s" % params)
        try:
            conn.request("POST", pvo_service, params, headers)
            response = conn.getresponse()
            if response.status != 200:
                print ("Fout bij het schrijven naar %s / %s. Response: %s %s %s" % (pvo_host, pvo_systemid, response.status, response.reason, response.read()))
            else: 
                print ("Delta P1 telegram in %s / %s gelogd op: %s (UTC) / %s (Local). Response: %s %s" % (pvo_host, pvo_systemid, p1_timestamp_utc, p1_timestamp,response.status, response.reason))
        except:
            show_error()
            print ("Fout bij het schrijven naar %s / %s."  % (pvo_host, pvo_systemid))      
    except:
        show_error()
        print ("Fout bij het verbinden met %s / %s."  % (pvo_host, pvo_systemid))      
    return
    
######################
#EmonCMS output      #
######################
def emoncms_p1_telegram():
    print ("P1 telegram versturen naar EmonCMS %s." % (emoncms_hostname))
# Prepare the data
    json = "{p1_timestamp_utc:" + p1_timestamp_utc
    json += ",p1_meterreading_in_1:" + "%0.3f"%p1_meterreading_in_1
    json += ",p1_unit_meterreading_in_1:" + p1_unitmeterreading_in_1
    json += ",p1_meterreading_in_2:" + "%0.3f"%p1_meterreading_in_2
    json += ",p1_unit_meterreading_in_2:" + p1_unitmeterreading_in_2
    json += ",p1_meterreading_out_1:" + "%0.3f"%p1_meterreading_out_1
    json += ",p1_unit_meterreading_out_1:" + p1_unitmeterreading_out_1
    json += ",p1_meterreading_out_2:" + "%0.3f"%p1_meterreading_out_2
    json += ",p1_unit_meterreading_out_2:" + p1_unitmeterreading_out_2
    json += ",p1_current_power_in:" + "%0.3f"%p1_current_power_in
    json += ",p1_unit_current_power_in:" + p1_unit_current_power_in
    json += ",p1_current_power_out:" + "%0.3f"%p1_current_power_out
    json += ",p1_unit_current_power_out:" + p1_unit_current_power_out
    json += ",p1_meterreading_prd:" + "%0.3f"%p1_meterreading_prd
    json += ",p1_unit_meterreading_prd:" + p1_unitmeterreading_prd    
    json += ",p1_channel_1.timestamp:" + p1_channel_1.timestamp
    json += ",p1_channel_1.type_desc:" + p1_channel_1.type_desc
    json += ",p1_channel_1.meterreading:" + "%0.3f"%p1_channel_1.meterreading
    json += ",p1_channel_1.unit:" + p1_channel_1.unit
    json += ",p1_channel_2.timestamp:" + p1_channel_2.timestamp
    json += ",p1_channel_2.type_desc:" + p1_channel_2.type_desc
    json += ",p1_channel_2.meterreading:" + "%0.3f"%p1_channel_2.meterreading
    json += ",p1_channel_2.unit:" + p1_channel_2.unit
    json += ",p1_channel_3.timestamp:" + p1_channel_3.timestamp
    json += ",p1_channel_3.type_desc:" + p1_channel_3.type_desc
    json += ",p1_channel_3.meterreading:" + "%0.3f"%p1_channel_3.meterreading
    json += ",p1_channel_3.unit:" + p1_channel_3.unit
    json += ",p1_channel_4.timestamp:" + p1_channel_4.timestamp
    json += ",p1_channel_4.type_desc:" + p1_channel_4.type_desc
    json += ",p1_channel_4.meterreading:" + "%0.3f"%p1_channel_4.meterreading
    json += ",p1_channel_4.unit:" + p1_channel_4.unit + "}"    
    # Prepare the URL 
    url  ="http://" + emoncms_hostname + "/" +  "input/post.json?node=1&apikey=" + emoncms_apikey + "&json=" + urllib.parse.quote(json)
#    print(url)
# Send the data to emoncms
    try:
        response = urllib.request.urlopen(url)
    except URLError as response:
        if hasattr(response, 'reason'):
            print ("Fout bij benaderen van %s. Response: %s %s" % (emoncms_hostname, response.status, response.reason))

        elif hasattr(response, 'code'):
            print ("Fout bij het verwerken van request door %s. Response: %s %s" % (emoncms_hostname, response.status, response.code))
    else:
        print ("P1 telegram verwerkt door %s op: %s (UTC) / %s (Local). Response: %s %s" % (emoncms_hostname, p1_timestamp_utc, p1_timestamp,response.status, response.reason))

    return    
        
#################################################################
# Start of procedures to add other metering data to p1_telegram #
#################################################################

#################################################################
# PV Inverter Data into one of the extra channels               #
#################################################################
def get_pv_data(channelA,p1_channelA,channelB,p1_channelB):
    query = "select pv_timestamp, pv_equipmentmodel, pv_equipmentid, pv_energy_cum, pv_unit_energy_cum, pv_energy_interval, pv_unit_energy_interval, pv_power, pv_unit_power from pv_log order by pv_timestamp desc"
#    print(query)
    try:
        if (output_mode == "mysql"):
            db = mysql.connector.connect(user=p1_mysql_user, password=p1_mysql_passwd, host=p1_mysql_host, database=p1_mysql_db)
        else:
            db = sqlite3.connect('pv_log.db')   
        c = db.cursor()
        c.execute(query)
        pv_timestamp, pv_equipmentmodel, pv_equipmentid, pv_energy_cum, pv_unit_energy_cum, pv_energy_interval, pv_unit_energy_interval, pv_power, pv_unit_power = c.fetchone()
        p1_channelA.id = channelA
        p1_channelA.type_id = 1
        p1_channelA.type_desc = "E-Production volume"
        p1_channelA.equipment_id = pv_equipmentid
        p1_channelA.timestamp = pv_timestamp
        #str(datetime.strftime(pv_timestamp, "%Y-%m-%d %H:%M:%S" ))
        p1_channelA.meterreading = pv_energy_cum
        p1_channelA.unit = pv_unit_energy_cum
        p1_channelA.valveposition = 1
        print ("PV volume %s toegevoegd aan P1 telegram - kanaal %s" % (pv_timestamp, channelA ) )
        if channelB != 0:
            p1_channelB.id = channelB
            p1_channelB.type_id = 1
            p1_channelB.type_desc = "E-Production power"
            p1_channelB.equipment_id = pv_equipmentid
            p1_channelB.timestamp = pv_timestamp
            #str(datetime.strftime(pv_timestamp, "%Y-%m-%d %H:%M:%S" ))
            p1_channelB.meterreading = pv_power
            p1_channelB.unit = pv_unit_power
            p1_channelB.valveposition = 1
            print ("PV vermogen %s toegevoegd aan P1 telegram - kanaal %s" % (pv_timestamp, channelB ) )
        #c.close()
        db.close()
    except:
        show_error()
        if (output_mode == "mysql"):
            print ("Fout bij het openen / lezen van database %s / %s. PV telegram niet opgehaald."  % (p1_mysql_host, p1_mysql_db))
        else:
            print ("Fout bij het openen / lezen van database 'pv_log.db'. PV telegram niet opgehaald.")    

    return
#################################################################
# PV Inverter Data into the prd fields                          #
#################################################################
def get_prd_data():
    global p1_meterreading_prd, p1_unitmeterreading_prd, p1_current_power_prd, p1_unit_current_power_prd
    query = "select pv_timestamp, pv_equipmentmodel, pv_equipmentid, pv_energy_cum, pv_unit_energy_cum, pv_energy_interval, pv_unit_energy_interval, pv_power, pv_unit_power from pv_log order by pv_timestamp desc"
#    print(query)
    try:
        if (output_mode == "mysql"):
            db = mysql.connector.connect(user=p1_mysql_user, password=p1_mysql_passwd, host=p1_mysql_host, database=p1_mysql_db)
        else:
            db = sqlite3.connect('pv_log.db')   
        c = db.cursor()
        c.execute(query)
        pv_timestamp, pv_equipmentmodel, pv_equipmentid, pv_energy_cum, pv_unit_energy_cum, pv_energy_interval, pv_unit_energy_interval, pv_power, pv_unit_power = c.fetchone()
        p1_meterreading_prd = pv_energy_cum
        p1_unitmeterreading_prd = pv_unit_energy_cum
        p1_current_power_prd = pv_power
        p1_unit_current_power_prd = pv_unit_power
        print ("PV volume / vermogen %s toegevoegd aan P1 telegram productie" % pv_timestamp )
        #c.close()
        db.close()
    except:
        show_error()
        if (output_mode == "mysql"):
            print ("Fout bij het openen / lezen van database %s / %s. PV telegram niet opgehaald."  % (p1_mysql_host, p1_mysql_db))
        else:
            print ("Fout bij het openen / lezen van database 'pv_log.db'. PV telegram niet opgehaald.")    

    return
#################################################################
# Heat Data into one of the extra channels                      #
#################################################################
def get_heat_data(channelA,p1_channelA,channelB,p1_channelB):
    query = "select heat_timestamp, heat_equipment_id, heat_meterreading_energy, heat_unitmeterreading_energy, heat_meterreading_volume, heat_unitmeterreading_volume from heat_log order by heat_timestamp desc"
#    print(query)
    try:
        if (output_mode == "mysql"):
            db = mysql.connector.connect(user=p1_mysql_user, password=p1_mysql_passwd, host=p1_mysql_host, database=p1_mysql_db)
        else:
            db = sqlite3.connect('heat_log.db')   
        c = db.cursor()
        c.execute(query)
        heat_timestamp, heat_equipment_id, heat_meterreading_energy, heat_unitmeterreading_energy, heat_meterreading_volume, heat_unitmeterreading_volume = c.fetchone()
        p1_channelA.id = channelA
        p1_channelA.type_id = 5
        p1_channelA.type_desc = "Heat energy"
        p1_channelA.equipment_id = heat_equipment_id
        p1_channelA.timestamp = heat_timestamp
        #str(datetime.strftime(heat_timestamp, "%Y-%m-%d %H:%M:%S" ))
        p1_channelA.meterreading = heat_meterreading_energy
        p1_channelA.unit = heat_unitmeterreading_energy
        p1_channelA.valveposition = 1
        print ("Warmte energie %s toegevoegd aan P1 telegram - kanaal %s" % (heat_timestamp, channelA ) )
        if channelB != 0:
            p1_channelB.id = channelB
            p1_channelB.type_id = 5
            p1_channelB.type_desc = "Heat flow"
            p1_channelB.equipment_id = heat_equipment_id
            p1_channelB.timestamp = heat_timestamp
            #str(datetime.strftime(heat_timestamp, "%Y-%m-%d %H:%M:%S" ))
            p1_channelB.meterreading = heat_meterreading_volume
            p1_channelB.unit = heat_unitmeterreading_volume
            p1_channelB.valveposition = 1
            print ("Warmte flow %s toegevoegd aan P1 telegram - kanaal %s" % (heat_timestamp, channelB ) )
        #c.close()
        db.close()
    except:
        show_error()
        if (output_mode == "mysql"):
            print ("Fout bij het openen / lezen van database %s / %s. Warmte telegram niet opgehaald."  % (p1_mysql_host, p1_mysql_db))
        else:
            print ("Fout bij het openen / lezen van database 'heat_log.db'. Warmte telegram niet opgehaald.")    
    return
#################################################################
# S0 Pulse Counter Data into the prd fields                     #
#################################################################
def get_prd_s0_data(id,meter):
# Use the total s0 volume to improve performance. In the S0 Datalogger, make sure it is not reset!!
    query = "select s0_timestamp, s0_id, s0_m" + meter + "_volume_total, s0_m" + meter + "_volume_total_unit from s0_log where s0_id = '" + id + "' order by s0_timestamp desc limit 1"
#    print(query)
    try:
        if (output_mode == "mysql"):
            db = mysql.connector.connect(user=p1_mysql_user, password=p1_mysql_passwd, host=p1_mysql_host, database=p1_mysql_db)
        else:
            db = sqlite3.connect('s0_log.db')
        c = db.cursor()
        c.execute(query)
        s0_timestamp, s0_id, s0_volume_total, s0_volume_total_unit = c.fetchone()
        pv_timestamp, pv_equipmentmodel, pv_equipmentid, pv_energy_cum, pv_unit_energy_cum, pv_energy_interval, pv_unit_energy_interval, pv_power, pv_unit_power = c.fetchone()
        p1_meterreading_prd = s0_volume_total
        p1_unitmeterreading_prd = s0_volume_total_unit
        p1_current_power_prd = 0
        p1_unit_current_power_prd = ''
        print ("S0 PV volume %s toegevoegd aan P1 telegram productie" % pv_timestamp )
        #c.close()
        db.close()
    except:
        show_error()
        if (output_mode == "mysql"):
            print ("Fout bij het openen / lezen van database %s / %s. S0 telegram niet opgehaald."  % (p1_mysql_host, p1_mysql_db))
        else:
            print ("Fout bij het openen / lezen van database 's0_log.db'. S0 telegram niet opgehaald.")    
    return
#################################################################
# S0 Pulse Counter Data into one of the extra channels          #
#################################################################
def get_s0_data(id,meter,channel,p1_channel,type_id,type_desc):
# Use the total s0 volume to improve performance. In the S0 Datalogger, make sure it is not reset!!
    query = "select s0_timestamp, s0_id, s0_m" + meter + "_volume_total, s0_m" + meter + "_volume_total_unit from s0_log where s0_id = '" + id + "' order by s0_timestamp desc limit 1"
#    print(query)
    try:
        if (output_mode == "mysql"):
            db = mysql.connector.connect(user=p1_mysql_user, password=p1_mysql_passwd, host=p1_mysql_host, database=p1_mysql_db)
        else:
            db = sqlite3.connect('s0_log.db')
        c = db.cursor()
        c.execute(query)
        s0_timestamp, s0_id, s0_volume_total, s0_volume_total_unit = c.fetchone()
        p1_channel.id = channel
        p1_channel.type_id = type_id
        p1_channel.type_desc = type_desc
        p1_channel.equipment_id = s0_id  + "-" + meter
        p1_channel.timestamp = str(datetime.strftime(s0_timestamp, "%Y-%m-%d %H:%M:%S" ))
        p1_channel.meterreading = s0_volume_total
        p1_channel.unit = s0_volume_total_unit
        p1_channel.valveposition = "1"
        print ("S0 %s %s toegevoegd aan P1 telegram - kanaal %s" % (type_desc, s0_timestamp,channel))
        #c.close()
        db.close()
    except:
        show_error()
        if (output_mode == "mysql"):
            print ("Fout bij het openen / lezen van database %s / %s. S0 telegram niet opgehaald."  % (p1_mysql_host, p1_mysql_db))
        else:
            print ("Fout bij het openen / lezen van database 's0_log.db'. S0 telegram niet opgehaald.")    
    return
#################################################################
# Electricity sub-meter  into one of the extra channels         #
#################################################################
def get_power_data(channel,p1_channel,type_id,type_desc):
    query = "select power_timestamp, power_equipment_id, power_meterreading_1_tot, power_unitmeterreading_1_tot from power_log order by power_timestamp desc"
#    print(query)
    try:
        if (output_mode == "mysql"):
            db = mysql.connector.connect(user=p1_mysql_user, password=p1_mysql_passwd, host=p1_mysql_host, database=p1_mysql_db)
        else:
            db = sqlite3.connect('power_log.db')   
        c = db.cursor()
        c.execute(query)
        power_timestamp, power_equipment_id, power_meterreading_1_tot, power_unitmeterreading_1_tot = c.fetchone()
        p1_channel.id = channel
        p1_channel.type_id = type_id
        p1_channel.type_desc = type_desc
        p1_channel.equipment_id = row.power_equipment_id
        p1_channel.timestamp = str(row.power_timestamp)
        p1_channel.meterreading = power_meterreading_1_tot
        p1_channel.unit = power_unitmeterreading_1_tot
        p1_channel.valveposition = "1"
        print ("Elektra %s %s toegevoegd aan P1 telegram - kanaal %s" % (type_desc, power_timestamp,channel) )
        #c.close()
        db.close()
    except:
        show_error()
        if (output_mode == "mysql"):
            print ("Fout bij het openen / lezen van database %s / %s. Elektra telegram niet opgehaald."  % (p1_mysql_host, p1_mysql_db))
        else:
            print ("Fout bij het openen / lezen van database 'power_log.db'. Elektra telegram niet opgehaald.")    

    return
#################################################################
# End of procedures to add other metering data to p1_telegram   #
#################################################################    
    
################################################################################################################################################
#Main program
################################################################################################################################################
print("%s %s" % (progname, version))
comport="TEST"
win_os = (os.name == 'nt')


if win_os:
    print("Windows Mode")
else:
    print("Non-Windows Mode")
print("Python version %s.%s.%s" % sys.version_info[:3])
print("pySerial version %s" % serial.VERSION)
print ("Control-C to abort")

################################################################################################################################################
#Commandline arguments parsing
################################################################################################################################################    
parser = argparse.ArgumentParser(prog=progname, description='P1 Datalogger - www.smartmeterdashboard.nl', epilog="Copyright (c) 2011-2016 J. van der Linde. Although there is a explicit copyright on this sourcecode, anyone may use it freely under a 'Creative Commons Naamsvermelding-NietCommercieel-GeenAfgeleideWerken 3.0 Nederland' license.")
parser.add_argument("-c", "--comport", help="COM-port name (COMx or /dev/...) that identifies the port your P1CC is connected to")


parser.add_argument("-l", "--loginterval", help="Log frequency in seconds, default=30", default=30, type=int)
parser.add_argument("-o", "--output", help="Output mode, default='screen'", default='screen', choices=['screen', 'csv', 'mysql', 'sqlite'])
parser.add_argument("-systime", "--systemtime", help="Use system-time instead of P1 meter-time, default='N'", default='N', choices=['Y', 'N'])

parser.add_argument("-pvo", "--pvoutput", help="Output to PVOutput ==EXPERIMENTAL==, default='N'", default='N', choices=['Y', 'N'])
parser.add_argument("-pvoapi", "--pvoutputapikey", help="PVOutput.org API key")
parser.add_argument("-pvosys", "--pvoutputsystemid", help="PVOutput.org system id", type=int)

parser.add_argument("-emoncmso", "--emoncmsoutput", help="Output EmonCMS ==EXPERIMENTAL==, default='N'", default='N', choices=['Y', 'N'])
parser.add_argument("-emoncmshost", "--emoncmshostname", help="EmonCMS hostname, default='localhost'",default='localhost')
parser.add_argument("-emoncmsapi", "--emoncmsapikey", help="EmonCMS write API key")

parser.add_argument("-s", "--server", help="MySQL server, default='localhost'", default='localhost')
parser.add_argument("-u", "--user", help="MySQL user, default='root'", default='root')
parser.add_argument("-p", "--password", help="MySQL user password, default='password'", default='password')
parser.add_argument("-d", "--database", help="MySQL database name, default=p1'", default='p1')
parser.add_argument("-v", "--version", help="DSMR COM-port setting version, default=4'", choices=['2','3','4'], default='4')
args = parser.parse_args()

if args.comport == None:
    parser.print_help()
    print ("\r")
    print("%s: error: The following arguments are required: -c/--comport." % progname)
    if win_os:
        print("Available ports for argument -c/--comport:") 
        for n,s in scan_serial():
            print ( n, " - ", s )
    else:
        print("Allowed values for argument -c/--comport: Any '/dev/....' string that identifies the port your P1CC is connected to.") 
    print ("Program aborted.")
    sys.exit()
comport = args.comport

use_systemtime = (args.systemtime == "Y")
pvo_output = (args.pvoutput == "Y")
emoncms_output = (args.emoncmsoutput == "Y")
log_interval = args.loginterval

if emoncms_output and (args.emoncmsapikey == None or args.emoncmshostname == None):
    parser.print_help()
    print ("\r")
    print("%s: error: If -emoncmso/--emoncmsoutput is 'Y', the following arguments are required: -emoncmsoapi/--emoncmsoapikey and -emoncmshost/--emoncmshostname." % progname)
    print ("Program aborted.")
    sys.exit()

if pvo_output and (args.pvoutputapikey == None or args.pvoutputsystemid == None):
    parser.print_help()
    print ("\r")
    print("%s: error: If -pvo/--pvoutput is 'Y', the following arguments are required: -pvoapi/--pvoutputapikey and -pvosys/--pvoutputsystemid." % progname)
    print ("Program aborted.")
    sys.exit()

if pvo_output and log_interval < 60:
    log_interval = 60
    print("%s: warning: If -pvo/--pvoutput is 'Y', the log interval should be 60 or higher. Log interval 60 used." % progname)
if import_db and log_interval < 30:
    log_interval = 30
    print("%s: warning: When enriching P1 telegrams with other data, the log interval should be 30 or higher. Log interval 30 used." % progname)
if log_interval < 10:
    log_interval = 10
    print("%s: warning: As data is received only once per 10 seconds, it is useless to set the log interval lower than 10. Log interval 10 used." % progname)
    
output_mode = args.output
dsmr_version = args.version
pvo_apikey = args.pvoutputapikey
pvo_systemid = args.pvoutputsystemid
pvo_prev_date = ""

emoncms_hostname = args.emoncmshostname
emoncms_apikey = args.emoncmsapikey

#Show startup arguments
print ("\r")
print ("Startup parameters:")
print ("Output mode             : %s" % output_mode)
print ("PVOutput.org logging    : %s" % pvo_output)
if pvo_output:
    print ("- PVOutput.org API key  : %s" % pvo_apikey)
    print ("- PVOutput.org system ID: %s" % pvo_systemid)
print ("EmonCMS logging         : %s" % emoncms_output)    
if emoncms_output:
    print ("- EmonCMS API key       : %s" % emoncms_apikey)
    print ("- EmonCMS hostname      : %s" % emoncms_hostname)
print ("Log interval            : %s seconds" % log_interval)
print ("DSMR COM-port setting   : %s" % dsmr_version)
print ("Use system time         : %s" % use_systemtime)

if (output_mode == "mysql") and MySQL_loaded:
    p1_mysql_host=args.server
    p1_mysql_user=args.user
    p1_mysql_passwd=args.password
    p1_mysql_db=args.database   
    print ("MySQL database credentials used:")
    print ("- Server  : %s" % p1_mysql_host)
    print ("- User    : %s" % p1_mysql_user)
    print ("- Password: %s" % p1_mysql_passwd)
    print ("- Database: %s" % p1_mysql_db)
if (output_mode == "mysql") and not MySQL_loaded:
   print("%s: warning: MySQL Connector/Python not found. Output mode 'mysql' not allowed. Output mode 'csv' used instead." % progname)
   output_mode = "csv"
   import_db = False
if (output_mode == "sqlite") and SQLite_loaded:
    print ("SQLite database used  : p1_log.db")
if (output_mode == "sqlite") and not SQLite_loaded:
   print("%s: warning: SQLite module not found. Output mode 'sqlite' not allowed. Output mode 'csv' used instead." % progname)
   output_mode = "csv"
   import_db = False

#################################################################################################################################################
        
#Set COM port config
if comport != "TEST":
    ser = serial.Serial()
    if dsmr_version == '2' or dsmr_version == '3':
        ser.baudrate = 9600
        ser.bytesize=serial.SEVENBITS
        ser.parity=serial.PARITY_EVEN
        ser.stopbits=serial.STOPBITS_ONE
        ser.xonxoff=1
    if dsmr_version == '4':
        ser.baudrate = 115200
        ser.bytesize=serial.EIGHTBITS
        ser.parity=serial.PARITY_NONE
        ser.stopbits=serial.STOPBITS_ONE
        ser.xonxoff=1
    ser.rtscts=0
    ser.timeout=20
    ser.port=str(comport)
    print ("COM-port                : %s" % comport )
else:
    print ("Inputfile assigned      : 'p1test.log'")

#Open COM port
if comport != "TEST":
    try:
        ser.open()
    except:
        sys.exit ("Error opening %s. Program aborted."  % comport)
else:
    try:
        ser = open("p1test.log", "rt")   
    except:
        sys.exit ("Error opening 'p1test.log'. Program aborted.")      


#Initialize
p1_telegram=False
p1_meter_supplier=""
p1_timestamp=""
p1_timestamp_utc=""
p1_dsmr_version="30"
p1_current_threshold=0
p1_unit_current_threshold=""
p1_current_switch_position=1
p1_meterreading_prd = 0
p1_unitmeterreading_prd = ""
p1_current_power_prd = 0
p1_unit_current_power_prd = ""
p1_powerfailures=0
p1_long_powerfailures=0
p1_long_powerfailures_log=""
p1_voltage_sags_l1=0
p1_voltage_sags_l2=0
p1_voltage_sags_l3=0
p1_voltage_swells_l1=0
p1_voltage_swells_l2=0
p1_voltage_swells_l3=0
p1_instantaneous_current_l1=0
p1_unit_instantaneous_current_l1=""
p1_instantaneous_current_l2=0
p1_unit_instantaneous_current_l2=""
p1_instantaneous_current_l3=0
p1_unit_instantaneous_current_l3=""
p1_instantaneous_active_power_in_l1=0
p1_unit_instantaneous_active_power_in_l1=""
p1_instantaneous_active_power_in_l2=0
p1_unit_instantaneous_active_power_in_l2=""
p1_instantaneous_active_power_in_l3=0
p1_unit_instantaneous_active_power_in_l3=""
p1_instantaneous_active_power_out_l1=0
p1_unit_instantaneous_active_power_out_l1=""
p1_instantaneous_active_power_out_l2=0
p1_unit_instantaneous_active_power_out_l2=""
p1_instantaneous_active_power_out_l3=0
p1_unit_instantaneous_active_power_out_l3=""
p1_voltage_l1=0
p1_unit_voltage_l1=""
p1_voltage_l2=0
p1_unit_voltage_l2=""
p1_voltage_l3=0
p1_unit_voltage_l3=""
p1_prev_meterreading_out_1 = 0
p1_prev_meterreading_out_2 = 0
p1_prev_meterreading_in_1 = 0
p1_prev_meterreading_in_2 = 0
p1_prev_meterreading_channel_1 = 0 
p1_prev_meterreading_channel_2 = 0
p1_prev_meterreading_channel_3 = 0
p1_prev_meterreading_channel_4 = 0
pvo_volume_initialize = False
pvo_prev_date=""

while 1:
    p1_line=''
#Read 1 line
    try:
        p1_raw = ser.readline()
    except:
        if comport != "TEST":
            sys.exit ("Error reading %s. Program aborted."  % comport)
            ser.close()
        else:
            sys.exit ("Error reading 'p1test.log'. Program aborted.")                  
            ser.close()
    if comport == "TEST" and len(p1_raw) == 0:
            ser.close()  
            sys.exit ("Finished reading 'p1test.log'. Program ended.")                  
    p1_str=p1_raw
    if comport != "TEST":
        p1_str=str(p1_raw, "utf-8")










    p1_line=p1_str.strip()
    
    #Inspect 1st character
    if p1_line[0:1] == "/":
#Start of new P1 telegram
        p1_telegram=True
#P1 Timestamp to cover DSMR 3 and before OR when use_systemtime is set to True       
        secs_since_epoch=time.time()
        p1_timestamp_utc = time.strftime("%Y-%m-%d %H:%M:%S" , time.gmtime(secs_since_epoch))
        p1_timestamp = time.strftime("%Y-%m-%d %H:%M:%S" , time.localtime(secs_since_epoch))        
#Initialize P1 channeldata
        p1_channel_1=P1_ChannelData()
        p1_channel_2=P1_ChannelData()
        p1_channel_3=P1_ChannelData()
        p1_channel_4=P1_ChannelData()
        
#Only proceed if P1 telegram start is recognized.        
    if p1_telegram:
        if p1_line[0:1] == "/":
#Header information 
#eg. /KMP5 KA6U001511209910 (Kamstrup Enexis)
#eg. /ISk5\2ME382-1003 (InkraEmeco Liander)
#eg. /XMX5XMXABCE000018914 (Landis&Gyr Stedin, Xemex communicatiemodule)
#eg. /KFM5KAIFA-METER (Kaifa)
            p1_meter_supplier=p1_line[1:4]
            p1_header=p1_line
        elif p1_line[4:9] == "1.0.0":
            if  use_systemtime == False:
#P1 Timestamp (DSMR 4)
#eg. 0-0:1.0.0(101209113020W)
                if p1_line[10:23] != "000101010000W":
#Check if meter clock is running
                    p1_timestamp="20"+p1_line[10:12]+"-"+p1_line[12:14]+"-"+p1_line[14:16]+" "+p1_line[16:18]+":"+p1_line[18:20]+":"+p1_line[20:22]
                    p1_timestamp_dt=datetime.strptime(p1_timestamp, "%Y-%m-%d %H:%M:%S")
                    #print("DST indicator: ", p1_line[22:23])
#Determine local time
                    p1_timestamp=p1_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")
                    if p1_line[22:23] == "W":
#No DST, subtract 1 hour to get UTC!                
                        p1_timestamp_dt=p1_timestamp_dt - timedelta(hours=1)
                    else:
#DST, subtract 2 hours to get UTC!  
                        p1_timestamp_dt=p1_timestamp_dt - timedelta(hours=2)
                    p1_timestamp_utc=p1_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    use_systemtime = True
                    print ("%s: warning: invalid P1-telegram date/time value '%s', system date/time used instead: '%s' (UTC) / '%s' (Local)" % (progname, p1_line[10:23], p1_timestamp_utc, p1_timestamp) )
        elif p1_line[4:9] == "0.2.8":
#DSMR Version (DSMR V4)
#eg. 1-3:0.2.8(40)
            p1_lastpos=len(p1_line)-1
            p1_dsmr_version=p1_line[10:p1_lastpos]
            
        elif p1_line[4:10] == "96.1.1":
#####
#Channel 0 = E
#####
#Equipment identifier (Electricity)
#eg. 0-0:96.1.1(204B413655303031353131323039393130)
            p1_lastpos=len(p1_line)-1
            p1_equipment_id=p1_line[11:p1_lastpos]
            
        elif p1_line[4:9] == "1.8.1":
#Meter Reading electricity delivered to client (normal tariff)
#eg. 1-0:1.8.1(00721.000*kWh) (DSMR 3)
#eg. 1-0:1.8.1(000038.851*kWh) (DSMR 4)
#        p1_meterreading_in_1=float(p1_line[10:19])
#        p1_unitmeterreading_in_1=p1_line[20:23]
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_meterreading_in_1=float(p1_line[p1_num_start:p1_num_end])        
            p1_unitmeterreading_in_1=p1_line[p1_num_end+1:p1_lastpos]
        elif p1_line[4:9] == "1.8.2":
#Meter Reading electricity delivered to client (low tariff)
#eg. 1-0:1.8.2(00392.000*kWh)
#        p1_meterreading_in_2=float(p1_line[10:19])
#        p1_unitmeterreading_in_2=p1_line[20:23]
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_meterreading_in_2=float(p1_line[p1_num_start:p1_num_end])        
            p1_unitmeterreading_in_2=p1_line[p1_num_end+1:p1_lastpos]
        elif p1_line[4:9] == "2.8.1":
#Meter Reading electricity delivered by client (normal tariff)
#eg. 1-0:2.8.1(00000.000*kWh)
#        p1_meterreading_out_1=float(p1_line[10:19])
#        p1_unitmeterreading_out_1=p1_line[20:23]
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_meterreading_out_1=float(p1_line[p1_num_start:p1_num_end])        
            p1_unitmeterreading_out_1=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:9] == "2.8.2":
#Meter Reading electricity delivered by client (low tariff)
#eg. 1-0:2.8.2(00000.000*kWh)
#        p1_meterreading_out_2=float(p1_line[10:19])
#        p1_unitmeterreading_out_2=p1_line[20:23]
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_meterreading_out_2=float(p1_line[p1_num_start:p1_num_end])        
            p1_unitmeterreading_out_2=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:11] == "96.14.0":
#Tariff indicator electricity
#eg. 0-0:96.14.0(0001)
#alternative 0-0:96.14.0(1)
            p1_lastpos=len(p1_line)-1
            p1_current_tariff=int(p1_line[12:p1_lastpos])

        elif p1_line[4:9] == "1.7.0":
#Actual electricity power delivered to client (+P)
#eg. 1-0:1.7.0(0000.91*kW)
#        p1_current_power_in=float(p1_line[10:17])
#        p1_unit_current_power_in=p1_line[18:20]
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_current_power_in=float(p1_line[p1_num_start:p1_num_end])        
            p1_unit_current_power_in=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:9] == "2.7.0":
#Actual electricity power delivered by client (-P)
#1-0:2.7.0(0000.00*kW)
#        p1_current_power_out=float(p1_line[10:17])
#        p1_unit_current_power_out=p1_line[18:20]
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_current_power_out=float(p1_line[p1_num_start:p1_num_end])        
            p1_unit_current_power_out=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:10] == "17.0.0":
#Actual threshold Electricity
#Companion standard, eg Kamstrup, Xemex
#eg. 0-0:17.0.0(999*A)
#Iskraemeco
#eg. 0-0:17.0.0(0999.00*kW)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_current_threshold=float(p1_line[p1_num_start:p1_num_end])        
            p1_unit_current_threshold=p1_line[p1_num_end+1:p1_lastpos]
                 
        elif p1_line[4:11] == "96.3.10":
#Actual switch position Electricity (in/out/enabled).
#eg. 0-0:96.3.10(1), default to 1
            p1_value=p1_line[12:13]
            if not isinstance(p1_value, int):
               p1_value=1
            p1_current_switch_position=int(p1_value)
        elif p1_line[4:11] == "96.7.21":
#Number of powerfailures in any phase (DSMR4)
#eg. 0-0:96.7.21(00004)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_powerfailures=int(float(p1_line[p1_num_start:p1_lastpos]))
        
        elif p1_line[4:10] == "96.7.9":
#Number of long powerfailures in any phase (DSMR4)
#eg. 0-0:96.7.9(00002)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_long_powerfailures=int(float(p1_line[p1_num_start:p1_lastpos]))
        
        elif p1_line[4:11] == "99.97.0":
#Powerfailure eventlog (DSMR4)
#eg. 1-0:99:97.0(2)(0:96.7.19)(101208152415W)(0000000240*s)(101208151004W)(00000000301*s)
#    1-0:99.97.0(0)(0-0:96.7.19)
            p1_lastpos=len(p1_line)
            p1_log_start= p1_line.find("0:96.7.19") +10
            p1_long_powerfailures_log=p1_line[p1_log_start:p1_lastpos]
        
        elif p1_line[4:11] == "32.32.0":
#Number of Voltage sags L1 (DSMR4)
#eg. 1-0:32.32.0(00002)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_voltage_sags_l1=int(float(p1_line[p1_num_start:p1_lastpos]))

        elif p1_line[4:11] == "52.32.0":
#Number of Voltage sags L2 (DSMR4)
#eg. 1-0:52.32.0(00002)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_voltage_sags_l2=int(float(p1_line[p1_num_start:p1_lastpos]))

        elif p1_line[4:11] == "72.32.0":
#Number of Voltage sags L3 (DSMR4)
#eg. 1-0:72.32.0(00002)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_voltage_sags_l3=int(float(p1_line[p1_num_start:p1_lastpos]))
        
        elif p1_line[4:11] == "32.36.0":
#Number of Voltage swells L1 (DSMR4)
#eg. 1-0:32.36.0(00002)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_voltage_swells_l1=int(float(p1_line[p1_num_start:p1_lastpos]))

        elif p1_line[4:11] == "52.36.0":
#Number of Voltage swells L2 (DSMR4)
#eg. 1-0:52.36.0(00002)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_voltage_swells_l2=int(float(p1_line[p1_num_start:p1_lastpos]))

        elif p1_line[4:11] == "72.36.0":
#Number of Voltage swells L3 (DSMR4)
#eg. 1-0:72.36.0(00002)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_voltage_swells_l3=int(float(p1_line[p1_num_start:p1_lastpos]))

        elif p1_line[4:10] == "31.7.0":
#Instantaneous current L1 in A (DSMR4)
#eg. 1-0:31.7.0.255(001*A)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_current_l1=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_current_l1=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:10] == "51.7.0":
#Instantaneous current L2 in A (DSMR4)
#eg. 1-0:51.7.0.255(002*A)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_current_l2=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_current_l2=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:10] == "71.7.0":
#Instantaneous current L3 in A (DSMR4)
#eg. 1-0:71.7.0.255(003*A)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_current_l3=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_current_l3=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:10] == "21.7.0":
#Instantaneous active power L1 (+P) in W (DSMR4)          
#eg 1-0:21.7.0.255(01.111*kW)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_active_power_in_l1=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_active_power_in_l1=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:10] == "41.7.0":
#Instantaneous active power L2 (+P) in W (DSMR4)           
#eg 1-0:41.7.0.255(02.222*kW)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_active_power_in_l2=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_active_power_in_l2=p1_line[p1_num_end+1:p1_lastpos]            

        elif p1_line[4:10] == "61.7.0":
#Instantaneous active power L3 (+P) in W (DSMR4)           
#eg 1-0:61.7.0.255(03.333*kW)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_active_power_in_l3=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_active_power_in_l3=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:10] == "22.7.0":
#Instantaneous active power L1 (+P) in W  (DSMR4)          
#eg 1-0:22.7.0.255(04.444*kW)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_active_power_out_l1=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_active_power_out_l1=p1_line[p1_num_end+1:p1_lastpos]

        elif p1_line[4:10] == "42.7.0":
#Instantaneous active power L2 (+P) in W  (DSMR4)          
#eg 1-0:42.7.0.255(05.555*kW)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_active_power_out_l2=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_active_power_out_l2=p1_line[p1_num_end+1:p1_lastpos]            

        elif p1_line[4:10] == "62.7.0":
#Instantaneous active power L3 (+P) in W (DSMR4)           
#eg 1-0:62.7.0.255(06.666*kW)
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_instantaneous_active_power_out_l3=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_instantaneous_active_power_out_l3=p1_line[p1_num_end+1:p1_lastpos]    

        elif p1_line[4:10] == "32.7.0":
#Voltage level L1 in V (DSMR4)            
#1-0:32.7.0(00234*V) 
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_voltage_l1=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_voltage_l1=p1_line[p1_num_end+1:p1_lastpos]   

        elif p1_line[4:10] == "52.7.0":            
#Voltage level L2 in V (DSMR4)            
#1-0:52.7.0(00234*V) 
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_voltage_l2=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_voltage_l2=p1_line[p1_num_end+1:p1_lastpos]    

        elif p1_line[4:10] == "72.7.0":            
#Voltage level L3 in V (DSMR4)            
#1-0:72.7.0(00234*V) 
            p1_lastpos=len(p1_line)-1
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_voltage_l3=int(float(p1_line[p1_num_start:p1_num_end]))
            p1_unit_voltage_l3=p1_line[p1_num_end+1:p1_lastpos]  
            
        elif p1_line[4:11] == "96.13.1":
#Text message code: numeric 8 digits
#eg. 0-0:96.13.1()
            p1_lastpos=len(p1_line)-1
#        p1_message_code=p1_line[12:p1_lastpos]
            p1_message_code=bytes.fromhex(p1_line[12:p1_lastpos]).decode('utf-8')
        elif p1_line[4:11] == "96.13.0":
    #Text message max 1024 characters.
    #eg. 0-0:96.13.0()
            p1_lastpos=len(p1_line)-1
            p1_message_text=bytes.fromhex(p1_line[12:p1_lastpos]).decode('utf-8')
#        p1_line[12:p1_lastpos]
#####
#Channels 1/2/3/4: MBus connected meters
#####
        elif p1_line[4:10] == "24.1.0":
#Device-Type
#eg. 0-1:24.1.0(3)
#or 0-1:24.1.0(03) 3=Gas;5=Heat;6=Cooling
#or 0-1:24.1.0(03) 3/7=Gas;5=Heat;6=Cooling (Standard OBIS: 1-Electricity / 4-HeatCostAllocation / 5-Cooling / 6-Heat / 7-Gas / 8-ColdWater / 9-HotWater)

            p1_channel=int(p1_line[2:3])
            p1_lastpos=len(p1_line)-1
            p1_value=int(p1_line[11:p1_lastpos])
            if p1_value in [3,7]:
                 p1_value2="Gas"
            elif p1_value == 4:
                 p1_value2="HeatCost"                 
            elif p1_value == 5:
                 p1_value2="Heat"
            elif p1_value == 6:
                 p1_value2="Cold"
            elif p1_value == 8:
                 p1_value2="Cold water"
            elif p1_value == 9:
                 p1_value2="Hot water"             
            else:
                 p1_value2="Unknown"
#self, id=None, type_id=None, type_desc=None, equipment_id=None, timestamp=None, meterreading=None, unit=None, valveposition=None
            if p1_channel==1:
                p1_channel_1.id=p1_channel
                p1_channel_1.type_id = p1_value
                p1_channel_1.type_desc= p1_value2
            elif p1_channel==2:
                p1_channel_2.id=p1_channel
                p1_channel_2.type_id = p1_value
                p1_channel_2.type_desc= p1_value2
            elif p1_channel==3:
                p1_channel_3.id=p1_channel
                p1_channel_3.type_id = p1_value
                p1_channel_3.type_desc= p1_value2
            elif p1_channel==4:
                p1_channel_4.id=p1_channel
                p1_channel_4.type_id = p1_value
                p1_channel_4.type_desc= p1_value2

        elif p1_line[4:10] == "96.1.0":
#Equipment identifier
#eg. 0-1:96.1.0(3238303039303031303434303132303130)
            p1_channel=int(p1_line[2:3])
            p1_lastpos=len(p1_line)-1
            p1_value=p1_line[11:p1_lastpos]
#self, id=None, type_id=None, type_desc=None, equipment_id=None, timestamp=None, meterreading=None, unit=None, valveposition=None
            if p1_channel==1:
                p1_channel_1.equipment_id=p1_value
            elif p1_channel==2:
                p1_channel_2.equipment_id=p1_value
            elif p1_channel==3:
                p1_channel_3.equipment_id=p1_value
            elif p1_channel==4:
                p1_channel_4.equipment_id=p1_value

        elif p1_line[4:10] == "24.3.0":
#Last hourly value delivered to client (DSMR < V4)
#eg. Kamstrup/Iskraemeco:
#0-1:24.3.0(110403140000)(000008)(60)(1)(0-1:24.2.1)(m3)
#(00437.631)
#eg. Companion Standard:
#0-1:24.3.0(110403140000)(000008)(60)(1)(0-1:24.2.1)(m3)(00437.631)
            p1_channel=int(p1_line[2:3])
#DSMR3 Gas datetime is naive, for now just store the datetime            
            p1_channel_timestamp="20"+p1_line[11:13]+"-"+p1_line[13:15]+"-"+p1_line[15:17]+" "+p1_line[17:19]+":"+p1_line[19:21]+":"+p1_line[21:23]
            p1_lastpos=len(p1_line)-1
#Value is in next line
            p1_unit=p1_line[p1_lastpos-2:p1_lastpos]

#Read 1 line
            p1_raw = ser.readline()
            p1_str=p1_raw
            if comport != "TEST":
                p1_str=str(p1_raw, "utf-8")





















                p1_line=p1_str.strip()

#self, id=None, type_id=None, type_desc=None, equipment_id=None, timestamp= None, meterreading=None, unit=None, valveposition=None
            if p1_channel==1:
                p1_channel_1.timestamp=p1_channel_timestamp
                p1_channel_1.meterreading=float(p1_line[1:10])
                p1_channel_1.unit=p1_unit
            elif p1_channel==2:
                p1_channel_2.timestamp=p1_channel_timestamp
                p1_channel_2.meterreading=float(p1_line[1:10])
                p1_channel_2.unit=p1_unit
            elif p1_channel==3:
                p1_channel_3.timestamp=p1_channel_timestamp
                p1_channel_3.meterreading=float(p1_line[1:10])
                p1_channel_3.unit=p1_unit
            elif p1_channel==4:
                p1_channel_4.timestamp=p1_channel_timestamp
                p1_channel_4.meterreading=float(p1_line[1:10])
                p1_channel_4.unit=p1_unit

        elif p1_line[4:10] == "24.2.1":
#Last hourly value delivered to client (DSMR v4)
#eg. 0-1:24.2.1(101209110000W)(12785.123*m3)
            p1_channel=int(p1_line[2:3])
            p1_channel_timestamp="20"+p1_line[11:13]+"-"+p1_line[13:15]+"-"+p1_line[15:17]+" "+p1_line[17:19]+":"+p1_line[19:21]+":"+p1_line[21:23]
            p1_channel_timestamp_dt=datetime.strptime(p1_channel_timestamp, "%Y-%m-%d %H:%M:%S")
            #print("Channel DST indicator: ", p1_line[23:24])
            if p1_line[23:24] == "W":
#No DST, subtract 1 hour to get UTC!                
                p1_channel_timestamp_dt=p1_channel_timestamp_dt - timedelta(hours=1)
            else:
#DST, subtract 2 hours to get UTC!  
                p1_channel_timestamp_dt=p1_channel_timestamp_dt - timedelta(hours=2)
            p1_channel_timestamp=p1_channel_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")
            
            p1_lastpos=len(p1_line)-1
            p1_line=p1_line[25:p1_lastpos]
            p1_lastpos=len(p1_line)
            p1_num_start = p1_line.find("(") +1
            p1_num_end = p1_line.find("*")
            p1_value=float(p1_line[p1_num_start:p1_num_end])        
            p1_unit=p1_line[p1_num_end+1:p1_lastpos]
#self, id=None, type_id=None, type_desc=None, equipment_id=None, timestamp= None, meterreading=None, unit=None, valveposition=None
            if p1_channel==1:
                p1_channel_1.timestamp=p1_channel_timestamp
                p1_channel_1.meterreading=p1_value
                p1_channel_1.unit=p1_unit
            elif p1_channel==2:
                p1_channel_2.timestamp=p1_channel_timestamp
                p1_channel_2.meterreading=p1_value
                p1_channel_2.unit=p1_unit
            elif p1_channel==3:
                p1_channel_3.timestamp=p1_channel_timestamp
                p1_channel_3.meterreading=p1_value
                p1_channel_3.unit=p1_unit
            elif p1_channel==4:
                p1_channel_4.timestamp=p1_channel_timestamp
                p1_channel_4.meterreading=p1_value
                p1_channel_4.unit=p1_unit

        elif p1_line[4:10] == "24.4.0":
#Valve position (on/off/released)
#eg. 0-1:24.4.0()
#eg. 0-1:24.4.0(1)
#Valveposition defaults to '1'(=Open) if invalid value
            p1_channel=int(p1_line[2:3])
            p1_lastpos=len(p1_line)-1
            p1_value=p1_line[12:p1_lastpos].strip()
            if not isinstance(p1_value, int):
               p1_value=1
            if p1_channel==1:
                p1_channel_1.valveposition=p1_value
            elif p1_channel==2:
                p1_channel_2.valveposition=p1_value
            elif p1_channel==3:
                p1_channel_3.valveposition=p1_value
            elif p1_channel==4:
                p1_channel_4.valveposition=p1_value

        elif p1_line[0:1] == "" or p1_line[0:1] == " ":
#Empty line
            p1_value=""
            
        elif p1_line[0:1] == "!":
#End of P1 telegram
#
#in DSMR 4 telegrams there might be a checksum following the "!".
#eg. !141B
#CRC16 value calculated over the preceding characters in the data message (from “/” to “!” using the polynomial: x16+x15+x2+1).
#the checksum is discarded
#





#Output if we also have the start-of-telegram
             if p1_telegram == True:
################################################################
#Start of functionality to add other meterdata to p1-telegram  #
################################################################
#Comment out / remove when not applicable                      #
################################################################
                if import_db:
                    pass
######################HEAT: Mandatory 1st ChannelID, 1st ChannelDataElement, optional 2nd ChannelID, 2nd ChannelDataElement
#                   get_heat_data(1,p1_channel_1,2,p1_channel_2)
######################POWER SUB METERING: ChannelID, ChannelDataElement, TypeID, TypeDescription
# Gebruiken van Power date in channel # als productie.
#                   get_power_data(#,p1_channel_#,1,"E-Production volume")
######################S0 SUB METERING: S0-ID, S0-Register, ChannelID, ChannelDataElement, TypeID, TypeDescription
#                   get_s0_data('25325','1',3,p1_channel_3,1,"E-Production volume")
######################S0 SUB METERING: S0-ID, S0-Register into PRD
#                   get_prd_s0_data('25325','1')
######################PV INVERTER into channel: Mandatory 1st ChannelID, 1st ChannelDataElement, optional 2nd ChannelID, 2nd ChannelDataElement
#                   get_pv_data(3,p1_channel_3,4,p1_channel_4)
######################PV INVERTER into PRD
#                   get_prd_data()
################################################################
#End of functionality to add other meterdata to p1-telegram    #
################################################################
#Output to screen
                if output_mode=="screen": print_p1_telegram()
#Output to csv_file
                if output_mode=="csv": csv_p1_telegram()
#Output to database
                if output_mode=="mysql": mysql_p1_telegram()
                if output_mode=="sqlite": sqlite_p1_telegram()             
#Output to PVOutput.org
                if pvo_output: pvo_p1_telegram()
#Output to EmonCMS
                if emoncms_output: emoncms_p1_telegram()                
################################################################
                p1_telegram=False
                sleep(log_interval)
        else:
#Always dump unrecognized data in identified telegram to screen
            print ("Error interpreting P1-telegram, unrecognized data encountered: '%s'. Wait for new telegram." % p1_line )
#Discard telegram with unrecognized data, make sure to wait for new telegram, to avoid that data gets mixed
            p1_telegram=False 

#Close port and show status
try:
    ser.close()
except:
    sys.exit ("Error opening %s. Program aborted."  % comport)
      









