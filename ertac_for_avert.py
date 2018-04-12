#!/usr/bin/python

# ertac_for_avert.py

"""ERTAC EGU AVERT Converter"""

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

VERSION = "2.1"

import sys
try:
    import getopt, logging, os, time, re, csv, datetime, StringIO, httplib, calendar
except ImportError:
    print >> sys.stderr, "Fatal error: can't import all required modules."
    print >> sys.stderr, "Run python -V to find your Python version."
    raise


try:
    import sqlite3
except ImportError:
    try:
        from pysqlite2 import dbapi2 as sqlite3
    except ImportError:
        print >> sys.stderr, "Fatal error: can't import all required modules."
        print >> sys.stderr, "No SQLite3 available with this Python."
        raise

try:
    import ertac_lib, ertac_tables, ertac_reports, geonames
except ImportError:
    print >> sys.stderr, "Fatal error: can't import all required modules."
    print >> sys.stderr, "Put all ERTAG EGU library code in directory with preprocessor and projection model."
    raise

# File locations:
#
# All CSV input and output data files should be in the current directory.
#
# Input file names are the hard-coded CSV files listed below, optionally
# prefixed with the string specified with the -i command-line switch.
#
# All Python and SQL code files and the fixed CSV files for lookup tables can
# (and should) be in a separate non-data directory, but must be kept together.
#
# Example usage: assuming for example that there is a valid set of intermediate
# CSV files present in ~/va_data and that the program code files are in
# ~/ertac_code, then change into the data directory and run the post processing
# program by the following two commands:
#
# cd ~/va_data
# ../ertac_code/ertac_posprocess.py


def usage(progname):
    """Print brief usage message showing command-line options.

    Keyword arguments:
    progname -- program name to be inserted in usage message

    """

    print """
Usage: %s [OPTION]...

  -h, --help        print this message.

  -d, --debug       log extended debugging information.
  -q, --quiet       quiet operation (no status messages).
  -v, --verbose     verbose status messages (default).
  --run-integrity.                      run a check for debugging to make sure gload totals match by region, fuel bin, and in total
  --sql-database=existing database.     use sql database at location rather than loading inputs
  --input-prefix-pre=prefix.            prefix used in preprocessor
  --input-prefix-proj=prefix.           prefix used in projection
  --input-prefix-pp=prefix.             prefix used in post processing
  --state=state                         limit resutls to state or comma separate list of states
  -o prefix, --output-prefix=prefix.    output prefix for postprocessor results
""" % progname
avert_unit_inputs_columns = (
               ('ORISPL_Code', 'str', True, None),
               ('Unit ID', 'str', True, None),
               ('ertac fuel unit type bin', 'str', True, ertac_tables.fuel_set),
               ('eGRID Region', 'str', False, None)) #EGRID 2010 is preferable, but 

avert_unit_information_columns = (
               ('key', 'str', False, None),
               ('Match Row', 'str', False, None),
               ('Data Year', 'int', True, None),
               ('State', 'str', True, ertac_tables.state_set),
               ('County', 'str', True, None),
               ('Facility Name', 'str', True, None),
               ('ORISPL_Code', 'str', True, None),
               ('Unit ID', 'str', True, None),
               ('Lat', 'str', False, None),
               ('Long', 'str', False, None),
               ('needs_ipm_region', 'str', False, None),
               ('nerc_main_region', 'str', False, None),
               ('Owner', 'str', False, None),
               ('Clean Air Programs', 'str', False, None),
               ('Unit Type', 'str', True, ertac_tables.fuel_set),
               ('primary_fuel_type', 'str', False, None),
               ('secondary_or_substitute_fuel', 'str', False, None),
               ('SO2 Controls', 'str', False, None),
               ('NOX Controls', 'str', False, None),
               ('PM Controls', 'str', False, None),
               ('Fuel Definition', 'str', True, None),
               ('Unit Type Definition', 'str', False, None),
               ('eGRID Match', 'str', False, None),
               ('eGRID Region', 'str', False, None),
               ('AVERT Region Match', 'str', False, None),
               ('AVERT Region', 'str', False, None),
               ('AVERT Region Manual', 'str', False, None),
               ('Final AVERT Region', 'str', False, None),
               ('CapacityGen Match', 'str', False, None),
               ('Maximum Capacity (MW)', 'float', False, None),
               ('Gross Generation (MWh)', 'float', True, None), 
               ('Gross Heat Input (mmbtu)', 'float', True, None), 
               ('Gross CO2_mass (lb/hr)', 'float', True, None), 
               ('Gross SO2_mass (lb/hr)', 'float', True, None), 
               ('Gross NOX_mass (lbs/hr)', 'float', True, None), 
               ('Capacity Factor', 'float', False, None),
               ('ORSPL UNITID', 'str', True, None),
               ('AVERT Region', 'str', False, None))

unit_level_CO2_addon_enhanced = (('ORIS', None, None, None),
                       ('Unit ID', None, None, None),
                       ('Facility', None, None, None),
                       ('State', None, None, None),
                       ('ERTAC Region', None, None, None),
                       ('Fuel/Unit Type Bin', None, None, None),
                       ('Maximum hourly heat input (mmbtu)', None, None, None),
                       ('ERTAC heat rate (btu/kw-hr)', None, None, None),
                       ('Generation capacity (MW)', None, None, None),
                       ('# of hours in FY where unit operated at max hourly', None, None, None),
                       ('BY Utilization fraction', None, None, None),
                       ('FY Utilization fraction', None, None, None),
                       ('Base year generation (MW-hrs)', None, None, None),
                       ('Base year heat input (mmbtu)', None, None, None),
                       ('Base year CO2 (tons)', None, None, None),
                       ('Base year CO2 rate (tons/mmbtu)', None, None, None),
                       ('Base year CO2 rate (lbs/MWhr)', None, None, None),
                       ('Future year generation (MW-hrs)', None, None, None),
                       ('Future year heat input (mmbtu)', None, None, None),
                       ('Future year CO2 (tons)', None, None, None),
                       ('Future year CO2 rate (tons/mmbtu)', None, None, None),
                       ('Future year CO2 rate (lbs/MWhr)', None, None, None),
                       ('Base year SO2 rate (tons/mmbtu)', None, None, None),
                       ('Future year SO2 rate (tons/mmbtu)', None, None, None),
                       ('Primary Fuel Type', None, None, None),
                       ('New Unit?', None, None, None),
                       ('General Deficit Unit?', None, None, None),
                       ('Gasified?', None, None, None),
                       ('Online Start Date', None, None, None),
                       ('Retirement Date', None, None, None),
                       ('Longitude', None, None, None),
                       ('Latitude', None, None, None),
                       ("Is Unit NSPS Applicable?", None, None, None))

annual_summary_columns = (('oris', 'str', True, None),
                       ('unit id', 'str', True, None),
                       ('Facility Name', 'str', False, None),
                       ('State', 'str', True, ertac_tables.state_set),
                       ('FIPS Code', 'str', False, None),
                       ('ertac region', 'str', True, None),
                       ('ertac fuel unit type bin', 'str', True, ertac_tables.fuel_set),
                       ('BY ertac fuel unit type bin', 'str', True, ertac_tables.fuel_set),
                       ('max unit heat input (mmBtu)', 'float', False, None),
                       ('ertac heat rate (btu/kw-hr)', 'float', False, (3000.0, 20000.0)),
                       ('Generation Capacity (MW)', 'float', False, None),
                       ('Nameplate Capacity (MW)', 'float', False, None),
                       ('Number of FY Hours Operating', 'int', True, (0, 8760)),
                       ('Number of FY Hours Operating at Max', 'int', True, (0, 8760)),
                       ('BY Utilization fraction', 'float', False, (0.0, 1.0)),
                       ('FY Utilization fraction', 'float', False, (0.0, 1.0)),
                       ('Base year generation (MW-hrs)', 'float', False, None),
                       ('Base year heat input (mmbtu)', 'float', False, None),
                       ('Future year generation (MW-hrs)', 'float', False, None),
                       ('Future year heat input (mmbtu)', 'float', False, None),
                       ('BY Annual SO2 (tons)', 'float', False, None),
                       ('BY Average Annual SO2 Rate (lbs/mmbtu)', 'float', False, None),
                       ('BY Annual NOx (tons)', 'float', False, None),
                       ('BY Average Annual NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('BY OS NOx (tons)', 'float', False, None),
                       ('BY Average OS NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('BY OS heat input (mmbtu)', 'float', False, None),
                       ('BY OS generation (MW-hrs)', 'float', False, None),
                       ('BY NonOS NOx (tons)', 'float', False, None),
                       ('BY Average NonOS NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('FY Annual SO2 (tons)', 'float', False, None),
                       ('FY Average Annual SO2 Rate (lbs/mmbtu)', 'float', False, None),
                       ('FY Annual NOx (tons)', 'float', False, None),
                       ('FY Average Annual NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('FY OS NOx (tons)', 'float', False, None),
                       ('FY Average OS NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('FY OS heat input (mmbtu)', 'float', False, None),
                       ('FY OS generation (MW-hrs)', 'float', False, None),
                       ('FY NonOS NOx (tons)', 'float', False, None),
                       ('FY Average NonOS NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('Hierarchy Order', 'int', False, None),
                       ('Longitude', 'float', False, None),
                       ('Latitude', 'float', False, None),
                       ('Generation Deficit Unit?', 'str', False, ['Y','N']),
                       ('Retirement Date', 'str', False, None),
                       ('New Unit?', 'str', False, ['Y','N']),
                       ('data type', 'str', False, None))


def export_array_to_csv(array_data, prefix, basic_csv_file, connection, column_types, logfile):
    """Export table contents to a CSV file.

    Keyword arguments:
    table_name -- name of table to export
    prefix -- optional prefix added to each output file name
    basic_csv_file -- basic name of CSV file to be written, without prefix
    connection -- a valid database connection
    column_types -- a group of tuples describing each column, with column headers
    logfile -- file where logging messages will be written

    """
    if prefix is None:
        prefix = ""
    csv_file = prefix + basic_csv_file

    logging.info("  " + csv_file)
    print >> logfile
    try:
        cf = open(csv_file, 'wb')
    except IOError:
        print >> logfile, "File: " + csv_file + " -- Could not be written."
        return

    cw = csv.writer(cf)
    row_count = 0
    for row in array_data:
        cw.writerow(row)
        row_count += 1

    print >> logfile, "Wrote out", row_count, "data rows from array to file: " + csv_file
    
def load_intermediate_data(conn, in_prefix_pre, in_prefix_proj, input_prefix_pp, input_prefix_avert, logfile):
    """Load intermediate ERTAC EGU data from preprocessor for projection.

    Keyword arguments:
    conn           -- a valid database connection where the data will be stored
    in_prefix_pre  -- optional prefix added to each input file name generated from preprocessor
    in_prefix_post -- optional prefix added to each input file name generated from projection
    input_type     -- either ERTAC hourly diagnostic file or CAMD 
    logfile        -- file where logging messages will be written

    """

    ertac_lib.load_csv_into_table(None, os.path.join(os.path.relpath(sys.path[0]), 'states.csv'), 'states', conn, ertac_tables.states_columns, logfile)
    # This section will reject any input rows that are missing required fields,
    # have unreadable data, or violate key constraints, because it is impossible
    # to store that data in the database tables.
    ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_updated_uaf_v2.csv', 'calc_updated_uaf', conn, ertac_tables.calc_uaf_columns, logfile)
    conn.execute("""DELETE FROM calc_updated_uaf WHERE camd_by_hourly_data_type = 'Non-EGU'""")
    ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_input_variables_v2.csv', 'calc_input_variables', conn, ertac_tables.input_variable_columns, logfile)            
    ertac_lib.load_csv_into_table(input_prefix_pp, 'annual_unit_summary.csv', 'annual_summary', conn, annual_summary_columns, logfile)
    ertac_lib.load_csv_into_table(input_prefix_avert, 'avert_unit_inputs.csv', 'avert_unit_inputs', conn, avert_unit_inputs_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'unit_level_CO2_addon_enhanced.csv', 'unit_level_CO2_addon_enhanced', conn, unit_level_CO2_addon_enhanced, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'hourly_diagnostic_file_v2.csv', 'hourly_diagnostic_file', conn, ertac_reports.hourly_diagnostic_file, logfile)
    
    
def fix_inputs(conn, inputvars, logfile):
    """Summarize hourly data and merge base year and future year results.

    Keyword arguments:
    conn      -- a valid database connection where the data is stored
    inputvars -- a dictionary of input options
    logfile   -- file where logging messages will be written

    """

    #fix uaf and pusp
    logging.info("  Fixing Missing Information in Inputs")         
    print >> logfile, "  Fixing Missing Information in Inputs"
    conn.execute("""DELETE FROM calc_updated_uaf WHERE offline_start_date <= ? """, [str(inputvars['future_year']) + '-01-01'])
    conn.execute("""DELETE FROM calc_updated_uaf WHERE online_start_date > ? """, [str(inputvars['future_year']) + '-12-31'])


def process_results(conn, inputvars, out_prefix, logfile):
    """Summarize hourly data and merge base year and future year results.

    Keyword arguments:
    conn      -- a valid database connection where the data is stored
    inputvars -- a dictionary of input options
    logfile   -- file where logging messages will be written

    """
    
    logging.info("  Converting Files to AVERT Ready")         
    print >> logfile, "  Converting Files to AVERT Ready"
        
        # we don't need state or fuel_unit_type_bin here since we are already looping on those    
    conn.execute("""INSERT INTO avert_unit_information(
                        state,
                        facility_name,
                        county_name,
                        orispl_code,
                        unitid,
                        plant_latitude,
                        plant_longitude,
                        ertac_fuel_unit_type_bin,
                        primary_fuel_type,
                        secondary_or_substitute_fuel,
                        fuel_definition,
                        max_capacity,
                        fy_gen,
                        fy_hi,
                        fy_co2_mass,
                        fy_so2_mass,
                        fy_nox_mass,
                        uf,
                        orispl_unitid,
                        egrid_region,
                        data_year)
SELECT cuuaf.state,
cuuaf.facility_name,
county_name,
cuuaf.orispl_code,
cuuaf.unitid,
plant_latitude,
plant_longitude,
cuuaf.ertac_fuel_unit_type_bin,
cuuaf.primary_fuel_type,
cuuaf.secondary_or_substitute_fuel,
case when cuuaf.ertac_fuel_unit_type_bin == 'Oil' OR cuuaf.ertac_fuel_unit_type_bin == 'Coal' then cuuaf.ertac_fuel_unit_type_bin else 'Gas' end,
cuuaf.max_unit_heat_input * 1000/cuuaf.ertac_heat_rate,
ula.fy_gload,
ula.fy_heat_input,
fy_co2,
ula.fy_so2_mass,
ula.fy_nox_mass,
cuuaf.max_annual_ertac_uf,
cuuaf.orispl_code || ' ' || cuuaf.unitid,
aui.egrid_region,
?

FROM calc_updated_uaf cuuaf
LEFT JOIN annual_summary ula
ON cuuaf.orispl_code = ula.orispl_code
AND cuuaf.unitid = ula.unitid
AND cuuaf.ertac_fuel_unit_type_bin = ula.ertac_fuel_unit_type_bin 

LEFT JOIN unit_level_CO2_addon_enhanced ulcae
ON cuuaf.orispl_code = ulcae.orispl_code
AND cuuaf.unitid = ulcae.unitid
AND cuuaf.ertac_fuel_unit_type_bin = ulcae.ertac_fuel_unit_type_bin 

LEFT JOIN avert_unit_inputs aui
ON cuuaf.orispl_code = aui.orispl_code
AND cuuaf.unitid = aui.unitid
AND cuuaf.ertac_fuel_unit_type_bin = aui.ertac_fuel_unit_type_bin 

INNER JOIN
(SELECT orispl_code,
unitid,
ertac_fuel_unit_type_bin
FROM hourly_diagnostic_file
GROUP BY orispl_code,
unitid,
ertac_fuel_unit_type_bin) AS hdf
ON hdf.orispl_code = cuuaf.orispl_code
AND hdf.unitid = cuuaf.unitid
AND hdf.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
WHERE cuuaf.offline_start_date >= ?
ORDER BY cuuaf.orispl_code, cuuaf.unitid, cuuaf.ertac_fuel_unit_type_bin""", (inputvars['future_year'], inputvars['future_year']+'-01-01'))



                
    file_counts=0
    writers = [False,False,False,False,False]
    for file in ["avert_hourly_gload.csv", 
    "avert_hourly_heat_input.csv", 
    "avert_hourly_co2.csv", 
    "avert_hourly_so2.csv", 
    "avert_hourly_nox.csv"]:
    
        if out_prefix is None:
            out_prefix = ""
        csv_file = out_prefix + file
    
        logging.info("  " + csv_file)
        print >> logfile
        try:
            writers[file_counts] = csv.writer(open(csv_file, 'wb'))
        except IOError:
            print >> logfile, "File: " + csv_file + " -- Could not be written."
            return
        file_counts+=1
    
    
    avert_hourly_columns = ()         
    (unit_rows,) = conn.execute("""SELECT COUNT(*) from avert_unit_information""").fetchone()
    
    if unit_rows == 0:    
        logging.info("No units found in files.  Exiting.")
        print >> logfile, "No units found in files.  Exiting."
        return False;
    
    for (hour,) in conn.execute("""SELECT calendar_hour from calendar_hours;"""):   
        gload_hour_data = [None]*(unit_rows)
        heat_input_hour_data = [None]*(unit_rows)
        co2_hour_data = [None]*(unit_rows)
        so2_hour_data = [None]*(unit_rows)
        nox_hour_data = [None]*(unit_rows)
        
        logging.info("Processing hour: "+str(hour)+" - "+time.asctime())
            
        unit_count = 1  
        
        conn.execute("""DELETE FROM temp_hour_data;""")
        conn.execute("""INSERT INTO temp_hour_data (gload, heat_input, co2_mass, so2_mass, nox_mass, orispl_code, unitid, ertac_fuel_unit_type_bin) 
                        SELECT COALESCE(gload,0), 
                        COALESCE(heat_input,0), 
                        COALESCE(fy_co2_rate*heat_input,0), 
                        COALESCE(so2_mass,0), 
                        COALESCE(nox_mass,0),
                        hdf.orispl_code,
                        hdf.unitid,
                        hdf.ertac_fuel_unit_type_bin
                        FROM hourly_diagnostic_file as hdf
                        LEFT JOIN unit_level_CO2_addon_enhanced ulcae
                        ON hdf.orispl_code = ulcae.orispl_code
                        AND hdf.unitid = ulcae.unitid
                        AND hdf.ertac_fuel_unit_type_bin = ulcae.ertac_fuel_unit_type_bin
                        WHERE calendar_hour = ?
                        ORDER BY hdf.orispl_code, hdf.unitid, hdf.ertac_fuel_unit_type_bin""",[hour])
        
        for unit in conn.execute("""SELECT orispl_code, unitid, ertac_fuel_unit_type_bin FROM avert_unit_information""").fetchall():
            #avert_hourly_columns = avert_hourly_columns + (("Unit "+str(unit_count), None, None, None),)
  
            for data in conn.execute("""SELECT gload, heat_input, co2_mass, so2_mass, nox_mass
                        FROM temp_hour_data
                        WHERE orispl_code = ?
                        AND unitid = ?
                        AND ertac_fuel_unit_type_bin = ?
                        ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""",unit).fetchall():
            
                gload_hour_data[unit_count-1]=data[0]
                heat_input_hour_data[unit_count-1]=data[1]
                co2_hour_data[unit_count-1]=data[2]
                so2_hour_data[unit_count-1]=data[3]
                nox_hour_data[unit_count-1]=data[4]
            unit_count+=1
         
        writers[0].writerow(gload_hour_data)
        writers[1].writerow(heat_input_hour_data)
        writers[2].writerow(co2_hour_data)
        writers[3].writerow(so2_hour_data)
        writers[4].writerow(nox_hour_data)
     
        # Save changes
    conn.commit()
    
    ertac_lib.export_table_to_csv('avert_unit_information', out_prefix, 'avert_unit_information.csv', conn, avert_unit_information_columns, logfile)

def make_calendar_hours(base_year, future_year, conn):
    """Make lookup table between dates/hours and hour numbers.

    Keyword arguments:
    base_year -- the base year where generation is projected from
    future_year -- the year which is being projected
    connection -- a valid database connection

    """

    conn.executescript("""CREATE TEMPORARY TABLE calendar_hours
    (op_date TEXT NOT NULL,
    op_hour INTEGER NOT NULL,
    future_date TEXT,
    calendar_hour INTEGER,
    PRIMARY KEY (op_date, op_hour));""")

    d = datetime.date(int(base_year),1,1)
    delta = datetime.timedelta(days=1)
    calendar_hour = 1
    while d <= datetime.date(int(base_year),12,31):
        for hr in range(0, 24):
            conn.execute("""INSERT INTO calendar_hours (op_date, op_hour, calendar_hour) VALUES (?,?,?)""", [d.strftime("%Y-%m-%d"), hr, calendar_hour])
            calendar_hour += 1
        d += delta
            
    conn.execute("""UPDATE calendar_hours
    SET future_date = REPLACE(op_date, ?, ?)""", (base_year, future_year))
    
       
def create_for_avert_tables(conn):
    # Also need state lookup table, for abbreviation-FIPS code conversion.
    conn.executescript("""
    DROP TABLE IF EXISTS states;
    CREATE TABLE states
    (state_code TEXT NOT NULL,
    state_abbreviation TEXT NOT NULL COLLATE NOCASE,
    state_name TEXT NOT NULL COLLATE NOCASE,
    PRIMARY KEY (state_code),
    UNIQUE (state_abbreviation));""")  
  
    conn.executescript("""
    DROP TABLE IF EXISTS avert_unit_inputs;
CREATE TABLE avert_unit_inputs
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,    
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
egrid_region TEXT)""")
           
    conn.executescript("""
    DROP TABLE IF EXISTS avert_unit_information;
CREATE TABLE avert_unit_information
(key TEXT,
match_row TEXT,
data_year INT,
state TEXT NOT NULL COLLATE NOCASE,
county_name TEXT,
facility_name TEXT NOT NULL COLLATE NOCASE,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
plant_latitude REAL,
plant_longitude REAL,
needs_ipm_region TEXT,
nerc_main_region TEXT,
owner TEXT,
clean_air_programs TEXT,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
primary_fuel_type TEXT,
secondary_or_substitute_fuel TEXT,
so2_controls TEXT,
nox_controls TEXT,
pm_controls TEXT,
fuel_definition TEXT,
unit_type_definition TEXT,
egrid_match TEXT,
egrid_region TEXT,
avert_region_match TEXT,
avert_region TEXT,
avert_region_maual TEXT,
final_avert_region TEXT,
capacity_gen_match TEXT,
max_capacity REAL,
fy_gen REAL,
fy_hi REAL,
fy_co2_mass REAL,
fy_so2_mass REAL,
fy_nox_mass REAL,
uf REAL,
orispl_unitid TEXT,
avert_region2 TEXT,
PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));""")

    conn.executescript("""
DROP TABLE IF EXISTS temp_hour_data;
CREATE TABLE temp_hour_data (
gload REAL, 
heat_input REAL, 
co2_mass REAL, 
so2_mass REAL, 
nox_mass REAL, 
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));""")

    conn.executescript("""
DROP TABLE IF EXISTS unit_level_CO2_addon_enhanced;
CREATE TABLE unit_level_CO2_addon_enhanced(
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT NOT NULL COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
max_ertac_hi_hourly_summer REAL,
heat_rate REAL,
capacity REAL,
num_hrs_fy_max INTEGER,
by_uf REAL,
fy_uf REAL,
by_gen REAL,
by_hi REAL,
by_co2 REAL,
by_co2_rate REAL,
by_co2_rate2 REAL,
fy_gen REAL,
fy_hi REAL,
fy_co2 REAL,
fy_co2_rate REAL,
fy_co2_rate2 REAL,
by_so2_rate REAL,
fy_so2_rate REAL,
primary_fuel_type TEXT,
new_unit TEXT,
general_deficit_unit TEXT,
gasified TEXT,
online_start_date TEXT,
retirement_date TEXT,
longitude TEXT,
latitude TEXT,
is_unit_nsps_applicable TEXT,
PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));""")
    
def main(argv=None):
    # Main projection program begins here.
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "hdqv:o:",
            ["help", "debug", "quiet", "verbose", 
            "input-prefix-pre=", "input-prefix-proj=", "input-prefix-pp=", "input-prefix-avert=", "output-prefix=", "sql-database="])
    except getopt.GetoptError, err:
        print
        print str(err)
        usage(argv[0])
        return 2

    # Initializing option variables
    debug_level       = "INFO"
    input_prefix_pre  = None
    input_prefix_proj = None
    input_prefix_pp = None
    input_prefix_avert = None
    output_prefix     = ''
    inputvars         = {}
    sql_database      = ''
    
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage(argv[0])
            return 0
        elif opt in ("-d", "--debug"):
            debug_level = "DEBUG"
        elif opt in ("-q", "--quiet"):
            debug_level = "NONE"
        elif opt in ("-v", "--verbose"):
            debug_level = "INFO"
        elif opt in ("--run-qa"):
            debug_level = True
        elif opt in ("--input-prefix-pre"):
            input_prefix_pre = arg
        elif opt in ("--input-prefix-proj"):
            input_prefix_proj = arg
        elif opt in ("--input-prefix-pp"):
            input_prefix_pp = arg
        elif opt in ("--input-prefix-avert"):
            input_prefix_avert = arg
        elif opt in ("-o", "--output-prefix"):
            output_prefix = arg
        elif opt in ("--sql-database"):
            sql_database = arg
    
    if debug_level == "DEBUG":
        # Detailed logging to file for postmortem analysis.
        logging.basicConfig(
            filename='ertac_for_avert_debug_log.txt',
            filemode = 'w',
            format='%(asctime)s %(levelname)-8s %(filename)s %(lineno)d %(message)s',
            level=logging.DEBUG)

    elif debug_level == "INFO":
        # Brief logging to screen, to show program progress.
        logging.basicConfig(format='%(levelname)-8s %(message)s', level=logging.INFO)

    elif debug_level == "NONE":
        # No logging.
        logging.basicConfig()


    # Regular program operation log file, separate from detailed debug log above.
    if output_prefix is not None:
        logfilename = output_prefix + 'ertac_egu_for_avert_log.txt'
    else:
        logfilename = 'ertac_egu_for_avert_log.txt'

    try:
        logfile = open(logfilename, 'w')
    except IOError:
        print >> sys.stderr, "Log file: " + logfilename + " -- Could not be written.  Program will terminate."
        raise

    # Identify versions of Python and SQLite library, and record in log file.
    logging.info("Program started at " + time.asctime())
    logging.info("ERTAC EGU version: " + VERSION)
    logging.info("Running under python version: " + sys.version)
    logging.info("Using sqlite3 module version: " + sqlite3.version)
    logging.info("Linked against sqlite3 database library version: " + sqlite3.sqlite_version)
    print >> logfile, "Program started at " + time.asctime()
    print >> logfile, "Running under python version: " + sys.version
    print >> logfile, "Using sqlite3 module version: " + sqlite3.version
    print >> logfile, "Linked against sqlite3 database library version: " + sqlite3.sqlite_version
    print >> logfile, "Model code versions:"
    for file_name in [os.path.basename(sys.argv[0]), 'ertac_lib.py', 'ertac_tables.py', 'ertac_reports.py',
                      'create_preprocessor_output_tables.sql', 'create_projection_output_tables.sql']:
        print >> logfile, "  " + file_name + ": " + time.ctime(os.path.getmtime(os.path.join(sys.path[0], file_name)))

    # Workspace SQL DB (1) in memory or (2) as a file
    if sql_database == '':
        logging.info("Creating database tables in memory.")
        print >> logfile, "Creating database tables in memory."
        # The preprocessor output tables are used as the projection inputs.
        # The projection output tables produce all the reports.
        dbconn = sqlite3.connect(sql_database)
        ertac_lib.run_script_file('create_preprocessor_output_tables.sql', dbconn)
        ertac_lib.run_script_file('create_projection_output_tables.sql', dbconn)
        ertac_lib.run_script_file('create_postprocessing_tables.sql', dbconn)
        create_for_avert_tables(dbconn)
        # Load intermediate CSV data into tables, rejecting any rows that can't be
        # used.  There should be no invalid data at this stage, unless the
        # intermediate files were manually changed with erroneous data.
        logging.info("Loading intermediate data:")
        print >> logfile, "Loading intermediate data:"
        load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj,input_prefix_pp,input_prefix_avert, logfile)
        logging.info("Finished loading intermediate data.")
        print >> logfile,"Finished loading intermediate data."
        existing_db_file = False
    else:
        # Check if user specified DB file exists.
        # If not, create and populate the workspace database.
        if os.path.isfile(sql_database):
            logging.info("Found the existing DB:" + sql_database)
            print >> logfile, "Found the existing DB:" + sql_database
            dbconn = sqlite3.connect(sql_database)
            dbconn.text_factory = str
            existing_db_file = True
        else:
            logging.info("Not Found the existing DB, creating a new DB file:" + sql_database)
            print >> logfile, "Not Found the existing DB, creating a new DB file:" + sql_database
            dbconn = sqlite3.connect(sql_database)
            create_for_avert_tables(dbconn)
            # Load intermediate CSV data into tables, rejecting any rows that can't be
            # used.  There should be no invalid data at this stage, unless the
            # intermediate files were manually changed with erroneous data.
            logging.info("Loading intermediate data:")
            print >> logfile, "Loading intermediate data:"
            load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj, input_prefix_avert, logfile)
            logging.info("Finished loading intermediate data.")
            print >> logfile,"Finished loading intermediate data."
            existing_db_file = False

        
    # 20120406 Determine ozone season start/end dates and calendar hours.
    (inputvars['base_year'], inputvars['future_year'], ozone_start, ozone_end) = dbconn.execute("""SELECT DISTINCT
    base_year, future_year, ozone_start_date, ozone_end_date
    FROM calc_input_variables""").fetchone()
    ozone_start_base   = ertac_lib.convert_ozone_date(ozone_start, inputvars['base_year'])
    ozone_end_base     = ertac_lib.convert_ozone_date(ozone_end, inputvars['base_year'])
    ozone_start_future = ertac_lib.convert_ozone_date(ozone_start, inputvars['future_year'])
    ozone_end_future   = ertac_lib.convert_ozone_date(ozone_end, inputvars['future_year'])

    # Need to convert operating date/hour into calendar hour for outputs.
    make_calendar_hours(inputvars['base_year'], inputvars['future_year'], dbconn)
    (inputvars['ozone_start_hour'],) = dbconn.execute("""SELECT MIN(calendar_hour)
    FROM calendar_hours
    WHERE op_date >= ?""", (ozone_start_base,)).fetchone()

    (inputvars['ozone_end_hour'],) = dbconn.execute("""SELECT MAX(calendar_hour)
    FROM calendar_hours
    WHERE op_date <= ?""", (ozone_end_base,)).fetchone()

    fix_inputs(dbconn, inputvars, logfile)
   
    process_results(dbconn, inputvars, output_prefix, logfile)
    
    logging.info("Finished writing reports.")
    dbconn.close()
    logging.info("Program ended at " + time.asctime())
    print >> logfile
    print >> logfile, "Program ended at " + time.asctime()


    # End of main routine

if __name__ == '__main__':
    sys.exit(main())
