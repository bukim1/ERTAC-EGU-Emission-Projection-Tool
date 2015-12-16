#!/usr/bin/python

# ertac_for_avert.py

"""ERTAC EGU AVERT Converter"""

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

VERSION = "1.02"

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
  --ignore-pollutants=pollutants        comma separated list of pollutants to ignore
  --state=state                         limit resutls to state or comma separate list of states
  -o prefix, --output-prefix=prefix.    output prefix for postprocessor results
""" % progname
avert_unit_inputs_columns = (
               ('ORISPL_Code', 'str', True, None),
               ('Unit ID', 'str', True, None),
               ('ertac fuel unit type bin', 'str', True, ertac_tables.fuel_set),
               ('eGRID Region', 'str', False, None))

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
               ('Gross Generation (MWh)', 'float', False, None),
               ('Capacity Factor', 'float', False, None),
               ('ORSPL UNITID', 'str', True, None),
               ('AVERT Region', 'str', False, None))

avert_hourly_columns = (('Gross Load (MW-hr)', None, None, None),
                          ('Heat Input (mmbtu)', None, None, None),
                          ('CO2_mass (lb/hr)', None, None, None),
                          ('SO2_mass (lb/hr)', None, None, None),
                          ('NOX_mass (lbs/hr)', None, None, None))


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

def load_intermediate_data(conn, in_prefix_pre, in_prefix_proj, logfile):
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
    ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_updated_uaf.csv', 'calc_updated_uaf', conn, ertac_tables.uaf_columns, logfile)
    conn.execute("""DELETE FROM calc_updated_uaf WHERE camd_by_hourly_data_type = 'Non-EGU'""")
    ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_input_variables.csv', 'calc_input_variables', conn, ertac_tables.input_variable_columns, logfile)            
    ertac_lib.load_csv_into_table(in_prefix_proj, 'unit_level_activity.csv', 'unit_level_activity', conn, ertac_reports.unit_level_activity, logfile)
    ertac_lib.load_csv_into_table('', 'avert_unit_inputs.csv', 'avert_unit_inputs', conn, avert_unit_inputs_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'unit_level_CO2_addon_enhanced.csv', 'unit_level_CO2_addon_enhanced', conn, unit_level_CO2_addon_enhanced, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'hourly_diagnostic_file.csv', 'hourly_diagnostic_file', conn, ertac_reports.hourly_diagnostic_file, logfile)
    
    
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


def process_results(conn, inputvars, logfile):
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
primary_fuel_type,
secondary_or_substitute_fuel,
case when cuuaf.ertac_fuel_unit_type_bin == 'Oil' OR cuuaf.ertac_fuel_unit_type_bin == 'Coal' then cuuaf.ertac_fuel_unit_type_bin else 'Gas' end,
max_unit_heat_input * 1000/ertac_heat_rate,
fy_gen,
uf,
cuuaf.orispl_code || ' ' || cuuaf.unitid,
aui.egrid_region,
?

FROM calc_updated_uaf cuuaf
LEFT JOIN unit_level_activity ula
ON cuuaf.orispl_code = ula.orispl_code
AND cuuaf.unitid = ula.unitid
AND cuuaf.ertac_fuel_unit_type_bin = ula.ertac_fuel_unit_type_bin 
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
ORDER BY cuuaf.orispl_code, cuuaf.unitid, cuuaf.ertac_fuel_unit_type_bin""", (inputvars['future_year'], ))

    conn.execute("""INSERT INTO avert_hourly(gload, heat_input, co2_mass, so2_mass, nox_mass)
                        SELECT COALESCE(gload,0), COALESCE(heat_input,0), COALESCE(fy_co2_rate*heat_input, 0), COALESCE(so2_mass,0), COALESCE(nox_mass,0)  
                        FROM hourly_diagnostic_file as hdf
                        LEFT JOIN unit_level_CO2_addon_enhanced ulcae
                        ON hdf.orispl_code = ulcae.orispl_code
                        AND hdf.unitid = ulcae.unitid
                        AND hdf.ertac_fuel_unit_type_bin = ulcae.ertac_fuel_unit_type_bin
                        INNER JOIN calc_updated_uaf cuuaf
                        ON hdf.orispl_code = cuuaf.orispl_code
                        AND hdf.unitid = cuuaf.unitid
                        AND hdf.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
                        ORDER BY hdf.orispl_code, hdf.unitid, hdf.ertac_fuel_unit_type_bin, calendar_hour ASC""")

    (hourly_rows,) = conn.execute("""SELECT COUNT(*) from avert_hourly""").fetchone()
    (unit_rows,) = conn.execute("""SELECT COUNT(*) from avert_unit_information""").fetchone()
    message = "avert hourly has "+str(hourly_rows)+" rows, avert unit information has "+str(unit_rows)+" rows and there are "+str(hourly_rows -(unit_rows*8760))+" extra rows in the hourly file"
    logging.info(message)
    print >> logfile, message
    
        # Save changes
    conn.commit()

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
    
def write_final_data(conn, out_prefix, logfile):
    """Write out projected ERTAC EGU data reports.

    Keyword arguments:
    conn       -- a valid database connection where the data is stored
    out_prefix -- optional prefix added to each output file name
    logfile    -- file where logging messages will be written

    """
     
    ertac_lib.export_table_to_csv('avert_unit_information', out_prefix, 'avert_unit_information.csv', conn, avert_unit_information_columns, logfile)
    ertac_lib.export_table_to_csv('avert_hourly', out_prefix, 'avert_hourly.csv', conn, avert_hourly_columns, logfile)
 
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
DROP TABLE IF EXISTS unit_level_activity;
CREATE TABLE unit_level_activity(orispl_code TEXT NOT NULL COLLATE NOCASE,unitid TEXT NOT NULL COLLATE NOCASE,facility_name TEXT NOT NULL COLLATE NOCASE,state TEXT NOT NULL COLLATE NOCASE,ertac_region TEXT NOT NULL COLLATE NOCASE,ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,max_ertac_hi_hourly_summer REAL,heat_rate REAL,capacity REAL,num_hrs_fy_max INTEGER,uf REAL,by_gen REAL,by_hi REAL,by_hours REAL,fy_gen REAL,fy_hi REAL,fy_hours REAL, PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));""")
                         
    conn.executescript("""DROP TABLE IF EXISTS hourly_diagnostic_file;
CREATE TABLE hourly_diagnostic_file(ertac_region TEXT NOT NULL COLLATE NOCASE,ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,state TEXT NOT NULL COLLATE NOCASE,orispl_code TEXT NOT NULL COLLATE NOCASE,unitid TEXT NOT NULL COLLATE NOCASE,calendar_hour INTEGER NOT NULL,hierarchy_hour INTEGER NOT NULL,hourly_hi_limit TEXT NOT NULL COLLATE NOCASE,annual_hi_limit TEXT NOT NULL COLLATE NOCASE,cumulative_hi REAL,cumulative_gen REAL,gload REAL,heat_input REAL,so2_mass REAL,so2_rate REAL,nox_rate REAL,nox_mass REAL,PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, hierarchy_hour, orispl_code, unitid),UNIQUE (ertac_region, calendar_hour, ertac_fuel_unit_type_bin, orispl_code, unitid));""")
     
    conn.executescript("""DROP TABLE IF EXISTS calc_updated_uaf;
CREATE TABLE calc_updated_uaf(orispl_code TEXT NOT NULL COLLATE NOCASE,unitid TEXT NOT NULL COLLATE NOCASE,form860_plant_id TEXT,fips_code TEXT,county_code TEXT,county_name TEXT,state TEXT NOT NULL COLLATE NOCASE,needs_unit_id TEXT,form860_unit_id TEXT,plant_latitude REAL,plant_longitude REAL,inventory_stack_id TEXT,facility_name TEXT NOT NULL COLLATE NOCASE,needs_ipm_region TEXT,nerc_main_region TEXT,eia_region_old_nerc TEXT,ertac_region TEXT NOT NULL COLLATE NOCASE,other_consuming_regions TEXT,camd_by_hourly_data_type TEXT NOT NULL COLLATE NOCASE,annual_hi_partials REAL,camd_by_operating_status TEXT,camd_stack_info TEXT,online_start_date TEXT,offline_start_date TEXT,primary_fuel_type TEXT,main_fuel_characteristics TEXT,secondary_or_substitute_fuel TEXT,prime_mover_generator_unit_type TEXT,camd_unit_type TEXT,ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,max_ertac_hi_hourly_summer REAL,max_ertac_hi_hourly_winter REAL,hourly_base_max_actual_hi REAL,nameplate_capacity REAL,max_summer_capacity REAL,max_winter_capacity REAL,max_unit_heat_input REAL,calculated_by_uf REAL,max_annual_state_uf REAL,max_annual_ertac_uf REAL,operating_hours_by REAL,max_by_hourly_gload REAL,max_by_hourly_sload REAL,nominal_heat_rate REAL,calc_by_average_heat_rate REAL,ertac_heat_rate REAL,unit_annual_capacity_limit REAL,unit_max_optimal_load_threshold REAL,unit_min_optimal_load_threshold REAL,unit_ownership_code TEXT,multiple_ownership_notation TEXT,secondary_owner TEXT,tertiary_owner TEXT,new_unit_flag TEXT COLLATE NOCASE,capacity_limited_unit_flag TEXT COLLATE NOCASE,modifier_email_address TEXT,unit_completeness_check TEXT,PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));""")
               
    conn.executescript("""DROP TABLE IF EXISTS calc_input_variables;
CREATE TABLE calc_input_variables(ertac_region TEXT NOT NULL COLLATE NOCASE,ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,base_year TEXT NOT NULL,future_year TEXT NOT NULL,ozone_start_date TEXT,ozone_end_date TEXT,hourly_hierarchy_code TEXT NOT NULL COLLATE NOCASE,new_unit_max_size INTEGER NOT NULL,new_unit_min_size INTEGER NOT NULL,demand_cushion REAL NOT NULL,facility_1 TEXT COLLATE NOCASE,facility_2 TEXT COLLATE NOCASE,facility_3 TEXT COLLATE NOCASE,facility_4 TEXT COLLATE NOCASE,facility_5 TEXT COLLATE NOCASE,facility_6 TEXT COLLATE NOCASE,facility_7 TEXT COLLATE NOCASE,facility_8 TEXT COLLATE NOCASE,facility_9 TEXT COLLATE NOCASE,facility_10 TEXT COLLATE NOCASE,maximum_annual_ertac_uf REAL NOT NULL,capacity_demand_deficit_review INTEGER NOT NULL,unit_optimal_load_threshold_determinant REAL NOT NULL,proxy_percentage REAL NOT NULL,generic_so2_control_efficiency REAL NOT NULL,generic_scr_nox_rate REAL NOT NULL,generic_sncr_nox_rate REAL NOT NULL,new_unit_hierarchy_placement_percentile REAL NOT NULL,new_unit_emission_factor_percentile REAL NOT NULL,unit_min_optimal_load_threshold_determinant REAL NOT NULL,heat_input_calculation_percentile REAL NOT NULL,PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin));""")
     
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
uf REAL,
orispl_unitid TEXT,
avert_region2 TEXT,
PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));""")

    conn.executescript("""
DROP TABLE IF EXISTS avert_hourly;
CREATE TABLE avert_hourly
(gload REAL,
heat_input REAL,
co2_mass REAL,
so2_mass REAL,
nox_mass REAL);""")

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
            "input-prefix-pre=", "input-prefix-proj=", "output-prefix=", "sql-database="])
    except getopt.GetoptError, err:
        print
        print str(err)
        usage(argv[0])
        return 2

    # Initializing option variables
    debug_level       = "INFO"
    input_prefix_pre  = None
    input_prefix_proj = None
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
        create_for_avert_tables(dbconn)
        # Load intermediate CSV data into tables, rejecting any rows that can't be
        # used.  There should be no invalid data at this stage, unless the
        # intermediate files were manually changed with erroneous data.
        logging.info("Loading intermediate data:")
        print >> logfile, "Loading intermediate data:"
        load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj,logfile)
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
            load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj, logfile)
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
   
    process_results(dbconn, inputvars, logfile)
      
    logging.info("Writing out reports:")         
    write_final_data(dbconn, output_prefix, logfile)
        
    logging.info("Finished writing reports.")
    dbconn.close()
    logging.info("Program ended at " + time.asctime())
    print >> logfile
    print >> logfile, "Program ended at " + time.asctime()


    # End of main routine

if __name__ == '__main__':
    sys.exit(main())
