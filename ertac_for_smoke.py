#!/usr/bin/python

# ertac_postprocess.py

"""ERTAC EGU post processing"""

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

import sys
try:
    import getopt, logging, os, time, re, csv, datetime, StringIO
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

time_zones = ['GMT', 'ADT', 'AST', 'EDT', 'EST', 'CDT', 'CST', 'MDT', 'MST', 'PDT', 'PST']

#table layout based on http://www.smoke-model.org/version3.1/html/ch08s02s10.html
fy_emission_rate_columns = (
('ertac_region', 'str', True, None),
('ertac_fuel_unit_type_bin', 'str', True, ertac_tables.fuel_set),
('ORIS_FACILITY_CODE', 'str', True, None),
('ORIS_BOILER_ID', 'str', True, None),
('pm25_rate (lbs/mmBtu)', 'float', False, None),
('pm10_rate (lbs/mmBtu)', 'float', False, None),
('co_rate (lbs/mmBtu)', 'float', False, None),
('voc_rate (lbs/mmBtu)', 'float', False, None),
('nh3_rate (lbs/mmBtu)', 'float', False, None))

orl_columns = (('FIPS', 'str', True, None),
                       ('PLANTID', 'str', True, None),
                       ('POINTID', 'str', True, None),
                       ('STACKID', 'str', True, None),
                       ('SEGMENT', 'str', True, None),
                       ('PLANT', 'str', False, None),
                       ('SCC', 'str', True, None),
                       ('ERPTYPE', 'str', True, ['01','02','03','04','05','06']),
                       ('SRCTYPE', 'str', True, ['01','02','03','04']),
                       ('STKHGT', 'float', True, None),
                       ('STKDIAM', 'float', True, None),
                       ('STKTEMP', 'float', True, None),
                       ('STKFLOW', 'float', True, None),
                       ('STKVEL', 'float', True, None),
                       ('SIC', 'int', False, None),
                       ('MACT', 'str', False, None),
                       ('NAICS', 'str', False, None),
                       ('CTYPE', 'str', True, ['L','U']),
                       ('XLOC', 'float', True, None),
                       ('YLOC', 'float', True, None),
                       ('UTMZ', 'int', False, None),
                       ('CAS', 'str', True, None),
                       ('ANN_EMIS', 'real', True, None),
                       ('AVD_EMIS', 'real', False, None),
                       ('CEFF', 'real', False, None),
                       ('REFF', 'real', False, None),
                       ('CPRI', 'int', False, None),
                       ('CSEC', 'int', False, None),
                       ('NEI_UNIQUE_ID', 'str', False, None),
                       ('ORIS_FACILITY_CODE', 'str', True, None),
                       ('ORIS_BOILER_ID', 'str', True, None),
                       ('IPM_YN', 'str', False, ['Y','N']),
                       ('DATA_SOURCE', 'str', False, None))

ff10_columns = (('COUNTRY', 'str', True, None),
                      ('FIPS', 'str', True, None),
                      ('TRIBAL_CODE', 'str', True, None),
                       ('FACILITY_ID', 'str', True, None),
                       ('UNIT_ID', 'str', True, None),
                       ('REL_POINT_ID', 'str', True, None),
                       ('PROCESS_ID', 'str', True, None),
                       ('AGY_FACILITY_ID', 'str', True, None),
                       ('AGY_UNIT_ID', 'str', True, None),
                       ('AGY_REL_POINT_ID', 'str', True, None),
                       ('AGY_PROCESS_ID', 'str', True, None),
                       ('SCC', 'str', True, None),
                       ('POLL', 'str', True, None),
                       ('ANN_VALUE', 'real', True, None),
                       ('ANN_PCT_RED', 'real', False, None),
                       ('FACILITY_NAME', 'str', False, None),
                       ('ERPTYPE', 'str', True, ['01','02','03','04','05','06']),
                       ('STKHGT', 'float', True, None),
                       ('STKDIAM', 'float', True, None),
                       ('STKTEMP', 'float', True, None),
                       ('STKFLOW', 'float', True, None),
                       ('STKVEL', 'float', True, None),
                       ('NAICS', 'str', False, None),
                       ('LONGITUDE', 'float', True, None),
                       ('LATITUDE', 'float', True, None),                       
                       ('LL_DATUM', 'str', False, ['001','002','003','004']),
                       ('HORIZ_COLL_MTHD', 'str', False, None),
                       ('DESIGN_CAPACITY', 'float', False, None),
                       ('DESIGN_CAPACITY_UNITS', 'str', False, None),
                       ('REG_CODES', 'str', False, None),
                       ('FAC_SOURCE_TYPE', 'real', False, None),
                       ('UNIT_TYPE_CODE', 'real', False, None),
                       ('CONTROL_IDS', 'str', False, None),                       
                       ('CONTROL_MEASURES', 'str', False, None),                       
                       ('CURRENT_COST', 'real', False, None),                     
                       ('CUMULATIVE_COST', 'real', False, None),                 
                       ('PROJECTION_FACTOR', 'real', False, None),                 
                       ('SUBMITTER_FAC_ID', 'str', False, None),                        
                       ('CALC_METHOD', 'int', False, None),
                       ('DATA_SET_ID', 'int', False, None),                 
                       ('FACIL_CATEGORY_CODE', 'str', False, None),                        
                       ('ORIS_FACILITY_CODE', 'str', True, None),
                       ('ORIS_BOILER_ID', 'str', True, None),
                       ('IPM_YN', 'str', False, ['Y','N']),
                       ('CALC_YEAR', 'str', False, None),
                       ('DATE_UPDATED', 'date', True, None))

pt_columns = (('STATE_CODE', 'str', True, None),
              ('CNTY_CODE', 'str', True, None),
                       ('PLANTID', 'str', True, None),
                       ('POINTID', 'str', True, None),
                       ('STACKID', 'str', True, None),
                       ('SEGMENT', 'str', True, None),
                       ('POLCODE', 'str', True, None),
                       ('DATE', 'date', True, None),
                       ('TMZ', 'str', True, None),
                       ('HRVL1', 'float', False, None),
                       ('HRVL2', 'float', False, None),
                       ('HRVL3', 'float', False, None),
                       ('HRVL4', 'float', False, None),
                       ('HRVL5', 'float', False, None),
                       ('HRVL6', 'float', False, None),
                       ('HRVL7', 'float', False, None),
                       ('HRVL8', 'float', False, None),
                       ('HRVL9', 'float', False, None),
                       ('HRVL10', 'float', False, None),
                       ('HRVL11', 'float', False, None),
                       ('HRVL12', 'float', False, None),
                       ('HRVL13', 'float', False, None),
                       ('HRVL14', 'float', False, None),
                       ('HRVL15', 'float', False, None),
                       ('HRVL16', 'float', False, None),
                       ('HRVL17', 'float', False, None),
                       ('HRVL18', 'float', False, None),
                       ('HRVL19', 'float', False, None),
                       ('HRVL20', 'float', False, None),
                       ('HRVL21', 'float', False, None),
                       ('HRVL22', 'float', False, None),
                       ('HRVL23', 'float', False, None),
                       ('HRVL24', 'float', False, None),
                       ('DAY_TOT', 'float', False, None),
                       ('SCC', 'str', True, None)                       )

ff10_hourly_future_columns = (('COUNTRY', 'str', True, None),
                      ('FIPS', 'int', True, None),
                      ('TRIBAL_CODE', 'str', True, None),
                       ('FACILITY_ID', 'str', True, None),
                       ('UNIT_ID', 'str', True, None),
                       ('REL_POINT_ID', 'str', True, None),
                       ('PROCESS_ID', 'str', True, None),
                       ('SCC', 'str', True, None),
                       ('POLL', 'str', True, None),
                       ('OP_TYPE_CD', 'str', True, None),
                       ('CALC_METHOD', 'int', False, None),
                       ('DATE_UPDATED', 'date', True, None),
                       ('DATE', 'date', True, None),
                       ('DAY_TOT', 'float', False, None),
                       ('HRVL0', 'float', False, None), 
                       ('HRVL1', 'float', False, None),
                       ('HRVL2', 'float', False, None),
                       ('HRVL3', 'float', False, None),
                       ('HRVL4', 'float', False, None),
                       ('HRVL5', 'float', False, None),
                       ('HRVL6', 'float', False, None),
                       ('HRVL7', 'float', False, None),
                       ('HRVL8', 'float', False, None),
                       ('HRVL9', 'float', False, None),
                       ('HRVL10', 'float', False, None),
                       ('HRVL11', 'float', False, None),
                       ('HRVL12', 'float', False, None),
                       ('HRVL13', 'float', False, None),
                       ('HRVL14', 'float', False, None),
                       ('HRVL15', 'float', False, None),
                       ('HRVL16', 'float', False, None),
                       ('HRVL17', 'float', False, None),
                       ('HRVL18', 'float', False, None),
                       ('HRVL19', 'float', False, None),
                       ('HRVL20', 'float', False, None),
                       ('HRVL21', 'float', False, None),
                       ('HRVL22', 'float', False, None),
                       ('HRVL23', 'float', False, None), 
                       ('COMMENT', 'str', True, None))
                 
pusp_info_file_columns = (
                       ('ertac_region', 'str', True, None),
                       ('ertac_fuel_unit_type_bin', 'str', True, ertac_tables.fuel_set),
                       ('state', 'str', True, ertac_tables.state_set),
                       ('offline_start_date', 'str', False, None),
                       ('ORIS_FACILITY_CODE', 'str', True, None),
                       ('ORIS_BOILER_ID', 'str', True, None),
                       ('PLANTID', 'str', True, None),
                       ('POINTID', 'str', True, None),
                       ('STACKID', 'str', False, None),
                       ('SEGMENT', 'str', False, None),
                       ('STKHGT', 'float', False, None),
                       ('STKDIAM', 'float', False, None),
                       ('STKTEMP', 'float', False, None),
                       ('STKFLOW', 'float', False, None),
                       ('STKVEL', 'float', False, None),
                       ('SCC', 'str', False, None),
                       ('TIME_ZONE', 'str', False, time_zones),
                       ('NOX_PERCENTAGE', 'float', False, (0.0, 100.0)),
                       ('SOX_PERCENTAGE', 'float', False, (0.0, 100.0)),
                       ('PM25_PERCENTAGE', 'float', False, (0.0, 100.0)),
                       ('PM10_PERCENTAGE', 'float', False, (0.0, 100.0)),
                       ('CO_PERCENTAGE', 'float', False, (0.0, 100.0)),
                       ('VOC_PERCENTAGE', 'float', False, (0.0, 100.0)),
                       ('NH3_PERCENTAGE', 'float', False, (0.0, 100.0)),
                       ('SIC', 'int', False, None),
                       ('MACT', 'str', False, None),
                       ('NAICS', 'str', False, None),
                       ('COMMENTS', 'str', False, None))

additional_variables_columns = (
                       ('state', 'str', True, ertac_tables.state_set),
                       ('ertac_fuel_unit_type_bin', 'str', True, ertac_tables.fuel_set),
                       ('pm25_rate (lbs/mmBtu)', 'float', False, None),
                       ('pm10_rate (lbs/mmBtu)', 'float', False, None),
                       ('co_rate (lbs/mmBtu)', 'float', False, None),
                       ('voc_rate (lbs/mmBtu)', 'float', False, None),
                       ('nh3_rate (lbs/mmBtu)', 'float', False, None),
                       ('New Unit SCC', 'str', True, None),
                       ('New Unit STKHGT', 'float', True, None),
                       ('New Unit STKDIAM', 'float', True, None),
                       ('New Unit STKTEMP', 'float', True, None),
                       ('New Unit STKFLOW', 'float', True, None),
                       ('New Unit STKVEL', 'float', True, None),
                       ('COMMENTS', 'str', False, None)
                        )
        
additional_control_emission_columns = (('ORISPL_CODE', 'str', True, None),
                            ('UNITID', 'str', True, None),
                            ('Factor Start Date', 'date', False, None),
                            ('Factor End Date', 'date', False, None),
                            ('Pollutant', 'str', True, None),
                            ('Base Year Rate', 'float', False, None),
                            ('Emission Rate', 'float', False, None),
                            ('Control Efficiency', 'float', False, (0.0, 100.0)),
                            ('Programs for Pollutant', 'str', False, None),
                            ('Control Description', 'str', False, None),
                            ('Submitter email', 'str', False, None))


def load_intermediate_data(conn, in_prefix_pre, in_prefix_proj, input_type, logfile):
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
    if input_type == 'ERTAC':
        ertac_lib.load_csv_into_table(in_prefix_proj, 'hourly_diagnostic_file.csv', 'hourly_diagnostic_file', conn, ertac_reports.hourly_diagnostic_file, logfile)
    else:
        ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_hourly_base.csv', 'calc_hourly_base', conn, ertac_tables.calc_hourly_columns, logfile)
        
    ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_input_variables.csv', 'calc_input_variables', conn, ertac_tables.input_variable_columns, logfile)            
    ertac_lib.load_csv_into_table('', 'ertac_pusp_info_file.csv', 'ertac_pusp_info_file', conn, pusp_info_file_columns, logfile)
    ertac_lib.load_csv_into_table('', 'ertac_base_year_rates_and_additional_controls.csv', 'ertac_base_year_rates_and_additional_controls', conn, additional_control_emission_columns, logfile)
    ertac_lib.load_csv_into_table('', 'ertac_additional_variables.csv', 'ertac_additional_variables', conn, additional_variables_columns, logfile)


def export_table_to_csv_with_smoke_header(table_name, prefix, basic_csv_file, connection, column_types, header, logfile, frmt = None):
    """Export table contents to a CSV file.

    Keyword arguments:
    table_name -- name of table to export
    prefix -- optional prefix added to each output file name
    basic_csv_file -- basic name of CSV file to be written, without prefix
    connection -- a valid database connection
    column_types -- a group of tuples describing each column, with column headers
    logfile -- file where logging messages will be written
    write_header -- boolean flag for creating header row

    """

    if prefix is not None:
        csv_file = prefix + basic_csv_file
    else:
        csv_file = basic_csv_file

    dbcur = connection.execute("SELECT * FROM " + table_name)

    # Use group of column definitions if supplied; if empty list, fall back to
    # basic column names from table.
    if column_types:
        cols = column_types
    else:
        cols = dbcur.description

    logging.info("  " + csv_file)
    print >> logfile
    try:
        cf = open(csv_file, 'wb')
    except IOError:
        print >> logfile, "File: " + csv_file + " -- Could not be written."
        return

    row_count = 0     
    if frmt:
        #this needs to be redeveloped
        cw = csv.writer(cf, quoting=csv.QUOTE_NONE)
        
        for hr in header:
            for i, c in enumerate(hr):
                hr[i] = str(c)
            cw.writerow(["".join("%*s" % i for i in zip(frmt, hr))])
        cw.writerow(["".join("%*s" % i for i in zip(frmt, cols))])    
        for row in dbcur:
            for i, c in enumerate(row):
                row[i] = str(c)
            cw.writerow(["".join("%*s" % i for i in zip(frmt, row))])
            row_count += 1
    else:
        for hr in header:                    
            cf.write(hr+"\n")
        cf.write("#DESC " + ",".join(['"'+col[0]+'"' for col in cols])+"\n")  
        si = StringIO.StringIO()
        cw = csv.writer(cf, quoting=csv.QUOTE_NONNUMERIC)    
        for row in dbcur:
            cw.writerow(row)
            row_count += 1

    print >> logfile, "Wrote out", row_count, "data rows from table: " + table_name + " to file: " + csv_file

def build_prefix(prefix, inputvars):
    """Builds a prefix for output files.

    Keyword arguments:
    prefix    -- a prefix that had been supplied at a higher leve
    inputvars -- a dictionary of input options used in the nameing convention

    """
    #we need to add something to deal with *'s or whatever other junk that could show up in the file name - jakuta
    if 'region' in inputvars:
        prefix+=inputvars['region']+"_"
    if 'fuel_bin' in inputvars:
        prefix+=inputvars['fuel_bin']+"_"
    if 'state' in inputvars:
        prefix+=inputvars['state']+"_"
    if 'facility_name' in inputvars:
        prefix+=inputvars['facility_name']+"_"
        if 'unitid' in inputvars:
            prefix+=inputvars['unitid']+"_"
    elif 'orisid' in inputvars:
        prefix+=inputvars['orisid']+"_"
        if 'unitid' in inputvars:
            prefix+=inputvars['unitid']+"_"
    if 'time_span' in inputvars:
        prefix+=inputvars['time_span']+"_"
    return re.sub(r'[^a-zA-Z0-9_\-.() ]+', '', prefix)

def build_where(conn, table, inputvars, include_time, include_others):
    """Builds the where for the sql query.

    Keyword arguments:
    prefix         -- a prefix that had been supplied at a higher leve
    table          -- table being used in the sql query
    inputvars      -- a dictionary of input options used in the nameing convention
    include_time   -- include the calendar portion of the query (needed to deal with new unit queries)
    include_others -- include the other portions of the query (needed to deal with new unit queries)

    """
    query = ""
    inputs = []
    if include_others:
        if 'state' in inputvars:
            states = inputvars['state'].split(",")
            query+= " AND "+table+"state in ("+('?,'*len(states))[:-1]+") "
            inputs = inputs + list(states)

    return (query, inputs)

def convert_camd_to_hdf(conn, logfile):
    """Convert CAMD inputs to Hourly Diagnostic File

    Keyword arguments:
    conn      -- a valid database connection where the data is stored
    logfile   -- file where logging messages will be written

    """
    conn.executescript("""INSERT INTO hourly_diagnostic_file (ertac_region, 
            ertac_fuel_unit_type_bin, 
            state, 
            orispl_code,
            unitid,
            calendar_hour,
            hierarchy_hour, 
            gload,
            heat_input,
            so2_mass,
            so2_rate,
            nox_rate,
            nox_mass)
        SELECT ertac_region,
            ertac_fuel_unit_type_bin,
            state,
            orispl_code,
            unitid,
            calendar_hour,
            calendar_hour,
            gload,
            heat_input,
            so2_mass,
            so2_rate,
            nox_rate,
            nox_mass
        FROM calc_hourly_base hourly
        
        JOIN calendar_hours ch
        ON hourly.op_date = ch.op_date
        AND hourly.op_hour = ch.op_hour""")
    
def process_results(conn, inputvars, logfile):
    """Summarize hourly data and merge base year and future year results.

    Keyword arguments:
    conn      -- a valid database connection where the data is stored
    inputvars -- a dictionary of input options
    logfile   -- file where logging messages will be written

    """

    conn.execute("""DELETE FROM ertac_pusp_info_file WHERE offline_start_date < ? """, [str(inputvars['future_year']) + '-01-01'])
    conn.execute("""DELETE FROM calc_updated_uaf WHERE offline_start_date < ? """, [str(inputvars['future_year']) + '-01-01'])
    
    #we are going to divy everything up by state/fuel bin to ease the burden of lots of calls to huge dbs
    (where, inputs) = build_where(conn, 'calc_updated_uaf.', inputvars, False, True)
    query = """SELECT state, ertac_fuel_unit_type_bin FROM calc_updated_uaf WHERE 1 """+where+""" GROUP BY state, ertac_fuel_unit_type_bin"""

    for (state, fuel_unit_type_bin) in conn.execute(query, inputs).fetchall():
        logging.info("Processing - " + state + ", " + fuel_unit_type_bin)
        
        conn.executescript("""CREATE TEMPORARY TABLE fy_emissions
        (ertac_region TEXT NOT NULL COLLATE NOCASE,
        ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
        orispl_code TEXT NOT NULL COLLATE NOCASE,
        unitid TEXT NOT NULL COLLATE NOCASE,
        calendar_hour INTEGER NOT NULL,
        hierarchy_hour INTEGER NOT NULL,
        gload REAL,
        heat_input REAL,
        so2_rate REAL,
        so2_mass REAL,
        nox_rate REAL,
        nox_mass REAL,
        pm25_rate REAL,
        pm25_mass REAL,
        pm10_rate REAL,
        pm10_mass REAL,
        co_rate REAL,
        co_mass REAL,
        voc_rate REAL,
        voc_mass REAL,
        nh3_rate REAL,
        nh3_mass REAL,
        PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, calendar_hour, orispl_code, unitid),
        UNIQUE (ertac_region, ertac_fuel_unit_type_bin, calendar_hour, orispl_code, unitid));""")    
        
        #start by determining the appropriate emission rate for each of the four pollutants
        #with priority given to emission rate, control efficiency, base year rate, default rate
        #control efficiency is also multiplied times the base year or default rate
    
        logging.info("  Loading Default Emission Rates")
        print >> logfile, "  Loading Default Emission Rates"
        conn.execute("""INSERT INTO fy_emission_rates(ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid, pm25_rate, pm10_rate, co_rate, voc_rate, nh3_rate)
        SELECT  cuuaf.ertac_region, 
                cuuaf.ertac_fuel_unit_type_bin, 
                cuuaf.orispl_code, 
                cuuaf.unitid, 
                eav.pm25_rate,
                eav.pm10_rate,
                eav.co_rate,
                eav.voc_rate,
                eav.nh3_rate
                            
        FROM calc_updated_uaf cuuaf
        
        LEFT JOIN ertac_additional_variables eav
        ON cuuaf.state = eav.state
        AND cuuaf.ertac_fuel_unit_type_bin = eav.ertac_fuel_unit_type_bin
        
        WHERE cuuaf.state = ? AND cuuaf.ertac_fuel_unit_type_bin = ? """, [state, fuel_unit_type_bin])
    
        logging.info("  Processing Unit Level Emission Rates")
        print >> logfile, "  Processing Unit Level Emission Rates"
        
        for (polcode, column) in [['CO', 'co'], ['PM10', 'pm10'], ['PM2_5', 'pm25'], ['VOC', 'voc'], ['NH3', 'nh3']]:    
            if polcode not in inputvars['pollutants']:
                for (rate, future_rate, future_control, orispl_code, unitid) in conn.execute("""SELECT base_year_rate, emission_rate, control_efficiency, eac.orispl_code, eac.unitid FROM ertac_base_year_rates_and_additional_controls eac INNER JOIN fy_emission_rates fyer ON eac.orispl_code = fyer.orispl_code AND eac.unitid = fyer.unitid WHERE pollutant_code = ? """, [polcode]).fetchall():
                    if future_rate is not None:
                        rate = future_rate
                    else:
                        if future_control is not None:
                            if rate is not None:                            
                                rate = rate * (1.0 - future_control / 100.0)
                            else:
                                (default_rate,) = conn.execute("""SELECT """+column+"""_rate FROM fy_emission_rates WHERE orispl_code = ? AND unitid = ?""", [orispl_code, unitid]).fetchone() 
                                if default_rate is not None:
                                    rate = default_rate * (1.0 - future_control / 100.0)
                    if rate is not None:        
                        conn.execute("""UPDATE fy_emission_rates SET """+column+"""_rate = ? WHERE orispl_code = ? AND unitid = ?""", [rate, orispl_code, unitid]) 
    
        logging.info("  Calculating Emissions")
        print >> logfile, "  Calculating Emissions"
        conn.execute("""INSERT INTO fy_emissions(ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid, calendar_hour, hierarchy_hour, gload, heat_input, so2_rate, so2_mass, nox_rate, nox_mass, pm25_rate, pm10_rate, co_rate, voc_rate, nh3_rate, pm25_mass, pm10_mass, co_mass, voc_mass, nh3_mass)
        SELECT  hdf.ertac_region, 
                hdf.ertac_fuel_unit_type_bin, 
                hdf.orispl_code, 
                hdf.unitid, 
                calendar_hour, 
                hierarchy_hour, 
                gload, 
                heat_input, 
                so2_rate/2000.0, 
                so2_mass/2000.0, 
                nox_rate/2000.0, 
                nox_mass/2000.0,
                fyer.pm25_rate/2000.0,
                fyer.pm10_rate/2000.0,
                fyer.co_rate/2000.0,
                fyer.voc_rate/2000.0,
                fyer.nh3_rate/2000.0,
                fyer.pm25_rate * heat_input/2000.0,
                fyer.pm10_rate * heat_input/2000.0,
                fyer.co_rate * heat_input/2000.0,
                fyer.voc_rate * heat_input/2000.0,
                fyer.nh3_rate * heat_input/2000.0
                            
        FROM hourly_diagnostic_file hdf
        
        INNER JOIN fy_emission_rates fyer
        
        ON hdf.orispl_code = fyer.orispl_code
        AND hdf.unitid = fyer.unitid
        AND hdf.ertac_region = fyer.ertac_region
        AND hdf.ertac_fuel_unit_type_bin = fyer.ertac_fuel_unit_type_bin""")  
     
        logging.info("  Converting Files to SMOKE Ready")         
        print >> logfile, "  Converting Files to SMOKE Ready"
        (where, inputs) = build_where(conn, 'cuuaf.', inputvars, False, True)
        
        # we don't need state or fuel_unit_type_bin here since we are already looping on those    
        query = """SELECT COALESCE(substr(cuuaf.fips_code,1,2),"99"), COALESCE(substr(cuuaf.fips_code,3),"999"), eauaf.plantid, eauaf.pointid, eauaf.stackid, eauaf.segment, cuuaf.orispl_code, cuuaf.unitid, cuuaf.ertac_region, camd_by_hourly_data_type, eauaf.time_zone, COALESCE(eauaf.scc, new_unit_scc), plant_latitude, plant_longitude
                       
                FROM calc_updated_uaf cuuaf
                
                LEFT JOIN ertac_pusp_info_file eauaf            
                ON eauaf.orispl_code = cuuaf.orispl_code
                AND eauaf.unitid = cuuaf.unitid
                AND cuuaf.ertac_region = eauaf.ertac_region
                AND cuuaf.ertac_fuel_unit_type_bin = eauaf.ertac_fuel_unit_type_bin
                
                LEFT JOIN ertac_additional_variables  eav
                ON eav.state = cuuaf.state 
                AND eav.ertac_fuel_unit_type_bin = cuuaf.ertac_region    
                WHERE (cuuaf.offline_start_date >= ? OR cuuaf.offline_start_date is NULL)
                AND cuuaf.state = ? 
                AND cuuaf.ertac_fuel_unit_type_bin = ?"""

        for (statefips, countyfips, plantid, pointid, stackid, segment, orispl_code, unitid, region, camd_by_hourly_data_type, tz, scc, lat, lon) in conn.execute(query + where, [str(inputvars['future_year']) + '-01-01', state, fuel_unit_type_bin] + inputs).fetchall():
            if not tz:
                result = conn.execute("""SELECT eauaf.time_zone 
                                        FROM calc_updated_uaf cuuaf
                
                                        LEFT JOIN ertac_pusp_info_file eauaf            
                                        ON eauaf.orispl_code = cuuaf.orispl_code
                                        AND eauaf.unitid = cuuaf.unitid
                                        AND cuuaf.ertac_region = eauaf.ertac_region
                                        AND cuuaf.ertac_fuel_unit_type_bin = eauaf.ertac_fuel_unit_type_bin
                                        
                                        WHERE plant_latitude = ?
                                        AND plant_longitude = ?""", [lat, lon]).fetchone()
                if result is not None:
                    (tz,)=result
                else:                        
                    try:
                        geonames_client = geonames.GeonamesClient('ertacegu')
                        geonames_result = geonames_client.find_timezone({'lat': lat, 'lng': lon})
                        tz = geonames_result['timezoneId']
                        print >> logfile, "  Time zone looked up for "+orispl_code+"/"+unitid+" found to be " + tz
                
                    except geonames.GeonamesError, err:
                        logging.error('Error getting timezone for %s, %s: %s' % (orispl_code, unitid, err))
                    
            if (plantid and pointid and stackid and segment) or camd_by_hourly_data_type != 'Non-EGU':                  
                for (polcode, column) in [['NOX', 'nox'], ['SO2', 'so2'], ['CO', 'co'], ['PM10', 'pm10'], ['PM2_5', 'pm25'], ['VOC', 'voc'], ['NH3', 'nh3']]:           
                    if inputvars['output_type'] == 'ORL':
                        if not (plantid or pointid or stackid or segment):
                            percentage = 100.0                    
                            (stack_count, ) = conn.execute("""SELECT COUNT(*) FROM orl_future WHERE plantid = ? AND pointid = ? AND cas = ? """, [camd_by_hourly_data_type+"_" +  orispl_code, camd_by_hourly_data_type+"_" +  unitid, polcode]).fetchone() 
                            plant_info = [statefips, countyfips, camd_by_hourly_data_type+"_" +  orispl_code, camd_by_hourly_data_type+"_" +  unitid, stack_count+1, "1", polcode, '', 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,scc]                  
                            result = conn.execute("""SELECT new_unit_stkhgt, new_unit_stkdiam, new_unit_stktemp, new_unit_stkflow, new_unit_stkvel, new_unit_scc FROM ertac_additional_variables WHERE state = ? AND ertac_fuel_unit_type_bin = ?""", [state, fuel_unit_type_bin]).fetchone()
                            if result:
                                (stkhgt, stkdiam, stktemp, stkflow, stkvel, scc) = result
                            else:
                                (stkhgt, stkdiam, stktemp, stkflow, stkvel, scc) = [-9,-9,-9,-9,-9, -9]
                            plant_info[33] = scc
                        else:
                            plant_info = [statefips, countyfips, plantid, pointid, stackid, segment, polcode, '', 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,scc]
                            (percentage, ) = conn.execute("""SELECT COALESCE("""+column+"""_percentage, 0) 
                                        FROM ertac_pusp_info_file eauaf
                                        WHERE eauaf.plantid = ?
                                        AND eauaf.pointid = ?
                                        AND eauaf.stackid = ?
                                        AND eauaf.segment = ?""", [plantid, pointid, stackid, segment]).fetchone()
                        
                        d = 0
                        h = 0
                        daily_total=0
                        annual_total=0
                        
                        for (heat_input, mass) in conn.execute("""SELECT COALESCE(heat_input,0.0), COALESCE("""+column+"""_mass,0.0) FROM fy_emissions WHERE orispl_code = ? AND unitid = ? ORDER BY calendar_hour """, [orispl_code, unitid]).fetchall():
                            plant_info[h+8]=mass*percentage/100.0
                            daily_total+=plant_info[h+8]
                            annual_total+=plant_info[h+8]                        
                            h+=1 
                            if h == 24:
                                plant_info[32]=daily_total
                                plant_info[7]=(datetime.date(int(inputvars['base_year']),1,1) + datetime.timedelta(d)).strftime('%Y%m%d')                     
                                conn.execute("""INSERT INTO pt_hourly_future(state, county, plantid, pointid, stackid, segment, polcode, op_date, time_zone, hrval1, hrval2, hrval3, hrval4, hrval5, hrval6, hrval7, hrval8, hrval9, hrval10, hrval11, hrval12, hrval13, hrval14, hrval15, hrval16, hrval17, hrval18, hrval19, hrval20, hrval21, hrval22, hrval23, hrval24, day_tot, scc)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", plant_info)
                                h = 0
                                d += 1
                                daily_total = 0                  
        
                    
                        if not (plantid or pointid or stackid or segment):
                            conn.execute("""INSERT INTO orl_future(fips, plantid, pointid, stackid, segment, plant, scc, erprtype, srctype, stkhgt, stkdiam, stktemp, stkflow, stkvel, sic_code, mact, naics, ctype, lon, lat, utmz, cas, ann_emis, avd_emis, orispl_code, unitid, ipm_yn, data_source)
                            SELECT fips_code, ?, ?, ?, ?, facility_name, ?, '02', '01', ?, ?, ?, ?, ?, '', '', '', 'L', plant_longitude, plant_latitude, '', ?, ?, -9, cuuaf.orispl_code, cuuaf.unitid, 'N', 'ERTAC'
                            FROM calc_updated_uaf cuuaf WHERE cuuaf.orispl_code = ? AND cuuaf.unitid = ? AND cuuaf.ertac_region = ? AND cuuaf.ertac_fuel_unit_type_bin = ?""", [plant_info[2], plant_info[3], plant_info[4], plant_info[5], scc, stkhgt, stkdiam, stktemp, stkflow, stkvel, polcode, annual_total, orispl_code, unitid, region, fuel_unit_type_bin])
                        else:
                            conn.execute("""INSERT INTO orl_future(fips, plantid, pointid, stackid, segment, plant, scc, erprtype, srctype, stkhgt, stkdiam, stktemp, stkflow, stkvel, sic_code, mact, naics, ctype, lon, lat, utmz, cas, ann_emis, avd_emis, orispl_code, unitid, ipm_yn, data_source)
                            SELECT fips_code, plantid, pointid, stackid, segment, facility_name, COALESCE(scc,new_unit_scc), '02', '01', COALESCE(stkhgt,new_unit_stkhgt) , COALESCE(stkdiam,new_unit_stkdiam), COALESCE(stktemp,new_unit_stktemp), stkflow, COALESCE(stkvel,new_unit_stkvel), sic_code, mact, naics, 'L', plant_longitude, plant_latitude, '', ?, ?, -9, cuuaf.orispl_code, cuuaf.unitid, 'N', 'ERTAC'
                            FROM calc_updated_uaf cuuaf
                            LEFT JOIN ertac_pusp_info_file eauaf                           
                            
                            
                            ON eauaf.orispl_code = cuuaf.orispl_code
                            AND eauaf.unitid = cuuaf.unitid
                            AND cuuaf.ertac_region = eauaf.ertac_region
                            AND cuuaf.ertac_fuel_unit_type_bin = eauaf.ertac_fuel_unit_type_bin
                            
                            LEFT JOIN ertac_additional_variables  eav
                            ON eav.state = cuuaf.state 
                            AND eav.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin    
                
                            WHERE eauaf.plantid = ?
                            AND eauaf.pointid = ?
                            AND eauaf.stackid = ?
                            AND eauaf.segment = ?
                            AND eauaf.orispl_code = ?
                            AND eauaf.unitid = ?""", [polcode, annual_total, plantid, pointid, stackid, segment, orispl_code, unitid])
                
                    #ff10 section
                    else:                    
                        if (plantid is None or pointid is None or stackid is None or segment is None):
                            percentage = 100.0                    
                            (stack_count, ) = conn.execute("""SELECT COUNT(*) FROM ff10_future WHERE plantid = ? AND pointid = ? AND cas = ?""", [camd_by_hourly_data_type+"_" + orispl_code, camd_by_hourly_data_type+"_" + unitid, polcode]).fetchone() 
                            plant_info = ['US', statefips + countyfips, camd_by_hourly_data_type+"_" + orispl_code, camd_by_hourly_data_type+"_" + unitid, stack_count+1, "1", scc, polcode, time.strftime("%Y%m%d"), '', 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
                            result = conn.execute("""SELECT new_unit_stkhgt, new_unit_stkdiam, new_unit_stktemp, new_unit_stkflow, new_unit_stkvel, new_unit_scc FROM ertac_additional_variables WHERE state = ? AND ertac_fuel_unit_type_bin = ?""", [state, fuel_unit_type_bin]).fetchone()
                            if result:
                                (stkhgt, stkdiam, stktemp, stkflow, stkvel, scc) = result
                            else:                           
                                (stkhgt, stkdiam, stktemp, stkflow, stkvel, scc) = [-9,-9,-9,-9,-9, -9]
                            plant_info[6] = scc
                        else:
                            plant_info = ['US', statefips + countyfips, plantid, pointid, stackid, segment, scc, polcode, time.strftime("%Y%m%d"), '', 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
                            (percentage, ) = conn.execute("""SELECT COALESCE("""+column+"""_percentage, 0) 
                                        FROM ertac_pusp_info_file eauaf
                                        WHERE eauaf.plantid = ?
                                        AND eauaf.pointid = ?
                                        AND eauaf.stackid = ?
                                        AND eauaf.segment = ?""", [plantid, pointid, stackid, segment]).fetchone()

                        d = 0
                        h = 0
                        daily_total=0
                        annual_total=0
                        
                        for (heat_input, mass) in conn.execute("""SELECT COALESCE(heat_input,0.0), COALESCE("""+column+"""_mass,0.0) FROM fy_emissions WHERE orispl_code = ? AND unitid = ? ORDER BY calendar_hour """, [orispl_code, unitid]).fetchall():
                            plant_info[h+11]=mass*percentage/100.0
                            daily_total+=plant_info[h+11]
                            annual_total+=plant_info[h+11]                        
                            h+=1 
                            if h == 24:
                                plant_info[10]=daily_total
                                plant_info[9]=(datetime.date(int(inputvars['base_year']),1,1) + datetime.timedelta(d)).strftime('%Y%m%d')                   
                                conn.execute("""INSERT INTO ff10_hourly_future(country, fips, plantid, pointid, stackid, segment, scc, polcode, date_updated, op_date, day_tot, hrval1, hrval2, hrval3, hrval4, hrval5, hrval6, hrval7, hrval8, hrval9, hrval10, hrval11, hrval12, hrval13, hrval14, hrval15, hrval16, hrval17, hrval18, hrval19, hrval20, hrval21, hrval22, hrval23, hrval24)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", plant_info)
                                h = 0
                                d += 1
                                daily_total = 0                  
        
                        if not (plantid or pointid or stackid or segment): 
                            conn.execute("""INSERT INTO ff10_future(country, fips, plantid, pointid, stackid, segment, agy_plantid, agy_pointid, agy_stackid, agy_segment, scc, cas, ann_emis, ann_pct_red, plant, erprtype, stkhgt, stkdiam, stktemp, stkflow, stkvel, naics, lon, lat, ll_datum, srctype, orispl_code, unitid, ipm_yn, calc_year, date_updated)
                            SELECT 'US', fips_code, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', facility_name, '02', ?, ?, ?, ?, ?, '', plant_longitude, plant_latitude, '001', '01', cuuaf.orispl_code, cuuaf.unitid, 'N', ?, date('now')
                            FROM calc_updated_uaf cuuaf WHERE cuuaf.orispl_code = ? AND cuuaf.unitid = ? AND cuuaf.ertac_region = ? AND cuuaf.ertac_fuel_unit_type_bin = ?""", [plant_info[2], plant_info[3], plant_info[4], plant_info[5], plant_info[2], plant_info[3], plant_info[4], plant_info[5], scc, polcode, annual_total, stkhgt, stkdiam, stktemp, stkflow, stkvel, inputvars['base_year'], orispl_code, unitid, region, fuel_unit_type_bin])
                        else:
                            conn.execute("""INSERT INTO ff10_future(country, fips, plantid, pointid, stackid, segment, agy_plantid, agy_pointid, agy_stackid, agy_segment, scc, cas, ann_emis, ann_pct_red, plant, erprtype, stkhgt, stkdiam, stktemp, stkflow, stkvel, naics, lon, lat, ll_datum, srctype, orispl_code, unitid, ipm_yn, calc_year, date_updated)
                                SELECT 'US', fips_code, plantid, pointid, stackid, segment, plantid, pointid, stackid, segment, COALESCE(scc, new_unit_scc), ?, ?, '', facility_name, '02', COALESCE(stkhgt,new_unit_stkhgt) , COALESCE(stkdiam,new_unit_stkdiam), COALESCE(stktemp,new_unit_stktemp), stkflow, COALESCE(stkvel,new_unit_stkvel), naics, plant_longitude, plant_latitude, '001', '01', cuuaf.orispl_code, cuuaf.unitid, 'N', ?, date('now')
                                FROM calc_updated_uaf cuuaf
                                LEFT JOIN ertac_pusp_info_file eauaf
                                
                                ON eauaf.orispl_code = cuuaf.orispl_code
                                AND eauaf.unitid = cuuaf.unitid
                                AND cuuaf.ertac_region = eauaf.ertac_region
                                AND cuuaf.ertac_fuel_unit_type_bin = eauaf.ertac_fuel_unit_type_bin
                                
                                LEFT JOIN ertac_additional_variables  eav
                                ON eav.state = cuuaf.state 
                                AND eav.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin    
                                                        
                                WHERE eauaf.plantid = ?
                                AND eauaf.pointid = ?
                                AND eauaf.stackid = ?
                                AND eauaf.segment = ?
                                AND eauaf.orispl_code = ?
                                AND eauaf.unitid = ?""", [polcode, annual_total, inputvars['base_year'], plantid, pointid, stackid, segment, orispl_code, unitid])

            else:
                print >> logfile, orispl_code + ", " + unitid + ", " + polcode + "(" + camd_by_hourly_data_type + ") missing vital information for inclusion in smoke file" 
            
        conn.execute("""DROP TABLE fy_emissions""")
        
        # Save changes
    conn.commit()

def run_diagnostics(conn, inputvars, logfile):
    logging.info("Running Diangostics")   
    print >> logfile, "Running Diangostics"
    check_uaf_consistency(conn, inputvars, logfile)
    check_pusp_info_file_consistency(conn, inputvars, logfile)
    check_additional_control_consistency(conn, inputvars, logfile)
    check_additional_variables_consistency(conn, inputvars, logfile)

def check_uaf_consistency(conn, inputvars, logfile):
    missing_location_info = conn.execute("""SELECT orispl_code, unitid, state, camd_by_hourly_data_type FROM calc_updated_uaf WHERE fips_code IS NULL OR plant_latitude IS NULL OR plant_longitude IS NULL ORDER BY state""").fetchall()

    if len(missing_location_info) > 0:
        print >> logfile, "Warning:", len(missing_location_info), "facility/units in UAF are missing lat/long information necessary for SMOKE processing:"
        for unit in missing_location_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)

def check_pusp_info_file_consistency(conn, inputvars, logfile):
    non_unique_pusp_info_file = conn.execute("""SELECT plantid, pointid, stackid, segment, group_concat(state) FROM ertac_pusp_info_file GROUP BY plantid, pointid, stackid, segment HAVING COUNT(1) > 1""").fetchall()

    if len(non_unique_pusp_info_file) > 0:
        print >> logfile, "Warning:", len(non_unique_pusp_info_file), "plant/unit/stack/processes in PUSP Info have more than one entry:"
        for unit in non_unique_pusp_info_file:
            print >> logfile, "  " + ertac_lib.nice_str(unit)

    unit_uaf_not_pusp_info_file = conn.execute("""SELECT orispl_code, unitid FROM (SELECT * FROM calc_updated_uaf ORDER BY state) 
    EXCEPT SELECT orispl_code, unitid FROM ertac_pusp_info_file""").fetchall()

    if len(unit_uaf_not_pusp_info_file) > 0:
        print >> logfile, "Warning:", len(unit_uaf_not_pusp_info_file), "facility/units in UAF did not match any ORISPL_CODE, UNITID in PUSP Info File:"
        for unit in unit_uaf_not_pusp_info_file:
            result = conn.execute("""SELECT orispl_code, unitid, state, camd_by_hourly_data_type  FROM calc_updated_uaf WHERE orispl_code = ? AND unitid =? """, unit).fetchone()
            print >> logfile, "  " + ertac_lib.nice_str(result)

    unit_pusp_info_file_not_uaf = conn.execute("""SELECT orispl_code, unitid FROM ertac_pusp_info_file 
    EXCEPT SELECT orispl_code, unitid FROM calc_updated_uaf""").fetchall()

    if len(unit_pusp_info_file_not_uaf) > 0:
        print >> logfile, "Warning:", len(unit_pusp_info_file_not_uaf), "facility/units in additional UAF did not match any ORISPL_CODE, UNITID in UAF:"
        for unit in unit_pusp_info_file_not_uaf:
            result = conn.execute("""SELECT orispl_code, unitid, state FROM ertac_pusp_info_file WHERE orispl_code = ? AND unitid =? """, unit).fetchone()
            print >> logfile, "  " + ertac_lib.nice_str(result)
            
    for (column) in ['stkhgt', 'stkdiam', 'stktemp', 'stkflow', 'stkvel', 'scc']:    
        result = conn.execute("""SELECT orispl_code, unitid, state, plantid, pointid, stackid, segment  FROM ertac_pusp_info_file WHERE """+column+""" IS NULL  ORDER BY state""").fetchall()
        if len(result) > 0:
            print >> logfile, "Warning:", len(result), "facility/units were missing "+column+" and will use default"
            for unit in result:
                print >> logfile, "  " + ertac_lib.nice_str(unit)
                
    for (polcode, column) in [['NOX', 'nox'], ['SO2', 'so2'], ['CO', 'co'], ['PM10', 'pm10'], ['PM2_5', 'pm25'], ['VOC', 'voc'], ['NH3', 'nh3']]:    
        if polcode not in inputvars['pollutants']:    
            result = conn.execute("""SELECT SUM(COALESCE("""+column+"""_percentage,0)) as total, orispl_code, unitid FROM ertac_pusp_info_file GROUP BY orispl_code, unitid HAVING SUM(COALESCE("""+column+"""_percentage,0)) != 100.0  ORDER BY state""").fetchall()
            if len(result) > 0:
                print >> logfile, "Warning:", len(result), "facility/units in had a percentage distribution for "+polcode+" emissions that did not sum to 100:"
                for unit in result:
                    if unit[0] != 100:
                        result2 = conn.execute("""SELECT orispl_code, unitid, state, ? FROM ertac_pusp_info_file WHERE orispl_code = ? AND unitid =? """, unit).fetchone()
                        print >> logfile, "  " + ertac_lib.nice_str(result2)  

def qa_results(conn, inputvars, logfile):
    logging.info("Running QA Tests")   
    print >> logfile, "Running QA Tests"
    if inputvars['output_type'] == 'ORL':
        #TODO
        x = 1
    else:
        result = conn.execute("""SELECT cas, SUM(ann_emis) FROM ff10_future GROUP BY cas""")
        print >> logfile, "Displaying Annual Emissions By Pollutant"
        for row in result:
            print >> logfile, "  " + ertac_lib.nice_str(row)
            
        result = conn.execute("""SELECT ertac_region, cas, SUM(ann_emis) FROM ff10_future
                            LEFT JOIN calc_updated_uaf cuuaf
                            ON ff10_future.orispl_code = cuuaf.orispl_code
                            AND ff10_future.unitid = cuuaf.unitid
                            GROUP BY ertac_region, cas""")
        print >> logfile, "Displaying Annual Emissions By Region"
        for row in result:
            print >> logfile, "  " + ertac_lib.nice_str(row)
            
        result = conn.execute("""SELECT ertac_fuel_unit_type_bin, cas,SUM(ann_emis) FROM ff10_future
                            LEFT JOIN calc_updated_uaf cuuaf
                            ON ff10_future.orispl_code = cuuaf.orispl_code
                            AND ff10_future.unitid = cuuaf.unitid
                            GROUP BY ertac_fuel_unit_type_bin, cas""")
        print >> logfile, "Displaying Annual Emissions By Fuel/Unit Type"
        for row in result:
            print >> logfile, "  " + ertac_lib.nice_str(row)
            
        result = conn.execute("""SELECT orispl_code, unitid,count(*) FROM ff10_future GROUP BY orispl_code, unitid""")
        print >> logfile, "Displaying Distinct Units"
        for row in result:
            print >> logfile, "  " + ertac_lib.nice_str(row)
            
        result = conn.execute("""SELECT polcode, SUM(day_tot) FROM ff10_hourly_future GROUP BY polcode""")
        print >> logfile, "Displaying Sum of Daily Emissions By Pollutant"
        for row in result:
            print >> logfile, "  " + ertac_lib.nice_str(row)
            
        result = conn.execute("""SELECT fips, polcode, SUM(day_tot) FROM ff10_hourly_future GROUP BY fips, polcode""")
        print >> logfile, "Displaying Sum of Daily Emissions By Pollutant, County"
        for row in result:
            print >> logfile, "  " + ertac_lib.nice_str(row)

        result = conn.execute("""SELECT fips, polcode, SUM(hrval1 + hrval2 + hrval3 + hrval4 + hrval5 + hrval6 + hrval7 + hrval8 + hrval9 + hrval10 + hrval11 + hrval12 + hrval13 + hrval14 + hrval15 + hrval16 + hrval17 + hrval18 + hrval19 + hrval20 + hrval21 + hrval22 + hrval23 + hrval24) FROM ff10_hourly_future GROUP BY fips, polcode""")
        print >> logfile, "Displaying Sum of Hourly Emissions By Pollutant, County"
        for row in result:
            print >> logfile, "  " + ertac_lib.nice_str(row)              
            
            
def check_additional_control_consistency(conn, inputvars, logfile):
    """Check control/emissions for consistent dates, and presence of rate or efficiency.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year for the projection
    logfile -- file where logging messages will be written

    """
    print >> logfile
    
    unit_additional_control_not_uaf = conn.execute("""SELECT orispl_code, unitid FROM ertac_base_year_rates_and_additional_controls 
    EXCEPT SELECT orispl_code, unitid FROM calc_updated_uaf""").fetchall()

    if len(unit_additional_control_not_uaf) > 0:
        print >> logfile, "Warning:", len(unit_additional_control_not_uaf), "facility/units in additional control did not match any ORISPL_CODE, UNITID in UAF:"
        for unit in unit_additional_control_not_uaf:
            print >> logfile, "  " + ertac_lib.nice_str(unit)

    unit_additional_control_not_uaf = conn.execute("""SELECT orispl_code, unitid FROM (SELECT * FROM calc_updated_uaf ORDER BY state) 
    EXCEPT SELECT orispl_code, unitid FROM ertac_base_year_rates_and_additional_controls""").fetchall()

    if len(unit_additional_control_not_uaf) > 0:
        print >> logfile, "Warning:", len(unit_additional_control_not_uaf), "facility/units in UAF have no control entries:"
        for unit in unit_additional_control_not_uaf:
            print >> logfile, "  " + ertac_lib.nice_str(unit)
                        
    print >> logfile, "Checking control/emissions for consistent dates, and presence of rate or efficiency."
    # Check that factor_start_date < factor_end_date if both are present.
    inconsistent_dates = conn.execute("""SELECT *
    FROM ertac_base_year_rates_and_additional_controls
    WHERE factor_start_date IS NOT NULL
    AND factor_end_date IS NOT NULL
    AND factor_start_date > factor_end_date
    ORDER BY orispl_code, unitid, pollutant_code, factor_start_date, factor_end_date""").fetchall()

    if len(inconsistent_dates) > 0:
        print >> logfile, "Warning: control/emissions has factor_start_date > factor_end_date:"
        for dates in inconsistent_dates:
            print >> logfile, "  " + ertac_lib.nice_str(dates)

    # Where multiple factors exist for same pollutant at same unit, check that
    # dates do not overlap.
    heading_printed = False

    multiple_factors = conn.execute("""SELECT orispl_code, unitid, pollutant_code, COUNT(*)
    FROM ertac_base_year_rates_and_additional_controls
    GROUP BY orispl_code, unitid, pollutant_code
    HAVING COUNT(*) > 1
    ORDER BY orispl_code, unitid, pollutant_code""").fetchall()

    for (plant, unit, poll, cnt) in multiple_factors:
        factors = conn.execute("""SELECT factor_start_date, factor_end_date
        FROM ertac_base_year_rates_and_additional_controls
        WHERE orispl_code = ?
        AND unitid = ?
        AND pollutant_code = ?
        AND (emission_rate IS NOT NULL
        OR control_efficiency IS NOT NULL)
        ORDER BY COALESCE(factor_start_date, ?),
        COALESCE(factor_end_date, ?)""", (plant, unit, poll, ertac_lib.online_default, ertac_lib.offline_default))

        (prev_start, prev_end) = factors.fetchone()
        for (next_start, next_end) in factors.fetchall():
            if prev_end is None or next_start is None or prev_end >= next_start:
                if not heading_printed:
                    print >> logfile, "Warning: control/emissions has factors with missing or overlapping start/end dates:"
                    heading_printed = True
            if prev_end is None:
                print >> logfile, "  " + ertac_lib.nice_str((plant, unit, poll, prev_start, prev_end)) + " missing end date"
            if next_start is None:
                print >> logfile, "  " + ertac_lib.nice_str((plant, unit, poll, next_start, next_end)) + " missing start date"
            if prev_end >= next_start:
                print >> logfile, "  " + ertac_lib.nice_str((plant, unit, poll)) + ": " + ertac_lib.nice_str((prev_start, prev_end)) + " overlaps " + ertac_lib.nice_str((next_start, next_end))
            (prev_start, prev_end) = (next_start, next_end)

    # 20120423 Added warning for check that control/emissions data is for future years.
    day_after_base_year = ertac_lib.first_day_after(inputvars['base_year'])

    factors_past_dates = conn.execute("""SELECT *
    FROM ertac_base_year_rates_and_additional_controls
    WHERE (factor_start_date IS NULL
    OR factor_start_date < ?)
    AND (control_efficiency IS NOT NULL OR emission_rate IS NOT NULL)
    ORDER BY orispl_code, unitid, pollutant_code, factor_start_date, factor_end_date""", (day_after_base_year,)).fetchall()

    if len(factors_past_dates) > 0:
        print >> logfile, "Warning: control/emissions has factor_start_date missing, before, or during base year; will be ignored:"
        for dates in factors_past_dates:
            print >> logfile, "  " + ertac_lib.nice_str(dates)

def check_additional_variables_consistency(conn, inputvars, logfile):
    print >> logfile, "Checking additional variables."
    state_missing = conn.execute("""SELECT state, ertac_fuel_unit_type_bin FROM calc_updated_uaf 
    EXCEPT SELECT state, ertac_fuel_unit_type_bin FROM ertac_additional_variables GROUP BY state, ertac_fuel_unit_type_bin""").fetchall()

    if len(state_missing) > 0:
        print >> logfile, "Warning:", len(state_missing), "states/ertac fuel unit type bin combination in UAF did not match any state in additional variables:"
        for state in state_missing:
            print >> logfile, "  " + ertac_lib.nice_str(state)

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
    
def write_final_data(conn, inputvars, out_prefix, logfile):
    """Write out projected ERTAC EGU data reports.

    Keyword arguments:
    conn       -- a valid database connection where the data is stored
    out_prefix -- optional prefix added to each output file name
    logfile    -- file where logging messages will be written

    """
    # Final output data is exported as CSV files for reporting and use with
    # other programs.
    if inputvars['output_type'] == 'ORL':
        header = ["#ORL      POINT", 
                  "#TYPE     Point Sources", 
                  "#COUNTRY  US", 
                  "#YEAR     "+inputvars['base_year'], 
                  "#DESC     ANNUAL", 
                  "#DESC     ertacegu@gmail.com", 
                  "#DESC     Emissions, in short tons, from ERTAC (Dropped unused columns after Column 46)", 
                  "#DESC     " + time.strftime("%m/%d/%y")]
        export_table_to_csv_with_smoke_header('orl_future', out_prefix, 'orl_future.csv', conn, orl_columns, header, logfile)
        header[0] = "#ORL      POINT"
        header[6] = "#DESC     Emissions, in short tons, from ERTAC"
        #update the hourly one so its not csv but space
        export_table_to_csv_with_smoke_header('pt_hourly_future', out_prefix, 'pt_hourly_future.csv', conn, pt_columns, [], logfile, [2,3,15,12,12,12,5,8,3,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,10])
    else:    
        header = ["#FORMAT   FF10_POINT", 
                  "#TYPE     Point Sources", 
                  "#COUNTRY  US", 
                  "#YEAR     "+inputvars['base_year'], 
                  "#YEAR     "+inputvars['future_year'], 
                  "#DESC     ANNUAL", 
                  "#DESC     ertacegu@gmail.com", 
                  "#DESC     Emissions, in short tons, from ERTAC (Dropped unused columns after Column EE)", 
                  "#DESC     " + time.strftime("%m/%d/%y")]
        export_table_to_csv_with_smoke_header('ff10_future', out_prefix, 'ff10_future.csv', conn, ff10_columns, header, logfile)
        header[0] = "#FORMAT   FF10_HOURLY_POINT"
        header[6] = "#DESC     Emissions, in short tons, from ERTAC"
        export_table_to_csv_with_smoke_header('ff10_hourly_future', out_prefix, 'ff10_hourly_future.csv', conn, ff10_hourly_future_columns, header, logfile)
        
    ertac_lib.export_table_to_csv('fy_emission_rates', out_prefix, 'fy_emission_rate_columns.csv', conn, fy_emission_rate_columns, logfile)

def create_for_smoke_tables(conn):
    ertac_lib.run_script_file('create_for_smoke_tables.sql', conn)

    # Also need state lookup table, for abbreviation-FIPS code conversion.
    conn.executescript("""
    DROP TABLE IF EXISTS states;
    CREATE TABLE states
    (state_code TEXT NOT NULL,
    state_abbreviation TEXT NOT NULL COLLATE NOCASE,
    state_name TEXT NOT NULL COLLATE NOCASE,
    PRIMARY KEY (state_code),
    UNIQUE (state_abbreviation));""")

def main(argv=None):
    # Main projection program begins here.
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "hdqv:o:",
            ["help", "debug", "quiet", "verbose", 
            "input-prefix-pre=", "input-prefix-proj=", "output-prefix=", "orl-files=", "sql-database=", "state=", "ignore-pollutants=", "input-type=", "output-type=", "run-qa"])
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
    inputvars['pollutants'] = []  
    inputvars['input_type'] = 'ERTAC'
    inputvars['output_type'] = 'FF10'
    inputvars['run_qa'] = False
    
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
        elif opt in ("--output-type"):
            if arg.upper() not in ["ORL", "FF10"]:
               print "Output Type Not Valid: Defaulting To FF10"               
            else:
               inputvars['output_type'] = arg.upper()
        elif opt in ("--input-type"):
            if arg.upper() not in ["ERTAC", "CAMD"]:
               print "Input Type Not Valid: Defaulting To ERTAC"               
            else:
               inputvars['input_type'] = arg.upper()
        elif opt in ("--ignore-pollutants"):
            pol_clean = True
            for pol in arg.split(","):
                if pol.upper() not in ["CO", "VOC", "PM_10", "PM_25", "NH3"]:
                    pol_clean = False
                    print "Pollutant Not Valid: Defaulting To None Ignored"
                else:
                    inputvars['pollutants'].push(pol.upper())
            if not pol_clean:
                inputvars['pollutants'] = []
        elif opt in ("--state"):
            state_clean = True  
            for state in arg.split(","):
                if state.upper() not in ertac_tables.state_set:
                    state_clean = False
                    print "State Not Valid: Defaulting To No Selection"
            if state_clean:
                inputvars['state'] = arg
        else:
            assert False, "unhandled option"

    output_prefix = build_prefix(output_prefix, inputvars)
    
    if debug_level == "DEBUG":
        # Detailed logging to file for postmortem analysis.
        logging.basicConfig(
            filename='ertac_for_smoke_debug_log.txt',
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
        logfilename = output_prefix + 'ertac_egu_for_smoke_log.txt'
    else:
        logfilename = 'ertac_egu_for_smoke_log.txt'

    try:
        logfile = open(logfilename, 'w')
    except IOError:
        print >> sys.stderr, "Log file: " + logfilename + " -- Could not be written.  Program will terminate."
        raise

    # Identify versions of Python and SQLite library, and record in log file.
    logging.info("Program started at " + time.asctime())
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
        create_for_smoke_tables(dbconn)
        # Load intermediate CSV data into tables, rejecting any rows that can't be
        # used.  There should be no invalid data at this stage, unless the
        # intermediate files were manually changed with erroneous data.
        logging.info("Loading intermediate data:")
        print >> logfile, "Loading intermediate data:"
        load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj,inputvars['input_type'], logfile)
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
            create_for_smoke_tables(dbconn)
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
    if inputvars['input_type'] == 'CAMD':
	inputvars['future_year'] = inputvars['base_year']
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

    if existing_db_file:
        logging.info("Using the existing summary data for plots and CSV files...")
        print >> logfile, "Using the existing summary data for plots and CSV files..."
    else:
        logging.info("Summarizing data...")
        print >> logfile
        print >> logfile, "Summarizing data..."
        if inputvars['input_type'] == 'CAMD':
            convert_camd_to_hdf(dbconn, logfile)
        run_diagnostics(dbconn, inputvars, logfile)
        process_results(dbconn, inputvars, logfile)
	if inputvars['run_qa']:
            qa_results(dbconn, inputvars, logfile)
        
    # Export projection report tables as CSV files.
    logging.info("Writing out reports:")
    write_final_data(dbconn, inputvars, output_prefix, logfile)
    logging.info("Finished writing reports.")
    dbconn.close()
    logging.info("Program ended at " + time.asctime())
    print >> logfile
    print >> logfile, "Program ended at " + time.asctime()


    # End of main routine

if __name__ == '__main__':
    sys.exit(main())
