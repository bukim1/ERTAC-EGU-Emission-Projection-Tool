#!/usr/bin/python

# ertac_postprocess.py

"""ERTAC EGU post processing"""

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

VERSION = "3.1"
#Updated to v3.1 as of February 15, 2024

import sys

try:
    import getopt, logging, os, time, re
except ImportError:
    print("Fatal error: can't import all required modules.", file=sys.stderr)
    print("Run python -V to find your Python version.", file=sys.stderr)
    raise

try:
    import sqlite3
except ImportError:
    try:
        from pysqlite2 import dbapi2 as sqlite3
    except ImportError:
        print("Fatal error: can't import all required modules.", file=sys.stderr)
        print("No SQLite3 available with this Python.", file=sys.stderr)
        raise

try:
    import ertac_lib as ertac_lib
    from ertac_tables import *  # importaing all dictionaries, tables, etc
    from ertac_reports import *
except ImportError:
    print("Fatal error: can't import all required modules.", file=sys.stderr)
    print("Put all ERTAG EGU library code in directory with preprocessor and projection model.", file=sys.stderr)
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

# Post-processor specific table structure definition
hourly_activity_summary_columns = (('ertac region', 'str', True, None),
                                   ('ertac fuel unit type bin', 'str', True, fuel_set),
                                   ('BY ertac fuel unit type bin', 'str', True, fuel_set),
                                   ('oris', 'str', True, None),
                                   ('unit id', 'str', True, None),
                                   ('state', 'str', True, state_set),
                                   ('calendar hour', 'int', True, (0, 8784)),
                                   ('hierarchy hour', 'int', True, (0, 8784)),
                                   ('Operating at Max HI', 'str', True, ('Y', 'N')),
                                   ('BY hierarchy hour', 'int', True, (0, 8784)),
                                   ('BY gload (MW)', 'float', False, (0.0, 2300.0)),
                                   ('FY gload (MW)', 'float', False, (0.0, 2300.0)),
                                   ('BY heat input (mmBtu)', 'float', False, (0.0, 29000.0)),
                                   ('FY heat input (mmBtu)', 'float', False, (0.0, 29000.0)),
                                   ('BY so2 mass (Tons)', 'float', False, (0.0, 100000.0)),
                                   ('FY so2 mass (Tons)', 'float', False, (0.0, 100000.0)),
                                   ('BY nox mass (Tons)', 'float', False, (0.0, 20000.0)),
                                   ('FY nox mass (Tons)', 'float', False, (0.0, 20000.0)),
                                   ('BY co2 mass (Tons)', 'float', False, None), #JMJ 1/18/2024 adding CO2 to post process
                                   ('FY co2 mass (Tons)', 'float', False, None),
                                   ('hour specific growth rate', 'float', False, None),
                                   ('afygr', 'float', False, None),
                                   ('data type', 'str', False, None),
                                   ('facility name', 'str', False, None))

daily_unit_activity_summary_columns = (('ertac region', 'str', True, None),
                                       ('ertac fuel unit type bin', 'str', True, fuel_set),
                                       ('BY ertac fuel unit type bin', 'str', True, fuel_set),
                                       ('oris', 'str', True, None),
                                       ('unit id', 'str', True, None),
                                       ('state', 'str', True, state_set),
                                       ('calendar day', 'int', True, (0, 365)),
                                       ('Ozone Season?', 'int', True, (0,1)),
                                       ('BY gload (MW)', 'float', False, (0.0, 2300.0)),
                                       ('FY gload (MW)', 'float', False, (0.0, 2300.0)),
                                       ('BY heat input (mmBtu)', 'float', False, (0.0, 29000.0)),
                                       ('FY heat input (mmBtu)', 'float', False, (0.0, 29000.0)),
                                       ('BY so2 mass (Tons)', 'float', False, (0.0, 100000.0)),
                                       ('FY so2 mass (Tons)', 'float', False, (0.0, 100000.0)),
                                       ('BY nox mass (Tons)', 'float', False, (0.0, 20000.0)),
                                       ('FY nox mass (Tons)', 'float', False, (0.0, 20000.0)),
                                       ('BY co2 mass (Tons)', 'float', False, None), # JMJ 1/18/2024 adding CO2 to post process
                                       ('FY co2 mass (Tons)', 'float', False, None),
                                       ('data type', 'str', False, None),
                                       ('facility name', 'str', False, None))

hourly_regional_summary_columns = (('ertac region', 'str', True, None),
                                   ('ertac fuel unit type bin', 'str', True, fuel_set),
                                   ('data type', 'str', False, None),
                                   ('calendar hour', 'int', True, (0, 8784)),
                                   ('hierarchy hour', 'int', True, (0, 8784)),
                                   ('BY gload (MW)', 'float', False, (0.0, 2300.0)),
                                   ('FY gload (MW)', 'float', False, (0.0, 2300.0)),
                                   ('Number of units op hour max', 'int', True, None),
                                   ('BY heat input (mmBtu)', 'float', False, (0.0, 29000.0)),
                                   ('FY heat input (mmBtu)', 'float', False, (0.0, 29000.0)),
                                   ('BY so2 mass (Tons)', 'float', False, (0.0, 100000.0)),
                                   ('FY so2 mass (Tons)', 'float', False, (0.0, 100000.0)),
                                   ('BY co2 mass (Tons)', 'float', False, None), # JMJ 1/18/2024 adding CO2 to post process
                                   ('FY co2 mass (Tons)', 'float', False, None),
                                   ('BY nox mass (Tons)', 'float', False, (0.0, 20000.0)),
                                   ('FY nox mass (Tons)', 'float', False, (0.0, 20000.0)),
                                   ('hour specific growth rate', 'float', False, None),
                                   ('afygr', 'float', False, None))

hourly_state_summary_columns = (('state', 'str', True, state_set),
                                ('ertac fuel unit type bin', 'str', True, fuel_set),
                                ('calendar hour', 'int', True, (0, 8784)),
                                ('BY gload (MW)', 'float', False, (0.0, 2300.0)),
                                ('FY gload (MW)', 'float', False, (0.0, 2300.0)),
                                ('BY heat input (mmBtu)', 'float', False, (0.0, 29000.0)),
                                ('FY heat input (mmBtu)', 'float', False, (0.0, 29000.0)),
                                ('BY so2 mass (Tons)', 'float', False, (0.0, 100000.0)),
                                ('FY so2 mass (Tons)', 'float', False, (0.0, 100000.0)),
                                ('BY nox mass (Tons)', 'float', False, (0.0, 20000.0)),
                                ('FY nox mass (Tons)', 'float', False, (0.0, 20000.0)),
                                ('BY co2 mass (Tons)', 'float', False, None), # JMJ 1/18/2024 adding CO2 to post process
                                ('FY co2 mass (Tons)', 'float', False, None))

annual_summary_columns = (('oris', 'str', True, None),
                          ('unit id', 'str', True, None),
                          ('Facility Name', 'str', False, None),
                          ('State', 'str', True, state_set),
                          ('FIPS Code', 'str', False, None),
                          ('ertac region', 'str', True, None),
                          ('ertac fuel unit type bin', 'str', True, fuel_set),
                          ('BY ertac fuel unit type bin', 'str', True, fuel_set),
                          ('max unit heat input (mmBtu)', 'float', False, None),
                          ('ertac heat rate (btu/kw-hr)', 'float', False, (3000.0, 20000.0)),
                          ('Generation Capacity (MW)', 'float', False, None),
                          ('Nameplate Capacity (MW)', 'float', False, None),
                          ('Number of FY Hours Operating', 'int', True, (0, 8784)),
                          ('Number of FY Hours Operating at Max', 'int', True, (0, 8784)),
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
                          ('BY OS active days', 'float', False, None),
                          ('BY OS generation (MW-hrs)', 'float', False, None),
                          ('BY Average OS NOx/Active Day (ton/day)', 'float', False, None),
                          ('BY NonOS NOx (tons)', 'float', False, None),
                          ('BY Average NonOS NOx Rate (lbs/mmbtu)', 'float', False, None),
                          ('BY Annual CO2 (tons)', 'float', False, None),
                          ('BY Average Annual CO2 Rate (lbs/mmbtu)', 'float', False, None),
                          ('FY Annual SO2 (tons)', 'float', False, None),
                          ('FY Average Annual SO2 Rate (lbs/mmbtu)', 'float', False, None),
                          ('FY Hourly SO2 Mass Max (tons)', 'float', False, None),
                          ('FY Annual NOx (tons)', 'float', False, None),
                          ('FY Average Annual NOx Rate (lbs/mmbtu)', 'float', False, None),
                          ('FY Hourly NOx Mass Max (tons)', 'float', False, None),
                          ('FY OS NOx (tons)', 'float', False, None),
                          ('FY Average OS NOx Rate (lbs/mmbtu)', 'float', False, None),
                          ('FY OS heat input (mmbtu)', 'float', False, None),
                          ('FY OS active days', 'float', False, None),
                          ('FY OS generation (MW-hrs)', 'float', False, None),
                          ('FY Average OS NOx/Active Day (ton/day)', 'float', False, None),
                          ('FY NonOS NOx (tons)', 'float', False, None),
                          ('FY Average NonOS NOx Rate (lbs/mmbtu)', 'float', False, None),
                          ('FY Annual CO2 (tons)', 'float', False, None),
                          ('FY Average Annual CO2 Rate (lbs/mmbtu)', 'float', False, None),
                          ('Hierarchy Order', 'int', False, None),
                          ('Longitude', 'float', False, None),
                          ('Latitude', 'float', False, None),
                          ('Generation Deficit Unit?', 'str', False, ['Y', 'N']),
                          ('Retirement Date', 'str', False, None),
                          ('New Unit?', 'str', False, ['Y', 'N']),
                          ('data type', 'str', False, None),
                          ('program_codes', 'str', False, None))

gen_parms_columns = (('ertac_region', 'str', True, None),
                     ('ertac_fuel_unit_type_bin', 'str', True, fuel_set),
                     ('op_date', 'date', True, None),
                     ('op_hour', 'int', True, (0, 23)),
                     ('temporal_allocation_order', 'int', True, (1, 8784)),
                     ('hour_specific_growth_rate', 'float', False, None),
                     ('base_actual_generation', 'float', False, None),
                     ('base_retired_generation', 'float', False, None),
                     ('future_projected_generation', 'float', False, None),
                     ('future_projected_growth', 'float', False, None),
                     ('total_proxy_generation', 'float', False, None),
                     ('adjusted_projected_generation', 'float', False, None),
                     ('afygr', 'float', False, None),
                     ('excess_generation_pool', 'float', False, None))


def usage(progname):
    """Print brief usage message showing command-line options.

    Keyword arguments:
    progname -- program name to be inserted in usage message

    """

    print("""
Usage: %s [OPTION]...

  -h, --help        print this message.

  -d, --debug       log extended debugging information.
  -q, --quiet       quiet operation (no status messages).
  -v, --verbose     verbose status messages (default).
  --run-integrity.                      run a check for debugging to make sure gload totals match by region, fuel bin, and in total
  --sql-database=existing database.     use sql database at location rather than loading inputs
  --input-prefix-pre=prefix.            prefix used in preprocessor
  --input-prefix-proj=prefix.           prefix used in projection
  -o prefix, --output-prefix=prefix.    output prefix for postprocessor results

  --include-st-hr                       run the state level hourly summary
  --include-rg-hr                       run the regional level hourly summary
  --include-unit-day                    run the unit level daily summary
  --remove-feb29                        remove feb29 data during non-leap years

  --config-file=existing csv.           use csv to override all inputs except sql-database (only accepts double dashed, without the double dashes, e.g. input-prefix-proj)
  --state=state.                        limit analysis to this state
  --region=region.                      limit analysis to this ertac region
  --fuel-bin=fuel bin.                  limit analysis to this ertac fuel bin
  --orisid=orispl_code.                 limit analysis to this orispl code
  --unitid=unitid.                      limit analysis to this unit id (requires orispl code)
  --time-span=timespan.                 limit analysis to time predefined time span (Annual, FirstQtr, SecondQtr, ThirdQtr, FourthQtr, OzoneSeason)
""" % progname)


def load_intermediate_data(conn, in_prefix_pre, in_prefix_proj, inputvars, logfile):
    """Load intermediate ERTAC EGU data from preprocessor for projection.

    Keyword arguments:
    conn           -- a valid database connection where the data will be stored
    in_prefix_pre  -- optional prefix added to each input file name generated from preprocessor
    in_prefix_post -- optional prefix added to each input file name generated from projection
    logfile        -- file where logging messages will be written

    """

    # jmj fails when a necessary file is not load 150413
    ertac_lib.load_csv_into_table(None, os.path.join(os.path.relpath(sys.path[0]), 'states.csv'), 'states', conn,
                                  states_columns, logfile)
    # This section will reject any input rows that are missing required fields,
    # have unreadable data, or violate key constraints, because it is impossible
    # to store that data in the database tables.
    ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_growth_rates.csv', 'calc_growth_rates', conn,
                                  growth_rate_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_generation_parms_v2.csv', 'calc_generation_parms', conn,
                                  generation_parms_columns, logfile)

    if not ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_input_variables_v2.csv', 'calc_input_variables', conn,
                                         input_variable_columns, logfile):
        print("Fatal error: could not load necessary file calc_input_variables", file=sys.stderr)
        sys.exit(1)
    if not ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_unit_hierarchy.csv', 'calc_unit_hierarchy', conn,
                                         unit_hierarchy_columns, logfile):
        print("Fatal error: could not load necessary file calc_unit_hierarchy", file=sys.stderr)
        sys.exit(1)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'generic_units_created.csv', 'generic_units_created', conn,
                                  generic_units_created, logfile)

    if not ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_updated_uaf_v2.csv', 'calc_updated_uaf', conn,
                                         calc_uaf_columns, logfile):
        print("Fatal error: could not load necessary file calc_updated_uaf", file=sys.stderr)
        sys.exit(1)
    if not ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_hourly_base.csv', 'calc_hourly_base', conn,
                                         calc_hourly_columns, logfile):
        print("Fatal error: could not load necessary file calc_hourly_base", file=sys.stderr)
        sys.exit(1)

    if not ertac_lib.load_csv_into_table(in_prefix_proj, 'hourly_diagnostic_file_v2.csv', 'hourly_diagnostic_file',
                                         conn, hourly_diagnostic_file, logfile):
        print("Fatal error: could not load necessary file hourly_diagnostic_file", file=sys.stderr)
        sys.exit(1)

    (where, inputs) = build_where(conn, 'cuuaf.', inputvars, True, True)
    if where:
        logging.info("Removing lines from hourly diagnostic file and calc hourly base not needed for processing")
        query = """SELECT hdf.orispl_code, hdf.unitid FROM (SELECT * FROM hourly_diagnostic_file where hierarchy_hour = 1) as hdf
        LEFT JOIN (SELECT * FROM calc_updated_uaf cuuaf WHERE 1 """ + where + """) AS cuuaf
        ON hdf.orispl_code = cuuaf.orispl_code
        AND hdf.unitid = cuuaf.unitid
        AND hdf.ertac_region = cuuaf.ertac_region
        AND hdf.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
        WHERE cuuaf.orispl_code IS NULL"""
        for (orispl_code, unitid) in conn.execute(query, inputs).fetchall():
            conn.execute("""DELETE FROM hourly_diagnostic_file WHERE orispl_code = ? AND unitid = ?""",
                         [orispl_code, unitid])
            conn.execute("""DELETE FROM calc_hourly_base WHERE orispl_code = ? AND unitid = ?""", [orispl_code, unitid])


def summarize_hourly_results(conn, inputvars, logfile):
    """Summarize hourly data and merge base year and future year results.

    Keyword arguments:
    conn      -- a valid database connection where the data is stored
    inputvars -- a dictionary of input options
    logfile   -- file where logging messages will be written

    """

    # create a temp table of hierachy hours
    conn.executescript("""
        CREATE TEMPORARY TABLE calendar_hierarchy_hours
        (calendar_hour INTEGER NOT NULL,
        hierarchy_hour INTEGER NOT NULL,
        ertac_region TEXT NOT NULL COLLATE NOCASE,
        ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
        PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin,  hierarchy_hour)
        UNIQUE (ertac_region, ertac_fuel_unit_type_bin,  calendar_hour));
        
        INSERT INTO calendar_hierarchy_hours (ertac_region, ertac_fuel_unit_type_bin,  calendar_hour, hierarchy_hour)
        SELECT ertac_region, ertac_fuel_unit_type_bin,  calendar_hour, hierarchy_hour  
        FROM hourly_diagnostic_file hdf
        GROUP BY ertac_region, ertac_fuel_unit_type_bin,  calendar_hour, hierarchy_hour""")

    # we are going to divy everything up by state/fuel bin to ease the burden of lots of calls to huge dbs
    (where, inputs) = build_where(conn, 'calc_updated_uaf.', inputvars, False, True)
    
    #jmj 10/27/2022 remove feb29 if is not needed for review purposes
    if 'remove-feb29' in inputvars:
        (hour_count, ) = conn.execute("select count(distinct(calendar_hour)) as count from calendar_hierarchy_hours").fetchone()
        if(hour_count == 8784):
            where = where + " and calendar_hour <= 1416 and calendar_hour > 1440"
    query = """SELECT state, ertac_fuel_unit_type_bin FROM calc_updated_uaf WHERE 1 """ + where + """ GROUP BY state, ertac_fuel_unit_type_bin"""

    for (state, fuel_unit_type_bin) in conn.execute(query, inputs).fetchall():
        logging.info("Processing - " + state + ", " + fuel_unit_type_bin)

        # Full and Partial Reporters
        query = """SELECT hdf.ertac_region,
               hdf.ertac_fuel_unit_type_bin,
               hdf.ertac_fuel_unit_type_bin,
               hdf.orispl_code,
               hdf.unitid,
               hdf.state,
               hdf.calendar_hour,
               hdf.hierarchy_hour,
               hdf.hierarchy_hour,
               hourly_hi_limit,
               chb.gload,
               hdf.gload,
               chb.heat_input,
               hdf.heat_input,
               chb.so2_mass/2000,
               hdf.so2_mass/2000,
               chb.nox_mass/2000,
               hdf.nox_mass/2000,
               chb.co2_mass,
               hdf.co2_mass,
               cgp.hour_specific_growth_rate,
               cgp.afygr,
               upper(cuuaf.camd_by_hourly_data_type),
               cuuaf.facility_name
        FROM hourly_diagnostic_file hdf
        JOIN calendar_hours ch
        ON hdf.calendar_hour = ch.calendar_hour
    
        JOIN calc_updated_uaf cuuaf
        ON hdf.orispl_code = cuuaf.orispl_code
        AND hdf.unitid = cuuaf.unitid
        AND hdf.ertac_region = cuuaf.ertac_region
        AND hdf.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
    
        JOIN calc_generation_parms cgp
        ON cgp.op_date = ch.op_date
        AND cgp.op_hour = ch.op_hour
        AND cgp.ertac_region = cuuaf.ertac_region
        AND cgp.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
        
        JOIN calc_hourly_base chb
        ON chb.op_date = ch.op_date
        AND chb.op_hour = ch.op_hour
        AND cuuaf.orispl_code = chb.orispl_code
        AND cuuaf.unitid = chb.unitid
        AND cuuaf.ertac_region = chb.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = chb.ertac_fuel_unit_type_bin
    
        WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' 
            OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL')
            AND cuuaf.state = ? 
            AND cuuaf.ertac_fuel_unit_type_bin = ?
            AND (cuuaf.online_start_date <= ? 
            OR cuuaf.online_start_date IS NULL) 
            AND cuuaf.offline_start_date > ?"""

        (where, inputs) = build_where(conn, 'hdf.', inputvars, True, True)

        # Full and Partial Reporters
        query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, hourly_hi_limit, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, by_co2_mass, fy_co2_mass, hour_specific_growth_rate, afygr, data_type, facility_name)
        SELECT hdf.ertac_region,
               hdf.ertac_fuel_unit_type_bin,
               hdf.ertac_fuel_unit_type_bin,
               hdf.orispl_code,
               hdf.unitid,
               hdf.state,
               hdf.calendar_hour,
               hdf.hierarchy_hour,
               hdf.hierarchy_hour,
               hourly_hi_limit,
               chb.gload,
               hdf.gload,
               chb.heat_input,
               hdf.heat_input,
               chb.so2_mass/2000,
               hdf.so2_mass/2000,
               chb.nox_mass/2000,
               hdf.nox_mass/2000,
               chb.co2_mass,
               hdf.co2_mass,
               cgp.hour_specific_growth_rate,
               cgp.afygr,
               upper(cuuaf.camd_by_hourly_data_type),
               cuuaf.facility_name
        FROM hourly_diagnostic_file hdf
        JOIN calendar_hours ch
        ON hdf.calendar_hour = ch.calendar_hour
    
        JOIN calc_updated_uaf cuuaf
        ON hdf.orispl_code = cuuaf.orispl_code
        AND hdf.unitid = cuuaf.unitid
        AND hdf.ertac_region = cuuaf.ertac_region
        AND hdf.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
    
        JOIN calc_generation_parms cgp
        ON cgp.op_date = ch.op_date
        AND cgp.op_hour = ch.op_hour
        AND cgp.ertac_region = cuuaf.ertac_region
        AND cgp.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
        
        JOIN calc_hourly_base chb
        ON chb.op_date = ch.op_date
        AND chb.op_hour = ch.op_hour
        AND cuuaf.orispl_code = chb.orispl_code
        AND cuuaf.unitid = chb.unitid
        AND cuuaf.ertac_region = chb.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = chb.ertac_fuel_unit_type_bin
    
        WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' 
            OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL')
            AND cuuaf.state = ? 
            AND cuuaf.ertac_fuel_unit_type_bin = ?
            AND (cuuaf.online_start_date <= ? 
            OR cuuaf.online_start_date IS NULL) 
            AND cuuaf.offline_start_date > ?"""

        (where, inputs) = build_where(conn, 'hdf.', inputvars, True, True)
        conn.execute(query + where, [state, fuel_unit_type_bin, str(inputvars['base_year']) + '-12-31',
                                     str(inputvars['future_year']) + '-01-01'] + inputs)

        # query = """ INSERT INTO generation_parms (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour, temporal_allocation_order, hour_specific_growth_rate, base_actual_generation, base_retired_generation, future_projected_generation, future_projected_growth, total_proxy_generation, adjusted_projected_generation, afygr, excess_generation_pool)
        # SELECT ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour, temporal_allocation_order, hour_specific_growth_rate, base_actual_generation, base_retired_generation, future_projected_generation, future_projected_growth, total_proxy_generation, adjusted_projected_generation, afygr, excess_generation_pool
        # FROM calc_generation_parms cgp WHERE 1 """
        # (where, inputs) = build_where(conn, 'cgp.', inputvars, True, True)
        # conn.execute(query + where, inputs)

        #
        # Switchers
        query = """SELECT hdf.ertac_region,
               hdf.ertac_fuel_unit_type_bin,
               cuuaf.ertac_fuel_unit_type_bin,
               hdf.orispl_code,
               hdf.unitid,
               hdf.state,
               hdf.calendar_hour,
               hdf.hierarchy_hour,
               chh.hierarchy_hour,
               hourly_hi_limit,
               chb.gload,
               hdf.gload,
               chb.heat_input,
               hdf.heat_input,
               chb.so2_mass/2000,
               hdf.so2_mass/2000,
               chb.nox_mass/2000,
               hdf.nox_mass/2000,
               chb.co2_mass,
               hdf.co2_mass,
               cgp.hour_specific_growth_rate,
               cgp.afygr,
               'SWITCH',
               cuuaf.facility_name
        FROM hourly_diagnostic_file hdf
        JOIN calendar_hours ch
        ON hdf.calendar_hour = ch.calendar_hour
    
        JOIN calc_updated_uaf cuuaf
        ON hdf.orispl_code = cuuaf.orispl_code
        AND hdf.unitid = cuuaf.unitid
        AND hdf.ertac_region = cuuaf.ertac_region
        AND hdf.ertac_fuel_unit_type_bin != cuuaf.ertac_fuel_unit_type_bin
    
        JOIN calc_generation_parms cgp
        ON cgp.op_date = ch.op_date
        AND cgp.op_hour = ch.op_hour
        AND cgp.ertac_region = cuuaf.ertac_region
        AND cgp.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
    
        JOIN calendar_hierarchy_hours chh
        ON ch.calendar_hour = chh.calendar_hour
        AND cuuaf.ertac_region = chh.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = chh.ertac_fuel_unit_type_bin
         
        JOIN calc_hourly_base chb
        ON chb.op_date = ch.op_date
        AND chb.op_hour = ch.op_hour
        AND cuuaf.orispl_code = chb.orispl_code
        AND cuuaf.unitid = chb.unitid
        AND cuuaf.ertac_region = chb.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = chb.ertac_fuel_unit_type_bin
    
        WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' 
            OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL') 
            AND cuuaf.state = ? 
            AND cuuaf.ertac_fuel_unit_type_bin = ?
            AND (cuuaf.online_start_date <= ? 
            OR cuuaf.online_start_date IS NULL) 
            AND cuuaf.offline_start_date < ?"""

        (where, inputs) = build_where(conn, 'hdf.', inputvars, True, True)
        conn.execute(query + where, [state, fuel_unit_type_bin, str(inputvars['base_year']) + '-01-01',
                                     str(inputvars['future_year']) + '-01-01'] + inputs)

        #
        # Switchers
        query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, hourly_hi_limit, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, by_co2_mass, fy_co2_mass, hour_specific_growth_rate, afygr, data_type, facility_name)
        SELECT hdf.ertac_region,
               hdf.ertac_fuel_unit_type_bin,
               cuuaf.ertac_fuel_unit_type_bin,
               hdf.orispl_code,
               hdf.unitid,
               hdf.state,
               hdf.calendar_hour,
               hdf.hierarchy_hour,
               chh.hierarchy_hour,
               hourly_hi_limit,
               chb.gload,
               hdf.gload,
               chb.heat_input,
               hdf.heat_input,
               chb.so2_mass/2000,
               hdf.so2_mass/2000,
               chb.nox_mass/2000,
               hdf.nox_mass/2000,
               chb.co2_mass,
               hdf.co2_mass,
               cgp.hour_specific_growth_rate,
               cgp.afygr,
               'SWITCH',
               cuuaf.facility_name
        FROM hourly_diagnostic_file hdf
        JOIN calendar_hours ch
        ON hdf.calendar_hour = ch.calendar_hour
    
        JOIN calc_updated_uaf cuuaf
        ON hdf.orispl_code = cuuaf.orispl_code
        AND hdf.unitid = cuuaf.unitid
        AND hdf.ertac_region = cuuaf.ertac_region
        AND hdf.ertac_fuel_unit_type_bin != cuuaf.ertac_fuel_unit_type_bin
    
        JOIN calc_generation_parms cgp
        ON cgp.op_date = ch.op_date
        AND cgp.op_hour = ch.op_hour
        AND cgp.ertac_region = cuuaf.ertac_region
        AND cgp.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
    
        JOIN calendar_hierarchy_hours chh
        ON ch.calendar_hour = chh.calendar_hour
        AND cuuaf.ertac_region = chh.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = chh.ertac_fuel_unit_type_bin
         
        JOIN calc_hourly_base chb
        ON chb.op_date = ch.op_date
        AND chb.op_hour = ch.op_hour
        AND cuuaf.orispl_code = chb.orispl_code
        AND cuuaf.unitid = chb.unitid
        AND cuuaf.ertac_region = chb.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = chb.ertac_fuel_unit_type_bin
    
        WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' 
            OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL') 
            AND cuuaf.state = ? 
            AND cuuaf.ertac_fuel_unit_type_bin = ?
            AND (cuuaf.online_start_date <= ? 
            OR cuuaf.online_start_date IS NULL) 
            AND cuuaf.offline_start_date < ?"""

        (where, inputs) = build_where(conn, 'hdf.', inputvars, True, True)
        conn.execute(query + where, [state, fuel_unit_type_bin, str(inputvars['base_year']) + '-01-01',
                                     str(inputvars['future_year']) + '-01-01'] + inputs)

        # Switchers
        query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, hourly_hi_limit, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, by_co2_mass, fy_co2_mass, hour_specific_growth_rate, afygr, data_type, facility_name)
        SELECT hdf.ertac_region,
               hdf.ertac_fuel_unit_type_bin,
               cuuaf.ertac_fuel_unit_type_bin,
               hdf.orispl_code,
               hdf.unitid,
               hdf.state,
               hdf.calendar_hour,
               hdf.hierarchy_hour,
               NULL,
               hourly_hi_limit,
               0,
               hdf.gload,
               0,
               hdf.heat_input,
               0,
               hdf.so2_mass/2000,
               0,
               hdf.nox_mass/2000,
               0,
               hdf.co2_mass,
               cgp.hour_specific_growth_rate,
               cgp.afygr,
               'SWITCH',
               cuuaf.facility_name
        FROM hourly_diagnostic_file hdf
        JOIN calendar_hours ch
        ON hdf.calendar_hour = ch.calendar_hour
    
        JOIN calc_updated_uaf cuuaf
        ON hdf.orispl_code = cuuaf.orispl_code
        AND hdf.unitid = cuuaf.unitid
        AND hdf.ertac_region = cuuaf.ertac_region
        AND hdf.ertac_fuel_unit_type_bin != cuuaf.ertac_fuel_unit_type_bin
    
        JOIN calc_generation_parms cgp
        ON cgp.op_date = ch.op_date
        AND cgp.op_hour = ch.op_hour
        AND cgp.ertac_region = cuuaf.ertac_region
        AND cgp.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
    
        JOIN calendar_hierarchy_hours chh
        ON ch.calendar_hour = chh.calendar_hour
        AND cuuaf.ertac_region = chh.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = chh.ertac_fuel_unit_type_bin
         
        JOIN calc_hourly_base chb
        ON chb.op_date = ch.op_date
        AND chb.op_hour = ch.op_hour
        AND cuuaf.orispl_code = chb.orispl_code
        AND cuuaf.unitid = chb.unitid
        AND cuuaf.ertac_region = chb.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = chb.ertac_fuel_unit_type_bin
    
        WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' 
            OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL') 
            AND cuuaf.state = ? 
            AND cuuaf.ertac_fuel_unit_type_bin = ?
            AND (cuuaf.online_start_date <= ? 
            OR cuuaf.online_start_date IS NULL) 
            AND cuuaf.offline_start_date >= ? 
            AND cuuaf.offline_start_date <= ?"""

        (where, inputs) = build_where(conn, 'hdf.', inputvars, True, True)
        conn.execute(query + where, [state, fuel_unit_type_bin, str(inputvars['base_year']) + '-01-01',
                                     str(inputvars['future_year']) + '-01-01',
                                     str(inputvars['future_year']) + '-12-31'] + inputs)

        #
        # New Units
        query = """SELECT cuuaf.ertac_region,
               cuuaf.ertac_fuel_unit_type_bin,
               cuuaf.orispl_code,
               cuuaf.unitid,
               cuuaf.camd_by_hourly_data_type,
               cuuaf.facility_name
        FROM calc_updated_uaf cuuaf
        WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'NEW')
            AND cuuaf.state = ? 
            AND cuuaf.ertac_fuel_unit_type_bin = ?"""

        (where, inputs) = build_where(conn, 'cuuaf.', inputvars, False, True)
        for (region, fuel_bin, orisid, unitid, data_type, facility_name) in conn.execute(query + where, [state,
                                                                                                         fuel_unit_type_bin] + inputs).fetchall():

            # (old_unitid, ) = conn.execute("""SELECT unitid from calc_unit_hierarchy
            # WHERE orispl_code = ?
            # AND ertac_region = ?
            # AND ertac_fuel_unit_type_bin = ? ORDER BY unit_allocation_order ASC """, (orisid, region, fuel_bin)).fetchone()

            # Have to ensure it isn't a new unit that switched
            (switch_count,) = conn.execute("""SELECT count(*) FROM calc_updated_uaf cuuaf
            WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL') AND
            cuuaf.orispl_code = ? AND 
            cuuaf.ertac_region = ? AND 
            cuuaf.ertac_fuel_unit_type_bin != ? AND 
            cuuaf.unitid = ?""", [orisid, region, fuel_bin, unitid]).fetchone()

            if switch_count == 0:
                query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, hourly_hi_limit, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, by_co2_mass, fy_co2_mass, hour_specific_growth_rate, afygr, data_type, facility_name)
                SELECT hdf.ertac_region,
                       hdf.ertac_fuel_unit_type_bin,
                       hdf.ertac_fuel_unit_type_bin,
                       hdf.orispl_code,
                       hdf.unitid,
                       hdf.state,
                       hdf.calendar_hour,
                       hdf.hierarchy_hour,
                       NULL,
                       hourly_hi_limit,    
                       0,
                       hdf.gload,
                       0,
                       hdf.heat_input,
                       0,
                       hdf.so2_mass/2000,
                       0,
                       hdf.nox_mass/2000,
                       0,
                       hdf.co2_mass,
                       cgp.hour_specific_growth_rate,
                       cgp.afygr,
                       'NEW',
                       ?
                FROM hourly_diagnostic_file hdf
                JOIN calendar_hours ch
                ON hdf.calendar_hour = ch.calendar_hour    
        
                JOIN calc_generation_parms cgp
                ON cgp.op_date = ch.op_date
                AND cgp.op_hour = ch.op_hour
                AND cgp.ertac_region = hdf.ertac_region
                AND cgp.ertac_fuel_unit_type_bin = hdf.ertac_fuel_unit_type_bin
        
                WHERE hdf.orispl_code = ?
                AND hdf.ertac_region = ?
                AND hdf.ertac_fuel_unit_type_bin = ?
                AND hdf.unitid = ?"""

                (where, inputs) = build_where(conn, 'hdf.', inputvars, True, False)
                conn.execute(query + where, [facility_name, orisid, region, fuel_bin, unitid] + inputs)

        # get the units that only operated in the base year
        (where, inputs) = build_where(conn, 'cuuaf.', inputvars, False, True)
        query = """SELECT cuuaf.orispl_code, cuuaf.unitid, cuuaf.ertac_region, cuuaf.ertac_fuel_unit_type_bin 
        FROM calc_updated_uaf cuuaf 
        WHERE cuuaf.state = ? 
            AND cuuaf.ertac_fuel_unit_type_bin = ?
            AND(cuuaf.online_start_date <= ? 
            OR cuuaf.online_start_date IS NULL) 
            AND cuuaf.offline_start_date <= ? 
            AND (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' 
            OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL')"""

        for (orisid, unitid, region, fuel_bin) in conn.execute(query + where, [state, fuel_unit_type_bin,
                                                                               str(inputvars['base_year']) + '-01-01',
                                                                               str(inputvars[
                                                                                       'future_year']) + '-01-01'] + inputs).fetchall():
            # Have to ensure it isn't a new unit that switched
            (switch_count,) = conn.execute("""SELECT count(*) FROM hourly_activity_summary
            WHERE data_type = 'SWITCH' AND
            orispl_code = ? AND 
            ertac_region = ? AND 
            ertac_fuel_unit_type_bin != ? AND 
            unitid = ?""", [orisid, region, fuel_bin, unitid]).fetchone()

            if switch_count == 0:
                query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, hourly_hi_limit, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, by_co2_mass, fy_co2_mass, hour_specific_growth_rate, afygr, data_type, facility_name)
                SELECT cuuaf.ertac_region,
                       cuuaf.ertac_fuel_unit_type_bin,
                       cuuaf.ertac_fuel_unit_type_bin,
                       cuuaf.orispl_code,
                       cuuaf.unitid,
                       cuuaf.state,
                       ch.calendar_hour,
                       NULL,
                       chh.hierarchy_hour,
                       'N',    
                       chb.gload,
                       0,
                       chb.heat_input,
                       0,
                       chb.so2_mass/2000,
                       0/2000,
                       chb.nox_mass/2000,
                       0/2000,                       
                       chb.co2_mass,
                       0,
                       NULL,
                       NULL,
                       'RETIRED',
                       cuuaf.facility_name
                FROM calc_updated_uaf cuuaf
        
                LEFT JOIN calc_hourly_base chb
                ON cuuaf.orispl_code = chb.orispl_code
                AND cuuaf.unitid = chb.unitid
                AND cuuaf.ertac_region = chb.ertac_region
                AND cuuaf.ertac_fuel_unit_type_bin = chb.ertac_fuel_unit_type_bin
                
                JOIN calendar_hours ch
                ON chb.op_date = ch.op_date
                AND chb.op_hour = ch.op_hour
            
                JOIN calendar_hierarchy_hours chh
                ON ch.calendar_hour = chh.calendar_hour
                AND cuuaf.ertac_region = chh.ertac_region
                AND cuuaf.ertac_fuel_unit_type_bin = chh.ertac_fuel_unit_type_bin
                
                JOIN calc_generation_parms cgp
                ON cgp.op_date = ch.op_date
                AND cgp.op_hour = ch.op_hour
                AND cgp.ertac_region = cuuaf.ertac_region
                AND cgp.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
                    
                WHERE cuuaf.orispl_code = ?
                AND cuuaf.unitid = ?
                AND cuuaf.ertac_region = ?
                AND cuuaf.ertac_fuel_unit_type_bin = ?"""

                (where, inputs) = build_where(conn, 'cuuaf.', inputvars, True, False)
                conn.execute(query + where, [orisid, unitid, region, fuel_bin] + inputs)

    # jump out of the state, fuel bin loop here
    #JMJ 1/25/2024 making this required to calculate nox per active OSD
    conn.execute("""INSERT INTO daily_unit_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_day, ozone_season, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, by_co2_mass, fy_co2_mass, data_type, facility_name)
            SELECT 
            ertac_region, 
            ertac_fuel_unit_type_bin, 
            by_ertac_fuel_unit_type_bin, 
            orispl_code, 
            unitid, 
            state, 
            op_date,
            sum((CASE WHEN has.calendar_hour >= ? and has.calendar_hour <= ? THEN 1 ELSE 0 END))/24,
            sum(COALESCE(by_gload,0)), 
            sum(COALESCE(fy_gload,0)), 
            sum(COALESCE(by_heat_input,0)), 
            sum(COALESCE(fy_heat_input,0)), 
            sum(COALESCE(by_so2_mass,0)), 
            sum(COALESCE(fy_so2_mass,0)), 
            sum(COALESCE(by_nox_mass,0)), 
            sum(COALESCE(fy_nox_mass,0)), 
            sum(COALESCE(by_co2_mass,0)), 
            sum(COALESCE(fy_co2_mass,0)),
            data_type,
            facility_name

            FROM hourly_activity_summary has
            JOIN calendar_hours ch
            ON has.calendar_hour = ch.calendar_hour

            GROUP BY ertac_region,  ertac_fuel_unit_type_bin, orispl_code, unitid, state, data_type, facility_name, op_date""",
                 [inputvars['ozone_start_hour'], inputvars['ozone_end_hour']])

    conn.execute("""INSERT INTO annual_summary(ertac_region, 
            ertac_fuel_unit_type_bin, 
            by_ertac_fuel_unit_type_bin, 
            orispl_code, 
            unitid, 
            state, 
            fips_code, 
            longitude,
            latitude,
            retirement_date,
            hierarchy_order,
            max_unit_heat_input, 
            ertac_heat_rate, 
            generation_capacity, 
            nameplate_capacity,
            fy_op_hours_max, 
            fy_op_hours, 
            by_uf, 
            fy_uf, 
            by_gload, 
            fy_gload, 
            by_heat_input, 
            fy_heat_input, 
            by_os_heat_input, 
            fy_os_heat_input, 
            by_os_gload, 
            fy_os_gload, 
            by_so2_mass, 
            fy_so2_mass, 
            by_nox_mass, 
            fy_nox_mass, 
            by_os_nox_mass, 
            fy_os_nox_mass, 
            by_non_os_nox_mass, 
            fy_non_os_nox_mass,
            by_so2_rate, 
            fy_so2_rate, 
            by_nox_rate, 
            fy_nox_rate, 
            by_os_nox_rate, 
            fy_os_nox_rate, 
            by_non_os_nox_rate, 
            fy_non_os_nox_rate,
            by_os_active_days,
            fy_os_active_days,
            by_os_nox_active_day,
            fy_os_nox_active_day,
            fy_so2_max,
            fy_nox_max,
            by_co2_mass, 
            fy_co2_mass, 
            data_type,
            gdu_flag,
            new_unit_flag, 
            facility_name,
            program_codes)
        SELECT has.ertac_region, 
            has.ertac_fuel_unit_type_bin, 
            has.by_ertac_fuel_unit_type_bin, 
            has.orispl_code, 
            has.unitid, 
            has.state, 
            cuuaf.fips_code, 
            cuuaf.plant_longitude,
            cuuaf.plant_latitude,
            offline_start_date,
            unit_allocation_order,
            max_ertac_hi_hourly_summer, 
            ertac_heat_rate, 
            1000 * max_ertac_hi_hourly_summer / ertac_heat_rate, 
            cuuaf.nameplate_capacity,
            sum(hourly_hi_limit='Y'), 
            sum(COALESCE(has.fy_heat_input,0)>0), 
            sum(COALESCE(has.by_heat_input,0))/(max_ertac_hi_hourly_summer* ?), 
            sum(COALESCE(has.fy_heat_input,0))/(max_ertac_hi_hourly_summer* ?), 
            sum(COALESCE(has.by_gload,0)), 
            sum(COALESCE(has.fy_gload,0)), 
            sum(COALESCE(has.by_heat_input,0)), 
            sum(COALESCE(has.fy_heat_input,0)), 
            sum(COALESCE(has.by_heat_input*(has.calendar_hour > ? and has.calendar_hour <= ?),0)), 
            sum(COALESCE(has.fy_heat_input*(has.calendar_hour > ? and has.calendar_hour <= ?),0)), 
            sum(COALESCE(has.by_gload*(has.calendar_hour > ? and has.calendar_hour <= ?),0)), 
            sum(COALESCE(has.fy_gload*(has.calendar_hour > ? and has.calendar_hour <= ?),0)), 
            sum(COALESCE(has.by_so2_mass,0)), 
            sum(COALESCE(has.fy_so2_mass,0)),
            sum(COALESCE(has.by_nox_mass,0)), 
            sum(COALESCE(has.fy_nox_mass,0)), 
            
            sum(COALESCE(has.by_nox_mass*(has.calendar_hour > ? and has.calendar_hour <= ?),0)), 
            sum(COALESCE(has.fy_nox_mass*(has.calendar_hour > ? and has.calendar_hour <= ?),0)), 
            sum(COALESCE(has.by_nox_mass*(has.calendar_hour <= ? or has.calendar_hour > ?),0)), 
            sum(COALESCE(has.fy_nox_mass*(has.calendar_hour <= ? or has.calendar_hour > ?),0)), 
            
            2000*sum(COALESCE(has.by_so2_mass,0))/sum(COALESCE(has.by_heat_input,0)), 
            2000*sum(COALESCE(has.fy_so2_mass,0))/sum(COALESCE(has.fy_heat_input,0)), 
            2000*sum(COALESCE(has.by_nox_mass,0))/sum(COALESCE(has.by_heat_input,0)), 
            2000*sum(COALESCE(has.fy_nox_mass,0))/sum(COALESCE(has.fy_heat_input,0)),
            
            2000*sum(COALESCE(has.by_nox_mass*(has.calendar_hour > ? and has.calendar_hour <= ?),0))/ sum(COALESCE(has.by_heat_input*(has.calendar_hour > ? and has.calendar_hour <= ?),0)), 
            2000*sum(COALESCE(has.fy_nox_mass*(has.calendar_hour > ? and has.calendar_hour <= ?),0))/ sum(COALESCE(has.fy_heat_input*(has.calendar_hour > ? and has.calendar_hour <= ?),0)), 
            2000*sum(COALESCE(has.by_nox_mass*(has.calendar_hour <= ? or has.calendar_hour > ?),0))/ sum(COALESCE(has.by_heat_input*(has.calendar_hour <= ? or has.calendar_hour > ?),0)), 
            2000*sum(COALESCE(has.fy_nox_mass*(has.calendar_hour <= ? or has.calendar_hour > ?),0))/ sum(COALESCE(has.fy_heat_input*(has.calendar_hour <= ? or has.calendar_hour > ?),0)), 
            
            SUM((CASE WHEN duas.by_heat_input > 0 THEN 1 ELSE 0 END) *ozone_season)/24, 
            SUM((CASE WHEN duas.fy_heat_input > 0 THEN 1 ELSE 0 END) *ozone_season)/24,  
       
            sum(COALESCE(has.by_nox_mass*(has.calendar_hour > ? and has.calendar_hour <= ?),0))*24/ SUM((CASE WHEN duas.by_heat_input > 0 THEN 1 ELSE 0 END) *ozone_season), 
            sum(COALESCE(has.fy_nox_mass*(has.calendar_hour > ? and has.calendar_hour <= ?),0))*24/ SUM((CASE WHEN duas.fy_heat_input > 0 THEN 1 ELSE 0 END) *ozone_season),  
    
            max(COALESCE(has.fy_so2_mass,0)),
            max(COALESCE(has.fy_nox_mass,0)),
            
            sum(COALESCE(has.by_co2_mass,0)), 
            sum(COALESCE(has.fy_co2_mass,0)),
            
            has.data_type, 
            substr('YN', (coalesce(guc.unitid,-1) == -1)+1, 1), 
            substr('NY', (has.data_type == 'NEW')+1, 1), 
            has.facility_name,
            cuuaf.program_codes
        FROM hourly_activity_summary has
      
        JOIN calendar_hours ch
        ON has.calendar_hour = ch.calendar_hour
            
        LEFT JOIN daily_unit_activity_summary duas
        ON duas.orispl_code = has.orispl_code
        AND duas.unitid = has.unitid
        AND duas.ertac_region = has.ertac_region
        AND duas.ertac_fuel_unit_type_bin = has.ertac_fuel_unit_type_bin
        AND duas.calendar_day = ch.op_date
      
        LEFT JOIN calc_unit_hierarchy cuh
        ON has.orispl_code = cuh.orispl_code
        AND has.unitid = cuh.unitid
        AND has.ertac_region = cuh.ertac_region
        AND has.ertac_fuel_unit_type_bin = cuh.ertac_fuel_unit_type_bin
        
        LEFT JOIN generic_units_created guc
        ON has.orispl_code = guc.orispl_code
        AND has.unitid = guc.unitid
        AND has.ertac_region = guc.ertac_region
        AND has.ertac_fuel_unit_type_bin = guc.ertac_fuel_unit_type_bin
        
        JOIN calc_updated_uaf cuuaf
        ON cuuaf.orispl_code = has.orispl_code
        AND cuuaf.unitid = has.unitid
        AND cuuaf.ertac_region = has.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = has.ertac_fuel_unit_type_bin
        
        GROUP BY has.ertac_region, has.ertac_fuel_unit_type_bin, has.by_ertac_fuel_unit_type_bin, has.state, has.orispl_code, has.unitid, has.data_type, has.facility_name""",
                 [ertac_lib.hours_in_year(inputvars['base_year'], inputvars['future_year']),
                  ertac_lib.hours_in_year(inputvars['base_year'], inputvars['future_year'])] + [
                     inputvars['ozone_start_hour'], inputvars['ozone_end_hour']] * 18)


    if 'include-rg-hr' in inputvars:
        conn.execute("""INSERT INTO hourly_regional_activity_summary(ertac_region, ertac_fuel_unit_type_bin, data_type, calendar_hour, hierarchy_hour, by_gload, fy_gload, fy_op_max_count, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, by_co2_mass, fy_co2_mass, hour_specific_growth_rate, afygr)
        SELECT ertac_region, fuel_bin, data_type, calendar_hour, hierarchy_hour, sum(COALESCE(by_gload,0)), sum(COALESCE(fy_gload,0)), sum(op_max), sum(COALESCE(by_heat_input,0)), sum(COALESCE(fy_heat_input,0)), sum(COALESCE(by_so2_mass,0)), sum(COALESCE(fy_so2_mass,0)), sum(COALESCE(by_nox_mass,0)), sum(COALESCE(fy_nox_mass,0)), sum(COALESCE(by_co2_mass,0)), sum(COALESCE(fy_co2_mass,0)), MAX(hour_specific_growth_rate), MAX(afygr)    
    
        FROM
        (SELECT ertac_region, by_ertac_fuel_unit_type_bin as fuel_bin, data_type, calendar_hour, by_hierarchy_hour as hierarchy_hour, by_gload, 0 as fy_gload, 0 as op_max, by_heat_input, 0 as fy_heat_input, by_so2_mass, 0 as fy_so2_mass, by_nox_mass, 0 as fy_nox_mass, by_co2_mass, 0 as fy_co2_mass, hour_specific_growth_rate as hour_specific_growth_rate, afygr as afygr
        FROM hourly_activity_summary 
        WHERE by_hierarchy_hour IS NOT NULL
        UNION ALL
        SELECT has.ertac_region, has.ertac_fuel_unit_type_bin as fuel_bin, data_type, calendar_hour, hierarchy_hour, 0 as by_gload, fy_gload, (hourly_hi_limit='Y') as op_max, 0 as by_heat_input, fy_heat_input, 0 as by_so2_mass, fy_so2_mass, 0 as by_nox_mass, fy_nox_mass, 0 as by_co2_mass, fy_co2_mass, hour_specific_growth_rate, afygr
        FROM hourly_activity_summary has

        JOIN calc_updated_uaf cuuaf
        ON cuuaf.orispl_code = has.orispl_code
        AND cuuaf.unitid = has.unitid
        AND cuuaf.ertac_region = has.ertac_region
        AND cuuaf.ertac_fuel_unit_type_bin = has.ertac_fuel_unit_type_bin
        
        WHERE hierarchy_hour IS NOT NULL) 
        GROUP BY ertac_region,  fuel_bin, data_type, calendar_hour, hierarchy_hour""")

    if 'include-st-hr' in inputvars:
        conn.execute("""INSERT INTO hourly_state_activity_summary(state, ertac_fuel_unit_type_bin, calendar_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, by_co2_mass, fy_co2_mass)
        SELECT state, fuel_bin, calendar_hour, sum(COALESCE(by_gload,0)), sum(COALESCE(fy_gload,0)), sum(COALESCE(by_heat_input,0)), sum(COALESCE(fy_heat_input,0)), sum(COALESCE(by_so2_mass,0)), sum(COALESCE(fy_so2_mass,0)), sum(COALESCE(by_nox_mass,0)), sum(COALESCE(fy_nox_mass,0)), sum(COALESCE(by_co2_mass,0)), sum(COALESCE(fy_co2_mass,0))    
        FROM
        (SELECT state, by_ertac_fuel_unit_type_bin as fuel_bin, calendar_hour, by_hierarchy_hour as hierarchy_hour, by_gload, 0 as fy_gload, by_heat_input, 0 as fy_heat_input, by_so2_mass, 0 as fy_so2_mass, by_nox_mass, 0 as fy_nox_mass, by_co2_mass, 0 as fy_co2_mass
        FROM hourly_activity_summary
        WHERE by_hierarchy_hour IS NOT NULL
        UNION ALL
        SELECT state, ertac_fuel_unit_type_bin as fuel_bin, calendar_hour, hierarchy_hour, 0 as by_gload, fy_gload, 0 as by_heat_input, fy_heat_input, 0 as by_so2_mass, fy_so2_mass, 0 as by_nox_mass, fy_nox_mass, by_co2_mass, 0 as fy_co2_mass
        FROM hourly_activity_summary
            WHERE hierarchy_hour IS NOT NULL) 
        GROUP BY state,  fuel_bin, calendar_hour""")

    # remove the calendar hierarchy tables
    conn.execute("""DROP TABLE calendar_hierarchy_hours""")

    # Save changes
    conn.commit()


def run_intergrity_check(conn, logfile):
    (by_gload, fy_gload) = conn.execute("""SELECT SUM(by_gload), SUM(fy_gload) FROM annual_summary""").fetchone()
    print("Annual Summary BY GLOAD: " + str(by_gload), file=logfile)
    print("Annual Summary FY GLOAD: " + str(fy_gload), file=logfile)
    (by_gload, fy_gload) = conn.execute(
        """SELECT SUM(by_gload), SUM(fy_gload) FROM hourly_activity_summary""").fetchone()
    print("Hourly Summary BY GLOAD: " + str(by_gload), file=logfile)
    print("Hourly Summary FY GLOAD: " + str(fy_gload), file=logfile)
    (by_gload, fy_gload) = conn.execute(
        """SELECT SUM(by_gload), SUM(fy_gload) FROM hourly_regional_activity_summary""").fetchone()
    print("Hourly Regional Summary BY GLOAD: " + str(by_gload), file=logfile)
    print("Hourly Regional Summary FY GLOAD: " + str(fy_gload), file=logfile)
    (by_gload, fy_gload) = conn.execute(
        """SELECT SUM(by_gload), SUM(fy_gload) FROM hourly_state_activity_summary""").fetchone()
    print("Hourly State Summary BY GLOAD: " + str(by_gload), file=logfile)
    print("Hourly State Summary FY GLOAD: " + str(fy_gload), file=logfile)

    for (region, fuel_bin) in conn.execute(
            """SELECT ertac_region, ertac_fuel_unit_type_bin FROM annual_summary GROUP BY ertac_region, ertac_fuel_unit_type_bin""").fetchall():
        (gload,) = conn.execute(
            """SELECT SUM(by_gload) FROM annual_summary WHERE ertac_region = ? AND by_ertac_fuel_unit_type_bin = ? """,
            (region, fuel_bin)).fetchone()
        print(region + "/" + fuel_bin + " Annual Summary BY GLOAD: " + str(gload), file=logfile)
        (gload,) = conn.execute(
            """SELECT SUM(by_gload) FROM hourly_activity_summary WHERE ertac_region = ? AND by_ertac_fuel_unit_type_bin = ? """,
            (region, fuel_bin)).fetchone()
        print(region + "/" + fuel_bin + " Hourly Summary BY GLOAD: " + str(gload), file=logfile)
        (gload,) = conn.execute(
            """SELECT SUM(by_gload) FROM hourly_regional_activity_summary WHERE ertac_region = ? AND ertac_fuel_unit_type_bin = ? """,
            (region, fuel_bin)).fetchone()
        print(region + "/" + fuel_bin + " Hourly Regional Summary BY GLOAD: " + str(gload), file=logfile)

        (gload,) = conn.execute(
            """SELECT SUM(fy_gload) FROM annual_summary WHERE ertac_region = ? AND ertac_fuel_unit_type_bin = ? """,
            (region, fuel_bin)).fetchone()
        print(region + "/" + fuel_bin + " Annual Summary FY GLOAD: " + str(gload), file=logfile)
        (gload,) = conn.execute(
            """SELECT SUM(fy_gload) FROM hourly_activity_summary WHERE ertac_region = ? AND ertac_fuel_unit_type_bin = ? """,
            (region, fuel_bin)).fetchone()
        print(region + "/" + fuel_bin + " Hourly Summary FY GLOAD: " + str(gload), file=logfile)
        (gload,) = conn.execute(
            """SELECT SUM(fy_gload) FROM hourly_regional_activity_summary WHERE ertac_region = ? AND ertac_fuel_unit_type_bin = ? """,
            (region, fuel_bin)).fetchone()
        print(region + "/" + fuel_bin + " Hourly Regional Summary FY GLOAD: " + str(gload), file=logfile)


def build_prefix(prefix, inputvars):
    """Builds a prefix for output files.

    Keyword arguments:
    prefix    -- a prefix that had been supplied at a higher leve
    inputvars -- a dictionary of input options used in the nameing convention

    """
    # we need to add something to deal with *'s or whatever other junk that could show up in the file name - jakuta
    if 'region' in inputvars:
        prefix += inputvars['region'] + "_"
    if 'fuel_bin' in inputvars:
        prefix += inputvars['fuel_bin'] + "_"
    if 'state' in inputvars:
        prefix += inputvars['state'] + "_"
    if 'facility_name' in inputvars:
        prefix += inputvars['facility_name'] + "_"
        if 'unitid' in inputvars:
            prefix += inputvars['unitid'] + "_"
    elif 'orisid' in inputvars:
        prefix += inputvars['orisid'] + "_"
        if 'unitid' in inputvars:
            prefix += inputvars['unitid'] + "_"
    if 'time_span' in inputvars:
        prefix += inputvars['time_span'] + "_"
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
            query += " AND " + table + "state in (" + ('?,' * len(states))[:-1] + ") "
            inputs = inputs + list(states)
        if 'region' in inputvars:
            regions = inputvars['region'].split(",")
            query += " AND " + table + "ertac_region in (" + ('?,' * len(regions))[:-1] + ") "
            inputs = inputs + list(regions)
        if 'fuel_bin' in inputvars:
            query += " AND " + table + "ertac_fuel_unit_type_bin = ? "
            inputs.append(inputvars['fuel_bin'])
        if 'orisid' in inputvars:
            query += " AND " + table + "orispl_code = ? "
            inputs.append(inputvars['orisid'])
            if 'unitid' in inputvars:
                query += " AND " + table + "unitid = ? "
                inputs.append(inputvars['unitid'])
    if 'time_span' in inputvars and include_time:
        query += " AND " + table + "calendar_hour >= ? AND " + table + "calendar_hour <= ? "
        if inputvars['time_span'] == 'OzoneSeason':
            inputs.append(inputvars['ozone_start_hour'])
            inputs.append(inputvars['ozone_end_hour'])
        else:
            (base_year,) = conn.execute("""SELECT DISTINCT base_year FROM calc_input_variables""").fetchone()
            if inputvars['time_span'] == 'FirstQtr' or inputvars['time_span'] == 'FirstQuarter':
                start_date = base_year + "-01-01"
                end_date = base_year + "-03-31"
            elif inputvars['time_span'] == 'SecondQtr' or inputvars['time_span'] == 'SecondQuarter':
                start_date = base_year + "-04-01"
                end_date = base_year + "-06-30"
            elif inputvars['time_span'] == 'ThirdQtr' or inputvars['time_span'] == 'ThirdQuarter':
                start_date = base_year + "-07-01"
                end_date = base_year + "-09-30"
            elif inputvars['time_span'] == 'FourthQtr' or inputvars['time_span'] == 'FourthQuarter':
                start_date = base_year + "-10-01"
                end_date = base_year + "-12-31"
            (start_hour,) = conn.execute("""SELECT MIN(calendar_hour)FROM calendar_hours WHERE op_date >= ?""",
                                         (start_date,)).fetchone()
            (end_hour,) = conn.execute("""SELECT MAX(calendar_hour) FROM calendar_hours WHERE op_date <= ?""",
                                       (end_date,)).fetchone()
            inputs.append(start_hour)
            inputs.append(end_hour)

    return (query, inputs)


def print_to_index(inputvars, text):
    """Opens the file pointer for the index and writes to it.

    inputvars -- a dictionary of input options
    text      -- the text you would like to print
    """
    if 'index' in inputvars:
        index = open(inputvars['index'], 'a')
        print(text, file=index)
        index.close()


def write_final_data(conn, inputvars, out_prefix, logfile):
    """Write out projected ERTAC EGU data reports.

    Keyword arguments:
    conn       -- a valid database connection where the data is stored
    inputvars      -- a dictionary of input options used in the nameing convention
    out_prefix -- optional prefix added to each output file name
    logfile    -- file where logging messages will be written

    """
    # Final output data is exported as CSV files for reporting and use with
    # other programs.
    ertac_lib.export_table_to_csv('hourly_activity_summary', out_prefix + 'post_results/',
                                  'hourly_activity_summary.csv', conn, hourly_activity_summary_columns, logfile)
    if 'include-rg-hr' in inputvars:
        ertac_lib.export_table_to_csv('hourly_regional_activity_summary', out_prefix + 'post_results/',
                                      'hourly_regional_activity_summary.csv', conn, hourly_regional_summary_columns,
                                      logfile)
    if 'include-st-hr' in inputvars:
        ertac_lib.export_table_to_csv('hourly_state_activity_summary', out_prefix + 'post_results/',
                                      'hourly_state_activity_summary.csv', conn, hourly_state_summary_columns, logfile)
    if 'include-unit-day' in inputvars:
        ertac_lib.export_table_to_csv('daily_unit_activity_summary', out_prefix + 'post_results/',
                                      'daily_unit_activity_summary.csv', conn, daily_unit_activity_summary_columns,
                                      logfile)
    ertac_lib.export_table_to_csv('annual_summary', out_prefix + 'post_results/', 'annual_unit_summary.csv', conn,
                                  annual_summary_columns, logfile)
    # add state/regional dump


def create_postprocessing_tables(conn):
    ertac_lib.run_script_file('create_preprocessor_output_tables.sql', conn)
    ertac_lib.run_script_file('create_projection_output_tables.sql', conn)
    ertac_lib.run_script_file('create_postprocessing_tables.sql', conn)

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
                                   ["help", "debug", "quiet", "verbose", "run-integrity",
                                    "input-prefix-pre=", "input-prefix-proj=", "output-prefix=", "state=", "region=",
                                    "fuel-bin=", "orisid=", "time-span=", "include-st-hr", "include-rg-hr", "remove-feb29",
                                    "include-unit-day", "sql-database=", "config-file="])
    except getopt.GetoptError as err:
        print()
        print(str(err))
        usage(argv[0])
        return 2

    # Initializing option variables
    debug_level = "INFO"
    input_prefix_pre = None
    input_prefix_proj = None
    output_prefix = ''
    inputvars = {}
    sql_database = ''
    config_file = None
    run_integrity = None
    argument_list = ''

    for opt, arg in opts:
        argument_list += opt
        if arg:
            argument_list += arg
        argument_list += ", "

        if opt in ("-h", "--help"):
            usage(argv[0])
            return 0
        elif opt in ("-d", "--debug"):
            debug_level = "DEBUG"
        elif opt in ("-q", "--quiet"):
            debug_level = "NONE"
        elif opt in ("-v", "--verbose"):
            debug_level = "INFO"
        elif opt in ("--run-integrity"):
            run_integrity = True
        elif opt in ("--input-prefix-pre"):
            input_prefix_pre = arg
        elif opt in ("--input-prefix-proj"):
            input_prefix_proj = arg
        elif opt in ("-o", "--output-prefix"):
            output_prefix = arg
        elif opt in ("--state"):
            state_clean = True
            for state in arg.split(","):
                if state.upper() not in state_set:
                    state_clean = False
                    print("State Not Valid: Defaulting To No Selection")
            if state_clean:
                inputvars['state'] = arg
        elif opt in ("--region"):
            inputvars['region'] = arg
        elif opt in ("--fuel-bin"):
            if arg.upper() not in fuel_set:
                print("Fuel-Bin Not Valid: Defaulting To No Selection")
            else:
                inputvars['fuel_bin'] = arg
        elif opt in ("--orisid"):
            inputvars['orisid'] = arg
        elif opt in ("--time-span"):
            if arg not in ['Annual', 'FirstQtr', 'FirstQuarter' 'SecondQtr', 'SecondQuarter', 'ThirdQtr',
                           'ThirdQuarter', 'FourthQtr', 'FourthQuarter', 'OzoneSeason']:
                print("Timespan Not Valid: Defaulting To Annual")
            else:
                if arg != 'Annual':
                    inputvars['time_span'] = arg
        elif opt in ("--sql-database"):
            sql_database = arg
        elif opt in ("--config-file"):
            config_file = arg
        elif opt in ("--include-st-hr"):
            inputvars['include-st-hr'] = True
        elif opt in ("--include-rg-hr"):
            inputvars['include-rg-hr'] = True
        elif opt in ("--include-unit-day"):
            inputvars['include-unit-day'] = True
        elif opt in ("--remove-feb29"):
            inputvars['remove-feb29'] = True
        else:
            assert False, "unhandled option"

    if debug_level == "DEBUG":
        # Detailed logging to file for postmortem analysis.
        logging.basicConfig(
            filename='ertac_postprocessing_debug_log.txt',
            filemode='w',
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
        logfilename = output_prefix + 'ertac_egu_postprocessing_log.txt'
    else:
        logfilename = 'ertac_egu_postprocessing_log.txt'

    try:
        logfile = open(logfilename, 'w')
    except IOError:
        print("Log file: " + logfilename + " -- Could not be written.  Program will terminate.", file=sys.stderr)
        raise

    # Identify versions of Python and SQLite library, and record in log file.
    logging.info("Program started at " + time.asctime())
    logging.info("ERTAC Postprocessor version: " + VERSION)
    logging.info("Running under python version: " + sys.version)
    #logging.info("Using sqlite3 module version: " + sqlite3.version) #JMJ being depricated in Python 3.14
    logging.info("Linked against sqlite3 database library version: " + sqlite3.sqlite_version)

    print("Program started at " + time.asctime(), file=logfile)
    print("ERTAC Post Processor version: " + VERSION, file=logfile)
    print("Running under python version: " + sys.version, file=logfile)
    #print("Using sqlite3 module version: " + sqlite3.version, file=logfile) #JMJ being depricated in Python 3.14
    print("Linked against sqlite3 database library version: " + sqlite3.sqlite_version, file=logfile)
    print("Run with arguments" + argument_list, file=logfile)
    print("Model code versions:", file=logfile)
    for file_name in [os.path.basename(sys.argv[0]), 'ertac_lib.py', 'ertac_tables.py', 'ertac_reports.py',
                      'create_preprocessor_output_tables.sql', 'create_projection_output_tables.sql',
                      'create_postprocessing_tables.sql']:
        print("  " + file_name + ": " + time.ctime(os.path.getmtime(os.path.join(sys.path[0], file_name))),
              file=logfile)

    # Workspace SQL DB (1) in memory or (2) as a file
    if sql_database == '':
        logging.info("Creating database tables in memory.")
        print("Creating database tables in memory.", file=logfile)
        # The preprocessor output tables are used as the projection inputs.
        # The projection output tables produce all the reports.
        dbconn = sqlite3.connect(sql_database)
        create_postprocessing_tables(dbconn)
        # Load intermediate CSV data into tables, rejecting any rows that can't be
        # used.  There should be no invalid data at this stage, unless the
        # intermediate files were manually changed with erroneous data.
        logging.info("Loading intermediate data:")
        print("Loading intermediate data:", file=logfile)
        load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj, inputvars, logfile)
        logging.info("Finished loading intermediate data.")
        print("Finished loading intermediate data.", file=logfile)
        existing_db_file = False
    else:
        # Check if user specified DB file exists.
        # If not, create and populate the workspace database.
        if os.path.isfile(sql_database):
            logging.info("Found the existing DB:" + sql_database)
            print("Found the existing DB:" + sql_database, file=logfile)
            dbconn = sqlite3.connect(sql_database)
            dbconn.text_factory = str
            existing_db_file = True
        else:
            logging.info("Not Found the existing DB, creating a new DB file:" + sql_database)
            print("Not Found the existing DB, creating a new DB file:" + sql_database, file=logfile)
            dbconn = sqlite3.connect(sql_database)
            create_postprocessing_tables(dbconn)
            # Load intermediate CSV data into tables, rejecting any rows that can't be
            # used.  There should be no invalid data at this stage, unless the
            # intermediate files were manually changed with erroneous data.
            logging.info("Loading intermediate data:")
            print("Loading intermediate data:", file=logfile)
            load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj, inputvars, logfile)
            logging.info("Finished loading intermediate data.")
            print("Finished loading intermediate data.", file=logfile)
            existing_db_file = False

    # Loading configuration data
    if config_file:
        dbconn.executescript(
            """CREATE TEMPORARY TABLE config_variables (variable TEXT NOT NULL COLLATE NOCASE, value TEXT NOT NULL);""")
        ertac_lib.load_csv_into_table(None, config_file, 'config_variables', dbconn,
                                      (('variable', 'str', True, None), ('value', 'str', True, None)), sys.stderr)
        for (variable, value) in dbconn.execute(
                """SELECT variable, value FROM config_variables where variable in ('region', 'fuel-bin', 'orisid', 'unitid', 'time-span', 'by-hours')""").fetchall():
            inputvars[re.sub(r'-', '_', variable)] = value
            print("Updating input " + variable)
        for (variable, value) in dbconn.execute(
                """SELECT variable, value FROM config_variables where variable in ('show-graphs', 'large-graphs', 'use-lines')""").fetchall():
            inputvars[re.sub(r'-', '_', variable)] = True
            print("Updating input " + variable)
        dbconn.executescript("""DROP TABLE config_variables;""")

    output_prefix = build_prefix(output_prefix, inputvars)

    # 20120406 Determine ozone season start/end dates and calendar hours.
    (inputvars['base_year'], inputvars['future_year'], ozone_start, ozone_end) = dbconn.execute("""SELECT DISTINCT
    base_year, future_year, ozone_start_date, ozone_end_date
    FROM calc_input_variables""").fetchone()
    ozone_start_base = ertac_lib.convert_ozone_date(ozone_start, inputvars['base_year'])
    ozone_end_base = ertac_lib.convert_ozone_date(ozone_end, inputvars['base_year'])
    ozone_start_future = ertac_lib.convert_ozone_date(ozone_start, inputvars['future_year'])
    ozone_end_future = ertac_lib.convert_ozone_date(ozone_end, inputvars['future_year'])

    # Need to convert operating date/hour into calendar hour for outputs.
    ertac_lib.make_calendar_hours(inputvars['base_year'], inputvars['future_year'], dbconn)

    (inputvars['ozone_start_hour'],) = dbconn.execute("""SELECT MIN(calendar_hour)
    FROM calendar_hours
    WHERE op_date >= ?""", (ozone_start_base,)).fetchone()

    (inputvars['ozone_end_hour'],) = dbconn.execute("""SELECT MAX(calendar_hour)
    FROM calendar_hours
    WHERE op_date <= ?""", (ozone_end_base,)).fetchone()

    # this stretch makes the directory structure necessary for saving results
    if not os.path.exists(output_prefix + 'post_results'):
        os.makedirs(output_prefix + 'post_results')

    if existing_db_file:
        logging.info("Using the existing summary data for plots and CSV files...")
        print("Using the existing summary data for plots and CSV files...", file=logfile)
    else:
        logging.info("Summarizing data...")
        print(file=logfile)
        print("Summarizing data...", file=logfile)
        summarize_hourly_results(dbconn, inputvars, logfile)
        if run_integrity:
            logging.info("Running integrity check...")
            print("Running integrity check...", file=logfile)
            run_intergrity_check(dbconn, logfile)

    # Export projection report tables as CSV files.
    logging.info("Writing out reports:")
    write_final_data(dbconn, inputvars, output_prefix, logfile)
    logging.info("Finished writing reports.")
    dbconn.close()
    logging.info("Program ended at " + time.asctime())
    print(file=logfile)
    print("Program ended at " + time.asctime(), file=logfile)

    # End of main routine


if __name__ == '__main__':
    sys.exit(main())
