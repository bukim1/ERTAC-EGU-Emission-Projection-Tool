#!/usr/bin/python

# ertac_postprocess.py

"""ERTAC EGU post processing"""

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

import sys
try:
    import getopt, logging, os, time, re
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
    import numpy                  as np
    import matplotlib             as mpl
    import matplotlib.pyplot      as plt
    import matplotlib.collections as collections
    import matplotlib.cm as cm
    import pylab
    from   itertools              import chain
except ImportError:
    print >> sys.stderr, "Fatal error: can't import all required modules."
    print >> sys.stderr, "NumPy and Matplotlib must be installed."
    raise

try:
    import ertac_lib
    from   ertac_tables import * # importaing all dictionaries, tables, etc
    from   ertac_reports import *
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

# Post-processor specific table structure definition
hourly_activity_summary_columns = (('ertac region', 'str', True, None),
                       ('ertac fuel unit type bin', 'str', True, fuel_set),
                       ('FY ertac fuel unit type bin', 'str', True, fuel_set),
                       ('oris', 'str', True, None),
                       ('unit id', 'str', True, None),
                       ('old_unit id', 'str', False, None),
                       ('state', 'str', True, state_set),
                       ('calendar hour', 'int', True, (0, 8760)),
                       ('hierarchy hour', 'int', True, (0, 8760)),
                       ('BY hierarchy hour', 'int', True, (0, 8760)),
                       ('BY gload (MW)', 'float', False, (0.0, 2300.0)),
                       ('FY gload (MW)', 'float', False, (0.0, 2300.0)),
                       ('BY heat_input (mmBtu)', 'float', False, (0.0, 29000.0)),
                       ('FY heat_input (mmBtu)', 'float', False, (0.0, 29000.0)),
                       ('BY so2_mass (Tons)', 'float', False, (0.0, 100000.0)),
                       ('FY so2_mass (Tons)', 'float', False, (0.0, 100000.0)),
                       ('BY nox_mass (Tons)', 'float', False, (0.0, 20000.0)),
                       ('FY nox_mass (Tons)', 'float', False, (0.0, 20000.0)),
                       ('hour_specific_growth_rate', 'float', False, None),
                       ('afygr', 'float', False, None),
                       ('BY hour_specific_growth_rate', 'float', False, None),
                       ('BY afygr', 'float', False, None),
                       ('data_type', 'str', False, None),
                       ('facility_name', 'str', False, None))

hourly_regional_summary_columns = (('ertac region', 'str', True, None),
                       ('ertac fuel unit type bin', 'str', True, fuel_set),
                       ('calendar hour', 'int', True, (0, 8760)),
                       ('hierarchy hour', 'int', True, (0, 8760)),
                       ('BY gload (MW)', 'float', False, (0.0, 2300.0)),
                       ('FY gload (MW)', 'float', False, (0.0, 2300.0)),
                       ('BY heat_input (mmBtu)', 'float', False, (0.0, 29000.0)),
                       ('FY heat_input (mmBtu)', 'float', False, (0.0, 29000.0)),
                       ('BY so2_mass (Tons)', 'float', False, (0.0, 100000.0)),
                       ('FY so2_mass (Tons)', 'float', False, (0.0, 100000.0)),
                       ('BY nox_mass (Tons)', 'float', False, (0.0, 20000.0)),
                       ('FY nox_mass (Tons)', 'float', False, (0.0, 20000.0)),
                       ('hour_specific_growth_rate', 'float', False, None),
                       ('afygr', 'float', False, None))

hourly_state_summary_columns = (('state', 'str', True, state_set),
                       ('ertac fuel unit type bin', 'str', True, fuel_set),
                       ('calendar hour', 'int', True, (0, 8760)),
                       ('hierarchy hour', 'int', True, (0, 8760)),
                       ('BY gload (MW)', 'float', False, (0.0, 2300.0)),
                       ('FY gload (MW)', 'float', False, (0.0, 2300.0)),
                       ('BY heat_input (mmBtu)', 'float', False, (0.0, 29000.0)),
                       ('FY heat_input (mmBtu)', 'float', False, (0.0, 29000.0)),
                       ('BY so2_mass (Tons)', 'float', False, (0.0, 100000.0)),
                       ('FY so2_mass (Tons)', 'float', False, (0.0, 100000.0)),
                       ('BY nox_mass (Tons)', 'float', False, (0.0, 20000.0)),
                       ('FY nox_mass (Tons)', 'float', False, (0.0, 20000.0)))

annual_summary_columns = (('oris', 'str', True, None),
                       ('unit id', 'str', True, None),
                       ('old_unit id', 'str', False, None),
                       ('facility_name', 'str', False, None),
                       ('state', 'str', True, state_set),
                       ('ertac region', 'str', True, None),
                       ('ertac fuel unit type bin', 'str', True, fuel_set),
                       ('BY ertac fuel unit type bin', 'str', True, fuel_set),
                       ('max_unit_heat_input (mmBtu)', 'float', False, None),
                       ('ertac_heat_rate (btu/kw-hr)', 'float', False, (3000.0, 20000.0)),
                       ('nameplate_capacity (MW)', 'float', False, None),
                       ('FY op hour Max', 'int', True, (0, 8760)),
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
                       ('BY NonOS NOx (tons)', 'float', False, None),
                       ('BY Average NonOS NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('FY Annual SO2 (tons)', 'float', False, None),
                       ('FY Average Annual SO2 Rate (lbs/mmbtu)', 'float', False, None),
                       ('FY Annual NOx (tons)', 'float', False, None),
                       ('FY Average Annual NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('FY OS NOx (tons)', 'float', False, None),
                       ('FY Average OS NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('FY OS heat input (mmbtu)', 'float', False, None),
                       ('FY NonOS NOx (tons)', 'float', False, None),
                       ('FY Average NonOS NOx Rate (lbs/mmbtu)', 'float', False, None),
                       ('Hierarchy Order', 'int', False, None),
                       ('Longitude', 'float', False, None),
                       ('Latitude', 'float', False, None),
                       ('Generation Deficit Unit?', 'str', False, ['Y','N']),
                       ('Retirement Date', 'str', False, None),
                       ('New Unit?', 'str', False, ['Y','N']),
                       ('data_type', 'str', False, None))


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
# Setting up default plot parameters
mpl.rcParams['font.size']       = 12
mpl.rcParams['xtick.labelsize'] = 8
mpl.rcParams['ytick.labelsize'] = 8

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
  -o prefix, --output-prefix=prefix.    output prefix for postprocessor results

  --config-file=existing csv.           use csv to override all inputs except sql-database (only accepts double dashed, without the double dashes, e.g. input-prefix-proj)
  --state=state.                        limit analysis to this state
  --region=region.                      limit analysis to this ertac region
  --fuel-bin=fuel bin.                  limit analysis to this ertac fuel bin
  --orisid=orispl_code.                 limit analysis to this orispl code
  --unitid=unitid.                      limit analysis to this unit id (requires orispl code)
  --time-span=timespan.                 limit analysis to time predefined time span (Annual, FirstQtr, SecondQtr, ThirdQtr, FourthQtr, OzoneSeason)
  --show-graphs.                        display graphs to screen rather than saving
  --by-hourss.                          display graphs for 'Calendar' hours, 'Hierachy' hours, or 'Both' (defaults to Both)
  --large-graphs.                       display large, 1 panel hourly graphs
  --use-lines.                          display hourly graphs using lines
""" % progname

def load_intermediate_data(conn, in_prefix_pre, in_prefix_proj, inputvars, logfile):
    """Load intermediate ERTAC EGU data from preprocessor for projection.

    Keyword arguments:
    conn           -- a valid database connection where the data will be stored
    in_prefix_pre  -- optional prefix added to each input file name generated from preprocessor
    in_prefix_post -- optional prefix added to each input file name generated from projection
    logfile        -- file where logging messages will be written

    """

    ertac_lib.load_csv_into_table(None, os.path.join(os.path.relpath(sys.path[0]), 'states.csv'), 'states', conn, states_columns, logfile)
    # This section will reject any input rows that are missing required fields,
    # have unreadable data, or violate key constraints, because it is impossible
    # to store that data in the database tables.
    ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_hourly_base.csv', 'calc_hourly_base', conn, calc_hourly_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_updated_uaf.csv', 'calc_updated_uaf', conn, uaf_columns, logfile)
    (where, inputs) = build_where(conn, 'cuuaf.', inputvars, True, True)
   
    query= """SELECT chb.orispl_code, chb.unitid FROM calc_hourly_base chb
    LEFT JOIN (SELECT * FROM calc_updated_uaf cuuaf WHERE 1 """ + where + """) AS cuuaf
    ON cuuaf.orispl_code = chb.orispl_code
    AND cuuaf.unitid = chb.unitid
    AND cuuaf.ertac_region = chb.ertac_region
    AND cuuaf.ertac_fuel_unit_type_bin = chb.ertac_fuel_unit_type_bin
    WHERE cuuaf.orispl_code IS NULL"""
    #for (orispl_code, unitid) in conn.execute(query, inputs).fetchall():
    #    conn.execute("""DELETE FROM calc_hourly_base WHERE orispl_code = ? AND unitid = ?""", [orispl_code, unitid])
      
    ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_input_variables.csv', 'calc_input_variables', conn, input_variable_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_unit_hierarchy.csv', 'calc_unit_hierarchy', conn, unit_hierarchy_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'generic_units_created.csv', 'generic_units_created', conn, generic_units_created, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'hourly_diagnostic_file.csv', 'hourly_diagnostic_file', conn, hourly_diagnostic_file, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_unit_hierarchy.csv', 'calc_unit_hierarchy', conn, unit_hierarchy_columns, logfile)
   
    query= """SELECT hdf.orispl_code, hdf.unitid FROM hourly_diagnostic_file hdf
    LEFT JOIN (SELECT * FROM calc_updated_uaf cuuaf WHERE 1 """ + where + """) AS cuuaf
    ON hdf.orispl_code = cuuaf.orispl_code
    AND hdf.unitid = cuuaf.unitid
    AND hdf.ertac_region = cuuaf.ertac_region
    AND hdf.ertac_fuel_unit_type_bin = cuuaf.ertac_fuel_unit_type_bin
    WHERE cuuaf.orispl_code IS NULL"""
    for (orispl_code, unitid) in conn.execute(query, inputs).fetchall():
        conn.execute("""DELETE FROM hourly_diagnostic_file WHERE orispl_code = ? AND unitid = ?""", [orispl_code, unitid])
        
    ertac_lib.load_csv_into_table(in_prefix_pre, 'calc_growth_rates.csv', 'calc_growth_rates', conn, growth_rate_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix_proj, 'calc_generation_parms.csv', 'calc_generation_parms', conn, generation_parms_columns, logfile)


def summarize_hourly_results(conn, inputvars, logfile):
    """Summarize hourly data and merge base year and future year results.

    Keyword arguments:
    conn      -- a valid database connection where the data is stored
    inputvars -- a dictionary of input options
    logfile   -- file where logging messages will be written

    """
  
    #create a temp table of hierachy hours
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
    
    #we are going to divy everything up by state/fuel bin to ease the burden of lots of calls to huge dbs
    (where, inputs) = build_where(conn, 'calc_updated_uaf.', inputvars, False, True)
    query = """SELECT state, ertac_fuel_unit_type_bin FROM calc_updated_uaf WHERE 1 """+where+""" GROUP BY state, ertac_fuel_unit_type_bin"""

    for (state, fuel_unit_type_bin) in conn.execute(query, inputs).fetchall():
        logging.info("Processing - " + state + ", " + fuel_unit_type_bin)
        
        #Full and Partial Reporters
        query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, hour_specific_growth_rate, afygr, by_hour_specific_growth_rate, by_afygr, data_type, facility_name)
        SELECT hdf.ertac_region,
               hdf.ertac_fuel_unit_type_bin,
               hdf.ertac_fuel_unit_type_bin,
               hdf.orispl_code,
               hdf.unitid,
               hdf.state,
               hdf.calendar_hour,
               hdf.hierarchy_hour,
               hdf.hierarchy_hour,
               chb.gload,
               hdf.gload,
               chb.heat_input,
               hdf.heat_input,
               chb.so2_mass/2000,
               hdf.so2_mass/2000,
               chb.nox_mass/2000,
               hdf.nox_mass/2000,
               cgp.hour_specific_growth_rate,
               cgp.afygr,
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
        conn.execute(query + where, [state, fuel_unit_type_bin, str(inputvars['base_year']) + '-12-31', str(inputvars['future_year']) + '-01-01'] + inputs)
    

        #query = """ INSERT INTO generation_parms (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour, temporal_allocation_order, hour_specific_growth_rate, base_actual_generation, base_retired_generation, future_projected_generation, future_projected_growth, total_proxy_generation, adjusted_projected_generation, afygr, excess_generation_pool)
        #SELECT ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour, temporal_allocation_order, hour_specific_growth_rate, base_actual_generation, base_retired_generation, future_projected_generation, future_projected_growth, total_proxy_generation, adjusted_projected_generation, afygr, excess_generation_pool
        #FROM calc_generation_parms cgp WHERE 1 """
        #(where, inputs) = build_where(conn, 'cgp.', inputvars, True, True)
        #conn.execute(query + where, inputs)
    
         
        #
        #Switchers
        query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, hour_specific_growth_rate, afygr, by_hour_specific_growth_rate, by_afygr, data_type, facility_name)
        SELECT hdf.ertac_region,
               hdf.ertac_fuel_unit_type_bin,
               cuuaf.ertac_fuel_unit_type_bin,
               hdf.orispl_code,
               hdf.unitid,
               hdf.state,
               hdf.calendar_hour,
               hdf.hierarchy_hour,
               chh.hierarchy_hour,
               chb.gload,
               hdf.gload,
               chb.heat_input,
               hdf.heat_input,
               chb.so2_mass/2000,
               hdf.so2_mass/2000,
               chb.nox_mass/2000,
               hdf.nox_mass/2000,
               cgp.hour_specific_growth_rate,
               cgp.afygr,
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
            AND cuuaf.offline_start_date <= ?"""
    
        (where, inputs) = build_where(conn, 'hdf.', inputvars, True, True)
        conn.execute(query + where, [state, fuel_unit_type_bin, str(inputvars['base_year']) + '-01-01', str(inputvars['future_year']) + '-01-01'] + inputs)
    
        #
        #New Units
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
        for (region, fuel_bin, orisid, unitid, data_type, facility_name) in conn.execute(query + where, [state, fuel_unit_type_bin] + inputs).fetchall():
    
            #(old_unitid, ) = conn.execute("""SELECT unitid from calc_unit_hierarchy
            #WHERE orispl_code = ?
            #AND ertac_region = ?
            #AND ertac_fuel_unit_type_bin = ? ORDER BY unit_allocation_order ASC """, (orisid, region, fuel_bin)).fetchone()
    
            #Have to ensure it isn't a new unit that switched
            (switch_count,) = conn.execute("""SELECT count(*) FROM calc_updated_uaf cuuaf
            WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL') AND
            cuuaf.orispl_code = ? AND 
            cuuaf.ertac_region = ? AND 
            cuuaf.ertac_fuel_unit_type_bin != ? AND 
            cuuaf.unitid = ?""", [orisid, region, fuel_bin, unitid]).fetchone()
            
            if switch_count == 0:
                query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, old_unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, hour_specific_growth_rate, afygr, by_hour_specific_growth_rate, by_afygr, data_type, facility_name)
                SELECT hdf.ertac_region,
                       hdf.ertac_fuel_unit_type_bin,
                       hdf.ertac_fuel_unit_type_bin,
                       hdf.orispl_code,
                       hdf.unitid,
                       0,
                       hdf.state,
                       hdf.calendar_hour,
                       hdf.hierarchy_hour,
                       NULL,
                       0,
                       hdf.gload,
                       0,
                       hdf.heat_input,
                       0/2000,
                       hdf.so2_mass/2000,
                       0/2000,
                       hdf.nox_mass/2000,
                       cgp.hour_specific_growth_rate,
                       cgp.afygr,
                       NULL,
                       NULL,
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
      
        #get the units that only operated in the base year
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
            
        for (orisid, unitid, region, fuel_bin) in conn.execute(query + where, [state, fuel_unit_type_bin, str(inputvars['base_year']) + '-01-01', str(inputvars['future_year']) + '-01-01'] + inputs).fetchall():
           #Have to ensure it isn't a new unit that switched
            (switch_count,) = conn.execute("""SELECT count(*) FROM calc_updated_uaf cuuaf
            WHERE (upper(cuuaf.camd_by_hourly_data_type) = 'FULL' OR upper(cuuaf.camd_by_hourly_data_type) = 'PARTIAL') AND
            cuuaf.orispl_code = ? AND 
            cuuaf.ertac_region = ? AND 
            cuuaf.ertac_fuel_unit_type_bin != ? AND 
            cuuaf.unitid = ?""", [orisid, region, fuel_bin, unitid]).fetchone()
      
            if switch_count == 0:          
                query = """INSERT INTO hourly_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, state, calendar_hour, hierarchy_hour, by_hierarchy_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, hour_specific_growth_rate, afygr, by_hour_specific_growth_rate, by_afygr, data_type, facility_name)
                SELECT cuuaf.ertac_region,
                       cuuaf.ertac_fuel_unit_type_bin,
                       cuuaf.ertac_fuel_unit_type_bin,
                       cuuaf.orispl_code,
                       cuuaf.unitid,
                       cuuaf.state,
                       ch.calendar_hour,
                       NULL,
                       chh.hierarchy_hour,
                       chb.gload,
                       0,
                       chb.heat_input,
                       0,
                       chb.so2_mass/2000,
                       0/2000,
                       chb.nox_mass/2000,
                       0/2000,
                       NULL,
                       NULL,
                       cgp.hour_specific_growth_rate,
                       cgp.afygr,
                       'RETIRED',
                       cuuaf.facility_name
                FROM calc_updated_uaf cuuaf
        
                JOIN calc_hourly_base chb
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

 
    #jump out of the state, fuel bin loop here   
    conn.execute("""INSERT INTO annual_summary(ertac_region, 
            ertac_fuel_unit_type_bin, 
            by_ertac_fuel_unit_type_bin, 
            orispl_code, 
            unitid, 
            state, 
            longitude,
            latitude,
            retirement_date,
            hierarchy_order,
            max_unit_heat_input, 
            ertac_heat_rate, 
            nameplate_capacity, 
            fy_op_hours_max, 
            by_uf, 
            fy_uf, 
            by_gload, 
            fy_gload, 
            by_heat_input, 
            fy_heat_input, 
            by_os_heat_input, 
            fy_os_heat_input, 
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
            data_type,
            gdu_flag,
            new_unit_flag, 
            facility_name)
        SELECT has.ertac_region, 
            has.ertac_fuel_unit_type_bin, 
            by_ertac_fuel_unit_type_bin, 
            has.orispl_code, 
            has.unitid, 
            has.state, 
            cuuaf.plant_longitude,
            cuuaf.plant_latitude,
            offline_start_date,
            unit_allocation_order,
            max_ertac_hi_hourly_summer, 
            ertac_heat_rate, 
            1000 * max_ertac_hi_hourly_summer / ertac_heat_rate, 
            sum(COALESCE(fy_gload,0)>=(1000 * max_ertac_hi_hourly_summer / ertac_heat_rate)), 
            sum(COALESCE(by_heat_input,0))/(max_ertac_hi_hourly_summer*8760), 
            sum(COALESCE(fy_heat_input,0))/(max_ertac_hi_hourly_summer*8760), 
            sum(COALESCE(by_gload,0)), 
            sum(COALESCE(fy_gload,0)), 
            sum(COALESCE(by_heat_input,0)), 
            sum(COALESCE(fy_heat_input,0)), 
            sum(COALESCE(by_heat_input*(calendar_hour > 2880 and calendar_hour <= 6552),0)), 
            sum(COALESCE(fy_heat_input*(calendar_hour > 2880 and calendar_hour <= 6552),0)), 
            sum(COALESCE(by_so2_mass,0)), 
            sum(COALESCE(fy_so2_mass,0)), 
            sum(COALESCE(by_nox_mass,0)), 
            sum(COALESCE(fy_nox_mass,0)), 
            
            sum(COALESCE(by_nox_mass*(calendar_hour > 2880 and calendar_hour <= 6552),0)), 
            sum(COALESCE(fy_nox_mass*(calendar_hour > 2880 and calendar_hour <= 6552),0)), 
            sum(COALESCE(by_nox_mass*(calendar_hour <= 2880 or calendar_hour > 6552),0)), 
            sum(COALESCE(fy_nox_mass*(calendar_hour <= 2880 or calendar_hour > 6552),0)), 
            
            2000*sum(COALESCE(by_so2_mass,0))/sum(COALESCE(by_heat_input,0)), 
            2000*sum(COALESCE(fy_so2_mass,0))/sum(COALESCE(fy_heat_input,0)), 
            2000*sum(COALESCE(by_nox_mass,0))/sum(COALESCE(by_heat_input,0)), 
            2000*sum(COALESCE(fy_nox_mass,0))/sum(COALESCE(fy_heat_input,0)),
            
            2000*sum(COALESCE(by_nox_mass*(calendar_hour > 2880 and calendar_hour <= 6552),0))/ sum(COALESCE(by_heat_input*(calendar_hour > 2880 and calendar_hour <= 6552),0)), 
            2000*sum(COALESCE(fy_nox_mass*(calendar_hour > 2880 and calendar_hour <= 6552),0))/ sum(COALESCE(fy_heat_input*(calendar_hour > 2880 and calendar_hour <= 6552),0)), 
            2000*sum(COALESCE(by_nox_mass*(calendar_hour <= 2880 or calendar_hour > 6552),0))/ sum(COALESCE(by_heat_input*(calendar_hour <= 2880 or calendar_hour > 6552),0)), 
            2000*sum(COALESCE(fy_nox_mass*(calendar_hour <= 2880 or calendar_hour > 6552),0))/ sum(COALESCE(fy_heat_input*(calendar_hour <= 2880 or calendar_hour > 6552),0)), 
    
            has.data_type, 
            substr('YN', (coalesce(guc.unitid,-1) == -1)+1, 1), 
            substr('NY', (has.data_type == 'NEW')+1, 1), 
            has.facility_name
        FROM hourly_activity_summary has
        
      
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
        
        GROUP BY has.ertac_region, has.ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, has.state, has.orispl_code, has.unitid, has.data_type, has.facility_name""")
 
           
    conn.execute("""INSERT INTO hourly_regional_activity_summary(ertac_region, ertac_fuel_unit_type_bin, calendar_hour, hierarchy_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, hour_specific_growth_rate, afygr)
        SELECT ertac_region, fuel_bin, calendar_hour, hierarchy_hour, sum(COALESCE(by_gload,0)), sum(COALESCE(fy_gload,0)), sum(COALESCE(by_heat_input,0)), sum(COALESCE(fy_heat_input,0)), sum(COALESCE(by_so2_mass,0)), sum(COALESCE(fy_so2_mass,0)), sum(COALESCE(by_nox_mass,0)), sum(COALESCE(fy_nox_mass,0)), MAX(hour_specific_growth_rate), MAX(afygr)    
        FROM
        (SELECT ertac_region, by_ertac_fuel_unit_type_bin as fuel_bin, calendar_hour, by_hierarchy_hour as hierarchy_hour, by_gload, 0 as fy_gload, by_heat_input, 0 as fy_heat_input, by_so2_mass, 0 as fy_so2_mass, by_nox_mass, 0 as fy_nox_mass, by_hour_specific_growth_rate as hour_specific_growth_rate, by_afygr as afygr
        FROM hourly_activity_summary
        WHERE by_hierarchy_hour IS NOT NULL
        UNION ALL
        SELECT ertac_region, ertac_fuel_unit_type_bin as fuel_bin, calendar_hour, hierarchy_hour, 0 as by_gload, fy_gload, 0 as by_heat_input, fy_heat_input, 0 as by_so2_mass, fy_so2_mass, 0 as by_nox_mass, fy_nox_mass, hour_specific_growth_rate, afygr
        FROM hourly_activity_summary
        WHERE hierarchy_hour IS NOT NULL) 
        GROUP BY ertac_region,  fuel_bin, calendar_hour, hierarchy_hour""")
     
    conn.execute("""INSERT INTO hourly_state_activity_summary(state, ertac_fuel_unit_type_bin, calendar_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass)
        SELECT state, fuel_bin, calendar_hour, sum(COALESCE(by_gload,0)), sum(COALESCE(fy_gload,0)), sum(COALESCE(by_heat_input,0)), sum(COALESCE(fy_heat_input,0)), sum(COALESCE(by_so2_mass,0)), sum(COALESCE(fy_so2_mass,0)), sum(COALESCE(by_nox_mass,0)), sum(COALESCE(fy_nox_mass,0))    
        FROM
        (SELECT state, by_ertac_fuel_unit_type_bin as fuel_bin, calendar_hour, by_hierarchy_hour as hierarchy_hour, by_gload, 0 as fy_gload, by_heat_input, 0 as fy_heat_input, by_so2_mass, 0 as fy_so2_mass, by_nox_mass, 0 as fy_nox_mass
        FROM hourly_activity_summary
        WHERE by_hierarchy_hour IS NOT NULL
        UNION ALL
        SELECT state, ertac_fuel_unit_type_bin as fuel_bin, calendar_hour, hierarchy_hour, 0 as by_gload, fy_gload, 0 as by_heat_input, fy_heat_input, 0 as by_so2_mass, fy_so2_mass, 0 as by_nox_mass, fy_nox_mass
        FROM hourly_activity_summary
        WHERE hierarchy_hour IS NOT NULL) 
        GROUP BY state,  fuel_bin, calendar_hour""")

    #remove the calendar hierarchy tables
    conn.execute("""DROP TABLE calendar_hierarchy_hours""")

    # Save changes
    conn.commit()

def run_intergrity_check(conn, logfile):
    (by_gload, fy_gload) = conn.execute("""SELECT SUM(by_gload), SUM(fy_gload) FROM annual_summary""").fetchone()
    print >> logfile, "Annual Summary BY GLOAD: " + str(by_gload)
    print >> logfile, "Annual Summary FY GLOAD: " + str(fy_gload)
    (by_gload, fy_gload) = conn.execute("""SELECT SUM(by_gload), SUM(fy_gload) FROM hourly_activity_summary""").fetchone()
    print >> logfile, "Hourly Summary BY GLOAD: " + str(by_gload)
    print >> logfile, "Hourly Summary FY GLOAD: " + str(fy_gload)
    (by_gload, fy_gload) = conn.execute("""SELECT SUM(by_gload), SUM(fy_gload) FROM hourly_regional_activity_summary""").fetchone()
    print >> logfile, "Hourly Regional Summary BY GLOAD: " + str(by_gload)
    print >> logfile, "Hourly Regional Summary FY GLOAD: " + str(fy_gload)
    (by_gload, fy_gload) = conn.execute("""SELECT SUM(by_gload), SUM(fy_gload) FROM hourly_state_activity_summary""").fetchone()
    print >> logfile, "Hourly State Summary BY GLOAD: " + str(by_gload)
    print >> logfile, "Hourly State Summary FY GLOAD: " + str(fy_gload)
    
    for (region, fuel_bin) in conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin FROM annual_summary GROUP BY ertac_region, ertac_fuel_unit_type_bin""").fetchall():
        (gload,) = conn.execute("""SELECT SUM(by_gload) FROM annual_summary WHERE ertac_region = ? AND by_ertac_fuel_unit_type_bin = ? """, (region, fuel_bin)).fetchone()
        print >> logfile, region + "/" + fuel_bin + " Annual Summary BY GLOAD: " + str(gload)
        (gload,) = conn.execute("""SELECT SUM(by_gload) FROM hourly_activity_summary WHERE ertac_region = ? AND by_ertac_fuel_unit_type_bin = ? """, (region, fuel_bin)).fetchone()
        print >> logfile, region + "/" + fuel_bin + " Hourly Summary BY GLOAD: " + str(gload)
        (gload,) = conn.execute("""SELECT SUM(by_gload) FROM hourly_regional_activity_summary WHERE ertac_region = ? AND ertac_fuel_unit_type_bin = ? """, (region, fuel_bin)).fetchone()
        print >> logfile, region + "/" + fuel_bin + " Hourly Regional Summary BY GLOAD: " + str(gload)

        (gload,) = conn.execute("""SELECT SUM(fy_gload) FROM annual_summary WHERE ertac_region = ? AND ertac_fuel_unit_type_bin = ? """, (region, fuel_bin)).fetchone()
        print >> logfile, region + "/" + fuel_bin + " Annual Summary FY GLOAD: " + str(gload)
        (gload,) = conn.execute("""SELECT SUM(fy_gload) FROM hourly_activity_summary WHERE ertac_region = ? AND ertac_fuel_unit_type_bin = ? """, (region, fuel_bin)).fetchone()
        print >> logfile, region + "/" + fuel_bin + " Hourly Summary FY GLOAD: " + str(gload)
        (gload,) = conn.execute("""SELECT SUM(fy_gload) FROM hourly_regional_activity_summary WHERE ertac_region = ? AND ertac_fuel_unit_type_bin = ? """, (region, fuel_bin)).fetchone()
        print >> logfile, region + "/" + fuel_bin + " Hourly Regional Summary FY GLOAD: " + str(gload)


def sql_to_numpy(results, rowcount):
    """Converts sql to numpy.

    Keyword arguments:
    results  -- an iterator of sql results
    rowcount -- number of rows in the sql results

    """
    d = np.fromiter(chain.from_iterable(results), dtype=float, count=-1)
    return d.reshape(int(rowcount), -1)

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
        if 'region' in inputvars:
            regions = inputvars['region'].split(",")
            query+= " AND "+table+"ertac_region in ("+('?,'*len(regions))[:-1]+") "
            inputs= inputs + list(regions)
        if 'fuel_bin' in inputvars:
            query+= " AND "+table+"ertac_fuel_unit_type_bin = ? "
            inputs.append(inputvars['fuel_bin'])
        if 'orisid' in inputvars:
            query+= " AND "+table+"orispl_code = ? "
            inputs.append(inputvars['orisid'])
            if 'unitid' in inputvars:
                query+= " AND "+table+"unitid = ? "
                inputs.append(inputvars['unitid'])
    if 'time_span' in inputvars and include_time:
        query+=" AND "+table+"calendar_hour >= ? AND "+table+"calendar_hour <= ? "
        if inputvars['time_span'] == 'OzoneSeason':
            inputs.append(inputvars['ozone_start_hour'])
            inputs.append(inputvars['ozone_end_hour'])
        else:
            (base_year,) = conn.execute("""SELECT DISTINCT base_year FROM calc_input_variables""").fetchone()
            if inputvars['time_span'] == 'FirstQtr' or inputvars['time_span'] == 'FirstQuarter':
                start_date = base_year+"-01-01"
                end_date = base_year+"-03-31"
            elif inputvars['time_span'] == 'SecondQtr' or inputvars['time_span'] == 'SecondQuarter':
                start_date = base_year+"-04-01"
                end_date = base_year+"-06-30"
            elif inputvars['time_span'] == 'ThirdQtr' or inputvars['time_span'] == 'ThirdQuarter':
                start_date = base_year+"-07-01"
                end_date = base_year+"-09-30"
            elif inputvars['time_span'] == 'FourthQtr' or inputvars['time_span'] == 'FourthQuarter':
                start_date = base_year+"-10-01"
                end_date = base_year+"-12-31"
            (start_hour,) = conn.execute("""SELECT MIN(calendar_hour)FROM calendar_hours WHERE op_date >= ?""", (start_date,)).fetchone()
            (end_hour,) = conn.execute("""SELECT MAX(calendar_hour) FROM calendar_hours WHERE op_date <= ?""", (end_date,)).fetchone()
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
        print >> index, text
        index.close()

def shrink_legend():
    """Shrinks the font of the legend.
    """
    leg = plt.gca().get_legend()
    if leg:
        ltext  = leg.get_texts()
        plt.setp(ltext, fontsize='8')

def plot_state(conn, output_prefix, inputvars, logfile):
    """Plots all of the regional level graphs.

    Keyword arguments:
    conn          -- a valid database connection where the data is stored
    output_prefix -- a prefix supplied by the user
    inputvars     -- a dictionary of input options
    logfile       -- file where logging messages will be written

    """
    
    #you don't get a hiearchy hour option for state summaries and it won't print out if you chose hierarchy hours
    if 'by_hours' not in inputvars or inputvars['by_hours'] == 'Calendar':
        if 'by_hours' in inputvars:
            temp_by_hours=inputvars['by_hours']
        else:
            temp_by_hours = False
        inputvars['by_hours']='Calendar'        
        print_to_index(inputvars, "<h2>State Level Summaries</h2>")
        for (inputvars['state'], inputvars['fuel_bin']) in conn.execute("""SELECT asum.state, asum.ertac_fuel_unit_type_bin FROM annual_summary asum GROUP BY asum.state, asum.ertac_fuel_unit_type_bin""").fetchall():
            print_to_index(inputvars, "<br/><b>"+inputvars['state']+"/"+inputvars['fuel_bin']+"</b><br/>")
            plot_all_variables(conn, 'hourly_state_activity_summary',inputvars['state']+"/"+inputvars['fuel_bin'], output_prefix, inputvars, logfile)      
        if temp_by_hours:
            inputvars['by_hours'] = temp_by_hours
        else:
            del inputvars['by_hours']

def plot_regional(conn, output_prefix, inputvars, logfile):
    """Plots all of the regional level graphs.

    Keyword arguments:
    conn          -- a valid database connection where the data is stored
    output_prefix -- a prefix supplied by the user
    inputvars     -- a dictionary of input options
    logfile       -- file where logging messages will be written

    """
    print_to_index(inputvars, "<h2>Regional Level Summaries</h2>")
    #just in case state got send down to this sub routine
    state = None
    if 'state' in inputvars:
        state = inputvars['state']
        del inputvars['state']
    for (inputvars['region'], inputvars['fuel_bin'], inputvars['peak_growth_factor'], inputvars['annual_growth_factor'], inputvars['non_peak_growth_factor']) in conn.execute("""SELECT asum.ertac_region, asum.ertac_fuel_unit_type_bin, (peak_growth_factor), (annual_growth_factor), (non_peak_growth_factor)  FROM annual_summary asum JOIN calc_growth_rates cgr ON asum.ertac_region = cgr. ertac_region AND asum.ertac_fuel_unit_type_bin = cgr.ertac_fuel_unit_type_bin GROUP BY asum.ertac_region, asum.ertac_fuel_unit_type_bin""").fetchall():
        print_to_index(inputvars, "<br/><b>"+inputvars['region']+"/"+inputvars['fuel_bin']+"</b><br/>")
        plot_growth_rates(conn, output_prefix, inputvars, logfile)
        plot_all_variables(conn, 'hourly_regional_activity_summary',inputvars['region']+"/"+inputvars['fuel_bin'], output_prefix, inputvars, logfile)
        plot_all_growth_rates(conn, 'hourly_regional_activity_summary',inputvars['region']+"/"+inputvars['fuel_bin'], output_prefix, inputvars, logfile)
    if state:
        inputvars['state'] = state
        
def plot_growth_rates(conn, output_prefix, inputvars, logfile):
    """Plots the growth rate graphs.

    Keyword arguments:
    conn          -- a valid database connection where the data is stored
    output_prefix -- a prefix supplied by the user
    inputvars     -- a dictionary of input options
    logfile       -- file where logging messages will be written

    """
    (rowcount,) = conn.execute("""SELECT count(distinct(orispl_code||unitid)) from annual_summary WHERE ertac_region = ? and ertac_fuel_unit_type_bin = ? and data_type in ('FULL' ,'PARTIAL', 'SWITCH')""", (inputvars['region'], inputvars['fuel_bin'])).fetchone()
    results_units = conn.execute("""SELECT state || '\n' || facility_name || '\n' || unitid FROM annual_summary WHERE ertac_region = ? and ertac_fuel_unit_type_bin = ? and data_type in ('FULL' ,'PARTIAL', 'SWITCH')  GROUP BY orispl_code, unitid  ORDER BY (fy_heat_input/by_heat_input)""", (inputvars['region'], inputvars['fuel_bin']))
    results_gr = conn.execute("""SELECT (fy_heat_input/by_heat_input) FROM annual_summary WHERE ertac_region = ? and ertac_fuel_unit_type_bin = ? and data_type in ('FULL' ,'PARTIAL', 'SWITCH')  GROUP BY orispl_code, unitid ORDER BY (fy_heat_input/by_heat_input)""", (inputvars['region'], inputvars['fuel_bin']))
    results_glr = conn.execute("""SELECT (fy_gload/by_gload) FROM annual_summary WHERE ertac_region = ? and ertac_fuel_unit_type_bin = ? and data_type in ('FULL' ,'PARTIAL', 'SWITCH')  GROUP BY orispl_code, unitid  ORDER BY (fy_heat_input/by_heat_input)""", (inputvars['region'], inputvars['fuel_bin']))
    results_nr = conn.execute("""SELECT (fy_nox_mass/by_nox_mass) FROM annual_summary WHERE ertac_region = ? and ertac_fuel_unit_type_bin = ? and data_type in ('FULL' ,'PARTIAL', 'SWITCH')  GROUP BY orispl_code, unitid  ORDER BY (fy_heat_input/by_heat_input)""", (inputvars['region'], inputvars['fuel_bin']))
    results_sr = conn.execute("""SELECT (fy_so2_mass/by_so2_mass) FROM annual_summary WHERE ertac_region = ? and ertac_fuel_unit_type_bin = ? and data_type in ('FULL' ,'PARTIAL', 'SWITCH')  GROUP BY orispl_code, unitid  ORDER BY (fy_heat_input/by_heat_input)""", (inputvars['region'], inputvars['fuel_bin']))

    r_u = np.fromiter(chain.from_iterable(results_units.fetchall()), dtype='S64', count=-1)
    r_gr = sql_to_numpy(results_gr.fetchall(), rowcount)
    r_glr = sql_to_numpy(results_glr.fetchall(), rowcount)
    r_nr = sql_to_numpy(results_nr.fetchall(), rowcount)
    r_sr = sql_to_numpy(results_sr.fetchall(), rowcount)


    entry_count = 8
    for i in range(int((rowcount-1)/entry_count)+1):
        rc = min(entry_count, rowcount-(i*entry_count))
        print rc

        fig = make_figure()
        title = inputvars['region']+"/"+inputvars['fuel_bin']
        fig.suptitle(title+": Annual Averages ("+str(i+1)+")", fontsize=16)

        ind = np.arange(rc)
        ax1 = fig.add_subplot(111)    
        ax1.set_xlim(-.5, rc-.5)
                  
        ax1.plot(ind,r_gr[i*entry_count:(i*entry_count+rc)],alpha=.5, c='r')
        ax1.scatter(ind,r_gr[i*entry_count:(i*entry_count+rc)],alpha=.5, c='r', label='FY HI/BY HI', marker='+')

        ax1.plot(ind,r_nr[i*entry_count:(i*entry_count+rc)],alpha=.5, c='m')
        ax1.scatter(ind,r_nr[i*entry_count:(i*entry_count+rc)],alpha=.5, c='m', label='FY NOX/BY NOX', marker='x')

        ax1.plot(ind,r_sr[i*entry_count:(i*entry_count+rc)],alpha=.5, c='g')
        ax1.scatter(ind,r_sr[i*entry_count:(i*entry_count+rc)],alpha=.5, c='g', label='FY SOX/BY SOX', marker='D')

        print r_u[i*entry_count:(i*entry_count+rc)];
        print r_gr[i*entry_count:(i*entry_count+rc)];
        print r_glr[i*entry_count:(i*entry_count+rc)];
        print r_nr[i*entry_count:(i*entry_count+rc)];
        print r_sr[i*entry_count:(i*entry_count+rc)];
        pylab.xticks(ind, r_u[i*entry_count:(i*entry_count+rc)], horizontalalignment="center", fontsize=10)
        ax1.plot((-1,rc),(1, 1),c='black', zorder=-2)
        ax1.plot((-1,rc),(inputvars['peak_growth_factor'], inputvars['peak_growth_factor']),c='orange', zorder=-1, label='Peak GR: ' + str(round(inputvars['peak_growth_factor'],4)))
        ax1.plot((-1,rc),(inputvars['annual_growth_factor'], inputvars['annual_growth_factor']),c='yellow', zorder=-1, label='Annual GR: ' + str(round(inputvars['annual_growth_factor'],4)))
        ax1.plot((-1,rc),(inputvars['non_peak_growth_factor'], inputvars['non_peak_growth_factor']),c='teal', zorder=-1, label='Non-Peak GR: ' + str(round(inputvars['non_peak_growth_factor'],4)))
        ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.075), fancybox=True, shadow=True, ncol=5)

        shrink_legend()
        fig.subplots_adjust(left=.05, right=.99, top=.95, bottom=.15)
        if 'show_graphs' in inputvars:
            plt.show()
        else:
            figpath = "annual_unitlevel/"+build_prefix(output_prefix,inputvars)+"annual_per"+str(i+1)+".png"
            plt.savefig(inputvars['images_folder']+"/"+figpath)
            pylab.close(fig)
            print_to_index(inputvars, "<a href=\""+figpath+"\" target=\"new\">Annual Unit Percent Changes ("+str(i+1)+")</a> | ")

def plot_all_units(conn, output_prefix, inputvars, logfile):
    """Plots all of the unit level graphs.

    Keyword arguments:
    conn          -- a valid database connection where the data is stored
    output_prefix -- a prefix supplied by the user
    inputvars     -- a dictionary of input options
    logfile       -- file where logging messages will be written

    """
    print_to_index(inputvars, "<h2>Unit Level Summaries</h2>")
    last_region = ''
    last_fuel_bin = ''
    unitcount={'TOTAL':0,'FULL':0,'PARTIAL': 0,'SWITCH':0,'NEW':0,'RETIRED':0}
    for (inputvars['region'], inputvars['fuel_bin'], inputvars['orisid'], inputvars['unitid'], inputvars['old_unitid'], inputvars['data_type'], inputvars['facility_name'], inputvars['state'], by_fuel_bin) in conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid, old_unitid, data_type, facility_name, state, by_ertac_fuel_unit_type_bin
    FROM hourly_activity_summary
    WHERE 1 > 0
    GROUP BY ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid, data_type, by_ertac_fuel_unit_type_bin""").fetchall():
        unitcount['TOTAL']+=1
        unitcount[inputvars['data_type']]+=1
        #print "DEBUG:", inputvars['region'], inputvars['fuel_bin'], inputvars['orisid'], inputvars['unitid'], inputvars['data_type']
        #these queries take for ever and we only want to do them once per unit so we are using a temp table approach
        (where, inputs) = build_where(conn, '', inputvars, True, True)
        conn.execute("""INSERT INTO hourly_unit_activity_summary(ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, old_unitid, state, calendar_hour, hierarchy_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, hour_specific_growth_rate, afygr, data_type, facility_name, state)
        SELECT ertac_region, ertac_fuel_unit_type_bin, by_ertac_fuel_unit_type_bin, orispl_code, unitid, old_unitid, state, calendar_hour, hierarchy_hour, by_gload, fy_gload, by_heat_input, fy_heat_input, by_so2_mass, fy_so2_mass, by_nox_mass, fy_nox_mass, hour_specific_growth_rate, afygr, data_type, facility_name, state
        FROM hourly_activity_summary
        WHERE 1 > 0 """+where, inputs)

        if last_region != inputvars['region'] or last_fuel_bin != inputvars['fuel_bin']:
            last_region = inputvars['region']
            last_fuel_bin = inputvars['fuel_bin']
            print_to_index(inputvars, "<h3>"+inputvars['region']+"/"+inputvars['fuel_bin']+"</h3>")

        (rowcount,) = conn.execute("SELECT count(*) from hourly_unit_activity_summary WHERE 1 > 0").fetchone()
        if rowcount > 0:
            stitle = inputvars['state']+"/"+inputvars['facility_name']+"/"+inputvars['unitid']
            if inputvars['data_type'] == 'NEW':
                stitle+= " New Unit" # Based On "+inputvars['old_unitid']
                if inputvars['unitid'][0] == 'G':
                    stitle+= " (Generic)"
            if inputvars['data_type'] == 'SWITCH':
                stitle+= " Switched from "+by_fuel_bin                
            if inputvars['data_type'] == 'RETRIED':
                stitle+= " Retired" # Based On "+inputvars['old_unitid']
            print_to_index(inputvars, "<br/><b>"+stitle+"</b><br/>")
            plot_all_variables(conn, 'hourly_unit_activity_summary', stitle, output_prefix, inputvars, logfile)
        conn.execute("DELETE FROM hourly_unit_activity_summary")
    print_to_index(inputvars, "<p>Total Units:"+str(unitcount)+"</p>")

def plot_all_variables(conn, table, title, output_prefix, inputvars, logfile):
    """Plots all of the select variables by calendar/hieracrhy hour

    Keyword arguments:
    conn          -- a valid database connection where the data is stored
    table         -- table being used in the sql query
    title         -- title of the sub plot
    output_prefix -- a prefix supplied by the user
    inputvars     -- a dictionary of input options
    logfile       -- file where logging messages will be written

    """

    if 'by_hours' in inputvars:
        if inputvars['by_hours'] == 'Calendar':
            tf_array = [True]
            hour_array = ['Calendar Hours']
        else:
            tf_array = [False]
            hour_array = ['Hierarchy Hours']
    else:
        tf_array = [False, True]
        hour_array = ['Hierarchy Hours', 'Calendar Hours']

    if 'orisid' in inputvars:
        subfolder = 'hourly_unitlevel'
    else:
        if 'state' in inputvars:
            subfolder = 'hourly_state'
        else:
            subfolder = 'hourly_regional'

    #We don't need to make base vs future year plots for new units
    if ('data_type' in inputvars and inputvars['data_type'] != 'NEW' and inputvars['data_type'] != 'RETIRED') or 'data_type' not in inputvars:
        # Scatter Plots
        if 'use_large_graphs' in inputvars:
            plot_array = [('gload', 'Gross Load', 'Gross Load (MW)'), ('heat_input', 'Heat Input', 'Heat Input (mmBtu)'), ('so2_mass', 'SO2 Mass', 'SO2 Mass (Tons)'), ('nox_mass', 'NOx Mass', 'NOx Mass (Tons)')]
            for k, pa in zip(range(len(plot_array)), plot_array):
                fig = make_figure()
                fig.suptitle(title, fontsize=16)
                plot_base_vs_future_variable(conn, fig.add_subplot(111), table, pa[0], pa[1], pa[2], inputvars, logfile)
                plt.legend(loc='lower center', fancybox=True, shadow=True, ncol=5)
                shrink_legend()
                fig.subplots_adjust(left=.05, right=.99, top=.95)
                if 'show_graphs' in inputvars:
                    plt.show()
                else:
                    figpath = subfolder+"/"+build_prefix(output_prefix,inputvars)+"lg_bvfstr"+str(k)+".png"
                    plt.savefig(inputvars['images_folder']+"/"+figpath)
                    pylab.close(fig)
                    print_to_index(inputvars, "<a href=\""+figpath+"\" target=\"new\">"+pa[1]+" Base vs. Future</a> | ")
        else:
            plot_array = [[('gload', 'Gross Load (MW)', 'Gross Load (MW)'), ('heat_input', 'Heat Input (mmBtu)', 'Heat Input (mmBtu)')], [('so2_mass', 'SO2 Mass (Tons)', 'SO2 Mass (Tons)'), ('nox_mass', 'NOx Mass (Tons)', 'NOx Mass (Tons)')]]
            (fig, axarray) = make_subplots(2, 2)
            fig.suptitle(title)
            for k, paa in zip(range(len(plot_array)), plot_array):
                for i, pa in zip(range(len(paa)), paa):
                    returns = plot_base_vs_future_variable(conn, axarray[i][k], table, pa[0], pa[1], pa[2], inputvars, logfile)
                    if k == 0:
                        axarray[i][k].set_ylabel("Future Year")
                    if i == 1:
                        axarray[i][k].set_xlabel("Base Year")
    
            ### Got Warning as folling for legend (BK, 05/23/2012)
            ### Replaced with a line of code derived from Matplotlib developer's suggestion
            # warnings.warn("Legend does not support %s\nUse proxy artist instead.\n\nhttp://matplotlib.sourceforge.net/users/legend_guide.html#using-proxy-artist\n" % (str(orig_handle),))
            #/usr/lib/pymodules/python2.7/matplotlib/legend.py:610: UserWarning: Legend does not support [<matplotlib.lines.Line2D object at 0x386f3d0>]
            #Use proxy artist instead.
            #http://matplotlib.sourceforge.net/users/legend_guide.html#using-proxy-artist
            #fig.legend([plt.Line2D((0, 0), (1, 0), color="b")], returns[1],loc='lower center', fancybox=True, shadow=True, ncol=5)
            
            #I don't think that a a warning message is of that great of a concern unless there is a failure on some platoformas - JMJ 
            fig.legend(returns[0], returns[1], loc='lower center', fancybox=True, shadow=True, ncol=5)
    
    
            shrink_legend()
            fig.subplots_adjust(left=.075, wspace=.1, right=.95, top=.95)
            if 'show_graphs' in inputvars:
                plt.show()
            else:
                figpath = subfolder+"/"+build_prefix(output_prefix,inputvars)+"sm_bvf.png"
                plt.savefig(inputvars['images_folder']+"/"+figpath)
                pylab.close(fig)
                print_to_index(inputvars, "<a href=\""+figpath+"\" target=\"new\">Base vs. Future</a> | ")

    # Hourly Plots
    if 'use_large_graphs' in inputvars:
        plot_array = [('gload', 'Gross Load', 'Gross Load (MW)'), ('heat_input', 'Heat Input', 'Heat Input (mmBtu)')]
        for k, pa in zip(range(len(plot_array)), plot_array):
            for j, tf in zip(range(len(tf_array)), tf_array):
                fig = make_figure()
                fig.suptitle(title, fontsize=16)
                ax = fig.add_subplot(111)
                plot_hourly_variable(conn, ax, table, pa[0], pa[1], inputvars, tf, logfile)
                ax.set_ylabel(pa[2])
                ax.set_xlabel(hour_array[j])
                plt.legend(loc='lower center', fancybox=True, shadow=True, ncol=5)
                shrink_legend()
                fig.subplots_adjust(left=.05, right=.99, top=.95)
                if 'show_graphs' in inputvars:
                    plt.show()
                else:
                    figpath = subfolder+"/"+build_prefix(output_prefix,inputvars)+"lg_hourly"+str(k*2+j)+".png"
                    plt.savefig(inputvars['images_folder']+"/"+figpath)
                    pylab.close(fig)
                    print_to_index(inputvars, "<a href=\""+figpath+"\" target=\"new\">"+pa[1]+" by "+['Hierarchy Hours', 'Calendar Hours'][j]+"</a> | ")

    else:
        plot_array = [[('gload', 'Gross Load', 'Gross Load (MW)'), ('heat_input', 'Heat Input', 'Heat Input (mmBtu)')],
                      [('so2_mass', 'SO2 Mass', 'SO2 Mass (Tons)'), ('nox_mass', 'NOx Mass', 'NOx Mass (Tons)')]]
        for k, paa in zip(range(len(plot_array)), plot_array):
            (fig, axarray) = make_subplots(2, len(tf_array))
            fig.suptitle(title, fontsize=16)
            for i, pa in zip(range(len(paa)), paa):
                for j, tf in zip(range(len(tf_array)), tf_array):
                    if 'by_hours' in inputvars:
                        returns = plot_hourly_variable(conn, axarray[i], table, pa[0], pa[1], inputvars, tf, logfile)
                        axarray[i].set_ylabel(pa[2])
                        if i == 1:
                            axarray[i].set_xlabel(hour_array[j])
                    else:
                        returns = plot_hourly_variable(conn, axarray[i][j], table, pa[0], pa[1], inputvars, tf, logfile)
                        if j == 0:
                            axarray[i][j].set_ylabel(pa[2])
                        if i == 1:
                            axarray[i][j].set_xlabel(hour_array[j])


            fig.legend(returns[0], returns[1],loc='lower center', fancybox=True, shadow=True, ncol=5)
            shrink_legend()
            if 'by_hours' in inputvars:
                fig.subplots_adjust(left=.075, wspace=.1, right=.99, top=.9)
            else:
                fig.subplots_adjust(left=.075, wspace=.1, right=.95, top=.95)
            if 'show_graphs' in inputvars:
                plt.show()
            else:
                figpath = subfolder+"/"+build_prefix(output_prefix,inputvars)+"sm_hourly"+str(k)+".png"
                plt.savefig(inputvars['images_folder']+"/"+figpath)
                pylab.close(fig)
                print_to_index(inputvars, "<a href=\""+figpath+"\" target=\"new\">"+['Gross Load/Heat Input', 'Pollutants'][k]+" Hourly</a> | ")

def plot_hourly_variable(conn, ax1, table, variable, title, inputvars, use_calendar, logfile):
    """Keyword arguments:
    conn         -- a valid database connection where the data is stored
    ax1          -- matplotlib subplot pointer
    table        -- table being used in the sql query
    variable     -- variable being analyzed in the plot (gload, heat_input, so2_mass, nox_mass)
    title        -- title of the sub plot
    inputvars    -- a dictionary of input options
    use_calendar -- true/false variable, if true use calendar_hours if false use hierarchy_hours
    logfile      -- file where logging messages will be written

    """
    ax1.set_title(title)
    if use_calendar:
        orderby = 'calendar_hour'
    else:
        orderby = 'hierarchy_hour'

    (where, inputs) = build_where(conn, '', inputvars, True, True)
    (rowcount,) = conn.execute("SELECT count(*) from "+table+" WHERE 1 > 0"+where, inputs).fetchone()
    #these three queries can probably be merged and the scatters can be written using numpy
    #but i have been stumped trying to get numpy to work with a query that gets it all at once - jakuta 5/10/12

    results_hours = conn.execute("SELECT "+orderby+" from "+table+" WHERE 1 > 0 "+where+" ORDER BY " + orderby + " ASC", inputs)
    results_by = conn.execute("SELECT by_"+variable+" from "+table+" WHERE 1 > 0 "+where+" ORDER BY " + orderby + " ASC", inputs)
    results_fy = conn.execute("SELECT fy_"+variable+" from "+table+" WHERE 1 > 0 "+where+" ORDER BY " + orderby + " ASC", inputs)
    (by_max, fy_max) = conn.execute("SELECT COALESCE(MAX(by_"+variable+"),0), COALESCE(MAX(fy_"+variable+"),0) from "+table+" WHERE 1 > 0 "+where, inputs).fetchone()

    r_hours = sql_to_numpy(results_hours.fetchall(), rowcount)
    r_by = sql_to_numpy(results_by.fetchall(), rowcount)
    r_fy = sql_to_numpy(results_fy.fetchall(), rowcount)

    maxy = max(by_max, fy_max)*1.1
    if use_calendar:
        ax1.set_xlim(r_hours[0], r_hours[rowcount-1])
    else:
        ax1.set_xlim(0, 8760)
    ax1.set_ylim(0, maxy)
    ax1.set_xticks(ax1.get_xticks()[::2])
    ax1.set_yticks(ax1.get_yticks()[::2])

    returns1 = []
    returns2 = []
    if use_calendar:
        returns1.append(ax1.axvspan(inputvars['ozone_start_hour'], inputvars['ozone_end_hour'], color='aqua', label='Ozone Season', zorder=-1))
        returns2.append('Ozone Season')
        add_calendar_dashes(conn, ax1, 0, maxy)

    if 'use_lines' in inputvars:
        returns1.append(ax1.plot(r_hours,r_by,c='r', label='BY'))
        returns1.append(ax1.plot(r_hours,r_fy,c='b', label='FY'))
    else:
        returns1.append(ax1.scatter(r_hours,r_by,c='r', label='BY', marker='+', alpha=0.3,cmap=cm.Paired))
        returns1.append(ax1.scatter(r_hours,r_fy,c='b', label='FY', marker='x', alpha=0.3,cmap=cm.Paired))
    returns2.append('BY')
    returns2.append('FY')
    return (returns1, returns2)

def plot_all_growth_rates(conn, table, title, output_prefix, inputvars, logfile):
    """Plots a specific variable by calendar/hieracrhy hour

    Keyword arguments:
    conn          -- a valid database connection where the data is stored
    table         -- table being used in the sql query
    title         -- title of the sub plot
    output_prefix -- output prefix for graphs
    inputvars     -- a dictionary of input options
    logfile       -- file where logging messages will be written

    """

    tf_array = [True, False]
    hour_array = ['Calendar Hours', 'Hierarchy Hours']

    (fig, axarray) = make_subplots(2, 1)
    fig.suptitle(title, fontsize=16)
    for j, tf in zip(range(len(tf_array)), tf_array):
        returns = plot_hourly_growth_rate(conn, axarray[j], table, inputvars, tf, logfile)
        if j == 0:
            axarray[j].set_ylabel("Growth Rate")
        axarray[j].set_xlabel(hour_array[j])
    fig.legend(returns[0], returns[1],loc='lower center', fancybox=True, shadow=True, ncol=5)
    shrink_legend()
    fig.subplots_adjust(left=.075, wspace=.1, right=.99, top=.9)
    if 'show_graphs' in inputvars:
        plt.show()
    else:
        if 'state' in inputvars:
            subfolder = 'hourly_state'
        else:
            subfolder = 'hourly_regional'
        figpath = subfolder+"/"+build_prefix(output_prefix,inputvars)+"sm_hourly_gr.png"
        plt.savefig(inputvars['images_folder']+"/"+figpath)
        pylab.close(fig)
        print_to_index(inputvars, "<a href=\""+figpath+"\" target=\"new\">Growth Rates Hourly</a> | ")

def plot_hourly_growth_rate(conn, ax1, table, inputvars, use_calendar, logfile):
    """Plots growth rates by hour

    Keyword arguments:
    conn         -- a valid database connection where the data is stored
    ax1          -- matplotlib subplot pointer
    table        -- table being used in the sql query
    inputvars    -- a dictionary of input options
    use_calendar -- true/false variable, if true use calendar_hours if false use hierarchy_hours
    logfile      -- file where logging messages will be written

    """

    if use_calendar:
        orderby = 'calendar_hour'
    else:
        orderby = 'hierarchy_hour'

    (where, inputs) = build_where(conn, '', inputvars, True, True)
    (rowcount,) = conn.execute("SELECT count(*) from "+table+" WHERE 1 > 0 "+where, inputs).fetchone()
    #these three queries can probably be merged and the scatters can be written using numpy
    #but i have been stumped trying to get numpy to work with a query that gets it all at once - jakuta 5/10/12


    results_hours = conn.execute("SELECT "+orderby+" from "+table+" WHERE 1 > 0 "+where+" ORDER BY " + orderby + " ASC", inputs)
    results_hsgr = conn.execute("SELECT hour_specific_growth_rate from "+table+" WHERE 1 > 0 "+where+" ORDER BY " + orderby + " ASC", inputs)
    results_afygr = conn.execute("SELECT afygr from "+table+" WHERE 1 > 0 "+where+" ORDER BY " + orderby + " ASC", inputs)

    (maxh,maxa) = conn.execute("SELECT COALESCE(MAX(hour_specific_growth_rate), 0), COALESCE(MAX(afygr), 0) from "+table+" WHERE 1 > 0 "+where, inputs).fetchone()
    maxy = max(maxh,maxa)

    (minh,mina) = conn.execute("SELECT COALESCE(MIN(hour_specific_growth_rate), 0), COALESCE(MIN(afygr), 0) from "+table+" WHERE 1 > 0 "+where, inputs).fetchone()
    miny = min(minh,mina)


    r_hours = sql_to_numpy(results_hours.fetchall(), rowcount)
    r_hsgr = sql_to_numpy(results_hsgr.fetchall(), rowcount)
    r_afygr = sql_to_numpy(results_afygr.fetchall(), rowcount)

    if use_calendar:
        ax1.set_xlim(r_hours[0], r_hours[rowcount-1])
    else:
        ax1.set_xlim(0, 8760)
    ax1.set_ylim(miny*.95, maxy*1.05)
    ax1.set_xticks(ax1.get_xticks()[::2])
    ax1.set_yticks(ax1.get_yticks()[::2])

    if use_calendar:
        ax1.axvspan(inputvars['ozone_start_hour'], inputvars['ozone_end_hour'], color='aqua', zorder=-1)
        add_calendar_dashes(conn, ax1, miny*.95, maxy*1.05)

    returns1 = []
    returns2 = []
    if 'use_lines' in inputvars:
        returns1.append(ax1.plot(r_hours,r_hsgr,c='r', label='HSGR'))
        returns1.append(ax1.plot(r_hours,r_afygr,c='b', label='AFYGR'))
    else:
        returns1.append(ax1.scatter(r_hours,r_hsgr,c='r', marker='+', label='HSGR', alpha=0.3,cmap=cm.Paired))
        returns1.append(ax1.scatter(r_hours,r_afygr,c='b', marker='x', label='AFYGR', alpha=0.3,cmap=cm.Paired))

    ax1.plot((0,1),(8760, 1),c='black', zorder=-2)
    ax1.plot((0,inputvars['peak_growth_factor']),(8760, inputvars['peak_growth_factor']),c='orange', zorder=-1, label='Peak GR: ' + str(round(inputvars['peak_growth_factor'],4)))
    ax1.plot((0,inputvars['annual_growth_factor']),(8760, inputvars['annual_growth_factor']),c='yellow', zorder=-1, label='Annual GR: ' + str(round(inputvars['annual_growth_factor'],4)))
    ax1.plot((0,inputvars['non_peak_growth_factor']), (8760, inputvars['non_peak_growth_factor']),c='teal', zorder=-1, label='Non-Peak GR: ' + str(round(inputvars['non_peak_growth_factor'],4)))

    returns2.append('HSGR')
    returns2.append('AFYGR')
    return (returns1, returns2)


def plot_base_vs_future_variable(conn, ax1, table, variable, title, ylabel, inputvars, logfile):
    """Plots the base vs future graphs of a specific variable

    Keyword arguments:
    conn      -- a valid database connection where the data is stored
    ax1       -- matplotlib subplot pointer
    table     -- table being used in the sql query
    variable  -- variable being analyzed in the plot (gload, heat_input, so2_mass, nox_mass)
    title     -- title of the sub plot
    ylabel    -- ylabel for the sub plot
    inputvars -- a dictionary of input options
    logfile   -- file where logging messages will be written

    """
    ax1.set_title(title)

    (where, inputs) = build_where(conn, '', inputvars, True, True)
    (rowcount,) = conn.execute("SELECT count(*) from "+table+" WHERE 1 > 0 "+where, inputs).fetchone()
    #these two queries can probably be merged and the scatters can be written using numpy
    #but i have been stumped trying to get numpy to work - jakuta 5/10/12

    (by_max, fy_max) = conn.execute("SELECT COALESCE(MAX(by_"+variable+"),0), COALESCE(MAX(fy_"+variable+"),0) from "+table+" WHERE 1 > 0 "+where, inputs).fetchone()    
    results_by = conn.execute("SELECT by_"+variable+" from "+table+" WHERE 1 > 0 "+where+" ORDER BY calendar_hour ASC", inputs)
    results_fy = conn.execute("SELECT fy_"+variable+" from "+table+" WHERE 1 > 0 "+where+" ORDER BY calendar_hour ASC", inputs)

    r_by = sql_to_numpy(results_by.fetchall(), rowcount)
    r_fy = sql_to_numpy(results_fy.fetchall(), rowcount)

    maxy = max((by_max, fy_max))*1.1
    ax1.set_xlim(0, maxy)
    ax1.set_ylim(0, maxy)

    ax1.set_xticks(ax1.get_xticks()[::2])
    ax1.set_yticks(ax1.get_yticks()[::2])

    ax1.scatter(r_by,r_fy,c='r', marker='+', alpha=0.3,cmap=cm.Paired)
    returns1 = []
    returns2 = []
    returns1.append(ax1.plot((0,maxy),(0,maxy),c='b', label='1:1'))
    returns2.append('1:1')
    
    if 'region' in inputvars and 'fuel_bin' in inputvars:    
        (peak_growth_factor, non_peak_growth_factor) = conn.execute("""SELECT (peak_growth_factor), (non_peak_growth_factor)  FROM calc_growth_rates cgr WHERE cgr. ertac_region = ? AND cgr.ertac_fuel_unit_type_bin = ? """, (inputvars['region'], inputvars['fuel_bin'])).fetchone()

        peaklabel = 'Peak GR: ' + str(round(peak_growth_factor,4))
        if peak_growth_factor > 1:
            returns1.append(ax1.plot((0,maxy/peak_growth_factor),(0,maxy),c='r', label=peaklabel))
        else:
            returns1.append(ax1.plot((0,maxy),(0,maxy*peak_growth_factor),c='r', label=peaklabel))
        returns2.append(peaklabel)
        
        nonpeaklabel = 'Non-Peak GR: ' + str(round(non_peak_growth_factor,4))
        if non_peak_growth_factor > 1:
            returns1.append(ax1.plot((0,maxy/non_peak_growth_factor),(0,maxy),c='g', label=nonpeaklabel))
        else:
            returns1.append(ax1.plot((0,maxy),(0,maxy*non_peak_growth_factor),c='g', label=nonpeaklabel))
        returns2.append(nonpeaklabel)
        
    return (returns1, returns2)

def add_calendar_dashes(conn, ax1, min, max):
    month_dict = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    (base_year,) = conn.execute("""SELECT DISTINCT base_year FROM calc_input_variables""").fetchone()
    for month in range(1,13):
        if month < 10:
            m = '0'+str(month)
        else:
            m = str(month)
        (hour,) = conn.execute("SELECT MAX(calendar_hour) FROM calendar_hours WHERE op_date = '"+base_year+"-"+str(m)+"-01'").fetchone()
        ax1.plot([hour, hour],ax1.get_ybound(),c='black', ls=':')
        ax1.annotate(month_dict[month], [hour+(24*10), ax1.get_ybound()[1]*.9], xytext=None, xycoords='data', textcoords='data', arrowprops=None)


def make_figure():
    return plt.figure(figsize=(11, 8))

def make_subplots(rows, cols):
    return plt.subplots(rows, cols, figsize=(11, 8))

def write_final_data(conn, out_prefix, logfile):
    """Write out projected ERTAC EGU data reports.

    Keyword arguments:
    conn       -- a valid database connection where the data is stored
    out_prefix -- optional prefix added to each output file name
    logfile    -- file where logging messages will be written

    """
    # Final output data is exported as CSV files for reporting and use with
    # other programs.
    ertac_lib.export_table_to_csv('hourly_activity_summary', out_prefix+'post_results/', 'hourly_activity_summary.csv', conn, hourly_activity_summary_columns, logfile)
    ertac_lib.export_table_to_csv('hourly_regional_activity_summary', out_prefix+'post_results/', 'hourly_regional_activity_summary.csv', conn, hourly_regional_summary_columns, logfile)
    ertac_lib.export_table_to_csv('hourly_state_activity_summary', out_prefix+'post_results/', 'hourly_state_activity_summary.csv', conn, hourly_state_summary_columns, logfile)
    ertac_lib.export_table_to_csv('annual_summary', out_prefix+'post_results/', 'annual_unit_summary.csv', conn, annual_summary_columns, logfile)
    ertac_lib.export_table_to_csv('generation_parms', out_prefix+'post_results/', 'generation_parms.csv', conn, gen_parms_columns, logfile)
    #add state/regional dump

def create_postprocessing_tables(conn):
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
            "fuel-bin=", "orisid=", "unitid=", "time-span=", "show-graphs", "large-graphs",
            "use-lines", "by-hours=", "sql-database=", "config-file="])
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
    config_file       = None
    run_integrity     = None
    run_plots     = None

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
        elif opt in ("--input-prefix-pre"):
            input_prefix_pre = arg
        elif opt in ("--input-prefix-proj"):
            input_prefix_proj = arg
        elif opt in ("-o", "--output-prefix"):
            output_prefix = arg
        elif opt in ("--run-plots"):
            run_plots = True
        elif opt in ("--run-integrity"):
            run_integrity = True
        elif opt in ("--state"):
            state_clean = True  
            for state in arg.split(","):
                if state.upper() not in state_set:
                    state_clean = False
                    print "State Not Valid: Defaulting To No Selection"
            if state_clean:
                inputvars['state'] = arg
        elif opt in ("--region"):
            inputvars['region'] = arg
        elif opt in ("--fuel-bin"):
            if arg.upper() not in fuel_set:
                print "Fuel-Bin Not Valid: Defaulting To No Selection"
            else:
                inputvars['fuel_bin'] = arg
        elif opt in ("--orisid"):
            inputvars['orisid'] = arg
        elif opt in ("--unitid"):
            inputvars['unitid'] = arg
        elif opt in ("--time-span"):
            if arg not in ['Annual', 'FirstQtr', 'FirstQuarter' 'SecondQtr', 'SecondQuarter', 'ThirdQtr', 'ThirdQuarter', 'FourthQtr', 'FourthQuarter', 'OzoneSeason']:
                print "Timespan Not Valid: Defaulting To Annual"
            else:
                if arg != 'Annual':
                    inputvars['time_span'] = arg
        elif opt in ("--by-hours"):
            if arg not in ['Both', 'Calendar', 'Hierarchy']:
                print "By Hours Not Valid: Defaulting To Annual"
            else:
                if arg != 'Both':
                    inputvars['by_hours'] = arg
        elif opt in ("--show-graphs"):
            inputvars['show_graphs'] = True
        elif opt in ("--large-graphs"):
            inputvars['use_large_graphs'] = True
        elif opt in ("--use-lines"):
            inputvars['use_lines'] = True
        elif opt in ("--sql-database"):
            sql_database = arg
        elif opt in ("--config-file"):
            config_file = arg
        else:
            assert False, "unhandled option"


    if debug_level == "DEBUG":
        # Detailed logging to file for postmortem analysis.
        logging.basicConfig(
            filename='ertac_postprocessing_debug_log.txt',
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
        logfilename = output_prefix + 'ertac_egu_postprocessing_log.txt'
    else:
        logfilename = 'ertac_egu_postprocessing_log.txt'

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
        create_postprocessing_tables(dbconn)
        # Load intermediate CSV data into tables, rejecting any rows that can't be
        # used.  There should be no invalid data at this stage, unless the
        # intermediate files were manually changed with erroneous data.
        logging.info("Loading intermediate data:")
        print >> logfile, "Loading intermediate data:"
        load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj, inputvars, logfile)
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
            create_postprocessing_tables(dbconn)
            # Load intermediate CSV data into tables, rejecting any rows that can't be
            # used.  There should be no invalid data at this stage, unless the
            # intermediate files were manually changed with erroneous data.
            logging.info("Loading intermediate data:")
            print >> logfile, "Loading intermediate data:"
            load_intermediate_data(dbconn, input_prefix_pre, input_prefix_proj, inputvars, logfile)
            logging.info("Finished loading intermediate data.")
            print >> logfile,"Finished loading intermediate data."
            existing_db_file = False

    # Loading configuration data
    if config_file:
        dbconn.executescript("""CREATE TEMPORARY TABLE config_variables (variable TEXT NOT NULL COLLATE NOCASE, value TEXT NOT NULL);""")
        ertac_lib.load_csv_into_table(None, config_file, 'config_variables', dbconn, (('variable', 'str', True, None), ('value', 'str', True, None)), sys.stderr)
        for (variable, value) in dbconn.execute("""SELECT variable, value FROM config_variables where variable in ('region', 'fuel-bin', 'orisid', 'unitid', 'time-span', 'by-hours')""").fetchall():
            inputvars[re.sub(r'-','_',variable)]=value
            print "Updating input "+variable
        for (variable, value) in dbconn.execute("""SELECT variable, value FROM config_variables where variable in ('show-graphs', 'large-graphs', 'use-lines')""").fetchall():
            inputvars[re.sub(r'-','_',variable)]=True
            print "Updating input "+variable
        dbconn.executescript("""DROP TABLE config_variables;""")

    output_prefix = build_prefix(output_prefix, inputvars)

    # 20120406 Determine ozone season start/end dates and calendar hours.
    (inputvars['base_year'], inputvars['future_year'], ozone_start, ozone_end) = dbconn.execute("""SELECT DISTINCT
    base_year, future_year, ozone_start_date, ozone_end_date
    FROM calc_input_variables""").fetchone()
    ozone_start_base   = ertac_lib.convert_ozone_date(ozone_start, inputvars['base_year'])
    ozone_end_base     = ertac_lib.convert_ozone_date(ozone_end, inputvars['base_year'])
    ozone_start_future = ertac_lib.convert_ozone_date(ozone_start, inputvars['future_year'])
    ozone_end_future   = ertac_lib.convert_ozone_date(ozone_end, inputvars['future_year'])

    # Need to convert operating date/hour into calendar hour for outputs.
    ertac_lib.make_calendar_hours(inputvars['base_year'], inputvars['future_year'], dbconn)
    (inputvars['ozone_start_hour'],) = dbconn.execute("""SELECT MIN(calendar_hour)
    FROM calendar_hours
    WHERE op_date >= ?""", (ozone_start_base,)).fetchone()

    (inputvars['ozone_end_hour'],) = dbconn.execute("""SELECT MAX(calendar_hour)
    FROM calendar_hours
    WHERE op_date <= ?""", (ozone_end_base,)).fetchone()

    #this stretch makes the directory structure necessary for saving results
    if not os.path.exists(output_prefix+'post_results'):
        os.makedirs(output_prefix+'post_results')
    if 'show_graphs' not in inputvars and run_plots:
        if not os.path.exists(output_prefix+'post_results/hourly_unitlevel'):
            os.makedirs(output_prefix+'post_results/hourly_unitlevel')
        if not os.path.exists(output_prefix+'post_results/annual_unitlevel'):
            os.makedirs(output_prefix+'post_results/annual_unitlevel')
        if not os.path.exists(output_prefix+'post_results/hourly_regional'):
            os.makedirs(output_prefix+'post_results/hourly_regional')
        if not os.path.exists(output_prefix+'post_results/hourly_state'):
            os.makedirs(output_prefix+'post_results/hourly_state')
        index = open(output_prefix+'post_results/index.html', 'w')
        print >> index, "<h1>Index For ERTAC Post-Processing</h1>"
        print >> index, "<p>Start Time: "+ time.asctime()
        if 'region' in inputvars:
            print >> index, "<br/>Region: "+inputvars['region']
        if 'fuel_bin' in inputvars:
            print >> index, "<br/>Fuel Bin Type: "+inputvars['fuel_bin']
        if 'orisid' in inputvars:
            print >> index, "<br/>ORISPL Code: "+inputvars['orisid']
            if 'unitid' in inputvars:
                print >> index, "<br/>Unit ID: "+inputvars['unitid']
        inputvars['images_folder'] = output_prefix+'post_results'
        inputvars['index'] = output_prefix+'post_results/index.html'
        index.close()

    if existing_db_file:
        logging.info("Using the existing summary data for plots and CSV files...")
        print >> logfile, "Using the existing summary data for plots and CSV files..."
    else:
        logging.info("Summarizing data...")
        print >> logfile
        print >> logfile, "Summarizing data..."
        summarize_hourly_results(dbconn, inputvars, logfile)
        if run_integrity:
            logging.info("Running integrity check...")
            print >> logfile, "Running integrity check..."
            run_intergrity_check(dbconn, logfile)


    if run_plots:
        logging.info("Plotting.")
        print >> logfile
        print >> logfile, "Plotting."
        #Perform all of the plotting
        if 'ertac_region' in inputvars:
            plot_state(dbconn, '', inputvars, logfile)
        plot_regional(dbconn, '', inputvars, logfile)
        plot_all_units(dbconn, '', inputvars, logfile)

    # Export projection report tables as CSV files.
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
