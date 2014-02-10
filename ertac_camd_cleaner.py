#!/usr/bin/python

# ertac_postprocess.py

"""ERTAC EGU post processing"""

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

import sys
try:
    import getopt, logging, os, time, re, csv, datetime
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
    import ertac_lib, ertac_reports
    from   ertac_tables import * # importaing all dictionaries, tables, etc
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

                
def load_intermediate_data(conn, in_prefix_pre, logfile):
    """Load intermediate ERTAC EGU data from preprocessor for projection.

    Keyword arguments:
    conn           -- a valid database connection where the data will be stored
    in_prefix_pre  -- optional prefix added to each input file name generated from preprocessor
    in_prefix_post -- optional prefix added to each input file name generated from projection
    logfile        -- file where logging messages will be written

    """

    #ertac_lib.load_csv_into_table(None, os.path.join(os.path.relpath(sys.path[0]), 'states.csv'), 'states', conn, states_columns, logfile)
    # This section will reject any input rows that are missing required fields,
    # have unreadable data, or violate key constraints, because it is impossible
    # to store that data in the database tables.
    ertac_lib.load_csv_into_table(in_prefix_pre, 'camd_hourly_base.csv', 'camd_hourly_base', conn, camd_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix_pre, 'ertac_initial_uaf.csv', 'ertac_initial_uaf', conn, uaf_columns, logfile)
    


def process_results(conn, inputvars, logfile):
    """Summarize hourly data and merge base year and future year results.

    Keyword arguments:
    conn      -- a valid database connection where the data is stored
    inputvars -- a dictionary of input options
    logfile   -- file where logging messages will be written

    """

    logging.info("Processing Deletions")
    print >> logfile, "Processing Deletions"
    conn.execute("""UPDATE camd_hourly_base SET nox_mass = 0 WHERE nox_mass < 0""").fetchall()
    conn.execute("""UPDATE camd_hourly_base SET so2_mass = 0 WHERE so2_mass < 0""").fetchall()
    conn.execute("""DELETE FROM camd_hourly_base WHERE (gload IS NULL OR gload = 0) AND (sload IS NULL OR sload = 0) AND heat_input > 0""").fetchall()
    conn.execute("""DELETE, unitid FROM camd_hourly_base WHERE (gload > 0 OR sload > 0) AND (heat_input IS NULL OR heat_input = 0)""").fetchall()
   
    # Save changes
    conn.commit()

def run_diagnostics(conn, inputvars, logfile):
    logging.info("Running Diangostics")   
    print >> logfile, "Running Diangostics"
    
    zero_gload_info = conn.execute("""SELECT orispl_code, unitid FROM camd_hourly_base GROUP BY orispl_code, unitid HAVING SUM(COALESCE(COALESCE(gload, sload),0)) = 0""").fetchall()

    if len(zero_gload_info) > 0:
        print >> logfile, "Warning:", len(zero_gload_info), "facility/units in CAMD base file have either total gload or sload equal 0:"
        for unit in zero_gload_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)

    zero_hi_info = conn.execute("""SELECT orispl_code, unitid FROM camd_hourly_base GROUP BY orispl_code, unitid HAVING SUM(COALESCE(heat_input,0)) = 0""").fetchall()

    if len(zero_hi_info) > 0:
        print >> logfile, "Warning:", len(zero_hi_info), "facility/units in CAMD base file have total heat input equal 0:"
        for unit in zero_hi_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)

    missing_gload_info = conn.execute("""SELECT orispl_code, unitid, op_date, op_hour FROM camd_hourly_base WHERE (gload IS NULL OR gload = 0) AND (sload IS NULL OR sload = 0) AND heat_input > 0""").fetchall()
    
    if len(missing_gload_info) > 0:
        print >> logfile, "Warning:", len(missing_gload_info), "hours in CAMD base file have heat_input, but no gload or sload:"
        for unit in missing_gload_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)
    
    missing_hi_info = conn.execute("""SELECT orispl_code, unitid, op_date, op_hour FROM camd_hourly_base WHERE (gload > 0 OR sload > 0) AND (heat_input IS NULL OR heat_input = 0)""").fetchall()

    if len(missing_hi_info) > 0:
        print >> logfile, "Warning:", len(missing_hi_info), "hours in CAMD base file have gload or sload, but no heat_input:"
        for unit in missing_hi_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)

    large_gload_info = conn.execute("""SELECT chb.orispl_code, chb.unitid, op_date, op_hour FROM camd_hourly_base chb LEFT JOIN ertac_initial_uaf eif on chb.orispl_code = eif.orispl_code AND chb.unitid = eif.unitid  WHERE (gload > max_unit_heat_input * 2/3.412141633) OR (sload > max_unit_heat_input * 2/3.412141633)""").fetchall()

    if len(large_gload_info) > 0:
        print >> logfile, "Warning:", len(large_gload_info), "hours in CAMD base file have gload or sload that is more than twice max heat input (converted):"
        for unit in large_gload_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)
            
    large_hi_info = conn.execute("""SELECT chb.orispl_code, chb.unitid, op_date, op_hour FROM camd_hourly_base chb LEFT JOIN ertac_initial_uaf eif on chb.orispl_code = eif.orispl_code AND chb.unitid = eif.unitid  WHERE heat_input > max_unit_heat_input * 2""").fetchall()

    if len(large_hi_info) > 0:
        print >> logfile, "Warning:", len(large_hi_info), "hours in CAMD base file have heat input more that is than twice max max heat input:"
        for unit in large_hi_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)
     
    negative_nox_info = conn.execute("""SELECT orispl_code, unitid, op_date, op_hour FROM camd_hourly_base where nox_mass < 0""").fetchall()

    if len(negative_nox_info) > 0:
        print >> logfile, "Warning:", len(negative_nox_info), "hours in CAMD base file have negative NOX Mass:"
        for unit in negative_nox_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)
            
                
    negative_so2_info = conn.execute("""SELECT orispl_code, unitid, op_date, op_hour FROM camd_hourly_base where so2_mass < 0""").fetchall()

    if len(negative_so2_info) > 0:
        print >> logfile, "Warning:", len(negative_so2_info), "hours in CAMD base file have negative SO2 Mass:"
        for unit in negative_so2_info:
            print >> logfile, "  " + ertac_lib.nice_str(unit)
                                                 
def write_final_data(conn, out_prefix, logfile):
    """Write out projected ERTAC EGU data reports.

    Keyword arguments:
    conn       -- a valid database connection where the data is stored
    out_prefix -- optional prefix added to each output file name
    logfile    -- file where logging messages will be written

    """
    # Final output data is exported as CSV files for reporting and use with
    # other programs.
    ertac_lib.export_table_to_csv('calc_hourly_base', out_prefix, 'cleaned_camd_hourly.csv', conn, calc_hourly_columns, logfile)
   
 
def main(argv=None):
    # Main projection program begins here.
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "hdqv:o:",
            ["help", "debug", "quiet", "verbose", 
            "input-prefix-pre=", "input-prefix-proj=", "output-prefix=", "orl-files=", "sql-database=", "state=", "ignore-pollutants=", "output-type="])
    except getopt.GetoptError, err:
        print
        print str(err)
        usage(argv[0])
        return 2

    # Initializing option variables
    debug_level       = "INFO"
    input_prefix_pre  = None
    output_prefix     = ''
    inputvars         = {}
    delete_entries    = False

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
        elif opt in ("-o", "--output-prefix"):
            output_prefix = arg
        elif opt in ("--delete"):
            delete_entries = True
        else:
            assert False, "unhandled option"


    if debug_level == "DEBUG":
        # Detailed logging to file for postmortem analysis.
        logging.basicConfig(
            filename='ertac_camd_cleaner_debug_log.txt',
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
        logfilename = output_prefix + 'ertac_camd_cleaner_log.txt'
    else:
        logfilename = 'ertac_camd_cleaner_log.txt'

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
    logging.info("Creating database tables in memory.")
    print >> logfile, "Creating database tables in memory."
    # The preprocessor output tables are used as the projection inputs.
    # The projection output tables produce all the reports.
    dbconn = sqlite3.connect('')
    ertac_lib.run_script_file('create_preprocessor_input_tables.sql', dbconn)
   
    # Load intermediate CSV data into tables, rejecting any rows that can't be
    # used.  There should be no invalid data at this stage, unless the
    # intermediate files were manually changed with erroneous data.
    logging.info("Loading intermediate data:")
    print >> logfile, "Loading intermediate data:"
    load_intermediate_data(dbconn, input_prefix_pre, logfile)
    logging.info("Finished loading intermediate data.")
    print >> logfile,"Finished loading intermediate data."
    existing_db_file = False

    run_diagnostics(dbconn, inputvars, logfile)
    if delete_entries:
        process_results(dbconn, inputvars, logfile)
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
