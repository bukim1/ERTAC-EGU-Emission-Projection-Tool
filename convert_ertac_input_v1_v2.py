#!/usr/bin/python

# convert_ertac_input_v1_v2.py

"""Convert ERTAC EGU input files from V1 to V2 format where needed"""

VERSION = "2.0b"
# Updated to version 2.0b as of 8/13/2015.

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

import sys

try:
    import getopt, logging, os, time
except ImportError:
    print >> sys.stderr, "Fatal error: can't import all required modules."
    print >> sys.stderr, "Run python -V to find your Python version."
    raise

# This section was changed after Jin announced that he had been manually
# modifying every earlier copy of the model code in order to load SQLite3 into
# his unexpected older Python installation.
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
    import ertac_lib, ertac_tables
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
# Output file names are based on the inputs, with _v2 appended to indicate that
# the file formats are for model version 2.*; output files can have an optional
# prefix specified with the -o command-line switch.
#
# All Python and SQL code files and the fixed CSV files for lookup tables can
# (and should) be in a separate non-data directory, but must be kept together.
#
# Example usage: assuming for example that there is a valid set of input CSV
# files in V1 format present in ~/egu_data and that the program code files are
# located in ~/ertac2code, then change into the data directory and run the
# format conversion program by the following two commands:
#
# cd ~/egu_data
# ../ertac2code/convert_ertac_input_v1_v2.py



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

  -i prefix, --input-prefix=prefix.
  -o prefix, --output-prefix=prefix.
""" % progname



def main(argv=None):
    # Main file conversion program begins here.
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "hdqvi:o:", ["help", "debug", "quiet", "verbose", "input-prefix=", "output-prefix="])
    except getopt.GetoptError, err:
        print
        print str(err)
        usage(argv[0])
        return 2

    debug_level = "INFO"
    input_prefix = ""
    output_prefix = ""

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
        elif opt in ("-i", "--input-prefix"):
            input_prefix = arg
        elif opt in ("-o", "--output-prefix"):
            output_prefix = arg
        else:
            assert False, "unhandled option"

    if debug_level == "DEBUG":
        # Detailed logging to file for postmortem analysis.
        logging.basicConfig(
            filename = output_prefix + 'convert_input_debug_log.txt',
            filemode = 'w',
            format = '%(asctime)s %(levelname)-8s %(filename)s %(lineno)d %(message)s',
            level = logging.DEBUG)

    elif debug_level == "INFO":
        # Brief logging to screen, to show program progress.
        logging.basicConfig(format = '%(levelname)-8s %(message)s', level = logging.INFO)

    elif debug_level == "NONE":
        # No logging.
        logging.basicConfig()


    # Regular program operation log file, separate from detailed debug log above.
    logfilename = output_prefix + 'convert_input_log.txt'
    try:
        logfile = open(logfilename, 'w')
    except IOError:
        print >> sys.stderr, "Log file: " + logfilename + " -- Could not be written.  Program will terminate."
        raise


    # Identify versions of Python and SQLite library, and record in log file.
    # Also log revision dates of model code.
    logging.info("Program started at " + time.asctime())
    logging.info("convert_ertac_input_v1_v2 version: " + VERSION)
    #logging.info("Using ertac_lib version: " + ertac_lib.VERSION) # revert to lib v1.02 for now
    logging.info("Using ertac_tables version: " + ertac_tables.VERSION)
    logging.info("Running under python version: " + sys.version)
    logging.info("Using sqlite3 module version: " + sqlite3.version)
    logging.info("Linked against sqlite3 database library version: " + sqlite3.sqlite_version)
    print >> logfile, "Program started at " + time.asctime()
    print >> logfile, "convert_ertac_input_v1_v2 version: " + VERSION
    #print >> logfile, "Using ertac_lib version: " + ertac_lib.VERSION
    print >> logfile, "Using ertac_tables version: " + ertac_tables.VERSION
    print >> logfile, "Running under python version: " + sys.version
    print >> logfile, "Using sqlite3 module version: " + sqlite3.version
    print >> logfile, "Linked against sqlite3 database library version: " + sqlite3.sqlite_version
    print >> logfile, "Model code versions:"
    for file_name in [os.path.basename(sys.argv[0]), 'ertac_lib.py', 'ertac_tables.py',
            'create_preprocessor_input_tables.sql']:
        print >> logfile, "  " + file_name + ": " + time.ctime(os.path.getmtime(os.path.join(sys.path[0], file_name)))


    # Create and populate the working database.
    try:
        dbconn = sqlite3.connect('')
        dbconn.text_factory = str
    except:
        print >> sys.stderr, "Error while opening database.  Program will terminate."
        raise

    logging.info("Creating database tables.")
    ertac_lib.run_script_file('create_preprocessor_input_tables.sql', dbconn)

    # Actual work is straightforward: load V1 format CSV files into V1 format
    # internal tables, copy from V1 tables into new V2 tables, and export V2
    # tables out to CSV files.

    # Load input CSV data into tables, rejecting any rows that can't be used.
    logging.info("Loading input data:")
    load_v1_input_data(dbconn, input_prefix, logfile)
    logging.info("Finished loading input data.")

    # Export converted input files as input to V2 preprocessor.
    logging.info("Writing output data:")
    write_converted_v2_data(dbconn, output_prefix, logfile)
    logging.info("Finished writing output data.")
    dbconn.close()
    logging.info("Program ended at " + time.asctime())
    print >> logfile
    print >> logfile, "Program ended at " + time.asctime()

    # End of main routine



def load_v1_input_data(conn, in_prefix, logfile):
    """Load ERTAC input files in V1 format for conversion into V2 format.

    Keyword arguments:
    conn -- a valid database connection where the data will be stored
    in_prefix -- optional prefix added to each input file name
    logfile -- file where logging messages will be written

    """

    # This section will reject any input rows that are missing required fields,
    # have unreadable data, or violate key constraints, because it is impossible
    # to store that data in the database tables.
    if not ertac_lib.load_csv_into_table(in_prefix, 'ertac_input_variables.csv',
            'ertac_input_variables_v1', conn, ertac_tables.input_variable_columns_v1, logfile):
        msg = "Warning: could not load " + in_prefix + "ertac_input_variables.csv in V1 format, so will not create V2 format."
        logging.info(msg)
        print >> logfile, msg
    # From 8/10/2015 call, added hours_cap column to UAF, so have to read, copy,
    # and convert it as well.
    if not ertac_lib.load_csv_into_table(in_prefix, 'ertac_initial_uaf.csv',
            'ertac_initial_uaf_v1', conn, ertac_tables.uaf_columns_v1, logfile):
        msg = "Warning: could not load " + in_prefix + "ertac_initial_uaf.csv in V1 format, so will not create V2 format."
        logging.info(msg)
        print >> logfile, msg



def write_converted_v2_data(conn, out_prefix, logfile):
    """Write out converted ERTAC input files for use with V2 preprocessor.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    out_prefix -- optional prefix added to each output file name
    logfile -- file where logging messages will be written

    """

    # Copy from V1 tables to V2 tables and write out to CSV if not empty.
    if copy_ertac_input_variables(conn):
        ertac_lib.export_table_to_csv('ertac_input_variables', out_prefix,
            'ertac_input_variables_v2.csv', conn, ertac_tables.input_variable_columns, logfile)
        msg = "Existing data was converted to new format in " + out_prefix + "ertac_input_variables_v2.csv"
        logging.info(msg)
        print >> logfile, msg
    if copy_ertac_initial_uaf(conn):
        ertac_lib.export_table_to_csv('ertac_initial_uaf', out_prefix,
            'ertac_initial_uaf_v2.csv', conn, ertac_tables.uaf_columns, logfile)
        msg = "Existing data was converted to new format in " + out_prefix + "ertac_initial_uaf_v2.csv"
        logging.info(msg)
        print >> logfile, msg



def copy_ertac_input_variables(conn):
    """Copy data from V1 ertac_input_variables into new V2 format, returning number of rows processed."""
    # New ertac_input_variables has many extra columns inserted in a block within
    # the original columns, so copy all from V1 table into V2 columns corresponding
    # by name explicitly.
    rows_inserted = conn.execute("""INSERT INTO ertac_input_variables
        (ertac_region, ertac_fuel_unit_type_bin, base_year, future_year,
        ozone_start_date, ozone_end_date, hourly_hierarchy_code,
        new_unit_max_size, new_unit_min_size, demand_cushion,
        facility_1, facility_2, facility_3, facility_4, facility_5,
        facility_6, facility_7, facility_8, facility_9, facility_10,
        maximum_annual_ertac_uf, capacity_demand_deficit_review,
        unit_optimal_load_threshold_determinant, proxy_percentage,
        generic_so2_control_efficiency, generic_scr_nox_rate, generic_sncr_nox_rate,
        new_unit_hierarchy_placement_percentile, new_unit_emission_factor_percentile,
        unit_min_optimal_load_threshold_determinant, heat_input_calculation_percentile)
        SELECT ertac_region, ertac_fuel_unit_type_bin, base_year, future_year,
        ozone_start_date, ozone_end_date, hourly_hierarchy_code,
        new_unit_max_size, new_unit_min_size, demand_cushion,
        facility_1, facility_2, facility_3, facility_4, facility_5,
        facility_6, facility_7, facility_8, facility_9, facility_10,
        maximum_annual_ertac_uf, capacity_demand_deficit_review,
        unit_optimal_load_threshold_determinant, proxy_percentage,
        generic_so2_control_efficiency, generic_scr_nox_rate, generic_sncr_nox_rate,
        new_unit_hierarchy_placement_percentile, new_unit_emission_factor_percentile,
        unit_min_optimal_load_threshold_determinant, heat_input_calculation_percentile
        FROM ertac_input_variables_v1""").rowcount
    return rows_inserted



def copy_ertac_initial_uaf(conn):
    """Copy data from V1 ertac_initial_uaf into new V2 format, returning number of rows processed."""
    # New UAF has one extra column added to the right side, so only need to append
    # a single NULL entry when copying from old table to new.
    rows_inserted = conn.execute("""INSERT INTO ertac_initial_uaf
        SELECT *, NULL
        FROM ertac_initial_uaf_v1""").rowcount
    return rows_inserted



if __name__ == '__main__':
    sys.exit(main())
