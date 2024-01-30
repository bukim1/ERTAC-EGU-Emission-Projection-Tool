#!/usr/bin/python

# ertac_projection.py

"""ERTAC EGU projection algorithm"""

import sys

VERSION = "3.1"
#Updated to v3.1 as of January 18, 2024

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.


try:
    import getopt, logging, os, time
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
    import ertac_lib, ertac_tables, ertac_reports
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
# CSV files present in ~/egu_data and that the program code files are in
# ~/ertac2code, then change into the data directory and run the projection
# program by the following two commands:
#
# cd ~/egu_data
# ~/ertac2code/ertac_projection.py


def usage(progname):
    """Print brief usage message showing command-line options.

    Keyword arguments:
    progname -- program name to be inserted in usage message

    """

    print(("""
Usage: %s [OPTION]...

  -h, --help        print this message.

  -d, --debug       log extended debugging information.
  -q, --quiet       quiet operation (no status messages).
  -v, --verbose     verbose status messages (default).

  -i prefix, --input-prefix=prefix.
  -o prefix, --output-prefix=prefix.
  --suppress-gdus
""") % progname)


def main(argv=None):
    # Main projection program begins here.
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "hdqvi:o:",
                                   ["help", "debug", "quiet", "verbose", "input-prefix=", "output-prefix=",
                                    "suppress-gdus"])
    except getopt.GetoptError as err:
        print()
        print((str(err)))
        usage(argv[0])
        return 2

    debug_level = "INFO"
    input_prefix = ""
    output_prefix = ""

    inputvars = {}
    inputvars['add_generic_units'] = True

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
        elif opt in "--suppress-gdus":
            # jmj 6/2/2017 added an option to suppress gdu creation for diagnostic purposes
            inputvars['add_generic_units'] = False
        else:
            assert False, "unhandled option"

    if debug_level == "DEBUG":
        # Detailed logging to file for postmortem analysis.
        logging.basicConfig(
            filename=output_prefix + 'ertac_projection_debug_log.txt',
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
    logfilename = output_prefix + 'ertac_egu_projection_log.txt'
    try:
        logfile = open(logfilename, 'w')
    except IOError:
        print("Log file: " + logfilename + " -- Could not be written.  Program will terminate.", file=sys.stderr)
        raise

    # Identify versions of Python and SQLite library, and record in log file.
    logging.info("Program started at " + time.asctime())
    logging.info("ERTAC EGU projection version: " + VERSION)
    logging.info("Using ertac_lib version: " + ertac_lib.VERSION)
    logging.info("Using ertac_tables version: " + ertac_tables.VERSION)
    logging.info("Using ertac_reports version: " + ertac_reports.VERSION)
    logging.info("Running under python version: " + sys.version)
    #logging.info("Using sqlite3 module version: " + sqlite3.version) #JMJ being depricated in Python 3.14
    logging.info("Linked against sqlite3 database library version: " + sqlite3.sqlite_version)
    print("Program started at " + time.asctime(), file=logfile)
    print("ERTAC EGU projection version: " + VERSION, file=logfile)
    print("Using ertac_lib version: " + ertac_lib.VERSION, file=logfile)
    print("Using ertac_tables version: " + ertac_tables.VERSION, file=logfile)
    print("Using ertac_reports version: " + ertac_reports.VERSION, file=logfile)
    print("Running under python version: " + sys.version, file=logfile)
    #print("Using sqlite3 module version: " + sqlite3.version, file=logfile) #JMJ being depricated in Python 3.14
    print("Linked against sqlite3 database library version: " + sqlite3.sqlite_version, file=logfile)
    print("Model code versions:", file=logfile)
    for file_name in [os.path.basename(sys.argv[0]), 'ertac_lib.py', 'ertac_tables.py', 'ertac_reports.py',
                      'create_preprocessor_output_tables.sql', 'create_projection_output_tables.sql']:
        print("  " + file_name + ": " + time.ctime(os.path.getmtime(os.path.join(sys.path[0], file_name))),
              file=logfile)

    # jmj 6/2/2017 warn users that these results will result in lost generaiton
    if not inputvars['add_generic_units']:
        logging.info(
            "Warning: --suppress-gdus will likely result in incorrect allocated generation and should not be used for regulatory purposes.  It should only be used for diagnostic testing.")
        print(
            "\nWarning: --suppress-gdus will likely result in incorrect allocated generation and should not be used for regulatory purposes.  It should only be used for diagnostic testing.",
            file=logfile)

    # Create and populate the working database.
    try:
        dbconn = sqlite3.connect('')
        dbconn.text_factory = str
    except:
        print("Error while opening database.  Program will terminate.", file=sys.stderr)
        raise

    logging.info("Creating database tables.")
    # The preprocessor output tables are used as the projection inputs.
    # The projection output tables produce all the reports.
    ertac_lib.run_script_file('create_preprocessor_output_tables.sql', dbconn)
    ertac_lib.run_script_file('create_projection_output_tables.sql', dbconn)
    # Also need state lookup table, for abbreviation-FIPS code conversion.
    dbconn.executescript("""CREATE TABLE states
    (state_code TEXT NOT NULL,
    state_abbreviation TEXT NOT NULL COLLATE NOCASE,
    state_name TEXT NOT NULL COLLATE NOCASE,
    PRIMARY KEY (state_code),
    UNIQUE (state_abbreviation));""")

    # Load intermediate CSV data into tables, rejecting any rows that can't be
    # used.  There should be no invalid data at this stage, unless the
    # intermediate files were manually changed with erroneous data.
    logging.info("Loading intermediate data:")
    load_intermediate_data(dbconn, input_prefix, logfile)
    logging.info("Finished loading intermediate data.")

    # Fill in unspecified online/offline dates with sentinel values outside
    # normal date range.
    dbconn.execute("""UPDATE calc_updated_uaf
    SET online_start_date = ?
    WHERE online_start_date IS NULL""", (ertac_lib.online_default,))
    dbconn.execute("""UPDATE calc_updated_uaf
    SET offline_start_date = ?
    WHERE offline_start_date IS NULL""", (ertac_lib.offline_default,))
    dbconn.execute("""UPDATE calc_control_emissions
    SET factor_start_date = ?
    WHERE factor_start_date IS NULL""", (ertac_lib.online_default,))
    dbconn.execute("""UPDATE calc_control_emissions
    SET factor_end_date = ?
    WHERE factor_end_date IS NULL""", (ertac_lib.offline_default,))

    # 20120406 Determine ozone season start/end dates and calendar hours.
    (base_year, future_year, ozone_start, ozone_end) = dbconn.execute("""SELECT DISTINCT
    base_year, future_year, ozone_start_date, ozone_end_date
    FROM calc_input_variables""").fetchone()
    ozone_start_base = ertac_lib.convert_ozone_date(ozone_start, base_year)
    ozone_end_base = ertac_lib.convert_ozone_date(ozone_end, base_year)
    ozone_start_future = ertac_lib.convert_ozone_date(ozone_start, future_year)
    ozone_end_future = ertac_lib.convert_ozone_date(ozone_end, future_year)

    # Need to convert operating date/hour into calendar hour for outputs.
    ertac_lib.make_calendar_hours(base_year, future_year, dbconn)

    (ozone_start_hour,) = dbconn.execute("""SELECT MIN(calendar_hour)
    FROM calendar_hours
    WHERE op_date >= ?""", (ozone_start_base,)).fetchone()

    (ozone_end_hour,) = dbconn.execute("""SELECT MAX(calendar_hour)
    FROM calendar_hours
    WHERE op_date <= ?""", (ozone_end_base,)).fetchone()

    # Need table counting number of generic units created in each state, across
    # all regions and fuels, so generic unit IDs can be created with Gssuuu
    # naming scheme.
    # 20120501 Changed to include all possible state abbreviations, even if no
    # units were active.
    dbconn.executescript("""CREATE TEMPORARY TABLE generic_unit_counts
    (state TEXT NOT NULL COLLATE NOCASE,
    state_code TEXT NOT NULL,
    units_created INTEGER NOT NULL,
    PRIMARY KEY (state));

    INSERT INTO generic_unit_counts (state, state_code, units_created)
    SELECT DISTINCT state_abbreviation, state_code, 0
    FROM states;""")

    # Loop over regions, assigning generation and allocating ExGenPool for each
    # fuel bin within the region, then evaluating spinning reserve for the
    # region.
    logging.info("Assigning generation and evaluating spinning reserve.")
    print(file=logfile)
    print("Assigning generation and evaluating spinning reserve.", file=logfile)
    for (region,) in dbconn.execute("""SELECT DISTINCT ertac_region
    FROM calc_generation_parms
    ORDER BY ertac_region""").fetchall():
        assign_generation_all_fuels(dbconn, base_year, future_year, region, inputvars, logfile)
        evaluate_spinning_reserve(dbconn, region, logfile)

    # RW 9/14/2015 After generation assignment and generic unit creation have
    # initialized the demand_generation_deficit table, add any rows needed to
    # show demand transfers at other hours without deficits.
    dbconn.executescript("""INSERT INTO demand_generation_deficit
    (ertac_region, ertac_fuel_unit_type_bin, calendar_hour)
    SELECT transfer_region, transfer_fuel, calendar_hour
    FROM calc_demand_transfer_summary
    EXCEPT
    SELECT ertac_region, ertac_fuel_unit_type_bin, calendar_hour
    FROM demand_generation_deficit;

    UPDATE demand_generation_deficit
    SET generation_due_to_demand_transfer = (SELECT cdts.net_demand_change
    FROM calc_demand_transfer_summary cdts
    WHERE cdts.transfer_region = demand_generation_deficit.ertac_region
    AND cdts.transfer_fuel = demand_generation_deficit.ertac_fuel_unit_type_bin
    AND cdts.calendar_hour = demand_generation_deficit.calendar_hour);

    UPDATE demand_generation_deficit
    SET transfer_flag = 'T'
    WHERE generation_due_to_demand_transfer <> 0.0;""")

    # Summarize unit level generation and heat input.
    logging.info("Summarizing unit level generation and heat input.")
    print(file=logfile)
    print("Summarizing unit level generation and heat input.", file=logfile)
    summarize_unit_activity(dbconn, base_year, future_year, logfile)

    # Calculate future emissions.
    logging.info("Calculating future emissions.")
    print(file=logfile)
    print("Calculating future emissions.", file=logfile)
    calculate_future_emissions(dbconn, base_year, future_year, ozone_start_base, ozone_end_base, ozone_start_future,
                               ozone_end_future, logfile)

    # Summarize future emissions.
    logging.info("Summarizing future emissions.")
    print(file=logfile)
    print("Summarizing future emissions.", file=logfile)
    summarize_future_emissions(dbconn, ozone_start_hour, ozone_end_hour, logfile)

    # Summarize future capacity.
    logging.info("Summarizing future capacity.")
    print(file=logfile)
    print("Summarizing future capacity.", file=logfile)
    summarize_future_capacity(dbconn, logfile)

    # Clear out sentinel values for online/offline dates before data export.
    dbconn.execute("""UPDATE calc_updated_uaf
    SET online_start_date = NULL
    WHERE online_start_date = ?""", (ertac_lib.online_default,))
    dbconn.execute("""UPDATE calc_updated_uaf
    SET offline_start_date = NULL
    WHERE offline_start_date = ?""", (ertac_lib.offline_default,))
    dbconn.execute("""UPDATE calc_control_emissions
    SET factor_start_date = NULL
    WHERE factor_start_date = ?""", (ertac_lib.online_default,))
    dbconn.execute("""UPDATE calc_control_emissions
    SET factor_end_date = NULL
    WHERE factor_end_date = ?""", (ertac_lib.offline_default,))

    # Export projection report tables as CSV files.
    logging.info("Writing out projection reports:")
    write_final_data(dbconn, output_prefix, logfile)
    logging.info("Finished writing projection reports.")
    dbconn.close()
    logging.info("Program ended at " + time.asctime())
    print(file=logfile)
    print("Program ended at " + time.asctime(), file=logfile)

    # End of main routine


def load_intermediate_data(conn, in_prefix, logfile):
    """Load intermediate ERTAC EGU data from preprocessor for projection.

    Keyword arguments:
    conn -- a valid database connection where the data will be stored
    in_prefix -- optional prefix added to each input file name
    logfile -- file where logging messages will be written

    """
    # jmj fails when a necessary file is not load 150413
    ertac_lib.load_csv_into_table(None, os.path.join(os.path.relpath(sys.path[0]), 'states.csv'), 'states', conn,
                                  ertac_tables.states_columns, logfile)
    # This section will reject any input rows that are missing required fields,
    # have unreadable data, or violate key constraints, because it is impossible
    # to store that data in the database tables.
    # For V2, have expanded version of UAF, input variables, and generation parms,
    # and new table for demand transfers.
    if not ertac_lib.load_csv_into_table(in_prefix, 'calc_updated_uaf_v2.csv', 'calc_updated_uaf', conn,
                                         ertac_tables.calc_uaf_columns, logfile):
        print("Fatal error: could not load necessary file calc_updated_uaf_v2", file=sys.stderr)
        sys.exit(1)
    if not ertac_lib.load_csv_into_table(in_prefix, 'calc_unit_hierarchy.csv', 'calc_unit_hierarchy', conn,
                                         ertac_tables.unit_hierarchy_columns, logfile):
        print("Fatal error: could not load necessary file calc_unit_hierarchy", file=sys.stderr)
        sys.exit(1)
    if not ertac_lib.load_csv_into_table(in_prefix, 'calc_generation_proxy.csv', 'calc_generation_proxy', conn,
                                         ertac_tables.generation_proxy_columns, logfile):
        print("Fatal error: could not load necessary file calc_generation_proxy", file=sys.stderr)
        sys.exit(1)
    if not ertac_lib.load_csv_into_table(in_prefix, 'calc_generation_parms_v2.csv', 'calc_generation_parms', conn,
                                         ertac_tables.generation_parms_columns, logfile):
        print("Fatal error: could not load necessary file calc_generation_parms_v2", file=sys.stderr)
        sys.exit(1)
    if not ertac_lib.load_csv_into_table(in_prefix, 'calc_input_variables_v2.csv', 'calc_input_variables', conn,
                                         ertac_tables.input_variable_columns, logfile):
        print("Fatal error: could not load necessary file calc_input_variables_v2", file=sys.stderr)
        sys.exit(1)
    # For V2, added demand transfers.
    ertac_lib.load_csv_into_table(in_prefix, 'calc_demand_transfers.csv', 'calc_demand_transfers', conn,
                                  ertac_tables.demand_transfer_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_demand_transfer_summary.csv', 'calc_demand_transfer_summary', conn,
                                  ertac_tables.demand_transfer_summary_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_control_emissions.csv', 'calc_control_emissions', conn,
                                  ertac_tables.control_emission_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_state_total_listing.csv', 'calc_state_total_listing', conn,
                                  ertac_tables.state_total_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_group_total_listing.csv', 'calc_group_total_listing', conn,
                                  ertac_tables.group_total_columns, logfile)
    # jmj 6/2/2017 adding calc growth rates for error checking purposes
    ertac_lib.load_csv_into_table(in_prefix, 'calc_growth_rates.csv', 'calc_growth_rates', conn,
                                  ertac_tables.growth_rate_columns, logfile)
    # jmj 3/31/2014 moved this to the bottom so that any failures in other files happen first given the length of time to load these
    if not ertac_lib.load_csv_into_table(in_prefix, 'calc_hourly_base.csv', 'calc_hourly_base', conn,
                                         ertac_tables.calc_hourly_columns, logfile):
        print("Fatal error: could not load necessary file calc_hourly_base", file=sys.stderr)
        sys.exit(1)

    # jmj 7/24/2019 - abort code if calc_hourly_base is blank
    (chb_rows,) = conn.execute("""SELECT count(*) FROM calc_hourly_base""").fetchone()
    if chb_rows == 0:
        print(
            "Fatal error: calc_hourly_base contains no data and further processing cannot occur until this error is fixed",
            file=sys.stderr)
        sys.exit(1)

    if ertac_tables.fuel_set != ertac_tables.default_fuel_set:
        logging.info("Default fuel set overwritten.  Using: " + str(ertac_tables.fuel_set))
        print(file=logfile)
        print("Default fuel set overwritten.  Using: " + str(ertac_tables.fuel_set), file=logfile)

    if ertac_tables.state_set != ertac_tables.default_state_set:
        logging.info("Default state set overwritten.  Using: " + str(ertac_tables.state_set))
        print(file=logfile)
        print("Default state set overwritten.  Using: " + str(ertac_tables.state_set), file=logfile)


def assign_generation_all_fuels(conn, base_year, future_year, region, inputvars, logfile):
    """Assign generation (and excess) for all fuel bins for a single region.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year where generation is projected from
    future_year -- the year which is being projected
    region -- the current region being processed
    logfile -- file where logging messages will be written

    """
    for (fuel,) in conn.execute("""SELECT DISTINCT ertac_fuel_unit_type_bin
    FROM calc_generation_parms
    WHERE ertac_region = ?
    ORDER BY ertac_fuel_unit_type_bin""", (region,)).fetchall():

        logging.info(ertac_lib.nice_str((region, fuel)))
        print(ertac_lib.nice_str((region, fuel)), file=logfile)

        # jmj 7/24/2019 check to make sure you have enough hours in the year in calc hourly base for a region fuel so that an error isn't thrown crashing the program
        chb_rows = conn.execute("""SELECT count(ertac_region) 
            FROM calc_hourly_base b
            JOIN calendar_hours c
            ON b.op_date = c.op_date
            AND b.op_hour = c.op_hour
            WHERE ertac_region = ? and ertac_fuel_unit_type_bin = ?
            GROUP BY calendar_hour""", (region, fuel)).fetchall()

        if len(chb_rows) < ertac_lib.hours_in_year(base_year, future_year):
            logging.info("Region/Fuel Unit Type Bin does not have " + str(
                hours_in_fy) + " hours of data available to process (region: " + region + ", fuel: " + fuel + ", hours of data: " + str(
                len(chb_rows)) + ")")
            print("Region/Fuel Unit Type Bin does not have " + str(
                hours_in_fy) + " hours of data available to process (region: " + region + ", fuel: " + fuel + ", hours of data: " + str(
                len(chb_rows)) + ")", file=logfile)
        else:

            # Look up in input variables (for current region, fuel) new unit min/max
            # sizes, demand cushion, 10 facilities for generic units, max UF,
            # deficit_hour (typically 400), optimal load pct, new unit placement
            # pct, new unit EF pct.
            # For V2, add lookup of heat_rate_avg_method.  If not empty, will need to
            # compute set of hourly heat rates instead of single annual rate for units
            # in current region/fuel.  Calculation needs to be done here, before the
            # call to project_hourly() which loops over hours for current region/fuel
            # and calls assign_grown_gen() which uses the heat rate.

            # jmj 11/25/2019 add option to include hizg hours in analysis
            (heat_rate_avg_method, new_unit_max_size, new_unit_min_size, demand_cushion,
             facility_1, facility_2, facility_3, facility_4, facility_5, facility_6,
             facility_7, facility_8, facility_9, facility_10, max_uf, deficit_review_hour,
             optimal_load_pct, new_unit_placement_pct, new_unit_ef_pct, include_hizgs) = conn.execute("""SELECT
            heat_rate_avg_method, new_unit_max_size, new_unit_min_size, demand_cushion,
            facility_1, facility_2, facility_3, facility_4, facility_5, facility_6,
            facility_7, facility_8, facility_9, facility_10, maximum_annual_ertac_uf, capacity_demand_deficit_review,
            unit_optimal_load_threshold_determinant, new_unit_hierarchy_placement_percentile, new_unit_emission_factor_percentile, include_hizgs
            FROM calc_input_variables
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

            if heat_rate_avg_method is not None:
                calculate_heat_rates(conn, region, fuel, heat_rate_avg_method, logfile)

            # jmj 11/26/2019 add the include hizgs variable to inputs to pass it down the functions
            inputvars['include_hizgs'] = (include_hizgs == "TRUE")

            # Facility list is used to locate new generic units.  If supplied list
            # is empty, build one.
            facility_index = 0
            facility_list = [facility_1, facility_2, facility_3, facility_4, facility_5,
                             facility_6, facility_7, facility_8, facility_9, facility_10]
            while None in facility_list:
                facility_list.remove(None)
            if len(facility_list) == 0:
                # Get largest facilities (up to 10) based on total GLOAD.
                facility_totals = conn.execute("""SELECT orispl_code, SUM(gload)
                FROM calc_hourly_base
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                GROUP BY orispl_code
                ORDER BY SUM(gload) DESC, orispl_code""", (region, fuel)).fetchall()
                facility_list = [facility for (facility, gload) in facility_totals][:10]
            if len(facility_list) == 0:
                print("Warning: no available facilities for placement of new generic units for region/fuel:"
                      + ertac_lib.nice_str((region, fuel)), file=logfile)

            # Run the generation assignment algorithm until we don't need to add any
            # more new generic units.
            need_more_units = True
            created_units = 0
            while need_more_units:
                capacity_needed = project_hourly(conn, region, fuel, deficit_review_hour, max_uf, base_year,
                                                 future_year, inputvars, logfile)
                # 3.5 Y
                # jmj 6/2/2017 add the switch to turn off GDU creation
                if capacity_needed > 0.0 and inputvars['add_generic_units']:
                    # 9Y.2, 9Y.3: Need to add units and restart.
                    new_unit_count = add_generic_units(conn, region, fuel, capacity_needed,
                                                       new_unit_max_size, new_unit_min_size, facility_index,
                                                       facility_list,
                                                       max_uf, new_unit_placement_pct, base_year, future_year, logfile)
                    facility_index += new_unit_count
                    created_units += new_unit_count
                    facility_index %= len(facility_list)

                    # jmj 11/27/2020 added a break so that if 10,000 GDUs are created the loop breaks and puts a warning in the log file
                    if created_units > 9999:
                        print(
                            "Warning 10,000 GDUs were created in the following region and GDU loop was exited: " + ertac_lib.nice_str(
                                (region, fuel)), file=logfile)
                        logging.info(
                            "Warning 10,000 GDUs were created in the following region and GDU loop was exited.  See log for details.")
                        need_more_units = False
                else:
                    # 9Y: Reached end of hours without needing more generic units,
                    # so can proceed to handling excess generation pool next.
                    need_more_units = False

            # If generic units were added, log hours where demand exceeded available generation.
            # RW 9/14/2015 Include demand transfer results along with generic units
            # if both occur at same hour.

            (added_capacity,) = conn.execute("""SELECT SUM(new_unit_size)
            FROM generic_units_created
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

            # Python2 you can compare None with float but this is not possible in Python3
            # I am pretty sure we can remove the or and replace with and in this code since None < 1 = True is not valid anymore in python 3
            if added_capacity is not None and added_capacity > 0.0:
                # jmj 5/12/2017 now we just update this with the new generation from new units since
                # the rest of the data got saved earlier on
                conn.execute("""UPDATE demand_generation_deficit
                SET generation_after_new_units = generation_available + ?
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?""", (added_capacity, region, fuel))

            flag_negative_demand_transfers(conn, region, fuel, logfile)

            # 10: Allocate any excess generation pool up to optimal or maximal levels.
            allocate_excess_generation(conn, region, fuel, max_uf, base_year, future_year, logfile)


def calculate_heat_rates(conn, region, fuel, heat_rate_avg_method, logfile):
    """Calculate hour-specific heat rates, on an hourly, daily, monthly ... basis.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel being processed
    heat_rate_avg_method -- the desired time period size for calculating heat rates
    logfile -- file where logging messages will be written

    """
    # Driver for V2 calculation of heat rates on time scales from hourly up to
    # annual.  Same approach can also be used for emission rates, with different
    # data columns for inputs and lower/upper limits.
    calculate_rates(conn, region, fuel, heat_rate_avg_method,
                    'heat_input', 'gload', 'NULL', 1000.0,
                    'heat_rate_lower_limit', 'heat_rate_upper_limit',
                    'heat_rate_lower_stat', 'heat_rate_upper_stat',
                    'heat_rate_avg', 'heat_rate_os_avg', 'heat_rate_nonos_avg', logfile)


def calculate_nox_rates(conn, region, fuel, nox_avg_method, logfile):
    # For V2, using same methods as for heat rate calculation.
    # Results will be copied into pollutant-specific output table.
    calculate_rates(conn, region, fuel, nox_avg_method,
                    'nox_mass', 'heat_input', 'nox_rate', 1.0,
                    'nox_ef_lower_limit', 'nox_ef_upper_limit',
                    'nox_ef_lower_stat', 'nox_ef_upper_stat',
                    'nox_ef_avg', 'nox_ef_os_avg', 'nox_ef_nonos_avg', logfile)

    conn.executescript("""DROP TABLE IF EXISTS nox_hourly_rates;

    CREATE TEMPORARY TABLE nox_hourly_rates
    (region TEXT NOT NULL COLLATE NOCASE,
    fuel TEXT NOT NULL COLLATE NOCASE,
    plant TEXT NOT NULL COLLATE NOCASE,
    unit TEXT NOT NULL COLLATE NOCASE,
    calendar_hour INTEGER NOT NULL,
    calc_rate REAL,
    rate_type TEXT,
    rate_limit_flag TEXT,
    PRIMARY KEY (region, fuel, plant, unit, calendar_hour));

    INSERT INTO nox_hourly_rates
    SELECT region, fuel, plant, unit, calendar_hour,
    calc_rate, rate_type, rate_limit_flag
    FROM hourly_rates;""")


def calculate_so2_rates(conn, region, fuel, so2_avg_method, logfile):
    # For V2, using same methods as for heat rate calculation.
    # Results will be copied into pollutant-specific output table.
    calculate_rates(conn, region, fuel, so2_avg_method,
                    'so2_mass', 'heat_input', 'so2_rate', 1.0,
                    'so2_ef_lower_limit', 'so2_ef_upper_limit',
                    'so2_ef_lower_stat', 'so2_ef_upper_stat',
                    'so2_ef_avg', 'so2_ef_os_avg', 'so2_ef_nonos_avg', logfile)

    conn.executescript("""DROP TABLE IF EXISTS so2_hourly_rates;

    CREATE TEMPORARY TABLE so2_hourly_rates
    (region TEXT NOT NULL COLLATE NOCASE,
    fuel TEXT NOT NULL COLLATE NOCASE,
    plant TEXT NOT NULL COLLATE NOCASE,
    unit TEXT NOT NULL COLLATE NOCASE,
    calendar_hour INTEGER NOT NULL,
    calc_rate REAL,
    rate_type TEXT,
    rate_limit_flag TEXT,
    PRIMARY KEY (region, fuel, plant, unit, calendar_hour));

    INSERT INTO so2_hourly_rates
    SELECT region, fuel, plant, unit, calendar_hour,
    calc_rate, rate_type, rate_limit_flag
    FROM hourly_rates;""")


def calculate_rates(conn, region, fuel, avg_method,
                    numer_col, denom_col, rate_col, scale_factor,
                    hard_lower_limit_col, hard_upper_limit_col,
                    stat_lower_limit_col, stat_upper_limit_col,
                    annual_avg_col, os_avg_col, nonos_avg_col, logfile):
    """Calculate heat or emission rates, subject to lower/upper limits, with fallback to larger time intervals.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel being processed
    avg_method -- the desired time period size
    numer_col, denom_col -- the names of the numerator and denominator data columns in base year table
    rate_col -- the name of the input base year hourly rate column, or NULL if not reading rate column
    scale_factor -- multiplier to adjust numerator/denominator ratio for measurement units
    hard_lower_limit_col, hard_upper_limit_col -- names of hard lower/upper limit columns in UAF
    stat_lower_limit_col, stat_upper_limit_col -- names of statistical lower/upper limit columns in UAF
    annual_avg_col -- name of annual average rate column in UAF
    os_avg_col, nonos_avg_col -- names of OS and non-OS seasonal average rate columns in UAF
    logfile -- file where logging messages will be written

    """
    # Calculate heat rates or emission rates for time periods ranging from hours
    # up to years, applying optional limits and flagging values which had to be
    # replaced.
    # For heat rates only, need to divide heat_input / gload to get single-hour
    # heat rate; hourly emission rates are already included in the input.
    # For heat rates only, have to scale by factor of 1000.0 to get desired
    # measurement units (btu/kw-hr).
    # For seasonal (OS and non-OS) and annual rates, averages have already been
    # computed and stored in UAF, so no other calculation is needed here.
    # At end of this routine, the hourly_rates table will hold rates and flags
    # to be copied by calling routine into appropriate columns of
    # hourly_diagnostic_file.

    # Outline:
    # Copy base-year and UAF data for current region+fuel, and determine effective
    # limits from hard and statistical limits, which each may be present or absent.
    # Walk up list of increasing time intervals - do following at each one:
    #     Determine candidate rates at current interval size
    #     Copy rates into hourly_rate table where no value already exists
    #     Flag out-of-range values, to fall back to larger interval

    copy_hourly_subset(conn, region, fuel, numer_col, denom_col, rate_col, logfile)

    copy_uaf_limits(conn, region, fuel,
                    hard_lower_limit_col, hard_upper_limit_col,
                    stat_lower_limit_col, stat_upper_limit_col,
                    annual_avg_col, os_avg_col, nonos_avg_col, logfile)

    avg_method = avg_method.upper()
    need_fallback = False

    # Start at desired avg_method, skipping over smaller intervals, with fallback
    # to larger intervals ending with annual average.

    if avg_method != "HOURLY":
        # If not using single-hour rates, clear values so code for longer time
        # periods will calculate and fill in average rates.
        conn.execute("""UPDATE hourly_rates
        SET calc_rate = NULL
        WHERE calc_rate IS NOT NULL""")

    if avg_method == "HOURLY":
        # If no hourly rate was copied in, calculate directly now.
        if rate_col == "NULL":
            conn.execute("""UPDATE hourly_rates
            SET calc_rate = ? * numer_val / denom_val
            WHERE numer_val > 0.0
            AND denom_val > 0.0
            AND calc_rate IS NULL""", (scale_factor,))

        # Mark all calculated or copied hourly rates.
        conn.execute("""UPDATE hourly_rates
        SET rate_type = 'H'
        WHERE calc_rate IS NOT NULL""")

        flag_rate_limits(conn, "H", logfile)

        need_fallback = conn.execute("""SELECT 1 WHERE EXISTS
        (SELECT 1 FROM hourly_rates WHERE calc_rate IS NULL)""").fetchone()

    if (avg_method == "DAILY"
            or (avg_method == "HOURLY" and need_fallback)):
        # Compute daily totals, then average rates.  Fill in hourly_rates from
        # avg_rates where needed, then flag out-of-range values and fall back.
        calculate_average_rates(conn, "m_d", scale_factor, "D", logfile)

        flag_rate_limits(conn, "D", logfile)

        need_fallback = conn.execute("""SELECT 1 WHERE EXISTS
        (SELECT 1 FROM hourly_rates WHERE calc_rate IS NULL)""").fetchone()

    if (avg_method == "MONTHLY"
            or (avg_method in ("HOURLY", "DAILY") and need_fallback)):
        # Now compute whole-month totals and averages if needed.
        calculate_average_rates(conn, "mon", scale_factor, "M", logfile)

        flag_rate_limits(conn, "M", logfile)

        need_fallback = conn.execute("""SELECT 1 WHERE EXISTS
        (SELECT 1 FROM hourly_rates WHERE calc_rate IS NULL)""").fetchone()

    if (avg_method == "QUARTERLY"
            or (avg_method in ("HOURLY", "DAILY", "MONTHLY") and need_fallback)):
        # Now compute quarterly totals and averages if needed.
        calculate_average_rates(conn, "qtr", scale_factor, "Q", logfile)

        flag_rate_limits(conn, "Q", logfile)

        need_fallback = conn.execute("""SELECT 1 WHERE EXISTS
        (SELECT 1 FROM hourly_rates WHERE calc_rate IS NULL)""").fetchone()

    if avg_method == "OS/NON-OS":
        # Quarterly fallback skips past OS and non-OS, uses annual average as
        # final fallback option.  OS and non-OS averages were already calculated
        # in UAF and copied into unit_limits table, so fill hourly_rates where
        # needed and fall back if any rates are missing or out of range.
        conn.executescript("""UPDATE hourly_rates
        SET calc_rate = (SELECT u.rate_os_avg
            FROM unit_limits u
            WHERE u.region = hourly_rates.region
            AND u.fuel = hourly_rates.fuel
            AND u.plant = hourly_rates.plant
            AND u.unit = hourly_rates.unit),
        rate_type = 'O'
        WHERE o_n = 'O'
        AND calc_rate IS NULL;

        UPDATE hourly_rates
        SET calc_rate = (SELECT u.rate_nonos_avg
            FROM unit_limits u
            WHERE u.region = hourly_rates.region
            AND u.fuel = hourly_rates.fuel
            AND u.plant = hourly_rates.plant
            AND u.unit = hourly_rates.unit),
        rate_type = 'N'
        WHERE o_n = 'N'
        AND calc_rate IS NULL;""")

        flag_rate_limits(conn, "O", logfile)
        flag_rate_limits(conn, "N", logfile)

        need_fallback = conn.execute("""SELECT 1 WHERE EXISTS
        (SELECT 1 FROM hourly_rates WHERE calc_rate IS NULL)""").fetchone()

    if avg_method == "ANNUAL" or need_fallback:
        # Annual averages were already calculated in UAF and copied into unit_limits
        # table, so fill hourly_rates where needed.  Don't flag and fall back if
        # annual average is out of range, because there is no other rate to use.
        conn.execute("""UPDATE hourly_rates
        SET calc_rate = (SELECT u.rate_annual_avg
            FROM unit_limits u
            WHERE u.region = hourly_rates.region
            AND u.fuel = hourly_rates.fuel
            AND u.plant = hourly_rates.plant
            AND u.unit = hourly_rates.unit),
        rate_type = 'A'
        WHERE calc_rate IS NULL""")


def copy_hourly_subset(conn, region, fuel, numer_col, denom_col, rate_col, logfile):
    """Copy subset of base-year hourly data for average rate calculations.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel being processed
    numer_col, denom_col -- the names of the numerator and denominator data columns in base year table
    rate_col -- the name of the input base year hourly rate column, or NULL if not reading rate column
    logfile -- file where logging messages will be written

    """
    # Make and fill table with copied hourly rate data for current region/fuel,
    # together with grouping columns to allow aggregation of hourly values up to
    # daily, monthly, ... levels as needed.
    conn.executescript("""DROP TABLE IF EXISTS hourly_rates;

    CREATE TEMPORARY TABLE hourly_rates
    (region TEXT NOT NULL COLLATE NOCASE,
    fuel TEXT NOT NULL COLLATE NOCASE,
    plant TEXT NOT NULL COLLATE NOCASE,
    unit TEXT NOT NULL COLLATE NOCASE,
    calendar_hour INTEGER NOT NULL,
    m_d TEXT,
    mon TEXT,
    qtr TEXT,
    o_n TEXT,
    numer_val REAL,
    denom_val REAL,
    calc_rate REAL,
    rate_type TEXT,
    rate_limit_flag TEXT,
    PRIMARY KEY (region, fuel, plant, unit, calendar_hour));""")

    # Have to build SQL string dynamically because column names can't be
    # parameters.
    conn.execute("""INSERT INTO hourly_rates (region, fuel, plant, unit,
    calendar_hour, m_d, mon, qtr, o_n,
    numer_val, denom_val, calc_rate)
    SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid,
    calendar_hour, m_d, mon, qtr, o_n,
    """ + ', '.join([numer_col, denom_col, rate_col]) + """
    FROM calc_hourly_base b
    JOIN calendar_hours c
    ON b.op_date = c.op_date
    AND b.op_hour = c.op_hour
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?""", (region, fuel))


def copy_uaf_limits(conn, region, fuel,
                    hard_lower_limit_col, hard_upper_limit_col,
                    stat_lower_limit_col, stat_upper_limit_col,
                    annual_avg_col, os_avg_col, nonos_avg_col, logfile):
    """Copy hard and statistical rate limits from UAF along with annual and seasonal average rates.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel being processed
    hard_lower_limit_col, hard_upper_limit_col -- names of hard lower/upper limit columns in UAF
    stat_lower_limit_col, stat_upper_limit_col -- names of statistical lower/upper limit columns in UAF
    annual_avg_col -- name of annual average rate column in UAF
    os_avg_col, nonos_avg_col -- names of OS and non-OS seasonal average rate columns in UAF
    logfile -- file where logging messages will be written

    """
    # Create and fill table with copied limits and rates from UAF, and set the
    # effective limits based on hard and/or statistical limits if present.
    conn.executescript("""DROP TABLE IF EXISTS unit_limits;

    CREATE TEMPORARY TABLE unit_limits
    (region TEXT NOT NULL COLLATE NOCASE,
    fuel TEXT NOT NULL COLLATE NOCASE,
    plant TEXT NOT NULL COLLATE NOCASE,
    unit TEXT NOT NULL COLLATE NOCASE,
    hard_lower_limit REAL,
    hard_upper_limit REAL,
    stat_lower_limit REAL,
    stat_upper_limit REAL,
    effective_lower_limit REAL,
    effective_upper_limit REAL,
    rate_annual_avg REAL,
    rate_os_avg REAL,
    rate_nonos_avg REAL,
    PRIMARY KEY (region, fuel, plant, unit));""")

    # Have to build SQL string dynamically because column names can't be
    # parameters.
    conn.execute("""INSERT INTO unit_limits (region, fuel, plant, unit,
    hard_lower_limit, hard_upper_limit,
    stat_lower_limit, stat_upper_limit,
    rate_annual_avg, rate_os_avg, rate_nonos_avg)
    SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid,
    """ + ', '.join([hard_lower_limit_col, hard_upper_limit_col,
                     stat_lower_limit_col, stat_upper_limit_col,
                     annual_avg_col, os_avg_col, nonos_avg_col]) + """
    FROM calc_updated_uaf
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?""", (region, fuel))

    # Hard limits or statistical limits can each be NULL.  If neither hard nor
    # statistical limit is present, use dummy value.  If both types exist,
    # choose the more restrictive value.
    conn.executescript("""UPDATE unit_limits
    SET effective_lower_limit = COALESCE(hard_lower_limit, stat_lower_limit, -1.0e30),
    effective_upper_limit = COALESCE(hard_upper_limit, stat_upper_limit, 1.0e30);

    UPDATE unit_limits
    SET effective_lower_limit = stat_lower_limit
    WHERE stat_lower_limit > hard_lower_limit;

    UPDATE unit_limits
    SET effective_upper_limit = stat_upper_limit
    WHERE stat_upper_limit < hard_upper_limit;""")


def calculate_average_rates(conn, period_col, scale_factor, rate_type, logfile):
    """Calculate average rates over specified time period and copy results into hourly_rates table.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    period_col -- name of column to group hourly data into longer period
    scale_factor -- multiplier to adjust numerator/denominator ratio for measurement units
    rate_type -- marker character for type of rate period
    logfile -- file where logging messages will be written

    """
    # Make average rate table
    # Sum numerator and denominator columns grouping by period_col
    # Divide and scale
    # Copy into hourly_rates
    conn.executescript("""DROP TABLE IF EXISTS avg_rates;

    CREATE TEMPORARY TABLE avg_rates
    (region TEXT NOT NULL COLLATE NOCASE,
    fuel TEXT NOT NULL COLLATE NOCASE,
    plant TEXT NOT NULL COLLATE NOCASE,
    unit TEXT NOT NULL COLLATE NOCASE,
    time_period TEXT NOT NULL COLLATE NOCASE,
    total_numer REAL,
    total_denom REAL,
    avg_rate REAL,
    PRIMARY KEY (region, fuel, plant, unit, time_period));""")

    conn.execute("""INSERT INTO avg_rates (region, fuel, plant, unit,
    time_period, total_numer, total_denom)
    SELECT region, fuel, plant, unit,
    """ + period_col + """ AS time_period,
    SUM(numer_val) AS total_numer, SUM(denom_val) AS total_denom
    FROM hourly_rates
    GROUP BY region, fuel, plant, unit, time_period""")

    conn.execute("""UPDATE avg_rates
    SET avg_rate = ? * total_numer / total_denom
    WHERE total_numer > 0.0
    AND total_denom > 0.0""", (scale_factor,))

    conn.execute("""UPDATE hourly_rates
    SET calc_rate = (SELECT a.avg_rate
        FROM avg_rates a
        WHERE a.region = hourly_rates.region
        AND a.fuel = hourly_rates.fuel
        AND a.plant = hourly_rates.plant
        AND a.unit = hourly_rates.unit
        AND a.time_period = hourly_rates.""" + period_col + """),
    rate_type = ?
    WHERE calc_rate IS NULL;""", (rate_type,))


def flag_rate_limits(conn, rate_type, logfile):
    """Erase and flag any rates of specified type that fall outside of allowed ranges.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    rate_type -- marker character for type of rate period
    logfile -- file where logging messages will be written

    """
    # Flag values outside of limits, but don't change any existing flag from earlier pass.
    conn.execute("""UPDATE hourly_rates
    SET calc_rate = NULL,
    rate_type = NULL,
    rate_limit_flag = COALESCE(rate_limit_flag, 'L')
    WHERE calc_rate IS NOT NULL
    AND rate_type = ?
    AND calc_rate < (SELECT u.effective_lower_limit
        FROM unit_limits u
        WHERE u.region = hourly_rates.region
        AND u.fuel = hourly_rates.fuel
        AND u.plant = hourly_rates.plant
        AND u.unit = hourly_rates.unit)""", (rate_type,))

    conn.execute("""UPDATE hourly_rates
    SET calc_rate = NULL,
    rate_type = NULL,
    rate_limit_flag = COALESCE(rate_limit_flag, 'U')
    WHERE calc_rate IS NOT NULL
    AND rate_type = ?
    AND calc_rate > (SELECT u.effective_upper_limit
        FROM unit_limits u
        WHERE u.region = hourly_rates.region
        AND u.fuel = hourly_rates.fuel
        AND u.plant = hourly_rates.plant
        AND u.unit = hourly_rates.unit)""", (rate_type,))


def project_hourly(conn, region, fuel, deficit_review_hour, max_uf, base_year, future_year, inputvars, logfile):
    """Project hourly generation as outlined on page 2 of the block diagram.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel bin being processed
    deficit_review_hour -- the time when the processing loop decides whether more units are required
    max_uf -- default maximum utilization fraction for any unit in this region/fuel
    base_year -- the base year where generation is projected from
    future_year -- the year which is being projected
    logfile -- file where logging messages will be written

    """
    # 20120320 For changed test at 3.5Y, need to have total of maximum
    # generation capacity from all available units in future year.
    (max_gen_capacity,) = conn.execute("""SELECT SUM(1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate)
    FROM calc_updated_uaf uaf
    JOIN calc_unit_hierarchy hier
    ON uaf.ertac_region = hier.ertac_region
    AND uaf.ertac_fuel_unit_type_bin = hier.ertac_fuel_unit_type_bin
    AND uaf.orispl_code = hier.orispl_code
    AND uaf.unitid = hier.unitid
    WHERE uaf.ertac_region = ?
    AND uaf.ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
    if max_gen_capacity is None:
        max_gen_capacity = 0.0
    max_future_generation = 0.0
    # jmj 3/10/2017 added this variable to make sure another hour later in the process isn't higher
    max_unaccounted_excess_generation = 0
    max_unaccounted_excess_generation_hierarchy_hour = 0
    deficit_review_hour_generation_deficit = 0
    deficit_review_hour_generation_hierarchy_hour = 0

    # 2
    # RW 9/18/2015 Instead of updating hourly growth rates in preprocessor, add
    # the effects of demand transfers here after accounting for proxy generation,
    # in order to update AFYGR before generation loads are assigned, and to
    # determine any excess generation.
    for (date, hour, hierarchy_hour, future_projected_generation, net_demand_transfer) in conn.execute("""SELECT op_date,
    op_hour, temporal_allocation_order, future_projected_generation, net_demand_transfer
    FROM calc_generation_parms
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?
    ORDER BY temporal_allocation_order""", (region, fuel)).fetchall():
        if future_projected_generation is None:
            future_projected_generation = 0.0

        # jmj 7/14/2017 the max future generation calculation wasn't considering transfers
        if max_future_generation > future_projected_generation + net_demand_transfer:
            max_future_generation = future_projected_generation + net_demand_transfer
            deficit_review_hour_generation_hierarchy_hour = hierarchy_hour
        # 3: Calculate TotalProxy, AFYGrowth, AFYGR for current region, fuel, date, hour.
        (total_proxy,) = conn.execute("""SELECT SUM(gload_proxy)
        FROM calc_generation_proxy
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND op_date = ?
        AND op_hour = ?""", (region, fuel, date, hour)).fetchone()
        if total_proxy is None:
            total_proxy = 0.0

        conn.execute("""UPDATE calc_generation_parms
        SET total_proxy_generation = ?,
        adjusted_projected_generation = MAX(future_projected_generation + net_demand_transfer - ?, 0.0),
        afygr = CASE WHEN base_actual_generation > base_retired_generation
                     THEN MAX(future_projected_generation + net_demand_transfer - ?, 0.0) / (base_actual_generation - base_retired_generation)
                     ELSE 0.0 END
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND op_date = ?
        AND op_hour = ?""", (total_proxy, total_proxy, total_proxy, region, fuel, date, hour))

        # 3.5:
        if hierarchy_hour == deficit_review_hour:
            # 3.5Y: 20120320 Changed test to be based on maximum hourly
            # generation capacity instead of largest excess generation pool.

            # jmj 5/12/2017 recoded deficit review hour code to project the rest of the hours
            # before returnin the function to get a GDU so that we can get an accurate estimate of
            # when deficits occur before GDU creation
            # 3.5Ya
            if max_future_generation > max_gen_capacity:
                # 3.5YaY.1
                deficit_review_hour_generation_deficit = max_future_generation - max_gen_capacity
        # 4
        (afygr, calendar_hour) = conn.execute("""SELECT afygr, calendar_hour
        FROM calc_generation_parms
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND op_date = ?
        AND op_hour = ?""", (region, fuel, date, hour)).fetchone()

        # 4N.1, 4.2
        assign_proxy_gen(conn, region, fuel, date, hour, calendar_hour, hierarchy_hour, max_uf, base_year, future_year,
                         logfile)
        # 5
        assign_grown_gen(conn, region, fuel, date, hour, calendar_hour, hierarchy_hour, afygr, max_uf, base_year,
                         future_year, inputvars, logfile)

        # Did any new or existing unit hit a limit at this hour, leaving excess
        # generation?
        # 8
        (assigned_gen,) = conn.execute("""SELECT SUM(gload)
        FROM hourly_diagnostic_file
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND hierarchy_hour = ?""", (region, fuel, hierarchy_hour)).fetchone()

        if assigned_gen is None:
            assigned_gen = 0.0
        if future_projected_generation + net_demand_transfer > assigned_gen:
            excess_generation_pool = future_projected_generation + net_demand_transfer - assigned_gen

            # jmj 03/08/2017 - determine if any units are capacity limited and subtract their capacity
            # note retired units are likely not needed to be included because they are not included
            # the hierarchy and are excluded before the for loop starts here
            # 8.1
            (future_date,) = conn.execute("""SELECT future_date
            FROM calendar_hours
            WHERE calendar_hour = ?""", (calendar_hour,)).fetchone()

            (available_capacity,) = conn.execute("""SELECT SUM(CASE WHEN 1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate > uaf.unit_max_optimal_load_threshold THEN 1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate ELSE uaf.unit_max_optimal_load_threshold END)
                FROM calc_updated_uaf uaf            

                JOIN hourly_diagnostic_file hourly
                ON hourly.orispl_code = uaf.orispl_code
                AND hourly.unitid = uaf.unitid
                AND hourly.ertac_fuel_unit_type_bin = uaf.ertac_fuel_unit_type_bin

                WHERE (hourly.annual_hi_limit = 'N' AND hourly.annual_oh_limit = 'N')
                AND hourly.ertac_region = ?
                AND hourly.ertac_fuel_unit_type_bin = ?
                AND hourly.hierarchy_hour = ?
                AND online_start_date <= ? 
                AND offline_start_date >= ? """, (region, fuel, hierarchy_hour, future_date, future_date)).fetchone()

            if available_capacity is None:
                available_capacity = 0
            if available_capacity - assigned_gen < excess_generation_pool and max_unaccounted_excess_generation < excess_generation_pool - (
                    available_capacity - assigned_gen):
                max_unaccounted_excess_generation = excess_generation_pool - (available_capacity - assigned_gen)
                max_unaccounted_excess_generation_hierarchy_hour = hierarchy_hour
        else:
            excess_generation_pool = 0.0

        conn.execute("""UPDATE calc_generation_parms
        SET excess_generation_pool = ?
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND temporal_allocation_order = ?""",
                     (excess_generation_pool, region, fuel, hierarchy_hour))

    # jmj 5/12/2017 recoded deficit review hour code to project the rest of the hours
    # before returnin the function to get a GDU so that we can get an accurate estimate of
    # when deficits occur before GDU creation
    if deficit_review_hour_generation_deficit > 0:
        # 9Y.1
        log_deficit_hours(conn, region, fuel, logfile)
        if inputvars['add_generic_units']:
            conn.execute("""DELETE FROM hourly_diagnostic_file
                    WHERE ertac_region = ?
                    AND ertac_fuel_unit_type_bin = ?""", (region, fuel))
        print("Hiearchy Hour: " + str(
            deficit_review_hour_generation_hierarchy_hour) + " needed capacity during the deficit review of " + str(
            deficit_review_hour_generation_deficit), file=logfile)
        return deficit_review_hour_generation_deficit

    # jmj 3/10/2017 we do need a generic unit to deal with excess generation
    if max_unaccounted_excess_generation > 0:
        # 9Y.1: Early return to add generic units and restart.
        log_deficit_hours(conn, region, fuel, logfile)
        if inputvars['add_generic_units']:
            conn.execute("""DELETE FROM hourly_diagnostic_file
                    WHERE ertac_region = ?
                    AND ertac_fuel_unit_type_bin = ?""", (region, fuel))

        print("Hiearchy Hour: " + str(
            max_unaccounted_excess_generation_hierarchy_hour) + " triggered a max_unaccounted_excess_generation of " + str(
            max_unaccounted_excess_generation), file=logfile)
        return max_unaccounted_excess_generation

    # If we didn't take the early exit to add generic units and restart the
    # process, and therefore have finished projecting all the hours for this
    # region/fuel, return 0 to avoid triggering new unit creation after the
    # deficit review hour.
    return 0.0


def log_deficit_hours(conn, region, fuel, logfile):
    """Log hours that did not have enough capacity to meet generation.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel bin being processed
    logfile -- file where logging messages will be written
    """
    # If generic units were added, log hours where demand exceeded available generation.
    # RW 9/14/2015 Include demand transfer results along with generic units
    # if both occur at same hour.

    # 5/12/2017 jmj we are moving this code so that is occurs earlier in the process
    # since the lacking calculation was inaccurate in the case that GDUs were created as
    # the result of the excess generation pool not being able to be met due to excess curtailment

    (added_capacity,) = conn.execute("""SELECT SUM(new_unit_size)
    FROM generic_units_created
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

    # 5/12/2017 jmj we now want to change this check because it should only run before a GDU has been created
    # once one gets created it throws off the lacking calculation
    if added_capacity is None or added_capacity == 0.0:

        # jmj 4/13/2017 rewrote this algorithm to check every hour since capacity could change from hour to hour due to the
        # disappearing generation bug
        for (
                date, hour, calendar_hour, hierarchy_hour, future_projected_generation,
                net_demand_transfer) in conn.execute("""SELECT op_date,
        op_hour, calendar_hour, temporal_allocation_order, future_projected_generation, net_demand_transfer
        FROM calc_generation_parms
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        ORDER BY temporal_allocation_order""", (region, fuel)).fetchall():

            (future_date,) = conn.execute("""SELECT future_date
            FROM calendar_hours
            WHERE calendar_hour = ?""", (calendar_hour,)).fetchone()

            (available_capacity,) = conn.execute("""SELECT SUM(CASE WHEN hourly.annual_hi_limit = 'N' AND hourly.annual_oh_limit = 'N' THEN (CASE WHEN 1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate > uaf.unit_max_optimal_load_threshold THEN 1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate ELSE uaf.unit_max_optimal_load_threshold END) ELSE COALESCE(hourly.gload, 0) END)
                FROM calc_updated_uaf uaf            

                JOIN hourly_diagnostic_file hourly
                ON hourly.orispl_code = uaf.orispl_code
                AND hourly.unitid = uaf.unitid
                AND hourly.ertac_fuel_unit_type_bin = uaf.ertac_fuel_unit_type_bin
                
                WHERE hourly.ertac_region = ?
                AND hourly.ertac_fuel_unit_type_bin = ?
                AND hourly.hierarchy_hour = ?
                AND online_start_date <= ? 
                AND offline_start_date >= ? """, (region, fuel, hierarchy_hour, future_date, future_date)).fetchone()

            if available_capacity is None:
                available_capacity = 0

            if future_projected_generation + net_demand_transfer > available_capacity:
                conn.execute("""INSERT INTO demand_generation_deficit
                (ertac_region, ertac_fuel_unit_type_bin, calendar_hour, hierarchy_hour,
                generation_needed, generation_due_to_demand_transfer,
                total_generation_needed, generation_available, generation_after_new_units,
                generation_lacking, deficit_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                             (region, fuel, calendar_hour, hierarchy_hour,
                              future_projected_generation, net_demand_transfer,
                              future_projected_generation + net_demand_transfer, available_capacity, available_capacity,
                              future_projected_generation + net_demand_transfer - available_capacity, 'D'))
            else:
                conn.execute("""INSERT INTO demand_generation_deficit
                (ertac_region, ertac_fuel_unit_type_bin, calendar_hour, hierarchy_hour,
                generation_needed, generation_due_to_demand_transfer,
                total_generation_needed, generation_available, generation_after_new_units,
                generation_lacking, deficit_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                             (region, fuel, calendar_hour, hierarchy_hour,
                              future_projected_generation, net_demand_transfer,
                              future_projected_generation + net_demand_transfer, available_capacity, available_capacity,
                              0, ''))


def assign_proxy_gen(conn, region, fuel, date, hour, calendar_hour, hierarchy_hour, max_uf, base_year, future_year,
                     logfile):
    """Assign proxy generation to all new units, subject to operating limits.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel bin being processed
    date -- the current date (string)
    hour -- the current hour (0-23)
    calendar hour -- the hour number since midnight January 1 (1-8760)
    hierarchy_hour -- the rank number of the hour, from high load to low (1-8760)
    max_uf -- default maximum utilization fraction for any unit in this region/fuel
    future_year -- the year which is being projected
    logfile -- file where logging messages will be written

    """
    # Assign proxy generation in the hourly_diagnostic_file based on
    # calc_generation_proxy, subject to hourly HI and annual UF limits.

    # jmj 10/22/2013 commenting out the original sql draw to get information about future gen and total proxy
    # for (state, plant, unit, gload) in conn.execute("""SELECT state, orispl_code, unitid, gload_proxy
    for (state, plant, unit, gload, future_gen, total_proxy) in conn.execute("""SELECT state, prox.orispl_code, prox.unitid, gload_proxy, future_projected_generation, total_proxy_generation
    FROM calc_generation_proxy AS prox
    LEFT JOIN calc_generation_parms AS parms
    ON prox.ertac_region = parms.ertac_region
    AND prox.ertac_fuel_unit_type_bin = parms.ertac_fuel_unit_type_bin
    AND prox.op_date = parms.op_date
    AND prox.op_hour = parms.op_hour
    WHERE prox.ertac_region = ?
    AND prox.ertac_fuel_unit_type_bin = ?
    AND prox.op_date = ?
    AND prox.op_hour = ?""", (region, fuel, date, hour)).fetchall():

        if gload is None:
            gload = 0.0

        # jmj 10/22/2013 apply a percent reduction to the gross load if the proxy generation is higher than the future generation needed
        if total_proxy > 0 and future_gen < total_proxy:
            gload = round(gload * future_gen / total_proxy, 12)

        if hierarchy_hour > 1:
            # Get previous hour's running totals.
            (cumulative_hi, cumulative_gen, cumulative_op_hours) = conn.execute("""SELECT cumulative_hi, cumulative_gen, cumulative_op_hours
            FROM hourly_diagnostic_file
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (region, fuel, plant, unit, hierarchy_hour - 1)).fetchone()
        else:
            cumulative_hi = cumulative_gen = cumulative_op_hours = 0.0

        (unit_max_hi, unit_max_uf, unit_max_gload, unit_heat_rate, hours_cap) = conn.execute("""SELECT max_ertac_hi_hourly_summer,
        COALESCE(unit_annual_capacity_limit, max_annual_ertac_uf),
        max_by_hourly_gload, ertac_heat_rate, hours_cap
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

        if unit_max_uf is None:
            unit_max_uf = max_uf

        if unit_heat_rate is not None and gload is not None and gload > 0.0:
            heat_input = unit_heat_rate * gload / 1000.0
        else:
            heat_input = 0.0

        # 6, 7
        # jmj 9/4/2019 correct for max heat input check (was > should be >=)
        if unit_max_hi is not None and heat_input >= unit_max_hi:
            hourly_hi_limit = 'Y'
            heat_input = unit_max_hi
            gload = round(heat_input * 1000.0 / unit_heat_rate, 12)
        else:
            hourly_hi_limit = 'N'

        if unit_max_hi is not None and unit_max_uf is not None:
            unit_annual_hi_limit_value = ertac_lib.hours_in_year(base_year, future_year) * unit_max_hi * unit_max_uf
            if cumulative_hi + heat_input > unit_annual_hi_limit_value:
                annual_hi_limit = 'Y'
                heat_input = unit_annual_hi_limit_value - cumulative_hi
                gload = round(heat_input * 1000.0 / unit_heat_rate, 12)
            else:
                annual_hi_limit = 'N'
        else:
            annual_hi_limit = 'N'

        if gload > 0.0:
            cumulative_op_hours += 1.0

        # jmj 3/9/2017 add the code to limit by operating hours too
        if hours_cap is not None and cumulative_op_hours > hours_cap:
            cumulative_op_hours -= 1.0
            annual_oh_limit = 'Y'
            heat_input = 0
            gload = 0
        else:
            annual_oh_limit = 'N'

        conn.execute("""INSERT INTO hourly_diagnostic_file
        (ertac_region, ertac_fuel_unit_type_bin, state, orispl_code, unitid,
        calendar_hour, hierarchy_hour, hourly_hi_limit, annual_hi_limit, annual_oh_limit, 
        cumulative_hi, cumulative_gen, cumulative_op_hours, gload, heat_input, heat_rate, generation_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                     (region, fuel, state, plant, unit,
                      calendar_hour, hierarchy_hour, hourly_hi_limit, annual_hi_limit, annual_oh_limit,
                      cumulative_hi + heat_input, cumulative_gen + gload, cumulative_op_hours, gload, heat_input,
                      unit_heat_rate, 'P'))


def assign_grown_gen(conn, region, fuel, date, hour, calendar_hour, hierarchy_hour, afygr, max_uf, base_year,
                     future_year, inputvars, logfile):
    """Assign grown generation to all existing units, subject to operating limits.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel bin being processed
    date -- the current date (string)
    hour -- the current hour (0-23)
    calendar hour -- the hour number since midnight January 1 (1-8760)
    hierarchy_hour -- the rank number of the hour, from high load to low (1-8760)
    afygr -- the adjusted future year growth rate to be applied to base year generation
    max_uf -- default maximum utilization fraction for any unit in this region/fuel
    base_year -- the base year where generation is projected from
    future_year -- the year which is being projected
    logfile -- file where logging messages will be written

    """
    # Assign grown generation in the hourly_diagnostic_file based on
    # calc_hourly_base, subject to hourly HI and annual UF limits.
    # 20120302 Need to handle retired or capacity-limited units complementary to
    # the fill_base_year_calc_generation_parms routine in the preprocessor.

    # For V2, need to look up heat_rate_avg_method to see if we'll be overriding
    # the original V1 behavior.
    (heat_rate_avg_method,) = conn.execute("""SELECT heat_rate_avg_method
    FROM calc_input_variables
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

    if inputvars['include_hizgs']:
        query_bindings = (base_year, future_year, base_year, future_year, region, fuel, date, hour)
    else:
        query_bindings = (base_year, future_year, region, fuel, date, hour)

    for (state, plant, unit, hizg_hi, gload) in conn.execute(
            """SELECT hourly.state, hourly.orispl_code, hourly.unitid, """ +
            ("""CASE WHEN COALESCE(gload, 0) = 0
    AND REPLACE(hourly.op_date, ?, ?) < uaf.offline_start_date
    THEN hourly.heat_input
    ELSE NULL END,""" if inputvars['include_hizgs'] else """NULL,""") +
            """CASE WHEN COALESCE(uaf.capacity_limited_unit_flag, 'N') = 'Y'
    OR REPLACE(hourly.op_date, ?, ?) >= uaf.offline_start_date
    THEN 0.0
    ELSE hourly.gload END 
    FROM calc_hourly_base hourly
    JOIN calc_updated_uaf uaf
    ON hourly.orispl_code = uaf.orispl_code
    AND hourly.unitid = uaf.unitid
    AND hourly.ertac_fuel_unit_type_bin = uaf.ertac_fuel_unit_type_bin
    JOIN calendar_hours c
    ON hourly.op_date = c.op_date
    AND hourly.op_hour = c.op_hour
    WHERE hourly.ertac_region = ?
    AND hourly.ertac_fuel_unit_type_bin = ?
    AND hourly.op_date = ?
    AND hourly.op_hour = ?""", query_bindings).fetchall():

        if gload is None:
            gload = 0.0
        else:
            gload = gload * afygr

        if hierarchy_hour > 1:
            # Get previous hour's running totals.
            (cumulative_hi, cumulative_gen, cumulative_op_hours) = conn.execute("""SELECT cumulative_hi, cumulative_gen, cumulative_op_hours
            FROM hourly_diagnostic_file
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (region, fuel, plant, unit, hierarchy_hour - 1)).fetchone()
        else:
            cumulative_hi = cumulative_gen = cumulative_op_hours = 0.0

        (unit_max_hi, unit_max_uf, unit_max_gload, nominal_heat_rate, unit_heat_rate, hours_cap) = conn.execute("""SELECT
        max_ertac_hi_hourly_summer, COALESCE(unit_annual_capacity_limit, max_annual_ertac_uf),
        max_by_hourly_gload, nominal_heat_rate, ertac_heat_rate, hours_cap
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

        # For V2, if heat_rate_avg_method was specified, override V1 constant
        # heat rate with new calculated value.
        # RW 9/30/2015 Except, if nominal_heat_rate was specified in UAF to be
        # used instead of V1 annual rate, we still use that value now (which the
        # preprocessor copied into ertac_heat_rate).
        if heat_rate_avg_method is not None and nominal_heat_rate is None:
            heat_rate_result = conn.execute("""SELECT
            calc_rate, rate_type, rate_limit_flag
            FROM hourly_rates
            WHERE region = ?
            AND fuel = ?
            AND plant = ?
            AND unit = ?
            AND calendar_hour = ?""", (region, fuel, plant, unit, calendar_hour)).fetchone()
            if heat_rate_result is not None:
                (unit_heat_rate, heat_rate_type, heat_rate_limit_flag) = heat_rate_result
            else:
                (unit_heat_rate, heat_rate_type, heat_rate_limit_flag) = (None, None, None)
        else:
            # Using constant rate (nominal or average) from UAF as before.
            (heat_rate_type, heat_rate_limit_flag) = (None, None)

        if unit_max_uf is None:
            unit_max_uf = max_uf

        if unit_heat_rate is not None and gload is not None and gload > 0:
            heat_input = unit_heat_rate * gload / 1000.0
            hizg_hi = None  # jmj 11/25/2019 set hizg to none since the unit got generation in this hour
        else:
            # jmj 6/10/2019 get heat input from calc_hour_base so we can maintain any start up/shutdown/maintenance emissions
            heat_input = hizg_hi if hizg_hi is not None else 0.0

        # 6, 7
        # jmj 9/4/2019 correct for max heat input check (was > should be >=)
        if unit_max_hi is not None and heat_input >= unit_max_hi:
            hourly_hi_limit = 'Y'
            heat_input = unit_max_hi
            # jmj 6/10/2019 make sure gload isn't recalculated if its a hizg hour
            if hizg_hi is None:
                gload = round(heat_input * 1000.0 / unit_heat_rate, 12)
        else:
            hourly_hi_limit = 'N'

        if unit_max_hi is not None and unit_max_uf is not None:
            unit_annual_hi_limit_value = ertac_lib.hours_in_year(base_year, future_year) * unit_max_hi * unit_max_uf
            if cumulative_hi + heat_input > unit_annual_hi_limit_value:
                annual_hi_limit = 'Y'
                heat_input = unit_annual_hi_limit_value - cumulative_hi
                # jmj 6/10/2019 make sure gload isn't recalculated if its a hizg hour
                if hizg_hi is None:
                    gload = round(heat_input * 1000.0 / unit_heat_rate, 12)
            else:
                annual_hi_limit = 'N'
        else:
            annual_hi_limit = 'N'

        if gload > 0.0:
            cumulative_op_hours += 1.0

        # jmj 3/9/2017 add the code to limit by operating hours too
        if hours_cap is not None and cumulative_op_hours > hours_cap:
            cumulative_op_hours -= 1.0
            annual_oh_limit = 'Y'
            heat_input = 0
            gload = 0
        else:
            annual_oh_limit = 'N'

        conn.execute("""INSERT INTO hourly_diagnostic_file
        (ertac_region, ertac_fuel_unit_type_bin, state, orispl_code, unitid,
        calendar_hour, hierarchy_hour, hourly_hi_limit, annual_hi_limit, annual_oh_limit,
        cumulative_hi, cumulative_gen, cumulative_op_hours, gload, heat_input,
        heat_rate, heat_rate_type, heat_rate_limit_flag, generation_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                     (region, fuel, state, plant, unit,
                      calendar_hour, hierarchy_hour, hourly_hi_limit, annual_hi_limit, annual_oh_limit,
                      cumulative_hi + heat_input, cumulative_gen + gload, cumulative_op_hours, gload, heat_input,
                      unit_heat_rate, heat_rate_type, heat_rate_limit_flag, 'GH' if (hizg_hi is not None) else 'G'))


def add_generic_units(conn, region, fuel, capacity_needed, new_unit_max_size, new_unit_min_size,
                      facility_index, facility_list, max_uf, new_unit_placement_pct, base_year, future_year, logfile):
    """3.5Y.1: Create new generic units to satisfy capacity needs.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel bin being processed
    capacity_needed -- the amount of additional capacity needed
    new_unit_max_size -- the maximum size of a new unit to create for the current fuel
    new_unit_min_size -- the minimum size of a new unit to create for the current fuel
    facility_index -- the current index location in the list of facilities for adding new units
    facility_list -- the list of facilities where new units can be added
    max_uf -- default maximum utilization fraction for any unit in this region/fuel
    new_unit_placement_pct -- placement percentile for new units within unit hierarchy
    base_year -- the base year where generation is projected from
    future_year -- the year which is being projected
    logfile -- file where logging messages will be written

    """
    unit_count = 0
    while capacity_needed > 0.0:
        if capacity_needed > new_unit_min_size:
            unit_size = new_unit_max_size
        else:
            unit_size = new_unit_min_size
        capacity_needed -= unit_size
        unit_count += 1
        # 3.5Y.2 Add unit to UAF and unit hierarchy, and assign proxy generation to it.
        plant = facility_list[facility_index]
        facility_index += 1
        facility_index %= len(facility_list)
        (state,) = conn.execute("""SELECT state FROM calc_updated_uaf WHERE ertac_region = ? AND orispl_code = ?""",
                                (region, plant)).fetchone()
        conn.execute("""UPDATE generic_unit_counts SET units_created = units_created + 1 WHERE state = ?""", (state,))
        (state_code, units_created) = conn.execute(
            """SELECT state_code, units_created FROM generic_unit_counts WHERE state = ?""", (state,)).fetchone()
        unit = "G" + state_code + str(units_created).zfill(3)

        #JMJ 1/25/2024 changed the new unit generic message to provide more information
        logging.info("  Creating new generic unit ("+str(unit_count)+"): " + ertac_lib.nice_str((region, fuel, plant, unit))+" - Capacity needed after creation: "+str(capacity_needed))
        print("  Creating new generic unit ("+str(unit_count)+"): " + ertac_lib.nice_str((region, fuel, plant, unit))+" - Capacity needed after creation: "+str(capacity_needed), file=logfile)

        plant_columns = conn.execute("SELECT " + ertac_tables.uaf_plant_column_names + """ FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND orispl_code = ?""", (region, plant)).fetchone()
        online_start_date = ertac_lib.first_day_of(future_year)
        offline_start_date = ertac_lib.offline_default
        # Need to have heat rate for new unit.  Base this on average heat rate of other units for same region/fuel.
        (ertac_heat_rate,) = conn.execute("""SELECT AVG(ertac_heat_rate)
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
        # Need to have max heat input for new unit.  Compute from specified unit size and average heat rate.
        max_ertac_hi_hourly_summer = unit_size * ertac_heat_rate / 1000.0
        unit_column_names = 'unitid, camd_by_hourly_data_type, online_start_date, offline_start_date, ertac_fuel_unit_type_bin, max_ertac_hi_hourly_summer, nameplate_capacity, max_annual_ertac_uf, ertac_heat_rate, ' + ertac_tables.uaf_plant_column_names
        unit_columns = [unit, 'NEW', online_start_date, offline_start_date, fuel, max_ertac_hi_hourly_summer, unit_size,
                        max_uf, ertac_heat_rate]
        unit_columns.extend(plant_columns)
        parameter_list = '(?' + ', ?' * (len(unit_columns) - 1) + ')'
        conn.execute("INSERT INTO calc_updated_uaf (" + unit_column_names + ") VALUES " + parameter_list, unit_columns)
        (facility_name, plant_latitude, plant_longitude) = conn.execute("""SELECT facility_name, plant_latitude, plant_longitude
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND orispl_code = ?""", (region, plant)).fetchone()
        conn.execute("INSERT INTO generic_units_created VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                     (region, fuel, unit_size, plant, unit, facility_name, plant_latitude, plant_longitude))
        # 3.6: Insert into unit hierarchy.
        (max_rank,) = conn.execute("""SELECT MAX(unit_allocation_order)
        FROM calc_unit_hierarchy
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
        # 20120501 Handle case where region/fuel had no existing units in
        # hierarchy.
        if max_rank is None:
            max_rank = 0
            anchor_rank = 0
        else:
            # jmj do an else here where you also find the highest New Unit allcoation and have that be the anchor rank
            # no need to check if max rank is none since
            (new_unit_rank,) = conn.execute("""SELECT MAX(unit_allocation_order)
                FROM calc_unit_hierarchy hier
                LEFT JOIN calc_updated_uaf uaf
                ON uaf.ertac_region = hier.ertac_region
                AND uaf.ertac_fuel_unit_type_bin = hier.ertac_fuel_unit_type_bin
                AND uaf.orispl_code = hier.orispl_code
                AND uaf.unitid = hier.unitid
                WHERE hier.ertac_region = ?
                AND hier.ertac_fuel_unit_type_bin = ?
                AND uaf.camd_by_hourly_data_type = 'NEW'""", (region, fuel)).fetchone()
            if new_unit_rank:
                anchor_rank = new_unit_rank
            else:
                anchor_rank = max_rank - int(max_rank * new_unit_placement_pct / 100.0)

        for (ranked_plant, ranked_unit) in conn.execute("""SELECT orispl_code, unitid
        FROM calc_unit_hierarchy
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND unit_allocation_order > ?
        ORDER BY unit_allocation_order DESC""", (region, fuel, anchor_rank)).fetchall():
            conn.execute("""UPDATE calc_unit_hierarchy
            SET unit_allocation_order = unit_allocation_order + 1
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?""", (region, fuel, ranked_plant, ranked_unit))
        (state, name, max_heat_input, heat_rate) = conn.execute("""SELECT state, facility_name,
        max_ertac_hi_hourly_summer, ertac_heat_rate
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit)).fetchone()
        conn.execute("""INSERT INTO calc_unit_hierarchy (ertac_region,
        ertac_fuel_unit_type_bin, orispl_code, unitid, unit_allocation_order, state)
        VALUES (?, ?, ?, ?, ?, ?)""", (region, fuel, plant, unit, anchor_rank + 1, state))

        # Assign proxy generation.
        ertac_lib.compute_proxy_generation(conn, region, fuel, plant, unit, state, name, base_year, future_year,
                                           logfile)

    return unit_count


def flag_negative_demand_transfers(conn, region, fuel, logfile):
    badhours = False
    for (calendar_hour, future_projected_generation, net_demand_transfer, excess_generation_pool) in conn.execute("""SELECT calendar_hour, future_projected_generation, net_demand_transfer, excess_generation_pool
    FROM calc_generation_parms
    WHERE future_projected_generation + net_demand_transfer < 0
    AND ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchall():
        print("Fatal Error: calendar hour: " + str(calendar_hour) + " for region: " + str(region) + ", fuel: " + str(
            fuel) + " has a negative net demand transfer (" + str(
            net_demand_transfer) + ") that is larger than the available generation in that hour (" + str(
            future_projected_generation) + ")", file=logfile)
        badhours = True
    if badhours:
        logging.info("Warning code failed due to negative demand transfers.  See log for details.")
        exit(0)


def allocate_excess_generation(conn, region, fuel, max_uf, base_year, future_year, logfile):
    """10: Allocate any excess generation in two passes, first raising outputs to optimal threshold, then to maximum.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel bin being processed
    max_uf -- default maximum utilization fraction for any unit in this region/fuel
    future_year -- the year which is being projected
    logfile -- file where logging messages will be written

    """
    # 10.5

    # Need to check annual limits at last hierarchical hour; if units already
    # were assigned full annual capacity in page 2 AFYGR process, no more
    # generation can be assigned to those units.
    (last_hour,) = conn.execute("""SELECT MAX(temporal_allocation_order)
    FROM calc_generation_parms
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

    for (hierarchy_hour, excess_generation) in conn.execute("""SELECT temporal_allocation_order, excess_generation_pool
    FROM calc_generation_parms
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?
    ORDER BY temporal_allocation_order""", (region, fuel)).fetchall():

        # 11: First pass, do not raise above optimal level.
        # For V2, get specific value of unit_heat_rate used at this hour from
        # hourly_diagnostic_file, instead of constant ertac_heat_rate from UAF.
        for (plant, unit, unit_order, calendar_hour, hourly_hi_limit, annual_hi_limit, annual_oh_limit, initial_gload,
             initial_heat_input, unit_heat_rate, generation_flag) in conn.execute("""SELECT hier.orispl_code,
        hier.unitid, hier.unit_allocation_order, hourly.calendar_hour, hourly.hourly_hi_limit, hourly.annual_hi_limit, hourly.annual_oh_limit, hourly.gload, hourly.heat_input, hourly.heat_rate, hourly.generation_flag
        FROM calc_unit_hierarchy hier
        JOIN hourly_diagnostic_file hourly
        ON hier.ertac_region = hourly.ertac_region
        AND hier.ertac_fuel_unit_type_bin = hourly.ertac_fuel_unit_type_bin
        AND hier.orispl_code = hourly.orispl_code
        AND hier.unitid = hourly.unitid
        WHERE hier.ertac_region = ?
        AND hier.ertac_fuel_unit_type_bin = ?
        AND hourly.hierarchy_hour = ?
        ORDER BY unit_allocation_order""", (region, fuel, hierarchy_hour)).fetchall():

            (future_date,) = conn.execute("""SELECT future_date
            FROM calendar_hours
            WHERE calendar_hour = ?""", (calendar_hour,)).fetchone()

            (online, offline) = conn.execute("""SELECT online_start_date, offline_start_date
            FROM calc_updated_uaf
            WHERE orispl_code = ?
            AND unitid = ?
            AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel)).fetchone()

            gload = initial_gload
            heat_input = initial_heat_input

            if hierarchy_hour > 1:
                # Get previous hour's running totals.
                (cumulative_hi, cumulative_gen, cumulative_op_hours) = conn.execute("""SELECT cumulative_hi, cumulative_gen, cumulative_op_hours
                FROM hourly_diagnostic_file
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?
                AND hierarchy_hour = ?""", (region, fuel, plant, unit, hierarchy_hour - 1)).fetchone()
            else:
                cumulative_hi = cumulative_gen = cumulative_op_hours = 0.0

            # Get unit's status at final hour.
            (last_hour_annual_hi_limit, last_hour_annual_oh_limit, last_hour_cumulative_hi, last_hour_cumulative_gen,
             last_hour_cumulative_op_hours) = conn.execute("""SELECT annual_hi_limit, annual_oh_limit, cumulative_hi, cumulative_gen, cumulative_op_hours
            FROM hourly_diagnostic_file
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (region, fuel, plant, unit, last_hour)).fetchone()

            (unit_max_hi, unit_max_uf, unit_max_gload, unit_optimal_load, hours_cap) = conn.execute("""SELECT max_ertac_hi_hourly_summer,
            COALESCE(unit_annual_capacity_limit, max_annual_ertac_uf),
            max_by_hourly_gload, unit_max_optimal_load_threshold, hours_cap
            FROM calc_updated_uaf
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

            if unit_max_uf is None:
                unit_max_uf = max_uf

            if unit_heat_rate is not None and unit_optimal_load is not None:
                unit_opt_hi = unit_heat_rate * unit_optimal_load / 1000.0
                if initial_heat_input < unit_opt_hi and excess_generation > 0.0 and last_hour_annual_hi_limit == 'N' and last_hour_annual_oh_limit == 'N' and future_date >= online and future_date < offline:
                    gload = initial_gload + excess_generation
                    excess_generation = 0.0
                    heat_input = unit_heat_rate * gload / 1000.0
                    # Hourly optimum?
                    if heat_input > unit_opt_hi:
                        excess_generation += round((heat_input - unit_opt_hi) * 1000.0 / unit_heat_rate, 12)
                        heat_input = unit_opt_hi
                        gload = round(heat_input * 1000.0 / unit_heat_rate, 12)
                    # Annual limit?
                    if unit_max_hi is not None and unit_max_uf is not None:
                        unit_annual_hi_limit_value = ertac_lib.hours_in_year(base_year,
                                                                             future_year) * unit_max_hi * unit_max_uf
                        headroom = unit_annual_hi_limit_value - last_hour_cumulative_hi
                        if heat_input > initial_heat_input + headroom:
                            # We used all available capacity through the end of the year.
                            excess_generation += round((heat_input - (
                                    initial_heat_input + headroom)) * 1000.0 / unit_heat_rate, 12)
                            heat_input = initial_heat_input + headroom
                            gload = round(heat_input * 1000.0 / unit_heat_rate, 12)
                            last_hour_annual_hi_limit = 'Y'  # rw fixed typo == vs = found by jj

            if gload > 0.0:
                cumulative_op_hours += 1.0
            # jmj 3/9/2017 add the code to limit by operating hours too
            if hours_cap is not None and cumulative_op_hours > hours_cap:
                cumulative_op_hours -= 1.0
                annual_oh_limit = 'Y'
                heat_input = 0
                gload = 0

            # Change values at current hour.
            conn.execute("""UPDATE hourly_diagnostic_file
            SET hourly_hi_limit = ?, annual_hi_limit = ?, annual_oh_limit = ?, cumulative_hi = ?, cumulative_gen = ?, cumulative_op_hours = ?, gload = ?, heat_input = ?, generation_flag = ?
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (
                hourly_hi_limit, annual_hi_limit, annual_oh_limit, cumulative_hi + heat_input, cumulative_gen + gload,
                cumulative_op_hours, gload, heat_input, generation_flag + 'O', region, fuel, plant, unit,
                hierarchy_hour))

            # Change cumulative HI and annual limit flag for last hour.
            conn.execute("""UPDATE hourly_diagnostic_file
            SET annual_hi_limit = ?, cumulative_hi = ?, cumulative_gen = ?
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (
                last_hour_annual_hi_limit, last_hour_cumulative_hi + heat_input - initial_heat_input,
                last_hour_cumulative_gen + gload - initial_gload, region, fuel, plant, unit, last_hour))

        # 15: Second pass, raise to maximum if necessary.
        # For V2, get specific value of unit_heat_rate used at this hour from
        # hourly_diagnostic_file, instead of constant ertac_heat_rate from UAF.
        for (plant, unit, unit_order, calendar_hour, hourly_hi_limit, annual_hi_limit, annual_oh_limit, initial_gload,
             initial_heat_input, unit_heat_rate) in conn.execute("""SELECT hier.orispl_code,
        hier.unitid, hier.unit_allocation_order, hourly.calendar_hour, hourly.hourly_hi_limit, hourly.annual_hi_limit, hourly.annual_oh_limit, hourly.gload, hourly.heat_input, hourly.heat_rate
        FROM calc_unit_hierarchy hier
        JOIN hourly_diagnostic_file hourly
        ON hier.ertac_region = hourly.ertac_region
        AND hier.ertac_fuel_unit_type_bin = hourly.ertac_fuel_unit_type_bin
        AND hier.orispl_code = hourly.orispl_code
        AND hier.unitid = hourly.unitid
        WHERE hier.ertac_region = ?
        AND hier.ertac_fuel_unit_type_bin = ?
        AND hourly.hierarchy_hour = ?
        ORDER BY unit_allocation_order""", (region, fuel, hierarchy_hour)).fetchall():

            (future_date,) = conn.execute("""SELECT future_date
            FROM calendar_hours
            WHERE calendar_hour = ?""", (calendar_hour,)).fetchone()

            (online, offline) = conn.execute("""SELECT online_start_date, offline_start_date
            FROM calc_updated_uaf
            WHERE orispl_code = ?
            AND unitid = ?
            AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel)).fetchone()

            gload = initial_gload
            heat_input = initial_heat_input

            if hierarchy_hour > 1:
                # Get previous hour's running totals.
                (cumulative_hi, cumulative_gen, cumulative_op_hours) = conn.execute("""SELECT cumulative_hi, cumulative_gen, cumulative_op_hours
                FROM hourly_diagnostic_file
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?
                AND hierarchy_hour = ?""", (region, fuel, plant, unit, hierarchy_hour - 1)).fetchone()
            else:
                cumulative_hi = cumulative_gen = cumulative_op_hours = 0.0

            # Get unit's status at final hour.
            (last_hour_annual_hi_limit, last_hour_annual_oh_limit, last_hour_cumulative_hi, last_hour_cumulative_gen,
             last_hour_cumulative_op_hours) = conn.execute("""SELECT annual_hi_limit, annual_oh_limit, cumulative_hi, cumulative_gen, cumulative_op_hours
            FROM hourly_diagnostic_file
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (region, fuel, plant, unit, last_hour)).fetchone()

            (unit_max_hi, unit_max_uf, unit_max_gload, ertac_heat_rate, hours_cap) = conn.execute("""SELECT max_ertac_hi_hourly_summer,
            COALESCE(unit_annual_capacity_limit, max_annual_ertac_uf),
            max_by_hourly_gload, ertac_heat_rate, hours_cap
            FROM calc_updated_uaf
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

            if unit_max_uf is None:
                unit_max_uf = max_uf

            if unit_heat_rate is not None and unit_max_hi is not None:
                if initial_heat_input < unit_max_hi and excess_generation > 0.0 and last_hour_annual_hi_limit == 'N' and last_hour_annual_oh_limit == 'N' and future_date >= online and future_date < offline:
                    gload = initial_gload + excess_generation
                    excess_generation = 0.0
                    heat_input = unit_heat_rate * gload / 1000.0
                    # Hourly limit?
                    if heat_input > unit_max_hi:
                        excess_generation += round((heat_input - unit_max_hi) * 1000.0 / unit_heat_rate, 12)
                        heat_input = unit_max_hi
                        gload = round(heat_input * 1000.0 / unit_heat_rate, 12)
                    # Annual limit?
                    if unit_max_hi is not None and unit_max_uf is not None:
                        unit_annual_hi_limit_value = ertac_lib.hours_in_year(base_year,
                                                                             future_year) * unit_max_hi * unit_max_uf
                        headroom = unit_annual_hi_limit_value - last_hour_cumulative_hi
                        if heat_input > initial_heat_input + headroom:
                            # We used all available capacity through the end of the year.
                            excess_generation += round((heat_input - (
                                    initial_heat_input + headroom)) * 1000.0 / unit_heat_rate, 12)
                            heat_input = initial_heat_input + headroom
                            gload = round(heat_input * 1000.0 / unit_heat_rate, 12)
                            last_hour_annual_hi_limit = 'Y'  # rw fixed typo == vs = found by jj

                # Might have raised to hourly limit, but could have backed down for annual limit.
                if heat_input >= unit_max_hi:
                    hourly_hi_limit = 'Y'

            if gload > 0.0:
                cumulative_op_hours += 1.0

            # jmj 3/9/2017 add the code to limit by operating hours too
            if hours_cap is not None and cumulative_op_hours > hours_cap:
                cumulative_op_hours -= 1.0
                annual_oh_limit = 'Y'
                heat_input = 0
                gload = 0

            # Change values at current hour.
            conn.execute("""UPDATE hourly_diagnostic_file
            SET hourly_hi_limit = ?, annual_hi_limit = ?, annual_oh_limit = ?, cumulative_hi = ?, cumulative_gen = ?, cumulative_op_hours = ?, gload = ?, heat_input = ?, generation_flag = ?
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (
                hourly_hi_limit, annual_hi_limit, annual_oh_limit, cumulative_hi + heat_input, cumulative_gen + gload,
                cumulative_op_hours, gload, heat_input, generation_flag + 'M', region, fuel, plant, unit,
                hierarchy_hour))

            # Change cumulative HI and annual limit flag for last hour.
            conn.execute("""UPDATE hourly_diagnostic_file
            SET annual_hi_limit = ?, cumulative_hi = ?, cumulative_gen = ?
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (
                last_hour_annual_hi_limit, last_hour_cumulative_hi + heat_input - initial_heat_input,
                last_hour_cumulative_gen + gload - initial_gload, region, fuel, plant, unit, last_hour))

        # Excess generation may not all have been allocated yet; update stored pool values.
        conn.execute("""UPDATE calc_generation_parms
        SET excess_generation_pool = ?
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND temporal_allocation_order = ?""", (excess_generation, region, fuel, hierarchy_hour))


def evaluate_spinning_reserve(conn, region, logfile):
    """Determine whether there is enough reserve capacity at each hour for the current region.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the ERTAC region
    logfile -- file where logging messages will be written

    """
    # Find total generating capacity in region.
    # Find total load and largest unit operating at each hour in region, and
    # determine hourly demand hierarchy across all fuels in region.
    # Store hourly pass/fail results.

    (demand_cushion,) = conn.execute("""SELECT demand_cushion
    FROM calc_input_variables
    WHERE ertac_region = ?""", (region,)).fetchone()

    (total_capacity,) = conn.execute("""SELECT SUM(1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate)
    FROM calc_updated_uaf uaf
    JOIN calc_unit_hierarchy hier
    ON uaf.ertac_region = hier.ertac_region
    AND uaf.ertac_fuel_unit_type_bin = hier.ertac_fuel_unit_type_bin
    AND uaf.orispl_code = hier.orispl_code
    AND uaf.unitid = hier.unitid
    WHERE uaf.ertac_region = ?""", (region,)).fetchone()
    if total_capacity is None:
        total_capacity = 0.0

    hierarchy_hour = 1

    # RW 9/10/2015 Update total load calculation to include demand transfers.
    for (date, hour, total_load, total_transfer) in conn.execute("""SELECT op_date, op_hour,
    SUM(future_projected_generation) AS total_load, SUM(net_demand_transfer) AS total_transfer
    FROM calc_generation_parms
    WHERE ertac_region = ?
    GROUP BY op_date, op_hour
    ORDER BY total_load DESC, op_date, op_hour""", (region,)).fetchall():

        if total_load is None:
            total_load = 0.0
        if total_transfer is None:
            total_transfer = 0.0

        (calendar_hour,) = conn.execute("""SELECT calendar_hour
        FROM calendar_hours
        WHERE op_date = ?
        AND op_hour = ?""", (date, hour)).fetchone()

        # 20: What is the max capacity of any unit in this region operating at
        # this particular calendar hour?
        # For V2, get specific value of unit_heat_rate used at this hour from
        # hourly_diagnostic_file, instead of constant ertac_heat_rate from UAF.
        (max_unit_capacity,) = conn.execute("""SELECT MAX(1000.0 * uaf.max_ertac_hi_hourly_summer / hourly.heat_rate)
        FROM hourly_diagnostic_file hourly
        JOIN calc_updated_uaf uaf
        ON hourly.ertac_region = uaf.ertac_region
        AND hourly.ertac_fuel_unit_type_bin = uaf.ertac_fuel_unit_type_bin
        AND hourly.orispl_code = uaf.orispl_code
        AND hourly.unitid = uaf.unitid
        WHERE hourly.ertac_region = ?
        AND hourly.calendar_hour = ?
        AND hourly.gload > 0""", (region, calendar_hour)).fetchone()
        if max_unit_capacity is None:
            max_unit_capacity = 0.0
        # 21
        reserve_needed = max_unit_capacity * demand_cushion
        amount_available_without_transfers = total_capacity - total_load
        amount_available_including_transfers = total_capacity - (total_load + total_transfer)
        # 23
        if reserve_needed > amount_available_including_transfers:
            pass_fail = 'F'
            deficit = reserve_needed - amount_available_including_transfers
        else:
            pass_fail = 'P'
            deficit = None
        # 23Y1
        conn.execute("""INSERT INTO reserve_capacity_needed
        (ertac_region, calendar_hour, hierarchy_hour, pass_fail,
        reserve_needed, amount_available_without_transfers,
        amount_available_including_transfers, deficit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                     (region, calendar_hour, hierarchy_hour, pass_fail,
                      reserve_needed, amount_available_without_transfers,
                      amount_available_including_transfers, deficit))

        hierarchy_hour += 1


def summarize_unit_activity(conn, base_year, future_year, logfile):
    """23.5Y1: Summarize unit level generation and heat input.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    # Summarize generation and heat input from hourly diagnostic table into unit level activity.
    conn.execute("""INSERT INTO unit_level_activity
    (ertac_region, ertac_fuel_unit_type_bin, state, orispl_code, unitid,
    facility_name, fy_gen, fy_hi, fy_hours)
    SELECT ertac_region, ertac_fuel_unit_type_bin, state, orispl_code, unitid,
    ' ', SUM(gload), SUM(heat_input), SUM(CASE WHEN gload > 0 THEN 1 ELSE 0 END)
    FROM hourly_diagnostic_file
    GROUP BY ertac_region, ertac_fuel_unit_type_bin, state, orispl_code, unitid""")
    for (plant, unit, region, fuel, rowid) in conn.execute("""SELECT orispl_code, unitid,
    ertac_region, ertac_fuel_unit_type_bin, rowid
    FROM unit_level_activity""").fetchall():
        # 20120305 Changed to use ertac_heat_rate instead of calc_by_average_heat_rate,
        # to avoid undefined division for new units, which don't have base year data,
        # and to be consistent with all other uses of heat rate.
        # RW 8/21/2015 Added OS and non-OS heat rate based on 8/10/2015 call.
        (fac_name, max_hi, heat_rate, os_heat_rate, nonos_heat_rate, by_hours) = conn.execute("""SELECT facility_name,
        max_ertac_hi_hourly_summer, ertac_heat_rate, heat_rate_os_avg, heat_rate_nonos_avg, operating_hours_by
        FROM calc_updated_uaf
        WHERE orispl_code = ?
        AND unitid = ?
        AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel)).fetchone()
        if heat_rate is not None and heat_rate > 0.0:
            if max_hi is None:
                x = 1
            else:
                gen_cap = round(1000.0 * max_hi / heat_rate, 12)
        else:
            gen_cap = None
        (hours_at_max,) = conn.execute("""SELECT COUNT(*)
        FROM hourly_diagnostic_file
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?
        AND hourly_hi_limit = 'Y'""", (region, fuel, plant, unit)).fetchone()
        (by_gen, by_hi) = conn.execute("""SELECT SUM(gload), SUM(heat_input)
        FROM calc_hourly_base
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit)).fetchone()
        conn.execute("""UPDATE unit_level_activity
        SET facility_name = ?,
        max_ertac_hi_hourly_summer = ?,
        heat_rate = ?,
        os_heat_rate = ?,
        nonos_heat_rate = ?,
        capacity = ?,
        num_hrs_fy_max = ?,
        by_gen = ?,
        by_hi = ?,
        by_hours = ?
        WHERE rowid = ?""", (
            fac_name, max_hi, heat_rate, os_heat_rate, nonos_heat_rate, gen_cap, hours_at_max, by_gen, by_hi, by_hours,
            rowid))
    conn.execute("""UPDATE unit_level_activity
    SET uf = fy_hi / (? * max_ertac_hi_hourly_summer)""", (ertac_lib.hours_in_year(base_year, future_year),))


def calculate_future_emissions(conn, base_year, future_year, ozone_start_base, ozone_end_base, ozone_start_future,
                               ozone_end_future, logfile):
    """26: Calculate future hourly emissions.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year where generation is projected from
    future_year -- the futuer year where generation is projected to
    ozone_start_base, ozone_end_base -- the ozone season dates in the base year
    ozone_start_future, ozone_end_future -- the ozone season dates in the future year
    logfile -- file where logging messages will be written

    """

    # 1/31/2018 jmj added an option to use controls in the base year if you are doing a by = fy runs
    if base_year < future_year:
        first_day = ertac_lib.first_day_after(base_year)
    else:
        first_day = ertac_lib.first_day_of(base_year)

    # Determine average emission rates from base year activity.
    # 20120406 Updated to calculate seasonal NOx rates instead of annual.
    conn.executescript("""CREATE TEMPORARY TABLE by_emission_summary
    (ertac_region TEXT NOT NULL COLLATE NOCASE,
    ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
    orispl_code TEXT NOT NULL COLLATE NOCASE,
    unitid TEXT NOT NULL COLLATE NOCASE,
    total_so2_mass REAL,
    total_heat_input REAL,
    so2_rate REAL,
    PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid));

    CREATE TEMPORARY TABLE by_os_emission_summary
    (ertac_region TEXT NOT NULL COLLATE NOCASE,
    ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
    orispl_code TEXT NOT NULL COLLATE NOCASE,
    unitid TEXT NOT NULL COLLATE NOCASE,
    total_os_nox_mass REAL,
    total_os_heat_input REAL,
    os_nox_rate REAL,
    PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid));

    CREATE TEMPORARY TABLE by_nonos_emission_summary
    (ertac_region TEXT NOT NULL COLLATE NOCASE,
    ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
    orispl_code TEXT NOT NULL COLLATE NOCASE,
    unitid TEXT NOT NULL COLLATE NOCASE,
    total_nonos_nox_mass REAL,
    total_nonos_heat_input REAL,
    nonos_nox_rate REAL,
    PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid));

    INSERT INTO by_emission_summary(ertac_region, ertac_fuel_unit_type_bin,
    orispl_code, unitid, total_so2_mass, total_heat_input)
    SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid,
    SUM(so2_mass), SUM(heat_input)
    FROM calc_hourly_base
    GROUP BY ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid;

    CREATE TEMPORARY TABLE future_dates
    (future_date TEXT NOT NULL,
    first_calendar_hour INTEGER,
    last_calendar_hour INTEGER,
    PRIMARY KEY (future_date));

    INSERT INTO future_dates (future_date)
    SELECT DISTINCT future_date
    FROM calendar_hours
    ORDER BY future_date;

    UPDATE future_dates
    SET first_calendar_hour = (SELECT MIN(calendar_hour) FROM calendar_hours
    WHERE calendar_hours.future_date = future_dates.future_date),
    last_calendar_hour = (SELECT MAX(calendar_hour) FROM calendar_hours
    WHERE calendar_hours.future_date = future_dates.future_date);

    CREATE INDEX IF NOT EXISTS control_emissions_dates
    ON calc_control_emissions (orispl_code, unitid, pollutant_code, factor_start_date, factor_end_date);""")

    conn.execute("""INSERT INTO by_os_emission_summary(ertac_region, ertac_fuel_unit_type_bin,
    orispl_code, unitid, total_os_nox_mass, total_os_heat_input)
    SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid,
    SUM(nox_mass), SUM(heat_input)
    FROM calc_hourly_base
    WHERE op_date BETWEEN ? and ?
    GROUP BY ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid""", (ozone_start_base, ozone_end_base))

    conn.execute("""INSERT INTO by_nonos_emission_summary(ertac_region, ertac_fuel_unit_type_bin,
    orispl_code, unitid, total_nonos_nox_mass, total_nonos_heat_input)
    SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid,
    SUM(nox_mass), SUM(heat_input)
    FROM calc_hourly_base
    WHERE op_date NOT BETWEEN ? and ?
    GROUP BY ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid""", (ozone_start_base, ozone_end_base))

    conn.executescript("""DELETE FROM by_emission_summary
    WHERE total_heat_input IS NULL
    OR total_heat_input = 0.0;

    DELETE FROM by_os_emission_summary
    WHERE total_os_heat_input IS NULL
    OR total_os_heat_input = 0.0;

    DELETE FROM by_nonos_emission_summary
    WHERE total_nonos_heat_input IS NULL
    OR total_nonos_heat_input = 0.0;

    UPDATE by_emission_summary
    SET so2_rate = total_so2_mass / total_heat_input;

    UPDATE by_os_emission_summary
    SET os_nox_rate = total_os_nox_mass / total_os_heat_input;

    UPDATE by_nonos_emission_summary
    SET nonos_nox_rate = total_nonos_nox_mass / total_nonos_heat_input;""")

    # Pick emission rates from existing units to apply to new units for each region/fuel.
    for (region, fuel) in conn.execute("""SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin
    FROM by_emission_summary
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall():

        (new_unit_ef_pct,) = conn.execute("""SELECT new_unit_emission_factor_percentile
        FROM calc_input_variables
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

        so2_list = conn.execute("""SELECT so2_rate
        FROM by_emission_summary
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND so2_rate > 0.0
        ORDER BY so2_rate DESC""", (region, fuel)).fetchall()
        if len(so2_list) > 0:
            slot = int(len(so2_list) * new_unit_ef_pct / 100.0)
            if slot < 0:
                slot = 0
            if slot >= len(so2_list):
                slot = len(so2_list) - 1
            new_unit_so2_rate = so2_list[slot][0]
        else:
            new_unit_so2_rate = None

        # 20120406 New units will use the cleaner OS NOx rate year-round.
        nox_list = conn.execute("""SELECT os_nox_rate
        FROM by_os_emission_summary
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND os_nox_rate > 0.0
        ORDER BY os_nox_rate DESC""", (region, fuel)).fetchall()
        if len(nox_list) > 0:
            slot = int(len(nox_list) * new_unit_ef_pct / 100.0)
            if slot < 0:
                slot = 0
            if slot >= len(nox_list):
                slot = len(nox_list) - 1
            new_unit_nox_rate = nox_list[slot][0]
        else:
            new_unit_nox_rate = None

        # For V2, if current region/fuel has a specified averaging method for NOx
        # and/or SO2, compute and store hourly rates for existing units, and use
        # those rates later instead of fixed annual or OS/non-OS rates.
        (nox_avg_method, so2_avg_method) = conn.execute("""SELECT
        nox_avg_method, so2_avg_method
        FROM calc_input_variables
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

        if nox_avg_method is not None:
            calculate_nox_rates(conn, region, fuel, nox_avg_method, logfile)

        if so2_avg_method is not None:
            calculate_so2_rates(conn, region, fuel, so2_avg_method, logfile)

        # Calculate future emissions for new and existing units in current region/fuel.
        for (plant, unit, by_type) in conn.execute("""SELECT orispl_code, unitid, camd_by_hourly_data_type
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchall():

            by_type = by_type.upper()
            if by_type != 'NEW':
                # Look up base-year rates for existing unit.
                so2_result = conn.execute("""SELECT so2_rate
                FROM by_emission_summary
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?""", (region, fuel, plant, unit)).fetchone()
                if so2_result is not None:
                    (by_so2_rate,) = so2_result
                else:
                    by_so2_rate = None

                nox_result = conn.execute("""SELECT os_nox_rate
                FROM by_os_emission_summary
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?""", (region, fuel, plant, unit)).fetchone()
                if nox_result is not None:
                    (by_os_nox_rate,) = nox_result
                else:
                    by_os_nox_rate = None

                nox_result = conn.execute("""SELECT nonos_nox_rate
                FROM by_nonos_emission_summary
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?""", (region, fuel, plant, unit)).fetchone()
                if nox_result is not None:
                    (by_nonos_nox_rate,) = nox_result
                else:
                    by_nonos_nox_rate = None

            # Find applicable SO2 and NOx control/emission values for current unit for each future date.
            for (future_date, first_calendar_hour, last_calendar_hour) in conn.execute("""SELECT future_date,
            first_calendar_hour, last_calendar_hour
            FROM future_dates
            ORDER BY future_date""").fetchall():

                if by_type == 'NEW':
                    so2_rate = new_unit_so2_rate
                    nox_rate = new_unit_nox_rate
                else:
                    so2_rate = by_so2_rate
                    # 20120423 Simplfied handling of OS vs non-OS emission rates for NOx.
                    if future_date >= ozone_start_future and future_date <= ozone_end_future:
                        nox_rate = by_os_nox_rate
                    else:
                        nox_rate = by_nonos_nox_rate

                # For V2, added rate type and limit flags for emissions where
                # rates can now vary from hour to hour.  Leave empty if using
                # V1 annual or OS/non-OS rates.
                # Except, if using explicit rate from control/emissions table,
                # mark with 'R'; if using control efficiency, mark with 'C'.
                (so2_rate_type, so2_rate_limit_flag,
                 nox_rate_type, nox_rate_limit_flag) = (None, None, None, None)

                so2_result = conn.execute("""SELECT emission_rate, control_efficiency
                FROM calc_control_emissions
                WHERE orispl_code = ?
                AND unitid = ?
                AND pollutant_code = ?
                AND factor_start_date BETWEEN ? AND ?
                AND factor_end_date >= ?""", (plant, unit, 'SO2', first_day, future_date, future_date)).fetchone()

                nox_result = conn.execute("""SELECT emission_rate, control_efficiency
                FROM calc_control_emissions
                WHERE orispl_code = ?
                AND unitid = ?
                AND pollutant_code = ?
                AND factor_start_date BETWEEN ? AND ?
                AND factor_end_date >= ?""", (plant, unit, 'NOx', first_day, future_date, future_date)).fetchone()

                if so2_result is not None:
                    (future_so2_rate, future_so2_control) = so2_result
                else:
                    (future_so2_rate, future_so2_control) = (None, None)

                if future_so2_rate is not None:
                    so2_rate = future_so2_rate
                    so2_rate_type = 'R'
                elif future_so2_control is not None and so2_rate is not None:
                    so2_rate = so2_rate * (1.0 - future_so2_control / 100.0)
                    so2_rate_type = 'C'

                if nox_result is not None:
                    (future_nox_rate, future_nox_control) = nox_result
                else:
                    (future_nox_rate, future_nox_control) = (None, None)

                if future_nox_rate is not None:
                    nox_rate = future_nox_rate
                    nox_rate_type = 'R'
                elif future_nox_control is not None and nox_rate is not None:
                    nox_rate = nox_rate * (1.0 - future_nox_control / 100.0)
                    nox_rate_type = 'C'

                conn.execute("""UPDATE hourly_diagnostic_file
                SET so2_mass = heat_input * ?,
                so2_rate = ?,
                so2_rate_type = ?,
                so2_rate_limit_flag = ?,
                nox_mass = heat_input * ?,
                nox_rate = ?,
                nox_rate_type = ?,
                nox_rate_limit_flag = ?
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?
                AND calendar_hour BETWEEN ? AND ?
                AND heat_input > 0""", (so2_rate, so2_rate, so2_rate_type, so2_rate_limit_flag,
                                        nox_rate, nox_rate, nox_rate_type, nox_rate_limit_flag,
                                        region, fuel, plant, unit, first_calendar_hour, last_calendar_hour))

                # For V2, may need separate hourly rates for SO2 and/or NOx.
                # Look up each hourly rate within current day and use that to
                # update estimated emissions in hourly diagnostic file.
                # Current day's results from calc_control_emissions for future
                # rates and/or controls still matter here.  Explicit rate takes
                # precedence over anything else, and control efficiency can
                # modify hourly rate (and add 'C' to rate type marker).
                if ((so2_avg_method is not None or nox_avg_method is not None)
                        and by_type != 'NEW'):

                    for calendar_hour in range(first_calendar_hour, last_calendar_hour + 1):

                        if so2_avg_method is not None:
                            so2_result = conn.execute("""SELECT
                            calc_rate, rate_type, rate_limit_flag
                            FROM so2_hourly_rates
                            WHERE region = ?
                            AND fuel = ?
                            AND plant = ?
                            AND unit = ?
                            AND calendar_hour = ?""", (region, fuel, plant, unit, calendar_hour)).fetchone()
                            if so2_result is not None:
                                (so2_rate, so2_rate_type, so2_rate_limit_flag) = so2_result
                            else:
                                (so2_rate, so2_rate_type, so2_rate_limit_flag) = (None, None, None)
                            if future_so2_rate is not None:
                                so2_rate = future_so2_rate
                                so2_rate_type = 'R'
                            elif future_so2_control is not None and so2_rate is not None:
                                so2_rate = so2_rate * (1.0 - future_so2_control / 100.0)
                                so2_rate_type += 'C'
                        # Else so2_avg_method wasn't specified, so so2_rate and
                        # so2_rate_type from above will still hold for all hours
                        # of this day.

                        if nox_avg_method is not None:
                            nox_result = conn.execute("""SELECT
                            calc_rate, rate_type, rate_limit_flag
                            FROM nox_hourly_rates
                            WHERE region = ?
                            AND fuel = ?
                            AND plant = ?
                            AND unit = ?
                            AND calendar_hour = ?""", (region, fuel, plant, unit, calendar_hour)).fetchone()
                            if nox_result is not None:
                                (nox_rate, nox_rate_type, nox_rate_limit_flag) = nox_result
                            else:
                                (nox_rate, nox_rate_type, nox_rate_limit_flag) = (None, None, None)
                            if future_nox_rate is not None:
                                nox_rate = future_nox_rate
                                nox_rate_type = 'R'
                            elif future_nox_control is not None and nox_rate is not None:
                                nox_rate = nox_rate * (1.0 - future_nox_control / 100.0)
                                nox_rate_type += 'C'
                        # Else nox_avg_method wasn't specified, so nox_rate and
                        # nox_rate_type from above will still hold for all hours
                        # of this day.

                        conn.execute("""UPDATE hourly_diagnostic_file
                        SET so2_mass = heat_input * ?,
                        so2_rate = ?,
                        so2_rate_type = ?,
                        so2_rate_limit_flag = ?,
                        nox_mass = heat_input * ?,
                        nox_rate = ?,
                        nox_rate_type = ?,
                        nox_rate_limit_flag = ?
                        WHERE ertac_region = ?
                        AND ertac_fuel_unit_type_bin = ?
                        AND orispl_code = ?
                        AND unitid = ?
                        AND calendar_hour = ?
                        AND heat_input > 0""", (so2_rate, so2_rate, so2_rate_type, so2_rate_limit_flag,
                                                nox_rate, nox_rate, nox_rate_type, nox_rate_limit_flag,
                                                region, fuel, plant, unit, calendar_hour))


def summarize_future_emissions(conn, ozone_start_hour, ozone_end_hour, logfile):
    """27: Summarize future annual and seasonal emissions for states and groups of states.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    ozone_start_hour, ozone_end_hour -- the first and last calendar hours of the ozone season
    logfile -- file where logging messages will be written

    """
    # Total annual and seasonal emissions.
    conn.executescript("""CREATE TEMPORARY TABLE fy_emission_summary
    (state TEXT NOT NULL COLLATE NOCASE,
    period_pollutant TEXT NOT NULL COLLATE NOCASE,
    total_tons REAL,
    PRIMARY KEY (state, period_pollutant));

    INSERT INTO fy_emission_summary(state, period_pollutant, total_tons)
    SELECT state, 'Annual SO2', SUM(so2_mass) / 2000.0
    FROM hourly_diagnostic_file
    GROUP BY state;

    INSERT INTO fy_emission_summary(state, period_pollutant, total_tons)
    SELECT state, 'Annual NOx', SUM(nox_mass) / 2000.0
    FROM hourly_diagnostic_file
    GROUP BY state;""")

    # 20120406 Updated to use ozone season from input variables.
    conn.execute("""INSERT INTO fy_emission_summary(state, period_pollutant, total_tons)
    SELECT state, 'OS NOx', SUM(nox_mass) / 2000.0
    FROM hourly_diagnostic_file
    WHERE calendar_hour BETWEEN ? AND ?
    GROUP BY state""", (ozone_start_hour, ozone_end_hour))

    conn.executescript("""INSERT INTO state_caps (state_abbreviation, cap_time_period_pollutant,
    cap_tons, year_applicable, fy_emissions, comments)
    SELECT stl.state_abbreviation, stl.cap_time_period || ' ' || stl.cap_pollutant,
    stl.cap_tons, stl.year_applicable, emis.total_tons, stl.comments
    FROM calc_state_total_listing stl
    JOIN fy_emission_summary emis
    ON stl.state_abbreviation = emis.state
    AND stl.cap_time_period || ' ' || stl.cap_pollutant = emis.period_pollutant;

    INSERT INTO group_caps (group_name, cap_time_period_pollutant,
    cap_tons, year_applicable, fy_emissions, comments)
    SELECT gtl.group_name, gtl.cap_time_period || ' ' || gtl.cap_pollutant,
    gtl.cap_tons, gtl.year_applicable, SUM(emis.total_tons), gtl.comments
    FROM calc_group_total_listing gtl
    JOIN fy_emission_summary emis
    ON gtl.states_included LIKE '%' || emis.state || '%'
    AND gtl.cap_time_period || ' ' || gtl.cap_pollutant = emis.period_pollutant
    GROUP BY gtl.group_name, gtl.cap_time_period || ' ' || gtl.cap_pollutant,
    gtl.year_applicable;""")


def summarize_future_capacity(conn, logfile):
    """28: Summarize future generating capacity.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    # Total base year and future year activity.
    # RW 9/14/2015 Include annual summary of transfers for region+fuel.
    conn.executescript("""INSERT INTO capacity_and_fy_demand
    (ertac_region, ertac_fuel_unit_type_bin, by_gen, by_hi)
    SELECT ertac_region, ertac_fuel_unit_type_bin, SUM(gload), SUM(heat_input)
    FROM calc_hourly_base
    GROUP BY ertac_region, ertac_fuel_unit_type_bin;

    UPDATE capacity_and_fy_demand
    SET fy_gen_including_transfers = (SELECT SUM(gload) FROM hourly_diagnostic_file hourly
    WHERE hourly.ertac_region = capacity_and_fy_demand.ertac_region
    AND hourly.ertac_fuel_unit_type_bin = capacity_and_fy_demand.ertac_fuel_unit_type_bin);

    UPDATE capacity_and_fy_demand
    SET fy_hi = (SELECT SUM(heat_input) FROM hourly_diagnostic_file hourly
    WHERE hourly.ertac_region = capacity_and_fy_demand.ertac_region
    AND hourly.ertac_fuel_unit_type_bin = capacity_and_fy_demand.ertac_fuel_unit_type_bin);

    UPDATE capacity_and_fy_demand
    SET fy_transfers = (SELECT SUM(net_demand_change) FROM calc_demand_transfer_summary cdts
    WHERE cdts.transfer_region = capacity_and_fy_demand.ertac_region
    AND cdts.transfer_fuel = capacity_and_fy_demand.ertac_fuel_unit_type_bin);

    UPDATE capacity_and_fy_demand
    SET new_gen = (SELECT SUM(1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate) FROM calc_updated_uaf uaf
    WHERE uaf.ertac_region = capacity_and_fy_demand.ertac_region
    AND uaf.ertac_fuel_unit_type_bin = capacity_and_fy_demand.ertac_fuel_unit_type_bin
    AND uaf.camd_by_hourly_data_type = 'NEW');

    INSERT INTO capacity_and_fy_reserve
    (ertac_region, reserve_met, max_deficit)
    SELECT ertac_region, 'Y', MAX(deficit)
    FROM reserve_capacity_needed
    GROUP BY ertac_region;

    UPDATE capacity_and_fy_reserve
    SET total_transfers = (SELECT SUM(cafd.fy_transfers)
    FROM capacity_and_fy_demand cafd
    WHERE cafd.ertac_region = capacity_and_fy_reserve.ertac_region);

    UPDATE capacity_and_fy_reserve
    SET reserve_met = 'N'
    WHERE max_deficit > 0.0;""")

    # jmj 6/2/2017 add a check to make sure growth rates were honored
    #JMJ 1/18/2024 adding an edit to the check for no growth in the base year
    for (region, unit_type, calc_growth_rate, growth_rate) in conn.execute(
            """SELECT cfd.ertac_region, cfd.ertac_fuel_unit_type_bin, CASE WHEN by_gen = 0 THEN NULL ELSE (COALESCE(fy_gen_including_transfers,0)-COALESCE(fy_transfers,0))/by_gen END,COALESCE(annual_growth_factor,0) 
                FROM capacity_and_fy_demand cfd 
                JOIN calc_growth_rates cgr 
                ON cfd.ertac_region = cgr.ertac_region 
                AND cfd.ertac_fuel_unit_type_bin = cgr.ertac_fuel_unit_type_bin""").fetchall():
        if calc_growth_rate is None:
            logging.info(
                "Warning: could not determine if AGR was honored because AGR could not be calculated since BY gen = 0 for region: " + region + ", fuel/unit type bin: " + unit_type)
            print(
                "Warning: could not determine if AGR was honored because AGR could not be calculated since BY gen = 0 for region: " + region + ", fuel/unit type bin: " + unit_type, file=logfile)
        else:
            if round(calc_growth_rate, 12) != round(growth_rate, 12):
                logging.info(
                    "Warning: annual growth rate was not honored for region: " + region + ", fuel/unit type bin: " + unit_type + ", allocated annual growth rate: " + str(
                        round(calc_growth_rate, 12)) + ", input variable annual growth rate: " + str(
                        round(growth_rate, 12)))
                print(
                    "Warning: annual growth rate was not honored for region: " + region + ", fuel/unit type bin: " + unit_type + ", allocated annual growth rate: " + str(
                        calc_growth_rate) + ", input variable annual growth rate: " + str(growth_rate), file=logfile)


def write_final_data(conn, out_prefix, logfile):
    """Write out projected ERTAC EGU data reports.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    out_prefix -- optional prefix added to each output file name
    logfile -- file where logging messages will be written

    """
    # Final output data is exported as CSV files for reporting and use with
    # other programs.
    ertac_lib.export_table_to_csv('calc_generation_parms', out_prefix, 'calc_generation_parms_v2.csv', conn,
                                  ertac_tables.generation_parms_columns, logfile)
    ertac_lib.export_table_to_csv('calc_generation_proxy', out_prefix, 'calc_generation_proxy.csv', conn,
                                  ertac_tables.generation_proxy_columns, logfile)
    ertac_lib.export_table_to_csv('calc_unit_hierarchy', out_prefix, 'calc_unit_hierarchy.csv', conn,
                                  ertac_tables.unit_hierarchy_columns, logfile)
    ertac_lib.export_table_to_csv('calc_updated_uaf', out_prefix, 'calc_updated_uaf_v2.csv', conn,
                                  ertac_tables.calc_uaf_columns, logfile)
    ertac_lib.export_table_to_csv('demand_generation_deficit', out_prefix, 'demand_generation_deficit_v2.csv', conn,
                                  ertac_reports.demand_generation_deficit, logfile)
    ertac_lib.export_table_to_csv('generic_units_created', out_prefix, 'generic_units_created.csv', conn,
                                  ertac_reports.generic_units_created, logfile)
    ertac_lib.export_table_to_csv('reserve_capacity_needed', out_prefix, 'reserve_capacity_needed_v2.csv', conn,
                                  ertac_reports.reserve_capacity_needed, logfile)
    ertac_lib.export_table_to_csv('unit_level_activity', out_prefix, 'unit_level_activity_v2.csv', conn,
                                  ertac_reports.unit_level_activity, logfile)
    ertac_lib.export_table_to_csv('cap_analysis', out_prefix, 'cap_analysis.csv', conn, ertac_reports.cap_analysis,
                                  logfile)
    ertac_lib.export_table_to_csv('unit_generic_controls', out_prefix, 'unit_generic_controls.csv', conn,
                                  ertac_reports.unit_generic_controls, logfile)
    ertac_lib.export_table_to_csv('capacity_and_fy_demand', out_prefix, 'capacity_and_fy_demand_v2.csv', conn,
                                  ertac_reports.capacity_and_fy_demand, logfile)
    ertac_lib.export_table_to_csv('capacity_and_fy_reserve', out_prefix, 'capacity_and_fy_reserve_v2.csv', conn,
                                  ertac_reports.capacity_and_fy_reserve, logfile)
    ertac_lib.export_table_to_csv('state_caps', out_prefix, 'state_caps.csv', conn, ertac_reports.state_caps, logfile)
    ertac_lib.export_table_to_csv('group_caps', out_prefix, 'group_caps.csv', conn, ertac_reports.group_caps, logfile)
    ertac_lib.export_table_to_csv('hourly_diagnostic_file', out_prefix, 'hourly_diagnostic_file_v2.csv', conn,
                                  ertac_reports.hourly_diagnostic_file, logfile)


if __name__ == '__main__':
    sys.exit(main())
