#!/usr/bin/python

# ertac_preprocess.py

"""Preprocessing steps for ERTAC EGU projection"""

VERSION = "3.0"
# Updated to version 3.0 as of November 2, 2022

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

import sys

try:
    import getopt, logging, os, time, math
except ImportError:
    print("Fatal error: can't import all required modules.", file=sys.stderr)
    print("Run python -V to find your Python version.", file=sys.stderr)
    raise

# modifying every earlier copy of the model code in order to load SQLite3 into
# his unexpected older Python installation.
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
    import ertac_lib, ertac_tables
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
# Similarly, output file names can also have an optional prefix specified with
# the -o command-line switch.
#
# All Python and SQL code files and the fixed CSV files for lookup tables can
# (and should) be in a separate non-data directory, but must be kept together.
#
# Example usage: assuming for example that there is a valid set of input CSV
# files present in ~/egu_data and that the program code files are located in
# ~/ertac2code, then change into the data directory and run the preprocessing
# program by the following two commands:
#
# cd ~/egu_data
# ~/ertac2code/ertac_preprocess.py


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
  --suppress_pr   suppress partial year reporter messages.
  --keep_feb29 do not delete leap year data (need for SMOKE ready runs)
""") % progname)


def main(argv=None):
    # Main preprocessing program begins here.
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "hdqvi:o:",
                                   ["help", "debug", "quiet", "verbose", "input-prefix=", "output-prefix=",
                                    "suppress_pr","keep_feb29"])
    except getopt.GetoptError as err:
        print()
        print((str(err)))
        usage(argv[0])
        return 2

    debug_level = "INFO"
    input_prefix = ""
    output_prefix = ""
    suppress_pr_messages = False
    remove_feb29 = True

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

        # jmj 11/20/2013 adding suppression of partial year reporter messages
        elif opt in "--suppress_pr":
            suppress_pr_messages = True
            
        # jmj 10/19/2022 adding the ability to keep the new year manually for runs needed for smoke processing
        elif opt in "--keep_feb29":
            remove_feb29 = False
        else:
            assert False, "unhandled option"

    if debug_level == "DEBUG":
        # Detailed logging to file for postmortem analysis.
        logging.basicConfig(
            filename=output_prefix + 'ertac_preprocess_debug_log.txt',
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
    logfilename = output_prefix + 'ertac_egu_preprocessor_log.txt'
    try:
        logfile = open(logfilename, 'w')
    except IOError:
        print("Log file: " + logfilename + " -- Could not be written.  Program will terminate.", file=sys.stderr)
        raise

    # Identify versions of Python and SQLite library, and record in log file.
    # Also log revision dates of model code.
    logging.info("Program started at " + time.asctime())
    logging.info("ERTAC EGU preprocessor version: " + VERSION)
    logging.info("Using ertac_lib version: " + ertac_lib.VERSION)
    logging.info("Using ertac_tables version: " + ertac_tables.VERSION)
    logging.info("Running under python version: " + sys.version)
    logging.info("Using sqlite3 module version: " + sqlite3.version)
    logging.info("Linked against sqlite3 database library version: " + sqlite3.sqlite_version)
    print("Program started at " + time.asctime(), file=logfile)
    print("ERTAC EGU preprocessor version: " + VERSION, file=logfile)
    print("Using ertac_lib version: " + ertac_lib.VERSION, file=logfile)
    print("Using ertac_tables version: " + ertac_tables.VERSION, file=logfile)
    print("Running under python version: " + sys.version, file=logfile)
    print("Using sqlite3 module version: " + sqlite3.version, file=logfile)
    print("Linked against sqlite3 database library version: " + sqlite3.sqlite_version, file=logfile)
    print("Model code versions:", file=logfile)
    for file_name in [os.path.basename(sys.argv[0]), 'ertac_lib.py', 'ertac_tables.py',
                      'create_preprocessor_input_tables.sql', 'create_preprocessor_output_tables.sql']:
        print("  " + file_name + ": " + time.ctime(os.path.getmtime(os.path.join(sys.path[0], file_name))),
              file=logfile)

    # Create and populate the working database.
    try:
        dbconn = sqlite3.connect('')
        dbconn.text_factory = str
    except:
        print("Error while opening database.  Program will terminate.", file=sys.stderr)
        raise

    logging.info("Creating database tables.")
    ertac_lib.run_script_file('create_preprocessor_input_tables.sql', dbconn)
    ertac_lib.run_script_file('create_preprocessor_output_tables.sql', dbconn)

    # 1, 1.02, 1.03, 2, 3: Load input CSV data into tables, rejecting any rows
    # that can't be used.
    logging.info("Loading input data:")
    load_initial_data(dbconn, input_prefix, logfile)
    logging.info("Finished loading input data.")

    # Cross-table consistency checks done before anything else.  Problems here
    # may prevent rest of model from running.
    # 1. Years: valid base and future years in input variables, growth rates,
    # and in CAMD and non-CAMD hourly data.
    # 2. Ozone season start/end dates: must be consistent and valid throughout
    # input variables.
    # 3. Regions, fuel bins: matching in input variables, growth rates, and UAF.
    # 4. Facilities: list of facility IDs in input variables for new unit
    # placement should all be found in UAF with same region/fuel; facility ID
    # and state should be consistent in hourly data and UAF.
    # 5. Units: facility/unit IDs in control/emissions, hourly data, and UAF.

    # Years
    logging.info("Validating base_year and future_year for input variables, growth rates, and hourly data.")
    (base_year, future_year) = validate_base_and_future_years(dbconn, logfile)

    # 20120406 Added ozone season validation
    logging.info("Validating ozone season start/end dates for input variables.")
    (ozone_start_base, ozone_end_base) = validate_ozone_season(dbconn, base_year, logfile)

    # Regions, fuel bins
    logging.info("Validating regions and fuel bins for input variables, growth rates, and UAF.")
    validate_regions_and_fuel_bins(dbconn, logfile)

    # Facilities
    logging.info("Validating facilities for input variables, hourly data, and UAF.")
    validate_facilities(dbconn, logfile)

    # Units
    logging.info("Validating facility/units for control/emissions, hourly data, and UAF.")
    validate_units(dbconn, logfile)

    # Within-table consistency checks next, for multiple-field interactions that
    # can't be tested by the simpler range checks.
    # 1. Check in UAF for consistent facility data, and for appropriate
    # online/offline dates for units switching fuels.
    # 2. Check in growth rates that peak->formula transition is before
    # formula->nonpeak transition.
    # 3. Check in input variables that new unit min size <= new unit max size.
    # 4. Check in control/emissions that factor_start_date < factor_end_date, if
    # both are present, for non-overlapping date ranges if multiple factors
    # exist for the same pollutant at the same unit, and that either
    # emission_rate or control_efficiency is present.
    # 5. Check in group total listing that list of states is consistent for each
    # group, and that all states are valid.
    # 6. For V2, check in demand transfer table that same region+fuel isn't used
    # as both origin and destination at the same hour.

    # UAF
    logging.info("Checking UAF for consistent facility data and unit online/offline data.")
    check_uaf_consistency(dbconn, base_year, logfile)

    # Growth rates
    logging.info("Checking growth rates for consistent transition hours.")
    check_growth_rate_consistency(dbconn, logfile)

    # Input variables
    logging.info("Checking input variables for consistent heat rate and EF min/max and new unit sizes.")
    check_input_variable_consistency(dbconn, logfile)

    # Control/emissions
    logging.info("Checking control/emissions for consistent dates, and presence of rate or efficiency.")
    check_control_emissions_consistency(dbconn, base_year, logfile)
    check_seasonal_control_emissions_consistency(dbconn, base_year, future_year, logfile)

    # Group total listing
    logging.info("Checking group total listing for consistent lists of valid states.")
    check_group_total_listing_consistency(dbconn, logfile)

    # Demand transfers
    logging.info("Checking demand transfers for consistent hourly origin/destination.")
    check_demand_transfer_consistency(dbconn, logfile)

    # 1.01: Delete hourly data from non-EGU units.
    logging.info("Removing non-EGU data.")
    remove_non_egu_data(dbconn, logfile)

    # 1.02, 1.03: Run range checks and warn about outliers.
    logging.info("Checking input data ranges:")
    check_initial_data_ranges(dbconn, logfile)
    logging.info("Finished checking input data ranges.")

    # Copy remaining input data to calc_* tables before updating growth rates or
    # UAF data.  This will make sure that format changes made to the inputs,
    # such as converted dates, are passed to the second phase, and that a
    # complete matched set of output files from the first phase (optionally
    # prefixed according to the -o switch for output) can be read in during the
    # second phase, using the -i switch for the input prefix.
    # RW 8/26/2015 Since calc UAF now has many extra columns that aren't in the
    # initial UAF, have to name columns for INSERT.
    logging.info("Copying remaining input data to calc_* tables.")
    dbconn.executescript("""DELETE FROM calc_updated_uaf;
    INSERT INTO calc_updated_uaf
    (orispl_code, unitid, form860_plant_id, fips_code, county_code, county_name,
    state, needs_unit_id, form860_unit_id, plant_latitude, plant_longitude,
    inventory_stack_id, facility_name, needs_ipm_region, nerc_main_region,
    eia_region_old_nerc, ertac_region, other_consuming_regions, camd_by_hourly_data_type,
    annual_hi_partials, camd_by_operating_status, camd_stack_info, online_start_date,
    offline_start_date, primary_fuel_type, main_fuel_characteristics, secondary_or_substitute_fuel,
    prime_mover_generator_unit_type, camd_unit_type, ertac_fuel_unit_type_bin,
    max_ertac_hi_hourly_summer, max_ertac_hi_hourly_winter, hourly_base_max_actual_hi,
    nameplate_capacity, max_summer_capacity, max_winter_capacity, max_unit_heat_input,
    calculated_by_uf, max_annual_state_uf, max_annual_ertac_uf, operating_hours_by,
    max_by_hourly_gload, max_by_hourly_sload, nominal_heat_rate, calc_by_average_heat_rate,
    ertac_heat_rate, unit_annual_capacity_limit, unit_max_optimal_load_threshold,
    unit_min_optimal_load_threshold, unit_ownership_code, multiple_ownership_notation,
    secondary_owner, tertiary_owner, new_unit_flag, capacity_limited_unit_flag,
    modifier_email_address, unit_completeness_check, hours_cap, program_codes)
    SELECT * FROM ertac_initial_uaf;

    DELETE FROM calc_growth_rates;
    INSERT INTO calc_growth_rates
    SELECT * FROM ertac_growth_rates;

    DELETE FROM calc_input_variables;
    INSERT INTO calc_input_variables
    SELECT * FROM ertac_input_variables;

    DELETE FROM calc_demand_transfers;
    INSERT INTO calc_demand_transfers
    SELECT * FROM ertac_demand_transfers;

    -- Need to have summary of all tranfers in or out, even if other endpoint is
    -- outside the scope of the current model run.
    DELETE FROM calc_demand_transfer_summary;
    INSERT INTO calc_demand_transfer_summary (transfer_region, transfer_fuel, calendar_hour, net_demand_change)
    SELECT transfer_region, transfer_fuel, calendar_hour, SUM(demand_transfer)
    FROM
        (SELECT destination_region AS transfer_region, destination_fuel AS transfer_fuel, calendar_hour, demand_transfer
        FROM ertac_demand_transfers
        UNION ALL
        SELECT origin_region AS transfer_region, origin_fuel AS transfer_fuel, calendar_hour, -demand_transfer
        FROM ertac_demand_transfers)
    GROUP BY transfer_region, transfer_fuel, calendar_hour;

    DELETE FROM calc_control_emissions;
    INSERT INTO calc_control_emissions
    SELECT * FROM ertac_control_emissions;

    DELETE FROM calc_state_total_listing;
    INSERT INTO calc_state_total_listing
    SELECT * FROM state_total_listing;

    DELETE FROM calc_group_total_listing;
    INSERT INTO calc_group_total_listing
    SELECT * FROM group_total_listing;""")

    # 1.04: jmj function to convert seasonal control into regular controls
    insert_seasonal_controls(dbconn, base_year, future_year, logfile)

    # Fill in unspecified online/offline dates with sentinel values outside
    # normal date range.
    dbconn.execute("""UPDATE calc_updated_uaf
    SET online_start_date = ?
    WHERE online_start_date IS NULL""", (ertac_lib.online_default,))
    dbconn.execute("""UPDATE calc_updated_uaf
    SET offline_start_date = ?
    WHERE offline_start_date IS NULL""", (ertac_lib.offline_default,))

    # Due to fuel switching, have to link base year hourly data to correct fuel
    # bin for that unit at that time; future year could have different fuel,
    # with same unit marked as new.  In worst cases, fuel switch may have taken
    # place sometime during base year, or could happen during the projected
    # future year.

    # Copy CAMD hourly data, with region and fuel bin from UAF, into calculated
    # hourly base.
    logging.info("Copying base-year CAMD hourly data with region and fuel bin from UAF.")
    copy_base_year_hourly(dbconn, base_year, logfile)

    # 1.0?: jmj 6/10/2019 adding base year operating calculations prior to filling in partial year reports
    logging.info("Updating base year hourly emission rates.")
    calculate_base_year_emission_rates(dbconn, logfile)

    # Add/replace any non-CAMD data into calculated hourly base.
    logging.info("Copying base-year non-CAMD hourly data with region and fuel bin from UAF.")
    copy_base_year_noncamd_hourly(dbconn, base_year, logfile)

    # 20120503 The calendar_hours table can't be filled with date/time from
    # calc_hourly_base until after calc_hourly_base is filled.
    ertac_lib.make_calendar_hours(base_year, future_year, dbconn)

    # 20120203 New edit check to warn about planned new units in UAF for
    # region/fuel with no activity in base year.
    logging.info("Checking UAF for new units in region/fuel with no base-year activity.")
    check_uaf_new_units_no_base_year(dbconn, base_year, future_year, logfile)

    # 5/2/2014 jmj adding a check to make sure there is no base year data for a unit after its retirement date
    logging.info("Checking UAF for retired units in region/fuel with base-year activity.")
    check_uaf_retired_units_with_base_year(dbconn, base_year, logfile)

    # 20120301 New edit check to warn about region/fuel with fewer than 10 units
    # active in base year.
    logging.info("Checking base-year hourly data for region/fuel with fewer than 10 units.")
    check_hourly_base_scarce_region_fuel(dbconn, logfile)

    # 20120430 New edit check to warn about region/fuel where all units retired.
    logging.info("Checking for region/fuel with all units retired in future.")
    check_all_units_retired(dbconn, future_year, logfile)

    # If base year was a leap year but future year isn't, delete recorded
    # Feb. 29 data before ranking days/hours for hierarchy.
    # jmj 10/19/2022 adding a flag to allow the leap year to be kept so that projection from leap year can work in SMOKE
    if remove_feb29 and ertac_lib.is_leap_year(base_year) and not ertac_lib.is_leap_year(future_year):
        logging.info("Deleting February 29 base year hourly data.")
        delete_feb29_data(dbconn, base_year, future_year, logfile)

    # 2.03: Fill in missing GLOAD in calc_hourly_base for units that report SLOAD
    # instead.
    # RW 9/17/2015 Moved this back before temporal ranking, so units reporting
    # only SLOAD will be able contribute GLOAD to totals.  Don't know why Joseph
    # had moved this to go after temporal hierarchy determination.
    logging.info("Filling GLOAD for units that report only SLOAD.")
    fill_gload_from_sload(dbconn, logfile)

    # 2.04: RW 8/26/2015 Compute and store the per-unit statistical ranges, annual and
    # OS/non-OS averages in the UAF, along with region/fuel hard lower and upper
    # limits for heat rate and EFs.
    logging.info("Calculating unit operating statistics to store in UAF.")
    calc_unit_stats(dbconn, logfile)

    # 1.05: Fill in the 1-hour, 6-hour, and 24-hour temporal hierarchies within
    # each region and fuel bin, based on decreasing total hourly GLOAD.
    logging.info("Filling temporal hierarchies.")
    fill_temporal_hierarchies(dbconn, logfile)

    # 2.05: jmj 11/20/2013 adding base year operating calculations prior to filling in partial year reports
    logging.info("Calculating base year operating hours.")
    calculate_base_year_op_hours(dbconn, logfile)

    # 1.06, 1.07: Fill remaining hours of any partial-year data using flat
    # profile (if part-HI supplied) or 0, reporting summary.
    # jmj adding a variable to allow suppression of the partial year messages
    logging.info("Filling partial-year data.")
    fill_partial_year(dbconn, base_year, suppress_pr_messages, logfile)

    # Need to summarize base year hourly generation in calc_generation_parms
    # before calculating hourly growth rates, since non-peak growth rate is
    # affected by base year generation.
    logging.info("Filling in base year generation in calc_generation_parms.")
    fill_base_year_calc_generation_parms(dbconn, base_year, future_year, logfile)

    # 3.01: Then can calculate non-peak growth rate values and hour-specific
    # growth rates for each region/fuel.
    logging.info("Calculating non-peak growth factors.")
    calculate_non_peak_growth_factors(dbconn, logfile)

    # 1.08: Then can calculate future year hourly growth and generation, further
    # updating calc_generation_parms.
    logging.info("Calculating future hourly growth and generation.")
    calculate_future_generation_growth(dbconn, logfile)

    # For V2, add net demand transfers to calc_generation_parms, independently
    # from effects of hourly growth rates.
    logging.info("Updating calc_generation_parms for demand tranfers.")
    update_calc_generation_parms_transfers(dbconn, logfile)

    # 2.06: Calculate base year average heat rate for every unit; use that value
    # to update ertac heat rate in UAF unless state-supplied nominal heat rate
    # is included.
    logging.info("Calculating average heat rate and ERTAC heat rate.")
    calculate_heat_rates(dbconn, future_year, logfile)

    # 2.07: Calculate percentile-based max ERTAC heat input hourly summer for
    # every unit.
    logging.info("Calculating max heat inputs.")
    calculate_heat_inputs(dbconn, logfile)

    # 2.06: Calculate percentile-based unit optimal load thresholds.
    logging.info("Calculating optimal load thresholds.")
    calculate_optimal_loads(dbconn, logfile)

    # 2.08: Calculate max_annual_ertac_uf for every unit, from larger of actual
    # or defaults, unless max_annual_state_uf overrides all others.
    logging.info("Calculating utilization fractions.")
    calculate_utilization_fractions(dbconn, base_year, future_year, logfile)

    # 1.09: Determine unit allocation order based on utilization within each
    # region/fuel bin.
    logging.info("Setting unit allocation order.")
    fill_unit_hierarchy(dbconn, base_year, future_year, logfile)

    # Calculate and update max_by_hourly_gload, needed for calculating proxy
    # generation.
    logging.info("Calculating max gload for existing units.")
    calc_max_gload(dbconn, logfile)

    # 2.09: Calculate hourly proxy generation for new units.
    logging.info("Calculating hourly proxy generation for new units.")
    calc_hourly_proxy(dbconn, base_year, future_year, logfile)

    # Clear out sentinel values for online/offline dates before UAF range checks
    # and data export.
    dbconn.execute("""UPDATE calc_updated_uaf
    SET online_start_date = NULL
    WHERE online_start_date = ?""", (ertac_lib.online_default,))
    dbconn.execute("""UPDATE calc_updated_uaf
    SET offline_start_date = NULL
    WHERE offline_start_date = ?""", (ertac_lib.offline_default,))

    # 2.10, 2.11: Run range checks on calculated UAF, for warnings before
    # projection phase.
    logging.info("Checking output data ranges:")
    ertac_lib.check_data_ranges('calc_updated_uaf', dbconn, ertac_tables.uaf_columns, logfile)

    # 1.10, 2.12: Export intermediate files as input to post-processing steps.
    logging.info("Writing output data:")
    write_calculated_data(dbconn, output_prefix, logfile)
    logging.info("Finished writing output data.")
    dbconn.close()
    logging.info("Program ended at " + time.asctime())
    print(file=logfile)
    print("Program ended at " + time.asctime(), file=logfile)

    # End of main routine


def load_initial_data(conn, in_prefix, logfile):
    """1, 1.02, 1.03, 2, 3: Load lookup tables and initial ERTAC EGU data for preprocessing.

    Keyword arguments:
    conn -- a valid database connection where the data will be stored
    in_prefix -- optional prefix added to each input file name
    logfile -- file where logging messages will be written

    """

    # jmj fails when a necessary file is not load 150413
    ertac_lib.load_csv_into_table(None, os.path.join(os.path.relpath(sys.path[0]), 'states.csv'), 'states', conn,
                                  ertac_tables.states_columns, logfile)
    ertac_lib.load_csv_into_table(None, os.path.join(os.path.relpath(sys.path[0]), 'counties.csv'), 'counties', conn,
                                  ertac_tables.counties_columns, logfile)

    # This section will reject any input rows that are missing required fields,
    # have unreadable data, or violate key constraints, because it is impossible
    # to store that data in the database tables.
    if not ertac_lib.load_csv_into_table(in_prefix, 'ertac_initial_uaf_v2.csv', 'ertac_initial_uaf', conn,
                                         ertac_tables.uaf_columns, logfile):
        print("Fatal error: could not load necessary file ertac_initial_uaf_v2", file=sys.stderr)
        sys.exit(1)
    if not ertac_lib.load_csv_into_table(in_prefix, 'ertac_growth_rates.csv', 'ertac_growth_rates', conn,
                                         ertac_tables.growth_rate_columns, logfile):
        print("Fatal error: could not load necessary file ertac_growth_rates", file=sys.stderr)
        sys.exit(1)
    if not ertac_lib.load_csv_into_table(in_prefix, 'ertac_input_variables_v2.csv', 'ertac_input_variables', conn,
                                         ertac_tables.input_variable_columns, logfile):
        print("Fatal error: could not load necessary file ertac_input_variables_v2", file=sys.stderr)
        sys.exit(1)
    # For V2, added new optional input for demand transfers.
    ertac_lib.load_csv_into_table(in_prefix, 'ertac_demand_transfer.csv', 'ertac_demand_transfers', conn,
                                  ertac_tables.demand_transfer_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'ertac_control_emissions.csv', 'ertac_control_emissions', conn,
                                  ertac_tables.control_emission_columns, logfile)
    # jmj 10/24/2013 adding a new spreadsheet with seasonal controls
    ertac_lib.load_csv_into_table(in_prefix, 'ertac_seasonal_control_emissions.csv', 'ertac_seasonal_control_emissions',
                                  conn, ertac_tables.seasonal_control_emission_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'state_total_listing.csv', 'state_total_listing', conn,
                                  ertac_tables.state_total_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'group_total_listing.csv', 'group_total_listing', conn,
                                  ertac_tables.group_total_columns, logfile)

    # jmj 3/31/2014 moved these to the bottom so that any failures in other files happen first given the length of time to load these
    # if not ertac_lib.load_csv_into_table(in_prefix, 'camd_hourly_base.csv', 'camd_hourly_base', conn, ertac_tables.camd_columns, logfile) or ertac_lib.load_csv_into_table(in_prefix, 'ertac_hourly_noncamd.csv', 'ertac_hourly_noncamd', conn, ertac_tables.camd_columns, logfile):
    #    print >> sys.stderr, "Fatal error: could not load necessary files camd_hourly_base or noncamd_hourly_base"
    #    sys.exit(1)

    # For V2, partially reverted to allow either or both hourly files missing,
    # so basic QA of other inputs by preprocessor can still be done without a
    # fatal error if these are absent.
    ertac_lib.load_csv_into_table(in_prefix, 'camd_hourly_base.csv', 'camd_hourly_base', conn,
                                  ertac_tables.camd_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'ertac_hourly_noncamd.csv', 'ertac_hourly_noncamd', conn,
                                  ertac_tables.camd_columns, logfile)

    if ertac_tables.fuel_set != ertac_tables.default_fuel_set:
        logging.info("Default fuel set overwritten.  Using: " + str(ertac_tables.fuel_set))
        print(file=logfile)
        print("Default fuel set overwritten.  Using: " + str(ertac_tables.fuel_set), file=logfile)

    if ertac_tables.state_set != ertac_tables.default_state_set:
        logging.info("Default state set overwritten.  Using: " + str(ertac_tables.state_set))
        print(file=logfile)
        print("Default state set overwritten.  Using: " + str(ertac_tables.state_set), file=logfile)

    if os.path.isfile(in_prefix + 'fuel_unit_type_bins.csv'):
        print(file=logfile)
        print(
            "Warning: found file " + in_prefix + "fuel_unit_type_bins.csv, which will not be read as an input file.  To have it read as such it must be in the same directory as the code base and have no prefix.",
            file=logfile)

    if os.path.isfile(in_prefix + 'states.csv'):
        print(file=logfile)
        print(
            "Warning: found file " + in_prefix + "statesS.csv, which will not be read as an input file.  To have it read as such it must be in the same directory as the code base and have no prefix.",
            file=logfile)


def validate_base_and_future_years(conn, logfile):
    """Validate the base_year and future_year for input variables, growth rates, and hourly data.

    Returns one base_year and one future_year if uniquely specified, or
    terminates if there are fatal unrecoverable problems.  Missing or
    inconsistent years can not be used in the model.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    # Check the following:
    # 1. ERTAC_INPUT_VARIABLES should have identical values for BASE_YEAR and
    # FUTURE_YEAR on every row, with BASE_YEAR < FUTURE_YEAR.
    # 2. ERTAC_GROWTH_RATES should have identical FUTURE_YEAR on every row, and
    # should match ERTAC_INPUT_VARIABLES.FUTURE_YEAR value.
    # 3. CAMD and non-CAMD hourly data should all come from BASE_YEAR.
    # Any date mismatches are considered fatal errors and must be corrected.
    print(file=logfile)
    print("Validating base_year and future_year for input variables, growth rates, and hourly data.", file=logfile)

    year_list = conn.execute("SELECT DISTINCT base_year, future_year FROM ertac_input_variables").fetchall()
    if len(year_list) == 0:
        ertac_lib.log_and_exit(logfile, "Error: ERTAC_INPUT_VARIABLES is empty.")
    elif len(year_list) > 1:
        for years in year_list:
            print("  " + ertac_lib.nice_str(years), file=logfile)
        ertac_lib.log_and_exit(logfile,
                               "Error: ERTAC_INPUT_VARIABLES does not have exactly one BASE_YEAR and FUTURE_YEAR.")

    (base_year, future_year) = year_list[0]
    # jmj allows base year = future year runs 150413
    if base_year > future_year:
        ertac_lib.log_and_exit(logfile, "Error: ERTAC_INPUT_VARIABLES has BASE_YEAR > FUTURE_YEAR.")

    growth_year_list = conn.execute("SELECT DISTINCT base_year, future_year FROM ertac_growth_rates").fetchall()
    if len(growth_year_list) != 1:
        for years in growth_year_list:
            print("  " + ertac_lib.nice_str(years), file=logfile)
        ertac_lib.log_and_exit(logfile,
                               "Error: ERTAC_GROWTH_RATES does not have exactly one BASE_YEAR and FUTURE_YEAR.")

    (growth_rate_base_year, growth_rate_future_year) = growth_year_list[0]
    # jmj allows base year = future year runs 150413
    if growth_rate_base_year > growth_rate_future_year:
        ertac_lib.log_and_exit(logfile, "Error: ERTAC_GROWTH_RATES has BASE_YEAR > FUTURE_YEAR.")

    if growth_rate_base_year != base_year or growth_rate_future_year != future_year:
        ertac_lib.log_and_exit(logfile,
                               "Error: ERTAC_GROWTH_RATES does not match BASE_YEAR and FUTURE_YEAR from ERTAC_INPUT_VARIABLES.")

    camd_year_list = conn.execute("SELECT DISTINCT SUBSTR(op_date, 1, 4) FROM camd_hourly_base").fetchall()
    if len(camd_year_list) > 1:
        for (year,) in camd_year_list:
            print("  " + year, file=logfile)
        ertac_lib.log_and_exit(logfile, "Error: CAMD_HOURLY_BASE has data from multiple base years.")

    if len(camd_year_list) == 1:
        (camd_base_year,) = camd_year_list[0]
        if camd_base_year != base_year:
            ertac_lib.log_and_exit(logfile,
                                   "Error: CAMD_HOURLY_BASE has data from different base year than ERTAC_INPUT_VARIABLES.")

    noncamd_year_list = conn.execute("SELECT DISTINCT SUBSTR(op_date, 1, 4) FROM ertac_hourly_noncamd").fetchall()
    if len(noncamd_year_list) > 1:
        for (year,) in noncamd_year_list:
            print("  " + year, file=logfile)
        ertac_lib.log_and_exit(logfile, "Error: ERTAC_HOURLY_NONCAMD has data from multiple base years.")

    if len(noncamd_year_list) == 1:
        (noncamd_base_year,) = noncamd_year_list[0]
        if noncamd_base_year != base_year:
            ertac_lib.log_and_exit(logfile,
                                   "Error: ERTAC_HOURLY_NONCAMD has data from different base year than ERTAC_INPUT_VARIABLES.")

    # If nothing failed for base_year or future_year, return the good values.
    return base_year, future_year


def validate_ozone_season(conn, base_year, logfile):
    """Validate the ozone season start/end dates for input variables.

    Returns ozone season start and end dates within base year, or terminates if
    there are fatal unrecoverable problems.  Inconsistent or invalid ozone dates
    can not be used in the model.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year for the projection
    logfile -- file where logging messages will be written

    """
    # Check the following:

    # 1. ERTAC_INPUT_VARIABLES should have consistent values for
    # OZONE_START_DATE and OZONE_END_DATE on every row.
    # 2. The ozone season dates should have d-mmm formats with valid month
    # abbreviations.
    # 3. When converted into the base year, OZONE_START_BASE should be earlier
    # than OZONE_END_BASE.
    print(file=logfile)
    print("Validating ozone season start/end dates for input variables.", file=logfile)

    ozone_list = conn.execute("SELECT DISTINCT ozone_start_date, ozone_end_date FROM ertac_input_variables").fetchall()
    if len(ozone_list) != 1:
        for ozone_dates in ozone_list:
            print("  " + ertac_lib.nice_str(ozone_dates), file=logfile)
        ertac_lib.log_and_exit(logfile,
                               "Error: ERTAC_INPUT_VARIABLES does not have exactly one OZONE_START_DATE and OZONE_END_DATE.")

    (ozone_start, ozone_end) = ozone_list[0]
    ozone_start_base = ertac_lib.convert_ozone_date(ozone_start, base_year)
    ozone_end_base = ertac_lib.convert_ozone_date(ozone_end, base_year)
    if ozone_start_base is None or ozone_end_base is None:
        print("  " + ertac_lib.nice_str((ozone_start, ozone_end)), file=logfile)
        ertac_lib.log_and_exit(logfile,
                               "Error: ERTAC_INPUT_VARIABLES does not have valid OZONE_START_DATE and OZONE_END_DATE.")

    if ozone_start_base >= ozone_end_base:
        ertac_lib.log_and_exit(logfile, "Error: ERTAC_INPUT_VARIABLES has OZONE_START_DATE >= OZONE_END_DATE.")

    # If nothing failed, return the good values.
    return ozone_start_base, ozone_end_base


def validate_regions_and_fuel_bins(conn, logfile):
    """Validate regions and fuel bins for input variables, growth rates, and UAF.

    Checks for mismatched sets of ERTAC_REGION and ERTAC_FUEL_UNIT_TYPE_BIN
    across the ERTAC_INPUT_VARIABLES, ERTAC_GROWTH_RATES, and ERTAC_INITIAL_UAF
    tables.  Differences here are not necessarily fatal, but should be reviewed
    to make sure the model run conditions are properly set up.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    # Check the following:
    # 1. ERTAC_INPUT_VARIABLES and ERTAC_GROWTH_RATES should generally have the
    # same sets of regions and fuel bins.  Differences between them could skew
    # future projected operation.
    # 2. ERTAC_INPUT_VARIABLES and ERTAC_INITIAL_UAF may have different sets of
    # regions and fuel bins, as long as the UAF has a subset of what's found in
    # the input variables.  A "small" UAF is OK for running a limited model.
    # However, any region or fuel bin found in the UAF but not in the input
    # variables should be corrected.
    print(file=logfile)
    print("Validating regions and fuel bins for input variables, growth rates, and UAF.", file=logfile)

    region_fuel_input_not_growth = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin FROM ertac_input_variables
                                            EXCEPT SELECT ertac_region, ertac_fuel_unit_type_bin FROM ertac_growth_rates""").fetchall()

    region_fuel_growth_not_input = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin FROM ertac_growth_rates
                                            EXCEPT SELECT ertac_region, ertac_fuel_unit_type_bin FROM ertac_input_variables""").fetchall()

    region_fuel_input_not_uaf = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin FROM ertac_input_variables
                                         EXCEPT SELECT ertac_region, ertac_fuel_unit_type_bin FROM ertac_initial_uaf""").fetchall()

    region_fuel_uaf_not_input = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin FROM ertac_initial_uaf
                                         EXCEPT SELECT ertac_region, ertac_fuel_unit_type_bin FROM ertac_input_variables""").fetchall()

    if len(region_fuel_input_not_growth) > 0:
        print("Warning: regions and fuel bins found in input variables, but not in growth rates:", file=logfile)
        for reg_fuel in region_fuel_input_not_growth:
            print("  " + ertac_lib.nice_str(reg_fuel), file=logfile)

    if len(region_fuel_growth_not_input) > 0:
        print("Warning: regions and fuel bins found in growth rates, but not in input variables:", file=logfile)
        for reg_fuel in region_fuel_growth_not_input:
            print("  " + ertac_lib.nice_str(reg_fuel), file=logfile)

    if len(region_fuel_input_not_uaf) > 0:
        print("Warning: regions and fuel bins found in input variables, but not in UAF:", file=logfile)
        for reg_fuel in region_fuel_input_not_uaf:
            print("  " + ertac_lib.nice_str(reg_fuel), file=logfile)

    if len(region_fuel_uaf_not_input) > 0:
        print("Warning: regions and fuel bins found in UAF, but not in input variables:", file=logfile)
        for reg_fuel in region_fuel_uaf_not_input:
            print("  " + ertac_lib.nice_str(reg_fuel), file=logfile)


def validate_facilities(conn, logfile):
    """Validate consistent facility information in input variables, hourly data, and UAF.

    Check that list of facility IDs in input variables are all found in UAF, and
    have same region.  Check that facility ID and state are consistent in hourly
    data and UAF.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Validating facilities for input variables, hourly data, and UAF.", file=logfile)

    # Check list of oris plant IDs from input variables for generic new unit locations against UAF.
    plant_region_input_not_uaf = conn.execute("""SELECT facility_1, ertac_region FROM ertac_input_variables WHERE facility_1 IS NOT NULL
    UNION SELECT facility_2, ertac_region FROM ertac_input_variables WHERE facility_2 IS NOT NULL
    UNION SELECT facility_3, ertac_region FROM ertac_input_variables WHERE facility_3 IS NOT NULL
    UNION SELECT facility_4, ertac_region FROM ertac_input_variables WHERE facility_4 IS NOT NULL
    UNION SELECT facility_5, ertac_region FROM ertac_input_variables WHERE facility_5 IS NOT NULL
    UNION SELECT facility_6, ertac_region FROM ertac_input_variables WHERE facility_6 IS NOT NULL
    UNION SELECT facility_7, ertac_region FROM ertac_input_variables WHERE facility_7 IS NOT NULL
    UNION SELECT facility_8, ertac_region FROM ertac_input_variables WHERE facility_8 IS NOT NULL
    UNION SELECT facility_9, ertac_region FROM ertac_input_variables WHERE facility_9 IS NOT NULL
    UNION SELECT facility_10, ertac_region FROM ertac_input_variables WHERE facility_10 IS NOT NULL
    EXCEPT SELECT orispl_code, ertac_region FROM ertac_initial_uaf""").fetchall()

    if len(plant_region_input_not_uaf) > 0:
        print("Warning: facilities and regions found in input variables, but not in UAF:", file=logfile)
        for plant in plant_region_input_not_uaf:
            print("  " + ertac_lib.nice_str(plant), file=logfile)

    # Check oris plant ID and state from CAMD hourly data against UAF.
    plant_state_hourly_not_uaf = conn.execute("""SELECT orispl_code, state FROM camd_hourly_base
    EXCEPT SELECT orispl_code, state FROM ertac_initial_uaf""").fetchall()

    if len(plant_state_hourly_not_uaf) > 0:
        print("Warning: facilities and states found in CAMD hourly data, but not in UAF:", file=logfile)
        for plant in plant_state_hourly_not_uaf:
            print("  " + ertac_lib.nice_str(plant), file=logfile)

    # Check oris plant ID and state from non-CAMD hourly data against UAF.
    plant_state_noncamd_hourly_not_uaf = conn.execute("""SELECT orispl_code, state FROM ertac_hourly_noncamd
    EXCEPT SELECT orispl_code, state FROM ertac_initial_uaf""").fetchall()

    if len(plant_state_noncamd_hourly_not_uaf) > 0:
        print("Warning: facilities and states found in non-CAMD hourly data, but not in UAF:", file=logfile)
        for plant in plant_state_noncamd_hourly_not_uaf:
            print("  " + ertac_lib.nice_str(plant), file=logfile)


def validate_units(conn, logfile):
    """Validate presence of facility/unit in control/emissions, hourly data, and UAF.

    Check that list of facility/unit IDs in control/emissions are found in UAF,
    and that facility/unit IDs in CAMD and non-CAMD hourly data are found in UAF.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Validating facility/units for control/emissions, hourly data, and UAF.", file=logfile)

    # Check list of facility/unit IDs from control/emissions table against UAF.
    unit_control_not_uaf = conn.execute("""SELECT orispl_code, unitid FROM ertac_control_emissions
    EXCEPT SELECT orispl_code, unitid FROM ertac_initial_uaf""").fetchall()

    if len(unit_control_not_uaf) > 0:
        print("Warning:", len(unit_control_not_uaf),
              "facility/units in control/emissions data did not match any ORISPL_CODE, UNITID in UAF:", file=logfile)
        for unit in unit_control_not_uaf:
            print("  " + ertac_lib.nice_str(unit), file=logfile)

    # Check list of facility/unit IDs from seasonal control/emissions table against UAF.
    unit_control_not_uaf = conn.execute("""SELECT orispl_code, unitid FROM ertac_seasonal_control_emissions
    EXCEPT SELECT orispl_code, unitid FROM ertac_initial_uaf""").fetchall()

    if len(unit_control_not_uaf) > 0:
        print("Warning:", len(unit_control_not_uaf),
              "facility/units in seasonal control/emissions data did not match any ORISPL_CODE, UNITID in UAF:",
              file=logfile)
        for unit in unit_control_not_uaf:
            print("  " + ertac_lib.nice_str(unit), file=logfile)

    # Check list of facility/unit IDs from CAMD hourly data against UAF.
    unit_hourly_not_uaf = conn.execute("""SELECT orispl_code, unitid FROM camd_hourly_base
    EXCEPT SELECT orispl_code, unitid FROM ertac_initial_uaf""").fetchall()

    if len(unit_hourly_not_uaf) > 0:
        print("Warning:", len(unit_hourly_not_uaf),
              "facility/units in CAMD hourly data did not match any ORISPL_CODE, UNITID in UAF:", file=logfile)
        for unit in unit_hourly_not_uaf:
            # jmj 9/3/2014 - add in an extra check for units with leading zeros
            plantarray = list(unit[1])
            while plantarray[0] == "0":
                plantarray.remove("0")
            if unit[1] != "".join(plantarray):
                if len(conn.execute("""SELECT orispl_code, state FROM ertac_initial_uaf WHERE
                orispl_code = ? and unitid=?""", [unit[0], "".join(plantarray)]).fetchall()) > 0:
                    print("  " + ertac_lib.nice_str(
                        unit) + " has an entry in the UAF that seems similar, but does not have leading 0's",
                          file=logfile)
                else:
                    print("  " + ertac_lib.nice_str(unit), file=logfile)
            else:
                print("  " + ertac_lib.nice_str(unit), file=logfile)

    # Check list of facility/unit IDs from non-CAMD hourly data against UAF.
    unit_noncamd_hourly_not_uaf = conn.execute("""SELECT orispl_code, unitid FROM ertac_hourly_noncamd
    EXCEPT SELECT orispl_code, unitid FROM ertac_initial_uaf""").fetchall()

    if len(unit_noncamd_hourly_not_uaf) > 0:
        print("Warning:", len(unit_noncamd_hourly_not_uaf),
              "facility/units in non-CAMD hourly data did not match any ORISPL_CODE, UNITID in UAF:", file=logfile)
        for unit in unit_noncamd_hourly_not_uaf:
            # jmj 9/3/2014 - add in an extra check for units with leading zeros
            plantarray = list(unit[1])
            while plantarray[0] == "0":
                plantarray.remove("0")
            if unit[1] != "".join(plantarray):
                if len(conn.execute("""SELECT orispl_code, state FROM ertac_initial_uaf WHERE
                orispl_code = ? and unitid=?""", [unit[0], "".join(plantarray)]).fetchall()) > 0:
                    print("  " + ertac_lib.nice_str(
                        unit) + " has an entry in the UAF that seems similar, but does not have leading 0's",
                          file=logfile)
                else:
                    print("  " + ertac_lib.nice_str(unit), file=logfile)
            else:
                print("  " + ertac_lib.nice_str(unit), file=logfile)


def check_uaf_consistency(conn, base_year, logfile):
    """Check UAF for consistent facility data and unit online/offline data.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year for the projection
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking for consistent UAF data.", file=logfile)

    # Confirm that all units at the same ORIS facility are assigned to the same
    # ERTAC region, so that facility size rankings within regions are reliable
    # for new generic unit creation, and spinning reserve requirements within
    # regions can be determined accurately.  Verify other facility descriptive
    # columns, so any new generic units can be created with proper facility
    # information.
    conn.execute("CREATE TEMPORARY TABLE uaf_plant_details AS SELECT DISTINCT "
                 + ertac_tables.uaf_plant_column_names + " FROM ertac_initial_uaf")

    conn.execute("""CREATE TEMPORARY TABLE uaf_inconsistent_plants
    AS SELECT orispl_code, COUNT(*)
    FROM uaf_plant_details
    GROUP BY orispl_code
    HAVING COUNT(*) > 1;""")

    inconsistent_plants = conn.execute("""SELECT details.*
    FROM uaf_inconsistent_plants inconsistent
    JOIN uaf_plant_details details
    ON inconsistent.orispl_code = details.orispl_code
    ORDER BY orispl_code""").fetchall()

    if len(inconsistent_plants) > 0:
        print("Warning: UAF has inconsistent details for ORIS plants:", file=logfile)
        print("  " + ertac_tables.uaf_plant_column_names, file=logfile)
        for plant in inconsistent_plants:
            print("  " + ertac_lib.nice_str(plant), file=logfile)

    conn.executescript("""DROP TABLE uaf_plant_details;
    DROP TABLE uaf_inconsistent_plants;""")

    # Check that units with specified online and offline dates were online before offline.
    wrong_date_order = conn.execute("""SELECT orispl_code, unitid, ertac_fuel_unit_type_bin, online_start_date, offline_start_date
    FROM ertac_initial_uaf
    WHERE online_start_date IS NOT NULL
    AND offline_start_date IS NOT NULL
    AND online_start_date >= offline_start_date
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin, online_start_date, offline_start_date""").fetchall()

    if len(wrong_date_order) > 0:
        print("Warning: UAF has inconsistent online/offline dates:", file=logfile)
        for unit_dates in wrong_date_order:
            print("  " + ertac_lib.nice_str(unit_dates), file=logfile)

    # Walk through UAF records for units that occur multiple times due to fuel
    # switching, and check that online/offline dates do not have overlapping
    # periods of operation.  Only the earliest row for a unit is allowed to have
    # an unspecified online date, and only the latest row is allowed to have an
    # unspecified offline date.
    heading_printed = False

    fuel_switch_units = conn.execute("""SELECT orispl_code, unitid, COUNT(*)
    FROM ertac_initial_uaf
    GROUP BY orispl_code, unitid
    HAVING COUNT(*) > 1
    ORDER BY orispl_code, unitid""").fetchall()

    for (plant, unit, cnt) in fuel_switch_units:

        unit_fuels = conn.execute("""SELECT ertac_fuel_unit_type_bin, online_start_date, offline_start_date
        FROM ertac_initial_uaf
        WHERE orispl_code = ?
        AND unitid = ?
        ORDER BY COALESCE(online_start_date, ?),
        COALESCE(offline_start_date, ?)""", (plant, unit, ertac_lib.online_default, ertac_lib.offline_default))

        (prev_fuel, prev_on, prev_off) = unit_fuels.fetchone()
        for (next_fuel, next_on, next_off) in unit_fuels.fetchall():
            if prev_off is None or next_on is None or prev_off > next_on:
                if not heading_printed:
                    # Only show this heading once before the first error.
                    print("Warning: UAF has fuel-switch units with missing or overlapping online/offline dates:",
                          file=logfile)
                    heading_printed = True
            if prev_off is None:
                print("  " + plant + ", " + unit + ":", (prev_fuel, prev_on, prev_off), "missing offline date",
                      file=logfile)
            if next_on is None:
                print("  " + plant + ", " + unit + ":", (next_fuel, next_on, next_off), "missing online date",
                      file=logfile)
            if prev_off > next_on:
                print("  " + plant + ", " + unit + ":", (prev_fuel, prev_on, prev_off), "overlaps",
                      (next_fuel, next_on, next_off), file=logfile)
            (prev_fuel, prev_on, prev_off) = (next_fuel, next_on, next_off)

    # Check that NEW units have future online dates, and Full/Partial units have
    # empty or past online dates.
    day_after_base_year = ertac_lib.first_day_after(base_year)

    new_units_past_dates = conn.execute("""SELECT orispl_code, unitid, ertac_fuel_unit_type_bin, camd_by_hourly_data_type, online_start_date
    FROM ertac_initial_uaf
    WHERE camd_by_hourly_data_type = 'NEW'
    AND (online_start_date IS NULL OR online_start_date < ?)
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""", (day_after_base_year,)).fetchall()

    if len(new_units_past_dates) > 0:
        print(
            "Warning: UAF has NEW units with online_start_date missing, before, or during base year; will be treated as Full:",
            file=logfile)
        for bad_new_unit in new_units_past_dates:
            # 20120510 Columns were in wrong order.
            (plant, unit, fuel, by_type, online) = bad_new_unit
            print("  " + ertac_lib.nice_str(bad_new_unit), file=logfile)
            conn.execute("""UPDATE ertac_initial_uaf
            SET camd_by_hourly_data_type = 'Full'
            WHERE orispl_code = ?
            AND unitid = ?
            AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel))

    old_units_future_dates = conn.execute("""SELECT orispl_code, unitid, ertac_fuel_unit_type_bin, camd_by_hourly_data_type, online_start_date
    FROM ertac_initial_uaf
    WHERE camd_by_hourly_data_type IN ('Full', 'Partial')
    AND online_start_date >= ?
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""", (day_after_base_year,)).fetchall()

    if len(old_units_future_dates) > 0:
        print("Warning: UAF has Full/Partial units with online start date after base year; will be treated as NEW:",
              file=logfile)
        for bad_old_unit in old_units_future_dates:
            (plant, unit, fuel, by_type, online) = bad_old_unit
            print("  " + ertac_lib.nice_str(bad_old_unit), file=logfile)
            conn.execute("""UPDATE ertac_initial_uaf
            SET camd_by_hourly_data_type = 'NEW'
            WHERE orispl_code = ?
            AND unitid = ?
            AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel))

    # Check that units have consistent capacity limit values and flags; both empty or both filled.
    inconsistent_limits = conn.execute("""SELECT orispl_code, unitid, ertac_fuel_unit_type_bin,
    unit_annual_capacity_limit, capacity_limited_unit_flag
    FROM ertac_initial_uaf
    WHERE (unit_annual_capacity_limit IS NOT NULL
        AND capacity_limited_unit_flag IS NULL)
    OR (unit_annual_capacity_limit IS NULL
        AND capacity_limited_unit_flag = 'Y')
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""").fetchall()

    if len(inconsistent_limits) > 0:
        print("Warning: UAF has inconsistent capacity limit values and flags:", file=logfile)
        for unit_limit in inconsistent_limits:
            print("  " + ertac_lib.nice_str(unit_limit), file=logfile)


def check_growth_rate_consistency(conn, logfile):
    """Check growth rates for consistent transition hours.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking for consistent growth rate transition hours.", file=logfile)
    # Check that peak->formula transition hour is before formula->nonpeak
    # transition hour.
    inconsistent_hours = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin,
    transition_hour_peak_2_formula, transition_hour_formula_2_nonpeak
    FROM ertac_growth_rates
    WHERE transition_hour_peak_2_formula >= transition_hour_formula_2_nonpeak
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall()

    if len(inconsistent_hours) > 0:
        print("Warning: growth rates has inconsistent hours for peak->formula and formula->nonpeak:", file=logfile)
        for hours in inconsistent_hours:
            print("  " + ertac_lib.nice_str(hours), file=logfile)


def check_input_variable_consistency(conn, logfile):
    """Check input variables for consistent heat rate and EF min/max and new unit sizes.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking input variables for consistent heat rate min/max.", file=logfile)
    # Check that heat rate min <= heat rate max.
    inconsistent_limits = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin,
    heat_rate_min, heat_rate_max
    FROM ertac_input_variables
    WHERE heat_rate_min > heat_rate_max
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall()
    if len(inconsistent_limits) > 0:
        print("Warning: input variables has heat_rate_max < heat_rate_min:", file=logfile)
        for limits in inconsistent_limits:
            print("  " + ertac_lib.nice_str(limits), file=logfile)

    print(file=logfile)
    print("Checking input variables for consistent NOx EF min/max.", file=logfile)
    # Check that NOx EF min <= NOx EF max.
    inconsistent_limits = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin,
    nox_min_ef, nox_max_ef
    FROM ertac_input_variables
    WHERE nox_min_ef > nox_max_ef
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall()
    if len(inconsistent_limits) > 0:
        print("Warning: input variables has nox_max_ef < nox_min_ef:", file=logfile)
        for limits in inconsistent_limits:
            print("  " + ertac_lib.nice_str(limits), file=logfile)

    print(file=logfile)
    print("Checking input variables for consistent SO2 EF min/max.", file=logfile)
    # Check that SO2 EF min <= SO2 EF max.
    inconsistent_limits = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin,
    so2_min_ef, so2_max_ef
    FROM ertac_input_variables
    WHERE so2_min_ef > so2_max_ef
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall()
    if len(inconsistent_limits) > 0:
        print("Warning: input variables has so2_max_ef < so2_min_ef:", file=logfile)
        for limits in inconsistent_limits:
            print("  " + ertac_lib.nice_str(limits), file=logfile)

    print(file=logfile)
    print("Checking input variables for consistent new unit sizes.", file=logfile)
    # Check that new unit min size <= new unit max size.
    inconsistent_sizes = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin,
    new_unit_max_size, new_unit_min_size
    FROM ertac_input_variables
    WHERE new_unit_max_size < new_unit_min_size
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall()
    if len(inconsistent_sizes) > 0:
        print("Warning: input variables has new_unit_max_size < new_unit_min_size:", file=logfile)
        for sizes in inconsistent_sizes:
            print("  " + ertac_lib.nice_str(sizes), file=logfile)


def check_control_emissions_consistency(conn, base_year, logfile):
    """Check control/emissions for consistent dates, and presence of rate or efficiency.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year for the projection
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking control/emissions for consistent dates, and presence of rate or efficiency.", file=logfile)
    # Check that factor_start_date < factor_end_date if both are present.
    inconsistent_dates = conn.execute("""SELECT *
    FROM ertac_control_emissions
    WHERE factor_start_date IS NOT NULL
    AND factor_end_date IS NOT NULL
    AND factor_start_date > factor_end_date
    ORDER BY orispl_code, unitid, pollutant_code, factor_start_date, factor_end_date""").fetchall()

    if len(inconsistent_dates) > 0:
        print("Warning: control/emissions has factor_start_date > factor_end_date:", file=logfile)
        for dates in inconsistent_dates:
            print("  " + ertac_lib.nice_str(dates), file=logfile)

    # Where multiple factors exist for same pollutant at same unit, check that
    # dates do not overlap.
    heading_printed = False

    multiple_factors = conn.execute("""SELECT orispl_code, unitid, pollutant_code, COUNT(*)
    FROM ertac_control_emissions
    GROUP BY orispl_code, unitid, pollutant_code
    HAVING COUNT(*) > 1
    ORDER BY orispl_code, unitid, pollutant_code""").fetchall()

    for (plant, unit, poll, cnt) in multiple_factors:
        factors = conn.execute("""SELECT factor_start_date, factor_end_date
        FROM ertac_control_emissions
        WHERE orispl_code = ?
        AND unitid = ?
        AND pollutant_code = ?
        ORDER BY COALESCE(factor_start_date, ?),
        COALESCE(factor_end_date, ?)""", (plant, unit, poll, ertac_lib.online_default, ertac_lib.offline_default))

        (prev_start, prev_end) = factors.fetchone()
        for (next_start, next_end) in factors.fetchall():
            if prev_end is None or next_start is None or prev_end >= next_start:
                if not heading_printed:
                    print("Warning: control/emissions has factors with missing or overlapping start/end dates:",
                          file=logfile)
                    heading_printed = True
            if prev_end is None:
                print("  " + ertac_lib.nice_str((plant, unit, poll, prev_start, prev_end)) + " missing end date",
                      file=logfile)
            if next_start is None:
                print("  " + ertac_lib.nice_str((plant, unit, poll, next_start, next_end)) + " missing start date",
                      file=logfile)
            if prev_end >= next_start:
                print("  " + ertac_lib.nice_str((plant, unit, poll)) + ": " + ertac_lib.nice_str(
                    (prev_start, prev_end)) + " overlaps " + ertac_lib.nice_str((next_start, next_end)), file=logfile)
            (prev_start, prev_end) = (next_start, next_end)

    # 20120423 Added warning for check that control/emissions data is for future years.
    day_after_base_year = ertac_lib.first_day_after(base_year)

    factors_past_dates = conn.execute("""SELECT *
    FROM ertac_control_emissions
    WHERE factor_start_date IS NULL
    OR factor_start_date < ?
    ORDER BY orispl_code, unitid, pollutant_code, factor_start_date, factor_end_date""",
                                      (day_after_base_year,)).fetchall()

    if len(factors_past_dates) > 0:
        print("Warning: control/emissions has factor_start_date missing, before, or during base year; will be ignored:",
              file=logfile)
        for dates in factors_past_dates:
            print("  " + ertac_lib.nice_str(dates), file=logfile)

    # Check that either emission_rate or control_efficiency is present.
    missing_rate_control = conn.execute("""SELECT *
    FROM ertac_control_emissions
    WHERE emission_rate IS NULL
    AND control_efficiency IS NULL""").fetchall()

    if len(missing_rate_control) > 0:
        print("Warning: control/emissions has neither emission_rate nor control_efficiency:", file=logfile)
        for no_control in missing_rate_control:
            print("  " + ertac_lib.nice_str(no_control), file=logfile)


# jmj 10/24/2013 - adding a new function to check the new seasonal control table
# it is nearly identical to check_control_emissions_consistency, except it make sure the
# two new columns are also dates
def check_seasonal_control_emissions_consistency(conn, base_year, future_year, logfile):
    """Check control/emissions for consistent dates, and presence of rate or efficiency.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year for the projection
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking seasonal control/emissions for consistent dates, and presence of rate or efficiency.", file=logfile)

    # Check that factor_start_date < factor_end_date if both are present.
    inconsistent_dates = conn.execute("""SELECT *
    FROM ertac_seasonal_control_emissions
    WHERE factor_start_date IS NOT NULL
    AND factor_end_date IS NOT NULL
    AND factor_start_date > factor_end_date
    ORDER BY orispl_code, unitid, pollutant_code, factor_start_date, factor_end_date""").fetchall()

    if len(inconsistent_dates) > 0:
        print("Warning: seasonal control/emissions has factor_start_date > factor_end_date:", file=logfile)
        for dates in inconsistent_dates:
            print("  " + ertac_lib.nice_str(dates), file=logfile)

    # Where multiple factors exist for same pollutant at same unit, check that
    # dates do not overlap.
    heading_printed = False

    for poll in ['NOX', 'SO2']:
        multiple_factors = conn.execute("""SELECT orispl_code, unitid,COUNT(*)
        FROM ertac_seasonal_control_emissions
        WHERE pollutant_code = ?
        GROUP BY orispl_code, unitid, pollutant_code
        ORDER BY orispl_code, unitid, pollutant_code""", [poll, ]).fetchall()

        errors = ""
        for (plant, unit, cnt) in multiple_factors:
            factor_query = """SELECT factor_start_date, factor_end_date, season_start_month, season_start_date, season_end_month, season_end_date
            FROM ertac_seasonal_control_emissions
            WHERE orispl_code = ?
            AND unitid = ?
            AND pollutant_code = ?
            AND factor_end_date >= ?
            AND factor_start_date < ?
            ORDER BY season_start_month, season_start_date, season_end_month, season_end_date"""

            fct = conn.execute(factor_query, (
                plant, unit, poll, ertac_lib.first_day_of(future_year), ertac_lib.first_day_after(future_year)))
            # jmj 9/17/2019 adding factor dates to this check to avoid units with multiple factors

            # jmj 2/19/21 there were issues with the rowcount so this was rewritten though it now takes two of the same query to make it happen
            if len(fct.fetchall()) > 0:
                factors = conn.execute(factor_query, (
                    plant, unit, poll, ertac_lib.first_day_of(future_year), ertac_lib.first_day_after(future_year)))

                (factor_start_date, factor_end_date, prev_start_month, prev_start_date, prev_end_month,
                 prev_end_date) = factors.fetchone()

                for (next_factor_start_date, next_factor_end_date, next_start_month, next_start_date, next_end_month,
                     next_end_date) in factors.fetchall():
                    if prev_start_month > prev_end_month or (
                            prev_start_month == prev_end_month and prev_start_date >= prev_end_date):
                        errors += "  " + ertac_lib.nice_str((plant, unit, poll)) + ": " + ertac_lib.nice_str((
                            factor_start_date,
                            factor_end_date,
                            prev_start_month,
                            prev_start_date,
                            prev_end_month,
                            prev_end_date)) + " has a start date on or after the end date\n"
                    if prev_end_month > next_start_month or (
                            prev_end_month == next_start_month and prev_end_date >= next_start_date):
                        errors += "  " + ertac_lib.nice_str((plant, unit, poll)) + ": " + ertac_lib.nice_str((
                            factor_start_date,
                            factor_end_date,
                            prev_start_month,
                            prev_start_date,
                            prev_end_month,
                            prev_end_date)) + " overlaps " + ertac_lib.nice_str(
                            (factor_start_date, factor_end_date, next_start_month, next_start_date, next_end_month,
                             next_end_date)) + "\n"
                        (prev_start_month, prev_start_date, prev_end_month, prev_end_date) = (
                            next_start_month, next_start_date, next_end_month, next_end_date)

                # repeating to get the last line in the file jmj 2/19/21
                if prev_start_month > prev_end_month or (
                        prev_start_month == prev_end_month and prev_start_date >= prev_end_date):
                    errors += "  " + ertac_lib.nice_str((plant, unit, poll)) + ": " + ertac_lib.nice_str((
                        factor_start_date,
                        factor_end_date,
                        prev_start_month,
                        prev_start_date,
                        prev_end_month,
                        prev_end_date)) + " has a start date on or after the end date\n"

        if errors != "":
            print(
                "Warning: seasonal control/emissions has seasonal factors with missing or overlapping start/end dates:",
                file=logfile)
            print(errors, file=logfile)

    errors = ""
    for (plant, unit, poll, ssm, ssd, sem, sed, fsdate, fedate) in conn.execute("""SELECT ece.orispl_code, ece.unitid, ece.pollutant_code,
            season_start_month, season_start_date, season_end_month, season_end_date,
            ece.factor_start_date, ece.factor_end_date
            FROM ertac_seasonal_control_emissions esce
            INNER JOIN ertac_control_emissions ece
            ON esce.orispl_code = ece.orispl_code
            AND esce.unitid = ece.unitid
            AND esce.pollutant_code = ece.pollutant_code
            WHERE esce.factor_end_date >= ?
            AND esce.factor_start_date < ?
            ORDER BY ece.orispl_code, ece.unitid, ece.pollutant_code""", (
            ertac_lib.first_day_of(future_year), ertac_lib.first_day_after(future_year))).fetchall():

        if len(fsdate.split("-")) == 3:
            (fsy, fsm, fsd) = fsdate.split("-")
        else:
            (fsy, fsm, fsd) = (base_year, 1, 1)

        if len(fedate.split("-")) == 3:
            (fey, fem, fed) = fedate.split("-")
        else:
            (fey, fem, fed) = (2200, 12, 31)

        # jmj convert this to actual date checks
        if int(future_year) > int(fsy) or (
                int(future_year) == int(fsy) and (ssm > int(fsm) or (ssm == int(fsm) and ssd >= int(fsd)))):
            if int(future_year) < int(fey) or (
                    int(future_year) == int(fey) and (ssm < int(fem) or (ssm == int(fem) and ssd <= int(fed)))):
                errors += "  " + ertac_lib.nice_str((plant, unit, poll)) + ": " + ertac_lib.nice_str(
                    (ssm, ssd, sem, sed)) + " has a start date that overlaps an entry in the conrol file\n"
        else:
            if int(future_year) < int(fey) or (
                    int(future_year) == int(fey) and (sem < int(fem) or (sem == int(fem) and sed <= int(fed)))):
                if int(future_year) > int(fsy) or (
                        int(future_year) == int(fsy) and (sem > int(fsm) or (sem == int(fsm) and sed >= int(fsd)))):
                    errors += "  " + ertac_lib.nice_str((plant, unit, poll)) + ": " + ertac_lib.nice_str(
                        (ssm, ssd, sem, sed)) + " has a end date that overlaps an entry in the conrol file\n"

    if errors != "":
        print("Warning: seasonal control/emissions has seasonal factors that overlap entries in the control file:",
              file=logfile)
        print(errors, file=logfile)

    # 20120423 Added warning for check that control/emissions data is for future years.
    day_after_base_year = ertac_lib.first_day_after(base_year)

    factors_past_dates = conn.execute("""SELECT *
    FROM ertac_seasonal_control_emissions
    WHERE factor_start_date IS NULL
    OR factor_start_date < ?
    ORDER BY orispl_code, unitid, pollutant_code, factor_start_date, factor_end_date""",
                                      (day_after_base_year,)).fetchall()

    if len(factors_past_dates) > 0:
        print(
            "Warning: seasonal control/emissions has factor_start_date missing, before, or during base year; will be ignored:",
            file=logfile)
        for dates in factors_past_dates:
            print("  " + ertac_lib.nice_str(dates), file=logfile)

    # Check that either emission_rate or control_efficiency is present.
    missing_rate_control = conn.execute("""SELECT *
    FROM ertac_seasonal_control_emissions
    WHERE emission_rate IS NULL
    AND control_efficiency IS NULL""").fetchall()

    if len(missing_rate_control) > 0:
        print("Warning: seasonal control/emissions has neither emission_rate nor control_efficiency:", file=logfile)
        for no_control in missing_rate_control:
            print("  " + ertac_lib.nice_str(no_control), file=logfile)


def check_group_total_listing_consistency(conn, logfile):
    """Check group total listing for consistent lists of valid states.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking group total listing for consistent lists of valid states.", file=logfile)

    # Check for same list of states for same group.
    conn.executescript("""CREATE TEMPORARY TABLE group_states
    AS SELECT DISTINCT group_name, states_included
    FROM group_total_listing;

    CREATE TEMPORARY TABLE group_inconsistent_states
    AS SELECT group_name, COUNT(*)
    FROM group_states
    GROUP BY group_name
    HAVING COUNT(*) > 1;""")

    inconsistent_groups = conn.execute("""SELECT group_states.*
    FROM group_inconsistent_states inconsistent
    JOIN group_states
    ON inconsistent.group_name = group_states.group_name""").fetchall()

    if len(inconsistent_groups) > 0:
        print("Warning: group_total_listing has inconsistent states:", file=logfile)
        for group in inconsistent_groups:
            print("  " + ertac_lib.nice_str(group), file=logfile)

    # Check for all states valid.  Split list of states for each group name, and
    # check each individual state against set of all valid states.
    heading_printed = False
    for (group, states) in conn.execute("SELECT group_name, states_included FROM group_states").fetchall():
        invalid_states = set([state.strip() for state in states.split(',')]) - ertac_tables.state_set
        if len(invalid_states) > 0:
            if not heading_printed:
                print("Warning: group_total_listing includes invalid states:", file=logfile)
                heading_printed = True
            print("  " + repr(group) + ": " + ertac_lib.nice_str(invalid_states), file=logfile)

    conn.executescript("""DROP TABLE group_states;
    DROP TABLE group_inconsistent_states;""")


def check_demand_transfer_consistency(conn, logfile):
    """Check demand transfers for consistent use of origin and destination at each hour.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking demand transfers for consistent hourly origin/destination.", file=logfile)

    # The table/index structure allows multiple entries for the same origin and
    # hour with different destinations, and for the same hour and destination
    # with different origins, but prevents multiple entries all having the same
    # origin+hour+destination in order to avoid redundant excess transfers.
    # Here we check for the same region+fuel being used for an origin and for a
    # destination at the same hour, because it should perform only one role at a
    # given time.  Find intersection of origin region/fuel/hour with destination
    # region/fuel/hour and list any matches from both sides.
    conn.executescript("""CREATE TEMPORARY TABLE both_ways
    (region TEXT NOT NULL COLLATE NOCASE,
    fuel TEXT NOT NULL COLLATE NOCASE,
    hour INTEGER NOT NULL,
    PRIMARY KEY (region, fuel, hour));

    INSERT INTO both_ways
    SELECT origin_region, origin_fuel, calendar_hour
    FROM ertac_demand_transfers
    INTERSECT
    SELECT destination_region, destination_fuel, calendar_hour
    FROM ertac_demand_transfers;""")

    inconsistent_roles = conn.execute("""SELECT bw1.rowid AS both, 'Orig.', edt1.*
    FROM both_ways bw1
    JOIN ertac_demand_transfers edt1
    ON bw1.region = edt1.origin_region AND bw1.fuel = edt1.origin_fuel AND bw1.hour = edt1.calendar_hour
    UNION
    SELECT bw2.rowid, 'Dest.', edt2.*
    FROM both_ways bw2
    JOIN ertac_demand_transfers edt2
    ON bw2.region = edt2.destination_region AND bw2.fuel = edt2.destination_fuel AND bw2.hour = edt2.calendar_hour
    ORDER BY both, origin_region, origin_fuel, calendar_hour, destination_region, destination_fuel""").fetchall()

    if len(inconsistent_roles) > 0:
        print("Warning: ertac_demand_transfers has same region+fuel in inconsistent roles at same hour:", file=logfile)
        for transfer in inconsistent_roles:
            print("  " + ertac_lib.nice_str(transfer), file=logfile)

    conn.execute("""DROP TABLE both_ways""")


def remove_non_egu_data(conn, logfile):
    """1.01: Remove hourly data from units marked in UAF as non-EGU.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Removing non-EGU hourly data:", file=logfile)

    oris_unit_list = conn.execute("""SELECT orispl_code, unitid FROM ertac_initial_uaf
    WHERE camd_by_hourly_data_type = 'Non-EGU'
    ORDER BY orispl_code, unitid""").fetchall()

    print("There are", len(oris_unit_list),
          "units marked in the UAF as Non-EGU to be removed from the CAMD hourly data.", file=logfile)

    rows_affected = 0
    for (plant, unit) in oris_unit_list:
        rows_affected += conn.execute("""DELETE FROM camd_hourly_base
        WHERE orispl_code = ? AND unitid = ?""", (plant, unit)).rowcount
    print("Removed", rows_affected, "hourly rows from CAMD data.", file=logfile)


def check_initial_data_ranges(conn, logfile):
    """1.02, 1.03: Check value ranges for input database tables.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    # This section will warn about data rows with out-of-range values, but will
    # not remove them from the tables.  Users may wish to change and re-run the
    # input data, or may decide to ignore the warnings.
    ertac_lib.check_data_ranges('camd_hourly_base', conn, ertac_tables.camd_columns, logfile)
    ertac_lib.check_data_ranges('ertac_hourly_noncamd', conn, ertac_tables.camd_columns, logfile)
    ertac_lib.check_data_ranges('ertac_initial_uaf', conn, ertac_tables.uaf_columns, logfile)
    ertac_lib.check_data_ranges('ertac_growth_rates', conn, ertac_tables.growth_rate_columns, logfile)
    ertac_lib.check_data_ranges('ertac_input_variables', conn, ertac_tables.input_variable_columns, logfile)
    # For V2, added range checks for new optional demand transfers.
    ertac_lib.check_data_ranges('ertac_demand_transfers', conn, ertac_tables.demand_transfer_columns, logfile)
    ertac_lib.check_data_ranges('ertac_control_emissions', conn, ertac_tables.control_emission_columns, logfile)
    # jmj 10/24/2013 add the seasonal control check
    ertac_lib.check_data_ranges('ertac_seasonal_control_emissions', conn,
                                ertac_tables.seasonal_control_emission_columns, logfile)
    ertac_lib.check_data_ranges('state_total_listing', conn, ertac_tables.state_total_columns, logfile)
    ertac_lib.check_data_ranges('group_total_listing', conn, ertac_tables.group_total_columns, logfile)


def insert_seasonal_controls(conn, base_year, future_year, logfile):
    """1.04: Convert seasonal controls into annual controls and insert into the file.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year to copy and process
    future_year -- the future year to copy and process
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Inserting seasonal controls into annual controls:", file=logfile)

    rows_affected = conn.execute("""INSERT INTO calc_control_emissions
    SELECT orispl_code, unitid, season_start_month || '/' || season_start_date || '/' || ? , season_end_month || '/' || season_end_date || '/' || ?, pollutant_code, emission_rate, control_efficiency, control_programs, control_description, submitter_email
    FROM ertac_seasonal_control_emissions
    WHERE factor_start_date < ?
    AND factor_end_date >= ?
    AND factor_start_date >= ?""", [future_year, future_year, ertac_lib.first_day_after(future_year),
                                    ertac_lib.first_day_of(future_year), ertac_lib.first_day_after(base_year)]).rowcount
    print("Inserted", rows_affected, "seasonal controls.", file=logfile)


def copy_base_year_hourly(conn, base_year, logfile):
    """Copy CAMD hourly data from base year, adding region/fuel bin from UAF.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year to copy and process
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Copying base-year CAMD hourly data:", file=logfile)
    # Copy the base year hourly operation data, along with the assigned ERTAC
    # region and fuel bin, based on matching the ORIS and unit IDs with correct
    # rows in the UAF, using the specified online and offline dates, if any.
    # This includes the possibility that a unit had a fuel switch sometime
    # during the base year, in which case part of the year's data will link with
    # the earlier fuel bin, and part with the later, depending on the transition
    # dates listed in the UAF.

    # jmj 9/18/14 convert the gross load in calc_hourly_base from MW to MW-hr
    # RW 9/17/2015 Doris and Jin verified that SLOAD should be scaled like GLOAD
    # for fractional hours.
    # RW 9/25/2015 Doris got confirmation that heat input has already been converted
    # from rate (mmbtu/hr) to input amount (mmbtu) during the hour and doesn't need
    # further adjustment for fractional hours.
    conn.execute("DELETE FROM calc_hourly_base")
    rows_affected = conn.execute("""INSERT INTO calc_hourly_base
        (ertac_region,
        ertac_fuel_unit_type_bin,
        state,
        facility_name,
        orispl_code,
        unitid,
        op_date,
        op_hour,
        op_time,
        gload,
        sload,
        so2_mass,
        so2_mass_flag,
        so2_rate,
        so2_rate_flag,
        nox_rate,
        nox_rate_flag,
        nox_mass,
        nox_mass_flag,
        co2_mass,
        co2_mass_flag,
        co2_rate,
        co2_rate_flag,
        heat_input)
    SELECT uaf.ertac_region, uaf.ertac_fuel_unit_type_bin,
        hourly.state,
        hourly.facility_name,
        hourly.orispl_code,
        hourly.unitid,
        hourly.op_date,
        hourly.op_hour,
        hourly.op_time,
        hourly.gload * hourly.op_time,
        hourly.sload * hourly.op_time,
        hourly.so2_mass,
        hourly.so2_mass_flag,
        hourly.so2_rate,
        hourly.so2_rate_flag,
        hourly.nox_rate,
        hourly.nox_rate_flag,
        hourly.nox_mass,
        hourly.nox_mass_flag,
        hourly.co2_mass,
        hourly.co2_mass_flag,
        hourly.co2_rate,
        hourly.co2_rate_flag,
        hourly.heat_input
    FROM calc_updated_uaf uaf
    JOIN camd_hourly_base hourly
    ON uaf.orispl_code = hourly.orispl_code
    AND uaf.unitid = hourly.unitid
    AND uaf.online_start_date <= hourly.op_date
    AND uaf.offline_start_date > hourly.op_date""").rowcount
    print("Copied", rows_affected, "rows of CAMD hourly data.", file=logfile)


def copy_base_year_noncamd_hourly(conn, base_year, logfile):
    """Copy non-CAMD hourly data from base year, adding region/fuel bin from UAF.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year to copy and process
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Copying base-year non-CAMD hourly data:", file=logfile)
    # Similar to the previous copying of CAMD hourly data for the base year,
    # this section adds state-supplied non-CAMD data where no CAMD data exists,
    # and updates any rows where non-CAMD values will replace the CAMD records
    # for that unit and time.

    # jmj 9/18/14 convert the gross load in calc_hourly_base from MW to MW-hr
    # RW 9/17/2015 Doris and Jin verified that SLOAD should be scaled like GLOAD
    # for fractional hours.
    rows_affected = conn.execute("""INSERT OR REPLACE INTO calc_hourly_base
        (ertac_region,
        ertac_fuel_unit_type_bin,
        state,
        facility_name,
        orispl_code,
        unitid,
        op_date,
        op_hour,
        op_time,
        gload,
        sload,
        so2_mass,
        so2_mass_flag,
        so2_rate,
        so2_rate_flag,
        nox_rate,
        nox_rate_flag,
        nox_mass,
        nox_mass_flag,
        co2_mass,
        co2_mass_flag,
        co2_rate,
        co2_rate_flag,
        heat_input)
    SELECT uaf.ertac_region, uaf.ertac_fuel_unit_type_bin,
        hourly.state,
        hourly.facility_name,
        hourly.orispl_code,
        hourly.unitid,
        hourly.op_date,
        hourly.op_hour,
        hourly.op_time,
        hourly.gload * hourly.op_time,
        hourly.sload * hourly.op_time,
        hourly.so2_mass,
        hourly.so2_mass_flag,
        hourly.so2_rate,
        hourly.so2_rate_flag,
        hourly.nox_rate,
        hourly.nox_rate_flag,
        hourly.nox_mass,
        hourly.nox_mass_flag,
        hourly.co2_mass,
        hourly.co2_mass_flag,
        hourly.co2_rate,
        hourly.co2_rate_flag,
        hourly.heat_input
    FROM calc_updated_uaf uaf
    JOIN ertac_hourly_noncamd hourly
    ON uaf.orispl_code = hourly.orispl_code
    AND uaf.unitid = hourly.unitid
    AND uaf.online_start_date <= hourly.op_date
    AND uaf.offline_start_date > hourly.op_date""").rowcount
    print("Copied", rows_affected, "rows of non-CAMD hourly data.", file=logfile)


def check_uaf_retired_units_with_base_year(conn, base_year, logfile):
    """Check UAF for retired units in region/fuel with base-year activity after retirement.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year for the projection
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking UAF for retired units in region/fuel with base-year activity after retirement.", file=logfile)

    # Need to know all retried units; i.e. retired before end of base year
    for retired_unit in conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid, offline_start_date
        FROM calc_updated_uaf
        WHERE camd_by_hourly_data_type NOT IN ('Non-EGU')
        AND offline_start_date < ?""",
                                     [ertac_lib.first_day_after(base_year)]).fetchall():

        retired_unit_with_base_year = conn.execute("""SELECT count(*)
        FROM calc_hourly_base
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?
        and op_date > ? """, retired_unit).fetchall()
        if len(retired_unit_with_base_year) > 0:
            print("Unit retired but has base year data after retirement  " + ertac_lib.nice_str(retired_unit),
                  file=logfile)


def check_uaf_new_units_no_base_year(conn, base_year, future_year, logfile):
    """Check UAF for new units in region/fuel with no base-year activity.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year for the projection
    future_year -- the future year where new units would be active
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking UAF for new units in region/fuel with no base-year activity.", file=logfile)
    # Need to know which region/fuel combinations exist in base year hourly
    # data, to find any new units in vacant region/fuel combinations.
    conn.execute("""CREATE TEMPORARY TABLE region_fuel_base
    AS SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin
    FROM calc_hourly_base""")
    # Need to know all new units; i.e. starting after base year and operating
    # during future year.
    conn.execute("""CREATE TEMPORARY TABLE new_units
    AS SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid, state, facility_name
    FROM calc_updated_uaf
    WHERE online_start_date >= ?
    AND offline_start_date > ?""",
                 (ertac_lib.first_day_after(base_year), ertac_lib.first_day_of(future_year)))
    # Any region/fuel without any hourly data from base year, but with new
    # units, is a problem.
    conn.execute("""CREATE TEMPORARY TABLE region_fuel_vacant
    AS SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin
    FROM new_units
    EXCEPT SELECT ertac_region, ertac_fuel_unit_type_bin
    FROM region_fuel_base""")
    new_units_no_base_year = conn.execute("""SELECT nu.*
    FROM new_units nu
    JOIN region_fuel_vacant rfv
    ON nu.ertac_region = rfv.ertac_region
    AND nu.ertac_fuel_unit_type_bin = rfv.ertac_fuel_unit_type_bin
    ORDER BY ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid""").fetchall()
    if len(new_units_no_base_year) > 0:
        print("Warning: new units for regions and fuel bins that had no activity in base year:", file=logfile)
        for new_unit in new_units_no_base_year:
            print("  " + ertac_lib.nice_str(new_unit), file=logfile)
    conn.executescript("""DROP TABLE region_fuel_base;
    DROP TABLE new_units;
    DROP TABLE region_fuel_vacant;""")


def check_hourly_base_scarce_region_fuel(conn, logfile):
    """Check base-year hourly data for region/fuel with fewer than 10 units.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking base-year hourly data for region/fuel with fewer than 10 units.", file=logfile)
    # Count active units within each region/fuel.
    conn.execute("""CREATE TEMPORARY TABLE region_fuel_plant_unit_base
    AS SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid
    FROM calc_hourly_base""")
    scarce_regions_fuels = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin, COUNT(*)
    FROM region_fuel_plant_unit_base
    GROUP BY ertac_region, ertac_fuel_unit_type_bin
    HAVING COUNT(*) < 10
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall()
    if len(scarce_regions_fuels) > 0:
        print("Warning: regions and fuel bins that had fewer than 10 units in base year:", file=logfile)
        for region_fuel_count in scarce_regions_fuels:
            print("  " + ertac_lib.nice_str(region_fuel_count), file=logfile)
    conn.execute("""DROP TABLE region_fuel_plant_unit_base""")


def check_all_units_retired(conn, future_year, logfile):
    """Check for all existing units in region/fuel retired in future.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    future_year -- the future year where existing units might be retired
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Checking for region/fuel with all units retired in future.", file=logfile)

    # Need to know all units that exist in base year hourly data.
    conn.execute("""CREATE TEMPORARY TABLE region_fuel_plant_unit_base
    AS SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid
    FROM calc_hourly_base""")

    # Look up offline dates for all existing units.  If the latest offline date
    # in a region/fuel is before future year, then all those units are retired
    # in the future.
    region_fuel_all_retired = conn.execute("""SELECT rfpu.ertac_region, rfpu.ertac_fuel_unit_type_bin, MAX(uaf.offline_start_date)
    FROM region_fuel_plant_unit_base rfpu
    JOIN calc_updated_uaf uaf
    ON rfpu.orispl_code = uaf.orispl_code
    AND rfpu.unitid = uaf.unitid
    AND rfpu.ertac_fuel_unit_type_bin = uaf.ertac_fuel_unit_type_bin
    GROUP BY rfpu.ertac_region, rfpu.ertac_fuel_unit_type_bin
    HAVING MAX(uaf.offline_start_date) < ?
    ORDER BY rfpu.ertac_region, rfpu.ertac_fuel_unit_type_bin""", (ertac_lib.first_day_of(future_year),)).fetchall()

    if len(region_fuel_all_retired) > 0:
        print("Warning: all existing units for region/fuel will be retired in future year:", file=logfile)
        for (region, fuel, max_offline) in region_fuel_all_retired:
            print("  " + ertac_lib.nice_str((region, fuel)), file=logfile)

    conn.execute("""DROP TABLE region_fuel_plant_unit_base""")


def fill_temporal_hierarchies(conn, logfile):
    """1.05: Calculate relative rankings of each hour's total generation, within region/fuel.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Calculating temporal hierarchies.", file=logfile)

    conn.executescript("""CREATE TEMPORARY TABLE total_hourly_demand
    AS SELECT ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour, SUM(gload) total_gload
    FROM calc_hourly_base
    GROUP BY ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour;

    CREATE TEMPORARY TABLE region_fuel_hier
    AS SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin
    FROM total_hourly_demand;

    DELETE FROM calc_1hour_hierarchy;
    DELETE FROM calc_6hour_hierarchy;
    DELETE FROM calc_24hour_hierarchy;""")

    for (region, fuel) in conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin
    FROM region_fuel_hier
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall():
        # Look up hierarchy type for current region/fuel.  Match may not exist
        # in input variables.
        hier_result = conn.execute("""SELECT UPPER(hourly_hierarchy_code)
        FROM ertac_input_variables
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
        if hier_result is None:
            print("  Warning: ertac_input_variables has no hierarchy code for region/fuel: "
                  + ertac_lib.nice_str((region, fuel)), file=logfile)
        else:
            (hier_type,) = hier_result
            # If hierarchy is more than hourly, collapse multiple hours into
            # buckets to be ranked.  Otherwise, just rank individual hours for
            # current region/fuel.  This can be extended to any other bucket
            # sizes (such as 2, 3, 4, 8, or 12) that evenly divide 24 hours.
            if hier_type == 'HOURLY':
                bucket_size = 1
            elif hier_type == '6-HOUR':
                bucket_size = 6
                target_table = 'calc_6hour_hierarchy'
                target_column = 'six_hour_allocation_order'
            elif hier_type == '24-HOUR':
                bucket_size = 24
                target_table = 'calc_24hour_hierarchy'
                target_column = 'twentyfour_hour_allocation_order'
            else:
                bucket_size = 0
                print("  Warning: ertac_input_variables has unknown hierarchy code '"
                      + hier_type + "' for region/fuel: " + ertac_lib.nice_str((region, fuel)), file=logfile)

            if bucket_size > 1:
                conn.execute("""CREATE TEMPORARY TABLE demand_subset
                AS SELECT op_date,
                ? * CAST(op_hour / ? AS INTEGER) bucket_start,
                MAX(total_gload) max_gload
                FROM total_hourly_demand
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                GROUP BY op_date, bucket_start""", (bucket_size, bucket_size, region, fuel))
                sorted_demand = conn.execute("""SELECT op_date, bucket_start
                FROM demand_subset
                ORDER BY COALESCE(max_gload, 0.0) DESC, op_date, bucket_start""").fetchall()
                base_rank = 1
                for (op_date, op_hour) in sorted_demand:
                    for i in range(bucket_size):
                        conn.execute("INSERT INTO " + target_table
                                     + " (ertac_region, ertac_fuel_unit_type_bin, "
                                     + target_column + ", op_date, op_hour) VALUES (?, ?, ?, ?, ?)",
                                     (region, fuel, base_rank + i, op_date, op_hour + i))
                    base_rank += bucket_size
                conn.execute("DROP TABLE demand_subset")

            elif bucket_size == 1:
                sorted_demand = conn.execute("""SELECT op_date, op_hour
                FROM total_hourly_demand
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                ORDER BY COALESCE(total_gload, 0.0) DESC, op_date, op_hour""", (region, fuel)).fetchall()
                rank = 1
                for (op_date, op_hour) in sorted_demand:
                    conn.execute("INSERT INTO calc_1hour_hierarchy"
                                 + " (ertac_region, ertac_fuel_unit_type_bin, "
                                 + "one_hour_allocation_order, op_date, op_hour) VALUES (?, ?, ?, ?, ?)",
                                 (region, fuel, rank, op_date, op_hour))
                    rank += 1

    conn.executescript("""DROP TABLE total_hourly_demand;
    DROP TABLE region_fuel_hier;""")


def fill_partial_year(conn, base_year, suppress_pr_messages, logfile):
    """1.06, 1.07: Fill in the remaining hours of any partially-reported units.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year where data has been reported
    suppress_pr_messages -- true if you want to suppress all of the warning messages
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Filling in remaining hours for partial-year reporters:", file=logfile)
    # Need to summarize hourly data to determine which units don't have complete
    # data, how much of their annual heat input has been reported, and their
    # overall average heat rate and emission rates.
    # Need set of all dates and hours from all units, to use when filling
    # non-reported hours for partial-year units.
    conn.executescript("""CREATE TEMPORARY TABLE hourly_summary
    AS SELECT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid,
    SUM(gload) gload_total, SUM(so2_mass) so2_total,
    SUM(nox_mass) nox_total, SUM(co2_mass) co2_total,
    SUM(heat_input) heat_total, COUNT(*) hours_total
    FROM calc_hourly_base
    GROUP BY ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid;

    CREATE TEMPORARY TABLE op_dates_hours
    AS SELECT DISTINCT op_date, op_hour
    FROM calc_hourly_base;""")

    # Max number of hours should usually be 8760, but might be 8784 if base year
    # is a leap year and CAMD reports 366 days.  Unknown if this ever happens.
    hour_result = conn.execute("SELECT COUNT(*) FROM op_dates_hours").fetchone()
    if hour_result is None:
        print("  Warning: there is no hourly data, so no partial-year data to fill.", file=logfile)
        return

    (max_hours,) = hour_result

    # jmj 5/2/2014 warn about units marked as partial year reporters that are full year reporters
    non_partial_reporters = conn.execute("""SELECT hourly_summary.ertac_region, hourly_summary.ertac_fuel_unit_type_bin, hourly_summary.orispl_code, hourly_summary.unitid
    FROM hourly_summary
    LEFT JOIN calc_updated_uaf
    ON calc_updated_uaf.orispl_code = hourly_summary.orispl_code
    AND calc_updated_uaf.unitid = hourly_summary.unitid
    AND calc_updated_uaf.ertac_region = hourly_summary.ertac_region
    AND calc_updated_uaf.ertac_fuel_unit_type_bin = hourly_summary.ertac_fuel_unit_type_bin
    WHERE hours_total = ? and camd_by_hourly_data_type in ('Partial')
    ORDER BY hourly_summary.orispl_code, hourly_summary.unitid, hourly_summary.ertac_fuel_unit_type_bin""",
                                         (max_hours,)).fetchall()
    if len(non_partial_reporters) > 0:
        print("  Warning: units marked as Partial-Year Reporters reported for the Full Year", file=logfile)
        for plant in non_partial_reporters:
            print("  " + ertac_lib.nice_str(plant), file=logfile)

    # Loop over all units with partial data, looking up their UAF status to
    # determine how to fill remainder.
    partial_reporters = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin,
    orispl_code, unitid, gload_total, so2_total, nox_total, co2_total, heat_total, hours_total
    FROM hourly_summary
    WHERE hours_total < ?
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""", (max_hours,)).fetchall()
    if len(partial_reporters) == 0:
        print("  All units reported same number of hours: " + str(max_hours), file=logfile)
        return

    for (region, fuel, plant, unit, gload_total, so2_total, nox_total, co2_total,
         heat_total, hours_total) in partial_reporters:
        # UAF must have this plant,unit,fuel combo because that's where the fuel
        # bin information came from for this unit.  Need annual HI from UAF to
        # know how much to distribute to unrecorded hours, and online/offline
        # dates if unit started or shut down (for this fuel) during base year.
        (state, facility_name, by_type, annual_hi, online, offline) = conn.execute("""SELECT state,
        facility_name, camd_by_hourly_data_type, annual_hi_partials, online_start_date, offline_start_date
        FROM calc_updated_uaf
        WHERE orispl_code = ?
        AND unitid = ?
        AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel)).fetchone()
        if by_type.upper() != 'PARTIAL':
            print("  Warning: unit " + ertac_lib.nice_str((plant, unit, fuel))
                  + " is marked as '" + by_type + "' but will be treated as Partial due to incomplete data.",
                  file=logfile)
            by_type = 'Partial'
            conn.execute("""UPDATE calc_updated_uaf
            SET camd_by_hourly_data_type = 'Partial'
            WHERE orispl_code = ?
            AND unitid = ?
            AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel))
        # Fill missing hours with NULL values if UAF has no annual HI for this
        # unit, or if HI has all been consumed by reported hours.  If there is
        # some HI to be distributed, determine how many hours will receive flat
        # equal profile.  Only missing hours between online/offline dates are
        # eligible; any others will be set to NULL due to inactivity.
        # Need all dates/times not reported by this unit, and need subset of
        # those when it was active.
        conn.execute("""CREATE TEMPORARY TABLE unrecorded_hours
        AS SELECT op_date, op_hour
        FROM op_dates_hours
        EXCEPT SELECT op_date, op_hour
        FROM calc_hourly_base
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit))
        (unrecorded_hours,) = conn.execute("SELECT COUNT(*) FROM unrecorded_hours").fetchone()
        conn.execute("""CREATE TEMPORARY TABLE active_unrecorded_hours
        AS SELECT op_date, op_hour
        FROM unrecorded_hours
        WHERE op_date >= ?
        AND op_date < ?""", (online, offline))
        (active_unrecorded_hours,) = conn.execute("SELECT COUNT(*) FROM active_unrecorded_hours").fetchone()

        # jmj add pr message suppression option
        if not suppress_pr_messages:
            print("  Unit: " + ertac_lib.nice_str((plant, unit, fuel))
                  + " has", unrecorded_hours, "unrecorded hours,",
                  active_unrecorded_hours, "of those seemingly active.", file=logfile)
            print("    UAF Annual_HI_Partials:", annual_hi, ", Reported heat total:", heat_total, file=logfile)
        else:
            print("  Unit: " + ertac_lib.nice_str((plant, unit, fuel)) + " is being treated as a Partial-Year Reporter",
                  file=logfile)

        if annual_hi is not None and heat_total is not None and annual_hi > heat_total and active_unrecorded_hours > 0:
            # Divide remaining HI evenly among hours, for flat operation.
            # Set hourly GLOAD and emissions as fractions of recorded totals,
            # based on ratio of hourly HI to recorded total.
            hourly_hi = round((annual_hi - heat_total) / active_unrecorded_hours, 12)
            # jmj add pr message suppression option
            if not suppress_pr_messages:
                print("    Remainder of Annual_HI_Partials from UAF will be distributed across active hours.",
                      file=logfile)
                print("    Hourly HI will be", hourly_hi, "with GLOAD and emissions in proportion.", file=logfile)
            hi_ratio = round(hourly_hi / heat_total, 12)
            hourly_gload = gload_total * hi_ratio if gload_total is not None else None
            hourly_so2 = so2_total * hi_ratio if so2_total is not None else None
            hourly_nox = nox_total * hi_ratio if nox_total is not None else None
            hourly_co2 = co2_total * hi_ratio if co2_total is not None else None
            rows_affected = conn.execute("""INSERT INTO calc_hourly_base
            (ertac_region, ertac_fuel_unit_type_bin, state, facility_name,
            orispl_code, unitid, op_date, op_hour, op_time, gload, so2_mass,
            nox_mass, co2_mass, heat_input)
            SELECT ?, ?, ?, ?, ?, ?, op_date, op_hour, 1.0, ?, ?, ?, ?, ?
            FROM active_unrecorded_hours""", (region, fuel, state, facility_name, plant, unit,
                                              hourly_gload, hourly_so2, hourly_nox, hourly_co2, hourly_hi)).rowcount
            # jmj add pr message suppression option
            if not suppress_pr_messages:
                print("    Filled values for", rows_affected, "active rows.", file=logfile)

            if unrecorded_hours > active_unrecorded_hours:
                # Not active for all unrecorded hours, so fill remainder with NULL.
                rows_affected = conn.execute("""INSERT INTO calc_hourly_base
                (ertac_region, ertac_fuel_unit_type_bin, state, facility_name,
                orispl_code, unitid, op_date, op_hour)
                SELECT ?, ?, ?, ?, ?, ?, op_date, op_hour
                FROM (SELECT op_date, op_hour FROM unrecorded_hours
                EXCEPT SELECT op_date, op_hour FROM active_unrecorded_hours)""",
                                             (region, fuel, state, facility_name, plant, unit)).rowcount
                # jmj add pr message suppression option
                if not suppress_pr_messages:
                    print("    Also filled NULL for", rows_affected, "inactive rows.", file=logfile)

        else:
            # No HI to be distributed.
            rows_affected = conn.execute("""INSERT INTO calc_hourly_base
            (ertac_region, ertac_fuel_unit_type_bin, state, facility_name,
            orispl_code, unitid, op_date, op_hour)
            SELECT ?, ?, ?, ?, ?, ?, op_date, op_hour
            FROM unrecorded_hours""",
                                         (region, fuel, state, facility_name, plant, unit)).rowcount
            # jmj add pr message suppression option
            if not suppress_pr_messages:
                print("    All unrecorded hours will be filled with NULL values.", file=logfile)
                print("    Filled NULL for", rows_affected, "rows.", file=logfile)

        conn.executescript("""DROP TABLE unrecorded_hours;
        DROP TABLE active_unrecorded_hours;""")

    conn.executescript("""DROP TABLE hourly_summary;
    DROP TABLE op_dates_hours;""")


def fill_base_year_calc_generation_parms(conn, base_year, future_year, logfile):
    """Fill in base year actual and retired generation in calc_generation_parms table.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year where data has been reported
    future_year -- the future year where generation could be retired or limited
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Filling in base year generation in calc_generation_parms.", file=logfile)
    # Summarize base year generation within region/fuel combinations for each
    # hour, also noting future reductions due to retired or capacity-limited
    # units.
    # If UAF does not have an offline date for unit, treat it as still operating
    # far into the future.  Otherwise, have to compare operating date
    # (translated from base year to future year) against offline date to see if
    # unit is retired in future.
    # Need to include temporal allocation order for hours, from different tables
    # depending on hierarchy type for region/fuel.
    # Each of the temporal hierarchy tables has a different name, and a
    # different name for the allocation order column, but the same process is
    # used for all of them.
    conn.execute("DELETE FROM calc_generation_parms")
    for (hier_tbl, order_col) in [('calc_1hour_hierarchy', 'one_hour_allocation_order'),
                                  ('calc_6hour_hierarchy', 'six_hour_allocation_order'),
                                  ('calc_24hour_hierarchy', 'twentyfour_hour_allocation_order')]:
        # Can't use SQL parameter substition for object names, so have to build
        # statement partially using string concatenation.
        conn.execute("""INSERT INTO calc_generation_parms
        (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour,
        temporal_allocation_order, base_actual_generation, base_retired_generation)
        SELECT
        hourly.ertac_region, hourly.ertac_fuel_unit_type_bin, hourly.op_date, hourly.op_hour,
        temporal.""" + order_col + """,
        SUM(hourly.gload),
        SUM(CASE WHEN COALESCE(uaf.capacity_limited_unit_flag, 'N') = 'Y'
            OR REPLACE(hourly.op_date, ?, ?) >= uaf.offline_start_date
            THEN hourly.gload
            ELSE 0.0 END)
        FROM calc_hourly_base hourly
        JOIN """ + hier_tbl + """ temporal
        ON hourly.ertac_region = temporal.ertac_region
        AND hourly.ertac_fuel_unit_type_bin = temporal.ertac_fuel_unit_type_bin
        AND hourly.op_date = temporal.op_date
        AND hourly.op_hour = temporal.op_hour
        JOIN calc_updated_uaf uaf
        ON hourly.orispl_code = uaf.orispl_code
        AND hourly.unitid = uaf.unitid
        AND hourly.ertac_fuel_unit_type_bin = uaf.ertac_fuel_unit_type_bin
        GROUP BY hourly.ertac_region, hourly.ertac_fuel_unit_type_bin, hourly.op_date, hourly.op_hour""",
                     (base_year, future_year))
    # If any of the totals were missing altogether, replace with 0 to make later
    # calculations simpler.
    conn.executescript("""UPDATE calc_generation_parms
    SET base_actual_generation = 0.0
    WHERE base_actual_generation IS NULL;
    UPDATE calc_generation_parms
    SET base_retired_generation = 0.0
    WHERE base_retired_generation IS NULL;""")


def calculate_non_peak_growth_factors(conn, logfile):
    """3.01: Calculate non-peak growth factors, using secant root-finding method.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Calculating non-peak growth factors.", file=logfile)
    # We're given an annual average growth factor and a peak growth factor, and
    # need to find the appropriate value for a non-peak factor so that the
    # resulting annual growth meets the specified value, when each hour has a
    # factor varying from peak to non-peak along a two-plateau linear
    # transition.  This problem is solved separately for each region/fuel
    # combination by starting with the annual and peak values as two initial
    # guesses for the non-peak value, then using the secant method to improve
    # the estimate of the non-peak factor.  Equation may not have a solution if
    # transition points are set too late, with all or most generation within
    # peak initial period.

    # Solve independently for each region/fuel in calc_growth_rate table, if we
    # have some base year generation to use.
    for (region, fuel, avg_factor, peak_factor, peak_hour, nonpeak_hour) in conn.execute("""SELECT ertac_region,
    ertac_fuel_unit_type_bin, annual_growth_factor, peak_growth_factor,
    transition_hour_peak_2_formula, transition_hour_formula_2_nonpeak
    FROM calc_growth_rates
    WHERE transition_formula = 'Linear'
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall():
        # jmj 9/18/14 add a fix to set the nonpeak factor equal to the average if the peak and average are the same
        if avg_factor == peak_factor:
            # Update current row of calc_growth_rates.
            conn.execute("""UPDATE calc_growth_rates
                SET non_peak_growth_factor = ?
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?""", (avg_factor, region, fuel))
        else:

            # We need annual total generation (summed over hours within current
            # region/fuel) from base year.
            (base_annual_gen,) = conn.execute("""SELECT SUM(base_actual_generation)
            FROM calc_generation_parms
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
            if base_annual_gen:
                # Can only solve for growth factor if base year had some generation
                # for this region/fuel.
                # Use peak factor and average factor as first two guesses for secant
                # method.  Want future_gen = base_gen * avg_factor, so compare
                # estimated future total against target.
                x0 = peak_factor
                y0 = estimate_future_generation(conn, region, fuel, peak_factor, x0, peak_hour,
                                                nonpeak_hour) - base_annual_gen * avg_factor
                x = avg_factor
                i = 0
                while True:
                    i = i + 1
                    y = estimate_future_generation(conn, region, fuel, peak_factor, x, peak_hour,
                                                   nonpeak_hour) - base_annual_gen * avg_factor
                    if abs(y) <= 1e-12 * (1 + abs(y0)):  # Small enough Y result, so stop.
                        break
                    if y == y0:  # Flat line, can not proceed.
                        logging.warning(
                            "Secant root-finding failed: impending division by zero for " + ertac_lib.nice_str(
                                (region, fuel)))
                        print(
                            "Warning: Secant root-finding failed: impending division by zero for " + ertac_lib.nice_str(
                                (region, fuel)), file=logfile)
                        print("  Transition hours to formula and to non-peak rate may need to be set earlier.",
                              file=logfile)
                        break
                    # Find delta X for next improved guess.
                    dx = - y * (x - x0) / (y - y0)
                    if abs(dx) <= 1e-12 * max(abs(x0), abs(x)):  # X values close enough, so stop.
                        break
                    if i > 10:  # Avoid runaway non-convergence.
                        # Warn has been depreciated since python 3.3. Use warning instead
                        logging.warning(
                            "Secant root-finding failed: excess iterations for " + ertac_lib.nice_str((region, fuel)))
                        print("Warning: Secant root-finding failed: excess iterations for " + ertac_lib.nice_str(
                            (region, fuel)), file=logfile)
                        break
                    # If we didn't hit any termination condition, repeat for next improved estimate.
                    (x0, y0, x) = (x, y, x + dx)

                # If numerical solution is physically impossible, reset negative
                # factor to 0, recompute all resulting hourly growth, and warn.
                if x < 0.0:
                    x = 0.0
                    y = estimate_future_generation(conn, region, fuel, peak_factor, x, peak_hour,
                                                   nonpeak_hour) - base_annual_gen * avg_factor
                    logging.warning("Impossible negative non-peak growth factor reset to 0.0 for " + ertac_lib.nice_str(
                        (region, fuel)))
                    print("Warning: Impossible negative non-peak growth factor reset to 0.0 for " + ertac_lib.nice_str(
                        (region, fuel)), file=logfile)
                    print("  Average growth factor is too low to be reached.", file=logfile)

                # Update current row of calc_growth_rates.
                conn.execute("""UPDATE calc_growth_rates
                SET non_peak_growth_factor = ?
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?""", (x, region, fuel))


def estimate_future_generation(conn, region, fuel, peak_factor, nonpeak_factor, peak_hour, nonpeak_hour):
    """Compute total future generation for one region/fuel, given particular peak and non-peak growth factors.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current ERTAC region
    fuel -- the current fuel bin
    peak_factor -- the initial peak growth factor for high-demand hours
    nonpeak_factor -- the final non-peak growth factor for low-demand hours
    peak_hour -- the last hour receiving the full peak factor before the transition
    nonpeak_hour -- the first hour receiving the non-peak factor after the transition

    """
    # Loop over calc_generation_parms for this region/fuel, calculate
    # hour-specific growth rate, apply to base generation, and add to running
    # total.
    total_future_gen = 0.0
    for (current_hour, base_gen) in conn.execute("""SELECT temporal_allocation_order,
    base_actual_generation
    FROM calc_generation_parms
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchall():
        hourly_factor = compute_linear_formula_growth_rate(peak_factor, nonpeak_factor, peak_hour, nonpeak_hour,
                                                           current_hour)
        # 20120109 Factors *ARE* direct multipliers now, instead of percentage changes.
        future_gen = base_gen * hourly_factor
        total_future_gen += future_gen
    return total_future_gen


def compute_linear_formula_growth_rate(peak_factor, nonpeak_factor, peak_hour, nonpeak_hour, current_hour):
    """Compute growth rate for a particular hour, using linear change from peak to non-peak level.

    Keyword arguments:
    peak_factor -- the initial peak growth factor for high-demand hours
    nonpeak_factor -- the final non-peak growth factor for low-demand hours
    peak_hour -- the last hour receiving the full peak factor before the transition
    nonpeak_hour -- the first hour receiving the non-peak factor after the transition
    current_hour -- the hour to evaluate

    Returns a floating-point growth rate that is peak, non-peak, or somewhere in between.

    """
    if current_hour <= peak_hour:
        current_factor = peak_factor
    elif current_hour < nonpeak_hour:
        current_factor = peak_factor + (nonpeak_factor - peak_factor) * float(current_hour - peak_hour) / float(
            nonpeak_hour - peak_hour)
    else:
        current_factor = nonpeak_factor
    return current_factor


def calculate_future_generation_growth(conn, logfile):
    """1.08: Calculate future hourly growth and generation, based on estimated growth factors.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    # For each region/fuel combination in calc_growth_rates, loop over the
    # matching hourly rows in calc_generation_parms, computing and using the
    # hour-specific growth rate to calculate the future generation at that hour.
    for (region, fuel, peak_factor, nonpeak_factor, peak_hour, nonpeak_hour) in conn.execute("""SELECT ertac_region,
    ertac_fuel_unit_type_bin, peak_growth_factor, non_peak_growth_factor,
    transition_hour_peak_2_formula, transition_hour_formula_2_nonpeak
    FROM calc_growth_rates
    WHERE transition_formula = 'Linear'
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall():

        for (rowid, current_hour, base_gen) in conn.execute("""SELECT rowid,
        temporal_allocation_order, base_actual_generation
        FROM calc_generation_parms
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchall():
            hourly_factor = compute_linear_formula_growth_rate(peak_factor, nonpeak_factor, peak_hour, nonpeak_hour,
                                                               current_hour)
            # 20120109 Factor is direct multiplier now, instead of percentage change.
            future_gen = base_gen * hourly_factor
            future_growth = future_gen - base_gen
            conn.execute("""UPDATE calc_generation_parms
            SET hour_specific_growth_rate = ?,
            future_projected_generation = ?,
            future_projected_growth = ?
            WHERE rowid = ?""", (hourly_factor, future_gen, future_growth, rowid))


def update_calc_generation_parms_transfers(conn, logfile):
    """Update estimated future generation to include demand transfers.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """

    print(file=logfile)
    print("Implementing demand transfers.", file=logfile)
    # Fill in all hourly rows in calc_generation_parms with calendar_hour value,
    # and include any net demand transfer for every region/fuel/hour where some
    # demand transfer has been specified.
    conn.executescript("""UPDATE calc_generation_parms
    SET calendar_hour = (SELECT calendar_hours.calendar_hour
    FROM calendar_hours
    WHERE calendar_hours.op_date = calc_generation_parms.op_date
    AND calendar_hours.op_hour = calc_generation_parms.op_hour);

    UPDATE calc_generation_parms
    SET net_demand_transfer = (SELECT net_demand_change
    FROM calc_demand_transfer_summary
    WHERE calc_demand_transfer_summary.transfer_region = calc_generation_parms.ertac_region
    AND calc_demand_transfer_summary.transfer_fuel = calc_generation_parms.ertac_fuel_unit_type_bin
    AND calc_demand_transfer_summary.calendar_hour = calc_generation_parms.calendar_hour);

    UPDATE calc_generation_parms
    SET net_demand_transfer = 0
    WHERE net_demand_transfer IS NULL;""")

    negative_generation_hours = conn.execute("""SELECT ertac_region, ertac_fuel_unit_type_bin, calendar_hour, net_demand_transfer, future_projected_generation
    FROM calc_generation_parms
    WHERE future_projected_generation + net_demand_transfer < 0
    ORDER BY calendar_hour""").fetchall()

    if len(negative_generation_hours) > 0:
        logging.warning(
            "At least one hour has a demand transfer that results in negative generation in an hour and will cause the projection to fail - see log file for details.")
        print(
            "Warning: the following region/fuel unit type bin/calendar hours have a negative demand transfer that will result in negative generation and cause the projection to fail:",
            file=logfile)
        for ngh in negative_generation_hours:
            print("  " + ertac_lib.nice_str(ngh), file=logfile)


def fill_gload_from_sload(conn, logfile):
    """2.02: Fill missing GLOAD for units that report SLOAD instead.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Filling in missing GLOAD values in hourly data:", file=logfile)
    rows_affected = conn.execute("""UPDATE calc_hourly_base
    SET gload = 0.1 * sload
    WHERE gload IS NULL
    AND sload IS NOT NULL""").rowcount
    print("Updated", rows_affected, "hourly rows that had SLOAD without GLOAD.", file=logfile)


def delete_feb29_data(conn, base_year, future_year, logfile):
    """Delete February 29 base year hourly data that won't be used for non-leap future year.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year where data will be deleted
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Deleting base year hourly data from February 29:", file=logfile)
    rows_affected = conn.execute("DELETE FROM calc_hourly_base WHERE op_date = ?", (base_year + "-02-29",)).rowcount
    print("Deleted", rows_affected, "hourly rows.", file=logfile)

    # jmj 1/31/2018 calendar hours was not used after the feb 29 deleltion code before, but after demand transfers were implemented
    # it was so the table needs to be recreated to properly deal with the removal of feb29
    conn.executescript("""DROP TABLE calendar_hours""")
    ertac_lib.make_calendar_hours(base_year, future_year, conn)


# jmj 6/10/2019 added function to calculate emission rates to correct rounding issues
def calculate_base_year_emission_rates(conn, logfile):
    """2.03: Calculate base year hours of operation and emission rates.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Calculating base year emission rates.", file=logfile)
    rows_affected = conn.execute("""UPDATE calc_hourly_base
    SET so2_rate = so2_mass/heat_input,
        so2_rate_flag = 'ERTAC Calculated',
        nox_rate = nox_mass/heat_input,
        nox_rate_flag = 'ERTAC Calculated',
        co2_rate = co2_mass/heat_input,
        co2_rate_flag = 'ERTAC Calculated'
    WHERE heat_input > 0""").rowcount
    print("Updated", rows_affected, "hourly rows that had heat input with recalculated emissions rate.", file=logfile)


def calc_unit_stats(conn, logfile):
    """2.04: Calculate unit operating statistics and store results in UAF.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Calculating unit operating statistics to store in UAF.", file=logfile)

    conn.executescript("""CREATE TEMPORARY TABLE region_fuel_plant_unit
    (region TEXT NOT NULL COLLATE NOCASE,
    fuel TEXT NOT NULL COLLATE NOCASE,
    plant TEXT NOT NULL COLLATE NOCASE,
    unit TEXT NOT NULL COLLATE NOCASE,
    PRIMARY KEY (region, fuel, plant, unit));

    CREATE TEMPORARY TABLE unit_hourly
    (op_date TEXT NOT NULL,
    op_hour INTEGER NOT NULL,
    gload REAL,
    so2_mass REAL,
    so2_rate REAL,
    nox_rate REAL,
    nox_mass REAL,
    heat_input REAL,
    heat_rate REAL,
    PRIMARY KEY (op_date, op_hour));

    INSERT INTO region_fuel_plant_unit
    SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid
    FROM calc_hourly_base;""")

    for (region, fuel, plant, unit) in conn.execute("""SELECT region, fuel, plant, unit
    FROM region_fuel_plant_unit
    ORDER BY region, fuel, plant, unit""").fetchall():
        # Get Ozone season, hard limits, and SD multipliers from input variables.
        (base_year, ozone_start_date, ozone_end_date, heat_rate_min, heat_rate_max, heat_rate_stdev,
         nox_min_ef, nox_max_ef, nox_stdev, so2_min_ef, so2_max_ef, so2_stdev) = conn.execute("""SELECT
        base_year, ozone_start_date, ozone_end_date, heat_rate_min, heat_rate_max, heat_rate_stdev,
        nox_min_ef, nox_max_ef, nox_stdev, so2_min_ef, so2_max_ef, so2_stdev
        FROM calc_input_variables
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

        # Get hourly data for unit and calculate hourly heat rates where possible.
        conn.execute("""DELETE FROM unit_hourly""")
        conn.execute("""INSERT INTO unit_hourly
        (op_date, op_hour, gload, so2_mass, so2_rate, nox_rate, nox_mass, heat_input)
        SELECT op_date, op_hour, gload, so2_mass, so2_rate, nox_rate, nox_mass, heat_input
        FROM calc_hourly_base
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit))
        conn.execute("""UPDATE unit_hourly
        SET heat_rate = 1000.0 * heat_input / gload
        WHERE heat_input > 0.0
        AND gload > 0.0""")

        # We only want statistics for positive rates, ignoring zeros, so the
        # population sizes may vary and each must be counted separately.
        # Note: the _sd values are sample standard deviations; the _stdev values
        # are multiplier factors to define an interval around the mean.
        rate_list = conn.execute("""SELECT so2_rate FROM unit_hourly WHERE so2_rate > 0.0""").fetchall()
        (so2_mean, so2_sd) = calc_list_stats(rate_list)
        rate_list = conn.execute("""SELECT nox_rate FROM unit_hourly WHERE nox_rate > 0.0""").fetchall()
        (nox_mean, nox_sd) = calc_list_stats(rate_list)
        rate_list = conn.execute("""SELECT heat_rate FROM unit_hourly WHERE heat_rate > 0.0""").fetchall()
        (hr_mean, hr_sd) = calc_list_stats(rate_list)

        if heat_rate_stdev is not None and hr_sd is not None:
            heat_rate_lower_stat = hr_mean - heat_rate_stdev * hr_sd
            heat_rate_upper_stat = hr_mean + heat_rate_stdev * hr_sd
        else:
            (heat_rate_lower_stat, heat_rate_upper_stat) = (None, None)

        if nox_stdev is not None and nox_sd is not None:
            nox_ef_lower_stat = nox_mean - nox_stdev * nox_sd
            nox_ef_upper_stat = nox_mean + nox_stdev * nox_sd
        else:
            (nox_ef_lower_stat, nox_ef_upper_stat) = (None, None)

        if so2_stdev is not None and so2_sd is not None:
            so2_ef_lower_stat = so2_mean - so2_stdev * so2_sd
            so2_ef_upper_stat = so2_mean + so2_stdev * so2_sd
        else:
            (so2_ef_lower_stat, so2_ef_upper_stat) = (None, None)

        # Warn if hard limits and statistical limits have disjoint ranges.
        if ((heat_rate_max is not None and heat_rate_lower_stat is not None and heat_rate_max < heat_rate_lower_stat)
                or
                (
                        heat_rate_min is not None and heat_rate_upper_stat is not None and heat_rate_min > heat_rate_upper_stat)):
            print(("Warning: unit has disjoint hard limits and statistical limits on heat rate:  "
                   + ertac_lib.nice_str((region, fuel, plant, unit, heat_rate_min, heat_rate_max,
                                         heat_rate_lower_stat, heat_rate_upper_stat))), file=logfile)

        if ((nox_max_ef is not None and nox_ef_lower_stat is not None and nox_max_ef < nox_ef_lower_stat)
                or
                (nox_min_ef is not None and nox_ef_upper_stat is not None and nox_min_ef > nox_ef_upper_stat)):
            print(("Warning: unit has disjoint hard limits and statistical limits on NOx rate:  "
                   + ertac_lib.nice_str((region, fuel, plant, unit, nox_min_ef, nox_max_ef,
                                         nox_ef_lower_stat, nox_ef_upper_stat))), file=logfile)

        if ((so2_max_ef is not None and so2_ef_lower_stat is not None and so2_max_ef < so2_ef_lower_stat)
                or
                (so2_min_ef is not None and so2_ef_upper_stat is not None and so2_min_ef > so2_ef_upper_stat)):
            print(("Warning: unit has disjoint hard limits and statistical limits on SO2 rate:  "
                   + ertac_lib.nice_str((region, fuel, plant, unit, so2_min_ef, so2_max_ef,
                                         so2_ef_lower_stat, so2_ef_upper_stat))), file=logfile)

        # Calculate annual and OS/non-OS average rates from total annual and
        # seasonal activity.
        (ann_gload, ann_so2, ann_nox, ann_hi) = conn.execute("""SELECT SUM(gload),
            SUM(so2_mass), SUM(nox_mass), SUM(heat_input) FROM unit_hourly""").fetchone()
        os_start = ertac_lib.convert_ozone_date(ozone_start_date, base_year)
        os_end = ertac_lib.convert_ozone_date(ozone_end_date, base_year)
        (os_gload, os_so2, os_nox, os_hi) = conn.execute("""SELECT SUM(gload),
            SUM(so2_mass), SUM(nox_mass), SUM(heat_input) FROM unit_hourly
            WHERE op_date BETWEEN ? AND ?""", (os_start, os_end)).fetchone()
        (nonos_gload, nonos_so2, nonos_nox, nonos_hi) = conn.execute("""SELECT SUM(gload),
            SUM(so2_mass), SUM(nox_mass), SUM(heat_input) FROM unit_hourly
            WHERE op_date NOT BETWEEN ? AND ?""", (os_start, os_end)).fetchone()

        if ann_gload is not None and ann_gload > 0.0 and ann_hi is not None:
            ann_heat_rate = round(1000.0 * ann_hi / ann_gload, 12)
        else:
            ann_heat_rate = None

        if ann_hi is not None and ann_hi > 0.0 and ann_so2 is not None:
            ann_so2_rate = round(ann_so2 / ann_hi, 12)
        else:
            ann_so2_rate = None

        if ann_hi is not None and ann_hi > 0.0 and ann_nox is not None:
            ann_nox_rate = round(ann_nox / ann_hi, 12)
        else:
            ann_nox_rate = None

        if os_gload is not None and os_gload > 0.0 and os_hi is not None:
            os_heat_rate = round(1000.0 * os_hi / os_gload, 12)
        else:
            os_heat_rate = None

        if os_hi is not None and os_hi > 0.0 and os_so2 is not None:
            os_so2_rate = round(os_so2 / os_hi, 12)
        else:
            os_so2_rate = None

        if os_hi is not None and os_hi > 0.0 and os_nox is not None:
            os_nox_rate = round(os_nox / os_hi, 12)
        else:
            os_nox_rate = None

        if nonos_gload is not None and nonos_gload > 0.0 and nonos_hi is not None:
            nonos_heat_rate = round(1000.0 * nonos_hi / nonos_gload, 12)
        else:
            nonos_heat_rate = None

        if nonos_hi is not None and nonos_hi > 0.0 and nonos_so2 is not None:
            nonos_so2_rate = round(nonos_so2 / nonos_hi, 12)
        else:
            nonos_so2_rate = None

        if nonos_hi is not None and nonos_hi > 0.0 and nonos_nox is not None:
            nonos_nox_rate = round(nonos_nox / nonos_hi, 12)
        else:
            nonos_nox_rate = None

        # Store limits and average rates in UAF.
        conn.execute("""UPDATE calc_updated_uaf
        SET heat_rate_lower_limit = ?,
        heat_rate_upper_limit = ?,
        heat_rate_lower_stat = ?,
        heat_rate_upper_stat = ?,
        heat_rate_avg = ?,
        heat_rate_os_avg = ?,
        heat_rate_nonos_avg = ?,
        nox_ef_lower_limit = ?,
        nox_ef_upper_limit = ?,
        nox_ef_lower_stat = ?,
        nox_ef_upper_stat = ?,
        nox_ef_avg = ?,
        nox_ef_os_avg = ?,
        nox_ef_nonos_avg = ?,
        so2_ef_lower_limit = ?,
        so2_ef_upper_limit = ?,
        so2_ef_lower_stat = ?,
        so2_ef_upper_stat = ?,
        so2_ef_avg = ?,
        so2_ef_os_avg = ?,
        so2_ef_nonos_avg = ?
        WHERE orispl_code = ?
        AND unitid = ?
        AND ertac_fuel_unit_type_bin = ?""", (heat_rate_min, heat_rate_max,
                                              heat_rate_lower_stat, heat_rate_upper_stat,
                                              ann_heat_rate, os_heat_rate, nonos_heat_rate,
                                              nox_min_ef, nox_max_ef, nox_ef_lower_stat, nox_ef_upper_stat,
                                              ann_nox_rate, os_nox_rate, nonos_nox_rate,
                                              so2_min_ef, so2_max_ef, so2_ef_lower_stat, so2_ef_upper_stat,
                                              ann_so2_rate, os_so2_rate, nonos_so2_rate,
                                              plant, unit, fuel))

    conn.executescript("""DROP TABLE region_fuel_plant_unit;
    DROP TABLE unit_hourly;""")


def calc_list_stats(some_list):
    """Calculate mean and sample standard deviation of a list of numbers which are each in 1-element tuples.

    Keyword arguments:
    some_list -- the list of numbers

    Returns tuple of (mean, sd)

    """
    n = 1.0 * len(some_list)
    if n < 1.0:
        return None, None
    # Python docs say that math.fsum() is more accurate than regular sum().
    mean = math.fsum(x[0] for x in some_list) / n
    if n < 2.0:
        return mean, None
    sum_square_dev = math.fsum((x[0] - mean) ** 2 for x in some_list)
    sample_var = sum_square_dev / (n - 1.0)
    sample_sd = math.sqrt(sample_var)
    return mean, sample_sd


# jmj 11/20/2013 adding base year operating calculations
# jmj 9/17/2014 changed the methodology to sum op_time
def calculate_base_year_op_hours(conn, logfile):
    """2.05: Calculate base year hours of operation and emission rates.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Calculating base year hours of operation.", file=logfile)
    for (hours, fuel, plant, unit) in conn.execute("""SELECT sum(op_time), ertac_fuel_unit_type_bin, orispl_code, unitid
    FROM calc_hourly_base
    GROUP BY orispl_code, unitid""").fetchall():
        if hours:
            conn.execute("""UPDATE calc_updated_uaf
                    SET operating_hours_by = ?
                    WHERE orispl_code = ?
                    AND unitid = ?
                    AND ertac_fuel_unit_type_bin = ?""", (hours, plant, unit, fuel))


def calculate_heat_rates(conn, future_year, logfile):
    """2.06: Calculate average heat rate and update ERTAC heat rate.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    future_year -- the future year where generation will be allocated
    logfile -- file where logging messages will be written

    """
    # PS2.02 - Calculate average heat rate (store in column AS) from each unit's
    # base year data.  Use state-supplied nominal heat rate (column AR if
    # present) or actual base year average as ERTAC heat rate (store in column
    # AT).  For new units, which have no base year records and thus no base year
    # average heat rate, leave any previously-supplied value in AT unchanged,
    # unless AR has an overriding value filled in.
    print(file=logfile)
    print("Calculating average heat rates.", file=logfile)
    for (plant, unit, fuel, total_hi, total_gload) in conn.execute("""SELECT orispl_code,
    unitid, ertac_fuel_unit_type_bin, SUM(heat_input), SUM(gload)
    FROM calc_hourly_base
    GROUP BY orispl_code, unitid, ertac_fuel_unit_type_bin
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""").fetchall():

        # 20120410 Added warning for no heat input or no gload in hourly data.
        if total_hi is None or total_hi == 0.0 or total_gload is None or total_gload == 0.0:
            print("  Warning: unit " + ertac_lib.nice_str((plant, unit, fuel))
                  + " has no heat input or gload in hourly data, so can't calculate BY average heat rate", file=logfile)
        else:
            avg_heat_rate = round(total_hi * 1000.0 / total_gload, 12)
            conn.execute("""UPDATE calc_updated_uaf
            SET calc_by_average_heat_rate = ?
            WHERE orispl_code = ?
            AND unitid = ?
            AND ertac_fuel_unit_type_bin = ?""", (avg_heat_rate, plant, unit, fuel))

    conn.execute("""UPDATE calc_updated_uaf
    SET ertac_heat_rate = COALESCE(nominal_heat_rate, calc_by_average_heat_rate)
    WHERE COALESCE(nominal_heat_rate, calc_by_average_heat_rate) IS NOT NULL""")

    # 20120410 Added warning for units without ertac_heat_rate.
    # jmj updated to exclude Non-EGU's and retired units
    units_no_heat_rate = conn.execute("""SELECT orispl_code, unitid, ertac_fuel_unit_type_bin
    FROM calc_updated_uaf
    WHERE (ertac_heat_rate IS NULL OR ertac_heat_rate = 0.0)
    AND camd_by_hourly_data_type NOT IN ('Non-EGU')
    AND offline_start_date > ?
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""", [ertac_lib.first_day_of(future_year)]).fetchall()
    if len(units_no_heat_rate) > 0:
        print(file=logfile)
        print("Warning: units with no ERTAC_HEAT_RATE:", file=logfile)
        for unusable_unit in units_no_heat_rate:
            print("  " + ertac_lib.nice_str(unusable_unit), file=logfile)


def calculate_heat_inputs(conn, logfile):
    """2.07: Calculate percentile-based max heat input.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    # Calculate 95th or other percentile heat input from all the non-null,
    # non-zero values in each unit's base year.
    print(file=logfile)
    print("Calculating max heat inputs.", file=logfile)
    for (region, fuel, plant, unit) in conn.execute("""SELECT DISTINCT ertac_region,
    ertac_fuel_unit_type_bin, orispl_code, unitid
    FROM calc_hourly_base
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""").fetchall():

        percentile_result = conn.execute("""SELECT heat_input_calculation_percentile
        FROM ertac_input_variables
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
        if percentile_result is None:
            print("  Warning: ertac_input_variables has no heat_input_calculation_percentile for region/fuel: "
                  + ertac_lib.nice_str((region, fuel)), file=logfile)
        else:
            (heat_input_percentile,) = percentile_result
            heat_list = conn.execute("""SELECT heat_input
            FROM calc_hourly_base
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND heat_input > 0.0
            ORDER BY heat_input DESC""", (region, fuel, plant, unit)).fetchall()
            if len(heat_list) > 0:
                slot = int(len(heat_list) * (1.0 - heat_input_percentile / 100.0))
                # Make sure we haven't rounded off to an illegal position.
                if slot < 0:
                    slot = 0
                if slot >= len(heat_list):
                    slot = len(heat_list) - 1
                max_heat_input = heat_list[slot][0]
                conn.execute("""UPDATE calc_updated_uaf
                SET hourly_base_max_actual_hi = ?
                WHERE orispl_code = ?
                AND unitid = ?
                AND ertac_fuel_unit_type_bin = ?""", (max_heat_input, plant, unit, fuel))

    # For units where heat input values weren't available, compute equivalent based
    # on generation capacity.
    for (rowid, hourly_base_max_actual_hi, max_unit_heat_input, nameplate_capacity,
         max_summer_capacity, max_winter_capacity, ertac_heat_rate) in conn.execute("""SELECT rowid,
    hourly_base_max_actual_hi, max_unit_heat_input, nameplate_capacity,
    max_summer_capacity, max_winter_capacity, ertac_heat_rate
    FROM calc_updated_uaf""").fetchall():
        # FIXED THIS STATEMENT TO ADDRESS TAKING THE MAX OF A POTENTIAL NONE VALUE
        max_ertac_hi_hourly_summer_list = [hourly_base_max_actual_hi, max_unit_heat_input]
        max_ertac_hi_hourly_summer = max(i for i in max_ertac_hi_hourly_summer_list if i is not None)
        if max_ertac_hi_hourly_summer is None:
            # Neither heat input available, so convert generation capacity if possible
            if max(nameplate_capacity, max_summer_capacity,
                   max_winter_capacity) is not None and ertac_heat_rate is not None:
                max_ertac_hi_hourly_summer = ertac_heat_rate * max(nameplate_capacity, max_summer_capacity,
                                                                   max_winter_capacity) / 1000.0
        if max_ertac_hi_hourly_summer is not None:
            conn.execute("""UPDATE calc_updated_uaf
            SET max_ertac_hi_hourly_summer = ?
            WHERE rowid = ?""", (max_ertac_hi_hourly_summer, rowid))

    # 20120410 Added warning for units without max_ertac_hi_hourly_summer.
    units_no_max_hi = conn.execute("""SELECT orispl_code, unitid, ertac_fuel_unit_type_bin
    FROM calc_updated_uaf
    WHERE max_ertac_hi_hourly_summer IS NULL OR max_ertac_hi_hourly_summer = 0.0
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""").fetchall()
    if len(units_no_max_hi) > 0:
        print(file=logfile)
        print("Warning: units with no MAX_ERTAC_HI_HOURLY_SUMMER:", file=logfile)
        for unusable_unit in units_no_max_hi:
            print("  " + ertac_lib.nice_str(unusable_unit), file=logfile)


def calculate_optimal_loads(conn, logfile):
    """2.08: Calculate percentile-based optimal load threshold.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    # Calculate 90th or other percentile gross load from all the non-null,
    # non-zero values in each unit's base year.
    print(file=logfile)
    print("Calculating optimal load thresholds.", file=logfile)
    for (region, fuel, plant, unit) in conn.execute("""SELECT DISTINCT ertac_region,
    ertac_fuel_unit_type_bin, orispl_code, unitid
    FROM calc_hourly_base
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""").fetchall():

        percentile_result = conn.execute("""SELECT unit_optimal_load_threshold_determinant
        FROM ertac_input_variables
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
        if percentile_result is None:
            print("  Warning: ertac_input_variables has no unit_optimal_load_threshold_determinant for region/fuel: "
                  + ertac_lib.nice_str((region, fuel)), file=logfile)
        else:
            (unit_optimal_load_threshold_determinant,) = percentile_result
            gload_list = conn.execute("""SELECT gload
            FROM calc_hourly_base
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND gload > 0.0
            ORDER BY gload DESC""", (region, fuel, plant, unit)).fetchall()
            if len(gload_list) > 0:
                slot = int(len(gload_list) * (1.0 - unit_optimal_load_threshold_determinant / 100.0))
                if slot < 0:
                    slot = 0
                if slot >= len(gload_list):
                    slot = len(gload_list) - 1
                unit_max_optimal_load_threshold = gload_list[slot][0]
                conn.execute("""UPDATE calc_updated_uaf
                SET unit_max_optimal_load_threshold = ?
                WHERE orispl_code = ?
                AND unitid = ?
                AND ertac_fuel_unit_type_bin = ?""",
                             (unit_max_optimal_load_threshold, plant, unit, fuel))
            else:
                # 20120410 Added warning for no gload in hourly data.
                print("  Warning: unit " + ertac_lib.nice_str((plant, unit, fuel))
                      + " has no gload in hourly data, so can't calculate UNIT_MAX_OPTIMAL_LOAD_THRESHOLD",
                      file=logfile)


def calculate_utilization_fractions(conn, base_year, future_year, logfile):
    """2.09: Calculate actual and maximum utilization fractions.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    future_year -- future year for projections
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Calculating utilization fractions.", file=logfile)

    # 20120203 Changed to use max_ertac_hi_hourly_summer instead of hourly_base_max_actual_heat_input.
    for (plant, unit, fuel, region, by_type, max_hi, state_input_uf, offline_start_date) in conn.execute("""SELECT orispl_code, unitid, ertac_fuel_unit_type_bin,
    ertac_region, camd_by_hourly_data_type, max_ertac_hi_hourly_summer, max_annual_state_uf, offline_start_date
    FROM calc_updated_uaf
    ORDER BY orispl_code, unitid, ertac_fuel_unit_type_bin""").fetchall():

        # Compute total heat input for unit, to find calculated UF.  Use the
        # larger of calculated UF or the defalt UF from input variables, except
        # if there is a state-supplied UF value to override the others.
        (total_hi,) = conn.execute("""SELECT SUM(heat_input)
        FROM calc_hourly_base
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

        # 20120228 Added warning for no heat input in hourly data.
        if total_hi is None and by_type.upper() in ['FULL', 'PARTIAL']:
            print("  Warning: unit " + ertac_lib.nice_str((region, fuel, plant, unit))
                  + " has no heat input in hourly data, so can't calculate utilization fraction", file=logfile)

        if total_hi is not None and max_hi is not None and max_hi > 0.0:
            calculated_uf = round(total_hi / (max_hi * ertac_lib.hours_in_year(base_year, future_year)), 12)
        else:
            calculated_uf = None

        default_result = conn.execute("""SELECT maximum_annual_ertac_uf
        FROM ertac_input_variables
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
        if default_result is None:
            default_uf = None
        else:
            (default_uf,) = default_result
        if state_input_uf is not None:
            max_ertac_uf = state_input_uf
        # FIXED THIS STATEMENT TO ADDRESS TAKING THE MAX OF A POTENTIAL NONE VALUE
        else:
            max_ertac_uf_list = [calculated_uf, default_uf]
            max_ertac_uf = max(i for i in max_ertac_uf_list if i is not None)

        conn.execute("""UPDATE calc_updated_uaf
        SET calculated_by_uf = ?,
        max_annual_ertac_uf = ?
        WHERE orispl_code = ?
        AND unitid = ?
        AND ertac_fuel_unit_type_bin = ?""",
                     (calculated_uf, max_ertac_uf, plant, unit, fuel))


def calc_hourly_proxy(conn, base_year, future_year, logfile):
    """2.10: Calculate hourly proxy generation for new units.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year when the units were not yet active
    future_year -- the future year where generation will be created
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Calculating hourly proxy generation for new units.", file=logfile)
    conn.execute("DELETE FROM calc_generation_proxy")

    for (region, fuel) in conn.execute("""SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin
    FROM calc_hourly_base
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall():

        # Find any new units in UAF for each region/fuel we're using.  Can't
        # rely on camd_BY_hourly_data_type for unit status, because if fuel bin
        # changed between base year and future year, need proxy generation for
        # new fuel, even if UAF shows multiple "Full" records for that unit.

        # For example plant 2434, unit 006001 is listed once as "Full" from
        # 1970-2009 for Coal, and another time as "Full" with an online start
        # date of 2009 for Oil.

        # So, if we're using a 2007 base year, the hourly data is from Coal,
        # which is shutdown and not projected into the future, and we need to
        # create proxy entries for a "New" oil unit, even though the UAF says
        # "Full" there.

        # On the other hand, if we're starting with a base year of 2010, the
        # CAMD hourly data is from Oil and does get projected into the future,
        # and there is no proxy data in that case.

        # To find the correct set of new units, we have to have an online date
        # that is after the base year, and before or during the future year.  A
        # missing online date is presumed to be already operating, and not new.

        # We also have to have on offline date that is empty (meaning not shut
        # down), or else falls during or after the future year.

        day_after_base = ertac_lib.first_day_after(base_year)
        day_after_future = ertac_lib.first_day_after(future_year)
        first_future = ertac_lib.first_day_of(future_year)

        for (plant, unit, state, name) in conn.execute("""
        SELECT orispl_code, unitid, state, facility_name
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND online_start_date >= ?
        AND online_start_date < ?
        AND offline_start_date > ?
        AND camd_by_hourly_data_type NOT IN ('Non-EGU')
        ORDER BY orispl_code, unitid""", (region, fuel, day_after_base,
                                          day_after_future, first_future)).fetchall():
            ertac_lib.compute_proxy_generation(conn, region, fuel, plant, unit, state, name, base_year, future_year,
                                               logfile)


def fill_unit_hierarchy(conn, base_year, future_year, logfile):
    """1.09: Set the unit allocation order based on utilization.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year where data has been reported
    future_year -- the future year where generation will be allocated
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Setting unit allocation order.", file=logfile)
    conn.execute("DELETE FROM calc_unit_hierarchy")
    # Assignment of unit hierarchy is done in two passes.
    # First, for all units which operated during the base year and will still be
    # operating in the future year, ranks are assigned based on utilization
    # fraction (from the base year).
    # Second, for all new units that went online after the base year, a
    # percentile-based insertion position is chosen among the already-existing
    # units, and the new units are inserted after that rank, bumping some
    # existing units further down the list.
    for (region, fuel) in conn.execute("""SELECT DISTINCT ertac_region, ertac_fuel_unit_type_bin
    FROM calc_updated_uaf
    ORDER BY ertac_region, ertac_fuel_unit_type_bin""").fetchall():

        # Rank all units active during at least some part of both base year and
        # future year for current region and fuel.
        # 20120210 Don't rank non-EGUs; order based on calculated_BY_UF.
        allocation_order = 1

        for (plant, unit, state) in conn.execute("""SELECT orispl_code, unitid, state
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND online_start_date < ?
        AND offline_start_date > ?
        AND camd_by_hourly_data_type <> 'Non-EGU'
        ORDER BY calculated_by_uf DESC, orispl_code, unitid""",
                                                 (region, fuel, ertac_lib.first_day_after(base_year),
                                                  ertac_lib.first_day_of(future_year))).fetchall():
            conn.execute("""INSERT INTO calc_unit_hierarchy (ertac_region,
            ertac_fuel_unit_type_bin, orispl_code, unitid, unit_allocation_order, state)
            VALUES (?, ?, ?, ?, ?, ?)""",
                         (region, fuel, plant, unit, allocation_order, state))

            allocation_order += 1

        # Now get all new units not active during base year but active during
        # future year.
        # jmj 12/28/2018 adding a check for non-EGUs to prevent addition of new non-EGUs
        new_units = conn.execute("""SELECT orispl_code, unitid, state
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND online_start_date >= ?
        AND online_start_date < ?
        AND offline_start_date > ?
        AND camd_by_hourly_data_type <> 'Non-EGU'
        ORDER BY max_annual_ertac_uf DESC, orispl_code, unitid""",
                                 (region, fuel, ertac_lib.first_day_after(base_year),
                                  ertac_lib.first_day_after(future_year),
                                  ertac_lib.first_day_of(future_year))).fetchall()

        if len(new_units) > 0:
            # Figure out where the new unit ranks start, and move existing units
            # to make room.
            input_result = conn.execute("""SELECT new_unit_hierarchy_placement_percentile
            FROM ertac_input_variables
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

            if input_result is None:
                print("  Warning: no new_unit_hierarchy_placement_percentile in input variables "
                      + "for region/fuel " + ertac_lib.nice_str((region, fuel)) + " so will set to 95%", file=logfile)
                placement_percentile = 95.0
            else:
                (placement_percentile,) = input_result

            # Current value in allocation_order is next available rank.  How far
            # do we need to back up?
            anchor_rank = allocation_order - int(allocation_order * placement_percentile / 100.0)

            # Leave ranks 1 through anchor_rank unchanged, and move other
            # existing units to make room for the new units that will be
            # inserted in the gap.
            for (plant, unit) in conn.execute("""SELECT orispl_code, unitid
            FROM calc_unit_hierarchy
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND unit_allocation_order > ?
            ORDER BY unit_allocation_order DESC""", (region, fuel, anchor_rank)).fetchall():
                conn.execute("""UPDATE calc_unit_hierarchy
                SET unit_allocation_order = unit_allocation_order + ?
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?""", (len(new_units), region, fuel, plant, unit))

            # Now can insert new units, starting after the anchor position.
            allocation_order = anchor_rank + 1

            for (plant, unit, state) in new_units:
                conn.execute("""INSERT INTO calc_unit_hierarchy (ertac_region,
                ertac_fuel_unit_type_bin, orispl_code, unitid, unit_allocation_order, state)
                VALUES (?, ?, ?, ?, ?, ?)""",
                             (region, fuel, plant, unit, allocation_order, state))

                allocation_order += 1


def calc_max_gload(conn, logfile):
    """Calculate and update max_by_hourly_gload, needed for calculating proxy generation.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    logfile -- file where logging messages will be written

    """
    print(file=logfile)
    print("Calculating max gload for existing units.", file=logfile)

    for (region, fuel, plant, unit, max_gload) in conn.execute("""SELECT ertac_region,
    ertac_fuel_unit_type_bin, orispl_code, unitid, MAX(gload)
    FROM calc_hourly_base
    GROUP BY ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid""").fetchall():
        conn.execute("""UPDATE calc_updated_uaf
        SET max_by_hourly_gload = ?
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND  unitid = ?""", (max_gload, region, fuel, plant, unit))


def write_calculated_data(conn, out_prefix, logfile):
    """1.09, 2.12: Write out preprocessed ERTAC EGU data for next phase.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    out_prefix -- optional prefix added to each output file name
    logfile -- file where logging messages will be written

    """
    # Intermediate-stage data is exported, instead of simply being passed within
    # the database to the next phase, so that users can review and manually
    # modify the data if necessary.
    # For V2, have expanded version of UAF, input variables, and generation parms,
    # and new table for demand transfers.
    ertac_lib.export_table_to_csv('calc_hourly_base', out_prefix, 'calc_hourly_base.csv', conn,
                                  ertac_tables.calc_hourly_columns, logfile)
    ertac_lib.export_table_to_csv('calc_updated_uaf', out_prefix, 'calc_updated_uaf_v2.csv', conn,
                                  ertac_tables.calc_uaf_columns, logfile)
    ertac_lib.export_table_to_csv('calc_unit_hierarchy', out_prefix, 'calc_unit_hierarchy.csv', conn,
                                  ertac_tables.unit_hierarchy_columns, logfile)
    # Use actual column names for temporal hierarchy tables, since their ordering columns are named differently.
    ertac_lib.export_table_to_csv('calc_1hour_hierarchy', out_prefix, 'calc_1hour_hierarchy.csv', conn, None, logfile)
    ertac_lib.export_table_to_csv('calc_6hour_hierarchy', out_prefix, 'calc_6hour_hierarchy.csv', conn, None, logfile)
    ertac_lib.export_table_to_csv('calc_24hour_hierarchy', out_prefix, 'calc_24hour_hierarchy.csv', conn, None, logfile)
    ertac_lib.export_table_to_csv('calc_generation_proxy', out_prefix, 'calc_generation_proxy.csv', conn,
                                  ertac_tables.generation_proxy_columns, logfile)
    ertac_lib.export_table_to_csv('calc_generation_parms', out_prefix, 'calc_generation_parms_v2.csv', conn,
                                  ertac_tables.generation_parms_columns, logfile)
    ertac_lib.export_table_to_csv('calc_growth_rates', out_prefix, 'calc_growth_rates.csv', conn,
                                  ertac_tables.growth_rate_columns, logfile)
    ertac_lib.export_table_to_csv('calc_input_variables', out_prefix, 'calc_input_variables_v2.csv', conn,
                                  ertac_tables.input_variable_columns, logfile)
    ertac_lib.export_table_to_csv('calc_demand_transfers', out_prefix, 'calc_demand_transfers.csv', conn,
                                  ertac_tables.demand_transfer_columns, logfile)
    # 9/21/2015 Export transfer summary as well as detailed demand transfers.
    ertac_lib.export_table_to_csv('calc_demand_transfer_summary', out_prefix, 'calc_demand_transfer_summary.csv', conn,
                                  ertac_tables.demand_transfer_summary_columns, logfile)
    ertac_lib.export_table_to_csv('calc_control_emissions', out_prefix, 'calc_control_emissions.csv', conn,
                                  ertac_tables.control_emission_columns, logfile)
    ertac_lib.export_table_to_csv('calc_state_total_listing', out_prefix, 'calc_state_total_listing.csv', conn,
                                  ertac_tables.state_total_columns, logfile)
    ertac_lib.export_table_to_csv('calc_group_total_listing', out_prefix, 'calc_group_total_listing.csv', conn,
                                  ertac_tables.group_total_columns, logfile)


if __name__ == '__main__':
    sys.exit(main())
