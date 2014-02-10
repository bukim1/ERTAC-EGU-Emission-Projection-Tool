#!/usr/bin/python

# ertac_projection.py

"""ERTAC EGU projection algorithm"""

# Check to see if all necessary library modules can be loaded.  If not, we're
# running an unsupported version of Python, or there is no SQLite3 module
# available, or the ERTAC EGU code isn't all present in the code directory.

VERSION = "1.01"

import sys

try:
    import getopt, logging, os, time
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
    import ertac_lib, ertac_tables, ertac_reports
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
# ~/ertac_code, then change into the data directory and run the projection
# program by the following two commands:
#
# cd ~/va_data
# ../ertac_code/ertac_projection.py



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
    # Main projection program begins here.
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
    input_prefix = None
    output_prefix = None

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
            filename='ertac_projection_debug_log.txt',
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
        logfilename = output_prefix + 'ertac_egu_projection_log.txt'
    else:
        logfilename = 'ertac_egu_projection_log.txt'

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
    print >> logfile, "ERTAC EGU version: " + VERSION
    print >> logfile, "Running under python version: " + sys.version
    print >> logfile, "Using sqlite3 module version: " + sqlite3.version
    print >> logfile, "Linked against sqlite3 database library version: " + sqlite3.sqlite_version
    print >> logfile, "Model code versions:"
    for file_name in [os.path.basename(sys.argv[0]), 'ertac_lib.py', 'ertac_tables.py', 'ertac_reports.py',
                      'create_preprocessor_output_tables.sql', 'create_projection_output_tables.sql']:
        print >> logfile, "  " + file_name + ": " + time.ctime(os.path.getmtime(os.path.join(sys.path[0], file_name)))


    # Create and populate the working database.
    try:
        dbconn = sqlite3.connect('')
        dbconn.text_factory = str
    except:
        print >> sys.stderr, "Error while opening database.  Program will terminate."
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
    print >> logfile
    print >> logfile, "Assigning generation and evaluating spinning reserve."
    for (region,) in dbconn.execute("""SELECT DISTINCT ertac_region
    FROM calc_generation_parms
    ORDER BY ertac_region""").fetchall():
        assign_generation_all_fuels(dbconn, base_year, future_year, region, logfile)
        evaluate_spinning_reserve(dbconn, region, logfile)

    # Summarize unit level generation and heat input.
    logging.info("Summarizing unit level generation and heat input.")
    print >> logfile
    print >> logfile, "Summarizing unit level generation and heat input."
    summarize_unit_activity(dbconn, logfile)

    # Calculate future emissions.
    logging.info("Calculating future emissions.")
    print >> logfile
    print >> logfile, "Calculating future emissions."
    calculate_future_emissions(dbconn, base_year, ozone_start_base, ozone_end_base, ozone_start_future, ozone_end_future, logfile)

    # Summarize future emissions.
    logging.info("Summarizing future emissions.")
    print >> logfile
    print >> logfile, "Summarizing future emissions."
    summarize_future_emissions(dbconn, ozone_start_hour, ozone_end_hour, logfile)

    # Summarize future capacity.
    logging.info("Summarizing future capacity.")
    print >> logfile
    print >> logfile, "Summarizing future capacity."
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
    print >> logfile
    print >> logfile, "Program ended at " + time.asctime()

    # End of main routine



def load_intermediate_data(conn, in_prefix, logfile):
    """Load intermediate ERTAC EGU data from preprocessor for projection.

    Keyword arguments:
    conn -- a valid database connection where the data will be stored
    in_prefix -- optional prefix added to each input file name
    logfile -- file where logging messages will be written

    """
    ertac_lib.load_csv_into_table(None, os.path.join(os.path.relpath(sys.path[0]), 'states.csv'), 'states', conn, ertac_tables.states_columns, logfile)
    # This section will reject any input rows that are missing required fields,
    # have unreadable data, or violate key constraints, because it is impossible
    # to store that data in the database tables.
    ertac_lib.load_csv_into_table(in_prefix, 'calc_hourly_base.csv', 'calc_hourly_base', conn, ertac_tables.calc_hourly_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_updated_uaf.csv', 'calc_updated_uaf', conn, ertac_tables.uaf_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_unit_hierarchy.csv', 'calc_unit_hierarchy', conn, ertac_tables.unit_hierarchy_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_generation_proxy.csv', 'calc_generation_proxy', conn, ertac_tables.generation_proxy_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_generation_parms.csv', 'calc_generation_parms', conn, ertac_tables.generation_parms_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_input_variables.csv', 'calc_input_variables', conn, ertac_tables.input_variable_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_control_emissions.csv', 'calc_control_emissions', conn, ertac_tables.control_emission_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_state_total_listing.csv', 'calc_state_total_listing', conn, ertac_tables.state_total_columns, logfile)
    ertac_lib.load_csv_into_table(in_prefix, 'calc_group_total_listing.csv', 'calc_group_total_listing', conn, ertac_tables.group_total_columns, logfile)



def assign_generation_all_fuels(conn, base_year, future_year, region, logfile):
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
        print >> logfile, ertac_lib.nice_str((region, fuel))

        # Look up in input variables (for current region, fuel) new unit min/max
        # sizes, demand cushion, 10 facilities for generic units, max UF,
        # deficit_hour (typically 400), optimal load pct, new unit placement
        # pct, new unit EF pct.
        (new_unit_max_size, new_unit_min_size, demand_cushion, facility_1, facility_2, facility_3,
        facility_4, facility_5, facility_6, facility_7, facility_8, facility_9, facility_10, max_uf,
        deficit_review_hour, optimal_load_pct, new_unit_placement_pct, new_unit_ef_pct) = conn.execute("""SELECT new_unit_max_size,
        new_unit_min_size, demand_cushion, facility_1, facility_2, facility_3, facility_4, facility_5, facility_6,
        facility_7, facility_8, facility_9, facility_10, maximum_annual_ertac_uf, capacity_demand_deficit_review,
        unit_optimal_load_threshold_determinant, new_unit_hierarchy_placement_percentile, new_unit_emission_factor_percentile
        FROM calc_input_variables
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()

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
            print >> logfile, "Warning: no available facilities for placement of new generic units for region/fuel:" \
                + ertac_lib.nice_str((region, fuel))

        # Run the generation assignment algorithm until we don't need to add any
        # more new generic units.
        need_more_units = True
        while need_more_units:
            capacity_needed = project_hourly(conn, region, fuel, deficit_review_hour, max_uf, base_year, future_year, logfile)
            # 3.5 Y
            if capacity_needed > 0.0:
                # 3.5Y.2, 3.6: Early exit kicked back here from 3.5Y.1, so need
                # to add units and restart.
                new_unit_count = add_generic_units(conn, region, fuel, capacity_needed,
                    new_unit_max_size, new_unit_min_size, facility_index, facility_list,
                    max_uf, new_unit_placement_pct, base_year, future_year, logfile)
                facility_index += new_unit_count
                facility_index %= len(facility_list)
            else:
                # 9Y: Reached end of hours without needing more generic units,
                # so can proceed to handling excess generation pool next.
                need_more_units = False

        # If generic units were added, log hours where demand exceeded available generation.
        (added_capacity,) = conn.execute("""SELECT SUM(new_unit_size)
        FROM generic_units_created
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
        if added_capacity is not None and added_capacity > 0.0:
            (available_capacity,) = conn.execute("""SELECT SUM(1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate)
            FROM calc_updated_uaf uaf
            JOIN calc_unit_hierarchy hier
            ON uaf.ertac_region = hier.ertac_region
            AND uaf.ertac_fuel_unit_type_bin = hier.ertac_fuel_unit_type_bin
            AND uaf.orispl_code = hier.orispl_code
            AND uaf.unitid = hier.unitid
            WHERE uaf.ertac_region = ?
            AND uaf.ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
            initial_capacity = available_capacity - added_capacity
            for (date, hour, hierarchy_hour, future_projected_generation) in conn.execute("""SELECT op_date,
            op_hour, temporal_allocation_order, future_projected_generation
            FROM calc_generation_parms
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND future_projected_generation > ?
            ORDER BY temporal_allocation_order""", (region, fuel, initial_capacity)).fetchall():
                (calendar_hour,) = conn.execute("""SELECT calendar_hour
                FROM calendar_hours
                WHERE op_date = ?
                AND op_hour = ?""", (date, hour)).fetchone()
                conn.execute("""INSERT INTO demand_generation_deficit VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (region, fuel, calendar_hour, hierarchy_hour, future_projected_generation,
                             initial_capacity, future_projected_generation - initial_capacity, available_capacity))

        # 10: Allocate any excess generation pool up to optimal or maximal levels.
        allocate_excess_generation(conn, region, fuel, max_uf, logfile)



def project_hourly(conn, region, fuel, deficit_review_hour, max_uf, base_year, future_year, logfile):
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

    # 2
    for (date, hour, hierarchy_hour, future_projected_generation) in conn.execute("""SELECT op_date,
    op_hour, temporal_allocation_order, future_projected_generation
    FROM calc_generation_parms
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?
    ORDER BY temporal_allocation_order""", (region, fuel)).fetchall():
        if future_projected_generation is None:
            future_projected_generation = 0.0
        max_future_generation = max(max_future_generation, future_projected_generation)
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
        adjusted_projected_generation = MAX(future_projected_generation - ?, 0.0),
        afygr = CASE WHEN base_actual_generation > base_retired_generation
                     THEN MAX(future_projected_generation - ?, 0.0) / (base_actual_generation - base_retired_generation)
                     ELSE 0.0 END
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND op_date = ?
        AND op_hour = ?""", (total_proxy, total_proxy, total_proxy, region, fuel, date, hour))

        # 3.5:
        if hierarchy_hour == deficit_review_hour:
            # 3.5Y: 20120320 Changed test to be based on maximum hourly
            # generation capacity instead of largest excess generation pool.
            if max_future_generation > max_gen_capacity:
                # 3.5Y.1: Early return to add generic units and restart.
                conn.execute("""DELETE FROM hourly_diagnostic_file
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?""", (region, fuel))
                return max_future_generation - max_gen_capacity
        # 4
        (afygr,) = conn.execute("""SELECT afygr
        FROM calc_generation_parms
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND op_date = ?
        AND op_hour = ?""", (region, fuel, date, hour)).fetchone()

        (calendar_hour,) = conn.execute("""SELECT calendar_hour
        FROM calendar_hours
        WHERE op_date = ?
        AND op_hour = ?""", (date, hour)).fetchone()

        # 4Y2, 4.5
        assign_proxy_gen(conn, region, fuel, date, hour, calendar_hour, hierarchy_hour, max_uf, logfile)
        # 4Y.2.a, 5
        assign_grown_gen(conn, region, fuel, date, hour, calendar_hour, hierarchy_hour, afygr, max_uf, base_year, future_year, logfile)

        # Did any new or existing unit hit a limit at this hour, leaving excess
        # generation?
        (assigned_gen,) = conn.execute("""SELECT SUM(gload)
        FROM hourly_diagnostic_file
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND hierarchy_hour = ?""", (region, fuel, hierarchy_hour)).fetchone()

        if assigned_gen is None:
            assigned_gen = 0.0

        if future_projected_generation > assigned_gen:
            excess_generation_pool = future_projected_generation - assigned_gen
        else:
            excess_generation_pool = 0.0
        # 8:
        conn.execute("""UPDATE calc_generation_parms
        SET excess_generation_pool = ?
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND temporal_allocation_order = ?""",
        (excess_generation_pool, region, fuel, hierarchy_hour))

    # If we didn't take the early exit to add generic units and restart the
    # process, and therefore have finished projecting all the hours for this
    # region/fuel, return 0 to avoid triggering new unit creation after the
    # deficit review hour.
    return 0.0



def assign_proxy_gen(conn, region, fuel, date, hour, calendar_hour, hierarchy_hour, max_uf, logfile):
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
    logfile -- file where logging messages will be written

    """
    # Assign proxy generation in the hourly_diagnostic_file based on
    # calc_generation_proxy, subject to hourly HI and annual UF limits.
    
    #jmj 10/22/2013 commenting out the original sql draw to get infomration about future gen and total proxy
    #for (state, plant, unit, gload) in conn.execute("""SELECT state, orispl_code, unitid, gload_proxy
    for (state, plant, unit, gload, future_gen, total_proxy) in conn.execute("""SELECT state, orispl_code, unitid, gload_proxy, future_projected_generation, total_proxy_generation
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
            
        #jmj 10/22/2013 apply a percent reduction to the gross load if the proxy generation is higher than the future generation needed
        if total_proxy > 0 and future_gen < total_proxy:
            gload = gload * future_gen/total_proxy

        if hierarchy_hour > 1:
            # Get previous hour's running totals.
            (cumulative_hi, cumulative_gen) = conn.execute("""SELECT cumulative_hi, cumulative_gen
            FROM hourly_diagnostic_file
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (region, fuel, plant, unit, hierarchy_hour - 1)).fetchone()
        else:
            cumulative_hi = cumulative_gen = 0.0

        (unit_max_hi, unit_max_uf, unit_max_gload, unit_heat_rate) = conn.execute("""SELECT max_ertac_hi_hourly_summer,
        COALESCE(unit_annual_capacity_limit, max_annual_ertac_uf),
        max_by_hourly_gload, ertac_heat_rate
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

        if unit_max_uf is None:
            unit_max_uf = max_uf

        if unit_heat_rate is not None and gload is not None:
            heat_input = unit_heat_rate * gload / 1000.0
        else:
            heat_input = 0.0
        # 6, 7
        if unit_max_hi is not None and heat_input > unit_max_hi:
            hourly_hi_limit = 'Y'
            heat_input = unit_max_hi
            gload = heat_input * 1000.0 / unit_heat_rate
        else:
            hourly_hi_limit = 'N'

        if unit_max_hi is not None and unit_max_uf is not None:
            unit_annual_hi_limit_value = 8760 * unit_max_hi * unit_max_uf
            if cumulative_hi + heat_input > unit_annual_hi_limit_value:
                annual_hi_limit = 'Y'
                heat_input = unit_annual_hi_limit_value - cumulative_hi
                gload = heat_input * 1000.0 / unit_heat_rate
            else:
                annual_hi_limit = 'N'
        else:
            annual_hi_limit = 'N'

        conn.execute("""INSERT INTO hourly_diagnostic_file
        (ertac_region, ertac_fuel_unit_type_bin, state, orispl_code, unitid,
        calendar_hour, hierarchy_hour, hourly_hi_limit, annual_hi_limit,
        cumulative_hi, cumulative_gen, gload, heat_input)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (region, fuel, state, plant, unit,
        calendar_hour, hierarchy_hour, hourly_hi_limit, annual_hi_limit,
        cumulative_hi + heat_input, cumulative_gen + gload, gload, heat_input))



def assign_grown_gen(conn, region, fuel, date, hour, calendar_hour, hierarchy_hour, afygr, max_uf, base_year, future_year, logfile):
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
    for (state, plant, unit, gload) in conn.execute("""SELECT hourly.state, hourly.orispl_code, hourly.unitid,
    CASE WHEN COALESCE(uaf.capacity_limited_unit_flag, 'N') = 'Y'
    OR REPLACE(hourly.op_date, ?, ?) >= uaf.offline_start_date
    THEN 0.0
    ELSE hourly.gload END
    FROM calc_hourly_base hourly
    JOIN calc_updated_uaf uaf
    ON hourly.orispl_code = uaf.orispl_code
    AND hourly.unitid = uaf.unitid
    AND hourly.ertac_fuel_unit_type_bin = uaf.ertac_fuel_unit_type_bin
    WHERE hourly.ertac_region = ?
    AND hourly.ertac_fuel_unit_type_bin = ?
    AND hourly.op_date = ?
    AND hourly.op_hour = ?""", (base_year, future_year, region, fuel, date, hour)).fetchall():

        if gload is None:
            gload = 0.0
        else:
            gload = gload * afygr

        if hierarchy_hour > 1:
            # Get previous hour's running totals.
            (cumulative_hi, cumulative_gen) = conn.execute("""SELECT cumulative_hi, cumulative_gen
            FROM hourly_diagnostic_file
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (region, fuel, plant, unit, hierarchy_hour - 1)).fetchone()
        else:
            cumulative_hi = cumulative_gen = 0.0

        (unit_max_hi, unit_max_uf, unit_max_gload, unit_heat_rate) = conn.execute("""SELECT max_ertac_hi_hourly_summer,
        COALESCE(unit_annual_capacity_limit, max_annual_ertac_uf),
        max_by_hourly_gload, ertac_heat_rate
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

        if unit_max_uf is None:
            unit_max_uf = max_uf

        if unit_heat_rate is not None and gload is not None:
            heat_input = unit_heat_rate * gload / 1000.0
        else:
            heat_input = 0.0
        # 6, 7
        if unit_max_hi is not None and heat_input > unit_max_hi:
            hourly_hi_limit = 'Y'
            heat_input = unit_max_hi
            gload = heat_input * 1000.0 / unit_heat_rate
        else:
            hourly_hi_limit = 'N'

        if unit_max_hi is not None and unit_max_uf is not None:
            unit_annual_hi_limit_value = 8760 * unit_max_hi * unit_max_uf
            if cumulative_hi + heat_input > unit_annual_hi_limit_value:
                annual_hi_limit = 'Y'
                heat_input = unit_annual_hi_limit_value - cumulative_hi
                gload = heat_input * 1000.0 / unit_heat_rate
            else:
                annual_hi_limit = 'N'
        else:
            annual_hi_limit = 'N'

        conn.execute("""INSERT INTO hourly_diagnostic_file
        (ertac_region, ertac_fuel_unit_type_bin, state, orispl_code, unitid,
        calendar_hour, hierarchy_hour, hourly_hi_limit, annual_hi_limit,
        cumulative_hi, cumulative_gen, gload, heat_input)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (region, fuel, state, plant, unit,
        calendar_hour, hierarchy_hour, hourly_hi_limit, annual_hi_limit,
        cumulative_hi + heat_input, cumulative_gen + gload, gload, heat_input))



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
        (state,) = conn.execute("""SELECT state FROM calc_updated_uaf WHERE ertac_region = ? AND orispl_code = ?""", (region, plant)).fetchone()
        conn.execute("""UPDATE generic_unit_counts SET units_created = units_created + 1 WHERE state = ?""", (state,))
        (state_code, units_created) = conn.execute("""SELECT state_code, units_created FROM generic_unit_counts WHERE state = ?""", (state,)).fetchone()
        unit = "G" + state_code + str(units_created).zfill(3)
        logging.info("Creating new generic unit: " + ertac_lib.nice_str((region, fuel, plant, unit)))
        print >> logfile, "Creating new generic unit: " + ertac_lib.nice_str((region, fuel, plant, unit))
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
        unit_columns = [unit, 'NEW', online_start_date, offline_start_date, fuel, max_ertac_hi_hourly_summer, unit_size, max_uf, ertac_heat_rate]
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
            #jmj do an else here where you also find the highest New Unit allcoation and have that be the anchor rank
            #no need to check if max rank is none since
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
        ertac_lib.compute_proxy_generation(conn, region, fuel, plant, unit, state, name, base_year, future_year, logfile)

    return unit_count



def allocate_excess_generation(conn, region, fuel, max_uf, logfile):
    """10: Allocate any excess generation in two passes, first raising outputs to optimal threshold, then to maximum.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    region -- the current region being processed
    fuel -- the current fuel bin being processed
    max_uf -- default maximum utilization fraction for any unit in this region/fuel
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
        for (plant, unit, unit_order, calendar_hour, hourly_hi_limit, annual_hi_limit, initial_gload, initial_heat_input) in conn.execute("""SELECT hier.orispl_code,
        hier.unitid, hier.unit_allocation_order, hourly.calendar_hour, hourly.hourly_hi_limit, hourly.annual_hi_limit, hourly.gload, hourly.heat_input
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
                (cumulative_hi, cumulative_gen) = conn.execute("""SELECT cumulative_hi, cumulative_gen
                FROM hourly_diagnostic_file
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?
                AND hierarchy_hour = ?""", (region, fuel, plant, unit, hierarchy_hour - 1)).fetchone()
            else:
                cumulative_hi = cumulative_gen = 0.0

            # Get unit's status at final hour.
            (last_hour_annual_hi_limit, last_hour_cumulative_hi, last_hour_cumulative_gen) = conn.execute("""SELECT annual_hi_limit, cumulative_hi, cumulative_gen
            FROM hourly_diagnostic_file
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (region, fuel, plant, unit, last_hour)).fetchone()

            (unit_max_hi, unit_max_uf, unit_max_gload, unit_heat_rate, unit_optimal_load) = conn.execute("""SELECT max_ertac_hi_hourly_summer,
            COALESCE(unit_annual_capacity_limit, max_annual_ertac_uf),
            max_by_hourly_gload, ertac_heat_rate, unit_max_optimal_load_threshold
            FROM calc_updated_uaf
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

            if unit_max_uf is None:
                unit_max_uf = max_uf

            if unit_heat_rate is not None and unit_optimal_load is not None:
                unit_opt_hi = unit_heat_rate * unit_optimal_load / 1000.0
                if initial_heat_input < unit_opt_hi and excess_generation > 0.0 and last_hour_annual_hi_limit == 'N' and future_date >= online and future_date < offline:
                    gload = initial_gload + excess_generation
                    excess_generation = 0.0
                    heat_input = unit_heat_rate * gload / 1000.0
                    # Hourly optimum?
                    if heat_input > unit_opt_hi:
                        excess_generation += (heat_input - unit_opt_hi) * 1000.0 / unit_heat_rate
                        heat_input = unit_opt_hi
                        gload = heat_input * 1000.0 / unit_heat_rate
                    # Annual limit?
                    if unit_max_hi is not None and unit_max_uf is not None:
                        unit_annual_hi_limit_value = 8760 * unit_max_hi * unit_max_uf
                        headroom = unit_annual_hi_limit_value - last_hour_cumulative_hi
                        if heat_input > initial_heat_input + headroom:
                            # We used all available capacity through the end of the year.
                            excess_generation += (heat_input - (initial_heat_input + headroom)) * 1000.0 / unit_heat_rate
                            heat_input = initial_heat_input + headroom
                            gload = heat_input * 1000.0 / unit_heat_rate
                            last_hour_annual_hi_limit == 'Y'

            # Change values at current hour.
            conn.execute("""UPDATE hourly_diagnostic_file
            SET hourly_hi_limit = ?, annual_hi_limit = ?, cumulative_hi = ?, cumulative_gen = ?, gload = ?, heat_input = ?
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (hourly_hi_limit, annual_hi_limit, cumulative_hi + heat_input, cumulative_gen + gload, gload, heat_input, region, fuel, plant, unit, hierarchy_hour))

            # Change cumulative HI and annual limit flag for last hour.
            conn.execute("""UPDATE hourly_diagnostic_file
            SET annual_hi_limit = ?, cumulative_hi = ?, cumulative_gen = ?
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (last_hour_annual_hi_limit, last_hour_cumulative_hi + heat_input - initial_heat_input, last_hour_cumulative_gen + gload - initial_gload, region, fuel, plant, unit, last_hour))

        # 15: Second pass, raise to maximum if necessary.
        for (plant, unit, unit_order, calendar_hour, hourly_hi_limit, annual_hi_limit, initial_gload, initial_heat_input) in conn.execute("""SELECT hier.orispl_code,
        hier.unitid, hier.unit_allocation_order, hourly.calendar_hour, hourly.hourly_hi_limit, hourly.annual_hi_limit, hourly.gload, hourly.heat_input
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
                (cumulative_hi, cumulative_gen) = conn.execute("""SELECT cumulative_hi, cumulative_gen
                FROM hourly_diagnostic_file
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?
                AND hierarchy_hour = ?""", (region, fuel, plant, unit, hierarchy_hour - 1)).fetchone()
            else:
                cumulative_hi = cumulative_gen = 0.0

            # Get unit's status at final hour.
            (last_hour_annual_hi_limit, last_hour_cumulative_hi, last_hour_cumulative_gen) = conn.execute("""SELECT annual_hi_limit, cumulative_hi, cumulative_gen
            FROM hourly_diagnostic_file
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (region, fuel, plant, unit, last_hour)).fetchone()

            (unit_max_hi, unit_max_uf, unit_max_gload, unit_heat_rate) = conn.execute("""SELECT max_ertac_hi_hourly_summer,
            COALESCE(unit_annual_capacity_limit, max_annual_ertac_uf),
            max_by_hourly_gload, ertac_heat_rate
            FROM calc_updated_uaf
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

            if unit_max_uf is None:
                unit_max_uf = max_uf

            if unit_heat_rate is not None and unit_max_hi is not None:
                if initial_heat_input < unit_max_hi and excess_generation > 0.0 and last_hour_annual_hi_limit == 'N' and future_date >= online and future_date < offline:
                    gload = initial_gload + excess_generation
                    excess_generation = 0.0
                    heat_input = unit_heat_rate * gload / 1000.0
                    # Hourly limit?
                    if heat_input > unit_max_hi:
                        excess_generation += (heat_input - unit_max_hi) * 1000.0 / unit_heat_rate
                        heat_input = unit_max_hi
                        gload = heat_input * 1000.0 / unit_heat_rate
                    # Annual limit?
                    if unit_max_hi is not None and unit_max_uf is not None:
                        unit_annual_hi_limit_value = 8760 * unit_max_hi * unit_max_uf
                        headroom = unit_annual_hi_limit_value - last_hour_cumulative_hi
                        if heat_input > initial_heat_input + headroom:
                            # We used all available capacity through the end of the year.
                            excess_generation += (heat_input - (initial_heat_input + headroom)) * 1000.0 / unit_heat_rate
                            heat_input = initial_heat_input + headroom
                            gload = heat_input * 1000.0 / unit_heat_rate
                            last_hour_annual_hi_limit == 'Y'

                # Might have raised to hourly limit, but could have backed down for annual limit.
                if heat_input >= unit_max_hi:
                    hourly_hi_limit = 'Y'

            # Change values at current hour.
            conn.execute("""UPDATE hourly_diagnostic_file
            SET hourly_hi_limit = ?, annual_hi_limit = ?, cumulative_hi = ?, cumulative_gen = ?, gload = ?, heat_input = ?
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (hourly_hi_limit, annual_hi_limit, cumulative_hi + heat_input, cumulative_gen + gload, gload, heat_input, region, fuel, plant, unit, hierarchy_hour))

            # Change cumulative HI and annual limit flag for last hour.
            conn.execute("""UPDATE hourly_diagnostic_file
            SET annual_hi_limit = ?, cumulative_hi = ?, cumulative_gen = ?
            WHERE ertac_region = ?
            AND ertac_fuel_unit_type_bin = ?
            AND orispl_code = ?
            AND unitid = ?
            AND hierarchy_hour = ?""", (last_hour_annual_hi_limit, last_hour_cumulative_hi + heat_input - initial_heat_input, last_hour_cumulative_gen + gload - initial_gload, region, fuel, plant, unit, last_hour))

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

    for (date, hour, total_load) in conn.execute("""SELECT op_date, op_hour, SUM(future_projected_generation) AS total_load
    FROM calc_generation_parms
    WHERE ertac_region = ?
    GROUP BY op_date, op_hour
    ORDER BY total_load DESC, op_date, op_hour""", (region,)).fetchall():

        if total_load is None:
            total_load = 0.0

        (calendar_hour,) = conn.execute("""SELECT calendar_hour
        FROM calendar_hours
        WHERE op_date = ?
        AND op_hour = ?""", (date, hour)).fetchone()

        # 20: What is the max capacity of any unit in this region operating at
        # this particular calendar hour?
        (max_unit_capacity,) = conn.execute("""SELECT MAX(1000.0 * uaf.max_ertac_hi_hourly_summer / uaf.ertac_heat_rate)
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
        amount_available = total_capacity - total_load
        # 23
        if reserve_needed > amount_available:
            pass_fail = 'F'
            deficit = reserve_needed - amount_available
        else:
            pass_fail = 'P'
            deficit = None
        # 23Y1
        conn.execute("""INSERT INTO reserve_capacity_needed
        (ertac_region, calendar_hour, hierarchy_hour, pass_fail,
        reserve_needed, amount_available, deficit)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (region, calendar_hour, hierarchy_hour, pass_fail,
        reserve_needed, amount_available, deficit))

        hierarchy_hour += 1



def summarize_unit_activity(conn, logfile):
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
        (fac_name, max_hi, heat_rate, by_hours) = conn.execute("""SELECT facility_name,
        max_ertac_hi_hourly_summer, ertac_heat_rate, operating_hours_by
        FROM calc_updated_uaf
        WHERE orispl_code = ?
        AND unitid = ?
        AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel)).fetchone()
        if heat_rate is not None and heat_rate > 0.0:
            if max_hi is None:
                x = 1
            else:
                gen_cap = 1000.0 * max_hi / heat_rate
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
        capacity = ?,
        num_hrs_fy_max = ?,
        by_gen = ?,
        by_hi = ?,
        by_hours = ?
        WHERE rowid = ?""", (fac_name, max_hi, heat_rate, gen_cap, hours_at_max, by_gen, by_hi, by_hours, rowid))
    conn.execute("""UPDATE unit_level_activity
    SET uf = fy_hi / (8760.0 * max_ertac_hi_hourly_summer)""")



def calculate_future_emissions(conn, base_year, ozone_start_base, ozone_end_base, ozone_start_future, ozone_end_future, logfile):
    """26: Calculate future hourly emissions.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    base_year -- the base year where generation is projected from
    ozone_start_base, ozone_end_base -- the ozone season dates in the base year
    ozone_start_future, ozone_end_future -- the ozone season dates in the future year
    logfile -- file where logging messages will be written

    """

    day_after_base = ertac_lib.first_day_after(base_year)
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
 
        print region + " " + fuel    
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

        # Calculate future emissions for new and existing units in current region/fuel.
        for (plant, unit, by_type) in conn.execute("""SELECT orispl_code, unitid, camd_by_hourly_data_type
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchall():

            if by_type.upper() != 'NEW':
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

                if by_type.upper() == 'NEW':
                    so2_rate = new_unit_so2_rate
                    nox_rate = new_unit_nox_rate
                else:
                    so2_rate = by_so2_rate
                    # 20120423 Simplfied handling of OS vs non-OS emission rates for NOx.
                    if future_date >= ozone_start_future and future_date <= ozone_end_future:
                        nox_rate = by_os_nox_rate
                    else:
                        nox_rate = by_nonos_nox_rate

                so2_result = conn.execute("""SELECT emission_rate, control_efficiency
                FROM calc_control_emissions
                WHERE orispl_code = ?
                AND unitid = ?
                AND pollutant_code = ?
                AND factor_start_date BETWEEN ? AND ?
                AND factor_end_date >= ?""", (plant, unit, 'SO2', day_after_base, future_date, future_date)).fetchone()

                nox_result = conn.execute("""SELECT emission_rate, control_efficiency
                FROM calc_control_emissions
                WHERE orispl_code = ?
                AND unitid = ?
                AND pollutant_code = ?
                AND factor_start_date BETWEEN ? AND ?
                AND factor_end_date >= ?""", (plant, unit, 'NOx', day_after_base, future_date, future_date)).fetchone()

                if so2_result is not None:
                    (future_so2_rate, future_so2_control) = so2_result
                    if future_so2_rate is not None:
                        so2_rate = future_so2_rate
                    elif future_so2_control is not None and so2_rate is not None:
                        so2_rate = so2_rate * (1.0 - future_so2_control / 100.0)

                if nox_result is not None:
                    (future_nox_rate, future_nox_control) = nox_result
                    if future_nox_rate is not None:
                        nox_rate = future_nox_rate
                    elif future_nox_control is not None and nox_rate is not None:
                        nox_rate = nox_rate * (1.0 - future_nox_control / 100.0)

                conn.execute("""UPDATE hourly_diagnostic_file
                SET so2_mass = heat_input * ?,
                so2_rate = ?,
                nox_mass = heat_input * ?,
                nox_rate = ?
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND orispl_code = ?
                AND unitid = ?
                AND calendar_hour BETWEEN ? AND ?
                AND heat_input > 0""", (so2_rate, so2_rate, nox_rate, nox_rate,
                                        region, fuel, plant, unit, first_calendar_hour, last_calendar_hour))



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
    conn.executescript("""INSERT INTO capacity_and_fy_demand
    (ertac_region, ertac_fuel_unit_type_bin, by_gen, by_hi)
    SELECT ertac_region, ertac_fuel_unit_type_bin, SUM(gload), SUM(heat_input)
    FROM calc_hourly_base
    GROUP BY ertac_region, ertac_fuel_unit_type_bin;

    UPDATE capacity_and_fy_demand
    SET fy_gen = (SELECT SUM(gload) FROM hourly_diagnostic_file hourly
    WHERE hourly.ertac_region = capacity_and_fy_demand.ertac_region
    AND hourly.ertac_fuel_unit_type_bin = capacity_and_fy_demand.ertac_fuel_unit_type_bin);

    UPDATE capacity_and_fy_demand
    SET fy_hi = (SELECT SUM(heat_input) FROM hourly_diagnostic_file hourly
    WHERE hourly.ertac_region = capacity_and_fy_demand.ertac_region
    AND hourly.ertac_fuel_unit_type_bin = capacity_and_fy_demand.ertac_fuel_unit_type_bin);

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
    SET reserve_met = 'N'
    WHERE max_deficit > 0.0;""")



def write_final_data(conn, out_prefix, logfile):
    """Write out projected ERTAC EGU data reports.

    Keyword arguments:
    conn -- a valid database connection where the data is stored
    out_prefix -- optional prefix added to each output file name
    logfile -- file where logging messages will be written

    """
    # Final output data is exported as CSV files for reporting and use with
    # other programs.
    ertac_lib.export_table_to_csv('calc_generation_parms', out_prefix, 'calc_generation_parms.csv', conn, ertac_tables.generation_parms_columns, logfile)
    ertac_lib.export_table_to_csv('calc_generation_proxy', out_prefix, 'calc_generation_proxy.csv', conn, ertac_tables.generation_proxy_columns, logfile)
    ertac_lib.export_table_to_csv('calc_unit_hierarchy', out_prefix, 'calc_unit_hierarchy.csv', conn, ertac_tables.unit_hierarchy_columns, logfile)
    ertac_lib.export_table_to_csv('calc_updated_uaf', out_prefix, 'calc_updated_uaf.csv', conn, ertac_tables.uaf_columns, logfile)
    ertac_lib.export_table_to_csv('demand_generation_deficit', out_prefix, 'demand_generation_deficit.csv', conn, ertac_reports.demand_generation_deficit, logfile)
    ertac_lib.export_table_to_csv('generic_units_created', out_prefix, 'generic_units_created.csv', conn, ertac_reports.generic_units_created, logfile)
    ertac_lib.export_table_to_csv('reserve_capacity_needed', out_prefix, 'reserve_capacity_needed.csv', conn, ertac_reports.reserve_capacity_needed, logfile)
    ertac_lib.export_table_to_csv('unit_level_activity', out_prefix, 'unit_level_activity.csv', conn, ertac_reports.unit_level_activity, logfile)
    ertac_lib.export_table_to_csv('cap_analysis', out_prefix, 'cap_analysis.csv', conn, ertac_reports.cap_analysis, logfile)
    ertac_lib.export_table_to_csv('unit_generic_controls', out_prefix, 'unit_generic_controls.csv', conn, ertac_reports.unit_generic_controls, logfile)
    ertac_lib.export_table_to_csv('capacity_and_fy_demand', out_prefix, 'capacity_and_fy_demand.csv', conn, ertac_reports.capacity_and_fy_demand, logfile)
    ertac_lib.export_table_to_csv('capacity_and_fy_reserve', out_prefix, 'capacity_and_fy_reserve.csv', conn, ertac_reports.capacity_and_fy_reserve, logfile)
    ertac_lib.export_table_to_csv('state_caps', out_prefix, 'state_caps.csv', conn, ertac_reports.state_caps, logfile)
    ertac_lib.export_table_to_csv('group_caps', out_prefix, 'group_caps.csv', conn, ertac_reports.group_caps, logfile)
    ertac_lib.export_table_to_csv('hourly_diagnostic_file', out_prefix, 'hourly_diagnostic_file.csv', conn, ertac_reports.hourly_diagnostic_file, logfile)



if __name__ == '__main__':
    sys.exit(main())
