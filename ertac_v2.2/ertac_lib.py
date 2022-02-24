# ertac_lib.py, not executed directly

"""Utility routines for ERTAC EGU projection"""

VERSION = "2.2"
# Updated to version 2.0b as of 9/22/2015.

import sys, csv, logging, os, re, datetime

# This section was changed, as in the main programs, to try loading built-in or
# add-on SQLite3 module, for older versions of Python.
try:
    import sqlite3
except ImportError:
    try:
        from pysqlite2 import dbapi2 as sqlite3
    except ImportError:
        print >> sys.stderr, "Fatal error: can't import all required modules."
        print >> sys.stderr, "No SQLite3 available with this Python."
        raise



def run_script_file(file_name, connection):
    """Run a SQL script file for a SQLite database.

    Keyword arguments:
    file_name -- name of script file to be executed
    connection -- a valid database connection

    """

    # SQL scripts should be in same directory as Python code.
    path_to_file = os.path.join(sys.path[0], file_name)
    sql_file = open(path_to_file, 'r')
    sql_text = sql_file.read()
    connection.executescript(sql_text)
    connection.commit()



def nice_str(group):
    """Convert a group of items into a nicely-formatted string for printing.

    Keyword arguments:
    group -- the group of items, in a Python list or tuple

    Returns string

    """
    # When printing out a group of items to review questionable input data,
    # there are two obvious easy options:
    # 1. print group
    # 2. for item in group:
    #        print item,
    #    print
    # Option 1 mostly works: it surrounds the group with square brackets or
    # parentheses, it separates the items with commas, and the individual item
    # formats look OK, except for floating-point numbers.  Floats are shown as
    # very long double-precision numbers showing the low-order inaccuracies of
    # converting between decimal and binary numeric formats.
    # Option 2 doesn't surround or separate the group items, leaves one extra
    # blank space between items, removes the quotes around strings (making
    # multi-word strings harder to recognize), but does give better-looking
    # floating-point numbers.
    # Both options include the word None where there was no data at all, such as
    # a missing input value or a SQL NULL in a table.  This could be confusing
    # when comparing the printed output to the input files.
    # To get a nicer string representation of the group for printing, we'll use
    # str() to convert most items, which is what the print statement does in
    # option 2, but we'll suppress the use of None, and use repr() for strings
    # so they're still enclosed in quotes in the final string.
    group_str = ['' if item is None else repr(item) if type(item) is str else str(item) for item in group]
    return '(' + ', '.join(group_str) + ')'



def load_csv_into_table(prefix, basic_csv_file, table_name, connection, column_types, logfile, delete_old_rows=True):
    """Load contents of a CSV file into a database table.

    Keyword arguments:
    prefix -- optional prefix added to each input file name
    basic_csv_file -- basic name of CSV file to be loaded, without prefix
    table_name -- name of table receiving data
    connection -- a valid database connection
    column_types -- a group of tuples with each column's possible header text,
                    data type, required status, and optional data ranges or
                    groups of valid values
    logfile -- file where logging messages will be written
    delete_old_rows -- boolean flag for delete/append existing data

    """

    if prefix is None:
        prefix = ""
    csv_file = prefix + basic_csv_file

    # Need to know number of columns in table to create parameter list for
    # INSERT ... VALUES() clause.
    column_count = count_columns(table_name, connection)
    if column_count != len(column_types):
        print >> sys.stderr, "Programmer error: Number of columns in table " + table_name
        print >> sys.stderr, "created by SQL script does not match ertac_tables.py list of column types."
        sys.exit(1)
    parameter_list = '(?' + ', ?' * (column_count - 1) + ')'

    if delete_old_rows:
        connection.execute("DELETE FROM " + table_name)

    # If CSV file has one or more header rows, try to recognize and skip when
    # loading, based on non-numeric data and header text matches.  This replaces
    # earlier use of csv.Sniffer which was not reliable with many of the files
    # of example test data.
    # If file contains more columns than defined in table, excess trailing data
    # will be discarded with no warning here, by slicing input row to proper
    # size.
    # If file has too few columns, program will warn about short lines.
    # If CSV file has spaces after commas before numbers (with or without
    # quotes) SQLite will store as text rather than number, affecting sort
    # order, etc.  Need to strip blanks to prevent this.
    # Updated file opening mode from rb to rU to better handle Linux/Windows
    # cross-platform file exchange.

    logging.info("  " + csv_file)
    print >> logfile
    # Catch IOError on file opening, in case input file is missing or unreadable.
    try:
        cf = open(csv_file, 'rU')
    except IOError:
        print >> logfile, "File: " + csv_file + " -- Could not be read."
        #jmj allows checks to see if loading fails or not 150413
        return False
    print >> logfile, "Loading input data from file: " + csv_file
    cr = csv.reader(cf)
    row_count = 0
    for row in cr:
        if len(row) < column_count:
            print >> logfile, "File: " + csv_file + " line:", cr.line_num, "-- Can't use short row:", row
            continue

        # Missing input data from CSV file looks like an empty string; convert
        # to Python None, then to SQL NULL.  Otherwise, Python and SQLite will
        # store an empty string anywhere we don't actually have data, which will
        # result in incorrect counts, percentiles, etc.
        # May have to handle sqlite3.IntegrityError due to NULL data.
        new_row = [col.strip() if col.strip()!='' else None for col in row]

        # Convert numeric data to actual numeric data types.
        # CAMD dates look like mm-dd-yyyy, but need to allow slashes too.
        missing_required = []
        non_number = []
        non_date = []
        for i, (col, (header_text, col_type, required, allowed)) in enumerate(zip(new_row, column_types)):
            if col is None and required:
                missing_required.append(header_text)
            if col is not None and col_type == 'int':
                try:
                    new_row[i] = int(col.replace(',', ''))
                except ValueError:
                    non_number.append(header_text + ': ' + col)
            if col is not None and col_type == 'float':
                try:
                    new_row[i] = float(col.replace(',', ''))
                except ValueError:
                    non_number.append(header_text + ': ' + col)
            if col is not None and col_type in ('date', 'date-first', 'date-last', 'year-only', 'd-mmm'):
                # Convert mm-dd-yyyy CAMD dates and m/d/yyyy non-CAMD dates into
                # consistent ISO 8601 yyyy-mm-dd format.  For some date columns,
                # want to allow a complete date or just a plain year to be
                # specified, and treat plain year as the first or last date of
                # that year.  For some columns we actually want just a year
                # alone, without month or day.  For ozone season start/end, we
                # want day and valid month abbreviation without year.
                date_match = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', col)
                iso8601_match = re.match(r'^\d{4}-\d{2}-\d{2}$', col)
                year_match = re.match(r'^\d{4}$', col)
                d_mmm_match = re.match(r'^(\d{1,2})-([A-Za-z]{3})$', col)
                if date_match and col_type in ('date', 'date-first', 'date-last'):
                    new_row[i] = date_match.group(3) + '-' + date_match.group(1).zfill(2) + '-' + date_match.group(2).zfill(2)
                elif iso8601_match and col_type in ('date', 'date-first', 'date-last'):
                    new_row[i] = col
                elif year_match and col_type == 'date-first':
                    new_row[i] = col + '-01-01'
                elif year_match and col_type == 'date-last':
                    new_row[i] = col + '-12-31'
                elif year_match and col_type == 'year-only':
                    new_row[i] = col
                elif d_mmm_match and col_type == 'd-mmm' and d_mmm_match.group(2).upper() in month_dict:
                    new_row[i] = col
                else:
                    non_date.append(header_text + ': ' + col)

        if missing_required or non_number or non_date:
            # Not a valid data row, might be header row or might be corrupt.
            # If any column values match heading names, ignore probable header line.
            header_match = 0
            for col, (header_text, col_type, required, allowed) in zip(row, column_types):
                if col.replace(' ', '_').upper() == header_text.replace(' ', '_').upper():
                    header_match += 1
            if header_match:
                print >> logfile, "File: " + csv_file + " line:", cr.line_num, "-- Probable header line not stored in database:", row
            else:
                print >> logfile, "File: " + csv_file + " line:", cr.line_num, "-- Can't use bad input row;",
                if missing_required:
                    print >> logfile, "missing data in one or more required columns:", missing_required, ";",
                if non_number:
                    print >> logfile, "non-numeric data in one or more numeric columns:", non_number, ";",
                if non_date:
                    print >> logfile, "unusable data in one or more date columns:", non_date, ";",
                print >> logfile, "-- Row data:", row
        else:
            # Normal-looking data
            try:
                connection.execute("INSERT INTO " + table_name + " VALUES " + parameter_list, new_row[:column_count])
                row_count += 1
            except sqlite3.IntegrityError as err_msg:
                print >> logfile, "File: " + csv_file + " line:", cr.line_num, "-- Can't use bad input row;", err_msg, "-- Row data:", row
            except sqlite3.ProgrammingError as err_msg:
                print >> logfile, "File: " + csv_file + " line:", cr.line_num, "-- Can't use bad input row;", err_msg, "-- Row data:", row

    print >> logfile, "File: " + csv_file + "; read", cr.line_num, "lines, stored", row_count, "data rows in table: " + table_name
    connection.commit()
    #jmj allows checks to see if loading fails or not 150413
    return True


# 20120406 Added month abbreviations and ozone season validation and conversion
month_dict = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
              'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}



def convert_ozone_date(ozone_date, year):
    """Convert ozone season start/end date from d-mmm format into ISO 8601 yyyy-mm-dd format for a specific year.

    Keyword arguments:
    ozone_date -- the day and month when the ozone season starts or ends
    year -- the base or future year

    Returns string representing date if valid

    """

    # Ozone date expected in d-mmm format, such as 1-APR, 31-OCT, etc.
    d_mmm_match = re.match(r'^(\d{1,2})-([A-Za-z]{3})$', ozone_date)
    if d_mmm_match and d_mmm_match.group(2).upper() in month_dict:
        return year + '-' + month_dict[d_mmm_match.group(2).upper()] + '-' + d_mmm_match.group(1).zfill(2)
    else:
        return None



def count_columns(table_name, connection):
    """Find number of columns in a database table.

    Keyword arguments:
    table_name -- name of table to analyze
    connection -- a valid database connection

    Returns int

    """

    # SQLite data dictionary is very minimal, doesn't provide anything like
    # INFORMATION_SCHEMA.COLUMNS that we could query to determine column count.
    # However, sqlite3.Cursor.description returns a tuple of 7-tuples containing
    # column names from the last query, even if empty, so the size of that is
    # the column count.
    dbcur = connection.execute("SELECT * FROM " + table_name + " WHERE 1 < 0")
    return len(dbcur.description)



def log_and_exit(logfile, error_message):
    """Print fatal error message to logfile and stderr, and exit program.

    Keyword arguments:
    logfile -- file where logging messages will be written
    error_message -- message to be printed

    """

    # Make sure fatal errors are written into the log, and displayed on screen
    # even when in -q (quiet) mode.
    print >> logfile, error_message + "  Program will terminate."
    print >> sys.stderr, error_message + "  Program will terminate."
    sys.exit(1)



def log_warn(logfile, error_message):
    """Print non-fatal error message to logfile, and continue running.

    Keyword arguments:
    logfile -- file where logging messages will be written
    error_message -- message to be printed

    """

    # This routine exists as an alternative to log_and_exit(), so that errors
    # detected during data validation can be treated as more or less severe by
    # calling one or the other error logging function.
    print >> logfile, error_message



# Unspecified online/offline dates will be treated as operating long before and
# after normal dates for model.
online_default = '1900-01-01'
offline_default = '2199-12-31'



def first_day_of(year):
    """Return first day of (string) year as ISO 8601 date string."""
    if re.match(r'^\d{4}$', year):
        return year + '-01-01'



def first_day_after(year):
    """Return first day after (string) year as ISO 8601 date string."""
    if re.match(r'^\d{4}$', year):
        return str(int(year) + 1) + '-01-01'



def is_leap_year(year):
    """Determine if (string) year is a leap year."""
    if re.match(r'^\d{4}$', year):
        iy = int(year)
        if iy % 4 == 0:
            # Divisible by 4, need to check 100-year and 400-year exceptions.
            if iy % 100 == 0:
                if iy % 400 == 0:
                    return True     # 2000 was a leap year
                else:
                    return False    # 2100 will not be
            else:
                return True         # 2004, 2008, 2012, ... are
        else:
            return False            # common year


def hours_in_year(base_year, future_year):
    return (8784 if (is_leap_year(base_year) and is_leap_year(future_year)) else 8760)

def make_calendar_hours(base_year, future_year, connection):
    """Make lookup table between dates/hours and hour numbers.

    Keyword arguments:
    base_year -- the base year where generation is projected from
    future_year -- the year which is being projected
    connection -- a valid database connection

    """
    # Updated for V2 to add columns for grouping by day, month, quarter, and
    # Ozone/non-Ozone season.
    connection.executescript("""CREATE TEMPORARY TABLE calendar_hours
    (op_date TEXT NOT NULL,
    op_hour INTEGER NOT NULL,
    future_date TEXT,
    calendar_hour INTEGER,
    m_d TEXT,
    mon TEXT,
    qtr TEXT,
    o_n TEXT,
    PRIMARY KEY (op_date, op_hour));

    INSERT INTO calendar_hours (op_date, op_hour)
    SELECT DISTINCT op_date, op_hour
    FROM calc_hourly_base
    ORDER BY op_date, op_hour;""")

    connection.execute("""UPDATE calendar_hours
    SET future_date = REPLACE(op_date, ?, ?)""", (base_year, future_year))

    # Current model requires that all regions and fuels have same Ozone season
    # dates, so emissions can be summarized up to OS and non-OS totals.  So, we
    # only have to figure out Ozone season dates once.  Fill in grouping columns
    # for day (mm-dd), month, quarter, and OS/non-OS.
    (base_year, ozone_start_date, ozone_end_date) = connection.execute("""SELECT
    base_year, ozone_start_date, ozone_end_date
    FROM calc_input_variables""").fetchone()
    os_start = convert_ozone_date(ozone_start_date, base_year)
    os_end = convert_ozone_date(ozone_end_date, base_year)

    connection.executescript("""UPDATE calendar_hours
    SET m_d = SUBSTR(op_date, 6, 5),
        mon = SUBSTR(op_date, 6, 2),
        o_n = 'N';

    UPDATE calendar_hours
    SET qtr = CASE WHEN mon IN ('12', '01', '02')
        THEN 'W'
        WHEN mon IN ('03', '04', '05')
        THEN 'X'
        WHEN mon IN ('06', '07', '08')
        THEN 'Y'
        WHEN mon IN ('09', '10', '11')
        THEN 'Z'
        END;""")

    connection.execute("""UPDATE calendar_hours
    SET o_n = 'O'
    WHERE op_date BETWEEN ? and ?""", (os_start, os_end))

    calendar_hour = 1
    for (rowid,) in connection.execute("""SELECT rowid
    FROM calendar_hours
    ORDER BY op_date, op_hour""").fetchall():
        connection.execute("""UPDATE calendar_hours
        SET calendar_hour = ?
        WHERE rowid = ?""", (calendar_hour, rowid))
        calendar_hour += 1

    connection.executescript("""CREATE UNIQUE INDEX calendar_hour
    ON calendar_hours (calendar_hour);

    CREATE UNIQUE INDEX calendar_future
    ON calendar_hours (future_date, op_hour);""")



def check_data_ranges(table_name, connection, column_types, logfile):
    """Check data for valid ranges.

    Keyword arguments:
    table_name -- name of table to be checked
    connection -- a valid database connection
    column_types -- a group of tuples describing each column, including data
                    ranges or groups of valid values
    logfile -- file where logging messages will be written

    """

    logging.info("  " + table_name)
    print >> logfile
    print >> logfile, "Checking data ranges for table: " + table_name

    dbcur = connection.execute("SELECT * FROM " + table_name)
    for row in dbcur:
        warnings = []
        for (col, (header_text, col_type, required, allowed)) in zip(row, column_types):
            if col is not None and allowed:
                if col_type == 'str':
                    if col.upper() not in allowed:
                        warnings.append(header_text + ' "' + col + '" does not have allowed value')
                if col_type in ('int', 'float'):
                    if col < allowed[0]:
                        warnings.append(header_text + ' ' + str(col) + ' below minimum value ' + str(allowed[0]))
                    if col > allowed[1]:
                        warnings.append(header_text + ' ' + str(col) + ' above maximum value ' + str(allowed[1]))
                if col_type in ('date', 'date-first', 'date-last', 'year-only'):
                    if col < allowed[0]:
                        warnings.append(header_text + ' ' + col + ' before minimum value ' + allowed[0])
                    if col > allowed[1]:
                        warnings.append(header_text + ' ' + col + ' after maximum value ' + allowed[1])
        if warnings:
            print >> logfile, "Table: " + table_name + " -- Warning:", warnings, "-- Row data:", row



def compute_proxy_generation(connection, region, fuel, plant, unit, state, name, base_year, future_year, logfile):
    """Compute proxy generation load for planned or generic new units.

    Keyword arguments:
    connection -- a valid database connection
    region, fuel, plant, unit -- the identifiers of the new unit
    state -- the state where the unit is located
    name -- the name of the plant where the unit is located
    base_year -- the base year where generation is projected from
    future_year -- the year which is being projected
    logfile -- file where logging messages will be written

    """

    # Look up proxy percentage
    input_result = connection.execute("""SELECT proxy_percentage
    FROM calc_input_variables
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?""", (region, fuel)).fetchone()
    if input_result is None:
        print >> logfile, "  Warning: no proxy percentage in input variables for region/fuel " \
            + nice_str((region, fuel)) + " so will set to 50%"
        proxy_percentage = 50.0
    else:
        (proxy_percentage,) = input_result

    # Find unit operating limits
    (unit_max_hi, unit_max_uf, unit_heat_rate, unit_online, unit_offline) = connection.execute("""SELECT max_ertac_hi_hourly_summer,
    max_annual_ertac_uf, ertac_heat_rate, online_start_date, offline_start_date
    FROM calc_updated_uaf
    WHERE orispl_code = ?
    AND unitid = ?
    AND ertac_fuel_unit_type_bin = ?""", (plant, unit, fuel)).fetchone()

    if unit_max_hi is None or unit_max_hi == 0.0 or unit_heat_rate is None or unit_heat_rate == 0.0:
        print >> logfile, "  Warning: new unit " + nice_str((region, fuel, plant, unit)) \
            + " has missing max_ertac_hi_hourly_summer or ertac_heat_rate in UAF, so can't calculate optimal load or gload_proxy"

    # Since calc_generation_proxy stores gload values instead of heat input,
    # need to compute hourly and annual limits in terms of gload.
    if unit_max_hi > 0.0 and unit_heat_rate > 0.0:
        hourly_gload_limit = unit_max_hi * 1000.0 / unit_heat_rate
    else:
        hourly_gload_limit = None

    if unit_max_hi > 0.0 and unit_max_uf > 0.0 and unit_heat_rate > 0.0:
        annual_gload_limit = unit_max_hi * 1000.0 * 8760.0 * unit_max_uf / unit_heat_rate
    else:
        annual_gload_limit = None

    new_old_ratio = 0.0

    # Find position of next-lower existing unit in same region and fuel bin,
    # then find that actual unit to set scaled optimal load for new unit.
    # Non-coal units will also use scaled copy of existing unit to set hourly
    # activity.
    (new_unit_order,) = connection.execute("""SELECT unit_allocation_order
    FROM calc_unit_hierarchy hier
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?
    AND orispl_code = ?
    AND unitid = ?""", (region, fuel, plant, unit)).fetchone()

    (old_unit_order,) = connection.execute("""SELECT MIN(hier.unit_allocation_order)
    FROM calc_unit_hierarchy hier
    JOIN calc_updated_uaf uaf
    ON hier.orispl_code = uaf.orispl_code
    AND hier.unitid = uaf.unitid
    AND hier.ertac_fuel_unit_type_bin = uaf.ertac_fuel_unit_type_bin
    WHERE hier.ertac_region = ?
    AND hier.ertac_fuel_unit_type_bin = ?
    AND hier.unit_allocation_order > ?
    AND uaf.camd_by_hourly_data_type <> 'NEW'""", (region, fuel, new_unit_order)).fetchone()

    if old_unit_order is None:
        print >> logfile, "  Warning: no available existing unit to set optimal load or gload_proxy for new unit " \
            + nice_str((region, fuel, plant, unit)) + " at rank", new_unit_order
    else:
        (old_plant, old_unit) = connection.execute("""SELECT orispl_code, unitid
        FROM calc_unit_hierarchy
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND unit_allocation_order = ?""", (region, fuel, old_unit_order)).fetchone()

        (old_max_heat_input, old_heat_rate, old_optimal_load) = connection.execute("""SELECT max_ertac_hi_hourly_summer,
        ertac_heat_rate, unit_max_optimal_load_threshold
        FROM calc_updated_uaf
        WHERE ertac_region = ?
        AND ertac_fuel_unit_type_bin = ?
        AND orispl_code = ?
        AND unitid = ?""", (region, fuel, old_plant, old_unit)).fetchone()

        if old_max_heat_input is None or old_max_heat_input == 0.0 or old_heat_rate is None or old_heat_rate == 0.0 or old_optimal_load is None or old_optimal_load == 0.0:
            print >> logfile, "  Warning: old unit " + nice_str((region, fuel, old_plant, old_unit)) \
                + " has missing max_ertac_hi_hourly_summer or ertac_heat_rate or unit_max_optimal_load_threshold in UAF, so can't calculate optimal load or gload_proxy for new unit " \
                + nice_str((region, fuel, plant, unit))

        if unit_max_hi > 0.0 and unit_heat_rate > 0.0 and old_max_heat_input > 0.0 and old_heat_rate > 0.0:
            new_old_ratio = (unit_max_hi / unit_heat_rate) / (old_max_heat_input / old_heat_rate)

    if new_old_ratio > 0.0 and old_optimal_load > 0.0:
        new_optimal_load = new_old_ratio * old_optimal_load
        # 20120424 In case old unit was running overloaded, do not set new
        # unit's optimal load above its hourly max.
        if new_optimal_load > hourly_gload_limit:
            new_optimal_load = hourly_gload_limit
        connection.execute("""UPDATE calc_updated_uaf
        SET unit_max_optimal_load_threshold = ?
        WHERE orispl_code = ?
        AND unitid = ?
        AND ertac_fuel_unit_type_bin = ?""", (new_optimal_load, plant, unit, fuel))

    # Create hourly records for a new unit, then determine that unit's
    # individual hourly proxy generation.
    connection.execute("""INSERT INTO calc_generation_proxy
    (ertac_region, ertac_fuel_unit_type_bin, state, facility_name,
    orispl_code, unitid, op_date, op_hour)
    SELECT ?, ?, ?, ?, ?, ?, op_date, op_hour
    FROM calendar_hours""", (region, fuel, state, name, plant, unit))

    cumulative_gload = 0.0

    # Assign proxy load in temporal hierarchy order so annual limits aren't exceeded.
    for (op_date, op_hour) in connection.execute("""SELECT op_date, op_hour
    FROM calc_generation_parms
    WHERE ertac_region = ?
    AND ertac_fuel_unit_type_bin = ?
    ORDER BY temporal_allocation_order""", (region, fuel)).fetchall():

        # Check if new unit only operates for part of future year
        future_date = op_date.replace(base_year, future_year)
        if future_date >= unit_online and future_date < unit_offline:
            
            gload_proxy = None
            # Calculate GLOAD if possible
            if fuel.upper() == 'COAL':
                # 20120119 Formula was wrong in design document; should be
                # dividing by heat_rate, not multiplying it.
                # For new coal unit, calculate constant load of gload_proxy =
                # max_ertac_hi_hourly_summer / ertac_heat_rate * proxy_percentage
                if unit_max_hi > 0.0 and unit_heat_rate > 0.0 and proxy_percentage > 0.0:
                    gload_proxy = unit_max_hi * 1000.0 / unit_heat_rate * proxy_percentage / 100.0

            elif new_old_ratio > 0.0:
                # For new non-coal unit, copy scaled generation from old unit as
                # proxy load for new unit.
                gres = connection.execute("""SELECT gload
                FROM calc_hourly_base
                WHERE ertac_region = ?
                AND ertac_fuel_unit_type_bin = ?
                AND op_date = ?
                AND op_hour = ?
                AND orispl_code = ?
                AND unitid = ?""", (region, fuel, op_date, op_hour, old_plant, old_unit)).fetchone()
                
                #JMJ 11/27/2020  reworked this logic so that when null results were returned it a warning, also cleaned up the gload_proxy = None logic
                if gres is not None: 
                    if gres[0] is not None and gres[0] > 0.0:
                        gload_proxy = gres[0] * new_old_ratio
                else:
                    print >> logfile, "Something odd occurred with calculation of gload proxy and there is possible corruption in camd_hourly_base for: "+str((region, fuel, op_date, op_hour, old_plant, old_unit))

            # Check against gload limits
            if gload_proxy is not None:
                if hourly_gload_limit is not None and gload_proxy > hourly_gload_limit:
                    # 20120508 Variable name was wrong
                    gload_proxy = hourly_gload_limit
                if annual_gload_limit is not None and gload_proxy > annual_gload_limit - cumulative_gload:
                    gload_proxy = annual_gload_limit - cumulative_gload
                cumulative_gload += gload_proxy

                if gload_proxy > 0.0:
                    connection.execute("""UPDATE calc_generation_proxy
                    SET gload_proxy = ?,
                    op_time = 1.0
                    WHERE ertac_region = ?
                    AND ertac_fuel_unit_type_bin = ?
                    AND op_date = ?
                    AND op_hour = ?
                    AND orispl_code = ?
                    AND unitid = ?""", (gload_proxy, region, fuel, op_date, op_hour, plant, unit))



def export_table_to_csv(table_name, prefix, basic_csv_file, connection, column_types, logfile, write_header=True):
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

    if prefix is None:
        prefix = ""
    csv_file = prefix + basic_csv_file

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

    cw = csv.writer(cf)
    row_count = 0
    if write_header:
        names = [col[0] for col in cols]
        cw.writerow(names)
    for row in dbcur:
        cw.writerow(row)
        row_count += 1

    print >> logfile, "Wrote out", row_count, "data rows from table: " + table_name + " to file: " + csv_file
