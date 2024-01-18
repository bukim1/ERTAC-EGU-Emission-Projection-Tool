# ertac_tables.py

"""ERTAC EGU table characteristics"""

VERSION = "3.0"
#Updated to v3.0 as of November 2, 2021

import sys, os, csv

# For structural checking of CSV input data, and to create header rows for CSV
# output, we use the following groups of column characteristics.  For each
# column there is a tuple of information, including header text, data type, and
# required status.
# The data type should be 'int' or 'float' for numeric data that's actually used
# for calculation; 'date', 'date-first', 'date-last', or 'year-only' for string
# data representing dates; or, 'str' for regular text, including labels, names,
# and descriptions.
# The required flag is intended to make sure that key columns have some data to
# uniquely identify rows when reading CSV files.
# The next item, if present, has a group of allowed values (for text fields) or
# the minimum and maximum range (for numeric and date fields).  Groups of
# allowed text values should be all-uppercase here for later comparisons.
# Additional data checks are performed in other parts of the system, separate
# from the range checks.

# RW 9/30/2015 Updated earliest and latest allowed base year and future year.
base_year_range = ('2007', '2020')
future_year_range = ('2007', '2050')

#jmj 9/17/2017 now allows for the state set to be read in just in case you want to run this in canada or something
default_state_set = set(['AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE',
                 'FL', 'GA', 'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY',
                 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT',
                 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK',
                 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UM', 'UT',
                 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY'])

try:
    cf = open(os.path.join(os.path.relpath(sys.path[0]), 'states.csv'), 'rU')
    states = []
    cr = csv.reader(cf)
    for row in cr:
        if row[0] != 'State FIPS':
            states.append(row[1])                
    state_set = set(states)
except IOError:
    # All allowed fuel bin types.
    # All state abbreviations, for checks that don't need the states lookup table.
    state_set = default_state_set


#jmj 9/7/2017 now allows for a file to be read in with fuel unit type bins set in the same spot as the state file
default_fuel_set = set(['BOILER GAS', 'COAL', 'COMBINED CYCLE GAS', 'OIL', 'SIMPLE CYCLE GAS'])
try:
    cf = open(os.path.join(os.path.relpath(sys.path[0]), 'fuel_unit_type_bins.csv'), 'rU')
    fuels = []
    optional_fuels = []
    cr = csv.reader(cf)
    for row in cr:
        if row[0] != 'Fuel/Unit Type Bin':
            if row[1] == 'Y':
                fuels.append(row[0])
            else:
                optional_fuels.append(row[0])                
    fuel_set = set(fuels)
    optional_fuel_set = set(optional_fuels)
except IOError:
    # All allowed fuel bin types.
    fuel_set = default_fuel_set
    # Other fuel types where demand may be transferred, from 7/8/2015 call.
    # Joseph had already added DEMAND RESPONSE in the regular fuels.
    optional_fuel_set = set(['CHP', 'DEMAND RESPONSE', 'HYDRO', 'NUCLEAR', 'RENEWABLE', 'SOLAR', 'WIND'])

expanded_fuel_set = fuel_set | optional_fuel_set

# All averaging methods allowed for emission factors and heat rate, from 6/24/2015 call.
avg_method_set = set(['HOURLY', 'DAILY', 'MONTHLY', 'QUARTERLY', 'OS/NON-OS', 'ANNUAL'])

# RW 9/17/2015 Based on tests by Doris and Jin, and on data headers in recent
# file downloads from EPA CAMD, it seems that SLOAD on input should actually
# have units of "1000lb/hr" before we convert it to "1000 lbs" for the
# calc_hourly_base table, like the conversion of GLOAD from "MW" (power) to
# "MW-hr" (energy).
camd_columns = (('state', 'str', True, state_set),
                ('facility_name', 'str', True, None),
                ('orispl_code', 'str', True, None),
                ('unitid', 'str', True, None),
                ('op_date', 'date', True, None),
                ('op_hour', 'int', True, (0, 23)),
                ('op_time', 'float', False, (0.0, 1.0)),
                ('gload (MW)', 'float', False, (0.0, 2300.0)),
                ('sload (1000lb/hr)', 'float', False, (0.0, 240000.0)),
                ('so2_mass (lbs)', 'float', False, (0.0, 100000.0)),
                ('so2_mass_measure_flg', 'str', False, None),
                ('so2_rate (lbs/mmBtu)', 'float', False, None),
                ('so2_rate_measure_flg', 'str', False, None),
                ('nox_rate (lbs/mmBtu)', 'float', False, None),
                ('nox_rate_measure_flg', 'str', False, None),
                ('nox_mass (lbs)', 'float', False, (0.0, 20000.0)),
                ('nox_mass_measure_flg', 'str', False, None),
                ('co2_mass (tons)', 'float', False, (0.0, 2900.0)),
                ('co2_mass_measure_flg', 'str', False, None),
                ('co2_rate (tons/mmBtu)', 'float', False, None),
                ('co2_rate_measure_flg', 'str', False, None),
                ('heat_input (mmBtu)', 'float', False, (0.0, 29000.0)))

# Need this copy of CAMD hourly structure with region and fuel bin
# added, for passing from first to second phase of model.
#jmj 9/18/14 calc hourly will now be MW-hours rather than MW
calc_hourly_columns = (('ertac_region', 'str', True, None),
                       ('ertac_fuel_unit_type_bin', 'str', True, fuel_set),
                       ('state', 'str', True, state_set),
                       ('facility_name', 'str', True, None),
                       ('orispl_code', 'str', True, None),
                       ('unitid', 'str', True, None),
                       ('op_date', 'date', True, None),
                       ('op_hour', 'int', True, (0, 23)),
                       ('op_time', 'float', False, (0.0, 1.0)),
                       ('gload (MW-hr)', 'float', False, (0.0, 2300.0)),
                       ('sload (1000 lbs)', 'float', False, (0.0, 240000.0)),
                       ('so2_mass (lbs)', 'float', False, (0.0, 100000.0)),
                       ('so2_mass_measure_flg', 'str', False, None),
                       ('so2_rate (lbs/mmBtu)', 'float', False, None),
                       ('so2_rate_measure_flg', 'str', False, None),
                       ('nox_rate (lbs/mmBtu)', 'float', False, None),
                       ('nox_rate_measure_flg', 'str', False, None),
                       ('nox_mass (lbs)', 'float', False, (0.0, 20000.0)),
                       ('nox_mass_measure_flg', 'str', False, None),
                       ('co2_mass (tons)', 'float', False, (0.0, 2900.0)),
                       ('co2_mass_measure_flg', 'str', False, None),
                       ('co2_rate (tons/mmBtu)', 'float', False, None),
                       ('co2_rate_measure_flg', 'str', False, None),
                       ('heat_input (mmBtu)', 'float', False, (0.0, 29000.0)))

# Important plant characteristic columns which should be consistent in UAF.
uaf_plant_column_names = 'orispl_code, state, ertac_region, facility_name, ' \
    + 'fips_code, county_name, plant_latitude, plant_longitude'

# Alternative detailed list of plant characteristics for more thorough checks.
# uaf_plant_column_names = 'orispl_code, state, ertac_region, facility_name, ' \
#     + 'fips_code, county_name, plant_latitude, plant_longitude, ' \
#     + 'form860_plant_id, county_code, needs_ipm_region, nerc_main_region, ' \
#     + 'eia_region_old_nerc, other_consuming_regions'

# From 8/10/2015 call, added new hours_cap column to UAF for V2; keep V1 definition
# to help conversion.
uaf_columns_v1 = (('orispl_code', 'str', True, None),
               ('unitid', 'str', True, None),
               ('form860_plant_id', 'str', False, None),
               ('fips_code', 'str', False, None),
               ('county_code', 'str', False, None),
               ('county_name', 'str', False, None),
               ('state', 'str', True, state_set),
               ('needs_unit_id', 'str', False, None),
               ('form860_unit_id', 'str', False, None),
               ('plant_latitude', 'str', False, None),
               ('plant_longitude', 'str', False, None),
               ('inventory_stack_id', 'str', False, None),
               ('facility_name', 'str', True, None),
               ('needs_ipm_region', 'str', False, None),
               ('nerc_main_region', 'str', False, None),
               ('eia_region_old_nerc', 'str', False, None),
               ('ertac_region', 'str', True, None),
               ('other_consuming_regions', 'str', False, None),
               ('BY_camd_hourly_data_type', 'str', True, ('FULL', 'NEW', 'NON-CAMD', 'NON-EGU', 'PARTIAL')),
               ('BY_annual_HI_for_partials', 'float', False, None),
               ('BY_camd_operating_status', 'str', False, None),
               ('camd_stack_info', 'str', False, None),
               ('online_start_date', 'date-first', False, None),
               ('offline_start_date', 'date-first', False, None),
               ('primary_fuel_type', 'str', False, None),
               ('main_fuel_characteristics', 'str', False, None),
               ('secondary_or_substitute_fuel', 'str', False, None),
               ('prime_mover_generator_unit_type', 'str', False, None),
               ('camd_unit_type', 'str', False, None),
               ('ertac_fuel_unit_type_bin', 'str', True, fuel_set),
               ('max_ertac_heat_input_hourly_summer (mmBtu)', 'float', False, (0.0, 25000.0)),
               ('max_ertac_heat_input_hourly_winter (mmBtu)', 'float', False, (0.0, 25000.0)),
               ('hourly_base_max_actual_heat_input (mmBtu)', 'float', False, (0.0, 25000.0)),
               ('nameplate_capacity (MW)', 'float', False, None),
               ('max_summer_capacity (MW)', 'float', False, None),
               ('max_winter_capacity (MW)', 'float', False, None),
               ('max_unit_heat_input (mmBtu)', 'float', False, None),
               ('calculated_BY_UF', 'float', False, (0.0, 1.0)),
               ('max_annual_ertac_UF_state_input', 'float', False, None),
               ('max_annual_ertac_UF', 'float', False, (0.0, 1.0)),
               ('BY_operating_hours', 'float', False, (0.0, 8784.0)),
               #should may hourly gload change too to MW-hr?
               ('max_BY_hourly_gload (MW)', 'float', False, (0.0, 1500.0)),
               ('max_BY_hourly_sload (1000 lbs)', 'float', False, (0.0, 250000.0)),
               ('nominal_heat_rate (btu/kw-hr)', 'float', False, None),
               ('calculated_BY_average_heat_rate (btu/kw-hr)', 'float', False, (3000.0, 20000.0)),
               ('ertac_heat_rate (btu/kw-hr)', 'float', False, (3000.0, 20000.0)),
               ('unit_annual_capacity_limit', 'float', False, (0.0, 1.0)),
               ('unit_optimal_load_threshold', 'float', False, None),
               ('unit_minimum_optimal_load_threshold', 'float', False, None),
               ('unit_ownership_code', 'str', False, None),
               ('multiple_ownership_notation', 'str', False, None),
               ('secondary_owner', 'str', False, None),
               ('tertiary_owner', 'str', False, None),
               ('new_unit_flag', 'str', False, ('Y',)),
               ('capacity_limited_unit_flag', 'str', False, ('Y',)),
               ('modifier_email_address', 'str', False, None),
               ('unit_completeness_check', 'str', False, None))

# From 8/10/2015 call, added new hours_cap column for V2.
uaf_columns = uaf_columns_v1 + (('hours_cap', 'float', False, None),
                                ('program_codes', 'str', False, None)) # single item needs trailing comma

# From 8/20/2015 call, added more columns only to calc_updated_uaf, for the
# per-unit min/max limits (hard and statistical) and annual/seasonal averages
# for heat rate and SO2, NOx, and CO2 rates.
calc_uaf_columns = uaf_columns + (
    ('Heat Rate hard lower limit (btu/kw-hr)', 'float', False, None),
    ('Heat Rate hard upper limit (btu/kw-hr)', 'float', False, None),
    ('Heat Rate stat lower limit (btu/kw-hr)', 'float', False, None),
    ('Heat Rate stat upper limit (btu/kw-hr)', 'float', False, None),
    ('Heat Rate annual avg (btu/kw-hr)', 'float', False, None),
    ('Heat Rate OS avg (btu/kw-hr)', 'float', False, None),
    ('Heat Rate non-OS avg (btu/kw-hr)', 'float', False, None),
    ('NOx EF hard lower limit (lbs/mmBtu)', 'float', False, None),
    ('NOx EF hard upper limit (lbs/mmBtu)', 'float', False, None),
    ('NOx EF stat lower limit (lbs/mmBtu)', 'float', False, None),
    ('NOx EF stat upper limit (lbs/mmBtu)', 'float', False, None),
    ('NOx EF annual avg (lbs/mmBtu)', 'float', False, None),
    ('NOx EF OS avg (lbs/mmBtu)', 'float', False, None),
    ('NOx EF non-OS avg (lbs/mmBtu)', 'float', False, None),
    ('SO2 EF hard lower limit (lbs/mmBtu)', 'float', False, None),
    ('SO2 EF hard upper limit (lbs/mmBtu)', 'float', False, None),
    ('SO2 EF stat lower limit (lbs/mmBtu)', 'float', False, None),
    ('SO2 EF stat upper limit (lbs/mmBtu)', 'float', False, None),
    ('SO2 EF annual avg (lbs/mmBtu)', 'float', False, None),
    ('SO2 EF OS avg (lbs/mmBtu)', 'float', False, None),
    ('SO2 EF non-OS avg (lbs/mmBtu)', 'float', False, None),
    ('CO2 EF hard lower limit (tons/mmBtu)', 'float', False, None),
    ('CO2 EF hard upper limit (tons/mmBtu)', 'float', False, None),
    ('CO2 EF stat lower limit (tons/mmBtu)', 'float', False, None),
    ('CO2 EF stat upper limit (tons/mmBtu)', 'float', False, None),
    ('CO2 EF annual avg (tons/mmBtu)', 'float', False, None),
    ('CO2 EF OS avg (tons/mmBtu)', 'float', False, None),
    ('CO2 EF non-OS avg (tons/mmBtu)', 'float', False, None)
)

# 1/6/2012 updates added base year to structure, and confirmed new ranges for
# growth factors as multipliers instead of percentage changes.
growth_rate_columns = (('NEEDS (IPM) Region', 'str', False, None),
                       ('NEMS Region', 'str', False, None),
                       ('NERC Region', 'str', False, None),
                       ('ERTAC Region', 'str', True, None),
                       ('ERTAC Fuel/Unit Type Bin', 'str', True, fuel_set),
                       ('Base Year', 'year-only', True, base_year_range),
                       ('Future Year', 'year-only', True, future_year_range),
                       ('Average Growth Rate', 'float', True, (0.5, 100)),
                       ('Peak Growth Rate', 'float', True, (0.5, 100)),
                       ('NonPeak Growth Rate', 'float', False, (0.0, 2.0)),
                       ('Transition Hour Peak to Formula', 'int', True, (1, 500)),
                       ('Transition Hour to NonPeak', 'int', True, (50, 4000)),
                       ('Transition Formula', 'str', True, ('LINEAR',)))

# 10/6/2011 updates added base year to structure.
# 4/6/2012 added validation for ozone season dates
# 8/20/2015 New input variables V2 will replace previous; keep V1 definition to
# help conversion.
input_variable_columns_v1 = (('ERTAC Region', 'str', True, None),
                          ('ERTAC Fuel/Unit type bin', 'str', True, fuel_set),
                          ('Base Year', 'year-only', True, base_year_range),
                          ('Future Year', 'year-only', True, future_year_range),
                          ('Ozone Season Start Date', 'd-mmm', True, None),
                          ('Ozone Season End Date', 'd-mmm', True, None),
                          ('Hourly Hierarchy', 'str', True, ('HOURLY', '6-HOUR', '24-HOUR')),
                          ('New Unit Max Size', 'int', True, (100, 1000)),
                          ('New Unit Min Size', 'int', True, (20, 500)),
                          ('Demand Cushion', 'float', True, (0.5, 2.0)),
                          ('Facility #1', 'str', False, None),
                          ('Facility #2', 'str', False, None),
                          ('Facility #3', 'str', False, None),
                          ('Facility #4', 'str', False, None),
                          ('Facility #5', 'str', False, None),
                          ('Facility #6', 'str', False, None),
                          ('Facility #7', 'str', False, None),
                          ('Facility #8', 'str', False, None),
                          ('Facility #9', 'str', False, None),
                          ('Facility #10', 'str', False, None),
                          ('Maximum annual ERTAC UF', 'float', True, (0.0, 1.0)),
                          ('Capacity Demand Deficit Review', 'int', True, (100, 1000)),
                          ('Unit Optimal Load Threshold Determinant', 'float', True, (0.0, 100.0)),
                          ('Proxy % (for coal only)', 'float', True, (20.0, 90.0)),
                          ('Generic SO2 control efficiency', 'float', True, (0.0, 100.0)),
                          ('Generic SCR NOx control (lbs/mmBtu)', 'float', True, (0.01, 0.20)),
                          ('Generic SNCR NOx control (lbs/mmBtu)', 'float', True, (0.01, 0.20)),
                          ('New unit percentile for placement in the Unit Allocation Hierarchy', 'float', True, (0.0, 100.0)),
                          ('Percentile for emission factor calculations for new units.', 'float', True, (0.0, 100.0)),
                          ('Unit minimum optimal load threshold percentile', 'float', True, (0.0, 100.0)),
                          ('Percentile for maximum heat input hourly calculations', 'float', True, (0.0, 100.0)))

# 8/20/2015 New version of input variables from Mark with many inserted optional
# columns.
# jmj 11/25/2019 add a column for including including hizg hours 
input_variable_columns = (
    ('ERTAC Region', 'str', True, None),
    ('ERTAC Fuel/Unit type bin', 'str', True, fuel_set),
    ('Base Year', 'year-only', True, base_year_range),
    ('Future Year', 'year-only', True, future_year_range),
    ('Ozone Season Start Date', 'd-mmm', True, None),
    ('Ozone Season End Date', 'd-mmm', True, None),
    ('Hourly Hierarchy', 'str', True, ('HOURLY', '6-HOUR', '24-HOUR')),
    ('HTRate Avg Method', 'str', False, avg_method_set),
    ('HTRate Min (btu/kw-hr)', 'float', False, None),
    ('HTRate Max (btu/kw-hr)', 'float', False, None),
    ('HTRate STDEV', 'float', False, None),
    ('NOx Avg Method', 'str', False, avg_method_set),
    ('NOx Min EF (lbs/mmBtu)', 'float', False, None),
    ('NOx Max EF (lbs/mmBtu)', 'float', False, None),
    ('NOx STDEV', 'float', False, None),
    ('SO2 Avg Method', 'str', False, avg_method_set),
    ('SO2 Min EF (lbs/mmBtu)', 'float', False, None),
    ('SO2 Max EF (lbs/mmBtu)', 'float', False, None),
    ('SO2 STDEV', 'float', False, None),
    ('CO2 Avg Method', 'str', False, avg_method_set),
    ('CO2 Min EF (tons/mmBtu)', 'float', False, None),
    ('CO2 Max EF (tons/mmBtu)', 'float', False, None),
    ('CO2 STDEV', 'float', False, None),
    ('Default CO2 Rate (tons/mmBtu)', 'float', False, None),
    ('New Unit Max Size', 'int', True, (100, 1000)),
    ('New Unit Min Size', 'int', True, (20, 500)),
    ('Demand Cushion', 'float', True, (0.5, 2.0)),
    ('Facility #1', 'str', False, None),
    ('Facility #2', 'str', False, None),
    ('Facility #3', 'str', False, None),
    ('Facility #4', 'str', False, None),
    ('Facility #5', 'str', False, None),
    ('Facility #6', 'str', False, None),
    ('Facility #7', 'str', False, None),
    ('Facility #8', 'str', False, None),
    ('Facility #9', 'str', False, None),
    ('Facility #10', 'str', False, None),
    ('Maximum annual ERTAC UF', 'float', True, (0.0, 1.0)),
    ('Capacity Demand Deficit Review', 'int', True, (100, 1000)),
    ('Unit Optimal Load Threshold Determinant', 'float', True, (0.0, 100.0)),
    ('Proxy % (for coal only)', 'float', True, (20.0, 90.0)),
    ('Generic SO2 control efficiency', 'float', True, (0.0, 100.0)),
    ('Generic SCR NOx control (lbs/mmBtu)', 'float', True, (0.01, 0.20)),
    ('Generic SNCR NOx control (lbs/mmBtu)', 'float', True, (0.01, 0.20)),
    ('New unit percentile for placement in the Unit Allocation Hierarchy', 'float', True, (0.0, 100.0)),
    ('Percentile for emission factor calculations for new units.', 'float', True, (0.0, 100.0)),
    ('Unit minimum optimal load threshold percentile', 'float', True, (0.0, 100.0)),
    ('Percentile for maximum heat input hourly calculations', 'float', True, (0.0, 100.0)),
    ('Include HIZG hours?', 'str', False, ('TRUE','FALSE'))
)

control_emission_columns = (('ORISPL_CODE', 'str', True, None),
                            ('UNITID', 'str', True, None),
                            ('Factor Start Date', 'date', False, None),
                            ('Factor End Date', 'date', False, None),
                            ('Pollutant', 'str', True, None),
                            ('Emission Rate', 'float', False, None),
                            ('Control Efficiency', 'float', False, (0.0, 100.0)),
                            ('Programs for Pollutant', 'str', False, None),
                            ('Control Description', 'str', False, None),
                            ('Submitter email', 'str', False, None))

seasonal_control_emission_columns = (('ORISPL_CODE', 'str', True, None),
                            ('UNITID', 'str', True, None),
                            ('Factor Start Date', 'date', False, None),
                            ('Factor End Date', 'date', False, None),
                            ('Season Start Month', 'int', True, None),
                            ('Season End Month', 'int', True, None),
                            ('Season Start Date', 'int', True, None),
                            ('Season End Date', 'int', True, None),
                            ('Pollutant', 'str', True, None),
                            ('Emission Rate', 'float', False, None),
                            ('Control Efficiency', 'float', False, (0.0, 100.0)),
                            ('Programs for Pollutant', 'str', False, None),
                            ('Control Description', 'str', False, None),
                            ('Submitter email', 'str', False, None))

unit_hierarchy_columns = (('ertac_region', 'str', True, None),
                          ('ertac_fuel_unit_type_bin', 'str', True, fuel_set),
                          ('orispl_code', 'str', True, None),
                          ('unitid', 'str', True, None),
                          ('unit_allocation_order', 'int', True, None),
                          ('submitter_email', 'str', False, None),
                          ('state', 'str', True, state_set))

temporal_hierarchy_columns = (('ertac_region', 'str', True, None),
                              ('ertac_fuel_unit_type_bin', 'str', True, fuel_set),
                              ('temporal_allocation_order', 'int', True, (1, 8784)),
                              ('op_date', 'date', True, None),
                              ('op_hour', 'int', True, (0, 23)))

generation_proxy_columns = (('ertac_region', 'str', True, None),
                            ('ertac_fuel_unit_type_bin', 'str', True, fuel_set),
                            ('state', 'str', True, state_set),
                            ('facility_name', 'str', True, None),
                            ('orispl_code', 'str', True, None),
                            ('unitid', 'str', True, None),
                            ('op_date', 'date', True, None),
                            ('op_hour', 'int', True, (0, 23)),
                            ('op_time', 'float', False, None),
                            ('gload_proxy', 'float', False, None))

# From 9/2/2015 call, add net_demand_transfer column; also need to add calendar_hour.
generation_parms_columns = (('ertac_region', 'str', True, None),
                            ('ertac_fuel_unit_type_bin', 'str', True, fuel_set),
                            ('op_date', 'date', True, None),
                            ('op_hour', 'int', True, (0, 23)),
                            ('calendar_hour', 'int', True, (1, 8784)),
                            ('temporal_allocation_order', 'int', True, (1, 8784)),
                            ('hour_specific_growth_rate', 'float', False, None),
                            ('base_actual_generation', 'float', False, None),
                            ('base_retired_generation', 'float', False, None),
                            ('future_projected_generation', 'float', False, None),
                            ('future_projected_growth', 'float', False, None),
                            ('total_proxy_generation', 'float', False, None),
                            ('net_demand_transfer', 'float', False, None),
                            ('adjusted_projected_generation', 'float', False, None),
                            ('afygr', 'float', False, None),
                            ('excess_generation_pool', 'float', False, None))

states_columns = (('state_code', 'str', True, None),
                  ('state_abbreviation', 'str', True, None),
                  ('state_name', 'str', True, None))

counties_columns = (('state_abbreviation', 'str', True, None),
                    ('state_code', 'str', True, None),
                    ('county_code', 'str', True, None),
                    ('county_name', 'str', True, None))

# New tables added in 10/6/2011 specification updates for emission caps.
state_total_columns = (('State Abbreviation', 'str', True, state_set),
                       ('Cap Time Period (OS or annual)', 'str', True, ('ANNUAL', 'OS')),
                       ('Cap Pollutant (SO2 or NOx)', 'str', True, ('NOX', 'SO2')),
                       ('Cap (TPY or TPOS)', 'float', True, None),
                       ('Year Applicable', 'year-only', True, None),
                       ('Comments', 'str', False, None),
                       ('Contact Info', 'str', False, None))

group_total_columns = (('Group Name', 'str', True, None),
                       ('States Included', 'str', True, None),
                       ('Cap Time Period (OS or annual)', 'str', True, ('ANNUAL', 'OS')),
                       ('Cap Pollutant (SO2 or NOx)', 'str', True, ('NOX', 'SO2')),
                       ('Cap (TPY or TPOS)', 'float', True, None),
                       ('Year Applicable', 'year-only', True, None),
                       ('Comments', 'str', False, None),
                       ('Contact Info', 'str', False, None))

# 8/10/2015 Changes in terminology and column order for newly-added optional
# energy tranfer table ERTAC_DEMAND_TRANSFERS.
demand_transfer_columns = (
    ('Demand Origin ERTAC Region', 'str', True, None),
    ('Demand Origin Fuel/Unit Type Bin', 'str', True, expanded_fuel_set),
    ('Calendar Hour', 'int', True, (1, 8784)),
    ('Demand Transfer (MW-hrs)', 'float', True, (0.0, 5000.0)),
    ('Demand Destination ERTAC Region', 'str', True, None),
    ('Demand Destination Fuel/Unit Type Bin', 'str', True, expanded_fuel_set)
)

# 9/21/2015 Add persistent record of total net demand change due to transfers.
demand_transfer_summary_columns = (
    ('Transfer ERTAC Region', 'str', True, None),
    ('Transfer Fuel/Unit Type Bin', 'str', True, expanded_fuel_set),
    ('Calendar Hour', 'int', True, (1, 8784)),
    ('Net Demand Change (MW-hrs)', 'float', True, None)
)