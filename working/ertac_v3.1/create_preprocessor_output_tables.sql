-- create_preprocessor_output_tables.sql

-- Create all output tables for ERTAC EGU pre-processor to pass data on to
-- later post-processing stages.

-- Updated to version 2.0c as of 9/22/2015, for use with new 2.* model.
-- Previous table definitions have been expanded from V1 to V2 for:
-- calc_input_variables
-- calc_updated_uaf
-- calc_generation_parms
-- New tables have been added for:
-- calc_demand_transfers
-- calc_demand_transfer_summary

-- CALC_HOURLY_BASE, p.42, same format as CAMD_HOURLY_BASE and
-- ERTAC_HOURLY_NONCAMD in input, with fuel bin added to accomodate units with
-- fuel switches.  Since we had to add the fuel bin, also added ertac_region to
-- make some of the summary stages simpler by reducing table joins.
-- 4/12/2012 Changed relative order of key columns in index.
DROP TABLE IF EXISTS calc_hourly_base;
CREATE TABLE calc_hourly_base
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT NOT NULL COLLATE NOCASE,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
op_date TEXT NOT NULL,
op_hour INTEGER NOT NULL,
op_time REAL,
gload REAL,
sload REAL,
so2_mass REAL,
so2_mass_flag TEXT,
so2_rate REAL,
so2_rate_flag TEXT,
nox_rate REAL,
nox_rate_flag TEXT,
nox_mass REAL,
nox_mass_flag TEXT,
co2_mass REAL,
co2_mass_flag TEXT,
co2_rate REAL,
co2_rate_flag TEXT,
heat_input REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour, orispl_code, unitid));

-- CALC_UPDATED_UAF, p.43, same format as ERTAC_INITIAL_UAF in input.
-- From 8/10/2015 call, added new hours_cap column for V2.
-- From 9/2/2015 call, appended more columns for computed limits and averages.
DROP TABLE IF EXISTS calc_updated_uaf;
CREATE TABLE calc_updated_uaf
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
form860_plant_id TEXT,
fips_code TEXT,
county_code TEXT,
county_name TEXT,
state TEXT NOT NULL COLLATE NOCASE,
needs_unit_id TEXT,
form860_unit_id TEXT,
plant_latitude REAL,
plant_longitude REAL,
inventory_stack_id TEXT,
facility_name TEXT NOT NULL COLLATE NOCASE,
needs_ipm_region TEXT,
nerc_main_region TEXT,
eia_region_old_nerc TEXT,
ertac_region TEXT NOT NULL COLLATE NOCASE,
other_consuming_regions TEXT,
camd_by_hourly_data_type TEXT NOT NULL COLLATE NOCASE,
annual_hi_partials REAL,
camd_by_operating_status TEXT,
camd_stack_info TEXT,
online_start_date TEXT,
offline_start_date TEXT,
primary_fuel_type TEXT,
main_fuel_characteristics TEXT,
secondary_or_substitute_fuel TEXT,
prime_mover_generator_unit_type TEXT,
camd_unit_type TEXT,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
max_ertac_hi_hourly_summer REAL,
max_ertac_hi_hourly_winter REAL,
hourly_base_max_actual_hi REAL,
nameplate_capacity REAL,
max_summer_capacity REAL,
max_winter_capacity REAL,
max_unit_heat_input REAL,
calculated_by_uf REAL,
max_annual_state_uf REAL,
max_annual_ertac_uf REAL,
operating_hours_by REAL,
max_by_hourly_gload REAL,
max_by_hourly_sload REAL,
nominal_heat_rate REAL,
calc_by_average_heat_rate REAL,
ertac_heat_rate REAL,
unit_annual_capacity_limit REAL,
unit_max_optimal_load_threshold REAL,
unit_min_optimal_load_threshold REAL,
unit_ownership_code TEXT,
multiple_ownership_notation TEXT,
secondary_owner TEXT,
tertiary_owner TEXT,
new_unit_flag TEXT COLLATE NOCASE,
capacity_limited_unit_flag TEXT COLLATE NOCASE,
modifier_email_address TEXT,
unit_completeness_check TEXT,
hours_cap REAL,
program_codes TEXT,
heat_rate_lower_limit REAL,
heat_rate_upper_limit REAL,
heat_rate_lower_stat REAL,
heat_rate_upper_stat REAL,
heat_rate_avg REAL,
heat_rate_os_avg REAL,
heat_rate_nonos_avg REAL,
nox_ef_lower_limit REAL,
nox_ef_upper_limit REAL,
nox_ef_lower_stat REAL,
nox_ef_upper_stat REAL,
nox_ef_avg REAL,
nox_ef_os_avg REAL,
nox_ef_nonos_avg REAL,
so2_ef_lower_limit REAL,
so2_ef_upper_limit REAL,
so2_ef_lower_stat REAL,
so2_ef_upper_stat REAL,
so2_ef_avg REAL,
so2_ef_os_avg REAL,
so2_ef_nonos_avg REAL,
co2_ef_lower_limit REAL,
co2_ef_upper_limit REAL,
co2_ef_lower_stat REAL,
co2_ef_upper_stat REAL,
co2_ef_avg REAL,
co2_ef_os_avg REAL,
co2_ef_nonos_avg REAL,
PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));

-- CALC_UNIT_HIERARCHY, p.48
DROP TABLE IF EXISTS calc_unit_hierarchy;
CREATE TABLE calc_unit_hierarchy
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
unit_allocation_order INTEGER NOT NULL,
submitter_email TEXT,
state TEXT NOT NULL COLLATE NOCASE,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, unit_allocation_order));

-- CALC_1HOUR_HIERARCHY, p.49
DROP TABLE IF EXISTS calc_1hour_hierarchy;
CREATE TABLE calc_1hour_hierarchy
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
one_hour_allocation_order INTEGER NOT NULL,
op_date TEXT NOT NULL,
op_hour INTEGER NOT NULL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, one_hour_allocation_order));

-- CALC_6HOUR_HIERARCHY, p.50
DROP TABLE IF EXISTS calc_6hour_hierarchy;
CREATE TABLE calc_6hour_hierarchy
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
six_hour_allocation_order INTEGER NOT NULL,
op_date TEXT NOT NULL,
op_hour INTEGER NOT NULL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, six_hour_allocation_order));

-- CALC_24HOUR_HIERARCHY, p.51
DROP TABLE IF EXISTS calc_24hour_hierarchy;
CREATE TABLE calc_24hour_hierarchy
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
twentyfour_hour_allocation_order INTEGER NOT NULL,
op_date TEXT NOT NULL,
op_hour INTEGER NOT NULL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, twentyfour_hour_allocation_order));

-- CALC_GENERATION_PROXY, p.52
-- As with CALC_HOURLY_BASE, need to have fuel bin to uniquely identify
-- corresponding unit in UAF, so added region and fuel bin.
-- 4/12/2012 Changed relative order of key columns in index.
DROP TABLE IF EXISTS calc_generation_proxy;
CREATE TABLE calc_generation_proxy
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT NOT NULL COLLATE NOCASE,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
op_date TEXT NOT NULL,
op_hour INTEGER NOT NULL,
op_time REAL,
gload_proxy REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour, orispl_code, unitid));

-- CALC_GENERATION_PARMS, p.53
-- From 9/2/2015 call, add net_demand_transfer column; also need to add calendar_hour.
DROP TABLE IF EXISTS calc_generation_parms;
CREATE TABLE calc_generation_parms
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
op_date TEXT NOT NULL,
op_hour INTEGER NOT NULL,
calendar_hour INTEGER,
temporal_allocation_order INTEGER NOT NULL,
hour_specific_growth_rate REAL,
base_actual_generation REAL,
base_retired_generation REAL,
future_projected_generation REAL,
future_projected_growth REAL,
total_proxy_generation REAL,
net_demand_transfer REAL,
adjusted_projected_generation REAL,
afygr REAL,
excess_generation_pool REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, calendar_hour),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, temporal_allocation_order));

-- CALC_GROWTH_RATES
-- Same format as ERTAC_GROWTH_RATES.
-- 1/6/2012 update added base year to structure.
DROP TABLE IF EXISTS calc_growth_rates;
CREATE TABLE calc_growth_rates
(needs_ipm_region TEXT,
nems_region TEXT,
nerc_region TEXT,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
base_year TEXT NOT NULL,
future_year TEXT NOT NULL,
annual_growth_factor REAL NOT NULL,
peak_growth_factor REAL NOT NULL,
non_peak_growth_factor REAL,
transition_hour_peak_2_formula INTEGER NOT NULL,
transition_hour_formula_2_nonpeak INTEGER NOT NULL,
transition_formula TEXT NOT NULL COLLATE NOCASE,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin));

-- CALC_INPUT_VARIABLES
-- Same format as ERTAC_INPUT_VARIABLES.
-- 10/6/2011 updates added base year to structure.
-- Updated V2 definition based on 8/20/2015 file from Mark.
DROP TABLE IF EXISTS calc_input_variables;
CREATE TABLE calc_input_variables
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
base_year TEXT NOT NULL,
future_year TEXT NOT NULL,
ozone_start_date TEXT,
ozone_end_date TEXT,
hourly_hierarchy_code TEXT NOT NULL COLLATE NOCASE,
heat_rate_avg_method TEXT,
heat_rate_min REAL,
heat_rate_max REAL,
heat_rate_stdev REAL,
nox_avg_method TEXT,
nox_min_ef REAL,
nox_max_ef REAL,
nox_stdev REAL,
so2_avg_method TEXT,
so2_min_ef REAL,
so2_max_ef REAL,
so2_stdev REAL,
co2_avg_method TEXT,
co2_min_ef REAL,
co2_max_ef REAL,
co2_stdev REAL,
default_co2_rate REAL,
new_unit_max_size INTEGER NOT NULL,
new_unit_min_size INTEGER NOT NULL,
demand_cushion REAL NOT NULL,
facility_1 TEXT COLLATE NOCASE,
facility_2 TEXT COLLATE NOCASE,
facility_3 TEXT COLLATE NOCASE,
facility_4 TEXT COLLATE NOCASE,
facility_5 TEXT COLLATE NOCASE,
facility_6 TEXT COLLATE NOCASE,
facility_7 TEXT COLLATE NOCASE,
facility_8 TEXT COLLATE NOCASE,
facility_9 TEXT COLLATE NOCASE,
facility_10 TEXT COLLATE NOCASE,
maximum_annual_ertac_uf REAL NOT NULL,
capacity_demand_deficit_review INTEGER NOT NULL,
unit_optimal_load_threshold_determinant REAL NOT NULL,
proxy_percentage REAL NOT NULL,
generic_so2_control_efficiency REAL NOT NULL,
generic_scr_nox_rate REAL NOT NULL,
generic_sncr_nox_rate REAL NOT NULL,
new_unit_hierarchy_placement_percentile REAL NOT NULL,
new_unit_emission_factor_percentile REAL NOT NULL,
unit_min_optimal_load_threshold_determinant REAL NOT NULL,
heat_input_calculation_percentile REAL NOT NULL,
include_hizgs TEXT COLLATE NOCASE,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin));

-- CALC_CONTROL_EMISSIONS
-- Same format as ERTAC_CONTROL_EMISSIONS.
DROP TABLE IF EXISTS calc_control_emissions;
CREATE TABLE calc_control_emissions
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
factor_start_date TEXT,
factor_end_date TEXT,
pollutant_code TEXT NOT NULL COLLATE NOCASE,
emission_rate REAL,
control_efficiency REAL,
control_programs TEXT,
control_description TEXT,
submitter_email TEXT);

-- CALC_STATE_TOTAL_LISTING
-- Same format as STATE_TOTAL_LISTING, table added for 10/6/2011 spec updates.
DROP TABLE IF EXISTS calc_state_total_listing;
CREATE TABLE calc_state_total_listing
(state_abbreviation TEXT NOT NULL COLLATE NOCASE,
cap_time_period TEXT NOT NULL COLLATE NOCASE,
cap_pollutant TEXT NOT NULL COLLATE NOCASE,
cap_tons REAL NOT NULL,
year_applicable TEXT NOT NULL,
comments TEXT,
contact_info TEXT,
PRIMARY KEY (state_abbreviation, cap_time_period, cap_pollutant, year_applicable));

-- CALC_GROUP_TOTAL_LISTING
-- Same format as GROUP_TOTAL_LISTING, table added for 10/6/2011 spec updates.
DROP TABLE IF EXISTS calc_group_total_listing;
CREATE TABLE calc_group_total_listing
(group_name TEXT NOT NULL COLLATE NOCASE,
states_included TEXT NOT NULL COLLATE NOCASE,
cap_time_period TEXT NOT NULL COLLATE NOCASE,
cap_pollutant TEXT NOT NULL COLLATE NOCASE,
cap_tons REAL NOT NULL,
year_applicable TEXT NOT NULL,
comments TEXT,
contact_info TEXT,
PRIMARY KEY (group_name, cap_time_period, cap_pollutant, year_applicable));

-- CALC_DEMAND_TRANSFERS
-- Same format as ERTAC_DEMAND_TRANSFERS from input.
DROP TABLE IF EXISTS calc_demand_transfers;
CREATE TABLE calc_demand_transfers
(origin_region TEXT NOT NULL COLLATE NOCASE,
origin_fuel TEXT NOT NULL COLLATE NOCASE,
calendar_hour INTEGER NOT NULL,
demand_transfer REAL NOT NULL,
destination_region TEXT NOT NULL COLLATE NOCASE,
destination_fuel TEXT NOT NULL COLLATE NOCASE,
PRIMARY KEY (origin_region, origin_fuel, calendar_hour, destination_region, destination_fuel),
UNIQUE (destination_region, destination_fuel, calendar_hour, origin_region, origin_fuel));

-- CALC_DEMAND_TRANSFER_SUMMARY
-- Net change in demand due to transfers in and/or out.
DROP TABLE IF EXISTS calc_demand_transfer_summary;
CREATE TABLE calc_demand_transfer_summary
(transfer_region TEXT NOT NULL COLLATE NOCASE,
transfer_fuel TEXT NOT NULL COLLATE NOCASE,
calendar_hour INTEGER NOT NULL,
net_demand_change REAL NOT NULL,
PRIMARY KEY (transfer_region, transfer_fuel, calendar_hour));
