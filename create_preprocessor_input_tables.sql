-- create_preprocessor_input_tables.sql

-- Create all input tables for ERTAC EGU preprocessor.

-- This file should be in the code directory with the Python files, not in the
-- data directory.

-- Column names and data types are based on the descriptions in the 4/21/2011
-- design document, as last updated by "Data Files Needed January 6, 2012.xlsx",
-- with the following differences:
-- Character field lengths are not enforced by SQLite, so are not specified.
-- Some digit strings (e.g. ORIS ID, FIPS codes) are actually labels rather than
-- numbers, and are stored as text rather than integer.
-- Character fields used for joining tables and selecting data subsets are
-- specified as COLLATE NOCASE to overcome possibly-inconsistent capitalization
-- in the data files.

-- CAMD_HOURLY_BASE, p.29
DROP TABLE IF EXISTS camd_hourly_base;
CREATE TABLE camd_hourly_base
(state TEXT NOT NULL COLLATE NOCASE,
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
PRIMARY KEY (orispl_code, unitid, op_date, op_hour));

-- ERTAC_HOURLY_NONCAMD, p.30, same format as CAMD_HOURLY_BASE
DROP TABLE IF EXISTS ertac_hourly_noncamd;
CREATE TABLE ertac_hourly_noncamd
(state TEXT NOT NULL COLLATE NOCASE,
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
PRIMARY KEY (orispl_code, unitid, op_date, op_hour));

-- ERTAC_INITIAL_UAF, p.31
-- In order to cope with fuel switches, need to include fuel type in primary key
-- to distinguish among possible multiple listings for same plant+unit.  Earlier
-- attempt to identify unique units by including camd_by_hourly_data_type failed
-- when UAF data had a repeated "FULL" unit with different fuels in different
-- starting years, both before 2010.
DROP TABLE IF EXISTS ertac_initial_uaf;
CREATE TABLE ertac_initial_uaf
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
PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));

-- ERTAC_GROWTH_RATES, p.36
-- 1/6/2012 update added base year to structure.
DROP TABLE IF EXISTS ertac_growth_rates;
CREATE TABLE ertac_growth_rates
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

-- ERTAC_INPUT_VARIABLES, p.38
-- 10/6/2011 updates added base year to structure.
DROP TABLE IF EXISTS ertac_input_variables;
CREATE TABLE ertac_input_variables
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
base_year TEXT NOT NULL,
future_year TEXT NOT NULL,
ozone_start_date TEXT,
ozone_end_date TEXT,
hourly_hierarchy_code TEXT NOT NULL COLLATE NOCASE,
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
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin));

-- ERTAC_CONTROL_EMISSIONS, p.40
DROP TABLE IF EXISTS ertac_control_emissions;
CREATE TABLE ertac_control_emissions
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

-- ERTAC_SEASONAL_CONTROL_EMISSIONS
-- this is a new table to ease the process of keeping seasonal controls up to date
DROP TABLE IF EXISTS ertac_seasonal_control_emissions;
CREATE TABLE ertac_seasonal_control_emissions
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
factor_start_date TEXT,
factor_end_date TEXT,
season_start_month INTEGER NOT NULL,
season_end_month INTEGER NOT NULL,
season_start_date INTEGER NOT NULL,
season_end_date INTEGER NOT NULL,
pollutant_code TEXT NOT NULL COLLATE NOCASE,
emission_rate REAL,
control_efficiency REAL,
control_programs TEXT,
control_description TEXT,
submitter_email TEXT);

-- STATES, lookup table of postal abbreviations and FIPS state codes.
DROP TABLE IF EXISTS states;
CREATE TABLE states
(state_code TEXT NOT NULL,
state_abbreviation TEXT NOT NULL COLLATE NOCASE,
state_name TEXT NOT NULL COLLATE NOCASE,
PRIMARY KEY (state_code),
UNIQUE (state_abbreviation));

-- COUNTIES, lookup table of FIPS state/county codes and county names.
DROP TABLE IF EXISTS counties;
CREATE TABLE counties
(state_abbreviation TEXT NOT NULL COLLATE NOCASE,
state_code TEXT NOT NULL,
county_code TEXT NOT NULL,
county_name TEXT NOT NULL COLLATE NOCASE,
PRIMARY KEY (state_code, county_code));

-- State total listing for emission caps, table added for 10/6/2011 spec updates.
DROP TABLE IF EXISTS state_total_listing;
CREATE TABLE state_total_listing
(state_abbreviation TEXT NOT NULL COLLATE NOCASE,
cap_time_period TEXT NOT NULL COLLATE NOCASE,
cap_pollutant TEXT NOT NULL COLLATE NOCASE,
cap_tons REAL NOT NULL,
year_applicable TEXT NOT NULL,
comments TEXT,
contact_info TEXT,
PRIMARY KEY (state_abbreviation, cap_time_period, cap_pollutant, year_applicable));

-- Group total listing for emission caps, table added for 10/6/2011 spec updates.
DROP TABLE IF EXISTS group_total_listing;
CREATE TABLE group_total_listing
(group_name TEXT NOT NULL COLLATE NOCASE,
states_included TEXT NOT NULL COLLATE NOCASE,
cap_time_period TEXT NOT NULL COLLATE NOCASE,
cap_pollutant TEXT NOT NULL COLLATE NOCASE,
cap_tons REAL NOT NULL,
year_applicable TEXT NOT NULL,
comments TEXT,
contact_info TEXT,
PRIMARY KEY (group_name, cap_time_period, cap_pollutant, year_applicable));
