-- create_postprocessing_tables.sql

-- Create all output tables for ERTAC EGU postprocessor.
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

-- CALC_UPDATED_UAF, p.43, same format as ERTAC_INITIAL_UAF in input.
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


-- CALC_INPUT_VARIABLES
-- Same format as ERTAC_INPUT_VARIABLES.
-- 10/6/2011 updates added base year to structure.
DROP TABLE IF EXISTS calc_input_variables;
CREATE TABLE calc_input_variables
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

-- CALC_GENERATION_PARMS, p.53
DROP TABLE IF EXISTS calc_generation_parms;
CREATE TABLE calc_generation_parms
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
op_date TEXT NOT NULL,
op_hour INTEGER NOT NULL,
temporal_allocation_order INTEGER NOT NULL,
hour_specific_growth_rate REAL,
base_actual_generation REAL,
base_retired_generation REAL,
future_projected_generation REAL,
future_projected_growth REAL,
total_proxy_generation REAL,
adjusted_projected_generation REAL,
afygr REAL,
excess_generation_pool REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour),
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

-- 19.1 generic_units_created
DROP TABLE IF EXISTS generic_units_created;
CREATE TABLE generic_units_created
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
new_unit_size REAL NOT NULL,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT NOT NULL COLLATE NOCASE,
plant_latitude REAL,
plant_longitude REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid));

DROP TABLE IF EXISTS hourly_diagnostic_file;
CREATE TABLE hourly_diagnostic_file
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
calendar_hour INTEGER NOT NULL,
hierarchy_hour INTEGER NOT NULL,
hourly_hi_limit TEXT NOT NULL COLLATE NOCASE,
annual_hi_limit TEXT NOT NULL COLLATE NOCASE,
cumulative_hi REAL,
cumulative_gen REAL,
gload REAL,
heat_input REAL,
so2_mass REAL,
so2_rate REAL,
nox_rate REAL,
nox_mass REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, hierarchy_hour, orispl_code, unitid),
UNIQUE (ertac_region, calendar_hour, ertac_fuel_unit_type_bin, orispl_code, unitid));

DROP TABLE IF EXISTS hourly_activity_summary;
CREATE TABLE hourly_activity_summary
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
by_ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE,
calendar_hour INTEGER NOT NULL,
hierarchy_hour INTEGER,
by_hierarchy_hour INTEGER,
by_gload REAL,
fy_gload REAL,
by_heat_input REAL,
fy_heat_input REAL,
by_so2_mass REAL,
fy_so2_mass REAL,
by_nox_mass REAL,
fy_nox_mass REAL,
hour_specific_growth_rate REAL,
afygr REAL,
data_type TEXT COLLATE NOCASE,
facility_name TEXT COLLATE NOCASE,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid, calendar_hour),
UNIQUE (ertac_region, calendar_hour, ertac_fuel_unit_type_bin, orispl_code, unitid));

DROP TABLE IF EXISTS hourly_regional_activity_summary;
CREATE TABLE hourly_regional_activity_summary
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
calendar_hour INTEGER NOT NULL,
hierarchy_hour INTEGER NOT NULL,
by_gload REAL,
fy_gload REAL,
fy_op_max_count REAL,
by_heat_input REAL,
fy_heat_input REAL,
by_so2_mass REAL,
fy_so2_mass REAL,
by_nox_mass REAL,
fy_nox_mass REAL,
hour_specific_growth_rate REAL,
afygr REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, calendar_hour),
UNIQUE (ertac_region, calendar_hour, ertac_fuel_unit_type_bin));

DROP TABLE IF EXISTS hourly_state_activity_summary;
CREATE TABLE hourly_state_activity_summary
(state TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
calendar_hour INTEGER NOT NULL,
by_gload REAL,
fy_gload REAL,
by_heat_input REAL,
fy_heat_input REAL,
by_so2_mass REAL,
fy_so2_mass REAL,
by_nox_mass REAL,
fy_nox_mass REAL,
PRIMARY KEY (state, ertac_fuel_unit_type_bin, calendar_hour),
UNIQUE (state, ertac_fuel_unit_type_bin, calendar_hour));

DROP TABLE IF EXISTS annual_summary;
CREATE TABLE annual_summary
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE,
fips_code TEXT NOT NULL COLLATE NOCASE,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
by_ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
max_unit_heat_input REAL,
ertac_heat_rate REAL,
generation_capacity REAL,
nameplate_capacity REAL,
fy_op_hours_max REAL,
by_uf REAL,
fy_uf REAL,
by_gload REAL,
by_heat_input REAL,
fy_gload REAL,
fy_heat_input REAL,
by_so2_mass REAL,
by_so2_rate REAL,
by_nox_mass REAL,
by_nox_rate REAL,
by_os_nox_mass REAL,
by_os_nox_rate REAL,
by_os_heat_input REAL,
by_non_os_nox_mass REAL,
by_non_os_nox_rate REAL,
fy_so2_mass REAL,
fy_so2_rate REAL,
fy_nox_mass REAL,
fy_nox_rate REAL,
fy_os_nox_mass REAL,
fy_os_nox_rate REAL,
fy_os_heat_input REAL,
fy_non_os_nox_mass REAL,
fy_non_os_nox_rate REAL,
hierarchy_order REAL,
longitude REAL,
latitude REAL,
gdu_flag TEXT,
retirement_date TEXT,
new_unit_flag TEXT,
data_type TEXT COLLATE NOCASE,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin,  orispl_code, unitid),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid));


-- CALC_GENERATION_PARMS, p.53
DROP TABLE IF EXISTS generation_parms;
CREATE TABLE generation_parms
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
op_date TEXT NOT NULL,
op_hour INTEGER NOT NULL,
temporal_allocation_order INTEGER NOT NULL,
hour_specific_growth_rate REAL,
base_actual_generation REAL,
base_retired_generation REAL,
future_projected_generation REAL,
future_projected_growth REAL,
total_proxy_generation REAL,
adjusted_projected_generation REAL,
afygr REAL,
excess_generation_pool REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, op_date, op_hour),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, temporal_allocation_order));