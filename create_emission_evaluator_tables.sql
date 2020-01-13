-- create_postprocessing_tables.sql

DROP TABLE IF EXISTS rolling_average_inputs;
CREATE TABLE rolling_average_inputs
(identifier TEXT NOT NULL, 
ertac_fuel_unit_type_bin TEXT COLLATE NOCASE, 
state, pollutant TEXT NOT NULL COLLATE NOCASE, 
required_start_date TEXT NOT NULL, 
required_end_date TEXT NOT NULL,
averaging_hours INTEGER NOT NULL,
avoid_crossing_days TEXT NOT NULL COLLATE NOCASE, 
threshold REAL,
PRIMARY KEY (identifier, state, ertac_fuel_unit_type_bin));

DROP TABLE IF EXISTS rolling_average_outputs;
CREATE TABLE rolling_average_outputs
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT NOT NULL COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE, 
fips_code TEXT,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE, 
identifier TEXT NOT NULL, 
averaging_hours INTEGER NOT NULL, 
threshold REAL,
pollutant TEXT NOT NULL, 
number_of_violations INTEGER, 
max_rolling_average REAL,
avg_rolling_average REAL,
PRIMARY KEY (identifier, orispl_code, unitid, state, ertac_fuel_unit_type_bin));

DROP TABLE IF EXISTS altered_rates;
CREATE TABLE altered_rates
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT NOT NULL COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE, 
fips_code TEXT,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE, 
type TEXT NOT NULL, 
existing_rate REAL,
new_rate REAL,
pollutant TEXT NOT NULL, 
time_spans TEXT NOT NULL,
PRIMARY KEY (orispl_code, unitid, state, ertac_fuel_unit_type_bin));

DROP TABLE IF EXISTS default_control_emissions;
CREATE TABLE default_controls
(pollutant_code TEXT NOT NULL COLLATE NOCASE,
emission_rate REAL,
control_efficiency REAL,
control_equipment TEXT);

DROP TABLE IF EXISTS expanded_controls;
CREATE TABLE expanded_controls
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
factor_start_date TEXT,
factor_end_date TEXT,
pollutant_code TEXT NOT NULL COLLATE NOCASE,
emission_rate REAL,
control_efficiency REAL,
control_programs TEXT,
control_description TEXT,
submitter_email TEXT,
state TEXT,
facility_name TEXT,
control_equipment TEXT);


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

-- 28 state_caps
DROP TABLE IF EXISTS state_caps;
CREATE TABLE state_caps
(state_abbreviation TEXT NOT NULL COLLATE NOCASE,
cap_time_period_pollutant TEXT NOT NULL COLLATE NOCASE,
cap_tons REAL NOT NULL,
year_applicable TEXT NOT NULL,
fy_emissions REAL,
comments TEXT,
PRIMARY KEY (state_abbreviation, cap_time_period_pollutant, year_applicable));

-- 28 group_caps
DROP TABLE IF EXISTS group_caps;
CREATE TABLE group_caps
(group_name TEXT NOT NULL COLLATE NOCASE,
cap_time_period_pollutant TEXT NOT NULL COLLATE NOCASE,
cap_tons REAL NOT NULL,
year_applicable TEXT NOT NULL,
fy_emissions REAL,
comments TEXT,
PRIMARY KEY (group_name, cap_time_period_pollutant, year_applicable));