-- create_projection_output_tables.sql

-- Create all output tables for ERTAC EGU projection algorithm.

-- Updated to version 2.0c as of 9/3/2015.
-- From 9/2/2015 call, add columns for demand transfer effects to the following:
-- demand_generation_deficit
-- reserve_capacity_needed
-- capacity_and_fy_demand
-- capacity_and_fy_reserve

-- Each output was described by Doris in the Reporting Functions document.

-- 19.1 demand_generation_deficit
-- RW 9/3/2015 demand_generation_deficit can include rows due to creation of new
-- units (V1) and also rows that only show demand transfer effects (V2), so many
-- columns can't be declared NOT NULL anymore.
DROP TABLE IF EXISTS demand_generation_deficit;
CREATE TABLE demand_generation_deficit
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
calendar_hour INTEGER NOT NULL,
hierarchy_hour INTEGER,
generation_needed REAL,
generation_due_to_demand_transfer REAL,
total_generation_needed REAL,
generation_available REAL,
generation_lacking REAL,
generation_after_new_units REAL,
deficit_flag TEXT,
transfer_flag TEXT,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, hierarchy_hour),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, calendar_hour));

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

-- 23Y1 reserve_capacity_needed
DROP TABLE IF EXISTS reserve_capacity_needed;
CREATE TABLE reserve_capacity_needed
(ertac_region TEXT NOT NULL COLLATE NOCASE,
calendar_hour INTEGER NOT NULL,
hierarchy_hour INTEGER NOT NULL,
pass_fail TEXT NOT NULL COLLATE NOCASE,
reserve_needed REAL NOT NULL,
amount_available_without_transfers REAL NOT NULL,
amount_available_including_transfers REAL NOT NULL,
deficit REAL,
PRIMARY KEY (ertac_region, hierarchy_hour),
UNIQUE (ertac_region, calendar_hour));

-- 23.5Y1 unit_level_activity
-- Added OS and non-OS heat rate based on 8/10/2015 call.
DROP TABLE IF EXISTS unit_level_activity;
CREATE TABLE unit_level_activity
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT NOT NULL COLLATE NOCASE,
state TEXT NOT NULL COLLATE NOCASE,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
max_ertac_hi_hourly_summer REAL,
heat_rate REAL,
os_heat_rate REAL,
nonos_heat_rate REAL,
capacity REAL,
num_hrs_fy_max INTEGER,
uf REAL,
by_gen REAL,
by_hi REAL,
by_hours REAL,
fy_gen REAL,
fy_hi REAL,
fy_hours REAL,
PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));

-- 27.5B cap_analysis
DROP TABLE IF EXISTS cap_analysis;
CREATE TABLE cap_analysis
(state_or_group TEXT NOT NULL COLLATE NOCASE,
cap_time_period_pollutant TEXT NOT NULL COLLATE NOCASE,
cap_tons REAL NOT NULL,
year_applicable TEXT NOT NULL,
fy_emissions REAL,
fy_emissions_gen_control REAL,
comments TEXT,
PRIMARY KEY (state_or_group, cap_time_period_pollutant, year_applicable));

-- 27.5B unit_generic_controls
DROP TABLE IF EXISTS unit_generic_controls;
CREATE TABLE unit_generic_controls
(orispl_code TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
capacity REAL,
age INTEGER,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
cap_time_period_pollutant TEXT NOT NULL COLLATE NOCASE,
by_emission_rate REAL,
fy_emission_rate REAL,
by_emissions REAL,
fy_emissions REAL,
PRIMARY KEY (orispl_code, unitid, ertac_fuel_unit_type_bin));

-- 28 capacity_and_fy_demand
DROP TABLE IF EXISTS capacity_and_fy_demand;
CREATE TABLE capacity_and_fy_demand
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
by_gen REAL,
by_hi REAL,
fy_gen_including_transfers REAL,
fy_hi REAL,
fy_transfers REAL,
new_gen REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin));

-- 28 capacity_and_fy_reserve
DROP TABLE IF EXISTS capacity_and_fy_reserve;
CREATE TABLE capacity_and_fy_reserve
(ertac_region TEXT NOT NULL COLLATE NOCASE,
total_transfers REAL,
reserve_met TEXT NOT NULL COLLATE NOCASE,
max_deficit REAL,
PRIMARY KEY (ertac_region));

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

-- 28 hourly_diagnostic_file
-- Report description did not include region/fuel, but if fuel switch occurs
-- during future year, then plant/unit does not uniquely identify specific
-- part of unit's operation.  Added region/fuel columns to table.
-- 4/12/2012 Changed relative order of key columns in index.
-- Additional flag columns based on 8/10/2015 call for period types and limits for heat rate and emission factors.
-- CO2 columns added for potential future use after 8/20/2015 call.

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
annual_oh_limit TEXT NOT NULL COLLATE NOCASE,
cumulative_hi REAL,
cumulative_gen REAL,
cumulative_op_hours REAL,
gload REAL,
heat_input REAL,
heat_rate REAL,
heat_rate_type TEXT,
heat_rate_limit_flag TEXT,
so2_mass REAL,
so2_rate REAL,
so2_rate_type TEXT,
so2_rate_limit_flag TEXT,
nox_rate REAL,
nox_mass REAL,
nox_rate_type TEXT,
nox_rate_limit_flag TEXT,
co2_mass REAL,
co2_rate REAL,
co2_rate_type TEXT,
co2_rate_limit_flag TEXT,
generation_flag TEXT,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, hierarchy_hour, orispl_code, unitid),
UNIQUE (ertac_region, calendar_hour, ertac_fuel_unit_type_bin, orispl_code, unitid));
