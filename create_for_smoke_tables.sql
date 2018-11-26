-- create_for_smoke_tables.sql
-- Create all output tables for ERTAC EGU postprocessor.
-- CALC_HOURLY_BASE, p.42, same format as CAMD_HOURLY_BASE and
-- ERTAC_HOURLY_NONCAMD in input, with fuel bin added to accomodate units with
-- fuel switches.  Since we had to add the fuel bin, also added ertac_region to
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
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin));

CREATE TABLE fy_emission_rates
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
state TEXT,
pm25_rate REAL,
pm25_source TEXT,
pm10_rate REAL,
pm10_source TEXT,
co_rate REAL,
co_source TEXT,
voc_rate REAL,
voc_source TEXT,
nh3_rate REAL,
nh3_source TEXT,
cl2_rate REAL,
cl2_source TEXT,
hcl_rate REAL,
hcl_source TEXT,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid, unitid));


DROP TABLE IF EXISTS ertac_pusp_info_file;
CREATE TABLE ertac_pusp_info_file
(ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
state TEXT,
offline_start_date TEXT,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
plantid TEXT,
pointid TEXT,
stackid TEXT,
segment TEXT,
agy_plantid TEXT,
agy_pointid TEXT,
agy_stackid TEXT,
agy_segment TEXT,
stkhgt REAL,
stkdiam REAL,
stktemp REAL,
stkflow REAL,
stkvel REAL,
scc TEXT,
time_zone TEXT,
nox_percentage REAL,
so2_percentage REAL,
pm25_percentage REAL,
pm10_percentage REAL,
co_percentage REAL,
voc_percentage REAL,
nh3_percentage REAL,
hap_percentage REAL,
sic_code REAL,
mact REAL,
naics TEXT,
comments TEXT,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, state, orispl_code, unitid, plantid, pointid, stackid, segment));

-- Same format as ERTAC_CONTROL_EMISSIONS.
DROP TABLE IF EXISTS ertac_base_year_rates_and_additional_controls;
CREATE TABLE ertac_base_year_rates_and_additional_controls
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
factor_start_date TEXT,
factor_end_date TEXT,
pollutant_code TEXT NOT NULL COLLATE NOCASE,
base_year_rate REAL,
emission_rate REAL,
control_efficiency REAL,
control_programs TEXT,
control_description TEXT,
submitter_email TEXT);

DROP TABLE IF EXISTS ertac_additional_variables;
CREATE TABLE ertac_additional_variables
(state TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
pm25_rate REAL,
pm10_rate REAL,
co_rate REAL,
voc_rate REAL,
nh3_rate REAL,
cl2_rate REAL,
hcl_rate REAL,
new_unit_scc TEXT,
new_unit_stkhgt REAL,
new_unit_stkdiam REAL,
new_unit_stktemp REAL,
new_unit_stkflow REAL,
new_unit_stkvel REAL,
comments TEXT,
PRIMARY KEY (state, ertac_fuel_unit_type_bin));

DROP TABLE IF EXISTS ertac_rpo_listing;
CREATE TABLE ertac_rpo_listing
(rpo TEXT NOT NULL COLLATE NOCASE,
states TEXT NOT NULL COLLATE NOCASE);

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
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin, hierarchy_hour, orispl_code, unitid),
UNIQUE (ertac_region, calendar_hour, ertac_fuel_unit_type_bin, orispl_code, unitid));

DROP TABLE IF EXISTS orl_future;
CREATE TABLE orl_future
(fips TEXT,
plantid TEXT,
pointid TEXT,
stackid TEXT,
segment TEXT,
plant TEXT,
scc TEXT,
erprtype TEXT,
srctype TEXT,
stkhgt REAL,
stkdiam REAL,
stktemp REAL,
stkflow REAL,
stkvel REAL,
sic_code REAL,
mact REAL,
naics TEXT,
ctype TEXT,
lon REAL,
lat REAL,
utmz REAL,
cas TEXT,
ann_emis REAL,
avd_emis REAL,
ceff REAL,
reff REAL, 
cpri REAL, 
csec REAL, 
nei_unique_id TEXT,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
ipm_yn TEXT,
data_source TEXT,
PRIMARY KEY (plantid, pointid, stackid, segment, orispl_code, unitid, cas));

DROP TABLE IF EXISTS ff10_future;
CREATE TABLE ff10_future
(
country TEXT,
fips TEXT,
tribalcode TEXT,
plantid TEXT,
pointid TEXT,
stackid TEXT,
segment TEXT,
agy_plantid TEXT,
agy_pointid TEXT,
agy_stackid TEXT,
agy_segment TEXT,
scc TEXT,
cas TEXT,
ann_emis REAL,
ann_pct_red REAL,
plant TEXT,
erprtype TEXT,
stkhgt REAL,
stkdiam REAL,
stktemp REAL,
stkflow REAL,
stkvel REAL,
naics TEXT,
lon REAL,
lat REAL,
ll_datum TEXT,
horiz_coll_mthd TEXT,
design_capacity REAL,
design_capacity_units TEXT,
reg_codes TEXT,
srctype TEXT,
unit_typce_cod TEXT,
control_ids TEXT,
control_measures TEXT,
current_cost REAL,
cumulative_cost REAL,
projection_fact REAL,
submitter_fac_id TEXT,
calc_method REAL,
data_set_id TEXT,
facil_category_code TEXT,
orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
ipm_yn TEXT,
calc_year TEXT,
date_updated INTEGER,
fug_height REAL,
fug_width_ydim REAL,
fug_length_xdim REAL,
fug_angle REAL,
zipcode TEXT,
annual_avg_hours_per_year REAL,
jan_value REAL DEFAULT 0,
feb_value REAL DEFAULT 0,
mar_value REAL DEFAULT 0,
apr_value REAL DEFAULT 0,
may_value REAL DEFAULT 0,
jun_value REAL DEFAULT 0,
jul_value REAL DEFAULT 0,
aug_value REAL DEFAULT 0,
sep_value REAL DEFAULT 0,
oct_value REAL DEFAULT 0,
nov_value REAL DEFAULT 0,
dec_value REAL DEFAULT 0,
jan_pctred REAL,
feb_pctred REAL,
mar_pctred REAL,
apr_pctred REAL,
may_pctred REAL,
jun_pctred REAL,
jul_pctred REAL,
aug_pctred REAL,
sep_pctred REAL,
oct_pctred REAL,
nov_pctred REAL,
dec_pctred REAL,
comment TEXT,
PRIMARY KEY (plantid, pointid, stackid, segment, orispl_code, unitid, scc, cas, facil_category_code));

-- Create all output tables for ERTAC EGU postprocessor.
DROP TABLE IF EXISTS pt_hourly_future;
CREATE TABLE pt_hourly_future
(state TEXT NOT NULL COLLATE NOCASE,
county TEXT NOT NULL COLLATE NOCASE,
plantid TEXT,
pointid TEXT,
stackid TEXT,
segment TEXT,
polcode TEXT,
op_date TEXT NOT NULL,
time_zone TEXT,
hrval1 REAL DEFAULT 0,
hrval2 REAL DEFAULT 0,
hrval3 REAL DEFAULT 0,
hrval4 REAL DEFAULT 0,
hrval5 REAL DEFAULT 0,
hrval6 REAL DEFAULT 0,
hrval7 REAL DEFAULT 0,
hrval8 REAL DEFAULT 0,
hrval9 REAL DEFAULT 0,
hrval10 REAL DEFAULT 0,
hrval11 REAL DEFAULT 0,
hrval12 REAL DEFAULT 0,
hrval13 REAL DEFAULT 0,
hrval14 REAL DEFAULT 0,
hrval15 REAL DEFAULT 0,
hrval16 REAL DEFAULT 0,
hrval17 REAL DEFAULT 0,
hrval18 REAL DEFAULT 0,
hrval19 REAL DEFAULT 0,
hrval20 REAL DEFAULT 0,
hrval21 REAL DEFAULT 0,
hrval22 REAL DEFAULT 0,
hrval23 REAL DEFAULT 0,
hrval24 REAL DEFAULT 0,
day_tot REAL DEFAULT 0,
scc TEXT);

-- Create all output tables for ERTAC EGU postprocessor.
DROP TABLE IF EXISTS ff10_hourly_future;
CREATE TABLE ff10_hourly_future
(
country TEXT NOT NULL COLLATE NOCASE,
fips TEXT NOT NULL COLLATE NOCASE,
tribalcode TEXT,
plantid TEXT,
pointid TEXT,
stackid TEXT,
segment TEXT,
scc TEXT,
polcode TEXT,
op_type_cd TEXT,
calc_method TEXT,
date_updated INTEGER,
op_date TEXT NOT NULL,
day_tot REAL DEFAULT 0,
hrval1 REAL DEFAULT 0,
hrval2 REAL DEFAULT 0,
hrval3 REAL DEFAULT 0,
hrval4 REAL DEFAULT 0,
hrval5 REAL DEFAULT 0,
hrval6 REAL DEFAULT 0,
hrval7 REAL DEFAULT 0,
hrval8 REAL DEFAULT 0,
hrval9 REAL DEFAULT 0,
hrval10 REAL DEFAULT 0,
hrval11 REAL DEFAULT 0,
hrval12 REAL DEFAULT 0,
hrval13 REAL DEFAULT 0,
hrval14 REAL DEFAULT 0,
hrval15 REAL DEFAULT 0,
hrval16 REAL DEFAULT 0,
hrval17 REAL DEFAULT 0,
hrval18 REAL DEFAULT 0,
hrval19 REAL DEFAULT 0,
hrval20 REAL DEFAULT 0,
hrval21 REAL DEFAULT 0,
hrval22 REAL DEFAULT 0,
hrval23 REAL DEFAULT 0,
hrval24 REAL DEFAULT 0,
comment text);


DROP TABLE IF EXISTS annual_summary;
CREATE TABLE annual_summary
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT COLLATE NOCASE,
state TEXT COLLATE NOCASE,
fips_code TEXT COLLATE NOCASE,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
by_ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
max_unit_heat_input REAL,
ertac_heat_rate REAL,
generation_capacity REAL,
nameplate_capacity REAL,
fy_op_hours INTEGER,
fy_op_hours_max INTEGER,
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
by_os_gload REAL,
by_non_os_nox_mass REAL,
by_non_os_nox_rate REAL,
fy_so2_mass REAL,
fy_so2_rate REAL,
fy_so2_max REAL,
fy_nox_mass REAL,
fy_nox_rate REAL,
fy_nox_max REAL,
fy_os_nox_mass REAL,
fy_os_nox_rate REAL,
fy_os_heat_input REAL,
fy_os_gload REAL,
fy_non_os_nox_mass REAL,
fy_non_os_nox_rate REAL,
hierarchy_order INTEGER,
longitude REAL,
latitude REAL,
gdu_flag TEXT,
retirement_date TEXT,
new_unit_flag TEXT,
data_type TEXT COLLATE NOCASE,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin,  orispl_code, unitid),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid));

DROP TABLE IF EXISTS annual_summary_with_other_pollutants;
CREATE TABLE annual_summary_with_other_pollutants
(orispl_code TEXT NOT NULL COLLATE NOCASE,
unitid TEXT NOT NULL COLLATE NOCASE,
facility_name TEXT COLLATE NOCASE,
state TEXT COLLATE NOCASE,
fips_code TEXT COLLATE NOCASE,
ertac_region TEXT NOT NULL COLLATE NOCASE,
ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
by_ertac_fuel_unit_type_bin TEXT NOT NULL COLLATE NOCASE,
max_unit_heat_input REAL,
ertac_heat_rate REAL,
generation_capacity REAL,
nameplate_capacity REAL,
fy_op_hours INTEGER,
fy_op_hours_max INTEGER,
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
by_os_gload REAL,
by_non_os_nox_mass REAL,
by_non_os_nox_rate REAL,
fy_so2_mass REAL,
fy_so2_rate REAL,
fy_so2_max REAL,
fy_nox_mass REAL,
fy_nox_rate REAL,
fy_nox_max REAL,
fy_os_nox_mass REAL,
fy_os_nox_rate REAL,
fy_os_heat_input REAL,
fy_os_gload REAL,
fy_non_os_nox_mass REAL,
fy_non_os_nox_rate REAL,
hierarchy_order INTEGER,
longitude REAL,
latitude REAL,
gdu_flag TEXT,
retirement_date TEXT,
new_unit_flag TEXT,
data_type TEXT COLLATE NOCASE,
fy_pm25_mass REAL,
fy_pm25_rate REAL,
fy_pm10_mass REAL,
fy_pm10_rate REAL,
fy_co_mass REAL,
fy_co_rate REAL,
fy_voc_mass REAL,
fy_voc_rate REAL,
fy_nh3_mass REAL,
fy_nh3_rate REAL,
fy_cl2_mass REAL,
fy_cl2_rate REAL,
fy_hcl_mass REAL,
fy_hcl_rate REAL,
PRIMARY KEY (ertac_region, ertac_fuel_unit_type_bin,  orispl_code, unitid),
UNIQUE (ertac_region, ertac_fuel_unit_type_bin, orispl_code, unitid));