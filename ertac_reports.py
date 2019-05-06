# ertac_reports.py

"""ERTAC EGU report characteristics"""

VERSION = "2.1.2"
# Updated to version 2.0c as of 9/3/2015.
# From 9/2/2015 call, add columns for demand transfer effects to the following:
# demand_generation_deficit
# reserve_capacity_needed
# capacity_and_fy_demand
# capacity_and_fy_reserve

# Similar to ertac_tables.py, these simpler definitions include only header text
# for CSV output, since we don't load or validate this data from external files.

demand_generation_deficit = (('ERTAC Region', None, None, None),
                             ('ERTAC Fuel/Unit Type Bin', None, None, None),
                             ('Calendar Hour', None, None, None),
                             ('Hierarchy Hour', None, None, None),
                             ('Generation Needed (MW-hrs)', None, None, None),
                             ('Generation Due to Demand Transfer (MW-hrs)', None, None, None),
                             ('Total Generation Needed (MW-hrs)', None, None, None),
                             ('Generation Available (MW-hrs)', None, None, None),
                             ('Lacking (MW-hrs)', None, None, None),
                             ('Available after new unit creation', None, None, None),
                             ('Deficit Flag', None, None, None),
                             ('Transfer Flag', None, None, None))

generic_units_created = (('ERTAC Region', None, None, None),
                         ('ERTAC Fuel/Unit Type Bin', None, None, None),
                         ('New Unit Size (MW)', None, None, None),
                         ('ORIS Location', None, None, None),
                         ('Unit ID', None, None, None),
                         ('Unit Location', None, None, None),
                         ('Unit Latitude', None, None, None),
                         ('Unit Longitude', None, None, None))

reserve_capacity_needed = (('ERTAC Region', None, None, None),
                           ('Calendar hour', None, None, None),
                           ('Hierarchy hour', None, None, None),
                           ('Pass/fail for reserve capacity requirements', None, None, None),
                           ('Reserve capacity needed', None, None, None),
                           ('Amount available without transfers', None, None, None),
                           ('Amount available including transfers', None, None, None),
                           ('Deficit (MW-hrs)', None, None, None))

# Added OS and non-OS heat rate based on 8/10/2015 call.
unit_level_activity = (('ORIS', None, None, None),
                       ('Unit ID', None, None, None),
                       ('Facility', None, None, None),
                       ('State', None, None, None),
                       ('ERTAC Region', None, None, None),
                       ('Fuel/Unit Type Bin', None, None, None),
                       ('Maximum hourly heat input (mmbtu)', None, None, None),
                       ('ERTAC heat rate (btu/kw-hr)', None, None, None),
                       ('OS heat rate (btu/kw-hr)', None, None, None),
                       ('Non-OS heat rate (btu/kw-hr)', None, None, None),
                       ('Generation capacity (MW)', None, None, None),
                       ('# of hours in FY where unit operated at max hourly', None, None, None),
                       ('Utilization fraction', None, None, None),
                       ('Base year generation (MW-hrs)', None, None, None),
                       ('Base year heat input (mmbtu)', None, None, None),
                       ("Base year hours op'd", None, None, None),
                       ('Future year generation (MW-hrs)', None, None, None),
                       ('Future year heat input (mmbtu)', None, None, None),
                       ("Future year hours op'd", None, None, None))

cap_analysis = (('State/Region Cap', None, None, None),
                ('Cap type/Pollutant', None, None, None),
                ('Cap amount', None, None, None),
                ('Year Applicable', None, None, None),
                ('FY Emissions, no program generated control', None, None, None),
                ('FY Emissions, all program generated control', None, None, None),
                ('Cap Comments, if any', None, None, None))

unit_generic_controls = (('ORIS', None, None, None),
                         ('Facility Name', None, None, None),
                         ('Unit ID', None, None, None),
                         ('Generation Capacity (MW)', None, None, None),
                         ('Unit Age', None, None, None),
                         ('ERTAC Region', None, None, None),
                         ('ERTAC Fuel/Unit Type bin', None, None, None),
                         ('Pollutant', None, None, None),
                         ('Base year emissions rate (lbs/mmbtu)', None, None, None),
                         ('Future year emissions rate (lbs/mmbtu)', None, None, None),
                         ('Base year emissions (tons/yr or OS)', None, None, None),
                         ('Future year emissions (tons/yr or OS)', None, None, None))

capacity_and_fy_demand = (('Region', None, None, None),
                          ('Bin', None, None, None),
                          ('Annual BY gen (MW-hrs)', None, None, None),
                          ('Annual BY HI (mmbtu)', None, None, None),
                          ('Annual FY gen (MW-hrs) (including transfers)', None, None, None),
                          ('Annual FY HI (mmbtu)', None, None, None),
                          ('Annual FY transfers (MW-hrs)', None, None, None),
                          ('Sum of new generation created (MW)', None, None, None))

capacity_and_fy_reserve = (('Region', None, None, None),
                           ('Total transfers (MW-hrs)', None, None, None),
                           ('Reserve capacity met?', None, None, None),
                           ('Max amount needed (MW)', None, None, None))

state_caps = (('State', None, None, None),
              ('Cap Pollutant', None, None, None),
              ('Cap (TPY or T/OS)', None, None, None),
              ('Cap Year', None, None, None),
              ('FY Emissions', None, None, None),
              ('Comments', None, None, None))

group_caps = (('Group', None, None, None),
              ('Cap Pollutant', None, None, None),
              ('Cap (TPY or T/OS)', None, None, None),
              ('Cap Year', None, None, None),
              ('FY Emissions', None, None, None),
              ('Comments', None, None, None))

# Additional flag columns based on 8/10/2015 call for period types and limits for heat rate and emission factors.
# CO2 columns added for potential future use after 8/20/2015 call.
hourly_diagnostic_file = (('ERTAC Region', None, None, None),
                          ('ERTAC Fuel/Unit Type Bin', None, None, None),
                          ('State', None, None, None),
                          ('ORIS', None, None, None),
                          ('UNIT ID', None, None, None),
                          ('Operating Hour', None, None, None),
                          ('Hierarchy Hour', None, None, None),
                          ('Did the hour hit the hourly heat input limitation for the unit?', None, None, None),
                          ('Has the cumulative heat input hit an annual cap?', None, None, None),
                          ('Did the hour hit the hourly no of op hour limit for the unit?', None, None, None), #jmj 3/9/2017 added for op hour cap implementation
                          ('Cumulative HI (mmBtu)', None, None, None),
                          ('Cumulative Gen (MW-hrs)', None, None, None),
                          ('Cumulative Operating Hours', None, None, None), # Suggested 7/15/2015 by Joseph
                          ('Gross Load (MW-hr)', None, None, None),
                          ('Heat Input (mmBtu)', None, None, None),
                          ('Heat Rate (btu/kw-hr)', None, None, None),
                          ('Heat Rate Period Type', None, None, None),
                          ('Heat Rate Limit Flag', None, None, None),
                          ('SO2_mass (lbs)', None, None, None),
                          ('SO2_rate (lbs/mmBtu)', None, None, None),
                          ('SO2 Period Type', None, None, None),
                          ('SO2 Limit Flag', None, None, None),
                          ('NOX_rate (lbs/mmBtu)', None, None, None),
                          ('NOX_mass (lbs)', None, None, None),
                          ('NOx Period Type', None, None, None),
                          ('NOx Limit Flag', None, None, None),
                          ('CO2_mass (tons)', None, None, None),
                          ('CO2_rate (tons/mmBtu)', None, None, None),
                          ('CO2 Period Type', None, None, None),
                          ('CO2 Limit Flag', None, None, None))
