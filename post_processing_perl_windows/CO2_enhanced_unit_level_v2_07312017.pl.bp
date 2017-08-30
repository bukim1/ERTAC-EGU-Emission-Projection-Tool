
# 07/31/2017: revise the CO2 postprocessing script to work for code 2.1; 
# many of version2.1 outputs have changed; any code outputs marked with v2 and 
# required for this script will need changes
# header of p_calc_updated_uaf_v2.txt has not changed;
# header of p_hourly_diagnostic_file_v2.txt changed the most;
# header of p_unit_level_activity_v2.txt has two extra columns;

# 12/10/2014: Doris wanted to make "gas" a separate fuel all by itself with a factor
# of 116.39 lbs/mmbtu; She sent me updated flowchart indicating where to make changes

# convert small letters in unitID to capital letters (Doris noticed a problem with 
# 2039-erps11 unit whose online/offline dates did not show up in enhanced outputs

# ********************* heat-input based BY/FY CO2 estimates ***********************
# INfolder should have no trailing slash;
# usage: perl CO2_enhanced_unit_level.pl < dummy.txt projection_year /deq/OTC/models/ERTAC/BY_2011_CONUS-v2.1-v1.0/REG2_fictitious/outputs REG2_fictitious

# remember to change future projection year so that retirement year can be determined!!

my $projection_year = $ARGV[0];
my $INfolder = $ARGV[1];
my $OUTfile  = qw(cases/) . $ARGV[2] . "_p_unit_level_CO2_addon_enhanced.csv";

print "input folder is ... $INfolder\n";
print "output FILE is ... $OUTfile\n";

my $INfile1 = `ls $INfolder/*_p_calc_updated_uaf_v2.csv`;         # use v2 output
my $INfile2 = `ls $INfolder/*calc_hourly_base.csv`;
my $INfile3 = `ls $INfolder/*_p_hourly_diagnostic_file_v2.csv`;   # use v2 output
my $INfile4 = `ls $INfolder/*_p_unit_level_activity_v2.csv`;      # use v2 output
my $INfile5 = `ls $INfolder/*_p_calc_unit_hierarchy.csv`;        

$INfile1 =~ s/\n//g;
$INfile2 =~ s/\n//g;
$INfile3 =~ s/\n//g;
$INfile4 =~ s/\n//g;
$INfile5 =~ s/\n//g;

print "$INfile1\n";
print "$INfile2\n";
print "$INfile3\n";
print "$INfile4\n";
print "$INfile5\n";

# ************************** earlier version ***************************

open(OUT, ">$OUTfile");
#open(OUT2, ">$OUTfile2");

my @header = qw(ORIS,Unit ID,Facility,State,ERTAC Region,Fuel/Unit Type Bin,Maximum hourly heat input (mmbtu),ERTAC heat rate (btu/kw-hr),Generation capacity (MW),num of hours in FY where unit operated at max hourly,BY Utilization fraction,FY Utilization fraction,Base year generation (MW-hrs),Base year heat input (mmbtu),BY_CO2_tons,BY_CO2_lbs_mmbtu,BY_CO2_lbs_MWhr,Future year generation (MW-hrs),Future year heat input (mmbtu),FY_CO2_tons,FY_CO2_lbs_mmbtu,FY_CO2_lbs_MWhr,BY_SO2_lbs_mmbtu,FY_SO2_lbs_mmbtu,Primary_Fuel_Type,New Unit?,Generation Deficit Unit?,Gasified?,Unit_Online_Start_date,Retirement Date,Longitude,Latitude,Is Unit NSPS Applicable?);

print OUT "@header\n";
#print OUT2 "@header\n";

# ****************** updated UAF after the preprocessing *********************
# ********************* for extracting calculated_BY_UF **********************

open(IN1, "<$INfile1");

%UAF;
%FuelSwitch;
%FuelSwInfo;

while (<IN1>) {

    chomp;            # to delete \n or <LF>
    s/\cM//;          # to delete <CR>

    next if (/ertac_region/);

#    my @array = split(",", $_);

    my @Args = split(",",$_);
    my @array;

    while (@Args) {
        my $a = shift(@Args);
    if ($a =~ /^"(.*)/) {
      $a = $1;
      $a .= ',' . shift(@Args);
      while (not $a =~ /(.*)"/) {
        $a .= ',' . shift(@Args); }
      $a =~ s/"$//;
    } # end of if
      push(@array,$a);
    } # end of while

    my $orispl_code              = $array[0];
    my $unitid                   = $array[1];

    my $plant_latitude           = $array[9];
    my $plant_longitude          = $array[10];
    my $facility_name            = $array[12];

    my $ertac_region             = $array[16];
    my $BY_camd_hourly_data_type = $array[18];
    my $online_start_date        = $array[22];
    my $offline_start_date       = $array[23];
    my $primary_fuel_type        = $array[24];
    my $ertac_fuel_unit_type_bin = $array[29];
    my $max_ertac_heat_input_hourly_summer = $array[30];

    my $calculated_BY_UF         = $array[37];
    my $max_annual_ertac_UF_state_input = $array[38];
    my $max_annual_ertac_UF      = $array[39];
    my $ertac_heat_rate          = $array[45];
    my $new_unit_flag            = $array[53];

# convert everything to small letters first

    $ertac_fuel_unit_type_bin =~ y/A-Z/a-z/;    
    $primary_fuel_type =~ y/A-Z/a-z/;    

    $unitid =~ y/a-z/A-Z/;    # added on 11/25/2014

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{UF} 
        = $calculated_BY_UF;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{Lat} 
        = $plant_latitude;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{Long} 
        = $plant_longitude;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{RetireDate} 
        = $offline_start_date;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{PrimaryFuel} 
        = $primary_fuel_type;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{StartDate} 
        = $online_start_date;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{NewUnitQ} 
        = $new_unit_flag;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{HIHourlySum} 
        = $max_ertac_heat_input_hourly_summer; 

    if (!defined($FuelSwitch{$orispl_code}{$unitid})) {

        $FuelSwitch{$orispl_code}{$unitid} = "N";
        $FuelSwInfo{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid} = 
                   $BY_camd_hourly_data_type; }

    else {   # if defined....

        $FuelSwitch{$orispl_code}{$unitid} = "Y";  # as long as combo is repeated, 
                                                   # N will be overwritten by Y
        $FuelSwInfo{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid} = 
                   $BY_camd_hourly_data_type; }

}

close(IN1);

#****************** CAMD data *************************

# CAMD is for the base year, and Proj is for the future year!
# CAMD data is based on output AFTER pre-processing, when zero or bad
# data have already been removed; therefore, it contains fewer units
# than hourly diagnostic file!
# however, to be able to include future year data, units in hourly
# diagonistic file should be used;

my @Julian = qw(31 59 90 120 151 181
                212 243 273 304 334 365);

my @days_of_month = qw(31 28 31 30 31 30
                       31 31 30 31 30 31);

#****************** CAMD data *************************

#my %CAMD;      # hourly data   # no use for this script
my %BY;        # annual sum

open(IN2, "<$INfile2");

while (<IN2>) {

    chomp;            # to delete \n or <LF>
    s/\cM//;          # to delete <CR>

    next if (/so2_rate/);

#     my @array = split(",", $_);

    my @Args = split(",",$_);
    my @array;

    while (@Args) {
        my $a = shift(@Args);
    if ($a =~ /^"(.*)/) {
      $a = $1;
      $a .= ',' . shift(@Args);
      while (not $a =~ /(.*)"/) {
        $a .= ',' . shift(@Args); }
      $a =~ s/"$//;
    } # end of if
      push(@array,$a);
    } # end of while
    
     my $ertac_region = $array[0];
     my $ertac_fuelbin = $array[1];
     my $state = $array[2];
     my $facility_name = $array[3];
     my $orispl_code = $array[4];
     my $unitid = $array[5];
     my $op_date = $array[6];
     my $op_hour = $array[7];
     my $op_time = $array[8];
     my $gload  = $array[9];
     my $sload  = $array[10];
     my $so2_mass  = $array[11];
     my $so2_mass_measure_flg = $array[12];
     my $so2_rate  = $array[13];
     my $so2_rate_measure_flg = $array[14];
     my $nox_rate  = $array[15];
     my $nox_rate_measure_flg = $array[16];
     my $nox_mass  = $array[17];
     my $nox_mass_measure_flg = $array[18];
     my $co2_mass  = $array[19];
     my $co2_mass_measure_flg = $array[20];
     my $co2_rate  = $array[21];
     my $co2_rate_measure_flg = $array[22];
     my $heat_input = $array[23];

# convert everything to small letters first (very important for consistency!)

     $ertac_fuelbin =~ y/A-Z/a-z/;    

     $unitid =~ y/a-z/A-Z/;    # added on 11/25/2014

# ************ base year hourly activity data ***********************

#    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{Gload} = $gload;
#    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{HI}    = $heat_input;     

#    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{SO2} = $so2_mass;     
#    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{NOX} = $nox_mass;     

# ************ base year annual summation ***********************

    if (!defined($BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{CO2})) {
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{CO2} = $co2_mass; }
    else { 
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{CO2} = 
        $BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{CO2} + $co2_mass; }

    if (!defined($BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX})) {
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} = $nox_mass; }
    else { 
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} = 
        $BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} + $nox_mass; }

    if (!defined($BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2})) {
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} = $so2_mass; }
    else { 
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} = 
        $BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} + $so2_mass; }

    if (!defined($BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{Gload})) {
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{Gload} = $gload; }
    else { 
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{Gload} = 
        $BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{Gload} + $gload; }

    if (!defined($BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{Sload})) {
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{Sload} = $sload; }
    else { 
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{Sload} = 
        $BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{Sload} + $sload; }

    if (!defined($BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI})) {
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} = $heat_input; }
    else { 
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} = 
        $BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} + $heat_input; }

} # end of while loop          

close(IN2);

#****************** Hourly diagnostic *************************

#my %Proj;      # hourly data
my %FY;        # annual sum

open(IN3, "<$INfile3");

while (<IN3>) {

    chomp;            # to delete \n or <LF>
    s/\cM//;          # to delete <CR>

#    next if (/Cumulative Gen (MW-hrs)/);
    next if (/Cumulative Gen/);

    my @array = split(",", $_);

    my $Region = $array[0];
    my $FuelBin = $array[1];
    my $State = $array[2];
    my $ORIS = $array[3];
    my $UNIT_ID = $array[4];
    my $Op_Hour = $array[5];
    my $Hierarchy_Hour = $array[6];
    my $limit = $array[7];
    my $annual_cap = $array[8];

    my $Cumulative_HI = $array[10];
    my $Cumulative_Gen = $array[11];

    my $Gross_Load = $array[13];
    my $Heat_Input  = $array[14];

    my $SO2_mass  = $array[18];
    my $SO2_rate  = $array[19];
    my $NOX_rate  = $array[22];
    my $NOX_mass  = $array[23];

# convert everything to small letters first

    $FuelBin =~ y/A-Z/a-z/;    

    $UNIT_ID =~ y/a-z/A-Z/;    # added on 11/25/2014

# ************ future year annual summation ***********************

    if (!defined($FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI})) {
	$FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} = $Heat_Input; }
    else { 
	$FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} = 
        $FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} + $Heat_Input; }

    if (!defined($FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX})) {
	$FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} = $NOX_mass; }
    else { 
	$FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} = 
        $FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} + $NOX_mass; }

    if (!defined($FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2})) {
	$FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} = $SO2_mass; }
    else { 
	$FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} = 
        $FY{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} + $SO2_mass; }

} # end of while loop;

close(IN3);

# ********************* unit hierarchy after the projection **********************

my %V;  # store ranking or allocation order

open(IN5, "<$INfile5");

while (<IN5>) {

    chomp;            # to delete \n or <LF>
    s/\cM//;          # to delete <CR>

    next if (/ertac_region/);

    my @array = split(",", $_);

    my $ertac_region = $array[0];
    my $ertac_fuelbin = $array[1];
    my $orispl_code = $array[2];
    my $unitid = $array[3];
    my $unit_allocation_order = $array[4];
    my $submitter_email = $array[5];
    my $state = $array[6];

    $ertac_fuelbin =~ y/A-Z/a-z/;    

    $unitid =~ y/a-z/A-Z/;    # added on 11/25/2014

    $V{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid} = $unit_allocation_order;
}

close(IN5);

# *********************  p_unit_level_activity.csv after projection **********************

open(IN4, "<$INfile4");

while (<IN4>) {

    chomp;            # to delete \n or <LF>
    s/\cM//;          # to delete <CR>

    next if (/ERTAC Region/);

#    my @array = split(",", $_);

    my @Args = split(",",$_);
    my @array;

    while (@Args) {
        my $a = shift(@Args);
    if ($a =~ /^"(.*)/) {
      $a = $1;
      $a .= ',' . shift(@Args);
      while (not $a =~ /(.*)"/) {
        $a .= ',' . shift(@Args); }
      $a =~ s/"$//;
    } # end of if
      push(@array,$a);
    } # end of while

    my $ORIS                 = $array[0];
    my $UnitID               = $array[1];
    my $Facility             = $array[2];
    my $State                = $array[3];
    my $ERTAC_Region         = $array[4];
    my $Fuel_Unit_Type_Bin   = $array[5];
    my $Maximum_hourly_heat_input = $array[6];
    my $ERTAC_heat_rate      = $array[7];

    my $OS_heat_rate         = $array[8];   # new column in v2
    my $Non_OS_heat_rate     = $array[9];   # new column in v2

    my $Generation_capacity  = $array[10];
    my $hours_in_FY_where_unit_operated_at_max_hourly = $array[11];
 
# convert everything to small letters first

    $Fuel_Unit_Type_Bin =~ y/A-Z/a-z/;    

    $UnitID =~ y/a-z/A-Z/;    # added on 11/25/2014

#    $array[5] = $Fuel_Unit_Type_Bin;    # override array[5]

    my $Utilization_fraction = $array[12];   # this is future UF
    my $Base_year_generation = $array[13];
    my $Base_year_heat_input = $array[14];
    my $Base_year_hours_oped = $array[15];
    my $Future_year_generation  = $array[16];
    my $Future_year_heat_input  = $array[17];
    my $Future_year_hours_oped  = $array[18];

    $Facility =~ s/\,//g;      # get rid of comma for EMF

# *************** check whether a unit is gasified *****************

    if ($FuelSwitch{$ORIS}{$UnitID} eq "Y") {

       if ( ($Fuel_Unit_Type_Bin eq "boiler gas") || ($Fuel_Unit_Type_Bin eq "coal") ) {
           $Gasified = "Y"; }
       else {
           $Gasified = ""; }
    }
    else {
           $Gasified = ""; }

# *************** check whether a unit is generic or not *****************

    if ( (substr($UnitID,0,1) =~ /G/)     && (substr($UnitID,1,1) =~ /[0-9]/) && 
         (substr($UnitID,2,1) =~ /[0-9]/) && (substr($UnitID,3,1) =~ /[0-9]/) &&
         (substr($UnitID,4,1) =~ /[0-9]/) && (substr($UnitID,5,1) =~ /[0-9]/) ) {   
          $Generic = "Y"; }
    else { $Generic = "N"; }

    my ($on_year, $on_month, $on_date) = split("\-", 
                             $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{StartDate});

    my $numerical_year = sprintf("%4d", $on_year);
    my $numerical_mon  = sprintf("%2d", $on_month);

    if ($Generic eq "Y") {$numerical_year = $projection_year;}    # must eq in if, = messes things up!!

    my ($off_year, $off_month, $off_date) = split("\-", 
                             $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{RetireDate});

    my $off_numerical_year = sprintf("%4d", $off_year);
    my $off_numerical_mon  = sprintf("%2d", $off_month);

# *************** check whether a unit is NSPS applicable *****************

    if ( ($numerical_year >= 2015) && ($Gasified eq "") ) {
          $NSPS = "Y"; }
    else {
          $NSPS = "N"; }

# **************************** calcuations begin ****************************
# ************************* setting up basic calculation ********************
# ************************* BY calculations for all units *******************
# BY sequence: (1) tons/year, (2) lbs/mmbtu, and (3) lbs/MWhr

    if ($BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{CO2} == 0.0) {
        $BY_CO2_tons = "NR"; }
    else {
        $BY_CO2_tons = $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{CO2}; }

    if ($BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} == 0.0) {
        $BY_CO2_lbs_mmbtu = "NR"; 
        $BY_SO2_lbs_mmbtu = "NR"; }
    else {
        $BY_CO2_lbs_mmbtu = $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{CO2} * 2000.0 /
                            $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; 

        $BY_SO2_lbs_mmbtu = $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} /
                            $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; }

    if ($BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Gload} == 0.0) {
        $BY_CO2_lbs_MWhr = "NR"; }
    else {
        $BY_CO2_lbs_MWhr = $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{CO2} * 2000.0 /
                           $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Gload}; }

# base year data for generic units should all be NA (not applicable)

    if ( ($Generic eq "Y") || 
         ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NewUnitQ} eq "Y") ) {

        $BY_CO2_tons = "NA"; 
        $BY_CO2_lbs_mmbtu = "NA"; 
        $BY_SO2_lbs_mmbtu = "NA"; 
        $BY_CO2_lbs_MWhr = "NA"; }

# ****************************** NR (not reported) units ************************
# *********** no CO2 tons/hr, but has heat inputs for the hours *****************
# units with missing data need a way of estimating its base year activities

    if ( ($BY_CO2_tons eq "NR") && ($BY_CO2_lbs_mmbtu ne "NR") ) {

       if ( ($Fuel_Unit_Type_Bin eq "oil") && (
            ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} eq "residual oil") ||
            ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} eq "other oil")) ) {
  
            $BY_CO2_lbs_mmbtu = 171.98; }

       elsif ( ($Fuel_Unit_Type_Bin eq "oil") && (
            ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} ne "residual oil") &&
            ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} ne "other oil")) ) {

            $BY_CO2_lbs_mmbtu = 159.66; }

       elsif ( ($Fuel_Unit_Type_Bin eq "combined cycle gas") || 
               ($Fuel_Unit_Type_Bin eq "simple cycle gas") ||
               ($Fuel_Unit_Type_Bin eq "boiler gas") ||  
               ($Fuel_Unit_Type_Bin eq "gas") )  {

 	    $BY_CO2_lbs_mmbtu = 116.39; }

       else {             # ($Fuel_Unit_Type_Bin eq "coal") {
  
            $BY_CO2_lbs_mmbtu = 211.91; }

# after getting out of unit type bin in which HI-based CO2 have become available, 
# calculate other parameters

       $BY_CO2_tons = $BY_CO2_lbs_mmbtu * 
                      $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} / 2000.0; 

       if ($BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Gload} == 0.0) {
           $BY_CO2_lbs_MWhr = "NR"; }
       else {
           $BY_CO2_lbs_MWhr = $BY_CO2_tons * 2000.0 / 
                              $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Gload}; }

    } # end of loop of ($BY_CO2_tons == "NR") && ($BY_CO2_lbs_mmbtu ne "NR") loop

# ************************* FY calculations begin here ****************************
# ****************** setting up or a prelude for actual calculations **************

    if ($FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} == 0.0 ) {
        $FY_SO2_lbs_mmbtu = "NR"; }
    else {
        $FY_SO2_lbs_mmbtu = $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} /
                            $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; }

# MEHIHS = maximum ertac HI hourly summer, obtained from UAF and its unit is mmbtu/hr

    my $MEHIHS = $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HIHourlySum};

# unit for the following conversion1 and conversion2 is lbs/mmbtu, 
# they will be used as $FY_CO2_lbs_mmbtu for new units

    my $conversion1 = (1000.0 * 1000.0) / $ERTAC_heat_rate;   # 1000.0 is lbs/MW-hr
    my $conversion2 = (1100.0 * 1000.0) / $ERTAC_heat_rate;   # 1100.0 is lbs/MW-hr

# ******************* calculating three lesser numbers *********************

    if ($conversion1 >= 159.66) {               # for MEHIHS higher than 850 mmbtu/hr
       $lesser_oil_hi = 159.66; }             
    else {
       $lesser_oil_hi = $conversion1; }

    if ($conversion2 >= 159.66) {               # for MEHIHS lower than 850 mmbtu/hr
       $lesser_oil_lo = 159.66; }              
    else {
       $lesser_oil_lo = $conversion2; }         

    if ($conversion1 >= 116.36) {               # for MEHIHS higher than 850 mmbtu/hr
       $lesser_gas_hi = 116.36; }             
    else {
       $lesser_gas_hi = $conversion1; }

    if ($conversion2 >= 116.36) {               # for MEHIHS lower than 850 mmbtu/hr
       $lesser_gas_lo = 116.36; }              
    else {
       $lesser_gas_lo = $conversion2; }         

# **************************** FY calculations *********************************
# ******************************** New Units ***********************************

    if ( ( ($Generic eq "Y") ||
           ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NewUnitQ} eq "Y") ||
           ($FuelSwInfo{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID} eq "NEW") ) &&
         (exempt_plants($ORIS,$UnitID) ne "1") ) { # equal 1 means defined (on the  list), 
                                                   # on the list means exempt, 
                                                   # exempt means it belongs to existing

        if ($numerical_year >= 2015) {    # later than 2015

           if ($Gasified eq "Y") {
               $FY_CO2_lbs_mmbtu = 116.39; }

           elsif ($Fuel_Unit_Type_Bin eq "coal") {
               $FY_CO2_lbs_mmbtu = $conversion2; }

           elsif ($Fuel_Unit_Type_Bin eq "boiler gas") {
               $FY_CO2_lbs_mmbtu = $conversion2; }

           elsif ($Fuel_Unit_Type_Bin eq "simple cycle gas") {

               if ( $MEHIHS >= 850.0 ) {
                    $FY_CO2_lbs_mmbtu = $lesser_gas_hi; }
               else {
                    $FY_CO2_lbs_mmbtu = $lesser_gas_lo; } }

           elsif ($Fuel_Unit_Type_Bin eq "combined cycle gas") {

               if ( $MEHIHS >= 850.0 ) {
                    $FY_CO2_lbs_mmbtu = $lesser_gas_hi; }
               else {
                    $FY_CO2_lbs_mmbtu = $lesser_gas_lo; } }

           else {                     # oil unit

               if ( $MEHIHS >= 850.0 ) {
                    $FY_CO2_lbs_mmbtu = $lesser_oil_hi; }
               else {
                    $FY_CO2_lbs_mmbtu = $lesser_oil_lo; } }

        } # end of later than 2014 loop
         
        else {             # earlier than 2015

           if ($Fuel_Unit_Type_Bin eq "coal") {
               $FY_CO2_lbs_mmbtu = 211.91; }

           elsif ($Fuel_Unit_Type_Bin eq "gas") {
               $FY_CO2_lbs_mmbtu = 116.39; }

           elsif ($Fuel_Unit_Type_Bin eq "boiler gas") {
               $FY_CO2_lbs_mmbtu = 116.39; }

           elsif ($Fuel_Unit_Type_Bin eq "simple cycle gas") {
               $FY_CO2_lbs_mmbtu = 116.39; }

           elsif ($Fuel_Unit_Type_Bin eq "combined cycle gas") {
               $FY_CO2_lbs_mmbtu = 116.39; }

           else {                     # oil unit
               $FY_CO2_lbs_mmbtu = 159.66; }

        } # end of earlier than 2014 loop

    }  # end of generic / new unit loop

# **************************** FY calculations *********************************
# ***************************** Existing Units *********************************

   else {     # existing unit begins

       if ( ($BY_CO2_lbs_mmbtu eq "NR") && ($Fuel_Unit_Type_Bin eq "oil") && (
            ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} eq "residual oil") ||
            ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} eq "other oil")) ) {
  
            $FY_CO2_lbs_mmbtu = 171.98; }

       elsif ( ($BY_CO2_lbs_mmbtu eq "NR") && ($Fuel_Unit_Type_Bin eq "oil") && (
            ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} ne "residual oil") &&
            ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} ne "other oil")) ) {

            $FY_CO2_lbs_mmbtu = 159.66; }

       elsif (($BY_CO2_lbs_mmbtu >= 75.0) && ($BY_CO2_lbs_mmbtu <= 300.0)) {  # inside range

          if ($Fuel_Unit_Type_Bin eq "coal") {

               if ($FY_SO2_lbs_mmbtu >= (0.7 * $BY_SO2_lbs_mmbtu) ) { # no DSI or FGD is installed

                   $FY_CO2_lbs_mmbtu = $BY_CO2_lbs_mmbtu; }

               else {                                                # DSI or FGD is installed

                   $FY_CO2_lbs_mmbtu = 1.02 * $BY_CO2_lbs_mmbtu; } 

          } # end of if

          else {

               $FY_CO2_lbs_mmbtu = $BY_CO2_lbs_mmbtu; }

       } # end of if

       else {    # outside 75.0 < $BY_CO2_lbs_mmbtu < 300.0 range

          if ( ($Fuel_Unit_Type_Bin eq "oil") && (
               ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} eq "residual oil") ||
               ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} eq "other oil")) ) {
  
               $FY_CO2_lbs_mmbtu = 171.98; }

          elsif ( ($Fuel_Unit_Type_Bin eq "oil") && (
                 ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} ne "residual oil") &&
                 ($UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel} ne "other oil")) ) {

               $FY_CO2_lbs_mmbtu = 159.66; }

          elsif ( ($Fuel_Unit_Type_Bin eq "combined cycle gas") || 
                  ($Fuel_Unit_Type_Bin eq "simple cycle gas") ||
                  ($Fuel_Unit_Type_Bin eq "boiler gas") || 
                  ($Fuel_Unit_Type_Bin eq "gas") ) {

               $FY_CO2_lbs_mmbtu = 116.39; } 

          else {             # ($Fuel_Unit_Type_Bin eq "coal") {
  
 	       $FY_CO2_lbs_mmbtu = 211.91; }

       }  # end of else  # end of out of range

   } # end of existing units

# **************************** FY calculations *********************************
# *********************** FY CO2_lbs_MWhr for all units ************************

   $FY_CO2_lbs_MWhr = $FY_CO2_lbs_mmbtu * $ERTAC_heat_rate / 1000.0; 

   $FY_CO2_tons     = $FY_CO2_lbs_MWhr * $Future_year_generation / 2000.0;

# **************************** FY calculations *********************************
# ** section to overwrite the above two lines $FY_CO2_lbs_MWhr, $FY_CO2_tons ***
# ***************************** Retiring Units *********************************
# screen out retiring units (i.e., prior to 2018) and fuel switch, 
# but blank (or 0) should be counted in else below
# years earlier than 2018 will be considered retiring 
# mid-year retiring such as 2017-05-01 will be considered active

   if ( ($off_numerical_year < $projection_year) && ($off_numerical_year != 0) ) {  

        $FY_CO2_tons = "NA"; 
        $FY_CO2_lbs_mmbtu = "NA"; 
        $FY_SO2_lbs_mmbtu = "NA"; 
        $FY_CO2_lbs_MWhr = "NA"; 
#        next;  # out of loop and move on to the next unit, 
                # this does not work, b/c must go through printf

   }  # end of retiring units loop

 # ********************************** print ***********************************

   printf OUT ("%s," x 33 . "\n", 
          $ORIS, $UnitID,
          $Facility, $State, 
          $ERTAC_Region, $Fuel_Unit_Type_Bin,
          $Maximum_hourly_heat_input,
          $ERTAC_heat_rate, 
          $Generation_capacity,
          $hours_in_FY_where_unit_operated_at_max_hourly,
          $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{UF},
          $Utilization_fraction,
          $Base_year_generation,
          $Base_year_heat_input,
          $BY_CO2_tons, $BY_CO2_lbs_mmbtu, $BY_CO2_lbs_MWhr, 
          $Future_year_generation,
          $Future_year_heat_input,
          $FY_CO2_tons, $FY_CO2_lbs_mmbtu, $FY_CO2_lbs_MWhr,
          $BY_SO2_lbs_mmbtu, $FY_SO2_lbs_mmbtu, 
          $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{PrimaryFuel},
          $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NewUnitQ},
          $Generic, $Gasified,
          $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{StartDate},
          $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{RetireDate},
          $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Long},
          $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Lat},
          $NSPS );

#          $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HIHourlySum},            
#          $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Gload},

} # end of while loop loop

close(IN4);

# ************* subroutine for SCC description *****************

#my $response = exempt_plants(55856,01);  # returns 1
#my $response = exempt_plants(72,6);      # returns empty string
#print "$response\n";

sub exempt_plants {

my $oris  = $_[0];
my $unit  = $_[1];

$Expt{1004}{CTG1} = "Edwardsport";
$Expt{2721}{6}    = "Cliffside";
$Expt{55856}{01}  = "Prairie State Generating Company";
$Expt{55856}{02}  = "Prairie State Generating Company";
$Expt{56611}{S01} = "Sandy creek";
$Expt{56671}{001} = "Longview Power";
$Expt{56186}{1}   = "Spiritwood Station";
$Expt{56808}{1}   = "Virginia City Hybrid energy Center";
$Expt{56808}{2}   = "Virginia City Hybrid energy Center";
$Expt{6195}{2}    = "John Twitty Energy Center";

my $YN = defined($Expt{$oris}{$unit});

return $YN;

} # end of subroutine




