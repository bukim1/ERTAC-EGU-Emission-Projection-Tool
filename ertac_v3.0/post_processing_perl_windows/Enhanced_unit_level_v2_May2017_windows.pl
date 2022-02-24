
# Post-processing script for ERTAC version2.1 code
# contact: Jin-Sheng.Lin@deq.virginia.gov
#
# Note that perl packages are needed to be able to run this script.  Most machines 
# usually have perl already installed.  If not, ask your system administrator for help.
# To use this script, please follow the steps below:
#
# (1) Edit the "inputs.txt" file (in the package downloaded).  
#
#     There are three lines to be edited:  
#     (a) The first line is the folder where the ERTAC EGU version2.1 outputs are located.  
#     (b) The second line is an existing folder where you would like the enhanced file (output) 
#         from this script to be located.  
#     (c) The third line is a file identifier with which you would like the enhanced output to be 
#         associated.  
#         For example, an identifier CONUS-v2.0 will give you an enhanced file output of
#         CONUS-v2-0_p_unit_level_activity_enhanced_v2.csv.
#
# (2) Bring up cdm box on Windows.
# (3) Change to the folder to where the perl script and inputs.txt are located.
# (4) Type "Enhanced_unit_level_v2_May2017_windows.pl < inputs.txt" on the prompt.
#
# If everything goes well, an enhanced file should be in the output folder specified in the 
# second line of the inputs.txt.

my @INarray;

open(INput, "<inputs.txt");

$INarray[0] = (<INput>);
$INarray[1] = (<INput>);
$INarray[2] = (<INput>);

close(INput);

for my $i (0..$#INarray) {

    $INarray[$i] =~ s/\cM//; 
    $INarray[$i] =~ s/\n//; 
    $INarray[$i] =~ s/ *$//;     # space at the beginning
    $INarray[$i] =~ s/^ *//; }   # space at the end

my $INfolder = $INarray[0];
my $OUTfile  = $INarray[1] . "/" . $INarray[2] . 
    "_p_unit_level_activity_enhanced_v2.csv";

print " input folder is ... $INfolder\n";
print "output FILE is ... $OUTfile\n";

my $IN1_pattern = "$INfolder/*_p_calc_updated_uaf_v2.csv";        # use v2 output 
my $IN2_pattern = "$INfolder/*calc_hourly_base.csv";
my $IN3_pattern = "$INfolder/*_p_hourly_diagnostic_file_v2.csv";  # use v2 output 
my $IN4_pattern = "$INfolder/*_p_unit_level_activity_v2.csv";     # use v2 output 
my $IN5_pattern = "$INfolder/*_p_calc_unit_hierarchy.csv";

for my $INfl (glob ("$IN1_pattern")) { $INfile1 = $INfl; }
for my $INfl (glob ("$IN2_pattern")) { $INfile2 = $INfl; }
for my $INfl (glob ("$IN3_pattern")) { $INfile3 = $INfl; }
for my $INfl (glob ("$IN4_pattern")) { $INfile4 = $INfl; }
for my $INfl (glob ("$IN5_pattern")) { $INfile5 = $INfl; }

print "$INfile1\n";
print "$INfile2\n";
print "$INfile3\n";
print "$INfile4\n";
print "$INfile5\n";

# ************************** earlier version ***************************

open(OUT, ">$OUTfile");

my @header = qw(ORIS,Unit ID,Facility,State,ERTAC Region,Fuel/Unit Type Bin,Maximum hourly heat input (mmbtu),ERTAC heat rate (btu/kw-hr),OS heat rate (btu/kw-hr),Non-OS heat rate (btu/kw-hr),Generation capacity (MW),num of hours in FY where unit operated at max hourly,BY Utilization fraction,FY Utilization fraction,Base year generation (MW-hrs),Base year heat input (mmbtu),Future year generation (MW-hrs),Future year heat input (mmbtu),BY Annual SO2 (tons),BY Average Annual SO2 Rate (lbs/mmbtu),BY Annual NOx (tons),BY Average Annual NOx Rate (lbs/mmbtu),BY OS NOx (tons),BY Average OS NOx Rate (lbs/mmbtu),BY OS heat input (mmbtu),BY NonOS NOx (tons),BY Average NonOS NOx Rate (lbs/mmbtu),FY Annual SO2 (tons),FY Average Annual SO2 Rate (lbs/mmbtu),FY Annual NOx (tons),FY Average Annual NOx Rate (lbs/mmbtu),FY OS NOx (tons),FY Average OS NOx Rate (lbs/mmbtu),FY OS heat input (mmbtu),FY NonOS NOx (tons),FY Average NonOS NOx Rate (lbs/mmbtu),Hierarchy Order,Longitude,Latitude,Generation Deficit Unit?, Retirement Date, New Unit?, Gasified?);

print OUT "@header\n";

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
    my $ertac_fuel_unit_type_bin = $array[29];

    my $calculated_BY_UF         = $array[37];
    my $max_annual_ertac_UF_state_input = $array[38];
    my $max_annual_ertac_UF      = $array[39];
    my $new_unit_flag            = $array[53];

# convert everything to small letters first

    $ertac_fuel_unit_type_bin =~ y/A-Z/a-z/;    

    $unitid =~ y/a-z/A-Z/;    # added on 11/25/2014

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{UF} 
        = $calculated_BY_UF;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{Lat} 
        = $plant_latitude;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{Long} 
        = $plant_longitude;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{RetireDate} 
        = $offline_start_date;

    $UAF{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid}{NewUnitQ} 
        = $new_unit_flag;

    if (!defined($FuelSwitch{$orispl_code}{$unitid})) {

        $FuelSwitch{$orispl_code}{$unitid} = "N";
        $FuelSwInfo{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid} = 
                   $BY_camd_hourly_data_type; }

    else {   # if defined....

        $FuelSwitch{$orispl_code}{$unitid} = "Y";  # as long as combo is repeated, 
                                                   # N will be overwritten by Y
        $FuelSwInfo{$ertac_region}{$ertac_fuel_unit_type_bin}{$orispl_code}{$unitid} = 
                   $BY_camd_hourly_data_type; }

#    if (!defined($FuelSwitch{$orispl_code}{$unitid})) {
#        $FuelSwitch{$orispl_code}{$unitid} = "N";}
#    else {
#        $FuelSwitch{$orispl_code}{$unitid} = "Y";}

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

my %CAMD;      # hourly data
my %BY;        # annual sum
my %BYOS;      # BY ozone season NOx sum
my %BYNonOS;   # BY non-ozone season NOx sum

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

     my ($year, $month, $date) = split("-", $op_date);

     if (substr($month, 0, 1) eq "0") {
         $month = substr($month, 1, 1); }

     if (substr($date, 0, 1) eq "0") {
         $date = substr($date, 1, 1); }

     if ($month eq "1") {
         $elapse_hrs = ($date - 1) * 24 + $op_hour + 1; }
     else {
         $elapse_hrs = $Julian[$month-2] * 24 + 
                       ($date - 1) * 24 + $op_hour + 1; }

# ************ base year hourly activity data ***********************

#     print "$op_date, $year, $month, $date, $op_hour, $elapse_hrs\n";

#    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{Gload} 
#          = $gload;
#    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{HI} 
#          = $heat_input;     

#    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{SO2_mass} 
#          = $so2_mass;     
#    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{NOX_mass} 
#          = $nox_mass;     

    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{Gload} = $gload;
    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{HI}    = $heat_input;     

    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{SO2} = $so2_mass;     
    $CAMD{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{$elapse_hrs}{NOX} = $nox_mass;     

# ************ base year annual summation ***********************

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

    if (!defined($BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI})) {
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} = $heat_input; }
    else { 
	$BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} = 
        $BY{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} + $heat_input; }

# ************ base year ozone season NOx/SO2 summation ***********************

    if ( ($month eq "5") || ($month eq "6") ||($month eq "7") ||
         ($month eq "8") || ($month eq "9") ) {

#    print "$month,";

    if (!defined($BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI})) {
	$BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} = $heat_input; }
    else { 
	$BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} = 
        $BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} + $heat_input; }

    if (!defined($BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX})) {
	$BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} = $nox_mass; }
    else { 
	$BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} = 
        $BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} + $nox_mass; }

    if (!defined($BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2})) {
	$BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} = $so2_mass; }
    else { 
	$BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} = 
        $BYOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} + $so2_mass; }

    } # end of month if


# ************ base year Non-ozone season NOx/SO2 summation *********************

    if ( ($month eq "1") || ($month eq "2") ||($month eq "3") ||
         ($month eq "4") || ($month eq "10") ||($month eq "11") ||
         ($month eq "12") ) {

    if (!defined($BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI})) {
	$BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} = $heat_input; }
    else { 
	$BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} = 
        $BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{HI} + $heat_input; }

    if (!defined($BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX})) {
	$BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} = $nox_mass; }
    else { 
	$BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} = 
        $BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{NOX} + $nox_mass; }

    if (!defined($BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2})) {
	$BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} = $so2_mass; }
    else { 
	$BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} = 
        $BYNonOS{$ertac_region}{$ertac_fuelbin}{$orispl_code}{$unitid}{SO2} + $so2_mass; }

    } # end of month if

} # end of while loop          

close(IN2);

#****************** Hourly diagnostic *************************

my %Proj;      # hourly data
my %FY;        # annual sum
my %FYOS;      # FY ozone season NOx sum
my %FYNonOS;   # FY Non-ozone season NOx sum

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

# ************ future year ozone season NOx/SO2 summation *******************
# May 1 hour 1 = 2881; September 30 hour 24 = 6552

    my $num_hour = sprintf("%8.1f", $Op_Hour);

#    print "$Op_Hour, $num_hour\n"; 

    if ( ($num_hour >= 2881) && ($num_hour <= 6552) ) {

#    print "$num_hour,";

    if (!defined($FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI})) {
	$FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} = $Heat_Input; }
    else { 
	$FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} = 
        $FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} + $Heat_Input; }

    if (!defined($FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX})) {
	$FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} = $NOX_mass; }
    else { 
	$FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} = 
        $FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} + $NOX_mass; }

    if (!defined($FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2})) {
	$FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} = $SO2_mass; }
    else { 
	$FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} = 
        $FYOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} + $SO2_mass; }

    } # end of ozone season Op_Hour if

# ************ future year Nonozone season NOx/SO2 summation ***************
    else {

    if (!defined($FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI})) {
	$FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} = $Heat_Input; }
    else { 
	$FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} = 
        $FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{HI} + $Heat_Input; }

    if (!defined($FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX})) {
	$FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} = $NOX_mass; }
    else { 
	$FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} = 
        $FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{NOX} + $NOX_mass; }

    if (!defined($FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2})) {
	$FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} = $SO2_mass; }
    else { 
	$FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} = 
        $FYNonOS{$Region}{$FuelBin}{$ORIS}{$UNIT_ID}{SO2} + $SO2_mass; }

    } # end of non-ozone season hourly sum if/else

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

my %Cmpl;

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
    $array[5] = $Fuel_Unit_Type_Bin;    # override array[5]

    $UnitID =~ y/a-z/A-Z/;    # added on 11/25/2014
    $array[1] = $UnitID;      # override array[1]

    my $Utilization_fraction = $array[12];   # this is future UF
    my $Base_year_generation = $array[13];
    my $Base_year_heat_input = $array[14];
    my $Base_year_hours_oped = $array[15];
    my $Future_year_generation  = $array[16];
    my $Future_year_heat_input  = $array[17];
    my $Future_year_hours_oped  = $array[18];

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

# ******************** begin cut and paste **********************
# *************** remove, paste, concatenate ********************

    my $chain_1 = "";   # initialization for each row;
    my $chain_2 = "";   # initialization for each row;

#    for my $i (0..9) {
    for my $i (0..11) {  # two extra new columns in v2
        $array[$i] =~ s/\,//g; 
        $chain_1 = $chain_1 . $array[$i] . ","; }

# ***** add BY UF to the end of chain_1 *****

    $chain_1 .= $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{UF}; 

# ************ first version of chain_2 has been commented out ********

#    for my $i (10..16) {  # starting element 10 is the FY UF
#        $array[$i] =~ s/\,//g; 
#        $chain_2 = $chain_2 . $array[$i] . ","; }

# element 13 and 16 are no longer needed!    # version 1
# so join only 10 to 12, and 14 to 15

# element 15 and 18 are no longer needed!    # version 2
# so join only 12 to 14, and 16 to 17

#    for my $i (10..12) {  # starting element 10 is the FY UF
    for my $i (12..14)  {  # starting element 10 is the FY UF, two extra columns
        $array[$i] =~ s/\,//g; 
        $chain_2 = $chain_2 . $array[$i] . ","; }

#    for my $i (14..15) {  
    for my $i (16..17) {  
        $array[$i] =~ s/\,//g; 
        $chain_2 = $chain_2 . $array[$i] . ","; }

# ********************* end of cut and paste *********************

# ******* check whether any of annual, ozone, non-ozone season HI is zero *********
# zero causes error of illegal dividing by zero in perl 

    if ($BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} == 0.0) {
       $ratio2 = "-999.9";
       $ratio4 = "-999.9"; }
    else {
       $ratio2  = $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} / 
                  $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI};        # (2)
       $ratio4  = $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} / 
                  $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; }      # (4)

    if ($BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} == 0.0) {
       $ratio6 = "-999.9"; }
    else {
       $ratio6  = $BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} / 
                  $BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; }    # (6)

    if ($BYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} == 0.0) {
       $ratio8 = "-999.9"; }
    else {
       $ratio8  = $BYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} / 
                  $BYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; } # (8)

    if ($FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} == 0.0) {
       $ratio10 = "-999.9"; 
       $ratio12 = "-999.9"; }
    else {
       $ratio10 = $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} /
                  $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI};        # (10)
       $ratio12 = $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} / 
                  $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; }      # (12)

    if ($FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} == 0.0) {
       $ratio14 = "-999.9"; }
    else {
       $ratio14 = $FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} /
                  $FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; }    # (14)

    if ($FYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} == 0.0) {
       $ratio16 = "-999.9"; }
    else {
       $ratio16 = $FYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} /
                  $FYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI}; } # (16)

# for a unit prior to fuel switch, its base year (and future year!) emissions 
# will be zero'ed out, 
# those BY/FY emissions will be put to post-fuel-switch units

# utilization fraction zero is no longer needed
#    if ( ($FuelSwitch{$ORIS}{$UnitID} eq "Y") && 
#         ($Utilization_fraction == 0.0) ) {     # 0 is what's labeled by Robert

    if ($FuelSwitch{$ORIS}{$UnitID} eq "Y") {

# *************** fuel switch with new fuel ***********************
# ********************** BY are zero ******************************

        if ($FuelSwInfo{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID} eq "NEW") {

# note that if ($Utilization_fraction eq "0") will NOT work!! 
# the UF is numerical, not character string!  FY info is ok (i.e., not needing
# 0.0, b/c fuel type bin is a distict columns, and the program will calculate 
# correctly, although the program prints 0 not 0.0!

    $chain_2 .= "0.0" . "," .
        	"0.0" . "," .
                "0.0" . "," .
                "0.0" . "," .
                "0.0" . "," .
                "0.0" . "," .
                "0.0" . "," .
                "0.0" . "," .
                "0.0" . "," .
                $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} 
                 * (1.0 / 2000.0) . "," .
                $ratio10 . "," . 
                $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . "," .
                $ratio12 . "," . 
                $FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . "," .  
                $ratio14 . "," . 
                $FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} . "," .  
                $FYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . "," .  
                $ratio16 . ","; 

        } # end of if


# *************** fuel switch but non-retiring unit ***********************
# $V defined means the unit in the post-processed hierarchy, 
# i.e., it's eligible for receiving FY generation (non-retiring unit)
# ne NEW mean the unit is FULL
# (substr($V{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}, 0, 1) != /[0-9]/) ) {

     elsif ( ($FuelSwInfo{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID} ne "NEW") && 
             (defined($V{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID})) ) {

     $chain_2 .= $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} * 
                 (1.0 / 2000.0) .  "," ;                                        # (1)
     $chain_2 .= $ratio2 . ",";
     $chain_2 .= $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                 (1.0 / 2000.0) . ",";                                          # (3)
     $chain_2 .= $ratio4 . ",";
     $chain_2 .= $BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                 (1.0 / 2000.0) . ",";                                          # (5)
     $chain_2 .= $ratio6 . ",";
     $chain_2 .= $BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} . ",";                
                                                                                # Joe request
     $chain_2 .= $BYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                 (1.0 / 2000.0) . ",";                                          # (7)
     $chain_2 .= $ratio8 . ",";

     $chain_2 .= $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} 
                 * (1.0 / 2000.0) . "," .
                $ratio10 . "," . 
                $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . "," .
                $ratio12 . "," . 
                $FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . "," .  
                $ratio14 . "," . 
                $FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} . "," .  
                $FYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . "," .  
                $ratio16 . ","; 

     } # end of eseif

# *************** fuel switch with old fuel ***********************
# ********************** FY are zero ******************************
# $V is not defined means the unit is retired alrady!

        else {

     $chain_2 .= $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} * 
                 (1.0 / 2000.0) . "," ;              # (1)
     $chain_2 .= $ratio2 . ",";
     $chain_2 .= $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                 (1.0 / 2000.0). ",";                # (3)
     $chain_2 .= $ratio4 . ",";
     $chain_2 .= $BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                 (1.0 / 2000.0) . ",";               # (5)
     $chain_2 .= $ratio6 . ",";
     $chain_2 .= $BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} . ",";  
                                                     # Joe request
     $chain_2 .= $BYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                 (1.0 / 2000.0) . ",";               # (7)
     $chain_2 .= $ratio8 . ",";
     $chain_2 .= "0.0" . "," .
                 "0.0" . "," .
                 "0.0" . "," . 
                 "0.0" . "," .
                 "0.0" . "," .
                 "0.0" . "," . 
                 "0.0" . "," .
                 "0.0" . "," . 
                 "0.0" . ","; }

    } # end of if

# **************** end of special fuel switch treatment *******************
# ****************** non-fuel switched units ******************************

    else {
    $chain_2 .= $BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} * 
                (1.0 / 2000.0) . "," ;                                                   # (1)
    $chain_2 .= $ratio2 . ",";
    $chain_2 .=	$BY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                (1.0 / 2000.0) . ",";                                                     # (3)
    $chain_2 .= $ratio4 . ",";
    $chain_2 .= $BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                (1.0 / 2000.0) . ",";                                                    # (5)
    $chain_2 .= $ratio6 . ",";
    $chain_2 .= $BYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} . ","; 
                                                                                         # Joe req
    $chain_2 .= $BYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} * 
                (1.0 / 2000.0) . ",";                                                    # (7)
    $chain_2 .= $ratio8 . ",";
    $chain_2 .= $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{SO2} 
                 * (1.0 / 2000.0) . ",";                                                 # (9)
    $chain_2 .= $ratio10 . ",";
    $chain_2 .= $FY{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . ",";                                                 # (11)
    $chain_2 .= $ratio12 . ",";
    $chain_2 .= $FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . ",";                                                 # (13)
    $chain_2 .= $ratio14 . ",";
    $chain_2 .= $FYOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{HI} . "," ;    # Joe req 
    $chain_2 .= $FYNonOS{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NOX} 
                 * (1.0 / 2000.0) . ",";                                                 # (15)
    $chain_2 .= $ratio16 . ",";
    } # end of else

# ************* add four extra elements to the end of chain_2 ***********

    $chain_2 .= $V{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID} . ","; 
    $chain_2 .= $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Long} . ","; 
    $chain_2 .= $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{Lat} . ","; 
    $chain_2 .= $Generic . ","; 
    $chain_2 .= $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{RetireDate} . ","; 
    $chain_2 .= $UAF{$ERTAC_Region}{$Fuel_Unit_Type_Bin}{$ORIS}{$UnitID}{NewUnitQ} . ","; 
    $chain_2 .= $Gasified;

#    print "$chain_1\n";
#    print "$chain_2\n";

    print OUT "$chain_1,$chain_2\n";

} # end of while IN loop

close(IN4);

