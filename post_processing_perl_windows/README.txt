
Note that perl packages are needed to be able to run this script.  Most machines 
usually have perl already installed.  If not, ask your system administrator for help.

To use this script, please follow the steps below:

(1) Edit the "inputs.txt" file (in the package downloaded).  

    There are three lines to be edited:  
    (a) The first line is the folder where the ERTAC EGU outputs are located.  
    (b) The second line is an existing folder where you would like the enhanced file (output) 
        from this script to be located.  
    (c) The third line is a file identifier with which you would like the enhanced output to be 
        associated.  
        For example, an identifier CONUS-v2.0 will give you an enhanced file output of
        CONUS-v2-0_p_unit_level_activity_addon_enhanced.csv.

(2) Bring up cdm box on Windows.

(3) Change to the folder to where the perl script and inputs.txt are located.

(4) Type "perl addon_enhanced_unit_level_10032013.pl < inputs.txt" on the prompt.

If everything goes well, an enhanced file should be in the output folder specified in the 
second line of the inputs.txt.

