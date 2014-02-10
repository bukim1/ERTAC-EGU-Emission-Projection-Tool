ERTAC EGU readme file - 5/10/2012 update

The enclosed code includes the following changes made since the 5/2 version:

1. The fix (and the re-fixed fix) to the misplaced calendar table creation in
the preprocessor is included here.  That was already available last week on 5/3,
and is included as part of this updated version.

2. A typo in a variable name in the library code has been corrected.  Joseph had
identified this problem on Monday.

3. A problem caused by incorrectly-entered UAF information about fuel-switched
units has been solved.  Joseph had found in a 7-state test case on Tuesday that
creation of the 13th generic unit ('MACE', 'Oil', '7153', 'G10002') failed in
the projection run.  This was because the generic new unit was being based on
('MACE', 'Oil', '2434', '006001') which switched from Coal to Oil in 2009 but
was incorrectly listed as 'Full' instead of 'NEW' for base year 2007.  That data
problem had been noted in the preprocessor log, and the preprocessor should have
changed the Oil unit to 'NEW' in the UAF, but a code error prevented that change
from being successful.

Ironically, ('2434', '006001') is the example unit discussed in a comment block
about how to determine which units need hourly proxy generation calculated in
the preprocessor.
