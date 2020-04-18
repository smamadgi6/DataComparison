# cpg

This test suite is implemented to capture all the mismatches between  OSSCR and CRDB databases. OSSCR is a current snapsot of CRDB  centralized database and is updated by a daily batch process to keep it up to date. Any create/update/delete transaction to OSSCR is updated in CRDB after trading completes usually within 5 minutes. A delete transaction in OSSCR marks the record in CRDB as Inactive. The input to the test script is a JSON file of one or more OSSCR table records. Default input file has all 16 tables. 

Once the test suite runs, it performs Create/Update/Delete accordingly and displays the below response of OSSCR API
•	Success : TRUE or FALSE
•	MethodResponse : Primary key values for NEW1, NEW2 and so on (For create)
•	ErrorResponse :  Error messages if any

The first section *****CREATE TEST - MYSQL OSSCR DATA COMPARISON********** displays the data comparison results of input JSON with OSSCR data in MYSQL DB.
Any data mismatches are reported with Test passed/Failed for each table

The second section ********CREATE TEST - ORACLE CRDB DATA COMPARISON******* displays the data comparison results of input JSON with CRDB data in ORACLE.
The wait loop shows up with elapsed time until trading completes in CRDB. Usually trading completes within 5 minutes, the wait loop should be changed in the script (elapsed) to wait longer.

Once its completed, it displays the primary key value in CRDB. 
Any data mismatches are reported with Test passed/Failed for each table.

