# Timescale

This tool measures the processing time of each query generated from a provided input file against a database and outputs benchmark stats once
all queries have been run. The tool provides the following outputs \
* \# of queries run
* The total processing time across all queries
* The minimum query time (for a single query)
* The median query time
* The average query time
* The maximum query time. 

The tool has two files listed undet the src directory:  
*global_setup.py* contains the configurations related to the database  
*timescale.py* is the main program file.

## Command to run the program
```bash
python src/timescale.py -w no_of_concurrent_workers -f path_to_input_file
```
The tool takes two input parameters provided as flags  
*-w:* no of concurrent workers that should execute queries, default value is 1  
*-f:* Path to input file that contains the parameters to execute the queries.
