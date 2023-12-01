# create separate functions to be used against postgress that for a given date, schema name and table name creates does the following
# 1. creates a partition with naming concention of <schema_name>.<table_name>_p<date>, needs to test that partition does not already exist and if it does then it does not create it
# 2. drops a partition, needs to check that partition exists before dropping it
# 3. executes analyse command on partition needs to check that partition exists before analysing it
# 4. detaches a partition needs to check that partition exists before detaching it
# 5. attaches a partition need to check that partition exists before attaching it
# 6. creates a new table based on a source table with a partition wittn naming convention of <schema_name>.<table_name>_p<date>, needs to check that new table does not already exist before creating it and that the source table exists
# 7. logging and exception management sshould be Implemented
# 8. validate date format
# include a main function that calls all the above functions and handles exceptions