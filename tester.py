

# file to test different methods 

# %%
import data_cleaning
import data_extraction
import database_utils

# %%
tester = database_utils.DatabaseConnector()

print(type(tester.list_db_tables())) 
# this prints a list containing 3 tables, works fine

# %%
tester_1 = data_extraction.DataExtractor()

tester_1.read_rds_table(tester, 'legacy_users')

# %%
