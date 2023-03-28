

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
table = tester_1.read_rds_table(tester, 'legacy_users')
table.info()

# %%
table.isna().sum()

# %%
table.duplicated().sum()

# %%
# saving as excel file in current directory to visualize table

import pandas as pd
import os

directory = os.getcwd()
table.to_csv('user_data', path_or_buf= directory)
