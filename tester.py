

# file to test different methods 

# %%
import pandas as pd
import data_cleaning
import data_extraction
import database_utils

tester = database_utils.DatabaseConnector()

print(type(tester.list_db_tables())) 
# this prints a list containing 3 tables, works fine


tester_1 = data_extraction.DataExtractor()

tester_1.read_rds_table(tester, 'legacy_users')


table = tester_1.read_rds_table(tester, 'legacy_users')
table.info()

# %%
table.isna().sum()

# %%
table.duplicated().sum()

# %%
# saving as excel file in current directory to visualize table

import pandas as pd


table.to_csv('user_data.csv')

# %%
# the last values in the table do not correspond to their position id, eventhough there are not duplicated values in any column
# we sort the table values 

table.sort_values(by=['index'])
# %% seeing that we get mixed values anyway, drop index values

table.drop(columns= ['index'], inplace= True)
table


# %% replace /n in column address for ''

table['address'] = table['address'].replace('\n', ' ', regex= True)
table
# %% change datetime
date_format = "%Y-%m-%d"
table['date_of_birth']= pd.to_datetime(table['date_of_birth'], infer_datetime_format= True, format= date_format, errors= 'coerce')

table['date_of_birth']
# %% delete null values

table.dropna()

# %%

table['join_date'] = pd.to_datetime(table['date_of_birth'], infer_datetime_format= True, format = date_format, errors= 'coerce')

# %%
table['join_date']
# %%
table.dropna()
# %%
table.reset_index(drop=True, inplace=True)
table

# %% packages to run for card details extraction (pdf)

import pandas as pd

import tabula


link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
dfs = tabula.read_pdf(link, pages= 'all')
#the dfs stores 279 pages, which are 279 list elements
# it is necessary to concatenate these 279 elements, which are 279 pdf pages


dfs = pd.concat(dfs)

dfs.info()


# %% To upload tables to local database

import data_cleaning
import data_extraction

card_data = data_cleaning.DataCleaning().clean_card_data()

#database_utils.DatabaseConnector().upload_to_db(card_data, 'dim_card_details')











                      

# %%
