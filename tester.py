

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
import database_utils

user_data = data_cleaning.DataCleaning().clean_user_data()
card_data = data_cleaning.DataCleaning().clean_card_data()

database_utils.DatabaseConnector().upload_to_db(user_data, 'dim_users')
database_utils.DatabaseConnector().upload_to_db(card_data, 'dim_card_details')

## need to fix the error in upload to db in order to being able to connect and upload the dataframe


# %% Better version of data cleaning

import data_cleaning
import data_extraction
import database_utils
import numpy as np
import pandas as pd
from datetime import datetime

card_data = data_extraction.DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

new_1 = card_data['date_payment_confirmed']
print('First length of new_1 and new_2 is:', len(new_1))
new_1 = pd.to_datetime(new_1, errors= 'coerce')
print('Length of new_1 is;', len(new_1))

new_2 = card_data['date_payment_confirmed']

new_2 = pd.to_datetime(new_2, errors= 'coerce', format= '%Y-%m-%d')
print('Length of new_2 is:', len(new_2))

new_2.dropna()

print('Length of new_2 dropping na is:', len(new_2))

#for y in new:
        #card_data['date_payment_confirmed'][y] = datetime.strptime(card_data['date_payment_confirmed'][y], '%d %B %Y')


# %% Another approach to data cleaning

import data_cleaning
import data_extraction
import database_utils
import pandas as pd
from datetime import datetime


card_data = data_extraction.DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

def replace_date_format(date_string):
    try:
        # try to parse the date string using the format of date_string_1
        date_object = datetime.strptime(date_string, '%Y-%m-%d')
        return date_string #return the original date string if it is already in the correct format
   
    except ValueError:
        pass

    try:

        date_object = datetime.strptime(date_string, '%Y %B %d')
        return date_object.strftime('%Y-%m-%d')  # return the date string in the format of date_string_1
    
    except ValueError:
        pass
    
    try:
        date_object = datetime.strptime(date_string, '%B %Y %d')      
        return date_string # return the original date string if it cannot be parsed using either format
    except ValueError:
        pass
    # apply the replace_date_format function to the timestamp column of the dataframe
    return date_string

card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].apply(replace_date_format())

#print the updated dataframe
print(card_data['date_payment_confirmed'])



    















                      

# %%
