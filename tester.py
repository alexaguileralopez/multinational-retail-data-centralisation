

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

from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

user_data = DataCleaning().clean_user_data()
#card_data = data_cleaning.DataCleaning().clean_card_data()

DatabaseConnector().upload_to_db(dataframe= user_data, table_name= 'dim_users')
#database_utils.DatabaseConnector().upload_to_db(card_data, 'dim_card_details')

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

new_2 = new_2.dropna()


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



    















                      

# %% Task 5: Extract and clean the details of each store
import requests
import pandas as pd

return_number_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/50'

key = { 'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}


num_stores = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers= key)
store = requests.get(retrieve_store_endpoint, headers= key)

num_stores.json() # status code 200 (OK), gives a literal 'number_stores': 451
print(num_stores.json()['number_stores'])


#retrieving a json file from the url
retrieved_stores = requests.get(retrieve_store_endpoint, headers= key).json()



#turning json into pandas normalized dataframe
retrieved_stores = pd.json_normalize(retrieved_stores)

print(retrieved_stores)



retrieved_store_2_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/51'
retrieved_store_2 = requests.get(retrieved_store_2_url, headers= key).json()
retrieved_store_2 = pd.json_normalize(retrieved_store_2)
print(retrieved_store_2)




# necessary to figure out how to retrieve the store endpoint given a specific store number

# %%
import requests
import pandas as pd

number_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
key = { 'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

number_stores = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers= key).json()['number_stores']

df = pd.DataFrame

for i in range(0,5):
    params = {'store_number': i}
    store_data = requests.get(retrieve_store_endpoint_url, params = params, headers= key)
    print(store_data.json())

# %%
import requests
import pandas as pd
from pandas import json_normalize

key = { 'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

number_stores = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers= key).json()['number_stores']
df = pd.DataFrame()

for i in range(0,number_stores):
    store_data = requests.get(retrieve_store_endpoint_url.format(store_number = i), headers= key).json()
    store_data = json_normalize(store_data)
    df = pd.concat([df, store_data])


# %%

from data_extraction import DataExtractor

df = DataExtractor().retrieve_stores_data()



# %%
import data_extraction
import data_cleaning
import database_utils

clean_store_data = data_cleaning.DataCleaning().clean_store_data()
database_utils.DatabaseConnector().upload_to_db(dataframe = clean_store_data, table_name= 'dim_store_details')
# %% testing data extraction from s3
# necessary to install s3fs
import boto3
import pandas as pd

# create a client object (connection to S3) using default config and all buckets within S3
client = boto3.client('s3')

path = 's3://data-handling-public/products.csv'

df = pd.read_csv(path)

df.head()

# %%
import data_extraction
import data_cleaning
import pandas as pd

df = data_extraction.DataExtractor().extract_from_s3()

df['weight'] = df['weight'].astype(str).apply(lambda x : float(x[:-2]) if x.endswith('kg') 
                                else float(x[:-1]) * 0.001 if x.endswith('g') 
                                else float(x[:-2]) * 0.001 if x.endswith('ml')
                                else float(x))



# %%

import data_extraction

df = data_extraction.DataExtractor().extract_from_s3()

weights_col = df['weight'].values.astype(str)

counter_wrong = 0

for weight in weights_col:
    if weight.endswith('kg'):
        float(weight[:-2])
    elif weight.endswith('ml'):
        float(weight[:-2])
        weight = weight * 0.001
    elif weight.endswith('g'):
        if 'x' in weight:
            weight = weight[:-1]
            weight = weight.replace('x', '*')
            float(eval(weight)) * 0.001
            
        else: 
            weight = float(weight[:-1])
            weight = weight * 0.001
    
    else:
        counter_wrong = counter_wrong +1

# %% Trying functions to remove units from the weight column in the dataframe 'products'

import pandas as pd
import re # for the case where we have expressions such as 12 x 100g

s = pd.Series(['1.6kg', '100g', '10kg', '650g', '500ml', '16oz', '0g', '12 x 100g', '2 x 10kg'])

def weight_transform(x):

    pattern = r'(\d+(?:\.\d+)?)(?:\s*x\s*)(\d+(?:\.\d+)?)*\s*(kg|g|ml|oz)'

    match = re.match(pattern, x)

    if match:
        value1 = float(match.group(1))
        value2 = float(match.group(2)) if match.group(2) else 1
        unit = match.group(3)

        weight = value1 * value2

        if unit == 'g' or 'ml':
            weight /= 1000
        
        elif unit == 'oz':
            weight /= 35.27
        
        return weight
    
    elif 'kg' in x:
        float(x)
    

for weight in s:
    print(weight_transform(weight))



    

# %%

import pandas as pd
import re

s = pd.Series(['1.6kg', '100g', '10kg', '650g', '500ml', '16oz', '0g', '12 x 100g', '4 x 400g', '2 x 10kg', 'MX180RYSHX'])
pattern = r'(\d+\.\d+|\d+)\s*x\s*(\d+\.\d+|\d+)\s*(kg|g|ml|oz)?'
conversion_dict = {'kg': 1, 'g': 0.001, 'ml': 0.001, 'oz': 0.0283495}

s = s.apply(lambda x: float(re.search(pattern, x).group(1)) * float(re.search(pattern, x).group(2)) * conversion_dict.get(re.search(pattern, x).group(3), 0.001) if 'x' in x
            else float(x[:-2]) if x.endswith('kg') 
            else float(x[:-2])*0.001 if x.endswith('ml') 
            else float(x[:-1])*0.001 if x.endswith('g')
            else float(x[:-2])*0.0283495 if x.endswith('oz')
            else None if pd.isna(x)
            else None)
s = s.round(2)

print(s)
# %% Trying the function developed above in the actual dataframe:
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

df1 = DataCleaning().convert_product_weights(df = DataExtractor().extract_from_s3())
df = DataCleaning().clean_products_data(df=df1)
DatabaseConnector().upload_to_db(dataframe= df, table_name= 'dim_products')


# %% TASK 7:

from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

tester = DatabaseConnector()
df1 = DataExtractor().read_rds_table(tester, 'orders_table')
df = DataCleaning().clean_orders_data(df1)
print(df)


# %% TASK 8 
from data_extraction import DataExtractor
import boto3
import numpy as np
from awscli.customizations.s3.utils import split_s3_bucket_key
import numpy as np
import pandas as pd
import requests
from datetime import datetime, time
import json
import re

url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

response = requests.get(url)
if response.status_code == 200:
    df = pd.read_json(url)
else:
    print( f'Error {response.status_code}')

#nulls = df.isna().sum()
#print('Number of nulls: \n', nulls) # displays 0 nulls, and there are many rows in the df with 'NULL' as element

# count rows to check wether the next steps will work

df.info() # 120161 entries

# axis = 1 to apply to each row, NOT operator to invert result, 
# so that function returns TRUE for rows not containing the string 'NULL'
df = df[df.apply(lambda row: not row.astype(str).str.contains('NULL').any(), axis=1)]
df.info() # 120146 entries, it has reduced

# date_format = "%H:%M:%S"
# df['timestamp'] = pd.to_datetime(df['timestamp'], errors= 'coerce', format= date_format)

# the following function transforms the timestamp to time object, and sets nan to those values that do not match the format
df['timestamp'] = df['timestamp'].apply(lambda x: datetime.strptime(x, '%H:%M:%S').time() 
                                        if isinstance(x, str) and re.match(r'\d{2}:\d{2}:\d{2}', x) 
                                        # using regular expressions to match 2 digits followed by colon
                                        else np.nan)
df = df.dropna() #to remove those that do not contain timestamp and were set as NAN
df.info() # 120146 entries, it has reduced
df = df.drop_duplicates(subset= ['date_uuid'], keep= 'last').reset_index(drop=True)
df['year'] = df['year'].astype('int')
df['day'] = df['day'].astype('int')
df['time_period'] = df['time_period'].astype('str')
df['month'] = df['month'].astype('str')
df['date_uuid'] = df['date_uuid'].astype('str') # memory usage is reduced by 1 MB
df = df.dropna() 

print('After looking for NULL strings:\n')
df.info() # 120123 entries, it has reduced

# %%RUNNING CODE FROM TASK 8 STEP BY STEP
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

df1 = DataExtractor().extract_date_details()
df = DataCleaning().clean_date_data(df1)

df.info()

DatabaseConnector().upload_to_db(dataframe= df, table_name= 'dim_date_times')



# %% RE-UPLOADING CARD DATA WITH NEW CLEANING METHOD
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector


card_data = DataCleaning().clean_card_data()

DatabaseConnector().upload_to_db(dataframe= card_data, table_name= 'dim_card_details')
# %%
