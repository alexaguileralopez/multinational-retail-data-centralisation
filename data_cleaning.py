import pandas as pd
import numpy as np 
import data_extraction
import database_utils
from datetime import datetime

class DataCleaning:
    
    def __init__(self):
        pass


    def clean_user_data(self):

        user_data = data_extraction.DataExtractor().read_rds_table(database_connector_instance= database_utils.DatabaseConnector(), 
                                                    table_name= 'legacy_users')


        # select which columns have to have unique values, and drop the duplicates

        clean_user_data = user_data.drop_duplicates(subset= ['email_address', 'address', 'phone_number', 'user_uuid'], keep = 'last').reset_index(drop = True)

        clean_user_data.drop(columns= ['index'], inplace= True)

        # replace newline sign \n in adress column by ' '

        clean_user_data['address'] = clean_user_data['address'].replace('\n', ' ', regex= True)

        #stablish a date format and apply it to the 2 columns containing dates
        date_format = "%Y-%m-%d"
        clean_user_data["date_of_birth"] = pd.to_datetime(clean_user_data["date_of_birth"], format= date_format, errors= 'coerce')
        clean_user_data["join_date"] = pd.to_datetime(clean_user_data["join_date"], format= date_format, errors='coerce')

        #last step is to drop null values, and to reset the index, as 
        # less values will appear now that some will be dropped
        clean_user_data.dropna()
        clean_user_data.reset_index(drop= True, inplace= True)

        return clean_user_data
    


    def clean_card_data(self):

        #get data (it is a pd dataframe)
        card_data = data_extraction.DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        
        # first drop the values that contain NULL as a string  
        #card_data.drop(card_data['expiry_date']==''.index, inplace= True)

        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors= 'coerce', format= '%Y-%m-%d')

        card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], errors= 'coerce', format= '%m/%y')

        card_data.dropna()
        card_data.reset_index(drop= True, inplace= True)

        return card_data
    
    
    def clean_store_data(self):

        store_data = data_extraction.DataExtractor().retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}")

        # transforming opening date column to datetime object
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors= 'coerce', format= '%Y-%m-%d')
        # drop index column, as it will be useful when dropping rows with N/A
        store_data = store_data.drop(columns= 'index')
        
        # cleaning dirty data
        store_data.dropna()
        store_data.drop_duplicates(subset= ['address', 'store_code'])
        store_data['address'] = store_data['address'].replace('\n', ' ', regex= True)
        store_data.reset_index(drop= True, inplace= True)


        return store_data
    

    def convert_product_weights(self, df):

        df['weight'] = df['weight'].astype(str)


        for weight in df['weight']:
            if weight.endswith('kg'):
                weight = float(weight[:-2])
            elif weight.endswith('ml'):
                weight = float(weight[:-2])
                weight = weight * 0.001
            elif weight.endswith('g'):
                weight = float(weight[:-1])
                weight = weight * 0.001
            else:
                float(weight)

        return df


    
        




        




