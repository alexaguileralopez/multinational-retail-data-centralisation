import pandas as pd
import numpy as np 
import data_extraction
import database_utils

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
        
        # values can be duplicate so no cleaning in that sense

       ## check formatting errors first
       ## Assign date_format
        date_format_1= "%m/%y" # year with 2 digits instead of 4 (2026 -> 26)
        date_format_2= "%Y-%m-%d"
    
       #transform date to standard datetime in expiry_date column
        card_data['expiry_date']= pd.to_datetime(card_data["expiry_date"], format = date_format_1)
        card_data['date_payment_confirmed']=pd.to_datetime(card_data["date_payment_confirmed"], format= date_format_2)

        #drop null values
        card_data.dropna()

        return card_data
    
    
    def clean_store_data(self):

        store_data = data_extraction.DataExtractor().retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}")

        store_data.dropna(how = 'all')
        store_data.drop_duplicates()


        return store_data
    
    def convert_product_weights(self, products_df):

        return products_df


    
        




        




