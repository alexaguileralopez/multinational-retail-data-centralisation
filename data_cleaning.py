import pandas as pd
import numpy as np 
import data_extraction
import database_utils

class DataCleaning:
    
    def __init__(self):
        pass

    def clean_user_data(self):

        user_data = data_extraction.DataExtractor().read_rds_table(database_connector_instance= database_utils.DatabaseConnector(), 
                                                    table_name= 'legacy_user')
        # drop rows with null values, however, from exploring the data, we know 
        # there are no null values
        user_data.dropna()

        # select which columns have to have unique values, and drop the duplicates

        clean_user_data = user_data.drop_duplicates(subset= ['email_adress', 'address', 'phone_number', 'user_uuid'], keep = 'last').reset_index(drop = True)


        #stablish a date format and apply it to the columns containing dates
        date_format = "%Y%m%d"
        pd.to_datetime(clean_user_data["date_of_birth"], format= date_format)
        pd.to_datetime(clean_user_data["join_date"], format= date_format)

        
        return clean_user_data

    def clean_card_data(self):

        #retrieve data and put into pd dataframe
        card_data = data_extraction.DataExtractor().retrieve_pdf_data("https://portal.theaicore.com/pathway/8955a07c-2223-4757-9f74-2aa287aa1aca#:~:text=document%20at%20following-,link,-.%0AThen%20return")
        card_data = pd.DataFrame(card_data)

        # drop duplicates
        card_data.dropna(how = 'all')

       ## check formatting errors first

       ## Assign date_format
        date_format = "%Y%m%d"
        # date_format = "%d-%m-%Y"
        pd.to_datetime(card_data["date_payment_confirmed"], format=date_format)

       #transform date to standard datetime
        card_data= pd.to_datetime(card_data["date_payment_confirmed"])

        # transforming into pd dataframe and dropping duplicated rows
        card_data.drop_duplicates()

        return card_data
    
    def clean_store_data(self):

        store_data = data_extraction.DataExtractor().retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}")

        store_data.dropna(how = 'all')
        store_data.drop_duplicates()


        return store_data
    
    def convert_product_weights(self, products_df):

        return products_df


    
        




        




