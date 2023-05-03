import pandas as pd
import numpy as np 
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from datetime import datetime
import re

class DataCleaning:
    
    def __init__(self):
        pass


    def clean_user_data(self):

        user_data = DataExtractor().read_rds_table(database_connector_instance= DatabaseConnector(), 
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
    


    def clean_card_data(self, card_data = DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")):
        
        # first drop the values that contain NULL as a string  
        #card_data.drop(card_data['expiry_date']==''.index, inplace= True)

        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors= 'coerce', format= '%Y-%m-%d')

        card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], errors= 'coerce', format= '%m/%y')

        card_data.dropna()
        card_data.reset_index(drop= True, inplace= True)

        return card_data
    
    
    # input the old dataframe as an argument here
    def clean_store_data(self, store_data = DataExtractor().retrieve_stores_data() ):

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
    

    def convert_product_weights(self, df ):

        df['weight'] = df['weight'].astype('string')
        df['weight'] = df['weight'].fillna('NO VALUE')

        # checking the dataframe, we can see that the units that are used are the following:
        conversion_dict = {'kg': 1, 'g': 0.001, 'ml': 0.001, 'oz': 0.0283495}

        # In some cases, the data is stored in the form '12 x 100g', 
        # and we will use regular expressions to find that pattern
        pattern = r'(\d+\.\d+|\d+)\s*x\s*(\d+\.\d+|\d+)\s*(kg|g|ml|oz)?'
        
        # using lambda fxns to change the units
        # the first expression searchs for the mentioned pattern and 
        # replaces it multiplying the 2 'groups' of numbers
        # It also checks wether the units are correct and applies the conversion dictionary
        df['weight'] = df['weight'].apply(lambda x: float(re.search(pattern, x).group(1)) * float(re.search(pattern, x).group(2)) * conversion_dict.get(re.search(pattern, x).group(3), 0.001) if ' x ' in x and pd.notna(x)
           
           # the rest of the expressions look for the units with 'endswith' 
            else float(x[:-2]) if x.endswith('kg') 
            else float(x[:-2])*0.001 if x.endswith('ml') 
            else float(x[:-1])*0.001 if x.endswith('g')
            else float(x[:-2])*0.0283495 if x.endswith('oz')
            #else np.NAN if pd.isna(x)
            else np.NAN)  ## we use np.NAN because it is a float value and allows operations
        
        # rounding the weight in kg to 2 decimals
        df['weight'] = df['weight'].apply(lambda x: round(x, 2) if pd.isna(x) == False else np.NAN)



        return df
    
    def clean_products_data(self, df= pd.DataFrame):

        # check for duplicates in EAN, uuid, and product_code columns
        #df = self.convert_product_weights()


        df = df.drop_duplicates(subset= ['EAN', 'uuid', 'product_code'], keep = 'last').reset_index(drop= True)
        

        #stablish a date format and apply it to columns containing dates
        date_format = "%Y-%m-%d"
        df['date_added'] = pd.to_datetime(df['date_added'], errors= 'coerce', format= date_format)

        # remove non existing values
        df = df.dropna()

                

        return df
    
    
    def clean_orders_data(self, df = pd.DataFrame):
        
        df = df.drop(columns= ['first_name', 'last_name', '1'])
        
        
        return df
        
        

        
    













    
        




        




