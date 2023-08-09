import pandas as pd
import numpy as np 
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from datetime import datetime
import re

class DataCleaning:
    
    def convert_date(date_str):
        """
        Convert a date string to a datetime object.

        This function attempts to convert a date string into a datetime object using different formats.

        Notes:
            - The function tries different date formats to convert the input string.
            - If conversion is successful and the resulting datetime object is not NaT, it is returned.
            - If conversion fails using all formats, NaT is returned.

        """
        formats = ['%d %b %Y', '%Y %b %d','%Y %B %d','%Y-%m-%d', '%B %Y %d' ] # the formats the data is in

        for fmt in formats:
            try: 
                date_obj = pd.to_datetime(date_str, format= fmt)
                if not pd.isna(date_obj):
                    return date_obj
            except:
                pass
        return pd.NaT

    
    def clean_user_data(self, user_data):

        """
        Clean user-related data in the given DataFrame.

        This method performs a standard data cleaning process on user-related columns within the DataFrame.

        Notes:
            - Duplicate rows based on specified columns are dropped, keeping the last occurrence.
            - The 'index' column is dropped.
            - The 'address' column is cleaned by replacing newline characters with spaces.
            - The 'convert_date' method is used to convert date columns to datetime objects.
            - Rows with missing 'date_of_birth' and 'join_date' values are dropped.
            - The 'date_of_birth' and 'join_date' columns are formatted to 'YYYY-MM-DD'.
            - Rows with missing values are dropped, and the DataFrame index is reset.

        """

        clean_user_data = user_data.drop_duplicates(subset= ['email_address', 'address', 'phone_number', 'user_uuid'], keep = 'last').reset_index(drop = True)  # select which columns have to have unique values, and drop the duplicates
        clean_user_data.drop(columns= ['index'], inplace= True)
        clean_user_data['address'] = clean_user_data['address'].replace('\n', ' ', regex= True)
        clean_user_data['date_of_birth'] = clean_user_data['date_of_birth'].apply(self.convert_date) # passing method, not calling
        clean_user_data['join_date'] = clean_user_data['date_of_birth'].apply(self.convert_date)
        clean_user_data = clean_user_data[~clean_user_data['date_of_birth'].isna()]
        clean_user_data = clean_user_data[~clean_user_data['join_date'].isna()]
        clean_user_data['date_of_birth'] = clean_user_data['date_of_birth'].dt.strftime('%Y-%m-%d')
        clean_user_data['join_date'] = clean_user_data['join_date'].dt.strftime('%Y-%m-%d')
        clean_user_data.dropna(subset=['date_of_birth'], inplace= True)
        clean_user_data.dropna(subset=['join_date'], inplace= True)
        clean_user_data.dropna()
        clean_user_data.reset_index(drop= True, inplace= True)

        return clean_user_data
    

    def clean_card_data(self, card_data):
        
        """
        Clean card-related data in the given DataFrame.

        This method performs cleaning operations on card-related columns within the DataFrame.

        Notes:
            - Rows with 'NULL' values in any column are dropped.
            - The 'convert_date' method is used to convert date columns to datetime objects.
            - The 'expiry_date' and 'date_payment_confirmed' columns are formatted to 'YYYY-MM-DD'.
            - The resulting DataFrame is reset to have a continuous index.

        """

        card_data = card_data[card_data.apply(lambda row: not row.astype(str).str.contains('NULL').any(), axis=1)]        
        card_data['expiry_date'] = card_data['expiry_date'].apply(self.convert_date) #apply formatting
        card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].apply(self.convert_date)
        card_data['expiry_date'] = card_data['expiry_date'].dt.strftime('%Y-%m-%d') #convert dates to string following formats
        card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].dt.strftime('%Y-%m-%d')
        card_data.reset_index(drop= True, inplace= True)

        return card_data
    
    
    
    def clean_store_data(self, store_data):

        """
        Clean store-related data in the given DataFrame.

        This method performs cleaning operations on store-related columns within the DataFrame.

        Notes:
            - The 'convert_date' method is used to convert 'opening_date' column to datetime objects.
            - Rows with missing 'opening_date' values are dropped.
            - The 'opening_date' column is formatted to 'YYYY-MM-DD'.
            - Rows with missing 'staff_numbers' values are dropped.
            - The 'staff_numbers' column is cleaned using regular expressions and converted to numeric values.
            - Duplicate rows based on 'address' and 'store_code' are dropped.
            - The 'address' column is cleaned by replacing newline characters with spaces.

        """
        
        store_data['opening_date'] = store_data['opening_date'].apply(self.convert_date)
        store_data = store_data[~store_data['opening_date'].isna()]
        store_data['opening_date'] = store_data['opening_date'].dt.strftime('%Y-%m-%d')
        store_data.dropna(subset=['opening_date'], inplace= True)
        store_data = store_data.drop(columns= 'index')
        store_data['staff_numbers'] = store_data['staff_numbers'].str.extract('(\d+)', expand=False) 
        store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors='coerce')
        store_data.dropna(subset=['staff_numbers'], inplace= True) #inplace True ensures the original df is modified
        store_data.drop_duplicates(subset= ['address', 'store_code'])
        store_data['address'] = store_data['address'].replace('\n', ' ', regex= True)
        store_data.reset_index(drop= True, inplace= True)

        return store_data
    

    def convert_product_weights(self, df ):

        """
        Convert product weights into the same unit (kilograms).

        This method converts product weights into the same unit (kilograms) and standardizes their format.

        Notes:
            - The method searches for patterns like '12 x 100g' and converts them to kilograms.
            - It also uses a conversion dictionary for specific units (kg, g, ml, oz).
            - The resulting weights are rounded to four decimal places.

        """

        df['weight'] = df['weight'].astype('string')
        df['weight'] = df['weight'].fillna('NO VALUE')

        conversion_dict = {'kg': 1, 'g': 0.001, 'ml': 0.001, 'oz': 0.0283495}        
        pattern = r'(\d+\.\d+|\d+)\s*x\s*(\d+\.\d+|\d+)\s*(kg|g|ml|oz)?' 

        df['weight'] = df['weight'].apply(lambda x: float(re.search(pattern, x).group(1)) * float(re.search(pattern, x).group(2)) * conversion_dict.get(re.search(pattern, x).group(3), 0.001) if ' x ' in x and pd.notna(x)
           
            else float(x[:-2]) if x.endswith('kg') 
            else float(x[:-2])*0.001 if x.endswith('ml') 
            else float(x[:-1])*0.001 if x.endswith('g') 
            else float(x[:-2])*0.0283495 if x.endswith('oz')
            else float(x[:-3]) * 0.001 if x.endswith('.') # '77g .'
            else np.nan if pd.notna(x)
            else np.NAN)

        df['weight'] = df['weight'].apply(lambda x: round(x, 4) if pd.isna(x) == False else np.NAN) # rounding

        return df
    
    def clean_products_data(self, df):

        """
        This method performs cleaning operations on product-related columns within the DataFrame.

        Notes:
            - The 'convert_date' method is used to convert 'date_added' column to datetime objects.
            - Rows with missing 'date_added' values are dropped.
            - The 'date_added' column is formatted to 'YYYY-MM-DD'.
            - Duplicate rows based on 'EAN', 'uuid', and 'product_code' are dropped, keeping the last occurrence.
            - Rows with missing values are dropped.
        """
        
        df['date_added'] = df['date_added'].apply(self.convert_date)
        df = df[~df['date_added'].isna()] # drop NA
        df['date_added'] = df['date_added'].dt.strftime('%Y-%m-%d')       
        df = df.drop_duplicates(subset= ['EAN', 'uuid', 'product_code'], keep = 'last')
        df = df.reset_index(drop= True)
        df = df.dropna()

        return df
    
    
    def clean_orders_data(self, df):

        ''' Method drops columns first name, last name and column named 1'''
        
        df = df.drop(columns= ['first_name', 'last_name', '1'])
                
        return df
    
    def clean_date_data(self, df):

        """
        This method performs various cleaning operations on date-related columns within the DataFrame.
        
        Notes:
            - This method drops rows containing 'NULL' values in any column.
            - The 'timestamp' column is converted to a time object with the format 'HH:MM:SS'.
            - Duplicate rows with the same 'date_uuid' are dropped, keeping the last occurrence.
            - Columns 'year', 'day', 'month', 'time_period', and 'date_uuid' are typecasted for consistency.
            - Rows with missing values are dropped.
        """
        
        df = df[df.apply(lambda row: not row.astype(str).str.contains('NULL').any(), axis=1)]
        df['timestamp'] = df['timestamp'].apply(lambda x: datetime.strptime(x, '%H:%M:%S').time() 
                                        if isinstance(x, str) and re.match(r'\d{2}:\d{2}:\d{2}', x) # using regular expressions to match 2 digits followed by colon  
                                        else np.nan)
        df = df.dropna()
        df = df.drop_duplicates(subset= ['date_uuid'], keep= 'last').reset_index(drop=True)
        df['year'] = df['year'].astype('int')
        df['day'] = df['day'].astype('int')
        df['time_period'] = df['time_period'].astype('str')
        df['month'] = df['month'].astype('str')
        df['date_uuid'] = df['date_uuid'].astype('str') # memory usage is reduced by 1 MB
        df = df.dropna() 

        return df
    
        
        
if __name__ == '__main__':

    database_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()


    '''User data'''
    user_data = data_extractor.read_rds_table(table_name= 'legacy_users')
    clean_user_data = data_cleaner.clean_user_data(user_data= user_data)
    database_connector.upload_to_db(dataframe= clean_user_data, table_name= 'user_data')

    '''Card data'''
    card_data = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    clean_card_data = data_cleaner.clean_card_data(card_data= card_data)
    database_connector.upload_to_db(dataframe= clean_card_data, table_name= 'card_data')

    '''Store data'''
    store_data = data_extractor.retrieve_stores_data(url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', header = 'header_details.yaml')
    clean_store_data = data_cleaner.clean_store_data(store_data= store_data)
    database_connector.upload_to_db(dataframe= clean_store_data,table_name= 'store_data' )

    '''Products data'''
    products_data = data_extractor.extract_from_s3(path= 'data-handling-public/products.csv')
    products_data_v1 = data_cleaner.convert_product_weights(products_data)
    clean_products_data = data_cleaner.clean_products_data(products_data_v1)
    database_connector.upload_to_db(dataframe= clean_products_data, table_name = 'products_data')

    ''' Orders data'''
    orders_data = data_extractor.read_rds_table(table_name= 'orders_table')
    clean_orders_data = data_cleaner.clean_orders_data(orders_data)
    database_connector.upload_to_db(dataframe= clean_orders_data, table_name= 'orders_data')

    '''Events data'''
    date_data = data_extractor.extract_date_details(url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    clean_date_data = data_cleaner.clean_date_data(date_data)
    database_connector.upload_to_db(dataframe= clean_date_data, table_name= 'date_data')



        
    













    
        




        




