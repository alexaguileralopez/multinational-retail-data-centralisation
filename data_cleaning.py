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

        user_data = DataExtractor().read_rds_table(table_name= 'legacy_users')


        # select which columns have to have unique values, and drop the duplicates

        clean_user_data = user_data.drop_duplicates(subset= ['email_address', 'address', 'phone_number', 'user_uuid'], keep = 'last').reset_index(drop = True)

        clean_user_data.drop(columns= ['index'], inplace= True)

        # replace newline sign \n in adress column by ' '

        clean_user_data['address'] = clean_user_data['address'].replace('\n', ' ', regex= True)

        #stablish a date format and apply it to the 2 columns containing dates

        formats = ['%d %b %Y', '%Y %b %d','%Y %B %d','%Y-%m-%d', '%B %Y %d' ] # the formats the data is in
        def convert_date(date_str):
            for fmt in formats:
                try: 
                    date_obj = pd.to_datetime(date_str, format= fmt)
                    if not pd.isna(date_obj):
                        return date_obj
                except:
                    pass
            return pd.NaT
        
        
        clean_user_data['date_of_birth'] = clean_user_data['date_of_birth'].apply(convert_date)
        clean_user_data['join_date'] = clean_user_data['date_of_birth'].apply(convert_date)

        clean_user_data = clean_user_data[~clean_user_data['date_of_birth'].isna()]
        clean_user_data = clean_user_data[~clean_user_data['join_date'].isna()]

        clean_user_data['date_of_birth'] = clean_user_data['date_of_birth'].dt.strftime('%Y-%m-%d')
        clean_user_data['join_date'] = clean_user_data['join_date'].dt.strftime('%Y-%m-%d')

        clean_user_data.dropna(subset=['date_of_birth'], inplace= True)
        clean_user_data.dropna(subset=['join_date'], inplace= True)

        
        
        #date_format = "%Y-%m-%d"
        #clean_user_data["date_of_birth"] = pd.to_datetime(clean_user_data["date_of_birth"], format= date_format, errors= 'coerce')
        #clean_user_data["join_date"] = pd.to_datetime(clean_user_data["join_date"], format= date_format, errors='coerce')

        #last step is to drop null values, and to reset the index, as 
        # less values will appear now that some will be dropped
        clean_user_data.dropna()
        clean_user_data.reset_index(drop= True, inplace= True)

        return clean_user_data
    


    def clean_card_data(self, card_data = DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")):
        
        # first drop the values that contain NULL as a string  
        #card_data.drop(card_data['expiry_date']==''.index, inplace= True)

        # card data to store values that do not contain the string 'NULL'
        card_data = card_data[card_data.apply(lambda row: not row.astype(str).str.contains('NULL').any(), axis=1)]

        formats = ['%d %b %Y', '%Y %b %d','%Y %B %d','%Y-%m-%d', '%B %Y %d', '%m/%y' ] # the formats the data is in
        def convert_date(date_str):
            for fmt in formats:
                try: 
                    date_obj = pd.to_datetime(date_str, format= fmt)
                    if not pd.isna(date_obj):
                        return date_obj
                except:
                    pass
            return pd.NaT
        
        #store_data['opening_date'] = store_data['opening_date'].apply(convert_date)
        card_data['expiry_date'] = card_data['expiry_date'].apply(convert_date)
        card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].apply(convert_date)
        

       
        ######card_data = card_data[~card_data['expiry_date'].isna()]
        ######card_data = card_data[~card_data['date_payment_confirmed'].isna()]

        #store_data['opening_date'] = store_data['opening_date'].dt.strftime('%Y-%m-%d')
        card_data['expiry_date'] = card_data['expiry_date'].dt.strftime('%Y-%m-%d')
        card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].dt.strftime('%Y-%m-%d')

        
        #card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors= 'coerce', infer_datetime_format= True, format= '%Y-%m-%d')

        #card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], errors= 'coerce', format= '%m/%y')

        #card_data.dropna()
        card_data.reset_index(drop= True, inplace= True)

        return card_data
    
    
    # input the old dataframe as an argument here
    def clean_store_data(self, store_data = DataExtractor().retrieve_stores_data() ):

        # as the opening date data is in different formats
        # the solution is to accept all of them and then transform all to the same format
        formats = ['%d %b %Y', '%Y %b %d','%Y %B %d','%Y-%m-%d', '%B %Y %d' ] # the formats the data is in
        def convert_date(date_str):
            for fmt in formats:
                try: 
                    date_obj = pd.to_datetime(date_str, format= fmt)
                    if not pd.isna(date_obj):
                        return date_obj
                except:
                    pass
            return pd.NaT
        
        store_data['opening_date'] = store_data['opening_date'].apply(convert_date)

        store_data = store_data[~store_data['opening_date'].isna()]

        store_data['opening_date'] = store_data['opening_date'].dt.strftime('%Y-%m-%d')

        
        store_data.dropna(subset=['opening_date'], inplace= True)


        # drop index column, as it will be useful when dropping rows with N/A
        store_data = store_data.drop(columns= 'index')
        
        # cleaning dirty data

        #dropping those rows that contain invalid values for number of staff members
        #store_data['staff_numbers'] = store_data['staff_numbers'].str.replace('n', '', regex=False)
        #store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors= 'coerce')
        store_data['staff_numbers'] = store_data['staff_numbers'].str.extract('(\d+)', expand=False) 
        # (\d+) extracts one or more digits and thus characters are removed
        store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors='coerce')

        store_data.dropna(subset=['staff_numbers'], inplace= True) #inplace True ensures the original df is modified
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
        #pattern = r'(\d+\.\d+|\d+)\s*x\s*(\d+\.\d+|\d+|)\s*(kg|g|ml|oz|\s)?\.?'
        #pattern = r'(\d+\.\d+|\d+)\s*x\s*(\d+\.\d+|\d+|)\s*(kg|g|ml|oz)?(?:\s|\.)?'
        #pattern = r'(\d+\.\d+|\d+)\s*x\s*(\d+\.\d+|\d+|)\s*(kg|g|ml|oz|\s)?\.?\s*$'
        

        
        
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
            else float(x[:-3]) * 0.001 if x.endswith('.') # '77g .'
            #else float(x[:-2])*0.001 if x.endswith('g .')
            #else float(x[:-2] + x[-1]) * 0.001 if re.match(r'\d+[a-z]+\s\.$', x) # there is one value with '77g .'
            else np.nan if pd.notna(x)
            #else np.NAN if pd.isna(x)
            else np.NAN)  ## we use np.NAN because it is a float value and allows operations
        
        # rounding the weight in kg to 4 decimals (some products weight around 0.001kg)
        df['weight'] = df['weight'].apply(lambda x: round(x, 4) if pd.isna(x) == False else np.NAN)



        return df
    
    def clean_products_data(self, df= pd.DataFrame):


        formats = ['%d %b %Y', '%Y %b %d','%Y %B %d','%Y-%m-%d', '%B %Y %d'] # the formats the data is in
        def convert_date(date_str):
            for fmt in formats:
                try: 
                    date_obj = pd.to_datetime(date_str, format= fmt)
                    if not pd.isna(date_obj):
                        return date_obj
                except:
                    pass
            return pd.NaT
        
        df['date_added'] = df['date_added'].apply(convert_date)
        
        df = df[~df['date_added'].isna()] # drop NA
        
        df['date_added'] = df['date_added'].dt.strftime('%Y-%m-%d')       

        df = df.drop_duplicates(subset= ['EAN', 'uuid', 'product_code'], keep = 'last')
        df = df.reset_index(drop= True)
        
        # remove non existing values
        df = df.dropna()

                

        return df
    
    
    def clean_orders_data(self, df = pd.DataFrame):
        
        df = df.drop(columns= ['first_name', 'last_name', '1'])
        
        ## a change that could be implemented is to delete the columns level_0 and
        ## index, as they have the same values and they work as indexes. 
        
        return df
    
    def clean_date_data(self, df = pd.DataFrame):

        #df = df.drop(columns= [None])
        
        # df just stores elements not containing string 'NULL' in the dataframe
        # axis = 1 to apply to each row, NOT operator to invert result, 
        # so that function returns TRUE for rows not containing the string 'NULL'
        df = df[df.apply(lambda row: not row.astype(str).str.contains('NULL').any(), axis=1)]
        
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

        return df
    
        
        

        
    













    
        




        




