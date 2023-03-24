import pandas as pd
import database_utils
import tabula
import requests
import boto3




#### note: I don't know where to place this dictionary
header_details = {
    'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
}

class DataExtractor:
    

    def __init__(self):

        
        self.DatabaseConnector = database_utils.DatabaseConnector()

        #### note: I don't know where to place this dictionary
        self.header_details = {
             'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
            }

        
        

    def read_rds_table(self):
        
        # get the name of the table containing user data
        table_name = self.DatabaseConnector.list_db_tables()

        # extract table containing user data and return as a pandas DataFrame
        table = pd.read_sql_table('name', table_name)


        return table
    
    def retrieve_pdf_data(self, pdf_path):
        #use tabula package to extract data from pdf document and convert it to pandas df
        
        dfs = tabula.read_pdf(pdf_path)
        
        return dfs
    

    def list_number_of_stores(self, number_of_stores_endpoint, header_details):

        ## header details haven't been used yet as get requests don't need the key??

        number_of_stores = requests.get(number_of_stores_endpoint)


        return number_of_stores
    

    # extracting all stores from the API saving them in pandas dataframe
    def retrieve_stores_data(self, retrieve_store_endpoint):

        stores = requests.get(retrieve_store_endpoint)
        df = pd.DataFrame(stores)

        return df
    

    def extract_from_s3(self, product_adress):

        s3_client = boto3.client('s3')
        #
        

        return df


    






hello = DataExtractor()
hello.retrieve_pdf_data("https://portal.theaicore.com/pathway/8955a07c-2223-4757-9f74-2aa287aa1aca#:~:text=document%20at%20following-,link,-.%0AThen%20return")
