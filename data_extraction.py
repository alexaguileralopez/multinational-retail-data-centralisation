import pandas as pd
import database_utils
import tabula
import requests
import boto3
import sqlalchemy as text




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

        
        
    # method reads database from RDS
    def read_rds_table(self, database_connector_instance = database_utils.DatabaseConnector(), table_name = str):
        
        # get the name of the table containing user data
        
        tables = database_connector_instance.list_db_tables()

        print(tables) # prints out the different tables in the dataset

        engine = database_connector_instance.init_db_engine()

        with engine.connect() as conn:
            # extracts the sql table wanted and converts it to pd dataframe 
            df = pd.read_sql_table(table_name, conn)

        return df
    
    
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
        df = pd.DataFrame(stores) # is this necessary or does the previous one already return a pd dataframe?

        return df
    
    # product info is stored in s3 bucket on AWS in CSV format
    def extract_from_s3(self, product_adress):

        # download and extract the info returning pandas dataframe
        s3_client = boto3.client('s3')
        
        # reading prduct address, which should contain a csv file
        df = pd.read_csv(product_adress)


        return df


    






