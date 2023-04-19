import pandas as pd
import database_utils
import tabula
import requests
import boto3
import sqlalchemy as text
from pandas import json_normalize
import yaml






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
    
    # installed tabula in conda as well 
    def retrieve_pdf_data(self, pdf_path):
        #use tabula package to extract data from pdf document and convert it to pandas df
        
        dfs = tabula.read_pdf(pdf_path, pages= 'all')
        # dfs is now a list with 279 dataframes (each page)
        dfs = pd.concat(dfs)
        # dfs is now a pandas dataframe containing all the elements
        
        return dfs
    

    def list_number_of_stores(self, number_of_stores_endpoint, header_details):

        ## header details haven't been used yet as get requests don't need the key??
        
        number_of_stores = requests.get(number_of_stores_endpoint, header_details)


        return number_of_stores.json()['number_stores']
    

    # extracting all stores from the API saving them in pandas dataframe
    def retrieve_stores_data(self, retrieve_store_url):

        #pd dataframe where data is going to be stored
        df = pd.DataFrame()
        #get header details
        with open('header_details.yaml') as f:
            header_details = yaml.safe_load(f)

        
        number_stores = self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header_details= header_details)
        for i in range(0,number_stores):
            store_data = requests.get(retrieve_store_url.format(store_number = i), headers= header_details).json()
            store_data = json_normalize(store_data)
            df = pd.concat([df, store_data])

        return df
    
    # product info is stored in s3 bucket on AWS in CSV format
    def extract_from_s3(self, product_adress):

        # download and extract the info returning pandas dataframe
        s3_client = boto3.client('s3')
        
        # reading prduct address, which should contain a csv file
        df = pd.read_csv(product_adress)


        return df


    






