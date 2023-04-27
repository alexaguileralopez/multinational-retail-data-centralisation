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
    

    def list_number_of_stores(self, url='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header = 'header_details.yaml'):

        with open(header) as f:
            header= yaml.safe_load(f)

        response = requests.get(url, headers= header)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            return f'Error {response.status_code}'
    

    # extracting all stores from the API saving them in pandas dataframe
    def retrieve_stores_data(self, url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', header = 'header_details.yaml'):

        #pd dataframe where data is going to be stored
        df = pd.DataFrame()
        #get header details
        with open(header) as f:
            header = yaml.safe_load(f)

        number_stores = self.list_number_of_stores()
        for i in range(0,number_stores):
            store_data = requests.get(url.format(store_number = i), headers= header).json()
            store_data = json_normalize(store_data)
            df = pd.concat([df, store_data])

        return df
    
    # product info is stored in s3 bucket on AWS in CSV format
    def extract_from_s3(self):

        client = boto3.client('s3')

        path = 's3://data-handling-public/products.csv'

        df = pd.read_csv(path)


        return df


    






