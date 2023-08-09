import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
import sqlalchemy as text
from pandas import json_normalize
import yaml


class DataExtractor:
    
    ''' Class used to extract data from various sources including PDF documents; 
    an AWS RDS database; RESTful API, JSON and CSV files.'''

    def __init__(self, database_connector_instance=DatabaseConnector()):

        self.database_connector_instance = database_connector_instance 
        
    def read_rds_table(self, table_name):

        """
        Read data from an RDS database table and convert it to a DataFrame.
        This method reads data from the specified RDS database table using the provided DatabaseConnector instance.

        Notes:
            - The method lists available tables in the dataset using the DatabaseConnector instance.
            - An SQL engine is initialized using the DatabaseConnector instance.
            - The specified table's data is extracted and converted to a DataFrame.
            - The resulting DataFrame is returned.

        """
                     
        tables = self.database_connector_instance.list_db_tables()
        print(tables) # prints out the different tables in the dataset

        engine = self.database_connector_instance.init_db_engine()
        with engine.connect() as conn:
            df = pd.read_sql_table(table_name, conn)

        return df
    

    def retrieve_pdf_data(self, pdf_path):

        """
        Retrieve information from a PDF and convert it to a pandas DataFrame.
        This method utilizes the tabula library to extract information from a PDF and convert it to a DataFrame.
        Notes:
            - The tabula library is used to read the PDF and extract data.
            - Data is extracted from all pages specified using the 'pages' parameter.
            - Extracted DataFrames are concatenated into a single DataFrame.
            - The resulting DataFrame is returned.

        """
        
        dfs = tabula.read_pdf(pdf_path, pages= 'all')
        dfs = pd.concat(dfs)
        
        return dfs
    

    def list_number_of_stores(self):

        """
        List the number of stores from a RESTful API.
        This method queries a RESTful API to retrieve the number of stores and returns the count.

        Notes:
            - The header details are loaded from the specified YAML file.
            - A GET request is made to the provided URL using the header.
            - If the response status code is 200, the number of stores from the JSON response is returned.
            - If the response status code is not 200, an error message with the status code is returned.
        """

        with open(header) as f:
            header= yaml.safe_load(f)

        response = requests.get(url, headers= header)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            return f'Error {response.status_code}'
    

    def retrieve_stores_data(self, url, header):

        """
        Retrieve stores data from an API and save it in a pandas DataFrame.
        This method queries an API to retrieve stores data and saves it in a pandas DataFrame.

        Notes:
            - The header details are loaded from the specified YAML file.
            - The 'list_number_of_stores' method is used to determine the number of stores.
            - A loop iterates over store numbers and requests data for each store from the API.
            - The extracted store data is normalized using 'json_normalize'.
            - Data for all stores is concatenated into a single DataFrame and returned.

        """

        df = pd.DataFrame()
        
        with open(header) as f: #get header details
            header = yaml.safe_load(f)

        number_stores = self.list_number_of_stores( url='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', 
                                                   header = 'header_details.yaml')
        for i in range(0,number_stores):
            store_data = requests.get(url.format(store_number = i), headers= header).json()
            store_data = json_normalize(store_data)
            df = pd.concat([df, store_data])

        return df

    def extract_from_s3(self, path):

        """
        Extract product information stored in an S3 bucket on AWS from a CSV file.
        This method uses the AWS SDK and pandas library to extract product information from a CSV file in an S3 bucket.
        """

        client = boto3.client('s3')
        df = pd.read_csv(path, index_col= 0) 
        return df
    
    def extract_date_details(self, url):

        """
        Extract date details from a URL.
        This method extracts date details from a provided URL. If the response status code is 200,
        it reads the JSON data from the URL and returns a DataFrame with the details.

        """
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_json(url)
            return df
        else:
            return f'Error {response.status_code}'
            



    






