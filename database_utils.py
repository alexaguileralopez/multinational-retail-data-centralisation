import psycopg2
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import pandas as pd
import data_extraction 
import data_cleaning


class DatabaseConnector:

    def __init__(self):
        pass

    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            creds = yaml.safe_load(f)

            #returns dictionary of credentials
        return creds
        
    def init_db_engine(self):
        
        # call read db creds once and get credentials
        creds = self.read_db_creds()
        #calls read db creds method to get credentials from yaml file
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'

        #creates engine with the data supplied by the creds dictionary
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine


    def list_db_tables(self):
        
        #receive engine created in method to initialise it
        engine = self.init_db_engine()
        # connect to engine
        with engine.connect() as connection:
            #selects all tables from sales data (doesn't exist yet)
            result = connection.execute(text("SELECT * FROM sales_data"))
            return result

    def upload_to_db(self):

        dim_users = self.list_db_tables()

        dim_card_details = data_cleaning.DataCleaning().clean_card_data()

        dim_store_details = data_cleaning.DataCleaning().clean_store_data()

        return dim_users, dim_card_details, dim_store_details
    
    

    










