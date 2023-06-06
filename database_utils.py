import psycopg2
from psycopg2 import Error
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import pandas as pd
## from data_cleaning import DataCleaning
## from database_utils import DatabaseConnector
import mysql 



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
            #selects all tables from dataset
            result = connection.execute(text("""SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' """))


            result = result.fetchall()
            # returns list of all tables inside the database
            return result
        

    # to make use of this method, it is necessary to provide a dataframe (from data cleaning),
    # the table name that it is going to be saved as
    def upload_to_db(self, dataframe = pd.DataFrame, table_name=str):

        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = '************'
        DATABASE = 'Sales_Data'
        PORT = 5432
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        
        postgres_str = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        cnx = create_engine(postgres_str)
        print("Engine created")
        dataframe.to_sql(table_name, con = cnx, index=False, if_exists= 'replace')
        print("Table uploaded to Sales Data")



        



    
    

    










