import psycopg2
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd


class DatabaseConnector:

    def __init__(self):

        
        
        pass

    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            creds = yaml.safe_load(f)

            #returns dictionary of credentials
        return creds
        
    def init_db_engine(self):
        
        #calls read db creds method to get credentials from yaml file
        HOST = self.read_db_creds().get('RDS_HOST')
        USER = self.read_db_creds().get('RDS_HOST')
        PASSWORD = self.read_db_creds().get('RDS_PASSWORD')
        DATABASE = self.read_db_creds().get('RDS_DATABASE')
        PORT = self.read_db_creds().get('RDS_PORT')
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'

        #creates engine with the data supplied by the creds dictionary
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine


    def init_db_tables(self):
        
        #receive engine created in method to initialise it
        engine = self.init_db_engine()
        # connect to engine
        engine.connect()
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        engine.execute('''SELECT *''').fetchall()

        

        
hello = DatabaseConnector()
hello.init_db_tables()









