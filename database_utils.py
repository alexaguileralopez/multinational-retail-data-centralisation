import psycopg2
import yaml
from sqlalchemy import create_engine
import pandas as pd


class DatabaseConnector:

    def __init__(self):
        
        self.creds = {}
        pass

    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            self.creds = yaml.safe_load(f)

            #returns dictionary of credentials
        return self.creds
        
    def init_db_engine(self):
        
        HOST = self.creds['RDS_HOST']
        USER = self.creds['RDS_USER']
        PASSWORD = self.creds['RDS_PASSWORD']
        DATABASE = self.creds['RDS_DATABASE']
        PORT = self.creds['RDS_PORT']
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'

        #connect to postgres database
        with psycopg2.connect(dbname = DATABASE, user = USER,password=PASSWORD, host=HOST, port=PORT) as conn:
            
            #create engine
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
            #connect engine
            engine.connect()


    def init_db_tables(self):
        
        pass

    

hello = DatabaseConnector()
hello.read_db_creds()
print(hello.creds)





