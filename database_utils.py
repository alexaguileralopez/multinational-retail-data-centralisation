import psycopg2
from psycopg2 import Error
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import pandas as pd
import mysql 

class DatabaseConnector:


    def read_db_creds(self):

        """
        Obtain the credentials of the local database.
        This method reads database credentials from a YAML file.

        """

        with open('creds_and_testing/db_creds.yaml') as f:
            creds = yaml.safe_load(f)
        return creds
        
    def init_db_engine(self):
        
        """
        Initialize a database engine using the obtained credentials.
        This method creates a database engine using the credentials obtained from the credentials dictionary.

        """

        creds = self.read_db_creds()
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine


    def list_db_tables(self):

        """
        List all tables in the connected database.
        This method uses the engine to select tables from an external database and returns a list of table names.

        """
        engine = self.init_db_engine()
        with engine.connect() as connection:

            result = connection.execute(text("""SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' """)) # Selects all tables from dataset
            result = result.fetchall()

            return result

    def upload_to_db(self, dataframe = pd.DataFrame, table_name=str):

        """
        Upload a pandas DataFrame to the connected database.
        This method takes a pandas DataFrame and uploads it to the connected PostgreSQL database.

        """

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



        



    
    

    










