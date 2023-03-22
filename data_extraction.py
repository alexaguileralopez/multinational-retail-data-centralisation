import pandas as pd
import database_utils


class DataExtractor:
    

    def __init__(self):

        
        self.DatabaseConnector = database_utils.DatabaseConnector()
        

    def read_rds_table(self):
        
        # get the name of the table containing user data
        table_name = self.DatabaseConnector.list_db_tables()

        # extract table containing user data and return as a pandas DataFrame
        table = pd.read_sql_table('name', table_name)


        return table


hello = DataExtractor()
hello.read_rds_table()


