import pandas as pd
import database_utils
import tabula



class DataExtractor:
    

    def __init__(self):

        
        self.DatabaseConnector = database_utils.DatabaseConnector()
        
        
        

    def read_rds_table(self):
        
        # get the name of the table containing user data
        table_name = self.DatabaseConnector.list_db_tables()

        # extract table containing user data and return as a pandas DataFrame
        table = pd.read_sql_table('name', table_name)


        return table
    
    def retrieve_pdf_data(self, pdf_path):
        #use tabula package to extract data from pdf document and convert it to pandas df
        
        dfs = tabula.read_pdf(pdf_path)
        
        return dfs





hello = DataExtractor()
hello.retrieve_pdf_data("https://portal.theaicore.com/pathway/8955a07c-2223-4757-9f74-2aa287aa1aca#:~:text=document%20at%20following-,link,-.%0AThen%20return")
