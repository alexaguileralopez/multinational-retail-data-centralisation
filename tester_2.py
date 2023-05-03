##from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

# trying to clean products data

df1 = DataExtractor().extract_from_s3()
print('Data Extraction gone well') 
df2 = DataCleaning().convert_product_weights(df1)
print('Weight conversion gone well')
nans = df2.isna().sum()
print(nans)
df_new = DataCleaning().clean_products_data(df2)
#print(df2)

nans = df_new.isna().sum()

print(nans)

DatabaseConnector().upload_to_db(dataframe= df_new, table_name= 'dim_products')
