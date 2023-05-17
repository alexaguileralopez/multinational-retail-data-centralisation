# multinational-retail-data-centralisation
Data Handling scenario from AiCore course.

This project presents a scenario where a multinational company that sells various goods around the globe needs help in becoming more data-driven and make its sales data accessible from one centralised location. 
Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. The goal of the project is to produce a system that stores the current company data in a database so that it is accessed from one centralised location and acts as a single source of truth for sales data. After that, by querying this database, anyone can obtain up-to-date metrics for the business.

## Milestone 2:

The first mission is to extract all the data from the multitude of data sources, and then store it in a database that will be created.

### Task 1:

The first step is to set up a new database within pgadmin4, and name it sales_data. This is the database that will store the extracted information. 

### Task 2:

In this taks, 3 classes are created.
The first one, DataExtractor, contained in data_extraction.py is a class that contains methods to extract the data from various data sources such as CSV files, pdf files, an API and a s3 bucket.
The second one, DatabaseConnector, contained in database_utils.py, is the class that is used to connect to the database and upload the dataframes to it.
The third, DataCleaning, contained in data_cleaning.py, contains methods that clean the data extracted. 

### Task 3: Extract the user data

The user data is stored in a AWS RDS database. To access it, a .yaml containing the credentials needs to be created. 

After that, a method called read_db_creds within the DatabaseConnector is created. This method reads the credentials of the yaml file and returns a dictionary with those credentials.

Another method called init_db_engine is created to generate an sqlalchemy database engine that takes in the last method, and creates an engine with those credentials.

In the database, there are 3 tables from where we can extract information. A method within the Data Extractor called list_db_tables is created to list those tables.

In order to obtain the information from that table, a method in the Data Extraction class needs to be created. read_rds_table is a method that takes a Database Connector and a table name and creates an engine to extract the table with the table name of choice. Making use of pandas:

    engine = database_connector_instance.init_db_engine()

        with engine.connect() as conn:
            # extracts the sql table wanted and converts it to pd dataframe 
            df = pd.read_sql_table(table_name, conn)

        return df 

It returns a pandas dataframe.

In order to clean that extracted data from RDS, a method in the Data Cleaning class needs to be created. clean_user_data is the name of the method that makes use of various properties of pandas dataframes to clean the data. 
The data has various columns:

index,first_name,last_name,date_of_birth,company,email_address,address,country,country_code,phone_number,join_date,user_uuid.

The first step taken, is to eliminate those rows that contain duplicates in certain columns:

    clean_user_data = user_data.drop_duplicates(subset= ['email_address', 'address', 'phone_number', 'user_uuid'], keep = 'last').reset_index(drop = True)

A user only has a unique email adress, adress, phone number and user id. This way, those who are duplicated are eliminated, and just one of the rows is left in the dataframe.

In addition, the index column is dropped and, in the adress text, the new lines are removed in order to smooth the reading:

    clean_user_data.drop(columns= ['index'], inplace= True)
    clean_user_data['address'] = clean_user_data['address'].replace('\n', ' ', regex= True)

A date format is stablished and applied to eliminate the incorrect data in those columns:

    formats = ['%d %b %Y', '%Y %b %d','%Y %B %d','%Y-%m-%d', '%B %Y %d' ] # the formats the data is in
    def convert_date(date_str):
        for fmt in formats:
            try: 
                date_obj = pd.to_datetime(date_str, format= fmt)
                if not pd.isna(date_obj):
                    return date_obj
            except:
                pass
        return pd.NaT
    
This function is defined inside all of the methods that clean columns containing dates. An improvement here could be to stablish this function as a class method, with the formats as arguments. That way, it would just be necessary to call the method inside other methods instead of defining the function within each of the cleaning methods. 
        
Following that last step, the function is applied to those columns containing dates, and na are removed from those columns, thus cleaning the rows that contain incorrect data about dates. Later, strftime('%Y-%m-%d') converts these date time objects to strings with the specified format. 

    clean_user_data['date_of_birth'] = clean_user_data['date_of_birth'].apply(convert_date)
    clean_user_data['join_date'] = clean_user_data['date_of_birth'].apply(convert_date)

    # remove na from dates 
    clean_user_data = clean_user_data[~clean_user_data['date_of_birth'].isna()]
    clean_user_data = clean_user_data[~clean_user_data['join_date'].isna()]

    clean_user_data['date_of_birth'] = clean_user_data['date_of_birth'].dt.strftime('%Y-%m-%d')
    clean_user_data['join_date'] = clean_user_data['join_date'].dt.strftime('%Y-%m-%d')

The rows containing 'na' are removed from the dataset:

    clean_user_data.dropna(subset=['date_of_birth'], inplace= True)
    clean_user_data.dropna(subset=['join_date'], inplace= True)

After that, a method in the database connector is created to upload the dataframe to the Sales Data database created earlier. The method takes a dataframe and a table name as arguments, to store that dataframe with that name in the Sales Data database:

    postgres_str = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    cnx = create_engine(postgres_str)
    print("Engine created")
    dataframe.to_sql(table_name, con = cnx, index=False, if_exists= 'replace')
    print("Table uploaded to Sales Data")

The table is uploaded as 'dim_user_details'.

### Task 4: Extracting users and cleaning card details

By making use of the package tabula, information can be extracted from a pdf document. In this case, card_details.pdf:

    dfs = tabula.read_pdf(pdf_path, pages= 'all')
    # dfs is now a list with 279 dataframes (each page)
    dfs = pd.concat(dfs)
    # dfs is now a pandas dataframe containing all the elements

This method extracts all pages from the pdf and concatenates them to create a unique dataframe, later to be cleaned.

To clean this dataframe, a lambda function is applied to drop all those rows that contain the string 'NULL' in any of their elements:

    card_data = card_data[card_data.apply(lambda row: not row.astype(str).str.contains('NULL').any(), axis=1)]

Then, the convert_date function is applied to take care of the 'expiry_date' and 'date_payment_confirmed' columns, and they are transformed to strings. 

Finally, the dataframe is uploaded to the database using upload_to_db, it goes by the name dim_card_details:

    def upload_to_db(self, dataframe = pd.DataFrame, table_name=str):

### Task 5: Extract and clean the details of each store:

In this task, the data is retrieved through the use of an API. A method in the Data Extractor called list_number_of_stores is created to obtain the information about the number of stores from an endpoint, and another method called retrieve_stores_data retrieves the information from each store's endpoint. 

Creating a yaml file by the name of header_details.yaml, the key to the API is stored. This yaml file is loaded and stored as a string. 

The first method makes use of the requests library, the method 'get' retrieves the necessary information. If it works, the method returns the value 'number of stores' within the json file retrieved from the API. 

    def list_number_of_stores(self, url='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header = 'header_details.yaml'):

        with open(header) as f:
            header= yaml.safe_load(f)

        response = requests.get(url, headers= header)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            return f'Error {response.status_code}'

This returns a value of 451 stores, which is then used in the next method to retrieve the information of all stores. This method is called within the retrieve_stores_data and the value 451 is used in a for loop to extract all stores information.

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

From this dataframe, the previously mentioned function convert_date is applied to the column 'opening_date'. 
To clean the rest of the data, the method 'extract' is used to extract numeric values from the column 'staff_numbers':

    #dropping those rows that contain invalid values for number of staff members
    store_data['staff_numbers'] = store_data['staff_numbers'].str.extract('(\d+)', expand=False)
    # (\d+) extracts one or more digits and thus characters are removed
    store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors='coerce')

After that, duplicates from columns such as address or store_code are removed, and the structure of the address is improved by removing '\n'.

After that, the table is uploaded to the database by the name dim_store_details.

## Task 6: Extract and clean product details

For this task, the data is extracted from an AWS S3 bucket. 
Creating a method called extract_from_s3 in the Data Extractor, a csv file is extracted:

    def extract_from_s3(self):

        client = boto3.client('s3')

        path = 's3://data-handling-public/products.csv'

        df = pd.read_csv(path, index_col= 0) #index col = 0 to get rid of an extra index column

In the data cleaning process, the first step is to transform the weight column of the table to the same unit, in this case kg. To do so, regular expressions are used to define what kind of data formats can be encountered in this column.
Weight can be in: kg, ml, g, units x g, oz,... A conversion dictionary is created referencing kg as the main unit:

    # checking the dataframe, we can see that the units that are used are the following:
    conversion_dict = {'kg': 1, 'g': 0.001, 'ml': 0.001, 'oz': 0.0283495}

For the case in which data is stored as units x g, the following regular expression is used:

    pattern = r'(\d+\.\d+|\d+)\s*x\s*(\d+\.\d+|\d+)\s*(kg|g|ml|oz)?'

The first section (\d+\.\d+|\d+), matches a number with decimal places or an integer. The second (\s*x\s*) matches the letter 'x' surrounded by optional whitespace characters (\s). It allows for variations in spacing around the 'x' character. The third, (\d+\.\d+|\d+), matches another number (decimal or integer). Lastly, the pattern looks for any of the units mentioned earlier. 

A lambda function is applied to the weight, to search for the pattern groups and convert to float kg units. The rest of the conditions account for the cases in which the units are specified. 

    df['weight'] = df['weight'].apply(lambda x: float(re.search(pattern, x).group(1)) * float(re.search(pattern, x).group(2)) * conversion_dict.get(re.search(pattern, x).group(3), 0.001) if ' x ' in x and pd.notna(x)
        
        # the rest of the expressions look for the units with 'endswith' 
        else float(x[:-2]) if x.endswith('kg') 
        else float(x[:-2])*0.001 if x.endswith('ml') 
        else float(x[:-1])*0.001 if x.endswith('g') 
        else float(x[:-2])*0.0283495 if x.endswith('oz')
        else float(x[:-3]) * 0.001 if x.endswith('.') # '77g .'
        #else float(x[:-2])*0.001 if x.endswith('g .')
        #else float(x[:-2] + x[-1]) * 0.001 if re.match(r'\d+[a-z]+\s\.$', x) # there is one value with '77g .'
        else np.nan if pd.notna(x)
        #else np.NAN if pd.isna(x)
        else np.NAN)  ## we use np.NAN because it is a float value and allows operations

In the final part, the weight is rounded to 4 decimals:

    df['weight'] = df['weight'].apply(lambda x: round(x, 4) if pd.isna(x) == False else np.NAN)

This data is passed onto another method called clean_products_data, which uses convert_date to correct the column with 'date_added', drops duplicates from column with unique identifiers and codes, and drops 'na' values.

Later, this dataframe is uploaded to the database by the name of 'dim_products'.

## Task 7: Retrieve and clean the orders data:

The orders table which acts as the single source of truth for all orders the company has made in the past is stored in a database on AWS RDS.
Using list_db_tables, and read_rds_table, the orders_table is extracted. 

    def clean_orders_data(self, df = pd.DataFrame):
        
        df = df.drop(columns= ['first_name', 'last_name', '1'])
        
        ## a change that could be implemented is to delete the columns level_0 and
        ## index, as they have the same values and they work as indexes. 
        
        return df

Columns 'first_name', 'last_name' and '1' are removed, and the dataframe is returned and uploaded to the database as orders_table.

## Task 8: Retrieve and clean the date events data:

The events data is stored in a json file, and so the method to retrieve the information is the following:

    def extract_date_details(self, url ='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_json(url)
            return df
        else:
            return f'Error {response.status_code}'

The data then is cleaned with the following steps in the clean_date_data method:

    # the following function transforms the timestamp to time object, and sets nan to those values that do not match the format
    df['timestamp'] = df['timestamp'].apply(lambda x: datetime.strptime(x, '%H:%M:%S').time() 
                                    if isinstance(x, str) and re.match(r'\d{2}:\d{2}:\d{2}', x) 
                                    # using regular expressions to match 2 digits followed by colon
                                    else np.nan)


After that, duplicates are removed, and the data types are changed to their most suitable types to see changes in storage:

    df.info() # 120146 entries, it has reduced
    df = df.drop_duplicates(subset= ['date_uuid'], keep= 'last').reset_index(drop=True)
    df['year'] = df['year'].astype('int')
    df['day'] = df['day'].astype('int')
    df['time_period'] = df['time_period'].astype('str')
    df['month'] = df['month'].astype('str')
    df['date_uuid'] = df['date_uuid'].astype('str') # memory usage is reduced by 1 MB
    df = df.dropna() 

    print('After looking for NULL strings:\n')
    df.info() # 120123 entries, it has reduced

Lastly, the dataframe is uploaded to the database as dim_date_times.


 