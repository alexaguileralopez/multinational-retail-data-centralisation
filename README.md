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

### Task 6: Extract and clean product details

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

### Task 7: Retrieve and clean the orders data:

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


## MILESTONE 3: CREATE THE DATABASE SCHEMA

### TASK 1:

For the first task, the datatypes from the orders_table should be changed:

|   orders_table   | current data type  | required data type |
|------------------|--------------------|--------------------|
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR(?)         |
| store_code       | TEXT               | VARCHAR(?)         |
| product_code     | TEXT               | VARCHAR(?)         |
| product_quantity | BIGINT             | SMALLINT           |


The queries for these changes were:

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE AS UUID
USING date_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE AS UUID
USING user_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE AS SMALLINT;

In order to get the largest number in the column, the following query was used in all cases where 
this needed to be done:

SELECT MAX(LENGTH(card_number)) AS max_length FROM orders_table;

This query returned the maximum length of the card_number (19), store_code(12), product_code(11), and their type was changed using:

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19);


## TASK 2

The changes to implement here were the following:

| dim_user_table | current data type | required data type |
| -------------- | ----------------- | ------------------ |
| first_name     | TEXT              | VARCHAR(255)       |
| last_name      | TEXT              | VARCHAR(255)       |
| date_of_birth  | TEXT              | DATE               |
| country_code   | TEXT              | VARCHAR(?)         |
| user_uuid      | TEXT              | UUID               |
| join_date      | TEXT              | DATE               |

The queries were exactly the same as before, but date_of_birth and join_date were different:

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE
USING date_of_birth::date;

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(3);


ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE
USING join_date::date;

For the country_code, the query mentioned in task 1 that selcts the length of the largest element returned a length of 3, which was implemented to the column:

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(3);

## TASK 3:

The changes to implement here were the following:

| store_details_table | current data type |   required data type   |
|---------------------|-------------------|------------------------|
| longitude           | TEXT              | FLOAT                  |
| locality            | TEXT              | VARCHAR(255)           |
| store_code          | TEXT              | VARCHAR(?)             |
| staff_numbers       | TEXT              | SMALLINT               |
| opening_date        | TEXT              | DATE                   |
| store_type          | TEXT              | VARCHAR(255) NULLABLE  |
| latitude            | TEXT              | FLOAT                  |
| country_code        | TEXT              | VARCHAR(?)             |
| continent           | TEXT              | VARCHAR(255)           |

This time, there were 2 latitude columns, one of which had to be dropped using:

ALTER TABLE dim_store_details
DROP COLUMN lat;

Also, there was a row that represented the business's website change the location column values where they're null to N/A. To do so, the longitude value [null] was used as a reference using the WHERE clause:

UPDATE dim_store_details
SET locality = 'N/A'
WHERE longitude IS NULL;

For the case where store_type is VARCHAR(255), no changes were made from previous queries using the same datatype as Postgre does allow nullable VARCHAR by default.

## TASK 4:

The first change to do in the dim_products table was to remove the pound sign from the column product_price. That was done using:

UPDATE dim_products SET product_price = REPLACE(product_price, '£', '')

In this task, a new column called weight_class had to be created, and the values were distributed between light, mid sized, heavy and truck required according to the table below:

| weight_class | weight range(kg) |
|--------------|-----------------|
| Light        | < 2             |
| Mid_Sized    | >= 2 - < 40     |
| Heavy        | >= 40 - < 140   |
| Truck_Required | >= 140        |

To do so, the following SQL code was used:

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_products
SET weight_class = 
  CASE 
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
  END;


## TASK 5:

The first subtask here, was to rename the column 'removed' to 'still_available'. It was done this way:

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

After all columns created and changed, the following changes of data type were made:

| dim_products | current data type | required data type |
|--------------|-------------------|--------------------|
| product_price | TEXT              | FLOAT              |
| weight        | TEXT              | FLOAT              |
| EAN           | TEXT              | VARCHAR(?)         |
| product_code  | TEXT              | VARCHAR(?)         |
| date_added    | TEXT              | DATE               |
| uuid          | TEXT              | UUID               |
| still_available | TEXT           | BOOL               |
| weight_class    | TEXT           | VARCHAR(?)         |

To do so, the queries implemented were the same ones as done earler. For the bool values, the following code was generated:

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE boolean 
USING (still_available = 'Still_available');

This updated the data type of the column still_available to boolean and converted the elements containing 'Still_available' to True and all other values to False.

## TASK 6:

For this task, the changes were made to the dim_date_times:

| dim_date_times | current data type | required data type |
|----------------|-------------------|--------------------|
| month          | TEXT              | VARCHAR(?)         |
| year           | TEXT              | VARCHAR(?)         |
| day            | TEXT              | VARCHAR(?)         |
| time_period    | TEXT              | VARCHAR(?)         |
| date_uuid      | TEXT              | UUID               |

The calculation of the VARCHAR(?) was done as usual:

SELECT MAX(LENGTH(month)) AS max_length FROM dim_date_times;

And the date_uuid:

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE AS UUID
USING date_uuid::UUID;

## TASK 7:

The task was to update the card details table:

|    dim_card_details    | current data type | required data type |
|------------------------|-------------------|--------------------|
| card_number            | TEXT              | VARCHAR(?)         |
| expiry_date            | TEXT              | VARCHAR(?)         |
| date_payment_confirmed | TEXT              | DATE               |

The length of each variable was calculated:

SELECT MAX(LENGTH(card_number)) AS max_length FROM dim_card_details;

And the changes were made with those values:

  ALTER TABLE dim_card_details
  ALTER COLUMN card_number TYPE AS VARCHAR(22);

## TASK 8:

In this task, the primary keys were added to each of the tables prefixed with dim. There is one of the headers in the orders_table that exists in one  dim-prefixed table. To update the columns in the dim tables with a primary key that matches the same column in the order table, the following was done:

  ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
  ALTER TABLE dim_date_times ADD PRIMARY KEY(date_uuid);
  ALTER TABLE dim_products ADD PRIMARY KEY(product_code);
  ALTER TABLE dim_store_details ADD PRIMARY KEY(store_code);
  ALTER TABLE dim_users ADD PRIMARY KEY(user_uuid);

## TASK 9:

In this task, the foreign keys in the orders_table were created to reference the primary keys in other tables. The foreign key constraints were created to reference the primary keys of the other table.

To check first if the keys could be created, the SQL JOIN clause was used to combine two tables based on the common column. 

  SELECT *
  FROM table1
  JOIN table2 ON table1.column1 = table2.column2
  WHERE table1.column1 = table2.column2;

  SELECT *
  FROM orders_table
  JOIN dim_users ON orders_table.user_uuid = dim_users.user_uuid
  WHERE dim_users.user_uuid NOT IN (
    SELECT DISTINCT user_uuid
    FROM orders_table
  );

  If this query returns no rows, that means the column from orders_table contains the user_uuid column from dim_users (or any column given). 


These foreign key constraints stablish a relationship between a column in the child column (dim tables) and a column in the parent table (orders_table or truth table). This will ensure that the values in the foreign key column of the child table always correspond to existing values in the primary key column of the parent table, or to a null value in the foreign key.

  ALTER TABLE orders_table
  ADD CONSTRAINT orders_table_date_uuid_fk
  FOREIGN KEY (date_uuid)
  REFERENCES dim_date_times (date_uuid);

  ALTER TABLE orders_table
  ADD CONSTRAINT orders_table_user_uuid_fk
  FOREIGN KEY (user_uuid)
  REFERENCES dim_users (user_uuid);

  ALTER TABLE orders_table
  ADD CONSTRAINT orders_table_card_number_fk
  FOREIGN KEY (card_number)
  REFERENCES dim_card_details (card_number);

In this last query, an error was found:
ERROR:  insert or update on table "orders_table" violates foreign key constraint "orders_table_card_number_fk"
DETAIL:  Key (card_number)=(4971858637664481) is not present in table "dim_card_details".

To solve it, the card_details original pdf with all the data was checked, and there were some values with '?' in the card number, which were eliminated using the following query:

  UPDATE dim_card_details SET card_number = REPLACE(price_column, '?', '')


  ALTER TABLE orders_table 
  ADD CONSTRAINT orders_table_store_code_fk
  FOREIGN KEY (store_code)
  REFERENCES dim_store_details (store_code);

  ALTER TABLE orders_table
  ADD CONSTRAINT orders_table_product_code_fk
  FOREIGN KEY (product_code)
  REFERENCES dim_products (product_code);


## MILESTONE 4: QUERYING THE DATA

Following the last milestone, the resulting database schema is:

![Database Star Schema](database_schema.png)


### TASK 1:
The Operations team would like to know which countries we currently operate in and which country now has the most stores. Perform a query on the database to get the information.

  SELECT country_code, COUNT(*) AS to_number_stores
  FROM dim_store_details
  GROUP BY country_code
  ORDER BY to_number_stores DESC;

This returns the following:

| country | total_no_stores |
|---------|----------------|
| GB      | 266            |
| DE      | 141            |
| US      | 34             |


### TASK 2:

The business stakeholders would like to know which locations currently have the most stores. They would like to close some stores before opening more in other locations. Find out which locations have the most stores currently. 

  SELECT locality, COUNT(*) AS total_number_stores
  FROM dim_store_details
  GROUP BY locality
  ORDER BY total_number_stores DESC
  LIMIT 7;

With a limit of 7, the result is:

| locality       | total_no_stores |
|----------------|----------------|
| Chapletown     | 14             |
| Belper         | 13             |
| Bushley        | 12             |
| Exeter         | 11             |
| High Wycombe   | 10             |
| Arbroath       | 10             |
| Rutherglen     | 10             |


## TASK 3:

Query the database to find out which months typically have the most sales.

First, selecting the months column and a total_sales column:

  SELECT dim_date_times.month, SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales

Then,

  FROM orders_table
  JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
  JOIN dim_products ON orders_table.product_code = dim_products.product_code
  GROUP BY dim_date_times.month
  ORDER BY total_sales DESC;

August is the month with the most sales, followed by January and October. The last month is February, followed by April and November.

| month | total_sales        |
|-------|--------------------|
| 8     | 673295.6799999983  |
| 1     | 668041.4499999986  |
| 10    | 657335.8399999985  |
| 5     | 650321.4299999985  |
| 7     | 645741.699999999   |
| 3     | 645462.9999999991  |
| 6     | 635578.9899999985  |
| 12    | 635329.0899999985  |
| 9     | 633993.6199999992  |
| 11    | 630757.0799999996  |
| 4     | 630022.7699999996  |
| 2     | 616452.9899999991  |


## TASK 4:

The company is looking to increase its online sales. They want to know how many sales are happening online vs offline. Calculate how many products were sold and the amount of sales made for online and offline purchases.


  SELECT
    CASE
      WHEN dim_store_details.store_type = 'Web Portal' THEN 'ONLINE'
      ELSE 'OFFLINE'
    END AS location,
    COUNT(*) AS total_sales,
    SUM(orders_table.product_quantity) AS number_sales
    
  FROM 
    orders_table 
    INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
  GROUP BY
    location;


![Result table](task_4.png)

The 'SELECT' clause specifies that the columns that will be shown are 'location', distinguishing between online and offline, total_sales as a count of each time that sales are online or offline, and number of sales, as the sum of product quantity in online or offline sales. 

The 'INNER JOIN' connects the tables dim_store_details and orders_table by 'store_code', and 'GROUP BY' groups the result set by the 'location' column. Hence, the functions COUNT and SUM operate on distinct sales types separately. 


## TASK 5:

The sales team wants to know which of the different store types is generated the most revenue so they know where to focus. Find out the total and percentage of sales coming from each of the different store types.

  SELECT 
    dim_store_details.store_type,
    COUNT(*) AS total_sales,
    ROUND(COUNT(*) * 100.0/SUM(COUNT(*)) OVER (),2) AS percentage_total
    
  FROM 
    orders_table 
    INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
  GROUP BY
    dim_store_details.store_type
  ORDER BY percentage_total DESC;

The query returns:

![Result table 5](task_5.png)

 'COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()' calculates the ratio of the count of rows for each sales type to the total count of all rows in the result set.

 ## TASK 6:

The company stakeholders want assurances that the company has been doing well recently. Find which months in which years have had the most sales historically.

By selecting information from orders_table, dim_products, and dim_date_times, it is possible to retrieve the necessary information to find the best sales month for each year. 



  SELECT year, month, ROUND(sales_amount::numeric, 2) AS total_sales
  FROM (
      SELECT dim_date_times.year, dim_date_times.month, SUM(orders_table.product_quantity * dim_products.product_price) AS sales_amount,
            ROW_NUMBER() OVER (PARTITION BY dim_date_times.year ORDER BY SUM(orders_table.product_quantity * dim_products.product_price) DESC) AS rn
      FROM dim_date_times 
      JOIN orders_table  ON orders_table.date_uuid = dim_date_times.date_uuid
      JOIN dim_products ON dim_products.product_code = orders_table.product_code
      GROUP BY dim_date_times.year, dim_date_times.month
  ) subquery
  WHERE rn = 1
  ORDER BY total_sales DESC
  LIMIT 10;

![Task 6 Result](task_6.png)

Here, a subquery is needed to get the sales amount by year and month.
Multiplying the quantity of products sold ('orders_table.product_quantity') with their respective prices ('dim_products.product_price) gives the sales amount per order. Grouping the sales by year and month gives the total sales amount for each month in each year. 

Using ROW_NUMBER(), each month within a year is given a row, being the one with the highest sales amount row number 1 (rn = 1).
In this case, rows are partitioned by the values in the 'year' column of the dim_date_times table.

Taking the results from the last subquery:

Sales amount, year, and month are selected, and filtering the results where row number (rn) is equal to 1, the result obtained is the best month per year in terms of sales.


## TASK 7:
The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.

SELECT country_code, SUM(staff_numbers) AS total_staff_numbers
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

![Task 7 Result](task_7.png)

## TASK 8:

The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.

This is done by selecting store type, country code, and sales while stablishing country_code = 'DE'

SELECT  store_type, country_code, ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric,2) AS sales

  FROM orders_table
  JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
  JOIN dim_products ON orders_table.product_code = dim_products.product_code
  WHERE country_code = 'DE'
  GROUP BY store_type, country_code
  ORDER BY sales ASC;

![Task 8 Result](task_8.png)

## TASK 9:

Sales would like to get an accurate metric for how quickly the company is making sales.
Determine the average time taken between each sale grouped by year.

  SELECT
    year::integer,
    json_build_object(
      'hours', AVG(EXTRACT(HOUR FROM time_diff))::integer,
      'minutes', AVG(EXTRACT(MINUTE FROM time_diff))::integer,
      'seconds', AVG(EXTRACT(SECOND FROM time_diff))::integer,
    'milliseconds', AVG(EXTRACT(MILLISECOND FROM time_diff))::integer
    ) AS avg_time_diff
  FROM (
    SELECT
      year::integer,
      CASE WHEN LEAD(timestamp) OVER (PARTITION BY year::integer ORDER BY month::integer, day::integer, timestamp) >= timestamp
        THEN LEAD(timestamp) OVER (PARTITION BY year::integer ORDER BY month::integer, day::integer, timestamp) - timestamp
        ELSE timestamp - LEAD(timestamp) OVER (PARTITION BY year::integer ORDER BY month::integer, day::integer, timestamp)
      END AS time_diff
    FROM dim_date_times
  ) subquery
  GROUP BY year
  ORDER BY AVG(EXTRACT(EPOCH FROM time_diff)) DESC;

The most important query here, is the subquery where the use of LEAD is put into practice. This function allows to access the value of a column from a subsequent row in the same result set. It is used to retrieve the next timestamp value within each group defined by the 'PARTITION BY' clause.

LEAD(timestamp) OVER(PARTITION BY year ORDER BY month, day, timestamp) would give the next timestamp value within each group defined by the PARTITION BY clause and ordered by month, day, and timestamp. Hence, if the next timestamp is subtracted from the present one, the result is the time difference between ordered timestamps. 

This query obtains the difference between all of the timestamps ordered, and can lead to errors because some of the results are negative. To prevent that, the use of CASE is put into practice to define the case where timestamp difference is negative or positive and take its absolute value.

![Task 9 Result 1](task_9_1.png)

Another, and better approach is to use the year, month, day columns to create a new column called complete_timestamp. Then, take the difference between consecutive complete_timestamp values and average them per year. 

  SELECT
    year,
    CAST(AVG(time_diff) AS INTERVAL) AS avg_time_diff 
  FROM (
    SELECT
      year,
      month,
      day,
      timestamp,
      TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS') AS complete_timestamp,
      LEAD(TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS')) OVER (PARTITION BY year ORDER BY year, month, day, timestamp) - TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS') AS time_diff
    FROM dim_date_times
  ) subquery
  GROUP BY year
  ORDER BY avg_time_diff DESC;

![Task 9 Result 2](task_9_2.png)


The final and best approach is to use that TO_TIMESTAMP function where day, month and year are taken as integers. After that, year and the average of time difference grouped per year are taken (as an interval type object). Finally, from that interval object, a json object is built extracting the HOUR, MINUTE and SECOND from the interval. 

SELECT
  year,
  json_build_object(
    'hours', EXTRACT(HOUR FROM time_diff_year),
    'minutes', EXTRACT(MINUTE FROM time_diff_year),
	'seconds', EXTRACT(SECOND FROM time_diff_year)) AS avg_time_diff_json
FROM (
  SELECT
    year,
    AVG(time_diff) AS time_diff_year
  FROM (
    SELECT
      year,
      timestamp,
      LEAD(TO_TIMESTAMP(year::integer || '-' || month::integer || '-' || day::integer || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS')) OVER (PARTITION BY year::integer ORDER BY year::integer, month::integer, day::integer, timestamp) - TO_TIMESTAMP(year::integer || '-' || month::integer || '-' || day::integer || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS') AS time_diff
    FROM dim_date_times
  ) subquery
  GROUP BY year
) AS avg_time_diff
ORDER BY time_diff_year DESC;


![Task 9 Final Result](task_9_3.png)
 