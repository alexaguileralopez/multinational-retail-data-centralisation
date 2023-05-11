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

    #stablish a date format and apply it to the 2 columns containing dates
        date_format = "%Y-%m-%d"
        clean_user_data["date_of_birth"] = pd.to_datetime(clean_user_data["date_of_birth"], format= date_format, errors= 'coerce')
        clean_user_data["join_date"] = pd.to_datetime(clean_user_data["join_date"], format= date_format, errors='coerce')

The last step is to eliminate NULL values and reset the index:

    clean_user_data.dropna()
    clean_user_data.reset_index(drop= True, inplace= True)

After that, a method in the database connector is created to upload the dataframe to the Sales Data database created earlier. The method takes a dataframe and a table name as arguments, to store that dataframe with that name in the Sales Data database:

    postgres_str = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    cnx = create_engine(postgres_str)
    print("Engine created")
    dataframe.to_sql(table_name, con = cnx, index=False, if_exists= 'replace')
    print("Table uploaded to Sales Data")


