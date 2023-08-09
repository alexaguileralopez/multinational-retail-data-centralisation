-- DOCUMENT WITH THE SQL COMMANDS TO BUILD THE DATABASE SCHEMA

-- Modifying the data types in orders_table

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE AS UUID
USING date_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE AS UUID
USING user_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE AS SMALLINT;

SELECT MAX(LENGTH(card_number)) AS max_length FROM orders_table; -- this command is used to get the maximum length of the card_number, 
                                                                 --later used to change its type VARCHAR(19)

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19);

-- Modifying data types in dim_users

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

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(3); -- again, finding the largest value and applying it

-- Modifying data types in dim_store_details

ALTER TABLE dim_store_details
DROP COLUMN lat; --useless column

UPDATE dim_store_details
SET locality = 'N/A'
WHERE longitude IS NULL; --changing N/A to null wherever longitude value is null

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR (255);

ALTER TABLE dim_store_details 
ALTER COLUMN store_code TYPE VARCHAR(?);

ALTER TABLE dim_store_details 
ALTER COLUMN staff_numbers TYPE VARCHAR (3);

ALTER TABLE dim_store_details 
ALTER COLUMN opening_date TYPE DATE
USING opening_date::date;

ALTER TABLE dim_store_details 
ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT
USING latitude::double_precision;

ALTER TABLE dim_store_details 
ALTER COLUMN country_code TYPE VARCHAR (3);

ALTER TABLE dim_store_details 
ALTER COLUMN continent TYPE VARCHAR(255);

-- Removing pound sign from product_price in dim_products
UPDATE dim_products SET product_price = REPLACE(product_price, '£', '');

-- Distributing products in dim_products in different price ranges:

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

-- Renaming column 'removed' to 'still availble' in dim_products
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Same queries as earlier were implemented to define data types
-- In addition, for the bool values, the following code was used
-- Converting 'Still_availble' to True and all others to False

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE boolean 
USING (still_available = 'Still_available');

-- Changing data types in dim_date_types:

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE AS UUID
USING date_uuid::UUID;

-- all other types were VARCHAR(?), and the calculation of length was used:

SELECT MAX(LENGTH(month)) AS max_length FROM dim_date_times;

--Changing data types in dim_card_details:

SELECT MAX(LENGTH(card_number)) AS max_length FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE AS VARCHAR(22);

ALTER TABLE dim_card_details 
ALTER COLUMN date_payment_confirmed TYPE DATE
USING date_payment_confirmed::date;

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(10);


-- Adding primary keys to each of the tables prefixed with dim

ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
ALTER TABLE dim_date_times ADD PRIMARY KEY(date_uuid);
ALTER TABLE dim_products ADD PRIMARY KEY(product_code);
ALTER TABLE dim_store_details ADD PRIMARY KEY(store_code);
ALTER TABLE dim_users ADD PRIMARY KEY(user_uuid);

-- Adding foreign keys to orders_table

ALTER TABLE orders_table
ADD CONSTRAINT orders_table_date_uuid_fk
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT orders_table_user_uuid_fk
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

-- this corrects an error in the data cleaning in python
UPDATE dim_card_details SET card_number = REPLACE(price_column, '?', ''); 

ALTER TABLE orders_table
ADD CONSTRAINT orders_table_card_number_fk
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table 
ADD CONSTRAINT orders_table_store_code_fk
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT orders_table_product_code_fk
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);



