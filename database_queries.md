# Sales Data queries
This is a file that contains the queries made in the Sales_Data Database in PgAdmin.

## TASK 1:

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

ALTER TABLE dim_user_table
ALTER COLUMN date_of_birth TYPE DATE;

For the country_code, the query mentioned in task 1 that selcts the length of the largest element returned a length of 2, which was implemented to the column:

ALTER TABLE dim_user_table
ALTER COLUMN country_code TYPE VARCHAR(2);

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

ALTER TABLE store_details_table
DROP COLUMN lat;

Also, there was a row that represented the business's website change the location column values where they're null to N/A. To do so, the longitude value [null] was used as a reference using the WHERE clause:

UPDATE dim_store_details
SET locality = 'N/A'
WHERE longitude IS NULL;

For the case where store_type is VARCHAR(255), no changes were made from previous queries using the same datatype as Postgre does allow nullable VARCHAR by default.

## TASK 4:

The first change to do in the dim_products table was to remove the pound sign from the column product_price. That was done using:

UPDATE dim_products SET price_column = REPLACE(price_column, '£', '')

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

