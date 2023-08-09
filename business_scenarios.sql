-- DOCUMENT WITH Q&A OF THE BUSINESS SCENARIO 

/* The Operations team would like to know which countries we currently operate in and which country now has the most stores. */

SELECT country_code, COUNT(*) AS to_number_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY to_number_stores DESC;


--------
/* The business stakeholders would like to know which locations currently have the most stores. 
They would like to close some stores before opening more in other locations. Find out which locations have the most stores currently. */


SELECT locality, COUNT(*) AS total_number_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_number_stores DESC
LIMIT 7;

-------
/*Query the database to find out which months typically have the most sales.*/

SELECT dim_date_times.month, SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales

FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales DESC;

--------
/* The company is looking to increase its online sales. They want to know how many sales are happening online vs offline. 
Calculate how many products were sold and the amount of sales made for online and offline purchases. */

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


/* The 'SELECT' clause specifies that the columns that will be shown are 'location', distinguishing between online and offline,
 total_sales as a count of each time that sales are online or offline, and number of sales, as the sum of product quantity in online or offline sales. 

The 'INNER JOIN' connects the tables dim_store_details and orders_table by 'store_code', and 'GROUP BY' groups the result set by the 'location' column. 
Hence, the functions COUNT and SUM operate on distinct sales types separately.*/

--------
/*The sales team wants to know which of the different store types is generated the most revenue so they know where to focus. 
Find out the total and percentage of sales coming from each of the different store types.*/

  SELECT 
    dim_store_details.store_type,
    COUNT(*) AS total_sales,
    ROUND(COUNT(*) * 100.0/SUM(COUNT(*)) OVER (),2) AS percentage_total -- calculates the ratio of the count of rows for each sales type to the total count of all rows in the result set.
    
  FROM 
    orders_table 
    INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
  GROUP BY
    dim_store_details.store_type
  ORDER BY percentage_total DESC;

/* The company stakeholders want assurances that the company has been doing well recently. 
Find which months in which years have had the most sales historically.

By selecting information from orders_table, dim_products, and dim_date_times, it is possible to retrieve the necessary information to find the 
best sales month for each year. 

Here, a subquery is needed to get the sales amount by year and month. Multiplying the quantity of products sold ('orders_table.product_quantity')
with their respective prices ('dim_products.product_price) gives the sales amount per order. Grouping the sales by year and month gives the total 
sales amount for each month in each year. 

Using ROW_NUMBER(), each month within a year is given a row, being the one with the highest sales amount row number 1 (rn = 1).
In this case, rows are partitioned by the values in the 'year' column of the dim_date_times table.

Taking the results from the last subquery:

Sales amount, year, and month are selected, and filtering the results where row number (rn) is equal to 1, the result obtained 
is the best month per year in terms of sales.
*/

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

  /* The operations team would like to know the overall staff numbers in each location around the world. Perform a query
   to determine the staff numbers in each of the countries the company sells in. */

SELECT country_code, SUM(staff_numbers) AS total_staff_numbers
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

/* The sales team is looking to expand their territory in Germany. Determine which type of store is 
generating the most sales in Germany.*/

SELECT  store_type, country_code, ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric,2) AS sales

  FROM orders_table
  JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
  JOIN dim_products ON orders_table.product_code = dim_products.product_code
  WHERE country_code = 'DE'
  GROUP BY store_type, country_code
  ORDER BY sales ASC;



 




