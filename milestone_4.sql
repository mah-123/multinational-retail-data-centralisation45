-- Task 1 no of stores in different
SELECT country_code AS country,
		SUM(country_code) AS total_no_stores
FROM 
	dim_store_details
GROUP BY
		country
ORDER BY
		total_no_stores DESC;

-- Task 2 locations with most stores
SELECT locality,
		SUM(country_code) AS total_no_stores
FROM 
	dim_store_details
GROUP BY
		locality
ORDER BY
		total_no_stores DESC;

-- Task 3 total_sales by month
SELECT SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
		dim_date_times.month
FROM
	orders_table
JOIN
	dim_products
ON 
	orders_table.product_code = dim_products.product_code
JOIN
	dim_date_times
ON
	orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
	dim_date_times.month
ORDER BY
	total_sales;

-- Task 4  number_of_sales by WEB vs Offline
SELECT COUNT(orders_table.user_uuid) AS numbers_of_sales,
		SUM(orders_table.product_quantity) AS product_quantity_count,
		dim_store_details.store_type
CASE
	WHEN dim_store_details.store_type = 'Web%' THEN WEB 
	ELSE "Offline" 
END AS "location"
FROM
	orders_table
JOIN
	dim_products
ON
	orders_table.product_code = dim_products.product_code
JOIN
	dim_store_details
ON
	orders_table.store_code = dim_store_details.store_code
GROUP BY
	CASE
	WHEN dim_store_details.store_type = 'Web%' THEN WEB 
	ELSE "Offline" 
END

-- Task 5 total_sales store_type by percentage
SELECT	dim_store_details.store_type,
		SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
		SUM(orders_table.product_quantity * dim_products.product_price) * 100 /SUM(SUM(orders_table.product_quantity * dim_products.product_price)) 
		OVER () AS "percentage_total(%)"
FROM
	orders_table
JOIN 
	dim_products
ON
	orders_table.product_code = dim_products.product_code
JOIN	
	dim_store_details
ON
	orders_table.store_code = dim_store_details.store_code
GROUP BY
		dim_store_details.store_type
ORDER BY
		total_sales DESC;

-- Task 6
SELECT	SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
		dim_date_times.year,
		dim_date_times.month
FROM
	orders_table
JOIN 
	dim_products
ON
	orders_table.product_code = dim_products.product_code
JOIN	
	dim_date_times
ON
	orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
		dim_date_times.year,
		dim_date_times.month
ORDER BY
		total_sales DESC;

-- Task 7 query number of staffs across different country
SELECT	SUM(staff_numbers) AS total_staff_numbers,
		country_code
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY
		total_staff_numbers DESC;

-- Task 8 German store_type selling the most
SELECT	SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales,
		dim_store_details.store_type,
		dim_store_details.country_code
FROM
	orders_table
JOIN
	dim_products
ON	orders_table.product_code = dim_products.product_code
JOIN
	dim_store_details
ON	orders_table.store_code = dim_store_details.store_code
WHERE
	dim_store_details.country_code = 'DE'
GROUP BY
		dim_store_details.store_type,
		dim_store_details.country_code
ORDER BY
		total_sales;

-- Task 9 Earliest sale_time made for each stores 
WITH orginal_time AS(
	SELECT 	dim_date_times.year,
			dim_date_times.month,
			dim_date_times.day,
			dim_date_times.timestamp
	FROM dim_date_times
	JOIN orders_table 
	ON dim_date_times.date_uuid = orders_table.date_uuid
), 
complete_datetime AS(
	SELECT year, month, day, timestamp,
		TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, '-', timestamp), 'YYYY/MM/DD/HH24:MI:ss') as Full_datetime
		--CONCAT(year, '-', month, '-', day, timestamp)::TIMESTAMP without time zone AS Full_datetime
	FROM orginal_time
	ORDER BY Full_datetime DESC
),
lead_time AS(
	SELECT year,
		Full_datetime,
		LEAD (Full_datetime, 1) OVER (
		ORDER BY Full_datetime DESC) AS next_time
	FROM complete_datetime
),
avg_times AS(
SELECT year,
		AVG(Full_datetime - next_time) AS avg_times
FROM lead_time
GROUP BY year
ORDER BY avg_times DESC
)

SELECT year,
		CONCAT('"hours:"', (EXTRACT(HOUR FROM avg_times)), ',',
		'"minutes:"', (EXTRACT(MINUTE FROM avg_times)), ',',
		'"seconds:"', ROUND(EXTRACT(SECOND FROM avg_times)), ',',
		'"miliseconds:"', ROUND((EXTRACT( SECOND FROM avg_times)- FLOOR(EXTRACT(SECOND FROM avg_times)))*100)
			  ) as actual_time_taken
FROM avg_times
GROUP BY year, avg_times
ORDER BY avg_times DESC;