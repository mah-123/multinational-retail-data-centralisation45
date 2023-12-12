-- Task 1 altering orders_table
ALTER TABLE orders_table
    ALTER date_uuid TYPE UUID
    USING date_uuid::uuid;
ALTER TABLE orders_table
    ALTER user_uuid TYPE UUID
    USING user_uuid_uuid::uuid;
ALTER TABLE orders_table
    ALTER card_number TYPE VARCHAR(19);
ALTER TABLE orders_table
    ALTER store_code TYPE VARCHAR(12);
ALTER TABLE orders_table
    ALTER product_code TYPE VARCHAR(11);
ALTER TABLE orders_table
    ALTER product_quantity TYPE SMALLINT;

-- Task 2 altering dim_users table
ALTER TABLE dim_users
    ALTER first_name TYPE VARCHAR(255);
ALTER TABLE dim_users
    ALTER surname_name TYPE VARCHAR(255);
ALTER TABLE dim_users
    ALTER date_of_birth TYPE DATE;
ALTER TABLE dim_users
    ALTER country_code TYPE VARCHAR(2);
ALTER TABLE dim_users
    ALTER user_uuid TYPE UUID
    USING user_uuid::uuid;
ALTER TABLE dim_users
    ALTER join_date TYPE DATE
    USING join_date::date;

-- Task 3 alter dim_store_details
/* Note: the column lat are all null vals which did not require to be
merged. Instead it was deleted from the column.
*/
ALTER TABLE dim_store_details    
    DROP COLUMN lat; 
ALTER TABLE dim_store_details
    ALTER longitude TYPE FLOAT;
ALTER TABLE dim_store_details
    ALTER latitude TYPE FLOAT;
ALTER TABLE dim_store_details
    ALTER locality TYPE VARCHAR(255);
ALTER TABLE dim_store_details
    ALTER store_code TYPE VARCHAR(12);
ALTER TABLE dim_store_details
    ALTER staff_numbers TYPE SMALLINT;
ALTER TABLE dim_store_details
    ALTER opening_date TYPE DATE;
ALTER TABLE dim_store_details
    ALTER store_type TYPE VARCHAR(255);
ALTER TABLE dim_store_details
    ALTER country_code TYPE VARCHAR(2);
ALTER TABLE dim_store_details
    ALTER continet TYPE VARCHAR(255);

-- Task 4 alter dim_products price and add weight class table
UPDATE TABLE dim_products
    SET product_price = TRIM('£' FROM product_price);
ALTER TABLE dim_products
    ADD weight_class TYPE VARCHAR(14)
WHERE
    light = weight < 2,
    Mid_Sized = weight >= 2 AND weight < 40,
    Heavy = weight >= 40 AND weight < 140,
    Truck_required = weight >= 140;

-- Task 5 alter dim_products table
ALTER TABLE dim_products
    RENAME Removed TO still_available;
ALTER TABLE dim_products
    ALTER product_price TYPE FLOAT;
ALTER TABLE dim_products
    ALTER weight TYPE FLOAT;
ALTER TABLE dim_products
    ALTER "EAN" TYPE VARCHAR(17);
ALTER TABLE dim_products
    ALTER date_added TYPE DATE;
ALTER TABLE dim_products
    ALTER uuid TYPE UUID
    USING uuid::uuid;
UPDATE dim_products
    SET still_available = 
        CASE WHEN still_available = 'Still_available' THEN TRUE
        ELSE FALSE;
ALTER TABLE dim_products
    ALTER still_available TYPE BOOLEAN;
ALTER TABLE dim_products
    ALTER weight_class TYPE VARCHAR(14);

-- Task 6 alter dim_date_times
ALTER TABLE dim_date_times
    ALTER "month" TYPE VARCHAR(2);
ALTER TABLE dim_date_times
    ALTER "year" TYPE VARCHAR(4);
ALTER TABLE dim_date_times
    ALTER "day" TYPE VARCHAR(2);
ALTER TABLE dim_date_times
    ALTER time_period TYPE VARCHAR(10);
ALTER TABLE dim_date_times
    ALTER date_uuid TYPE UUID
    USING date_uuid::uuid;

-- Task 7 alter dim_card_details
ALTER TABLE dim_card_details
    ALTER card_number TYPE VARCHAR(19);
ALTER TABLE dim_card_details
    ALTER "expiry_date" TYPE VARCHAR(5);
ALTER TABLE dim_card_details
    ALTER date_payment_confirmed TYPE DATE
    USING date_payment_confirmed::date;

-- Task 8 ADD Primary key to all dim tables
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);
ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);
ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);

-- Task 9
ALTER TABLE orders_table
    ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
ALTER TABLE orders_table
    ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
ALTER TABLE orders_table
    ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
ALTER TABLE orders_table
    ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
ALTER TABLE orders_table
    ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);