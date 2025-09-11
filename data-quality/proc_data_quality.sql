/*
===============================================================================
DQ Health Check Procedure for Silver Layer
===============================================================================
Purpose:
    Runs comprehensive data quality (DQ) checks across all  tables in the
    'silver' schema. Identifies and logs issues such as:
        - Null or duplicate keys
        - Invalid formats or domain values
        - Out-of-range or illogical data
        - Referential integrity and business rule violations

    All issues are logged in [silver].[dq_health] for monitoring, investigation,
    and downstream remediation.

    This procedure supports post-ETL validation, regulatory compliance, and
    ongoing DQ monitoring.

Example Usage:
    -- 1. Run the stored procedure after the ETL process completes:
        EXEC silver.dq_health_check;

    -- 2. View the most recent DQ issues:
        SELECT TOP 20 *
        FROM silver.dq_health
        ORDER BY detected_at DESC;

    -- 3. Produce a summary of issues for reporting or dashboards:
        SELECT dq_table, dq_column, dq_type, dq_issue, COUNT(*) AS issue_count
        FROM silver.dq_health
        GROUP BY dq_table, dq_column, dq_type, dq_issue
        ORDER BY issue_count DESC;

===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.dq_health_check AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Create or Truncate DQ Health Table
    IF OBJECT_ID('silver.dq_health', 'U') IS NOT NULL
        TRUNCATE TABLE silver.dq_health;
    ELSE
        CREATE TABLE silver.dq_health (
            dq_issue_id INT IDENTITY(1,1) PRIMARY KEY,
            dq_table VARCHAR(100) NOT NULL,
            dq_column VARCHAR(100) NULL,
            dq_type VARCHAR(100) NOT NULL,
            dq_issue VARCHAR(300) NOT NULL,
            dq_value NVARCHAR(200) NULL,
            primary_key VARCHAR(200) NULL,
            detected_at DATETIME2 DEFAULT GETDATE()
        )
    -- ====================================================================
    -- Checking 'silver.crm_customer'
    -- ====================================================================
    -- Null check for customer_id (PK)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'customer_id', 'null_check',
        'customer_id is NULL',
        NULL, NULL, GETDATE()
    FROM silver.crm_customer
    WHERE customer_id IS NULL;

    -- Check for missing or empty emails (required for all customers)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'email', 'null_check',
        'email is NULL or empty',
        email, CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE email IS NULL OR TRIM(email) = '';

    -- Check for missing or empty usernames (required for all customers)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'username', 'null_check',
        'username is NULL or empty',
        username, CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE username IS NULL OR TRIM(username) = '';

    -- Check for duplicate customer IDs (should be unique)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'customer_id', 'duplicate_key',
        'customer_id is duplicated',
        CAST(customer_id AS NVARCHAR(50)), CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM (
        SELECT customer_id
        FROM silver.crm_customer
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    ) t;

    -- Check for invalid email formats (should contain "@" and ".")
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'email', 'format',
        'email format is invalid',
        email, CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE email IS NOT NULL
    AND (CHARINDEX('@', email) = 0 OR CHARINDEX('.', email) = 0);

    -- Check for invalid gender values (should be 'M', 'F', or NULL)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'gender', 'domain',
        'gender value is invalid',
        gender, CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE gender IS NOT NULL
    AND UPPER(TRIM(gender)) NOT IN ('M', 'F');

    -- Check for future birth dates (should not be after today)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'birth_date', 'out_of_range',
        'birth_date is in the future',
        CONVERT(NVARCHAR(25), birth_date, 120), CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE birth_date IS NOT NULL
    AND birth_date > CAST(GETDATE() AS DATE);

    -- Check for missing or empty phone numbers (optional, but flagged if empty)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'phone_number', 'null_check',
        'phone_number is NULL or empty',
        phone_number, CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE phone_number IS NULL OR TRIM(phone_number) = '';

    -- Check for missing values in is_loyalty_member (should be 0 or 1)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'is_loyalty_member', 'null_check',
        'is_loyalty_member is NULL',
        NULL, CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE is_loyalty_member IS NULL;

    -- Check for missing values in is_fraud_suspected (should be 0 or 1)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'is_fraud_suspected', 'null_check',
        'is_fraud_suspected is NULL',
        NULL, CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE is_fraud_suspected IS NULL;

    -- Check for registration datetimes set in the future (should not be after now)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer', 'registration_datetime', 'out_of_range',
        'registration_datetime is in the future',
        CONVERT(NVARCHAR(50), registration_datetime, 120), CAST(customer_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer
    WHERE registration_datetime IS NOT NULL
    AND registration_datetime > GETDATE();

    -- ====================================================================
    -- Checking 'silver.crm_customer_addresses'
    -- ====================================================================
    -- Check for missing address IDs (should never be NULL)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer_addresses', 'address_id', 'null_check',
        'address_id is NULL',
        NULL, NULL, GETDATE()
    FROM silver.crm_customer_addresses
    WHERE address_id IS NULL;

    -- Check for missing customer IDs (each address must belong to a customer)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer_addresses', 'customer_id', 'null_check',
        'customer_id is NULL',
        NULL, CAST(address_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer_addresses
    WHERE customer_id IS NULL;

    -- Check referential integrity: customer_id does not exist in crm_customer
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer_addresses', 'customer_id', 'referential',
        'customer_id does not exist in crm_customer',
        CAST(a.customer_id AS NVARCHAR(50)), CAST(a.address_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer_addresses a
    LEFT JOIN silver.crm_customer c ON a.customer_id = c.customer_id
    WHERE a.customer_id IS NOT NULL AND c.customer_id IS NULL;

    -- Check for invalid postal codes (must be alphanumeric and length 3-10)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.crm_customer_addresses', 'postal_code', 'format',
        'postal_code is invalid (non-alphanumeric or wrong length)',
        postal_code, CAST(address_id AS VARCHAR(50)), GETDATE()
    FROM silver.crm_customer_addresses
    WHERE postal_code IS NOT NULL
    AND (LEN(postal_code) < 3 OR LEN(postal_code) > 10 OR postal_code LIKE '%[^A-Za-z0-9-]%');

    -- ====================================================================
    -- Checking 'silver.erp_product'
    -- ====================================================================
    -- Check for missing product IDs (should never be NULL)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product', 'product_id', 'null_check',
        'product_id is NULL',
        NULL, NULL, GETDATE()
    FROM silver.erp_product
    WHERE product_id IS NULL;

    -- Check for duplicate product IDs (should be unique)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product', 'product_id', 'duplicate_key',
        'product_id is duplicated',
        CAST(product_id AS NVARCHAR(50)), CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM (
        SELECT product_id
        FROM silver.erp_product
        WHERE product_id IS NOT NULL
        GROUP BY product_id
        HAVING COUNT(*) > 1
    ) t;

    -- Check for negative unit_price values (should be zero or positive)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product', 'unit_price', 'out_of_range',
        'unit_price is negative',
        CAST(unit_price AS NVARCHAR(50)), CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_product
    WHERE unit_price < 0;

    -- Check for negative cost_price values (should be zero or positive)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product', 'cost_price', 'out_of_range',
        'cost_price is negative',
        CAST(cost_price AS NVARCHAR(50)), CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_product
    WHERE cost_price < 0;

    -- Check for business rule violation: cost_price should not exceed unit_price
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product', 'cost_price', 'business_rule',
        'cost_price is greater than unit_price',
        CONCAT('cost_price:', cost_price, ', unit_price:', unit_price),
        CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_product
    WHERE cost_price > unit_price
    AND unit_price IS NOT NULL
    AND cost_price IS NOT NULL;

    -- Check for extreme profit_margin (>1000%) [possible data error]
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product', 'profit_margin', 'out_of_range',
        'profit_margin is greater than 1000%',
        CAST(ROUND(profit_margin, 2) AS NVARCHAR(50)), CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_product
    WHERE profit_margin > 1000;

    -- Check for missing or out-of-range ratings (should be between 0 and 5)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product', 'rating', 'out_of_range',
        'rating is NULL or out of range [0,5]',
        CAST(rating AS NVARCHAR(50)), CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_product
    WHERE rating IS NULL OR rating < 0 OR rating > 5;

    -- Check for negative review_count values (should be zero or positive)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product', 'review_count', 'out_of_range',
        'review_count is negative',
        CAST(review_count AS NVARCHAR(50)), CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_product
    WHERE review_count < 0;

    -- ====================================================================
    -- Checking 'silver.erp_product_prices'
    -- ====================================================================
    -- Check for missing product IDs (every price must relate to a product)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product_prices', 'product_id', 'null_check',
        'product_id is NULL',
        NULL, NULL, GETDATE()
    FROM silver.erp_product_prices
    WHERE product_id IS NULL;

    -- Check for invalid local_price (should be > 0 and <= 999999.99)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product_prices', 'local_price', 'out_of_range',
        'local_price is out of allowed range',
        CAST(local_price AS NVARCHAR(50)), CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_product_prices
    WHERE local_price <= 0 OR local_price > 999999.99;

    -- Check for effective_date out of allowed range (not before 2023-01-01, not in future)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_product_prices', 'effective_date', 'out_of_range',
        'effective_date is out of allowed range',
        CONVERT(NVARCHAR(25), effective_date, 120), CAST(product_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_product_prices
    WHERE effective_date < '2023-01-01'
    OR effective_date > CAST(GETDATE() AS DATE);

    -- ====================================================================
    -- Checking 'silver.erp_shipment_company'
    -- ====================================================================
    -- Check for missing shipment_company_id (should never be NULL)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_shipment_company', 'shipment_company_id', 'null_check',
        'shipment_company_id is NULL',
        NULL, NULL, GETDATE()
    FROM silver.erp_shipment_company
    WHERE shipment_company_id IS NULL;

    -- Check for missing or empty company_name (required for all shipment companies)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_shipment_company', 'company_name', 'null_check',
        'company_name is NULL or empty',
        company_name, CAST(shipment_company_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_shipment_company
    WHERE company_name IS NULL OR TRIM(company_name) = '';

    -- Check for missing or empty country_code (should be present and valid)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_shipment_company', 'country_code', 'null_check',
        'country_code is NULL or empty',
        country_code, CAST(shipment_company_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_shipment_company
    WHERE country_code IS NULL OR TRIM(country_code) = '';

    -- Check for company_name length violation (should not exceed 30)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_shipment_company', 'company_name', 'format',
        'company_name length exceeds 30 characters',
        company_name, CAST(shipment_company_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_shipment_company
    WHERE LEN(company_name) > 30;

    -- Check for invalid country_code format (should be ISO code: 2-5 alphabetic characters)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_shipment_company', 'country_code', 'format',
        'country_code format is invalid',
        country_code, CAST(shipment_company_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_shipment_company
    WHERE LEN(TRIM(country_code)) < 2
    OR LEN(TRIM(country_code)) > 5
    OR country_code LIKE '%[^A-Za-z]%';

    -- Check for duplicate shipment_company_id (should be unique)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_shipment_company', 'shipment_company_id', 'duplicate_key',
        'shipment_company_id is duplicated',
        CAST(shipment_company_id AS NVARCHAR(50)), CAST(shipment_company_id AS VARCHAR(50)), GETDATE()
    FROM (
        SELECT shipment_company_id
        FROM silver.erp_shipment_company
        WHERE shipment_company_id IS NOT NULL
        GROUP BY shipment_company_id
        HAVING COUNT(*) > 1
    ) t;

    -- Check for all service flags unset (at least one must be TRUE)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_shipment_company', NULL, 'business_rule',
        'no delivery type flag set (should have at least one)',
        NULL, CAST(shipment_company_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_shipment_company
    WHERE ISNULL(is_standard, 0) = 0
    AND ISNULL(is_express, 0) = 0
    AND ISNULL(is_registered, 0) = 0
    AND ISNULL(is_international, 0) = 0;

    -- ====================================================================
    -- Checking 'silver.erp_payment_channel'
    -- ====================================================================
    -- Check for missing or empty payment_channel_id (should not be NULL or empty)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'payment_channel_id', 'null_check',
        'payment_channel_id is NULL or empty',
        payment_channel_id, payment_channel_id, GETDATE()
    FROM silver.erp_payment_channel
    WHERE payment_channel_id IS NULL OR TRIM(payment_channel_id) = '';

    -- Check for missing or empty channel_name
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'channel_name', 'null_check',
        'channel_name is NULL or empty',
        channel_name, payment_channel_id, GETDATE()
    FROM silver.erp_payment_channel
    WHERE channel_name IS NULL OR TRIM(channel_name) = '';

    -- Check for missing or empty provider_name
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'provider_name', 'null_check',
        'provider_name is NULL or empty',
        provider_name, payment_channel_id, GETDATE()
    FROM silver.erp_payment_channel
    WHERE provider_name IS NULL OR TRIM(provider_name) = '';

    -- Check for missing or empty country_code
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'country_code', 'null_check',
        'country_code is NULL or empty',
        country_code, payment_channel_id, GETDATE()
    FROM silver.erp_payment_channel
    WHERE country_code IS NULL OR TRIM(country_code) = '';

    -- Check payment_channel_id length (<= 15)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'payment_channel_id', 'format',
        'payment_channel_id length exceeds 15 characters',
        payment_channel_id, payment_channel_id, GETDATE()
    FROM silver.erp_payment_channel
    WHERE LEN(payment_channel_id) > 15;

    -- Check channel_name length (<= 20)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'channel_name', 'format',
        'channel_name length exceeds 20 characters',
        channel_name, payment_channel_id, GETDATE()
    FROM silver.erp_payment_channel
    WHERE LEN(channel_name) > 20;

    -- Check provider_name length (<= 20)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'provider_name', 'format',
        'provider_name length exceeds 20 characters',
        provider_name, payment_channel_id, GETDATE()
    FROM silver.erp_payment_channel
    WHERE LEN(provider_name) > 20;

    -- Check for invalid country_code format ('GLOBAL' or two uppercase letters)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'country_code', 'format',
        'country_code format is invalid',
        country_code, payment_channel_id, GETDATE()
    FROM silver.erp_payment_channel
    WHERE UPPER(TRIM(country_code)) <> 'GLOBAL'
    AND (LEN(TRIM(country_code)) <> 2 OR country_code LIKE '%[^A-Z]%');

    -- Check for duplicate payment_channel_id (should be unique)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_payment_channel', 'payment_channel_id', 'duplicate_key',
        'payment_channel_id is duplicated',
        payment_channel_id, payment_channel_id, GETDATE()
    FROM (
        SELECT payment_channel_id
        FROM silver.erp_payment_channel
        WHERE payment_channel_id IS NOT NULL
        GROUP BY payment_channel_id
        HAVING COUNT(*) > 1
    ) t;

    -- ====================================================================
    -- Checking 'silver.erp_orders'
    -- ====================================================================
    -- Null or empty order_id (guard TRIM for numeric)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'order_id', 'null_check', 'order_id is NULL or empty', NULL, NULL, GETDATE()
    FROM silver.erp_orders
    WHERE order_id IS NULL
    OR TRIM(TRY_CONVERT(NVARCHAR(50), order_id)) = '';

    -- Duplicate order_id (should be unique)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'order_id', 'duplicate_key', 'order_id is duplicated',
        order_id, order_id, GETDATE()
    FROM (
        SELECT order_id
        FROM silver.erp_orders
        WHERE order_id IS NOT NULL
        GROUP BY order_id
        HAVING COUNT(*) > 1
    ) t;

    -- Null or negative total_price
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'total_price', 'out_of_range', 'total_price is NULL or negative',
        CAST(total_price AS NVARCHAR(50)), CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE total_price IS NULL OR total_price < 0;

    -- Unusually high total_price (business threshold)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'total_price', 'out_of_range', 'total_price exceeds upper limit',
        CAST(total_price AS NVARCHAR(50)), CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE total_price > 1000000;

    -- total_price is zero for COMPLETED order
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'total_price', 'consistency', 'total_price is zero for COMPLETED order',
        CAST(total_price AS NVARCHAR(50)), CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE total_price = 0 AND UPPER(order_status) = 'COMPLETED';

    -- Null or empty order_status
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'order_status', 'null_check', 'order_status is NULL or empty',
        order_status, CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE order_status IS NULL OR TRIM(order_status) = '';

    -- order_status exceeds max length
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'order_status', 'format', 'order_status length exceeds 15 characters',
        order_status, CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE LEN(order_status) > 15;

    -- order_status not allowed (domain violation)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'order_status', 'domain', 'order_status value is not allowed',
        order_status, CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE order_status IS NOT NULL AND UPPER(order_status) NOT IN ('COMPLETED', 'CANCELLED');

    -- Null or empty country_code
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'country_code', 'null_check', 'country_code is NULL or empty',
        country_code, CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE country_code IS NULL OR TRIM(country_code) = '';

    -- country_code not ISO 2-letter
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'country_code', 'format', 'country_code is not ISO 2-letter',
        country_code, CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE LEN(TRIM(country_code)) <> 2 OR country_code LIKE '%[^A-Za-z]%';

    -- Null shipping_address_id
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'shipping_address_id', 'null_check', 'shipping_address_id is NULL',
        NULL, CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE shipping_address_id IS NULL;

    -- Null shipment_company_id
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'shipment_company_id', 'null_check', 'shipment_company_id is NULL',
        NULL, CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE shipment_company_id IS NULL;

    -- order_datetime in the future
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'order_datetime', 'out_of_range', 'order_datetime is in the future',
        CONVERT(NVARCHAR(50), order_datetime, 120), CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE order_datetime > GETDATE();

    -- order_datetime before 2023-01-01
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'order_datetime', 'out_of_range', 'order_datetime is before 2023-01-01',
        CONVERT(NVARCHAR(50), order_datetime, 120), CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE order_datetime < '2023-01-01';

    -- Referential: customer_id not in crm_customer (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'customer_id', 'referential', 'customer_id does not exist in crm_customer',
        CAST(o.customer_id AS VARCHAR(50)), CAST(o.order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders o
    LEFT JOIN silver.crm_customer c ON o.customer_id = c.customer_id
    WHERE o.customer_id IS NOT NULL AND c.customer_id IS NULL;

    -- Referential: shipping_address_id not in crm_customer_addresses (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'shipping_address_id', 'referential', 'shipping_address_id does not exist in crm_customer_addresses',
        CAST(o.shipping_address_id AS VARCHAR(50)), CAST(o.order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders o
    LEFT JOIN silver.crm_customer_addresses a ON o.shipping_address_id = a.address_id
    WHERE o.shipping_address_id IS NOT NULL AND a.address_id IS NULL;

    -- Referential: shipment_company_id not in erp_shipment_company (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'shipment_company_id', 'referential', 'shipment_company_id does not exist in erp_shipment_company',
        CAST(o.shipment_company_id AS VARCHAR(50)), CAST(o.order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders o
    LEFT JOIN silver.erp_shipment_company s ON o.shipment_company_id = s.shipment_company_id
    WHERE o.shipment_company_id IS NOT NULL AND s.shipment_company_id IS NULL;

    -- Business logic: order_status CANCELLED but is_cancelled not 1
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'status_flags', 'consistency', 'order_status is CANCELLED but is_cancelled is not 1',
        CONCAT('order_status:', order_status, ', is_cancelled:', is_cancelled), CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE UPPER(order_status) = 'CANCELLED' AND ISNULL(is_cancelled, 0) <> 1;

    -- Business logic: order_status COMPLETED but is_cancelled or is_returned is 1
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_orders', 'status_flags', 'consistency', 'order_status is COMPLETED but is_cancelled or is_returned is 1',
        CONCAT('is_cancelled:', is_cancelled, ', is_returned:', is_returned), CAST(order_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_orders
    WHERE UPPER(order_status) = 'COMPLETED' AND (ISNULL(is_cancelled, 0) = 1 OR ISNULL(is_returned, 0) = 1);

    -- ====================================================================
    -- Checking 'silver.erp_order_detail'
    -- ====================================================================

    -- Duplicate order_detail_id (should be unique)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'order_detail_id', 'duplicate_key',
        'order_detail_id is duplicated',
        order_detail_id, order_detail_id, GETDATE()
    FROM (
        SELECT order_detail_id
        FROM silver.erp_order_detail
        WHERE order_detail_id IS NOT NULL
        GROUP BY order_detail_id
        HAVING COUNT(*) > 1
    ) t;

    -- Null or empty order_id (guard TRIM for numeric)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'order_id', 'null_check',
        'order_id is NULL or empty',
        od.order_id, od.order_detail_id, GETDATE()
    FROM silver.erp_order_detail od
    WHERE od.order_id IS NULL
    OR TRIM(TRY_CONVERT(NVARCHAR(50), od.order_id)) = '';

    -- quantity is NULL or <= 0 (should be positive)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'quantity', 'out_of_range',
        'quantity is NULL or <= 0',
        CAST(quantity AS NVARCHAR(50)), CAST(order_detail_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_order_detail
    WHERE quantity IS NULL OR quantity <= 0;

    -- unit_price is NULL or negative
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'unit_price', 'out_of_range',
        'unit_price is NULL or negative',
        CAST(unit_price AS NVARCHAR(50)), CAST(order_detail_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_order_detail
    WHERE unit_price IS NULL OR unit_price < 0;

    -- discount_amount is negative
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'discount_amount', 'out_of_range',
        'discount_amount is negative',
        CAST(discount_amount AS NVARCHAR(50)), CAST(order_detail_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_order_detail
    WHERE discount_amount < 0;

    -- sales_amount is negative or NULL
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'sales_amount', 'out_of_range',
        'sales_amount is NULL or negative',
        CAST(sales_amount AS NVARCHAR(50)), CAST(order_detail_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_order_detail
    WHERE sales_amount IS NULL OR sales_amount < 0;

    -- recalc_sales_amount is negative or NULL
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'recalc_sales_amount', 'out_of_range',
        'recalc_sales_amount is NULL or negative',
        CAST(recalc_sales_amount AS NVARCHAR(50)), CAST(order_detail_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_order_detail
    WHERE recalc_sales_amount IS NULL OR recalc_sales_amount < 0;

    -- order_datetime in the future
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'order_datetime', 'out_of_range',
        'order_datetime is in the future',
        CONVERT(NVARCHAR(50), order_datetime, 120), CAST(order_detail_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_order_detail
    WHERE order_datetime > GETDATE();

    -- Referential: order_id not in erp_orders (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'order_id', 'referential',
        'order_id does not exist in erp_orders',
        od.order_id, od.order_detail_id, GETDATE()
    FROM silver.erp_order_detail od
    LEFT JOIN silver.erp_orders o ON od.order_id = o.order_id
    WHERE od.order_id IS NOT NULL AND o.order_id IS NULL;

    -- Referential: product_id not in erp_product (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_order_detail', 'product_id', 'referential',
        'product_id does not exist in erp_product',
        CAST(od.product_id AS VARCHAR(50)), od.order_detail_id, GETDATE()
    FROM silver.erp_order_detail od
    LEFT JOIN silver.erp_product p ON od.product_id = p.product_id
    WHERE od.product_id IS NOT NULL AND p.product_id IS NULL;

    -- ====================================================================
    -- Checking 'silver.erp_invoice'
    -- ====================================================================
    -- Null or empty invoice_id (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice', 'invoice_id', 'null_check',
        'invoice_id is NULL or empty',
        NULL, NULL, GETDATE()
    FROM silver.erp_invoice
    WHERE invoice_id IS NULL OR TRIM(invoice_id) = '';

    -- unit_price, tax_amount, final_amount are NULL or negative (>= 0 required)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice', col, 'out_of_range',
        CONCAT(col, ' is NULL or negative'),
        CAST(val AS NVARCHAR(50)), CAST(invoice_id AS VARCHAR(50)), GETDATE()
    FROM (
        SELECT invoice_id, 'unit_price' AS col, unit_price AS val FROM silver.erp_invoice
        UNION ALL
        SELECT invoice_id, 'tax_amount', tax_amount FROM silver.erp_invoice
        UNION ALL
        SELECT invoice_id, 'final_amount', final_amount FROM silver.erp_invoice
    ) x
    WHERE val IS NULL OR val < 0;

    -- invoice_datetime in the future
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice', 'invoice_datetime', 'out_of_range',
        'invoice_datetime is in the future',
        CONVERT(NVARCHAR(50), invoice_datetime, 120), CAST(invoice_id AS VARCHAR(50)), GETDATE()
    FROM silver.erp_invoice
    WHERE invoice_datetime > GETDATE();

    -- Business rule: unit_price + tax_amount should match final_amount (within tolerance)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice', 'amount_check', 'business_rule',
        'unit_price plus tax_amount does not equal final_amount',
        CONCAT('unit_price:', unit_price, ', tax_amount:', tax_amount, ', final_amount:', final_amount),
        invoice_id, GETDATE()
    FROM silver.erp_invoice
    WHERE unit_price IS NOT NULL AND tax_amount IS NOT NULL AND final_amount IS NOT NULL
    AND ABS(ROUND(unit_price + tax_amount, 2) - ROUND(final_amount, 2)) > 0.01;

    -- Referential: order_id not found in erp_orders (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice', 'order_id', 'referential',
        'order_id does not exist in erp_orders',
        i.order_id, i.invoice_id, GETDATE()
    FROM silver.erp_invoice i
    LEFT JOIN silver.erp_orders o ON i.order_id = o.order_id
    WHERE i.order_id IS NOT NULL AND o.order_id IS NULL;

    -- ====================================================================
    -- Checking 'silver.erp_invoice_detail'
    -- ====================================================================
    -- Null or empty invoice_detail_id (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'invoice_detail_id', 'null_check',
        'invoice_detail_id is NULL or empty',
        NULL, NULL, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE invoice_detail_id IS NULL OR TRIM(invoice_detail_id) = '';

    -- Null or empty invoice_id (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'invoice_id', 'null_check',
        'invoice_id is NULL or empty',
        invoice_id, invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE invoice_id IS NULL OR TRIM(invoice_id) = '';

    -- product_id is NULL
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'product_id', 'null_check',
        'product_id is NULL',
        NULL, invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE product_id IS NULL;

    -- quantity is NULL or <= 0
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'quantity', 'out_of_range',
        'quantity is NULL or <= 0',
        CAST(quantity AS NVARCHAR(50)), invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE quantity IS NULL OR quantity <= 0;

    -- sales_amount is NULL or < 0
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'sales_amount', 'out_of_range',
        'sales_amount is NULL or negative',
        CAST(sales_amount AS NVARCHAR(50)), invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE sales_amount IS NULL OR sales_amount < 0;

    -- tax_amount is NULL or < 0
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'tax_amount', 'out_of_range',
        'tax_amount is NULL or negative',
        CAST(tax_amount AS NVARCHAR(50)), invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE tax_amount IS NULL OR tax_amount < 0;

    -- invoice_datetime is NULL
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'invoice_datetime', 'null_check',
        'invoice_datetime is NULL',
        NULL, invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE invoice_datetime IS NULL;

    -- invoice_detail_id exceeds 20 characters
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'invoice_detail_id', 'format',
        'invoice_detail_id length exceeds 20 characters',
        invoice_detail_id, invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE LEN(invoice_detail_id) > 20;

    -- invoice_id exceeds 15 characters
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'invoice_id', 'format',
        'invoice_id length exceeds 15 characters',
        invoice_id, invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE LEN(invoice_id) > 15;

    -- invoice_datetime in the future
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'invoice_datetime', 'out_of_range',
        'invoice_datetime is in the future',
        CONVERT(NVARCHAR(50), invoice_datetime, 120), invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE invoice_datetime > GETDATE();

    -- Referential: invoice_id not found in erp_invoice (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'invoice_id', 'referential',
        'invoice_id does not exist in erp_invoice',
        d.invoice_id, d.invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail d
    LEFT JOIN silver.erp_invoice i ON d.invoice_id = i.invoice_id
    WHERE d.invoice_id IS NOT NULL AND i.invoice_id IS NULL;

    -- Referential: product_id not found in erp_product (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'product_id', 'referential',
        'product_id does not exist in erp_product',
        CAST(d.product_id AS VARCHAR(50)), d.invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail d
    LEFT JOIN silver.erp_product p ON d.product_id = p.product_id
    WHERE d.product_id IS NOT NULL AND p.product_id IS NULL;

    -- Business rule: line_total must match sales_amount + tax_amount
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'line_total', 'business_rule',
        'line_total does not match sales_amount plus tax_amount',
        CONCAT('Expected:', ROUND(sales_amount + tax_amount,2), ', Actual:', ROUND(line_total,2)),
        invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE sales_amount IS NOT NULL AND tax_amount IS NOT NULL AND line_total IS NOT NULL
    AND ABS(ROUND(sales_amount + tax_amount,2) - ROUND(line_total,2)) > 0.01;

    -- tax_rate out of expected range (<0 or >1)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'tax_rate', 'out_of_range',
        'tax_rate is out of range',
        CAST(tax_rate AS NVARCHAR(50)), invoice_detail_id, GETDATE()
    FROM silver.erp_invoice_detail
    WHERE tax_rate < 0 OR tax_rate > 1;

    -- Duplicate invoice_detail_id
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT
        'silver.erp_invoice_detail', 'invoice_detail_id', 'duplicate_key',
        'invoice_detail_id is duplicated',
        invoice_detail_id, invoice_detail_id, GETDATE()
    FROM (
        SELECT invoice_detail_id
        FROM silver.erp_invoice_detail
        WHERE invoice_detail_id IS NOT NULL
        GROUP BY invoice_detail_id
        HAVING COUNT(*) > 1
    ) t;

    -- ====================================================================
    -- Checking 'silver.erp_payment'
    -- ====================================================================
    -- Null or empty payment_id (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'payment_id', 'null_check',
        'payment_id is NULL or empty',
        NULL, NULL, GETDATE()
    FROM silver.erp_payment
    WHERE payment_id IS NULL OR TRIM(payment_id) = '';

    -- Null or empty order_id (guard TRIM for numeric)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'order_id', 'null_check',
        'order_id is NULL or empty',
        p.order_id, p.payment_id, GETDATE()
    FROM silver.erp_payment p
    WHERE p.order_id IS NULL
    OR TRIM(TRY_CONVERT(NVARCHAR(50), p.order_id)) = '';

    -- Null or empty payment_channel_id (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'payment_channel_id', 'null_check',
        'payment_channel_id is NULL or empty',
        payment_channel_id, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE payment_channel_id IS NULL OR TRIM(payment_channel_id) = '';

    -- payment_datetime is NULL
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'payment_datetime', 'null_check',
        'payment_datetime is NULL',
        NULL, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE payment_datetime IS NULL;

    -- final_amount is NULL or negative
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'final_amount', 'out_of_range',
        'final_amount is NULL or negative',
        CAST(final_amount AS NVARCHAR(50)), payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE final_amount IS NULL OR final_amount < 0;

    -- Null or empty transaction_status (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'transaction_status', 'null_check',
        'transaction_status is NULL or empty',
        transaction_status, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE transaction_status IS NULL OR TRIM(transaction_status) = '';

    -- Null is_fraud (BIT)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'is_fraud', 'null_check',
        'is_fraud is NULL',
        NULL, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE is_fraud IS NULL;

    -- Null or empty refund_status (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'refund_status', 'null_check',
        'refund_status is NULL or empty',
        refund_status, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE refund_status IS NULL OR TRIM(refund_status) = '';

    -- payment_id length > 15
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'payment_id', 'format',
        'payment_id length exceeds 15 characters',
        payment_id, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE LEN(payment_id) > 15;

    -- transaction_status not allowed (domain violation)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'transaction_status', 'domain',
        'transaction_status value is not allowed',
        transaction_status, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE transaction_status IS NOT NULL
    AND UPPER(transaction_status) NOT IN ('COMPLETED','FAILED','CANCELLED','PENDING','REFUNDED');

    -- refund_status not allowed (domain violation)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'refund_status', 'domain',
        'refund_status value is not allowed',
        refund_status, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE refund_status IS NOT NULL
    AND UPPER(REPLACE(refund_status, ' ', '')) NOT IN 
        ('REFUNDED','NOTREFUNDED','NOTAPPLICABLE','PROCESSING','DISPUTED');

    -- payment_datetime in future
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'payment_datetime', 'out_of_range',
        'payment_datetime is in the future',
        CONVERT(NVARCHAR(50), payment_datetime, 120), payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE payment_datetime > GETDATE();

    -- Referential: order_id not found in erp_orders (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'order_id', 'referential',
        'order_id does not exist in erp_orders',
        p.order_id, p.payment_id, GETDATE()
    FROM silver.erp_payment p
    LEFT JOIN silver.erp_orders o ON p.order_id = o.order_id
    WHERE p.order_id IS NOT NULL AND o.order_id IS NULL;

    -- Referential: payment_channel_id not found in erp_payment_channel (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'payment_channel_id', 'referential',
        'payment_channel_id does not exist in erp_payment_channel',
        p.payment_channel_id, p.payment_id, GETDATE()
    FROM silver.erp_payment p
    LEFT JOIN silver.erp_payment_channel pc ON p.payment_channel_id = pc.payment_channel_id
    WHERE p.payment_channel_id IS NOT NULL AND pc.payment_channel_id IS NULL;

    -- Duplicate payment_id
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'payment_id', 'duplicate_key',
        'payment_id is duplicated',
        payment_id, payment_id, GETDATE()
    FROM (
        SELECT payment_id
        FROM silver.erp_payment
        WHERE payment_id IS NOT NULL
        GROUP BY payment_id
        HAVING COUNT(*) > 1
    ) t;

    -- Business rule: Only COMPLETED can be refunded
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'business_rule', 'business_rule',
        'payment is refunded but not COMPLETED',
        refund_status, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE UPPER(refund_status) = 'REFUNDED'
    AND UPPER(transaction_status) <> 'COMPLETED';

    -- Business rule: FAILED payments should not be refunded or in process/disputed
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'business_rule', 'business_rule',
        'FAILED payment with invalid refund_status',
        refund_status, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE UPPER(transaction_status) = 'FAILED'
    AND UPPER(REPLACE(refund_status, ' ', '')) NOT IN ('NOTAPPLICABLE','NOTREFUNDED');

    -- Business rule: is_refunded = 1 only if refund_status = REFUNDED
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'business_rule', 'business_rule',
        'is_refunded = 1 but refund_status is not REFUNDED',
        refund_status, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE ISNULL(is_refunded, 0) = 1 AND UPPER(refund_status) <> 'REFUNDED';

    -- Business rule: is_fraud = 1 and transaction_status = COMPLETED
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'business_rule', 'business_rule',
        'is_fraud is 1 but transaction is COMPLETED',
        CAST(is_fraud AS NVARCHAR(50)), payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE ISNULL(is_fraud, 0) = 1 AND UPPER(transaction_status) = 'COMPLETED';

    -- Business rule: Processing or Disputed only allowed on non-COMPLETED
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_payment', 'business_rule', 'business_rule',
        'Processing/disputed refund_status for completed payment',
        refund_status, payment_id, GETDATE()
    FROM silver.erp_payment
    WHERE UPPER(transaction_status) = 'COMPLETED'
    AND UPPER(refund_status) IN ('PROCESSING','DISPUTED');

    -- ====================================================================
    -- Checking 'silver.erp_shipment'
    -- ====================================================================
    -- Null or empty shipment_id (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_id', 'null_check',
        'shipment_id is NULL or empty',
        NULL, NULL, GETDATE()
    FROM silver.erp_shipment
    WHERE shipment_id IS NULL OR TRIM(shipment_id) = '';

    -- Null or empty order_id (guard TRIM for numeric)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'order_id', 'null_check',
        'order_id is NULL or empty',
        s.order_id, s.shipment_id, GETDATE()
    FROM silver.erp_shipment s
    WHERE s.order_id IS NULL
    OR TRIM(TRY_CONVERT(NVARCHAR(50), s.order_id)) = '';

    -- Null shipment_company_id
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_company_id', 'null_check',
        'shipment_company_id is NULL',
        NULL, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE shipment_company_id IS NULL;

    -- Null shipping_address_id
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipping_address_id', 'null_check',
        'shipping_address_id is NULL',
        NULL, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE shipping_address_id IS NULL;

    -- Null shipment_date
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_date', 'null_check',
        'shipment_date is NULL',
        NULL, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE shipment_date IS NULL;

    -- Null delivery_date
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'delivery_date', 'null_check',
        'delivery_date is NULL',
        NULL, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE delivery_date IS NULL;

    -- Null or empty shipment_type (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_type', 'null_check',
        'shipment_type is NULL or empty',
        shipment_type, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE shipment_type IS NULL OR TRIM(shipment_type) = '';

    -- Null or empty shipment_status (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_status', 'null_check',
        'shipment_status is NULL or empty',
        shipment_status, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE shipment_status IS NULL OR TRIM(shipment_status) = '';

    -- Null or empty delivery_status (text)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'delivery_status', 'null_check',
        'delivery_status is NULL or empty',
        delivery_status, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE delivery_status IS NULL OR TRIM(delivery_status) = '';

    -- shipment_id length > 15
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_id', 'format',
        'shipment_id length exceeds 15 characters',
        shipment_id, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE LEN(shipment_id) > 15;

    -- shipment_type not allowed (domain violation)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_type', 'domain',
        'shipment_type value is not allowed',
        shipment_type, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE shipment_type IS NOT NULL
    AND UPPER(shipment_type) NOT IN ('EXPRESS', 'STANDARD');

    -- shipment_status not allowed (domain violation)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_status', 'domain',
        'shipment_status value is not allowed',
        shipment_status, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE shipment_status IS NOT NULL
    AND UPPER(shipment_status) NOT IN ('DELIVERED', 'RETURNED', 'IN TRANSIT', 'LOST', 'PROCESSING');

    -- delivery_status not allowed (domain violation)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'delivery_status', 'domain',
        'delivery_status value is not allowed',
        delivery_status, shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE delivery_status IS NOT NULL
    AND UPPER(delivery_status) NOT IN ('SUCCESSFUL', 'DELAYED', 'RETURNED', 'PENDING', 'FAILED');

    -- shipment_date > delivery_date (invalid logic)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'date_logic', 'consistency',
        'shipment_date is after delivery_date',
        CONCAT('shipment_date:', shipment_date, ', delivery_date:', delivery_date), shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE shipment_date IS NOT NULL AND delivery_date IS NOT NULL AND shipment_date > delivery_date;

    -- Referential: order_id not found in erp_orders (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'order_id', 'referential',
        'order_id does not exist in erp_orders',
        s.order_id, s.shipment_id, GETDATE()
    FROM silver.erp_shipment s
    LEFT JOIN silver.erp_orders o ON s.order_id = o.order_id
    WHERE s.order_id IS NOT NULL AND o.order_id IS NULL;

    -- Referential: shipment_company_id not found in erp_shipment_company (QUALIFIED)
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_company_id', 'referential',
        'shipment_company_id does not exist in erp_shipment_company',
        CAST(s.shipment_company_id AS VARCHAR(50)), s.shipment_id, GETDATE()
    FROM silver.erp_shipment s
    LEFT JOIN silver.erp_shipment_company c ON s.shipment_company_id = c.shipment_company_id
    WHERE s.shipment_company_id IS NOT NULL AND c.shipment_company_id IS NULL;

    -- Invalid status/delivery_status combination
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'status_combo', 'consistency',
        'invalid shipment_status/delivery_status combination',
        CONCAT('shipment_status:', shipment_status, ', delivery_status:', delivery_status), shipment_id, GETDATE()
    FROM silver.erp_shipment
    WHERE NOT (
        (UPPER(shipment_status) = 'DELIVERED'  AND UPPER(delivery_status) IN ('SUCCESSFUL', 'DELAYED'))
    OR (UPPER(shipment_status) = 'RETURNED'   AND UPPER(delivery_status) = 'RETURNED')
    OR (UPPER(shipment_status) = 'LOST'       AND UPPER(delivery_status) = 'FAILED')
    OR (UPPER(shipment_status) = 'IN TRANSIT' AND UPPER(delivery_status) = 'PENDING')
    OR (UPPER(shipment_status) = 'PROCESSING' AND UPPER(delivery_status) = 'PENDING')
    );

    -- Duplicate shipment_id
    INSERT INTO silver.dq_health (dq_table, dq_column, dq_type, dq_issue, dq_value, primary_key, detected_at)
    SELECT 'silver.erp_shipment', 'shipment_id', 'duplicate_key',
        'shipment_id is duplicated',
        shipment_id, shipment_id, GETDATE()
    FROM (
        SELECT shipment_id
        FROM silver.erp_shipment
        WHERE shipment_id IS NOT NULL
        GROUP BY shipment_id
        HAVING COUNT(*) > 1
    ) t;
END
