/*
===============================================================================
Stored Procedure: Load Silver Layer (silver -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'silver' schema.
	Actions Performed:
		- Truncates silver tables.
		- Inserts transformed and cleansed data from silver into silver tables.
		
Parameters:
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: silver.crm_customer';
		TRUNCATE TABLE silver.crm_customer;
		PRINT '>> Inserting Data Into: silver.crm_customer';
		INSERT INTO silver.crm_customer (
		customer_id, username, email, first_name, 
		last_name, gender, age, age_group, 
		birth_date, phone_number, registration_datetime, 
		is_loyalty_member, is_fraud_suspected, 
		customer_segment, dwh_load_date
		) 
		SELECT 
			customer_id, 
			TRIM(username) username, 
			CASE 
                WHEN email LIKE '%@%' AND email LIKE '%.%' THEN lower(TRIM(email)) 
                ELSE NULL 
            END AS email, 
			TRIM(first_name) first_name, 
			TRIM(last_name) last_name, 
			CASE 
              WHEN UPPER(TRIM(gender)) = 'FEMALE' THEN 'F'
              WHEN UPPER(TRIM(gender)) = 'MALE'   THEN 'M'
              ELSE 'N/A' 
            END AS gender, 
			CASE 
                WHEN birth_date IS NOT NULL
                THEN DATEDIFF(year, birth_date, getdate())
                ELSE NULL
            END AS age,
            CASE 
                WHEN birth_date IS NULL THEN NULL
                WHEN DATEDIFF(year, birth_date, getdate()) < 18 THEN 'Under 18' 
                WHEN DATEDIFF(year, birth_date, getdate()) < 25 THEN '18-24' 
                WHEN DATEDIFF(year, birth_date, getdate()) < 35 THEN '25-34' 
                WHEN DATEDIFF(year, birth_date, getdate()) < 45 THEN '35-44' 
                WHEN DATEDIFF(year, birth_date, getdate()) < 55 THEN '45-54' 
                WHEN DATEDIFF(year, birth_date, getdate()) < 65 THEN '55-64' 
                ELSE '65+'  
            END AS age_group, 
			birth_date, 
			TRIM(phone_number) as phone_number, 
			CAST(registration_datetime AS DATETIME2(3)) registration_datetime, 
			CASE 
                WHEN UPPER(TRIM(is_loyalty_member)) = 'TRUE' THEN 1 
                WHEN UPPER(TRIM(is_loyalty_member)) = 'FALSE' THEN 0 
                ELSE NULL 
            END AS is_loyalty_member, 
			CASE 
                WHEN UPPER(TRIM(is_fraud_suspected)) = 'TRUE' THEN 1 
                WHEN UPPER(TRIM(is_fraud_suspected)) = 'FALSE' THEN 0 
                ELSE NULL 
            END AS is_fraud_suspected, 
			UPPER(TRIM(customer_segment)) AS customer_segment,
			GETDATE() AS dwh_load_date
		FROM 
			(
				SELECT 
				*, 
				ROW_NUMBER() OVER (
					PARTITION BY customer_id ORDER BY registration_date DESC) as rn 
				FROM bronze.crm_customer 
				WHERE customer_id IS NOT NULL
			) t 
		WHERE rn = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_customer_addresses';
		TRUNCATE TABLE silver.crm_customer_addresses;
		PRINT '>> Inserting Data Into: silver.crm_customer_addresses';
        INSERT INTO silver.crm_customer_addresses (
            address_id,
            customer_id,
            country,
            country_code,
            province,
            province_code,
            city,
            district,
            postal_code,
            full_address,
            dwh_load_date
        )
        SELECT
            address_id,
            customer_id,
            TRIM(country),
            UPPER(TRIM(country_code)),
            TRIM(province),
            UPPER(TRIM(province_code)),
            TRIM(city),
            TRIM(district),
            CASE
                WHEN postal_code IS NOT NULL
                    AND LEN(TRIM(postal_code)) BETWEEN 3 AND 10
                    AND PATINDEX('%[^A-Z0-9-]%', UPPER(postal_code)) = 0
                THEN UPPER(TRIM(postal_code))
                ELSE NULL
            END AS postal_code,
            TRIM(full_address),
            GETDATE() AS dwh_load_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY address_id, customer_id
                    ORDER BY LEN(full_address) DESC, address_id
                ) AS rn
            FROM bronze.crm_customer_addresses
            WHERE address_id IS NOT NULL
                AND customer_id IS NOT NULL
                AND full_address IS NOT NULL
        ) filtered
        WHERE rn = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_payment_channel';
		TRUNCATE TABLE silver.erp_payment_channel;
		PRINT '>> Inserting Data Into: silver.erp_payment_channel';
		INSERT INTO silver.erp_payment_channel (
            payment_channel_id,
            channel_name,
            provider_name,
            country_code,
            is_banktransfer,
            is_card,
            is_wallet,
            dwh_load_date
        )
        SELECT
            payment_channel_id,
            clean_channel_name AS channel_name,
            clean_provider_name AS provider_name,
            clean_country_code AS country_code,
            is_banktransfer,
            is_card,
            is_wallet,
            GETDATE() AS dwh_load_date
        FROM (
            SELECT
                payment_channel_id,
                CASE WHEN LEN(TRIM(channel_name)) > 0 THEN UPPER(TRIM(channel_name)) ELSE NULL
                END AS clean_channel_name,
                CASE WHEN LEN(TRIM(provider_name)) > 0 THEN UPPER(TRIM(provider_name)) ELSE NULL
                END AS clean_provider_name,
                CASE
                    WHEN UPPER(TRIM(country_code)) = 'GLOBAL' THEN 'GLOBAL'
                    WHEN LEN(TRIM(country_code)) = 2 AND TRIM(country_code) NOT LIKE '%[^A-Z]%' THEN UPPER(TRIM(country_code))
                    ELSE NULL
                END AS clean_country_code,
                CASE
                    WHEN UPPER(TRIM(channel_name)) IN (
                        'APPLE PAY', 'GOOGLE PAY', 'SAMSUNG PAY', 'ANDROID PAY',
                        'VIVA WALLET', 'MOBILEPAY', 'PAYCONIQ', 'MB WAY', 'PAYLIB',
                        'POSTEPAY', 'PAYSERA', 'VIPPS', 'SWISH', 'TWINT',
                        'ALIPAY', 'WECHAT PAY', 'PAYM', 'ZELLE',
                        'PREPAID CARD', 'DIGITAL WALLET', 'E-WALLET'
                    )
                    OR UPPER(TRIM(provider_name)) IN (
                        'PAYPAL', 'VIVA WALLET', 'MOBILEPAY', 'PAYCONIQ', 'PAYLIB', 
                        'PAYSERA', 'VIPPS', 'SWISH', 'TWINT', 'ALIPAY', 'WECHAT',
                        'APPLE', 'GOOGLE',
                        'POSTE ITALIANE', 'REVOLUT', 'MONZO', 'N26'
                    ) THEN 1 ELSE 0 END AS is_wallet,
                CASE
                    WHEN UPPER(TRIM(channel_name)) IN (
                        'CREDIT CARD', 'DEBIT CARD', 'CARTE BANCAIRE', 'CREDIT', 'DEBIT',
                        'CARD PAYMENT', 'CARD', 'VISA', 'MASTERCARD', 'MAESTRO',
                        'AMERICAN EXPRESS', 'AMEX', 'DINERS', 'DISCOVER', 'JCB',
                        'DANKORT', 'BANCOMAT', 'GIROCARD', 'CARTASI'
                    )
                    OR UPPER(TRIM(provider_name)) IN (
                        'VISA', 'MASTERCARD', 'MAESTRO', 'AMERICAN EXPRESS', 'AMEX',
                        'DINERS', 'DISCOVER', 'JCB', 'UNIONPAY',
                        'JCC', 'BANKART', 'NEXI', 'WORLDPAY', 'ADYEN',
                        'STRIPE', 'SQUARE', 'PAYMENTWALL'
                    ) THEN 1 ELSE 0 END AS is_card,
                CASE
                    WHEN UPPER(TRIM(channel_name)) IN (
                        'BANK TRANSFER', 'WIRE TRANSFER', 'ONLINE BANKING', 'BANK LINK',
                        'INSTANT TRANSFER', 'FASTER PAYMENTS', 'REAL TIME PAYMENTS',
                        'SEPA DIRECT DEBIT', 'SEPA CREDIT TRANSFER', 'BANCONTACT',
                        'IDEAL', 'SOFORT', 'GIROPAY', 'EPS', 'MYBANK', 'TRUSTLY',
                        'PAYDIREKT', 'PRZELEWY24', 'DOTPAY', 'BLIK', 'PAYBYBANK',
                        'MULTIBANCO', 'BIZUM', 'PIX', 'INTERAC', 'BACS', 'ACH',
                        'DIRECT DEBIT', 'STANDING ORDER', 'BANK PAYMENT',
                        'INTERNET BANKING', 'MOBILE BANKING', 'TELEPHONE BANKING'
                    )
                    OR UPPER(TRIM(provider_name)) IN (
                        'SEPA', 'GIROPAY', 'BIZUM', 'KLARNA', 'TRUSTLY',
                        'MULTIBANCO', 'PAYDIREKT', 'PRZELEWY24', 'DOTPAY', 'BLIK',
                        'SWIFT',
                        'LOCAL BANKS', 'SWEDBANK', 'SEB PANK', 'SEB BANKA', 
                        'OSUUSPANKKI', 'SLOVENSKA SPORITELNA', 'ERSTE BANK',
                        'RAIFFEISEN', 'SANTANDER', 'BBVA', 'BNP PARIBAS',
                        'SOFORT', 'PAYBYBANK', 'ECOSPEND', 'BANKED'
                    ) THEN 1 ELSE 0 END AS is_banktransfer,
                ROW_NUMBER() OVER (PARTITION BY payment_channel_id ORDER BY payment_channel_id) AS rn
            FROM bronze.erp_payment_channel
        ) t
        WHERE t.rn = 1
            AND clean_channel_name IS NOT NULL
            AND clean_provider_name IS NOT NULL
            AND clean_country_code IS NOT NULL
            AND (is_wallet = 1 OR is_card = 1 OR is_banktransfer = 1);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_shipment_company';
		TRUNCATE TABLE silver.erp_shipment_company;
		PRINT '>> Inserting Data Into: silver.erp_shipment_company';
        ;WITH cleaned AS (
            SELECT
                shipment_company_id,
                TRIM(company_name) AS company_name,
                CASE
                    WHEN LEN(TRIM(country_code)) = 2 AND TRIM(country_code) NOT LIKE '%[^A-Z]%' THEN UPPER(TRIM(country_code))
                    ELSE NULL
                END AS country_code,
                UPPER(REPLACE(REPLACE(TRIM(operating_countries), ' ,', ','), ', ', ',')) AS operating_countries,
                UPPER(REPLACE(REPLACE(TRIM(delivery_types),          ' ,', ','), ', ', ',')) AS delivery_types
            FROM bronze.erp_shipment_company
        ),
        counts AS (
            SELECT
                c.*,
                oc_ct.oc_count
            FROM cleaned AS c
            CROSS APPLY (
                SELECT COUNT(*) AS oc_count
                FROM STRING_SPLIT(c.operating_countries, ',')
            ) AS oc_ct
        ),
        flags AS (
            SELECT
                cnt.*,
                CASE WHEN ',' + cnt.delivery_types + ',' LIKE '%,STANDARD,%'       THEN 1 ELSE 0 END AS is_standard,
                CASE WHEN ',' + cnt.delivery_types + ',' LIKE '%,EXPRESS,%'        THEN 1 ELSE 0 END AS is_express,
                CASE WHEN ',' + cnt.delivery_types + ',' LIKE '%,REGISTERED,%'     THEN 1 ELSE 0 END AS is_registered,
                CASE WHEN ',' + cnt.delivery_types + ',' LIKE '%,INTERNATIONAL,%'  THEN 1 ELSE 0 END AS is_international
            FROM counts AS cnt
        ),
        derived AS (
            SELECT
                f.shipment_company_id,
                f.company_name,
                f.country_code,
                f.is_standard,
                f.is_express,
                f.is_registered,
                f.is_international,
                CASE
                    WHEN f.oc_count > 5 THEN 'GLOBAL_COURIER'
                    WHEN f.oc_count > 2 THEN 'REGIONAL_COURIER'
                    ELSE 'LOCAL_COURIER'
                END AS company_type,
                ROW_NUMBER() OVER (
                    PARTITION BY f.shipment_company_id
                    ORDER BY f.shipment_company_id
                ) AS rn
            FROM flags AS f
        )
        INSERT INTO silver.erp_shipment_company (
            shipment_company_id, company_name, company_type, country_code,
            is_standard, is_express, is_registered, is_international, dwh_load_date
        )
        SELECT
            shipment_company_id,
            company_name,
            company_type,
            country_code,
            is_standard,
            is_express,
            is_registered,
            is_international,
            GETDATE() AS dwh_load_date
        FROM derived
        WHERE rn = 1 AND country_code IS NOT NULL
        ORDER BY shipment_company_id;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_product';
		TRUNCATE TABLE silver.erp_product;
		PRINT '>> Inserting Data Into: silver.erp_product';
        INSERT INTO silver.erp_product (
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            unit_price,
            cost_price,
            profit_margin,
            profit_amount,
            rating,
            review_count,
            dwh_load_date
        )
        SELECT 
            product_id,
            COALESCE(UPPER(TRIM(product_name)), 'UNKNOWN_PRODUCT') AS product_name,
            COALESCE(UPPER(TRIM(category)), 'UNCATEGORIZED') AS category,
            COALESCE(UPPER(TRIM(sub_category)), 'UNCATEGORIZED') AS sub_category,
            COALESCE(UPPER(TRIM(brand)), 'NO_BRAND') AS brand,
            CASE 
                WHEN unit_price < 0 THEN NULL 
                ELSE ROUND(COALESCE(unit_price, 0), 2) 
            END AS unit_price,
            CASE 
                WHEN cost_price < 0 THEN NULL 
                ELSE ROUND(COALESCE(cost_price, 0), 2) 
            END AS cost_price,
            -- Only calculate profit_margin if both prices positive and cost<unit_price
            CASE
                WHEN cost_price > 0 AND unit_price > 0 AND cost_price < unit_price
                THEN CAST(ROUND(((unit_price - cost_price) / cost_price) * 100, 2) AS DECIMAL(10,2))
                ELSE NULL
            END AS profit_margin,
            CASE
                WHEN unit_price > 0 AND cost_price > 0 
                THEN unit_price - cost_price
                ELSE NULL
            END AS profit_amount,
            -- Ratings forced into 0-5.0
            CASE
                WHEN rating BETWEEN 0 AND 5 THEN rating
                WHEN rating > 5 THEN 5.0
                ELSE NULL
            END AS rating,
            -- Negative reviews become 0
            CASE 
                WHEN review_count < 0 THEN 0 
                ELSE COALESCE(review_count, 0) 
            END AS review_count,
            GETDATE() AS dwh_load_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id, product_name, category, sub_category, brand) AS rn
            FROM bronze.erp_product
            WHERE product_id IS NOT NULL
        ) t
        WHERE rn = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_product_prices';
		TRUNCATE TABLE silver.erp_product_prices;
		PRINT '>> Inserting Data Into: silver.erp_product_prices';
        INSERT INTO silver.erp_product_prices (
            product_id,
            country_code,
            local_price,
            price_type,
            effective_date,
            dwh_load_date
        )
        SELECT
            product_id,
            country_code,
            local_price,
            price_type,
            effective_date,
            GETDATE() AS dwh_load_date
        FROM (
            SELECT
                product_id,
                CASE
                    WHEN LEN(TRIM(country_code)) = 2 AND TRIM(country_code) NOT LIKE '%[^A-Z]%' THEN UPPER(TRIM(country_code))
                    ELSE NULL
                END AS country_code,
                CASE
                    WHEN local_price > 0 AND local_price <= 999999.99 THEN ROUND(local_price, 2) 
                    ELSE NULL
                END AS local_price,
                CASE
                    WHEN TRIM(price_type) IN ('RETAIL', 'WHOLESALE', 'DISCOUNT', 'PROMOTIONAL') THEN UPPER(TRIM(price_type))
                    ELSE NULL
                END AS price_type,
                CASE
                    WHEN effective_date >= '2023-01-01' AND effective_date <= CAST(GETDATE() AS DATE) THEN CAST(effective_date AS DATE)
                    ELSE NULL
                END AS effective_date,
                ROW_NUMBER() OVER (PARTITION BY product_id, country_code, effective_date, price_type ORDER BY  effective_date DESC) AS rn
            FROM bronze.erp_product_prices
        ) t
        WHERE rn = 1
            AND country_code IS NOT NULL
            AND local_price IS NOT NULL
            AND price_type IS NOT NULL
            AND effective_date IS NOT NULL;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_orders';
		TRUNCATE TABLE silver.erp_orders;
		PRINT '>> Inserting Data Into: silver.erp_orders';
        INSERT INTO silver.erp_orders (
            order_id,
            customer_id,
            shipping_address_id,
            shipment_company_id,
            order_datetime,
            total_price,
            order_status,
            is_cancelled,
            is_returned,
            return_reason,
            cancellation_reason,
            country_code,
            dwh_load_date
        )
        SELECT
            order_id,
            customer_id,
            shipping_address_id,
            shipment_company_id,
            order_datetime,
            total_price,
            order_status_fnl,
            -- Set is_cancelled to 1 only if both final status is CANCELLED AND original flag was TRUE
            CASE WHEN order_status_fnl = 'CANCELLED' AND UPPER(TRIM(is_cancelled)) ='TRUE' THEN 1 ELSE 0 END AS is_cancelled,
            -- Set is_returned to 1 only if both final status is RETURNED AND original flag was TRUE
            CASE WHEN order_status_fnl = 'RETURNED' AND UPPER(TRIM(is_returned)) = 'TRUE' THEN 1 ELSE 0 END AS is_returned,
            -- Populate return_reason only if final status is RETURNED
            CASE WHEN order_status_fnl = 'RETURNED' THEN UPPER(TRIM(return_reason)) ELSE NULL END AS return_reason,
            -- Populate cancellation_reason only if final status is CANCELLED (using return_reason column)
            CASE WHEN order_status_fnl = 'CANCELLED' THEN UPPER(TRIM(return_reason)) ELSE NULL END AS cancellation_reason,
            country_code,
            GETDATE() AS dwh_load_date
        FROM (
            SELECT
            order_id,
            customer_id,
            shipping_address_id,
            shipment_company_id,
            CAST(CONCAT(order_date, ' ', order_time) AS DATETIME2) AS order_datetime,
            total_price,
            -- Determine final order status with consistent string handling
            CASE
                WHEN UPPER(TRIM(order_status)) = 'COMPLETED' AND UPPER(TRIM(is_returned)) = 'FALSE' AND UPPER(TRIM(is_cancelled)) = 'FALSE' THEN 'COMPLETED'
                WHEN UPPER(TRIM(order_status)) = 'COMPLETED' AND UPPER(TRIM(is_returned)) = 'TRUE' AND UPPER(TRIM(is_cancelled)) = 'FALSE' THEN 'RETURNED'
                WHEN UPPER(TRIM(order_status)) = 'CANCELLED' AND UPPER(TRIM(is_returned)) = 'FALSE' AND UPPER(TRIM(is_cancelled)) = 'TRUE' THEN 'CANCELLED'
                WHEN UPPER(TRIM(order_status)) = 'CANCELLED' AND UPPER(TRIM(is_returned)) = 'TRUE' AND UPPER(TRIM(is_cancelled)) = 'TRUE' THEN 'CANCELLED'
                ELSE NULL
            END AS order_status_fnl,
            is_cancelled,
            is_returned,
            return_reason,
            CASE
                WHEN LEN(TRIM(country_code)) = 2 AND TRIM(country_code) NOT LIKE '%[^A-Z]%' THEN UPPER(TRIM(country_code))
                ELSE NULL
            END AS country_code,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC, order_time DESC) AS rn
            FROM bronze.erp_orders
            WHERE order_id IS NOT NULL
                AND customer_id IS NOT NULL
                AND order_date IS NOT NULL
                AND order_time IS NOT NULL
                AND total_price IS NOT NULL AND total_price >= 0
                AND order_date <= CAST(GETDATE() AS DATE) 
                AND order_status IS NOT NULL AND TRIM(order_status) != ''
                AND country_code IS NOT NULL AND TRIM(country_code) != ''
        ) t
        WHERE rn = 1 

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_order_detail';
		TRUNCATE TABLE silver.erp_order_detail;
		PRINT '>> Inserting Data Into: silver.erp_order_detail';
        INSERT INTO silver.erp_order_detail (
            order_detail_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_amount,
            sales_amount,
            recalc_sales_amount,
            sales_match_flag,
            order_datetime,
            dwh_load_date
        )
        SELECT 
            t.order_detail_id,
            t.order_id,
            t.product_id,
            t.quantity,
            ROUND(t.unit_price, 2) AS unit_price,
            ROUND(t.discount_amount, 2) AS discount_amount,
            ROUND(t.sales_amount, 2) AS sales_amount,
            ROUND((t.quantity * t.unit_price) - t.discount_amount,2) AS recalc_sales_amount, --(quantity * unit_price) - discount_amount
            CASE 
                WHEN ABS(ROUND((t.quantity * t.unit_price) - t.discount_amount, 2) - ROUND(t.sales_amount,2)) <= 0.01
                THEN 1 ELSE 0 END AS sales_match_flag, -- 1 if sales_amount_ and recalc_sales_amount match within 0.01, else 0
            t.order_datetime,
            GETDATE() AS dwh_load_date
        FROM (
            SELECT
                od.order_detail_id,
                od.order_id,
                od.product_id,
                od.quantity,
                od.unit_price,
                od.discount_amount,
                od.sales_amount,
                od.order_datetime,
                ROW_NUMBER() OVER (PARTITION BY od.order_detail_id, od.order_id ORDER BY od.order_datetime DESC, od.product_id DESC) AS rn
            FROM bronze.erp_order_detail od
            JOIN silver.erp_orders o ON od.order_id = o.order_id
            JOIN silver.erp_product p ON od.product_id = p.product_id
            WHERE 
                od.order_detail_id IS NOT NULL
                AND od.order_id IS NOT NULL
                AND od.product_id IS NOT NULL
                AND od.quantity IS NOT NULL AND od.quantity > 0
                AND od.unit_price IS NOT NULL AND od.unit_price >= 0
                AND od.discount_amount IS NOT NULL AND od.discount_amount >= 0
                AND od.sales_amount IS NOT NULL AND od.sales_amount >= 0
                AND od.order_datetime IS NOT NULL
                AND od.order_datetime <= GETDATE()
        ) t
        WHERE t.rn = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_invoice';
		TRUNCATE TABLE silver.erp_invoice;
		PRINT '>> Inserting Data Into: silver.erp_invoice';
        INSERT INTO silver.erp_invoice (
            invoice_id,
            order_id,
            unit_price,
            tax_amount,
            final_amount,
            invoice_status,
            invoice_datetime,
            is_amount_correct,
            dwh_load_date
        )
        SELECT
            TRIM(t.invoice_id),
            t.order_id,
            ROUND(t.unit_price,2) AS unit_price,
            ROUND(t.tax_amount, 2) AS tax_amount,
            ROUND(t.final_amount,2) AS final_amount, 
            UPPER(TRIM(t.invoice_status)) AS invoice_status,
            t.invoice_datetime,
            CASE
                WHEN ROUND(t.unit_price + t.tax_amount, 2) = ROUND(t.final_amount, 2) THEN 1 ELSE 0
            END AS is_amount_correct, -- Audit flag: 1 if invoice totals add up correctly, else 0
            GETDATE() AS dwh_load_date
        FROM (
            SELECT
                b.invoice_id,
                b.order_id,
                b.unit_price,
                b.tax_amount,
                b.final_amount,
                b.invoice_status,
                b.invoice_datetime,
                ROW_NUMBER() OVER (PARTITION BY b.invoice_id ORDER BY b.invoice_datetime DESC, b.order_id DESC) AS rn
            FROM bronze.erp_invoice b
            JOIN silver.erp_orders o ON b.order_id = o.order_id
            WHERE b.invoice_id IS NOT NULL AND b.invoice_id != ''
                AND b.order_id IS NOT NULL
                AND b.unit_price IS NOT NULL AND b.unit_price >= 0
                AND b.tax_amount IS NOT NULL AND b.tax_amount >= 0
                AND b.final_amount IS NOT NULL AND b.final_amount >= 0
                AND b.invoice_datetime IS NOT NULL
                AND b.invoice_datetime <= GETDATE()
        ) t
        WHERE t.rn = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_invoice_detail';
		TRUNCATE TABLE silver.erp_invoice_detail;
		PRINT '>> Inserting Data Into: silver.erp_invoice_detail';
		INSERT INTO silver.erp_invoice_detail (
			invoice_detail_id,
			invoice_id,
			product_id,
			quantity,
			sales_amount,
			tax_amount,
			line_total,
			tax_rate,
			invoice_datetime,
			dwh_load_date
		)
		SELECT
			t.invoice_detail_id,
			t.invoice_id,
			t.product_id,
			t.quantity,
			ROUND(t.sales_amount, 2) AS sales_amount,
			ROUND(t.tax_amount, 2)   AS tax_amount,
			ROUND(t.sales_amount + t.tax_amount, 2) AS line_total,
			CASE 
                WHEN t.sales_amount = 0 THEN 0 
                ELSE CAST(ROUND(t.tax_amount / t.sales_amount, 4) AS DECIMAL(9, 4)) 
			END AS tax_rate,
			t.invoice_datetime,
			GETDATE() AS dwh_load_date
		FROM (
			SELECT
			d.invoice_detail_id,
			d.invoice_id,
			d.product_id,
			d.quantity,
			d.sales_amount,
			d.tax_amount,
			d.invoice_datetime,
			ROW_NUMBER() OVER (PARTITION BY d.invoice_detail_id, d.invoice_id,d.product_id ORDER BY d.invoice_datetime DESC, d.product_id DESC) AS rn
			FROM bronze.erp_invoice_detail d
			JOIN silver.erp_invoice i ON d.invoice_id = i.invoice_id
			JOIN silver.erp_product p ON d.product_id = p.product_id
			WHERE 
                d.invoice_detail_id IS NOT NULL AND d.invoice_detail_id != ''
			    AND d.invoice_id IS NOT NULL AND d.invoice_id !=''
			    AND d.product_id IS NOT NULL
                AND d.quantity IS NOT NULL AND d.quantity > 0
                AND d.sales_amount IS NOT NULL AND d.sales_amount >= 0
                AND d.tax_amount IS NOT NULL AND d.tax_amount >= 0
                AND d.invoice_datetime IS NOT NULL
                AND d.invoice_datetime <= GETDATE()
		) t
		WHERE t.rn = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_payment';
		TRUNCATE TABLE silver.erp_payment;
		PRINT '>> Inserting Data Into: silver.erp_payment';
        INSERT INTO silver.erp_payment (
            payment_id,
            order_id,
            payment_channel_id,
            payment_datetime,
            final_amount,
            transaction_status,
            is_fraud,
            refund_status,
            is_refunded,
            rule_violation,
            dwh_load_date
        )
        SELECT
            t.payment_id,
            t.order_id,
            t.payment_channel_id,
            t.payment_datetime,
            ROUND(t.final_amount, 2) AS final_amount,
            UPPER(TRIM(t.transaction_status)) AS transaction_status,
            CASE WHEN UPPER(TRIM(t.is_fraud)) IN ('TRUE','1') THEN 1 ELSE 0 END AS is_fraud,
            UPPER(TRIM(t.refund_status)) AS refund_status,
            CASE 
                WHEN UPPER(TRIM(t.transaction_status)) = 'COMPLETED' AND UPPER(TRIM(t.refund_status)) = 'REFUNDED' THEN 1
                ELSE 0
            END AS is_refunded,
            -- Business rule violation flag (not filtering)
            CASE
                WHEN UPPER(TRIM(t.refund_status)) = 'REFUNDED' AND UPPER(TRIM(t.transaction_status)) != 'COMPLETED'
                    THEN 'BR1: Refunded but not completed'
                WHEN UPPER(TRIM(t.transaction_status)) = 'FAILED'
                    AND UPPER(TRIM(t.refund_status)) NOT IN ('NOT APPLICABLE', 'NOT_REFUNDED','NOT REFUNDED','NOTAPPLICABLE')
                    THEN 'BR2: Failed payment with invalid refund_status'
                WHEN
                    (CASE WHEN UPPER(TRIM(t.transaction_status)) = 'COMPLETED' AND UPPER(TRIM(t.refund_status)) = 'REFUNDED' THEN 1 ELSE 0 END) !=
                    (CASE WHEN UPPER(TRIM(t.refund_status)) = 'REFUNDED' THEN 1 ELSE 0 END)
                    THEN 'BR3: is_refunded flag mismatch'
                WHEN CASE WHEN UPPER(TRIM(t.is_fraud)) IN ('TRUE','1') THEN 1 ELSE 0 END = 1
                    AND UPPER(TRIM(t.transaction_status)) = 'COMPLETED'
                    THEN 'BR4: Fraud flagged but completed'
                WHEN UPPER(TRIM(t.transaction_status)) = 'COMPLETED'
                    AND UPPER(TRIM(t.refund_status)) IN ('PROCESSING', 'DISPUTED')
                    THEN 'BR5: Processing/disputed status for completed'
                ELSE NULL
            END AS rule_violation,
            GETDATE() AS dwh_load_date
        FROM (
            SELECT
                p.payment_id,
                p.order_id,
                p.payment_channel_id,
                p.payment_datetime,
                p.final_amount,
                p.transaction_status,
                p.is_fraud,
                p.refund_status,
                ROW_NUMBER() OVER (PARTITION BY p.payment_id ORDER BY p.payment_datetime DESC, p.order_id DESC) AS rn
            FROM bronze.erp_payment p
            JOIN silver.erp_orders o ON p.order_id = o.order_id
            JOIN silver.erp_payment_channel pc ON p.payment_channel_id = pc.payment_channel_id
            WHERE p.payment_id IS NOT NULL AND p.payment_id != ''
                AND p.order_id IS NOT NULL
                AND p.payment_channel_id IS NOT NULL AND p.payment_channel_id != ''
                AND p.payment_datetime IS NOT NULL AND p.payment_datetime <= GETDATE()
                AND p.final_amount IS NOT NULL AND p.final_amount >= 0
                AND p.transaction_status IS NOT NULL AND p.transaction_status != ''
                AND p.is_fraud IS NOT NULL AND p.is_fraud != ''
                AND p.refund_status IS NOT NULL AND p.refund_status != ''
        ) t
        WHERE t.rn = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_shipment';
		TRUNCATE TABLE silver.erp_shipment;
		PRINT '>> Inserting Data Into: silver.erp_shipment';
		INSERT INTO silver.erp_shipment (
			shipment_id,
			order_id,
			shipment_company_id,
			shipping_address_id,
			shipment_date,
			delivery_date,
			shipment_type,
			shipment_status,
			delivery_status,
			transit_days,
			is_delayed,
			is_failed,
			is_returned_delivery,
			is_lost,
			is_valid_status_combo,
			dwh_load_date
		)
		SELECT
			t.shipment_id,
			t.order_id,
			t.shipment_company_id,
			t.shipping_address_id,
			t.shipment_date,
			t.delivery_date,
			t.shipment_type,
			t.shipment_status,
			t.delivery_status,
			DATEDIFF(day, t.shipment_date, t.delivery_date) AS transit_days, -- Days between shipment and delivery
			CASE WHEN t.delivery_status = 'DELAYED' THEN 1 ELSE 0 END AS is_delayed,
			CASE WHEN t.delivery_status = 'FAILED' THEN 1 ELSE 0 END AS is_failed,
			CASE WHEN t.delivery_status = 'RETURNED' THEN 1 ELSE 0 END AS is_returned_delivery,
			CASE WHEN t.shipment_status  = 'LOST' THEN 1 ELSE 0 END AS is_lost,
			-- check for valid status combinations
			CASE
				WHEN t.shipment_status = 'DELIVERED' AND t.delivery_status IN ('SUCCESSFUL', 'DELAYED') THEN 1
				WHEN t.shipment_status = 'RETURNED' AND t.delivery_status = 'RETURNED' THEN 1
				WHEN t.shipment_status = 'LOST' AND t.delivery_status = 'FAILED' THEN 1
				WHEN t.shipment_status = 'IN TRANSIT' AND t.delivery_status = 'PENDING' THEN 1
				WHEN t.shipment_status = 'PROCESSING' AND t.delivery_status = 'PENDING' THEN 1
				ELSE 0
			END AS is_valid_status_combo,
			GETDATE() AS dwh_load_date
		FROM (
			SELECT
				TRIM(s.shipment_id) AS shipment_id,
				s.order_id,
				s.shipment_company_id,
				s.shipping_address_id,
				s.shipment_date,
				s.delivery_date,
				UPPER(TRIM(s.shipment_type)) AS shipment_type,
				UPPER(TRIM(s.shipment_status)) AS shipment_status,
				UPPER(TRIM(s.delivery_status)) AS delivery_status,
				ROW_NUMBER() OVER (PARTITION BY TRIM(s.shipment_id) ORDER BY s.shipment_date DESC) AS rn
			FROM bronze.erp_shipment s
			JOIN silver.erp_orders o ON s.order_id = o.order_id
			JOIN silver.erp_shipment_company c ON s.shipment_company_id = c.shipment_company_id
			WHERE s.shipment_id IS NOT NULL
				AND s.order_id IS NOT NULL
				AND s.shipment_company_id IS NOT NULL
				AND s.shipping_address_id IS NOT NULL
				AND s.shipment_date IS NOT NULL
				AND s.delivery_date IS NOT NULL
				AND s.shipment_date <= s.delivery_date
				AND UPPER(TRIM(s.shipment_type)) IN ('EXPRESS', 'STANDARD')
				AND UPPER(TRIM(s.shipment_status)) IN ('DELIVERED', 'RETURNED', 'IN TRANSIT', 'LOST', 'PROCESSING')
				AND UPPER(TRIM(s.delivery_status)) IN ('SUCCESSFUL', 'DELAYED', 'RETURNED', 'PENDING', 'FAILED')
		) t
		WHERE t.rn = 1;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
