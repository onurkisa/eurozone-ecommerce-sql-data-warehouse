/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID ('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
	customer_id,
	username,
	email,
	first_name,
	last_name,
	CONCAT(first_name, ' ',last_name) AS full_name,
	gender,
	age,
	age_group,
	birth_date,
	phone_number,
	registration_datetime,
	is_loyalty_member,
	is_fraud_suspected,
	customer_segment
FROM silver.crm_customer
GO;
-- =============================================================================
-- Create Dimension: gold.dim_address
-- =============================================================================
IF OBJECT_ID('gold.dim_address', 'V') IS NOT NULL
	DROP VIEW gold.dim_address;
GO

CREATE VIEW gold.dim_address AS
SELECT
	ROW_NUMBER() OVER (ORDER BY address_id) AS address_key,
	address_id,
	customer_id,
	country,
	country_code,
	province,
	province_code,
	district,
	postal_code,
	full_address
FROM silver.crm_customer_addresses
GO;
-- =============================================================================
-- Create Dimension: gold.dim_product
-- =============================================================================
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS
WITH avg_price_cte AS ( 
    SELECT AVG(unit_price * 1.0) AS avg_unit_price -- Calculate the average unit price of all products for dynamic price tiering
    FROM silver.erp_product
)
SELECT
    ROW_NUMBER() OVER (ORDER BY p.product_id) AS product_key, -- Surrogate key for dimensional modeling
    p.product_id,
    TRIM(p.product_name) AS product_name,
    TRIM(p.brand) AS brand,
    p.unit_price,
    p.cost_price,
    p.profit_margin,
    p.profit_amount,
    CASE
        WHEN p.unit_price < 0.5 * ap.avg_unit_price THEN 'LOW'
        WHEN p.unit_price >= 0.5 * ap.avg_unit_price AND p.unit_price <= 1.5 * ap.avg_unit_price THEN 'MEDIUM'
        WHEN p.unit_price > 1.5 * ap.avg_unit_price THEN 'HIGH'
        ELSE 'UNKNOWN'
    END AS price_tier,  -- Dynamic price_tier based on how product price compares to overall average
    p.rating,
    CASE
        WHEN p.rating >= 4.5 THEN 'EXCELLENT'
        WHEN p.rating >= 4.0 THEN 'GOOD'
        WHEN p.rating >= 3.0 THEN 'AVERAGE'
        WHEN p.rating IS NOT NULL THEN 'POOR'
        ELSE 'NOT_RATED'
    END as rating_category, -- Bucketing rating into clear business-friendly categories
    p.review_count,
    CASE 
        WHEN p.review_count >= 500 THEN 5
        WHEN p.review_count >= 300 THEN 4
        WHEN p.review_count >= 100 THEN 3
        WHEN p.review_count >= 50 THEN 2
        ELSE 1
    END as popularity_score, -- Score based on review count for popularity ranking
    CASE
        WHEN p.profit_margin > 70 AND p.rating > 4.0 THEN 'HIGH_VALUE_HIGH_QUALITY'
        WHEN p.profit_margin > 50 THEN 'HIGH_VALUE'
        WHEN p.rating > 4.0 THEN 'HIGH_QUALITY'
        ELSE 'STANDARD'
    END AS competitive_advantage -- Flag products with both high margin and high rating as best-in-class
FROM silver.erp_product p
CROSS JOIN avg_price_cte ap
GO;

-- =============================================================================
-- Create Dimension: gold.dim_product_prices
-- =============================================================================
IF OBJECT_ID('gold.dim_product_prices', 'V') IS NOT NULL
    DROP VIEW gold.dim_product_prices;
GO

CREATE VIEW gold.dim_product_prices AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id, country_code, price_type) AS price_key,
    product_id,
    country_code,
    price_type,
    local_price,
    effective_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, country_code, price_type
            ORDER BY effective_date DESC
        ) AS rn
    FROM silver.erp_product_prices
) latest_price
WHERE rn = 1;
GO;

-- =============================================================================
-- Create Dimension: gold.dim_payment_channel
-- =============================================================================
IF OBJECT_ID('gold.dim_payment_channel', 'V') IS NOT NULL
    DROP VIEW gold.dim_payment_channel;
GO

CREATE VIEW gold.dim_payment_channel AS
SELECT
    ROW_NUMBER() OVER (ORDER BY payment_channel_id) AS payment_channel_key,
    payment_channel_id,
    channel_name,
    provider_name,
    country_code,
    is_banktransfer,
    is_card,
    is_wallet
FROM silver.erp_payment_channel;
GO;

-- =============================================================================
-- Create Dimension: gold.dim_shipment_company
-- =============================================================================
IF OBJECT_ID('gold.dim_shipment_company', 'V') IS NOT NULL
    DROP VIEW gold.dim_shipment_company;
GO

CREATE VIEW gold.dim_shipment_company AS
SELECT
    ROW_NUMBER() OVER (ORDER BY shipment_company_id) AS shipment_company_key,
    shipment_company_id,
    company_name,
    company_type,
    country_code,
    is_standard,
    is_express,
    is_registered,
    is_international
FROM silver.erp_shipment_company;
GO;

-- =============================================================================
/* Create Dimension: gold.dim_date
    Purpose: Creates a standard date dimension in the gold layer, generating a full range of dates from 2023-01-01 to 2024-12-31.
    Includes attributes for calendar analytics (year, quarter, month, week, weekday), readable names, and workday/weekend flags.
    Enables time-based reporting and joins across the warehouse.
*/
-- =============================================================================
IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO

CREATE VIEW gold.dim_date AS
SELECT
    CONVERT(INT, CONVERT(CHAR(8), DATEADD(DAY, n, '2023-01-01'), 112)) AS date_key, -- SARGable date key (yyyymmdd)
    DATEADD(DAY, n, '2023-01-01') AS date_value,
    DATEPART(YEAR, DATEADD(DAY, n, '2023-01-01')) AS calendar_year,
    DATEPART(QUARTER, DATEADD(DAY, n, '2023-01-01')) AS calendar_quarter,
    DATEPART(MONTH, DATEADD(DAY, n, '2023-01-01')) AS calendar_month,
    DATEPART(ISO_WEEK, DATEADD(DAY, n, '2023-01-01')) AS week_of_year,
    DATEPART(WEEKDAY, DATEADD(DAY, n, '2023-01-01')) AS day_of_week, -- SQL Server default: 1=Sunday, 7=Saturday
    DATEPART(DAY, DATEADD(DAY, n, '2023-01-01')) AS day_of_month,
    DATENAME(MONTH, DATEADD(DAY, n, '2023-01-01')) AS month_name,
    DATENAME(WEEKDAY, DATEADD(DAY, n, '2023-01-01')) AS day_name,
    CASE WHEN DATEPART(WEEKDAY, DATEADD(DAY, n, '2023-01-01')) IN (1,7) THEN 1 ELSE 0 END AS is_weekend, -- 1 if Saturday/Sunday, else 0
    CASE WHEN DATEPART(WEEKDAY, DATEADD(DAY, n, '2023-01-01')) IN (2,3,4,5,6) THEN 1 ELSE 0 END AS is_business_day  -- 1 if Monday-Friday, else 0
FROM (
    SELECT TOP (DATEDIFF(DAY, '2023-01-01', '2024-12-31') + 1)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM master..spt_values
) AS tally
GO;

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
ROW_NUMBER() OVER( ORDER BY o.order_id, od.product_id, od.quantity, od.unit_price, od.sales_amount) as sales_key, -- Surrogate key for each sales transaction row
c.customer_key,
p.product_key,
a.address_key,
o.order_id,
d.date_key,
o.order_datetime,
od.quantity,
od.unit_price,
od.discount_amount,
od.sales_amount,
(od.quantity * p.cost_price) AS cost_amount, -- Total cost for this line item (based on product cost)
(od.sales_amount - (od.quantity * cost_price)) AS profit_amount, -- Profit per row: revenue minus cost
CASE WHEN od.sales_amount > 0 THEN CAST(ROUND((od.sales_amount - (od.quantity * p.cost_price)) / od.sales_amount * 100,2) AS DECIMAL(10,2))
	ELSE NULL
END as profit_margin, -- Margin as a percentage (NULL if sales_amount is 0
o.order_status,
o.is_cancelled,
o.is_returned,
o.return_reason,
o.cancellation_reason
FROM
silver.erp_order_detail od
LEFT JOIN silver.erp_orders o on od.order_id = o.order_id
LEFT JOIN gold.dim_customers c on o.customer_id = c.customer_id
LEFT JOIN gold.dim_product p on od.product_id = p.product_id
LEFT JOIN gold.dim_address a on a.address_id = o.shipping_address_id
LEFT JOIN gold.dim_date d on  CONVERT(INT, CONVERT(CHAR(8), o.order_datetime, 112)) = d.date_key -- SARGable date_key mapping
WHERE od.quantity > 0
	AND od.unit_price >= 0
	AND od.sales_amount IS NOT NULL
GO;

-- =============================================================================
-- Create Fact Table: gold.fact_invoice
-- =============================================================================
IF OBJECT_ID('gold.fact_invoice', 'V') IS NOT NULL
    DROP VIEW gold.fact_invoice;
GO

CREATE VIEW gold.fact_invoice AS
SELECT
ROW_NUMBER() OVER (ORDER BY id.invoice_id, id.invoice_detail_id, id.product_id) AS invoice_key,     -- Surrogate key for each invoice line
id.invoice_id,
id.invoice_detail_id,
i.order_id,
c.customer_key,
p.product_key,
a.address_key,
d.date_key,
id.invoice_datetime,
id.quantity as quantity,
id.sales_amount as id_sales_amount,
id.tax_amount as id_tax_amount,
id.line_total,
id.tax_rate as id_tax_rate,
i.invoice_status as i_invoice_status
FROM
silver.erp_invoice_detail id
JOIN silver.erp_invoice i on id.invoice_id = i.invoice_id
LEFT JOIN silver.erp_orders o on i.order_id = o.order_id
LEFT JOIN gold.dim_product p on id.product_id = p.product_id
LEFT JOIN gold.dim_customers c on o.customer_id = c.customer_id
LEFT JOIN gold.dim_address a on o.shipping_address_id = a.address_id
LEFT JOIN gold.dim_date d on CONVERT(INT, CONVERT(CHAR(8), id.invoice_datetime, 112)) = d.date_key -- SARGable date_key mapping
GO;
-- =============================================================================
-- Create Fact Table: gold.fact_payment
-- =============================================================================
IF OBJECT_ID('gold.fact_payments', 'V') IS NOT NULL
    DROP VIEW gold.fact_payments;
GO

CREATE VIEW gold.fact_payments AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.payment_id) AS payment_key, -- Surrogate key for each payment transaction
    p.payment_id,
    p.order_id,
    c.customer_key,
    a.address_key,
    pc.payment_channel_key,
    d.date_key AS payment_date_key,
    p.payment_datetime,
    p.final_amount AS payment_amount,
    p.transaction_status,
    p.is_fraud,
    p.is_refunded,
    p.refund_status
FROM silver.erp_payment p
LEFT JOIN silver.erp_orders o ON p.order_id = o.order_id
LEFT JOIN gold.dim_customers c ON o.customer_id = c.customer_id
LEFT JOIN gold.dim_address a ON o.shipping_address_id = a.address_id
LEFT JOIN gold.dim_payment_channel pc ON p.payment_channel_id = pc.payment_channel_id
LEFT JOIN gold.dim_date d ON CONVERT(INT, CONVERT(CHAR(8), p.payment_datetime, 112)) = d.date_key -- SARGable date_key mapping
WHERE p.rule_violation IS NULL -- Only include valid (non-violating) payment records
GO;

-- =============================================================================
-- Create Fact Table: gold.fact_shipment
-- =============================================================================

IF OBJECT_ID('gold.fact_shipments', 'V') IS NOT NULL
    DROP VIEW gold.fact_shipments;
GO
CREATE VIEW gold.fact_shipments AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.shipment_id) AS shipment_key, -- Surrogate key for each shipment record
    s.shipment_id,
    s.order_id,
    c.customer_key,
    a.address_key,
    sc.shipment_company_key,
    d_ship.date_key AS shipment_date_key,
    d_del.date_key AS delivery_date_key,
    s.shipment_date,
    s.delivery_date,
    s.shipment_type,
    s.shipment_status,              
    s.delivery_status,              
    s.transit_days,
    s.is_delayed,
    s.is_failed,
    s.is_returned_delivery,
    s.is_lost
FROM silver.erp_shipment s
JOIN silver.erp_orders o ON s.order_id = o.order_id
JOIN gold.dim_customers c ON o.customer_id = c.customer_id
JOIN gold.dim_address a ON o.shipping_address_id = a.address_id
JOIN gold.dim_shipment_company sc ON s.shipment_company_id = sc.shipment_company_id
-- Map both shipment and delivery dates to their date keys for timeline analytics
JOIN gold.dim_date d_ship ON CONVERT(INT, CONVERT(CHAR(8), s.shipment_date, 112)) = d_ship.date_key
JOIN gold.dim_date d_del ON s.delivery_date IS NOT NULL -- Only valid shipment/delivery status combos included
                                    AND CONVERT(INT, CONVERT(CHAR(8), s.delivery_date, 112)) = d_del.date_key
WHERE s.is_valid_status_combo =1
GO;
