/*
===============================================================================
DDL Script: Create silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

IF OBJECT_ID('silver.crm_customer', 'U') IS NOT NULL
    DROP TABLE silver.crm_customer;
GO

CREATE TABLE silver.crm_customer (
    customer_id INT,
    username VARCHAR(50),
    email VARCHAR(75),
    first_name NVARCHAR(25),
    last_name NVARCHAR(50),
    gender VARCHAR(1),
    age TINYINT,
    age_group VARCHAR(10),
    birth_date DATE,
    phone_number VARCHAR(25),
    registration_datetime DATETIME2(3),
    is_loyalty_member BIT,
    is_fraud_suspected BIT,
    customer_segment VARCHAR(10),
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID ('silver.crm_customer_addresses', 'U') IS NOT NULL
    DROP TABLE silver.crm_customer_addresses;
GO

CREATE TABLE silver.crm_customer_addresses (
    address_id INT,
    customer_id INT,
    country VARCHAR(15),
    country_code VARCHAR(5),
    province NVARCHAR(50),
    province_code VARCHAR(15),
    city VARCHAR(30),
    district NVARCHAR(50),
    postal_code VARCHAR(10),
    full_address NVARCHAR(100),
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID ('silver.erp_payment_channel', 'U') IS NOT NULL
    DROP TABLE silver.erp_payment_channel;
GO

CREATE TABLE silver.erp_payment_channel (
    payment_channel_id VARCHAR(15),
    channel_name VARCHAR(20),
    provider_name VARCHAR(20),
    country_code VARCHAR(6),
    is_banktransfer BIT,
    is_card BIT,
    is_wallet BIT,
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID ('silver.erp_shipment_company', 'U') IS NOT NULL
    DROP TABLE silver.erp_shipment_company;
GO

CREATE TABLE silver.erp_shipment_company (
    shipment_company_id INT,
    company_name VARCHAR(30),
    company_type VARCHAR(20),
    country_code VARCHAR(5),
    is_standard BIT,
    is_express BIT,
    is_registered BIT,
    is_international BIT,
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID ('silver.erp_product', 'U') IS NOT NULL
    DROP TABLE silver.erp_product;
GO

CREATE TABLE silver.erp_product (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(25),
    sub_category VARCHAR(25),
    brand VARCHAR(25),
    unit_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    profit_margin DECIMAL(10,2),
    profit_amount DECIMAL(10,2),
    rating DECIMAL(2,1),
    review_count INT,
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID ('silver.erp_product_prices', 'U') IS NOT NULL
    DROP TABLE silver.erp_product_prices;
GO

CREATE TABLE silver.erp_product_prices (
    product_id INT,
    country_code VARCHAR(5),
    local_price DECIMAL(10,2),
    price_type VARCHAR(20),
    effective_date DATE,
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID ('silver.erp_orders', 'U') IS NOT NULL
    DROP TABLE silver.erp_orders;
GO

CREATE TABLE silver.erp_orders (
    order_id VARCHAR(50),
    customer_id INT,
    shipping_address_id INT,
    shipment_company_id INT,
    order_datetime DATETIME2(3),
    total_price DECIMAL(10,2),
    order_status VARCHAR(15),
    is_cancelled BIT,
    is_returned BIT,
    return_reason VARCHAR(25),
    cancellation_reason VARCHAR(30),
    country_code VARCHAR(5),
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO
---------
IF OBJECT_ID ('silver.erp_order_detail', 'U') IS NOT NULL
    DROP TABLE silver.erp_order_detail;
GO

CREATE TABLE silver.erp_order_detail (
    order_detail_id VARCHAR(50),
    order_id VARCHAR(50),
    product_id INT,
    quantity TINYINT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    sales_amount DECIMAL (10,2),
    recalc_sales_amount DECIMAL(10,2),
    sales_match_flag BIT,
    order_datetime DATETIME2(3),
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID ('silver.erp_invoice', 'U') IS NOT NULL
    DROP TABLE silver.erp_invoice;
GO

CREATE TABLE silver.erp_invoice (
    invoice_id VARCHAR(15),
    order_id VARCHAR(50),
    unit_price DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    invoice_status VARCHAR(10),
    invoice_datetime DATETIME2(3),
    is_amount_correct BIT,
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO


IF OBJECT_ID ('silver.erp_invoice_detail', 'U') IS NOT NULL
    DROP TABLE silver.erp_invoice_detail;
GO

CREATE TABLE silver.erp_invoice_detail (
    invoice_detail_id VARCHAR(20),
    invoice_id VARCHAR(15),
    product_id INT,
    quantity TINYINT,
    sales_amount DECIMAL (10,2),
    tax_amount DECIMAL(10,2),
    line_total DECIMAL(10,2),
    tax_rate DECIMAL(9, 4),
    invoice_datetime DATETIME2(3),
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO 

IF OBJECT_ID ('silver.erp_payment', 'U') IS NOT NULL
    DROP TABLE silver.erp_payment;
GO

CREATE TABLE silver.erp_payment (
    payment_id VARCHAR(15),
    order_id VARCHAR(50),
    payment_channel_id VARCHAR(15),
    payment_datetime DATETIME2(3),
    final_amount DECIMAL(10,2),
    transaction_status VARCHAR(15),
    is_fraud BIT,
    refund_status VARCHAR(15),
    is_refunded BIT,
    rule_violation VARCHAR(100), 
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID ('silver.erp_shipment', 'U') IS NOT NULL
    DROP TABLE silver.erp_shipment;
GO

CREATE TABLE silver.erp_shipment (
    shipment_id VARCHAR(15),
    order_id VARCHAR(50),
    shipment_company_id INT,
    shipping_address_id INT,
    shipment_date DATE,
    delivery_date DATE,
    shipment_type VARCHAR(10),
    shipment_status VARCHAR(15),
    delivery_status VARCHAR(15),
    transit_days SMALLINT,
    is_delayed BIT,
    is_failed BIT,
    is_returned_delivery BIT,
    is_lost BIT,
    is_valid_status_combo BIT,
    dwh_load_date DATETIME2(3) DEFAULT SYSDATETIME()
);
GO
