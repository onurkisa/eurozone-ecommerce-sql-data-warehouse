/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.crm_customer', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customer;
GO

CREATE TABLE bronze.crm_customer (
    customer_id INT,
    username VARCHAR(50),
    email VARCHAR(75),
    first_name NVARCHAR(25),
    last_name NVARCHAR(50),
    gender VARCHAR(10),
    birth_date DATE,
    phone_number VARCHAR(25),
    registration_date DATE,
    registration_datetime DATETIME2,
    is_loyalty_member VARCHAR(5),
    is_fraud_suspected VARCHAR(5),
    customer_segment VARCHAR(10)
);
GO

IF OBJECT_ID ('bronze.crm_customer_addresses', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customer_addresses;
GO

CREATE TABLE bronze.crm_customer_addresses (
    address_id INT,
    customer_id INT,
    country VARCHAR(15),
    country_code VARCHAR(5),
    province NVARCHAR(50),
    province_code VARCHAR(15),
    city VARCHAR(30),
    district NVARCHAR(50),
    postal_code VARCHAR(10),
    full_address NVARCHAR(100)
);
GO


IF OBJECT_ID ('bronze.erp_payment_channel', 'U') IS NOT NULL
    DROP TABLE bronze.erp_payment_channel;
GO

CREATE TABLE bronze.erp_payment_channel (
    payment_channel_id VARCHAR(15),
    channel_name VARCHAR(20),
    provider_name VARCHAR(20),
    country_code VARCHAR(6),
    payment_type VARCHAR(15)
);
GO

IF OBJECT_ID ('bronze.erp_shipment_company', 'U') IS NOT NULL
    DROP TABLE bronze.erp_shipment_company;
GO

CREATE TABLE bronze.erp_shipment_company (
    shipment_company_id INT,
    company_name VARCHAR(30),
    country_code VARCHAR(5),
    operating_countries VARCHAR(75),
    delivery_types VARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_product', 'U') IS NOT NULL
    DROP TABLE bronze.erp_product;
GO

CREATE TABLE bronze.erp_product (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(25),
    sub_category VARCHAR(25),
    brand VARCHAR(25),
    unit_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    rating DECIMAL(2,1),
    review_count INT
);
GO

IF OBJECT_ID ('bronze.erp_product_prices', 'U') IS NOT NULL
    DROP TABLE bronze.erp_product_prices;
GO

CREATE TABLE bronze.erp_product_prices (
    product_id INT,
    country_code VARCHAR(5),
    local_price DECIMAL(10,2),
    price_type VARCHAR(20),
    effective_date DATE
);
GO


IF OBJECT_ID ('bronze.erp_orders', 'U') IS NOT NULL
    DROP TABLE bronze.erp_orders;
GO

CREATE TABLE bronze.erp_orders (
    order_id VARCHAR(50),
    customer_id INT,
    order_date DATE,
    order_time TIME,
    shipping_address_id INT,
    shipment_company_id INT,
    total_price DECIMAL(10,2),
    order_status VARCHAR(15),
    is_cancelled VARCHAR(5),
    is_returned VARCHAR(5),
    return_reason VARCHAR(25),
    country_code VARCHAR(5)
);
GO

IF OBJECT_ID ('bronze.erp_order_detail', 'U') IS NOT NULL
    DROP TABLE bronze.erp_order_detail;
GO

CREATE TABLE bronze.erp_order_detail (
    order_detail_id VARCHAR(50),
    order_id VARCHAR(50),
    product_id INT,
    quantity TINYINT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    sales_amount DECIMAL (10,2),
    order_datetime DATETIME2
);
GO

IF OBJECT_ID ('bronze.erp_invoice', 'U') IS NOT NULL
    DROP TABLE bronze.erp_invoice;
GO

CREATE TABLE bronze.erp_invoice (
    invoice_id VARCHAR(15),
    order_id VARCHAR(50),
    unit_price DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    invoice_status VARCHAR(10),
    invoice_datetime DATETIME2
);
GO

IF OBJECT_ID ('bronze.erp_invoice_detail', 'U') IS NOT NULL
    DROP TABLE bronze.erp_invoice_detail;
GO

CREATE TABLE bronze.erp_invoice_detail (
    invoice_detail_id VARCHAR(20),
    invoice_id VARCHAR(15),
    product_id INT,
    quantity TINYINT,
    sales_amount DECIMAL (10,2),
    tax_amount DECIMAL(10,2),
    invoice_datetime DATETIME2
);
GO

IF OBJECT_ID ('bronze.erp_payment', 'U') IS NOT NULL
    DROP TABLE bronze.erp_payment;
GO

CREATE TABLE bronze.erp_payment (
    payment_id VARCHAR(15),
    order_id VARCHAR(50),
    payment_channel_id VARCHAR(15),
    payment_datetime DATETIME2,
    final_amount DECIMAL(10,2),
    transaction_status VARCHAR(15),
    is_fraud VARCHAR(5),
    refund_status VARCHAR(15)
);
GO

IF OBJECT_ID ('bronze.erp_shipment', 'U') IS NOT NULL
    DROP TABLE bronze.erp_shipment;
GO

CREATE TABLE bronze.erp_shipment (
    shipment_id VARCHAR(15),
    order_id VARCHAR(50),
    shipment_company_id INT,
    shipping_address_id INT,
    shipment_date DATE,
    delivery_date DATE,
    shipment_type VARCHAR(10),
    shipment_status VARCHAR(15),
    delivery_status VARCHAR(15)
);
GO
