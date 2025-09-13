/*
===============================================================================
DDL Script: Create Gold Aggregation Views
===============================================================================
Script Purpose:
    Creates the core aggregation views for the Gold layer without materialization.
    Aggregations:
      - Sales by Day x Product x Geography
      - Sales by Day x Customer Segment x Geography
      - Payments by Day x Channel x Geography
      - Shipments by Day x Company x Geography
      - Invoice Tax Totals by Day x Geography
Usage:
    Run this script after base Gold dimension/fact views are available.
===============================================================================
*/

-- =============================================================================
-- Aggregation: gold.aggr_sales_daily_product
-- Purpose: Daily product performance with geography (country/province)
-- =============================================================================
IF OBJECT_ID('gold.aggr_sales_daily_product', 'V') IS NOT NULL
    DROP VIEW gold.aggr_sales_daily_product;
GO

CREATE VIEW gold.aggr_sales_daily_product AS
SELECT
  f.date_key,
  f.product_key,
  p.brand,
  p.price_tier,
  a.country_code,
  a.country,
  a.province_code,
  a.province,
  SUM(f.quantity) AS total_qty,
  SUM(f.sales_amount) AS gross_revenue,
  SUM(f.discount_amount) AS total_discount,
  SUM(f.cost_amount) AS total_cost,
  SUM(f.profit_amount) AS gross_profit,
  SUM(CASE WHEN f.is_cancelled = 1 OR f.is_returned = 1 THEN 0 ELSE f.sales_amount END) AS net_revenue,
  SUM(CASE WHEN f.is_cancelled = 1 OR f.is_returned = 1 THEN 0 ELSE f.profit_amount END) AS net_profit
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_key = p.product_key
JOIN gold.dim_address a ON f.address_key = a.address_key
GROUP BY
  f.date_key,
  f.product_key,
  p.brand,
  p.price_tier,
  a.country_code,
  a.country,
  a.province_code,
  a.province;
GO

-- =============================================================================
-- Aggregation: gold.aggr_sales_daily_segment
-- Purpose: Daily sales by customer segment/loyalty with geography
-- =============================================================================
IF OBJECT_ID('gold.aggr_sales_daily_segment', 'V') IS NOT NULL
    DROP VIEW gold.aggr_sales_daily_segment;
GO

CREATE VIEW gold.aggr_sales_daily_segment AS
SELECT
  f.date_key,
  c.customer_segment,
  c.is_loyalty_member,
  a.country_code,
  a.country,
  a.province_code,
  a.province,
  SUM(f.quantity) AS total_qty,
  SUM(f.sales_amount) AS gross_revenue,
  SUM(f.profit_amount) AS gross_profit,
  SUM(CASE WHEN f.is_cancelled = 1 OR f.is_returned = 1 THEN 0 ELSE f.sales_amount END) AS net_revenue,
  SUM(CASE WHEN f.is_cancelled = 1 OR f.is_returned = 1 THEN 0 ELSE f.profit_amount END) AS net_profit
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
JOIN gold.dim_address  a ON f.address_key   = a.address_key
GROUP BY
  f.date_key,
  c.customer_segment,
  c.is_loyalty_member,
  a.country_code,
  a.country,
  a.province_code,
  a.province;
GO

-- =============================================================================
-- Aggregation: gold.aggr_payments_daily_channel
-- Purpose: Daily payments by channel/provider with geography
-- =============================================================================
IF OBJECT_ID('gold.aggr_payments_daily_channel', 'V') IS NOT NULL
    DROP VIEW gold.aggr_payments_daily_channel;
GO

CREATE VIEW gold.aggr_payments_daily_channel AS
SELECT
  p.payment_date_key,
  pc.payment_channel_key,
  pc.channel_name,
  pc.provider_name,
  a.country_code,
  a.country,
  a.province_code,
  a.province,
  COUNT_BIG(*) AS payment_count,
  SUM(p.payment_amount) AS total_amount,
  SUM(CASE WHEN p.transaction_status = 'SUCCESS' THEN p.payment_amount ELSE 0 END) AS success_amount,
  SUM(CASE WHEN p.transaction_status = 'FAILED'  THEN p.payment_amount ELSE 0 END) AS failed_amount,
  SUM(CASE WHEN p.is_refunded = 1               THEN p.payment_amount ELSE 0 END) AS refunded_amount
FROM gold.fact_payments p
JOIN gold.dim_payment_channel pc ON p.payment_channel_key = pc.payment_channel_key
JOIN gold.dim_address a ON p.address_key = a.address_key
GROUP BY
  p.payment_date_key,
  pc.payment_channel_key,
  pc.channel_name,
  pc.provider_name,
  a.country_code,
  a.country,
  a.province_code,
  a.province;
GO

-- =============================================================================
-- Aggregation: gold.aggr_shipments_daily_company
-- Purpose: Daily shipment SLA metrics by carrier with geography
-- =============================================================================
IF OBJECT_ID('gold.aggr_shipments_daily_company', 'V') IS NOT NULL
    DROP VIEW gold.aggr_shipments_daily_company;
GO

CREATE VIEW gold.aggr_shipments_daily_company AS
SELECT
  s.shipment_date_key,
  sc.shipment_company_key,
  sc.company_name,
  a.country_code,
  a.country,
  a.province_code,
  a.province,
  COUNT_BIG(*) AS shipments,
  SUM(CASE WHEN s.is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_shipments,
  SUM(CASE WHEN s.is_failed  = 1 THEN 1 ELSE 0 END) AS failed_shipments,
  AVG(CAST(s.transit_days AS float)) AS avg_transit_days,
  CAST(100.0 * SUM(CASE WHEN s.is_delayed = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT_BIG(*), 0) AS decimal(5,2)) AS delayed_pct
FROM gold.fact_shipments s
JOIN gold.dim_shipment_company sc ON s.shipment_company_key = sc.shipment_company_key
JOIN gold.dim_address a ON s.address_key = a.address_key
GROUP BY
  s.shipment_date_key,
  sc.shipment_company_key,
  sc.company_name,
  a.country_code,
  a.country,
  a.province_code,
  a.province;
GO

-- =============================================================================
-- Aggregation: gold.aggr_invoice_tax_daily
-- Purpose: Daily invoice tax summary with geography
-- =============================================================================
IF OBJECT_ID('gold.aggr_invoice_tax_daily', 'V') IS NOT NULL
    DROP VIEW gold.aggr_invoice_tax_daily;
GO

CREATE VIEW gold.aggr_invoice_tax_daily AS
SELECT
  i.date_key,
  a.country_code,
  a.country,
  a.province_code,
  a.province,
  COUNT_BIG(*) AS line_count,
  SUM(i.id_sales_amount) AS subtotal_amount,
  SUM(i.id_tax_amount) AS tax_amount,
  SUM(i.line_total) AS total_amount
FROM gold.fact_invoice i
JOIN gold.dim_address a ON i.address_key = a.address_key
GROUP BY
  i.date_key,
  a.country_code,
  a.country,
  a.province_code,
  a.province;
GO
