# Data Catalog for Gold Layer 

## Overview
The Gold layer provides business-ready, analytics-optimized data modeled as a star schema on SQL Server. It exposes dimensional (conformed) views, fact views, and aggregate views built on the cleansed Silver layer. These views are intended for BI, reporting, and ad‑hoc analytics and encapsulate business logic, standard derivations, and SARGable date keys.

---

## Dimensions

### 1. gold.dim_customers
- Purpose: Customer dimension enriched with demographic attributes and unified full name for user-friendly display and filtering.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| customer_key | BIGINT | Surrogate key uniquely identifying each customer record in the dimension table.  |
| customer_id | INT | Unique numerical identifier assigned to each customer.    |
| username | VARCHAR(50) | Customer’s account username. |
| email | VARCHAR(75) | Primary email address. |
| first_name | NVARCHAR(25) | Customer first name. |
| last_name | NVARCHAR(50) | Customer last name. |
| full_name | NVARCHAR(76) | Concatenated full name: `first_name + ' ' + last_name`. |
| gender | VARCHAR(1) | Gender code (e.g., M/F). |
| age | TINYINT | Customer age. |
| age_group | VARCHAR(10) | Derived age bucket label. |
| birth_date | DATE | Date of birth. |
| phone_number | VARCHAR(25) | Primary phone number. |
| registration_datetime | DATETIME2(3) | Account registration timestamp. |
| is_loyalty_member | BIT | 1 if enrolled in loyalty program; else 0. |
| is_fraud_suspected | BIT | 1 if flagged for potential fraud; else 0. |
| customer_segment | VARCHAR(10) | Derived segment label (e.g., VIP/Regular). |

---

### 2. gold.dim_address
- Purpose: Customer address dimension for shipping and geography-based analytics and aggregation.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| address_key | BIGINT | Surrogate key uniquely identifying each address. |
| address_id | INT | Unique numerical identifier assigned to each address record. |
| customer_id | INT | Unique numerical identifier assigned to each customer.|
| country | VARCHAR(15) | Country name. |
| country_code | VARCHAR(5) | ISO-like country code. |
| province | NVARCHAR(50) | Province/state name. |
| province_code | VARCHAR(15) | Province/state code. |
| district | NVARCHAR(50) | District/county. |
| postal_code | VARCHAR(10) | Postal/ZIP code. |
| full_address | NVARCHAR(100) | Full address string for display. |

---

### 3. gold.dim_product
- Purpose: Product dimension with price, margin, rating, popularity, and business-friendly price and quality tiers.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| product_key | BIGINT | Surrogate key uniquely identifying each product record in the product dimension table.  |
| product_id | INT | Natural product identifier from ERP. |
| product_name | VARCHAR(100) | Cleaned product name. |
| brand | VARCHAR(25) | Brand name. |
| unit_price | DECIMAL(10,2) | Standard unit sales price. |
| cost_price | DECIMAL(10,2) | Unit cost. |
| profit_margin | DECIMAL(10,2) | Margin percentage at unit level. |
| profit_amount | DECIMAL(10,2) | Profit amount at unit level. |
| price_tier | VARCHAR(7) | Derived tier vs. average price: LOW/MEDIUM/HIGH/UNKNOWN. |
| rating | DECIMAL(2,1) | Average star rating. |
| rating_category | VARCHAR(10) | Bucketed rating: EXCELLENT/GOOD/AVERAGE/POOR/NOT_RATED. |
| review_count | INT | Count of product reviews. |
| popularity_score | INT | 1–5 score derived from review_count. |
| competitive_advantage | VARCHAR(22) | Composite label of margin/quality leadership. |

---

### 4. gold.dim_product_prices
- Purpose: Latest per-product, per-country price entries by price_type for localized pricing.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| price_key | BIGINT | Surrogate key for each price record. |
| product_id | INT | Product natural key. |
| country_code | VARCHAR(5) | Price’s country code. |
| price_type | VARCHAR(20) | Price type (e.g., LIST, PROMO). |
| local_price | DECIMAL(10,2) | Latest local currency price. |
| effective_date | DATE | Effective date of the price. |

---

### 5. gold.dim_payment_channel
- Purpose: Payment channel dimension for categorizing and analyzing payment behaviors.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| payment_channel_key | BIGINT | Surrogate key for payment channel. |
| payment_channel_id | VARCHAR(15) | Natural key from ERP. |
| channel_name | VARCHAR(20) | Payment channel (e.g., CARD, WALLET). |
| provider_name | VARCHAR(20) | Provider name. |
| country_code | VARCHAR(6) | Provider country code. |
| is_banktransfer | BIT | 1 if bank transfer; else 0. |
| is_card | BIT | 1 if card; else 0. |
| is_wallet | BIT | 1 if wallet; else 0. |

---

### 6. gold.dim_shipment_company
- Purpose: Shipment company/carrier dimension supporting delivery and SLA analytics.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| shipment_company_key | BIGINT | Surrogate key for the shipment company. |
| shipment_company_id | INT | Natural carrier ID from ERP. |
| company_name | VARCHAR(30) | Carrier name. |
| company_type | VARCHAR(20) | Carrier type (e.g., EXPRESS). |
| country_code | VARCHAR(5) | Carrier country code. |
| is_standard | BIT | 1 if standard shipping; else 0. |
| is_express | BIT | 1 if express shipping; else 0. |
| is_registered | BIT | 1 if registered shipping; else 0. |
| is_international | BIT | 1 if international shipping; else 0. |

---

### 7. gold.dim_date
- Purpose: Calendar date dimension with standard attributes for time-series analytics. Covers 2023-01-01 to 2024-12-31.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| date_key | INT | SARGable date key in `YYYYMMDD` format. |
| date_value | DATE | Actual calendar date. |
| calendar_year | INT | Calendar year (e.g., 2023). |
| calendar_quarter | INT | Quarter number (1–4). |
| calendar_month | INT | Month number (1–12). |
| week_of_year | INT | ISO week number. |
| day_of_week | INT | Day of week (1=Sunday … 7=Saturday). |
| day_of_month | INT | Day of month (1–31). |
| month_name | NVARCHAR(30) | Month name (e.g., January). |
| day_name | NVARCHAR(30) | Weekday name (e.g., Monday). |
| is_weekend | INT | 1 if Saturday/Sunday; else 0. |
| is_business_day | INT | 1 if Monday–Friday; else 0. |

---

## Facts

### 8. gold.fact_sales
- Purpose: Line-level sales fact derived from orders and order details with costs and profitability.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| sales_key | BIGINT | Surrogate row key for each sales line. |
| customer_key | BIGINT | FK to `gold.dim_customers`. |
| product_key | BIGINT | FK to `gold.dim_product`. |
| address_key | BIGINT | FK to `gold.dim_address` (shipping). |
| order_id | VARCHAR(50) | Order surrogate/natural key from ERP. |
| date_key | INT | FK to `gold.dim_date` for order date. |
| order_datetime | DATETIME2(3) | Order timestamp. |
| quantity | TINYINT | Quantity in the line. |
| unit_price | DECIMAL(10,2) | Unit sales price. |
| discount_amount | DECIMAL(10,2) | Discount applied to the line. |
| sales_amount | DECIMAL(10,2) | Extended line amount after discount. |
| cost_amount | DECIMAL(13,2) | Quantity × product cost (estimated). |
| profit_amount | DECIMAL(13,2) | Sales amount − cost amount. |
| profit_margin | DECIMAL(10,2) | Profit margin percentage for the line. |
| order_status | VARCHAR(15) | Current order status. |
| is_cancelled | BIT | 1 if cancelled; else 0. |
| is_returned | BIT | 1 if returned; else 0. |
| return_reason | VARCHAR(25) | Reason for return, if any. |
| cancellation_reason | VARCHAR(30) | Reason for cancellation, if any. |

---

### 9. gold.fact_invoice
- Purpose: Invoice line fact with sales, tax, and totals aligned to orders and products for financial reconciliation.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| invoice_key | BIGINT | Surrogate row key for each invoice line. |
| invoice_id | VARCHAR(15) | Invoice identifier. |
| invoice_detail_id | VARCHAR(20) | Invoice line identifier. |
| order_id | VARCHAR(50) | Related order key. |
| customer_key | BIGINT | FK to `gold.dim_customers`. |
| product_key | BIGINT | FK to `gold.dim_product`. |
| address_key | BIGINT | FK to `gold.dim_address`. |
| date_key | INT | FK to `gold.dim_date` for invoice date. |
| invoice_datetime | DATETIME2(3) | Invoice timestamp. |
| quantity | TINYINT | Line quantity. |
| id_sales_amount | DECIMAL(10,2) | Line subtotal (before tax). |
| id_tax_amount | DECIMAL(10,2) | Tax amount for the line. |
| line_total | DECIMAL(10,2) | Line total (subtotal + tax). |
| id_tax_rate | DECIMAL(9,4) | Applied tax rate. |
| i_invoice_status | VARCHAR(10) | Invoice status at header level. |

---

### 10. gold.fact_payments
- Purpose: Payment transactions aligned to orders, channels, and geography with fraud/refund indicators.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| payment_key | BIGINT | Surrogate key for the payment record. |
| payment_id | VARCHAR(15) | Payment identifier. |
| order_id | VARCHAR(50) | Related order key. |
| customer_key | BIGINT | FK to `gold.dim_customers`. |
| address_key | BIGINT | FK to `gold.dim_address`. |
| payment_channel_key | BIGINT | FK to `gold.dim_payment_channel`. |
| payment_date_key | INT | FK to `gold.dim_date` for payment date. |
| payment_datetime | DATETIME2(3) | Payment timestamp. |
| payment_amount | DECIMAL(10,2) | Final amount of the payment. |
| transaction_status | VARCHAR(15) | Payment transaction status. |
| is_fraud | BIT | 1 if suspected/confirmed fraud; else 0. |
| is_refunded | BIT | 1 if refunded; else 0. |
| refund_status | VARCHAR(15) | Refund status text. |

---

### 11. gold.fact_shipments
- Purpose: Shipment events with shipment/delivery dates, SLA indicators, and carrier linkage.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| shipment_key | BIGINT | Surrogate key for the shipment record. |
| shipment_id | VARCHAR(15) | Shipment identifier. |
| order_id | VARCHAR(50) | Related order key. |
| customer_key | BIGINT | FK to `gold.dim_customers`. |
| address_key | BIGINT | FK to `gold.dim_address`. |
| shipment_company_key | BIGINT | FK to `gold.dim_shipment_company`. |
| shipment_date_key | INT | FK to `gold.dim_date` for shipment date. |
| delivery_date_key | INT | FK to `gold.dim_date` for delivery date. |
| shipment_date | DATE | Shipment date. |
| delivery_date | DATE | Delivery date (if delivered). |
| shipment_type | VARCHAR(10) | Shipment type (e.g., STD/EXP). |
| shipment_status | VARCHAR(15) | Shipment status flag. |
| delivery_status | VARCHAR(15) | Delivery status flag. |
| transit_days | SMALLINT | Number of days in transit. |
| is_delayed | BIT | 1 if delayed; else 0. |
| is_failed | BIT | 1 if failed; else 0. |
| is_returned_delivery | BIT | 1 if returned-to-sender; else 0. |
| is_lost | BIT | 1 if shipment lost; else 0. |

---

## Aggregations

### 12. gold.aggr_sales_daily_product
- Purpose: Daily product performance with geography; additive measures for revenue and profit.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| date_key | INT | FK to `gold.dim_date`. |
| product_key | BIGINT | FK to `gold.dim_product`. |
| brand | VARCHAR(25) | Product brand. |
| price_tier | VARCHAR(7) | Product price tier (LOW/MEDIUM/HIGH/UNKNOWN). |
| country_code | VARCHAR(5) | Country code (from shipping address). |
| country | VARCHAR(15) | Country name. |
| province_code | VARCHAR(15) | Province/state code. |
| province | NVARCHAR(50) | Province/state name. |
| total_qty | INT | Sum of quantities. |
| gross_revenue | DECIMAL(38,2) | Sum of sales_amount. |
| total_discount | DECIMAL(38,2) | Sum of discount_amount. |
| total_cost | DECIMAL(38,2) | Sum of cost_amount. |
| gross_profit | DECIMAL(38,2) | Sum of profit_amount. |
| net_revenue | DECIMAL(38,2) | Revenue excluding cancelled/returned lines. |
| net_profit | DECIMAL(38,2) | Profit excluding cancelled/returned lines. |

---

### 13. gold.aggr_sales_daily_segment
- Purpose: Daily revenue/profit by customer segment and loyalty with geography.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| date_key | INT | FK to `gold.dim_date`. |
| customer_segment | VARCHAR(10) | Segment label. |
| is_loyalty_member | BIT | Segment split by loyalty membership. |
| country_code | VARCHAR(5) | Country code (from shipping address). |
| country | VARCHAR(15) | Country name. |
| province_code | VARCHAR(15) | Province/state code. |
| province | NVARCHAR(50) | Province/state name. |
| total_qty | INT | Sum of quantities. |
| gross_revenue | DECIMAL(38,2) | Sum of sales_amount. |
| gross_profit | DECIMAL(38,2) | Sum of profit_amount. |
| net_revenue | DECIMAL(38,2) | Revenue excluding cancelled/returned lines. |
| net_profit | DECIMAL(38,2) | Profit excluding cancelled/returned lines. |

---

### 14. gold.aggr_payments_daily_channel
- Purpose: Daily payments by channel/provider and geography; counts and funnel amounts.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| payment_date_key | INT | FK to `gold.dim_date`. |
| payment_channel_key | BIGINT | FK to `gold.dim_payment_channel`. |
| channel_name | VARCHAR(20) | Channel name. |
| provider_name | VARCHAR(20) | Provider name. |
| country_code | VARCHAR(5) | Country code (from shipping address). |
| country | VARCHAR(15) | Country name. |
| province_code | VARCHAR(15) | Province/state code. |
| province | NVARCHAR(50) | Province/state name. |
| payment_count | BIGINT | Number of payment rows. |
| total_amount | DECIMAL(38,2) | Sum of payment_amount. |
| success_amount | DECIMAL(38,2) | Sum of amounts with status SUCCESS. |
| failed_amount | DECIMAL(38,2) | Sum of amounts with status FAILED. |
| refunded_amount | DECIMAL(38,2) | Sum of amounts for refunded payments. |

---

### 15. gold.aggr_shipments_daily_company
- Purpose: Daily shipment SLA metrics by carrier with geography.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| shipment_date_key | INT | FK to `gold.dim_date` for shipment date. |
| shipment_company_key | BIGINT | FK to `gold.dim_shipment_company`. |
| company_name | VARCHAR(30) | Carrier name. |
| country_code | VARCHAR(5) | Country code (from shipping address). |
| country | VARCHAR(15) | Country name. |
| province_code | VARCHAR(15) | Province/state code. |
| province | NVARCHAR(50) | Province/state name. |
| shipments | BIGINT | Number of shipments. |
| delayed_shipments | INT | Count of delayed shipments. |
| failed_shipments | INT | Count of failed shipments. |
| avg_transit_days | FLOAT | Average days in transit. |
| delayed_pct | DECIMAL(5,2) | Percentage of delayed shipments. |

---

### 16. gold.aggr_invoice_tax_daily
- Purpose: Daily invoice tax totals by geography for finance and statutory reporting.
- Columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| date_key | INT | FK to `gold.dim_date`. |
| country_code | VARCHAR(5) | Country code (from shipping address). |
| country | VARCHAR(15) | Country name. |
| province_code | VARCHAR(15) | Province/state code. |
| province | NVARCHAR(50) | Province/state name. |
| line_count | BIGINT | Number of invoice lines. |
| subtotal_amount | DECIMAL(38,2) | Sum of line subtotals. |
| tax_amount | DECIMAL(38,2) | Sum of tax amounts. |
| total_amount | DECIMAL(38,2) | Sum of line totals. |
