# **EUETRADE Data Warehouse - Naming Conventions**

This document defines the official naming conventions for the EUETRADE data  warehouse architecture. 
These standards are implemented throughout all  database objects including schemas, tables, views, columns, and stored  procedures.
These conventions ensure data consistency, improve code  maintainability across all data warehouse layers.

## **Table of Contents**

1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   - [Bronze Rules](#bronze-rules)
   - [Silver Rules](#silver-rules)
   - [Gold Rules](#gold-rules)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
   - [Booleans & Flags](#booleans--flags)
   - [Dates & Times](#dates--times)
   - [Amounts, Rates, Percentages](#amounts-rates-percentages)
4. [Stored Procedure Naming Conventions](#stored-procedure-naming-conventions)

---

## **General Principles**

- **Case Convention**: Use `snake_case` with lowercase letters and underscores (`_`) to separate words
- **Language**: Use English for all object names
- **Reserved Words**: Avoid SQL reserved words as object names
  
---

## **Table Naming Conventions**

### **Bronze Rules**

- All names must start with the source system name, and table names must match their original names without renaming.

**Pattern**: `<sourcesystem>_<entity>`

- **`<sourcesystem>`**: Name of the source system (e.g., `crm`, `erp`)
- **`<entity>`**: Exact table name from the source system without modification

**Examples**:
- `crm_customer` → Customer information from the CRM system
- `erp_orders` → Order information from the ERP system

### **Silver Rules**

- All names must start with the source system name, and table names must match their original names without renaming.

**Pattern**: `<sourcesystem>_<entity>`

- **`<sourcesystem>`**: Name of the source system (e.g., `crm`, `erp`)
- **`<entity>`**: Exact table name from the source system without modification

**Examples**:
- `crm_customer` → Customer information from the CRM system.
- `erp_orders` → Order information from the ERP system.

### **Gold Rules**

- All names must use meaningful, business-aligned names for tables, starting with the category prefix.

**Pattern**: `<category>_<entity>`

- **`<category>`**: Describes the table role (see category patterns below)
- **`<entity>`**: Business-meaningful name aligned with domain terminology

**Examples**:
- `dim_customers` → Dimension table for customer data
- `fact_sales` → Fact table containing sales transactions
- `aggr_sales_daily_segment` → Daily sales aggregation by customer segment

#### **Glossary of Category Patterns**

| Pattern   | Purpose                    | Examples                                           |
|-----------|----------------------------|---------------------------------------------------|
| `dim_`    | Dimension table           | `dim_customer`, `dim_product`                     |
| `fact_`   | Fact table               | `fact_sales`                                      |
| `aggr_`   | Aggregated table/view     | `aggr_sales_daily_segment`, `aggr_sales_daily_product` |

---

## **Column Naming Conventions**

### **Surrogate Keys**

- All primary keys in dimension tables use consistent key naming.

**Pattern**: `<table_name>_key`
- **`<table_name>`**: Name of the table or entity the key represents
- **`_key`**: Suffix indicating surrogate key column

**Example**:
- `customer_key` → Surrogate key in the `dim_customers` table

### **Technical Columns**

- System-generated metadata columns are clearly distinguished from business data.

**Pattern**: `dwh_<column_name>`

- **`dwh`**: Prefix exclusively for system-generated metadata
- **`<column_name>`**: Descriptive name indicating the column's purpose

**Example**:
- `dwh_load_date` → System-generated column storing record load timestamp

### **Booleans & Flags**

- Boolean columns use semantic prefixes for clarity.

**Patterns**:
- `is_<condition>` → State or status or possession indicators

**Data Types**:
- Use `BIT` type in SQL Server for Silver/Gold layers
- Preserve source data types in Bronze layer

**Examples**:
- `is_cancelled`, `is_returned`, `is_delayed`

### **Dates & Times**

- Temporal columns use suffixes to indicate precision and format.

**Patterns**:

| Suffix        | Data Type              | Purpose                    | Example           |
|---------------|------------------------|----------------------------|-------------------|
| `_date`       | `DATE`                 | Date only                  | `order_date`      |
| `_time`       | `TIME`                 | Time only                  | `process_time`    |
| `_datetime`   | `DATETIME/DATETIME2`   | Full timestamp             | `payment_datetime`|
| `_date_key`   | `INT`                  | Date dimension key (yyyymmdd) | `date_key`     |

**Examples**:
- `order_date`, `order_datetime`, `payment_datetime`, `date_key`

### **Amounts, Rates, Percentages**

- Financial and metric columns follow domain-specific patterns.

**Patterns**:
- **Monetary amounts**: `_amount` suffix
- **Rates/percentages**: `_rate`, `_percent`, or domain-specific names

**Examples**:
- **Amounts**: `sales_amount`, `tax_amount`, `profit_amount`
- **Rates**: `tax_rate`, `profit_margin`, `discount_percent`

---

## **Stored Procedure Naming Conventions**

- Data loading procedures follow layer-specific naming patterns.

**Pattern**: `load_<layer>`

- **`<layer>`**: Target data warehouse layer (`bronze`, `silver`, `gold`)

**Examples**:
- `load_bronze` → Stored procedure for loading data into the Bronze layer
- `load_silver` → Stored procedure for loading data into the Silver layer
- `load_gold` → Stored procedure for loading data into the Gold layer

