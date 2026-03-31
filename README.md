# SQL Data Warehouse using SQL Server

A modern data warehouse implementation built on SQL Server, following the medallion architecture (Bronze → Silver → Gold) to transform raw CRM and ERP data into business-ready analytics tables.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Data Pipeline](#data-pipeline)
- [Scripts Reference](#scripts-reference)
- [Contributing](#contributing)

---

## 🎯 Overview

This SQL Data Warehouse consolidates customer, product, and sales data from multiple source systems (CRM and ERP) into a unified, clean, and analytics-ready environment. The system applies progressive data quality improvements across three layers, enabling reliable business intelligence and reporting.

**Key Benefits:**

- Unified view of customer and product data from disparate sources
- Progressive data quality improvements through layered transformations
- Clear separation of concerns (raw data, cleaned data, analytics-ready data)
- Maintainable and reproducible ETL pipelines

---

## 🏗️ Architecture

This data warehouse follows the **Medallion Architecture** pattern with three distinct layers:

### Bronze Layer 🥉

**Purpose:** Raw data ingestion layer  
**Characteristics:**

- Exact replicas of source system data
- Minimal transformation
- Serves as the system of record for raw data
- Used for auditing and debugging

**Tables:**

- `bronze.crm_cust_info` - Customer master from CRM
- `bronze.crm_prd_info` - Product information from CRM
- `bronze.crm_sales_details` - Sales transactions from CRM
- `bronze.erp_cust_az12` - Additional customer attributes from ERP
- `bronze.erp_loc_a101` - Location/geography data from ERP
- `bronze.erp_px_cat_g1v2` - Product categories from ERP

### Silver Layer 🥈

**Purpose:** Cleaned and standardized data layer  
**Characteristics:**

- Data quality improvements applied (data type validation, null handling)
- Removal of duplicates and inconsistencies
- Standard naming conventions and formats
- Additional metadata (load timestamps, etc.)
- Ready for analytics joins and aggregations

**Tables:**

- `silver.crm_cust_info` - Cleaned customer data
- `silver.crm_prd_info` - Cleaned product data
- `silver.crm_sales_details` - Cleaned sales data
- `silver.erp_cust_az12` - Cleaned ERP customer attributes
- `silver.erp_loc_a101` - Cleaned location data
- `silver.erp_px_cat_g1v2` - Cleaned product categories

### Gold Layer 🏅

**Purpose:** Business-ready analytics layer  
**Characteristics:**

- Pre-joined, aggregated, and denormalized data
- Business logic applied (cross-system master data resolution)
- Optimized for reporting and BI tools
- Direct query targets for dashboards and reports

**Views/Queries:**

- **Customer 360:** Unified customer view combining CRM and ERP attributes with gender reconciliation rules
- **Product Master:** Enriched product data with category hierarchy
- **Data Quality Report:** Gender data mapping analysis

---

## 📝 Data Sources

### CRM System

Files located in `datasets/source_crm/`:

- **cust_info.csv** - Customer master with demographic information
- **prd_info.csv** - Product catalog with pricing and lifecycle dates
- **sales_details.csv** - Sales order line items and transactions

### ERP System

Files located in `datasets/source_erp/`:

- **CUST_AZ12.csv** - Extended customer attributes (birthdate, gender)
- **LOC_A101.csv** - Location and country information
- **PX_CAT_G1V2.csv** - Product category hierarchy and maintenance details

---

## 📁 Project Structure

```
sql-data_warehouse-project/
├── README.md                          # This file
├── datasets/                          # Source data files
│   ├── source_crm/                    # CRM source files
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/                    # ERP source files
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
├── scripts/                           # SQL scripts
│   ├── init_database.sql              # Database and schema initialization
│   ├── bronze/                        # Bronze layer scripts
│   │   ├── ddl_bronze.sql             # Table definitions
│   │   ├── proc_load_bronze.sql       # Data loading procedures
│   │   └── run.sql                    # Execute bronze layer build
│   ├── silver/                        # Silver layer scripts
│   │   ├── ddl_silver.sql             # Table definitions
│   │   ├── data_cleaning.sql          # Data cleansing logic
│   │   ├── proc_load_silver.sql       # Data loading procedures
│   │   └── run.sql                    # Execute silver layer build (if applicable)
│   └── gold/                          # Gold layer scripts
│       └── main_query.sql             # Business analytics queries
├── docs/                              # Documentation
└── tests/                             # Test scripts (planned)
```

---

## 🔧 Prerequisites

- **SQL Server** 2016 or higher (SQL Server Express, Standard, or Enterprise)
- **SQL Server Management Studio (SSMS)** or Azure Data Studio
- **Disk Space:** Minimum 500MB for sample data and warehouse
- Source CSV files in the `datasets/` directory

---

## ✅ Getting Started

### Step 1: Initialize the Database

Run the database initialization script to create the DataWarehouse database and schemas:

```sql
-- Open in SSMS and execute:
-- File: scripts/init_database.sql
```

**This script will:**

- Drop any existing `DataWarehouse` database (⚠️ WARNING: Data loss!)
- Create a fresh `DataWarehouse` database
- Create three schemas: `bronze`, `silver`, and `gold`

### Step 2: Load Bronze Layer

Create tables and load raw data:

```sql
-- Step 2a: Create bronze tables
-- File: scripts/bronze/ddl_bronze.sql

-- Step 2b: Load data into bronze tables
-- File: scripts/bronze/proc_load_bronze.sql
-- (Uses BulkInsert or equivalent to load CSVs)

-- Step 2c: Run the complete bronze pipeline
-- File: scripts/bronze/run.sql
```

### Step 3: Load Silver Layer

Create cleaned tables and transform data:

```sql
-- Step 3a: Create silver tables
-- File: scripts/silver/ddl_silver.sql

-- Step 3b: Execute data cleaning and transformations
-- File: scripts/silver/data_cleaning.sql

-- Step 3c: Load cleaned data into silver tables
-- File: scripts/silver/proc_load_silver.sql
```

### Step 4: Query Gold Layer Analytics

Run business analytics queries:

```sql
-- File: scripts/gold/main_query.sql
-- This file contains pre-built analytics queries ready for BI tools
```

---

## 🔄 Data Pipeline

```
Source Systems
    ↓
CRM System ──┐       ERP System ──┐
    ├─────────┼──────────────────┤
    ↓        ↓                   ↓
┌─────────────────────────────────────┐
│   BRONZE LAYER (Raw Data)           │
│   - crm_cust_info                   │
│   - crm_prd_info                    │
│   - crm_sales_details               │
│   - erp_cust_az12                   │
│   - erp_loc_a101                    │
│   - erp_px_cat_g1v2                 │
└──────────────┬──────────────────────┘
               │ Data Cleaning & Validation
┌──────────────▼──────────────────────┐
│   SILVER LAYER (Clean Data)         │
│   - crm_cust_info (cleaned)         │
│   - crm_prd_info (cleaned)          │
│   - crm_sales_details (cleaned)     │
│   - erp_cust_az12 (cleaned)         │
│   - erp_loc_a101 (cleaned)          │
│   - erp_px_cat_g1v2 (cleaned)       │
└──────────────┬──────────────────────┘
               │ Joins & Aggregations
┌──────────────▼──────────────────────┐
│   GOLD LAYER (Analytics Ready)      │
│   - Customer 360 View               │
│   - Product Master                  │
│   - Sales Analytics                 │
└─────────────────────────────────────┘
               │
        BI Tools / Dashboards
        (Power BI, Tableau, etc.)
```

---

## 📜 Scripts Reference

| Script                        | Purpose                          | Notes                                    |
| ----------------------------- | -------------------------------- | ---------------------------------------- |
| `init_database.sql`           | Initialize database and schemas  | ⚠️ Destructive - drops existing DB       |
| `bronze/ddl_bronze.sql`       | Create bronze layer tables       | Defines raw data structure               |
| `bronze/proc_load_bronze.sql` | Load source CSV data             | Uses BulkInsert or OPENROWSET            |
| `bronze/run.sql`              | Execute complete bronze pipeline | One-stop bronze layer execution          |
| `silver/ddl_silver.sql`       | Create silver layer tables       | Adds metadata columns (dwh_create_date)  |
| `silver/data_cleaning.sql`    | Data quality improvements        | Removes duplicates, validates data types |
| `silver/proc_load_silver.sql` | Transform and load silver tables | Applies business rules                   |
| `gold/main_query.sql`         | Analytics queries                | Ready for BI and reporting tools         |

---

## 📌 Key Transformations

### Silver Layer - Data Quality Improvements

- **Type Validation:** Converts dates to proper DATE/DATETIME2 types
- **Null Handling:** Standardizes null representations
- **Duplicates:** Removes duplicate records from source
- **Metadata:** Adds `dwh_create_date` timestamp to track load time

### Gold Layer - Business Logic

- **Customer 360:** Merges CRM and ERP customer data with conflict resolution rules
  - Uses CRM as master for gender when available
  - Falls back to ERP data (`erp_cust_az12.gen`) if CRM is null or 'n/a'
- **Product Enrichment:** Joins products with category hierarchy
- **Dimension Conformity:** Ensures consistent dimensions across fact tables


## 👤 Author

Abhishek Prajapati


**Last Updated:** March 2026
