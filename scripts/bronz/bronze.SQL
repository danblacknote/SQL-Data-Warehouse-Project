```sql
/**************************************************************************************
SCRIPT: Create Bronze Layer Tables for Data Warehouse
PURPOSE: Create staging tables in the bronze schema for raw data ingestion
         These tables will hold unprocessed data from CRM and ERP source systems
ARCHITECTURE: Medallion Architecture (Bronze -> Silver -> Gold)
              Bronze: Raw, unprocessed source data
AUTHOR: Deneke Zewdu
DATE: Dec 29,2025
VERSION: 1.0
NOTES: 
- Tables are dropped and recreated (idempotent design)
- All tables use appropriate data types based on source data characteristics
- Date columns use DATE type for temporal data integrity
- Character data uses NVARCHAR for Unicode support
**************************************************************************************/

USE DataWarehouse;
GO

-- =============================================================================
-- CRM SYSTEM TABLES
-- These tables store raw customer, product, and sales data from the CRM system
-- =============================================================================

-- Drop CRM Customer Information table if it exists (idempotent operation)
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

-- Create CRM Customer Information table
-- Purpose: Store raw customer demographic and registration data from CRM
-- Source: CRM system customer master data
CREATE TABLE bronze.crm_cust_info(
    cts_id INT,                     -- Customer system ID (sequential integer)
    cst_key NVARCHAR(50),           -- Customer business key (unique identifier)
    cst_firstname NVARCHAR(50),     -- Customer first name
    cst_lastname NVARCHAR(50),      -- Customer last name
    cst_marital_status NVARCHAR(30),-- Marital status (Single, Married, etc.)
    cst_gendr NVARCHAR(30),         -- Gender (Male, Female, Other)
    cst_create_date DATE            -- Customer account creation date
);
GO

-- Drop CRM Product Information table if it exists
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

-- Create CRM Product Information table
-- Purpose: Store raw product catalog data from CRM
-- Source: CRM system product master data
CREATE TABLE bronze.crm_prd_info(
    prd_id INT,                     -- Product system ID (sequential integer)
    prd_key NVARCHAR(50),           -- Product business key (SKU/Item number)
    prd_nm NVARCHAR(50),            -- Product name/description
    prd_cost INT,                   -- Product cost (integer currency, consider DECIMAL for precision)
    prd_line NVARCHAR(30),          -- Product line/category
    prd_start_dt DATE,              -- Product effective start date
    prd_end_dt DATE                 -- Product effective end date (NULL for active products)
);
GO

-- Drop CRM Sales Details table if it exists
IF OBJECT_ID('bronze.crm_sls_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sls_details;

-- Create CRM Sales Details table
-- Purpose: Store raw transactional sales data from CRM
-- Source: CRM system sales transactions
-- NOTE: Date fields are INT - this likely contains date keys (YYYYMMDD format)
--       Consider converting to DATE type in silver layer transformation
CREATE TABLE bronze.crm_sls_details(
    sls_ord_num NVARCHAR(50),       -- Sales order number (alphanumeric)
    sls_prd_key NVARCHAR(50),       -- Product key (links to crm_prd_info)
    sls_cust_id INT,                -- Customer ID (links to crm_cust_info)
    sls_order_dt INT,               -- Order date (integer format YYYYMMDD)
    sls_ship_dt INT,                -- Ship date (integer format YYYYMMDD)
    sls_due_dt INT,                 -- Due date (integer format YYYYMMDD)
    sls_sales INT,                  -- Sales amount (integer currency)
    sls_quantity INT,               -- Quantity sold
    sls_price INT                   -- Unit price (integer currency)
);
GO

-- =============================================================================
-- ERP SYSTEM TABLES
-- These tables store raw customer and product data from the ERP system
-- Note: ERP data has different structure and field names than CRM
-- =============================================================================

-- Drop ERP Customer Information table if it exists
IF OBJECT_ID('bronze.erp_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_info;

-- Create ERP Customer Information table
-- Purpose: Store raw customer demographic data from ERP
-- Source: ERP system customer master (different structure than CRM)
CREATE TABLE bronze.erp_cust_info(
    cid NVARCHAR(50),               -- Customer ID (different format than CRM)
    bdate DATE,                     -- Birth date
    gen NVARCHAR(30)                -- Gender
    -- NOTE: Missing comma removed from original code
);
GO

-- Drop ERP Customer Location table if it exists
IF OBJECT_ID('bronze.erp_cust_loc', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_loc;

-- Create ERP Customer Location table
-- Purpose: Store raw customer geographic data from ERP
-- Source: ERP system customer address/location data
CREATE TABLE bronze.erp_cust_loc(
    cid NVARCHAR(50),               -- Customer ID (links to erp_cust_info)
    cntry NVARCHAR(50)              -- Country of residence
);
GO

-- Drop ERP Product Category table if it exists
IF OBJECT_ID('bronze.erp_prd_cat', 'U') IS NOT NULL
    DROP TABLE bronze.erp_prd_cat;

-- Create ERP Product Category table
-- Purpose: Store raw product categorization data from ERP
-- Source: ERP system product hierarchy/category data
CREATE TABLE bronze.erp_prd_cat(
    id NVARCHAR(50),                -- Product ID/SKU
    cta NVARCHAR(50),               -- Product category
    subcat NVARCHAR(50),            -- Product subcategory
    maintenance NVARCHAR(50)        -- Maintenance type/classification
);
GO

/**************************************************************************************

