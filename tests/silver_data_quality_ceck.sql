/*
================================================================================
COMPREHENSIVE DATA QUALITY CHECK SCRIPT
================================================================================
DESCRIPTION:
This script performs exhaustive data quality checks on both bronze (raw) and 
silver (transformed) layers. It validates all transformation logic from the 
silver.load_silver procedure and identifies data issues that require attention.

OBJECTIVES:
1. Validate all data transformations from bronze to silver layer
2. Identify data quality issues in source (bronze) data
3. Verify business rule application in silver layer
4. Detect transformation errors and inconsistencies
================================================================================
*/

-- ============================================================================
-- PART 1: CRM CUSTOMER DATA QUALITY CHECKS
-- ============================================================================

-- Check 1.1: Invalid marital status codes in bronze
/*
TRANSFORMATION LOGIC (from load_silver):
  M → 'Married', S → 'Single', others → 'N/A'
PURPOSE: Identify source values that don't match expected codes
*/
SELECT DISTINCT 
    cst_marital_status,
    COUNT(*) as record_count
FROM bronze.crm_cust_info
GROUP BY cst_marital_status
ORDER BY record_count DESC;

-- Check 1.2: Invalid gender codes in bronze
/*
TRANSFORMATION LOGIC (from load_silver):
  F/Female → 'Female', M/MALE → 'Male', others → 'N/A'
*/
SELECT DISTINCT 
    cst_gendr,
    COUNT(*) as record_count
FROM bronze.crm_cust_info
GROUP BY cst_gendr
ORDER BY record_count DESC;

-- Check 1.3: Verify standardization in silver layer
SELECT DISTINCT 
    cst_marital_status,
    cst_gendr
FROM silver.crm_cust_info;

-- Check 1.4: Missing customer names
SELECT 
    cts_id,
    cst_firstname,
    cst_lastname
FROM bronze.crm_cust_info
WHERE TRIM(cst_firstname) = '' 
   OR TRIM(cst_lastname) = ''
   OR cst_firstname IS NULL 
   OR cst_lastname IS NULL;


-- ============================================================================
-- PART 2: CRM PRODUCT DATA QUALITY CHECKS
-- ============================================================================

-- Check 2.1: Invalid product costs (negative or missing)
/*
TRANSFORMATION LOGIC (from load_silver):
  ISNULL(prd_cost, 0) → Default NULL to 0
*/
SELECT 
    prd_id,
    prd_key,
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check 2.2: Invalid product line codes
/*
TRANSFORMATION LOGIC (from load_silver):
  M → 'Mountain', R → 'Road', S → 'Other Sales', T → 'Touring', others → 'N/A'
*/
SELECT DISTINCT 
    prd_line,
    COUNT(*) as record_count
FROM bronze.crm_prd_info
GROUP BY prd_line
ORDER BY record_count DESC;

-- Check 2.3: Verify standardized product lines in silver
SELECT DISTINCT
    prd_line
FROM silver.crm_prd_info;

-- Check 2.4: Invalid date ranges (start date after end date)
SELECT 
    prd_id,
    prd_key,
    prd_start_dt,
    prd_end_dt
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- Check 2.5: Product key structure validation
/*
TRANSFORMATION LOGIC (from load_silver):
  substring(prd_key, 1, 5) as cat_id
  substring(prd_key, 7, Len(prd_key)) as prd_key
PURPOSE: Check if product keys follow expected format
*/
SELECT 
    prd_key,
    LEN(prd_key) as key_length,
    CASE 
        WHEN LEN(prd_key) < 7 THEN 'Invalid: Too short'
        ELSE 'Valid length'
    END as validation_status
FROM bronze.crm_prd_info
WHERE LEN(prd_key) < 7;


-- ============================================================================
-- PART 3: CRM SALES DATA QUALITY CHECKS
-- ============================================================================

-- Check 3.1: Invalid date formats in bronze
/*
TRANSFORMATION LOGIC (from load_silver):
  Convert integer YYYYMMDD to DATE, invalid → NULL
VALIDATION: Must be positive, 8 characters, within 19000101-20500101
*/
SELECT 
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    COUNT(*) as record_count
FROM bronze.crm_sls_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
   OR sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8
   OR sls_due_dt <= 0 OR LEN(sls_due_dt) != 8
GROUP BY sls_order_dt, sls_ship_dt, sls_due_dt;

-- Check 3.2: Date range validation (specific bounds)
SELECT 
    'Order Date' as date_field,
    sls_order_dt,
    COUNT(*) as invalid_count
FROM bronze.crm_sls_details
WHERE sls_order_dt > 20500101 OR sls_order_dt < 19000101
GROUP BY sls_order_dt
UNION ALL
SELECT 
    'Ship Date',
    sls_ship_dt,
    COUNT(*)
FROM bronze.crm_sls_details
WHERE sls_ship_dt > 20500101 OR sls_ship_dt < 19000101
GROUP BY sls_ship_dt
UNION ALL
SELECT 
    'Due Date',
    sls_due_dt,
    COUNT(*)
FROM bronze.crm_sls_details
WHERE sls_due_dt > 20500101 OR sls_due_dt < 19000101
GROUP BY sls_due_dt;

-- Check 3.3: Sales calculation inconsistencies
/*
TRANSFORMATION LOGIC (from load_silver):
  If sls_sales ≠ quantity × price OR invalid → recalculate
  sls_price negative/NULL → recalculate from sales/quantity
*/
SELECT 
    sls_ord_num,
    sls_sales,
    sls_quantity,
    sls_price,
    sls_quantity * sls_price as calculated_sales,
    CASE 
        WHEN sls_sales != sls_quantity * sls_price THEN 'Calculation mismatch'
        WHEN sls_quantity IS NULL THEN 'Missing quantity'
        WHEN sls_price IS NULL THEN 'Missing price'
        WHEN sls_sales IS NULL THEN 'Missing sales'
        WHEN sls_quantity <= 0 THEN 'Invalid quantity'
        WHEN sls_price <= 0 THEN 'Invalid price'
        ELSE 'Valid'
    END as issue_type
FROM bronze.crm_sls_details
WHERE sls_sales != sls_quantity * sls_price 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL 
   OR sls_sales IS NULL
   OR sls_quantity <= 0 
   OR sls_price <= 0;

-- Check 3.4: Zero division risk
SELECT 
    sls_ord_num,
    sls_quantity,
    sls_price,
    sls_sales
FROM bronze.crm_sls_details
WHERE sls_quantity = 0 AND (sls_price IS NULL OR sls_price <= 0);


-- ============================================================================
-- PART 4: ERP CUSTOMER DATA QUALITY CHECKS
-- ============================================================================

-- Check 4.1: Customer ID prefix cleanup validation
/*
TRANSFORMATION LOGIC (from load_silver):
  Remove 'NAS' prefix from customer IDs
*/
SELECT DISTINCT
    cid,
    CASE 
        WHEN cid LIKE 'NAS%' THEN 'Has NAS prefix'
        ELSE 'No prefix'
    END as prefix_status
FROM bronze.erp_cust_info
WHERE cid LIKE 'NAS%';

-- Check 4.2: Future birth dates
/*
TRANSFORMATION LOGIC (from load_silver):
  Future bdate → NULL
*/
SELECT 
    cid,
    bdate
FROM bronze.erp_cust_info
WHERE bdate > GETDATE();

-- Check 4.3: Invalid gender codes
/*
TRANSFORMATION LOGIC (from load_silver):
  M/MALE → 'Male', F/FEMALE → 'Female', others → 'N/A'
*/
SELECT DISTINCT
    gen,
    COUNT(*) as record_count
FROM bronze.erp_cust_info
GROUP BY gen
ORDER BY record_count DESC;


-- ============================================================================
-- PART 5: ERP CUSTOMER LOCATION DATA QUALITY CHECKS
-- ============================================================================

-- Check 5.1: Hyphen removal from customer IDs
/*
TRANSFORMATION LOGIC (from load_silver):
  REPLACE(cid, '-', '')
*/
SELECT 
    cid,
    REPLACE(cid, '-', '') as cleaned_cid
FROM bronze.erp_cust_loc
WHERE cid LIKE '%-%';

-- Check 5.2: Country code standardization
/*
TRANSFORMATION LOGIC (from load_silver):
  DE → 'Germany', US/USA → 'United States', empty/NULL → 'N/A'
*/
SELECT DISTINCT
    cntry,
    COUNT(*) as record_count
FROM bronze.erp_cust_loc
GROUP BY cntry
ORDER BY record_count DESC;

-- Check 5.3: Missing country information
SELECT 
    cid,
    cntry
FROM bronze.erp_cust_loc
WHERE TRIM(cntry) = '' OR cntry IS NULL;


-- ============================================================================
-- PART 6: ERP PRODUCT CATEGORY DATA QUALITY CHECKS
-- ============================================================================

-- Check 6.1: Direct copy validation (no transformations)
/*
Note: erp_prd_cat has no transformations in load_silver
PURPOSE: Verify data integrity in direct copy
*/
SELECT 
    'Bronze' as source_layer,
    COUNT(*) as record_count
FROM bronze.erp_prd_cat
UNION ALL
SELECT 
    'Silver',
    COUNT(*)
FROM silver.erp_prd_cat;

-- Check 6.2: Missing category values
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_prd_cat
WHERE cat IS NULL OR subcat IS NULL OR maintenance IS NULL;


-- ============================================================================
-- PART 7: TRANSFORMATION VALIDATION SUMMARY
-- ============================================================================

-- Summary 7.1: Count of records with transformation issues
SELECT 
    'CRM Customer - Invalid Marital Status' as check_type,
    COUNT(*) as issue_count
FROM bronze.crm_cust_info
WHERE UPPER(TRIM(cst_marital_status)) NOT IN ('M', 'S')
UNION ALL
SELECT 
    'CRM Customer - Invalid Gender',
    COUNT(*)
FROM bronze.crm_cust_info
WHERE UPPER(TRIM(cst_gendr)) NOT IN ('F', 'FEMALE', 'M', 'MALE')
UNION ALL
SELECT 
    'CRM Product - Invalid Cost',
    COUNT(*)
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL
UNION ALL
SELECT 
    'CRM Sales - Invalid Dates',
    COUNT(*)
FROM bronze.crm_sls_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
   OR sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8
   OR sls_due_dt <= 0 OR LEN(sls_due_dt) != 8
UNION ALL
SELECT 
    'ERP Customer - Future Birth Dates',
    COUNT(*)
FROM bronze.erp_cust_info
WHERE bdate > GETDATE();

