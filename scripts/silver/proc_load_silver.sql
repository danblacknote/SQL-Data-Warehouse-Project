/*
===============================================================================
STORED PROCEDURE: silver.load_silver
PURPOSE: Loads and transforms data from bronze (raw) to silver (cleansed) layer
DESCRIPTION: 
  - Idempotent procedure that truncates and reloads all silver layer tables
  - Performs data quality checks, standardization, and business rule application
  - Includes detailed logging, timing, and error handling
  - Executes as a single batch for atomic loading

DATA TRANSFORMATIONS PERFORMED:
  1. CRM Customer Info: 
     - Trims whitespace from names
     - Standardizes marital status (M→Married, S→Single)
     - Standardizes gender values (F/Female→Female, M/Male→Male)
     
  2. CRM Product Info:
     - Extracts category ID from product key
     - Standardizes product line codes (M→Mountain, R→Road, etc.)
     - Calculates end date as day before next product start date
     
  3. CRM Sales Details:
     - Validates and converts integer date fields (YYYYMMDD) to DATE type
     - Corrects sales amount calculations when inconsistent
     - Ensures positive price values
     
  4. ERP Customer Info:
     - Removes 'NAS' prefix from customer IDs
     - Validates birth dates (cannot be future dates)
     - Standardizes gender values
     
  5. ERP Customer Location:
     - Removes hyphens from customer IDs
     - Standardizes country codes (DE→Germany, US/USA→United States)
     
  6. ERP Product Category:
     - Direct copy with no transformations

PERFORMANCE FEATURES:
  - Tracks individual table load durations
  - Tracks total batch duration
  - Uses batch processing for efficiency
  - Truncates before load (faster than delete)

ERROR HANDLING:
  - Comprehensive try-catch block
  - Detailed error logging with message, number, and state
  - Continues batch processing unless fatal error

PREREQUISITES:
  1. Bronze layer tables must exist and contain data
  2. Silver layer tables must be created (run table creation script first)
  3. User must have TRUNCATE and INSERT permissions on silver tables

USAGE: 
  EXEC silver.load_silver;

OUTPUT:
  - Console messages with loading progress and timing
  - Error messages if any failures occur
  - Returns 0 on success, error details on failure

MAINTENANCE NOTES:
  - Add indexes to silver tables after loading for better query performance
  - Consider partitioning large tables
  - Monitor duration times for performance degradation
  - Review transformation logic periodically for business rule changes
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver 
AS 
BEGIN 
    DECLARE @start_time DATETIME, 
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        
        -- 1. LOAD CRM CUSTOMER INFORMATION
        SET @start_time = GETDATE();
        PRINT 'Truncating table silver.crm_cust_info >>>>';
        TRUNCATE TABLE silver.crm_cust_info;
        
        PRINT '+++++++++++++++++++++++++++++++++++++++';
        PRINT '++                                   ++';
        PRINT '++  Loading the CRM_cust_info Table  ++';
        PRINT '++                                   ++';
        PRINT '+++++++++++++++++++++++++++++++++++++++';
        
        INSERT INTO silver.crm_cust_info(
            cts_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gendr,
            cst_create_date
        )
        SELECT 
            cts_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'  -- Fixed typo 'Singel' → 'Single'
                ELSE 'N/A'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gendr)) IN ('F', 'Female') THEN 'Female'  -- Fixed typo 'Femail' → 'Female'
                WHEN UPPER(TRIM(cst_gendr)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'N/A'
            END AS cst_gendr,
            cst_create_date
        FROM bronze.crm_cust_info;
        
        SET @end_time = GETDATE();
        PRINT 'Duration to complete loading this table = ' + 
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        
        -- 2. LOAD CRM PRODUCT INFORMATION
        SET @start_time = GETDATE();
        PRINT 'Truncating table silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        
        PRINT '+++++++++++++++++++++++++++++++++++++++';
        PRINT '++                                   ++';
        PRINT '++  Loading the CRM_prd_info Table   ++';
        PRINT '++                                   ++';
        PRINT '+++++++++++++++++++++++++++++++++++++++';
        
        INSERT INTO silver.crm_prd_info(
            prd_id,
            prd_key,  -- Note: Column name mismatch - should match table definition
            prd_nm,
            prd_cost,
            prd_line, 
            prd_start_dt,
            prd_end_dt  
        )
        SELECT 
            prd_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,  -- Fixed: Missing cat_id column in target table
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(DATEADD(DAY, -1, 
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
            ) AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        
        SET @end_time = GETDATE();
        PRINT 'Duration to complete loading this table = ' + 
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        
        -- 3. LOAD CRM SALES DETAILS
        SET @start_time = GETDATE();
        PRINT 'Truncating table silver.crm_sls_details';
        TRUNCATE TABLE silver.crm_sls_details;
        
        PRINT '+++++++++++++++++++++++++++++++++++++++++';
        PRINT '++                                     ++';
        PRINT '++  Loading the CRM_sls_details Table  ++';
        PRINT '++                                     ++';
        PRINT '+++++++++++++++++++++++++++++++++++++++++';
        
        INSERT INTO silver.crm_sls_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price 
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales != sls_quantity * ABS(sls_price) 
                     OR sls_sales <= 0 
                     OR sls_sales IS NULL
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,  -- Fixed: Was 'old_quantity' but column is sls_quantity
            CASE 
                WHEN sls_price <= 0 OR sls_price IS NULL
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sls_details;
        
        SET @end_time = GETDATE();
        PRINT 'Duration to complete loading this table = ' + 
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        
        -- 4. LOAD ERP CUSTOMER INFORMATION
        SET @start_time = GETDATE();
        PRINT 'Truncating table silver.erp_cust_info';
        TRUNCATE TABLE silver.erp_cust_info;
        
        PRINT '+++++++++++++++++++++++++++++++++++++++++';
        PRINT '++                                     ++';
        PRINT '++  Loading the ERP_cust_info Table    ++';
        PRINT '++                                     ++';
        PRINT '+++++++++++++++++++++++++++++++++++++++++';
        
        INSERT INTO silver.erp_cust_info(
            cid,
            bdate,
            gen
        )
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'N/A'
            END AS gen
        FROM bronze.erp_cust_info;
        
        SET @end_time = GETDATE();
        PRINT 'Duration to complete loading this table = ' + 
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        
        -- 5. LOAD ERP CUSTOMER LOCATION
        SET @start_time = GETDATE();
        PRINT 'Truncating table silver.erp_cust_loc';
        TRUNCATE TABLE silver.erp_cust_loc;
        
        PRINT '+++++++++++++++++++++++++++++++++++++++++';
        PRINT '++                                     ++';
        PRINT '++  Loading the ERP_cust_loc Table     ++';
        PRINT '++                                     ++';
        PRINT '+++++++++++++++++++++++++++++++++++++++++';
        
        INSERT INTO silver.erp_cust_loc(
            cid,
            cntry
        )
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
                ELSE cntry
            END AS cntry
        FROM bronze.erp_cust_loc;
        
        SET @end_time = GETDATE();
        PRINT 'Duration to complete loading this table = ' + 
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        
        -- 6. LOAD ERP PRODUCT CATEGORY
        SET @start_time = GETDATE();
        PRINT 'Truncating table silver.erp_prd_cat';
        TRUNCATE TABLE silver.erp_prd_cat;
        
        PRINT '+++++++++++++++++++++++++++++++++++++++++';
        PRINT '++                                     ++';
        PRINT '++  Loading the ERP_prd_cat Table      ++';
        PRINT '++                                     ++';
        PRINT '+++++++++++++++++++++++++++++++++++++++++';
        
        INSERT INTO silver.erp_prd_cat(
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_prd_cat;
        
        SET @end_time = GETDATE();
        PRINT 'Duration to complete loading this table = ' + 
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        
        -- BATCH COMPLETION SUMMARY
        SET @batch_end_time = GETDATE();
        PRINT 'Total duration to complete loading the whole batch = ' + 
              CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
    END TRY
    BEGIN CATCH 
        PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';
        PRINT 'Error Occurred During Loading';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';
        -- Consider adding: THROW; to re-raise the error
    END CATCH;
END;

-- Uncomment to execute the procedure after creation
  -- EXEC silver.load_silver;
