
/*
DATA WAREHOUSE: GOLD LAYER VIEW INITIALIZATION
PURPOSE: Builds the analytical foundation by creating dimension and fact views in the Gold layer.
         These views transform integrated Silver layer data into a business-friendly dimensional model
         optimized for reporting, dashboards, and analytics.

DESCRIPTION:
This script creates three core views for the Gold layer:
1. dim_customer – Master customer dimension integrating CRM and ERP data with cleaned demographics.
2. dim_product – Current product catalog with category hierarchy, filtering out historical records.
3. fact_sales – Central sales fact table linking transactions to customer and product dimensions.

KEY LOGIC IMPLEMENTED:
- Surrogate Key Generation: Uses row_number() for stable dimension keys (customer_key, product_key).
- Data Quality Handling: Standardizes gender values with fallback logic (N/A handling).
- Historical Filtering: Excludes ended products (prd_end_dt IS NULL) to show current catalog only.
- Cross-System Integration: Joins CRM (primary) and ERP (supplemental) tables for complete attributes.

DEPENDENCIES: 
- Silver layer tables: crm_cust_info, erp_cust_info, erp_cust_loc, crm_prd_info, erp_prd_cat, crm_sls_details.
- Execution Order: dim_customer and dim_product must be created before fact_sales.

AUTHOR: [Your Name/Team]
CREATED: [Date]
VERSION: 1.0
NOTES: These views establish the star schema foundation for all downstream reporting.
*/





create view gold.dim_customer as
select 
      row_number() over(order by cts_id) as customer_key,
      ci.cts_id,
      ci.cst_key,
      ci.cst_firstname as first_name,
      ci.cst_lastname as last_name,
      ci.cst_marital_status as marital_ststus,
      
      case when ci.cst_gendr != 'n/a' then ci.cst_gendr
           else coalesce(eci.gen, 'N/A')   --- Considering CRM table as a master table 
           end as gender,
    
      ci.cst_create_date as created_date,
      eci.bdate as birth_date,
      cl.cntry as country
      
      from silver.crm_cust_info ci
      left join silver.erp_cust_info eci
      on ci.cst_key = eci.cid
      left join silver.erp_cust_loc cl
      on ci.cst_key = cl.cid

----------------------------------------------------------------------------------


create view gold.dim_product as 
SELECT 
       row_number() over(order by cpi.prd_key,cpi.prd_start_dt) as product_key,
      cpi.prd_id as product_id,
      cpi.cat_id as catagory_id,
      cpi.prd_key as product_number,
      cpi.prd_nm as product_name,
      cpi.prd_cost as cost,
      cpi.prd_line as product_line,
      cpi.prd_start_dt as start_date,

      pc.cat as catagory,
      pc.subcat as subcatagory,
      pc.maintenance
  FROM [DataWarehouse].[silver].[crm_prd_info] cpi
  left join silver.erp_prd_cat pc
  on cpi.cat_id= pc.id
  where cpi.prd_end_dt is null ---Filtering out all historical data




-----------------------------------------------------------------------------------



create view gold.fact_saels as
SELECT
      sd.sls_ord_num,
      pr.product_key,
      dc.customer_key,
      sd.sls_order_dt,
      sd.sls_ship_dt,
      sd.sls_due_dt,
      sd.sls_sales,
      sd.sls_quantity,
      sd.sls_price,
      dhw_create_date
  FROM silver.crm_sls_details sd
  left join gold.dim_customers dc
  on sd.sls_cust_id= dc.customer_key
  left join gold.dim_product pr
  on sd.sls_prd_key= pr.product_key



  

