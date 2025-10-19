-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "6d92e802-0ef3-b21e-439c-f98c3968ed05",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "6d92e802-0ef3-b21e-439c-f98c3968ed05",
-- META           "type": "Datawarehouse"
-- META         },
-- META         {
-- META           "id": "863d4730-2016-4730-802e-d2b90f4baf60",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- 
-- #### DDL Script: Create Gold Views
-- ##### Script Purpose:
--     This script creates views for the Gold layer in the data warehouse. 
--     The Gold layer represents the final dimension and fact tables (Star Schema)
-- 
--     Each view performs transformations and combines data from the Silver layer 
--     to produce a clean, enriched, and business-ready dataset.
-- 
-- ##### Usage: 
--     These views can be queried directly for analytics and reporting.
-- 


-- CELL ********************


-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

CREATE OR ALTER VIEW dbo.vw_dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    la.city                            AS city,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM [Baraa_LH].[dbo].[crm_cust_info] ci
LEFT JOIN [Baraa_LH].[dbo].erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN [Baraa_LH].[dbo].erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

PRINT 'Customer Dimension view created successfully' GO
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================


CREATE OR ALTER VIEW dbo.vw_dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM [Baraa_LH].[dbo].crm_prd_info pn
LEFT JOIN [Baraa_LH].[dbo].erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO
PRINT 'Product Dimension view created successfully' GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================


CREATE OR ALTER VIEW dbo.vw_fact_sales AS
SELECT
    sd.sls_ord_num                              AS order_number,
    pr.product_key                              AS product_key,
    cu.customer_key                             AS customer_key,
    sd.sls_order_dt                             AS order_date,
    sd.sls_ship_dt                              AS shipping_date,
    sd.sls_due_dt                               AS due_date,
    sd.sls_quantity                             AS quantity,
    sd.sls_price                                AS price,
    sd.sls_sales                                AS revenue,
    (pr.cost * sd.sls_quantity)                 AS COGS,
    sd.sls_sales - (pr.cost * sd.sls_quantity)  AS profit
FROM [Baraa_LH].[dbo].crm_sales_details sd
LEFT JOIN dbo.vw_dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN dbo.vw_dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
PRINT 'Fact Sales view created successfully'

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

DROP TABLE IF EXISTS Baraa_WH.dbo.Dates;

CREATE TABLE Baraa_WH.dbo.Dates AS
SELECT [Date]
      ,[Day]
      ,[Year]
      ,[Month] AS [Month_Num]
      ,[Monthshort] AS [Month_Short]
      ,[MonthName] AS [Month]
      ,[Weekday2] AS Weekday
      ,[Day_of_Week]
      ,[Quarter]
      ,[Year_Month]
      ,[Month_Year]
FROM [Baraa_LH].[dbo].dim_date;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
