# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

lakehouse_name= 'xyz'
warehouse_name= 'xyz'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MAGIC %%tsql -artifact {warehouse_name} -type Warehouse 
# MAGIC 
# MAGIC -- =============================================================================
# MAGIC -- Create Dimension: gold.dim_customers
# MAGIC -- =============================================================================
# MAGIC 
# MAGIC CREATE OR ALTER VIEW dbo.vw_dim_customers3 AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
# MAGIC     ci.cst_id                          AS customer_id,
# MAGIC     ci.cst_key                         AS customer_number,
# MAGIC     ci.cst_firstname                   AS first_name,
# MAGIC     ci.cst_lastname                    AS last_name,
# MAGIC     la.cntry                           AS country,
# MAGIC     la.city                            AS city,
# MAGIC     ci.cst_marital_status              AS marital_status,
# MAGIC     CASE 
# MAGIC         WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
# MAGIC         ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
# MAGIC     END                                AS gender,
# MAGIC     ca.bdate                           AS birthdate,
# MAGIC     ci.cst_create_date                 AS create_date
# MAGIC FROM [{lakehouse_name}].[dbo].[crm_cust_info] ci
# MAGIC LEFT JOIN [{lakehouse_name}].[dbo].erp_cust_az12 ca
# MAGIC     ON ci.cst_key = ca.cid
# MAGIC LEFT JOIN [{lakehouse_name}].[dbo].erp_loc_a101 la
# MAGIC     ON ci.cst_key = la.cid;

# METADATA ********************

# META {
# META   "language": "sql",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MAGIC %%tsql -artifact {warehouse_name} -type Warehouse 
# MAGIC 
# MAGIC -- =============================================================================
# MAGIC -- Create Dimension: gold.dim_products
# MAGIC -- =============================================================================
# MAGIC 
# MAGIC CREATE OR ALTER VIEW dbo.vw_dim_products2 AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
# MAGIC     pn.prd_id       AS product_id,
# MAGIC     pn.prd_key      AS product_number,
# MAGIC     pn.prd_nm       AS product_name,
# MAGIC     pn.cat_id       AS category_id,
# MAGIC     pc.cat          AS category,
# MAGIC     pc.subcat       AS subcategory,
# MAGIC     pc.maintenance  AS maintenance,
# MAGIC     pn.prd_cost     AS cost,
# MAGIC     pn.prd_line     AS product_line,
# MAGIC     pn.prd_start_dt AS start_date
# MAGIC FROM [{lakehouse_name}].[dbo].crm_prd_info pn
# MAGIC LEFT JOIN [{lakehouse_name}].[dbo].erp_px_cat_g1v2 pc
# MAGIC     ON pn.cat_id = pc.id
# MAGIC WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

# METADATA ********************

# META {
# META   "language": "sql",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MAGIC %%tsql -artifact {warehouse_name} -type Warehouse 
# MAGIC 
# MAGIC -- =============================================================================
# MAGIC -- Create Fact Table: gold.fact_sales
# MAGIC -- =============================================================================
# MAGIC 
# MAGIC 
# MAGIC CREATE OR ALTER VIEW dbo.vw_fact_sales2 AS
# MAGIC SELECT
# MAGIC     sd.sls_ord_num                              AS order_number,
# MAGIC     pr.product_key                              AS product_key,
# MAGIC     cu.customer_key                             AS customer_key,
# MAGIC     sd.sls_order_dt                             AS order_date,
# MAGIC     sd.sls_ship_dt                              AS shipping_date,
# MAGIC     sd.sls_due_dt                               AS due_date,
# MAGIC     sd.sls_quantity                             AS quantity,
# MAGIC     sd.sls_price                                AS price,
# MAGIC     sd.sls_sales                                AS revenue,
# MAGIC     (pr.cost * sd.sls_quantity)                 AS COGS,
# MAGIC     sd.sls_sales - (pr.cost * sd.sls_quantity)  AS profit
# MAGIC FROM [{lakehouse_name}].[dbo].crm_sales_details sd
# MAGIC LEFT JOIN dbo.vw_dim_products pr
# MAGIC     ON sd.sls_prd_key = pr.product_number
# MAGIC LEFT JOIN dbo.vw_dim_customers cu
# MAGIC     ON sd.sls_cust_id = cu.customer_id;

# METADATA ********************

# META {
# META   "language": "sql",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MAGIC %%tsql -artifact {warehouse_name} -type Warehouse
# MAGIC -- ===========================================================================
# MAGIC -- CREATE DATE TABLE
# MAGIC -- ===========================================================================
# MAGIC DROP TABLE IF EXISTS {warehouse_name}.dbo.Dates;
# MAGIC 
# MAGIC CREATE TABLE {warehouse_name}.dbo.Dates AS
# MAGIC SELECT [Date]
# MAGIC       ,[Day]
# MAGIC       ,[Year]
# MAGIC       ,[Month] AS [Month_Num]
# MAGIC       ,[Monthshort] AS [Month_Short]
# MAGIC       ,[MonthName] AS [Month]
# MAGIC       ,[Weekday2] AS Weekday
# MAGIC       ,[Day_of_Week]
# MAGIC       ,[Quarter]
# MAGIC       ,[Year_Month]
# MAGIC       ,[Month_Year]
# MAGIC FROM [{lakehouse_name}].[dbo].dim_date;


# METADATA ********************

# META {
# META   "language": "sql",
# META   "language_group": "jupyter_python"
# META }
