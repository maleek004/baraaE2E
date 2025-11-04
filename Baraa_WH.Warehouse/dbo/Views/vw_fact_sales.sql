-- Auto Generated (Do not modify) DA584DA4BAD64F59B50814CB0246F98A1F579B2B68D251D5549DFCD88FC5D551


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================


CREATE   VIEW dbo.vw_fact_sales AS
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