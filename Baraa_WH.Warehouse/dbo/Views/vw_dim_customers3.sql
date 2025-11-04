-- Auto Generated (Do not modify) 52B7372AD659598C3235D639D46890C36A63E8709FD20FD5B13C38ECC5458E33


-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

CREATE   VIEW dbo.vw_dim_customers3 AS
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