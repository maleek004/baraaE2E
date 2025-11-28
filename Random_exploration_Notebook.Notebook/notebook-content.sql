-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "3a15daa0-1517-4964-82d7-ea00507b8389",
-- META       "default_lakehouse_name": "Baraa_LH",
-- META       "default_lakehouse_workspace_id": "9bb22313-ca99-4235-bcf0-16cf2704e122",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "3a15daa0-1517-4964-82d7-ea00507b8389"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "6d92e802-0ef3-b21e-439c-f98c3968ed05",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "863d4730-2016-4730-802e-d2b90f4baf60",
-- META           "type": "Lakewarehouse"
-- META         },
-- META         {
-- META           "id": "6d92e802-0ef3-b21e-439c-f98c3968ed05",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- ### Some fixes that i made 
-- * Inferred that the category id 'CO_PE' meant Components and Pedals and used that information to fill null values in the category and subcategory columns 
-- * found out that the day difference between the due date and order date is a constant 12 and used that information to fill the rows where order date is null  
-- 
-- ### Some observations
-- * **important:** The granularity of the sales data is at the product per order *(product line)* level ... so each product bought for each order sits on a different row
-- * some products are sold at prices lower than their cost which leads to a few losses in the dataset . product key of some of these products that were sold on discount at least once: (249,294,251,248,104,282,290)  
-- *  the Average order items per order is 2. orders with the highest items have 8 items 
-- * 17 products have NULL values as their product line ... Which i am still unable to fix 
-- * There are 337 customers (out of 18484) whose country is unknown - i have added a random country between ['Australia', 'United States', 'Canada', 'Germany','United Kingdom', 'France'] to these entries.


-- CELL ********************

select order_number , COUNT(*) frequency
from vw_fact_sales
group by order_number
order by frequency desc

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP 10 * FROM vw_fact_sales

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT   * FROM vw_fact_sales s 
WHERE customer

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT COUNT(*)
FROM [Baraa_LH].[dbo].[crm_sales_details]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT * FROM vw_dim_products WHERE product_line = 'other sales'

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT product_line 
FROM vw_dim_products
WHERE category = 'Components'

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT * FROM vw_dim_products
WHERE product_line = 'n/a'

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT order_number) AS distinct_order,
        Count(*) AS total_records 
FROM vw_fact_sales


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

WITH orders AS (SELECT order_number , COUNT(*) Order_items
FROM vw_fact_sales
GROUP BY order_number 
)

SELECT AVG(Order_items)
FROM orders

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT 		[Date], 
			Day,
			[Day_of_Week],
			[Weekday],
			[Weekday2],
			[Month],
			[MonthName],
			[Monthshort],
			[Quarter],
			[Year],
			[Year_Month],
			[EOM],
			[EOQ]
FROM [Baraa_LH].[dbo].[dim_date]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

WITH diff AS (SELECT DATEDIFF(day, due_date , order_date) as due_days
FROM vw_fact_sales)
SELECT due_days , count(*) 
FROM diff
GROUP BY due_days


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT category_id
FROM vw_dim_products

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT category 
FROM vw_dim_products

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP 10 * FROM vw_dim_products WHERE subcategory IS NOT NULL

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
