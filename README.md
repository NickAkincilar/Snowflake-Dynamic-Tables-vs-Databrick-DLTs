# Snowflake-Dynamic-Tables-vs-Databrick-DLTs
Comparative Analysis: Snowflake Dynamic Tables (DT) vs. Databricks Declarative Pipelines (Delta Live Tables, DLTs)

The goal of this test was to execute identical data pipelines without platform-specific "tuning" that could bias the results. This benchmark utilizes the standard TPCH-SF10 dataset (with a 600M row lineitem table) to ensure the test is repeatable and transparent. I used the exact same set of tables with identical rows, with identical CDC changes & code for each platform to make things aples-to-apples with no code enhancements or changes to alter results.

Databricks objects were built as part of a DLT Pipeline which was configured to run a Job that used Table triggers. Each refresh was incremental.

# Test Methodology & Pipeline Design

![Test Architecture](https://cdn-images-1.medium.com/v2/resize:fit:2400/1*4hoeGll5NkxcE-f_quIb3g.png)

The benchmark was structured into seven distinct phases to ensure an "apples-to-apples" comparison, moving from raw data ingestion to complex business logic.

## Step-by-Step Breakdown
- **Step 1:** Raw Source Replication I created a raw landing zone using CTAS (Create Table As Select) queries to replicate the TPCH-SF10 tables. To eliminate data variance, tables were copied from Snowflake to Databricks as Delta tables, ensuring identical row counts and values. Data in these tables will later be updated during the simulation steps 4 to 7.

- **Step 2:** Staging Change Simulation I built staging tables containing four discrete batches of data. These batches were designed to simulate real-world volatility, ranging from heavy volume (thousands of changes in all 3 tables) in batches 001 & 002 to light volume (minor updates in just customerstable) on batches 003 & 004. Staging tables had the exact same data on both platforms to make sure each platform performed the exact same DML operations in each run.

- **Step 3:** Initial Pipeline Build The initial "Cold Start" of the Bronze, Silver, and Gold layers. This phase performs a full refresh of all nine tables (three per layer) to establish the baseline state before incremental changes are applied.
  - **Bronze layer:**
    This layer is identical as the RAW set of table which mimic CDC changes of source and lands it on the platform. No modifications are made to tables using basic CTAS queries. Here is an example. Note code is identical on both Snowflake & Databricks
```sql
CREATE OR REPLACE DYNAMIC TABLE BRONZE_CUSTOMER
TARGET_LAG = 'DOWNSTREAM'
REFRESH_MODE = INCREMENTAL 
WAREHOUSE = TPCH_DT_WH
AS
SELECT
  C_CUSTKEY,
  C_NAME,
  C_ADDRESS,
  C_NATIONKEY,
  C_PHONE,
  C_ACCTBAL,
  C_MKTSEGMENT,
  C_COMMENT
FROM RAW.RAW_CUSTOMER
WHERE C_CUSTKEY IS NOT NULL
```
  - **Silver layer:** 
    This layer is replicates the bronze layer tables but also performs data clean up & adds additional columns for business metrics & dimensions to be later used by the gold layer. Silver layer does not perform any table joins but simply enriches the existing bronze tables.

```sql
CREATE OR REPLACE DYNAMIC TABLE SILVER_CUSTOMER
TARGET_LAG = 'DOWNSTREAM'
REFRESH_MODE = INCREMENTAL 
WAREHOUSE = TPCH_DT_WH
AS
SELECT
  C_CUSTKEY,
  UPPER(TRIM(C_NAME)) AS C_NAME,
  TRIM(C_ADDRESS) AS C_ADDRESS,
  C_NATIONKEY,
  REGEXP_REPLACE(C_PHONE, '[^0-9-]', '') AS C_PHONE,
  C_ACCTBAL,
  UPPER(TRIM(C_MKTSEGMENT)) AS C_MKTSEGMENT,
  TRIM(C_COMMENT) AS C_COMMENT,
  CASE 
    WHEN C_ACCTBAL < 0 THEN 'NEGATIVE'
    WHEN C_ACCTBAL = 0 THEN 'ZERO'
    WHEN C_ACCTBAL BETWEEN 0 AND 1000 THEN 'LOW'
    WHEN C_ACCTBAL BETWEEN 1000 AND 5000 THEN 'MEDIUM'
    WHEN C_ACCTBAL BETWEEN 5000 AND 10000 THEN 'HIGH'
    ELSE 'VERY_HIGH'
  END AS ACCOUNT_BALANCE_TIER
FROM BRONZE.BRONZE_CUSTOMER
```
  - **Gold layer:**
    This layer is what the business will end up using for BI & Analytics. It joins various tables from silver layer to build three different aggregate level reporting tables.
```sql
CREATE OR REPLACE DYNAMIC TABLE GOLD_DAILY_SALES_SUMMARY
TARGET_LAG = '1 minute'
REFRESH_MODE = INCREMENTAL 
WAREHOUSE = TPCH_DT_WH
AS
SELECT
  o.O_ORDERDATE AS SALE_DATE,
  o.ORDER_YEAR,
  o.ORDER_QUARTER,
  o.ORDER_MONTH,
  o.ORDER_DAY_OF_WEEK,
  o.O_ORDERSTATUS,
  o.ORDER_STATUS_DESC,
  COUNT(DISTINCT o.O_ORDERKEY) AS TOTAL_ORDERS,
  COUNT(DISTINCT o.O_CUSTKEY) AS UNIQUE_CUSTOMERS,
  SUM(l.FINAL_PRICE) AS TOTAL_REVENUE,
  SUM(l.DISCOUNTED_PRICE) AS TOTAL_DISCOUNTED_REVENUE,
  SUM(l.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNTS_GIVEN,
  SUM(l.TAX_AMOUNT) AS TOTAL_TAX_COLLECTED,
  AVG(l.FINAL_PRICE) AS AVG_LINE_ITEM_REVENUE,
  SUM(l.L_QUANTITY) AS TOTAL_QUANTITY_SOLD,
  COUNT(l.L_LINENUMBER) AS TOTAL_LINE_ITEMS,
  ROUND(AVG(l.DISCOUNT_PERCENT), 2) AS AVG_DISCOUNT_PERCENT,
  COUNT(CASE WHEN o.ORDER_SIZE_CATEGORY = 'ENTERPRISE' THEN 1 END) AS ENTERPRISE_ORDERS,
  COUNT(CASE WHEN o.ORDER_SIZE_CATEGORY = 'LARGE' THEN 1 END) AS LARGE_ORDERS,
  COUNT(CASE WHEN o.ORDER_SIZE_CATEGORY = 'MEDIUM' THEN 1 END) AS MEDIUM_ORDERS,
  COUNT(CASE WHEN o.ORDER_SIZE_CATEGORY = 'SMALL' THEN 1 END) AS SMALL_ORDERS,
  COUNT(CASE WHEN o.PRIORITY_RANK = 1 THEN 1 END) AS URGENT_ORDERS,
  ROUND(AVG(l.SHIPPING_DAYS), 1) AS AVG_SHIPPING_DAYS,
  COUNT(CASE WHEN l.DELIVERY_PERFORMANCE = 'ON_TIME' THEN 1 END) AS ON_TIME_DELIVERIES,
  ROUND(COUNT(CASE WHEN l.DELIVERY_PERFORMANCE = 'ON_TIME' THEN 1 END) * 100.0 / NULLIF(COUNT(l.L_LINENUMBER), 0), 2) AS ON_TIME_DELIVERY_RATE
FROM SILVER.SILVER_ORDERS o
LEFT JOIN SILVER.SILVER_LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY 
GROUP BY 
  o.O_ORDERDATE, o.ORDER_YEAR, o.ORDER_QUARTER, o.ORDER_MONTH, 
  o.ORDER_DAY_OF_WEEK, o.O_ORDERSTATUS, o.ORDER_STATUS_DESC
```
- **Steps 4 to 7:**
  Incremental Simulation Cycles A Python notebook executes four data batches. Between each batch, the system pauses for 180 seconds to simulate a standard production CDC (Change Data Capture) interval. This allows you to measure how efficiently each platform identified and processed incremental changes.

# Snowflake Setup
- Import the [Snowflake notebook](https://github.com/NickAkincilar/Snowflake-Dynamic-Tables-vs-Databrick-DLTs/blob/main/source_files/Snowflake_DynamicTables_Benchmark.ipynb) in to new workspaces and start running each cell.
- To disable the Dynamic table refreshes, you can manually disable the refresh for 3 GOLD layer tables which trigger refreshes for the rest of the downstream tables.

# Databricks Setup
- Stany tuned. I am trying to find a way to package the DLT pipeline so it can be deployed from a notebook.

# Results


## Performance

| Run Number | Databricks Execution Time | Snowflake Execution Time | Diff |
| :--- | :--- | :--- | :--- |
| **Full Refresh** | 374 | 146 | Snow 260% faster |
| **1** | 123 | 100 | Snow 20% faster |
| **2** | 123 | 101 | Snow 20% faster |
| **3** | 73 | 37 | Snow 200% faster |
| **4** | 83 | 38 | Snow 220% faster |
| **5** | 6 | 30 | Not needed |


## Cost
| Category | Databricks | Snowflake Enterprise | Snowflake Standard | % Diff |
| :--- | :--- | :--- | :--- | :--- |
| **Including full refresh** | $1.64 | $0.49 | $0.32 | Snow 3X to 5X Cheaper |
| **Incremental runs only** | $0.91 | $0.33 | $0.22 | Snow 3X to 4X Cheaper |
