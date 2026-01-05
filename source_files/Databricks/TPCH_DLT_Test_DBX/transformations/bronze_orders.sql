CREATE OR REPLACE MATERIALIZED VIEW my_uc_catalog.dlt_test.BRONZE_ORDERS
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  O_ORDERKEY,
  O_CUSTKEY,
  O_ORDERSTATUS,
  O_TOTALPRICE,
  O_ORDERDATE,
  O_ORDERPRIORITY,
  O_CLERK,
  O_SHIPPRIORITY,
  O_COMMENT
FROM my_uc_catalog.dlt_test.RAW_ORDERS
WHERE O_ORDERKEY IS NOT NULL;


