CREATE OR REPLACE MATERIALIZED VIEW my_uc_catalog.dlt_test.BRONZE_CUSTOMER
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
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
FROM my_uc_catalog.dlt_test.RAW_CUSTOMER
WHERE C_CUSTKEY IS NOT NULL;



