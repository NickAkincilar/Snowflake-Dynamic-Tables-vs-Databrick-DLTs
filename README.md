# Snowflake-Dynamic-Tables-vs-Databrick-DLTs
Comparative Analysis: Snowflake Dynamic Tables (DT) vs. Databricks Declarative Pipelines (Delta Live Tables, DLTs)

# Snowflake 
- Import the [Snowflake notebook](https://github.com/NickAkincilar/Snowflake-Dynamic-Tables-vs-Databrick-DLTs/blob/main/source_files/Snowflake_DynamicTables_Benchmark.ipynb) in to new workspaces and start running each cell.
- To disable the Dynamic table refreshes, you can manually disable the refresh for 3 GOLD layer tables which trigger refreshes for the rest of the downstream tables.

# Databricks:
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
