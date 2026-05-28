# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, enriches it with merchant information, and then aggregates it into merchant performance metrics and daily summaries.

## Data Flow Diagram

```
+----------------+      +------------------+      +------------------+      +------------------+
|  Source        | ---> |  Bronze Layer     | ---> |  Silver Layer    | ---> |  Gold Layer      |
|  (Dirty & Clean)|      |  (bronze_transactions) |      |  (silver_transactions) |      |  (gold_merchant_performance, gold_daily_summary) |
+----------------+      +------------------+      +------------------+      +------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a Bronze, Silver, and Gold layer to separate raw data ingestion, data cleaning and enrichment, and aggregation, respectively.
- **Quality Flags**: Transactions are flagged as "CLEAN" or "FAILED" to distinguish between successful and failed transactions.
- **Merchant Enrichment**: Merchant details are joined with transactions to enrich the data before aggregation.
- **Aggregations**: Separate aggregations are computed for merchant performance and daily summaries to provide detailed and high-level insights.

## Known Limitations
- **Single Source**: The pipeline currently only processes data from a single source. Adding more sources would require modifications.
- **Static Merchant Data**: Merchant data is loaded once and not updated unless the pipeline is rerun. This could lead to stale merchant information.
- **No Error Handling**: The pipeline does not have robust error handling, which could lead to data loss in case of failures.
- **No Timestamps in Gold**: The gold layer does not store timestamps, which could be useful for time-series analysis.

## Dependencies
- **DuckDB**: The pipeline uses DuckDB for data storage and processing.
- **MERCHANTS**: A list of merchant data used for enriching transaction records.
- **TRANSACTIONS_CLEAN & TRANSACTIONS_DIRTY**: Lists of clean and dirty transaction data used as input.