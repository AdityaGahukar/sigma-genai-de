# Pipeline Overview

This pipeline processes transaction data, transforming it from bronze to silver and finally to gold layers. It runs to ensure data is cleansed, enriched, and summarized for reporting and analytics. If this pipeline stops, downstream reports and dashboards will lack updated data, impacting decision-making.

## Pipeline Steps

1. **get_connection()** - Establishes a connection to the DuckDB database.
2. **setup_tables(con)** - Sets up necessary tables in the database.
3. **load_merchants(con)** - Loads merchant data into the `merchants` table.
4. **load_bronze(con, transactions)** - Loads raw transaction data into the `bronze_transactions` table.
5. **transform_bronze_to_silver(transactions, merchants)** - Transforms bronze transactions into silver transactions.
6. **load_silver(con, silver_rows)** - Loads transformed transactions into the `silver_transactions` table.
7. **compute_merchant_performance(silver_rows)** - Computes merchant performance metrics.
8. **compute_daily_summary(silver_rows)** - Computes daily summary metrics.
9. **load_gold(con, merchant_perf, daily_summary)** - Loads merchant performance and daily summary into gold tables.

## Schedule / Trigger

This pipeline runs every night at 2 AM IST. It is triggered by a cron job on the server.

## Failure Modes

1. **Database Connection Failure** - Root cause: Network issues or database server down. Symptom: Pipeline logs show "Connection refused."
2. **Table Setup Failure** - Root cause: SQL syntax error. Symptom: Pipeline logs show "Syntax error."
3. **Merchant Data Load Failure** - Root cause: Corrupt merchant data. Symptom: Pipeline logs show "Data load failed."
4. **Bronze Data Load Failure** - Root cause: Missing transaction fields. Symptom: Pipeline logs show "Missing field error."
5. **Silver Transformation Failure** - Root cause: Invalid transaction data. Symptom: Pipeline logs show "Transformation error."

## Recovery Actions

1. **Database Connection Failure**
   - Check network connectivity.
   - Restart the database server.
   - Retry the pipeline.
2. **Table Setup Failure**
   - Review and correct the SQL syntax.
   - Retry the pipeline.
3. **Merchant Data Load Failure**
   - Validate and correct the merchant data.
   - Retry the pipeline.
4. **Bronze Data Load Failure**
   - Ensure all required fields are present in transaction data.
   - Retry the pipeline.
5. **Silver Transformation Failure**
   - Investigate and correct invalid transaction data.
   - Retry the pipeline.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver` function.

## Escalation Contacts

1. **On-call DE:** Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead:** Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager:** Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

After a successful run, verify:
- The number of records in `silver_transactions` matches expected counts.
- Merchant performance metrics are reasonable.
- Daily summary metrics align with expected values.