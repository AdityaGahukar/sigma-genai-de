import logging
import os
import shutil
import json
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, sum, count, max, broadcast, when, coalesce, mode, to_date, lit

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("Starting ingest_bronze stage")
        transactions_df = (spark.read.format("csv")
                          .option("header", "true")
                          .option("inferSchema", "false")
                          .load(input_path)
                          .withColumn("ingestion_timestamp", lit(run_date))
                          .withColumn("source_file", lit("transactions.csv"))
                          .withColumn("pipeline_run_id", lit(run_id)))
        
        merchants_df = (spark.read.format("csv")
                        .option("header", "true")
                        .option("inferSchema", "false")
                        .load(input_path.replace("transactions", "merchants"))
                        .withColumn("ingestion_timestamp", lit(run_date))
                       .withColumn("source_file", lit("merchants.csv"))
                       .withColumn("pipeline_run_id", lit(run_id)))
        
        transactions_partition_path = os.path.join(output_path, "transactions", f"ingestion_timestamp={run_date}")
        merchants_partition_path = os.path.join(output_path, "merchants", f"ingestion_timestamp={run_date}")
        
        shutil.rmtree(transactions_partition_path, ignore_errors=True)
        shutil.rmtree(merchants_partition_path, ignore_errors=True)
        
        transactions_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(os.path.join(output_path, "transactions"))
        merchants_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(os.path.join(output_path, "merchants"))
        
        logging.info(f"[Stage: ingest_bronze] transactions: {transactions_df.count():,} rows")
        logging.info(f"[Stage: ingest_bronze] merchants: {merchants_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in ingest_bronze stage: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("Starting transform_silver stage")
        transactions_df = (spark.read.format("parquet")
                           .load(bronze_path)
                          .where(col("ingestion_timestamp") == run_date))
        
        merchants_df = (spark.read.format("parquet")
                        .load(merchants_path)
                       .where(col("ingestion_timestamp") == run_date)
                       .cache())  # Cache the small merchants dataframe
        
        transactions_df = transactions_df.withColumn("amount", col("amount").cast(FloatType()))
        transactions_df = transactions_df.withColumn("transaction_date", col("transaction_date").cast(DateType()))
        transactions_df = transactions_df.withColumn("transaction_id", col("transaction_id").cast(StringType()))
        transactions_df = transactions_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))
        
        transactions_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
        logging.info(f"[Stage: transform_silver] after_filter: {transactions_df.count():,} rows")
        
        transactions_dedup_df = (transactions_df.groupBy("transaction_id")
                                 .agg({"ingestion_timestamp": "max"})
                                 .withColumnRenamed("max(ingestion_timestamp)", "ingestion_timestamp"))
        
        transactions_enriched_df = (transactions_df.join(broadcast(merchants_df), transactions_df.merchant_id == merchants_df.merchant_id, "left_outer")
                                    .withColumn("quality_flag", 
                                                when(col("merchant_id").isNull(), "UNMATCHED")
                                                .otherwise("CLEAN")))
        
        partition_path = os.path.join(output_path, f"transaction_date={run_date}")
        shutil.rmtree(partition_path, ignore_errors=True)
        
        transactions_enriched_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
        logging.info(f"[Stage: transform_silver] output: {transactions_enriched_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in transform_silver stage: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting build_merchant_performance stage")
        silver_df = spark.read.parquet(silver_path).where(col("transaction_date") == run_date)  # Partition pruning
        
        merchant_performance_df = (
            silver_df.filter(col("status") == "COMPLETED")
            .groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date")
            .agg(
                sum(col("amount")).alias("total_revenue"),
                count("*").alias("txn_count"),
                count(when(col("status") == "FAILED", 1)).alias("failed_txns"),
                count("*").alias("total_txns")
            )
           .withColumn("failure_rate_pct", (col("failed_txns") / col("total_txns") * 100).cast("float"))
           .drop("failed_txns")
        )
        
        partition_path = os.path.join(output_path, f"transaction_date={run_date}")
        shutil.rmtree(partition_path, ignore_errors=True)
        
        merchant_performance_df.repartition("transaction_date").write.mode("overwrite").parquet(output_path)
        logging.info(f"[Stage: build_merchant_performance] output: {merchant_performance_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in build_merchant_performance stage: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("Starting build_customer_ltv stage")
        silver_df = spark.read.parquet(silver_path).filter(col("status") == "COMPLETED")
        
        customer_ltv_df = (
            silver_df.groupBy("customer_id")
           .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                min("transaction_date").alias("first_txn_date"),
                max("transaction_date").alias("last_txn_date"),
                mode("payment_method").over(Window.partitionBy("customer_id")).alias("preferred_payment_method")
            )
        )
        
        partition_path = output_path
        shutil.rmtree(partition_path, ignore_errors=True)
        
        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        logging.info(f"[Stage: build_customer_ltv] output: {customer_ltv_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in build_customer_ltv stage: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting build_daily_summary stage")
        silver_df = spark.read.parquet(silver_path).where(col("transaction_date") == run_date)  # Partition pruning
        
        daily_summary_df = (
            silver_df.groupBy("transaction_date")
           .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
                count("*").alias("total_txns"),
                count(col("customer_id").distinct()).alias("unique_customers"),
                count(col("merchant_id").distinct()).alias("unique_merchants"),
                count(when(col("status") == "FAILED", 1)).alias("failed_txns"),
                count("*").alias("total_txns")
            )
           .withColumn("failure_rate_pct", (col("failed_txns") / col("total_txns") * 100).cast("float"))
           .drop("failed_txns")
        )
        
        partition_path = os.path.join(output_path, f"transaction_date={run_date}")
        shutil.rmtree(partition_path, ignore_errors=True)
        
        daily_summary_df.repartition("transaction_date").write.mode("overwrite").parquet(output_path)
        logging.info(f"[Stage: build_daily_summary] output: {daily_summary_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in build_daily_summary stage: {e}")
        raise

def main():
    try:
        logging.info("Starting main function")
        spark = (SparkSession.builder
                .appName("Sigma DataTech Transaction Analytics Pipeline")
                 .getOrCreate())
        
        input_path = "s3://path/to/input/data"
        bronze_path = "s3://path/to/bronze/data"
        silver_path = "s3://path/to/silver/data"
        merchants_path = "s3://path/to/merchants/data"
        gold_output_dir = "s3://path/to/gold/data"
        run_date = "2026-05-27"
        run_id = "run_id_12345"
        
        started_at = datetime.now().isoformat()
        
        ingest_bronze(spark, input_path, bronze_path, run_date, run_id)
        transform_silver(spark, bronze_path, merchants_path, silver_path, run_date)
        
        run_gold(spark, silver_path, gold_output_dir, run_date)
        
        completed_at = datetime.now().isoformat()
        
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": run_id,
            "run_status": "SUCCESS",
            "started_at": started_at,
            "completed_at": completed_at
        }
        
        with open(f"s3://path/to/metadata/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)
    except Exception as e:
        completed_at = datetime.now().isoformat()
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": run_id,
            "run_status": "FAILED",
            "error_message": str(e),
            "started_at": started_at,
            "completed_at": completed_at
        }
        
        with open(f"s3://path/to/metadata/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)
        
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("Starting run_gold function")
        build_merchant_performance(spark, silver_path, f"{gold_output_dir}/merchant_performance", run_date)
        build_customer_ltv(spark, silver_path, f"{gold_output_dir}/customer_ltv")
        build_daily_summary(spark, silver_path, f"{gold_output_dir}/daily_summary", run_date)
    except Exception as e:
        logging.error(f"Error in run_gold function: {e}")
        raise

if __name__ == "__main__":
    main()
