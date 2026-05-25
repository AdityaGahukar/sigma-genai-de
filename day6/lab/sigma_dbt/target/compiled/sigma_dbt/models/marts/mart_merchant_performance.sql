WITH filtered_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM SIGMA_DE.PUBLIC.stg_fact_transactions
    WHERE status IN ('COMPLETED', 'FAILED')
),

merchant_details AS (
    SELECT
        merchant_id,
        merchant_name,
        category,
        city,
        onboarded_date
    FROM SIGMA_DE.PUBLIC.dim_merchant
),

aggregated_metrics AS (
    SELECT
        ft.merchant_id,
        COUNT(ft.transaction_id) AS total_transactions,
        COUNT(CASE WHEN ft.status = 'FAILED' THEN 1 END) AS failed_count,
        SUM(CASE WHEN ft.status = 'COMPLETED' THEN ft.amount ELSE 0 END) AS total_revenue,
        COUNT(DISTINCT ft.customer_id) AS unique_customers
    FROM filtered_transactions ft
    GROUP BY ft.merchant_id
),

avg_transaction_value AS (
    SELECT
        merchant_id,
        AVG(amount) AS avg_transaction_value
    FROM filtered_transactions
    WHERE status = 'COMPLETED'
    GROUP BY merchant_id
),

final_metrics AS (
    SELECT
        am.merchant_id,
        md.merchant_name,
        md.category,
        md.city,
        md.onboarded_date,
        am.total_transactions,
        am.failed_count,
        am.total_revenue,
        am.unique_customers,
        atv.avg_transaction_value,
        (am.failed_count::FLOAT / am.total_transactions) * 100 AS failure_rate_pct
    FROM aggregated_metrics am
    JOIN merchant_details md ON am.merchant_id = md.merchant_id
    JOIN avg_transaction_value atv ON am.merchant_id = atv.merchant_id
)

SELECT * FROM final_metrics