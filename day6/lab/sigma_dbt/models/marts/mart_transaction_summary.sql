WITH completed_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM {{ ref('stg_transactions') }}
    WHERE status = 'COMPLETED'
),

failure_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM {{ ref('stg_transactions') }}
    WHERE status = 'FAILED'
),

total_transactions AS (
    SELECT
        COUNT(transaction_id) AS transaction_count
    FROM {{ ref('stg_transactions') }}
),

revenue_summary AS (
    SELECT
        SUM(amount) AS total_revenue
    FROM completed_transactions
),

failure_rate_summary AS (
    SELECT
        COUNT(transaction_id) AS failure_count
    FROM failure_transactions
),

failure_rate_calculation AS (
    SELECT
        (failure_count::DECIMAL / transaction_count::DECIMAL) * 100 AS failure_rate_pct
    FROM failure_rate_summary, total_transactions
),

transaction_summary AS (
    SELECT
        ct.transaction_id,
        ct.amount,
        ct.status,
        ct.transaction_date,
        ct.payment_method,
        dm.merchant_name,
        ct.customer_id
    FROM completed_transactions ct
    JOIN {{ source('sigma_analytics', 'dim_merchant') }} dm
        ON ct.merchant_id = dm.merchant_id
)

SELECT
    ts.transaction_id,
    ts.merchant_name,
    rs.total_revenue,
    frc.failure_rate_pct,
    tt.transaction_count
FROM transaction_summary ts
JOIN revenue_summary rs ON 1=1
JOIN failure_rate_calculation frc ON 1=1
JOIN total_transactions tt ON 1=1