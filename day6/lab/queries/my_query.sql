SELECT c.customer_name,
       c.email,
       SUM(t.amount) AS total_spend,
       COUNT(*) AS transaction_count
FROM dim_customer c
JOIN fact_transactions t ON c.customer_id = t.customer_id
WHERE t.status IN ('COMPLETED', 'FAILED')
GROUP BY c.customer_name, c.email
ORDER BY total_spend DESC;