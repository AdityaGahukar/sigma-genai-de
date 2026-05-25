# NL2SQL vs Cortex Analyst — Sigma DataTech Evaluation
Team: Sigma Interns
Date: 2026-05-25

## 5-Question Head-to-Head Results

| # | Question | Module 2 SQL Correct? | Cortex SQL Correct? | Module 2 Time | Cortex Time |
|---|----------|-----------------------|---------------------|---------------|-------------|
| 1 | Total transaction count | YES | YES | ~2s | 28.2s |
| 2 | Failed transaction count | YES | YES | ~2s | 71.2s |
| 3 | Highest revenue merchant | YES | YES | ~2s | 64.3s |
| 4 | Failure rate by payment method | YES | YES | ~2s | 31.3s |
| 5 | Total revenue (with COMPLETED filter) | YES | YES | ~2s | 27.0s |

## Observations

### Where Module 2 NL2SQL was better:
- Faster response times in the small lab (lower latency per question).
- Fine-grained audit logging and an explicit validator that blocks unsafe SQL (e.g., rejected `DROP TABLE`).
- Easier to instrument custom checks, validators, and bespoke preprocessing logic.

### Where Cortex Analyst was better:
- Stronger data residency and governance — SQL generation and execution stay inside Snowflake.
- Maintains a canonical semantic model (YAML) with metrics and verified queries, reducing prompt engineering and drift.
- Consistent business-rule application via `metrics` (less risk of omission for rules like revenue = COMPLETED only).

### Business Rule Accuracy
Question 5 is the critical test — revenue must only count COMPLETED transactions.
- Module 2: Yes — generated SQL used `CASE WHEN STATUS='COMPLETED'` / `WHERE STATUS='COMPLETED'` to compute revenue.
- Cortex: Yes — used the semantic model/metric (`STATUS = 'COMPLETED'`) when generating SQL and executing the metric.

## Your Recommendation

Recommendation: Hybrid approach — use **Cortex Analyst** as the default production self-serve layer, and retain the Module 2 NL2SQL pipeline as a developer/debugging tool and for custom validations.

Reason: Cortex offers better maintainability, governance, and data residency for production users because the semantic model centralizes business rules. Keep the NL2SQL pipeline for rapid experimentation, advanced prompt-driven logic, safety validators, and auditability where you need custom controls.

---

Results sources:
- Module 2 NL2SQL run (audit log & answers)
- Cortex Analyst run saved to `cortex_results.json`

Saved to `comparison_analysis.md` on 2026-05-25.
