-- Canonical attrition rate calculation
-- This query demonstrates how to calculate attrition rate for any time period

WITH period_params AS (
  SELECT 
    '2024-01-01'::date AS period_start,
    '2024-12-31'::date AS period_end
),

-- Get terminations in the period
terminations AS (
  SELECT 
    COUNT(*) AS termination_count,
    EXTRACT(year FROM termination_date) AS year,
    EXTRACT(quarter FROM termination_date) AS quarter
  FROM marts.people.fct_attrition_events
  CROSS JOIN period_params
  WHERE termination_date BETWEEN period_start AND period_end
  GROUP BY year, quarter
),

-- Get average headcount for the period
-- This is a simplified calculation - in practice you'd want daily headcount snapshots
avg_headcount AS (
  SELECT 
    COUNT(*) AS total_employees,
    -- Approximate average headcount
    COUNT(*) AS avg_headcount_estimate
  FROM marts.people.dim_employees
  WHERE is_active = true
),

-- Calculate attrition rate
attrition_calculation AS (
  SELECT 
    t.year,
    t.quarter,
    t.termination_count,
    h.avg_headcount_estimate,
    ROUND(
      (t.termination_count::float / h.avg_headcount_estimate::float) * 100, 
      2
    ) AS attrition_rate_pct
  FROM terminations t
  CROSS JOIN avg_headcount h
)

SELECT 
  year,
  quarter,
  termination_count,
  avg_headcount_estimate,
  attrition_rate_pct,
  CASE 
    WHEN attrition_rate_pct > 15 THEN 'High'
    WHEN attrition_rate_pct > 10 THEN 'Moderate' 
    ELSE 'Low'
  END AS attrition_risk_level
FROM attrition_calculation
ORDER BY year, quarter;