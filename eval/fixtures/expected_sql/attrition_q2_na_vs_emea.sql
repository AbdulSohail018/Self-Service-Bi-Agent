-- Expected SQL for: "Show me attrition trends for Q2 in North America vs EMEA"
SELECT 
    r.region_name,
    DATE_TRUNC('month', ae.termination_date) as month,
    COUNT(*) as terminations,
    COUNT(*) * 100.0 / LAG(COUNT(*)) OVER (PARTITION BY r.region_name ORDER BY DATE_TRUNC('month', ae.termination_date)) - 100 as growth_rate_pct
FROM marts.people.fct_attrition_events ae
JOIN marts.people.dim_employees e ON ae.employee_id = e.employee_id  
JOIN seeds.hr_regions r ON e.region_id = r.region_id
WHERE ae.termination_date >= '2024-04-01' 
  AND ae.termination_date < '2024-07-01'
  AND r.region_name IN ('North America', 'EMEA')
GROUP BY r.region_name, DATE_TRUNC('month', ae.termination_date)
ORDER BY month, r.region_name
LIMIT 1000;