{{ config(
    materialized='table',
    docs={'node_color': '#80CE5D'}
) }}

with attrition_events as (
    select * from {{ ref('stg_hr_attrition_events') }}
),

employees as (
    select * from {{ ref('dim_employees') }}
),

final as (
    select
        a.event_id,
        a.employee_id,
        a.termination_date,
        a.termination_type,
        a.reason_category,
        a.voluntary,
        a.exit_interview_completed,
        a.termination_year,
        a.termination_month,
        a.termination_quarter,
        a.voluntary_flag,
        a.exit_interview_flag,
        
        -- Employee information at time of termination
        e.full_name,
        e.department,
        e.job_title,
        e.hire_date,
        e.gender,
        e.salary,
        e.salary_band,
        e.manager_name,
        e.region_id,
        e.region_name,
        e.region_group,
        
        -- Calculate tenure at termination
        {{ datediff('e.hire_date', 'a.termination_date', 'day') }} as tenure_at_termination_days,
        round({{ datediff('e.hire_date', 'a.termination_date', 'day') }} / 365.25, 1) as tenure_at_termination_years,
        
        -- Tenure category at termination
        case
            when {{ datediff('e.hire_date', 'a.termination_date', 'day') }} / 365.25 < 1 then '< 1 year'
            when {{ datediff('e.hire_date', 'a.termination_date', 'day') }} / 365.25 < 3 then '1-3 years'
            when {{ datediff('e.hire_date', 'a.termination_date', 'day') }} / 365.25 < 5 then '3-5 years'
            when {{ datediff('e.hire_date', 'a.termination_date', 'day') }} / 365.25 < 10 then '5-10 years'
            else '10+ years'
        end as tenure_category_at_termination,
        
        -- Time-based flags
        case when a.termination_date >= current_date() - interval '12 months' then true else false end as is_recent_termination,
        case when a.termination_date >= current_date() - interval '3 months' then true else false end as is_very_recent_termination,
        
        -- Risk indicators
        case
            when a.reason_category in ('Performance', 'Restructuring') then 'High Risk'
            when a.reason_category in ('Compensation', 'Work-Life Balance') then 'Medium Risk'
            else 'Low Risk'
        end as attrition_risk_indicator,
        
        current_timestamp() as _updated_at
        
    from attrition_events a
    join employees e on a.employee_id = e.employee_id
)

select * from final