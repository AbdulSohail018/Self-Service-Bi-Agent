{{ config(
    materialized='table',
    docs={'node_color': '#80CE5D'}
) }}

with employees as (
    select * from {{ ref('stg_hr_employees') }}
),

regions as (
    select * from {{ ref('stg_hr_regions') }}
),

-- Add manager information
managers as (
    select
        employee_id as manager_employee_id,
        full_name as manager_name,
        department as manager_department
    from employees
    where is_manager = true
),

final as (
    select
        e.employee_id,
        e.first_name,
        e.last_name,
        e.full_name,
        e.email,
        e.department,
        e.job_title,
        e.hire_date,
        e.birth_date,
        e.gender,
        e.salary,
        e.manager_id,
        e.region_id,
        e.status,
        e.tenure_days,
        e.tenure_years,
        e.age_years,
        e.is_active,
        e.is_manager,
        e.salary_band,
        
        -- Manager information
        m.manager_name,
        m.manager_department,
        
        -- Region information
        r.region_name,
        r.region_code,
        r.region_group,
        r.timezone as region_timezone,
        
        -- Tenure categorization
        case
            when e.tenure_years < 1 then '< 1 year'
            when e.tenure_years < 3 then '1-3 years'
            when e.tenure_years < 5 then '3-5 years'
            when e.tenure_years < 10 then '5-10 years'
            else '10+ years'
        end as tenure_category,
        
        -- Age categorization
        case
            when e.age_years < 25 then '< 25'
            when e.age_years < 35 then '25-34'
            when e.age_years < 45 then '35-44'
            when e.age_years < 55 then '45-54'
            else '55+'
        end as age_category,
        
        -- Employment status details
        case
            when e.status = 'Active' then 'Currently Employed'
            when e.status = 'Terminated' then 'No Longer Employed'
            else 'Unknown Status'
        end as employment_status_desc,
        
        current_timestamp() as _updated_at
        
    from employees e
    left join regions r on e.region_id = r.region_id
    left join managers m on e.manager_id = m.manager_employee_id
)

select * from final