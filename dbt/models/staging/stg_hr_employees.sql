{{ config(
    materialized='view',
    docs={'node_color': '#F7931E'}
) }}

with source_data as (
    select * from {{ ref('hr_employees') }}
),

cleaned as (
    select
        employee_id,
        first_name,
        last_name,
        email,
        department,
        job_title,
        hire_date,
        birth_date,
        gender,
        salary,
        manager_id,
        region_id,
        status,
        
        -- Calculated fields
        concat(first_name, ' ', last_name) as full_name,
        
        -- Tenure calculation
        {{ datediff('hire_date', 'current_date()', 'day') }} as tenure_days,
        round({{ datediff('hire_date', 'current_date()', 'day') }} / 365.25, 1) as tenure_years,
        
        -- Age calculation
        {{ datediff('birth_date', 'current_date()', 'year') }} as age_years,
        
        -- Flags
        case when status = '{{ var("active_status") }}' then true else false end as is_active,
        case when manager_id is null then true else false end as is_manager,
        
        -- Salary bands
        case 
            when salary < 50000 then 'Entry Level'
            when salary < 75000 then 'Mid Level'
            when salary < 100000 then 'Senior Level'
            else 'Executive Level'
        end as salary_band,
        
        current_timestamp() as _loaded_at
        
    from source_data
)

select * from cleaned