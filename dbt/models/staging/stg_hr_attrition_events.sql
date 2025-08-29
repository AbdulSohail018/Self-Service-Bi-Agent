{{ config(
    materialized='view',
    docs={'node_color': '#F7931E'}
) }}

with source_data as (
    select * from {{ ref('hr_attrition_events') }}
),

cleaned as (
    select
        event_id,
        employee_id,
        termination_date,
        termination_type,
        reason_category,
        voluntary,
        exit_interview_completed,
        
        -- Date extractions
        extract(year from termination_date) as termination_year,
        extract(month from termination_date) as termination_month,
        extract(quarter from termination_date) as termination_quarter,
        
        -- Flags
        case when voluntary = true then 'Voluntary' else 'Involuntary' end as voluntary_flag,
        case when exit_interview_completed = true then 'Completed' else 'Not Completed' end as exit_interview_flag,
        
        current_timestamp() as _loaded_at
        
    from source_data
)

select * from cleaned