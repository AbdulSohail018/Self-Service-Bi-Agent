{{ config(
    materialized='view',
    docs={'node_color': '#F7931E'}
) }}

with source_data as (
    select * from {{ ref('hr_regions') }}
),

cleaned as (
    select
        region_id,
        region_name,
        region_code,
        timezone,
        country_count,
        
        -- Standardized region grouping
        case
            when region_id = 'NA' then 'Americas'
            when region_id = 'EMEA' then 'Europe & Africa'
            when region_id = 'APAC' then 'Asia Pacific'
            else 'Other'
        end as region_group,
        
        current_timestamp() as _loaded_at
        
    from source_data
)

select * from cleaned