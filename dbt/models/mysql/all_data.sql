{{ config(materialized='table') }}

with sensor_data as (
    select * from {{ source('dwh', 'sensor') }}
),

final as(
    select * from sensor_data
)

select * from final 