{{ config(materialized='table') }}

with trips_data as (
    select *
    from {{ ref('fact_trips') }}
)
select 
    -- Revenue grouping 
    service_type,
    extract(year from pickup_datetime) as year_ind,
    ceil(extract(month from pickup_datetime) / 3) as quarter_ind,
    -- Quarterly Revenue calculation 
    sum(total_amount) as revenue_quarterly_total_amount
from trips_data
group by 1,2,3
order by 1,2,3
