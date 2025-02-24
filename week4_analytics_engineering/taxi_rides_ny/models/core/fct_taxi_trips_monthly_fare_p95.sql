{{ config(materialized='table') }}

with trips_data as (
    select *
    from {{ ref('fact_trips') }}
)
select 
    -- Revenue grouping 
    service_type,
    pickup_datetime,
    extract(year from pickup_datetime) as year_ind,
    extract(month from pickup_datetime) as month_ind,
    fare_amount
from trips_data
where fare_amount > 0 and trip_distance > 0 and lower(payment_type_description) in ('cash', 'credit card')