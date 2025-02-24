{{
    config(
        materialized='table'
    )
}}

with dim_fhv as (
    select *
    from {{ ref('dim_fhv_trips') }}
)
 

select *, 
       TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, second) as trip_duration
from dim_fhv
