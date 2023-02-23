{{ config(materialized='view') }}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata') }}
)
select
    -- identifiers
    pulocationid as  pickup_locationid,
    dolocationid as  dropoff_locationid,
    
    -- timestamps
    pickup_datetime as pickup_datetime,
    dropoff_datetime as dropoff_datetime,
    
    -- trip info
    sr_flag,
    dispatching_base_num,
    affiliated_base_number as affiliated_base_num
from tripdata


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
