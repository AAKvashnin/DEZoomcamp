select service_type, count(*)/sum(count(*)) over () as ratio
from dbt_aakvashnin.fact_trips
where pickup_datetime>='2019-01-01' and pickup_datetime<'2021-01-01'
group by service_type;