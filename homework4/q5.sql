select to_char(pickup_datetime,'Month'), count(*) from dbt_aakvashnin.fact_fhv_trips group by to_char(pickup_datetime,'Month') order by count(*) desc;