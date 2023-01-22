/* Question 3 */
select count(*)
	   from green_taxi_trips
where lpep_pickup_datetime>='2019-01-15'
and lpep_dropoff_datetime<'2019-01-16';

/*Question 4 */
select lpep_pickup_datetime, lpep_dropoff_datetime, trip_distance
from green_taxi_trips
order by trip_distance desc;

/*Question 5 */
select passenger_count, count(*) from green_taxi_trips
where lpep_pickup_datetime>='2019-01-01'
and lpep_pickup_datetime<'2019-01-02'
and passenger_count in (2,3)
group by passenger_count;

/* Question 6 */
select tip_amount, d."Zone"
from green_taxi_trips t
join taxi_zone_lookup p on t."PULocationID"=p."LocationID"
join taxi_zone_lookup d on t."DOLocationID"=d."LocationID"
where p."Zone"='Astoria'
order by tip_amount desc
limit 10;