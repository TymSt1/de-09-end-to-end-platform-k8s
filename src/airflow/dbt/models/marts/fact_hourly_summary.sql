-- Fact: hourly performance summary
select
    event_date,
    event_hour,
    count(*) as trip_count,
    round(avg(total_amount)::numeric, 2) as avg_fare,
    round(sum(total_amount)::numeric, 2) as hourly_revenue,
    round(avg(trip_distance_miles)::numeric, 2) as avg_distance,
    sum(passenger_count) as total_passengers,
    round(avg(tip_percentage)::numeric, 2) as avg_tip_pct,
    count(case when is_rush_hour then 1 end) as rush_hour_trips,
    count(case when distance_category = 'long' then 1 end) as long_trips
from {{ ref('int_rides_enriched') }}
group by event_date, event_hour
order by event_date, event_hour