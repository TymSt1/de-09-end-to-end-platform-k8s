-- Dimension: zone performance summary
select
    pickup_zone_id,
    pickup_zone_name,
    pickup_borough,
    count(*) as total_trips,
    round(avg(total_amount)::numeric, 2) as avg_fare,
    round(avg(tip_percentage)::numeric, 2) as avg_tip_pct,
    round(avg(trip_distance_miles)::numeric, 2) as avg_distance,
    round(avg(duration_minutes)::numeric, 2) as avg_duration,
    round(sum(total_amount)::numeric, 2) as total_revenue,
    round(avg(passenger_count)::numeric, 1) as avg_passengers,
    row_number() over (order by sum(total_amount) desc) as revenue_rank
from {{ ref('int_rides_enriched') }}
group by pickup_zone_id, pickup_zone_name, pickup_borough