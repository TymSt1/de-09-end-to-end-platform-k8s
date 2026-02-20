-- Dimension: payment type analysis
select
    payment_type,
    count(*) as trip_count,
    round(avg(total_amount)::numeric, 2) as avg_fare,
    round(avg(tip_percentage)::numeric, 2) as avg_tip_pct,
    round(sum(total_amount)::numeric, 2) as total_revenue,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as pct_of_trips,
    round(avg(trip_distance_miles)::numeric, 2) as avg_distance,
    count(case when fare_category = 'premium' then 1 end) as premium_trips
from {{ ref('int_rides_enriched') }}
group by payment_type