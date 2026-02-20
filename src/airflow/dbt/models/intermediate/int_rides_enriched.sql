-- Enriched rides with derived business categories
select
    ride_id,
    event_ts,
    event_date,
    event_hour,
    pickup_zone_id,
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_id,
    dropoff_zone_name,
    dropoff_borough,
    passenger_count,
    trip_distance_miles,
    duration_minutes,
    payment_type,
    rate_code,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    fare_per_mile,
    fare_per_minute,
    tip_percentage,
    is_rush_hour,
    is_weekend,

    -- Distance category
    case
        when trip_distance_miles < 2 then 'short'
        when trip_distance_miles < 10 then 'medium'
        else 'long'
    end as distance_category,

    -- Fare category
    case
        when total_amount < 15 then 'budget'
        when total_amount < 40 then 'standard'
        else 'premium'
    end as fare_category,

    -- Tip category
    case
        when tip_percentage = 0 then 'no_tip'
        when tip_percentage < 15 then 'low_tip'
        when tip_percentage < 20 then 'standard_tip'
        else 'generous_tip'
    end as tip_category,

    -- Borough flow
    pickup_borough || ' → ' || dropoff_borough as borough_flow

from {{ ref('stg_rides') }}