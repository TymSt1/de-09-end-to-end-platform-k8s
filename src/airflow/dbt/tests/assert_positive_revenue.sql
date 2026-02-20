-- Custom test: ensure all zones have positive revenue
select
    pickup_zone_id,
    total_revenue
from {{ ref('dim_zones') }}
where total_revenue <= 0