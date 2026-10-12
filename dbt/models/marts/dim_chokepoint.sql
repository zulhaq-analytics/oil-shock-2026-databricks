with ids as (
    select distinct chokepoint_id, chokepoint_name
    from {{ ref('stg_portwatch__chokepoints') }}
)

select
    i.chokepoint_id,
    i.chokepoint_name,
    coalesce(r.route_region, 'Other')                as route_region,
    coalesce(r.oil_relevant, false)                  as oil_relevant
from ids i
left join {{ ref('ref_chokepoints') }} r
    on i.chokepoint_name = r.chokepoint_name
