with base as (
    select
        s.chokepoint_id,
        s.chokepoint_name,
        s.transit_date,
        s.ships_total,
        s.ships_tanker,
        s.capacity_tanker_mt,
        avg(s.ships_total)  over w7 as ships_total_7d_avg,
        avg(s.ships_tanker) over w7 as ships_tanker_7d_avg
    from {{ ref('stg_portwatch__chokepoints') }} s
    window w7 as (partition by s.chokepoint_id order by s.transit_date
                  rows between 6 preceding and current row)
),

normal_2025 as (
    select
        chokepoint_id,
        avg(ships_total)  as ships_total_2025_avg,
        avg(ships_tanker) as ships_tanker_2025_avg
    from {{ ref('stg_portwatch__chokepoints') }}
    where year(transit_date) = 2025
    group by chokepoint_id
)

select
    concat_ws('|', b.chokepoint_id, cast(b.transit_date as string)) as row_key,
    b.*,
    n.ships_total_2025_avg,
    n.ships_tanker_2025_avg,
    round(100 * b.ships_total_7d_avg  / nullif(n.ships_total_2025_avg, 0), 1)  as ships_total_pct_of_2025,
    round(100 * b.ships_tanker_7d_avg / nullif(n.ships_tanker_2025_avg, 0), 1) as ships_tanker_pct_of_2025
from base b
left join normal_2025 n using (chokepoint_id)
