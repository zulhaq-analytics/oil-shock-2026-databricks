-- Singapore bunker hub, one row per month from 2019: fuel sold, fuel mix, ship arrivals, and both vs the 2025 average
with s as (
    select * from {{ ref('stg_sg_mpa__monthly') }}
    where month_start >= '2019-01-01'
),

m as (
    select
        month_start,
        max(case when dataset = 'bunker_total' then value end)                                   as bunker_sales_kt,
        sum(case when dataset = 'bunker_by_type' and category like '%Low Sulphur Fuel Oil%'
                 then value end)                                                                  as vlsfo_kt,
        sum(case when dataset = 'bunker_by_type' and category like '%Marine Fuel Oil%'
                 then value end)                                                                  as hsfo_kt,
        sum(case when dataset = 'bunker_by_type'
                  and (category like '%Gas Oil%' or category like '%Diesel%') then value end)     as distillates_kt,
        max(case when dataset = 'arrivals_total' and measure = 'vessels' then value end)          as vessel_arrivals,
        max(case when dataset = 'arrivals_total' and measure = 'gross_tonnage_kgt' then value end) as arrivals_gt_kgt,
        max(case when dataset = 'arrivals_by_type' and measure = 'vessels'
                  and category = 'Container' then value end)                                      as container_arrivals,
        max(case when dataset = 'arrivals_by_type' and measure = 'vessels'
                  and category = 'Bulk Carrier' then value end)                                   as bulk_carrier_arrivals,
        max(case when dataset = 'arrivals_by_type' and measure = 'vessels'
                  and category = 'Tanker' then value end)                                         as tanker_arrivals
    from s
    group by month_start
),

base as (
    select avg(bunker_sales_kt) as bunker_2025_avg_kt,
           avg(vessel_arrivals)  as arrivals_2025_avg
    from m
    where year(month_start) = 2025
)

select
    m.month_start,
    m.bunker_sales_kt,
    m.vlsfo_kt,
    m.hsfo_kt,
    m.distillates_kt,
    m.bunker_sales_kt - coalesce(m.vlsfo_kt, 0) - coalesce(m.hsfo_kt, 0)
                      - coalesce(m.distillates_kt, 0)                    as other_fuels_kt,
    m.vessel_arrivals,
    m.arrivals_gt_kgt,
    m.container_arrivals,
    m.bulk_carrier_arrivals,
    m.tanker_arrivals,
    m.bunker_sales_kt * 1000 / nullif(m.vessel_arrivals, 0)              as bunker_t_per_arrival,
    b.bunker_2025_avg_kt,
    b.arrivals_2025_avg,
    m.bunker_sales_kt / nullif(b.bunker_2025_avg_kt, 0)                  as bunker_pct_of_2025,
    m.vessel_arrivals / nullif(b.arrivals_2025_avg, 0)                   as arrivals_pct_of_2025
from m
cross join base b
