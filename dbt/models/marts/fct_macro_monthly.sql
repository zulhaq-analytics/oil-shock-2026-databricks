with m as (
    select
        obs_date as month_start,
        max(case when series_id = 'CPIAUCSL' then value end) as us_cpi_index,
        max(case when series_id = 'INDPRO'   then value end) as us_industrial_production_index
    from {{ ref('stg_fred__observations') }}
    where series_id in ('CPIAUCSL', 'INDPRO')
    group by obs_date
)

select
    *,
    round(100 * (us_cpi_index / lag(us_cpi_index, 12) over (order by month_start) - 1), 2)
        as us_inflation_yoy_pct,
    round(100 * (us_industrial_production_index
                 / lag(us_industrial_production_index, 12) over (order by month_start) - 1), 2)
        as us_industrial_production_yoy_pct
from m
