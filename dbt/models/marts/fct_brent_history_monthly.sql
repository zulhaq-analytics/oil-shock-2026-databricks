with p as (
    select
        cast(date_trunc('month', price_date) as date) as month_start,
        avg(price)                                    as brent_nominal,
        count(*)                                      as trading_days
    from {{ ref('fct_price_daily') }}
    where series_id = 'RBRTE' and price is not null
    group by 1
),

c as (
    select obs_date as month_start, value as cpi
    from {{ ref('stg_fred__observations') }}
    where series_id = 'CPIAUCSL'
),

last_c as (
    select max_by(cpi, month_start) as cpi_last, max(month_start) as cpi_last_month
    from c
),

e as (
    select cast(event_month as date) as month_start, event_label
    from {{ ref('ref_oil_history_events') }}
)

select
    p.month_start,
    p.brent_nominal,
    p.trading_days,
    coalesce(c.cpi, l.cpi_last)                                  as cpi,
    p.brent_nominal * l.cpi_last / coalesce(c.cpi, l.cpi_last)   as brent_real,
    l.cpi_last_month                                             as real_dollars_as_of,
    e.event_label
from p
cross join last_c l
left join c on c.month_start = p.month_start
left join e on e.month_start = p.month_start
