{{ config(severity = 'warn') }}
-- Warns if EIA's monthly average Brent physical differs from the IMF monthly Brent by more than 5% (from 2019).
with eia as (
    select date_trunc('MONTH', price_date) as month_start, avg(price) as eia_brent
    from {{ ref('fct_price_daily') }}
    where series_id = 'RBRTE'
    group by 1
),
imf as (
    select month_start, price as imf_brent
    from {{ ref('fct_price_monthly') }}
    where series_id = 'POILBREUSDM'
)
select
    i.month_start,
    round(e.eia_brent, 2)                                   as eia_brent,
    round(i.imf_brent, 2)                                   as imf_brent,
    round(100 * (e.eia_brent - i.imf_brent) / i.imf_brent, 1) as pct_diff
from imf i
join eia e on cast(i.month_start as date) = cast(e.month_start as date)
where i.month_start >= '2019-01-01'
  and abs(e.eia_brent - i.imf_brent) / i.imf_brent > 0.05
