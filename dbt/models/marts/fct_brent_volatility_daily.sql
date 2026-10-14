with p as (
    select price_date, avg(price) as brent_price
    from {{ ref('fct_price_daily') }}
    where series_id = 'RBRTE' and price is not null
    group by price_date
),

r as (
    select
        price_date,
        brent_price,
        ln(brent_price / lag(brent_price) over (order by price_date)) as log_return
    from p
),

w as (
    select
        price_date,
        brent_price,
        log_return,
        stddev_samp(log_return) over (order by price_date rows between 29 preceding and current row) as sd_30,
        count(log_return)       over (order by price_date rows between 29 preceding and current row) as n_30
    from r
)

select
    price_date,
    brent_price,
    log_return,
    case when n_30 >= 20 then sd_30 * sqrt(252) end as volatility_30d,
    n_30 as returns_in_window
from w
